use super::*;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn target(id: i32, phase: &str) -> ExternalTerminationTargetStatus {
    ExternalTerminationTargetStatus {
        node_id: id,
        pod_uid: format!("uid-{id}"),
        boot_id: format!("boot-{id}"),
        deadline_at: (Utc::now() + chrono::Duration::seconds(120)).to_rfc3339(),
        phase: phase.into(),
        ..Default::default()
    }
}

#[test]
fn broker_deadline_only_shrinks_and_uses_probe_start() {
    let start = Utc::now();
    let mut state = target(2, "Binding");
    shorten_deadline(&mut state, start, Duration::from_secs(100));
    let deadline = state.deadline_at.clone();
    shorten_deadline(&mut state, start + chrono::Duration::seconds(2), Duration::from_secs(100));
    assert_eq!(state.deadline_at, deadline);
    assert!(external_target_remaining(&state) <= Duration::from_secs(100));
}

#[test]
fn serialized_owner_survives_earlier_new_deadline_and_restart() {
    let mut owner = target(2, "RemovingBusinessMember");
    owner.membership_owner = true;
    let mut later = target(3, "WaitingMembershipRemoval");
    later.deadline_at = Utc::now().to_rfc3339();
    let encoded = serde_json::to_value(vec![later, owner]).unwrap();
    let states: Vec<ExternalTerminationTargetStatus> = serde_json::from_value(encoded).unwrap();
    assert_eq!(external_membership_owner(&states), Some(1));
}

#[test]
fn finalizer_keeps_removal_phase_monotonic_and_unfenced_absence_never_removes_id() {
    let mut state = target(2, "RemovingBusinessMember");
    state.membership_owner = true;
    let pod: Pod = serde_json::from_value(pod_json(2, true)).unwrap();
    observe_missing_pod(&mut state, &[pod], "rmqtt");
    assert_eq!(state.phase, "RemovingBusinessMember");
    observe_missing_pod(&mut state, &[], "rmqtt");
    assert_eq!(state.phase, "Completed");
    assert!(!state.membership_owner);
}

fn pod_json(id: i32, deleting: bool) -> Value {
    let mut pod = json!({"metadata": {"name": format!("rmqtt-{}", id-1), "uid": format!("uid-{id}"), "resourceVersion":"1", "finalizers":[FINALIZER],
        "ownerReferences":[{"kind":"StatefulSet", "name":"rmqtt", "uid":"sts", "controller":true}]},
        "status":{"phase":"Running", "conditions":[{"type":"Ready", "status":"True"}]}});
    if deleting {
        pod["metadata"]["deletionTimestamp"] =
            json!((Utc::now() + chrono::Duration::seconds(120)).to_rfc3339());
    }
    pod
}

struct State {
    pods: BTreeMap<i32, Value>,
    status: Value,
    voters: Vec<i32>,
    learners: Vec<i32>,
    business: Vec<i32>,
    leader: i32,
    requests: Vec<(i32, Value)>,
    fail_prepare: usize,
    fail_leave: usize,
    slow_status: Option<i32>,
    persisted_batches: Vec<Value>,
    cluster_patches: Vec<Value>,
    data_loss: bool,
    sts_replicas: i32,
    template_has_finalizer: bool,
    fail_clear_on: Option<i32>,
    allow_unconfirmed_finalizer_release: bool,
    events: Vec<String>,
    stateful_set_patches: Vec<Value>,
    stateful_set: Option<Value>,
}

fn merge(value: &mut Value, patch: &Value) {
    if let Some(fields) = patch.as_object() {
        if !value.is_object() {
            *value = json!({});
        }
        for (key, new) in fields {
            merge(&mut value[key], new);
        }
    } else {
        *value = patch.clone();
    }
}

impl State {
    fn stateful_set_snapshot(&self) -> Value {
        self.stateful_set.clone().unwrap_or_else(|| {
            json!({
                "metadata":{"name":"rmqtt","uid":"sts","resourceVersion":"7","annotations":{}},
                "spec":{
                    "replicas":self.sts_replicas,
                    "template":{
                        "metadata":{"finalizers":if self.template_has_finalizer {json!([FINALIZER])} else {json!([])}},
                        "spec":{"containers":[{"name":"rmqtt","image":"example/rmqtt:running","env":[
                            {"name":"RMQTT_GRACEFUL_SHUTDOWN_ENABLED","value":self.template_has_finalizer.to_string()}
                        ]}]}
                    }
                }
            })
        })
    }

    fn respond(&mut self, method: &str, path: &str, body: Value) -> (u16, Value) {
        if let Some(id) = path.strip_prefix("/broker/").and_then(|id| id.parse::<i32>().ok()) {
            self.requests.push((id, body.clone()));
            let action = body["action"].as_str().unwrap();
            // A learner has not finished plugin startup, so even its HTTP
            // control API is unavailable until promotion.
            if self.learners.contains(&id) {
                return (503, json!({"error":"learner HTTP not started"}));
            }
            match action {
                "membership" => {
                    return (
                        200,
                        json!({"id":id, "leaderId":self.leader, "isLeader":id==self.leader, "started":true,"available":true,
                    "caughtUp":true,"snapshotRestored":true,"committedIndex":10,"voters":self.voters,"learners":self.learners,
                    "raftPeers":(1..=5).map(|id|json!({"id":id,"available":true,"recentActive":true,"matchedIndex":10})).collect::<Vec<_>>(),
                    "businessPeers":self.business.iter().map(|id|json!({"id":id})).collect::<Vec<_>>()}),
                    )
                }
                "termination_status" => {
                    return (
                        200,
                        json!({"nodeId":id,"podUid":self.pods[&id]["metadata"]["uid"],"bootId":format!("boot-{id}"),"enabled":true,"terminating":true,"remainingMillis":100000}),
                    )
                }
                "termination_bind" | "termination_complete" => {
                    if action == "termination_complete" {
                        self.events.push(format!("complete-{id}"));
                        if body["expectedPodUid"].as_str().is_some_and(|uid| {
                            self.pods.get(&id).is_none_or(|pod| pod["metadata"]["uid"] != uid)
                        }) {
                            return (409, json!({"error":"Pod UID mismatch"}));
                        }
                    }
                    if action == "termination_complete"
                        && !self.pods[&id]["metadata"]["finalizers"]
                            .as_array()
                            .unwrap()
                            .contains(&json!(FINALIZER))
                    {
                        if id > self.sts_replicas {
                            self.pods.remove(&id);
                        } else {
                            let mut replacement = pod_json(id, false);
                            replacement["metadata"]["uid"] = json!(format!("replacement-{id}"));
                            replacement["status"]["conditions"] = json!([]);
                            self.pods.insert(id, replacement);
                            self.learners.push(id);
                        }
                    }
                    return (200, json!({"terminating":true,"completed":action=="termination_complete"}));
                }
                "prepare_drain" => {
                    if !self.business.contains(&(body["id"].as_i64().unwrap() as i32)) {
                        return (503, json!({"error":"drain target no longer has a business gRPC address"}));
                    }
                    if self.fail_prepare > 0 && id == 1 {
                        self.fail_prepare -= 1;
                        return (503, json!({}));
                    }
                    return (
                        200,
                        json!({"ok":true,"reliable_handoff":true,"allowedTargetNodeIds":body["allowedTargetNodeIds"]}),
                    );
                }
                "transfer_leader" => self.leader = body["id"].as_i64().unwrap() as i32,
                "drain_status" => {
                    return (
                        200,
                        json!({"strict_safe_to_leave":true,"session_handoff_complete":true,"data_loss_detected":self.data_loss}),
                    )
                }
                "routing_status" => return (200, json!({"converged":true})),
                "clear_drain" => {
                    self.events.push(format!("clear-{id}-{}", body["id"]));
                    if self.fail_clear_on == Some(id) {
                        return (503, json!({"error":"clear failed"}));
                    }
                }
                "leave" => {
                    if self.fail_leave > 0 {
                        self.fail_leave -= 1;
                        return (503, json!({}));
                    }
                    self.voters.retain(|v| *v != id);
                    self.business.retain(|v| *v != id);
                }
                "remove_voter" => self.voters.retain(|v| Some(*v as i64) != body["id"].as_i64()),
                "force_node_remove" => self.business.retain(|v| Some(*v as i64) != body["id"].as_i64()),
                "promote_learner" => {
                    let node = body["id"].as_i64().unwrap() as i32;
                    self.learners.retain(|v| *v != node);
                    self.voters.push(node);
                    if !self.business.contains(&node) {
                        self.business.push(node);
                    }
                    self.pods.get_mut(&node).unwrap()["status"]["conditions"] =
                        json!([{"type":"Ready","status":"True"}]);
                }
                _ => {}
            }
            return (200, json!({"ok":true,"forceDrainFinalized":true}));
        }
        if path.contains("endpointslices?") {
            return (200, json!({"items":[]}));
        }
        if path.ends_with("/status") {
            merge(&mut self.status, &body["status"]);
            if body["status"].get("externalTerminationCleanup").is_some() {
                self.events.push(if body["status"]["externalTerminationCleanup"].is_null() {
                    "cleanup-finished".into()
                } else {
                    "cleanup-staged".into()
                });
            }
            self.persisted_batches.push(self.status["externalTerminationBatch"].clone());
            return (200, json!({"status":self.status}));
        }
        if path.contains("/statefulsets/") {
            if method == "GET" {
                return (200, self.stateful_set_snapshot());
            }
            if method == "PATCH" {
                let mut stateful_set = self.stateful_set_snapshot();
                merge(&mut stateful_set, &body);
                self.template_has_finalizer = stateful_set["spec"]["template"]["metadata"]["finalizers"]
                    .as_array()
                    .is_some_and(|values| values.iter().any(|value| value == FINALIZER));
                self.events.push("statefulset-template".into());
                self.stateful_set_patches.push(body.clone());
                self.stateful_set = Some(stateful_set);
                return (200, body);
            }
        }
        if path.contains("/rmqttclusters/") && method == "PATCH" {
            self.events.push("cluster-finalizer".into());
            self.cluster_patches.push(body.clone());
            return (200, body);
        }
        if path.contains("/pods?") {
            return (200, json!({"items":self.pods.values().collect::<Vec<_>>()}));
        }
        if let Some(id) = path.rsplit("/rmqtt-").next().and_then(|id| id.parse::<i32>().ok()).map(|id| id + 1)
        {
            if let Some(pod) = self.pods.get_mut(&id) {
                if method == "DELETE" {
                    if body["preconditions"]["uid"].as_str().is_some_and(|uid| pod["metadata"]["uid"] != uid)
                    {
                        return (409, json!({}));
                    }
                    pod["metadata"]["deletionTimestamp"] =
                        json!((Utc::now() + chrono::Duration::seconds(120)).to_rfc3339());
                    pod["status"]["conditions"] = json!([]);
                }
                if method == "PATCH" {
                    if body["metadata"].get("finalizers").is_some() {
                        self.events.push(format!("pod-finalizer-{id}"));
                    }
                    merge(pod, &body);
                    if body["metadata"]["finalizers"] == json!([])
                        && pod["metadata"].get("deletionTimestamp").is_some()
                    {
                        assert!(
                            self.allow_unconfirmed_finalizer_release
                                || (!self.voters.contains(&id) && !self.business.contains(&id)),
                            "fence released before membership confirmation"
                        );
                        *pod = pod_json(id, false);
                        pod["metadata"]["uid"] = json!(format!("replacement-{id}"));
                        if !self.template_has_finalizer {
                            pod["metadata"]["finalizers"] = json!([]);
                        }
                        self.learners.push(id);
                        pod["status"]["conditions"] = json!([]);
                    }
                }
                return (200, pod.clone());
            }
        }
        (404, json!({}))
    }
}

struct Fixture {
    state: Arc<Mutex<State>>,
    kube: KubeClient,
    stop: Arc<AtomicBool>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
impl Fixture {
    fn new() -> Self {
        let state = Arc::new(Mutex::new(State {
            pods: (1..=5).map(|id| (id, pod_json(id, id == 2))).collect(),
            status: json!({"operation":{"type":"RemoveVoter","nodeId":5,"phase":"Preempted","fromReplicas":5,"toReplicas":4,"requestedReplicas":4},"confChange":{"type":"RemoveVoter","nodeId":5,"fromReplicas":5,"toReplicas":4,"requestedReplicas":4}}),
            voters: vec![1, 2, 3, 4, 5],
            learners: vec![],
            business: vec![1, 2, 3, 4, 5],
            leader: 2,
            requests: vec![],
            fail_prepare: 1,
            fail_leave: 1,
            slow_status: None,
            persisted_batches: Vec::new(),
            cluster_patches: Vec::new(),
            data_loss: false,
            sts_replicas: 5,
            template_has_finalizer: true,
            fail_clear_on: None,
            allow_unconfirmed_finalizer_release: false,
            events: Vec::new(),
            stateful_set_patches: Vec::new(),
            stateful_set: None,
        }));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker_state = state.clone();
        std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                };
                let worker_state = worker_state.clone();
                std::thread::spawn(move || {
                    // Accepted sockets inherit nonblocking mode on macOS.
                    // The per-connection worker uses blocking buffered reads.
                    stream.set_nonblocking(false).unwrap();
                    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut first = String::new();
                    if reader.read_line(&mut first).is_err() {
                        return;
                    }
                    let parts = first.split_whitespace().collect::<Vec<_>>();
                    if parts.len() < 2 {
                        return;
                    }
                    let mut length = 0;
                    loop {
                        let mut line = String::new();
                        // Parallel leader probes intentionally cancel losing
                        // requests. EOF/reset is expected, not a mock failure.
                        if !matches!(reader.read_line(&mut line), Ok(n) if n > 0) {
                            return;
                        }
                        if line == "\r\n" {
                            break;
                        }
                        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                    }
                    let mut body = vec![0; length];
                    if reader.read_exact(&mut body).is_err() {
                        return;
                    }
                    let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let (code, value, slow) = {
                        let mut state = worker_state.lock().unwrap();
                        let slow = body["action"] == "termination_status"
                            && state.slow_status.is_some_and(|id| parts[1] == format!("/broker/{id}"));
                        let (code, value) = state.respond(parts[0], parts[1], body);
                        (code, value, slow)
                    };
                    if slow {
                        std::thread::sleep(Duration::from_millis(2200));
                    }
                    let value = value.to_string();
                    let response = format!("HTTP/1.1 {code} mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",value.len(),value);
                    let _ = std::io::Write::write_all(&mut stream, response.as_bytes());
                });
            }
        });
        Self { state, kube: KubeClient { client: Client::new(), base_url: url, token: String::new() }, stop }
    }
    async fn round(&self) -> bool {
        let status = self.state.lock().unwrap().status.clone();
        let replicas = self.state.lock().unwrap().sts_replicas;
        let cluster: RmqttCluster=serde_json::from_value(json!({"metadata":{"name":"rmqtt"},"spec":{"replicas":4,"gracefulShutdown":{"enabled":true}},"status":status})).unwrap();
        TEST_BROKER_URL
            .scope(
                self.kube.base_url.clone(),
                handle_external_pod_termination(&self.kube, "test", &cluster, replicas, "sts"),
            )
            .await
            .unwrap()
    }

    fn cluster(&self, replicas: i32) -> RmqttCluster {
        serde_json::from_value(json!({"metadata":{"name":"rmqtt"},"spec":{"replicas":replicas,
            "gracefulShutdown":{"enabled":true},"imageUpdate":{"stabilizationSeconds":0},
            "raft":{"scaleInStabilizationSeconds":0}},"status":self.state.lock().unwrap().status}))
        .unwrap()
    }

    async fn finish_batch(&self) {
        for _ in 0..30 {
            {
                let mut state = self.state.lock().unwrap();
                if let Some(targets) = state
                    .status
                    .get_mut("externalTerminationBatch")
                    .and_then(|batch| batch.get_mut("targets"))
                    .and_then(Value::as_array_mut)
                {
                    for target in targets {
                        if target["phase"] == "WaitingEndpointRemoval" {
                            target["endpointAbsentSince"] =
                                json!((Utc::now() - chrono::Duration::seconds(60)).to_rfc3339());
                        }
                    }
                }
            }
            if !self.round().await {
                assert_eq!(self.state.lock().unwrap().status["externalTerminationBatch"], Value::Null);
                return;
            }
        }
        panic!("batch did not finish: {}", self.state.lock().unwrap().status);
    }
}

#[test]
fn participant_exclusion_requires_authoritative_non_business_learner() {
    let mut membership: MembershipStatus =
        serde_json::from_value(json!({"voters":[1,2],"learners":[3],"businessPeers":[{"id":1},{"id":2}]}))
            .unwrap();
    assert!(joining_without_business(Some(&membership), 3));
    assert!(!joining_without_business(None, 3));
    assert!(!joining_without_business(Some(&membership), 2));
    membership.business_peers.push(json!({"id":3}));
    assert!(!joining_without_business(Some(&membership), 3));
}

#[test]
fn planned_drain_proof_requires_exact_uid_and_both_memberships_absent() {
    let mut value = pod_json(2, true);
    value["metadata"]["annotations"] = json!({PLANNED_DRAIN_UID:"uid-2"});
    let pod: Pod = serde_json::from_value(value.clone()).unwrap();
    let mut view: MembershipStatus =
        serde_json::from_value(json!({"voters":[1],"businessPeers":[{"id":1}]})).unwrap();
    assert!(planned_removal_confirmed(&pod, 2, Some(&view)));
    assert!(!planned_removal_confirmed(&pod, 2, None));
    view.learners.push(2);
    assert!(!planned_removal_confirmed(&pod, 2, Some(&view)));
    view.learners.clear();
    view.business_peers.push(json!({"id":2}));
    assert!(!planned_removal_confirmed(&pod, 2, Some(&view)));
    view.business_peers.pop();
    value["metadata"]["uid"] = json!("replacement-2");
    assert!(!planned_removal_confirmed(&serde_json::from_value(value).unwrap(), 2, Some(&view)));
}

#[tokio::test]
async fn external_batch_yields_to_original_learner_recovery_for_all_operation_kinds() {
    for kind in ["AddLearner", "RollingImageUpdate", "RollingRestart", "PodRestart"] {
        let fixture = Fixture::new();
        {
            let mut state = fixture.state.lock().unwrap();
            state.leader = 1;
            state.fail_prepare = 0;
            state.fail_leave = 0;
            state.voters.retain(|id| *id != 5);
            state.business.retain(|id| *id != 5);
            state.learners.push(5);
            state.pods.get_mut(&5).unwrap()["status"]["conditions"] = json!([]);
            state.pods.get_mut(&5).unwrap()["metadata"]["uid"] = json!("replacement-5");
            state.status = json!({"operation":{"type":kind,"nodeId":5,"phase":"CatchingUp","fromReplicas":4,"toReplicas":5,"requestedReplicas":5}});
        }
        let saved = fixture.state.lock().unwrap().status["operation"].clone();
        fixture.finish_batch().await;
        {
            let state = fixture.state.lock().unwrap();
            assert_eq!(state.status["operation"], saved);
            assert!(state.learners.contains(&5), "external batch must not take over unrelated promotion");
            assert!(!state.requests.iter().any(|(id, p)| *id == 5 && p["action"] != "membership"));
        }
        // Actually resume the pre-existing controller, not just assert that its
        // JSON survived. Learner promotion, readiness, and stabilization run.
        TEST_BROKER_URL
            .scope(fixture.kube.base_url.clone(), async {
                for _ in 0..3 {
                    let mut cluster = fixture.cluster(5);
                    if kind == "PodRestart" {
                        cluster.spec.pod_restart = Some(pod_restart::PodRestartTarget {
                            pod_name: "rmqtt-4".into(),
                            pod_uid: "uid-5".into(),
                            node_id: 5,
                            manual_restart: pod_restart::PodManualRestart::Rollback,
                        });
                    } else if kind == "RollingRestart" {
                        cluster.spec.rollout_restart_at = Some("test-restart".into());
                    }
                    let Some(operation) = cluster.status.as_ref().and_then(|s| s.operation.as_ref()) else {
                        break;
                    };
                    if kind == "AddLearner" {
                        continue_add_learner_and_promote(
                            &fixture.kube,
                            "test",
                            &cluster,
                            5,
                            &operation.to_conf_change(),
                        )
                        .await
                        .unwrap();
                    } else {
                        continue_image_rollout_recovery(&fixture.kube, "test", &cluster, 5, operation, 5)
                            .await
                            .unwrap();
                    }
                }
            })
            .await;
        let state = fixture.state.lock().unwrap();
        assert!(state.learners.is_empty(), "{kind}: {}", state.status);
        assert!(state.voters.contains(&5) && state.business.contains(&5));
        if kind == "AddLearner" || kind == "PodRestart" {
            assert_eq!(state.status["operation"], Value::Null, "{kind}: {}", state.status);
        } else {
            assert_eq!(state.status["operation"]["phase"], "Preparing", "{kind}: {}", state.status);
            assert_eq!(state.status["operation"]["nodeId"], 0);
        }
        if kind == "PodRestart" {
            assert_eq!(state.status["lastPodRestart"]["originalPodUid"], "uid-5");
            assert_eq!(state.status["lastPodRestart"]["result"], "Succeeded");
        }
    }
}

#[tokio::test]
async fn planned_rollout_deletion_binds_and_completes_without_second_drain() {
    for data_loss in [false, true] {
        let fixture = Fixture::new();
        {
            let mut state = fixture.state.lock().unwrap();
            state.leader = 1;
            state.data_loss = data_loss;
            state.voters.retain(|id| *id != 2);
            state.business.retain(|id| *id != 2);
            state.pods.insert(2, pod_json(2, false));
            state.status = json!({"operation":{"type":"PodRestart","nodeId":2,"phase":"DeletingPod"}});
        }
        let mut cluster = fixture.cluster(5);
        cluster.spec.pod_restart = Some(pod_restart::PodRestartTarget {
            pod_name: "rmqtt-1".into(),
            pod_uid: "uid-2".into(),
            node_id: 2,
            manual_restart: pod_restart::PodManualRestart::Rollback,
        });
        pod_restart::delete_rollout_pod(&fixture.kube, "test", &cluster, 2).await.unwrap();
        fixture.finish_batch().await;
        let state = fixture.state.lock().unwrap();
        assert!(!state.requests.iter().any(|(_, p)| matches!(
            p["action"].as_str(),
            Some(
                "prepare_drain"
                    | "drain"
                    | "force_drain_finalize"
                    | "leave"
                    | "remove_voter"
                    | "force_node_remove"
            )
        )));
        let complete = state.requests.iter().find(|(_, p)| p["action"] == "termination_complete").unwrap();
        assert_eq!(complete.1["expectedPodUid"], "uid-2");
        assert_eq!(complete.1["expectedBootId"], "boot-2");
        assert_eq!(complete.1["outcome"], if data_loss { "CompletedWithLoss" } else { "Completed" });
        assert!(state.requests.iter().any(|(_, p)| p["action"] == "termination_bind"));
        assert!(state.voters.contains(&2));
        assert!(state.pods[&2]["metadata"]["annotations"][PLANNED_DRAIN_UID].is_null());
    }
}

#[tokio::test]
async fn planned_and_external_deletion_overlap_without_repreparing_removed_member() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.leader = 1;
        state.fail_prepare = 0;
        state.fail_leave = 0;
        state.pods.insert(2, pod_json(2, false));
        state.pods.insert(3, pod_json(3, true));
        state.voters.retain(|id| *id != 2);
        state.business.retain(|id| *id != 2);
        state.status = json!({"operation":{"type":"RollingImageUpdate","nodeId":2,"phase":"DeletingPod"}});
    }
    let cluster = fixture.cluster(5);
    pod_restart::delete_rollout_pod(&fixture.kube, "test", &cluster, 2).await.unwrap();
    fixture.finish_batch().await;
    let state = fixture.state.lock().unwrap();
    assert!(state.voters.contains(&2) && state.voters.contains(&3));
    assert!(!state.requests.iter().any(|(_, p)| p["action"] == "prepare_drain" && p["id"] == 2));
    assert!(state.requests.iter().any(|(_, p)| p["action"] == "prepare_drain" && p["id"] == 3));
    assert!(state
        .requests
        .iter()
        .any(|(id, p)| *id == 2 && p["action"] == "termination_complete" && p["outcome"] == "Completed"));
}

#[tokio::test]
async fn planned_drain_marker_cannot_be_written_to_replacement_and_disaster_release_has_no_marker() {
    let fixture = Fixture::new();
    fixture.state.lock().unwrap().pods.insert(2, pod_json(2, false));
    let mut cluster = fixture.cluster(5);
    cluster.spec.pod_restart = Some(pod_restart::PodRestartTarget {
        pod_name: "rmqtt-1".into(),
        pod_uid: "old-uid-2".into(),
        node_id: 2,
        manual_restart: pod_restart::PodManualRestart::Rollback,
    });
    assert!(pod_restart::delete_rollout_pod(&fixture.kube, "test", &cluster, 2).await.is_err());
    {
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.pods[&2]["metadata"]["finalizers"], json!([FINALIZER]));
        assert!(state.pods[&2]["metadata"]["annotations"][PLANNED_DRAIN_UID].is_null());
        assert!(state.pods[&2]["metadata"]["deletionTimestamp"].is_null());
    }
    release_for_planned_delete(&fixture.kube, "test", &cluster, "rmqtt-1", Some("uid-2")).await.unwrap();
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.pods[&2]["metadata"]["finalizers"], json!([]));
    assert!(state.pods[&2]["metadata"]["annotations"][PLANNED_DRAIN_UID].is_null());
}

#[tokio::test]
async fn planned_scale_in_old_ordinal_completes_then_original_stabilization_finishes() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.leader = 1;
        state.pods.insert(2, pod_json(2, false));
        state.voters.retain(|id| *id != 5);
        state.business.retain(|id| *id != 5);
        state.sts_replicas = 4;
        state.status["operation"]["phase"] = json!("WaitingPodTermination");
    }
    let cluster = fixture.cluster(4);
    release_after_planned_drain(&fixture.kube, "test", &cluster, "rmqtt-4", Some("uid-5")).await.unwrap();
    // A controller retry before StatefulSet issues DELETE must not re-add the fence.
    ensure_fences(&fixture.kube, "test", &cluster, "sts").await.unwrap();
    assert_eq!(fixture.state.lock().unwrap().pods[&5]["metadata"]["finalizers"], json!([]));
    fixture.kube.delete("/api/v1/namespaces/test/pods/rmqtt-4").await.unwrap();
    fixture.finish_batch().await;
    TEST_BROKER_URL
        .scope(fixture.kube.base_url.clone(), async {
            for _ in 0..2 {
                let cluster = fixture.cluster(4);
                let op = cluster.status.as_ref().unwrap().operation.as_ref().unwrap();
                finish_scale_in_step(&fixture.kube, "test", &cluster, &op.to_conf_change()).await.unwrap();
            }
        })
        .await;
    let state = fixture.state.lock().unwrap();
    assert!(!state.pods.contains_key(&5));
    assert_eq!(state.status["phase"], "Running");
    assert_eq!(state.status["operation"], Value::Null);
    assert!(!state.requests.iter().any(|(_, p)| p["action"] == "prepare_drain"));
}

#[tokio::test]
async fn five_nodes_staggered_delete_retries_topology_and_recovers_before_saved_scale_in() {
    let fixture = Fixture::new();
    let saved = fixture.state.lock().unwrap().status["operation"].clone();
    assert!(fixture.round().await);
    assert!(fixture.round().await);
    // A later deletion must refresh the already-draining target even though
    // leadership transfer and a failed prepare happened in earlier rounds.
    fixture.state.lock().unwrap().pods.insert(3, pod_json(3, true));
    for _ in 0..24 {
        {
            let mut state = fixture.state.lock().unwrap();
            if let Some(targets) = state.status["externalTerminationBatch"]["targets"].as_array_mut() {
                for target in targets {
                    if target["phase"] == "WaitingEndpointRemoval" {
                        target["endpointAbsentSince"] =
                            json!((Utc::now() - chrono::Duration::seconds(60)).to_rfc3339());
                    }
                }
            }
        }
        if !fixture.round().await {
            break;
        }
    }
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.status["externalTerminationBatch"], Value::Null, "{}", state.status);
    assert_eq!(state.status["operation"], saved);
    assert!(state.voters.contains(&2) && state.voters.contains(&3));
    assert!(state.requests.iter().any(|(id, p)| *id == 3
        && p["action"] == "prepare_drain"
        && p["id"] == 2
        && p["allowedTargetNodeIds"] == json!([1, 4])));
    let leaves =
        state.requests.iter().filter(|(_, p)| p["action"] == "leave").map(|(id, _)| *id).collect::<Vec<_>>();
    assert_eq!(leaves, vec![2, 2, 3]);
}

#[tokio::test]
async fn fenced_dead_broker_removes_raft_then_business_without_resetting_phase() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.leader = 1;
        let mut dead = target(2, "WaitingMembershipRemoval");
        dead.deadline_at = (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
        dead.membership_owner = true;
        state.status["externalTerminationBatch"] = json!({"targets":[dead]});
    }
    for _ in 0..8 {
        if !fixture.round().await {
            break;
        }
    }
    let state = fixture.state.lock().unwrap();
    let actions = state.requests.iter().filter_map(|(_, p)| p["action"].as_str()).collect::<Vec<_>>();
    assert!(actions.contains(&"remove_voter") && actions.contains(&"force_node_remove"));
    assert_eq!(state.status["externalTerminationBatch"], Value::Null);
}

#[tokio::test]
async fn unfenced_ordinal_never_receives_id_only_cleanup() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.leader = 1;
        state.pods.get_mut(&2).unwrap()["metadata"]["finalizers"] = json!([]);
        let mut old = target(2, "WaitingMembershipRemoval");
        old.deadline_at = (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
        old.membership_owner = true;
        state.status["externalTerminationBatch"] = json!({"targets":[old]});
    }
    assert!(fixture.round().await);
    {
        let mut state = fixture.state.lock().unwrap();
        let mut replacement = pod_json(2, false);
        replacement["metadata"]["uid"] = json!("replacement-2");
        state.pods.insert(2, replacement);
    }
    assert!(!fixture.round().await, "{}", fixture.state.lock().unwrap().status);
    let state = fixture.state.lock().unwrap();
    assert!(!state
        .requests
        .iter()
        .any(|(_, p)| matches!(p["action"].as_str(), Some("remove_voter" | "force_node_remove" | "leave"))));
}

#[tokio::test]
async fn five_nodes_simultaneous_two_deletions_recover_both_ordinals() {
    let fixture = Fixture::new();
    fixture.state.lock().unwrap().pods.insert(3, pod_json(3, true));
    for _ in 0..24 {
        {
            let mut state = fixture.state.lock().unwrap();
            if let Some(targets) = state
                .status
                .get_mut("externalTerminationBatch")
                .and_then(|batch| batch.get_mut("targets"))
                .and_then(Value::as_array_mut)
            {
                for target in targets {
                    if target["phase"] == "WaitingEndpointRemoval" {
                        target["endpointAbsentSince"] =
                            json!((Utc::now() - chrono::Duration::seconds(60)).to_rfc3339());
                    }
                }
            }
        }
        if !fixture.round().await {
            break;
        }
    }
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.status["externalTerminationBatch"], Value::Null, "{}", state.status);
    assert!(state.voters.contains(&2) && state.voters.contains(&3));
    for (sender, target_id) in [(2, 3), (3, 2)] {
        assert!(state
            .requests
            .iter()
            .any(|(id, p)| *id == sender && p["action"] == "prepare_drain" && p["id"] == target_id));
    }
}

#[tokio::test]
async fn slow_first_broker_does_not_delay_persisting_the_second_binding() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.leader = 1;
        state.slow_status = Some(2);
        state.pods.insert(3, pod_json(3, true));
    }
    let started = Instant::now();
    assert!(fixture.round().await);
    assert!(started.elapsed() < Duration::from_secs(4));
    let state = fixture.state.lock().unwrap();
    assert_eq!(
        state.requests.iter().filter(|(id, p)| *id == 2 && p["action"] == "termination_status").count(),
        1
    );
    assert!(
        state.persisted_batches.iter().any(|batch| {
            let Some(targets) = batch["targets"].as_array() else {
                return false;
            };
            targets.iter().any(|t| t["nodeId"] == 3 && t["phase"] == "Bound")
                && targets.iter().any(|t| t["nodeId"] == 2 && t["lastError"].is_null())
        }),
        "second binding should be saved before first timeout is reported"
    );
}

#[tokio::test]
async fn cluster_teardown_releases_only_our_pod_and_cluster_finalizers() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.pods.insert(2, pod_json(2, false));
        state.pods.get_mut(&1).unwrap()["metadata"]["finalizers"] =
            json!([FINALIZER, "other.example/retain"]);
    }
    let cluster: RmqttCluster = serde_json::from_value(json!({"metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"42","deletionTimestamp":Utc::now().to_rfc3339(),"finalizers":[CLUSTER_FINALIZER,"other.example/cr"]},"spec":{"gracefulShutdown":{"enabled":true}}})).unwrap();
    assert!(reconcile_cluster_fence(&fixture.kube, "test", &cluster).await.unwrap());
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.pods[&1]["metadata"]["finalizers"], json!(["other.example/retain"]));
    assert_eq!(state.cluster_patches[0]["metadata"]["finalizers"], json!(["other.example/cr"]));
    assert_eq!(state.cluster_patches[0]["metadata"]["resourceVersion"], "42");
    assert!(state.requests.is_empty());
}

#[tokio::test]
async fn planned_scale_in_clears_only_survivors_after_the_target_pod_is_removed() {
    for enabled in [false, true] {
        let fixture = Fixture::new();
        fixture.state.lock().unwrap().pods.remove(&5);
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata":{"name":"rmqtt"},
            "spec":{"replicas":4,"gracefulShutdown":{"enabled":enabled}}
        }))
        .unwrap();
        TEST_BROKER_URL
            .scope(fixture.kube.base_url.clone(), super::clear_reliable_handoff("test", &cluster, 4, 5))
            .await;
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.requests.len(), 4);
        assert_eq!(state.requests.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        assert!(state.requests.iter().all(|(_, payload)| payload == &json!({"action":"clear_drain","id":5})));
    }
}

#[tokio::test]
async fn disabled_feature_does_not_apply_a_new_spec_image_to_a_legacy_template() {
    let fixture = Fixture::new();
    let template = json!({
        "metadata":{"name":"rmqtt","uid":"sts","resourceVersion":"7"},
        "spec":{
            "replicas":5,
            "template":{
                "metadata":{"finalizers":["other.example/template"]},
                "spec":{"containers":[{"name":"rmqtt","image":"example/rmqtt:old","env":[
                    {"name":"RMQTT_NAME","value":"rmqtt"}
                ]}]}
            }
        }
    });
    {
        let mut state = fixture.state.lock().unwrap();
        state.stateful_set = Some(template.clone());
        for id in 1..=5 {
            let mut pod = pod_json(id, false);
            pod["metadata"]["finalizers"] = json!(["other.example/pod"]);
            state.pods.insert(id, pod);
        }
    }
    let cluster: RmqttCluster = serde_json::from_value(json!({
        "metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"42"},
        "spec":{"replicas":5,"image":"example/rmqtt:new"}
    }))
    .unwrap();
    assert!(!reconcile_cluster_fence(&fixture.kube, "test", &cluster).await.unwrap());
    let state = fixture.state.lock().unwrap();
    assert!(state.stateful_set_patches.is_empty());
    assert!(state.cluster_patches.is_empty());
    assert!(state.requests.is_empty());
    assert!(state.events.is_empty());
    assert_eq!(state.stateful_set_snapshot(), template);
}

#[tokio::test]
async fn disabling_preserves_the_live_rollout_template_and_other_finalizers() {
    let fixture = Fixture::new();
    let template = json!({
        "metadata":{"name":"rmqtt","uid":"sts","resourceVersion":"7","labels":{"custom":"keep"}},
        "spec":{
            "replicas":5,"updateStrategy":{"type":"OnDelete"},
            "template":{
                "metadata":{
                    "finalizers":["other.example/template",FINALIZER],
                    "annotations":{"kubectl.kubernetes.io/restartedAt":"existing-restart"}
                },
                "spec":{
                    "terminationGracePeriodSeconds":120,
                    "nodeSelector":{"pool":"running-pool"},
                    "containers":[
                        {"name":"rmqtt","image":"example/rmqtt:rollout-pinned","imagePullPolicy":"Always",
                            "resources":{"requests":{"cpu":"8","memory":"8Gi"}},
                            "env":[
                                {"name":"RMQTT_GRACEFUL_SHUTDOWN_ENABLED","value":"true"},
                                {"name":"RMQTT_POD_UID","valueFrom":{"fieldRef":{"fieldPath":"metadata.uid"}}},
                                {"name":"UNRELATED","value":"unchanged"}
                            ]},
                        {"name":"sidecar","image":"example/sidecar:v1","args":["keep"]}
                    ]
                }
            }
        }
    });
    {
        let mut state = fixture.state.lock().unwrap();
        state.stateful_set = Some(template.clone());
        state.pods.insert(2, pod_json(2, false));
        state.pods.get_mut(&1).unwrap()["metadata"]["finalizers"] = json!([FINALIZER, "other.example/pod"]);
    }
    let cluster: RmqttCluster = serde_json::from_value(json!({
        "metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"42","finalizers":[CLUSTER_FINALIZER,"other.example/cr"]},
        "spec":{"replicas":3,"image":"example/rmqtt:newer-request","gracefulShutdown":{"enabled":false}},
        "status":{"operation":{"type":"ImageRollout","nodeId":2,"phase":"Draining","targetImage":"example/rmqtt:rollout-pinned"}}
    }))
    .unwrap();
    assert!(!reconcile_cluster_fence(&fixture.kube, "test", &cluster).await.unwrap());
    let mut expected = template;
    expected["spec"]["template"]["metadata"]["finalizers"] = json!(["other.example/template"]);
    expected["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = json!("false");
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.stateful_set_snapshot(), expected);
    assert_eq!(state.stateful_set_patches.len(), 1);
    assert_eq!(state.stateful_set_patches[0]["metadata"]["resourceVersion"], "7");
    assert_eq!(state.stateful_set_patches[0]["metadata"]["uid"], "sts");
    assert!(state.stateful_set_patches[0]["spec"].get("replicas").is_none());
    assert_eq!(state.pods[&1]["metadata"]["finalizers"], json!(["other.example/pod"]));
    assert_eq!(state.cluster_patches[0]["metadata"]["finalizers"], json!(["other.example/cr"]));
    assert!(state.requests.is_empty());
}

#[tokio::test]
async fn disabling_graceful_shutdown_durably_aborts_batch_before_releasing_fences() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.allow_unconfirmed_finalizer_release = true;
        state.fail_prepare = 0;
        state.status = json!({"phase":"ExternalTermination","drainingNodeId":2,"drainingNodeIds":[2],
            "externalTerminationBatch":{"targets":[target(2,"Draining")]}});
    }
    let status = fixture.state.lock().unwrap().status.clone();
    let cluster: RmqttCluster = serde_json::from_value(json!({
        "metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"42","finalizers":[CLUSTER_FINALIZER,"other.example/cr"]},
        "spec":{"replicas":5,"gracefulShutdown":{"enabled":false}},"status":status
    })).unwrap();
    assert!(!TEST_BROKER_URL
        .scope(fixture.kube.base_url.clone(), reconcile_cluster_fence(&fixture.kube, "test", &cluster))
        .await
        .unwrap());

    let state = fixture.state.lock().unwrap();
    assert_eq!(state.status["externalTerminationBatch"], Value::Null);
    assert_eq!(state.status["externalTerminationCleanup"], Value::Null);
    assert_eq!(state.status["drainingNodeId"], Value::Null);
    assert_eq!(state.status["drainingNodeIds"], json!([]));
    assert!(state.requests.iter().any(|(id, p)| *id == 2
        && p["action"] == "termination_complete"
        && p["outcome"] == "CompletedWithLoss"));
    for peer in [1, 3, 4, 5] {
        assert!(state
            .requests
            .iter()
            .any(|(id, p)| *id == peer && p["action"] == "clear_drain" && p["id"] == 2));
    }
    assert!(state
        .pods
        .values()
        .all(|pod| !pod["metadata"]["finalizers"].as_array().unwrap().contains(&json!(FINALIZER))));
    assert!(!state.template_has_finalizer);
    let template = state.stateful_set_patches.last().unwrap();
    assert_eq!(template["spec"]["template"]["metadata"]["finalizers"], json!([]));
    assert!(template["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .unwrap()
        .iter()
        .any(|env| env["name"] == "RMQTT_GRACEFUL_SHUTDOWN_ENABLED" && env["value"] == "false"));
    assert_eq!(state.cluster_patches.last().unwrap()["metadata"]["finalizers"], json!(["other.example/cr"]));
    let position = |event: &str| state.events.iter().position(|saved| saved == event).unwrap();
    assert!(position("cleanup-staged") < position("clear-1-2"));
    assert!(position("cleanup-finished") < position("statefulset-template"));
    assert!(position("statefulset-template") < position("pod-finalizer-2"));
}

#[tokio::test]
async fn failed_disabled_cleanup_is_retried_and_blocks_reenable_without_refencing_pods() {
    let fixture = Fixture::new();
    {
        let mut state = fixture.state.lock().unwrap();
        state.allow_unconfirmed_finalizer_release = true;
        state.fail_clear_on = Some(1);
        state.status = json!({"externalTerminationBatch":{"targets":[target(2,"Draining")]}});
    }
    let disabled: RmqttCluster = serde_json::from_value(json!({
        "metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"42","finalizers":[CLUSTER_FINALIZER]},
        "spec":{"replicas":5,"gracefulShutdown":{"enabled":false}},
        "status":fixture.state.lock().unwrap().status.clone()
    }))
    .unwrap();
    assert!(!TEST_BROKER_URL
        .scope(fixture.kube.base_url.clone(), reconcile_cluster_fence(&fixture.kube, "test", &disabled))
        .await
        .unwrap());
    {
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.status["externalTerminationBatch"], Value::Null);
        assert_eq!(state.status["externalTerminationCleanup"]["targets"][0]["nodeId"], 2);
        assert!(state
            .pods
            .values()
            .all(|pod| !pod["metadata"]["finalizers"].as_array().unwrap().contains(&json!(FINALIZER))));
    }

    let enabled_while_failing: RmqttCluster = serde_json::from_value(json!({
        "metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"43","finalizers":[]},
        "spec":{"replicas":5,"gracefulShutdown":{"enabled":true}},
        "status":fixture.state.lock().unwrap().status.clone()
    }))
    .unwrap();
    assert!(TEST_BROKER_URL
        .scope(
            fixture.kube.base_url.clone(),
            reconcile_cluster_fence(&fixture.kube, "test", &enabled_while_failing)
        )
        .await
        .unwrap());
    assert!(fixture
        .state
        .lock()
        .unwrap()
        .cluster_patches
        .iter()
        .all(|patch| patch["metadata"]["finalizers"] != json!([CLUSTER_FINALIZER])));

    {
        let mut state = fixture.state.lock().unwrap();
        state.fail_clear_on = None;
        // The replacement created after the emergency fence release must first
        // finish its ordinary membership recovery and start the control API.
        state.learners.retain(|id| *id != 2);
        state.pods.get_mut(&2).unwrap()["status"]["conditions"] = json!([{"type":"Ready","status":"True"}]);
    }
    let enabled_after_recovery: RmqttCluster = serde_json::from_value(json!({
        "metadata":{"name":"rmqtt","uid":"cr-uid","resourceVersion":"44","finalizers":[]},
        "spec":{"replicas":5,"gracefulShutdown":{"enabled":true}},
        "status":fixture.state.lock().unwrap().status.clone()
    }))
    .unwrap();
    assert!(!TEST_BROKER_URL
        .scope(
            fixture.kube.base_url.clone(),
            reconcile_cluster_fence(&fixture.kube, "test", &enabled_after_recovery)
        )
        .await
        .unwrap());
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.status["externalTerminationCleanup"], Value::Null);
    assert_eq!(state.cluster_patches.last().unwrap()["metadata"]["finalizers"], json!([CLUSTER_FINALIZER]));
}
