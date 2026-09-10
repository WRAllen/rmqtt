use super::*;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::sync::Arc;

fn cluster() -> RmqttCluster {
    serde_json::from_value(json!({
        "metadata": {"name": "rmqtt", "generation": 7},
        "spec": {"replicas": 3, "image": "rmqtt:v1", "imagePullPolicy": "IfNotPresent"},
    }))
    .unwrap()
}

fn pod_json(id: i32, requested: bool) -> Value {
    let mut pod = json!({
        "metadata": {
            "name": format!("rmqtt-{}", id - 1), "uid": format!("uid-{id}"),
            "annotations": {},
            "ownerReferences": [{"kind": "StatefulSet", "name": "rmqtt", "uid": "sts-uid", "controller": true}],
        },
        "spec": {"containers": [{"name": "rmqtt", "image": "rmqtt:v1", "imagePullPolicy": "IfNotPresent"}]},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
    });
    if requested {
        pod["metadata"]["annotations"][POD_MANUAL_RESTART_ANNOTATION] =
            json!(PodManualRestart::Rollback.as_str());
    }
    pod
}

fn target_cluster() -> RmqttCluster {
    let mut cluster = cluster();
    configure(
        &mut cluster,
        PodRestartTarget {
            pod_name: "rmqtt-1".into(),
            pod_uid: "uid-2".into(),
            node_id: 2,
            manual_restart: PodManualRestart::Rollback,
        },
    );
    cluster
}

fn membership() -> MembershipStatus {
    MembershipStatus {
        id: 2,
        leader_id: Some(2),
        is_leader: true,
        available: true,
        started: true,
        caught_up: true,
        snapshot_restored: true,
        voters: vec![1, 2, 3],
        committed_index: 10,
        raft_peers: (1..=3)
            .map(|id| RaftPeerStatus {
                id,
                available: true,
                recent_active: true,
                matched_index: 10,
                ..Default::default()
            })
            .collect(),
        business_peers: (1..=3).map(|id| json!({"id": id})).collect(),
        ..Default::default()
    }
}

fn stateful_set() -> StatefulSet {
    serde_json::from_value(json!({"metadata": {"uid": "sts-uid"}, "spec": {"replicas": 3}})).unwrap()
}

#[test]
fn requests_are_queued_by_ordinal_and_disappear_with_the_original_pod() {
    let mut pods: Vec<Pod> =
        serde_json::from_value(json!([pod_json(3, true), pod_json(2, true), pod_json(1, false),])).unwrap();
    let target = pending_target(&pods, "rmqtt", 3, "sts-uid").unwrap().unwrap();
    assert_eq!(target.node_id, 2);
    // Repeating true on this UID coalesces. Once it has been replaced, only the
    // other original Pod remains queued; siblings are not implicitly restarted.
    pods[1].metadata.uid = "replacement-2".into();
    pods[1].metadata.annotations.clear();
    assert_eq!(pending_target(&pods, "rmqtt", 3, "sts-uid").unwrap().unwrap().node_id, 3);
    pods[0].metadata.annotations.clear();
    assert!(pending_target(&pods, "rmqtt", 3, "sts-uid").unwrap().is_none());
}

#[test]
fn unrelated_terminating_and_ambiguous_pods_are_not_delete_requests() {
    for change in [
        json!({"name": "another-1"}),
        json!({"uid": ""}),
        json!({"deletionTimestamp": "2026-09-09T00:00:00Z"}),
        json!({"annotations": {}}),
        json!({"ownerReferences": []}),
        json!({"ownerReferences": [{"kind": "StatefulSet", "name": "rmqtt", "uid": "previous-sts", "controller": true}]}),
    ] {
        let mut value = pod_json(2, true);
        value["metadata"].as_object_mut().unwrap().extend(change.as_object().unwrap().clone());
        let pods = vec![serde_json::from_value(value).unwrap()];
        assert!(pending_target(&pods, "rmqtt", 3, "sts-uid").unwrap().is_none());
    }
    let pods = vec![serde_json::from_value(pod_json(2, true)).unwrap()];
    assert!(pending_target(&pods, "rmqtt", 1, "sts-uid").unwrap().is_none());
    assert!(pending_target(&pods, "rmqtt", 3, "").unwrap().is_none());
}

#[test]
fn manual_restart_rejects_unknown_values() {
    for value in ["", "true", "ForceContinue", "Rollback", "force", "rollback"] {
        let mut invalid: Pod = serde_json::from_value(pod_json(2, false)).unwrap();
        invalid.metadata.annotations.insert(POD_MANUAL_RESTART_ANNOTATION.into(), value.into());
        assert!(pending_target(&[invalid], "rmqtt", 3, "sts-uid").is_err());
    }

    for (value, manual_restart) in
        [("rollback-on-timeout", PodManualRestart::Rollback), ("force-on-timeout", PodManualRestart::Force)]
    {
        let mut requested: Pod = serde_json::from_value(pod_json(2, false)).unwrap();
        requested.metadata.annotations.insert("rmqtt.io/manual-restart".into(), value.into());
        assert_eq!(
            pending_target(&[requested], "rmqtt", 3, "sts-uid").unwrap().unwrap().manual_restart,
            manual_restart
        );
    }
}

#[test]
fn matching_image_is_not_proof_of_replacement_and_other_pods_never_match() {
    let cluster = target_cluster();
    let mut pod: Pod = serde_json::from_value(pod_json(2, true)).unwrap();
    assert!(!pod_uses_rollout_target(&pod, &cluster.spec));
    pod.metadata.uid = "new-uid".into();
    assert!(pod_uses_rollout_target(&pod, &cluster.spec));
    // Even if a template changed concurrently, recovering the new UID is safer
    // than trying to remove its member and deleting the replacement a second time.
    pod.spec.containers[0].image = "rmqtt:v2".into();
    assert!(pod_uses_rollout_target(&pod, &cluster.spec));
    pod.metadata.deletion_timestamp = Some("now".into());
    assert!(!pod_uses_rollout_target(&pod, &cluster.spec));
    let sibling: Pod = serde_json::from_value(pod_json(1, true)).unwrap();
    assert!(!pod_uses_rollout_target(&sibling, &cluster.spec));
}

#[test]
fn every_persisted_phase_resumes_the_same_uid_after_operator_restart() {
    for phase in [
        "PreparingNode",
        "TransferringLeader",
        "WaitingDrainStable",
        "DeletingPod",
        "WaitingPodRecreation",
        "CatchingUp",
        "Promoting",
        "Stabilizing",
        "RolledBack",
    ] {
        let original = target_cluster();
        let operation: OperationStatus = serde_json::from_value(image_rollout_operation_status_json(
            &original,
            2,
            3,
            phase,
            Some("2026-09-09T00:00:00Z".into()),
            None,
            false,
        ))
        .unwrap();
        let mut latest = cluster();
        latest.spec.image = "rmqtt:v2".into();
        latest.spec.rollout_restart_at = Some("new-full-rollout".into());
        latest.status = Some(ClusterStatus { operation: Some(operation.clone()), ..Default::default() });
        let resumed = resume_cluster(&latest, &operation).unwrap();
        assert_eq!(resumed.spec.image, "rmqtt:v1");
        assert_eq!(resumed.spec.pod_restart.as_ref().unwrap().pod_uid, "uid-2");
        assert!(resumed.spec.rollout_restart_at.is_none());
        let status = image_rollout_operation_status_json(&resumed, 2, 3, phase, None, None, false);
        assert_eq!(status["type"], "PodRestart");
        assert_eq!(status["id"], "PodRestart-uid-2");
        assert_eq!(status["targetPodUid"], "uid-2");
        assert_eq!(status["targetPodName"], "rmqtt-1");
        assert_eq!(status["manualRestart"], "rollback-on-timeout");
        assert_eq!(status["stableSince"], "2026-09-09T00:00:00Z");
        assert!(!image_rollout_terminal_target_superseded(&operation, &latest.spec));
    }
}

#[test]
fn malformed_saved_identity_cannot_fall_back_to_deleting_by_name() {
    let operation =
        image_rollout_operation_status_json(&target_cluster(), 2, 3, "DeletingPod", None, None, false);
    for (field, value) in [
        ("targetPodUid", json!(null)),
        ("targetPodUid", json!("")),
        ("targetPodName", json!("rmqtt-0")),
        ("manualRestart", json!(null)),
        ("manualRestart", json!("force")),
        ("nodeId", json!(0)),
        ("targetImage", json!(null)),
    ] {
        let mut malformed = operation.clone();
        malformed[field] = value;
        let saved = serde_json::from_value(malformed).unwrap();
        assert!(resume_cluster(&cluster(), &saved).is_err());
    }

    for phase in ["ForceFinalizing", "ForceFinalized"] {
        let mut inconsistent = operation.clone();
        inconsistent["phase"] = json!(phase);
        let saved = serde_json::from_value(inconsistent).unwrap();
        assert!(resume_cluster(&cluster(), &saved).is_err());
    }
}

#[test]
fn annotation_restart_freezes_its_own_timeout_policy() {
    let mut configured = cluster();
    configured.spec.raft.drain_timeout_action = DrainTimeoutAction::ForceContinue;
    configured.spec.image_update.stabilization_seconds = 0;
    let target = target_cluster().spec.pod_restart.unwrap();
    configure(&mut configured, target);
    assert!(!configured.spec.image_update.auto_rollout);
    assert!(operator_managed_rollout_enabled(&configured.spec));
    assert_eq!(configured.spec.raft.drain_timeout_action, DrainTimeoutAction::Rollback);
    assert_eq!(configured.spec.image_update.stabilization_seconds, 10);

    let mut forced = cluster();
    configure(
        &mut forced,
        PodRestartTarget {
            pod_name: "rmqtt-1".into(),
            pod_uid: "uid-2".into(),
            node_id: 2,
            manual_restart: PodManualRestart::Force,
        },
    );
    assert_eq!(forced.spec.raft.drain_timeout_action, DrainTimeoutAction::ForceContinue);
    for phase in ["ForceFinalizing", "ForceFinalized"] {
        let operation: OperationStatus = serde_json::from_value(image_rollout_operation_status_json(
            &forced,
            2,
            3,
            phase,
            None,
            Some("blocked"),
            false,
        ))
        .unwrap();
        let resumed = resume_cluster(&cluster(), &operation).unwrap();
        assert_eq!(resumed.spec.raft.drain_timeout_action, DrainTimeoutAction::ForceContinue);
        assert_eq!(resumed.spec.pod_restart.unwrap().manual_restart, PodManualRestart::Force);
    }
}

#[test]
fn restarting_the_leader_selects_a_ready_caught_up_sibling() {
    let cluster = target_cluster();
    let mut pods: Vec<Pod> =
        serde_json::from_value(json!([pod_json(1, false), pod_json(2, true), pod_json(3, false)])).unwrap();
    let mut membership = membership();
    // Image updates may be disabled; existing healthy peers can receive leadership.
    pods[0].spec.containers[0].image = "rmqtt:older".into();
    assert_eq!(rollout_leadership_target(&membership, &pods, "rmqtt", &cluster.spec, 2), Some(1));
    membership.raft_peers[0].matched_index = 9;
    assert_eq!(rollout_leadership_target(&membership, &pods, "rmqtt", &cluster.spec, 2), Some(3));
    pods[2].status.conditions.clear();
    assert_eq!(rollout_leadership_target(&membership, &pods, "rmqtt", &cluster.spec, 2), None);
}

#[test]
fn pod_request_annotation_cannot_be_inherited_by_every_new_pod() {
    let mut cluster = cluster();
    cluster.spec.pod.annotations.insert(POD_MANUAL_RESTART_ANNOTATION.into(), "force-on-timeout".into());
    assert!(validate_spec(&cluster.spec).is_err());
}

#[test]
fn force_on_timeout_requires_an_identity_matched_capability() {
    let mut supported = membership();
    supported.supports = json!({"forceDrainFinalize": true});
    assert!(validate_force_drain_finalize_support(2, &supported).is_ok());

    let mut wrong_identity = supported.clone();
    wrong_identity.id = 3;
    assert!(validate_force_drain_finalize_support(2, &wrong_identity).is_err());

    for capability in [Value::Null, json!(false), json!("true")] {
        let mut unsupported = membership();
        unsupported.supports = json!({"forceDrainFinalize": capability});
        assert!(validate_force_drain_finalize_support(2, &unsupported).is_err());
    }
}

#[test]
fn forced_cleanup_revalidates_membership_before_leave_or_delete() {
    let mut healthy = membership();
    healthy.id = 1;
    healthy.leader_id = Some(1);
    assert_eq!(
        forced_pod_restart_leave_decision(&healthy, 3, 2),
        ForcedPodRestartLeaveDecision::RequestLeave
    );

    let mut unhealthy = healthy.clone();
    unhealthy.raft_peers.retain(|peer| peer.id != 3);
    assert!(matches!(
        forced_pod_restart_leave_decision(&unhealthy, 3, 2),
        ForcedPodRestartLeaveDecision::Wait(reason) if reason.contains("no longer ready")
    ));

    let mut removed = healthy;
    removed.voters.retain(|id| *id != 2);
    removed.raft_peers.retain(|peer| peer.id != 2);
    removed.business_peers.retain(|peer| peer["id"] != 2);
    assert_eq!(forced_pod_restart_leave_decision(&removed, 3, 2), ForcedPodRestartLeaveDecision::DeletePod);
}

// Minimal local Kubernetes HTTP stub: captures the actual request bodies and
// rejects unexpected call counts without contacting any real cluster.
type Requests = Arc<Mutex<Vec<(String, Value)>>>;

fn kube_stub(responses: Vec<(u16, Value)>) -> (KubeClient, Requests, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let thread = std::thread::spawn(move || {
        for (status, response) in responses {
            let deadline = Instant::now() + Duration::from_secs(5);
            let (stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => panic!("missing expected Kubernetes request: {e}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((key, value)) = line.split_once(':') {
                    if key.eq_ignore_ascii_case("content-length") {
                        content_length = value.trim().parse().unwrap();
                    }
                }
            }
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            captured.lock().unwrap().push((
                request_line.trim().into(),
                if body.is_empty() { Value::Null } else { serde_json::from_slice(&body).unwrap() },
            ));
            let body = response.to_string();
            write!(reader.get_mut(), "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });
    let kube = KubeClient {
        client: Client::builder().no_proxy().timeout(Duration::from_secs(3)).build().unwrap(),
        base_url: format!("http://{address}"),
        token: "test".into(),
    };
    (kube, requests, thread)
}

#[tokio::test]
async fn accepting_a_request_only_persists_one_operation_and_does_not_delete() {
    let list = json!({"items": [pod_json(1, false), pod_json(2, true), pod_json(3, true)]});
    let (kube, requests, server) =
        kube_stub(vec![(200, list.clone()), (200, list.clone()), (200, list), (200, json!({}))]);
    assert!(start_pending(&kube, "test", &cluster(), &stateful_set(), &membership()).await.unwrap());
    server.join().unwrap();
    let requests = requests.lock().unwrap();
    assert!(requests[..3].iter().all(|(line, _)| line.starts_with("GET ")));
    assert!(requests[3]
        .0
        .starts_with("PATCH /apis/rmqtt.io/v1alpha1/namespaces/test/rmqttclusters/rmqtt/status"));
    let status = &requests[3].1["status"];
    assert_eq!(status["operation"]["type"], "PodRestart");
    assert_eq!(status["operation"]["nodeId"], 2);
    assert_eq!(status["operation"]["targetPodUid"], "uid-2");
    assert_eq!(status["operation"]["manualRestart"], "rollback-on-timeout");
    assert_eq!(status["operation"]["phase"], "PreparingNode");
    assert!(status["updatedReplicas"].is_null());
}

#[tokio::test]
async fn invalid_manual_restart_is_reported_without_starting_an_operation() {
    let mut invalid = pod_json(2, false);
    invalid["metadata"]["annotations"][POD_MANUAL_RESTART_ANNOTATION] = json!("force");
    let list = json!({"items": [pod_json(1, false), invalid, pod_json(3, false)]});
    let (kube, requests, server) = kube_stub(vec![(200, list), (200, json!({}))]);
    assert!(start_pending(&kube, "test", &cluster(), &stateful_set(), &membership()).await.unwrap());
    server.join().unwrap();

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let status = &requests[1].1["status"];
    assert_eq!(status["phase"], "InvalidPodRestartRequest");
    assert!(status["message"].as_str().unwrap().contains("rollback-on-timeout or force-on-timeout"));
    assert!(status.get("operation").is_none());
    assert!(requests.iter().all(|(line, _)| !line.starts_with("DELETE ")));
}

#[tokio::test]
async fn forced_cleanup_result_and_resume_phase_are_recorded_together() {
    let summary = ForceDrainFinalizeSummary {
        target_id: 2,
        reason: "pod_restart_drain_timeout".into(),
        blockers: "sessions=1".into(),
        nodes_acknowledged: 3,
        handoff_messages_discarded: 4,
        local_sessions_requested: 1,
        local_session_acks: 1,
        ..Default::default()
    };
    let (kube, requests, server) = kube_stub(vec![(200, json!({}))]);
    record_forced_pod_restart_drain_status(&kube, "test", &target_cluster(), &summary).await.unwrap();
    server.join().unwrap();

    let requests = requests.lock().unwrap();
    let status = &requests[0].1["status"];
    assert_eq!(status["phase"], "ForceFinalizingPodRestart");
    assert_eq!(status["operation"]["phase"], "ForceFinalized");
    assert!(status["operation"]["lastError"].is_null());
    assert_eq!(status["lastForcedDrain"]["targetId"], 2);
    assert_eq!(status["lastForcedDrain"]["nodesAcknowledged"], 3);
    assert_eq!(status["lastForcedDrain"]["handoffMessagesDiscarded"], 4);
}

#[tokio::test]
async fn active_operations_and_scaling_keep_annotations_queued_without_side_effects() {
    let (kube, requests, server) = kube_stub(vec![]);
    server.join().unwrap();
    for kind in ["PodRestart", "RollingRestart", "RollingImageUpdate", "RemoveVoter", "AddLearnerAndPromote"]
    {
        let mut cluster = cluster();
        cluster.status = Some(serde_json::from_value(json!({"operation": {"type": kind}})).unwrap());
        assert!(!start_pending(&kube, "test", &cluster, &stateful_set(), &membership()).await.unwrap());
    }
    for status in [
        json!({"confChange": {"type": "AddLearnerAndPromote"}}),
        json!({"quorumRecovery": {"phase": "WaitingNaturalRecovery"}}),
    ] {
        let mut cluster = cluster();
        cluster.status = Some(serde_json::from_value(status).unwrap());
        assert!(!start_pending(&kube, "test", &cluster, &stateful_set(), &membership()).await.unwrap());
    }
    let mut cluster = cluster();
    cluster.spec.replicas = 4;
    assert!(!start_pending(&kube, "test", &cluster, &stateful_set(), &membership()).await.unwrap());
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_unhealthy_sibling_prevents_starting_the_requested_restart() {
    let mut unhealthy = pod_json(1, false);
    unhealthy["status"]["conditions"] = json!([]);
    let list = json!({"items": [unhealthy, pod_json(2, true), pod_json(3, false)]});
    let (kube, requests, server) = kube_stub(vec![(200, list), (200, json!({}))]);
    assert!(start_pending(&kube, "test", &cluster(), &stateful_set(), &membership()).await.unwrap());
    server.join().unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests[1].1["status"]["phase"], "WaitingPodRestartPreconditions");
    assert!(requests[1].1["status"].get("operation").is_none());
}

#[tokio::test]
async fn deleting_the_original_uses_a_uid_precondition_and_never_retries_by_name() {
    let (kube, requests, server) = kube_stub(vec![(409, json!({"reason": "Conflict"}))]);
    delete_rollout_pod(&kube, "test", &target_cluster(), 2).await.unwrap();
    server.join().unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].0.starts_with("DELETE /api/v1/namespaces/test/pods/rmqtt-1 "));
    assert_eq!(requests[0].1["preconditions"]["uid"], "uid-2");
}

#[tokio::test]
async fn replacement_during_drain_prevents_requesting_leave_on_the_new_process() {
    let mut replacement = pod_json(2, false);
    replacement["metadata"]["uid"] = json!("replacement-2");
    let (kube, _, server) = kube_stub(vec![(200, replacement)]);
    assert!(!original_still_present(&kube, "test", &target_cluster()).await.unwrap());
    server.join().unwrap();
}

#[tokio::test]
async fn successful_completion_releases_lock_without_restarting_siblings() {
    let list = json!({"items": [pod_json(1, false), pod_json(2, false), pod_json(3, true)]});
    let (kube, requests, server) = kube_stub(vec![(200, list), (200, json!({}))]);
    record_completion(&kube, "test", &target_cluster(), 3, Some(&membership()), None).await.unwrap();
    server.join().unwrap();
    let requests = requests.lock().unwrap();
    let status = &requests[1].1["status"];
    assert_eq!(status["lastPodRestart"]["result"], "Succeeded");
    assert!(status["operation"].is_null());
    assert!(status.get("currentImage").is_none());
    assert!(status.get("observedRestartAt").is_none());
    assert!(requests.iter().all(|(line, _)| !line.starts_with("DELETE ")));
}

#[tokio::test]
async fn rollback_consumes_the_failed_request_and_reports_the_reason_without_deletion() {
    let (kube, requests, server) = kube_stub(vec![
        (200, pod_json(2, true)),
        (200, json!({})),
        (200, json!({"items": [pod_json(1, false), pod_json(2, false), pod_json(3, true)]})),
        (200, json!({})),
    ]);
    record_completion(&kube, "test", &target_cluster(), 3, None, Some("drain exceeded 300 seconds"))
        .await
        .unwrap();
    server.join().unwrap();
    let requests = requests.lock().unwrap();
    assert!(requests[1].0.starts_with("PATCH /api/v1/namespaces/test/pods/rmqtt-1 "));
    assert_eq!(requests[1].1["metadata"]["uid"], "uid-2");
    assert!(requests[1].1["metadata"]["annotations"][POD_MANUAL_RESTART_ANNOTATION].is_null());
    assert_eq!(requests[3].1["status"]["lastPodRestart"]["result"], "Failed");
    assert!(requests[3].1["status"]["lastPodRestart"]["message"].as_str().unwrap().contains("300 seconds"));
    assert!(requests[3].1["status"]["operation"].is_null());
    assert!(requests.iter().all(|(line, _)| !line.starts_with("DELETE ")));
}

#[tokio::test]
async fn rollback_does_not_consume_a_new_request_on_the_replacement_uid() {
    let mut replacement = pod_json(2, true);
    replacement["metadata"]["uid"] = json!("replacement-2");
    let (kube, requests, server) =
        kube_stub(vec![(200, replacement), (200, json!({"items": []})), (200, json!({}))]);
    record_completion(&kube, "test", &target_cluster(), 3, None, Some("rolled back")).await.unwrap();
    server.join().unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[..2].iter().all(|(line, _)| line.starts_with("GET ")));
    assert!(requests[2].0.contains("/rmqttclusters/rmqtt/status"));
}
