//! Short, restartable external-deletion steps. Membership ownership and drain
//! acknowledgements live in CR status, never in a reconcile-local boolean.
use super::*;
use futures::{stream::FuturesUnordered, StreamExt};

pub(super) const FINALIZER: &str = "rmqtt.io/external-membership-cleanup";
const PLANNED_DRAIN_UID: &str = "rmqtt.io/planned-drain-pod-uid";
const CLUSTER_FINALIZER: &str = "rmqtt.io/release-pod-cleanup-fences";
const REQUEST_BUDGET: Duration = Duration::from_secs(2);

#[cfg(test)]
#[path = "external_termination_tests.rs"]
mod tests;

#[cfg(test)]
tokio::task_local! { pub(super) static TEST_BROKER_URL: String; }

async fn request_once(namespace: &str, cluster: &RmqttCluster, id: i32, payload: Value) -> Result<Value> {
    try_rmqtt_action_value_once(namespace, cluster, id, payload).await.map_err(anyhow::Error::from)
}

fn has_planned_drain_marker(pod: &Pod) -> bool {
    !pod.metadata.uid.is_empty() && pod.metadata.annotations.get(PLANNED_DRAIN_UID) == Some(&pod.metadata.uid)
}

fn planned_removal_confirmed(pod: &Pod, id: i32, view: Option<&MembershipStatus>) -> bool {
    has_planned_drain_marker(pod)
        && view.is_some_and(|m| {
            !voter_exists(m, id) && !m.learners.contains(&id) && !member_exists(&m.business_peers, id)
        })
}

fn joining_without_business(view: Option<&MembershipStatus>, id: i32) -> bool {
    // Do not use Pod readiness: an unready/terminating business sender must
    // still acknowledge the barrier. Without an authoritative view, fail closed.
    view.is_some_and(|m| {
        m.learners.contains(&id) && !voter_exists(m, id) && !member_exists(&m.business_peers, id)
    })
}

async fn request(namespace: &str, cluster: &RmqttCluster, id: i32, payload: Value) -> Result<Value> {
    // No inline retries: another reconcile must get a chance to discover new
    // deletions. A timed-out mutation is ambiguous, not proof it didn't run.
    let value = tokio::time::timeout(REQUEST_BUDGET, request_once(namespace, cluster, id, payload.clone()))
        .await
        .context("external control request budget exhausted")??;
    if !matches!(
        payload["action"].as_str(),
        Some(
            "membership"
                | "termination_status"
                | "termination_bind"
                | "termination_complete"
                | "routing_status"
                | "drain_status"
        )
    ) && value.get("ok").and_then(Value::as_bool) != Some(true)
    {
        bail!("node {id} rejected {payload}: {value}");
    }
    Ok(value)
}

async fn membership(namespace: &str, cluster: &RmqttCluster, ids: &[i32]) -> Result<MembershipStatus> {
    let mut queries = ids
        .iter()
        .map(|id| async move {
            let value = request(namespace, cluster, *id, json!({"action": "membership"})).await?;
            serde_json::from_value::<MembershipStatus>(value).map_err(anyhow::Error::from)
        })
        .collect::<FuturesUnordered<_>>();
    while let Some(result) = queries.next().await {
        if let Ok(status) = result {
            if is_available_authoritative_leader(&status) {
                return Ok(status);
            }
        }
    }
    bail!("no available authoritative raft leader");
}

fn identity_payload(target: &ExternalTerminationTargetStatus, action: &str) -> Value {
    json!({
        "action": action,
        "expectedPodUid": target.pod_uid,
        "expectedBootId": target.boot_id,
    })
}

fn topology_key(pods: &[Pod], name: &str, participants: &[i32], destinations: &[i32]) -> String {
    let identities = participants
        .iter()
        .map(|id| (*id, pod_for_node(pods, name, *id).map(|pod| pod.metadata.uid.as_str())))
        .collect::<Vec<_>>();
    json!([identities, destinations]).to_string()
}

async fn broadcast(
    namespace: &str,
    cluster: &RmqttCluster,
    target: &ExternalTerminationTargetStatus,
    participants: &[i32],
    action: &str,
    destinations: &[i32],
) -> Result<()> {
    // Activate every remote sender before switching the target to Avoid.
    let remote = participants.iter().copied().filter(|id| *id != target.node_id).collect::<Vec<_>>();
    let send = |id| async move {
        let mut payload =
            if id == target.node_id { identity_payload(target, action) } else { json!({"action": action}) };
        payload["id"] = json!(target.node_id);
        if action == "prepare_drain" {
            payload["allowedTargetNodeIds"] = json!(destinations);
        }
        if action == "force_drain_finalize" {
            payload["reason"] = json!("external_termination_deadline");
        }
        let response = request(namespace, cluster, id, payload).await?;
        if action == "prepare_drain"
            && (response["reliable_handoff"] != true
                || response["allowedTargetNodeIds"] != json!(destinations))
        {
            bail!("node {id} did not acknowledge reliable handoff destinations {destinations:?}");
        }
        if action == "force_drain_finalize" && id == target.node_id && response["forceDrainFinalized"] != true
        {
            bail!("target {id} did not confirm local force finalization");
        }
        Ok::<(), anyhow::Error>(())
    };
    // Force finalization must reach the target even when a peer is down. Other
    // broadcasts also attempt every peer; no slow first peer starves the rest.
    if action == "activate_drain" {
        for result in futures::future::join_all(remote.into_iter().map(send)).await {
            result?;
        }
        if participants.contains(&target.node_id) {
            send(target.node_id).await?;
        }
    } else {
        for result in futures::future::join_all(participants.iter().copied().map(send)).await {
            result?;
        }
    }
    Ok(())
}

fn shorten_deadline(
    target: &mut ExternalTerminationTargetStatus,
    observed_at: DateTime<Utc>,
    remaining: Duration,
) {
    let candidate = observed_at + chrono::Duration::from_std(remaining).unwrap_or_default();
    let existing =
        DateTime::parse_from_rfc3339(&target.deadline_at).ok().map(|value| value.with_timezone(&Utc));
    target.deadline_at = existing
        .map_or(candidate, |deadline| deadline.min(candidate))
        .to_rfc3339_opts(SecondsFormat::Millis, true);
}

fn observe_missing_pod(target: &mut ExternalTerminationTargetStatus, pods: &[Pod], name: &str) {
    if target.phase == "Completed" || external_target_has_old_pod(pods, name, target) {
        return;
    }
    target.outcome = Some("CompletedWithLoss".into());
    // An unfenced old ordinal may already be reused between the Pod list and
    // a leader request. Never send id-only cleanup for it. Let the replacement
    // reuse/repair membership; retain its marker until recovery is verified.
    target.phase = "Completed".into();
    target.membership_owner = false;
    target.last_error =
        Some("old Pod identity disappeared without a cleanup fence; id-only removal is prohibited".into());
}

fn needs_forced_finalize(target: &ExternalTerminationTargetStatus, reserve: Duration) -> bool {
    matches!(
        target.phase.as_str(),
        "WaitingForSignal"
            | "Binding"
            | "Bound"
            | "WaitingEndpointRemoval"
            | "WaitingClientDrain"
            | "Draining"
            | "ReadyToLeave"
    ) && external_target_remaining(target) <= reserve
}

#[allow(clippy::too_many_arguments)]
async fn data_step(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    mut target: ExternalTerminationTargetStatus,
    participants: &[i32],
    destinations: &[i32],
    topology: &str,
    planned_removed: bool,
) -> ExternalTerminationTargetStatus {
    let result: Result<()> = async {
        if planned_removed && external_target_remaining(&target).is_zero() {
            target.phase = "WaitingMembershipRemoval".into();
            target.outcome.get_or_insert_with(|| "CompletedWithLoss".into());
            return Ok(());
        }
        // The original controller has already drained this exact Pod and
        // confirmed removal. Do not prepare it again after NodeRemove deleted
        // its gRPC address. Still bind SIGTERM and recheck membership before
        // acknowledging completion. Forced planned drains keep their loss flag.
        if planned_removed
            && matches!(
                target.phase.as_str(),
                "Bound" | "WaitingEndpointRemoval" | "WaitingClientDrain" | "Draining" | "ReadyToLeave"
            )
        {
            let status: DrainStatus = serde_json::from_value(
                request(namespace, cluster, target.node_id, json!({"action":"drain_status"})).await?,
            )?;
            target.outcome =
                Some(if status.scale_in_safe_to_leave() { "Completed" } else { "CompletedWithLoss" }.into());
            target.phase = "WaitingMembershipRemoval".into();
            return Ok(());
        }
        let reserve = Duration::from_secs(cluster.spec.graceful_shutdown.finalize_reserve_seconds);
        if !planned_removed && needs_forced_finalize(&target, reserve) {
            target.phase = "ForceFinalizing".into();
            target.outcome = Some("CompletedWithLoss".into());
        }
        match target.phase.as_str() {
            "WaitingForSignal" | "Binding" => {
                let probe_started = Utc::now();
                let status: TerminationStatus = serde_json::from_value(
                    request(namespace, cluster, target.node_id, json!({"action": "termination_status"}))
                        .await?,
                )?;
                if !status.enabled
                    || status.node_id != target.node_id
                    || status.pod_uid != target.pod_uid
                    || status.boot_id.is_empty()
                {
                    bail!("broker termination capability or identity mismatch");
                }
                if !status.terminating {
                    return Ok(());
                }
                if let Some(remaining) = status.remaining_millis {
                    // Anchor at request START, not response receipt. Network
                    // latency must never extend the broker's hard deadline.
                    shorten_deadline(&mut target, probe_started, Duration::from_millis(remaining));
                }
                target.boot_id = status.boot_id;
                if status.completed {
                    target.phase = "WaitingMembershipRemoval".into();
                } else {
                    let mut payload = identity_payload(&target, "termination_bind");
                    payload["remainingMillis"] = json!(external_target_remaining(&target).as_millis() as u64);
                    payload["coordinatorEpoch"] = json!(target.pod_uid);
                    request(namespace, cluster, target.node_id, payload).await?;
                    target.phase = "Bound".into();
                }
            }
            "Bound" | "WaitingEndpointRemoval" | "WaitingClientDrain" | "Draining" | "ReadyToLeave" => {
                if target.prepared_topology != topology {
                    target.stable_observations = 0;
                    if target.phase == "ReadyToLeave" {
                        target.phase = "Draining".into();
                    }
                    // No rollback clear_drain on error: an existing Avoid
                    // marker must survive a failed topology refresh.
                    broadcast(namespace, cluster, &target, participants, "prepare_drain", destinations)
                        .await?;
                    target.prepared_topology = topology.to_string();
                }
                if destinations.is_empty() {
                    bail!("no healthy handoff destination");
                }
                match target.phase.as_str() {
                    "Bound" => {
                        request(namespace, cluster, target.node_id, identity_payload(&target, "quiesce"))
                            .await?;
                        patch_pod_draining(
                            kube,
                            namespace,
                            &pod_name(&cluster.metadata.name, target.node_id),
                        )
                        .await?;
                        target.phase = "WaitingEndpointRemoval".into();
                        target.endpoint_absent_since = None;
                    }
                    "WaitingEndpointRemoval" => {
                        if pod_in_client_service_endpoints(
                            kube,
                            namespace,
                            &cluster.metadata.name,
                            &pod_name(&cluster.metadata.name, target.node_id),
                        )
                        .await?
                        {
                            target.endpoint_absent_since = None;
                        } else {
                            let since = target.endpoint_absent_since.get_or_insert_with(utc_now_rfc3339);
                            if Utc::now()
                                .signed_duration_since(DateTime::parse_from_rfc3339(since)?)
                                .num_seconds()
                                >= cluster.spec.raft.endpoint_removal_stable_seconds.max(1) as i64
                            {
                                broadcast(
                                    namespace,
                                    cluster,
                                    &target,
                                    participants,
                                    "activate_drain",
                                    destinations,
                                )
                                .await?;
                                // The external-deletion controller has a short
                                // per-request budget. Start the client handshake
                                // on the broker and poll its identity-bound result
                                // in later reconciliations rather than blocking.
                                let mut prepare = identity_payload(&target, "prepare_client_drain");
                                prepare["coordinatorEpoch"] = json!(target.pod_uid);
                                request(namespace, cluster, target.node_id, prepare).await?;
                                log::info!(
                                    "ams_mqtt_drain stage=external_prepare_requested namespace={namespace} cluster={} node_id={} pod_uid={}",
                                    cluster.metadata.name, target.node_id, target.pod_uid,
                                );
                                target.phase = "WaitingClientDrain".into();
                            }
                        }
                    }
                    "WaitingClientDrain" => {
                        let mut payload = identity_payload(&target, "client_drain_status");
                        payload["coordinatorEpoch"] = json!(target.pod_uid);
                        let status = request(namespace, cluster, target.node_id, payload).await?;
                        match status["phase"].as_str() {
                            Some("ready") => {
                                log::info!(
                                    "ams_mqtt_drain stage=external_clients_ready namespace={namespace} cluster={} node_id={} pod_uid={} drain_id={}",
                                    cluster.metadata.name, target.node_id, target.pod_uid,
                                    status["result"]["drainId"],
                                );
                                request(
                                    namespace,
                                    cluster,
                                    target.node_id,
                                    identity_payload(&target, "drain"),
                                )
                                .await?;
                                target.phase = "Draining".into();
                                target.stable_observations = 0;
                            }
                            Some("preparing") => {
                                bail!("waiting for MQTT clients to hand off their outbound work")
                            }
                            Some("failed") => bail!("MQTT client drain failed: {}", status["error"]),
                            Some("missing") => {
                                let mut prepare = identity_payload(&target, "prepare_client_drain");
                                prepare["coordinatorEpoch"] = json!(target.pod_uid);
                                request(namespace, cluster, target.node_id, prepare).await?;
                                bail!("restarted missing MQTT client drain preparation")
                            }
                            _ => bail!("invalid MQTT client drain status: {status}"),
                        }
                    }
                    "Draining" => {
                        let local = async {
                            serde_json::from_value::<DrainStatus>(
                                request(
                                    namespace,
                                    cluster,
                                    target.node_id,
                                    json!({"action": "drain_status"}),
                                )
                                .await?,
                            )
                            .map_err(anyhow::Error::from)
                        };
                        let routing = futures::future::join_all(participants.iter().map(|id| async {
                            serde_json::from_value::<RoutingStatus>(
                                request(
                                    namespace,
                                    cluster,
                                    *id,
                                    json!({"action": "routing_status", "id": target.node_id}),
                                )
                                .await?,
                            )
                            .map_err(anyhow::Error::from)
                        }));
                        let (status, routing) = tokio::join!(local, routing);
                        let status = status?;
                        let routing = routing.into_iter().collect::<Result<Vec<_>>>()?;
                        if status.data_loss_detected
                            || routing_handoff_requires_rollback(
                                &routing,
                                cluster.spec.raft.handoff_rollback_threshold_percent,
                            )
                        {
                            target.phase = "ForceFinalizing".into();
                            target.outcome = Some("CompletedWithLoss".into());
                            bail!(
                                "drain safety barrier detected loss or overload: {}",
                                drain_blockers(&status, &routing)
                            );
                        }
                        if status.scale_in_safe_to_leave() && routing_barrier_ready(&routing) {
                            target.stable_observations = target.stable_observations.saturating_add(1);
                            if target.stable_observations >= DRAIN_STABLE_OBSERVATIONS {
                                target.phase = "ReadyToLeave".into();
                                target.outcome = Some("Completed".into());
                            }
                        } else {
                            target.stable_observations = 0;
                            bail!("{}", drain_blockers(&status, &routing));
                        }
                    }
                    _ => {}
                }
            }
            "ForceFinalizing" => {
                // This is data cleanup, not membership change. All expiring
                // targets get a chance, even while another owns membership.
                if !external_target_remaining(&target).is_zero() && !target.boot_id.is_empty() {
                    broadcast(
                        namespace,
                        cluster,
                        &target,
                        participants,
                        "force_drain_finalize",
                        destinations,
                    )
                    .await?;
                    target.phase = "Leaving".into();
                } else {
                    target.phase = "WaitingMembershipRemoval".into();
                }
            }
            _ => {}
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => target.last_error = None,
        Err(error) => {
            target.stable_observations = 0;
            target.last_error = Some(format!("{error:#}"));
        }
    }
    target
}

async fn pod_snapshot(kube: &KubeClient, namespace: &str, name: &str) -> Result<Option<Pod>> {
    kube.get_json_optional(&format!("/api/v1/namespaces/{namespace}/pods/{name}")).await
}

async fn set_finalizer(kube: &KubeClient, namespace: &str, pod: &Pod, retain: bool) -> Result<()> {
    let mut finalizers = pod.metadata.finalizers.clone();
    finalizers.retain(|value| value != FINALIZER);
    if retain {
        finalizers.push(FINALIZER.into());
    }
    if finalizers == pod.metadata.finalizers {
        return Ok(());
    }
    // resourceVersion makes finalizer edits a CAS and preserves other owners.
    kube.merge_patch(&format!("/api/v1/namespaces/{namespace}/pods/{}", pod.metadata.name), &json!({"metadata": {
        "uid": pod.metadata.uid, "resourceVersion": pod.metadata.resource_version, "finalizers": finalizers
    }})).await
}

fn cleanup_targets(cluster: &RmqttCluster) -> Vec<ExternalTerminationTargetStatus> {
    let mut targets = cluster
        .status
        .as_ref()
        .and_then(|status| status.external_termination_cleanup.as_ref())
        .map(|cleanup| cleanup.targets.clone())
        .unwrap_or_default();
    if let Some(batch) = cluster.status.as_ref().and_then(|status| status.external_termination_batch.as_ref())
    {
        for target in &batch.targets {
            match targets.iter_mut().find(|saved| saved.node_id == target.node_id) {
                Some(saved) if saved.pod_uid != target.pod_uid => *saved = target.clone(),
                Some(_) => {}
                None => targets.push(target.clone()),
            }
        }
    }
    targets.sort_by_key(|target| target.node_id);
    targets
}

async fn patch_cleanup_state(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    targets: &[ExternalTerminationTargetStatus],
) -> Result<()> {
    patch_status(
        kube,
        namespace,
        &cluster.metadata.name,
        json!({
            "externalTerminationBatch": null,
            "externalTerminationCleanup": if targets.is_empty() {
                Value::Null
            } else {
                json!(ExternalTerminationCleanupStatus { targets: targets.to_vec() })
            },
            "drainingNodeId": null,
            "drainingNodeIds": [],
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn clear_disabled_drain_markers(
    namespace: &str,
    cluster: &RmqttCluster,
    pods: &[Pod],
    targets: Vec<ExternalTerminationTargetStatus>,
) -> Vec<ExternalTerminationTargetStatus> {
    let name = &cluster.metadata.name;
    let peers = pods
        .iter()
        .filter(|pod| pod.metadata.deletion_timestamp.is_none())
        .filter_map(|pod| {
            pod.metadata
                .name
                .strip_prefix(&format!("{name}-"))
                .and_then(|ordinal| ordinal.parse::<i32>().ok())
                .and_then(|ordinal| ordinal.checked_add(1))
        })
        .collect::<Vec<_>>();
    let mut pending = Vec::new();
    for target in targets {
        // A bound terminating broker can stop immediately instead of waiting
        // for its hard deadline. Identity mismatch on a replacement is safe.
        if !target.boot_id.is_empty() {
            let mut payload = identity_payload(&target, "termination_complete");
            payload["coordinatorEpoch"] = json!(target.pod_uid);
            payload["outcome"] = json!("CompletedWithLoss");
            if let Err(error) = request(namespace, cluster, target.node_id, payload).await {
                log::warn!(
                    "failed to release terminating broker while disabling graceful shutdown namespace={} cluster={} node_id={} error={:#}",
                    namespace,
                    cluster.metadata.name,
                    target.node_id,
                    error
                );
            }
        }
        let results = futures::future::join_all(peers.iter().map(|peer| {
            request(namespace, cluster, *peer, json!({"action":"clear_drain", "id":target.node_id}))
        }))
        .await;
        if results.iter().any(Result::is_err) {
            for (peer, result) in peers.iter().zip(&results) {
                if let Err(error) = result {
                    log::warn!(
                        "failed to clear disabled drain marker namespace={} cluster={} observer_node_id={} target_node_id={} error={:#}",
                        namespace,
                        cluster.metadata.name,
                        peer,
                        target.node_id,
                        error
                    );
                }
            }
            pending.push(target);
        }
    }
    pending
}

async fn disable_template_fence(kube: &KubeClient, namespace: &str, name: &str) -> Result<()> {
    let path = format!("/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}");
    let Some(stateful_set) = kube.get_json_optional::<Value>(&path).await? else {
        return Ok(());
    };
    let mut template_patch = json!({});
    if let Some(finalizers) =
        stateful_set.pointer("/spec/template/metadata/finalizers").and_then(Value::as_array)
    {
        if finalizers.iter().any(|value| value == FINALIZER) {
            template_patch["metadata"]["finalizers"] =
                json!(finalizers.iter().filter(|value| *value != FINALIZER).collect::<Vec<_>>());
        }
    }
    if let Some(containers) = stateful_set.pointer("/spec/template/spec/containers").and_then(Value::as_array)
    {
        // JSON merge patch replaces arrays. Keep the complete live container
        // documents, including sidecars, valueFrom entries and unknown fields.
        // The resourceVersion check prevents overwriting a concurrent edit.
        let mut containers = containers.clone();
        let mut changed = false;
        for container in containers.iter_mut().filter(|container| container["name"] == "rmqtt") {
            if let Some(env) = container.get_mut("env").and_then(Value::as_array_mut) {
                for entry in env.iter_mut().filter(|entry| entry["name"] == "RMQTT_GRACEFUL_SHUTDOWN_ENABLED")
                {
                    if entry["value"] != "false" || entry.get("valueFrom").is_some() {
                        entry["value"] = json!("false");
                        entry.as_object_mut().expect("environment entry is an object").remove("valueFrom");
                        changed = true;
                    }
                }
            }
        }
        if changed {
            template_patch["spec"]["containers"] = json!(containers);
        }
    }
    if template_patch.as_object().is_some_and(|fields| fields.is_empty()) {
        return Ok(());
    }
    // Disable only our previous template state. Re-rendering from ClusterSpec
    // here would bypass validation and an in-flight rollout's pinned image.
    kube.merge_patch(
        &path,
        &json!({
            "metadata": {
                "uid": stateful_set["metadata"]["uid"],
                "resourceVersion": stateful_set["metadata"]["resourceVersion"],
            },
            "spec": {"template": template_patch},
        }),
    )
    .await
}

pub(super) async fn reconcile_cluster_fence(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<bool> {
    let deleting = cluster.metadata.deletion_timestamp.is_some();
    let retain = cluster.spec.graceful_shutdown.enabled && !deleting;
    let present = cluster.metadata.finalizers.iter().any(|value| value == CLUSTER_FINALIZER);
    let pods = if !retain
        || cluster.status.as_ref().is_some_and(|status| {
            status.external_termination_cleanup.as_ref().is_some_and(|cleanup| !cleanup.targets.is_empty())
        }) {
        cluster_pods(kube, namespace, &cluster.metadata.name).await?
    } else {
        Vec::new()
    };

    if !deleting {
        let targets = if retain {
            cluster
                .status
                .as_ref()
                .and_then(|status| status.external_termination_cleanup.as_ref())
                .map(|cleanup| cleanup.targets.clone())
                .unwrap_or_default()
        } else {
            cleanup_targets(cluster)
        };
        if !targets.is_empty() {
            // Persist the abort before network calls. A crash cannot resurrect
            // the old batch as normal work when the feature is re-enabled.
            patch_cleanup_state(kube, namespace, cluster, &targets).await?;
            let pending = clear_disabled_drain_markers(namespace, cluster, &pods, targets).await;
            patch_cleanup_state(kube, namespace, cluster, &pending).await?;
            if retain && !pending.is_empty() {
                // Explicit re-enable waits until every surviving process has
                // forgotten the aborted targets. Existing Pods remain unfenced.
                return Ok(true);
            }
        }
    }

    if !retain {
        if !deleting {
            // Update the OnDelete template before releasing live Pod fences so
            // replacements cannot inherit the finalizer or a waiting broker.
            disable_template_fence(kube, namespace, &cluster.metadata.name).await?;
        }
        // Cluster teardown/feature disable must not orphan Pod finalizers. Run
        // even if a previous turn already removed the CR cleanup finalizer.
        for pod in &pods {
            if pod
                .metadata
                .owner_references
                .iter()
                .any(|o| o.controller && o.kind == "StatefulSet" && o.name == cluster.metadata.name)
            {
                set_finalizer(kube, namespace, pod, false).await?;
            }
        }
    }
    if retain != present {
        let mut finalizers = cluster.metadata.finalizers.clone();
        finalizers.retain(|value| value != CLUSTER_FINALIZER);
        if retain {
            finalizers.push(CLUSTER_FINALIZER.into());
        }
        kube.merge_patch(&format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{}",cluster.metadata.name), &json!({"metadata":{
            "uid":cluster.metadata.uid,"resourceVersion":cluster.metadata.resource_version,"finalizers":finalizers
        }})).await?;
    }
    Ok(deleting)
}

pub(super) async fn ensure_fences(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    sts_uid: &str,
) -> Result<()> {
    for pod in cluster_pods(kube, namespace, &cluster.metadata.name).await? {
        if !pod
            .metadata
            .owner_references
            .iter()
            .any(|owner| owner.controller && owner.kind == "StatefulSet" && owner.uid == sts_uid)
        {
            continue;
        }
        if cluster.spec.graceful_shutdown.enabled
            && pod.metadata.deletion_timestamp.is_none()
            && !has_planned_drain_marker(&pod)
        {
            set_finalizer(kube, namespace, &pod, true).await?;
        } else if !cluster.spec.graceful_shutdown.enabled {
            set_finalizer(kube, namespace, &pod, false).await?;
        }
    }
    Ok(())
}

/// Only normal scale-in/rollout call this AFTER their drain/removal barriers.
/// Disaster-recovery deletions deliberately use release_for_planned_delete
/// instead: releasing a finalizer alone is not proof of a completed drain.
pub(super) async fn release_after_planned_drain(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    pod_name: &str,
    expected_uid: Option<&str>,
) -> Result<()> {
    if !cluster.spec.graceful_shutdown.enabled {
        return Ok(());
    }
    if let Some(pod) = pod_snapshot(kube, namespace, pod_name).await? {
        if expected_uid.is_some_and(|uid| uid != pod.metadata.uid) {
            bail!("planned drain Pod UID changed before deletion");
        }
        let finalizers = pod.metadata.finalizers.iter().filter(|v| *v != FINALIZER).collect::<Vec<_>>();
        // One CAS records the intent and releases our fence; never annotate a
        // StatefulSet template or transfer this proof to a replacement UID.
        kube.merge_patch(
            &format!("/api/v1/namespaces/{namespace}/pods/{pod_name}"),
            &json!({"metadata": {
                "uid": pod.metadata.uid, "resourceVersion": pod.metadata.resource_version,
                "annotations": {PLANNED_DRAIN_UID: pod.metadata.uid}, "finalizers": finalizers
            }}),
        )
        .await?;
    }
    Ok(())
}

/// Operator-owned deletions already have their own membership/recovery fence.
/// Do not make them wait for the external-deletion controller on the next turn.
pub(super) async fn release_for_planned_delete(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    pod_name: &str,
    expected_uid: Option<&str>,
) -> Result<()> {
    if !cluster.spec.graceful_shutdown.enabled {
        return Ok(());
    }
    if let Some(pod) = pod_snapshot(kube, namespace, pod_name).await? {
        if expected_uid.is_none_or(|uid| uid == pod.metadata.uid) {
            set_finalizer(kube, namespace, &pod, false).await?;
        }
    }
    Ok(())
}

async fn membership_step(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    target: &mut ExternalTerminationTargetStatus,
    query_ids: &[i32],
) -> Result<()> {
    // Refresh immediately before mutation, not before potentially slow drains.
    let membership = membership(namespace, cluster, query_ids).await?;
    let pod = pod_snapshot(kube, namespace, &pod_name(&cluster.metadata.name, target.node_id)).await?;
    let old_pod = pod
        .as_ref()
        .filter(|pod| pod.metadata.uid == target.pod_uid && pod.metadata.deletion_timestamp.is_some());
    if old_pod.is_none() {
        target.phase = "Completed".into();
        target.membership_owner = false;
        target.outcome = Some("CompletedWithLoss".into());
        return Ok(());
    }
    let raft_member =
        voter_exists(&membership, target.node_id) || membership.learners.contains(&target.node_id);
    let business_member = member_exists(&membership.business_peers, target.node_id);
    if !raft_member && !business_member {
        if !target.boot_id.is_empty() && !external_target_remaining(target).is_zero() {
            let mut payload = identity_payload(target, "termination_complete");
            payload["coordinatorEpoch"] = json!(target.pod_uid);
            payload["outcome"] = json!(target.outcome.as_deref().unwrap_or("CompletedWithLoss"));
            // Failure is retried; never silently turn it into a clean outcome.
            request(namespace, cluster, target.node_id, payload).await?;
        }
        target.phase = "Completed".into();
        target.membership_owner = false;
        return Ok(());
    }
    if voter_exists(&membership, target.node_id)
        && !removal_preserves_recent_quorum(&membership, target.node_id)
    {
        bail!("removing node {} would not preserve recent quorum", target.node_id);
    }
    if target.phase == "Leaving" && !target.boot_id.is_empty() && !external_target_remaining(target).is_zero()
    {
        request(namespace, cluster, target.node_id, identity_payload(target, "leave")).await?;
        target.phase = "WaitingMembershipRemoval".into();
        return Ok(());
    }
    // Holding the *old* Pod object prevents StatefulSet ordinal reuse until
    // all authoritative removals have been observed. Re-list alone cannot
    // close this TOCTOU race. Legacy Pods without a fence wait for replacement.
    if !old_pod.is_some_and(|pod| pod.metadata.finalizers.iter().any(|value| value == FINALIZER)) {
        bail!("old Pod lacks cleanup finalizer; refusing id-only removal");
    }
    let leader = membership.leader_id.ok_or_else(|| anyhow!("leader unavailable"))?;
    if raft_member {
        request(namespace, cluster, leader, json!({"action": "remove_voter", "id": target.node_id})).await?;
        target.phase = "WaitingMembershipRemoval".into();
    } else if business_member {
        request(namespace, cluster, leader, json!({"action": "force_node_remove", "id": target.node_id}))
            .await?;
        target.phase = "RemovingBusinessMember".into();
    }
    Ok(())
}

pub(super) async fn handle_external_pod_termination(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    stateful_set_uid: &str,
) -> Result<bool> {
    tokio::time::timeout(
        Duration::from_secs(20),
        reconcile_batch(kube, namespace, cluster, current_replicas, stateful_set_uid),
    )
    .await
    .context("external batch round budget exhausted; persisted intents will resume next round")?
}

async fn reconcile_batch(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    stateful_set_uid: &str,
) -> Result<bool> {
    let name = &cluster.metadata.name;
    let pods = cluster_pods(kube, namespace, name).await?;
    let mut targets = cluster
        .status
        .as_ref()
        .and_then(|s| s.external_termination_batch.as_ref())
        .map(|b| b.targets.clone())
        .unwrap_or_default();
    // Include ordinals beyond current replicas: a planned scale-in may already
    // have changed the StatefulSet count while its last Pod is terminating.
    for pod in pods.iter().filter(|pod| pod.metadata.deletion_timestamp.is_some()) {
        let Some(node_id) = pod
            .metadata
            .name
            .strip_prefix(&format!("{name}-"))
            .and_then(|s| s.parse::<i32>().ok())
            .and_then(|n| n.checked_add(1))
        else {
            continue;
        };
        if stateful_set_uid.is_empty()
            || !pod.metadata.owner_references.iter().any(|o| {
                o.controller && o.kind == "StatefulSet" && o.name == *name && o.uid == stateful_set_uid
            })
        {
            continue;
        }
        upsert_external_termination_target(
            &mut targets,
            pod,
            node_id,
            cluster.spec.graceful_shutdown.timeout_seconds,
        );
    }
    if targets.is_empty() {
        return Ok(false);
    }
    for target in &mut targets {
        observe_missing_pod(target, &pods, name);
    }

    let query_ids =
        (1..=current_replicas).filter(|id| pod_for_node(&pods, name, *id).is_some()).collect::<Vec<_>>();
    let view = membership(namespace, cluster, &query_ids).await.ok();
    let planned = cluster.status.as_ref().and_then(|s| s.operation.as_ref()).filter(|op| {
        op.node_id > 0
            && op.phase != "RolledBack"
            && (op.kind == "RemoveVoter" || is_rollout_operation_kind(&op.kind))
    });
    let destinations = view
        .as_ref()
        .map(|m| {
            m.voters
                .iter()
                .copied()
                .filter(|id| {
                    !targets.iter().any(|t| t.node_id == *id)
                        && planned.is_none_or(|op| op.node_id != *id)
                        && raft_peer_caught_up(m, *id)
                        && pod_for_node(&pods, name, *id).is_some_and(pod_is_ready)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    // Terminating senders are INCLUDED until their process deadline or clean
    // completion, but are NEVER destinations. Unreachable live peers block a
    // clean barrier, rather than being mistaken for successfully drained peers.
    let participants = query_ids
        .iter()
        .copied()
        .filter(|id| {
            !joining_without_business(view.as_ref(), *id)
                && !pod_for_node(&pods, name, *id)
                    .is_some_and(|p| planned_removal_confirmed(p, *id, view.as_ref()))
                && !targets.iter().any(|t| {
                    t.node_id == *id && (t.phase == "Completed" || external_target_remaining(t).is_zero())
                })
        })
        .collect::<Vec<_>>();
    let topology = topology_key(&pods, name, &participants, &destinations);

    // Persist discovery before requests, including when leader transfer fails.
    patch_external_termination_batch(
        kube,
        namespace,
        cluster,
        current_replicas,
        &targets,
        "processing external termination batch".into(),
    )
    .await?;
    let inputs = targets
        .iter()
        .enumerate()
        .filter(|(_, t)| t.phase != "Completed" && !t.membership_owner)
        .map(|(index, target)| (index, target.clone()))
        .collect::<Vec<_>>();
    let mut steps = inputs
        .into_iter()
        .map(|(index, target)| {
            let planned_removed = pod_for_node(&pods, name, target.node_id).is_some_and(|pod| {
                pod.metadata.uid == target.pod_uid
                    && planned_removal_confirmed(pod, target.node_id, view.as_ref())
            });
            let participants = &participants;
            let destinations = &destinations;
            let topology = &topology;
            async move {
                (
                    index,
                    data_step(
                        kube,
                        namespace,
                        cluster,
                        target,
                        participants,
                        destinations,
                        topology,
                        planned_removed,
                    )
                    .await,
                )
            }
        })
        .collect::<FuturesUnordered<_>>();
    while let Some((index, target)) = steps.next().await {
        targets[index] = target;
        // Persist each result as it arrives, not after the slowest target.
        patch_external_termination_batch(
            kube,
            namespace,
            cluster,
            current_replicas,
            &targets,
            "persisted external termination progress".into(),
        )
        .await?;
    }

    // Refresh an already-paused planned drain too. Otherwise its migration
    // worker can keep sending snapshots to a subsequently deleted Pod.
    if let Some(op) = planned.filter(|op| !targets.iter().any(|t| t.node_id == op.node_id)) {
        if matches!(op.phase.as_str(), "Preempted" | "Draining") {
            let planned_target =
                ExternalTerminationTargetStatus { node_id: op.node_id, ..Default::default() };
            let peers = participants.iter().copied().filter(|id| *id != op.node_id).collect::<Vec<_>>();
            // Planned operations have no SIGTERM identity. The target is
            // updated separately using its current control endpoint.
            let _ =
                broadcast(namespace, cluster, &planned_target, &peers, "prepare_drain", &destinations).await;
            let _ = request(
                namespace,
                cluster,
                op.node_id,
                json!({"action":"prepare_drain", "id":op.node_id, "allowedTargetNodeIds":destinations}),
            )
            .await;
        }
    }

    // Transfer is a separate step; it does not bypass topology refresh/data
    // progress. Confirmation comes from a later authoritative leader response.
    if let Some(m) = &view {
        if let Some(target) =
            targets.iter().find(|t| Some(t.node_id) == m.leader_id && t.phase != "Completed")
        {
            if let Some(destination) = destinations.first() {
                if !target.boot_id.is_empty() {
                    let mut payload = identity_payload(target, "transfer_leader");
                    payload["id"] = json!(destination);
                    let _ = request(namespace, cluster, target.node_id, payload).await;
                }
            }
            return Ok(true);
        }
    }

    // A planned leave may have timed out after sending a Raft mutation. Finish
    // observing/retrying THAT owner before granting a different external owner.
    let planned_pending = planned
        .filter(|op| {
            matches!(
                op.phase.as_str(),
                "RequestingLeave"
                    | "WaitingMembershipRemoval"
                    | "RemovingStaleRaftMember"
                    | "RemovingStaleBusinessMember"
            )
        })
        .filter(|op| !targets.iter().any(|t| t.node_id == op.node_id));
    let mut membership_work_this_round = false;
    let mut planned_still_pending = false;
    if let Some(op) = planned_pending {
        let fresh = membership(namespace, cluster, &query_ids).await?;
        if voter_exists(&fresh, op.node_id)
            || fresh.learners.contains(&op.node_id)
            || member_exists(&fresh.business_peers, op.node_id)
        {
            planned_still_pending = true;
            membership_work_this_round = true;
            // Keep the original operation persisted. No id-only removal of a
            // running planned target; retry its identity-bound leave only.
            if let Some(pod) = pod_for_node(&pods, name, op.node_id) {
                let status =
                    request(namespace, cluster, op.node_id, json!({"action":"termination_status"})).await;
                if let Ok(status) = status {
                    if status["podUid"] == pod.metadata.uid
                        && (!voter_exists(&fresh, op.node_id)
                            || removal_preserves_recent_quorum(&fresh, op.node_id))
                    {
                        let _ = request(namespace, cluster, op.node_id, json!({"action":"leave", "expectedPodUid":pod.metadata.uid, "expectedBootId":status["bootId"]})).await;
                    }
                }
            }
        }
    }
    if let Some(index) = external_membership_owner(&targets).filter(|_| !planned_still_pending) {
        membership_work_this_round = true;
        if !targets[index].membership_owner {
            targets[index].membership_owner = true;
            if targets[index].phase == "ReadyToLeave" {
                targets[index].phase = "Leaving".into();
            }
            patch_external_termination_batch(
                kube,
                namespace,
                cluster,
                current_replicas,
                &targets,
                "persisted exclusive membership ownership".into(),
            )
            .await?;
        }
        if let Err(error) = membership_step(kube, namespace, cluster, &mut targets[index], &query_ids).await {
            targets[index].last_error = Some(format!("{error:#}"));
        }
        patch_external_termination_batch(
            kube,
            namespace,
            cluster,
            current_replicas,
            &targets,
            "observed external membership progress".into(),
        )
        .await?;
    }

    // Completion is durable BEFORE dropping the old Pod's fence. This also
    // runs after a controller restart between the status and metadata patches.
    let mut cleared = Vec::new();
    for target in targets.iter().filter(|t| t.phase == "Completed") {
        let Some(pod) = pod_snapshot(kube, namespace, &pod_name(name, target.node_id)).await? else {
            if target.node_id > current_replicas {
                cleared.push(target.node_id);
            }
            continue;
        };
        if pod.metadata.uid == target.pod_uid {
            set_finalizer(kube, namespace, &pod, false).await?;
        } else if pod.metadata.deletion_timestamp.is_none() {
            // Recover external replacements BEFORE resuming the saved scale-in
            // or rollout. This intentionally does not overwrite operation.
            let m = match membership(namespace, cluster, &query_ids).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            if m.learners.contains(&target.node_id)
                && raft_peer_caught_up(&m, target.node_id)
                && !membership_work_this_round
                && !targets.iter().any(|t| t.phase != "Completed")
            {
                let leader = m.leader_id.ok_or_else(|| anyhow!("leader unavailable"))?;
                request(namespace, cluster, leader, json!({"action":"promote_learner", "id":target.node_id}))
                    .await?;
                break; // one membership operation per round
            }
            if pod_is_ready(&pod)
                && voter_exists(&m, target.node_id)
                && raft_peer_caught_up(&m, target.node_id)
                && member_exists(&m.business_peers, target.node_id)
            {
                let clear_peers = query_ids
                    .iter()
                    .copied()
                    .filter(|id| {
                        pod_for_node(&pods, name, *id)
                            .is_some_and(|p| p.metadata.deletion_timestamp.is_none())
                            && !joining_without_business(Some(&m), *id)
                            && !pod_for_node(&pods, name, *id)
                                .is_some_and(|p| planned_removal_confirmed(p, *id, Some(&m)))
                    })
                    .collect::<Vec<_>>();
                let marker =
                    ExternalTerminationTargetStatus { node_id: target.node_id, ..Default::default() };
                // No old identity on replacement clear: only peers, then the
                // verified new process using its own Pod UID.
                let remote =
                    clear_peers.iter().copied().filter(|id| *id != target.node_id).collect::<Vec<_>>();
                if broadcast(namespace, cluster, &marker, &remote, "clear_drain", &[]).await.is_ok()
                    && request(namespace, cluster, target.node_id, json!({"action":"clear_drain", "id":target.node_id, "expectedPodUid":pod.metadata.uid})).await.is_ok() {
                    patch_pod_serving(kube, namespace, &pod.metadata.name).await?;
                    cleared.push(target.node_id);
                }
            }
        }
        if target.node_id > current_replicas {
            cleared.push(target.node_id);
        }
    }
    targets.retain(|target| !cleared.contains(&target.node_id));
    patch_external_termination_batch(
        kube,
        namespace,
        cluster,
        current_replicas,
        &targets,
        "external batch awaiting drain, membership confirmation or replacement recovery".into(),
    )
    .await?;
    // Preserve the existing disaster-recovery path if the shutdowns exhausted
    // quorum. The recovery workflow owns its own UID-fenced Pod recycling.
    let no_live_shutdowns = targets.iter().all(|target| external_target_remaining(target).is_zero());
    Ok(!(targets.is_empty() || view.is_none() && no_live_shutdowns))
}
