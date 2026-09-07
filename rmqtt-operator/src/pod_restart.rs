//! A Pod annotation requests one safe replacement. The persisted original UID
//! is the completion token: the replacement must never be drained/deleted again.
use super::*;

pub(super) const POD_DELETE_REQUESTED_ANNOTATION: &str = "rmqtt.io/delete-requested";

#[derive(Debug, Clone)]
pub(super) struct PodRestartTarget {
    pub pod_name: String,
    pub pod_uid: String,
    pub node_id: i32,
}

impl PodRestartTarget {
    pub fn is_replacement(&self, pod: &Pod) -> bool {
        pod.metadata.name == self.pod_name
            && !pod.metadata.uid.is_empty()
            && pod.metadata.uid != self.pod_uid
            && pod.metadata.deletion_timestamp.is_none()
    }
}

fn pending_target(
    pods: &[Pod],
    cluster_name: &str,
    replicas: i32,
    stateful_set_uid: &str,
) -> Option<PodRestartTarget> {
    if stateful_set_uid.is_empty() {
        return None;
    }
    // Deterministic ordering, independent of API list order. An active operation
    // is reconciled before this queue, so only one request can run at a time.
    (1..=replicas).find_map(|node_id| {
        let pod = pod_for_node(pods, cluster_name, node_id)?;
        (pod.metadata.deletion_timestamp.is_none()
            && !pod.metadata.uid.is_empty()
            && pod.metadata.annotations.get(POD_DELETE_REQUESTED_ANNOTATION).map(String::as_str)
                == Some("true")
            && pod.metadata.owner_references.iter().any(|owner| {
                owner.controller
                    && owner.kind == "StatefulSet"
                    && owner.name == cluster_name
                    && owner.uid == stateful_set_uid
            }))
        .then(|| PodRestartTarget {
            pod_name: pod.metadata.name.clone(),
            pod_uid: pod.metadata.uid.clone(),
            node_id,
        })
    })
}

fn configure(cluster: &mut RmqttCluster, target: PodRestartTarget) {
    cluster.spec.pod_restart = Some(target);
    cluster.spec.rollout_restart_at = None;
    // This user-facing graceful deletion request never opts into lossy forced
    // continuation, even if image upgrades/scale-in have that policy configured.
    cluster.spec.raft.drain_timeout_action = DrainTimeoutAction::Rollback;
    cluster.spec.image_update.stabilization_seconds = cluster.spec.image_update.stabilization_seconds.max(10);
}

pub(super) fn resume_cluster(cluster: &RmqttCluster, operation: &OperationStatus) -> Result<RmqttCluster> {
    let target = PodRestartTarget {
        pod_name: operation.target_pod_name.clone().context("PodRestart is missing targetPodName")?,
        pod_uid: operation.target_pod_uid.clone().context("PodRestart is missing targetPodUid")?,
        node_id: operation.node_id,
    };
    if operation.kind != "PodRestart"
        || target.pod_uid.is_empty()
        || target.node_id < 1
        || target.pod_name != pod_name(&cluster.metadata.name, target.node_id)
    {
        bail!("invalid PodRestart identity; refusing to restart another Pod");
    }
    let mut resumed = cluster.clone();
    resumed.spec.image = operation.target_image.clone().context("PodRestart is missing targetImage")?;
    resumed.spec.image_pull_policy =
        operation.target_image_pull_policy.clone().context("PodRestart is missing targetImagePullPolicy")?;
    configure(&mut resumed, target);
    Ok(resumed)
}

pub(super) async fn start_pending(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    stateful_set: &StatefulSet,
    membership: &MembershipStatus,
) -> Result<bool> {
    // Do not overwrite a membership change or rollout, even when invoked from
    // a future reconciliation path that has not checked the operation lock.
    if cluster
        .status
        .as_ref()
        .is_some_and(|s| s.operation.is_some() || s.conf_change.is_some() || s.quorum_recovery.is_some())
        || stateful_set.replicas() != cluster.spec.replicas
    {
        return Ok(false);
    }
    let name = &cluster.metadata.name;
    let replicas = stateful_set.replicas();
    if stateful_set.spec.template.metadata.annotations.contains_key(POD_DELETE_REQUESTED_ANNOTATION) {
        bail!("remove {POD_DELETE_REQUESTED_ANNOTATION} from the StatefulSet template; requests must target individual Pods to avoid repeated restarts");
    }
    let pods = cluster_pods(kube, namespace, name).await?;
    let Some(target) = pending_target(&pods, name, replicas, &stateful_set.metadata.uid) else {
        return Ok(false);
    };
    if replicas < 2
        || !membership_ready_for_replicas(membership, replicas)
        || !(1..=replicas).all(|id| {
            pod_for_node(&pods, name, id)
                .is_some_and(|pod| pod_is_ready(pod) && pod.metadata.deletion_timestamp.is_none())
        })
    {
        patch_status(kube, namespace, name, json!({
            "phase": "WaitingPodRestartPreconditions",
            "message": format!("{} is queued for graceful restart; waiting for a healthy cluster with at least two Pods", target.pod_name),
        })).await?;
        return Ok(true);
    }
    let node_id = target.node_id;
    let message = format!(
        "accepted graceful restart of {} (UID {}); other Pod requests remain queued",
        target.pod_name, target.pod_uid
    );
    let mut restart_cluster = cluster.clone();
    configure(&mut restart_cluster, target);
    // Persist before any side effect. Repeated annotations on the original Pod
    // coalesce, and an operator restart resumes this same UID and drain phase.
    patch_image_rollout_status(
        kube,
        namespace,
        &restart_cluster,
        replicas,
        "PreparingPodRestart",
        message,
        node_id,
        "PreparingNode",
        Some(membership),
        None,
        None,
        false,
    )
    .await?;
    Ok(true)
}

pub(super) async fn original_still_present(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<bool> {
    let Some(target) = cluster.spec.pod_restart.as_ref() else {
        return Ok(true);
    };
    let pod: Option<Pod> =
        kube.get_json_optional(&format!("/api/v1/namespaces/{namespace}/pods/{}", target.pod_name)).await?;
    Ok(pod.is_some_and(|pod| pod.metadata.uid == target.pod_uid && pod.metadata.deletion_timestamp.is_none()))
}

pub(super) async fn delete_rollout_pod(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
) -> Result<()> {
    let path = format!("/api/v1/namespaces/{namespace}/pods/{}", pod_name(&cluster.metadata.name, node_id));
    if let Some(target) = cluster.spec.pod_restart.as_ref() {
        if node_id != target.node_id {
            bail!("refusing to delete a different Pod during PodRestart");
        }
        // A conflict means StatefulSet already replaced the original. Reconcile
        // the new Pod's recovery instead of deleting it by its reused name.
        kube.delete_with_uid_precondition(&path, &target.pod_uid).await?;
        Ok(())
    } else {
        kube.delete(&path).await
    }
}

pub(super) async fn finish(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    membership: Option<&MembershipStatus>,
    error: Option<&str>,
) -> Result<()> {
    let target = cluster.spec.pod_restart.as_ref().context("missing PodRestart target")?;
    clear_reliable_handoff(namespace, cluster, replicas, target.node_id).await;
    record_completion(kube, namespace, cluster, replicas, membership, error).await
}

async fn record_completion(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    membership: Option<&MembershipStatus>,
    error: Option<&str>,
) -> Result<()> {
    let target = cluster.spec.pod_restart.as_ref().context("missing PodRestart target")?;
    let succeeded = membership.is_some();
    if !succeeded {
        // RolledBack already restored service. Consume this failed request
        // before releasing the lock, so it does not retry forever. metadata.uid
        // is immutable: a concurrent replacement makes this patch fail safely.
        let path = format!("/api/v1/namespaces/{namespace}/pods/{}", target.pod_name);
        let pod: Option<Pod> = kube.get_json_optional(&path).await?;
        if pod.is_some_and(|pod| pod.metadata.uid == target.pod_uid) {
            kube.merge_patch(
                &path,
                &json!({"metadata": {
                    "uid": target.pod_uid,
                    "annotations": {POD_DELETE_REQUESTED_ANNOTATION: null},
                }}),
            )
            .await?;
        }
    }
    let message = if succeeded {
        format!(
            "{} was gracefully restarted and its replacement passed the stability window",
            target.pod_name
        )
    } else {
        format!(
            "graceful restart of {} was rolled back; Pod retained: {}",
            target.pod_name,
            error.unwrap_or("drain failed")
        )
    };
    let mut status = json!({
        "phase": if succeeded { "Running" } else { "PodRestartFailed" },
        "message": message,
        "replicas": replicas,
        "readyReplicas": ready_pods(kube, namespace, &cluster.metadata.name).await?.len(),
        "desiredReplicas": cluster.spec.replicas,
        "updatedReplicas": null,
        "drainingNodeId": null,
        "operation": null,
        "confChange": null,
        "lastPodRestart": {
            "podName": target.pod_name,
            "originalPodUid": target.pod_uid,
            "result": if succeeded { "Succeeded" } else { "Failed" },
            "message": message,
            "completedAt": utc_now_rfc3339(),
        },
        "observedGeneration": cluster.metadata.generation,
    });
    if let Some(membership) = membership {
        status["leaderId"] = json!(membership.leader_id);
        status["members"] = json!(membership.business_peers);
        status["raftStatus"] = membership_status_json(membership, replicas);
    }
    // Unlike an entire image rollout, this says nothing about other Pods using
    // the target image and must not advance currentImage/observedRestartAt.
    patch_status(kube, namespace, &cluster.metadata.name, status).await
}

#[cfg(test)]
#[path = "pod_restart_tests.rs"]
mod tests;
