use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt;
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::time::sleep;

mod external_termination;
mod pod_restart;
use external_termination::handle_external_pod_termination;
use pod_restart::{PodRestartTarget, POD_MANUAL_RESTART_ANNOTATION};

const GROUP: &str = "rmqtt.io";
const VERSION: &str = "v1alpha1";
const PLURAL: &str = "rmqttclusters";
const FIELD_MANAGER: &str = "rmqtt-operator";
const DEFAULT_NAMESPACE: &str = "rmqtt-test";
const DEFAULT_LEADER_ELECTION_LEASE_NAME: &str = "rmqtt-operator-leader";
const DEFAULT_LEASE_DURATION_SECONDS: i64 = 30;
const IMAGE_DRAIN_TIMEOUT_ERROR: &str = "drain timeout expired before all safety barriers converged";
const KUBECTL_RESTARTED_AT_ANNOTATION: &str = "kubectl.kubernetes.io/restartedAt";
const DRAIN_STABLE_OBSERVATIONS: u8 = 2;
const DRAIN_OBSERVATION_INTERVAL_SECONDS: u64 = 2;
const DEFAULT_IMAGE: &str =
    "registry.cn-sh-02.sensecore.cn/ccr-ams-public/mirror/docker/rmqtt-test/rmqtt:latest";
const OPERATOR_BUILD_ID: &str = match option_env!("RMQTT_OPERATOR_BUILD_ID") {
    Some(build_id) => build_id,
    None => "dev",
};

static LAST_LOGGED_STATUS: OnceLock<Mutex<HashMap<String, Value>>> = OnceLock::new();
static LAST_RECOVERY_SCAN_LOG: OnceLock<Mutex<HashMap<String, RecoveryScanLogEntry>>> = OnceLock::new();

const RECOVERY_SCAN_LOG_HEARTBEAT_SECONDS: u64 = 30;
const QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS: u64 = 15;
const QUORUM_RECOVERY_NODE_UNHEALTHY_GRACE_SECONDS: u64 = 30;
const QUORUM_RECOVERY_NODE_MAX_RECYCLE_ATTEMPTS: u32 = 2;
// The Raft library accepts a transfer request before the target has actually
// won an election. Give that election ample time to finish, then retry with
// another eligible voter instead of waiting forever on a request Raft has
// already abandoned.
const LEADER_TRANSFER_RETRY_SECONDS: i64 = 30;

const RENDER_SCRIPT: &str = r###"#!/bin/sh
set -eu

WORK_DIR="${RMQTT_WORK_DIR:-/work}"
SOURCE_DIR="${RMQTT_SOURCE_DIR:-/app/rmqtt}"
BASE_CONFIG_DIR="${RMQTT_BASE_CONFIG_DIR:-}"
POD_NAME="${HOSTNAME}"
ORDINAL="${POD_NAME##*-}"
NODE_ID="$((ORDINAL + 1))"
REPLICAS="${RMQTT_REPLICAS:-3}"
HEADLESS="${RMQTT_HEADLESS_SERVICE}"
NAMESPACE="${POD_NAMESPACE}"
MQTT_PORT="${RMQTT_MQTT_PORT:-1883}"
MQTTS_PORT="${RMQTT_MQTTS_PORT:-8883}"
WS_PORT="${RMQTT_WS_PORT:-8080}"
WSS_PORT="${RMQTT_WSS_PORT:-8083}"
GRPC_PORT="${RMQTT_GRPC_PORT:-5363}"
RAFT_PORT="${RMQTT_RAFT_PORT:-6003}"
HTTP_PORT="${RMQTT_HTTP_API_PORT:-6060}"
RAFT_BOOTSTRAP="${RMQTT_RAFT_BOOTSTRAP:-false}"

case "${RAFT_BOOTSTRAP}" in
  true|false) ;;
  *) echo "invalid RMQTT_RAFT_BOOTSTRAP=${RAFT_BOOTSTRAP}; expected true or false" >&2; exit 1 ;;
esac

mkdir -p "${WORK_DIR}/rmqtt-plugins" "${WORK_DIR}/rmqtt-bin"
cp "${SOURCE_DIR}/rmqtt.toml" "${WORK_DIR}/rmqtt.toml"
cp "${SOURCE_DIR}"/rmqtt-plugins/*.toml "${WORK_DIR}/rmqtt-plugins/"
cp "${SOURCE_DIR}/rmqtt-bin/rmqtt.pem" "${WORK_DIR}/rmqtt-bin/" 2>/dev/null || true
cp "${SOURCE_DIR}/rmqtt-bin/rmqtt.key" "${WORK_DIR}/rmqtt-bin/" 2>/dev/null || true
if [ -n "${BASE_CONFIG_DIR}" ]; then
  if [ -f "${BASE_CONFIG_DIR}/rmqtt.toml" ]; then
    cp "${BASE_CONFIG_DIR}/rmqtt.toml" "${WORK_DIR}/rmqtt.toml"
  fi
  for config_file in "${BASE_CONFIG_DIR}"/rmqtt-*.toml; do
    [ -f "${config_file}" ] || continue
    cp "${config_file}" "${WORK_DIR}/rmqtt-plugins/"
  done
fi

grpc_peers=""
raft_peers=""
i=1
while [ "$i" -le "$REPLICAS" ]; do
  ordinal="$((i - 1))"
  # Both services are in the pod namespace. The short stable hostname lets
  # the resolver append the correct namespace search domain on its first try.
  host="${RMQTT_NAME}-${ordinal}.${HEADLESS}"
  grpc_peer="${i}@${host}:${GRPC_PORT}"
  raft_peer="${i}@${host}:${RAFT_PORT}"
  if [ -z "$grpc_peers" ]; then
    grpc_peers="\"${grpc_peer}\""
    raft_peers="\"${raft_peer}\""
  else
    grpc_peers="${grpc_peers}, \"${grpc_peer}\""
    raft_peers="${raft_peers}, \"${raft_peer}\""
  fi
  i="$((i + 1))"
done

sed -i "s/^node.id = .*/node.id = ${NODE_ID}/" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^rpc.server_addr = .*#rpc.server_addr = \"0.0.0.0:${GRPC_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i 's#^plugins.dir = .*#plugins.dir = "rmqtt-plugins/"#' "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.tcp.external.addr = .*#listener.tcp.external.addr = \"0.0.0.0:${MQTT_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.tls.external.addr = .*#listener.tls.external.addr = \"0.0.0.0:${MQTTS_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.ws.external.addr = .*#listener.ws.external.addr = \"0.0.0.0:${WS_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.wss.external.addr = .*#listener.wss.external.addr = \"0.0.0.0:${WSS_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i 's/#"rmqtt-cluster-raft"/"rmqtt-cluster-raft"/' "${WORK_DIR}/rmqtt.toml"

sed -i "s#^http_laddr = .*#http_laddr = \"0.0.0.0:${HTTP_PORT}\"#" "${WORK_DIR}/rmqtt-plugins/rmqtt-http-api.toml"
sed -i "s#^node_grpc_addrs = .*#node_grpc_addrs = [${grpc_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
sed -i "s#^raft_peer_addrs = .*#raft_peer_addrs = [${raft_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
if [ -f "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-broadcast.toml" ]; then
  sed -i "s#^node_grpc_addrs = .*#node_grpc_addrs = [${grpc_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-broadcast.toml"
fi
if grep -q '^leader_id = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i 's/^leader_id = .*/leader_id = 1/' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\nleader_id = 1\n' >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
if grep -q '^bootstrap = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i "s/^bootstrap = .*/bootstrap = ${RAFT_BOOTSTRAP}/" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\nbootstrap = %s\n' "${RAFT_BOOTSTRAP}" >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
if grep -q '^join_as_learner = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i 's/^join_as_learner = .*/join_as_learner = true/' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\njoin_as_learner = true\n' >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
# A quorum failure must not destroy the only processes that still own the
# in-memory Raft/session/router state. Kubernetes readiness and the operator
# control admission; the broker itself must remain alive for observation and
# optional survivor-based recovery.
if grep -q '^health\.exit_on_node_unavailable = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i 's/^health\.exit_on_node_unavailable = .*/health.exit_on_node_unavailable = false/' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\nhealth.exit_on_node_unavailable = false\n' >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
if grep -q '^#laddr = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i "s|^#laddr = .*|laddr = \"0.0.0.0:${RAFT_PORT}\"|" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  sed -i "s|^laddr = .*|laddr = \"0.0.0.0:${RAFT_PORT}\"|" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi

echo "rendered rmqtt config: node_id=${NODE_ID}, replicas=${REPLICAS}, pod=${POD_NAME}, raft_bootstrap=${RAFT_BOOTSTRAP}"
echo "raft peers: [${raft_peers}]"
echo "grpc peers: [${grpc_peers}]"
"###;

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct ObjectList<T> {
    #[serde(default)]
    items: Vec<T>,
}

#[derive(Debug, Deserialize, Clone)]
struct Metadata {
    name: String,
    #[serde(default)]
    uid: String,
    #[serde(rename = "resourceVersion", default)]
    resource_version: String,
    #[serde(rename = "deletionTimestamp", default)]
    deletion_timestamp: Option<String>,
    #[serde(default)]
    finalizers: Vec<String>,
    #[serde(default)]
    generation: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
struct RmqttCluster {
    metadata: Metadata,
    #[serde(default)]
    spec: ClusterSpec,
    #[serde(default)]
    status: Option<ClusterStatus>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ClusterSpec {
    #[serde(default = "default_replicas")]
    replicas: i32,
    #[serde(default = "default_image")]
    image: String,
    #[serde(default = "default_image_pull_policy")]
    image_pull_policy: String,
    #[serde(default)]
    image_update: ImageUpdateSpec,
    #[serde(default)]
    service: ServiceSpec,
    #[serde(default)]
    raft: RaftSpec,
    #[serde(default)]
    config: ConfigSpec,
    #[serde(default)]
    resources: Option<ResourceRequirementsSpec>,
    #[serde(default)]
    pod: BrokerPodSpec,
    #[serde(default)]
    monitoring: MonitoringSpec,
    #[serde(default)]
    graceful_shutdown: GracefulShutdownSpec,
    // Reconcile-only rollout target copied from the StatefulSet pod template.
    // This is never read from or written to RmqttCluster.spec.
    #[serde(skip)]
    rollout_restart_at: Option<String>,
    #[serde(skip)]
    pod_restart: Option<PodRestartTarget>,
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            image: default_image(),
            image_pull_policy: default_image_pull_policy(),
            image_update: ImageUpdateSpec::default(),
            service: ServiceSpec::default(),
            raft: RaftSpec::default(),
            config: ConfigSpec::default(),
            resources: None,
            pod: BrokerPodSpec::default(),
            monitoring: MonitoringSpec::default(),
            graceful_shutdown: GracefulShutdownSpec::default(),
            rollout_restart_at: None,
            pod_restart: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct GracefulShutdownSpec {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_graceful_shutdown_timeout_seconds")]
    timeout_seconds: u64,
    #[serde(default = "default_graceful_shutdown_finalize_reserve_seconds")]
    finalize_reserve_seconds: u64,
}

impl Default for GracefulShutdownSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout_seconds: default_graceful_shutdown_timeout_seconds(),
            finalize_reserve_seconds: default_graceful_shutdown_finalize_reserve_seconds(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ImageUpdateSpec {
    #[serde(default)]
    auto_rollout: bool,
    #[serde(default = "default_image_stabilization_seconds")]
    stabilization_seconds: u64,
}

impl Default for ImageUpdateSpec {
    fn default() -> Self {
        Self { auto_rollout: false, stabilization_seconds: default_image_stabilization_seconds() }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ServiceSpec {
    #[serde(default = "default_mqtt_port")]
    mqtt_port: i32,
    #[serde(default = "default_mqtts_port")]
    mqtts_port: i32,
    #[serde(default = "default_ws_port")]
    ws_port: i32,
    #[serde(default = "default_wss_port")]
    wss_port: i32,
    #[serde(default = "default_http_api_port")]
    http_api_port: i32,
    #[serde(default = "default_grpc_port")]
    grpc_port: i32,
    #[serde(default = "default_raft_port")]
    raft_port: i32,
}

impl Default for ServiceSpec {
    fn default() -> Self {
        Self {
            mqtt_port: default_mqtt_port(),
            mqtts_port: default_mqtts_port(),
            ws_port: default_ws_port(),
            wss_port: default_wss_port(),
            http_api_port: default_http_api_port(),
            grpc_port: default_grpc_port(),
            raft_port: default_raft_port(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ConfigSpec {
    #[serde(default)]
    base_config_map_name: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct ResourceRequirementsSpec {
    #[serde(default)]
    requests: ResourceListSpec,
    #[serde(default)]
    limits: ResourceListSpec,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct ResourceListSpec {
    #[serde(default)]
    cpu: Option<String>,
    #[serde(default)]
    memory: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct BrokerPodSpec {
    #[serde(default)]
    annotations: BTreeMap<String, String>,
    #[serde(default)]
    service_account_name: Option<String>,
    #[serde(default)]
    image_pull_secrets: Vec<NamedReference>,
    #[serde(default)]
    scheduler_name: Option<String>,
    #[serde(default)]
    affinity: Option<Value>,
    #[serde(default)]
    tolerations: Vec<Value>,
    #[serde(default)]
    enable_service_links: Option<bool>,
    #[serde(default)]
    termination_grace_period_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct NamedReference {
    name: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct MonitoringSpec {
    #[serde(default)]
    service_monitor: ServiceMonitorSpec,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ServiceMonitorSpec {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default = "default_metrics_path")]
    path: String,
    #[serde(default = "default_metrics_interval")]
    interval: String,
    #[serde(default = "default_metrics_scrape_timeout")]
    scrape_timeout: String,
}

impl Default for ServiceMonitorSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            labels: BTreeMap::new(),
            path: default_metrics_path(),
            interval: default_metrics_interval(),
            scrape_timeout: default_metrics_scrape_timeout(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct RaftSpec {
    #[serde(default = "default_min_replicas")]
    min_replicas: i32,
    #[serde(default = "default_drain_timeout_seconds")]
    drain_timeout_seconds: u64,
    #[serde(default = "default_endpoint_removal_stable_seconds")]
    endpoint_removal_stable_seconds: u64,
    #[serde(default = "default_scale_in_stabilization_seconds")]
    scale_in_stabilization_seconds: u64,
    #[serde(default = "default_handoff_rollback_threshold_percent")]
    handoff_rollback_threshold_percent: u8,
    #[serde(default)]
    drain_timeout_action: DrainTimeoutAction,
    #[serde(default)]
    allow_leader_scale_in: bool,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
enum DrainTimeoutAction {
    #[default]
    Rollback,
    ForceContinue,
}

impl Default for RaftSpec {
    fn default() -> Self {
        Self {
            min_replicas: default_min_replicas(),
            drain_timeout_seconds: default_drain_timeout_seconds(),
            endpoint_removal_stable_seconds: default_endpoint_removal_stable_seconds(),
            scale_in_stabilization_seconds: default_scale_in_stabilization_seconds(),
            handoff_rollback_threshold_percent: default_handoff_rollback_threshold_percent(),
            drain_timeout_action: DrainTimeoutAction::Rollback,
            allow_leader_scale_in: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PodList {
    #[serde(default)]
    items: Vec<Pod>,
}

#[derive(Debug, Deserialize)]
struct Pod {
    #[serde(default)]
    metadata: PodMetadata,
    #[serde(default)]
    spec: PodSpec,
    #[serde(default)]
    status: PodStatus,
}

#[derive(Debug, Deserialize, Default)]
struct PodMetadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    uid: String,
    #[serde(rename = "resourceVersion", default)]
    resource_version: String,
    #[serde(default)]
    finalizers: Vec<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
    #[serde(rename = "ownerReferences", default)]
    owner_references: Vec<PodOwnerReference>,
    #[serde(rename = "deletionTimestamp", default)]
    deletion_timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PodOwnerReference {
    kind: String,
    name: String,
    uid: String,
    #[serde(default)]
    controller: bool,
}

#[derive(Debug, Deserialize, Default)]
struct PodStatus {
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    conditions: Vec<PodCondition>,
}

#[derive(Debug, Deserialize, Default)]
struct PodSpec {
    #[serde(default)]
    containers: Vec<PodContainerSpec>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct PodContainerSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    image_pull_policy: String,
}

#[derive(Debug, Deserialize)]
struct PodCondition {
    #[serde(rename = "type")]
    kind: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct StatefulSet {
    #[serde(default)]
    metadata: StatefulSetMetadata,
    #[serde(default)]
    spec: StatefulSetSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetMetadata {
    #[serde(default)]
    uid: String,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

impl StatefulSet {
    fn replicas(&self) -> i32 {
        self.spec.replicas.unwrap_or_default()
    }

    fn rmqtt_container(&self) -> Option<&StatefulSetContainerSpec> {
        self.spec.template.spec.containers.iter().find(|container| container.name == "rmqtt")
    }

    fn broker_image_matches(&self, spec: &ClusterSpec) -> bool {
        self.rmqtt_container().is_some_and(|container| {
            container.image == spec.image && container.image_pull_policy == spec.image_pull_policy
        })
    }

    fn broker_template_matches(&self, spec: &ClusterSpec) -> bool {
        let pod = &self.spec.template.spec;
        let annotations = self
            .spec
            .template
            .metadata
            .annotations
            .iter()
            .filter(|(key, _)| key.as_str() != KUBECTL_RESTARTED_AT_ANNOTATION)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        let shutdown_env_matches = self.rmqtt_container().is_some_and(|container| {
            let value = |name: &str| {
                container.env.iter().find(|entry| entry.name == name).map(|entry| entry.value.as_str())
            };
            if spec.graceful_shutdown.enabled {
                value("RMQTT_GRACEFUL_SHUTDOWN_ENABLED") == Some("true")
                    && value("RMQTT_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS")
                        == Some(spec.graceful_shutdown.timeout_seconds.to_string().as_str())
                    && value("RMQTT_GRACEFUL_SHUTDOWN_FINALIZE_RESERVE_SECONDS")
                        == Some(spec.graceful_shutdown.finalize_reserve_seconds.to_string().as_str())
            } else {
                // A legacy template without the variable is already disabled;
                // a previously-enabled template must be explicitly rewritten.
                value("RMQTT_GRACEFUL_SHUTDOWN_ENABLED") != Some("true")
            }
        });
        let fence_matches = self
            .spec
            .template
            .metadata
            .finalizers
            .iter()
            .any(|value| value == external_termination::FINALIZER)
            == spec.graceful_shutdown.enabled;

        self.broker_image_matches(spec)
            && shutdown_env_matches
            && annotations == spec.pod.annotations
            && fence_matches
            && pod.service_account_name.as_deref().unwrap_or("default")
                == spec.pod.service_account_name.as_deref().unwrap_or("default")
            && pod.image_pull_secrets == spec.pod.image_pull_secrets
            && pod.scheduler_name.as_deref().unwrap_or("default-scheduler")
                == spec.pod.scheduler_name.as_deref().unwrap_or("default-scheduler")
            && pod.affinity == spec.pod.affinity
            && pod.tolerations == spec.pod.tolerations
            && pod.enable_service_links.unwrap_or(true) == spec.pod.enable_service_links.unwrap_or(false)
            && pod.termination_grace_period_seconds.unwrap_or(30)
                == effective_termination_grace_period_seconds(spec)
    }

    fn bootstrap_allowed(&self) -> bool {
        self.metadata.annotations.get("rmqtt.io/raft-bootstrap-allowed").is_some_and(|value| value == "true")
    }

    fn rollout_restart_at(&self) -> Option<&str> {
        self.spec.template.metadata.annotations.get(KUBECTL_RESTARTED_AT_ANNOTATION).map(String::as_str)
    }
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetSpec {
    #[serde(default)]
    replicas: Option<i32>,
    #[serde(default)]
    template: StatefulSetPodTemplateSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetPodTemplateSpec {
    #[serde(default)]
    metadata: StatefulSetPodTemplateMetadata,
    #[serde(default)]
    spec: StatefulSetPodSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetPodTemplateMetadata {
    #[serde(default)]
    annotations: BTreeMap<String, String>,
    #[serde(default)]
    finalizers: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StatefulSetPodSpec {
    #[serde(default)]
    containers: Vec<StatefulSetContainerSpec>,
    #[serde(default)]
    service_account_name: Option<String>,
    #[serde(default)]
    image_pull_secrets: Vec<NamedReference>,
    #[serde(default)]
    scheduler_name: Option<String>,
    #[serde(default)]
    affinity: Option<Value>,
    #[serde(default)]
    tolerations: Vec<Value>,
    #[serde(default)]
    enable_service_links: Option<bool>,
    #[serde(default)]
    termination_grace_period_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StatefulSetContainerSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    image_pull_policy: String,
    #[serde(default)]
    env: Vec<StatefulSetEnvVar>,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetEnvVar {
    #[serde(default)]
    name: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize, Default)]
struct DrainStatus {
    #[serde(default)]
    safe_to_leave: bool,
    #[serde(default)]
    strict_safe_to_leave: Option<bool>,
    #[serde(default)]
    session_handoff_complete: Option<bool>,
    #[serde(default)]
    blocking_reasons: Vec<String>,
    #[serde(default)]
    blocking_counts: BTreeMap<String, Option<i64>>,
    #[serde(default)]
    data_loss_detected: bool,
    #[serde(default)]
    counts: DrainCounts,
}

#[derive(Debug, Deserialize, Default)]
struct DrainCounts {
    #[serde(default)]
    message_work_items_remaining: u64,
    #[serde(default)]
    session_handoffs_completed: usize,
    #[serde(default)]
    connections: i64,
    #[serde(default)]
    sessions: i64,
    #[serde(default)]
    handoff_pending_messages: usize,
    #[serde(default)]
    handoff_pending_bytes: usize,
}

impl DrainStatus {
    fn scale_in_safe_to_leave(&self) -> bool {
        !self.data_loss_detected
            && self.strict_safe_to_leave.unwrap_or(self.safe_to_leave)
            && self.session_handoff_complete.unwrap_or(true)
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct TerminationStatus {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    terminating: bool,
    #[serde(default)]
    pod_uid: String,
    #[serde(default)]
    boot_id: String,
    #[serde(default)]
    remaining_millis: Option<u64>,
    #[serde(default)]
    completed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RmqttActionErrorKind {
    Timeout,
    Connection,
    TransientHttpStatus,
    HttpStatus,
    InvalidResponse,
}

impl RmqttActionErrorKind {
    fn label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Connection => "connection",
            Self::TransientHttpStatus => "transient_http_status",
            Self::HttpStatus => "http_status",
            Self::InvalidResponse => "invalid_response",
        }
    }
}

#[derive(Debug)]
struct RmqttActionError {
    kind: RmqttActionErrorKind,
    node_id: i32,
    payload: Value,
    detail: String,
}

impl RmqttActionError {
    fn new(kind: RmqttActionErrorKind, node_id: i32, payload: &Value, detail: impl Into<String>) -> Self {
        Self { kind, node_id, payload: payload.clone(), detail: detail.into() }
    }

    fn is_transient(&self) -> bool {
        matches!(
            self.kind,
            RmqttActionErrorKind::Timeout
                | RmqttActionErrorKind::Connection
                | RmqttActionErrorKind::TransientHttpStatus
        )
    }
}

impl fmt::Display for RmqttActionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rmqtt action {:?} payload {} on node {} failed: {}",
            self.kind, self.payload, self.node_id, self.detail
        )
    }
}

impl std::error::Error for RmqttActionError {}

fn error_chain_text(error: &(dyn std::error::Error + 'static)) -> String {
    let mut messages = Vec::new();
    let mut current = Some(error);
    while let Some(source) = current {
        let message = source.to_string();
        if messages.last() != Some(&message) {
            messages.push(message);
        }
        current = source.source();
    }
    messages.join(": ")
}

fn classify_reqwest_error(error: &reqwest::Error) -> RmqttActionErrorKind {
    if error.is_timeout() {
        RmqttActionErrorKind::Timeout
    } else {
        RmqttActionErrorKind::Connection
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeDrainStage {
    PrepareHandoff,
    StartQuiesce,
    RemoveClientEndpoint,
    ActivateHandoff,
    StartDrain,
    ObserveDrain,
    ObserveRouting,
}

impl fmt::Display for NodeDrainStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrepareHandoff => f.write_str("prepare reliable handoff"),
            Self::StartQuiesce => f.write_str("start broker quiesce"),
            Self::RemoveClientEndpoint => f.write_str("remove client endpoint"),
            Self::ActivateHandoff => f.write_str("activate reliable handoff"),
            Self::StartDrain => f.write_str("start broker drain"),
            Self::ObserveDrain => f.write_str("observe broker drain"),
            Self::ObserveRouting => f.write_str("observe routing barrier"),
        }
    }
}

#[derive(Debug)]
struct NodeDrainError {
    stage: NodeDrainStage,
    detail: String,
}

impl NodeDrainError {
    fn new(stage: NodeDrainStage, detail: impl Into<String>) -> Self {
        Self { stage, detail: detail.into() }
    }

    fn from_action(stage: NodeDrainStage, error: RmqttActionError) -> Self {
        Self { stage, detail: error.to_string() }
    }
}

impl fmt::Display for NodeDrainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed: {}", self.stage, self.detail)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum NodeDrainOutcome {
    Ready,
    Unsafe(String),
    TimedOut(String),
    Preempted(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DrainOperation {
    ScaleIn { survivors: Vec<i32>, victims: Vec<i32> },
    Rollout,
}

impl DrainOperation {
    fn scale_in(current_replicas: i32, requested_replicas: i32) -> Self {
        Self::ScaleIn {
            survivors: (1..=requested_replicas).collect(),
            victims: ((requested_replicas + 1)..=current_replicas).rev().collect(),
        }
    }

    fn allowed_handoff_node_ids(&self, current_replicas: i32, target_id: i32) -> Vec<i32> {
        (1..=current_replicas).filter(|node_id| *node_id != target_id).collect()
    }

    fn handoff_destination_constraint(&self, current_replicas: i32, target_id: i32) -> Option<Vec<i32>> {
        matches!(self, Self::ScaleIn { .. })
            .then(|| self.allowed_handoff_node_ids(current_replicas, target_id))
    }

    fn plan_node_ids(&self) -> (Vec<i32>, Vec<i32>) {
        match self {
            Self::ScaleIn { survivors, victims } => (survivors.clone(), victims.clone()),
            Self::Rollout => (Vec::new(), Vec::new()),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ForceDrainFinalizeResponse {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    target_id: i32,
    #[serde(default)]
    local_target: bool,
    #[serde(default)]
    force_drain_finalized: bool,
    #[serde(default)]
    handoff_messages_discarded: usize,
    #[serde(default)]
    handoff_bytes_discarded: usize,
    #[serde(default)]
    handoff_clients_affected: usize,
    #[serde(default)]
    local_sessions_requested: usize,
    #[serde(default)]
    local_session_acks: usize,
    #[serde(default)]
    session_handoffs_discarded: usize,
    #[serde(default)]
    router_states_removed: usize,
    #[serde(default)]
    routes_removed: usize,
}

#[derive(Debug, Default)]
struct ForceDrainFinalizeSummary {
    target_id: i32,
    reason: String,
    blockers: String,
    nodes_acknowledged: usize,
    handoff_messages_discarded: usize,
    handoff_bytes_discarded: usize,
    handoff_clients_affected: usize,
    local_sessions_requested: usize,
    local_session_acks: usize,
    session_handoffs_discarded: usize,
    router_states_removed: usize,
    routes_removed: usize,
}

impl ForceDrainFinalizeSummary {
    fn to_status_json(&self) -> Value {
        json!({
            "targetId": self.target_id,
            "reason": self.reason,
            "blockers": self.blockers,
            "nodesAcknowledged": self.nodes_acknowledged,
            "handoffMessagesDiscarded": self.handoff_messages_discarded,
            "handoffBytesDiscarded": self.handoff_bytes_discarded,
            "handoffClientsAffected": self.handoff_clients_affected,
            "localSessionsRequested": self.local_sessions_requested,
            "localSessionAcks": self.local_session_acks,
            "sessionHandoffsDiscarded": self.session_handoffs_discarded,
            "routerStatesRemoved": self.router_states_removed,
            "routesRemoved": self.routes_removed,
            "forcedAt": utc_now_rfc3339(),
        })
    }

    fn description(&self) -> String {
        format!(
            "force-finalized drain after timeout: discarded handoffMessages={} handoffBytes={} sessionHandoffs={} routerStates={} routes={}, terminatedSessions={}/{}; blockers={}",
            self.handoff_messages_discarded,
            self.handoff_bytes_discarded,
            self.session_handoffs_discarded,
            self.router_states_removed,
            self.routes_removed,
            self.local_session_acks,
            self.local_sessions_requested,
            self.blockers
        )
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RoutingStatus {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    target_id: i32,
    #[serde(default)]
    client_states: usize,
    #[serde(default)]
    routes: usize,
    #[serde(default)]
    active_forwards: usize,
    #[serde(default)]
    reliable_forwards: usize,
    #[serde(default)]
    plain_forwards: usize,
    #[serde(default)]
    handoff_pending: usize,
    #[serde(default)]
    handoff_completed: usize,
    #[serde(default)]
    handoff_pending_bytes: usize,
    #[serde(default)]
    handoff_oldest_age_millis: u64,
    #[serde(default)]
    handoff_progress_sequence: u64,
    #[serde(default)]
    handoff_capacity_percent: u8,
    #[serde(default)]
    handoff_capacity_waiters: usize,
    #[serde(default)]
    handoff_failed: usize,
    #[serde(default)]
    handoff_rejected: usize,
    #[serde(default)]
    handoff_invariant_mismatch_observations: usize,
    #[serde(default)]
    data_loss_detected: bool,
    #[serde(default)]
    converged: bool,
}

#[derive(Debug, Deserialize, Default)]
struct EndpointSlice {
    #[serde(default)]
    endpoints: Vec<Endpoint>,
}

#[derive(Debug, Deserialize, Default)]
struct Endpoint {
    #[serde(rename = "targetRef", default)]
    target_ref: Option<EndpointTargetRef>,
}

#[derive(Debug, Deserialize, Default)]
struct EndpointTargetRef {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct MembershipStatus {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    leader_id: Option<i32>,
    #[serde(default)]
    is_leader: bool,
    #[serde(default)]
    available: bool,
    #[serde(default)]
    started: bool,
    #[serde(default)]
    caught_up: bool,
    #[serde(default)]
    snapshot_restored: bool,
    #[serde(default)]
    draining: bool,
    #[serde(default)]
    applied_index: u64,
    #[serde(default)]
    committed_index: u64,
    #[serde(default)]
    last_index: u64,
    #[serde(default)]
    client_states: usize,
    #[serde(default)]
    voters: Vec<i32>,
    #[serde(default)]
    learners: Vec<i32>,
    #[serde(default)]
    raft_peers: Vec<RaftPeerStatus>,
    #[serde(default)]
    business_peers: Vec<Value>,
    #[serde(default)]
    supports: Value,
    #[serde(default)]
    quorum_recovery_active: bool,
    #[serde(default)]
    quorum_recovery_epoch: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct RaftPeerStatus {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    available: bool,
    #[serde(default)]
    matched_index: u64,
    #[serde(default)]
    recent_active: bool,
    #[serde(default)]
    pending_snapshot: u64,
    #[serde(default)]
    progress_state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VictimPodState {
    Present,
    Terminating,
    Absent,
}

#[derive(Debug, PartialEq, Eq)]
enum ForcedPodRestartLeaveDecision {
    DeletePod,
    Wait(String),
    RequestLeave,
}

impl fmt::Display for VictimPodState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Present => f.write_str("Present"),
            Self::Terminating => f.write_str("Terminating"),
            Self::Absent => f.write_str("Absent"),
        }
    }
}

#[derive(Debug)]
struct ScaleInObservation {
    pod_state: VictimPodState,
    stateful_set_replicas: i32,
    raft_member: bool,
    business_member: bool,
    membership: MembershipStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeJoinStep {
    CatchingUp,
    Promote,
    WaitingReady,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeJoinCompletionScope {
    Cluster,
    TargetPod,
}

#[derive(Debug)]
struct NodeJoinObservation {
    membership: MembershipStatus,
    ready_replicas: i32,
    target_pod_ready: bool,
    is_learner: bool,
    peer_available: bool,
    peer_caught_up: bool,
    business_member: bool,
    step: NodeJoinStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScaleInRecoveryDecision {
    RetryTargetRequest,
    RemoveStaleRaftMember,
    RemoveStaleBusinessMember,
    FinishScaleIn,
}

fn scale_in_recovery_decision(
    pod_state: VictimPodState,
    raft_member: bool,
    business_member: bool,
) -> ScaleInRecoveryDecision {
    if !raft_member && !business_member {
        return ScaleInRecoveryDecision::FinishScaleIn;
    }
    if pod_state == VictimPodState::Present {
        return ScaleInRecoveryDecision::RetryTargetRequest;
    }
    if raft_member {
        ScaleInRecoveryDecision::RemoveStaleRaftMember
    } else {
        ScaleInRecoveryDecision::RemoveStaleBusinessMember
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    #[serde(default)]
    phase: String,
    #[serde(default)]
    conf_change: Option<ConfChangeStatus>,
    #[serde(default)]
    operation: Option<OperationStatus>,
    #[serde(default)]
    observed_restart_at: Option<String>,
    #[serde(default)]
    quorum_recovery: Option<QuorumRecoveryStatus>,
    #[serde(default)]
    external_termination_batch: Option<ExternalTerminationBatchStatus>,
    #[serde(default)]
    external_termination_cleanup: Option<ExternalTerminationCleanupStatus>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ExternalTerminationCleanupStatus {
    #[serde(default)]
    targets: Vec<ExternalTerminationTargetStatus>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ExternalTerminationBatchStatus {
    #[serde(default)]
    targets: Vec<ExternalTerminationTargetStatus>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ExternalTerminationTargetStatus {
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    pod_uid: String,
    #[serde(default)]
    boot_id: String,
    #[serde(default)]
    deadline_at: String,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    stable_observations: u8,
    #[serde(default)]
    endpoint_absent_since: Option<String>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    last_error: Option<String>,
    /// Persisted before any membership mutation, retained across retries.
    #[serde(default)]
    membership_owner: bool,
    /// Successfully acknowledged topology (including participant incarnations).
    #[serde(default)]
    prepared_topology: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct QuorumRecoveryStatus {
    #[serde(default)]
    epoch: u64,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    seed_node_id: Option<i32>,
    #[serde(default)]
    old_voters: Vec<i32>,
    #[serde(default)]
    survivor_node_ids: Vec<i32>,
    #[serde(default)]
    rejected_node_ids: Vec<i32>,
    #[serde(default)]
    unreachable_node_ids: Vec<i32>,
    #[serde(default)]
    restarting_node_id: Option<i32>,
    #[serde(default)]
    restarted_node_ids: Vec<i32>,
    #[serde(default)]
    stabilizing_node_id: Option<i32>,
    #[serde(default)]
    stabilizing_started_at: Option<String>,
    #[serde(default)]
    unhealthy_node_id: Option<i32>,
    #[serde(default)]
    unhealthy_started_at: Option<String>,
    #[serde(default)]
    recycling_node_id: Option<i32>,
    #[serde(default)]
    recycling_pod_uid: Option<String>,
    #[serde(default)]
    recycle_attempts: BTreeMap<String, u32>,
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ConfChangeStatus {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: String,
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    from_replicas: i32,
    #[serde(default)]
    to_replicas: i32,
    #[serde(default)]
    requested_replicas: i32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct OperationStatus {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: String,
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    from_replicas: i32,
    #[serde(default)]
    to_replicas: i32,
    #[serde(default)]
    requested_replicas: i32,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    target_image: Option<String>,
    #[serde(default)]
    target_image_pull_policy: Option<String>,
    #[serde(default)]
    target_restart_at: Option<String>,
    #[serde(default)]
    target_pod_name: Option<String>,
    #[serde(default)]
    target_pod_uid: Option<String>,
    #[serde(default)]
    manual_restart: Option<String>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    leader_transfer_attempted_at: Option<String>,
    #[serde(default)]
    leader_transfer_retry_count: u32,
    #[serde(default)]
    stable_since: Option<String>,
    #[serde(default)]
    survivors: Vec<i32>,
    #[serde(default)]
    victims: Vec<i32>,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    last_error: Option<String>,
}

impl OperationStatus {
    fn to_conf_change(&self) -> ConfChangeStatus {
        ConfChangeStatus {
            kind: self.kind.clone(),
            node_id: self.node_id,
            from_replicas: self.from_replicas,
            to_replicas: self.to_replicas,
            requested_replicas: self.requested_replicas,
        }
    }

    fn scale_in_drain_operation(&self) -> Option<DrainOperation> {
        (self.kind == "RemoveVoter" && !self.survivors.is_empty() && !self.victims.is_empty()).then(|| {
            DrainOperation::ScaleIn { survivors: self.survivors.clone(), victims: self.victims.clone() }
        })
    }
}

fn is_rollout_operation_kind(kind: &str) -> bool {
    matches!(kind, "RollingImageUpdate" | "RollingRestart" | "PodRestart")
}

fn rollout_is_restart(spec: &ClusterSpec) -> bool {
    spec.rollout_restart_at.is_some() || spec.pod_restart.is_some()
}

fn operator_managed_rollout_enabled(spec: &ClusterSpec) -> bool {
    spec.image_update.auto_rollout || rollout_is_restart(spec)
}

fn rollout_phase<'a>(spec: &ClusterSpec, image_phase: &'a str, restart_phase: &'a str) -> &'a str {
    if rollout_is_restart(spec) {
        restart_phase
    } else {
        image_phase
    }
}

fn rollout_action(spec: &ClusterSpec) -> &'static str {
    if spec.pod_restart.is_some() {
        "single-pod restart"
    } else if rollout_is_restart(spec) {
        "rolling restart"
    } else {
        "image rollout"
    }
}

fn default_replicas() -> i32 {
    3
}

fn default_min_replicas() -> i32 {
    3
}

// Quorum recovery is an availability guarantee of the operator rather than a
// per-cluster opt-in. Keep the observation window out of the RMQTT publish path:
// it is consulted only after no available authoritative leader can be found.
const QUORUM_RECOVERY_DETECTION_SECONDS: u64 = 30;
const QUORUM_NATURAL_RECOVERY_SECONDS: u64 = 60;

fn quorum_recovery_wait_seconds() -> u64 {
    QUORUM_RECOVERY_DETECTION_SECONDS.saturating_add(QUORUM_NATURAL_RECOVERY_SECONDS)
}

fn default_image() -> String {
    DEFAULT_IMAGE.to_string()
}

fn default_image_pull_policy() -> String {
    "IfNotPresent".to_string()
}

fn default_image_stabilization_seconds() -> u64 {
    60
}

fn default_mqtt_port() -> i32 {
    1883
}

fn default_mqtts_port() -> i32 {
    8883
}

fn default_ws_port() -> i32 {
    8080
}

fn default_wss_port() -> i32 {
    8083
}

fn default_http_api_port() -> i32 {
    6060
}

fn default_grpc_port() -> i32 {
    5363
}

fn default_raft_port() -> i32 {
    6003
}

fn default_drain_timeout_seconds() -> u64 {
    1200
}

fn default_graceful_shutdown_timeout_seconds() -> u64 {
    100
}

fn default_graceful_shutdown_finalize_reserve_seconds() -> u64 {
    10
}

fn effective_termination_grace_period_seconds(spec: &ClusterSpec) -> i64 {
    spec.pod.termination_grace_period_seconds.unwrap_or_else(|| {
        if spec.graceful_shutdown.enabled {
            spec.graceful_shutdown.timeout_seconds.saturating_add(20) as i64
        } else {
            30
        }
    })
}

fn default_endpoint_removal_stable_seconds() -> u64 {
    5
}

fn default_scale_in_stabilization_seconds() -> u64 {
    60
}

fn default_handoff_rollback_threshold_percent() -> u8 {
    80
}

fn default_metrics_path() -> String {
    "/api/v1/metrics/prometheus/sum".to_string()
}

fn default_metrics_interval() -> String {
    "60s".to_string()
}

fn default_metrics_scrape_timeout() -> String {
    "10s".to_string()
}

fn lease_can_be_acquired(
    lease: &Value,
    candidate: &str,
    now: DateTime<Utc>,
    default_duration_seconds: i64,
) -> bool {
    let holder = lease.pointer("/spec/holderIdentity").and_then(Value::as_str);
    if holder.is_none() || holder == Some(candidate) {
        return true;
    }
    let duration_seconds = lease
        .pointer("/spec/leaseDurationSeconds")
        .and_then(Value::as_i64)
        .unwrap_or(default_duration_seconds);
    let Some(renew_time) = lease.pointer("/spec/renewTime").and_then(Value::as_str) else {
        return false;
    };
    let Ok(renew_time) = DateTime::parse_from_rfc3339(renew_time) else {
        return false;
    };
    now.signed_duration_since(renew_time.with_timezone(&Utc)).num_seconds() > duration_seconds
}

async fn try_acquire_operator_lease(
    kube: &KubeClient,
    namespace: &str,
    lease_name: &str,
    holder_identity: &str,
    lease_duration_seconds: i64,
) -> Result<bool> {
    let collection_path = format!("/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases");
    let resource_path = format!("{collection_path}/{lease_name}");
    let now = Utc::now();
    let now_text = kubernetes_microtime(now);
    let Some(existing) = kube.get_json_optional::<Value>(&resource_path).await? else {
        let lease = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": lease_name,
                "namespace": namespace
            },
            "spec": {
                "holderIdentity": holder_identity,
                "leaseDurationSeconds": lease_duration_seconds,
                "acquireTime": now_text,
                "renewTime": now_text,
                "leaseTransitions": 0
            }
        });
        return kube.create_json(&collection_path, &lease).await;
    };

    if !lease_can_be_acquired(&existing, holder_identity, now, lease_duration_seconds) {
        return Ok(false);
    }
    let resource_version = existing
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("leader Lease {namespace}/{lease_name} has no metadata.resourceVersion"))?;
    let previous_holder = existing.pointer("/spec/holderIdentity").and_then(Value::as_str);
    let changed_holder = previous_holder != Some(holder_identity);
    let previous_transitions =
        existing.pointer("/spec/leaseTransitions").and_then(Value::as_i64).unwrap_or(0);
    let acquire_time = if changed_holder {
        json!(now_text)
    } else {
        existing.pointer("/spec/acquireTime").cloned().unwrap_or_else(|| json!(now_text))
    };
    let lease = json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": lease_name,
            "namespace": namespace,
            "resourceVersion": resource_version
        },
        "spec": {
            "holderIdentity": holder_identity,
            "leaseDurationSeconds": lease_duration_seconds,
            "acquireTime": acquire_time,
            "renewTime": now_text,
            "leaseTransitions": previous_transitions + i64::from(changed_holder)
        }
    });
    kube.replace_json(&resource_path, &lease).await
}

async fn reconcile_while_renewing_lease(
    kube: &KubeClient,
    namespace: &str,
    lease_name: &str,
    holder_identity: &str,
    lease_duration_seconds: i64,
) -> Result<bool> {
    let renew_interval_seconds = (lease_duration_seconds / 3).max(1) as u64;
    let reconcile = reconcile_all(kube, namespace);
    tokio::pin!(reconcile);
    loop {
        tokio::select! {
            result = &mut reconcile => {
                result?;
                return Ok(true);
            }
            _ = sleep(Duration::from_secs(renew_interval_seconds)) => {
                if !try_acquire_operator_lease(
                    kube,
                    namespace,
                    lease_name,
                    holder_identity,
                    lease_duration_seconds,
                )
                .await?
                {
                    return Ok(false);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    if env::args_os().skip(1).any(|arg| arg == "--version" || arg == "-V") {
        println!("rmqtt-operator/{}-{OPERATOR_BUILD_ID}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(buf, "[{} {}] {}", buf.timestamp_millis(), record.level(), record.args())
        })
        .init();

    let namespace = env::var("NAMESPACE").unwrap_or_else(|_| {
        std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| DEFAULT_NAMESPACE.to_string())
    });
    let interval =
        env::var("RECONCILE_INTERVAL_SECONDS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(5);
    let lease_name = env::var("LEADER_ELECTION_LEASE_NAME")
        .unwrap_or_else(|_| DEFAULT_LEADER_ELECTION_LEASE_NAME.to_string());
    let lease_duration_seconds = env::var("LEADER_ELECTION_LEASE_DURATION_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 10)
        .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);
    let holder_identity = env::var("POD_NAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("rmqtt-operator-{}", std::process::id()));

    let kube = KubeClient::in_cluster().await?;
    log::info!(
        "operator started namespace={} reconcile_interval_seconds={} leader_election_lease={} lease_duration_seconds={} holder_identity={}",
        namespace,
        interval,
        lease_name,
        lease_duration_seconds,
        holder_identity
    );

    let mut held_lease = false;
    loop {
        match try_acquire_operator_lease(
            &kube,
            &namespace,
            &lease_name,
            &holder_identity,
            lease_duration_seconds,
        )
        .await
        {
            Ok(true) => {
                if !held_lease {
                    log::info!(
                        "operator acquired leader lease namespace={} lease={} holder_identity={}",
                        namespace,
                        lease_name,
                        holder_identity
                    );
                }
                held_lease = true;
                match reconcile_while_renewing_lease(
                    &kube,
                    &namespace,
                    &lease_name,
                    &holder_identity,
                    lease_duration_seconds,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        log::warn!(
                            "operator lost leader lease during reconciliation namespace={} lease={} holder_identity={}; in-flight reconciliation cancelled",
                            namespace,
                            lease_name,
                            holder_identity
                        );
                        held_lease = false;
                    }
                    Err(error) => {
                        log::error!(
                            "reconcile or leader lease renewal failed namespace={} lease={} error={:#}",
                            namespace,
                            lease_name,
                            error
                        );
                        held_lease = false;
                    }
                }
            }
            Ok(false) => {
                if held_lease {
                    log::warn!(
                        "operator lost leader lease namespace={} lease={} holder_identity={}; reconciliation paused",
                        namespace,
                        lease_name,
                        holder_identity
                    );
                }
                held_lease = false;
            }
            Err(error) => {
                log::warn!(
                    "operator leader lease request failed namespace={} lease={} holder_identity={} error={:#}; reconciliation paused",
                    namespace,
                    lease_name,
                    holder_identity,
                    error
                );
                held_lease = false;
            }
        }
        sleep(Duration::from_secs(interval)).await;
    }
}

async fn reconcile_all(kube: &KubeClient, namespace: &str) -> Result<()> {
    let path = format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}");
    let clusters: ObjectList<RmqttCluster> = kube.get_json(&path).await?;
    for cluster in clusters.items {
        if let Err(e) = reconcile_cluster(kube, namespace, cluster.clone()).await {
            log::error!(
                "cluster reconcile failed namespace={} cluster={} generation={:?} error={e:?}",
                namespace,
                cluster.metadata.name,
                cluster.metadata.generation
            );
            let _ = patch_status(
                kube,
                namespace,
                &cluster.metadata.name,
                json!({
                    "phase": "Degraded",
                    "message": e.to_string(),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await;
        }
    }
    Ok(())
}

async fn reconcile_cluster(kube: &KubeClient, namespace: &str, mut cluster: RmqttCluster) -> Result<()> {
    if external_termination::reconcile_cluster_fence(kube, namespace, &cluster).await? {
        return Ok(());
    }
    let name = cluster.metadata.name.as_str();
    log::debug!(
        "reconciling cluster namespace={} cluster={} generation={:?} desired_replicas={}",
        namespace,
        name,
        cluster.metadata.generation,
        cluster.spec.replicas
    );
    validate_spec(&cluster.spec)?;
    ensure_base_config_map_exists(kube, namespace, &cluster).await?;

    apply_broker_service_account(kube, namespace, &cluster).await?;
    apply_config_map(kube, namespace, &cluster).await?;
    apply_services(kube, namespace, &cluster).await?;
    apply_pod_disruption_budget(kube, namespace, &cluster).await?;
    apply_service_monitor(kube, namespace, &cluster).await?;

    let stateful_set = get_stateful_set(kube, namespace, name).await?;
    let Some(stateful_set) = stateful_set else {
        apply_stateful_set(kube, namespace, &cluster, 1, true).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Bootstrapping",
                "message": "starting the single authorized raft bootstrap node",
                "replicas": 1,
                "readyReplicas": 0,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    };

    let current_replicas = stateful_set.replicas();
    external_termination::ensure_fences(kube, namespace, &cluster, &stateful_set.metadata.uid).await?;
    let planned_draining_node_id = cluster
        .status
        .as_ref()
        .and_then(|status| {
            if status.operation.as_ref().is_some_and(|operation| {
                operation.phase == "RolledBack"
                    || (is_rollout_operation_kind(&operation.kind) && operation.phase == "Blocked")
            }) {
                return None;
            }
            status
                .conf_change
                .clone()
                .or_else(|| status.operation.as_ref().map(OperationStatus::to_conf_change))
        })
        .filter(|change| {
            change.kind == "RemoveVoter" || (is_rollout_operation_kind(&change.kind) && change.node_id > 0)
        })
        .map(|change| change.node_id);
    ensure_unready_pods_not_serving(
        kube,
        namespace,
        name,
        current_replicas,
        planned_draining_node_id,
        degraded_client_endpoints_enabled(&cluster),
    )
    .await?;
    if cluster.spec.graceful_shutdown.enabled
        && handle_external_pod_termination(
            kube,
            namespace,
            &cluster,
            current_replicas,
            &stateful_set.metadata.uid,
        )
        .await?
    {
        return Ok(());
    }
    if stateful_set.bootstrap_allowed() {
        finish_controlled_bootstrap(kube, namespace, &cluster, current_replicas).await?;
        return Ok(());
    }

    if cluster.status.as_ref().and_then(|status| status.observed_restart_at.as_ref()).is_none() {
        let baseline = stateful_set.rollout_restart_at().unwrap_or_default().to_string();
        patch_observed_restart_baseline(kube, namespace, name, &baseline).await?;
        cluster.status.get_or_insert_with(ClusterStatus::default).observed_restart_at =
            Some(baseline.clone());
        log::info!(
            "initialized rollout restart baseline namespace={} cluster={} restarted_at={:?}",
            namespace,
            name,
            baseline
        );
    }

    let pending_restart_at = pending_rollout_restart_at(&stateful_set, &cluster);

    if let Some(recovery) = cluster.status.as_ref().and_then(|status| status.quorum_recovery.clone()) {
        // Quorum recovery owns its membership repairs. Route even an active
        // confChange through the recovery guard so a missing target Pod or a
        // previously rebuilt node that regressed cannot be mistaken for
        // normal learner catch-up.
        continue_quorum_recovery(kube, namespace, &cluster, current_replicas, recovery).await?;
        return Ok(());
    }

    if cluster.status.as_ref().is_some_and(|status| status.phase == "WaitingRaftLeader")
        && leader_membership(kube, namespace, &cluster, current_replicas).await.is_err()
    {
        begin_quorum_recovery(
            kube,
            namespace,
            &cluster,
            current_replicas,
            "the previous reconciliation was already waiting for an authoritative raft leader",
        )
        .await?;
        return Ok(());
    }

    let image_rollout_operation = cluster
        .status
        .as_ref()
        .and_then(|status| status.operation.as_ref())
        .filter(|operation| is_rollout_operation_kind(&operation.kind))
        .cloned();
    if let Some(operation) = image_rollout_operation {
        if operation.kind == "PodRestart" {
            let restart_cluster = pod_restart::resume_cluster(&cluster, &operation)?;
            if !stateful_set.broker_template_matches(&restart_cluster.spec) {
                apply_stateful_set(kube, namespace, &restart_cluster, current_replicas, false).await?;
            }
            if operation.phase == "RolledBack" {
                return pod_restart::finish(
                    kube,
                    namespace,
                    &restart_cluster,
                    current_replicas,
                    None,
                    operation.last_error.as_deref(),
                )
                .await;
            }
            continue_image_rollout(kube, namespace, &restart_cluster, current_replicas, &operation).await?;
            return Ok(());
        }
        let mut requested_cluster = cluster.clone();
        requested_cluster.spec.rollout_restart_at.clone_from(&pending_restart_at);
        if image_rollout_terminal_target_superseded(&operation, &requested_cluster.spec) {
            if operation.node_id > 0 {
                clear_reliable_handoff(namespace, &cluster, current_replicas, operation.node_id).await;
                patch_pod_serving(kube, namespace, &pod_name(name, operation.node_id)).await?;
            }
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": rollout_phase(&requested_cluster.spec, "PreparingImageRollout", "PreparingRollingRestart"),
                    "message": format!(
                        "a new rollout target supersedes the terminal {} operation; clearing it before starting the new request",
                        operation.kind
                    ),
                    "replicas": current_replicas,
                    "readyReplicas": ready_pods(kube, namespace, name).await?.len(),
                    "desiredReplicas": cluster.spec.replicas,
                    "targetImage": cluster.spec.image,
                    "targetRestartAt": pending_restart_at,
                    "drainingNodeId": null,
                    "operation": null,
                    "confChange": null,
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        // Pin an in-flight rollout to the image that started it. If spec.image
        // changes again, the current rollout finishes first and the next
        // reconcile starts a second rollout for the newer image.
        let mut rollout_cluster = cluster.clone();
        if let Some(target_image) = operation.target_image.as_ref() {
            rollout_cluster.spec.image.clone_from(target_image);
        }
        if let Some(target_policy) = operation.target_image_pull_policy.as_ref() {
            rollout_cluster.spec.image_pull_policy.clone_from(target_policy);
        }
        rollout_cluster.spec.rollout_restart_at =
            pending_restart_at.or_else(|| operation.target_restart_at.clone());
        if !stateful_set.broker_template_matches(&rollout_cluster.spec) {
            apply_stateful_set(kube, namespace, &rollout_cluster, current_replicas, false).await?;
        }
        continue_image_rollout(kube, namespace, &rollout_cluster, current_replicas, &operation).await?;
        return Ok(());
    }

    if let Some(conf_change) = cluster.status.as_ref().and_then(|s| {
        s.conf_change.clone().or_else(|| s.operation.as_ref().map(OperationStatus::to_conf_change))
    }) {
        continue_conf_change(kube, namespace, &cluster, current_replicas, conf_change).await?;
        return Ok(());
    }

    if current_replicas < cluster.spec.replicas {
        let next_replicas = current_replicas + 1;
        if current_replicas > 0 {
            let membership = match leader_membership(kube, namespace, &cluster, current_replicas).await {
                Ok(membership) => membership,
                Err(e) => {
                    begin_quorum_recovery(
                        kube,
                        namespace,
                        &cluster,
                        current_replicas,
                        &format!("refusing to start the next scale-out step: {e}"),
                    )
                    .await?;
                    return Ok(());
                }
            };
            if !membership_ready_for_replicas(&membership, current_replicas) {
                patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "WaitingRaftMembers",
                        "message": format!("raft membership is not ready for current replicas {current_replicas}; refusing to start next confchange"),
                        "replicas": current_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "leaderId": membership.leader_id,
                        "raftStatus": membership_status_json(&membership, current_replicas),
                        "operation": null,
                        "confChange": null,
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                return Ok(());
            }
        }
        apply_stateful_set(kube, namespace, &cluster, next_replicas, false).await?;
        let conf_change = ConfChangeStatus {
            kind: "AddLearnerAndPromote".to_string(),
            node_id: next_replicas,
            from_replicas: current_replicas,
            to_replicas: next_replicas,
            requested_replicas: cluster.spec.replicas,
        };
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScalingOut",
                "message": format!(
                    "started node {next_replicas} as a non-voting learner; waiting for snapshot restore and log catch-up before promotion"
                ),
                "replicas": next_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "podStatus": {
                    "replicas": next_replicas,
                    "readyReplicas": ready_pods(kube, namespace, name).await?.len()
                },
                "operation": operation_status_json(&conf_change, "Preparing", "LearnerCatchUp"),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if current_replicas > cluster.spec.replicas {
        scale_in_one(kube, namespace, &cluster, current_replicas, current_replicas - 1).await?;
        return Ok(());
    }

    if !stateful_set.broker_template_matches(&cluster.spec) {
        let image_changed = !stateful_set.broker_image_matches(&cluster.spec);
        apply_stateful_set(kube, namespace, &cluster, current_replicas, false).await?;
        // imageUpdate.autoRollout governs binary changes only. Scheduling and
        // other Pod-template changes are applied to the OnDelete StatefulSet,
        // then wait for an explicit rollout restart so a CR metadata-only
        // change cannot unexpectedly restart a healthy Raft cluster.
        let auto_rollout = image_changed && cluster.spec.image_update.auto_rollout;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": if auto_rollout { "PreparingImageRollout" } else { "UpdatingStatefulSetTemplate" },
                "message": if auto_rollout {
                    json!("StatefulSet pod template was updated; preparing an operator-managed one-pod-at-a-time image rollout")
                } else if image_changed {
                    json!("StatefulSet image template was updated from RmqttCluster spec; imageUpdate.autoRollout is disabled, so existing Pods require an explicit rollout restart")
                } else {
                    json!("StatefulSet Pod configuration was updated from RmqttCluster spec; updateStrategy=OnDelete requires an explicit rollout restart before existing Pods use the new template")
                },
                "replicas": current_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": if auto_rollout {
                    image_rollout_operation_status_json(
                        &cluster,
                        0,
                        current_replicas,
                        "Preparing",
                        None,
                        None,
                        false
                    )
                } else {
                    Value::Null
                },
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let membership = match leader_membership(kube, namespace, &cluster, current_replicas).await {
        Ok(membership) => membership,
        Err(e) => {
            begin_quorum_recovery(
                kube,
                namespace,
                &cluster,
                current_replicas,
                &format!("all pods are locally ready but no authoritative leader is available: {e}"),
            )
            .await?;
            return Ok(());
        }
    };
    if let Some(node_id) = node_requiring_membership_repair(&membership, current_replicas) {
        let conf_change = ConfChangeStatus {
            kind: "AddLearnerAndPromote".to_string(),
            node_id,
            from_replicas: current_replicas,
            to_replicas: current_replicas,
            requested_replicas: cluster.spec.replicas,
        };
        continue_add_learner_and_promote(kube, namespace, &cluster, current_replicas, &conf_change).await?;
        return Ok(());
    }

    if ready_replicas < current_replicas {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingPodsReady",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if let Some(restart_at) = pending_restart_at {
        let mut restart_cluster = cluster.clone();
        restart_cluster.spec.rollout_restart_at = Some(restart_at.clone());
        let pods = cluster_pods(kube, namespace, name).await?;
        let outdated = outdated_pod_node_ids(&pods, name, &restart_cluster.spec, current_replicas);
        if outdated.is_empty() {
            finish_image_rollout(kube, namespace, &restart_cluster, current_replicas, &membership).await?;
            return Ok(());
        }
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "PreparingRollingRestart",
                "message": format!(
                    "detected kubectl rollout restart request {restart_at}; preparing an operator-managed one-pod-at-a-time restart; pending nodes={outdated:?}"
                ),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "targetImage": cluster.spec.image,
                "targetRestartAt": restart_at,
                "updatedReplicas": updated_pod_count(
                    &pods,
                    name,
                    &restart_cluster.spec,
                    current_replicas
                ),
                "operation": image_rollout_operation_status_json(
                    &restart_cluster,
                    0,
                    current_replicas,
                    "Preparing",
                    None,
                    None,
                    false
                ),
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if cluster.spec.image_update.auto_rollout {
        let pods = cluster_pods(kube, namespace, name).await?;
        let outdated = outdated_pod_node_ids(&pods, name, &cluster.spec, current_replicas);
        if !outdated.is_empty() {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "PreparingImageRollout",
                    "message": format!(
                        "detected RMQTT pods not using target image {}; pending nodes={outdated:?}",
                        cluster.spec.image
                    ),
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "raftStatus": membership_status_json(&membership, current_replicas),
                    "operation": image_rollout_operation_status_json(
                        &cluster,
                        0,
                        current_replicas,
                        "Preparing",
                        None,
                        None,
                        false
                    ),
                    "confChange": null,
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    }

    if membership_ready_for_replicas(&membership, current_replicas)
        && pod_restart::start_pending(kube, namespace, &cluster, &stateful_set, &membership).await?
    {
        return Ok(());
    }

    let leader_id = membership.leader_id;
    let members = membership.business_peers.clone();
    let raft_ready = membership_ready_for_replicas(&membership, current_replicas);
    if raft_ready {
        ensure_ready_pods_serving(kube, namespace, name, current_replicas).await?;
    }
    let phase =
        if ready_replicas == cluster.spec.replicas && raft_ready { "Running" } else { "WaitingRaftMembers" };

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": phase,
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "leaderId": leader_id,
            "members": members,
            "podStatus": {
                "replicas": current_replicas,
                "readyReplicas": ready_replicas
            },
            "raftStatus": {
                "leaderId": leader_id,
                "membersReady": raft_ready
            },
            "drainingNodeId": null,
            "message": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

async fn finish_controlled_bootstrap(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    if current_replicas != 1 {
        bail!("controlled raft bootstrap must run with exactly one replica, found {current_replicas}");
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    if ready_replicas < 1 {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Bootstrapping",
                "message": "waiting for the authorized bootstrap node to become raft-ready",
                "replicas": 1,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let membership = match leader_membership(kube, namespace, cluster, 1).await {
        Ok(membership) => membership,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "Bootstrapping",
                    "message": format!("waiting for bootstrap membership: {e}"),
                    "replicas": 1,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": null,
                    "confChange": null,
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };

    if !membership_ready_for_replicas(&membership, 1) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Bootstrapping",
                "message": "bootstrap node is running but its raft membership is not ready",
                "replicas": 1,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, 1),
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_pod_serving(kube, namespace, &pod_name(name, 1)).await?;

    // Disable bootstrap in the StatefulSet template before creating any
    // additional pods. OnDelete keeps the healthy bootstrap pod running, while
    // every future pod creation or restart receives join-existing-only mode.
    apply_stateful_set(kube, namespace, cluster, 1, false).await?;
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "BootstrapReady",
            "message": "controlled raft bootstrap completed; future starts must join the existing cluster",
            "replicas": 1,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, 1),
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
fn pod_termination_remaining(pod: &Pod, configured_timeout_seconds: u64) -> Duration {
    let configured = Duration::from_secs(configured_timeout_seconds);
    let Some(deletion_timestamp) = pod.metadata.deletion_timestamp.as_deref() else {
        return configured;
    };
    let Ok(deletion_timestamp) = DateTime::parse_from_rfc3339(deletion_timestamp) else {
        return configured;
    };
    deletion_timestamp
        .with_timezone(&Utc)
        .signed_duration_since(Utc::now())
        .to_std()
        .unwrap_or_default()
        .min(configured)
}

fn external_termination_deadline(pod: &Pod, configured_timeout_seconds: u64) -> DateTime<Utc> {
    pod.metadata
        .deletion_timestamp
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(|| Utc::now() + chrono::Duration::seconds(configured_timeout_seconds as i64))
}

fn external_target_remaining(target: &ExternalTerminationTargetStatus) -> Duration {
    DateTime::parse_from_rfc3339(&target.deadline_at)
        .ok()
        .and_then(|deadline| deadline.with_timezone(&Utc).signed_duration_since(Utc::now()).to_std().ok())
        .unwrap_or_default()
}

fn external_target_has_old_pod(pods: &[Pod], name: &str, target: &ExternalTerminationTargetStatus) -> bool {
    pod_for_node(pods, name, target.node_id)
        .is_some_and(|pod| pod.metadata.uid == target.pod_uid && pod.metadata.deletion_timestamp.is_some())
}

#[cfg(test)]
fn external_replacement_has_taken_ordinal(
    pods: &[Pod],
    name: &str,
    target: &ExternalTerminationTargetStatus,
) -> bool {
    pod_for_node(pods, name, target.node_id)
        .is_some_and(|pod| pod.metadata.uid != target.pod_uid && pod.metadata.deletion_timestamp.is_none())
}

fn upsert_external_termination_target(
    targets: &mut Vec<ExternalTerminationTargetStatus>,
    pod: &Pod,
    node_id: i32,
    configured_timeout_seconds: u64,
) {
    let replacement = ExternalTerminationTargetStatus {
        node_id,
        pod_uid: pod.metadata.uid.clone(),
        deadline_at: external_termination_deadline(pod, configured_timeout_seconds)
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        phase: "WaitingForSignal".to_string(),
        ..Default::default()
    };
    match targets.iter_mut().find(|target| target.node_id == node_id) {
        Some(existing) if existing.pod_uid != pod.metadata.uid => *existing = replacement,
        Some(_) => {}
        None => targets.push(replacement),
    }
}

fn external_membership_owner(targets: &[ExternalTerminationTargetStatus]) -> Option<usize> {
    if let Some(index) =
        targets.iter().position(|target| target.membership_owner && target.phase != "Completed")
    {
        return Some(index);
    }
    targets
        .iter()
        .enumerate()
        .filter(|(_, target)| {
            matches!(
                target.phase.as_str(),
                "ReadyToLeave" | "Leaving" | "WaitingMembershipRemoval" | "RemovingBusinessMember"
            )
        })
        .min_by_key(|(_, target)| (target.deadline_at.as_str(), target.node_id))
        .map(|(index, _)| index)
}

fn external_batch_json(targets: &[ExternalTerminationTargetStatus]) -> Value {
    if targets.is_empty() {
        Value::Null
    } else {
        json!(ExternalTerminationBatchStatus { targets: targets.to_vec() })
    }
}

async fn patch_external_termination_batch(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    targets: &[ExternalTerminationTargetStatus],
    message: String,
) -> Result<()> {
    let active_ids = targets
        .iter()
        .filter(|target| target.phase != "Completed")
        .map(|target| target.node_id)
        .collect::<Vec<_>>();
    patch_status(
        kube,
        namespace,
        &cluster.metadata.name,
        json!({
            "phase": "ExternalTermination",
            "message": message,
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": active_ids.first(),
            "drainingNodeIds": active_ids,
            "externalTerminationBatch": external_batch_json(targets),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

fn validate_spec(spec: &ClusterSpec) -> Result<()> {
    if spec.replicas < spec.raft.min_replicas {
        bail!("spec.replicas {} must be >= raft.minReplicas {}", spec.replicas, spec.raft.min_replicas);
    }
    if spec.replicas < 1 {
        bail!("spec.replicas must be >= 1");
    }
    if !matches!(spec.image_pull_policy.as_str(), "Always" | "IfNotPresent" | "Never") {
        bail!(
            "spec.imagePullPolicy must be one of Always, IfNotPresent or Never, got {}",
            spec.image_pull_policy
        );
    }
    if spec.image_update.auto_rollout && spec.image_update.stabilization_seconds < 10 {
        bail!("spec.imageUpdate.stabilizationSeconds must be >= 10 when autoRollout=true");
    }
    if spec.pod.annotations.contains_key(KUBECTL_RESTARTED_AT_ANNOTATION) {
        bail!(
            "spec.pod.annotations must not set reserved annotation {KUBECTL_RESTARTED_AT_ANNOTATION}; use kubectl rollout restart on the StatefulSet"
        );
    }
    if spec.pod.annotations.contains_key(POD_MANUAL_RESTART_ANNOTATION) {
        bail!("spec.pod.annotations must not set {POD_MANUAL_RESTART_ANNOTATION}; annotate an individual Pod instead");
    }
    if spec.raft.endpoint_removal_stable_seconds < 1 {
        bail!("spec.raft.endpointRemovalStableSeconds must be >= 1");
    }
    if spec.graceful_shutdown.enabled {
        if spec.graceful_shutdown.timeout_seconds == 0 {
            bail!("spec.gracefulShutdown.timeoutSeconds must be >= 1 when enabled=true");
        }
        if spec.graceful_shutdown.finalize_reserve_seconds >= spec.graceful_shutdown.timeout_seconds {
            bail!("spec.gracefulShutdown.finalizeReserveSeconds must be smaller than timeoutSeconds");
        }
        if effective_termination_grace_period_seconds(spec) <= spec.graceful_shutdown.timeout_seconds as i64 {
            bail!(
                "spec.pod.terminationGracePeriodSeconds must be greater than gracefulShutdown.timeoutSeconds"
            );
        }
    }
    if spec.raft.scale_in_stabilization_seconds < 10 {
        bail!("spec.raft.scaleInStabilizationSeconds must be >= 10");
    }
    if !(50..=95).contains(&spec.raft.handoff_rollback_threshold_percent) {
        bail!("spec.raft.handoffRollbackThresholdPercent must be between 50 and 95");
    }
    for (name, port) in [
        ("mqttPort", spec.service.mqtt_port),
        ("mqttsPort", spec.service.mqtts_port),
        ("wsPort", spec.service.ws_port),
        ("wssPort", spec.service.wss_port),
        ("httpApiPort", spec.service.http_api_port),
        ("grpcPort", spec.service.grpc_port),
        ("raftPort", spec.service.raft_port),
    ] {
        if !(1..=65535).contains(&port) {
            bail!("spec.service.{name} must be between 1 and 65535, got {port}");
        }
    }
    Ok(())
}

async fn ensure_base_config_map_exists(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<()> {
    let Some(config_map_name) = cluster.spec.config.base_config_map_name.as_deref() else {
        return Ok(());
    };
    kube.get_json::<Value>(&format!("/api/v1/namespaces/{namespace}/configmaps/{config_map_name}"))
        .await
        .with_context(|| {
            format!(
                "spec.config.baseConfigMapName references missing or unreadable ConfigMap {namespace}/{config_map_name}"
            )
        })?;
    Ok(())
}

async fn continue_conf_change(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: ConfChangeStatus,
) -> Result<()> {
    match conf_change.kind.as_str() {
        "AddLearnerAndPromote" => {
            continue_add_learner_and_promote(kube, namespace, cluster, current_replicas, &conf_change).await
        }
        "AddVoterDirect" => {
            continue_add_voter_direct(kube, namespace, cluster, current_replicas, &conf_change).await
        }
        "RemoveVoter" => {
            // The desired replica count may be restored while a drain is
            // blocked. Cancel the in-flight removal instead of continuing to
            // drain a node that is needed again.
            if current_replicas > conf_change.to_replicas && cluster.spec.replicas >= current_replicas {
                let victim_id = conf_change.node_id;
                let victim_pod = pod_name(&cluster.metadata.name, victim_id);
                rmqtt_action_value(namespace, cluster, victim_id, "undrain").await?;
                clear_reliable_handoff(namespace, cluster, current_replicas, victim_id).await;
                patch_pod_serving(kube, namespace, &victim_pod).await?;
                patch_status(
                    kube,
                    namespace,
                    &cluster.metadata.name,
                    json!({
                        "phase": "ScaleInCancelled",
                        "message": format!(
                            "cancelled removal of node {victim_id} because desired replicas changed to {}",
                            cluster.spec.replicas
                        ),
                        "replicas": current_replicas,
                        "readyReplicas": ready_pods(kube, namespace, &cluster.metadata.name).await?.len(),
                        "desiredReplicas": cluster.spec.replicas,
                        "drainingNodeId": null,
                        "operation": null,
                        "confChange": null,
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                return Ok(());
            }
            if current_replicas <= conf_change.to_replicas {
                let ready_replicas = ready_pods(kube, namespace, &cluster.metadata.name).await?.len() as i32;
                let membership = match leader_membership(kube, namespace, cluster, current_replicas).await {
                    Ok(membership) => membership,
                    Err(error) => {
                        patch_status(
                            kube,
                            namespace,
                            &cluster.metadata.name,
                            json!({
                                "phase": "WaitingRaftMembers",
                                "message": format!(
                                    "node {} was removed; waiting for an authoritative leader before continuing post-removal stabilization: {error}",
                                    conf_change.node_id
                                ),
                                "replicas": current_replicas,
                                "readyReplicas": ready_replicas,
                                "desiredReplicas": cluster.spec.replicas,
                                "drainingNodeId": null,
                                "operation": scale_in_operation_status_json(
                                    cluster,
                                    &conf_change,
                                    "WaitingPostRemovalHealth",
                                    Some(&error.to_string()),
                                    false
                                ),
                                "confChange": conf_change_status_json(&conf_change),
                                "observedGeneration": cluster.metadata.generation,
                            }),
                        )
                        .await?;
                        return Ok(());
                    }
                };
                let leader_id = membership.leader_id;
                let victim_id = conf_change.node_id;
                if voter_exists(&membership, victim_id) || membership.learners.contains(&victim_id) {
                    let leader_id =
                        leader_id.ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
                    rmqtt_action_value_with_payload(
                        namespace,
                        cluster,
                        leader_id,
                        json!({"action": "remove_voter", "id": victim_id}),
                    )
                    .await?;
                    patch_status(
                        kube,
                        namespace,
                        &cluster.metadata.name,
                        json!({
                            "phase": "RecoveringScaleIn",
                            "message": format!(
                                "pod for node {victim_id} is already absent; requested authoritative leader {leader_id} to remove its stale raft voter/learner membership"
                            ),
                            "replicas": current_replicas,
                            "readyReplicas": ready_replicas,
                            "desiredReplicas": cluster.spec.replicas,
                            "leaderId": leader_id,
                            "raftStatus": membership_status_json(&membership, current_replicas),
                            "operation": operation_status_json(&conf_change, "CleaningStaleRaftMember", "RemoveVoter"),
                            "confChange": conf_change_status_json(&conf_change),
                            "observedGeneration": cluster.metadata.generation,
                        }),
                    )
                    .await?;
                    return Ok(());
                }
                if member_exists(&membership.business_peers, victim_id) {
                    let leader_id =
                        leader_id.ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
                    rmqtt_action_value_with_payload(
                        namespace,
                        cluster,
                        leader_id,
                        json!({"action": "force_node_remove", "id": victim_id}),
                    )
                    .await?;
                    patch_status(
                        kube,
                        namespace,
                        &cluster.metadata.name,
                        json!({
                            "phase": "RecoveringScaleIn",
                            "message": format!(
                                "pod and raft membership for node {victim_id} are absent; requested cleanup of its stale business routing metadata"
                            ),
                            "replicas": current_replicas,
                            "readyReplicas": ready_replicas,
                            "desiredReplicas": cluster.spec.replicas,
                            "leaderId": leader_id,
                            "raftStatus": membership_status_json(&membership, current_replicas),
                            "operation": operation_status_json(&conf_change, "CleaningStaleBusinessMember", "RemoveVoter"),
                            "confChange": conf_change_status_json(&conf_change),
                            "observedGeneration": cluster.metadata.generation,
                        }),
                    )
                    .await?;
                    return Ok(());
                }
                finish_scale_in_step(kube, namespace, cluster, &conf_change).await
            } else {
                scale_in_one(kube, namespace, cluster, current_replicas, conf_change.to_replicas).await
            }
        }
        other => bail!("unsupported confChange type {other}"),
    }
}

async fn continue_add_voter_direct(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let target_replicas = conf_change.to_replicas;

    if current_replicas < target_replicas {
        apply_stateful_set(kube, namespace, cluster, target_replicas, false).await?;
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let membership = match leader_membership(kube, namespace, cluster, target_replicas).await {
        Ok(membership) => membership,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftLeader",
                    "message": format!(
                        "node {} was created but no authoritative raft leader membership is available: {e}",
                        conf_change.node_id
                    ),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "podStatus": {
                        "replicas": target_replicas,
                        "readyReplicas": ready_replicas
                    },
                    "operation": operation_status_json(conf_change, "WaitingLeader", "DirectAddVoter"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };
    let leader_id = membership.leader_id;
    let members = membership.business_peers.clone();
    let raft_member_exists = voter_exists(&membership, conf_change.node_id);
    let raft_peer_is_available = raft_peer_available(&membership, conf_change.node_id);
    let business_member_exists = member_exists(&members, conf_change.node_id);
    let raft_ready = membership_ready_for_replicas(&membership, target_replicas);

    if !raft_ready {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingRaftMembers",
                "message": format!(
                    "leader membership has not confirmed node {}; voter={}, raftPeerAvailable={}, businessPeer={}",
                    conf_change.node_id, raft_member_exists, raft_peer_is_available, business_member_exists
                ),
                "replicas": target_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": leader_id,
                "members": members,
                "podStatus": {
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, target_replicas),
                "operation": operation_status_json(conf_change, "WaitingLeaderMembership", "DirectAddVoter"),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if ready_replicas < target_replicas {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingPodsReady",
                "message": format!(
                    "leader confirmed node {} membership; waiting for its local readiness before enabling client traffic",
                    conf_change.node_id
                ),
                "replicas": target_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": leader_id,
                "members": members,
                "podStatus": {
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, target_replicas),
                "operation": operation_status_json(conf_change, "WaitingPodLocalReady", "DirectAddVoter"),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    ensure_ready_pods_serving(kube, namespace, name, target_replicas).await?;
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": if target_replicas == cluster.spec.replicas && raft_ready { "Running" } else { "ScalingOut" },
            "message": if target_replicas == cluster.spec.replicas {
                Value::Null
            } else {
                json!(format!("node {} joined; waiting to add next node toward desired replicas {}", conf_change.node_id, cluster.spec.replicas))
            },
            "replicas": target_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": leader_id,
            "members": members,
            "podStatus": {
                "replicas": target_replicas,
                "readyReplicas": ready_replicas
            },
            "raftStatus": {
                "leaderId": leader_id,
                "membersReady": raft_ready,
                "voters": membership.voters,
                "learners": membership.learners,
                "available": membership.available,
                "started": membership.started,
                "mode": "DirectAddVoter",
                "note": "legacy direct-voter operation recovered from an in-flight status created by an older operator"
            },
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

fn classify_node_join(
    membership: &MembershipStatus,
    node_id: i32,
    ready_replicas: i32,
    expected_replicas: i32,
) -> NodeJoinStep {
    classify_node_join_with_scope(
        membership,
        node_id,
        ready_replicas,
        expected_replicas,
        false,
        NodeJoinCompletionScope::Cluster,
    )
}

fn classify_membership_repair_node_join(
    membership: &MembershipStatus,
    node_id: i32,
    target_pod_ready: bool,
) -> NodeJoinStep {
    classify_node_join_with_scope(
        membership,
        node_id,
        0,
        0,
        target_pod_ready,
        NodeJoinCompletionScope::TargetPod,
    )
}

fn classify_node_join_with_scope(
    membership: &MembershipStatus,
    node_id: i32,
    ready_replicas: i32,
    expected_replicas: i32,
    target_pod_ready: bool,
    completion_scope: NodeJoinCompletionScope,
) -> NodeJoinStep {
    let is_voter = voter_exists(membership, node_id);
    let is_learner = membership.learners.contains(&node_id);
    let peer_available = raft_peer_available(membership, node_id);
    let peer_caught_up = raft_peer_caught_up(membership, node_id);
    let business_member = member_exists(&membership.business_peers, node_id);

    if !is_voter {
        if is_learner && peer_available && peer_caught_up {
            NodeJoinStep::Promote
        } else {
            NodeJoinStep::CatchingUp
        }
    } else if peer_caught_up
        && business_member
        && match completion_scope {
            NodeJoinCompletionScope::Cluster => {
                ready_replicas >= expected_replicas
                    && membership_ready_for_replicas(membership, expected_replicas)
            }
            NodeJoinCompletionScope::TargetPod => {
                target_pod_ready
                    && is_authoritative_leader(membership)
                    && membership.started
                    && membership.available
                    && membership.caught_up
                    && membership.snapshot_restored
                    && !membership.draining
            }
        }
    {
        NodeJoinStep::Ready
    } else {
        NodeJoinStep::WaitingReady
    }
}

async fn observe_node_join(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    membership_node_ids: &[i32],
    node_id: i32,
    expected_replicas: i32,
    completion_scope: NodeJoinCompletionScope,
) -> Result<NodeJoinObservation> {
    let name = cluster.metadata.name.as_str();
    let ready_pods = ready_pods(kube, namespace, name).await?;
    let ready_replicas = ready_pods.len() as i32;
    let target_pod_name = pod_name(name, node_id);
    let target_pod_ready = ready_pods.iter().any(|pod| pod.metadata.name == target_pod_name);
    let membership = leader_membership_from_nodes(namespace, cluster, membership_node_ids).await?;
    let is_learner = membership.learners.contains(&node_id);
    let peer_available = raft_peer_available(&membership, node_id);
    let peer_caught_up = raft_peer_caught_up(&membership, node_id);
    let business_member = member_exists(&membership.business_peers, node_id);
    let step = match completion_scope {
        NodeJoinCompletionScope::Cluster => {
            classify_node_join(&membership, node_id, ready_replicas, expected_replicas)
        }
        NodeJoinCompletionScope::TargetPod => {
            classify_membership_repair_node_join(&membership, node_id, target_pod_ready)
        }
    };

    Ok(NodeJoinObservation {
        membership,
        ready_replicas,
        target_pod_ready,
        is_learner,
        peer_available,
        peer_caught_up,
        business_member,
        step,
    })
}

async fn promote_joining_node(
    namespace: &str,
    cluster: &RmqttCluster,
    membership: &MembershipStatus,
    node_id: i32,
) -> Result<()> {
    rmqtt_action_value_with_payload(
        namespace,
        cluster,
        membership.id,
        json!({"action": "promote_learner", "id": node_id}),
    )
    .await?;
    Ok(())
}

async fn continue_add_learner_and_promote(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let target_replicas = conf_change.to_replicas;
    let node_id = conf_change.node_id;
    let membership_repair = conf_change.from_replicas == conf_change.to_replicas;

    if current_replicas < target_replicas {
        apply_stateful_set(kube, namespace, cluster, target_replicas, false).await?;
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let membership_node_ids = (1..=target_replicas).collect::<Vec<_>>();
    let observation = match observe_node_join(
        kube,
        namespace,
        cluster,
        &membership_node_ids,
        node_id,
        target_replicas,
        if membership_repair { NodeJoinCompletionScope::TargetPod } else { NodeJoinCompletionScope::Cluster },
    )
    .await
    {
        Ok(observation) => observation,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftLeader",
                    "message": format!("learner {node_id} is starting but no authoritative raft leader is available: {e}"),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": operation_status_json(conf_change, "WaitingLeader", "LearnerCatchUp"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };
    let NodeJoinObservation {
        membership,
        ready_replicas,
        target_pod_ready,
        is_learner,
        peer_available,
        peer_caught_up,
        business_member,
        step,
    } = observation;

    match step {
        NodeJoinStep::CatchingUp => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftCatchUp",
                    "message": format!(
                        "waiting for learner {node_id} to catch up; learner={is_learner}, raftPeerAvailable={peer_available}, caughtUp={peer_caught_up}, leaderCommit={}",
                        membership.committed_index
                    ),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "raftStatus": membership_status_json(&membership, target_replicas),
                    "operation": operation_status_json(conf_change, "CatchingUp", "LearnerCatchUp"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        NodeJoinStep::Promote => {
            // Keep scale-out aligned with image replacement recovery: the
            // leader-side Raft progress observed by classify_node_join is the
            // promotion gate. A joining learner intentionally does not finish
            // plugin startup (and therefore cannot serve its HTTP control API)
            // until it has become a voter, so querying its local membership
            // before promotion creates a circular wait.
            promote_joining_node(namespace, cluster, &membership, node_id).await?;
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "PromotingRaftLearner",
                    "message": format!("learner {node_id} caught up through index {}; promotion to voter requested", membership.committed_index),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "raftStatus": membership_status_json(&membership, target_replicas),
                    "operation": operation_status_json(conf_change, "Promoting", "LearnerCatchUp"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        NodeJoinStep::WaitingReady => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingPodsReady",
                    "message": format!(
                        "node {node_id} is a voter; waiting for local state-machine readiness and business NodeUp; caughtUp={peer_caught_up}, businessPeer={business_member}, targetPodReady={target_pod_ready}, readyReplicas={ready_replicas}/{target_replicas}"
                    ),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "members": membership.business_peers.clone(),
                    "raftStatus": membership_status_json(&membership, target_replicas),
                    "operation": operation_status_json(conf_change, "WaitingBusinessReady", "LearnerCatchUp"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        NodeJoinStep::Ready => {}
    }

    ensure_ready_pods_serving(kube, namespace, name, target_replicas).await?;
    let cluster_ready =
        ready_replicas >= target_replicas && membership_ready_for_replicas(&membership, target_replicas);
    let remaining_repair =
        if membership_repair { node_requiring_membership_repair(&membership, target_replicas) } else { None };
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": if membership_repair && !cluster_ready {
                "WaitingPodsReady"
            } else if target_replicas == cluster.spec.replicas {
                "Running"
            } else {
                "ScalingOut"
            },
            "message": if membership_repair && !cluster_ready {
                json!(match remaining_repair {
                    Some(next_node_id) => format!(
                        "node {node_id} membership repair completed; released its operation lock so node {next_node_id} can be repaired next"
                    ),
                    None => format!(
                        "node {node_id} membership repair completed; waiting for the remaining Pods and membership to converge"
                    ),
                })
            } else if target_replicas == cluster.spec.replicas {
                Value::Null
            } else {
                json!(format!("node {node_id} safely promoted and admitted; continuing toward {} replicas", cluster.spec.replicas))
            },
            "replicas": target_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "members": membership.business_peers.clone(),
            "raftStatus": membership_status_json(&membership, target_replicas),
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

// Keeping these fields explicit at call sites makes every destructive rollout
// transition auditable without hiding defaults in a builder.
#[allow(clippy::too_many_arguments)]
async fn patch_image_rollout_status(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    phase: &str,
    message: impl Into<String>,
    node_id: i32,
    operation_phase: &str,
    membership: Option<&MembershipStatus>,
    stable_since: Option<String>,
    last_error: Option<&str>,
    increment_retry: bool,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let mut status = json!({
        "phase": phase,
        "message": message.into(),
        "replicas": current_replicas,
        "readyReplicas": ready_replicas,
        "desiredReplicas": cluster.spec.replicas,
        "targetImage": cluster.spec.image,
        "targetRestartAt": cluster.spec.rollout_restart_at,
        "updatedReplicas": updated_pod_count(
            &cluster_pods(kube, namespace, name).await?,
            name,
            &cluster.spec,
            current_replicas
        ),
        "drainingNodeId": if node_id > 0 { json!(node_id) } else { Value::Null },
        "operation": image_rollout_operation_status_json(
            cluster,
            node_id,
            current_replicas,
            operation_phase,
            stable_since,
            last_error,
            increment_retry
        ),
        "confChange": null,
        "observedGeneration": cluster.metadata.generation,
    });
    if cluster.spec.pod_restart.is_some() {
        // A single replacement is not progress toward updating every image.
        status["updatedReplicas"] = Value::Null;
    }
    if let Some(membership) = membership {
        status["leaderId"] = json!(membership.leader_id);
        status["members"] = json!(membership.business_peers);
        status["raftStatus"] = membership_status_json(membership, current_replicas);
    }
    patch_status(kube, namespace, name, status).await
}

async fn finish_image_rollout(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    membership: &MembershipStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    ensure_ready_pods_serving(kube, namespace, name, current_replicas).await?;
    let mut status = json!({
        "phase": "Running",
        "message": null,
        "replicas": current_replicas,
        "readyReplicas": ready_replicas,
        "desiredReplicas": cluster.spec.replicas,
        "leaderId": membership.leader_id,
        "members": membership.business_peers,
        "raftStatus": membership_status_json(membership, current_replicas),
        "currentImage": cluster.spec.image,
        "targetImage": cluster.spec.image,
        "targetRestartAt": null,
        "updatedReplicas": current_replicas,
        "drainingNodeId": null,
        "operation": null,
        "confChange": null,
        "observedGeneration": cluster.metadata.generation,
    });
    if let Some(restart_at) = cluster.spec.rollout_restart_at.as_ref() {
        status["observedRestartAt"] = json!(restart_at);
    }
    patch_status(kube, namespace, name, status).await
}

fn rollout_remaining_node_ids(current_replicas: i32, node_id: i32) -> Vec<i32> {
    (1..=current_replicas).filter(|candidate| *candidate != node_id).collect()
}

fn rollout_leadership_target(
    membership: &MembershipStatus,
    pods: &[Pod],
    cluster_name: &str,
    spec: &ClusterSpec,
    victim_id: i32,
) -> Option<i32> {
    rollout_leadership_targets(membership, pods, cluster_name, spec, victim_id).into_iter().next()
}

fn rollout_leadership_targets(
    membership: &MembershipStatus,
    pods: &[Pod],
    cluster_name: &str,
    spec: &ClusterSpec,
    victim_id: i32,
) -> Vec<i32> {
    let mut candidates = membership
        .voters
        .iter()
        .copied()
        .filter(|candidate| *candidate != victim_id)
        .filter(|candidate| raft_peer_caught_up(membership, *candidate))
        .filter(|candidate| {
            pod_for_node(pods, cluster_name, *candidate).is_some_and(|pod| {
                pod_is_ready(pod)
                    && if spec.pod_restart.is_some() {
                        // These voters already belong to the same live cluster.
                        // A single-pod request must also work with autoRollout disabled.
                        true
                    } else if rollout_is_restart(spec) {
                        // A restart does not change the binary compatibility
                        // boundary. Any ready voter already using the target
                        // image can safely receive leadership, including when
                        // a newer restart request arrives mid-rollout.
                        pod_uses_target_image(pod, spec)
                    } else {
                        pod_uses_rollout_target(pod, spec)
                    }
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates
}

fn rollout_leadership_target_for_retry(
    membership: &MembershipStatus,
    pods: &[Pod],
    cluster_name: &str,
    spec: &ClusterSpec,
    victim_id: i32,
    retry_count: u32,
) -> Option<i32> {
    let candidates = rollout_leadership_targets(membership, pods, cluster_name, spec, victim_id);
    (!candidates.is_empty()).then(|| candidates[(retry_count as usize + 1) % candidates.len()])
}

fn leader_transfer_retry_due(operation: &OperationStatus, now: DateTime<Utc>) -> bool {
    let Some(last_attempt_at) = operation.leader_transfer_attempted_at.as_deref() else {
        return true;
    };
    let Ok(last_attempt_at) = DateTime::parse_from_rfc3339(last_attempt_at) else {
        return true;
    };
    now.signed_duration_since(last_attempt_at.with_timezone(&Utc)).num_seconds()
        >= LEADER_TRANSFER_RETRY_SECONDS
}

async fn wait_node_removed_from_membership(
    namespace: &str,
    cluster: &RmqttCluster,
    remaining_node_ids: &[i32],
    victim_id: i32,
) -> Result<MembershipStatus> {
    let mut last_error = None;
    for _ in 0..60 {
        let membership = match leader_membership_from_nodes(namespace, cluster, remaining_node_ids).await {
            Ok(membership) => membership,
            Err(error) => {
                last_error = Some(error.to_string());
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if !voter_exists(&membership, victim_id)
            && !membership.learners.contains(&victim_id)
            && !member_exists(&membership.business_peers, victim_id)
        {
            return Ok(membership);
        }
        sleep(Duration::from_secs(2)).await;
    }
    bail!(
        "node {victim_id} was not removed from raft voters and business peers before timeout{}",
        last_error.map(|error| format!("; last leader query error: {error}")).unwrap_or_default()
    )
}

async fn cancel_image_rollout_before_delete(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    node_id: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    if node_id > 0 {
        let _ = rmqtt_action_value(namespace, cluster, node_id, "undrain").await;
        clear_reliable_handoff(namespace, cluster, current_replicas, node_id).await;
        patch_pod_serving(kube, namespace, &pod_name(name, node_id)).await?;
    }
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ImageRolloutCancelled",
            "message": "automatic image rollout was disabled before the current pod was deleted",
            "replicas": current_replicas,
            "readyReplicas": ready_pods(kube, namespace, name).await?.len(),
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": null,
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn rollback_image_rollout_drain(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    node_id: i32,
    membership: Option<&MembershipStatus>,
    reason: &str,
) -> Result<()> {
    let action = rollout_action(&cluster.spec);
    let clear_error = match restore_node_after_failed_drain(
        kube,
        namespace,
        cluster,
        current_replicas,
        node_id,
    )
    .await
    {
        Ok(clear_error) => clear_error,
        Err(error) => {
            return patch_image_rollout_status(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    rollout_phase(&cluster.spec, "RollingBackImageDrain", "RollingBackRestartDrain"),
                    format!(
                        "{action} safety barrier failed for node {node_id}; keeping the pod out of Service while undrain is retried: {reason}; rollbackError={error}"
                    ),
                    node_id,
                    "RollingBackDrain",
                    membership,
                    None,
                    Some(&error.to_string()),
                    true,
                )
                .await;
        }
    };
    let message = if let Some(clear_error) = clear_error.as_deref() {
        format!(
            "{action} was rolled back before pod deletion and node {node_id} was restored to Service: {reason}; some handoff markers still need retry: {clear_error}"
        )
    } else {
        format!(
            "{action} was rolled back before pod deletion and node {node_id} was restored to Service: {reason}"
        )
    };
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "ImageRolloutBlocked", "RollingRestartBlocked"),
        message,
        node_id,
        "RolledBack",
        membership,
        None,
        Some(reason),
        false,
    )
    .await
}

async fn prepare_next_image_rollout_node(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let action = rollout_action(&cluster.spec);
    if !operator_managed_rollout_enabled(&cluster.spec) {
        return cancel_image_rollout_before_delete(kube, namespace, cluster, current_replicas, 0).await;
    }
    if current_replicas != cluster.spec.replicas {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ImageRolloutPaused",
                "message": format!("replica count changed before the next {action} step; pausing the rollout so scaling can reconcile first"),
                "replicas": current_replicas,
                "readyReplicas": ready_pods(kube, namespace, name).await?.len(),
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let pods = cluster_pods(kube, namespace, name).await?;
    let outdated = outdated_pod_node_ids(&pods, name, &cluster.spec, current_replicas);
    let membership = match leader_membership(kube, namespace, cluster, current_replicas).await {
        Ok(membership) => membership,
        Err(error) => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "WaitingRaftLeader",
                format!("waiting for an authoritative leader before {action}: {error}"),
                0,
                "Preparing",
                None,
                None,
                Some(&error.to_string()),
                true,
            )
            .await?;
            return Ok(());
        }
    };
    if outdated.is_empty() {
        return finish_image_rollout(kube, namespace, cluster, current_replicas, &membership).await;
    }
    let ready_replicas = pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32;
    if ready_replicas != current_replicas || !membership_ready_for_replicas(&membership, current_replicas) {
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(
                &cluster.spec,
                "WaitingImageRolloutPreconditions",
                "WaitingRollingRestartPreconditions",
            ),
            format!(
                "waiting for a fully healthy cluster before selecting the next {action} node; ready={ready_replicas}/{current_replicas}, pending={outdated:?}"
            ),
            0,
            "Preparing",
            Some(&membership),
            None,
            None,
            false,
        )
        .await?;
        return Ok(());
    }

    let leader_id =
        membership.leader_id.ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
    let victim_id =
        outdated.iter().copied().filter(|node_id| *node_id != leader_id).max().unwrap_or(leader_id);
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
        if let Some(restart_at) = cluster.spec.rollout_restart_at.as_deref() {
            format!(
                "selected node {victim_id} for one-pod-at-a-time restart request {restart_at}; current leader={leader_id}"
            )
        } else {
            format!(
                "selected node {victim_id} for one-pod-at-a-time image update to {}; current leader={leader_id}",
                cluster.spec.image
            )
        },
        victim_id,
        "PreparingNode",
        Some(&membership),
        None,
        None,
        false,
    )
    .await
}

async fn continue_image_rollout_recovery(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    operation: &OperationStatus,
    node_id: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let action = rollout_action(&cluster.spec);
    let remaining = rollout_remaining_node_ids(current_replicas, node_id);
    let observation = match observe_node_join(
        kube,
        namespace,
        cluster,
        &remaining,
        node_id,
        current_replicas,
        NodeJoinCompletionScope::Cluster,
    )
    .await
    {
        Ok(observation) => observation,
        Err(error) => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "WaitingRaftLeader",
                format!(
                    "updated node {node_id} is starting but no authoritative leader is available: {error}"
                ),
                node_id,
                "WaitingLeader",
                None,
                None,
                Some(&error.to_string()),
                true,
            )
            .await?;
            return Ok(());
        }
    };
    let NodeJoinObservation {
        membership,
        ready_replicas,
        target_pod_ready,
        is_learner,
        peer_available,
        peer_caught_up,
        business_member,
        step,
    } = observation;

    match step {
        NodeJoinStep::CatchingUp => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "WaitingRaftCatchUp",
                format!(
                    "waiting for updated learner {node_id} to catch up; learner={is_learner}, raftPeerAvailable={peer_available}, caughtUp={peer_caught_up}, leaderCommit={}",
                    membership.committed_index
                ),
                node_id,
                "CatchingUp",
                Some(&membership),
                None,
                None,
                false,
            )
            .await?;
            return Ok(());
        }
        NodeJoinStep::Promote => {
            promote_joining_node(namespace, cluster, &membership, node_id).await?;
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "PromotingRaftLearner",
                format!(
                    "updated learner {node_id} caught up through index {}; promotion to voter requested",
                    membership.committed_index
                ),
                node_id,
                "Promoting",
                Some(&membership),
                None,
                None,
                false,
            )
            .await?;
            return Ok(());
        }
        NodeJoinStep::WaitingReady => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "WaitingPodsReady",
                format!(
                    "updated node {node_id} is a voter; waiting for local readiness and business NodeUp; caughtUp={peer_caught_up}, businessPeer={business_member}, targetPodReady={target_pod_ready}, ready={ready_replicas}/{current_replicas}"
                ),
                node_id,
                "WaitingBusinessReady",
                Some(&membership),
                None,
                None,
                false,
            )
            .await?;
            return Ok(());
        }
        NodeJoinStep::Ready => {}
    }

    patch_pod_serving(kube, namespace, &pod_name(name, node_id)).await?;
    clear_reliable_handoff(namespace, cluster, current_replicas, node_id).await;
    let stabilization_seconds = cluster.spec.image_update.stabilization_seconds;
    if operation.phase != "Stabilizing" || operation.stable_since.is_none() {
        let stable_since = utc_now_rfc3339();
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
                rollout_phase(&cluster.spec, "StabilizingImageRollout", "StabilizingRollingRestart"),
                format!(
                    "node {node_id} completed its {action}; observing cluster stability for {stabilization_seconds} seconds before updating another pod"
                ),
            node_id,
            "Stabilizing",
            Some(&membership),
            Some(stable_since),
            None,
            false,
        )
        .await?;
        return Ok(());
    }
    if !operation_stabilization_complete(operation, stabilization_seconds) {
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(&cluster.spec, "StabilizingImageRollout", "StabilizingRollingRestart"),
            format!(
                "node {node_id} remains healthy after {action}; waiting for the {stabilization_seconds}-second stability window"
            ),
            node_id,
            "Stabilizing",
            Some(&membership),
            operation.stable_since.clone(),
            None,
            false,
        )
        .await?;
        return Ok(());
    }

    if cluster.spec.pod_restart.is_some() {
        return pod_restart::finish(kube, namespace, cluster, current_replicas, Some(&membership), None)
            .await;
    }

    if current_replicas != cluster.spec.replicas {
        clear_reliable_handoff(namespace, cluster, current_replicas, node_id).await;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": rollout_phase(&cluster.spec, "ImageRolloutPaused", "RollingRestartPaused"),
                "message": format!("node {node_id} completed its {action}; replica change will reconcile before the next rollout step"),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }
    clear_reliable_handoff(namespace, cluster, current_replicas, node_id).await;
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "PreparingImageRollout", "PreparingRollingRestart"),
        format!("node {node_id} passed the {action} stability window; selecting the next outdated pod"),
        0,
        "Preparing",
        Some(&membership),
        None,
        None,
        false,
    )
    .await
}

fn image_rollout_waiting_for_drain_after_timeout(operation: &OperationStatus) -> bool {
    matches!(operation.phase.as_str(), "WaitingDrainAfterTimeout" | "WaitingDrainStableAfterTimeout")
        || (operation.phase == "Blocked"
            && operation.last_error.as_deref() == Some(IMAGE_DRAIN_TIMEOUT_ERROR))
}

async fn continue_image_rollout_drain_after_timeout(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    node_id: i32,
    membership: &MembershipStatus,
) -> Result<()> {
    if cluster.spec.raft.drain_timeout_action == DrainTimeoutAction::ForceContinue {
        let blockers = "resuming a previously recorded drain timeout";
        let summary = force_finalize_node_drain(
            namespace,
            cluster,
            current_replicas,
            node_id,
            "drain_timeout_recovery",
            blockers,
        )
        .await?;
        record_forced_drain_status(kube, namespace, cluster, &summary).await?;
        return patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
            summary.description(),
            node_id,
            "RequestingLeave",
            Some(membership),
            None,
            None,
            false,
        )
        .await;
    }

    rollback_image_rollout_drain(
        kube,
        namespace,
        cluster,
        current_replicas,
        node_id,
        Some(membership),
        &format!(
            "node {node_id} drain exceeded {} seconds without proving all delivery barriers safe",
            cluster.spec.raft.drain_timeout_seconds
        ),
    )
    .await
}

async fn continue_image_rollout_before_recreation(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    operation: &OperationStatus,
    node_id: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let action = rollout_action(&cluster.spec);
    let pods = cluster_pods(kube, namespace, name).await?;
    let pod = pod_for_node(&pods, name, node_id);
    let pod_state = match pod {
        Some(pod) if pod.metadata.deletion_timestamp.is_some() => VictimPodState::Terminating,
        Some(_) => VictimPodState::Present,
        None => VictimPodState::Absent,
    };
    if pod_state != VictimPodState::Present {
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(&cluster.spec, "WaitingImagePodRecreation", "WaitingRestartedPodRecreation"),
            format!(
                "waiting for {} to be recreated from the target StatefulSet template",
                pod_name(name, node_id)
            ),
            node_id,
            "WaitingPodRecreation",
            None,
            None,
            None,
            false,
        )
        .await?;
        return Ok(());
    }
    if let Some(target) = cluster.spec.pod_restart.as_ref() {
        if pod.is_none_or(|pod| pod.metadata.uid != target.pod_uid) {
            // A replacement appeared between the caller's list and this one.
            // Let the next reconciliation observe its recovery, never drain it.
            return Ok(());
        }
    }

    let remaining = rollout_remaining_node_ids(current_replicas, node_id);
    let membership_result = if matches!(operation.phase.as_str(), "PreparingNode" | "TransferringLeader") {
        leader_membership(kube, namespace, cluster, current_replicas).await
    } else {
        leader_membership_from_nodes(namespace, cluster, &remaining).await
    };
    let membership = match membership_result {
        Ok(membership) => membership,
        Err(error) => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "WaitingRaftLeader",
                format!("waiting for an authoritative leader while updating node {node_id}: {error}"),
                node_id,
                operation.phase.as_str(),
                None,
                None,
                Some(&error.to_string()),
                true,
            )
            .await?;
            return Ok(());
        }
    };
    if operation.phase == "RollingBackDrain" {
        return rollback_image_rollout_drain(
            kube,
            namespace,
            cluster,
            current_replicas,
            node_id,
            Some(&membership),
            operation.last_error.as_deref().unwrap_or("rollout drain rollback is pending"),
        )
        .await;
    }
    if operation.phase == "RolledBack" {
        clear_reliable_handoff(namespace, cluster, current_replicas, node_id).await;
        patch_pod_serving(kube, namespace, &pod_name(name, node_id)).await?;
        return Ok(());
    }
    if image_rollout_waiting_for_drain_after_timeout(operation) {
        return continue_image_rollout_drain_after_timeout(
            kube,
            namespace,
            cluster,
            current_replicas,
            node_id,
            &membership,
        )
        .await;
    }
    if operation.phase == "ForceFinalizing" && cluster.spec.pod_restart.is_some() {
        let blockers = operation
            .last_error
            .as_deref()
            .unwrap_or("single-Pod drain timed out before all safety barriers converged");
        if let Err(error) = ensure_force_drain_finalize_supported(namespace, cluster, current_replicas).await
        {
            // The intent is already persisted and an earlier attempt may have
            // reached some brokers. Wait for a uniformly capable cluster; do
            // not roll back state which may already have been discarded.
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "ForceFinalizingPodRestart",
                format!(
                    "manualRestart=force-on-timeout cleanup for node {node_id} is waiting for every broker to advertise support: {error}"
                ),
                node_id,
                "ForceFinalizing",
                Some(&membership),
                None,
                Some(blockers),
                true,
            )
            .await?;
            return Ok(());
        }
        match force_finalize_node_drain(
            namespace,
            cluster,
            current_replicas,
            node_id,
            "pod_restart_drain_timeout",
            blockers,
        )
        .await
        {
            Ok(summary) => {
                record_forced_pod_restart_drain_status(kube, namespace, cluster, &summary).await?;
            }
            Err(error) => {
                // Force finalization may already have completed on a subset of
                // brokers. It is idempotent, so persist and retry instead of
                // attempting to restore a state which may have been discarded.
                patch_image_rollout_status(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    "ForceFinalizingPodRestart",
                    format!(
                        "manualRestart=force-on-timeout cleanup for node {node_id} is incomplete and will retry: {error}"
                    ),
                    node_id,
                    "ForceFinalizing",
                    Some(&membership),
                    None,
                    Some(blockers),
                    true,
                )
                .await?;
            }
        }
        return Ok(());
    }
    if operation.phase == "ForceFinalized" && cluster.spec.pod_restart.is_some() {
        match forced_pod_restart_leave_decision(&membership, current_replicas, node_id) {
            ForcedPodRestartLeaveDecision::DeletePod => {
                // The leave request may have completed immediately before an
                // operator crash. Membership is the durable completion proof,
                // so resume with the UID-protected Pod deletion.
                patch_image_rollout_status(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    "RestartingBroker",
                    format!(
                        "manualRestart=force-on-timeout cleanup completed and node {node_id} is absent from membership; deleting {}",
                        pod_name(name, node_id)
                    ),
                    node_id,
                    "DeletingPod",
                    Some(&membership),
                    None,
                    None,
                    false,
                )
                .await?;
                pod_restart::delete_rollout_pod(kube, namespace, cluster, node_id).await?;
                return Ok(());
            }
            ForcedPodRestartLeaveDecision::Wait(reason) => {
                patch_image_rollout_status(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    "WaitingForcedPodRestartPreconditions",
                    format!(
                        "manualRestart=force-on-timeout cleanup for node {node_id} completed; waiting to safely remove its membership: {reason}"
                    ),
                    node_id,
                    "ForceFinalized",
                    Some(&membership),
                    None,
                    Some(&reason),
                    false,
                )
                .await?;
                return Ok(());
            }
            ForcedPodRestartLeaveDecision::RequestLeave => {}
        }
        if let Err(error) = request_node_leave(namespace, cluster, node_id).await {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "RollingRestartBlocked",
                format!(
                    "manualRestart=force-on-timeout cleanup for node {node_id} completed, but controlled membership removal failed: {error}"
                ),
                node_id,
                "Blocked",
                Some(&membership),
                None,
                Some(&error.to_string()),
                true,
            )
            .await?;
            return Ok(());
        }
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            "RestartingBroker",
            format!(
                "manualRestart=force-on-timeout cleanup for node {node_id} completed; waiting for controlled membership removal"
            ),
            node_id,
            "WaitingMembershipRemoval",
            Some(&membership),
            None,
            None,
            false,
        )
        .await?;
        return Ok(());
    }
    if operation.phase == "Blocked" {
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(&cluster.spec, "ImageRolloutBlocked", "RollingRestartBlocked"),
            operation
                .last_error
                .clone()
                .unwrap_or_else(|| format!("{action} remains blocked while updating node {node_id}")),
            node_id,
            "Blocked",
            Some(&membership),
            None,
            operation.last_error.as_deref(),
            false,
        )
        .await?;
        return Ok(());
    }

    if matches!(
        operation.phase.as_str(),
        "RequestingLeave" | "WaitingMembershipRemoval" | "DeletingPod" | "WaitingPodRecreation"
    ) {
        if voter_exists(&membership, node_id)
            || membership.learners.contains(&node_id)
            || member_exists(&membership.business_peers, node_id)
        {
            if let Err(error) = request_node_leave(namespace, cluster, node_id).await {
                patch_image_rollout_status(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    rollout_phase(&cluster.spec, "ImageRolloutBlocked", "RollingRestartBlocked"),
                    format!("failed to resume leave for node {node_id}: {error}"),
                    node_id,
                    "Blocked",
                    Some(&membership),
                    None,
                    Some(&error.to_string()),
                    true,
                )
                .await?;
                return Ok(());
            }
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
                format!("waiting for node {node_id} to leave raft and business membership before recreation"),
                node_id,
                "WaitingMembershipRemoval",
                Some(&membership),
                None,
                None,
                false,
            )
            .await?;
            return Ok(());
        }
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
            format!(
                "node {node_id} left membership; deleting {} so StatefulSet recreates it for the {action}",
                pod_name(name, node_id)
            ),
            node_id,
            "DeletingPod",
            Some(&membership),
            None,
            None,
            false,
        )
        .await?;
        pod_restart::delete_rollout_pod(kube, namespace, cluster, node_id).await?;
        return Ok(());
    }

    if !membership_ready_for_replicas(&membership, current_replicas) {
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(
                &cluster.spec,
                "WaitingImageRolloutPreconditions",
                "WaitingRollingRestartPreconditions",
            ),
            format!("raft membership is not fully healthy; refusing to drain node {node_id} for {action}"),
            node_id,
            "PreparingNode",
            Some(&membership),
            None,
            None,
            false,
        )
        .await?;
        return Ok(());
    }
    if membership.leader_id == Some(node_id) {
        let retrying = operation.phase == "TransferringLeader";
        if retrying && !leader_transfer_retry_due(operation, Utc::now()) {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "TransferringRaftLeader",
                format!(
                    "waiting for node {node_id} to hand leadership to another caught-up voter; the request will be retried if the leader does not change within {LEADER_TRANSFER_RETRY_SECONDS} seconds"
                ),
                node_id,
                "TransferringLeader",
                Some(&membership),
                None,
                operation.last_error.as_deref(),
                false,
            )
            .await?;
            return Ok(());
        }

        let transfer_target = if retrying {
            rollout_leadership_target_for_retry(
                &membership,
                &pods,
                name,
                &cluster.spec,
                node_id,
                operation.leader_transfer_retry_count,
            )
        } else {
            rollout_leadership_target(&membership, &pods, name, &cluster.spec, node_id)
        }
        .ok_or_else(|| {
            anyhow!("no already-updated caught-up voter is available for {action} leadership transfer")
        })?;
        let transfer_result = rmqtt_action_value_with_payload(
            namespace,
            cluster,
            node_id,
            json!({"action": "transfer_leader", "id": transfer_target}),
        )
        .await;
        let transfer_error = transfer_result.as_ref().err().map(ToString::to_string);
        let message = if let Some(error) = transfer_error.as_deref() {
            format!(
                "leadership transfer request from node {node_id} to caught-up voter {transfer_target} failed and will retry: {error}"
            )
        } else if retrying {
            format!(
                "retried leadership transfer from node {node_id} to caught-up voter {transfer_target} because the previous request did not change the leader"
            )
        } else {
            format!(
                "requested leadership transfer from node {node_id} to updated caught-up voter {transfer_target} before {action}"
            )
        };
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            "TransferringRaftLeader",
            message,
            node_id,
            "TransferringLeader",
            Some(&membership),
            None,
            transfer_error.as_deref(),
            retrying,
        )
        .await?;
        return Ok(());
    }

    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        "QuiescingClientTraffic",
        format!(
            "preparing node {node_id} for safe replacement: quiescing new sessions, removing the broker from Service endpoints, and arming reliable message/session handoff"
        ),
        node_id,
        "Draining",
        Some(&membership),
        None,
        None,
        false,
    )
    .await?;
    if let Err(error) = prepare_node_for_drain(kube, namespace, cluster, current_replicas, node_id).await {
        rollback_image_rollout_drain(
            kube,
            namespace,
            cluster,
            current_replicas,
            node_id,
            Some(&membership),
            &error.to_string(),
        )
        .await?;
        return Ok(());
    }
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "DrainingForImageUpdate", "DrainingForRestart"),
        format!(
            "node {node_id} client cutover and proactive session relocation have started; waiting for messages, sessions, inflights, handoff, and routing barriers"
        ),
        node_id,
        "WaitingDrainStable",
        Some(&membership),
        None,
        None,
        false,
    )
    .await?;

    let mut forced_drain_description = None;
    match wait_node_drain_barriers(kube, namespace, cluster, current_replicas, node_id, true).await {
        Ok(NodeDrainOutcome::Ready) => {}
        Ok(NodeDrainOutcome::Preempted(reason)) => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                "ExternalTerminationPending",
                reason,
                node_id,
                "Preempted",
                Some(&membership),
                None,
                None,
                false,
            )
            .await?;
            return Ok(());
        }
        Ok(NodeDrainOutcome::Unsafe(blockers)) => {
            rollback_image_rollout_drain(
                kube,
                namespace,
                cluster,
                current_replicas,
                node_id,
                Some(&membership),
                &format!("handoff safety threshold was crossed before pod deletion; {blockers}"),
            )
            .await?;
            return Ok(());
        }
        Ok(NodeDrainOutcome::TimedOut(blockers)) => {
            if cluster.spec.raft.drain_timeout_action == DrainTimeoutAction::ForceContinue {
                if cluster.spec.pod_restart.is_some() {
                    if let Err(error) =
                        ensure_force_drain_finalize_supported(namespace, cluster, current_replicas).await
                    {
                        rollback_image_rollout_drain(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            node_id,
                            Some(&membership),
                            &format!(
                                "manualRestart=force-on-timeout capability check failed before destructive cleanup: {error}"
                            ),
                        )
                        .await?;
                        return Ok(());
                    }
                    // Persist intent before the first destructive request. A
                    // controller restart resumes idempotent force finalization
                    // instead of returning to the rollback path.
                    patch_image_rollout_status(
                        kube,
                        namespace,
                        cluster,
                        current_replicas,
                        "ForceFinalizingPodRestart",
                        format!(
                            "node {node_id} drain exceeded {} seconds; manualRestart=force-on-timeout cleanup is authorized",
                            cluster.spec.raft.drain_timeout_seconds
                        ),
                        node_id,
                        "ForceFinalizing",
                        Some(&membership),
                        None,
                        Some(&blockers),
                        false,
                    )
                    .await?;
                    return Ok(());
                }
                match force_finalize_node_drain(
                    namespace,
                    cluster,
                    current_replicas,
                    node_id,
                    "drain_timeout",
                    &blockers,
                )
                .await
                {
                    Ok(summary) => {
                        record_forced_drain_status(kube, namespace, cluster, &summary).await?;
                        forced_drain_description = Some(summary.description());
                    }
                    Err(error) => {
                        rollback_image_rollout_drain(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            node_id,
                            Some(&membership),
                            &format!("forced drain finalization failed after timeout: {error}"),
                        )
                        .await?;
                        return Ok(());
                    }
                }
            } else {
                rollback_image_rollout_drain(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    node_id,
                    Some(&membership),
                    &format!(
                        "drain exceeded {} seconds without convergence; {blockers}",
                        cluster.spec.raft.drain_timeout_seconds
                    ),
                )
                .await?;
                return Ok(());
            }
        }
        Err(error) => {
            rollback_image_rollout_drain(
                kube,
                namespace,
                cluster,
                current_replicas,
                node_id,
                Some(&membership),
                &error.to_string(),
            )
            .await?;
            return Ok(());
        }
    }

    let leave_message = forced_drain_description.unwrap_or_else(|| {
        format!("node {node_id} drain barriers are stable; requesting graceful leave before recreation")
    });
    if cluster.spec.pod_restart.is_some() {
        // Draining may take minutes. A leader change or a failed sibling during
        // that interval must prevent this explicit graceful deletion request.
        let blocker = match leader_membership(kube, namespace, cluster, current_replicas).await {
            Ok(fresh) => scale_in_removal_blocker(&fresh, current_replicas, node_id),
            Err(error) => Some(format!("cannot verify membership immediately before leave: {error}")),
        };
        if let Some(reason) = blocker {
            rollback_image_rollout_drain(
                kube,
                namespace,
                cluster,
                current_replicas,
                node_id,
                Some(&membership),
                &reason,
            )
            .await?;
            return Ok(());
        }
    }
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
        leave_message,
        node_id,
        "RequestingLeave",
        Some(&membership),
        None,
        None,
        false,
    )
    .await?;
    if !pod_restart::original_still_present(kube, namespace, cluster).await? {
        return Ok(());
    }
    if let Err(error) = request_node_leave(namespace, cluster, node_id).await {
        patch_image_rollout_status(
            kube,
            namespace,
            cluster,
            current_replicas,
            rollout_phase(&cluster.spec, "ImageRolloutBlocked", "RollingRestartBlocked"),
            format!("node {node_id} failed to leave before {action}: {error}"),
            node_id,
            "Blocked",
            Some(&membership),
            None,
            Some(&error.to_string()),
            true,
        )
        .await?;
        return Ok(());
    }
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
        format!("waiting for node {node_id} to disappear from raft and business membership"),
        node_id,
        "WaitingMembershipRemoval",
        Some(&membership),
        None,
        None,
        false,
    )
    .await?;
    let remaining = rollout_remaining_node_ids(current_replicas, node_id);
    let membership = match wait_node_removed_from_membership(namespace, cluster, &remaining, node_id).await {
        Ok(membership) => membership,
        Err(error) => {
            patch_image_rollout_status(
                kube,
                namespace,
                cluster,
                current_replicas,
                rollout_phase(&cluster.spec, "ImageRolloutBlocked", "RollingRestartBlocked"),
                error.to_string(),
                node_id,
                "Blocked",
                None,
                None,
                Some(&error.to_string()),
                true,
            )
            .await?;
            return Ok(());
        }
    };
    patch_image_rollout_status(
        kube,
        namespace,
        cluster,
        current_replicas,
        rollout_phase(&cluster.spec, "UpdatingBrokerImage", "RestartingBroker"),
        format!("node {node_id} left membership; deleting {} for {action}", pod_name(name, node_id)),
        node_id,
        "DeletingPod",
        Some(&membership),
        None,
        None,
        false,
    )
    .await?;
    pod_restart::delete_rollout_pod(kube, namespace, cluster, node_id).await
}

async fn continue_image_rollout(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    operation: &OperationStatus,
) -> Result<()> {
    if operation.node_id <= 0 {
        return prepare_next_image_rollout_node(kube, namespace, cluster, current_replicas).await;
    }
    let node_id = operation.node_id;
    let name = cluster.metadata.name.as_str();
    let pods = cluster_pods(kube, namespace, name).await?;
    let pod = pod_for_node(&pods, name, node_id);

    if !operator_managed_rollout_enabled(&cluster.spec) {
        let deletion_started = matches!(
            operation.phase.as_str(),
            "RequestingLeave"
                | "WaitingMembershipRemoval"
                | "DeletingPod"
                | "WaitingPodRecreation"
                | "WaitingLeader"
                | "CatchingUp"
                | "Promoting"
                | "WaitingBusinessReady"
                | "Stabilizing"
        ) || pod.is_none_or(|pod| pod_uses_rollout_target(pod, &cluster.spec));
        if !deletion_started {
            return cancel_image_rollout_before_delete(kube, namespace, cluster, current_replicas, node_id)
                .await;
        }
    }

    if pod.is_some_and(|pod| pod_uses_rollout_target(pod, &cluster.spec)) {
        return continue_image_rollout_recovery(
            kube,
            namespace,
            cluster,
            current_replicas,
            operation,
            node_id,
        )
        .await;
    }
    continue_image_rollout_before_recreation(kube, namespace, cluster, current_replicas, operation, node_id)
        .await
}

fn has_matching_scale_in_operation(cluster: &RmqttCluster, conf_change: &ConfChangeStatus) -> bool {
    cluster.status.as_ref().is_some_and(|status| {
        status.conf_change.as_ref().is_some_and(|existing| {
            existing.kind == conf_change.kind
                && existing.node_id == conf_change.node_id
                && existing.to_replicas == conf_change.to_replicas
        }) || status.operation.as_ref().is_some_and(|existing| {
            existing.kind == conf_change.kind
                && existing.node_id == conf_change.node_id
                && existing.to_replicas == conf_change.to_replicas
        })
    })
}

async fn finish_scale_in_step(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    conf_change: &ConfChangeStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let desired = conf_change.to_replicas;
    let victim_id = conf_change.node_id;
    let stateful_set_replicas = get_stateful_set(kube, namespace, name)
        .await?
        .map(|stateful_set| stateful_set.replicas())
        .unwrap_or(0);
    if stateful_set_replicas > desired {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScalingIn",
                "message": format!(
                    "node {victim_id} is absent from raft and business membership; scaling StatefulSet from {stateful_set_replicas} to {desired}"
                ),
                "replicas": stateful_set_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "drainingNodeId": victim_id,
                "operation": scale_in_operation_status_json(
                    cluster,
                    conf_change,
                    "ScalingStatefulSet",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        external_termination::release_after_planned_drain(
            kube,
            namespace,
            cluster,
            &pod_name(name, victim_id),
            None,
        )
        .await?;
        apply_stateful_set(kube, namespace, cluster, desired, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScalingIn",
                "message": format!(
                    "StatefulSet now targets {desired} replicas; waiting for node {victim_id} pod termination"
                ),
                "replicas": desired,
                "desiredReplicas": cluster.spec.replicas,
                "drainingNodeId": victim_id,
                "operation": scale_in_operation_status_json(
                    cluster,
                    conf_change,
                    "WaitingPodTermination",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
    }

    let ready_replicas = wait_ready_pod_count_at_most(kube, namespace, name, desired).await?;
    let membership = match leader_membership(kube, namespace, cluster, desired).await {
        Ok(membership) => membership,
        Err(error) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftMembers",
                    "message": format!(
                        "node {victim_id} was removed; waiting for an authoritative leader before continuing post-removal stabilization: {error}"
                    ),
                    "replicas": desired,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "drainingNodeId": null,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        "WaitingPostRemovalHealth",
                        Some(&error.to_string()),
                        false
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };
    if voter_exists(&membership, victim_id)
        || membership.learners.contains(&victim_id)
        || member_exists(&membership.business_peers, victim_id)
    {
        bail!("node {victim_id} reappeared in authoritative membership while finalizing scale-in");
    }

    let raft_ready = membership_ready_for_replicas(&membership, desired);
    if !raft_ready || ready_replicas != desired {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingRaftMembers",
                "message": format!(
                    "node {victim_id} was removed; waiting for the remaining {desired} replicas to become ready before starting the stability window"
                ),
                "replicas": desired,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "members": membership.business_peers.clone(),
                "podStatus": {
                    "replicas": desired,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, desired),
                "drainingNodeId": null,
                "operation": scale_in_operation_status_json(
                    cluster,
                    conf_change,
                    "WaitingPostRemovalHealth",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    ensure_ready_pods_serving(kube, namespace, name, conf_change.requested_replicas).await?;
    clear_reliable_handoff(namespace, cluster, desired, victim_id).await;
    let stabilization_seconds = cluster.spec.raft.scale_in_stabilization_seconds;
    let operation = cluster.status.as_ref().and_then(|status| status.operation.as_ref());
    let stable_since = operation
        .filter(|operation| {
            operation.kind == conf_change.kind
                && operation.node_id == conf_change.node_id
                && operation.phase == "Stabilizing"
        })
        .and_then(|operation| operation.stable_since.clone());
    if stable_since.is_none() {
        let stable_since = utc_now_rfc3339();
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "StabilizingScaleIn",
                "message": format!(
                    "node {victim_id} was removed; observing the remaining {desired} replicas for {stabilization_seconds} seconds before continuing scale-in"
                ),
                "replicas": desired,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "members": membership.business_peers.clone(),
                "podStatus": {
                    "replicas": desired,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, desired),
                "drainingNodeId": null,
                "operation": scale_in_stabilizing_operation_status_json(
                    cluster,
                    conf_change,
                    stable_since
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }
    if !operation.is_some_and(|operation| operation_stabilization_complete(operation, stabilization_seconds))
    {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "StabilizingScaleIn",
                "message": format!(
                    "the remaining {desired} replicas are healthy after removing node {victim_id}; waiting for the {stabilization_seconds}-second stability window"
                ),
                "replicas": desired,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "members": membership.business_peers.clone(),
                "podStatus": {
                    "replicas": desired,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, desired),
                "drainingNodeId": null,
                "operation": scale_in_stabilizing_operation_status_json(
                    cluster,
                    conf_change,
                    stable_since.expect("stable_since checked above")
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let phase = if desired == cluster.spec.replicas { "Running" } else { "ScalingIn" };
    let message = if desired == cluster.spec.replicas {
        Value::Null
    } else {
        json!(format!(
            "node {victim_id} passed the {stabilization_seconds}-second stability window; continuing toward desired replicas {}",
            cluster.spec.replicas
        ))
    };

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": phase,
            "message": message,
            "replicas": desired,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "members": membership.business_peers.clone(),
            "podStatus": {
                "replicas": desired,
                "readyReplicas": ready_replicas
            },
            "raftStatus": membership_status_json(&membership, desired),
            "drainingNodeId": null,
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn recover_scale_in_after_target_error(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
    failed_phase: &str,
    error: &str,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let victim_id = conf_change.node_id;
    log::warn!(
        "scale-in target request failed; verifying cluster facts namespace={} cluster={} node_id={} pod_name={} phase={} error={}",
        namespace,
        name,
        victim_id,
        pod_name(name, victim_id),
        failed_phase,
        error
    );

    let observation = match observe_scale_in_state(kube, namespace, cluster, current_replicas, victim_id)
        .await
    {
        Ok(observation) => observation,
        Err(observation_error) => {
            let combined_error =
                format!("{error}; unable to verify pod and authoritative membership: {observation_error}");
            patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "RecoveringScaleIn",
                        "message": format!(
                            "node {victim_id} request failed and current removal state cannot yet be verified; no destructive action will be taken"
                        ),
                        "replicas": current_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "drainingNodeId": victim_id,
                        "operation": scale_in_operation_status_json(
                            cluster,
                            conf_change,
                            "RetryingObservation",
                            Some(&combined_error),
                            true
                        ),
                        "confChange": conf_change_status_json(conf_change),
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
            return Ok(());
        }
    };

    let decision = scale_in_recovery_decision(
        observation.pod_state,
        observation.raft_member,
        observation.business_member,
    );
    log::warn!(
        "scale-in facts after target request failure namespace={} cluster={} node_id={} pod_name={} pod_state={} stateful_set_replicas={} raft_member={} business_member={} decision={:?}",
        namespace,
        name,
        victim_id,
        pod_name(name, victim_id),
        observation.pod_state,
        observation.stateful_set_replicas,
        observation.raft_member,
        observation.business_member,
        decision
    );

    match decision {
        ScaleInRecoveryDecision::RetryTargetRequest => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "ScalingIn",
                    "message": format!(
                        "node {victim_id} is still present in Kubernetes and membership; retrying the scale-in observation without deleting the pod"
                    ),
                    "replicas": observation.stateful_set_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": observation.membership.leader_id,
                    "raftStatus": membership_status_json(
                        &observation.membership,
                        observation.stateful_set_replicas
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        "Retrying",
                        Some(error),
                        true
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await
        }
        ScaleInRecoveryDecision::RemoveStaleRaftMember => {
            let leader_id = observation
                .membership
                .leader_id
                .ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
            let cleanup = try_rmqtt_action_value_with_payload(
                namespace,
                cluster,
                leader_id,
                json!({"action": "remove_voter", "id": victim_id}),
            )
            .await;
            let (phase, last_error) = match cleanup {
                Ok(_) => ("WaitingMembershipRemoval", None),
                Err(cleanup_error) => ("RetryingMembershipRemoval", Some(cleanup_error.to_string())),
            };
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RecoveringScaleIn",
                    "message": format!(
                        "node {victim_id} pod is {}; cleaning stale raft membership through leader {leader_id}",
                        observation.pod_state
                    ),
                    "replicas": observation.stateful_set_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": leader_id,
                    "raftStatus": membership_status_json(
                        &observation.membership,
                        conf_change.to_replicas
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        phase,
                        last_error.as_deref(),
                        last_error.is_some()
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await
        }
        ScaleInRecoveryDecision::RemoveStaleBusinessMember => {
            let leader_id = observation
                .membership
                .leader_id
                .ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
            let cleanup = try_rmqtt_action_value_with_payload(
                namespace,
                cluster,
                leader_id,
                json!({"action": "force_node_remove", "id": victim_id}),
            )
            .await;
            let (phase, last_error) = match cleanup {
                Ok(_) => ("WaitingMembershipRemoval", None),
                Err(cleanup_error) => ("RetryingMembershipRemoval", Some(cleanup_error.to_string())),
            };
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RecoveringScaleIn",
                    "message": format!(
                        "node {victim_id} pod and raft membership are absent; cleaning stale business membership through leader {leader_id}"
                    ),
                    "replicas": observation.stateful_set_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": leader_id,
                    "raftStatus": membership_status_json(
                        &observation.membership,
                        conf_change.to_replicas
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        phase,
                        last_error.as_deref(),
                        last_error.is_some()
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await
        }
        ScaleInRecoveryDecision::FinishScaleIn => {
            finish_scale_in_step(kube, namespace, cluster, conf_change).await
        }
    }
}

async fn block_scale_in_after_non_transient_target_error(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
    failed_phase: &str,
    error: &RmqttActionError,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let victim_id = conf_change.node_id;
    log::warn!(
        "scale-in target request returned a non-transient error namespace={} cluster={} node_id={} pod_name={} phase={} error={}",
        namespace,
        name,
        victim_id,
        pod_name(name, victim_id),
        failed_phase,
        error
    );
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScaleInBlocked",
            "message": format!(
                "node {victim_id} returned a non-transient error during {failed_phase}; no pod or membership removal was attempted"
            ),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "operation": scale_in_operation_status_json(
                cluster,
                conf_change,
                "Blocked",
                Some(&error.to_string()),
                true
            ),
            "confChange": conf_change_status_json(conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn rollback_scale_in_drain(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
    reason: &str,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let victim_id = conf_change.node_id;
    let rollback =
        restore_node_after_failed_drain(kube, namespace, cluster, current_replicas, victim_id).await;
    let rollback: Result<()> = match rollback {
        Ok(None) => Ok(()),
        Ok(Some(clear_error)) => Err(anyhow!("handoff cleanup still needs retry: {clear_error}")),
        Err(error) => Err(error),
    };
    let (operation_phase, draining_node_id, message, last_error) = match rollback {
        Ok(()) => (
            "RolledBack",
            Value::Null,
            format!("scale-in drain was rolled back and node {victim_id} was restored to Service: {reason}"),
            reason.to_string(),
        ),
        Err(error) => {
            let combined = format!("{reason}; rollbackError={error}");
            (
                "RollingBackDrain",
                json!(victim_id),
                format!(
                    "scale-in safety barrier failed for node {victim_id}; no leave will be issued while the incomplete rollback is retried: {combined}"
                ),
                combined,
            )
        }
    };
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScaleInBlocked",
            "message": message,
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": draining_node_id,
            "operation": scale_in_operation_status_json(
                cluster,
                conf_change,
                operation_phase,
                Some(&last_error),
                operation_phase == "RollingBackDrain"
            ),
            "confChange": conf_change_status_json(conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn scale_in_one(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    target_replicas: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let desired = target_replicas;
    let victim_id = current_replicas;
    let conf_change = ConfChangeStatus {
        kind: "RemoveVoter".to_string(),
        node_id: victim_id,
        from_replicas: current_replicas,
        to_replicas: desired,
        requested_replicas: cluster.spec.replicas,
    };
    let existing_operation =
        cluster.status.as_ref().and_then(|status| status.operation.as_ref()).filter(|operation| {
            operation.kind == conf_change.kind
                && operation.node_id == conf_change.node_id
                && operation.to_replicas == conf_change.to_replicas
        });
    let drain_operation = existing_operation
        .and_then(OperationStatus::scale_in_drain_operation)
        .unwrap_or_else(|| DrainOperation::scale_in(current_replicas, conf_change.requested_replicas));
    if existing_operation.is_some_and(|operation| operation.phase == "RollingBackDrain") {
        rollback_scale_in_drain(
            kube,
            namespace,
            cluster,
            current_replicas,
            &conf_change,
            existing_operation
                .and_then(|operation| operation.last_error.as_deref())
                .unwrap_or("scale-in drain rollback is pending"),
        )
        .await?;
        return Ok(());
    }
    if existing_operation.is_some_and(|operation| operation.phase == "RolledBack") {
        return Ok(());
    }
    if has_matching_scale_in_operation(cluster, &conf_change) {
        match observe_scale_in_state(kube, namespace, cluster, current_replicas, victim_id).await {
            Ok(observation)
                if scale_in_recovery_decision(
                    observation.pod_state,
                    observation.raft_member,
                    observation.business_member,
                ) != ScaleInRecoveryDecision::RetryTargetRequest =>
            {
                recover_scale_in_after_target_error(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    "Resuming",
                    "resuming an in-flight scale-in from authoritative cluster facts",
                )
                .await?;
                return Ok(());
            }
            Err(error) => {
                recover_scale_in_after_target_error(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    "Resuming",
                    &format!("failed to observe an in-flight scale-in: {error}"),
                )
                .await?;
                return Ok(());
            }
            Ok(_) => {}
        }
    }
    let membership = leader_membership(kube, namespace, cluster, current_replicas).await?;
    if !membership_ready_for_replicas(&membership, current_replicas) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScaleInBlocked",
                "message": format!("raft membership is not ready for current replicas {current_replicas}; refusing to remove node {victim_id}"),
                "replicas": current_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "Blocked",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }
    if !removal_preserves_recent_quorum(&membership, victim_id) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScaleInBlocked",
                "message": format!(
                    "removing node {victim_id} would leave no recently-active raft quorum"
                ),
                "replicas": current_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "Blocked",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScalingIn",
            "message": format!("draining node {victim_id}"),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, current_replicas),
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "Draining",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;

    if !cluster.spec.raft.allow_leader_scale_in && membership.leader_id == Some(victim_id) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScaleInBlocked",
                "message": format!("node {victim_id} is raft leader; leader scale-in is disabled"),
                "leaderId": victim_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "Blocked",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if membership.leader_id == Some(victim_id) {
        let transfer_target = membership
            .voters
            .iter()
            .copied()
            .filter(|id| *id != victim_id)
            .filter(|id| raft_peer_caught_up(&membership, *id))
            .min()
            .ok_or_else(|| anyhow!("no caught-up voter is available for leadership transfer"))?;
        rmqtt_action_value_with_payload(
            namespace,
            cluster,
            victim_id,
            json!({"action": "transfer_leader", "id": transfer_target}),
        )
        .await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "TransferringRaftLeader",
                "message": format!("requested leadership transfer from node {victim_id} to caught-up voter {transfer_target} before scale-in"),
                "replicas": current_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "drainingNodeId": victim_id,
                "leaderId": victim_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "TransferringLeader",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "QuiescingClientTraffic",
            "message": format!(
                "preparing node {victim_id} for safe removal: quiescing new sessions, removing the broker from Service endpoints, and arming reliable message/session handoff"
            ),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "Draining",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    // Scale-in is strictly serial: only the current victim may leave the
    // client Service. Future victims keep serving until their own drain step.
    if let Err(error) = prepare_scale_in_node_for_drain(
        kube,
        namespace,
        cluster,
        current_replicas,
        victim_id,
        &drain_operation,
    )
    .await
    {
        rollback_scale_in_drain(kube, namespace, cluster, current_replicas, &conf_change, &error.to_string())
            .await?;
        return Ok(());
    }
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScalingIn",
            "message": format!(
                "node {victim_id} client cutover and proactive session relocation have started; waiting for message, session handoff, and routing barriers to remain stable"
            ),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, current_replicas),
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "WaitingDrainStable",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;

    let mut forced_drain_description = None;
    match wait_node_drain_barriers(kube, namespace, cluster, current_replicas, victim_id, true).await {
        Ok(NodeDrainOutcome::Ready) => {}
        Ok(NodeDrainOutcome::Preempted(reason)) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "ExternalTerminationPending",
                    "message": reason,
                    "replicas": current_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        &conf_change,
                        "Preempted",
                        None,
                        false
                    ),
                    "confChange": conf_change_status_json(&conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        Ok(NodeDrainOutcome::Unsafe(blockers)) => {
            rollback_scale_in_drain(
                kube,
                namespace,
                cluster,
                current_replicas,
                &conf_change,
                &format!("drain safety threshold was crossed before node removal; {blockers}"),
            )
            .await?;
            return Ok(());
        }
        Ok(NodeDrainOutcome::TimedOut(blockers)) => {
            if cluster.spec.raft.drain_timeout_action == DrainTimeoutAction::ForceContinue {
                match force_finalize_node_drain(
                    namespace,
                    cluster,
                    current_replicas,
                    victim_id,
                    "drain_timeout",
                    &blockers,
                )
                .await
                {
                    Ok(summary) => {
                        record_forced_drain_status(kube, namespace, cluster, &summary).await?;
                        forced_drain_description = Some(summary.description());
                    }
                    Err(error) => {
                        rollback_scale_in_drain(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            &conf_change,
                            &format!("forced drain finalization failed after timeout: {error}"),
                        )
                        .await?;
                        return Ok(());
                    }
                }
            } else {
                rollback_scale_in_drain(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    &format!(
                        "drain exceeded {} seconds without convergence; {blockers}",
                        cluster.spec.raft.drain_timeout_seconds
                    ),
                )
                .await?;
                return Ok(());
            }
        }
        Err(error) => {
            rollback_scale_in_drain(
                kube,
                namespace,
                cluster,
                current_replicas,
                &conf_change,
                &error.to_string(),
            )
            .await?;
            return Ok(());
        }
    }

    // The drain can take minutes under load, so the membership snapshot taken
    // before it started is no longer safe to use for a destructive leave. In
    // particular, leadership or peer availability may have changed while
    // sessions were being relocated.
    let membership = match leader_membership(kube, namespace, cluster, current_replicas).await {
        Ok(membership) => membership,
        Err(error) => {
            rollback_scale_in_drain(
                kube,
                namespace,
                cluster,
                current_replicas,
                &conf_change,
                &format!(
                    "could not refresh authoritative membership immediately before removing node {victim_id}: {error}"
                ),
            )
            .await?;
            return Ok(());
        }
    };
    if let Some(blocker) = scale_in_removal_blocker(&membership, current_replicas, victim_id) {
        rollback_scale_in_drain(
            kube,
            namespace,
            cluster,
            current_replicas,
            &conf_change,
            &format!("membership changed while node {victim_id} was draining; {blocker}"),
        )
        .await?;
        return Ok(());
    }

    let leave_message = forced_drain_description.unwrap_or_else(|| {
        format!("node {victim_id} drain barriers are stable; requesting raft and business membership removal")
    });
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScalingIn",
            "message": leave_message,
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, current_replicas),
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "RequestingLeave",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    if let Err(error) = request_node_leave(namespace, cluster, victim_id).await {
        if !error.is_transient() {
            block_scale_in_after_non_transient_target_error(
                kube,
                namespace,
                cluster,
                current_replicas,
                &conf_change,
                "RequestingLeave",
                &error,
            )
            .await?;
        } else {
            recover_scale_in_after_target_error(
                kube,
                namespace,
                cluster,
                current_replicas,
                &conf_change,
                "RequestingLeave",
                &error.to_string(),
            )
            .await?;
        }
        return Ok(());
    }

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScalingIn",
            "message": format!("waiting for node {victim_id} to disappear from raft and business membership"),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "WaitingMembershipRemoval",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    let remaining = (1..=desired).collect::<Vec<_>>();
    if let Err(error) = wait_node_removed_from_membership(namespace, cluster, &remaining, victim_id).await {
        recover_scale_in_after_target_error(
            kube,
            namespace,
            cluster,
            current_replicas,
            &conf_change,
            "WaitingMembershipRemoval",
            &error.to_string(),
        )
        .await?;
        return Ok(());
    }
    finish_scale_in_step(kube, namespace, cluster, &conf_change).await
}

async fn wait_ready_pod_count_at_most(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    replicas: i32,
) -> Result<i32> {
    for _ in 0..30 {
        let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
        if ready_replicas <= replicas {
            return Ok(ready_replicas);
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(ready_pods(kube, namespace, name).await?.len() as i32)
}

async fn get_stateful_set(kube: &KubeClient, namespace: &str, name: &str) -> Result<Option<StatefulSet>> {
    let path = format!("/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}");
    match kube.get_json::<StatefulSet>(&path).await {
        Ok(sts) => Ok(Some(sts)),
        Err(e) if e.to_string().contains("404") => Ok(None),
        Err(e) => Err(e),
    }
}

async fn ready_pods(kube: &KubeClient, namespace: &str, name: &str) -> Result<Vec<Pod>> {
    Ok(cluster_pods(kube, namespace, name).await?.into_iter().filter(pod_is_ready).collect())
}

async fn cluster_pods(kube: &KubeClient, namespace: &str, name: &str) -> Result<Vec<Pod>> {
    let selector = format!("app.kubernetes.io/instance={name}");
    let path = format!("/api/v1/namespaces/{namespace}/pods?labelSelector={selector}");
    let pods: PodList = kube.get_json(&path).await?;
    Ok(pods.items)
}

fn pod_is_ready(pod: &Pod) -> bool {
    pod.status.phase.as_deref() == Some("Running")
        && pod.status.conditions.iter().any(|c| c.kind == "Ready" && c.status == "True")
}

fn pod_rmqtt_container(pod: &Pod) -> Option<&PodContainerSpec> {
    pod.spec.containers.iter().find(|container| container.name == "rmqtt")
}

fn pod_uses_target_image(pod: &Pod, spec: &ClusterSpec) -> bool {
    pod_rmqtt_container(pod).is_some_and(|container| {
        container.image == spec.image && container.image_pull_policy == spec.image_pull_policy
    })
}

fn pod_uses_rollout_target(pod: &Pod, spec: &ClusterSpec) -> bool {
    if let Some(target) = spec.pod_restart.as_ref() {
        return target.is_replacement(pod);
    }
    pod_uses_target_image(pod, spec)
        && spec.rollout_restart_at.as_ref().is_none_or(|target| {
            pod.metadata.annotations.get(KUBECTL_RESTARTED_AT_ANNOTATION) == Some(target)
        })
}

fn pod_for_node<'a>(pods: &'a [Pod], cluster_name: &str, node_id: i32) -> Option<&'a Pod> {
    let expected_name = pod_name(cluster_name, node_id);
    pods.iter().find(|pod| pod.metadata.name == expected_name)
}

fn outdated_pod_node_ids(pods: &[Pod], cluster_name: &str, spec: &ClusterSpec, replicas: i32) -> Vec<i32> {
    (1..=replicas)
        .filter(|node_id| {
            pod_for_node(pods, cluster_name, *node_id).is_none_or(|pod| !pod_uses_rollout_target(pod, spec))
        })
        .collect()
}

fn updated_pod_count(pods: &[Pod], cluster_name: &str, spec: &ClusterSpec, replicas: i32) -> i32 {
    (1..=replicas)
        .filter(|node_id| {
            pod_for_node(pods, cluster_name, *node_id).is_some_and(|pod| pod_uses_rollout_target(pod, spec))
        })
        .count() as i32
}

fn is_authoritative_leader(membership: &MembershipStatus) -> bool {
    membership.id > 0 && membership.is_leader && membership.leader_id == Some(membership.id)
}

fn is_available_authoritative_leader(membership: &MembershipStatus) -> bool {
    is_authoritative_leader(membership) && membership.started && membership.available
}

#[derive(Debug, Clone)]
struct LocalMembershipObservation {
    node_id: i32,
    membership: MembershipStatus,
}

#[derive(Debug, Default)]
struct LocalMembershipScan {
    observations: Vec<LocalMembershipObservation>,
    unreachable_node_ids: Vec<i32>,
    failure_kinds: BTreeMap<String, Vec<i32>>,
    failures: BTreeMap<i32, String>,
}

struct RecoveryScanLogEntry {
    signature: Value,
    logged_at: Instant,
}

async fn local_membership_observations(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
) -> LocalMembershipScan {
    let mut tasks = tokio::task::JoinSet::new();
    for node_id in 1..=replicas {
        let namespace = namespace.to_string();
        let cluster = cluster.clone();
        tasks.spawn(async move {
            let result =
                try_rmqtt_action::<MembershipStatus>(&namespace, &cluster, node_id, "membership").await;
            (node_id, result)
        });
    }

    let mut scan = LocalMembershipScan::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((node_id, Ok(membership))) => {
                scan.observations.push(LocalMembershipObservation { node_id, membership });
            }
            Ok((node_id, Err(error))) => {
                scan.unreachable_node_ids.push(node_id);
                let kind = error.kind.label();
                scan.failure_kinds.entry(kind.to_string()).or_default().push(node_id);
                scan.failures.insert(node_id, error.to_string());
                let target_pod = pod_name(&cluster.metadata.name, node_id);
                if error.is_transient() {
                    log::debug!(
                        "RMQTT membership query failed and will be included in the recovery scan summary node_id={node_id} pod_name={target_pod} kind={kind} error={error}"
                    );
                } else {
                    log::warn!(
                        "non-transient RMQTT membership query failure node_id={node_id} pod_name={target_pod} kind={kind} error={error}"
                    );
                }
            }
            Err(error) => log::warn!("local raft membership query task failed: {error}"),
        }
    }
    scan.observations.sort_by_key(|observation| observation.node_id);
    scan.unreachable_node_ids.sort_unstable();
    scan.unreachable_node_ids.dedup();
    for node_ids in scan.failure_kinds.values_mut() {
        node_ids.sort_unstable();
        node_ids.dedup();
    }
    scan
}

fn recovery_old_voters(observations: &[LocalMembershipObservation], replicas: i32) -> Vec<i32> {
    let mut voters = observations
        .iter()
        .filter(|observation| observation_has_recoverable_raft_state(observation))
        .max_by_key(|observation| {
            (
                observation.membership.applied_index,
                observation.membership.committed_index,
                observation.membership.voters.len(),
            )
        })
        .map(|observation| observation.membership.voters.clone())
        .unwrap_or_else(|| (1..=replicas).collect());
    if voters.is_empty() {
        voters = (1..=replicas).collect();
    }
    voters.sort_unstable();
    voters.dedup();
    voters
}

fn observation_has_recoverable_raft_state(observation: &LocalMembershipObservation) -> bool {
    let membership = &observation.membership;
    membership.id > 0
        && membership.id == observation.node_id
        && membership.snapshot_restored
        && !membership.voters.is_empty()
}

fn recoverable_voter_count(observations: &[LocalMembershipObservation], voters: &[i32]) -> usize {
    observations
        .iter()
        .filter(|observation| {
            voters.contains(&observation.membership.id) && observation_has_recoverable_raft_state(observation)
        })
        .map(|observation| observation.membership.id)
        .collect::<std::collections::HashSet<_>>()
        .len()
}

fn recovery_survivor_node_ids(observations: &[LocalMembershipObservation]) -> Vec<i32> {
    observations
        .iter()
        .filter(|observation| observation_has_recoverable_raft_state(observation))
        .map(|observation| observation.node_id)
        .collect()
}

fn recovery_rejected_node_ids(observations: &[LocalMembershipObservation]) -> Vec<i32> {
    observations
        .iter()
        .filter(|observation| !observation_has_recoverable_raft_state(observation))
        .map(|observation| observation.node_id)
        .collect()
}

fn recovery_node_locally_healthy(
    observations: &[LocalMembershipObservation],
    node_id: i32,
    leader_id: i32,
) -> bool {
    observations.iter().any(|observation| {
        let membership = &observation.membership;
        observation.node_id == node_id && recovery_membership_locally_healthy(membership, node_id, leader_id)
    })
}

fn recovery_membership_locally_healthy(membership: &MembershipStatus, node_id: i32, leader_id: i32) -> bool {
    membership.id == node_id
        && membership.leader_id == Some(leader_id)
        && membership.started
        && membership.available
        && membership.caught_up
        && membership.snapshot_restored
        && !membership.draining
        && membership.voters.contains(&node_id)
        && member_exists(&membership.business_peers, node_id)
}

fn recovery_node_healthy(
    pods: &[Pod],
    cluster_name: &str,
    observations: &[LocalMembershipObservation],
    node_id: i32,
    leader_id: i32,
) -> bool {
    pod_for_node(pods, cluster_name, node_id).is_some_and(pod_is_ready)
        && recovery_node_locally_healthy(observations, node_id, leader_id)
}

fn unhealthy_rebuilt_node_ids(
    recovery: &QuorumRecoveryStatus,
    pods: &[Pod],
    cluster_name: &str,
    observations: &[LocalMembershipObservation],
    leader_id: i32,
) -> Vec<i32> {
    recovery
        .restarted_node_ids
        .iter()
        .copied()
        .filter(|node_id| {
            Some(*node_id) != recovery.seed_node_id
                && !recovery_node_healthy(pods, cluster_name, observations, *node_id, leader_id)
        })
        .collect()
}

fn missing_recovery_target_blockers(
    pods: &[Pod],
    cluster_name: &str,
    target_node_id: i32,
) -> Option<Vec<i32>> {
    if pod_for_node(pods, cluster_name, target_node_id)
        .is_some_and(|pod| pod.metadata.deletion_timestamp.is_none())
    {
        return None;
    }
    Some(
        (1..target_node_id)
            .filter(|node_id| pod_for_node(pods, cluster_name, *node_id).is_none_or(|pod| !pod_is_ready(pod)))
            .collect(),
    )
}

fn recovery_node_stabilization_elapsed(recovery: &QuorumRecoveryStatus, node_id: i32) -> Option<u64> {
    if recovery.stabilizing_node_id != Some(node_id) {
        return None;
    }
    let started_at = recovery.stabilizing_started_at.as_deref()?;
    let started_at = DateTime::parse_from_rfc3339(started_at).ok()?;
    Some(Utc::now().signed_duration_since(started_at.with_timezone(&Utc)).num_seconds().max(0) as u64)
}

fn recovery_node_unhealthy_elapsed(recovery: &QuorumRecoveryStatus, node_id: i32) -> Option<u64> {
    if recovery.unhealthy_node_id != Some(node_id) {
        return None;
    }
    let started_at = recovery.unhealthy_started_at.as_deref()?;
    let started_at = DateTime::parse_from_rfc3339(started_at).ok()?;
    Some(Utc::now().signed_duration_since(started_at.with_timezone(&Utc)).num_seconds().max(0) as u64)
}

fn recovery_node_recycle_attempts(recovery: &QuorumRecoveryStatus, node_id: i32) -> u32 {
    recovery.recycle_attempts.get(&node_id.to_string()).copied().unwrap_or_default()
}

fn recovery_node_auto_recycle_candidate(pod_ready: bool, local_membership_healthy: bool) -> bool {
    !pod_ready && !local_membership_healthy
}

fn recovery_seed_membership_blocker(
    recovery: &QuorumRecoveryStatus,
    membership: &MembershipStatus,
) -> Option<String> {
    let Some(seed_node_id) = recovery.seed_node_id else {
        return Some("quorum recovery seed is missing".to_string());
    };
    if !is_available_authoritative_leader(membership) {
        return Some("the refreshed seed response is not an available authoritative leader".to_string());
    }
    if membership.id != seed_node_id {
        return Some(format!(
            "the refreshed leader is node {}, but recovery seed is node {seed_node_id}",
            membership.id
        ));
    }
    if membership.quorum_recovery_epoch != recovery.epoch {
        return Some(format!(
            "the refreshed recovery epoch is {}, but operator recovery epoch is {}",
            membership.quorum_recovery_epoch, recovery.epoch
        ));
    }
    None
}

async fn fresh_recovery_seed_membership(
    namespace: &str,
    cluster: &RmqttCluster,
    recovery: &QuorumRecoveryStatus,
) -> Result<MembershipStatus> {
    let seed_node_id = recovery.seed_node_id.ok_or_else(|| anyhow!("quorum recovery seed is missing"))?;
    let membership = rmqtt_action::<MembershipStatus>(namespace, cluster, seed_node_id, "membership").await?;
    if let Some(blocker) = recovery_seed_membership_blocker(recovery, &membership) {
        bail!("refreshed recovery seed validation failed: {blocker}");
    }
    Ok(membership)
}

async fn fresh_recovery_node_signals(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    expected_uid: &str,
    leader_id: i32,
) -> Result<Option<(bool, bool)>> {
    let pods = cluster_pods(kube, namespace, &cluster.metadata.name).await?;
    let Some(pod) = pod_for_node(&pods, &cluster.metadata.name, node_id) else {
        return Ok(None);
    };
    if pod.metadata.uid != expected_uid || pod.metadata.deletion_timestamp.is_some() {
        return Ok(None);
    }

    let pod_ready = pod_is_ready(pod);
    let payload = json!({"action": "membership"});
    let result = try_rmqtt_action_value_once(namespace, cluster, node_id, payload).await;
    let local_membership_healthy = match result {
        Ok(value) => serde_json::from_value::<MembershipStatus>(value)
            .ok()
            .is_some_and(|membership| recovery_membership_locally_healthy(&membership, node_id, leader_id)),
        Err(error) => {
            log::debug!(
                "fresh recovery node membership recheck failed node_id={} pod_name={} error_kind={} error={}",
                node_id,
                pod_name(&cluster.metadata.name, node_id),
                error.kind.label(),
                error
            );
            false
        }
    };
    Ok(Some((pod_ready, local_membership_healthy)))
}

fn recovery_node_recycle_blocker(
    recovery: &QuorumRecoveryStatus,
    membership: &MembershipStatus,
    node_id: i32,
) -> Option<String> {
    if recovery.seed_node_id == Some(node_id) {
        return Some(format!("node {node_id} is the active recovery seed"));
    }
    if membership.leader_id == Some(node_id) {
        return Some(format!("node {node_id} is the current raft leader"));
    }
    if !voter_exists(membership, node_id) {
        return Some(format!("node {node_id} is not a current voter"));
    }

    // Recycling does not first change Raft membership. The replacement joins
    // by removing its stale identity and adding the same ID back as a learner,
    // so the remaining live voters must satisfy the *current* configuration's
    // quorum while that happens. This is deliberately stricter than the
    // scale-in check, which reasons about the post-removal configuration.
    let quorum = membership.voters.len() / 2 + 1;
    let recently_active_remaining = membership
        .voters
        .iter()
        .copied()
        .filter(|candidate| *candidate != node_id)
        .filter(|candidate| {
            membership.leader_id == Some(*candidate)
                || membership.raft_peers.iter().any(|peer| {
                    peer.id == *candidate
                        && peer.available
                        && peer.recent_active
                        && peer.pending_snapshot == 0
                })
        })
        .count();
    if recently_active_remaining < quorum {
        return Some(format!(
            "only {recently_active_remaining} recently-active voters would remain, but the current {}-voter configuration still requires quorum {quorum}",
            membership.voters.len()
        ));
    }
    None
}

fn pod_delete_options(uid: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "preconditions": {
            "uid": uid,
        },
    })
}

fn select_recovery_seed(observations: &[LocalMembershipObservation]) -> Option<&LocalMembershipObservation> {
    observations.iter().filter(|observation| observation_has_recoverable_raft_state(observation)).max_by(
        |left, right| {
            (
                left.membership.snapshot_restored,
                left.membership.applied_index,
                left.membership.committed_index,
                left.membership.last_index,
                left.membership.caught_up,
                left.membership.client_states,
                std::cmp::Reverse(left.node_id),
            )
                .cmp(&(
                    right.membership.snapshot_restored,
                    right.membership.applied_index,
                    right.membership.committed_index,
                    right.membership.last_index,
                    right.membership.caught_up,
                    right.membership.client_states,
                    std::cmp::Reverse(right.node_id),
                ))
        },
    )
}

fn recovery_elapsed_seconds(recovery: &QuorumRecoveryStatus) -> i64 {
    let Some(started_at) = recovery.started_at.as_deref() else {
        return 0;
    };
    let Ok(started_at) = DateTime::parse_from_rfc3339(started_at) else {
        return 0;
    };
    Utc::now().signed_duration_since(started_at.with_timezone(&Utc)).num_seconds().max(0)
}

fn quorum_recovery_status_json(recovery: &QuorumRecoveryStatus) -> Value {
    json!({
        "epoch": recovery.epoch,
        "phase": recovery.phase,
        "startedAt": recovery.started_at,
        "seedNodeId": recovery.seed_node_id,
        "oldVoters": recovery.old_voters,
        "survivorNodeIds": recovery.survivor_node_ids,
        "rejectedNodeIds": recovery.rejected_node_ids,
        "unreachableNodeIds": recovery.unreachable_node_ids,
        "restartingNodeId": recovery.restarting_node_id,
        "restartedNodeIds": recovery.restarted_node_ids,
        "stabilizingNodeId": recovery.stabilizing_node_id,
        "stabilizingStartedAt": recovery.stabilizing_started_at,
        "unhealthyNodeId": recovery.unhealthy_node_id,
        "unhealthyStartedAt": recovery.unhealthy_started_at,
        "recyclingNodeId": recovery.recycling_node_id,
        "recyclingPodUid": recovery.recycling_pod_uid,
        "recycleAttempts": recovery.recycle_attempts,
        "lastError": recovery.last_error,
    })
}

fn recovery_scan_log_signature(
    recovery: &QuorumRecoveryStatus,
    scan: &LocalMembershipScan,
    ready_replicas: i32,
) -> Value {
    json!({
        "epoch": recovery.epoch,
        "phase": recovery.phase,
        "seedNodeId": recovery.seed_node_id,
        "restartingNodeId": recovery.restarting_node_id,
        "stabilizingNodeId": recovery.stabilizing_node_id,
        "unhealthyNodeId": recovery.unhealthy_node_id,
        "recyclingNodeId": recovery.recycling_node_id,
        "recycleAttempts": recovery.recycle_attempts,
        "readyReplicas": ready_replicas,
        "survivorNodeIds": recovery_survivor_node_ids(&scan.observations),
        "rejectedNodeIds": recovery_rejected_node_ids(&scan.observations),
        "unreachableNodeIds": scan.unreachable_node_ids,
        "failureKinds": scan.failure_kinds,
    })
}

fn recovery_scan_log_due(previous: Option<&RecoveryScanLogEntry>, signature: &Value, now: Instant) -> bool {
    previous.is_none_or(|entry| {
        entry.signature != *signature
            || now.saturating_duration_since(entry.logged_at)
                >= Duration::from_secs(RECOVERY_SCAN_LOG_HEARTBEAT_SECONDS)
    })
}

fn log_control_membership_scan(namespace: &str, name: &str, scan: &LocalMembershipScan, ready_replicas: i32) {
    let reachable_node_ids =
        scan.observations.iter().map(|observation| observation.node_id).collect::<Vec<_>>();
    let non_authoritative_node_ids = scan
        .observations
        .iter()
        .filter(|observation| !is_available_authoritative_leader(&observation.membership))
        .map(|observation| observation.node_id)
        .collect::<Vec<_>>();
    let signature = json!({
        "context": "leader_verification",
        "readyReplicas": ready_replicas,
        "reachableNodeIds": reachable_node_ids,
        "nonAuthoritativeNodeIds": non_authoritative_node_ids,
        "unreachableNodeIds": scan.unreachable_node_ids,
        "failureKinds": scan.failure_kinds,
    });
    let now = Instant::now();
    let cache = LAST_RECOVERY_SCAN_LOG.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{namespace}/{name}/control");
    let should_log = {
        let mut scans = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !recovery_scan_log_due(scans.get(&key), &signature, now) {
            false
        } else {
            scans.insert(key, RecoveryScanLogEntry { signature, logged_at: now });
            true
        }
    };
    if should_log {
        log::warn!(
            "event=RMQTT_MEMBERSHIP_SCAN context=leader_verification ready_replicas={} \
             reachable_node_ids={:?} non_authoritative_node_ids={:?} unreachable_node_ids={:?} \
             failure_kinds={:?} failures={:?} namespace={} cluster={}",
            ready_replicas,
            reachable_node_ids,
            non_authoritative_node_ids,
            scan.unreachable_node_ids,
            scan.failure_kinds,
            scan.failures,
            namespace,
            name,
        );
    }
}

fn log_recovery_membership_scan(
    namespace: &str,
    name: &str,
    recovery: &QuorumRecoveryStatus,
    scan: &LocalMembershipScan,
    ready_replicas: i32,
) {
    let signature = recovery_scan_log_signature(recovery, scan, ready_replicas);
    let now = Instant::now();
    let cache = LAST_RECOVERY_SCAN_LOG.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{namespace}/{name}/recovery");
    let should_log = {
        let mut scans = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !recovery_scan_log_due(scans.get(&key), &signature, now) {
            false
        } else {
            scans.insert(key, RecoveryScanLogEntry { signature, logged_at: now });
            true
        }
    };
    if !should_log {
        return;
    }

    let elapsed_seconds = recovery_elapsed_seconds(recovery) as u64;
    let wait_seconds = quorum_recovery_wait_seconds();
    let remaining_seconds = wait_seconds.saturating_sub(elapsed_seconds);
    let reachable_node_ids =
        scan.observations.iter().map(|observation| observation.node_id).collect::<Vec<_>>();
    let survivor_node_ids = recovery_survivor_node_ids(&scan.observations);
    let rejected_node_ids = recovery_rejected_node_ids(&scan.observations);
    let details = format!(
        "event=QUORUM_RECOVERY_MEMBERSHIP_SCAN epoch={} recovery_phase={} elapsed_seconds={} \
         wait_seconds={} remaining_seconds={} ready_replicas={} reachable_node_ids={:?} \
         survivor_node_ids={:?} rejected_node_ids={:?} unreachable_node_ids={:?} \
         failure_kinds={:?} failures={:?} namespace={} cluster={}",
        recovery.epoch,
        recovery.phase,
        elapsed_seconds,
        wait_seconds,
        remaining_seconds,
        ready_replicas,
        reachable_node_ids,
        survivor_node_ids,
        rejected_node_ids,
        scan.unreachable_node_ids,
        scan.failure_kinds,
        scan.failures,
        namespace,
        name,
    );
    if recovery.phase == "QuorumLost"
        || recovery.phase == "RecoveryBlocked"
        || !scan.unreachable_node_ids.is_empty()
        || !rejected_node_ids.is_empty()
    {
        log::warn!("{details}");
    } else {
        log::info!("{details}");
    }
}

async fn begin_quorum_recovery(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    leader_error: &str,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let pods = cluster_pods(kube, namespace, name).await?;
    let ready_replicas = pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32;
    let scan = local_membership_observations(namespace, cluster, current_replicas).await;
    let observations = &scan.observations;
    if let Some(leader) =
        observations.iter().find(|observation| is_available_authoritative_leader(&observation.membership))
    {
        apply_client_service(kube, namespace, cluster, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingRaftMembers",
                "message": "an authoritative raft leader appeared while quorum loss was being verified; destructive recovery was not started",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": leader.membership.leader_id,
                "raftStatus": membership_status_json(&leader.membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if ready_replicas > 0 {
        // A Ready Pod has independently passed the strict local readiness
        // check, including an available leader. Treat failure to query that
        // leader from the operator as a control-path problem, not proof of
        // quorum loss. In particular, do not alter client admission or arm a
        // destructive recovery from incomplete observations.
        log_control_membership_scan(namespace, name, &scan, ready_replicas);
        apply_client_service(kube, namespace, cluster, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingRaftLeader",
                "message": format!("{ready_replicas} Pod(s) still pass strict readiness, but the operator cannot query an available authoritative leader; retaining normal client admission: {leader_error}"),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let old_voters = recovery_old_voters(observations, current_replicas);
    let recoverable_voters = recoverable_voter_count(observations, &old_voters);
    let quorum = old_voters.len() / 2 + 1;
    let survivor_node_ids = recovery_survivor_node_ids(observations);
    let rejected_node_ids = recovery_rejected_node_ids(observations);
    if survivor_node_ids.is_empty() {
        apply_client_service(kube, namespace, cluster, false).await?;
    } else {
        ensure_degraded_survivor_admission(kube, namespace, cluster, current_replicas, observations).await?;
        // Kubernetes normally excludes NotReady Pods even when their explicit
        // serving label remains true. During a verified quorum-loss state only,
        // publish those locally validated survivors so the MQTT data plane can
        // keep using its preserved in-memory sessions and routes. This does not
        // weaken the Pod readiness gate used by rollout and membership control.
        apply_client_service(kube, namespace, cluster, true).await?;
    }

    let epoch = Utc::now().timestamp_millis() as u64;
    for observation in observations {
        let node_id = observation.node_id;
        if let Err(error) = rmqtt_action_value_with_payload(
            namespace,
            cluster,
            node_id,
            json!({
                "action": "prepare_quorum_recovery",
                "recoveryEpoch": epoch,
            }),
        )
        .await
        {
            log::warn!(
                "failed to prepare quorum recovery on survivor node_id={} pod_name={} epoch={} error={error:#}",
                node_id,
                pod_name(&cluster.metadata.name, node_id),
                epoch
            );
        }
    }

    let recovery_phase = if recoverable_voters >= quorum { "LeaderElectionPending" } else { "QuorumLost" };
    let recovery = QuorumRecoveryStatus {
        epoch,
        phase: recovery_phase.to_string(),
        started_at: Some(utc_now_rfc3339()),
        seed_node_id: None,
        old_voters,
        survivor_node_ids,
        rejected_node_ids,
        unreachable_node_ids: scan.unreachable_node_ids.clone(),
        restarting_node_id: None,
        restarted_node_ids: Vec::new(),
        stabilizing_node_id: None,
        stabilizing_started_at: None,
        unhealthy_node_id: None,
        unhealthy_started_at: None,
        recycling_node_id: None,
        recycling_pod_uid: None,
        recycle_attempts: BTreeMap::new(),
        last_error: Some(leader_error.to_string()),
    };
    log_recovery_membership_scan(namespace, name, &recovery, &scan, ready_replicas);
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": recovery_phase,
            "message": if recoverable_voters >= quorum {
                format!(
                    "no leader exists despite {recoverable_voters}/{} recoverable voters (quorum={quorum}); waiting {}s detection plus {}s natural recovery before selecting the best preserved seed",
                    recovery.old_voters.len(),
                    QUORUM_RECOVERY_DETECTION_SECONDS,
                    QUORUM_NATURAL_RECOVERY_SECONDS,
                )
            } else {
                format!(
                    "raft quorum is unavailable ({recoverable_voters}/{} recoverable voters, quorum={quorum}); waiting {}s detection plus {}s natural recovery before selecting the best preserved seed",
                    recovery.old_voters.len(),
                    QUORUM_RECOVERY_DETECTION_SECONDS,
                    QUORUM_NATURAL_RECOVERY_SECONDS,
                )
            },
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": null,
            "operation": null,
            "confChange": null,
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn continue_quorum_recovery(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    mut recovery: QuorumRecoveryStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let pods = cluster_pods(kube, namespace, name).await?;
    let ready_replicas = pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32;
    let scan = local_membership_observations(namespace, cluster, current_replicas).await;
    let observations = &scan.observations;
    recovery.survivor_node_ids = recovery_survivor_node_ids(observations);
    recovery.rejected_node_ids = recovery_rejected_node_ids(observations);
    recovery.unreachable_node_ids.clone_from(&scan.unreachable_node_ids);
    log_recovery_membership_scan(namespace, name, &recovery, &scan, ready_replicas);
    let leader =
        observations.iter().find(|observation| is_available_authoritative_leader(&observation.membership));
    if recovery.seed_node_id.is_none() && leader.is_none() && ready_replicas > 0 {
        recovery.phase = "LeaderElectionPending".to_string();
        recovery.started_at = Some(utc_now_rfc3339());
        recovery.last_error = Some(
            "strict-ready Pods exist but the operator cannot query an available authoritative leader"
                .to_string(),
        );
        apply_client_service(kube, namespace, cluster, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "LeaderElectionPending",
                "message": format!("{ready_replicas} Pod(s) still pass strict readiness; treating the missing leader response as a control-path failure and restarting the recovery detection window"),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }
    if recovery.seed_node_id.is_none() && !recovery.survivor_node_ids.is_empty() {
        ensure_degraded_survivor_admission(kube, namespace, cluster, current_replicas, observations).await?;
    }
    apply_client_service(kube, namespace, cluster, !recovery.survivor_node_ids.is_empty()).await?;

    if recovery.seed_node_id.is_some() {
        if let Some(leader) = leader {
            ensure_recovery_node_admission(
                kube,
                namespace,
                cluster,
                current_replicas,
                &pods,
                observations,
                leader.membership.id,
            )
            .await?;
        }
    }

    if let Some(conf_change) = cluster.status.as_ref().and_then(|status| status.conf_change.clone()) {
        if let Some(leader) = leader {
            return continue_quorum_recovery_conf_change(
                kube,
                namespace,
                cluster,
                current_replicas,
                recovery,
                &pods,
                observations,
                &leader.membership,
                conf_change,
            )
            .await;
        }
        // Without an authoritative seed leader the recovery state machine
        // below clears the stale operation and either waits or reselects a
        // survivor. Never continue a membership change without that leader.
    }

    if recovery.phase == "FreshBootstrapping" {
        let Some(leader) = leader else {
            recovery.last_error =
                Some("empty bootstrap node has not exposed an authoritative leader yet".to_string());
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "FreshBootstrapping",
                    "message": "waiting for the automatic empty-bootstrap node to become the recovery seed",
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": null,
                    "confChange": null,
                    "quorumRecovery": quorum_recovery_status_json(&recovery),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        };
        let seed_node_id = recovery.seed_node_id.unwrap_or(1);
        if leader.node_id != seed_node_id || !membership_ready_for_replicas(&leader.membership, 1) {
            recovery.phase = "RecoveryBlocked".to_string();
            recovery.last_error = Some(format!(
                "empty bootstrap returned unexpected seed state: expected node {seed_node_id}, observed node {} leaderId={:?}",
                leader.node_id, leader.membership.leader_id
            ));
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RecoveryBlocked",
                    "message": recovery.last_error,
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": null,
                    "confChange": null,
                    "quorumRecovery": quorum_recovery_status_json(&recovery),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        rmqtt_action_value_with_payload(
            namespace,
            cluster,
            seed_node_id,
            json!({
                "action": "prepare_quorum_recovery",
                "recoveryEpoch": recovery.epoch,
            }),
        )
        .await?;
        recovery.phase = "RebuildingQuorum".to_string();
        recovery.seed_node_id = Some(seed_node_id);
        recovery.restarted_node_ids = vec![seed_node_id];
        recovery.stabilizing_node_id = None;
        recovery.stabilizing_started_at = None;
        recovery.last_error = None;
        patch_pod_serving(kube, namespace, &pod_name(name, seed_node_id)).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "DegradedServing",
                "message": format!("empty bootstrap node {seed_node_id} is serving as the automatic recovery seed; rebuilding desired voters one at a time"),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": seed_node_id,
                "raftStatus": membership_status_json(&leader.membership, 1),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if recovery.phase == "RebuildingQuorum"
        || recovery.phase == "DegradedServing"
        || recovery.phase == "StabilizingRecoveryNode"
        || recovery.phase == "RecyclingRecoveryNode"
    {
        if let Some(leader) = leader {
            return rebuild_quorum_from_seed(
                kube,
                namespace,
                cluster,
                current_replicas,
                recovery,
                &leader.membership,
                &pods,
                observations,
            )
            .await;
        }
        recovery.last_error = Some("recovery seed has not exposed an authoritative leader yet".to_string());
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "EmergencyRecovering",
                "message": "the selected recovery seed is restarting its local raft runtime; client traffic remains limited to surviving service endpoints",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if let Some(leader) = leader {
        for observation in observations {
            let _ = rmqtt_action_value_with_payload(
                namespace,
                cluster,
                observation.node_id,
                json!({
                    "action": "cancel_quorum_recovery",
                    "recoveryEpoch": recovery.epoch,
                }),
            )
            .await;
        }
        apply_client_service(kube, namespace, cluster, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingRaftMembers",
                "message": "raft quorum and leader recovered naturally; emergency rebootstrap was cancelled",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": leader.membership.leader_id,
                "raftStatus": membership_status_json(&leader.membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let elapsed = recovery_elapsed_seconds(&recovery);
    let wait_seconds = quorum_recovery_wait_seconds();
    if elapsed < wait_seconds as i64 {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": recovery.phase,
                "message": format!(
                    "authoritative leader is still unavailable; recoverable survivors={:?}, rejected responses={:?}, unreachable nodes={:?}; natural recovery window {elapsed}/{wait_seconds}s",
                    recovery.survivor_node_ids,
                    recovery.rejected_node_ids,
                    recovery.unreachable_node_ids,
                ),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let Some(seed) = select_recovery_seed(observations) else {
        let old_pods = cluster_pods(kube, namespace, name)
            .await?
            .into_iter()
            .map(|pod| (pod.metadata.name, pod.metadata.uid))
            .collect::<Vec<_>>();
        for (old_pod, _) in &old_pods {
            patch_pod_unavailable(kube, namespace, old_pod).await?;
        }
        // Scale to zero before deletion so OrderedReady cannot recreate an old
        // ordinal while stale in-memory Raft processes are still terminating.
        apply_stateful_set(kube, namespace, cluster, 0, false).await?;
        for (old_pod, _) in &old_pods {
            external_termination::release_for_planned_delete(kube, namespace, cluster, old_pod, None).await?;
            kube.delete(&format!("/api/v1/namespaces/{namespace}/pods/{old_pod}")).await?;
        }
        let old_pod_uids =
            old_pods.iter().map(|(_, uid)| uid.as_str()).filter(|uid| !uid.is_empty()).collect::<Vec<_>>();
        for _ in 0..60 {
            let old_process_exists = cluster_pods(kube, namespace, name)
                .await?
                .iter()
                .any(|pod| old_pod_uids.contains(&pod.metadata.uid.as_str()));
            if !old_process_exists {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        let lingering_old_pods = cluster_pods(kube, namespace, name)
            .await?
            .into_iter()
            .filter(|pod| old_pod_uids.contains(&pod.metadata.uid.as_str()))
            .map(|pod| pod.metadata.name)
            .collect::<Vec<_>>();
        if !lingering_old_pods.is_empty() {
            recovery.phase = "RecoveryBlocked".to_string();
            recovery.last_error =
                Some(format!("old Pods did not terminate before empty bootstrap: {lingering_old_pods:?}"));
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RecoveryBlocked",
                    "message": recovery.last_error,
                    "replicas": 0,
                    "readyReplicas": 0,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": null,
                    "confChange": null,
                    "quorumRecovery": quorum_recovery_status_json(&recovery),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }

        apply_stateful_set(kube, namespace, cluster, 1, true).await?;
        recovery.phase = "FreshBootstrapping".to_string();
        recovery.seed_node_id = Some(1);
        recovery.survivor_node_ids.clear();
        recovery.rejected_node_ids.clear();
        recovery.unreachable_node_ids.clear();
        recovery.restarting_node_id = None;
        recovery.restarted_node_ids.clear();
        recovery.stabilizing_node_id = None;
        recovery.stabilizing_started_at = None;
        recovery.last_error =
            Some("no recoverable local raft state remained after the natural recovery window; automatically bootstrapping an empty cluster to restore availability".to_string());
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "FreshBootstrapping",
                "message": "all in-memory raft survivors are gone and every old Pod UID has terminated; recreating node 1 as an empty cluster to restore availability, with data loss",
                "replicas": 1,
                "readyReplicas": 0,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    };

    if seed.membership.supports.get("forceRebootstrap").and_then(Value::as_bool) != Some(true) {
        recovery.phase = "RecoveryBlocked".to_string();
        recovery.last_error =
            Some(format!("selected seed node {} does not advertise forceRebootstrap support", seed.node_id));
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "RecoveryBlocked",
                "message": recovery.last_error,
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let seed_node_id = seed.node_id;
    let old_non_seed_pods = cluster_pods(kube, namespace, name)
        .await?
        .into_iter()
        .filter(|pod| pod.metadata.name != pod_name(name, seed_node_id))
        .map(|pod| (pod.metadata.name, pod.metadata.uid))
        .collect::<Vec<_>>();
    for (target_pod, _) in &old_non_seed_pods {
        patch_pod_unavailable(kube, namespace, target_pod).await?;
        external_termination::release_for_planned_delete(kube, namespace, cluster, target_pod, None).await?;
        kube.delete(&format!("/api/v1/namespaces/{namespace}/pods/{target_pod}")).await?;
    }

    let old_pod_uids = old_non_seed_pods
        .iter()
        .map(|(_, uid)| uid.as_str())
        .filter(|uid| !uid.is_empty())
        .collect::<Vec<_>>();
    for _ in 0..60 {
        let old_process_exists = cluster_pods(kube, namespace, name)
            .await?
            .iter()
            .any(|pod| old_pod_uids.contains(&pod.metadata.uid.as_str()));
        if !old_process_exists {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let lingering_old_pods = cluster_pods(kube, namespace, name)
        .await?
        .into_iter()
        .filter(|pod| old_pod_uids.contains(&pod.metadata.uid.as_str()))
        .map(|pod| pod.metadata.name)
        .collect::<Vec<_>>();
    if !lingering_old_pods.is_empty() {
        recovery.phase = "RecoveryBlocked".to_string();
        recovery.last_error = Some(format!(
            "old raft survivor Pods did not terminate before seed rebootstrap: {lingering_old_pods:?}"
        ));
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "RecoveryBlocked",
                "message": recovery.last_error,
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    recovery.phase = "SelectingRecoverySeed".to_string();
    recovery.seed_node_id = Some(seed_node_id);
    recovery.restarting_node_id = None;
    recovery.restarted_node_ids.clear();
    recovery.stabilizing_node_id = None;
    recovery.stabilizing_started_at = None;
    recovery.unhealthy_node_id = None;
    recovery.unhealthy_started_at = None;
    recovery.recycling_node_id = None;
    recovery.recycling_pod_uid = None;
    recovery.recycle_attempts.clear();
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "SelectingRecoverySeed",
            "message": format!("selected node {seed_node_id} as the most advanced in-memory survivor; all other old raft processes are gone"),
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "operation": null,
            "confChange": null,
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;

    rmqtt_action_value_with_payload(
        namespace,
        cluster,
        seed_node_id,
        json!({
            "action": "force_rebootstrap",
            "recoveryEpoch": recovery.epoch,
        }),
    )
    .await?;
    patch_pod_serving(kube, namespace, &pod_name(name, seed_node_id)).await?;
    recovery.phase = "RebuildingQuorum".to_string();
    recovery.last_error = None;
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "DegradedServing",
            "message": format!("node {seed_node_id} is serving from preserved in-memory state as a singleton; rebuilding the remaining voters one at a time"),
            "replicas": current_replicas,
            "readyReplicas": ready_pods(kube, namespace, name).await?.len(),
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": seed_node_id,
            "operation": null,
            "confChange": null,
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn patch_quorum_recovery_hold(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    ready_replicas: i32,
    membership: &MembershipStatus,
    mut recovery: QuorumRecoveryStatus,
    message: String,
) -> Result<()> {
    recovery.phase = "DegradedServing".to_string();
    recovery.stabilizing_node_id = None;
    recovery.stabilizing_started_at = None;
    recovery.last_error = Some(message.clone());
    patch_status(
        kube,
        namespace,
        &cluster.metadata.name,
        json!({
            "phase": "DegradedServing",
            "message": message,
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(membership, current_replicas),
            "operation": null,
            "confChange": null,
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

fn clear_recovery_unhealthy_observation(recovery: &mut QuorumRecoveryStatus, node_id: i32) {
    if recovery.unhealthy_node_id == Some(node_id) {
        recovery.unhealthy_node_id = None;
        recovery.unhealthy_started_at = None;
    }
}

fn begin_recovery_node_stabilization(recovery: &mut QuorumRecoveryStatus, node_id: i32) {
    recovery.phase = "StabilizingRecoveryNode".to_string();
    recovery.restarting_node_id = Some(node_id);
    recovery.stabilizing_node_id = Some(node_id);
    recovery.stabilizing_started_at = Some(utc_now_rfc3339());
    recovery.last_error = None;
}

async fn patch_recovery_health_observation_reset(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    ready_replicas: i32,
    membership: &MembershipStatus,
    mut recovery: QuorumRecoveryStatus,
    node_id: i32,
    pod_ready: bool,
    local_membership_healthy: bool,
) -> Result<()> {
    clear_recovery_unhealthy_observation(&mut recovery, node_id);
    recovery.last_error = None;
    if local_membership_healthy {
        patch_pod_serving(kube, namespace, &pod_name(&cluster.metadata.name, node_id)).await?;
    }
    patch_status(
        kube,
        namespace,
        &cluster.metadata.name,
        json!({
            "phase": "DegradedServing",
            "message": format!(
                "recovery node {node_id} no longer has both recycle-failure signals; reset the continuous unhealthy window: podReady={pod_ready}, localMembershipHealthy={local_membership_healthy}"
            ),
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(membership, current_replicas),
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn continue_recovery_node_recycle(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    ready_replicas: i32,
    membership: &MembershipStatus,
    mut recovery: QuorumRecoveryStatus,
    pods: &[Pod],
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let node_id = recovery.recycling_node_id.ok_or_else(|| anyhow!("recovery recycle node is missing"))?;
    let expected_uid = recovery.recycling_pod_uid.clone().unwrap_or_default();
    recovery.restarting_node_id = Some(node_id);
    recovery.restarted_node_ids.retain(|restarted| *restarted != node_id);
    recovery.stabilizing_node_id = None;
    recovery.stabilizing_started_at = None;
    clear_recovery_unhealthy_observation(&mut recovery, node_id);

    match pod_for_node(pods, name, node_id) {
        Some(pod) if !expected_uid.is_empty() && pod.metadata.uid == expected_uid => {
            patch_pod_unavailable(kube, namespace, &pod.metadata.name).await?;
            let deleting = pod.metadata.deletion_timestamp.is_some();
            let mut refreshed_membership = None;
            let mut delete_accepted = true;
            if !deleting {
                if let Err(error) = wait_for_client_endpoint_removal(
                    kube,
                    namespace,
                    name,
                    &pod.metadata.name,
                    cluster.spec.raft.endpoint_removal_stable_seconds,
                )
                .await
                {
                    return patch_quorum_recovery_hold(
                        kube,
                        namespace,
                        cluster,
                        current_replicas,
                        ready_replicas,
                        membership,
                        recovery,
                        format!(
                            "waiting to retry recovery voter {node_id} recycle because its client endpoint was not removed safely: {error}"
                        ),
                    )
                    .await;
                }

                let seed_node_id = recovery
                    .seed_node_id
                    .ok_or_else(|| anyhow!("quorum recovery seed is missing before Pod recycle"))?;
                match fresh_recovery_node_signals(
                    kube,
                    namespace,
                    cluster,
                    node_id,
                    &expected_uid,
                    seed_node_id,
                )
                .await?
                {
                    Some((pod_ready, local_membership_healthy))
                        if !recovery_node_auto_recycle_candidate(pod_ready, local_membership_healthy) =>
                    {
                        recovery.recycling_node_id = None;
                        recovery.recycling_pod_uid = None;
                        recovery.phase = "RebuildingQuorum".to_string();
                        return patch_recovery_health_observation_reset(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            ready_replicas,
                            membership,
                            recovery,
                            node_id,
                            pod_ready,
                            local_membership_healthy,
                        )
                        .await;
                    }
                    Some(_) => {}
                    None => {
                        return patch_quorum_recovery_hold(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            ready_replicas,
                            membership,
                            recovery,
                            format!(
                                "waiting to retry recovery voter {node_id} recycle because its Pod UID or termination state changed during safety validation"
                            ),
                        )
                        .await;
                    }
                }

                let fresh = match fresh_recovery_seed_membership(namespace, cluster, &recovery).await {
                    Ok(fresh) => fresh,
                    Err(error) => {
                        return patch_quorum_recovery_hold(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            ready_replicas,
                            membership,
                            recovery,
                            format!(
                                "waiting to retry recovery voter {node_id} recycle because fresh seed validation failed: {error:#}"
                            ),
                        )
                        .await;
                    }
                };
                if !voter_exists(&fresh, node_id) {
                    recovery.recycling_node_id = None;
                    recovery.recycling_pod_uid = None;
                    recovery.phase = "RebuildingQuorum".to_string();
                    recovery.last_error = None;
                    return patch_status(
                        kube,
                        namespace,
                        name,
                        json!({
                            "phase": "DegradedServing",
                            "message": format!("recovery node {node_id} is no longer a voter; cancelled its pending Pod recycle and resumed learner repair"),
                            "replicas": current_replicas,
                            "readyReplicas": ready_replicas,
                            "desiredReplicas": cluster.spec.replicas,
                            "leaderId": fresh.leader_id,
                            "raftStatus": membership_status_json(&fresh, current_replicas),
                            "operation": null,
                            "confChange": null,
                            "quorumRecovery": quorum_recovery_status_json(&recovery),
                            "observedGeneration": cluster.metadata.generation,
                        }),
                    )
                    .await;
                }
                if let Some(blocker) = recovery_node_recycle_blocker(&recovery, &fresh, node_id) {
                    return patch_quorum_recovery_hold(
                        kube,
                        namespace,
                        cluster,
                        current_replicas,
                        ready_replicas,
                        &fresh,
                        recovery,
                        format!(
                            "waiting to retry recovery voter {node_id} recycle after fresh quorum validation: {blocker}"
                        ),
                    )
                    .await;
                }
                external_termination::release_for_planned_delete(
                    kube,
                    namespace,
                    cluster,
                    &pod.metadata.name,
                    Some(&expected_uid),
                )
                .await?;
                delete_accepted = kube
                    .delete_with_uid_precondition(
                        &format!("/api/v1/namespaces/{namespace}/pods/{}", pod.metadata.name),
                        &expected_uid,
                    )
                    .await?;
                refreshed_membership = Some(fresh);
            }
            let status_membership = refreshed_membership.as_ref().unwrap_or(membership);
            if !deleting && !delete_accepted {
                recovery.recycling_node_id = None;
                recovery.recycling_pod_uid = None;
                recovery.phase = "RebuildingQuorum".to_string();
                recovery.last_error = None;
                return patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "DegradedServing",
                        "message": format!(
                            "Pod UID precondition rejected deletion of recovery voter {node_id} UID {expected_uid}; cancelled the pending recycle and will inspect the current Pod"
                        ),
                        "replicas": current_replicas,
                        "readyReplicas": ready_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "leaderId": status_membership.leader_id,
                        "raftStatus": membership_status_json(status_membership, current_replicas),
                        "operation": null,
                        "confChange": null,
                        "quorumRecovery": quorum_recovery_status_json(&recovery),
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await;
            }
            let attempt = recovery_node_recycle_attempts(&recovery, node_id);
            recovery.phase = "RecyclingRecoveryNode".to_string();
            recovery.last_error = None;
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "DegradedServing",
                    "message": format!(
                        "{} old Pod UID {} for unhealthy recovery voter {node_id}; waiting for StatefulSet recreation (attempt {attempt}/{})",
                        if deleting {
                            "waiting for termination of"
                        } else {
                            "atomically deleted"
                        },
                        expected_uid,
                        QUORUM_RECOVERY_NODE_MAX_RECYCLE_ATTEMPTS,
                    ),
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": status_membership.leader_id,
                    "raftStatus": membership_status_json(status_membership, current_replicas),
                    "operation": null,
                    "confChange": null,
                    "quorumRecovery": quorum_recovery_status_json(&recovery),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        Some(pod) if !expected_uid.is_empty() && pod.metadata.uid != expected_uid => {
            let replacement_uid = pod.metadata.uid.clone();
            recovery.recycling_node_id = None;
            recovery.recycling_pod_uid = None;
            recovery.phase = "RebuildingQuorum".to_string();
            recovery.last_error = None;
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "DegradedServing",
                    "message": format!("replacement Pod UID {replacement_uid} is present for recovery node {node_id}; resuming learner catch-up"),
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "raftStatus": membership_status_json(membership, current_replicas),
                    "operation": null,
                    "confChange": null,
                    "quorumRecovery": quorum_recovery_status_json(&recovery),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        Some(_) => {
            recovery.recycling_node_id = None;
            recovery.recycling_pod_uid = None;
            recovery.phase = "RebuildingQuorum".to_string();
            recovery.last_error = Some(format!(
                "recycle state for node {node_id} had no original Pod UID; discarded the stale marker without deleting a Pod"
            ));
        }
        None => {}
    }

    let blockers = missing_recovery_target_blockers(pods, name, node_id).unwrap_or_default();
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "DegradedServing",
            "message": format!("waiting for replacement Pod for recycled recovery node {node_id}; OrderedReady blockers={blockers:?}"),
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(membership, current_replicas),
            "operation": null,
            "confChange": null,
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn handle_unhealthy_recovery_voter(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    ready_replicas: i32,
    membership: &MembershipStatus,
    mut recovery: QuorumRecoveryStatus,
    pods: &[Pod],
    observations: &[LocalMembershipObservation],
    node_id: i32,
    context: &str,
) -> Result<()> {
    recovery.stabilizing_node_id = None;
    recovery.stabilizing_started_at = None;

    let Some(target_pod) = pod_for_node(pods, &cluster.metadata.name, node_id) else {
        recovery.restarting_node_id = Some(node_id);
        recovery.restarted_node_ids.retain(|restarted| *restarted != node_id);
        clear_recovery_unhealthy_observation(&mut recovery, node_id);
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!("{context}: recovery voter {node_id} Pod is already absent; waiting for its replacement"),
        )
        .await;
    };
    if target_pod.metadata.deletion_timestamp.is_some() {
        recovery.restarting_node_id = Some(node_id);
        recovery.restarted_node_ids.retain(|restarted| *restarted != node_id);
        clear_recovery_unhealthy_observation(&mut recovery, node_id);
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} Pod is already terminating; waiting for its replacement"
            ),
        )
        .await;
    }

    let pod_ready = pod_is_ready(target_pod);
    let local_membership_healthy = recovery_node_locally_healthy(observations, node_id, membership.id);
    if !recovery_node_auto_recycle_candidate(pod_ready, local_membership_healthy) {
        return patch_recovery_health_observation_reset(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            node_id,
            pod_ready,
            local_membership_healthy,
        )
        .await;
    }

    let unhealthy_elapsed = recovery_node_unhealthy_elapsed(&recovery, node_id);
    if unhealthy_elapsed.is_none() {
        recovery.unhealthy_node_id = Some(node_id);
        recovery.unhealthy_started_at = Some(utc_now_rfc3339());
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} failed its local Pod/membership health check; observing for {}s before considering a safe Pod recycle",
                QUORUM_RECOVERY_NODE_UNHEALTHY_GRACE_SECONDS
            ),
        )
        .await;
    }

    let unhealthy_elapsed = unhealthy_elapsed.unwrap_or_default();
    if unhealthy_elapsed < QUORUM_RECOVERY_NODE_UNHEALTHY_GRACE_SECONDS {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} remains locally unhealthy; observation {unhealthy_elapsed}/{}s, with no further voter being added",
                QUORUM_RECOVERY_NODE_UNHEALTHY_GRACE_SECONDS
            ),
        )
        .await;
    }

    let attempts = recovery_node_recycle_attempts(&recovery, node_id);
    if attempts >= QUORUM_RECOVERY_NODE_MAX_RECYCLE_ATTEMPTS {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} is still locally unhealthy after {attempts} automatic recycle attempts; preserving the recovered leader and stopping automatic deletion"
            ),
        )
        .await;
    }

    if let Some(blocker) = recovery_node_recycle_blocker(&recovery, membership, node_id) {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} remains locally unhealthy, but it cannot be recycled safely: {blocker}; preserving degraded service"
            ),
        )
        .await;
    }

    if target_pod.metadata.uid.is_empty() {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!("{context}: recovery voter {node_id} Pod has no UID; refusing an unidentifiable delete"),
        )
        .await;
    }

    let attempt = attempts + 1;
    let pod_uid = target_pod.metadata.uid.clone();
    let pod_name = target_pod.metadata.name.clone();
    patch_pod_unavailable(kube, namespace, &pod_name).await?;
    if let Err(error) = wait_for_client_endpoint_removal(
        kube,
        namespace,
        &cluster.metadata.name,
        &pod_name,
        cluster.spec.raft.endpoint_removal_stable_seconds,
    )
    .await
    {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} remains unhealthy, but its client endpoint was not removed safely: {error}"
            ),
        )
        .await;
    }

    let seed_node_id =
        recovery.seed_node_id.ok_or_else(|| anyhow!("quorum recovery seed is missing before Pod recycle"))?;
    match fresh_recovery_node_signals(kube, namespace, cluster, node_id, &pod_uid, seed_node_id).await? {
        Some((pod_ready, local_membership_healthy))
            if !recovery_node_auto_recycle_candidate(pod_ready, local_membership_healthy) =>
        {
            return patch_recovery_health_observation_reset(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                node_id,
                pod_ready,
                local_membership_healthy,
            )
            .await;
        }
        Some(_) => {}
        None => {
            return patch_quorum_recovery_hold(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                format!(
                    "{context}: recovery voter {node_id} Pod UID or termination state changed during final safety validation; no delete was sent"
                ),
            )
            .await;
        }
    }

    let fresh_membership = match fresh_recovery_seed_membership(namespace, cluster, &recovery).await {
        Ok(fresh_membership) => fresh_membership,
        Err(error) => {
            return patch_quorum_recovery_hold(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                format!(
                    "{context}: recovery voter {node_id} remains unhealthy, but fresh seed validation failed before deletion: {error:#}"
                ),
            )
            .await;
        }
    };
    if !voter_exists(&fresh_membership, node_id) {
        clear_recovery_unhealthy_observation(&mut recovery, node_id);
        recovery.phase = "RebuildingQuorum".to_string();
        recovery.last_error = None;
        return patch_status(
            kube,
            namespace,
            &cluster.metadata.name,
            json!({
                "phase": "DegradedServing",
                "message": format!("recovery node {node_id} is no longer a voter after fresh validation; cancelled Pod recycle and resumed learner repair"),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": fresh_membership.leader_id,
                "raftStatus": membership_status_json(&fresh_membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await;
    }
    if let Some(blocker) = recovery_node_recycle_blocker(&recovery, &fresh_membership, node_id) {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            &fresh_membership,
            recovery,
            format!(
                "{context}: recovery voter {node_id} remains unhealthy, but fresh quorum validation blocked deletion: {blocker}"
            ),
        )
        .await;
    }

    recovery.phase = "RecyclingRecoveryNode".to_string();
    recovery.recycling_node_id = Some(node_id);
    recovery.recycling_pod_uid = Some(pod_uid.clone());
    recovery.recycle_attempts.insert(node_id.to_string(), attempt);
    recovery.restarting_node_id = Some(node_id);
    recovery.restarted_node_ids.retain(|restarted| *restarted != node_id);
    clear_recovery_unhealthy_observation(&mut recovery, node_id);
    recovery.last_error = None;
    patch_status(
        kube,
        namespace,
        &cluster.metadata.name,
        json!({
            "phase": "DegradedServing",
            "message": format!("{context}: safely recycling locally unhealthy recovery voter {node_id}, Pod UID {pod_uid} (attempt {attempt}/{})", QUORUM_RECOVERY_NODE_MAX_RECYCLE_ATTEMPTS),
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": fresh_membership.leader_id,
            "raftStatus": membership_status_json(&fresh_membership, current_replicas),
            "operation": null,
            "confChange": null,
            "quorumRecovery": quorum_recovery_status_json(&recovery),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    external_termination::release_for_planned_delete(kube, namespace, cluster, &pod_name, Some(&pod_uid))
        .await?;
    let deleted = kube
        .delete_with_uid_precondition(&format!("/api/v1/namespaces/{namespace}/pods/{pod_name}"), &pod_uid)
        .await?;
    if !deleted {
        recovery.recycling_node_id = None;
        recovery.recycling_pod_uid = None;
        recovery.phase = "RebuildingQuorum".to_string();
        recovery.last_error = None;
        if attempts == 0 {
            recovery.recycle_attempts.remove(&node_id.to_string());
        } else {
            recovery.recycle_attempts.insert(node_id.to_string(), attempts);
        }
        log::warn!(
            "recovery Pod delete UID precondition was rejected; cancelled the pending recycle without consuming an attempt node_id={} pod_name={} expected_pod_uid={} namespace={} cluster={}",
            node_id,
            pod_name,
            pod_uid,
            namespace,
            cluster.metadata.name,
        );
        return patch_status(
            kube,
            namespace,
            &cluster.metadata.name,
            json!({
                "phase": "DegradedServing",
                "message": format!(
                    "{context}: Pod UID precondition rejected deletion of recovery voter {node_id} UID {pod_uid}; cancelled the pending recycle without consuming an attempt"
                ),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": fresh_membership.leader_id,
                "raftStatus": membership_status_json(&fresh_membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await;
    }
    log::warn!(
        "event=QUORUM_RECOVERY_NODE_RECYCLE epoch={} node_id={} pod_name={} pod_uid={} attempt={}/{} namespace={} cluster={}",
        recovery.epoch,
        node_id,
        pod_name,
        pod_uid,
        attempt,
        QUORUM_RECOVERY_NODE_MAX_RECYCLE_ATTEMPTS,
        namespace,
        cluster.metadata.name,
    );
    Ok(())
}

async fn continue_quorum_recovery_conf_change(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    recovery: QuorumRecoveryStatus,
    pods: &[Pod],
    observations: &[LocalMembershipObservation],
    membership: &MembershipStatus,
    conf_change: ConfChangeStatus,
) -> Result<()> {
    if conf_change.kind != "AddLearnerAndPromote" {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32,
            membership,
            recovery,
            format!(
                "paused quorum rebuild because unexpected membership operation {} is active",
                conf_change.kind
            ),
        )
        .await;
    }

    if let Some(blocker) = recovery_seed_membership_blocker(&recovery, membership) {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32,
            membership,
            recovery,
            format!("paused quorum rebuild membership operation because seed validation failed: {blocker}"),
        )
        .await;
    }

    let ready_replicas = pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32;
    if let Some(node_id) = recovery.unhealthy_node_id {
        let pod_ready = pod_for_node(pods, &cluster.metadata.name, node_id).is_some_and(pod_is_ready);
        let local_membership_healthy = recovery_node_locally_healthy(observations, node_id, membership.id);
        if !recovery_node_auto_recycle_candidate(pod_ready, local_membership_healthy) {
            return patch_recovery_health_observation_reset(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                node_id,
                pod_ready,
                local_membership_healthy,
            )
            .await;
        }
    }
    let unhealthy =
        unhealthy_rebuilt_node_ids(&recovery, pods, &cluster.metadata.name, observations, membership.id);
    if unhealthy.len() > 1 {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "paused quorum rebuild before node {} because multiple rebuilt voters are locally unhealthy: {unhealthy:?}; automatic recycling is limited to one isolated failure",
                conf_change.node_id
            ),
        )
        .await;
    }
    if let Some(node_id) = unhealthy.first().copied() {
        return handle_unhealthy_recovery_voter(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            pods,
            observations,
            node_id,
            &format!("paused quorum rebuild before node {}", conf_change.node_id),
        )
        .await;
    }

    let target_node_id = conf_change.node_id;
    if let Some(blocking_predecessors) =
        missing_recovery_target_blockers(pods, &cluster.metadata.name, target_node_id)
    {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "waiting for recovery target node {target_node_id} Pod to be created; OrderedReady blockers={blocking_predecessors:?}; no learner wait was started"
            ),
        )
        .await;
    }

    if membership.voters.contains(&target_node_id) {
        if !recovery_node_healthy(pods, &cluster.metadata.name, observations, target_node_id, membership.id) {
            return handle_unhealthy_recovery_voter(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                pods,
                observations,
                target_node_id,
                "learner promotion completed but local startup did not remain healthy",
            )
            .await;
        }

        // A recovery membership change must return to the recovery state
        // machine instead of the generic scale-out completion path. The
        // generic path reports Running when this is the final desired node,
        // even though the mandatory recovery stabilization window is still
        // active.
        let mut recovery = recovery;
        clear_recovery_unhealthy_observation(&mut recovery, target_node_id);
        patch_pod_serving(kube, namespace, &pod_name(&cluster.metadata.name, target_node_id)).await?;
        begin_recovery_node_stabilization(&mut recovery, target_node_id);
        patch_status(
            kube,
            namespace,
            &cluster.metadata.name,
            json!({
                "phase": "StabilizingRecoveryNode",
                "message": format!(
                    "node {target_node_id} joined as a voter and is locally healthy; requiring {}s of continuous stability before continuing quorum recovery",
                    QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS
                ),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    continue_conf_change(kube, namespace, cluster, current_replicas, conf_change).await
}

async fn rebuild_quorum_from_seed(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    mut recovery: QuorumRecoveryStatus,
    membership: &MembershipStatus,
    pods: &[Pod],
    observations: &[LocalMembershipObservation],
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let seed_node_id = recovery.seed_node_id.ok_or_else(|| anyhow!("quorum recovery seed is missing"))?;
    if membership.id != seed_node_id || membership.quorum_recovery_epoch != recovery.epoch {
        recovery.phase = "RecoveryBlocked".to_string();
        recovery.last_error = Some(format!(
            "authoritative leader {} does not match recovery seed {} at epoch {}",
            membership.id, seed_node_id, recovery.epoch
        ));
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "RecoveryBlocked",
                "message": recovery.last_error,
                "replicas": current_replicas,
                "readyReplicas": ready_pods(kube, namespace, name).await?.len(),
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let ready_replicas = pods.iter().filter(|pod| pod_is_ready(pod)).count() as i32;
    if recovery.recycling_node_id.is_some() {
        return continue_recovery_node_recycle(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            pods,
        )
        .await;
    }
    if let Some(node_id) = recovery.unhealthy_node_id {
        let pod_ready = pod_for_node(pods, name, node_id).is_some_and(pod_is_ready);
        let local_membership_healthy = recovery_node_locally_healthy(observations, node_id, membership.id);
        if !recovery_node_auto_recycle_candidate(pod_ready, local_membership_healthy) {
            return patch_recovery_health_observation_reset(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                node_id,
                pod_ready,
                local_membership_healthy,
            )
            .await;
        }
    }
    let unhealthy = unhealthy_rebuilt_node_ids(&recovery, pods, name, observations, membership.id);
    if unhealthy.len() > 1 {
        return patch_quorum_recovery_hold(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            format!(
                "multiple rebuilt voters are locally unhealthy: {unhealthy:?}; preserving degraded service because automatic recycling is limited to one isolated failure"
            ),
        )
        .await;
    }
    if let Some(node_id) = unhealthy.first().copied() {
        let next_node = recovery.restarting_node_id;
        return handle_unhealthy_recovery_voter(
            kube,
            namespace,
            cluster,
            current_replicas,
            ready_replicas,
            membership,
            recovery,
            pods,
            observations,
            node_id,
            &format!(
                "paused quorum rebuild{} because a previously rebuilt voter regressed",
                next_node.map(|next_id| format!(" before node {next_id}")).unwrap_or_default()
            ),
        )
        .await;
    }

    if let Some(node_id) = recovery.restarting_node_id {
        if let Some(blocking_predecessors) = missing_recovery_target_blockers(pods, name, node_id) {
            return patch_quorum_recovery_hold(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                format!(
                    "waiting for recovery target node {node_id} Pod to be created; OrderedReady blockers={blocking_predecessors:?}; the operator will not wait for or promote a nonexistent learner"
                ),
            )
            .await;
        }
        let target_pod = pod_for_node(pods, name, node_id)
            .expect("missing recovery target is handled before membership repair");

        let target_ready = pod_is_ready(target_pod);
        let target_locally_healthy = recovery_node_locally_healthy(observations, node_id, membership.id);
        let target_complete = voter_exists(membership, node_id)
            && member_exists(&membership.business_peers, node_id)
            && target_ready
            && target_locally_healthy;
        if target_complete {
            clear_recovery_unhealthy_observation(&mut recovery, node_id);
            patch_pod_serving(kube, namespace, &pod_name(name, node_id)).await?;

            let stable_elapsed = recovery_node_stabilization_elapsed(&recovery, node_id);
            if stable_elapsed.is_none() {
                begin_recovery_node_stabilization(&mut recovery, node_id);
                patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "StabilizingRecoveryNode",
                        "message": format!("node {node_id} joined as a voter and is locally healthy; requiring {}s of continuous stability before continuing quorum recovery", QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS),
                        "replicas": current_replicas,
                        "readyReplicas": ready_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "leaderId": membership.leader_id,
                        "raftStatus": membership_status_json(membership, current_replicas),
                        "operation": null,
                        "confChange": null,
                        "quorumRecovery": quorum_recovery_status_json(&recovery),
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                return Ok(());
            }

            let stable_elapsed = stable_elapsed.unwrap_or_default();
            if stable_elapsed < QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS {
                recovery.phase = "StabilizingRecoveryNode".to_string();
                patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "StabilizingRecoveryNode",
                        "message": format!("node {node_id} remains locally healthy; stabilization {stable_elapsed}/{}s before rebuilding another node", QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS),
                        "replicas": current_replicas,
                        "readyReplicas": ready_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "leaderId": membership.leader_id,
                        "raftStatus": membership_status_json(membership, current_replicas),
                        "operation": null,
                        "confChange": null,
                        "quorumRecovery": quorum_recovery_status_json(&recovery),
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                return Ok(());
            }

            recovery.restarted_node_ids.push(node_id);
            recovery.restarted_node_ids.sort_unstable();
            recovery.restarted_node_ids.dedup();
            recovery.restarting_node_id = None;
            recovery.stabilizing_node_id = None;
            recovery.stabilizing_started_at = None;
            recovery.last_error = None;
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RebuildingQuorum",
                    "message": format!("node {node_id} remained locally healthy for {}s; selecting the next recovery node", QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS),
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "raftStatus": membership_status_json(membership, current_replicas),
                    "operation": null,
                    "confChange": null,
                    "quorumRecovery": quorum_recovery_status_json(&recovery),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }

        recovery.stabilizing_node_id = None;
        recovery.stabilizing_started_at = None;
        if voter_exists(membership, node_id) {
            return handle_unhealthy_recovery_voter(
                kube,
                namespace,
                cluster,
                current_replicas,
                ready_replicas,
                membership,
                recovery,
                pods,
                observations,
                node_id,
                "promoted recovery voter did not pass local Pod and membership health checks",
            )
            .await;
        }

        let conf_change = ConfChangeStatus {
            kind: "AddLearnerAndPromote".to_string(),
            node_id,
            from_replicas: current_replicas,
            to_replicas: current_replicas,
            requested_replicas: cluster.spec.replicas,
        };
        continue_add_learner_and_promote(kube, namespace, cluster, current_replicas, &conf_change).await?;
        return Ok(());
    }

    if let Some(node_id) = (1..=current_replicas)
        .filter(|node_id| *node_id != seed_node_id)
        .find(|node_id| !recovery.restarted_node_ids.contains(node_id))
    {
        recovery.restarting_node_id = Some(node_id);
        recovery.stabilizing_node_id = None;
        recovery.stabilizing_started_at = None;
        recovery.last_error = None;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "RebuildingQuorum",
                "message": format!("waiting for recreated node {node_id} to join seed {seed_node_id} as a learner"),
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if current_replicas < cluster.spec.replicas {
        let node_id = current_replicas + 1;
        apply_stateful_set(kube, namespace, cluster, node_id, false).await?;
        recovery.restarting_node_id = Some(node_id);
        recovery.stabilizing_node_id = None;
        recovery.stabilizing_started_at = None;
        recovery.last_error = None;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "RebuildingQuorum",
                "message": format!("creating node {node_id} to join recovery seed {seed_node_id} as a learner"),
                "replicas": node_id,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let members_ready = membership_ready_for_replicas(membership, current_replicas);
    if ready_replicas == current_replicas && members_ready {
        rmqtt_action_value_with_payload(
            namespace,
            cluster,
            seed_node_id,
            json!({
                "action": "cancel_quorum_recovery",
                "recoveryEpoch": recovery.epoch,
            }),
        )
        .await?;
        ensure_ready_pods_serving(kube, namespace, name, current_replicas).await?;
        apply_client_service(kube, namespace, cluster, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Running",
                "message": "raft quorum recovery completed; all desired voters and business members are ready",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
    } else {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "RebuildingQuorum",
                "message": "all recovery nodes were processed; waiting for final pod and membership convergence",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(membership, current_replicas),
                "operation": null,
                "confChange": null,
                "quorumRecovery": quorum_recovery_status_json(&recovery),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
    }
    Ok(())
}

async fn leader_membership(
    _kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
) -> Result<MembershipStatus> {
    let node_ids = (1..=replicas).collect::<Vec<_>>();
    leader_membership_from_nodes(namespace, cluster, &node_ids).await
}

async fn leader_membership_from_nodes(
    namespace: &str,
    cluster: &RmqttCluster,
    node_ids: &[i32],
) -> Result<MembershipStatus> {
    let mut observations = Vec::new();
    for id in node_ids {
        match rmqtt_action::<MembershipStatus>(namespace, cluster, *id, "membership").await {
            Ok(status) if is_available_authoritative_leader(&status) => return Ok(status),
            Ok(status) => observations.push(format!(
                "node {} reported id={}, leaderId={:?}, isLeader={}, started={}, available={}",
                id, status.id, status.leader_id, status.is_leader, status.started, status.available
            )),
            Err(e) => observations.push(format!("node {id} query failed: {e:#}")),
        }
    }
    bail!("no authoritative raft leader returned membership; {}", observations.join("; "));
}

async fn ensure_force_drain_finalize_supported(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
) -> Result<()> {
    for node_id in 1..=replicas {
        let status = rmqtt_action::<MembershipStatus>(namespace, cluster, node_id, "membership")
            .await
            .with_context(|| format!("query force-finalize capability from node {node_id}"))?;
        validate_force_drain_finalize_support(node_id, &status)?;
    }
    Ok(())
}

fn validate_force_drain_finalize_support(expected_node_id: i32, status: &MembershipStatus) -> Result<()> {
    if status.id != expected_node_id
        || status.supports.get("forceDrainFinalize").and_then(Value::as_bool) != Some(true)
    {
        bail!(
            "node {expected_node_id} did not advertise supports.forceDrainFinalize=true (reported id={}, supports={})",
            status.id,
            status.supports
        );
    }
    Ok(())
}

async fn observe_scale_in_state(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    victim_id: i32,
) -> Result<ScaleInObservation> {
    let name = cluster.metadata.name.as_str();
    let stateful_set_replicas = get_stateful_set(kube, namespace, name)
        .await?
        .map(|stateful_set| stateful_set.replicas())
        .unwrap_or(0);
    let victim_pod_name = pod_name(name, victim_id);
    let pod_state = match cluster_pods(kube, namespace, name)
        .await?
        .into_iter()
        .find(|pod| pod.metadata.name == victim_pod_name)
    {
        Some(pod) if pod.metadata.deletion_timestamp.is_some() => VictimPodState::Terminating,
        Some(_) => VictimPodState::Present,
        None => VictimPodState::Absent,
    };
    let remaining_node_ids =
        (1..=current_replicas).filter(|node_id| *node_id != victim_id).collect::<Vec<_>>();
    let membership = leader_membership_from_nodes(namespace, cluster, &remaining_node_ids).await?;
    let raft_member = voter_exists(&membership, victim_id) || membership.learners.contains(&victim_id);
    let business_member = member_exists(&membership.business_peers, victim_id);
    Ok(ScaleInObservation { pod_state, stateful_set_replicas, raft_member, business_member, membership })
}

async fn rmqtt_action<T>(namespace: &str, cluster: &RmqttCluster, node_id: i32, action: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    try_rmqtt_action(namespace, cluster, node_id, action).await.map_err(anyhow::Error::new)
}

async fn try_rmqtt_action<T>(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    action: &str,
) -> std::result::Result<T, RmqttActionError>
where
    T: for<'de> Deserialize<'de>,
{
    let payload = json!({ "action": action });
    let value = try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload.clone()).await?;
    serde_json::from_value(value).map_err(|error| {
        RmqttActionError::new(
            RmqttActionErrorKind::InvalidResponse,
            node_id,
            &payload,
            format!("decode response: {error}"),
        )
    })
}

async fn rmqtt_action_value(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    action: &str,
) -> Result<Value> {
    rmqtt_action_value_with_payload(namespace, cluster, node_id, json!({ "action": action })).await
}

async fn rmqtt_action_value_with_payload(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    payload: Value,
) -> Result<Value> {
    try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload)
        .await
        .map_err(anyhow::Error::new)
}

async fn try_rmqtt_action_value_with_payload(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    payload: Value,
) -> std::result::Result<Value, RmqttActionError> {
    let action = payload.get("action").and_then(Value::as_str);
    let retry_safe = action.is_some_and(|action| {
        matches!(
            action,
            "membership"
                | "termination_status"
                | "termination_bind"
                | "termination_complete"
                | "drain_status"
                | "routing_status"
                | "prepare_drain"
                | "activate_drain"
                | "clear_drain"
                | "force_drain_finalize"
                | "quiesce"
                | "drain"
                | "undrain"
                | "prepare_quorum_recovery"
                | "cancel_quorum_recovery"
                | "force_rebootstrap"
        )
    });
    let mut delay = Duration::from_millis(200);
    for attempt in 1..=3 {
        match try_rmqtt_action_value_once(namespace, cluster, node_id, payload.clone()).await {
            Err(error) if retry_safe && error.is_transient() && attempt < 3 => {
                if action == Some("membership") {
                    log::debug!(
                        "transient RMQTT membership request failed, retrying node_id={} pod_name={} attempt={}/3 error_kind={} error={}",
                        node_id,
                        pod_name(&cluster.metadata.name, node_id),
                        attempt,
                        error.kind.label(),
                        error
                    );
                } else {
                    log::warn!(
                        "transient RMQTT control request failed, retrying node_id={} pod_name={} payload={} attempt={}/3 error={}",
                        node_id,
                        pod_name(&cluster.metadata.name, node_id),
                        payload,
                        attempt,
                        error
                    );
                }
                sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            result => return result,
        }
    }
    unreachable!("RMQTT control request retry loop always returns")
}

async fn try_rmqtt_action_value_once(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    payload: Value,
) -> std::result::Result<Value, RmqttActionError> {
    let name = cluster.metadata.name.as_str();
    let http_port = cluster.spec.service.http_api_port;
    let url = format!(
        "http://{}-{}.{}-headless.{}.svc:{}/api/v1/plugins/{}/rmqtt-cluster-raft/send",
        name,
        node_id - 1,
        name,
        namespace,
        http_port,
        node_id
    );
    #[cfg(test)]
    let url = external_termination::TEST_BROKER_URL
        .try_with(|base| format!("{base}/broker/{node_id}"))
        .unwrap_or(url);
    let request_timeout = match payload.get("action").and_then(Value::as_str) {
        Some("force_rebootstrap") => Duration::from_secs(60),
        Some("termination_status" | "termination_bind" | "termination_complete") => Duration::from_secs(2),
        _ => Duration::from_secs(10),
    };
    let client = Client::builder().timeout(request_timeout).build().map_err(|error| {
        RmqttActionError::new(
            classify_reqwest_error(&error),
            node_id,
            &payload,
            format!("build HTTP client: {}", error_chain_text(&error)),
        )
    })?;
    let res = client.post(url).json(&payload).send().await.map_err(|error| {
        RmqttActionError::new(classify_reqwest_error(&error), node_id, &payload, error_chain_text(&error))
    })?;
    let status = res.status();
    let text = res.text().await.map_err(|error| {
        RmqttActionError::new(
            classify_reqwest_error(&error),
            node_id,
            &payload,
            format!("read response body: {}", error_chain_text(&error)),
        )
    })?;
    if !status.is_success() {
        let kind = if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            RmqttActionErrorKind::TransientHttpStatus
        } else {
            RmqttActionErrorKind::HttpStatus
        };
        return Err(RmqttActionError::new(kind, node_id, &payload, format!("{status} {text}")));
    }
    serde_json::from_str(&text).map_err(|error| {
        RmqttActionError::new(
            RmqttActionErrorKind::InvalidResponse,
            node_id,
            &payload,
            format!("parse response: {error}; body={text}"),
        )
    })
}

async fn request_node_leave(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
) -> std::result::Result<(), RmqttActionError> {
    let payload = json!({"action": "leave"});
    let response = try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload.clone()).await?;
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(RmqttActionError::new(
            RmqttActionErrorKind::InvalidResponse,
            node_id,
            &payload,
            format!("unexpected response: {response}"),
        ))
    }
}

async fn routing_barrier_status(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    target_id: i32,
) -> Result<Vec<RoutingStatus>> {
    let mut statuses = Vec::with_capacity(replicas as usize);
    for node_id in 1..=replicas {
        let value = rmqtt_action_value_with_payload(
            namespace,
            cluster,
            node_id,
            json!({"action": "routing_status", "id": target_id}),
        )
        .await?;
        let status = serde_json::from_value(value)
            .with_context(|| format!("decode routing status from node {node_id} for target {target_id}"))?;
        statuses.push(status);
    }
    Ok(statuses)
}

async fn broadcast_handoff_action(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    action: &str,
    target_id: i32,
    allowed_handoff_node_ids: Option<&[i32]>,
) -> Result<()> {
    let node_ids = handoff_action_node_ids(replicas, action, target_id);
    for node_id in node_ids {
        let mut payload = json!({"action": action, "id": target_id});
        if let Some(allowed) = allowed_handoff_node_ids {
            payload["allowedTargetNodeIds"] = json!(allowed);
        }
        let response = try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload)
            .await
            .map_err(anyhow::Error::new)?;
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            bail!(
                "node {node_id} returned an unexpected {action} response for target {target_id}: {response}"
            );
        }
        if action == "prepare_drain"
            && response.get("reliable_handoff").and_then(Value::as_bool) != Some(true)
        {
            bail!("node {node_id} has reliable handoff disabled for target {target_id}: {response}");
        }
        if action == "prepare_drain" {
            if let Some(expected) = allowed_handoff_node_ids {
                let acknowledged = response
                    .get("allowedTargetNodeIds")
                    .cloned()
                    .and_then(|value| serde_json::from_value::<Vec<i32>>(value).ok());
                if acknowledged.as_deref() != Some(expected) {
                    bail!(
                        "node {node_id} did not acknowledge allowed handoff targets {expected:?} for target {target_id}: {response}"
                    );
                }
            }
        }
    }
    Ok(())
}

async fn force_finalize_node_drain(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    target_id: i32,
    reason: &str,
    blockers: &str,
) -> Result<ForceDrainFinalizeSummary> {
    let mut summary = ForceDrainFinalizeSummary {
        target_id,
        reason: reason.to_string(),
        blockers: blockers.to_string(),
        ..ForceDrainFinalizeSummary::default()
    };
    let mut target_confirmed = false;

    for node_id in handoff_action_node_ids(replicas, "force_drain_finalize", target_id) {
        let payload = json!({
            "action": "force_drain_finalize",
            "id": target_id,
            "reason": reason,
        });
        let value = try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload)
            .await
            .map_err(anyhow::Error::new)?;
        let response = serde_json::from_value::<ForceDrainFinalizeResponse>(value.clone())
            .with_context(|| format!("decode force_drain_finalize response from node {node_id}: {value}"))?;
        if !response.ok || response.node_id != node_id || response.target_id != target_id {
            bail!(
                "node {node_id} returned an invalid force_drain_finalize response for target {target_id}: {value}"
            );
        }
        if response.local_target {
            target_confirmed = response.force_drain_finalized;
        }
        summary.nodes_acknowledged += 1;
        summary.handoff_messages_discarded += response.handoff_messages_discarded;
        summary.handoff_bytes_discarded += response.handoff_bytes_discarded;
        summary.handoff_clients_affected += response.handoff_clients_affected;
        summary.local_sessions_requested += response.local_sessions_requested;
        summary.local_session_acks += response.local_session_acks;
        summary.session_handoffs_discarded += response.session_handoffs_discarded;
        summary.router_states_removed += response.router_states_removed;
        summary.routes_removed += response.routes_removed;
    }

    if !target_confirmed {
        bail!("target node {target_id} did not confirm force-finalized local drain state");
    }
    log::warn!(
        "forced drain finalized namespace={} cluster={} target_id={} pod_name={} reason={} nodes_acknowledged={} handoff_messages_discarded={} handoff_bytes_discarded={} handoff_clients_affected={} local_sessions_requested={} local_session_acks={} session_handoffs_discarded={} router_states_removed={} routes_removed={} blockers={}",
        namespace,
        cluster.metadata.name,
        target_id,
        pod_name(&cluster.metadata.name, target_id),
        reason,
        summary.nodes_acknowledged,
        summary.handoff_messages_discarded,
        summary.handoff_bytes_discarded,
        summary.handoff_clients_affected,
        summary.local_sessions_requested,
        summary.local_session_acks,
        summary.session_handoffs_discarded,
        summary.router_states_removed,
        summary.routes_removed,
        blockers
    );
    Ok(summary)
}

async fn record_forced_drain_status(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    summary: &ForceDrainFinalizeSummary,
) -> Result<()> {
    kube.merge_patch(
        &format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{}/status", cluster.metadata.name),
        &json!({"status": {"lastForcedDrain": summary.to_status_json()}}),
    )
    .await
}

async fn record_forced_pod_restart_drain_status(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    summary: &ForceDrainFinalizeSummary,
) -> Result<()> {
    let description = summary.description();
    // Record the lossy result and the resumable phase atomically. Once this
    // patch lands, a controller restart must proceed toward membership removal
    // and must not execute force finalization a second time merely to recover
    // the discarded counters.
    kube.merge_patch(
        &format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{}/status", cluster.metadata.name),
        &json!({"status": {
            "phase": "ForceFinalizingPodRestart",
            "message": description,
            "lastForcedDrain": summary.to_status_json(),
            "operation": {
                "phase": "ForceFinalized",
                "lastAttemptAt": utc_now_rfc3339(),
                "lastError": null,
            },
        }}),
    )
    .await
}

fn handoff_action_node_ids(replicas: i32, action: &str, target_id: i32) -> Vec<i32> {
    let mut node_ids = (1..=replicas).collect::<Vec<_>>();
    if !(1..=replicas).contains(&target_id) {
        return node_ids;
    }
    node_ids.retain(|node_id| *node_id != target_id);
    if action == "activate_drain" {
        // Arm every remote sender first. The target switches to Avoid last,
        // immediately before its local drain action disconnects sessions.
        // This bounds the connected-but-not-delivering window to one control
        // request instead of all peer requests plus observation loops.
        node_ids.push(target_id);
    } else if action == "prepare_drain" {
        node_ids.insert(0, target_id);
    } else {
        node_ids.push(target_id);
    }
    node_ids
}

async fn prepare_reliable_handoff(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    target_id: i32,
    operation: &DrainOperation,
) -> Result<()> {
    let allowed_handoff_node_ids = operation.handoff_destination_constraint(replicas, target_id);
    if let Err(error) = broadcast_handoff_action(
        namespace,
        cluster,
        replicas,
        "prepare_drain",
        target_id,
        allowed_handoff_node_ids.as_deref(),
    )
    .await
    {
        clear_reliable_handoff(namespace, cluster, replicas, target_id).await;
        return Err(
            error.context(format!("not every RMQTT node enabled reliable handoff for node {target_id}"))
        );
    }
    Ok(())
}

async fn activate_reliable_handoff(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    target_id: i32,
) -> Result<()> {
    broadcast_handoff_action(namespace, cluster, replicas, "activate_drain", target_id, None)
        .await
        .context(format!("failed to activate reliable handoff away from node {target_id}"))
}

async fn clear_reliable_handoff(namespace: &str, cluster: &RmqttCluster, replicas: i32, target_id: i32) {
    if let Err(error) =
        broadcast_handoff_action(namespace, cluster, replicas, "clear_drain", target_id, None).await
    {
        log::warn!(
            "failed to clear reliable handoff marker namespace={} cluster={} target_id={} pod_name={} error={:#}",
            namespace,
            cluster.metadata.name,
            target_id,
            pod_name(&cluster.metadata.name, target_id),
            error
        );
    }
}

async fn pod_in_client_service_endpoints(
    kube: &KubeClient,
    namespace: &str,
    service_name: &str,
    pod_name: &str,
) -> Result<bool> {
    let path = format!(
        "/apis/discovery.k8s.io/v1/namespaces/{namespace}/endpointslices?labelSelector=kubernetes.io/service-name={service_name}"
    );
    let slices: ObjectList<EndpointSlice> = kube.get_json(&path).await?;
    Ok(slices
        .items
        .iter()
        .flat_map(|slice| slice.endpoints.iter())
        .any(|endpoint| endpoint.target_ref.as_ref().is_some_and(|target| target.name == pod_name)))
}

async fn wait_for_client_endpoint_removal(
    kube: &KubeClient,
    namespace: &str,
    service_name: &str,
    pod_name: &str,
    stable_seconds: u64,
) -> Result<()> {
    let stable_seconds = stable_seconds.max(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(stable_seconds + 30);
    let mut absent_since = None;
    loop {
        if pod_in_client_service_endpoints(kube, namespace, service_name, pod_name).await? {
            absent_since = None;
        } else {
            let since = absent_since.get_or_insert_with(tokio::time::Instant::now);
            if since.elapsed() >= Duration::from_secs(stable_seconds) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "pod {pod_name} remained in client Service EndpointSlices after {} seconds",
                stable_seconds + 30
            );
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn routing_barrier_ready(statuses: &[RoutingStatus]) -> bool {
    !statuses.is_empty()
        && statuses.iter().all(|status| {
            status.converged
                && status.client_states == 0
                && status.routes == 0
                && status.active_forwards == 0
                && status.reliable_forwards == 0
                && status.plain_forwards == 0
                && status.handoff_pending == 0
        })
}

fn routing_handoff_requires_rollback(statuses: &[RoutingStatus], threshold_percent: u8) -> bool {
    statuses.iter().any(|status| {
        status.data_loss_detected
            || status.handoff_failed > 0
            || status.handoff_rejected > 0
            || status.handoff_capacity_waiters > 0
            || status.handoff_capacity_percent >= threshold_percent
    })
}

async fn prepare_node_for_drain(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    node_id: i32,
) -> std::result::Result<(), NodeDrainError> {
    prepare_node_for_drain_with_operation(
        kube,
        namespace,
        cluster,
        replicas,
        node_id,
        &DrainOperation::Rollout,
    )
    .await
}

async fn prepare_scale_in_node_for_drain(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    node_id: i32,
    operation: &DrainOperation,
) -> std::result::Result<(), NodeDrainError> {
    debug_assert!(matches!(operation, DrainOperation::ScaleIn { .. }));
    prepare_node_for_drain_with_operation(kube, namespace, cluster, replicas, node_id, operation).await
}

async fn prepare_node_for_drain_with_operation(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    node_id: i32,
    operation: &DrainOperation,
) -> std::result::Result<(), NodeDrainError> {
    let name = cluster.metadata.name.as_str();
    let pod = pod_name(name, node_id);
    prepare_reliable_handoff(namespace, cluster, replicas, node_id, operation)
        .await
        .map_err(|error| NodeDrainError::new(NodeDrainStage::PrepareHandoff, error.to_string()))?;

    // Refuse new MQTT sessions on the broker before changing Service
    // endpoints, but keep existing connections alive.  This avoids both a
    // new-session race and a long pre-disconnect wait under continuous load.
    try_rmqtt_action_value_with_payload(namespace, cluster, node_id, json!({"action": "quiesce"}))
        .await
        .map_err(|error| NodeDrainError::from_action(NodeDrainStage::StartQuiesce, error))?;

    // Stop new Service traffic and wait for EndpointSlice convergence before
    // disconnecting established clients. Existing TCP sessions remain usable
    // during this short propagation window.
    patch_pod_draining(kube, namespace, &pod)
        .await
        .map_err(|error| NodeDrainError::new(NodeDrainStage::RemoveClientEndpoint, error.to_string()))?;
    wait_for_client_endpoint_removal(
        kube,
        namespace,
        name,
        &pod,
        cluster.spec.raft.endpoint_removal_stable_seconds,
    )
    .await
    .map_err(|error| NodeDrainError::new(NodeDrainStage::RemoveClientEndpoint, error.to_string()))?;

    // Arm remote senders before the target is disconnected. From this point
    // all traffic that still resolves to the old owner is held by the reliable
    // handoff queue until the relocated session is installed on a peer.
    activate_reliable_handoff(namespace, cluster, replicas, node_id)
        .await
        .map_err(|error| NodeDrainError::new(NodeDrainStage::ActivateHandoff, error.to_string()))?;
    try_rmqtt_action_value_with_payload(namespace, cluster, node_id, json!({"action": "drain"}))
        .await
        .map_err(|error| NodeDrainError::from_action(NodeDrainStage::StartDrain, error))?;
    Ok(())
}

async fn wait_node_drain_barriers(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    node_id: i32,
    allow_external_preemption: bool,
) -> std::result::Result<NodeDrainOutcome, NodeDrainError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(cluster.spec.raft.drain_timeout_seconds);
    let mut stable_rounds = 0u8;
    let mut last_progress_log = tokio::time::Instant::now() - Duration::from_secs(30);
    loop {
        if allow_external_preemption && cluster.spec.graceful_shutdown.enabled {
            let terminating = cluster_pods(kube, namespace, &cluster.metadata.name)
                .await
                .map_err(|error| NodeDrainError::new(NodeDrainStage::ObserveDrain, error.to_string()))?
                .into_iter()
                .find(|pod| pod.metadata.deletion_timestamp.is_some());
            if let Some(pod) = terminating {
                return Ok(NodeDrainOutcome::Preempted(format!(
                    "planned drain yielded to externally terminating pod {}",
                    pod.metadata.name
                )));
            }
        }
        let status = try_rmqtt_action::<DrainStatus>(namespace, cluster, node_id, "drain_status")
            .await
            .map_err(|error| NodeDrainError::from_action(NodeDrainStage::ObserveDrain, error))?;
        let routing = routing_barrier_status(namespace, cluster, replicas, node_id)
            .await
            .map_err(|error| NodeDrainError::new(NodeDrainStage::ObserveRouting, error.to_string()))?;
        let local_ready = status.scale_in_safe_to_leave();
        let routing_ready = routing_barrier_ready(&routing);
        if local_ready && routing_ready {
            stable_rounds += 1;
        } else {
            stable_rounds = 0;
        }
        let blockers = drain_blockers(&status, &routing);
        let cluster_handoff_messages_completed =
            routing.iter().map(|item| item.handoff_completed).sum::<usize>();
        if last_progress_log.elapsed() >= Duration::from_secs(30) {
            log::info!(
                "node drain progress namespace={} cluster={} node_id={} pod_name={} local_ready={} routing_ready={} stable_rounds={} message_work_items_remaining={} handoff_messages_completed={} session_handoffs_completed={} connections={} sessions={} handoff_pending_messages={} handoff_pending_bytes={} blockers={}",
                namespace,
                cluster.metadata.name,
                node_id,
                pod_name(&cluster.metadata.name, node_id),
                local_ready,
                routing_ready,
                stable_rounds,
                status.counts.message_work_items_remaining,
                cluster_handoff_messages_completed,
                status.counts.session_handoffs_completed,
                status.counts.connections,
                status.counts.sessions,
                status.counts.handoff_pending_messages,
                status.counts.handoff_pending_bytes,
                blockers
            );
            last_progress_log = tokio::time::Instant::now();
        }
        if status.data_loss_detected
            || routing_handoff_requires_rollback(
                &routing,
                cluster.spec.raft.handoff_rollback_threshold_percent,
            )
        {
            return Ok(NodeDrainOutcome::Unsafe(blockers));
        }
        if stable_rounds >= DRAIN_STABLE_OBSERVATIONS {
            return Ok(NodeDrainOutcome::Ready);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(NodeDrainOutcome::TimedOut(blockers));
        }
        sleep(Duration::from_secs(DRAIN_OBSERVATION_INTERVAL_SECONDS)).await;
    }
}

async fn restore_node_after_failed_drain(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    node_id: i32,
) -> Result<Option<String>> {
    try_rmqtt_action_value_with_payload(namespace, cluster, node_id, json!({"action": "undrain"}))
        .await
        .map_err(anyhow::Error::new)
        .context("failed to cancel broker drain")?;
    let clear_error = broadcast_handoff_action(namespace, cluster, replicas, "clear_drain", node_id, None)
        .await
        .err()
        .map(|error| error.to_string());
    patch_pod_serving(kube, namespace, &pod_name(&cluster.metadata.name, node_id)).await?;
    Ok(clear_error)
}

fn drain_blockers(status: &DrainStatus, routing: &[RoutingStatus]) -> String {
    let mut blockers = status
        .blocking_reasons
        .iter()
        .map(|reason| {
            status
                .blocking_counts
                .get(reason)
                .and_then(|count| *count)
                .map(|count| format!("{reason}={count}"))
                .unwrap_or_else(|| reason.clone())
        })
        .collect::<Vec<_>>();
    for item in routing.iter().filter(|item| {
        !item.converged
            || item.client_states > 0
            || item.routes > 0
            || item.active_forwards > 0
            || item.reliable_forwards > 0
            || item.plain_forwards > 0
            || item.handoff_pending > 0
    }) {
        blockers.push(format!(
            "node {} still sees target {} clientStates={} routes={} activeForwards={} reliableForwards={} plainForwards={} handoffPending={} handoffBytes={} handoffOldestMs={} handoffProgress={} handoffCapacity={}% handoffWaiters={} handoffFailed={} handoffRejected={} handoffInvariantMismatches={}",
            item.id,
            item.target_id,
            item.client_states,
            item.routes,
            item.active_forwards,
            item.reliable_forwards,
            item.plain_forwards,
            item.handoff_pending,
            item.handoff_pending_bytes,
            item.handoff_oldest_age_millis,
            item.handoff_progress_sequence,
            item.handoff_capacity_percent,
            item.handoff_capacity_waiters,
            item.handoff_failed,
            item.handoff_rejected,
            item.handoff_invariant_mismatch_observations
        ));
    }
    if blockers.is_empty() {
        "waiting for a second stable drain observation".to_string()
    } else {
        format!("blocking: {}", blockers.join(", "))
    }
}

async fn apply_config_map(kube: &KubeClient, namespace: &str, cluster: &RmqttCluster) -> Result<()> {
    let name = format!("{}-operator-scripts", cluster.metadata.name);
    let obj = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels(&cluster.metadata.name),
        },
        "data": {
            "render-rmqtt-config.sh": RENDER_SCRIPT,
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/configmaps/{name}"), &obj).await
}

async fn apply_broker_service_account(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<()> {
    let Some(service_account_name) = cluster.spec.pod.service_account_name.as_deref() else {
        return Ok(());
    };
    let obj = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": service_account_name,
            "namespace": namespace,
            "labels": labels(&cluster.metadata.name),
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/serviceaccounts/{service_account_name}"), &obj).await
}

async fn apply_services(kube: &KubeClient, namespace: &str, cluster: &RmqttCluster) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let headless = format!("{name}-headless");
    let labels = labels(name);
    let pod_selector = selector(name);

    let headless_obj = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": headless,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": true,
            "selector": pod_selector,
            "ports": headless_service_ports(&cluster.spec.service),
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/services/{name}-headless"), &headless_obj).await?;

    apply_client_service(kube, namespace, cluster, degraded_client_endpoints_enabled(cluster)).await
}

fn degraded_client_endpoints_enabled(cluster: &RmqttCluster) -> bool {
    cluster.status.as_ref().is_some_and(|status| {
        status.phase == "DegradedServing"
            || status.quorum_recovery.as_ref().is_some_and(|recovery| !recovery.survivor_node_ids.is_empty())
    })
}

fn client_service_object(namespace: &str, cluster: &RmqttCluster, publish_not_ready: bool) -> Value {
    let name = cluster.metadata.name.as_str();
    let mut client_selector = selector(name);
    client_selector.insert("rmqtt.io/serving".to_string(), "true".to_string());
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels(name),
        },
        "spec": {
            "publishNotReadyAddresses": publish_not_ready,
            "selector": client_selector,
            "ports": client_service_ports(&cluster.spec.service),
        }
    })
}

async fn apply_client_service(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    publish_not_ready: bool,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let client_obj = client_service_object(namespace, cluster, publish_not_ready);
    kube.apply(&format!("/api/v1/namespaces/{namespace}/services/{name}"), &client_obj).await
}

async fn apply_pod_disruption_budget(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let obj = json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels(name),
        },
        "spec": {
            "maxUnavailable": 1,
            "selector": {"matchLabels": selector(name)},
        }
    });
    kube.apply(&format!("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets/{name}"), &obj).await
}

async fn apply_service_monitor(kube: &KubeClient, namespace: &str, cluster: &RmqttCluster) -> Result<()> {
    let spec = &cluster.spec.monitoring.service_monitor;
    if !spec.enabled {
        return Ok(());
    }
    let name = cluster.metadata.name.as_str();
    let mut monitor_labels = labels(name);
    monitor_labels.extend(spec.labels.clone());
    let obj = json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": monitor_labels,
        },
        "spec": {
            "selector": {"matchLabels": selector(name)},
            "endpoints": [{
                "port": "http-api",
                "path": spec.path,
                "interval": spec.interval,
                "scrapeTimeout": spec.scrape_timeout,
            }]
        }
    });
    kube.apply(&format!("/apis/monitoring.coreos.com/v1/namespaces/{namespace}/servicemonitors/{name}"), &obj)
        .await
}

async fn apply_stateful_set(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    bootstrap_allowed: bool,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let obj = stateful_set_object(namespace, cluster, replicas, bootstrap_allowed);
    kube.apply(&format!("/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}"), &obj).await
}

fn stateful_set_object(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    bootstrap_allowed: bool,
) -> Value {
    let name = cluster.metadata.name.as_str();
    let labels = labels(name);
    let selector = selector(name);
    let scripts_name = format!("{name}-operator-scripts");
    let headless = format!("{name}-headless");
    let mut env = vec![
        json!({"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}}),
        json!({"name": "RMQTT_POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}),
        json!({"name": "RMQTT_POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}}),
        json!({"name": "RUST_BACKTRACE", "value": "1"}),
        json!({"name": "RMQTT_NAME", "value": name}),
        json!({"name": "RMQTT_HEADLESS_SERVICE", "value": format!("{name}-headless")}),
        json!({"name": "RMQTT_REPLICAS", "value": replicas.to_string()}),
        json!({"name": "RMQTT_MQTT_PORT", "value": cluster.spec.service.mqtt_port.to_string()}),
        json!({"name": "RMQTT_MQTTS_PORT", "value": cluster.spec.service.mqtts_port.to_string()}),
        json!({"name": "RMQTT_WS_PORT", "value": cluster.spec.service.ws_port.to_string()}),
        json!({"name": "RMQTT_WSS_PORT", "value": cluster.spec.service.wss_port.to_string()}),
        json!({"name": "RMQTT_HTTP_API_PORT", "value": cluster.spec.service.http_api_port.to_string()}),
        json!({"name": "RMQTT_GRPC_PORT", "value": cluster.spec.service.grpc_port.to_string()}),
        json!({"name": "RMQTT_RAFT_PORT", "value": cluster.spec.service.raft_port.to_string()}),
        json!({"name": "RMQTT_RAFT_BOOTSTRAP", "value": bootstrap_allowed.to_string()}),
        json!({"name": "RMQTT_GRACEFUL_SHUTDOWN_ENABLED", "value": cluster.spec.graceful_shutdown.enabled.to_string()}),
        json!({"name": "RMQTT_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", "value": cluster.spec.graceful_shutdown.timeout_seconds.to_string()}),
        json!({"name": "RMQTT_GRACEFUL_SHUTDOWN_FINALIZE_RESERVE_SECONDS", "value": cluster.spec.graceful_shutdown.finalize_reserve_seconds.to_string()}),
    ];
    let mut volume_mounts = vec![
        json!({"name": "work", "mountPath": "/work"}),
        json!({"name": "rmqtt-data", "mountPath": "/var/lib/rmqtt"}),
        json!({"name": "operator-scripts", "mountPath": "/operator-scripts", "readOnly": true}),
    ];
    let mut volumes = vec![
        json!({"name": "work", "emptyDir": {}}),
        json!({"name": "rmqtt-data", "emptyDir": {}}),
        json!({"name": "operator-scripts", "configMap": {"name": scripts_name, "defaultMode": 493}}),
    ];
    if let Some(base_config_map_name) = cluster.spec.config.base_config_map_name.as_deref() {
        env.push(json!({"name": "RMQTT_BASE_CONFIG_DIR", "value": "/base-config"}));
        volume_mounts.push(json!({
            "name": "base-config",
            "mountPath": "/base-config",
            "readOnly": true
        }));
        volumes.push(json!({
            "name": "base-config",
            "configMap": {"name": base_config_map_name}
        }));
    }

    let mut container = json!({
        "name": "rmqtt",
        "image": cluster.spec.image,
        "imagePullPolicy": cluster.spec.image_pull_policy,
        "command": ["/bin/sh", "-ec"],
        "args": [
            "/operator-scripts/render-rmqtt-config.sh && cd /work && exec /app/rmqtt/rmqtt-bin/rmqttd --config /work/rmqtt.toml --plugins-default-startups rmqtt-cluster-raft rmqtt-sys-topic rmqtt-http-api"
        ],
        "env": env,
        "ports": [
            {"name": "mqtt", "containerPort": cluster.spec.service.mqtt_port, "protocol": "TCP"},
            {"name": "mqtts", "containerPort": cluster.spec.service.mqtts_port, "protocol": "TCP"},
            {"name": "ws", "containerPort": cluster.spec.service.ws_port, "protocol": "TCP"},
            {"name": "wss", "containerPort": cluster.spec.service.wss_port, "protocol": "TCP"},
            {"name": "http-api", "containerPort": cluster.spec.service.http_api_port, "protocol": "TCP"},
            {"name": "cluster-grpc", "containerPort": cluster.spec.service.grpc_port, "protocol": "TCP"},
            {"name": "cluster-raft", "containerPort": cluster.spec.service.raft_port, "protocol": "TCP"}
        ],
        "startupProbe": {
            "tcpSocket": {"port": "http-api"},
            "periodSeconds": 5,
            "timeoutSeconds": 10,
            "failureThreshold": 120
        },
        "livenessProbe": {
            "tcpSocket": {"port": "mqtt"},
            "initialDelaySeconds": 20,
            "periodSeconds": 10,
            "timeoutSeconds": 10,
            "failureThreshold": 3
        },
        "readinessProbe": {
            "httpGet": {"path": "/api/v1/health/ready", "port": "http-api"},
            "initialDelaySeconds": 5,
            "periodSeconds": 5,
            "timeoutSeconds": 3,
            "failureThreshold": 12
        },
        "volumeMounts": volume_mounts
    });
    if let Some(resources) = cluster.spec.resources.as_ref() {
        container["resources"] = resource_requirements_json(resources);
    }

    let mut pod_spec = json!({
        "containers": [container],
        "volumes": volumes,
        "enableServiceLinks": cluster.spec.pod.enable_service_links.unwrap_or(false),
        "terminationGracePeriodSeconds": effective_termination_grace_period_seconds(&cluster.spec),
    });
    if let Some(service_account_name) = cluster.spec.pod.service_account_name.as_deref() {
        pod_spec["serviceAccountName"] = json!(service_account_name);
    }
    if !cluster.spec.pod.image_pull_secrets.is_empty() {
        pod_spec["imagePullSecrets"] = json!(cluster.spec.pod.image_pull_secrets);
    }
    if let Some(scheduler_name) = cluster.spec.pod.scheduler_name.as_deref() {
        pod_spec["schedulerName"] = json!(scheduler_name);
    }
    if let Some(affinity) = cluster.spec.pod.affinity.as_ref() {
        pod_spec["affinity"] = affinity.clone();
    }
    if !cluster.spec.pod.tolerations.is_empty() {
        pod_spec["tolerations"] = json!(cluster.spec.pod.tolerations);
    }

    let obj = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
            "annotations": {
                "rmqtt.io/raft-bootstrap-allowed": bootstrap_allowed.to_string()
            },
        },
        "spec": {
            "serviceName": headless,
            "replicas": replicas,
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "OnDelete"},
            "selector": {"matchLabels": selector},
            "template": {
                "metadata": {
                    "annotations": cluster.spec.pod.annotations,
                    "finalizers": if cluster.spec.graceful_shutdown.enabled {
                        vec![external_termination::FINALIZER]
                    } else { Vec::<&str>::new() },
                    "labels": {
                        "app.kubernetes.io/name": "rmqtt",
                        "app.kubernetes.io/instance": name,
                        // A new or recreated pod is excluded from the client
                        // Service until both local readiness and the leader's
                        // authoritative membership have been confirmed.
                        "rmqtt.io/serving": "false",
                        "rmqtt.io/draining": "false"
                    }
                },
                "spec": pod_spec
            }
        }
    });
    obj
}

fn resource_requirements_json(spec: &ResourceRequirementsSpec) -> Value {
    fn resource_list_json(spec: &ResourceListSpec) -> Value {
        let mut value = json!({});
        if let Some(cpu) = spec.cpu.as_deref() {
            value["cpu"] = json!(cpu);
        }
        if let Some(memory) = spec.memory.as_deref() {
            value["memory"] = json!(memory);
        }
        value
    }

    json!({
        "requests": resource_list_json(&spec.requests),
        "limits": resource_list_json(&spec.limits),
    })
}

async fn patch_pod_draining(kube: &KubeClient, namespace: &str, pod: &str) -> Result<()> {
    let patch = json!({
        "metadata": {
            "labels": {
                "rmqtt.io/serving": "false",
                "rmqtt.io/draining": "true"
            }
        }
    });
    kube.merge_patch(&format!("/api/v1/namespaces/{namespace}/pods/{pod}"), &patch).await
}

async fn patch_pod_unavailable(kube: &KubeClient, namespace: &str, pod: &str) -> Result<()> {
    let patch = json!({
        "metadata": {
            "labels": {
                "rmqtt.io/serving": "false",
                // `draining` is reserved for an explicit, coordinated
                // RemoveVoter operation. A transient readiness failure must
                // not be mistaken for a planned scale-in.
                "rmqtt.io/draining": "false"
            }
        }
    });
    kube.merge_patch(&format!("/api/v1/namespaces/{namespace}/pods/{pod}"), &patch).await
}

async fn patch_pod_serving(kube: &KubeClient, namespace: &str, pod: &str) -> Result<()> {
    let patch = json!({
        "metadata": {
            "labels": {
                "rmqtt.io/serving": "true",
                "rmqtt.io/draining": "false"
            }
        }
    });
    kube.merge_patch(&format!("/api/v1/namespaces/{namespace}/pods/{pod}"), &patch).await
}

async fn ensure_ready_pods_serving(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    replicas: i32,
) -> Result<()> {
    let pods = cluster_pods(kube, namespace, name).await?;
    for node_id in 1..=replicas {
        let expected_name = pod_name(name, node_id);
        let Some(pod) = pods.iter().find(|pod| pod.metadata.name == expected_name) else {
            continue;
        };
        if pod_is_ready(pod)
            && pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str) != Some("true")
        {
            patch_pod_serving(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

async fn ensure_degraded_survivor_admission(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    observations: &[LocalMembershipObservation],
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let survivor_node_ids = observations
        .iter()
        .filter(|observation| observation_has_recoverable_raft_state(observation))
        .map(|observation| observation.node_id)
        .collect::<std::collections::HashSet<_>>();
    let pods = cluster_pods(kube, namespace, name).await?;
    for node_id in 1..=replicas {
        let expected_name = pod_name(name, node_id);
        let Some(pod) = pods.iter().find(|pod| pod.metadata.name == expected_name) else {
            continue;
        };
        let serving = pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str);
        let draining = pod.metadata.labels.get("rmqtt.io/draining").map(String::as_str);
        if survivor_node_ids.contains(&node_id) {
            if serving != Some("true") || draining != Some("false") {
                patch_pod_serving(kube, namespace, &expected_name).await?;
            }
        } else if serving != Some("false") || draining != Some("false") {
            patch_pod_unavailable(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

async fn ensure_recovery_node_admission(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    pods: &[Pod],
    observations: &[LocalMembershipObservation],
    leader_id: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    for node_id in 1..=replicas {
        let Some(pod) = pod_for_node(pods, name, node_id) else {
            continue;
        };
        let expected_name = pod_name(name, node_id);
        let serving = pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str);
        let draining = pod.metadata.labels.get("rmqtt.io/draining").map(String::as_str);
        if recovery_node_locally_healthy(observations, node_id, leader_id) {
            if serving != Some("true") || draining != Some("false") {
                patch_pod_serving(kube, namespace, &expected_name).await?;
            }
        } else if serving != Some("false") || draining != Some("false") {
            patch_pod_unavailable(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

async fn ensure_unready_pods_not_serving(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    replicas: i32,
    planned_draining_node_id: Option<i32>,
    degraded_client_endpoints: bool,
) -> Result<()> {
    let pods = cluster_pods(kube, namespace, name).await?;
    for node_id in 1..=replicas {
        let expected_name = pod_name(name, node_id);
        let Some(pod) = pods.iter().find(|pod| pod.metadata.name == expected_name) else {
            continue;
        };
        if pod_is_ready(pod) {
            continue;
        }

        let expected_draining = planned_draining_node_id == Some(node_id);
        let serving = pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str);
        let draining = pod.metadata.labels.get("rmqtt.io/draining").map(String::as_str);
        if expected_draining {
            if serving != Some("false") || draining != Some("true") {
                patch_pod_draining(kube, namespace, &expected_name).await?;
            }
        } else if degraded_client_endpoints {
            // Admission is maintained from local membership observations in
            // the quorum-loss path. Avoid toggling a verified survivor out of
            // Service merely because strict Raft readiness requires a leader.
            continue;
        } else if serving != Some("false") || draining != Some("false") {
            patch_pod_unavailable(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

fn pending_rollout_restart_at(stateful_set: &StatefulSet, cluster: &RmqttCluster) -> Option<String> {
    let requested = stateful_set.rollout_restart_at()?;
    let observed = cluster.status.as_ref()?.observed_restart_at.as_deref()?;
    (requested != observed).then(|| requested.to_string())
}

async fn patch_observed_restart_baseline(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    restart_at: &str,
) -> Result<()> {
    kube.merge_patch(
        &format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{name}/status"),
        &json!({"status": {"observedRestartAt": restart_at}}),
    )
    .await
}

async fn patch_status(kube: &KubeClient, namespace: &str, name: &str, status: Value) -> Result<()> {
    let patch = json!({ "status": &status });
    let patched_resource = kube
        .merge_patch_response(
            &format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{name}/status"),
            &patch,
        )
        .await?;
    if patched_resource.as_ref().is_some_and(|resource| resource.get("status").is_none()) {
        log::warn!(
            "Kubernetes status patch response did not contain status; logging the requested partial status namespace={} cluster={}",
            namespace,
            name,
        );
    }
    log_status_transition(namespace, name, status_for_transition_log(&status, patched_resource.as_ref()));
    Ok(())
}

fn status_for_transition_log<'a>(
    requested_status: &'a Value,
    patched_resource: Option<&'a Value>,
) -> &'a Value {
    patched_resource.and_then(|resource| resource.get("status")).unwrap_or(requested_status)
}

fn status_log_signature(status: &Value) -> Value {
    json!({
        "phase": status.get("phase"),
        "observedGeneration": status.get("observedGeneration"),
        "replicas": status.get("replicas"),
        "readyReplicas": status.get("readyReplicas"),
        "desiredReplicas": status.get("desiredReplicas"),
        "targetImage": status.get("targetImage"),
        "targetRestartAt": status.get("targetRestartAt"),
        "updatedReplicas": status.get("updatedReplicas"),
        "leaderId": status.get("leaderId"),
        "membersReady": status.pointer("/raftStatus/membersReady"),
        "drainingNodeId": status.get("drainingNodeId"),
        "drainingNodeIds": status.get("drainingNodeIds"),
        "externalTerminationBatchTargets": status.pointer("/externalTerminationBatch/targets"),
        "externalTerminationCleanupTargets": status.pointer("/externalTerminationCleanup/targets"),
        "operationType": status.pointer("/operation/type"),
        "operationPhase": status.pointer("/operation/phase"),
        "operationMode": status.pointer("/operation/mode"),
        "operationNodeId": status.pointer("/operation/nodeId"),
        "operationRetryCount": status.pointer("/operation/retryCount"),
        "confChangeType": status.pointer("/confChange/type"),
        "confChangeNodeId": status.pointer("/confChange/nodeId"),
        "quorumRecoveryEpoch": status.pointer("/quorumRecovery/epoch"),
        "quorumRecoveryPhase": status.pointer("/quorumRecovery/phase"),
        "quorumRecoverySeedNodeId": status.pointer("/quorumRecovery/seedNodeId"),
        "quorumRecoveryRestartingNodeId": status.pointer("/quorumRecovery/restartingNodeId"),
        "quorumRecoveryStabilizingNodeId": status.pointer("/quorumRecovery/stabilizingNodeId"),
        "quorumRecoveryUnhealthyNodeId": status.pointer("/quorumRecovery/unhealthyNodeId"),
        "quorumRecoveryRecyclingNodeId": status.pointer("/quorumRecovery/recyclingNodeId"),
        "quorumRecoveryRecycleAttempts": status.pointer("/quorumRecovery/recycleAttempts"),
        "quorumRecoveryRejectedNodeIds": status.pointer("/quorumRecovery/rejectedNodeIds"),
        "quorumRecoveryUnreachableNodeIds": status.pointer("/quorumRecovery/unreachableNodeIds"),
    })
}

fn status_field(status: &Value, pointer: &str) -> String {
    match status.pointer(pointer) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Null) | None => "-".to_string(),
        Some(value) => value.to_string(),
    }
}

fn status_log_target(status: &Value) -> Option<(&'static str, i32)> {
    // Recovery can pause a pending join to repair an earlier voter. Show that
    // foreground target, not every referenced node or the next queued join.
    [
        ("recycling", "/quorumRecovery/recyclingNodeId"),
        ("unhealthy", "/quorumRecovery/unhealthyNodeId"),
        ("stabilizing", "/quorumRecovery/stabilizingNodeId"),
        ("restarting", "/quorumRecovery/restartingNodeId"),
        ("operation", "/operation/nodeId"),
        ("draining", "/drainingNodeId"),
        ("conf_change", "/confChange/nodeId"),
        ("seed", "/quorumRecovery/seedNodeId"),
    ]
    .into_iter()
    .find_map(|(role, pointer)| {
        status
            .pointer(pointer)
            .and_then(Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
            .filter(|id| *id > 0)
            .map(|id| (role, id))
    })
}

fn log_status_transition(namespace: &str, name: &str, status: &Value) {
    let signature = status_log_signature(status);
    let cache = LAST_LOGGED_STATUS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{namespace}/{name}");
    let changed = {
        let mut statuses = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if statuses.get(&key) == Some(&signature) {
            false
        } else {
            statuses.insert(key, signature);
            true
        }
    };
    if !changed {
        return;
    }

    let details = status_transition_details(namespace, name, status);
    if matches!(
        status_field(status, "/phase").as_str(),
        "Degraded"
            | "DegradedServing"
            | "ScaleInBlocked"
            | "ImageRolloutBlocked"
            | "RollingRestartBlocked"
            | "PodRestartFailed"
            | "QuorumLost"
            | "RecoveryBlocked"
    ) {
        log::warn!("{details}");
    } else {
        log::info!("{details}");
    }
}

fn status_transition_details(namespace: &str, name: &str, status: &Value) -> String {
    let phase = status_field(status, "/phase");
    let generation = status_field(status, "/observedGeneration");
    let replicas = status_field(status, "/replicas");
    let ready_replicas = status_field(status, "/readyReplicas");
    let desired_replicas = status_field(status, "/desiredReplicas");
    let target_image = status_field(status, "/targetImage");
    let target_restart_at = status_field(status, "/targetRestartAt");
    let updated_replicas = status_field(status, "/updatedReplicas");
    let leader_id = status_field(status, "/leaderId");
    let leader_pod_name = status
        .get("leaderId")
        .and_then(Value::as_i64)
        .and_then(|id| i32::try_from(id).ok())
        .filter(|id| *id > 0)
        .map(|id| pod_name(name, id))
        .unwrap_or_else(|| "-".to_string());
    let members_ready = status_field(status, "/raftStatus/membersReady");
    let draining_node_id = status_field(status, "/drainingNodeId");
    let draining_node_ids = status_field(status, "/drainingNodeIds");
    let operation_type = status_field(status, "/operation/type");
    let operation_phase = status_field(status, "/operation/phase");
    let operation_mode = status_field(status, "/operation/mode");
    let operation_node_id = status_field(status, "/operation/nodeId");
    let operation_retry_count = status_field(status, "/operation/retryCount");
    let quorum_recovery_epoch = status_field(status, "/quorumRecovery/epoch");
    let quorum_recovery_phase = status_field(status, "/quorumRecovery/phase");
    let quorum_recovery_seed_node_id = status_field(status, "/quorumRecovery/seedNodeId");
    let quorum_recovery_restarting_node_id = status_field(status, "/quorumRecovery/restartingNodeId");
    let quorum_recovery_stabilizing_node_id = status_field(status, "/quorumRecovery/stabilizingNodeId");
    let quorum_recovery_unhealthy_node_id = status_field(status, "/quorumRecovery/unhealthyNodeId");
    let quorum_recovery_recycling_node_id = status_field(status, "/quorumRecovery/recyclingNodeId");
    let quorum_recovery_recycle_attempts = status_field(status, "/quorumRecovery/recycleAttempts");
    let message = status_field(status, "/message");
    let (target_role, target_node_id, target_pod_name) = status_log_target(status)
        .map(|(role, id)| (role, id.to_string(), pod_name(name, id)))
        .unwrap_or_else(|| ("-", "-".to_string(), "-".to_string()));

    format!(
        "cluster status changed generation={generation} phase={phase} \
         target_role={target_role} target_node_id={target_node_id} target_pod_name={target_pod_name} \
         operation_node_id={operation_node_id} replicas={replicas} ready_replicas={ready_replicas} \
         desired_replicas={desired_replicas} \
         target_restart_at={target_restart_at} updated_replicas={updated_replicas} \
         leader_id={leader_id} leader_pod_name={leader_pod_name} members_ready={members_ready} draining_node_id={draining_node_id} draining_node_ids={draining_node_ids} \
         operation_type={operation_type} operation_phase={operation_phase} operation_mode={operation_mode} \
         operation_retry_count={operation_retry_count} quorum_recovery_epoch={quorum_recovery_epoch} \
         quorum_recovery_phase={quorum_recovery_phase} quorum_recovery_seed_node_id={quorum_recovery_seed_node_id} \
         quorum_recovery_restarting_node_id={quorum_recovery_restarting_node_id} \
         quorum_recovery_stabilizing_node_id={quorum_recovery_stabilizing_node_id} \
         quorum_recovery_unhealthy_node_id={quorum_recovery_unhealthy_node_id} \
         quorum_recovery_recycling_node_id={quorum_recovery_recycling_node_id} \
         quorum_recovery_recycle_attempts={quorum_recovery_recycle_attempts} message={message:?} \
         namespace={namespace} cluster={name} target_image={target_image}"
    )
}

fn labels(name: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), "rmqtt".to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    labels.insert("app.kubernetes.io/managed-by".to_string(), "rmqtt-operator".to_string());
    labels
}

fn selector(name: &str) -> BTreeMap<String, String> {
    let mut selector = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".to_string(), "rmqtt".to_string());
    selector.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    selector
}

fn client_service_ports(spec: &ServiceSpec) -> Value {
    json!([
        {"name": "mqtt", "port": spec.mqtt_port, "targetPort": "mqtt"},
        {"name": "mqtts", "port": spec.mqtts_port, "targetPort": "mqtts"},
        {"name": "ws", "port": spec.ws_port, "targetPort": "ws"},
        {"name": "wss", "port": spec.wss_port, "targetPort": "wss"},
        {"name": "http-api", "port": spec.http_api_port, "targetPort": "http-api"},
    ])
}

fn headless_service_ports(spec: &ServiceSpec) -> Value {
    json!([
        {"name": "cluster-grpc", "port": spec.grpc_port, "targetPort": "cluster-grpc"},
        {"name": "cluster-raft", "port": spec.raft_port, "targetPort": "cluster-raft"}
    ])
}

fn pod_name(cluster_name: &str, node_id: i32) -> String {
    format!("{}-{}", cluster_name, node_id - 1)
}

fn conf_change_status_json(conf_change: &ConfChangeStatus) -> Value {
    json!({
        "type": conf_change.kind,
        "nodeId": conf_change.node_id,
        "fromReplicas": conf_change.from_replicas,
        "toReplicas": conf_change.to_replicas,
        "requestedReplicas": conf_change.requested_replicas
    })
}

fn operation_status_json(conf_change: &ConfChangeStatus, phase: &str, mode: &str) -> Value {
    let now = utc_now_rfc3339();
    json!({
        "id": format!(
            "{}-{}-{}-{}",
            conf_change.kind, conf_change.node_id, conf_change.from_replicas, conf_change.to_replicas
        ),
        "type": conf_change.kind,
        "phase": phase,
        "nodeId": conf_change.node_id,
        "fromReplicas": conf_change.from_replicas,
        "toReplicas": conf_change.to_replicas,
        "requestedReplicas": conf_change.requested_replicas,
        "mode": mode,
        "startedAt": now,
        "lastAttemptAt": now,
        "retryCount": 0,
        "lastError": null
    })
}

fn scale_in_operation_status_json(
    cluster: &RmqttCluster,
    conf_change: &ConfChangeStatus,
    phase: &str,
    last_error: Option<&str>,
    increment_retry: bool,
) -> Value {
    let previous = cluster.status.as_ref().and_then(|status| status.operation.as_ref()).filter(|operation| {
        operation.kind == conf_change.kind
            && operation.node_id == conf_change.node_id
            && operation.from_replicas == conf_change.from_replicas
            && operation.to_replicas == conf_change.to_replicas
    });
    let now = utc_now_rfc3339();
    let started_at =
        previous.and_then(|operation| operation.started_at.clone()).unwrap_or_else(|| now.clone());
    let retry_count =
        previous.map(|operation| operation.retry_count).unwrap_or(0) + u32::from(increment_retry);
    let plan = DrainOperation::scale_in(conf_change.from_replicas, conf_change.requested_replicas);
    let (survivors, victims) = plan.plan_node_ids();

    json!({
        "id": format!(
            "{}-{}-{}-{}",
            conf_change.kind, conf_change.node_id, conf_change.from_replicas, conf_change.to_replicas
        ),
        "type": conf_change.kind,
        "phase": phase,
        "nodeId": conf_change.node_id,
        "fromReplicas": conf_change.from_replicas,
        "toReplicas": conf_change.to_replicas,
        "requestedReplicas": conf_change.requested_replicas,
        "mode": "RemoveVoter",
        "survivors": survivors,
        "victims": victims,
        "startedAt": started_at,
        "stableSince": null,
        "stabilizationSeconds": cluster.spec.raft.scale_in_stabilization_seconds,
        "lastAttemptAt": now,
        "retryCount": retry_count,
        "lastError": last_error
    })
}

fn scale_in_stabilizing_operation_status_json(
    cluster: &RmqttCluster,
    conf_change: &ConfChangeStatus,
    stable_since: String,
) -> Value {
    let mut operation = scale_in_operation_status_json(cluster, conf_change, "Stabilizing", None, false);
    operation["stableSince"] = json!(stable_since);
    operation["stabilizationSeconds"] = json!(cluster.spec.raft.scale_in_stabilization_seconds);
    operation
}

fn image_rollout_operation_status_json(
    cluster: &RmqttCluster,
    node_id: i32,
    replicas: i32,
    phase: &str,
    stable_since: Option<String>,
    last_error: Option<&str>,
    increment_retry: bool,
) -> Value {
    let previous = cluster
        .status
        .as_ref()
        .and_then(|status| status.operation.as_ref())
        .filter(|operation| is_rollout_operation_kind(&operation.kind));
    let now = utc_now_rfc3339();
    let started_at =
        previous.and_then(|operation| operation.started_at.clone()).unwrap_or_else(|| now.clone());
    let retry_count =
        previous.map(|operation| operation.retry_count).unwrap_or(0) + u32::from(increment_retry);
    let previous_transfer =
        previous.filter(|operation| operation.node_id == node_id && operation.phase == "TransferringLeader");
    let leader_transfer_attempted_at = (phase == "TransferringLeader").then(|| {
        previous_transfer
            .filter(|_| !increment_retry)
            .and_then(|operation| operation.leader_transfer_attempted_at.clone())
            .unwrap_or_else(|| now.clone())
    });
    let leader_transfer_retry_count = if phase == "TransferringLeader" {
        previous_transfer.map(|operation| operation.leader_transfer_retry_count).unwrap_or(0)
            + u32::from(increment_retry)
    } else {
        0
    };
    let stable_since = stable_since.or_else(|| {
        previous
            .filter(|operation| operation.node_id == node_id && operation.phase == phase)
            .and_then(|operation| operation.stable_since.clone())
    });
    let operation_kind = previous.map(|operation| operation.kind.as_str()).unwrap_or_else(|| {
        if cluster.spec.pod_restart.is_some() {
            "PodRestart"
        } else if rollout_is_restart(&cluster.spec) {
            "RollingRestart"
        } else {
            "RollingImageUpdate"
        }
    });
    let operation_mode =
        if rollout_is_restart(&cluster.spec) { "OperatorManagedRestart" } else { "OperatorManagedOnDelete" };
    let operation_id = cluster.spec.pod_restart.as_ref().map_or_else(
        || format!("{operation_kind}-{}-{node_id}", cluster.metadata.generation.unwrap_or_default()),
        |target| format!("PodRestart-{}", target.pod_uid),
    );

    json!({
        "id": operation_id,
        "type": operation_kind,
        "phase": phase,
        "nodeId": node_id,
        "fromReplicas": replicas,
        "toReplicas": replicas,
        "requestedReplicas": cluster.spec.replicas,
        "mode": operation_mode,
        "targetImage": cluster.spec.image,
        "targetImagePullPolicy": cluster.spec.image_pull_policy,
        "targetRestartAt": cluster.spec.rollout_restart_at,
        "targetPodName": cluster.spec.pod_restart.as_ref().map(|target| &target.pod_name),
        "targetPodUid": cluster.spec.pod_restart.as_ref().map(|target| &target.pod_uid),
        "manualRestart": cluster.spec.pod_restart.as_ref().map(|target| target.manual_restart.as_str()),
        "stabilizationSeconds": cluster.spec.image_update.stabilization_seconds,
        "startedAt": started_at,
        "stableSince": stable_since,
        "lastAttemptAt": now,
        "leaderTransferAttemptedAt": leader_transfer_attempted_at,
        "leaderTransferRetryCount": leader_transfer_retry_count,
        "retryCount": retry_count,
        "lastError": last_error
    })
}

fn operation_stabilization_complete(operation: &OperationStatus, seconds: u64) -> bool {
    let Some(stable_since) = operation.stable_since.as_deref() else {
        return false;
    };
    let Ok(stable_since) = DateTime::parse_from_rfc3339(stable_since) else {
        return false;
    };
    Utc::now().signed_duration_since(stable_since.with_timezone(&Utc)).num_seconds() >= seconds as i64
}

fn image_rollout_terminal_target_superseded(operation: &OperationStatus, spec: &ClusterSpec) -> bool {
    operation.kind != "PodRestart"
        && matches!(operation.phase.as_str(), "RolledBack" | "Blocked")
        && (operation.target_image.as_deref() != Some(spec.image.as_str())
            || operation.target_image_pull_policy.as_deref() != Some(spec.image_pull_policy.as_str())
            || operation.target_restart_at != spec.rollout_restart_at)
}

fn utc_now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn kubernetes_microtime(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn membership_status_json(membership: &MembershipStatus, expected_replicas: i32) -> Value {
    json!({
        "id": membership.id,
        "leaderId": membership.leader_id,
        "isLeader": membership.is_leader,
        "membersReady": membership_ready_for_replicas(membership, expected_replicas),
        "available": membership.available,
        "started": membership.started,
        "caughtUp": membership.caught_up,
        "snapshotRestored": membership.snapshot_restored,
        "draining": membership.draining,
        "appliedIndex": membership.applied_index,
        "committedIndex": membership.committed_index,
        "lastIndex": membership.last_index,
        "clientStates": membership.client_states,
        "voters": membership.voters,
        "learners": membership.learners,
        "raftPeers": membership.raft_peers.iter().map(|peer| json!({
            "id": peer.id,
            "available": peer.available,
            "matchedIndex": peer.matched_index,
            "recentActive": peer.recent_active,
            "pendingSnapshot": peer.pending_snapshot,
            "progressState": peer.progress_state,
        })).collect::<Vec<_>>(),
        "businessPeers": membership.business_peers,
        "supports": membership.supports,
        "quorumRecoveryActive": membership.quorum_recovery_active,
        "quorumRecoveryEpoch": membership.quorum_recovery_epoch,
    })
}

fn member_exists(members: &[Value], node_id: i32) -> bool {
    members.iter().any(|m| m.get("id").and_then(Value::as_i64) == Some(i64::from(node_id)))
}

fn contains_all_expected_members(members: &[Value], replicas: i32) -> bool {
    (1..=replicas).all(|node_id| member_exists(members, node_id))
}

fn voter_exists(membership: &MembershipStatus, node_id: i32) -> bool {
    membership.voters.contains(&node_id)
}

fn contains_all_expected_voters(membership: &MembershipStatus, replicas: i32) -> bool {
    (1..=replicas).all(|node_id| voter_exists(membership, node_id))
}

fn node_requiring_membership_repair(membership: &MembershipStatus, replicas: i32) -> Option<i32> {
    (1..=replicas)
        .find(|node_id| membership.learners.contains(node_id) || !voter_exists(membership, *node_id))
}

fn raft_peer_available(membership: &MembershipStatus, node_id: i32) -> bool {
    membership.raft_peers.iter().any(|peer| peer.id == node_id && peer.available)
}

fn raft_peer_caught_up(membership: &MembershipStatus, node_id: i32) -> bool {
    membership.raft_peers.iter().any(|peer| {
        peer.id == node_id
            && peer.available
            && peer.recent_active
            && peer.pending_snapshot == 0
            && peer.matched_index >= membership.committed_index
    })
}

fn removal_preserves_recent_quorum(membership: &MembershipStatus, victim_id: i32) -> bool {
    if !voter_exists(membership, victim_id) {
        return false;
    }
    let remaining =
        membership.voters.iter().copied().filter(|node_id| *node_id != victim_id).collect::<Vec<_>>();
    if remaining.is_empty() {
        return false;
    }
    let quorum = remaining.len() / 2 + 1;
    let recently_active = remaining
        .iter()
        .filter(|node_id| {
            membership.leader_id == Some(**node_id)
                || membership.raft_peers.iter().any(|peer| {
                    peer.id == **node_id && peer.available && peer.recent_active && peer.pending_snapshot == 0
                })
        })
        .count();
    recently_active >= quorum
}

fn contains_all_expected_available_raft_peers(membership: &MembershipStatus, replicas: i32) -> bool {
    (1..=replicas).all(|node_id| raft_peer_available(membership, node_id))
}

fn membership_ready_for_replicas(membership: &MembershipStatus, replicas: i32) -> bool {
    is_authoritative_leader(membership)
        && membership.started
        && membership.available
        && membership.caught_up
        && membership.snapshot_restored
        && !membership.draining
        && contains_all_expected_voters(membership, replicas)
        && contains_all_expected_available_raft_peers(membership, replicas)
        && contains_all_expected_members(&membership.business_peers, replicas)
}

fn scale_in_removal_blocker(
    membership: &MembershipStatus,
    current_replicas: i32,
    victim_id: i32,
) -> Option<String> {
    if !membership_ready_for_replicas(membership, current_replicas) {
        return Some(format!(
            "raft and business membership is no longer ready for {current_replicas} replicas"
        ));
    }
    if membership.leader_id == Some(victim_id) {
        return Some(format!("node {victim_id} became raft leader during drain"));
    }
    if !removal_preserves_recent_quorum(membership, victim_id) {
        return Some(format!(
            "removing node {victim_id} would no longer preserve a recently-active raft quorum"
        ));
    }
    None
}

fn forced_pod_restart_leave_decision(
    membership: &MembershipStatus,
    current_replicas: i32,
    victim_id: i32,
) -> ForcedPodRestartLeaveDecision {
    let still_in_membership = voter_exists(membership, victim_id)
        || membership.learners.contains(&victim_id)
        || member_exists(&membership.business_peers, victim_id);
    if !still_in_membership {
        return ForcedPodRestartLeaveDecision::DeletePod;
    }
    match scale_in_removal_blocker(membership, current_replicas, victim_id) {
        Some(reason) => ForcedPodRestartLeaveDecision::Wait(reason),
        None => ForcedPodRestartLeaveDecision::RequestLeave,
    }
}

struct KubeClient {
    client: Client,
    base_url: String,
    token: String,
}

impl KubeClient {
    async fn in_cluster() -> Result<Self> {
        let host = env::var("KUBERNETES_SERVICE_HOST").context("KUBERNETES_SERVICE_HOST is not set")?;
        let port = env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_string());
        let token = fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")
            .await
            .context("read serviceaccount token")?;
        let ca = fs::read("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
            .await
            .context("read serviceaccount ca.crt")?;
        let cert = reqwest::Certificate::from_pem(&ca).context("parse serviceaccount ca.crt")?;
        let client = Client::builder()
            .add_root_certificate(cert)
            .timeout(Duration::from_secs(20))
            .build()
            .context("build kubernetes http client")?;
        Ok(Self { client, base_url: format!("https://{host}:{port}"), token })
    }

    async fn get_json<T>(&self, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let res = self.send_with_retry(Method::GET, path, None, None).await?;
        self.decode(res, path).await
    }

    async fn get_json_optional<T>(&self, path: &str) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let res = self.send_with_retry(Method::GET, path, None, None).await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        self.decode(res, path).await.map(Some)
    }

    async fn create_json(&self, path: &str, obj: &Value) -> Result<bool> {
        let body = obj.to_string();
        let res = self.send_with_retry(Method::POST, path, Some("application/json"), Some(&body)).await?;
        self.expect_success_or_conflict(res, path).await
    }

    async fn replace_json(&self, path: &str, obj: &Value) -> Result<bool> {
        let body = obj.to_string();
        let res = self.send_with_retry(Method::PUT, path, Some("application/json"), Some(&body)).await?;
        self.expect_success_or_conflict(res, path).await
    }

    async fn apply(&self, path: &str, obj: &Value) -> Result<()> {
        let path = format!("{path}?fieldManager={FIELD_MANAGER}&force=true");
        let body = obj.to_string();
        let res = self
            .send_with_retry(Method::PATCH, &path, Some("application/apply-patch+yaml"), Some(&body))
            .await?;
        self.expect_success(res, &path).await
    }

    async fn merge_patch(&self, path: &str, patch: &Value) -> Result<()> {
        let body = patch.to_string();
        let res = self
            .send_with_retry(Method::PATCH, path, Some("application/merge-patch+json"), Some(&body))
            .await?;
        self.expect_success(res, path).await
    }

    async fn merge_patch_response(&self, path: &str, patch: &Value) -> Result<Option<Value>> {
        let body = patch.to_string();
        let res = self
            .send_with_retry(Method::PATCH, path, Some("application/merge-patch+json"), Some(&body))
            .await?;
        let status = res.status();
        let text = match res.text().await {
            Ok(text) => text,
            Err(error) if status.is_success() => {
                log::warn!(
                    "Kubernetes merge patch succeeded but its response body could not be read; falling back to the requested patch for logging path={} error={}",
                    path,
                    error,
                );
                return Ok(None);
            }
            Err(error) => {
                return Err(anyhow!(
                    "kubernetes request failed: {path}: {status}; response body error: {error}"
                ));
            }
        };
        if !status.is_success() {
            return Err(anyhow!("kubernetes request failed: {path}: {status} {text}"));
        }
        if text.trim().is_empty() {
            log::warn!(
                "Kubernetes merge patch returned an empty success response; falling back to the requested patch for logging path={}",
                path,
            );
            return Ok(None);
        }
        match serde_json::from_str(&text) {
            Ok(resource) => Ok(Some(resource)),
            Err(error) => {
                log::warn!(
                    "Kubernetes merge patch succeeded but its response was not valid JSON; falling back to the requested patch for logging path={} error={}",
                    path,
                    error,
                );
                Ok(None)
            }
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let res = self.send_with_retry(Method::DELETE, path, None, None).await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.expect_success(res, path).await
    }

    async fn delete_with_uid_precondition(&self, path: &str, uid: &str) -> Result<bool> {
        if uid.is_empty() {
            bail!("refusing to delete {path} without a Pod UID precondition");
        }
        let body = pod_delete_options(uid).to_string();
        let res = self.send_with_retry(Method::DELETE, path, Some("application/json"), Some(&body)).await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(true);
        }
        if res.status() == StatusCode::CONFLICT {
            return Ok(false);
        }
        self.expect_success(res, path).await?;
        Ok(true)
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        self.client
            .request(method, url)
            .header(AUTHORIZATION, format!("Bearer {}", self.token.trim()))
            .header(ACCEPT, "application/json")
    }

    async fn send_with_retry(
        &self,
        method: Method,
        path: &str,
        content_type: Option<&str>,
        body: Option<&str>,
    ) -> Result<reqwest::Response> {
        const MAX_ATTEMPTS: usize = 5;
        let mut delay = Duration::from_millis(200);
        for attempt in 1..=MAX_ATTEMPTS {
            let mut request = self.request(method.clone(), path);
            if let Some(content_type) = content_type {
                request = request.header(CONTENT_TYPE, content_type);
            }
            if let Some(body) = body {
                request = request.body(body.to_owned());
            }
            match request.send().await {
                Ok(response)
                    if attempt < MAX_ATTEMPTS
                        && (response.status() == StatusCode::TOO_MANY_REQUESTS
                            || response.status().is_server_error()) =>
                {
                    log::warn!(
                        "transient Kubernetes API response, retrying method={} path={} status={} attempt={}/{}",
                        method,
                        path,
                        response.status(),
                        attempt,
                        MAX_ATTEMPTS
                    );
                }
                Ok(response) => return Ok(response),
                Err(error) if attempt < MAX_ATTEMPTS => {
                    log::warn!(
                        "transient Kubernetes API transport error, retrying method={} path={} error={} attempt={}/{}",
                        method,
                        path,
                        error,
                        attempt,
                        MAX_ATTEMPTS
                    );
                }
                Err(error) => return Err(error.into()),
            }
            sleep(delay).await;
            delay = delay.saturating_mul(2).min(Duration::from_secs(2));
        }
        unreachable!("Kubernetes request retry loop always returns")
    }

    async fn decode<T>(&self, res: reqwest::Response, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if status == StatusCode::NOT_FOUND {
            bail!("404 not found: {path}");
        }
        if !status.is_success() {
            bail!("kubernetes request failed: {path}: {status} {text}");
        }
        serde_json::from_str(&text).with_context(|| format!("decode kubernetes response from {path}: {text}"))
    }

    async fn expect_success(&self, res: reqwest::Response, path: &str) -> Result<()> {
        let status = res.status();
        if status.is_success() {
            return Ok(());
        }
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("kubernetes request failed: {path}: {status} {text}"))
    }

    async fn expect_success_or_conflict(&self, res: reqwest::Response, path: &str) -> Result<bool> {
        let status = res.status();
        if status.is_success() {
            return Ok(true);
        }
        if status == StatusCode::CONFLICT {
            return Ok(false);
        }
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("kubernetes request failed: {path}: {status} {text}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recovery_observation(
        node_id: i32,
        applied_index: u64,
        committed_index: u64,
        client_states: usize,
    ) -> LocalMembershipObservation {
        LocalMembershipObservation {
            node_id,
            membership: MembershipStatus {
                id: node_id,
                started: true,
                caught_up: applied_index >= committed_index,
                snapshot_restored: true,
                applied_index,
                committed_index,
                last_index: applied_index,
                client_states,
                voters: vec![1, 2, 3, 4, 5],
                ..MembershipStatus::default()
            },
        }
    }

    #[test]
    fn recovery_seed_prefers_the_most_advanced_survivor() {
        let observations = vec![
            recovery_observation(1, 100, 100, 50),
            recovery_observation(2, 105, 105, 10),
            recovery_observation(3, 103, 103, 100),
        ];
        assert_eq!(select_recovery_seed(&observations).map(|seed| seed.node_id), Some(2));
    }

    #[test]
    fn recovery_seed_can_preserve_a_surviving_old_leader() {
        let mut old_leader = recovery_observation(2, 105, 105, 20);
        old_leader.membership.is_leader = true;
        old_leader.membership.leader_id = Some(2);
        // check_quorum has made the isolated old leader unavailable, so it is
        // not authoritative for normal changes but its local state is usable.
        old_leader.membership.available = false;
        let observations = vec![recovery_observation(1, 100, 100, 30), old_leader];

        assert!(observations
            .iter()
            .all(|observation| { !is_available_authoritative_leader(&observation.membership) }));
        assert_eq!(select_recovery_seed(&observations).map(|seed| seed.node_id), Some(2));
    }

    #[test]
    fn recovery_seed_uses_the_best_follower_when_the_old_leader_is_gone() {
        let observations = vec![recovery_observation(1, 100, 100, 30), recovery_observation(3, 104, 104, 20)];

        assert!(observations.iter().all(|observation| {
            !observation.membership.is_leader && observation.membership.leader_id.is_none()
        }));
        assert_eq!(select_recovery_seed(&observations).map(|seed| seed.node_id), Some(3));
    }

    #[test]
    fn stale_leader_identity_is_not_accepted_for_control_plane_changes() {
        let mut membership = recovery_observation(1, 100, 100, 10).membership;
        membership.is_leader = true;
        membership.leader_id = Some(1);
        membership.available = false;
        assert!(is_authoritative_leader(&membership));
        assert!(!is_available_authoritative_leader(&membership));

        membership.available = true;
        assert!(is_available_authoritative_leader(&membership));
    }

    #[test]
    fn recovery_seed_uses_client_state_count_only_after_raft_progress() {
        let observations = vec![recovery_observation(1, 100, 100, 10), recovery_observation(2, 100, 100, 20)];
        assert_eq!(select_recovery_seed(&observations).map(|seed| seed.node_id), Some(2));
    }

    #[test]
    fn quorum_recovery_counts_snapshot_restored_voters_even_without_a_leader() {
        let mut observations = vec![
            recovery_observation(1, 100, 100, 10),
            recovery_observation(2, 100, 100, 10),
            recovery_observation(6, 100, 100, 10),
        ];
        observations[0].membership.started = false;
        observations[0].membership.available = false;
        observations[1].membership.snapshot_restored = false;
        assert_eq!(recoverable_voter_count(&observations, &[1, 2, 3, 4, 5]), 1);
        assert_eq!(select_recovery_seed(&observations).map(|seed| seed.node_id), Some(1));
    }

    #[test]
    fn recovery_seed_rejects_unverified_or_mismatched_local_state() {
        let mut no_snapshot = recovery_observation(1, 100, 100, 10);
        no_snapshot.membership.snapshot_restored = false;
        let mut wrong_identity = recovery_observation(2, 110, 110, 20);
        wrong_identity.membership.id = 1;
        let mut no_voters = recovery_observation(3, 120, 120, 30);
        no_voters.membership.voters.clear();
        let observations = vec![no_snapshot, wrong_identity, no_voters];

        assert!(select_recovery_seed(&observations).is_none());
        assert_eq!(recoverable_voter_count(&observations, &[1, 2, 3, 4, 5]), 0);
        assert_eq!(recovery_old_voters(&observations, 5), vec![1, 2, 3, 4, 5]);
        assert!(recovery_survivor_node_ids(&observations).is_empty());
        assert_eq!(recovery_rejected_node_ids(&observations), vec![1, 2, 3]);
    }

    #[test]
    fn quorum_recovery_is_an_internal_availability_policy() {
        assert_eq!(QUORUM_RECOVERY_DETECTION_SECONDS, 30);
        assert_eq!(QUORUM_NATURAL_RECOVERY_SECONDS, 60);
        assert_eq!(quorum_recovery_wait_seconds(), 90);

        // A stale quorumRecovery field from an older CRD is ignored. Recovery
        // behavior no longer depends on a per-cluster opt-in.
        let spec: ClusterSpec = serde_json::from_value(json!({
            "raft": {
                "quorumRecovery": {
                    "enabled": false,
                    "action": "Wait",
                    "allowEmptyBootstrap": false
                }
            }
        }))
        .unwrap();
        validate_spec(&spec).unwrap();
    }

    fn recovery_ready_pod(node_id: i32) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": format!("rmqtt-{}", node_id - 1)},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }))
        .unwrap()
    }

    fn healthy_recovery_observation(node_id: i32, leader_id: i32) -> LocalMembershipObservation {
        let mut observation = recovery_observation(node_id, 100, 100, 10);
        observation.membership.leader_id = Some(leader_id);
        observation.membership.available = true;
        observation.membership.business_peers = vec![json!({"id": node_id})];
        observation
    }

    #[test]
    fn recovery_health_requires_both_ready_pod_and_responsive_local_membership() {
        let pods = vec![recovery_ready_pod(2), recovery_ready_pod(3)];
        let observations = vec![healthy_recovery_observation(2, 1)];

        assert!(recovery_node_healthy(&pods, "rmqtt", &observations, 2, 1));
        // A Ready Pod is not enough when its local membership request timed
        // out. This is the regression that previously allowed recovery to
        // move on and strand the next OrderedReady ordinal.
        assert!(!recovery_node_healthy(&pods, "rmqtt", &observations, 3, 1));
    }

    #[test]
    fn recovery_auto_recycle_requires_both_independent_failure_signals() {
        assert!(!recovery_node_auto_recycle_candidate(true, true));
        assert!(!recovery_node_auto_recycle_candidate(true, false));
        assert!(!recovery_node_auto_recycle_candidate(false, true));
        assert!(recovery_node_auto_recycle_candidate(false, false));
    }

    #[test]
    fn recovery_pauses_when_a_previously_rebuilt_node_regresses() {
        let pods = vec![recovery_ready_pod(1), recovery_ready_pod(2), recovery_ready_pod(3)];
        let observations = vec![healthy_recovery_observation(1, 1), healthy_recovery_observation(2, 1)];
        let recovery = QuorumRecoveryStatus {
            seed_node_id: Some(1),
            restarted_node_ids: vec![2, 3],
            restarting_node_id: Some(4),
            ..QuorumRecoveryStatus::default()
        };

        assert_eq!(unhealthy_rebuilt_node_ids(&recovery, &pods, "rmqtt", &observations, 1), vec![3]);
    }

    #[test]
    fn recovery_requires_a_continuous_node_stabilization_window() {
        let recovery = QuorumRecoveryStatus {
            stabilizing_node_id: Some(2),
            stabilizing_started_at: Some(
                (Utc::now() - chrono::Duration::seconds(QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS as i64))
                    .to_rfc3339(),
            ),
            ..QuorumRecoveryStatus::default()
        };

        assert!(
            recovery_node_stabilization_elapsed(&recovery, 2).unwrap_or_default()
                >= QUORUM_RECOVERY_NODE_STABILIZATION_SECONDS
        );
        assert!(recovery_node_stabilization_elapsed(&recovery, 3).is_none());
    }

    #[test]
    fn promoted_recovery_node_enters_stabilization_instead_of_running() {
        let mut recovery = QuorumRecoveryStatus {
            phase: "RebuildingQuorum".to_string(),
            restarting_node_id: Some(5),
            last_error: Some("old error".to_string()),
            ..QuorumRecoveryStatus::default()
        };

        begin_recovery_node_stabilization(&mut recovery, 5);

        assert_eq!(recovery.phase, "StabilizingRecoveryNode");
        assert_ne!(recovery.phase, "Running");
        assert_eq!(recovery.restarting_node_id, Some(5));
        assert_eq!(recovery.stabilizing_node_id, Some(5));
        assert!(recovery.stabilizing_started_at.is_some());
        assert!(recovery.last_error.is_none());
    }

    #[test]
    fn recovery_recycles_only_when_the_current_quorum_survives_the_restart() {
        let recovery = QuorumRecoveryStatus { seed_node_id: Some(1), ..Default::default() };
        let three_voters = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            voters: vec![1, 2, 3],
            raft_peers: vec![RaftPeerStatus {
                id: 2,
                available: true,
                recent_active: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(recovery_node_recycle_blocker(&recovery, &three_voters, 3).is_none());

        let two_voters = MembershipStatus { voters: vec![1, 2], ..three_voters.clone() };
        assert!(recovery_node_recycle_blocker(&recovery, &two_voters, 2)
            .is_some_and(|blocker| blocker.contains("still requires quorum 2")));
    }

    #[test]
    fn recovery_never_recycles_its_seed_or_leader() {
        let recovery = QuorumRecoveryStatus { seed_node_id: Some(1), ..Default::default() };
        let membership =
            MembershipStatus { id: 1, leader_id: Some(1), voters: vec![1, 2, 3], ..Default::default() };

        assert!(recovery_node_recycle_blocker(&recovery, &membership, 1)
            .is_some_and(|blocker| blocker.contains("recovery seed")));
    }

    #[test]
    fn recovery_seed_validation_requires_the_same_authoritative_epoch() {
        let recovery = QuorumRecoveryStatus { epoch: 42, seed_node_id: Some(1), ..Default::default() };
        let valid = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            started: true,
            available: true,
            quorum_recovery_epoch: 42,
            ..Default::default()
        };
        assert!(recovery_seed_membership_blocker(&recovery, &valid).is_none());

        let wrong_leader = MembershipStatus { id: 2, leader_id: Some(2), ..valid.clone() };
        assert!(recovery_seed_membership_blocker(&recovery, &wrong_leader)
            .is_some_and(|blocker| blocker.contains("recovery seed")));

        let wrong_epoch = MembershipStatus { quorum_recovery_epoch: 41, ..valid };
        assert!(recovery_seed_membership_blocker(&recovery, &wrong_epoch)
            .is_some_and(|blocker| blocker.contains("operator recovery epoch is 42")));
    }

    #[test]
    fn recovery_pod_delete_uses_a_uid_precondition() {
        let options = pod_delete_options("pod-uid-123");
        assert_eq!(options["kind"], "DeleteOptions");
        assert_eq!(options["preconditions"]["uid"], "pod-uid-123");
    }

    #[test]
    fn recovery_unhealthy_window_and_recycle_attempts_are_persisted() {
        let mut recycle_attempts = BTreeMap::new();
        recycle_attempts.insert("3".to_string(), 2);
        let recovery = QuorumRecoveryStatus {
            unhealthy_node_id: Some(3),
            unhealthy_started_at: Some(
                (Utc::now() - chrono::Duration::seconds(QUORUM_RECOVERY_NODE_UNHEALTHY_GRACE_SECONDS as i64))
                    .to_rfc3339(),
            ),
            recycling_node_id: Some(3),
            recycling_pod_uid: Some("old-pod-uid".to_string()),
            recycle_attempts,
            ..Default::default()
        };

        assert!(
            recovery_node_unhealthy_elapsed(&recovery, 3).unwrap_or_default()
                >= QUORUM_RECOVERY_NODE_UNHEALTHY_GRACE_SECONDS
        );
        assert_eq!(recovery_node_recycle_attempts(&recovery, 3), 2);
        assert_eq!(recovery_node_recycle_attempts(&recovery, 4), 0);
        let status = quorum_recovery_status_json(&recovery);
        assert_eq!(status["recyclingPodUid"], "old-pod-uid");
        assert_eq!(status["recycleAttempts"]["3"], 2);
    }

    #[test]
    fn recovery_scan_logs_on_change_or_heartbeat() {
        let now = Instant::now();
        let signature = json!({"unreachableNodeIds": [3, 4, 5]});
        assert!(recovery_scan_log_due(None, &signature, now));

        let recent = RecoveryScanLogEntry { signature: signature.clone(), logged_at: now };
        assert!(!recovery_scan_log_due(Some(&recent), &signature, now));
        assert!(recovery_scan_log_due(Some(&recent), &json!({"unreachableNodeIds": [4, 5]}), now));

        let stale = RecoveryScanLogEntry {
            signature: signature.clone(),
            logged_at: now - Duration::from_secs(RECOVERY_SCAN_LOG_HEARTBEAT_SECONDS),
        };
        assert!(recovery_scan_log_due(Some(&stale), &signature, now));
    }

    #[test]
    fn client_service_publishes_not_ready_addresses_only_for_verified_degraded_mode() {
        let normal: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {"phase": "Running"}
        }))
        .unwrap();
        assert!(!degraded_client_endpoints_enabled(&normal));
        assert_eq!(
            client_service_object("rmqtt-test", &normal, false)["spec"]["publishNotReadyAddresses"],
            false
        );

        let transient: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {"phase": "WaitingRaftLeader"}
        }))
        .unwrap();
        assert!(!degraded_client_endpoints_enabled(&transient));

        let observation_only: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {"phase": "DegradedServing"}
        }))
        .unwrap();
        assert!(degraded_client_endpoints_enabled(&observation_only));

        let unverified_recovery: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {
                "phase": "RecoveryBlocked",
                "quorumRecovery": {"phase": "RecoveryBlocked"}
            }
        }))
        .unwrap();
        assert!(!degraded_client_endpoints_enabled(&unverified_recovery));

        let recovering: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {
                "phase": "RecoveryBlocked",
                "quorumRecovery": {
                    "phase": "RecoveryBlocked",
                    "survivorNodeIds": [1]
                }
            }
        }))
        .unwrap();
        assert!(degraded_client_endpoints_enabled(&recovering));
        assert_eq!(
            client_service_object("rmqtt-test", &recovering, true)["spec"]["publishNotReadyAddresses"],
            true
        );
    }

    #[test]
    fn recovery_does_not_wait_for_a_learner_before_its_pod_exists() {
        let pods = vec![recovery_ready_pod(1), recovery_ready_pod(2)];

        assert_eq!(missing_recovery_target_blockers(&pods, "rmqtt", 4), Some(vec![3]));

        let mut with_target = pods;
        with_target.push(recovery_ready_pod(4));
        assert_eq!(missing_recovery_target_blockers(&with_target, "rmqtt", 4), None);
    }

    #[test]
    fn status_log_names_only_the_primary_target_and_existing_leader() {
        let routing = [RoutingStatus { id: 3, target_id: 5, handoff_pending: 1, ..Default::default() }];
        let blockers = drain_blockers(&DrainStatus::default(), &routing);
        assert!(blockers.contains("node 3 still sees target 5"));
        assert!(!blockers.contains("pod_name"));
        let status = json!({
            "phase": "DrainingNode",
            "leaderId": 2,
            "operation": {"nodeId": 5},
            "drainingNodeId": 5,
            "message": blockers,
        });
        let details = status_transition_details("studio-ams-system", "ams-rmqtt", &status);
        assert!(details.contains("target_role=operation target_node_id=5 target_pod_name=ams-rmqtt-4"));
        assert!(details.contains("leader_id=2 leader_pod_name=ams-rmqtt-1"));
        assert!(details.contains("node 3 still sees target 5"));
        assert!(!details.contains("ams-rmqtt-2"));
        assert_eq!(details.matches("pod_name=").count(), 2);
    }

    #[test]
    fn status_log_target_prefers_foreground_recovery_over_pending_join() {
        let mut status = json!({
            "operation": {"nodeId": 5},
            "quorumRecovery": {
                "seedNodeId": 1,
                "restartingNodeId": 5,
                "stabilizingNodeId": 4,
                "unhealthyNodeId": 3,
                "recyclingNodeId": 2,
            },
        });
        for (role, id, key) in [
            ("recycling", 2, "recyclingNodeId"),
            ("unhealthy", 3, "unhealthyNodeId"),
            ("stabilizing", 4, "stabilizingNodeId"),
            ("restarting", 5, "restartingNodeId"),
        ] {
            assert_eq!(status_log_target(&status), Some((role, id)));
            let details = status_transition_details("ns", "rmqtt", &status);
            assert_eq!(details.matches("target_pod_name=").count(), 1);
            assert!(details.contains(&format!("target_pod_name=rmqtt-{}", id - 1)));
            status["quorumRecovery"][key] = Value::Null;
        }
        assert_eq!(status_log_target(&status), Some(("operation", 5)));
        status["operation"] = Value::Null;
        assert_eq!(status_log_target(&status), Some(("seed", 1)));
    }

    #[test]
    fn status_log_target_handles_drain_and_membership_change_without_an_operation() {
        let mut status = json!({"drainingNodeId": 5, "confChange": {"nodeId": 4}});
        assert_eq!(status_log_target(&status), Some(("draining", 5)));
        status["drainingNodeId"] = Value::Null;
        assert_eq!(status_log_target(&status), Some(("conf_change", 4)));
    }

    #[test]
    fn status_log_target_never_maps_invalid_ids_or_member_lists_to_pods() {
        for invalid_id in [Value::Null, json!(0), json!(-1), json!(2147483648i64), json!("5")] {
            let status = json!({
                "leaderId": invalid_id,
                "operation": {"nodeId": invalid_id},
                "quorumRecovery": {"survivorNodeIds": [1, 2, 3]},
            });
            assert_eq!(status_log_target(&status), None);
            let details = status_transition_details("ns", "rmqtt", &status);
            assert!(details.contains("target_role=- target_node_id=- target_pod_name=-"));
            assert!(details.contains("leader_pod_name=-"));
        }
        assert_eq!(status_log_target(&json!({"leaderId": 2})), None);
        assert_eq!(status_log_target(&json!({})), None);
    }

    #[test]
    fn status_log_signature_deduplicates_message_only_updates() {
        let first = json!({
            "phase": "WaitingRaftCatchUp",
            "message": "learner 4 matched index 10",
            "observedGeneration": 2,
            "replicas": 4,
            "readyReplicas": 3,
            "desiredReplicas": 5,
            "leaderId": 1,
            "operation": {
                "type": "AddLearnerAndPromote",
                "phase": "CatchingUp",
                "mode": "LearnerCatchUp",
                "nodeId": 4
            }
        });
        let message_progress = json!({
            "phase": "WaitingRaftCatchUp",
            "message": "learner 4 matched index 20",
            "observedGeneration": 2,
            "replicas": 4,
            "readyReplicas": 3,
            "desiredReplicas": 5,
            "leaderId": 1,
            "operation": {
                "type": "AddLearnerAndPromote",
                "phase": "CatchingUp",
                "mode": "LearnerCatchUp",
                "nodeId": 4
            }
        });
        let promoted = json!({
            "phase": "PromotingRaftLearner",
            "message": "learner 4 caught up and promotion was requested",
            "observedGeneration": 2,
            "replicas": 4,
            "readyReplicas": 3,
            "desiredReplicas": 5,
            "leaderId": 1,
            "operation": {
                "type": "AddLearnerAndPromote",
                "phase": "Promoting",
                "mode": "LearnerCatchUp",
                "nodeId": 4
            }
        });

        assert_eq!(status_log_signature(&first), status_log_signature(&message_progress));
        assert_ne!(status_log_signature(&first), status_log_signature(&promoted));
    }

    #[test]
    fn status_transition_log_uses_complete_server_status() {
        let requested_status = json!({
            "phase": "PromotingRaftLearner",
            "operation": {"type": "AddLearnerAndPromote", "nodeId": 5}
        });
        let patched_resource = json!({
            "status": {
                "phase": "PromotingRaftLearner",
                "operation": {"type": "AddLearnerAndPromote", "nodeId": 5},
                "quorumRecovery": {
                    "epoch": 42,
                    "phase": "RebuildingQuorum",
                    "seedNodeId": 1,
                    "restartingNodeId": 5
                }
            }
        });

        let logged = status_for_transition_log(&requested_status, Some(&patched_resource));

        assert_eq!(status_field(logged, "/quorumRecovery/epoch"), "42");
        assert_eq!(status_field(logged, "/quorumRecovery/phase"), "RebuildingQuorum");
        assert_eq!(status_field(logged, "/quorumRecovery/restartingNodeId"), "5");
    }

    #[test]
    fn status_transition_log_reflects_server_side_recovery_clear() {
        let requested_status = json!({
            "phase": "Running",
            "quorumRecovery": null
        });
        let patched_resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        let logged = status_for_transition_log(&requested_status, Some(&patched_resource));

        assert_eq!(status_field(logged, "/quorumRecovery/phase"), "-");
    }

    #[test]
    fn stateful_set_bootstrap_annotation_is_explicit() {
        let enabled: StatefulSet = serde_json::from_value(json!({
            "metadata": {"annotations": {"rmqtt.io/raft-bootstrap-allowed": "true"}},
            "spec": {"replicas": 1},
        }))
        .unwrap();
        assert_eq!(enabled.replicas(), 1);
        assert!(enabled.bootstrap_allowed());

        let disabled: StatefulSet = serde_json::from_value(json!({
            "metadata": {"annotations": {"rmqtt.io/raft-bootstrap-allowed": "false"}},
            "spec": {"replicas": 5},
        }))
        .unwrap();
        assert_eq!(disabled.replicas(), 5);
        assert!(!disabled.bootstrap_allowed());

        let legacy: StatefulSet = serde_json::from_value(json!({"spec": {"replicas": 3}})).unwrap();
        assert!(!legacy.bootstrap_allowed());
    }

    #[test]
    fn stateful_set_detects_broker_template_image_drift() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:new",
            "imagePullPolicy": "IfNotPresent",
        }))
        .unwrap();
        let matching: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "metadata": {"annotations": {}},
                    "spec": {
                        "enableServiceLinks": false,
                        "terminationGracePeriodSeconds": 30,
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:new",
                            "imagePullPolicy": "IfNotPresent"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(matching.broker_image_matches(&spec));
        assert!(matching.broker_template_matches(&spec));

        let stale_image: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "metadata": {"annotations": {}},
                    "spec": {
                        "enableServiceLinks": false,
                        "terminationGracePeriodSeconds": 30,
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:old",
                            "imagePullPolicy": "IfNotPresent"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(!stale_image.broker_template_matches(&spec));

        let stale_pull_policy: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "metadata": {"annotations": {}},
                    "spec": {
                        "enableServiceLinks": false,
                        "terminationGracePeriodSeconds": 30,
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:new",
                            "imagePullPolicy": "Always"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(!stale_pull_policy.broker_template_matches(&spec));
    }

    #[test]
    fn stateful_set_detects_pod_scheduling_template_drift() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:test",
            "imagePullPolicy": "IfNotPresent",
            "pod": {
                "annotations": {"ams.studio.sensecore.cn/scheduler": "volcano"},
                "serviceAccountName": "ams-rmqtt",
                "imagePullSecrets": [{"name": "image-pull-secret-public"}],
                "schedulerName": "default-scheduler",
                "affinity": {
                    "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {}},
                    "podAntiAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [{
                            "labelSelector": {"matchLabels": {"app.kubernetes.io/name": "rmqtt"}},
                            "topologyKey": "kubernetes.io/hostname"
                        }]
                    }
                },
                "tolerations": [{"key": "dedicated", "operator": "Exists"}],
                "enableServiceLinks": false,
                "terminationGracePeriodSeconds": 30
            }
        }))
        .unwrap();
        let matching: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "ams.studio.sensecore.cn/scheduler": "volcano",
                            "kubectl.kubernetes.io/restartedAt": "2026-09-04T14:01:31+08:00"
                        }
                    },
                    "spec": {
                        "serviceAccountName": "ams-rmqtt",
                        "imagePullSecrets": [{"name": "image-pull-secret-public"}],
                        "schedulerName": "default-scheduler",
                        "affinity": {
                            "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {}},
                            "podAntiAffinity": {
                                "requiredDuringSchedulingIgnoredDuringExecution": [{
                                    "labelSelector": {"matchLabels": {"app.kubernetes.io/name": "rmqtt"}},
                                    "topologyKey": "kubernetes.io/hostname"
                                }]
                            }
                        },
                        "tolerations": [{"key": "dedicated", "operator": "Exists"}],
                        "enableServiceLinks": false,
                        "terminationGracePeriodSeconds": 30,
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:test",
                            "imagePullPolicy": "IfNotPresent"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(matching.broker_template_matches(&spec));

        let missing_pod_anti_affinity: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {"ams.studio.sensecore.cn/scheduler": "volcano"}
                    },
                    "spec": {
                        "serviceAccountName": "ams-rmqtt",
                        "imagePullSecrets": [{"name": "image-pull-secret-public"}],
                        "schedulerName": "default-scheduler",
                        "affinity": {
                            "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {}}
                        },
                        "tolerations": [{"key": "dedicated", "operator": "Exists"}],
                        "enableServiceLinks": false,
                        "terminationGracePeriodSeconds": 30,
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:test",
                            "imagePullPolicy": "IfNotPresent"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(missing_pod_anti_affinity.broker_image_matches(&spec));
        assert!(!missing_pod_anti_affinity.broker_template_matches(&spec));
    }

    #[test]
    fn production_compatibility_fields_deserialize_and_render() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "replicas": 3,
            "image": "example/rmqtt:test",
            "imagePullPolicy": "IfNotPresent",
            "service": {
                "mqttPort": 1883,
                "mqttsPort": 8883,
                "wsPort": 8080,
                "wssPort": 8083,
                "httpApiPort": 6060,
                "grpcPort": 5363,
                "raftPort": 6003
            },
            "config": {"baseConfigMapName": "ams-rmqtt-base-config"},
            "resources": {
                "requests": {"cpu": "8", "memory": "8Gi"},
                "limits": {"cpu": "16", "memory": "16Gi"}
            },
            "pod": {
                "annotations": {"ams.studio.sensecore.cn/scheduler": "volcano"},
                "serviceAccountName": "ams-rmqtt",
                "imagePullSecrets": [{"name": "image-pull-secret-public"}],
                "schedulerName": "default-scheduler",
                "enableServiceLinks": false,
                "terminationGracePeriodSeconds": 30,
                "affinity": {"nodeAffinity": {}},
                "tolerations": [{"key": "dedicated", "operator": "Exists"}]
            },
            "monitoring": {
                "serviceMonitor": {
                    "enabled": true,
                    "labels": {"release": "kube-prometheus-stack"}
                }
            },
            "raft": {"minReplicas": 3}
        }))
        .unwrap();

        validate_spec(&spec).unwrap();
        assert_eq!(spec.service.mqtts_port, 8883);
        assert_eq!(spec.service.ws_port, 8080);
        assert_eq!(spec.service.wss_port, 8083);
        assert_eq!(spec.config.base_config_map_name.as_deref(), Some("ams-rmqtt-base-config"));
        assert_eq!(spec.pod.service_account_name.as_deref(), Some("ams-rmqtt"));
        assert!(spec.monitoring.service_monitor.enabled);

        let resources = resource_requirements_json(spec.resources.as_ref().unwrap());
        assert_eq!(resources["requests"]["cpu"], "8");
        assert_eq!(resources["limits"]["memory"], "16Gi");

        let client_ports = client_service_ports(&spec.service);
        let names = client_ports
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|port| port.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["mqtt", "mqtts", "ws", "wss", "http-api"]);
    }

    #[test]
    fn render_script_overlays_base_config_and_dynamic_listener_ports() {
        assert!(RENDER_SCRIPT.contains("RMQTT_BASE_CONFIG_DIR"));
        assert!(RENDER_SCRIPT.contains("listener.tls.external.addr"));
        assert!(RENDER_SCRIPT.contains("listener.ws.external.addr"));
        assert!(RENDER_SCRIPT.contains("listener.wss.external.addr"));
        assert!(RENDER_SCRIPT.contains("rmqtt-cluster-broadcast.toml"));
        assert!(RENDER_SCRIPT.contains("health.exit_on_node_unavailable = false"));
    }

    #[test]
    fn stateful_set_preserves_runtime_and_scheduling_compatibility() {
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "ams-rmqtt"},
            "spec": {
                "replicas": 3,
                "image": "example/rmqtt:test",
                "imagePullPolicy": "IfNotPresent",
                "config": {"baseConfigMapName": "ams-rmqtt-base-config"},
                "resources": {
                    "requests": {"cpu": "8", "memory": "8Gi"},
                    "limits": {"cpu": "16", "memory": "16Gi"}
                },
                "pod": {
                    "annotations": {"ams.studio.sensecore.cn/scheduler": "volcano"},
                    "serviceAccountName": "ams-rmqtt",
                    "imagePullSecrets": [
                        {"name": "image-pull-secret-stack"},
                        {"name": "image-pull-secret-public"}
                    ],
                    "schedulerName": "default-scheduler",
                    "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {}}},
                    "tolerations": [{"key": "dedicated", "operator": "Exists"}]
                }
            }
        }))
        .unwrap();

        let object = stateful_set_object("studio-ams-system", &cluster, 3, false);
        assert_eq!(object["spec"]["podManagementPolicy"], "OrderedReady");
        assert_eq!(object["spec"]["updateStrategy"]["type"], "OnDelete");

        let pod = &object["spec"]["template"]["spec"];
        assert_eq!(pod["serviceAccountName"], "ams-rmqtt");
        assert_eq!(pod["schedulerName"], "default-scheduler");
        assert_eq!(pod["imagePullSecrets"].as_array().unwrap().len(), 2);

        let container = &pod["containers"][0];
        assert_eq!(container["resources"]["requests"]["cpu"], "8");
        assert_eq!(container["resources"]["limits"]["memory"], "16Gi");
        assert_eq!(container["startupProbe"]["tcpSocket"]["port"], "http-api");
        assert_eq!(container["livenessProbe"]["tcpSocket"]["port"], "mqtt");
        assert_eq!(container["readinessProbe"]["httpGet"]["path"], "/api/v1/health/ready");

        let port_names = container["ports"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|port| port.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            port_names,
            vec!["mqtt", "mqtts", "ws", "wss", "http-api", "cluster-grpc", "cluster-raft"]
        );
        assert!(pod["volumes"].as_array().unwrap().iter().any(|volume| volume["name"] == "base-config"));
    }

    #[test]
    fn membership_readiness_requires_all_expected_nodes() {
        let membership = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            voters: vec![1, 2, 3],
            learners: vec![],
            raft_peers: vec![
                RaftPeerStatus { id: 1, available: true, ..Default::default() },
                RaftPeerStatus { id: 2, available: true, ..Default::default() },
                RaftPeerStatus { id: 3, available: true, ..Default::default() },
            ],
            business_peers: vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
            supports: Value::Null,
            ..Default::default()
        };
        assert!(membership_ready_for_replicas(&membership, 3));
        assert!(!membership_ready_for_replicas(&membership, 4));
    }

    #[test]
    fn membership_readiness_requires_authoritative_leader_and_available_peers() {
        let mut membership = MembershipStatus {
            id: 2,
            leader_id: Some(1),
            is_leader: false,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            voters: vec![1, 2, 3],
            learners: vec![],
            raft_peers: vec![
                RaftPeerStatus { id: 1, available: true, ..Default::default() },
                RaftPeerStatus { id: 2, available: true, ..Default::default() },
                RaftPeerStatus { id: 3, available: true, ..Default::default() },
            ],
            business_peers: vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
            supports: Value::Null,
            ..Default::default()
        };
        assert!(!membership_ready_for_replicas(&membership, 3));

        membership.id = 1;
        membership.is_leader = true;
        membership.raft_peers[2].available = false;
        assert!(!membership_ready_for_replicas(&membership, 3));
    }

    #[test]
    fn membership_repair_detects_restarted_learner_without_replica_change() {
        let membership = MembershipStatus { voters: vec![1, 2], learners: vec![3], ..Default::default() };
        assert_eq!(node_requiring_membership_repair(&membership, 3), Some(3));

        let repaired = MembershipStatus { voters: vec![1, 2, 3], ..Default::default() };
        assert_eq!(node_requiring_membership_repair(&repaired, 3), None);
    }

    #[test]
    fn raft_peer_progress_uses_membership_camel_case_fields() {
        let peer: RaftPeerStatus = serde_json::from_value(json!({
            "id": 3,
            "available": true,
            "matchedIndex": 42,
            "recentActive": true,
            "pendingSnapshot": 0,
            "progressState": "Replicate"
        }))
        .unwrap();
        assert_eq!(peer.matched_index, 42);
        assert!(peer.recent_active);
        assert_eq!(peer.progress_state, "Replicate");
    }

    #[test]
    fn learner_catch_up_rejects_pending_snapshot_or_stale_match() {
        let mut membership = MembershipStatus {
            committed_index: 42,
            raft_peers: vec![RaftPeerStatus {
                id: 3,
                available: true,
                matched_index: 42,
                recent_active: true,
                pending_snapshot: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(raft_peer_caught_up(&membership, 3));

        membership.raft_peers[0].pending_snapshot = 40;
        assert!(!raft_peer_caught_up(&membership, 3));
        membership.raft_peers[0].pending_snapshot = 0;
        membership.raft_peers[0].matched_index = 41;
        assert!(!raft_peer_caught_up(&membership, 3));
    }

    #[test]
    fn learner_promotion_precedes_local_http_and_business_readiness() {
        let membership = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            committed_index: 42,
            voters: vec![1, 2],
            learners: vec![3],
            raft_peers: vec![RaftPeerStatus {
                id: 3,
                available: true,
                matched_index: 42,
                recent_active: true,
                pending_snapshot: 0,
                ..Default::default()
            }],
            // A joining learner is intentionally absent from businessPeers
            // and Pod readiness until after it becomes a voter and completes
            // business NodeUp. Neither may be a precondition for promotion.
            business_peers: Vec::new(),
            ..Default::default()
        };

        assert_eq!(classify_node_join(&membership, 3, 2, 3), NodeJoinStep::Promote);
    }

    #[test]
    fn scale_out_and_image_replacement_share_node_join_classification() {
        let mut membership = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            committed_index: 42,
            voters: vec![1, 2],
            learners: vec![3],
            raft_peers: vec![
                RaftPeerStatus {
                    id: 1,
                    available: true,
                    matched_index: 42,
                    recent_active: true,
                    ..Default::default()
                },
                RaftPeerStatus {
                    id: 2,
                    available: true,
                    matched_index: 42,
                    recent_active: true,
                    ..Default::default()
                },
                RaftPeerStatus {
                    id: 3,
                    available: true,
                    matched_index: 41,
                    recent_active: true,
                    ..Default::default()
                },
            ],
            business_peers: vec![json!({"id": 1}), json!({"id": 2})],
            ..Default::default()
        };
        assert_eq!(classify_node_join(&membership, 3, 2, 3), NodeJoinStep::CatchingUp);

        membership.raft_peers[2].matched_index = 42;
        assert_eq!(classify_node_join(&membership, 3, 2, 3), NodeJoinStep::Promote);

        membership.learners.clear();
        membership.voters.push(3);
        assert_eq!(classify_node_join(&membership, 3, 2, 3), NodeJoinStep::WaitingReady);

        membership.business_peers.push(json!({"id": 3}));
        assert_eq!(classify_node_join(&membership, 3, 3, 3), NodeJoinStep::Ready);
    }

    #[test]
    fn membership_repair_target_completion_does_not_wait_for_another_learner() {
        let membership = MembershipStatus {
            id: 5,
            leader_id: Some(5),
            is_leader: true,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            committed_index: 42,
            voters: vec![1, 2, 3, 5],
            learners: vec![4],
            raft_peers: (1..=5)
                .map(|id| RaftPeerStatus {
                    id,
                    available: true,
                    matched_index: 42,
                    recent_active: true,
                    pending_snapshot: 0,
                    ..Default::default()
                })
                .collect(),
            business_peers: (1..=5).map(|id| json!({"id": id})).collect(),
            ..Default::default()
        };

        // Node 3 is the active repair target and its Pod is one of the four
        // Ready Pods. Node 4 must not keep node 3's operation lock alive;
        // after node 3 completes, the next reconcile repairs learner 4.
        assert_eq!(classify_node_join(&membership, 3, 4, 5), NodeJoinStep::WaitingReady);
        assert_eq!(classify_membership_repair_node_join(&membership, 3, true), NodeJoinStep::Ready);
        assert_eq!(node_requiring_membership_repair(&membership, 5), Some(4));

        let mut unavailable_leader = membership.clone();
        unavailable_leader.available = false;
        assert_eq!(
            classify_membership_repair_node_join(&unavailable_leader, 3, true),
            NodeJoinStep::WaitingReady
        );
    }

    #[test]
    fn membership_repair_still_waits_for_its_target_pod() {
        let membership = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            committed_index: 42,
            voters: vec![1, 2, 3],
            raft_peers: vec![RaftPeerStatus {
                id: 3,
                available: true,
                matched_index: 42,
                recent_active: true,
                pending_snapshot: 0,
                ..Default::default()
            }],
            business_peers: vec![json!({"id": 3})],
            ..Default::default()
        };

        assert_eq!(classify_membership_repair_node_join(&membership, 3, false), NodeJoinStep::WaitingReady);
    }

    #[test]
    fn drain_requires_session_handoff_when_plugin_reports_it() {
        let migrated = DrainStatus {
            safe_to_leave: true,
            strict_safe_to_leave: Some(true),
            session_handoff_complete: Some(true),
            ..Default::default()
        };
        assert!(migrated.scale_in_safe_to_leave());

        let pending = DrainStatus {
            safe_to_leave: true,
            strict_safe_to_leave: Some(true),
            session_handoff_complete: Some(false),
            ..Default::default()
        };
        assert!(!pending.scale_in_safe_to_leave());

        let discarded = DrainStatus {
            safe_to_leave: true,
            strict_safe_to_leave: Some(true),
            session_handoff_complete: Some(true),
            data_loss_detected: true,
            ..Default::default()
        };
        assert!(!discarded.scale_in_safe_to_leave());
    }

    #[test]
    fn image_rollout_recovers_only_drain_timeout_blocks() {
        let legacy_timeout: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "Blocked",
            "nodeId": 1,
            "lastError": IMAGE_DRAIN_TIMEOUT_ERROR
        }))
        .unwrap();
        assert!(image_rollout_waiting_for_drain_after_timeout(&legacy_timeout));

        let waiting: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "WaitingDrainAfterTimeout",
            "nodeId": 1,
            "lastError": "temporary drain status error"
        }))
        .unwrap();
        assert!(image_rollout_waiting_for_drain_after_timeout(&waiting));

        let data_loss: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "Blocked",
            "nodeId": 1,
            "lastError": "drain reported data_loss_detected=true"
        }))
        .unwrap();
        assert!(!image_rollout_waiting_for_drain_after_timeout(&data_loss));
    }

    #[test]
    fn drain_blockers_include_live_counts() {
        let status = DrainStatus {
            blocking_reasons: vec!["sessions".to_string(), "message_queues".to_string()],
            blocking_counts: BTreeMap::from([
                ("sessions".to_string(), Some(1)),
                ("message_queues".to_string(), Some(295)),
            ]),
            ..Default::default()
        };
        assert_eq!(drain_blockers(&status, &[]), "blocking: sessions=1, message_queues=295");
    }

    #[test]
    fn handoff_actions_put_the_target_at_the_safe_edge_of_each_broadcast() {
        assert_eq!(handoff_action_node_ids(5, "prepare_drain", 3), vec![3, 1, 2, 4, 5]);
        assert_eq!(handoff_action_node_ids(5, "activate_drain", 3), vec![1, 2, 4, 5, 3]);
        assert_eq!(handoff_action_node_ids(5, "clear_drain", 3), vec![1, 2, 4, 5, 3]);
        assert_eq!(handoff_action_node_ids(3, "activate_drain", 9), vec![1, 2, 3]);
    }

    #[test]
    fn routing_barrier_requires_every_raft_node_to_drop_target_routes() {
        let ready = RoutingStatus { id: 1, target_id: 5, converged: true, ..Default::default() };
        let stale = RoutingStatus {
            id: 2,
            target_id: 5,
            client_states: 1,
            routes: 2,
            converged: false,
            ..Default::default()
        };
        let forwarding =
            RoutingStatus { id: 3, target_id: 5, active_forwards: 1, converged: false, ..Default::default() };
        let handoff =
            RoutingStatus { id: 4, target_id: 5, handoff_pending: 1, converged: false, ..Default::default() };
        let reliable = RoutingStatus {
            id: 4,
            target_id: 5,
            reliable_forwards: 1,
            converged: true,
            ..Default::default()
        };
        let plain =
            RoutingStatus { id: 4, target_id: 5, plain_forwards: 1, converged: true, ..Default::default() };

        assert!(routing_barrier_ready(&[
            RoutingStatus { id: 1, target_id: 5, converged: true, ..Default::default() },
            RoutingStatus { id: 2, target_id: 5, converged: true, ..Default::default() },
        ]));
        assert!(!routing_barrier_ready(&[ready, stale]));
        assert!(!routing_barrier_ready(&[forwarding]));
        assert!(!routing_barrier_ready(&[handoff]));
        assert!(!routing_barrier_ready(&[reliable]));
        assert!(!routing_barrier_ready(&[plain]));
        assert!(!routing_barrier_ready(&[]));
    }

    #[test]
    fn image_rollout_rolls_back_before_handoff_queue_overflows() {
        let healthy =
            RoutingStatus { id: 1, target_id: 3, handoff_capacity_percent: 79, ..Default::default() };
        assert!(!routing_handoff_requires_rollback(&[healthy], 80));

        for unsafe_status in [
            RoutingStatus { handoff_capacity_percent: 80, ..Default::default() },
            RoutingStatus { handoff_capacity_waiters: 1, ..Default::default() },
            RoutingStatus { handoff_failed: 1, ..Default::default() },
            RoutingStatus { handoff_rejected: 1, ..Default::default() },
            RoutingStatus { data_loss_detected: true, ..Default::default() },
        ] {
            assert!(routing_handoff_requires_rollback(&[unsafe_status], 80));
        }
    }

    #[test]
    fn scale_in_plan_routes_handoff_to_each_step_survivors() {
        let plan = DrainOperation::scale_in(5, 3);
        assert_eq!(plan, DrainOperation::ScaleIn { survivors: vec![1, 2, 3], victims: vec![5, 4] });
        assert_eq!(plan.allowed_handoff_node_ids(5, 5), vec![1, 2, 3, 4]);
        assert_eq!(plan.allowed_handoff_node_ids(4, 4), vec![1, 2, 3]);
        assert_eq!(DrainOperation::Rollout.allowed_handoff_node_ids(5, 5), vec![1, 2, 3, 4]);
        assert_eq!(plan.handoff_destination_constraint(5, 5), Some(vec![1, 2, 3, 4]));
        assert_eq!(plan.handoff_destination_constraint(4, 4), Some(vec![1, 2, 3]));
        assert_eq!(DrainOperation::Rollout.handoff_destination_constraint(5, 5), None);
    }

    #[test]
    fn scale_in_rechecks_membership_immediately_before_leave() {
        let mut membership = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            voters: vec![1, 2, 3, 4, 5],
            raft_peers: (1..=5)
                .map(|id| RaftPeerStatus { id, available: true, recent_active: true, ..Default::default() })
                .collect(),
            business_peers: (1..=5).map(|id| json!({"id": id})).collect(),
            ..Default::default()
        };
        assert_eq!(scale_in_removal_blocker(&membership, 5, 5), None);

        membership.id = 5;
        membership.leader_id = Some(5);
        assert!(scale_in_removal_blocker(&membership, 5, 5)
            .is_some_and(|blocker| blocker.contains("became raft leader")));

        membership.id = 1;
        membership.leader_id = Some(1);
        for peer in &mut membership.raft_peers {
            if peer.id != 1 {
                peer.recent_active = false;
            }
        }
        assert!(scale_in_removal_blocker(&membership, 5, 5)
            .is_some_and(|blocker| blocker.contains("recently-active raft quorum")));

        membership.business_peers.pop();
        assert!(scale_in_removal_blocker(&membership, 5, 5)
            .is_some_and(|blocker| blocker.contains("no longer ready")));
    }

    #[test]
    fn voter_removal_requires_a_recently_active_remaining_quorum() {
        let healthy = MembershipStatus {
            leader_id: Some(1),
            voters: vec![1, 2, 3, 4, 5],
            raft_peers: (2..=5)
                .map(|id| RaftPeerStatus { id, available: true, recent_active: true, ..Default::default() })
                .collect(),
            ..Default::default()
        };
        assert!(removal_preserves_recent_quorum(&healthy, 5));

        let unhealthy = MembershipStatus {
            raft_peers: vec![RaftPeerStatus {
                id: 2,
                available: true,
                recent_active: true,
                ..Default::default()
            }],
            ..healthy
        };
        assert!(!removal_preserves_recent_quorum(&unhealthy, 5));
        assert!(!removal_preserves_recent_quorum(&unhealthy, 6));
    }

    #[test]
    fn scale_in_recovery_never_removes_a_present_member_after_timeout() {
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Present, true, true),
            ScaleInRecoveryDecision::RetryTargetRequest
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Present, false, true),
            ScaleInRecoveryDecision::RetryTargetRequest
        );
    }

    #[test]
    fn scale_in_recovery_cleans_membership_only_after_pod_termination_started() {
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Terminating, true, true),
            ScaleInRecoveryDecision::RemoveStaleRaftMember
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Absent, false, true),
            ScaleInRecoveryDecision::RemoveStaleBusinessMember
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Absent, false, false),
            ScaleInRecoveryDecision::FinishScaleIn
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Present, false, false),
            ScaleInRecoveryDecision::FinishScaleIn
        );
    }

    #[test]
    fn rmqtt_action_error_distinguishes_transient_failures() {
        let payload = json!({"action": "drain_status"});
        let timeout = RmqttActionError::new(RmqttActionErrorKind::Timeout, 5, &payload, "timed out");
        let connection =
            RmqttActionError::new(RmqttActionErrorKind::Connection, 5, &payload, "connection reset");
        let http = RmqttActionError::new(RmqttActionErrorKind::HttpStatus, 5, &payload, "400 bad request");
        let invalid =
            RmqttActionError::new(RmqttActionErrorKind::InvalidResponse, 5, &payload, "invalid json");

        assert!(timeout.is_transient());
        assert!(connection.is_transient());
        assert!(!http.is_transient());
        assert!(!invalid.is_transient());
        assert_eq!(connection.kind.label(), "connection");
    }

    #[test]
    fn leader_lease_rejects_a_second_live_holder_and_allows_expired_takeover() {
        let now = DateTime::parse_from_rfc3339("2026-07-30T08:00:30Z").unwrap().with_timezone(&Utc);
        let live = json!({
            "spec": {
                "holderIdentity": "operator-a",
                "leaseDurationSeconds": 30,
                "renewTime": "2026-07-30T08:00:10Z"
            }
        });
        assert!(!lease_can_be_acquired(&live, "operator-b", now, 30));
        assert!(lease_can_be_acquired(&live, "operator-a", now, 30));

        let expired = json!({
            "spec": {
                "holderIdentity": "operator-a",
                "leaseDurationSeconds": 30,
                "renewTime": "2026-07-30T07:59:59Z"
            }
        });
        assert!(lease_can_be_acquired(&expired, "operator-b", now, 30));
    }

    #[test]
    fn leader_lease_does_not_take_over_an_unparseable_live_record() {
        let now = DateTime::parse_from_rfc3339("2026-07-30T08:00:30Z").unwrap().with_timezone(&Utc);
        let malformed = json!({
            "spec": {
                "holderIdentity": "operator-a",
                "leaseDurationSeconds": 30,
                "renewTime": "not-a-time"
            }
        });
        assert!(!lease_can_be_acquired(&malformed, "operator-b", now, 30));
        assert!(lease_can_be_acquired(&json!({"spec": {}}), "operator-b", now, 30));
    }

    #[test]
    fn kubernetes_lease_time_uses_microsecond_precision() {
        let time = DateTime::parse_from_rfc3339("2026-07-30T08:02:03.865123Z").unwrap().with_timezone(&Utc);
        assert_eq!(kubernetes_microtime(time), "2026-07-30T08:02:03.865123Z");
    }

    #[test]
    fn scale_in_operation_status_preserves_start_time_and_increments_retries() {
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "spec": {"replicas": 3},
            "status": {
                "phase": "ScalingIn",
                "operation": {
                    "type": "RemoveVoter",
                    "nodeId": 5,
                    "fromReplicas": 5,
                    "toReplicas": 4,
                    "requestedReplicas": 3,
                    "startedAt": "2026-07-30T08:00:00.000Z",
                    "retryCount": 2
                }
            }
        }))
        .unwrap();
        let conf_change = ConfChangeStatus {
            kind: "RemoveVoter".to_string(),
            node_id: 5,
            from_replicas: 5,
            to_replicas: 4,
            requested_replicas: 3,
        };

        let operation =
            scale_in_operation_status_json(&cluster, &conf_change, "Retrying", Some("timeout"), true);
        assert_eq!(operation["startedAt"], "2026-07-30T08:00:00.000Z");
        assert_eq!(operation["retryCount"], 3);
        assert_eq!(operation["lastError"], "timeout");
        assert_eq!(operation["phase"], "Retrying");
        assert_eq!(operation["survivors"], json!([1, 2, 3]));
        assert_eq!(operation["victims"], json!([5, 4]));
        assert_eq!(operation["stabilizationSeconds"], 60);
    }

    #[test]
    fn image_rollout_is_opt_in_and_has_a_safe_default_window() {
        let defaults: ClusterSpec = serde_json::from_value(json!({})).unwrap();
        assert!(!defaults.image_update.auto_rollout);
        assert_eq!(defaults.image_update.stabilization_seconds, 60);
        assert_eq!(defaults.raft.endpoint_removal_stable_seconds, 5);
        assert_eq!(defaults.raft.scale_in_stabilization_seconds, 60);
        assert_eq!(defaults.raft.drain_timeout_action, DrainTimeoutAction::Rollback);

        let force_continue: ClusterSpec = serde_json::from_value(json!({
            "raft": {"drainTimeoutAction": "ForceContinue"}
        }))
        .unwrap();
        assert_eq!(force_continue.raft.drain_timeout_action, DrainTimeoutAction::ForceContinue);

        let enabled: ClusterSpec = serde_json::from_value(json!({
            "imageUpdate": {
                "autoRollout": true,
                "stabilizationSeconds": 30
            }
        }))
        .unwrap();
        validate_spec(&enabled).unwrap();

        let invalid: ClusterSpec = serde_json::from_value(json!({
            "imageUpdate": {
                "autoRollout": true,
                "stabilizationSeconds": 9
            }
        }))
        .unwrap();
        assert!(validate_spec(&invalid).is_err());

        let invalid_scale_in_window: ClusterSpec = serde_json::from_value(json!({
            "raft": {
                "scaleInStabilizationSeconds": 9
            }
        }))
        .unwrap();
        assert!(validate_spec(&invalid_scale_in_window).is_err());

        let reserved_annotation: ClusterSpec = serde_json::from_value(json!({
            "pod": {
                "annotations": {
                    "kubectl.kubernetes.io/restartedAt": "managed-by-kubectl"
                }
            }
        }))
        .unwrap();
        assert!(validate_spec(&reserved_annotation).is_err());
    }

    #[test]
    fn image_rollout_detects_only_pods_not_using_the_target_template() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:new",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let pods: Vec<Pod> = serde_json::from_value(json!([
            {
                "metadata": {"name": "rmqtt-0"},
                "spec": {"containers": [{
                    "name": "rmqtt",
                    "image": "example/rmqtt:new",
                    "imagePullPolicy": "IfNotPresent"
                }]}
            },
            {
                "metadata": {"name": "rmqtt-1"},
                "spec": {"containers": [{
                    "name": "rmqtt",
                    "image": "example/rmqtt:old",
                    "imagePullPolicy": "IfNotPresent"
                }]}
            },
            {
                "metadata": {"name": "rmqtt-2"},
                "spec": {"containers": [{
                    "name": "rmqtt",
                    "image": "example/rmqtt:new",
                    "imagePullPolicy": "Always"
                }]}
            }
        ]))
        .unwrap();

        assert_eq!(updated_pod_count(&pods, "rmqtt", &spec, 3), 1);
        assert_eq!(outdated_pod_node_ids(&pods, "rmqtt", &spec, 3), vec![2, 3]);
    }

    #[test]
    fn stateful_set_exposes_kubectl_rollout_restart_annotation() {
        let stateful_set: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": "2026-08-04T01:02:03Z"
                        }
                    }
                }
            }
        }))
        .unwrap();

        assert_eq!(stateful_set.rollout_restart_at(), Some("2026-08-04T01:02:03Z"));
    }

    #[test]
    fn rollout_restart_requires_each_pod_to_inherit_the_target_annotation() {
        let mut spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:v1",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        spec.rollout_restart_at = Some("2026-08-04T01:02:03Z".to_string());
        let pods: Vec<Pod> = serde_json::from_value(json!([
            {
                "metadata": {
                    "name": "rmqtt-0",
                    "annotations": {"kubectl.kubernetes.io/restartedAt": "2026-08-04T01:02:03Z"}
                },
                "spec": {"containers": [{
                    "name": "rmqtt",
                    "image": "example/rmqtt:v1",
                    "imagePullPolicy": "IfNotPresent"
                }]}
            },
            {
                "metadata": {
                    "name": "rmqtt-1",
                    "annotations": {"kubectl.kubernetes.io/restartedAt": "2026-08-03T01:02:03Z"}
                },
                "spec": {"containers": [{
                    "name": "rmqtt",
                    "image": "example/rmqtt:v1",
                    "imagePullPolicy": "IfNotPresent"
                }]}
            },
            {
                "metadata": {"name": "rmqtt-2"},
                "spec": {"containers": [{
                    "name": "rmqtt",
                    "image": "example/rmqtt:v1",
                    "imagePullPolicy": "IfNotPresent"
                }]}
            }
        ]))
        .unwrap();

        assert_eq!(updated_pod_count(&pods, "rmqtt", &spec, 3), 1);
        assert_eq!(outdated_pod_node_ids(&pods, "rmqtt", &spec, 3), vec![2, 3]);
    }

    #[test]
    fn rollout_restart_is_pending_only_after_the_baseline_changes() {
        let stateful_set: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": "2026-08-04T01:02:03Z"
                        }
                    }
                }
            }
        }))
        .unwrap();
        let without_baseline: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"}
        }))
        .unwrap();
        let matching_baseline: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {"observedRestartAt": "2026-08-04T01:02:03Z"}
        }))
        .unwrap();
        let old_baseline: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "status": {"observedRestartAt": "2026-08-03T01:02:03Z"}
        }))
        .unwrap();

        assert_eq!(pending_rollout_restart_at(&stateful_set, &without_baseline), None);
        assert_eq!(pending_rollout_restart_at(&stateful_set, &matching_baseline), None);
        assert_eq!(
            pending_rollout_restart_at(&stateful_set, &old_baseline).as_deref(),
            Some("2026-08-04T01:02:03Z")
        );
    }

    #[test]
    fn rolling_restart_status_adopts_a_newer_request_without_losing_progress() {
        let mut cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt", "generation": 4},
            "spec": {
                "replicas": 3,
                "image": "example/rmqtt:v1",
                "imagePullPolicy": "IfNotPresent",
                "imageUpdate": {"stabilizationSeconds": 60}
            },
            "status": {
                "operation": {
                    "type": "RollingRestart",
                    "phase": "Stabilizing",
                    "nodeId": 2,
                    "fromReplicas": 3,
                    "toReplicas": 3,
                    "requestedReplicas": 3,
                    "targetImage": "example/rmqtt:v1",
                    "targetImagePullPolicy": "IfNotPresent",
                    "targetRestartAt": "2026-08-04T01:00:00Z",
                    "startedAt": "2026-08-04T01:00:01.000Z",
                    "stableSince": "2026-08-04T01:01:00.000Z",
                    "retryCount": 2
                }
            }
        }))
        .unwrap();
        cluster.spec.rollout_restart_at = Some("2026-08-04T02:00:00Z".to_string());

        let operation = image_rollout_operation_status_json(&cluster, 2, 3, "Stabilizing", None, None, false);
        assert_eq!(operation["type"], "RollingRestart");
        assert_eq!(operation["targetRestartAt"], "2026-08-04T02:00:00Z");
        assert_eq!(operation["startedAt"], "2026-08-04T01:00:01.000Z");
        assert_eq!(operation["stableSince"], "2026-08-04T01:01:00.000Z");
        assert_eq!(operation["retryCount"], 2);
    }

    #[test]
    fn leader_transfer_uses_an_updated_ready_caught_up_voter() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:new",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let pods: Vec<Pod> = serde_json::from_value(json!([
            {
                "metadata": {"name": "rmqtt-0"},
                "spec": {"containers": [{"name": "rmqtt", "image": "example/rmqtt:old", "imagePullPolicy": "IfNotPresent"}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
            },
            {
                "metadata": {"name": "rmqtt-1"},
                "spec": {"containers": [{"name": "rmqtt", "image": "example/rmqtt:new", "imagePullPolicy": "IfNotPresent"}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
            },
            {
                "metadata": {"name": "rmqtt-2"},
                "spec": {"containers": [{"name": "rmqtt", "image": "example/rmqtt:new", "imagePullPolicy": "IfNotPresent"}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
            }
        ]))
        .unwrap();
        let membership = MembershipStatus {
            committed_index: 100,
            voters: vec![1, 2, 3],
            raft_peers: vec![
                RaftPeerStatus {
                    id: 1,
                    available: true,
                    matched_index: 100,
                    recent_active: true,
                    ..Default::default()
                },
                RaftPeerStatus {
                    id: 2,
                    available: true,
                    matched_index: 100,
                    recent_active: true,
                    ..Default::default()
                },
                RaftPeerStatus {
                    id: 3,
                    available: true,
                    matched_index: 100,
                    recent_active: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(rollout_leadership_target(&membership, &pods, "rmqtt", &spec, 1), Some(2));
        assert_eq!(rollout_leadership_target_for_retry(&membership, &pods, "rmqtt", &spec, 1, 0), Some(3));
        assert_eq!(rollout_leadership_target_for_retry(&membership, &pods, "rmqtt", &spec, 1, 1), Some(2));
    }

    #[test]
    fn leader_transfer_retry_waits_for_its_deadline_and_recovers_from_bad_status_time() {
        let operation: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "TransferringLeader",
            "leaderTransferAttemptedAt": "2026-09-12T08:00:00Z"
        }))
        .unwrap();
        let before_deadline =
            DateTime::parse_from_rfc3339("2026-09-12T08:00:29Z").unwrap().with_timezone(&Utc);
        let at_deadline = DateTime::parse_from_rfc3339("2026-09-12T08:00:30Z").unwrap().with_timezone(&Utc);
        assert!(!leader_transfer_retry_due(&operation, before_deadline));
        assert!(leader_transfer_retry_due(&operation, at_deadline));

        let missing_time: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "TransferringLeader"
        }))
        .unwrap();
        assert!(leader_transfer_retry_due(&missing_time, at_deadline));
    }

    #[test]
    fn waiting_for_leader_transfer_preserves_the_attempt_timestamp() {
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt", "generation": 4},
            "spec": {"replicas": 3, "image": "example/rmqtt:v2"},
            "status": {
                "operation": {
                    "type": "RollingImageUpdate",
                    "phase": "TransferringLeader",
                    "nodeId": 1,
                    "leaderTransferAttemptedAt": "2026-09-12T08:00:00Z",
                    "leaderTransferRetryCount": 2
                }
            }
        }))
        .unwrap();

        let waiting =
            image_rollout_operation_status_json(&cluster, 1, 3, "TransferringLeader", None, None, false);
        assert_eq!(waiting["leaderTransferAttemptedAt"], "2026-09-12T08:00:00Z");
        assert_eq!(waiting["leaderTransferRetryCount"], 2);

        let retrying = image_rollout_operation_status_json(
            &cluster,
            1,
            3,
            "TransferringLeader",
            None,
            Some("previous transfer did not complete"),
            true,
        );
        assert_ne!(retrying["leaderTransferAttemptedAt"], "2026-09-12T08:00:00Z");
        assert_eq!(retrying["leaderTransferRetryCount"], 3);
    }

    #[test]
    fn rolling_restart_leader_transfer_does_not_require_the_new_restart_annotation() {
        let mut spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:v1",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        spec.rollout_restart_at = Some("2026-08-04T02:00:00Z".to_string());
        let pods: Vec<Pod> = serde_json::from_value(json!([
            {
                "metadata": {"name": "rmqtt-0"},
                "spec": {"containers": [{"name": "rmqtt", "image": "example/rmqtt:v1", "imagePullPolicy": "IfNotPresent"}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
            },
            {
                "metadata": {
                    "name": "rmqtt-1",
                    "annotations": {"kubectl.kubernetes.io/restartedAt": "2026-08-04T01:00:00Z"}
                },
                "spec": {"containers": [{"name": "rmqtt", "image": "example/rmqtt:v1", "imagePullPolicy": "IfNotPresent"}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
            }
        ]))
        .unwrap();
        let membership = MembershipStatus {
            committed_index: 100,
            voters: vec![1, 2],
            raft_peers: vec![
                RaftPeerStatus {
                    id: 1,
                    available: true,
                    matched_index: 100,
                    recent_active: true,
                    ..Default::default()
                },
                RaftPeerStatus {
                    id: 2,
                    available: true,
                    matched_index: 100,
                    recent_active: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(rollout_leadership_target(&membership, &pods, "rmqtt", &spec, 1), Some(2));
    }

    #[test]
    fn image_rollout_status_keeps_its_original_target_and_start_time() {
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt", "generation": 4},
            "spec": {
                "replicas": 3,
                "image": "example/rmqtt:v2",
                "imagePullPolicy": "IfNotPresent",
                "imageUpdate": {"autoRollout": true, "stabilizationSeconds": 60}
            },
            "status": {
                "operation": {
                    "type": "RollingImageUpdate",
                    "phase": "Stabilizing",
                    "nodeId": 2,
                    "fromReplicas": 3,
                    "toReplicas": 3,
                    "requestedReplicas": 3,
                    "targetImage": "example/rmqtt:v2",
                    "targetImagePullPolicy": "IfNotPresent",
                    "startedAt": "2026-07-30T08:00:00.000Z",
                    "stableSince": "2026-07-30T08:01:00.000Z",
                    "retryCount": 2
                }
            }
        }))
        .unwrap();

        let operation =
            image_rollout_operation_status_json(&cluster, 2, 3, "Stabilizing", None, Some("temporary"), true);
        assert_eq!(operation["targetImage"], "example/rmqtt:v2");
        assert_eq!(operation["startedAt"], "2026-07-30T08:00:00.000Z");
        assert_eq!(operation["stableSince"], "2026-07-30T08:01:00.000Z");
        assert_eq!(operation["retryCount"], 3);
    }

    #[test]
    fn terminal_image_rollout_can_be_superseded_by_a_new_target() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:v3",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let rolled_back: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "RolledBack",
            "targetImage": "example/rmqtt:v2",
            "targetImagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let blocked: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "Blocked",
            "targetImage": "example/rmqtt:v3",
            "targetImagePullPolicy": "Always"
        }))
        .unwrap();

        assert!(image_rollout_terminal_target_superseded(&rolled_back, &spec));
        assert!(image_rollout_terminal_target_superseded(&blocked, &spec));
    }

    #[test]
    fn image_rollout_target_change_does_not_interrupt_nonterminal_or_matching_terminal_operation() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:v3",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let matching_terminal: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "RolledBack",
            "targetImage": "example/rmqtt:v3",
            "targetImagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let in_flight: OperationStatus = serde_json::from_value(json!({
            "type": "RollingImageUpdate",
            "phase": "Draining",
            "targetImage": "example/rmqtt:v2",
            "targetImagePullPolicy": "IfNotPresent"
        }))
        .unwrap();

        assert!(!image_rollout_terminal_target_superseded(&matching_terminal, &spec));
        assert!(!image_rollout_terminal_target_superseded(&in_flight, &spec));
    }

    #[test]
    fn terminal_rolling_restart_is_superseded_only_by_a_new_restart_timestamp() {
        let mut spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:v1",
            "imagePullPolicy": "IfNotPresent"
        }))
        .unwrap();
        let operation: OperationStatus = serde_json::from_value(json!({
            "type": "RollingRestart",
            "phase": "Blocked",
            "targetImage": "example/rmqtt:v1",
            "targetImagePullPolicy": "IfNotPresent",
            "targetRestartAt": "2026-08-04T01:00:00Z"
        }))
        .unwrap();

        spec.rollout_restart_at = Some("2026-08-04T01:00:00Z".to_string());
        assert!(!image_rollout_terminal_target_superseded(&operation, &spec));

        spec.rollout_restart_at = Some("2026-08-04T02:00:00Z".to_string());
        assert!(image_rollout_terminal_target_superseded(&operation, &spec));
    }

    #[test]
    fn deletion_timestamp_is_treated_as_the_kubernetes_deadline() {
        let deadline = Utc::now() + chrono::Duration::seconds(50);
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {
                "deletionTimestamp": deadline.to_rfc3339(),
                "deletionGracePeriodSeconds": 120
            }
        }))
        .unwrap();

        let remaining = pod_termination_remaining(&pod, 100);
        assert!(remaining <= Duration::from_secs(50));
        assert!(remaining >= Duration::from_secs(45));
    }

    #[test]
    fn staggered_external_deletions_are_merged_into_one_batch() {
        let first_deadline = Utc::now() + chrono::Duration::seconds(90);
        let second_deadline = Utc::now() + chrono::Duration::seconds(80);
        let first: Pod = serde_json::from_value(json!({
            "metadata": {
                "name": "rmqtt-1",
                "uid": "pod-2-a",
                "deletionTimestamp": first_deadline.to_rfc3339()
            }
        }))
        .unwrap();
        let second: Pod = serde_json::from_value(json!({
            "metadata": {
                "name": "rmqtt-3",
                "uid": "pod-4-a",
                "deletionTimestamp": second_deadline.to_rfc3339()
            }
        }))
        .unwrap();
        let mut targets = Vec::new();

        upsert_external_termination_target(&mut targets, &first, 2, 100);
        targets[0].phase = "Draining".to_string();
        upsert_external_termination_target(&mut targets, &second, 4, 100);

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].node_id, 2);
        assert_eq!(targets[0].phase, "Draining");
        assert_eq!(targets[1].node_id, 4);
        assert_eq!(targets[1].phase, "WaitingForSignal");
    }

    #[test]
    fn recreated_node_uid_replaces_stale_external_termination_state() {
        let deadline = Utc::now() + chrono::Duration::seconds(90);
        let replacement: Pod = serde_json::from_value(json!({
            "metadata": {
                "name": "rmqtt-1",
                "uid": "pod-2-new",
                "deletionTimestamp": deadline.to_rfc3339()
            }
        }))
        .unwrap();
        let mut targets = vec![ExternalTerminationTargetStatus {
            node_id: 2,
            pod_uid: "pod-2-old".to_string(),
            boot_id: "old-boot".to_string(),
            deadline_at: deadline.to_rfc3339(),
            phase: "Completed".to_string(),
            stable_observations: 2,
            endpoint_absent_since: None,
            outcome: Some("Completed".to_string()),
            last_error: None,
            ..Default::default()
        }];

        upsert_external_termination_target(&mut targets, &replacement, 2, 100);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].pod_uid, "pod-2-new");
        assert_eq!(targets[0].phase, "WaitingForSignal");
        assert!(targets[0].boot_id.is_empty());
        assert_eq!(targets[0].stable_observations, 0);
    }

    #[test]
    fn survivor_cleanup_never_targets_a_replacement_process() {
        let pods: Vec<Pod> = serde_json::from_value(json!([{
            "metadata": {
                "name": "rmqtt-1",
                "uid": "pod-2-new"
            }
        }]))
        .unwrap();
        let old_target = ExternalTerminationTargetStatus {
            node_id: 2,
            pod_uid: "pod-2-old".to_string(),
            phase: "WaitingMembershipRemoval".to_string(),
            ..Default::default()
        };

        assert!(!external_target_has_old_pod(&pods, "rmqtt", &old_target));
        assert!(external_replacement_has_taken_ordinal(&pods, "rmqtt", &old_target));
    }

    #[test]
    fn external_membership_owner_is_single_and_earliest_deadline_first() {
        let target = |node_id, deadline: &str, phase: &str| ExternalTerminationTargetStatus {
            node_id,
            deadline_at: deadline.to_string(),
            phase: phase.to_string(),
            ..Default::default()
        };
        let targets = vec![
            target(2, "2026-09-17T10:00:30Z", "ForceFinalizing"),
            target(4, "2026-09-17T10:00:10Z", "Leaving"),
            target(5, "2026-09-17T10:00:00Z", "Draining"),
        ];

        assert_eq!(external_membership_owner(&targets), Some(1));
    }

    #[test]
    fn legacy_status_without_external_batch_remains_compatible() {
        let status: ClusterStatus = serde_json::from_value(json!({
            "phase": "Ready",
            "operation": null,
            "confChange": null
        }))
        .unwrap();

        assert!(status.external_termination_batch.is_none());
    }

    #[test]
    fn graceful_shutdown_keeps_the_legacy_grace_default_when_disabled() {
        let disabled = ClusterSpec::default();
        assert_eq!(effective_termination_grace_period_seconds(&disabled), 30);

        let mut enabled = disabled;
        enabled.graceful_shutdown.enabled = true;
        assert_eq!(effective_termination_grace_period_seconds(&enabled), 120);
    }

    #[test]
    fn graceful_shutdown_renders_identity_and_budget_environment() {
        let mut cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "spec": {
                "gracefulShutdown": {
                    "enabled": true,
                    "timeoutSeconds": 100,
                    "finalizeReserveSeconds": 10
                }
            }
        }))
        .unwrap();
        cluster.spec.raft.min_replicas = 1;
        let object = stateful_set_object("default", &cluster, 1, false);
        let env = object["spec"]["template"]["spec"]["containers"][0]["env"].as_array().unwrap();
        let value = |name: &str| {
            env.iter().find(|entry| entry["name"] == name).and_then(|entry| entry["value"].as_str())
        };

        assert_eq!(value("RMQTT_GRACEFUL_SHUTDOWN_ENABLED"), Some("true"));
        assert_eq!(value("RMQTT_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS"), Some("100"));
        assert_eq!(object["spec"]["template"]["spec"]["terminationGracePeriodSeconds"], 120);
        assert!(env.iter().any(|entry| entry["name"] == "RMQTT_POD_UID" && entry["valueFrom"].is_object()));
        assert!(object["spec"]["template"]["spec"]["containers"][0]["args"][0]
            .as_str()
            .unwrap()
            .contains("exec /app/rmqtt/rmqtt-bin/rmqttd"));
    }
}
