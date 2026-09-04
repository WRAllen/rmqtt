//! Runtime cluster node registry for business gRPC peers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use serde_json::json;
use tokio::sync::Mutex;

use rmqtt::{
    context::ServerContext,
    grpc::{GrpcClient, GrpcClients, Message, MessageReply, MessageType},
    types::{Addr, NodeId, NodeName},
    utils::{timestamp_millis, NodeAddr},
};

use super::config::PluginConfig;
use super::message::{ClusterNode, RaftGrpcMessage, RaftGrpcMessageReply};
use super::HashMap;

const GRPC_PROBE_TIMEOUT: Duration = Duration::from_secs(4);
// handy-grpc already retries its stream every three seconds. Rebuilding more
// aggressively would create connection churn during a real network outage.
const GRPC_REPAIR_COOLDOWN: Duration = Duration::from_secs(30);

fn validate_probe_reply(node_id: NodeId, reply: MessageReply) -> Result<()> {
    let MessageReply::Data(data) = reply else {
        return Err(anyhow!("unexpected business gRPC probe reply from node {node_id}: {reply:?}"));
    };
    let RaftGrpcMessageReply::GetNodeHealthStatus(status) = RaftGrpcMessageReply::decode(&data)? else {
        return Err(anyhow!("unexpected business gRPC probe payload from node {node_id}"));
    };
    if status.node_id != node_id {
        return Err(anyhow!("business gRPC probe for node {node_id} reached node {}", status.node_id));
    }
    if !status.is_running() {
        return Err(anyhow!(
            "business gRPC probe reached node {node_id}, but the node is not available: {:?}",
            status.descr
        ));
    }
    Ok(())
}

fn repair_attempt_due(last_attempt_at: Option<i64>, now: i64) -> bool {
    last_attempt_at.is_none_or(|last| now.saturating_sub(last) >= GRPC_REPAIR_COOLDOWN.as_millis() as i64)
}

#[derive(Clone)]
pub(crate) struct ClusterNodeManager {
    scx: ServerContext,
    nodes: Arc<DashMap<NodeId, ClusterNode, ahash::RandomState>>,
    clients: Arc<DashMap<NodeId, ClientEntry, ahash::RandomState>>,
    options: Arc<ClientOptions>,
    next_generation: Arc<AtomicU64>,
    repair_lock: Arc<Mutex<()>>,
    last_repair_attempts: Arc<DashMap<NodeId, i64, ahash::RandomState>>,
    repairs_in_progress: Arc<DashMap<NodeId, u64, ahash::RandomState>>,
}

#[derive(Clone)]
struct ClientEntry {
    addr: Addr,
    client: GrpcClient,
    generation: u64,
    created_at: i64,
    last_probe_succeeded_at: Option<i64>,
}

#[derive(Clone)]
struct ClientOptions {
    batch_size: usize,
    concurrency_limit: usize,
    timeout: Duration,
}

impl ClientOptions {
    fn from_config(cfg: &PluginConfig) -> Self {
        Self {
            batch_size: cfg.node_grpc_batch_size,
            concurrency_limit: cfg.node_grpc_client_concurrency_limit,
            timeout: cfg.node_grpc_client_timeout,
        }
    }
}

impl ClusterNodeManager {
    pub(crate) fn new(scx: ServerContext, cfg: &PluginConfig) -> Self {
        Self {
            scx,
            nodes: Arc::new(DashMap::default()),
            clients: Arc::new(DashMap::default()),
            options: Arc::new(ClientOptions::from_config(cfg)),
            next_generation: Arc::new(AtomicU64::new(0)),
            repair_lock: Arc::new(Mutex::new(())),
            last_repair_attempts: Arc::new(DashMap::default()),
            repairs_in_progress: Arc::new(DashMap::default()),
        }
    }

    async fn new_client(&self, grpc_addr: &str) -> Result<GrpcClient> {
        self.scx
            .node
            .new_grpc_client(
                grpc_addr,
                self.options.timeout,
                self.options.concurrency_limit,
                self.options.batch_size,
            )
            .await
    }

    async fn new_client_with_probe(
        &self,
        node_id: NodeId,
        grpc_addr: &str,
        message_type: MessageType,
    ) -> Result<GrpcClient> {
        let data = RaftGrpcMessage::GetNodeHealthStatus.encode()?;
        let timeout = self.options.timeout.min(GRPC_PROBE_TIMEOUT);
        let (client, reply) = GrpcClient::new_with_probe(
            grpc_addr,
            self.options.timeout,
            self.options.concurrency_limit,
            message_type,
            Message::Data(data),
            timeout,
        )
        .await?;
        validate_probe_reply(node_id, reply)?;
        Ok(client)
    }

    fn insert_client(&self, node_id: NodeId, addr: Addr, client: GrpcClient, probed: bool) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let now = timestamp_millis();
        self.clients.insert(
            node_id,
            ClientEntry {
                addr,
                client,
                generation,
                created_at: now,
                last_probe_succeeded_at: probed.then_some(now),
            },
        );
        generation
    }

    async fn probe_client(
        &self,
        node_id: NodeId,
        message_type: MessageType,
        mut client: GrpcClient,
    ) -> Result<()> {
        let data = RaftGrpcMessage::GetNodeHealthStatus.encode()?;
        let timeout = self.options.timeout.min(GRPC_PROBE_TIMEOUT);
        let reply = client.send_message(message_type, Message::Data(data), Some(timeout)).await?;
        validate_probe_reply(node_id, reply)
    }

    fn mark_probe_succeeded(&self, node_id: NodeId, generation: u64) {
        if let Some(mut entry) = self.clients.get_mut(&node_id) {
            if entry.generation == generation {
                entry.last_probe_succeeded_at = Some(timestamp_millis());
            }
        }
    }

    async fn replace_client_after_probe(
        &self,
        node_id: NodeId,
        expected_generation: Option<u64>,
        message_type: MessageType,
    ) -> Result<bool> {
        let node = self
            .nodes
            .get(&node_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| anyhow!("cluster node {node_id} does not exist"))?;
        if node_id == self.scx.node.id() {
            return Ok(false);
        }

        let current_generation = self.clients.get(&node_id).map(|entry| entry.generation);
        if current_generation != expected_generation {
            return Ok(false);
        }

        // Verify the target application with a unary request before starting
        // any long-lived streams. This avoids creating reconnecting background
        // tasks when the target itself is unavailable.
        let candidate = self.new_client_with_probe(node_id, &node.grpc_addr, message_type).await?;

        let current_generation = self.clients.get(&node_id).map(|entry| entry.generation);
        if current_generation != expected_generation {
            return Ok(false);
        }
        // Install the candidate before the duplex probe. If that probe fails,
        // retaining this client allows handy-grpc's own reconnect loop to
        // recover without creating a new client on every failed request.
        let generation = self.insert_client(node_id, node.grpc_addr.clone(), candidate.clone(), false);
        self.probe_client(node_id, message_type, candidate).await?;
        self.mark_probe_succeeded(node_id, generation);
        log::info!(
            "replaced and verified business gRPC client, node_id={node_id}, addr={}, generation={generation}",
            node.grpc_addr
        );
        Ok(true)
    }

    pub(crate) async fn seed_from_config(
        &self,
        node_grpc_addrs: &[NodeAddr],
        raft_peer_addrs: &[NodeAddr],
    ) -> Result<()> {
        for grpc_addr in node_grpc_addrs {
            let raft_addr = raft_peer_addrs
                .iter()
                .find(|addr| addr.id == grpc_addr.id)
                .map(|addr| addr.addr.clone())
                .unwrap_or_default();
            self.upsert(ClusterNode { id: grpc_addr.id, grpc_addr: grpc_addr.addr.clone(), raft_addr })
                .await?;
        }
        Ok(())
    }

    pub(crate) fn local_node_from_config(
        &self,
        node_grpc_addrs: &[NodeAddr],
        raft_peer_addrs: &[NodeAddr],
    ) -> Result<ClusterNode> {
        let id = self.scx.node.id();
        let grpc_addr = node_grpc_addrs
            .iter()
            .find(|addr| addr.id == id)
            .map(|addr| addr.addr.clone())
            .ok_or_else(|| anyhow::anyhow!("node_grpc_addrs does not contain local node id {}", id))?;
        let raft_addr = raft_peer_addrs
            .iter()
            .find(|addr| addr.id == id)
            .map(|addr| addr.addr.clone())
            .ok_or_else(|| anyhow::anyhow!("raft_peer_addrs does not contain local node id {}", id))?;
        Ok(ClusterNode { id, grpc_addr, raft_addr })
    }

    pub(crate) async fn upsert(&self, node: ClusterNode) -> Result<()> {
        let id = node.id;
        let grpc_addr = node.grpc_addr.clone();
        self.nodes.insert(id, node);

        if id == self.scx.node.id() {
            self.clients.remove(&id);
            return Ok(());
        }

        let client_current = self.clients.get(&id).map(|entry| entry.addr.clone());
        if client_current.as_ref() == Some(&grpc_addr) {
            return Ok(());
        }

        let client = self.new_client(&grpc_addr).await?;
        self.insert_client(id, grpc_addr, client, false);
        Ok(())
    }

    pub(crate) fn remove(&self, node_id: NodeId) {
        self.nodes.remove(&node_id);
        self.clients.remove(&node_id);
        self.last_repair_attempts.remove(&node_id);
        self.repairs_in_progress.remove(&node_id);
    }

    pub(crate) fn grpc_client(&self, node_id: NodeId) -> Option<GrpcClient> {
        self.clients.get(&node_id).map(|entry| entry.client.clone())
    }

    pub(crate) fn grpc_client_with_generation(&self, node_id: NodeId) -> Option<(u64, GrpcClient)> {
        self.clients.get(&node_id).map(|entry| (entry.generation, entry.client.clone()))
    }

    /// Verify the exact business gRPC path used for message forwarding. If
    /// the current client is stale, replace it only after a fresh client has
    /// answered the same application-level probe.
    pub(crate) async fn ensure_grpc_client_ready(
        &self,
        node_id: NodeId,
        message_type: MessageType,
    ) -> Result<bool> {
        if node_id == self.scx.node.id() {
            return Ok(false);
        }

        let current = self.grpc_client_with_generation(node_id);
        if let Some((generation, client)) = current.as_ref() {
            match self.probe_client(node_id, message_type, client.clone()).await {
                Ok(()) => {
                    self.mark_probe_succeeded(node_id, *generation);
                    return Ok(false);
                }
                Err(error) => {
                    log::warn!(
                        "business gRPC preflight failed; attempting one client refresh, node_id={node_id}, generation={generation}, error={error:?}"
                    );
                }
            }
        }

        let _repair_guard = self.repair_lock.lock().await;
        let expected_generation = current.map(|(generation, _)| generation);
        let latest = self.grpc_client_with_generation(node_id);
        if latest.as_ref().map(|(generation, _)| *generation) != expected_generation {
            if let Some((generation, client)) = latest {
                self.probe_client(node_id, message_type, client).await?;
                self.mark_probe_succeeded(node_id, generation);
                return Ok(false);
            }
        }
        let now = timestamp_millis();
        let last_attempt_at = self.last_repair_attempts.get(&node_id).map(|last| *last.value());
        if !repair_attempt_due(last_attempt_at, now) {
            return Err(anyhow!(
                "business gRPC client refresh for node {node_id} was already attempted within the last {} seconds",
                GRPC_REPAIR_COOLDOWN.as_secs()
            ));
        }
        self.last_repair_attempts.insert(node_id, now);
        self.replace_client_after_probe(node_id, expected_generation, message_type).await
    }

    /// Repair future sends after a transport failure. The failed request is
    /// intentionally not retried here because it may already have reached the
    /// remote broker; reliable handoff owns its own delivery-id based retry.
    pub(crate) fn try_begin_grpc_client_repair(&self, node_id: NodeId, failed_generation: u64) -> bool {
        if self.clients.get(&node_id).map(|entry| entry.generation) != Some(failed_generation) {
            return false;
        }
        let now = timestamp_millis();
        let last_attempt_at = self.last_repair_attempts.get(&node_id).map(|last| *last.value());
        if !repair_attempt_due(last_attempt_at, now) {
            return false;
        }
        match self.repairs_in_progress.entry(node_id) {
            dashmap::mapref::entry::Entry::Occupied(_) => false,
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(failed_generation);
                self.last_repair_attempts.insert(node_id, now);
                true
            }
        }
    }

    pub(crate) fn finish_grpc_client_repair(&self, node_id: NodeId, failed_generation: u64) {
        self.repairs_in_progress.remove_if(&node_id, |_, generation| *generation == failed_generation);
    }

    pub(crate) async fn repair_grpc_client_after_failure(
        &self,
        node_id: NodeId,
        failed_generation: u64,
        message_type: MessageType,
    ) -> Result<bool> {
        let _repair_guard = self.repair_lock.lock().await;
        if self.clients.get(&node_id).map(|entry| entry.generation) != Some(failed_generation) {
            return Ok(false);
        }
        self.replace_client_after_probe(node_id, Some(failed_generation), message_type).await
    }

    /// Number of already-started gRPC requests from this node to `node_id`.
    /// `GrpcClient` maintains this counter on the existing send path, so
    /// exposing it to the scale-in barrier adds no per-message work.
    pub(crate) fn grpc_active_tasks(&self, node_id: NodeId) -> usize {
        self.clients
            .get(&node_id)
            .map(|entry| entry.client.active_tasks().count().max(0) as usize)
            .unwrap_or_default()
    }

    pub(crate) fn grpc_clients(&self) -> GrpcClients {
        let mut clients = HashMap::default();
        for entry in self.clients.iter() {
            clients.insert(*entry.key(), (entry.addr.clone(), entry.client.clone()));
        }
        Arc::new(clients)
    }

    pub(crate) fn node_name(&self, id: NodeId) -> NodeName {
        self.nodes.get(&id).map(|node| format!("{}@{}", node.id, node.grpc_addr)).unwrap_or_default()
    }

    pub(crate) fn snapshot_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.iter().map(|entry| entry.value().clone()).collect()
    }

    pub(crate) async fn restore_nodes(&self, nodes: Vec<ClusterNode>) -> Result<()> {
        self.nodes.clear();
        self.clients.clear();
        self.last_repair_attempts.clear();
        self.repairs_in_progress.clear();
        for node in nodes {
            self.upsert(node).await?;
        }
        Ok(())
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        let nodes = self
            .nodes
            .iter()
            .map(|entry| {
                let node = entry.value();
                let client = self.clients.get(&node.id);
                json!({
                    "id": node.id,
                    "grpc_addr": node.grpc_addr,
                    "raft_addr": node.raft_addr,
                    "has_grpc_client": client.is_some(),
                    "grpc_client_generation": client.as_ref().map(|entry| entry.generation),
                    "grpc_client_created_at": client.as_ref().map(|entry| entry.created_at),
                    "grpc_last_probe_succeeded_at": client
                        .as_ref()
                        .and_then(|entry| entry.last_probe_succeeded_at),
                    "grpc_last_repair_attempt_at": self
                        .last_repair_attempts
                        .get(&node.id)
                        .map(|entry| *entry.value()),
                    "grpc_repair_in_progress": self.repairs_in_progress.contains_key(&node.id),
                })
            })
            .collect::<Vec<_>>();
        json!(nodes)
    }
}

#[cfg(test)]
mod tests {
    use rmqtt::types::NodeHealthStatus;

    use super::*;

    fn health_reply(node_id: NodeId) -> MessageReply {
        health_reply_with_state(node_id, true)
    }

    fn health_reply_with_state(node_id: NodeId, running: bool) -> MessageReply {
        MessageReply::Data(
            RaftGrpcMessageReply::GetNodeHealthStatus(NodeHealthStatus {
                node_id,
                running,
                leader_id: Some(1),
                descr: None,
            })
            .encode()
            .unwrap(),
        )
    }

    #[test]
    fn probe_reply_must_come_from_requested_node() {
        assert!(validate_probe_reply(2, health_reply(2)).is_ok());
        assert!(validate_probe_reply(2, health_reply(3)).is_err());
        assert!(validate_probe_reply(2, health_reply_with_state(2, false)).is_err());
        assert!(validate_probe_reply(2, MessageReply::Success).is_err());
    }

    #[test]
    fn failed_client_repairs_are_rate_limited() {
        let cooldown = GRPC_REPAIR_COOLDOWN.as_millis() as i64;
        assert!(repair_attempt_due(None, 1_000));
        assert!(!repair_attempt_due(Some(1_000), 1_000 + cooldown - 1));
        assert!(repair_attempt_due(Some(1_000), 1_000 + cooldown));
        assert!(!repair_attempt_due(Some(2_000), 1_000));
    }
}
