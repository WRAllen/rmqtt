use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahash::HashSet;
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::FutureExt;
use rust_box::task_exec_queue::{SpawnExt, TaskExecQueue};

use rmqtt::context::ServerContext;
use rmqtt::types::{ClientId, SubscriptionOptions};
use rmqtt::utils::timestamp_millis;
use rmqtt::{
    grpc::{GrpcClient, GrpcClients, Message, MessageBroadcaster, MessageReply, MessageSender, MessageType},
    router::Router,
    session::{OfflineInfo, Session, SessionState},
    shared::{DefaultShared, Entry, Shared},
    types::{
        From, Id, IsAdmin, NodeId, OfflineSession, Publish, Reason, SessionStatus, SessionSubs, SubRelations,
        SubRelationsMap, SubsSearchParams, SubsSearchResult, Subscribe, SubscribeReturn,
        SubscriptionClientIds, To, Tx, Unsubscribe,
    },
    types::{HealthInfo, MsgID, NodeHealthStatus, SharedGroup},
    Result,
};

use super::config::Handoff;
use super::handoff::{
    ForwardMode, HandoffDiscardStats, HandoffManager, HandoffResult, HandoffStats, PendingHandoff,
};
use super::message::{
    get_client_node_id, DeliveryId, Message as RaftMessage, MessageReply as RaftMessageReply,
    RaftGrpcMessage, RaftGrpcMessageReply, ReliableForward, ReliableForwardReply, ReliableForwardStatus,
    SessionHandoff, SessionHandoffReply,
};
use super::nodes::ClusterNodeManager;
use super::ClusterRouter;

#[inline]
pub(crate) fn effective_source_session_policy(
    clean_start: bool,
    clear_subscriptions: bool,
    source_draining: bool,
    is_admin: IsAdmin,
) -> (bool, bool) {
    if source_draining && !is_admin {
        (false, false)
    } else {
        (clean_start, clear_subscriptions)
    }
}

#[inline]
fn drain_target_allowed(allowed_targets: Option<&HashSet<NodeId>>, candidate: NodeId) -> bool {
    allowed_targets.is_none_or(|allowed_targets| allowed_targets.contains(&candidate))
}

pub struct ClusterLockEntry {
    scx: ServerContext,
    exec: TaskExecQueue,
    inner: Box<dyn Entry>,
    cluster_shared: ClusterShared,
    prev_node_id: Option<NodeId>,
}

impl ClusterLockEntry {
    #[inline]
    pub fn new(
        scx: ServerContext,
        exec: TaskExecQueue,
        inner: Box<dyn Entry>,
        cluster_shared: ClusterShared,
        prev_node_id: Option<NodeId>,
    ) -> Self {
        Self { scx, exec, inner, cluster_shared, prev_node_id }
    }
}

#[async_trait]
impl Entry for ClusterLockEntry {
    #[inline]
    async fn try_lock(&self) -> Result<Box<dyn Entry>> {
        if self.cluster_shared.is_draining() || self.cluster_shared.local_handoff_prepared() {
            return Err(anyhow!(
                "node {} is preparing to drain and is not accepting new MQTT sessions",
                self.scx.node.id()
            ));
        }
        let msg = RaftMessage::HandshakeTryLock { id: self.id() }.encode()?;
        let raft_mailbox = self.cluster_shared.router.raft_mailbox().await;
        let reply = async move { raft_mailbox.send_proposal(msg).await.map_err(anyhow::Error::new) }
            .spawn(&self.exec)
            .result()
            .await
            .map_err(|_| anyhow!("ClusterLockEntry::try_lock(..), task execution failure"))??;
        let mut prev_node_id = None;
        if !reply.is_empty() {
            match RaftMessageReply::decode(&reply)? {
                RaftMessageReply::Error(e) => {
                    return Err(anyhow!(e));
                }
                RaftMessageReply::HandshakeTryLock(prev_id) => {
                    prev_node_id = prev_id.map(|id| id.node_id);
                    log::debug!(
                        "{:?} ClusterLockEntry try_lock prev_node_id: {:?}",
                        self.session().map(|s| s.id.clone()),
                        prev_node_id
                    );
                }
                _ => {
                    log::error!("unreachable!(), reply: {reply:?}");
                    return Err(anyhow!("unreachable!()"));
                }
            }
        }
        Ok(Box::new(ClusterLockEntry::new(
            self.scx.clone(),
            self.exec.clone(),
            self.inner.try_lock().await?,
            self.cluster_shared.clone(),
            prev_node_id,
        )))
    }

    #[inline]
    fn id(&self) -> Id {
        self.inner.id()
    }

    #[inline]
    fn id_same(&self) -> Option<bool> {
        self.inner.id_same()
    }

    #[inline]
    fn exist(&self) -> bool {
        self.inner.exist()
    }

    #[inline]
    async fn set(&mut self, session: Session, tx: Tx) -> Result<()> {
        let id = session.id.clone();
        self.inner.set(session, tx).await?;
        let msg = RaftMessage::Connected { id: id.clone() }.encode()?;
        let raft_mailbox = self.cluster_shared.router.raft_mailbox().await;
        let reply = async move { raft_mailbox.send_proposal(msg).await.map_err(anyhow::Error::new) }
            .spawn(&self.exec)
            .result()
            .await
            .map_err(|_| anyhow!("ClusterLockEntry::set(..), task execution failure"));
        let reply = match reply {
            Ok(Ok(reply)) => reply,
            Ok(Err(e)) | Err(e) => {
                if let Err(remove_error) = self.inner.remove_with(&id).await {
                    log::error!(
                        "{id:?} failed to roll back local session after Connected proposal failure: {remove_error:?}"
                    );
                }
                return Err(e);
            }
        };
        if !reply.is_empty() {
            let reply = match RaftMessageReply::decode(&reply) {
                Ok(reply) => reply,
                Err(e) => {
                    if let Err(remove_error) = self.inner.remove_with(&id).await {
                        log::error!(
                            "{id:?} failed to roll back local session after Connected reply decode failure: {remove_error:?}"
                        );
                    }
                    return Err(e);
                }
            };
            match reply {
                RaftMessageReply::Error(e) => {
                    log::error!("RaftMessage::Connected reply: {e:?}");
                    if let Err(remove_error) = self.inner.remove_with(&id).await {
                        log::error!(
                            "{id:?} failed to roll back local session after Connected rejection: {remove_error:?}"
                        );
                    }
                    return Err(anyhow!(e));
                }
                _ => {
                    log::error!("unreachable!(), {reply:?}");
                    if let Err(remove_error) = self.inner.remove_with(&id).await {
                        log::error!(
                            "{id:?} failed to roll back local session after unexpected Connected reply: {remove_error:?}"
                        );
                    }
                    return Err(anyhow!("unreachable!()"));
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn remove(&mut self) -> Result<Option<(Session, Tx)>> {
        self.inner.remove().await
    }

    #[inline]
    async fn remove_with(&mut self, id: &Id) -> Result<Option<(Session, Tx)>> {
        self.inner.remove_with(id).await
    }

    #[inline]
    async fn kick(
        &mut self,
        clean_start: bool,
        clear_subscriptions: bool,
        is_admin: IsAdmin,
    ) -> Result<OfflineSession> {
        log::debug!(
            "{:?} ClusterLockEntry kick ..., clean_start: {}, clear_subscriptions: {}, is_admin: {}",
            self.session().map(|s| s.id.clone()),
            clean_start,
            clear_subscriptions,
            is_admin
        );
        let id = self.id();

        let prev_node_id = if is_admin {
            let raft_mailbox = self.cluster_shared.router.raft_mailbox().await;
            let node_id = get_client_node_id(raft_mailbox, &id.client_id).await?;
            node_id.unwrap_or(id.node_id)
        } else {
            self.prev_node_id.unwrap_or(id.node_id)
        };
        log::debug!("{:?} kick, prev_node_id: {:?}, is_admin: {}", id, self.prev_node_id, is_admin);
        if prev_node_id == id.node_id {
            //kicked from local
            let source_session_id = self.inner.session().map(|session| session.id.clone());
            let preserve_relocated = source_session_id
                .as_ref()
                .is_some_and(|source_id| self.cluster_shared.preserve_session_on_takeover(source_id));
            let (source_clean_start, source_clear_subscriptions) = effective_source_session_policy(
                clean_start,
                clear_subscriptions,
                preserve_relocated,
                is_admin,
            );
            let result = self.inner.kick(source_clean_start, source_clear_subscriptions, is_admin).await;
            if result.is_ok() && preserve_relocated && !is_admin {
                if let Some(source_id) = source_session_id.as_ref() {
                    self.cluster_shared.complete_relocated_takeover(source_id);
                }
            }
            result
        } else {
            //kicked from other node
            if let Some(client) = self.cluster_shared.grpc_client(prev_node_id) {
                let message_type = self.cluster_shared.message_type;
                let rw_timeout = self.cluster_shared.rw_timeout;
                let id1 = id.clone();
                let kick_fut = async move {
                    let msg_sender = MessageSender::new(
                        client,
                        message_type,
                        Message::Kick(id1, clean_start, true, is_admin), //clear_subscriptions
                        Some(rw_timeout),
                    );
                    match msg_sender.send().await {
                        Ok(reply) => {
                            if let MessageReply::Kick(kicked) = reply {
                                log::debug!("{id:?} kicked: {kicked:?}");
                                Ok(kicked)
                            } else if let MessageReply::Error(reason) = reply {
                                Err(anyhow!(
                                    "Message::Kick was rejected by source node {prev_node_id}: {reason}"
                                ))
                            } else {
                                Err(anyhow!(
                                    "unexpected Message::Kick reply from source node {prev_node_id}: {reply:?}"
                                ))
                            }
                        }
                        Err(e) => {
                            Err(anyhow!(
                                "Message::Kick transport failure from source node {prev_node_id} for {id:?}: {e:?}"
                            ))
                        }
                    }
                };

                let reply =
                    kick_fut.spawn(&self.exec).result().await.map_err(|e| anyhow!(e.to_string()))??;
                Ok(reply)
            } else {
                return Err(anyhow!(format!(
                    "kick error, grpc_client is not exist, prev_node_id: {:?}",
                    prev_node_id
                )));
            }
        }
    }

    #[inline]
    async fn online(&self) -> bool {
        self.cluster_shared.router.is_online(0, &self.id().client_id).await
    }

    #[inline]
    async fn is_connected(&self) -> bool {
        self.inner.is_connected().await
    }

    #[inline]
    fn session(&self) -> Option<Session> {
        self.inner.session()
    }

    #[inline]
    fn tx(&self) -> Option<Tx> {
        self.inner.tx()
    }

    #[inline]
    async fn subscribe(&self, subscribe: &Subscribe) -> Result<SubscribeReturn> {
        self.inner.subscribe(subscribe).await
    }

    #[inline]
    async fn unsubscribe(&self, unsubscribe: &Unsubscribe) -> Result<bool> {
        self.inner.unsubscribe(unsubscribe).await
    }

    #[inline]
    async fn publish(&self, from: From, p: Publish) -> std::result::Result<(), (From, Publish, Reason)> {
        self.inner.publish(from, p).await
    }

    #[inline]
    async fn subscriptions(&self) -> Option<Vec<SubsSearchResult>> {
        let id = self.cluster_shared.router.id(&self.id().client_id)?;
        if id.node_id == self.scx.node.id() {
            self.inner.subscriptions().await
        } else {
            //from other node
            if let Some(client) = self.cluster_shared.grpc_client(id.node_id) {
                let reply = MessageSender::new(
                    client,
                    self.cluster_shared.message_type,
                    Message::SubscriptionsGet(id.client_id.clone()),
                    Some(Duration::from_secs(10)),
                )
                .send()
                .await;
                match reply {
                    Ok(MessageReply::SubscriptionsGet(subs)) => subs,
                    Err(e) => {
                        log::warn!("Message::SubscriptionsGet, error: {e:?}");
                        None
                    }
                    _ => {
                        log::error!("unreachable!(), reply: {reply:?}");
                        None
                    }
                }
            } else {
                log::error!("get subscriptions error, grpc_client is not exist, node_id: {}", id.node_id,);
                None
            }
        }
    }
}

#[derive(Clone)]
pub struct ClusterShared {
    scx: ServerContext,
    exec: TaskExecQueue,
    forwards_exec: TaskExecQueue,
    inner: DefaultShared,
    router: ClusterRouter,
    nodes: ClusterNodeManager,
    draining: Arc<AtomicBool>,
    handoff: Arc<HandoffManager>,
    // Operator-provided destinations for proactive session relocation. This
    // map is populated only during planned maintenance, so normal forwarding
    // and session setup do not pay for membership filtering.
    drain_allowed_targets: Arc<DashMap<NodeId, Arc<HashSet<NodeId>>>>,
    // A source node keeps one retry copy of session state while it is
    // draining.  If the gRPC response is lost after extraction, the next
    // reconnect can fetch the same snapshot instead of observing NotExist.
    // The cache is used only during an operator-coordinated drain and is
    // cleared on rollback; a successful drain deletes the source Pod.
    drain_session_handoffs: Arc<DashMap<ClientId, OfflineInfo>>,
    // Full snapshots waiting for broker-to-broker relocation.  This map is
    // populated only after the local MQTT connection is offline and is kept
    // until a destination has installed both the session and its routes.
    proactive_session_handoffs: Arc<DashMap<ClientId, SessionHandoff>>,
    relocated_sessions: Arc<DashMap<ClientId, Id>>,
    session_handoff_active: Arc<AtomicUsize>,
    session_handoff_failures: Arc<AtomicUsize>,
    pub(crate) message_type: MessageType,
    exec_queue_busy_limit: isize,
    exec_queue_workers_busy_limit: isize,
    rw_timeout: Duration,
}

impl ClusterShared {
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn new(
        scx: ServerContext,
        exec: TaskExecQueue,
        router: ClusterRouter,
        nodes: ClusterNodeManager,
        draining: Arc<AtomicBool>,
        handoff_cfg: Handoff,
        message_type: MessageType,
        exec_queue_max: usize,
        exec_queue_workers: usize,
        rw_timeout: Duration,
    ) -> ClusterShared {
        let scx1 = scx.clone();
        let forwards_exec = scx.get_exec("RAFT_FORWARDS_EXEC");
        Self {
            scx,
            exec,
            forwards_exec,
            inner: DefaultShared::new(Some(scx1)),
            router,
            nodes,
            draining,
            handoff: Arc::new(HandoffManager::new(handoff_cfg)),
            drain_allowed_targets: Arc::new(DashMap::new()),
            drain_session_handoffs: Arc::new(DashMap::new()),
            proactive_session_handoffs: Arc::new(DashMap::new()),
            relocated_sessions: Arc::new(DashMap::new()),
            session_handoff_active: Arc::new(AtomicUsize::new(0)),
            session_handoff_failures: Arc::new(AtomicUsize::new(0)),
            message_type,
            exec_queue_busy_limit: (exec_queue_max as f64 * 0.7) as isize,
            exec_queue_workers_busy_limit: (exec_queue_workers as f64 * 0.9) as isize,
            rw_timeout,
        }
    }

    #[inline]
    pub(crate) fn inner(&self) -> &DefaultShared {
        &self.inner
    }

    #[inline]
    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn grpc_client(&self, node_id: u64) -> Option<GrpcClient> {
        self.nodes.grpc_client(node_id)
    }

    #[inline]
    fn grpc_client_with_generation(&self, node_id: u64) -> Option<(u64, GrpcClient)> {
        self.nodes.grpc_client_with_generation(node_id)
    }

    /// Schedule a best-effort repair for future requests after a transport
    /// failure. The failed request itself is never replayed here because the
    /// caller cannot know whether the remote broker accepted it.
    fn schedule_grpc_client_repair(&self, node_id: NodeId, failed_generation: u64, operation: &'static str) {
        let nodes = self.nodes.clone();
        if !nodes.try_begin_grpc_client_repair(node_id, failed_generation) {
            return;
        }
        let message_type = self.message_type;
        tokio::spawn(async move {
            let result =
                nodes.repair_grpc_client_after_failure(node_id, failed_generation, message_type).await;
            nodes.finish_grpc_client_repair(node_id, failed_generation);
            match result {
                Ok(true) => log::info!(
                    "business gRPC client repaired after {operation} failure, node_id={node_id}, failed_generation={failed_generation}"
                ),
                Ok(false) => {}
                Err(error) => log::warn!(
                    "business gRPC client repair failed after {operation} failure, node_id={node_id}, failed_generation={failed_generation}, error={error:?}"
                ),
            }
        });
    }

    /// During an operator-coordinated drain, a route selected immediately
    /// before session takeover can still point at the previous owner. Preserve
    /// that delivery in the same reliable queue as other drain-window traffic.
    /// Outside a drain this method leaves the established fast path unchanged.
    async fn reroute_stale_local_delivery(
        &self,
        dropped: (To, From, Publish, Reason),
    ) -> std::result::Result<(), (To, From, Publish, Reason)> {
        let (to, from, publish, reason) = dropped;
        let this_node_id = self.scx.node.id();
        if to.node_id != this_node_id {
            return Err((to, from, publish, reason));
        }
        if !self.handoff.operation_active() {
            return Err((to, from, publish, reason));
        }
        let Some(target_node_id) = self.router.get_ready_target_node_id(&to.client_id) else {
            return Err((to, from, publish, reason));
        };
        if target_node_id == this_node_id {
            return Err((to, from, publish, reason));
        }

        let mut opts = SubscriptionOptions::default();
        opts.set_qos(publish.qos);
        let target_client_id = to.client_id.clone();
        let relations = vec![(publish.topic.clone(), target_client_id.clone(), opts, None, None)];
        let pending = PendingHandoff::new(
            self.handoff.next_delivery_id(this_node_id),
            target_client_id,
            Some(target_node_id),
            Some(this_node_id),
            from.clone(),
            publish.clone(),
            relations,
        );
        if self.enqueue_handoff(pending).await.is_ok() {
            Ok(())
        } else {
            Err((to, from, publish, reason))
        }
    }

    pub(crate) async fn prepare_drain(
        &self,
        node_id: NodeId,
        allowed_targets: Option<Vec<NodeId>>,
    ) -> Result<(bool, bool)> {
        if !self.handoff.enabled() {
            return Ok((false, false));
        }

        // The operator invokes prepare_drain on every broker before it
        // diverts traffic. Verify the same bidirectional business gRPC path
        // that forwarding uses, and repair a stale client once if necessary.
        // This runs only during planned maintenance, never on the normal
        // publish path.
        let grpc_client_refreshed =
            self.nodes.ensure_grpc_client_ready(node_id, self.message_type).await.map_err(|error| {
                anyhow!("business gRPC preflight to drain target node {node_id} failed: {error:#}")
            })?;

        if let Some(allowed_targets) = allowed_targets {
            let allowed_targets =
                allowed_targets.into_iter().filter(|target_id| *target_id != node_id).collect::<HashSet<_>>();
            self.drain_allowed_targets.insert(node_id, Arc::new(allowed_targets));
        } else {
            self.drain_allowed_targets.remove(&node_id);
        }
        Ok((self.handoff.prepare_drain(node_id), grpc_client_refreshed))
    }

    #[inline]
    pub(crate) fn clear_drain(&self, node_id: NodeId) {
        self.handoff.clear_drain(node_id);
        self.drain_allowed_targets.remove(&node_id);
        if node_id == self.scx.node.id() {
            self.drain_session_handoffs.clear();
            self.proactive_session_handoffs.clear();
            self.session_handoff_failures.store(0, Ordering::Release);
        }
    }

    #[inline]
    pub(crate) fn force_discard_handoff_for_target(&self, node_id: NodeId) -> HandoffDiscardStats {
        self.handoff.discard_for_target(node_id)
    }

    pub(crate) fn force_clear_local_session_handoffs(&self) -> usize {
        let discarded = self.drain_session_handoffs.len() + self.proactive_session_handoffs.len();
        self.drain_session_handoffs.clear();
        self.proactive_session_handoffs.clear();
        self.session_handoff_failures.store(0, Ordering::Release);
        discarded
    }

    #[inline]
    pub(crate) fn cache_drain_session_handoff(&self, client_id: ClientId, offline_session: &OfflineSession) {
        if let OfflineSession::Exist(Some(offline_info)) = offline_session {
            self.drain_session_handoffs.insert(client_id, offline_info.clone());
        }
    }

    #[inline]
    pub(crate) fn cached_drain_session_handoff(&self, client_id: &ClientId) -> Option<OfflineSession> {
        self.drain_session_handoffs
            .get(client_id)
            .map(|offline_info| OfflineSession::Exist(Some(offline_info.clone())))
            .or_else(|| {
                self.proactive_session_handoffs
                    .get(client_id)
                    .map(|handoff| OfflineSession::Exist(Some(handoff.offline.clone())))
            })
    }

    #[inline]
    pub(crate) fn complete_pull_session_handoff(&self, client_id: &ClientId) {
        // Keep drain_session_handoffs as a retry copy in case the gRPC reply is
        // lost, but stop the proactive worker: the reconnecting broker is now
        // responsible for installing the snapshot and replacing its routes.
        if let Some((_, handoff)) = self.proactive_session_handoffs.remove(client_id) {
            self.drain_session_handoffs.insert(client_id.clone(), handoff.offline);
        }
    }

    #[inline]
    pub(crate) fn activate_drain(&self, node_id: NodeId) -> bool {
        self.handoff.activate_drain(node_id)
    }

    #[inline]
    pub(crate) fn handoff_stats(&self) -> HandoffStats {
        self.handoff.stats()
    }

    #[inline]
    pub(crate) fn session_handoff_pending(&self) -> usize {
        self.proactive_session_handoffs.len()
    }

    #[inline]
    pub(crate) fn session_handoff_active(&self) -> usize {
        self.session_handoff_active.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn session_handoff_failures(&self) -> usize {
        self.session_handoff_failures.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn preserve_session_on_takeover(&self, id: &Id) -> bool {
        self.is_draining()
            || self.relocated_sessions.get(&id.client_id).is_some_and(|relocated_id| *relocated_id == *id)
    }

    #[inline]
    pub(crate) fn complete_relocated_takeover(&self, id: &Id) {
        self.relocated_sessions.remove_if(&id.client_id, |_, relocated_id| *relocated_id == *id);
    }

    async fn propose_session_state(&self, message: RaftMessage<'_>) -> Result<()> {
        let encoded = message.encode()?;
        let reply =
            self.router.raft_mailbox().await.send_proposal(encoded).await.map_err(anyhow::Error::new)?;
        if reply.is_empty() {
            return Ok(());
        }
        if let RaftMessageReply::Error(reason) = RaftMessageReply::decode(&reply)? {
            return Err(anyhow!(reason));
        }
        Ok(())
    }

    /// Install a transferred snapshot as an offline session on this node.
    /// The original connection epoch is retained, so a concurrent real MQTT
    /// reconnect (which has a newer epoch) always wins.
    pub(crate) async fn accept_session_handoff(
        &self,
        handoff: SessionHandoff,
    ) -> Result<SessionHandoffReply> {
        let source_id = handoff.offline.id.clone();
        let destination_id = source_id.relocated_to(self.scx.node.id());

        if let Some(existing) = self.inner.entry(destination_id.clone()).session() {
            return Ok(SessionHandoffReply {
                installed: existing.id == destination_id,
                destination_id: existing.id.clone(),
            });
        }

        if let Some(status) = self.router.status(&source_id.client_id) {
            if status.id != source_id && status.id != destination_id {
                // A normal client reconnect has already established a newer
                // owner.  Never replace it with the older standby snapshot.
                return Ok(SessionHandoffReply { destination_id: status.id, installed: false });
            }
        }

        let mut local_entry = self.inner.entry(destination_id.clone()).try_lock().await?;
        if let Some(existing) = local_entry.session() {
            return Ok(SessionHandoffReply {
                installed: existing.id == destination_id,
                destination_id: existing.id.clone(),
            });
        }

        // Claim the replicated owner before installing routes. Connected uses
        // the preserved epoch and is rejected if a newer reconnect won the
        // race after the status check above.
        self.propose_session_state(RaftMessage::Connected { id: destination_id.clone() }).await?;

        let Some(listen_cfg) = self.scx.listen_cfgs.get(&source_id.lid()).map(|entry| entry.clone()) else {
            let _ = self
                .propose_session_state(RaftMessage::SessionTerminated { id: destination_id.clone() })
                .await;
            return Err(anyhow!("listener {} is not configured on relocation destination", source_id.lid()));
        };
        let connect_info = Arc::new(handoff.connect_info.relocated_to(self.scx.node.id()));
        let fitter = self.scx.extends.fitter_mgr().await.create(
            connect_info.clone(),
            destination_id.clone(),
            listen_cfg.clone(),
        );
        let max_inflight = fitter.max_inflight();
        let max_mqueue_len = fitter.max_mqueue_len();
        let session = match Session::new(
            destination_id.clone(),
            self.scx.clone(),
            max_mqueue_len,
            listen_cfg,
            fitter,
            None,
            max_inflight,
            handoff.offline.created_at,
            connect_info,
            true,
            handoff.superuser,
            false,
            timestamp_millis(),
            SessionSubs::new(),
            None,
            Some(source_id.clone()),
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                let _ = self
                    .propose_session_state(RaftMessage::SessionTerminated { id: destination_id.clone() })
                    .await;
                return Err(error);
            }
        };
        let expiry = Duration::from_millis(handoff.session_expiry_millis.max(1));
        let tx =
            match SessionState::offline_restart_with_state(session.clone(), handoff.offline, expiry).await {
                Ok(tx) => tx,
                Err(error) => {
                    let _ = self
                        .propose_session_state(RaftMessage::SessionTerminated { id: destination_id.clone() })
                        .await;
                    return Err(error);
                }
            };
        if self.router.status(&destination_id.client_id).is_some_and(|status| status.id != destination_id) {
            let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
            let _ =
                tx.unbounded_send(rmqtt::types::Message::Kick(ack_tx, destination_id.clone(), false, false));
            let _ = self
                .propose_session_state(RaftMessage::SessionTerminated { id: destination_id.clone() })
                .await;
            let current_id = self
                .router
                .status(&destination_id.client_id)
                .map(|status| status.id)
                .unwrap_or_else(|| destination_id.clone());
            return Ok(SessionHandoffReply { destination_id: current_id, installed: false });
        }
        if let Err(error) = local_entry.set(session, tx).await {
            let _ = self
                .propose_session_state(RaftMessage::SessionTerminated { id: destination_id.clone() })
                .await;
            return Err(error);
        }
        self.relocated_sessions.insert(destination_id.client_id.clone(), destination_id.clone());
        self.propose_session_state(RaftMessage::Disconnected { id: destination_id.clone() }).await?;
        Ok(SessionHandoffReply { destination_id, installed: true })
    }

    fn session_handoff_targets(&self, client_id: &ClientId) -> Vec<NodeId> {
        let local_id = self.scx.node.id();
        let allowed_targets = self.drain_allowed_targets.get(&local_id).map(|entry| entry.clone());
        let mut targets = self
            .nodes
            .snapshot_nodes()
            .into_iter()
            .map(|node| node.id)
            .filter(|node_id| {
                *node_id != local_id
                    && self.grpc_client(*node_id).is_some()
                    && !self.handoff.should_avoid_target(*node_id)
                    && drain_target_allowed(allowed_targets.as_deref(), *node_id)
            })
            .collect::<Vec<_>>();
        targets.sort_unstable();
        if !targets.is_empty() {
            let mut hasher = DefaultHasher::new();
            client_id.hash(&mut hasher);
            let offset = hasher.finish() as usize % targets.len();
            targets.rotate_left(offset);
        }
        targets
    }

    async fn send_session_handoff(
        &self,
        target_node_id: NodeId,
        handoff: &SessionHandoff,
    ) -> Result<SessionHandoffReply> {
        let (client_generation, mut client) = self
            .grpc_client_with_generation(target_node_id)
            .ok_or_else(|| anyhow!("grpc client for node {target_node_id} does not exist"))?;
        let data = RaftGrpcMessage::SessionHandoff(handoff.clone()).encode()?;
        let reply = client.send_message(self.message_type, Message::Data(data), Some(self.rw_timeout)).await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                self.schedule_grpc_client_repair(target_node_id, client_generation, "session handoff");
                return Err(error);
            }
        };
        match reply {
            MessageReply::Data(data) => {
                let RaftGrpcMessageReply::SessionHandoff(reply) = RaftGrpcMessageReply::decode(&data)? else {
                    return Err(anyhow!("unexpected session handoff reply from node {target_node_id}"));
                };
                Ok(reply)
            }
            MessageReply::Error(reason) => {
                Err(anyhow!("session handoff was rejected by node {target_node_id}: {reason}"))
            }
            reply => Err(anyhow!("unexpected session handoff reply from node {target_node_id}: {reply:?}")),
        }
    }

    /// Extract every disconnected local session and actively relocate it.
    /// This function is invoked by the drain-only worker and does not run on
    /// the normal publish, subscribe, or connection hot paths.
    pub(crate) async fn migrate_drained_sessions(&self) -> usize {
        if !self.is_draining() {
            return 0;
        }

        for entry in self.inner.iter().collect::<Vec<_>>() {
            let client_id = entry.id().client_id.clone();
            if self.proactive_session_handoffs.contains_key(&client_id) || entry.is_connected().await {
                continue;
            }
            let Some(session) = entry.session() else {
                continue;
            };
            let connect_info = match session.connect_info().await {
                Ok(connect_info) => connect_info.as_ref().clone(),
                Err(error) => {
                    log::warn!("failed to read connect info before relocating {client_id}: {error:?}");
                    continue;
                }
            };
            let superuser = session.superuser().await.unwrap_or(false);
            let disconnect = session.disconnect().await.unwrap_or(None);
            let session_expiry_millis = session
                .fitter
                .session_expiry_interval(disconnect.as_ref())
                .max(Duration::from_secs(600))
                .as_millis()
                .min(u64::MAX as u128) as u64;
            let mut locked = match entry.try_lock().await {
                Ok(locked) => locked,
                Err(_) => continue,
            };
            match locked.kick(false, false, false).await {
                Ok(OfflineSession::Exist(Some(offline))) => {
                    self.proactive_session_handoffs.insert(
                        client_id,
                        SessionHandoff { offline, connect_info, superuser, session_expiry_millis },
                    );
                }
                Ok(_) => {}
                Err(error) => log::debug!("session relocation snapshot is not ready yet: {error:?}"),
            }
        }

        let pending =
            self.proactive_session_handoffs.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
        let mut migrated = 0usize;
        let mut failures = 0usize;
        for client_id in pending {
            let Some(handoff) = self.proactive_session_handoffs.get(&client_id).map(|entry| entry.clone())
            else {
                continue;
            };
            self.session_handoff_active.fetch_add(1, Ordering::AcqRel);
            let mut accepted = false;
            for target_node_id in self.session_handoff_targets(&client_id) {
                match self.send_session_handoff(target_node_id, &handoff).await {
                    Ok(reply) => {
                        log::debug!(
                            "session {client_id} relocated from node {} to {:?}, installed={}",
                            self.scx.node.id(),
                            reply.destination_id,
                            reply.installed
                        );
                        accepted = true;
                        break;
                    }
                    Err(error) => {
                        log::debug!(
                            "session {client_id} relocation to node {target_node_id} will retry: {error:?}"
                        );
                    }
                }
            }
            self.session_handoff_active.fetch_sub(1, Ordering::AcqRel);
            if accepted {
                self.proactive_session_handoffs.remove(&client_id);
                migrated += 1;
            } else {
                failures += 1;
            }
        }
        self.session_handoff_failures.store(failures, Ordering::Release);
        migrated
    }

    #[inline]
    pub(crate) fn handoff_data_loss_detected(&self) -> bool {
        self.handoff.data_loss_detected()
    }

    #[inline]
    pub(crate) fn reliable_handoff_enabled(&self) -> bool {
        self.handoff.enabled()
    }

    #[inline]
    pub(crate) fn local_handoff_prepared(&self) -> bool {
        self.handoff.should_avoid_target(self.scx.node.id())
    }

    #[inline]
    pub(crate) fn handoff_pending_for_node(&self, node_id: NodeId) -> usize {
        self.handoff.pending_for_node(node_id)
    }

    #[inline]
    pub(crate) fn handoff_active_for_node(&self, node_id: NodeId) -> usize {
        self.handoff.active_for_node(node_id)
    }

    #[inline]
    pub(crate) fn plain_forwards_for_node(&self, node_id: NodeId) -> usize {
        self.handoff.plain_forwards_for_node(node_id)
    }

    async fn enqueue_handoff(&self, pending: PendingHandoff) -> std::result::Result<(), PendingHandoff> {
        let client_id = pending.target_client_id.clone();
        match self.handoff.enqueue(pending).await {
            Ok(start_worker) => {
                if start_worker {
                    let shared = self.clone();
                    tokio::spawn(async move {
                        loop {
                            let worker = shared.clone();
                            if AssertUnwindSafe(worker.run_handoff_worker(client_id.clone()))
                                .catch_unwind()
                                .await
                                .is_ok()
                            {
                                return;
                            }
                            shared.handoff.stop_worker(&client_id);
                            log::error!("reliable handoff worker panicked for client {client_id}");
                            tokio::time::sleep(shared.handoff.retry_max_interval()).await;
                            if !shared.handoff.restart_worker_if_pending(&client_id) {
                                return;
                            }
                        }
                    });
                }
                Ok(())
            }
            Err(pending) => Err(pending),
        }
    }

    async fn run_handoff_worker(self, client_id: ClientId) {
        let mut timeout_reported_for = None;
        let mut retry_delay = self.handoff.retry_interval();
        loop {
            let Some(batch) = self.handoff.next_batch(&client_id) else {
                return;
            };
            let Some(front) = batch.messages.first() else {
                self.handoff.stop_worker(&client_id);
                return;
            };

            // A SessionTerminated proposal removes both the client state and
            // its routes. Messages which entered handoff during that cleanup
            // race no longer have an MQTT session to receive them. Only retire
            // an unpinned batch: once a receiver accepts a delivery, the source
            // must keep polling that receiver for the idempotent completion.
            if !self.handoff.operation_active()
                && batch.pinned_node_id.is_none()
                && self.router.status(&front.target_client_id).is_none()
            {
                let retired = self.handoff.retire_unowned_batch(&client_id, &batch.messages);
                if retired > 0 {
                    log::info!(
                        "retired {retired} obsolete reliable handoff deliveries after session termination, client_id={client_id}"
                    );
                    retry_delay = self.handoff.retry_interval();
                }
                continue;
            }
            if front.created_at.elapsed() >= self.handoff.retry_timeout()
                && timeout_reported_for != Some(front.delivery_id)
            {
                log::error!(
                    "reliable handoff timed out but remains queued for fail-closed retry, delivery_id={:?}, client_id={}, age_ms={}",
                    front.delivery_id,
                    front.target_client_id,
                    front.created_at.elapsed().as_millis()
                );
                timeout_reported_for = Some(front.delivery_id);
            }

            // A proactively relocated session is deliberately offline until
            // the real MQTT client reconnects, but it can already own routes
            // and queue messages. Reliable handoff therefore follows any
            // replicated owner, not only an online socket.
            let ready_owner = self.router.get_target_node_id(&front.target_client_id);
            let dynamic_avoid = if batch.pinned_node_id.is_none()
                && front.target_node_id.is_some_and(|target| self.handoff.should_avoid_target(target))
            {
                front.target_node_id
            } else {
                batch.avoid_node_id
            };
            let target_node_id =
                batch.pinned_node_id.or_else(|| ready_owner.filter(|owner| Some(*owner) != dynamic_avoid));
            let Some(target_node_id) = target_node_id else {
                tokio::time::sleep(retry_delay).await;
                retry_delay = next_retry_delay(retry_delay, self.handoff.retry_max_interval());
                continue;
            };

            let retry_guard = self.handoff.retry_guard(target_node_id);
            let result = if target_node_id == self.scx.node.id() {
                let mut results = Vec::with_capacity(batch.messages.len());
                for pending in &batch.messages {
                    let status = match self
                        .inner
                        .forwards_to(pending.from.clone(), &pending.publish, pending.relations.clone())
                        .await
                    {
                        Ok(()) => ReliableForwardStatus::Delivered,
                        Err(_) => {
                            ReliableForwardStatus::Retryable("local target session is not ready".to_string())
                        }
                    };
                    let stop = matches!(status, ReliableForwardStatus::Retryable(_));
                    results.push(HandoffResult { delivery_id: pending.delivery_id, status });
                    if stop {
                        break;
                    }
                }
                Ok(results)
            } else {
                self.send_reliable_forward_batch(target_node_id, &batch.messages).await
            };
            drop(retry_guard);

            match result {
                Ok(results) => {
                    for result in &results {
                        if let ReliableForwardStatus::Rejected(reason) = &result.status {
                            log::error!(
                                "reliable handoff was rejected, delivery_id={:?}, client_id={}, target_node_id={}, reason={}",
                                result.delivery_id,
                                client_id,
                                target_node_id,
                                reason
                            );
                        }
                    }
                    let applied = self.handoff.apply_batch(&client_id, target_node_id, &results);
                    if applied.completed > 0 {
                        timeout_reported_for = None;
                    }
                    if applied.made_progress() {
                        retry_delay = self.handoff.retry_interval();
                    } else {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = next_retry_delay(retry_delay, self.handoff.retry_max_interval());
                    }
                }
                Err(e) => {
                    log::debug!(
                        "reliable handoff batch transport retry, first_delivery_id={:?}, batch_messages={}, client_id={}, target_node_id={}, error={:?}",
                        front.delivery_id,
                        batch.messages.len(),
                        front.target_client_id,
                        target_node_id,
                        e
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = next_retry_delay(retry_delay, self.handoff.retry_max_interval());
                }
            }
        }
    }

    async fn send_reliable_forward_batch(
        &self,
        target_node_id: NodeId,
        batch: &[PendingHandoff],
    ) -> Result<Vec<HandoffResult>> {
        let (client_generation, mut batch_client) = self
            .grpc_client_with_generation(target_node_id)
            .ok_or_else(|| anyhow!("grpc client for node {target_node_id} does not exist"))?;
        let forwards = batch
            .iter()
            .map(|pending| ReliableForward {
                delivery_id: pending.delivery_id,
                target_client_id: pending.target_client_id.clone(),
                from: pending.from.clone(),
                publish: pending.publish.clone(),
                relations: pending.relations.clone(),
            })
            .collect();
        let data = RaftGrpcMessage::ReliableForwardBatch(forwards).encode()?;
        let reply =
            batch_client.send_message(self.message_type, Message::Data(data), Some(self.rw_timeout)).await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                self.schedule_grpc_client_repair(target_node_id, client_generation, "reliable handoff batch");
                return Err(error);
            }
        };
        match reply {
            MessageReply::Data(data) => {
                let RaftGrpcMessageReply::ReliableForwardBatch(statuses) =
                    RaftGrpcMessageReply::decode(&data)?
                else {
                    return Err(anyhow!("unexpected reliable handoff reply from node {target_node_id}"));
                };
                if statuses.len() > batch.len()
                    || !statuses
                        .iter()
                        .zip(batch)
                        .all(|(status, pending)| status.delivery_id == pending.delivery_id)
                {
                    return Err(anyhow!(
                        "unexpected reliable handoff batch reply from node {target_node_id}"
                    ));
                }
                Ok(statuses
                    .into_iter()
                    .map(|status| HandoffResult { delivery_id: status.delivery_id, status: status.status })
                    .collect())
            }
            MessageReply::Error(reason) => {
                Err(anyhow!("reliable handoff was rejected by node {target_node_id}: {reason}"))
            }
            reply => {
                Err(anyhow!("unexpected reliable handoff gRPC reply from node {target_node_id}: {reply:?}"))
            }
        }
    }

    pub(crate) async fn accept_reliable_forward(
        &self,
        delivery_id: DeliveryId,
        target_client_id: ClientId,
        from: From,
        publish: Publish,
        relations: SubRelations,
    ) -> ReliableForwardStatus {
        if !self.handoff.enabled() {
            return ReliableForwardStatus::Rejected("reliable handoff is disabled".to_string());
        }
        if self.handoff.delivery_completed(delivery_id) {
            return ReliableForwardStatus::Duplicate;
        }
        if self.handoff.delivery_pending(delivery_id) {
            return ReliableForwardStatus::Accepted;
        }

        let owner = self.router.get_target_node_id(&target_client_id);
        let local_handoff_prepared = self.local_handoff_prepared();
        if self.is_draining()
            || local_handoff_prepared
            || owner != Some(self.scx.node.id())
            || !self.inner.exist(&target_client_id)
        {
            return ReliableForwardStatus::Retryable(
                "target node does not currently own a local session".to_string(),
            );
        }

        let pending =
            PendingHandoff::new(delivery_id, target_client_id, owner, None, from, publish, relations);
        match self.enqueue_handoff(pending).await {
            Ok(()) => ReliableForwardStatus::Accepted,
            Err(_) => ReliableForwardStatus::Rejected("reliable handoff queue limit exceeded".to_string()),
        }
    }

    pub(crate) async fn accept_reliable_forward_batch(
        &self,
        forwards: Vec<ReliableForward>,
    ) -> Vec<ReliableForwardReply> {
        let mut replies = Vec::with_capacity(forwards.len());
        for forward in forwards {
            let status = self
                .accept_reliable_forward(
                    forward.delivery_id,
                    forward.target_client_id,
                    forward.from,
                    forward.publish,
                    forward.relations,
                )
                .await;
            let stop =
                matches!(status, ReliableForwardStatus::Retryable(_) | ReliableForwardStatus::Rejected(_));
            replies.push(ReliableForwardReply { delivery_id: forward.delivery_id, status });
            if stop {
                break;
            }
        }
        replies
    }

    async fn enqueue_relations_handoff(
        &self,
        target_node_id: NodeId,
        from: From,
        publish: Publish,
        relations: SubRelations,
    ) -> std::result::Result<(), Vec<(To, From, Publish, Reason)>> {
        if !self.handoff.enabled() {
            return Err(relations
                .into_iter()
                .map(|relation| {
                    (
                        To::from(target_node_id, relation.1),
                        from.clone(),
                        publish.clone(),
                        Reason::from_static("reliable handoff is disabled"),
                    )
                })
                .collect());
        }

        let mut grouped = HashMap::<ClientId, SubRelations>::new();
        for relation in relations {
            grouped.entry(relation.1.clone()).or_default().push(relation);
        }
        let mut dropped = Vec::new();
        for (target_client_id, relations) in grouped {
            let pending = PendingHandoff::new(
                self.handoff.next_delivery_id(self.scx.node.id()),
                target_client_id.clone(),
                Some(target_node_id),
                self.handoff.should_avoid_target(target_node_id).then_some(target_node_id),
                from.clone(),
                publish.clone(),
                relations,
            );
            if self.enqueue_handoff(pending).await.is_err() {
                dropped.push((
                    To::from(target_node_id, target_client_id),
                    from.clone(),
                    publish.clone(),
                    Reason::from_static("reliable handoff queue limit exceeded"),
                ));
            }
        }
        if dropped.is_empty() {
            Ok(())
        } else {
            Err(dropped)
        }
    }

    #[inline]
    async fn forward_to_target_client(
        &self,
        from: From,
        publish: Publish,
        target_clientid: ClientId,
    ) -> std::result::Result<SubscriptionClientIds, Vec<(To, From, Publish, Reason)>> {
        let mut opts = SubscriptionOptions::default();
        opts.set_qos(publish.qos);
        let relations = vec![(publish.topic.clone(), target_clientid.clone(), opts, None, None)];
        let handoff_active = self.handoff.operation_active();
        let target_nodeid = if handoff_active {
            self.router
                .get_ready_target_node_id(&target_clientid)
                .or_else(|| self.router.get_target_node_id(&target_clientid))
        } else {
            self.router.get_target_node_id(&target_clientid)
        };
        let target_nodeid = target_nodeid.unwrap_or_else(|| self.scx.node.id());

        if target_nodeid == self.scx.node.id() {
            // During quiesce, do not add work to an existing local session.
            // Keeping its TCP connection alive is useful only if its current
            // queue and outbound inflight window are allowed to reach zero.
            if self.is_draining() || self.local_handoff_prepared() {
                self.enqueue_relations_handoff(target_nodeid, from, publish, relations).await?;
            } else {
                self.forwards_to(from, &publish, relations).await?;
            }
        } else {
            match self.handoff.begin_forward(target_nodeid, handoff_active) {
                ForwardMode::Reliable => {
                    self.enqueue_relations_handoff(target_nodeid, from, publish, relations).await?;
                }
                ForwardMode::Plain(plain_forward) => {
                    //forward other node
                    if let Some((client_generation, target_client)) =
                        self.grpc_client_with_generation(target_nodeid)
                    {
                        let retry_from = from.clone();
                        let retry_publish = publish.clone();
                        let retry_relation = relations[0].clone();
                        let message_type = self.message_type;
                        let rw_timeout = self.rw_timeout;

                        let msg_sender = MessageSender::new(
                            target_client,
                            message_type,
                            Message::ForwardsTo(from, publish, relations),
                            Some(rw_timeout),
                        )
                        .send();

                        self.scx.stats.forwards.inc();
                        let scx = self.scx.clone();
                        let shared = self.clone();
                        let forwards_fut = async move {
                            let _plain_forward = plain_forward;
                            match msg_sender.await {
                                Ok(MessageReply::Success) => {}
                                Ok(reply) => {
                                    log::warn!(
                                        "forwards Message::ForwardsTo to other node was rejected, to: {target_nodeid:?}, reply: {reply:?}"
                                    );
                                    if shared.handoff.is_draining_target(target_nodeid) {
                                        if let Err(droppeds) = shared
                                            .enqueue_relations_handoff(
                                                target_nodeid,
                                                retry_from,
                                                retry_publish,
                                                vec![retry_relation],
                                            )
                                            .await
                                        {
                                            for dropped in droppeds {
                                                scx.extends
                                                    .hook_mgr()
                                                    .message_dropped(
                                                        Some(dropped.0),
                                                        dropped.1,
                                                        dropped.2,
                                                        dropped.3,
                                                    )
                                                    .await;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::warn!(
                                        "forwards Message::ForwardsTo to other node, to: {target_nodeid:?}, error: {e:?}"
                                    );
                                    shared.schedule_grpc_client_repair(
                                        target_nodeid,
                                        client_generation,
                                        "direct forward",
                                    );
                                    if shared.handoff.is_draining_target(target_nodeid) {
                                        // A transport failure cannot tell whether the plain
                                        // request was accepted. Retrying without a DeliveryId
                                        // could duplicate it, so fail the rollout instead.
                                        shared.handoff.mark_ambiguous_plain_failure();
                                    }
                                }
                            }
                            scx.stats.forwards.dec();
                        };
                        let _reply = self.forwards_exec.spawn(forwards_fut).await;
                    } else {
                        drop(plain_forward);
                        log::warn!(
                            "forwards error, grpc_client is not exist, node_id: {target_nodeid}, relations: {relations:?}"
                        );
                        if self.handoff.is_draining_target(target_nodeid) {
                            self.enqueue_relations_handoff(target_nodeid, from, publish, relations).await?;
                        }
                    }
                }
            }
        }

        Ok(Some(vec![(target_clientid, None)]))
    }

    #[inline]
    async fn forward_to_subscriptions(
        &self,
        from: From,
        publish: Publish,
    ) -> std::result::Result<SubscriptionClientIds, Vec<(To, From, Publish, Reason)>> {
        let topic = &publish.topic;
        let mut relations_map = match self.scx.extends.router().await.matches(from.id.clone(), topic).await {
            Ok(relations_map) => relations_map,
            Err(e) => {
                log::warn!("forwards, from:{from:?}, topic:{topic:?}, error: {e:?}");
                SubRelationsMap::default()
            }
        };

        let sub_client_ids = self.inner()._collect_subscription_client_ids(&relations_map);

        let mut errs = Vec::new();

        let this_node_id = self.scx.node.id();
        if let Some(relations) = relations_map.remove(&this_node_id) {
            //forwards to local
            if let Err(e) = self.forwards_to(from.clone(), &publish, relations).await {
                errs.extend(e);
            }
        }
        if !relations_map.is_empty() {
            log::debug!("forwards to other nodes, relations_map:{relations_map:?}");
            let rw_timeout = self.rw_timeout;
            let handoff_active = self.handoff.operation_active();
            //forwards to other nodes
            let mut fut_senders = Vec::new();
            for (node_id, relations) in relations_map {
                match self.handoff.begin_forward(node_id, handoff_active) {
                    ForwardMode::Reliable => {
                        if let Err(droppeds) = self
                            .enqueue_relations_handoff(node_id, from.clone(), publish.clone(), relations)
                            .await
                        {
                            errs.extend(droppeds);
                        }
                    }
                    ForwardMode::Plain(plain_forward) => {
                        if let Some((client_generation, mut client)) =
                            self.grpc_client_with_generation(node_id)
                        {
                            let from = from.clone();
                            let publish = publish.clone();
                            let retry_from = from.clone();
                            let retry_publish = publish.clone();
                            let retry_relations = relations.clone();
                            let message_type = self.message_type;
                            let fut_sender = async move {
                                let _plain_forward = plain_forward;
                                let msg_sender = client.send_message(
                                    message_type,
                                    Message::ForwardsTo(from, publish, relations),
                                    Some(rw_timeout),
                                );
                                (
                                    node_id,
                                    client_generation,
                                    retry_from,
                                    retry_publish,
                                    retry_relations,
                                    msg_sender.await,
                                )
                            };
                            fut_senders.push(fut_sender.boxed());
                        } else {
                            drop(plain_forward);
                            log::warn!(
                                "forwards error, grpc_client is not exist, node_id: {node_id}, relations: {relations:?}"
                            );
                            if self.handoff.is_draining_target(node_id) {
                                if let Err(droppeds) = self
                                    .enqueue_relations_handoff(
                                        node_id,
                                        from.clone(),
                                        publish.clone(),
                                        relations,
                                    )
                                    .await
                                {
                                    errs.extend(droppeds);
                                }
                            }
                        }
                    }
                }
            }

            //@TODO ... If the received message is greater than the sending rate, it is necessary to control the message receiving rate
            if !fut_senders.is_empty() {
                self.scx.stats.forwards.inc();
                let scx = self.scx.clone();
                let shared = self.clone();
                let forwards_fut = async move {
                    let replys = futures::future::join_all(fut_senders).await;
                    for (node_id, client_generation, from, publish, relations, reply) in replys {
                        match reply {
                            Ok(MessageReply::Success) => {}
                            Ok(reply) => {
                                log::warn!(
                                    "forwards Message::ForwardsTo to other node was rejected, from: {from:?}, to: {node_id:?}, reply: {reply:?}"
                                );
                                if shared.handoff.is_draining_target(node_id) {
                                    if let Err(droppeds) = shared
                                        .enqueue_relations_handoff(node_id, from, publish, relations)
                                        .await
                                    {
                                        for dropped in droppeds {
                                            scx.extends
                                                .hook_mgr()
                                                .message_dropped(
                                                    Some(dropped.0),
                                                    dropped.1,
                                                    dropped.2,
                                                    dropped.3,
                                                )
                                                .await;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "forwards Message::ForwardsTo to other node, from: {from:?}, to: {node_id:?}, error: {e:?}"
                                );
                                shared.schedule_grpc_client_repair(
                                    node_id,
                                    client_generation,
                                    "subscription fan-out",
                                );
                                if shared.handoff.is_draining_target(node_id) {
                                    shared.handoff.mark_ambiguous_plain_failure();
                                }
                            }
                        }
                    }
                    scx.stats.forwards.dec();
                };
                let _reply = self.forwards_exec.spawn(forwards_fut).await;
            }
        }

        if errs.is_empty() {
            Ok(sub_client_ids)
        } else {
            Err(errs)
        }
    }
}

fn next_retry_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

#[async_trait]
impl Shared for ClusterShared {
    #[inline]
    fn entry(&self, id: Id) -> Box<dyn Entry> {
        Box::new(ClusterLockEntry::new(
            self.scx.clone(),
            self.exec.clone(),
            self.inner.entry(id),
            self.clone(),
            None,
        ))
    }

    #[inline]
    fn exist(&self, client_id: &str) -> bool {
        self.inner.exist(client_id)
    }

    #[inline]
    async fn forwards(
        &self,
        from: From,
        publish: Publish,
    ) -> std::result::Result<SubscriptionClientIds, Vec<(To, From, Publish, Reason)>> {
        log::debug!("[forwards] from: {from:?}, publish: {publish:?}");
        if let Some(target_clientid) = publish.target_clientid.clone() {
            self.forward_to_target_client(from, publish, target_clientid).await
        } else {
            self.forward_to_subscriptions(from, publish).await
        }
    }

    #[inline]
    async fn forwards_to(
        &self,
        from: From,
        publish: &Publish,
        relations: SubRelations,
    ) -> std::result::Result<(), Vec<(To, From, Publish, Reason)>> {
        if self.is_draining() || self.local_handoff_prepared() {
            return self
                .enqueue_relations_handoff(self.scx.node.id(), from, publish.clone(), relations)
                .await;
        }

        match self.inner.forwards_to(from, publish, relations).await {
            Ok(()) => Ok(()),
            Err(droppeds) => {
                let mut remaining = Vec::new();
                for dropped in droppeds {
                    if let Err(dropped) = self.reroute_stale_local_delivery(dropped).await {
                        remaining.push(dropped);
                    }
                }
                if remaining.is_empty() {
                    Ok(())
                } else {
                    Err(remaining)
                }
            }
        }
    }

    #[inline]
    async fn forwards_and_get_shareds(
        &self,
        from: From,
        publish: Publish,
    ) -> std::result::Result<(SubRelationsMap, SubscriptionClientIds), Vec<(To, From, Publish, Reason)>> {
        self.inner.forwards_and_get_shareds(from, publish).await
    }

    #[inline]
    fn iter(&self) -> Box<dyn Iterator<Item = Box<dyn Entry>> + Sync + Send + '_> {
        self.inner.iter()
    }

    #[inline]
    fn random_session(&self) -> Option<Session> {
        self.inner.random_session()
    }

    #[inline]
    async fn session_status(&self, client_id: &str) -> Option<SessionStatus> {
        let try_lock_timeout = self.router.try_lock_timeout;
        self.router.status(client_id).map(|s| SessionStatus {
            handshaking: s.handshaking(try_lock_timeout),
            id: s.id,
            online: s.online,
        })
    }

    #[inline]
    async fn client_states_count(&self) -> usize {
        self.router.states_count()
    }

    #[inline]
    fn sessions_count(&self) -> usize {
        self.inner.sessions_count()
    }

    #[inline]
    async fn query_subscriptions(&self, q: &SubsSearchParams) -> Vec<SubsSearchResult> {
        self.inner.query_subscriptions(q).await
    }

    #[inline]
    async fn message_load(
        &self,
        client_id: &str,
        topic_filter: &str,
        group: Option<&SharedGroup>,
    ) -> Result<Vec<(MsgID, From, Publish)>> {
        self.inner.message_load(client_id, topic_filter, group).await
    }

    #[inline]
    async fn subscriptions_count(&self) -> usize {
        self.inner.subscriptions_count().await
    }
    #[inline]
    fn get_grpc_clients(&self) -> GrpcClients {
        self.nodes.grpc_clients()
    }

    fn metrics_node_addrs(&self) -> Option<rmqtt::types::HashMap<NodeId, rmqtt::types::Addr>> {
        self.nodes.metrics_node_addrs()
    }

    #[inline]
    fn node_name(&self, id: NodeId) -> String {
        self.nodes.node_name(id)
    }

    #[inline]
    async fn check_health(&self) -> Result<HealthInfo> {
        let mut nodes_health_infos = Vec::new();
        let mut leader_ids = HashSet::default();
        let status = self.health_status().await?;
        if let Some(leader_id) = status.leader_id {
            if leader_id > 0 {
                leader_ids.insert(leader_id);
            }
        }

        nodes_health_infos.push(status);

        let data = RaftGrpcMessage::GetNodeHealthStatus.encode()?;
        let replys = MessageBroadcaster::new(
            self.nodes.grpc_clients(),
            self.message_type,
            Message::Data(data),
            Some(Duration::from_secs(5)),
        )
        .join_all()
        .await;

        for (node_id, reply) in replys {
            match reply {
                Ok(reply) => match reply {
                    MessageReply::Data(data) => {
                        let RaftGrpcMessageReply::GetNodeHealthStatus(o_status) =
                            RaftGrpcMessageReply::decode(&data)?
                        else {
                            return Err(anyhow!("unexpected raft node health response"));
                        };
                        if let Some(leader_id) = o_status.leader_id {
                            if leader_id > 0 {
                                leader_ids.insert(leader_id);
                            }
                        }
                        nodes_health_infos.push(o_status);
                    }
                    MessageReply::Error(err) => {
                        log::error!("Get RaftGrpcMessage::GetRaftStatus from other node, error: {err}");
                        nodes_health_infos.push(NodeHealthStatus {
                            node_id,
                            running: false,
                            leader_id: None,
                            descr: Some(err),
                        });
                    }
                    _ => {
                        log::error!("unreachable!(), reply: {reply:?}")
                    }
                },
                Err(e) => {
                    log::error!("Get RaftGrpcMessage::GetRaftStatus from other node, error: {e:?}");
                    nodes_health_infos.push(NodeHealthStatus {
                        node_id,
                        running: false,
                        leader_id: None,
                        descr: Some(e.to_string()),
                    });
                }
            }
        }
        let (running, descr) = if leader_ids.len() != 1 {
            (false, "Leader ID exception")
        } else if leader_ids.into_iter().next() == Some(0) {
            (false, "Leader does not exist")
        } else {
            (true, "Ok")
        };

        Ok(HealthInfo { running, descr: Some(descr.into()), nodes: nodes_health_infos })
    }

    #[inline]
    async fn health_status(&self) -> Result<NodeHealthStatus> {
        let mailbox = self.router.raft_mailbox().await;
        let status = mailbox.status().await.map_err(Error::new)?;
        let (running, leader_id) = if status.available() { (true, status.leader_id) } else { (false, 0) };
        Ok(NodeHealthStatus { node_id: status.id, running, leader_id: Some(leader_id), descr: None })
    }

    #[inline]
    fn operation_is_busy(&self) -> bool {
        self.exec.active_count() > self.exec_queue_workers_busy_limit
            || self.exec.waiting_count() > self.exec_queue_busy_limit
    }
}

#[cfg(test)]
mod drain_takeover_tests {
    use super::{drain_target_allowed, effective_source_session_policy};
    use ahash::HashSet;

    #[test]
    fn planned_drain_preserves_state_and_routes_for_client_reconnects() {
        assert_eq!(effective_source_session_policy(true, true, true, false), (false, false));
        assert_eq!(effective_source_session_policy(false, true, true, false), (false, false));
    }

    #[test]
    fn normal_takeover_and_admin_kick_keep_existing_policy() {
        assert_eq!(effective_source_session_policy(true, true, false, false), (true, true));
        assert_eq!(effective_source_session_policy(false, true, false, false), (false, true));
        assert_eq!(effective_source_session_policy(true, true, true, true), (true, true));
    }

    #[test]
    fn planned_drain_relocation_respects_current_step_targets() {
        let current_step_targets = [1, 2, 3, 4].into_iter().collect::<HashSet<_>>();
        assert!(drain_target_allowed(Some(&current_step_targets), 1));
        assert!(drain_target_allowed(Some(&current_step_targets), 4));
        assert!(!drain_target_allowed(Some(&current_step_targets), 5));
        assert!(drain_target_allowed(None, 5));
    }
}
