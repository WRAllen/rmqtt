//! Conservative, maintenance-only reconciliation of an unowned MQTT session.
//! A missing local entry is insufficient: session installation and snapshot
//! extraction deliberately have intervals without a shared-map entry.

use std::collections::BTreeSet;

use super::*;

#[derive(Clone)]
pub(super) struct SessionReconcileLimits {
    requests: Arc<tokio::sync::Semaphore>,
    checks: Arc<tokio::sync::Semaphore>,
}

impl Default for SessionReconcileLimits {
    fn default() -> Self {
        Self {
            requests: Arc::new(tokio::sync::Semaphore::new(1)),
            checks: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct SessionTransitions(Arc<DashMap<ClientId, usize>>);

pub(crate) struct SessionTransition {
    transitions: SessionTransitions,
    client_id: ClientId,
}

impl SessionTransitions {
    pub(super) fn begin(&self, client_id: ClientId) -> SessionTransition {
        *self.0.entry(client_id.clone()).or_default() += 1;
        SessionTransition { transitions: self.clone(), client_id }
    }

    fn active(&self, client_id: &str) -> bool {
        self.0.contains_key(client_id)
    }
}

impl Drop for SessionTransition {
    fn drop(&mut self) {
        if let dashmap::mapref::entry::Entry::Occupied(mut entry) =
            self.transitions.0.entry(self.client_id.clone())
        {
            if *entry.get() == 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }
}

#[async_trait]
trait ReconcileBackend: Sync {
    async fn members(&self) -> Result<Vec<NodeId>>;
    async fn probe(&self, node_id: NodeId, client_id: &ClientId) -> Result<bool>;
    fn local_busy(&self, client_id: &str) -> bool;
    fn owner(&self, client_id: &str) -> Option<Id>;
    async fn terminate(&self, expected: &Id) -> Result<bool>;
    async fn authoritative_owner(&self, client_id: &str) -> Result<Option<NodeId>>;
}

// The caller holds the local per-client entry lock for this whole operation.
// For Some(owner), the caller must be that owner. Existing local sessions,
// in-progress installs, retry snapshots and accepted deliveries all veto it.
async fn verify_absent<B: ReconcileBackend>(
    backend: &B,
    client_id: &ClientId,
    expected: Option<&Id>,
) -> Result<bool> {
    if backend.local_busy(client_id) || backend.owner(client_id).as_ref() != expected {
        return Ok(false);
    }
    let members = backend.members().await?;
    if members.is_empty() {
        return Ok(false);
    }
    let probes =
        futures::future::try_join_all(members.iter().map(|id| backend.probe(*id, client_id))).await?;
    if probes.into_iter().any(|busy| busy)
        || backend.local_busy(client_id)
        || backend.owner(client_id).as_ref() != expected
        || backend.members().await? != members
    {
        return Ok(false);
    }
    if let Some(expected) = expected {
        // Require an explicit acknowledgement that THIS connection Id was
        // removed. An empty reply from an older Raft leader is not proof.
        if !backend.terminate(expected).await? {
            return Ok(false);
        }
    }
    Ok(backend.authoritative_owner(client_id).await?.is_none()
        && backend.owner(client_id).is_none()
        && !backend.local_busy(client_id)
        && backend.members().await? == members)
}

#[async_trait]
impl ReconcileBackend for ClusterShared {
    async fn members(&self) -> Result<Vec<NodeId>> {
        let status = self.router.raft_mailbox().await.status().await.map_err(anyhow::Error::new)?;
        if !status.is_started() || !status.available() || !status.local_caught_up() {
            return Err(anyhow!("Raft is not ready for session reconciliation"));
        }
        // Do not mistake a missing business gRPC client for an absent MQTT
        // session. Include Raft members even when NodeDown removed metadata.
        let mut ids = status.voters.into_iter().chain(status.learners).collect::<BTreeSet<_>>();
        ids.extend(status.peers.keys().copied());
        ids.extend(self.nodes.snapshot_nodes().into_iter().map(|node| node.id));
        ids.insert(self.scx.node.id());
        Ok(ids.into_iter().collect())
    }

    async fn probe(&self, node_id: NodeId, client_id: &ClientId) -> Result<bool> {
        if node_id == self.scx.node.id() {
            // The coordinator already holds its local entry lock.
            return Ok(self.local_session_busy(client_id));
        }
        let reply = self
            .send_session_reconcile_message(
                node_id,
                RaftGrpcMessage::ProbeHandoffSession { client_id: client_id.clone() },
            )
            .await?;
        match reply {
            RaftGrpcMessageReply::HandoffSessionProbe { node_id: id, busy } if id == node_id => Ok(busy),
            _ => Err(anyhow!("invalid session probe response from node {node_id}")),
        }
    }

    fn local_busy(&self, client_id: &str) -> bool {
        self.local_session_busy(client_id)
    }

    fn owner(&self, client_id: &str) -> Option<Id> {
        self.router.id(client_id)
    }

    async fn terminate(&self, expected: &Id) -> Result<bool> {
        let reply = self
            .router
            .raft_mailbox()
            .await
            .send_proposal(RaftMessage::SessionTerminated { id: expected.clone() }.encode()?)
            .await
            .map_err(anyhow::Error::new)?;
        if reply.is_empty() {
            return Ok(false);
        }
        Ok(matches!(RaftMessageReply::decode(&reply)?, RaftMessageReply::SessionTerminated { removed: true }))
    }

    async fn authoritative_owner(&self, client_id: &str) -> Result<Option<NodeId>> {
        get_client_node_id(self.router.raft_mailbox().await, client_id).await
    }
}

impl ClusterShared {
    pub(crate) async fn lock_session_for_takeover(&self, id: Id) -> Result<Box<dyn Entry>> {
        // Snapshot extraction holds this same lock until its retry copy is
        // visible. Wait for that short critical section instead of mistaking
        // its detached entry for a nonexistent session.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(entry) = self.inner.entry(id.clone()).try_lock().await {
                    return entry;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("session takeover is waiting for another session transition"))
    }

    pub(crate) fn begin_session_transition(&self, client_id: ClientId) -> SessionTransition {
        self.session_transitions.begin(client_id)
    }

    pub(crate) fn local_session_busy(&self, client_id: &str) -> bool {
        self.session_transitions.active(client_id)
            || self.inner.exist(client_id) // Includes valid offline sessions.
            || self.drain_session_handoffs.contains_key(client_id)
            || self.proactive_session_handoffs.contains_key(client_id)
            || self.handoff.has_protected_delivery(client_id, self.scx.node.id())
    }

    pub(crate) async fn probe_handoff_session(&self, client_id: &ClientId) -> Result<bool> {
        let entry = self.inner.entry(Id::from(self.scx.node.id(), client_id.clone()));
        let Ok(_locked) = entry.try_lock().await else {
            return Ok(true);
        };
        let _members = self.members().await?;
        Ok(self.local_session_busy(client_id))
    }

    pub(crate) async fn reconcile_handoff_session(
        &self,
        client_id: &ClientId,
        expected: Option<&Id>,
    ) -> Result<bool> {
        // Bound maintenance work independently of the number of stuck clients.
        // Never wait for this permit while holding a session entry lock.
        let Ok(_permit) = self.reconcile_limits.checks.try_acquire() else {
            return Ok(false);
        };
        if expected.is_some_and(|id| id.node_id != self.scx.node.id() || id.client_id != client_id) {
            return Ok(false);
        }
        let entry = self.inner.entry(Id::from(self.scx.node.id(), client_id.clone()));
        let Ok(_locked) = entry.try_lock().await else {
            return Ok(false);
        };
        let gone = tokio::time::timeout(
            self.rw_timeout.min(Duration::from_secs(3)),
            verify_absent(self, client_id, expected),
        )
        .await??;
        if gone && expected.is_some() {
            log::warn!(
                "reconciled obsolete session owner after all members confirmed absence, client_id={client_id}, connection_id={:?}, node_id={}",
                expected, self.scx.node.id(),
            );
        }
        Ok(gone)
    }

    async fn send_session_reconcile_message(
        &self,
        node_id: NodeId,
        message: RaftGrpcMessage,
    ) -> Result<RaftGrpcMessageReply> {
        let (_, mut client) = self
            .grpc_client_with_generation(node_id)
            .ok_or_else(|| anyhow!("session reconciliation peer {node_id} is unavailable"))?;
        match client
            .send_message(self.message_type, Message::Data(message.encode()?), Some(self.rw_timeout))
            .await?
        {
            MessageReply::Data(data) => RaftGrpcMessageReply::decode(&data),
            reply => Err(anyhow!("session reconciliation peer {node_id} returned {reply:?}")),
        }
    }

    pub(super) async fn reconcile_queued_session(&self, client_id: &ClientId) -> Result<bool> {
        let Ok(_permit) = self.reconcile_limits.requests.try_acquire() else {
            return Ok(false);
        };
        let expected = self.router.id(client_id);
        let owner = expected.as_ref().map(|id| id.node_id).unwrap_or_else(|| self.scx.node.id());
        if owner == self.scx.node.id() {
            return self.reconcile_handoff_session(client_id, expected.as_ref()).await;
        }
        match self
            .send_session_reconcile_message(
                owner,
                RaftGrpcMessage::ReconcileHandoffSession {
                    client_id: client_id.clone(),
                    expected_owner: expected.clone(),
                },
            )
            .await?
        {
            RaftGrpcMessageReply::HandoffSessionReconciled { client_id: id, expected_owner, gone }
                if id == client_id && expected_owner == expected =>
            {
                Ok(gone)
            }
            _ => Err(anyhow!("invalid session reconciliation response from node {owner}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::*;

    struct MockBackend {
        state: Mutex<State>,
    }

    struct State {
        owner: Option<Id>,
        busy_nodes: Vec<NodeId>,
        unavailable_node: Option<NodeId>,
        transition: bool,
        reconnect_on_probe: bool,
        reconnect_on_terminate: bool,
        transition_on_probe: bool,
        acknowledge_removal: bool,
        leader_owner: Option<NodeId>,
        membership_change_at: usize,
        membership_reads: usize,
        termination_attempts: usize,
    }

    impl MockBackend {
        fn new(owner: Option<Id>) -> Self {
            Self {
                state: Mutex::new(State {
                    owner,
                    busy_nodes: Vec::new(),
                    unavailable_node: None,
                    transition: false,
                    reconnect_on_probe: false,
                    reconnect_on_terminate: false,
                    transition_on_probe: false,
                    acknowledge_removal: true,
                    leader_owner: None,
                    membership_change_at: usize::MAX,
                    membership_reads: 0,
                    termination_attempts: 0,
                }),
            }
        }
    }

    #[async_trait]
    impl ReconcileBackend for MockBackend {
        async fn members(&self) -> Result<Vec<NodeId>> {
            let mut s = self.state.lock();
            s.membership_reads += 1;
            Ok(if s.membership_reads >= s.membership_change_at { vec![1, 2, 3, 4] } else { vec![1, 2, 3] })
        }

        async fn probe(&self, node_id: NodeId, client_id: &ClientId) -> Result<bool> {
            let mut s = self.state.lock();
            if s.unavailable_node == Some(node_id) {
                return Err(anyhow!("unreachable or unsupported peer"));
            }
            if s.reconnect_on_probe {
                s.owner = Some(Id::new(3, 0, None, None, client_id.clone(), None));
            }
            s.transition |= s.transition_on_probe;
            Ok(s.busy_nodes.contains(&node_id))
        }

        fn local_busy(&self, _client_id: &str) -> bool {
            self.state.lock().transition
        }

        fn owner(&self, _client_id: &str) -> Option<Id> {
            self.state.lock().owner.clone()
        }

        async fn terminate(&self, expected: &Id) -> Result<bool> {
            let mut s = self.state.lock();
            s.termination_attempts += 1;
            if s.reconnect_on_terminate {
                s.owner = Some(expected.relocated_to(3));
            }
            if !s.acknowledge_removal || s.owner.as_ref() != Some(expected) {
                return Ok(false);
            }
            s.owner = None;
            Ok(true)
        }

        async fn authoritative_owner(&self, _client_id: &str) -> Result<Option<NodeId>> {
            Ok(self.state.lock().leader_owner)
        }
    }

    fn owner() -> Id {
        Id::new(1, 0, None, None, ClientId::from_static("old-client"), None)
    }

    #[tokio::test]
    async fn missing_session_with_stale_owner_is_conditionally_removed() {
        let id = owner();
        let backend = MockBackend::new(Some(id.clone()));
        assert!(verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
        assert_eq!(backend.state.lock().termination_attempts, 1);
    }

    #[tokio::test]
    async fn absent_owner_still_requires_every_peer_and_leader_confirmation() {
        let id = owner();
        let backend = MockBackend::new(None);
        backend.state.lock().leader_owner = Some(2);
        assert!(!verify_absent(&backend, &id.client_id, None).await.unwrap());
        backend.state.lock().leader_owner = None;
        assert!(verify_absent(&backend, &id.client_id, None).await.unwrap());
        assert_eq!(backend.state.lock().termination_attempts, 0);
    }

    #[tokio::test]
    async fn offline_session_or_retry_snapshot_on_any_peer_vetoes_cleanup() {
        let id = owner();
        for node_id in [1, 2, 3] {
            let backend = MockBackend::new(Some(id.clone()));
            backend.state.lock().busy_nodes.push(node_id);
            assert!(!verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
            assert_eq!(backend.state.lock().termination_attempts, 0);
        }
    }

    #[tokio::test]
    async fn unavailable_or_old_peer_is_not_treated_as_absent() {
        let id = owner();
        let backend = MockBackend::new(Some(id.clone()));
        backend.state.lock().unavailable_node = Some(3);
        assert!(verify_absent(&backend, &id.client_id, Some(&id)).await.is_err());
        assert_eq!(backend.state.lock().termination_attempts, 0);
    }

    #[tokio::test]
    async fn active_install_without_a_shared_entry_is_preserved() {
        let id = owner();
        let backend = MockBackend::new(Some(id.clone()));
        backend.state.lock().transition = true;
        assert!(!verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
        assert_eq!(backend.state.lock().membership_reads, 0);
    }

    #[tokio::test]
    async fn reconnect_or_install_starting_during_probes_vetoes_cleanup() {
        let id = owner();
        for reconnect in [false, true] {
            let backend = MockBackend::new(Some(id.clone()));
            backend.state.lock().reconnect_on_probe = reconnect;
            backend.state.lock().transition_on_probe = !reconnect;
            assert!(!verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
            assert_eq!(backend.state.lock().termination_attempts, 0);
        }
    }

    #[tokio::test]
    async fn reconnect_between_validation_and_raft_apply_survives() {
        let id = owner();
        let backend = MockBackend::new(Some(id.clone()));
        backend.state.lock().reconnect_on_terminate = true;
        assert!(!verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
        assert_eq!(backend.state.lock().owner.as_ref().unwrap().node_id, 3);
    }

    #[tokio::test]
    async fn missing_explicit_termination_ack_does_not_release_messages() {
        let id = owner();
        let backend = MockBackend::new(Some(id.clone()));
        backend.state.lock().acknowledge_removal = false;
        assert!(!verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
    }

    #[tokio::test]
    async fn membership_change_before_or_after_removal_defers_retirement() {
        let id = owner();
        for membership_change_at in [2, 3] {
            let backend = MockBackend::new(Some(id.clone()));
            backend.state.lock().membership_change_at = membership_change_at;
            assert!(!verify_absent(&backend, &id.client_id, Some(&id)).await.unwrap());
            if membership_change_at == 2 {
                assert_eq!(backend.state.lock().termination_attempts, 0);
            }
        }
    }

    #[test]
    fn overlapping_transitions_stay_busy_until_every_guard_is_dropped() {
        let transitions = SessionTransitions::default();
        let id = ClientId::from_static("client");
        let first = transitions.begin(id.clone());
        let second = transitions.begin(id.clone());
        drop(first);
        assert!(transitions.active(&id));
        drop(second);
        assert!(!transitions.active(&id));
        assert!(transitions.0.is_empty());
    }

    #[tokio::test]
    async fn cancellation_releases_the_transition_marker() {
        let transitions = SessionTransitions::default();
        let copied = transitions.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = copied.begin(ClientId::from_static("client"));
            tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        rx.await.unwrap();
        assert!(transitions.active("client"));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!transitions.active("client"));
    }
}
