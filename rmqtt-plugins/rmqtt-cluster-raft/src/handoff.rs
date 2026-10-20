use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;
use rand::random;
use tokio::sync::Notify;

use rmqtt::types::{ClientId, From, NodeId, Publish, SubRelation, SubRelations};

use super::config::Handoff;
use super::handoff_diagnostics::ForwardAttemptStage;
use super::message::{DeliveryId, ReliableForwardStatus};

#[derive(Clone, Debug)]
pub(crate) struct PendingHandoff {
    pub delivery_id: DeliveryId,
    pub target_client_id: ClientId,
    pub target_node_id: Option<NodeId>,
    pub avoid_node_id: Option<NodeId>,
    pub from: From,
    pub publish: Publish,
    pub relations: SubRelations,
    /// Recovery intent reconstructed from a session context, not a complete
    /// snapshot of a subscriber's protocol options. Bind it before delivery.
    pub shared_relation_unbound: bool,
    pub created_at: Instant,
    pub size: usize,
}

impl PendingHandoff {
    pub(crate) fn is_shared(&self) -> bool {
        self.relations.len() == 1 && (self.relations[0].4.is_some() || self.relations[0].2.has_shared_group())
    }

    pub(crate) fn new(
        delivery_id: DeliveryId,
        target_client_id: ClientId,
        target_node_id: Option<NodeId>,
        avoid_node_id: Option<NodeId>,
        from: From,
        publish: Publish,
        relations: SubRelations,
    ) -> Self {
        let size = publish.payload.len()
            + publish.topic.len()
            + target_client_id.len()
            + relations
                .iter()
                .map(|(topic_filter, client_id, _, sub_ids, group)| {
                    topic_filter.len()
                        + client_id.len()
                        + sub_ids.as_ref().map(Vec::len).unwrap_or_default() * std::mem::size_of::<usize>()
                        + group
                            .as_ref()
                            .map(|(group, _, clients)| {
                                group.len() + clients.iter().map(|client| client.len()).sum::<usize>()
                            })
                            .unwrap_or_default()
                })
                .sum::<usize>();
        Self {
            delivery_id,
            target_client_id,
            target_node_id,
            avoid_node_id,
            from,
            publish,
            relations,
            shared_relation_unbound: false,
            created_at: Instant::now(),
            size,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryState {
    Pending,
    Completed(Instant),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainTargetState {
    Prepared,
    Avoid,
}

#[derive(Debug)]
struct QueuedHandoff {
    pending: PendingHandoff,
    pinned_node_id: Option<NodeId>,
    remotely_completed: bool,
    transport_uncertain: bool,
    rejected: bool,
    // Observations only: never use these fields to release or reroute work.
    diagnostic_pin_origin: Option<&'static str>,
    diagnostic_accepted_seen: bool,
    diagnostic_first_uncertain_stage: Option<ForwardAttemptStage>,
}

impl QueuedHandoff {
    fn new(pending: PendingHandoff) -> Self {
        Self {
            pending,
            pinned_node_id: None,
            remotely_completed: false,
            transport_uncertain: false,
            rejected: false,
            diagnostic_pin_origin: None,
            diagnostic_accepted_seen: false,
            diagnostic_first_uncertain_stage: None,
        }
    }
}

// Debug emits every field in the rate-limited diagnostic log.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct HandoffDiagnosticSnapshot {
    pub head_delivery_id: DeliveryId,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub shared_messages: usize,
    pub pinned_messages: usize,
    pub uncertain_messages: usize,
    pub completed_behind_head: usize,
    pub head_pinned_node_id: Option<NodeId>,
    pub head_pin_origin: Option<&'static str>,
    pub head_accepted_seen: bool,
    pub head_first_uncertain_stage: Option<ForwardAttemptStage>,
    pub head_transport_uncertain: bool,
    pub head_rejected: bool,
    pub recovery_parent: Option<DeliveryId>,
}

#[derive(Default)]
struct ClientQueue {
    messages: VecDeque<QueuedHandoff>,
    worker_running: bool,
}

struct CapacityWaiterGuard<'a>(&'a AtomicUsize);

impl<'a> CapacityWaiterGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for CapacityWaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// An admission can reserve quota before its queue item is visible. A queue
/// audit must not repair those live reservations as if they were orphans.
struct AdmissionGuard<'a> {
    active: &'a AtomicUsize,
    progress: &'a AtomicU64,
}

impl<'a> AdmissionGuard<'a> {
    fn new(active: &'a AtomicUsize, progress: &'a AtomicU64) -> Self {
        active.fetch_add(1, Ordering::SeqCst);
        Self { active, progress }
    }
}

impl Drop for AdmissionGuard<'_> {
    fn drop(&mut self) {
        self.progress.fetch_add(1, Ordering::Release);
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HandoffBatch {
    pub messages: Vec<PendingHandoff>,
    pub pinned_node_id: Option<NodeId>,
    pub avoid_node_id: Option<NodeId>,
}

#[derive(Debug, Clone)]
pub(crate) struct HandoffResult {
    pub delivery_id: DeliveryId,
    pub status: ReliableForwardStatus,
}

pub(crate) struct RetryActivityGuard {
    global: Arc<AtomicUsize>,
    by_target: Arc<DashMap<NodeId, Arc<AtomicUsize>>>,
    target_node_id: NodeId,
}

pub(crate) struct PlainForwardGuard {
    counter: Option<Arc<AtomicUsize>>,
}

pub(crate) enum ForwardMode {
    Reliable,
    Plain(PlainForwardGuard),
}

impl Drop for PlainForwardGuard {
    fn drop(&mut self) {
        if let Some(counter) = self.counter.as_ref() {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for RetryActivityGuard {
    fn drop(&mut self) {
        self.global.fetch_sub(1, Ordering::SeqCst);
        if let Some(counter) = self.by_target.get(&self.target_node_id) {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BatchApplyResult {
    pub completed: usize,
    pub accepted: usize,
    pub retryable: usize,
    pub rejected: usize,
}

impl BatchApplyResult {
    pub(crate) fn made_progress(self) -> bool {
        self.completed > 0 || self.accepted > 0
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HandoffStats {
    pub pending_messages: usize,
    pub pending_bytes: usize,
    pub clients: usize,
    pub active_workers: usize,
    pub retrying_messages: usize,
    pub failed_messages: usize,
    pub rejected_messages: usize,
    pub oldest_age_millis: u64,
    pub progress_sequence: u64,
    pub capacity_percent: u8,
    pub capacity_waiters: usize,
    pub invariant_mismatch_observations: usize,
    /// Reliable handoff deliveries acknowledged since this process started.
    pub completed_messages: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HandoffDiscardStats {
    pub messages: usize,
    pub bytes: usize,
    pub clients: usize,
}

#[derive(Clone)]
pub(crate) struct HandoffManager {
    cfg: Handoff,
    boot_id: u64,
    sequence: Arc<AtomicU64>,
    queues: Arc<DashMap<ClientId, Arc<Mutex<ClientQueue>>>>,
    // Serializes maintenance queue creation, removal and cross-domain moves.
    // Never acquired by the ordinary publish fast path and never held over await.
    queue_mutation: Arc<Mutex<()>>,
    delivery_states: Arc<DashMap<DeliveryId, DeliveryState>>,
    // One protected parent per locally pending recovery child. Its duplicate
    // reply must not expire while this manager still owns the replacement.
    recovery_parents: Arc<DashMap<DeliveryId, DeliveryId>>,
    completed_order: Arc<Mutex<VecDeque<(Instant, DeliveryId)>>>,
    draining_targets: Arc<DashMap<NodeId, DrainTargetState>>,
    has_draining_targets: Arc<AtomicBool>,
    pending_by_target: Arc<DashMap<NodeId, Arc<AtomicUsize>>>,
    pending_messages: Arc<AtomicUsize>,
    pending_bytes: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
    retrying_messages: Arc<AtomicUsize>,
    active_by_target: Arc<DashMap<NodeId, Arc<AtomicUsize>>>,
    plain_forwards_by_target: Arc<DashMap<NodeId, Arc<AtomicUsize>>>,
    failed_messages: Arc<AtomicUsize>,
    rejected_messages: Arc<AtomicUsize>,
    progress_sequence: Arc<AtomicU64>,
    capacity_waiters: Arc<AtomicUsize>,
    admissions_in_progress: Arc<AtomicUsize>,
    invariant_mismatch_observations: Arc<AtomicUsize>,
    completed_messages: Arc<AtomicUsize>,
    completed_by_drain_target: Arc<DashMap<NodeId, Arc<AtomicUsize>>>,
    capacity_notify: Arc<Notify>,
    data_loss_detected: Arc<AtomicBool>,
}

impl HandoffManager {
    pub(crate) fn new(cfg: Handoff) -> Self {
        Self {
            cfg,
            boot_id: random(),
            sequence: Arc::new(AtomicU64::new(0)),
            queues: Arc::new(DashMap::new()),
            queue_mutation: Arc::new(Mutex::new(())),
            delivery_states: Arc::new(DashMap::new()),
            recovery_parents: Arc::new(DashMap::new()),
            completed_order: Arc::new(Mutex::new(VecDeque::new())),
            draining_targets: Arc::new(DashMap::new()),
            has_draining_targets: Arc::new(AtomicBool::new(false)),
            pending_by_target: Arc::new(DashMap::new()),
            pending_messages: Arc::new(AtomicUsize::new(0)),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            active_workers: Arc::new(AtomicUsize::new(0)),
            retrying_messages: Arc::new(AtomicUsize::new(0)),
            active_by_target: Arc::new(DashMap::new()),
            plain_forwards_by_target: Arc::new(DashMap::new()),
            failed_messages: Arc::new(AtomicUsize::new(0)),
            rejected_messages: Arc::new(AtomicUsize::new(0)),
            progress_sequence: Arc::new(AtomicU64::new(0)),
            capacity_waiters: Arc::new(AtomicUsize::new(0)),
            admissions_in_progress: Arc::new(AtomicUsize::new(0)),
            invariant_mismatch_observations: Arc::new(AtomicUsize::new(0)),
            completed_messages: Arc::new(AtomicUsize::new(0)),
            completed_by_drain_target: Arc::new(DashMap::new()),
            capacity_notify: Arc::new(Notify::new()),
            data_loss_detected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    pub(crate) fn retry_timeout(&self) -> Duration {
        self.cfg.retry_timeout
    }

    pub(crate) fn retry_interval(&self) -> Duration {
        self.cfg.retry_interval
    }

    pub(crate) fn retry_max_interval(&self) -> Duration {
        self.cfg.retry_max_interval
    }

    pub(crate) fn next_delivery_id(&self, origin_node_id: NodeId) -> DeliveryId {
        DeliveryId {
            origin_node_id,
            boot_id: self.boot_id,
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed) + 1,
        }
    }

    pub(crate) fn prepare_drain(&self, node_id: NodeId) -> bool {
        if !self.enabled() {
            return false;
        }
        self.completed_by_drain_target.entry(node_id).or_insert_with(|| Arc::new(AtomicUsize::new(0)));
        self.draining_targets
            .entry(node_id)
            .and_modify(|state| {
                if *state != DrainTargetState::Avoid {
                    *state = DrainTargetState::Prepared;
                }
            })
            .or_insert(DrainTargetState::Prepared);
        self.has_draining_targets.store(true, Ordering::Release);
        true
    }

    pub(crate) fn activate_drain(&self, node_id: NodeId) -> bool {
        if !self.enabled() {
            return false;
        }
        self.draining_targets.insert(node_id, DrainTargetState::Avoid);
        self.has_draining_targets.store(true, Ordering::Release);
        true
    }

    pub(crate) fn clear_drain(&self, node_id: NodeId) {
        self.draining_targets.remove(&node_id);
        self.completed_by_drain_target.remove(&node_id);
        if self.draining_targets.is_empty() {
            self.has_draining_targets.store(false, Ordering::Release);
        }

        // A rollback makes the original node eligible again. A remote pin is
        // deliberately retained: Accepted means that node may already have
        // delivered the message, so changing targets before the final
        // Delivered/Duplicate acknowledgement could create a duplicate.
        for queue_ref in self.queues.iter().map(|entry| entry.value().clone()).collect::<Vec<_>>() {
            let mut queue = queue_ref.lock();
            for item in &mut queue.messages {
                if item.pending.target_node_id == Some(node_id) || item.pending.avoid_node_id == Some(node_id)
                {
                    item.pending.avoid_node_id = None;
                }
            }
        }
        self.cleanup_completed();
        self.reset_operation_counters_if_idle();
        self.capacity_notify.notify_waiters();
    }

    /// Discard only deliveries tied to a force-finalized maintenance target.
    /// This is never called by the normal forwarding path.
    pub(crate) fn discard_for_target(&self, node_id: NodeId) -> HandoffDiscardStats {
        let queues =
            self.queues.iter().map(|entry| (entry.key().clone(), entry.value().clone())).collect::<Vec<_>>();
        let mut stats = HandoffDiscardStats::default();

        for (client_id, queue_ref) in queues {
            let mut queue = queue_ref.lock();
            let mut retained = VecDeque::with_capacity(queue.messages.len());
            let mut discarded = Vec::new();
            while let Some(item) = queue.messages.pop_front() {
                if item.pending.target_node_id == Some(node_id) || item.pending.avoid_node_id == Some(node_id)
                {
                    stats.messages += 1;
                    stats.bytes += item.pending.size;
                    discarded.push(item);
                } else {
                    retained.push_back(item);
                }
            }
            queue.messages = retained;
            drop(queue);

            if !discarded.is_empty() {
                stats.clients += 1;
                for item in discarded {
                    self.finish_item(item);
                }
            }
            self.remove_unused_queue(&client_id, &queue_ref);
        }

        if stats.messages > 0 {
            self.data_loss_detected.store(true, Ordering::Release);
            self.progress_sequence.fetch_add(1, Ordering::Release);
            self.capacity_notify.notify_waiters();
        }
        stats
    }

    pub(crate) fn is_draining_target(&self, node_id: NodeId) -> bool {
        self.enabled()
            && self.has_draining_targets.load(Ordering::Relaxed)
            && self.draining_targets.contains_key(&node_id)
    }

    pub(crate) fn should_avoid_target(&self, node_id: NodeId) -> bool {
        self.enabled()
            && self.has_draining_targets.load(Ordering::Relaxed)
            && self.draining_targets.get(&node_id).is_some_and(|state| *state == DrainTargetState::Avoid)
    }

    pub(crate) fn operation_active(&self) -> bool {
        self.enabled() && self.has_draining_targets.load(Ordering::Relaxed)
    }

    pub(crate) fn delivery_completed(&self, delivery_id: DeliveryId) -> bool {
        matches!(
            self.delivery_states.get(&delivery_id).map(|state| *state),
            Some(DeliveryState::Completed(_))
        )
    }

    pub(crate) fn delivery_pending(&self, delivery_id: DeliveryId) -> bool {
        matches!(self.delivery_states.get(&delivery_id).map(|state| *state), Some(DeliveryState::Pending))
    }

    pub(crate) fn mark_completed(&self, delivery_id: DeliveryId) {
        let completed_at = Instant::now();
        self.delivery_states.insert(delivery_id, DeliveryState::Completed(completed_at));
        let parent = self.recovery_parents.remove(&delivery_id).map(|(_, parent)| parent);
        if let Some(parent) = parent {
            // Start the parent's ordinary dedup window only after the local
            // replacement has finished, not when it first changed consumers.
            self.delivery_states.insert(parent, DeliveryState::Completed(completed_at));
        }
        {
            let mut completed = self.completed_order.lock();
            completed.push_back((completed_at, delivery_id));
            if let Some(parent) = parent {
                completed.push_back((completed_at, parent));
            }
        }
        self.progress_sequence.fetch_add(1, Ordering::Release);
        self.cleanup_completed();
    }

    pub(crate) async fn enqueue(&self, pending: PendingHandoff) -> Result<bool, PendingHandoff> {
        self.enqueue_with_hook(pending, || {}).await
    }

    pub(crate) fn shared_recovery_key(delivery_id: DeliveryId) -> String {
        format!(
            "\0shared-recovery/{}/{}/{}",
            delivery_id.origin_node_id, delivery_id.boot_id, delivery_id.sequence
        )
    }

    pub(crate) fn shared_local_key(client_id: &str) -> String {
        format!("\0shared-local/{client_id}")
    }

    pub(crate) fn is_shared_local_key(key: &str) -> bool {
        key.starts_with("\0shared-local/")
    }

    pub(crate) fn is_shared_recovery_key(key: &str) -> bool {
        key.starts_with("\0shared-recovery/")
    }

    /// Leave one slot and a bounded byte budget available for inbound local
    /// work. A one-slot legacy configuration retains its old behavior, but
    /// cannot safely originate a shared recovery obligation.
    fn outbound_limits(&self) -> (usize, usize) {
        if self.cfg.max_pending_messages < 2 || self.cfg.max_pending_bytes < 2 {
            return (self.cfg.max_pending_messages, self.cfg.max_pending_bytes);
        }
        let receive_bytes = self.shared_receive_bytes();
        (self.cfg.max_pending_messages - 1, self.cfg.max_pending_bytes - receive_bytes)
    }

    pub(crate) fn shared_receive_bytes(&self) -> usize {
        self.cfg.batch_max_bytes.max(1).min(self.cfg.max_pending_bytes / 2)
    }

    fn shared_size_supported(&self, pending: &PendingHandoff) -> bool {
        self.cfg.max_pending_messages >= 2
            && pending.is_shared()
            && pending.size <= self.shared_receive_bytes()
    }

    /// Independent recovery records are scheduled by a bounded coordinator;
    /// creating one does not start a per-message worker.
    pub(crate) async fn enqueue_shared_recovery(
        &self,
        mut pending: PendingHandoff,
    ) -> Result<String, PendingHandoff> {
        let key = Self::shared_recovery_key(pending.delivery_id);
        if self.delivery_states.contains_key(&pending.delivery_id) {
            return Ok(key);
        }
        if !self.enabled() || !self.shared_size_supported(&pending) {
            return Err(pending);
        }
        loop {
            let notified = self.capacity_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            pending = match self.try_enqueue_key(pending, &key, false, false) {
                Ok(_) => return Ok(key),
                Err(returned) => returned,
            };
            let waiter = CapacityWaiterGuard::new(&self.capacity_waiters);
            notified.as_mut().await;
            drop(waiter);
        }
    }

    /// RPC admission is nonblocking. Temporary capacity pressure is a
    /// Retryable condition, not a dropped or accepted delivery.
    pub(crate) fn try_enqueue_shared_local(&self, pending: PendingHandoff) -> Result<String, PendingHandoff> {
        let key = Self::shared_local_key(&pending.target_client_id);
        if self.delivery_states.contains_key(&pending.delivery_id) {
            return Ok(key);
        }
        if !self.enabled() || !self.shared_size_supported(&pending) {
            return Err(pending);
        }
        self.try_enqueue_key(pending, &key, true, false).map(|_| key)
    }

    fn try_enqueue_key(
        &self,
        pending: PendingHandoff,
        queue_key: &str,
        inbound: bool,
        start_worker: bool,
    ) -> Result<bool, PendingHandoff> {
        if self.delivery_states.contains_key(&pending.delivery_id) {
            return Ok(false);
        }
        let _admission = AdmissionGuard::new(&self.admissions_in_progress, &self.progress_sequence);
        let (message_limit, byte_limit) = if inbound {
            (self.cfg.max_pending_messages, self.cfg.max_pending_bytes)
        } else {
            self.outbound_limits()
        };
        if !reserve_counter(&self.pending_messages, 1, message_limit) {
            return Err(pending);
        }
        if !reserve_counter(&self.pending_bytes, pending.size, byte_limit) {
            self.pending_messages.fetch_sub(1, Ordering::SeqCst);
            return Err(pending);
        }
        match self.delivery_states.entry(pending.delivery_id) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                self.pending_messages.fetch_sub(1, Ordering::SeqCst);
                self.pending_bytes.fetch_sub(pending.size, Ordering::SeqCst);
                self.capacity_notify.notify_waiters();
                return Ok(false);
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(DeliveryState::Pending);
            }
        }
        if let Some(target) = pending.target_node_id {
            self.pending_by_target
                .entry(target)
                .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                .fetch_add(1, Ordering::SeqCst);
        }
        Ok(self.push_pending_to_queue(pending, queue_key.into(), start_worker, || {}))
    }

    async fn enqueue_with_hook<F>(
        &self,
        pending: PendingHandoff,
        before_queue_lock: F,
    ) -> Result<bool, PendingHandoff>
    where
        F: FnOnce(),
    {
        if !self.enabled() {
            return Err(pending);
        }
        if self.delivery_states.contains_key(&pending.delivery_id) {
            return Ok(false);
        }
        let (message_limit, byte_limit) = self.outbound_limits();
        if pending.size > byte_limit || message_limit == 0 {
            self.rejected_messages.fetch_add(1, Ordering::SeqCst);
            self.data_loss_detected.store(true, Ordering::Release);
            return Err(pending);
        }

        let size = pending.size;
        let _admission = loop {
            let notified = self.capacity_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let admission = AdmissionGuard::new(&self.admissions_in_progress, &self.progress_sequence);
            if reserve_counter(&self.pending_messages, 1, message_limit) {
                if reserve_counter(&self.pending_bytes, size, byte_limit) {
                    break admission;
                }
                self.pending_messages.fetch_sub(1, Ordering::SeqCst);
            }
            drop(admission);
            let waiter = CapacityWaiterGuard::new(&self.capacity_waiters);
            notified.as_mut().await;
            drop(waiter);
        };

        match self.delivery_states.entry(pending.delivery_id) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                self.pending_messages.fetch_sub(1, Ordering::SeqCst);
                self.pending_bytes.fetch_sub(size, Ordering::SeqCst);
                self.capacity_notify.notify_one();
                return Ok(false);
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(DeliveryState::Pending);
            }
        }
        if let Some(target_node_id) = pending.target_node_id {
            self.pending_by_target
                .entry(target_node_id)
                .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                .fetch_add(1, Ordering::SeqCst);
        }

        Ok(self.push_pending_to_client_queue(pending, before_queue_lock))
    }

    fn push_pending_to_client_queue<F>(&self, pending: PendingHandoff, before_queue_lock: F) -> bool
    where
        F: FnOnce(),
    {
        let client_id = pending.target_client_id.clone();
        self.push_pending_to_queue(pending, client_id, true, before_queue_lock)
    }

    fn push_pending_to_queue<F>(
        &self,
        pending: PendingHandoff,
        queue_key: ClientId,
        start_worker: bool,
        before_queue_lock: F,
    ) -> bool
    where
        F: FnOnce(),
    {
        let _mutation = self.queue_mutation.lock();
        let queue_entry =
            self.queues.entry(queue_key).or_insert_with(|| Arc::new(Mutex::new(ClientQueue::default())));
        let queue_ref = queue_entry.value().clone();

        // Keep the DashMap entry guard alive until the pending item and its
        // worker state are visible in the per-client queue. Otherwise an
        // empty-queue cleanup can remove the map entry after enqueue cloned
        // the Arc but before it acquires the queue lock. The item would then
        // be pushed into a detached queue, while the global pending/worker
        // counters could never be released.
        before_queue_lock();
        let mut queue = queue_ref.lock();
        queue.messages.push_back(QueuedHandoff::new(pending));
        self.progress_sequence.fetch_add(1, Ordering::Release);
        let start_worker = if queue.worker_running || !start_worker {
            false
        } else {
            queue.worker_running = true;
            self.active_workers.fetch_add(1, Ordering::SeqCst);
            true
        };
        drop(queue);
        drop(queue_entry);
        start_worker
    }

    /// Detach a local item which can no longer execute locally. It must not
    /// hold the local client's FIFO while awaiting another broker's ack.
    pub(crate) fn promote_shared_local_front(&self, queue_key: &str) -> Option<String> {
        if !Self::is_shared_local_key(queue_key) {
            return None;
        }
        self.move_shared_front(queue_key, |pending| Self::shared_recovery_key(pending.delivery_id))
    }

    /// Admission of a locally selected recovery task joins the local FIFO;
    /// this changes scheduling ownership, never delivery ownership or quota.
    pub(crate) fn move_shared_recovery_to_local(&self, queue_key: &str) -> Option<String> {
        if !Self::is_shared_recovery_key(queue_key) {
            return None;
        }
        self.move_shared_front(queue_key, |pending| Self::shared_local_key(&pending.target_client_id))
    }

    fn move_shared_front<F>(&self, queue_key: &str, destination: F) -> Option<String>
    where
        F: FnOnce(&PendingHandoff) -> String,
    {
        let _mutation = self.queue_mutation.lock();
        let source = self.queues.get(queue_key)?.value().clone();
        let (delivery_id, destination_key) = {
            let queue = source.lock();
            let head = queue.messages.front()?;
            if !head.pending.is_shared() || head.remotely_completed {
                return None;
            }
            (head.pending.delivery_id, destination(&head.pending))
        };
        // Finish all DashMap accesses before taking either queue mutex.
        // Creation/removal use the mutation gate, so an empty destination
        // cannot be detached between this lookup and appending the item.
        let destination = self
            .queues
            .entry(destination_key.clone().into())
            .or_insert_with(|| Arc::new(Mutex::new(ClientQueue::default())))
            .value()
            .clone();
        if Arc::ptr_eq(&source, &destination) {
            return None;
        }
        let mut source_queue = source.lock();
        let head = source_queue.messages.front()?;
        if head.pending.delivery_id != delivery_id || head.remotely_completed {
            return None;
        }
        let mut destination_queue = destination.lock();
        // A recovery key owns exactly one obligation. A nonempty collision
        // is an invariant failure; leave the source and its quota untouched.
        if Self::is_shared_recovery_key(&destination_key) && !destination_queue.messages.is_empty() {
            return None;
        }
        let item = source_queue.messages.pop_front().expect("validated shared queue head exists");
        destination_queue.messages.push_back(item);
        // Source execution is still owned by its caller until stop_worker;
        // the destination is claimed separately by the bounded scheduler.
        // No slot, byte, delivery state or target barrier counter changes.
        self.progress_sequence.fetch_add(1, Ordering::Release);
        Some(destination_key)
    }

    pub(crate) fn next_batch(&self, client_id: &str) -> Option<HandoffBatch> {
        let queue_ref = self.queues.get(client_id)?.clone();
        let mut queue = queue_ref.lock();
        let Some(front) = queue.messages.front() else {
            if queue.worker_running {
                queue.worker_running = false;
                self.active_workers.fetch_sub(1, Ordering::SeqCst);
            }
            drop(queue);
            self.remove_unused_queue(client_id, &queue_ref);
            return None;
        };

        let pinned_node_id = front.pinned_node_id;
        let avoid_node_id = front.pending.avoid_node_id;
        let target_client_id = &front.pending.target_client_id;
        let mut messages = Vec::with_capacity(self.cfg.batch_max_messages);
        let mut bytes = 0usize;
        for item in &queue.messages {
            if item.remotely_completed
                || item.pinned_node_id != pinned_node_id
                || &item.pending.target_client_id != target_client_id
                || item.pending.avoid_node_id != avoid_node_id
            {
                break;
            }
            let next_bytes = bytes.saturating_add(item.pending.size);
            if !messages.is_empty()
                && (messages.len() >= self.cfg.batch_max_messages || next_bytes > self.cfg.batch_max_bytes)
            {
                break;
            }
            bytes = next_bytes;
            messages.push(item.pending.clone());
        }
        Some(HandoffBatch { messages, pinned_node_id, avoid_node_id })
    }

    /// A pure local snapshot. Unlike stats(), this never audits or reconciles
    /// counters. Called only when a rate-limited diagnostic will be emitted.
    pub(crate) fn diagnostic_snapshot(&self, client_id: &str) -> Option<HandoffDiagnosticSnapshot> {
        let queue_ref = self.queues.get(client_id)?.clone();
        let queue = queue_ref.lock();
        let head = queue.messages.front()?;
        let mut snapshot = HandoffDiagnosticSnapshot {
            head_delivery_id: head.pending.delivery_id,
            queue_messages: queue.messages.len(),
            queue_bytes: 0,
            shared_messages: 0,
            pinned_messages: 0,
            uncertain_messages: 0,
            completed_behind_head: 0,
            head_pinned_node_id: head.pinned_node_id,
            head_pin_origin: head.diagnostic_pin_origin,
            head_accepted_seen: head.diagnostic_accepted_seen,
            head_first_uncertain_stage: head.diagnostic_first_uncertain_stage,
            head_transport_uncertain: head.transport_uncertain,
            head_rejected: head.rejected,
            recovery_parent: None,
        };
        for item in &queue.messages {
            snapshot.queue_bytes = snapshot.queue_bytes.saturating_add(item.pending.size);
            snapshot.shared_messages += usize::from(item.pending.is_shared());
            snapshot.pinned_messages += usize::from(item.pinned_node_id.is_some());
            snapshot.uncertain_messages += usize::from(item.transport_uncertain);
            snapshot.completed_behind_head += usize::from(item.remotely_completed);
        }
        drop(queue);
        snapshot.recovery_parent = self.recovery_parents.get(&snapshot.head_delivery_id).map(|id| *id);
        Some(snapshot)
    }

    pub(crate) fn apply_batch(
        &self,
        client_id: &str,
        target_node_id: NodeId,
        results: &[HandoffResult],
    ) -> BatchApplyResult {
        let Some(queue_ref) = self.queues.get(client_id).map(|entry| entry.clone()) else {
            return BatchApplyResult::default();
        };
        let mut queue = queue_ref.lock();
        let mut applied = BatchApplyResult::default();

        for result in results {
            let Some(item) =
                queue.messages.iter_mut().find(|item| item.pending.delivery_id == result.delivery_id)
            else {
                continue;
            };
            match &result.status {
                ReliableForwardStatus::Delivered | ReliableForwardStatus::Duplicate => {
                    let newly_completed = !item.remotely_completed;
                    item.remotely_completed = true;
                    if item.rejected {
                        item.rejected = false;
                        self.failed_messages.fetch_sub(1, Ordering::SeqCst);
                    }
                    applied.completed += usize::from(newly_completed);
                    if newly_completed {
                        if let Some(drain_target) = item.pending.avoid_node_id {
                            if let Some(completed) = self.completed_by_drain_target.get(&drain_target) {
                                completed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                ReliableForwardStatus::Accepted => {
                    let newly_accepted = item.pinned_node_id != Some(target_node_id);
                    item.diagnostic_pin_origin.get_or_insert("accepted");
                    item.diagnostic_accepted_seen = true;
                    item.pinned_node_id = Some(target_node_id);
                    if item.rejected {
                        item.rejected = false;
                        self.failed_messages.fetch_sub(1, Ordering::SeqCst);
                    }
                    if newly_accepted {
                        applied.accepted += 1;
                    } else {
                        applied.retryable += 1;
                    }
                }
                ReliableForwardStatus::Retryable(_) => {
                    applied.retryable += 1;
                }
                ReliableForwardStatus::Rejected(_) => {
                    if !item.rejected {
                        item.rejected = true;
                        self.failed_messages.fetch_add(1, Ordering::SeqCst);
                    }
                    applied.rejected += 1;
                }
            }
        }

        if applied.completed > 0 {
            self.completed_messages.fetch_add(applied.completed, Ordering::Relaxed);
        }

        let mut completed = Vec::new();
        while queue.messages.front().is_some_and(|item| item.remotely_completed) {
            if let Some(item) = queue.messages.pop_front() {
                completed.push(item);
            }
        }
        drop(queue);

        for item in completed {
            self.finish_item(item);
        }
        if applied.made_progress() {
            self.progress_sequence.fetch_add(1, Ordering::Release);
        }
        applied
    }

    /// Receiver queues and accepted remote pins cannot be retired by an
    /// absence probe. They must finish the existing delivery acknowledgement.
    pub(crate) fn has_protected_delivery(&self, client_id: &str, local_id: NodeId) -> bool {
        self.queues.iter().map(|entry| entry.value().clone()).collect::<Vec<_>>().iter().any(|queue| {
            queue.lock().messages.iter().any(|item| {
                item.pending.target_client_id == client_id
                    && (item.pinned_node_id.is_some()
                        || item.transport_uncertain
                        || item.pending.delivery_id.origin_node_id != local_id)
            })
        })
    }

    /// An ambiguous RPC must keep querying the same receiver. Changing its
    /// owner or consumer could execute an already accepted delivery twice.
    pub(crate) fn pin_uncertain_batch(
        &self,
        client_id: &str,
        batch: &[PendingHandoff],
        target_node_id: NodeId,
        diagnostic_stage: ForwardAttemptStage,
    ) {
        if let Some(entry) = self.queues.get(client_id) {
            for item in &mut entry.lock().messages {
                if batch.iter().any(|pending| pending.delivery_id == item.pending.delivery_id) {
                    item.diagnostic_pin_origin.get_or_insert("transport_error");
                    item.diagnostic_first_uncertain_stage.get_or_insert(diagnostic_stage);
                    item.transport_uncertain = true;
                    item.pinned_node_id = Some(target_node_id);
                }
            }
        }
    }

    pub(crate) fn shared_reassignment_allowed(&self, queue_key: &str, delivery_id: DeliveryId) -> bool {
        self.queues.get(queue_key).is_some_and(|entry| {
            entry.lock().messages.iter().any(|item| {
                item.pending.delivery_id == delivery_id
                    && item.pending.is_shared()
                    && item.pinned_node_id.is_none()
                    && !item.transport_uncertain
                    && !item.remotely_completed
            })
        })
    }

    /// A receiver leaving local delivery takes responsibility as a new source
    /// even when only the owner node, not the selected client, has changed.
    /// The protected parent can then answer Duplicate without queue admission
    /// or waiting for its child. This also releases the upstream's outbound
    /// slot when an inbound item has borrowed our reserved progress capacity.
    pub(crate) fn prepare_shared_recovery_child(&self, queue_key: &str, local_id: NodeId) -> bool {
        if !Self::is_shared_recovery_key(queue_key) {
            return false;
        }
        let Some(entry) = self.queues.get(queue_key) else {
            return false;
        };
        let mut queue = entry.lock();
        let Some(item) = queue.messages.front_mut() else {
            return false;
        };
        if !item.pending.is_shared()
            || item.pending.delivery_id.origin_node_id == local_id
            || item.pinned_node_id.is_some()
            || item.transport_uncertain
            || item.remotely_completed
        {
            return false;
        }
        let parent = item.pending.delivery_id;
        let child = self.next_delivery_id(local_id);
        self.delivery_states.insert(child, DeliveryState::Pending);
        self.recovery_parents.insert(child, parent);
        self.delivery_states.insert(parent, DeliveryState::Completed(Instant::now()));
        item.pending.delivery_id = child;
        self.progress_sequence.fetch_add(1, Ordering::Release);
        true
    }

    /// Reassign only an obligation which no downstream receiver has taken.
    /// The queue key stays stable; the chosen client may change independently.
    pub(crate) fn reassign_shared(
        &self,
        queue_key: &str,
        delivery_id: DeliveryId,
        local_id: NodeId,
        relation: SubRelation,
    ) -> bool {
        let Some(entry) = self.queues.get(queue_key) else {
            return false;
        };
        let mut queue = entry.lock();
        let Some(item) = queue.messages.iter_mut().find(|item| item.pending.delivery_id == delivery_id)
        else {
            return false;
        };
        if item.pinned_node_id.is_some()
            || item.transport_uncertain
            || item.remotely_completed
            || !item.pending.is_shared()
            || relation.4.as_ref().map(|g| &g.0).or_else(|| relation.2.shared_group())
                != item.pending.relations[0]
                    .4
                    .as_ref()
                    .map(|g| &g.0)
                    .or_else(|| item.pending.relations[0].2.shared_group())
            || relation.0 != item.pending.relations[0].0
        {
            return false;
        }
        // A receiver which accepted a foreign obligation becomes a new
        // recovery source. Use a new ID before forwarding back to a previous
        // owner, whose old ID can still be pending. Otherwise the two queues
        // could pin one another indefinitely.
        let replacement_id = if delivery_id.origin_node_id == local_id {
            delivery_id
        } else {
            self.next_delivery_id(local_id)
        };
        let mut replacement = PendingHandoff::new(
            replacement_id,
            relation.1.clone(),
            item.pending.target_node_id,
            item.pending.avoid_node_id,
            item.pending.from.clone(),
            item.pending.publish.clone(),
            vec![relation],
        );
        replacement.created_at = item.pending.created_at;
        let independent_shared =
            Self::is_shared_recovery_key(queue_key) || Self::is_shared_local_key(queue_key);
        if independent_shared && replacement.size > self.shared_receive_bytes() {
            return false;
        }
        if replacement.size > item.pending.size {
            let byte_limit =
                if independent_shared { self.outbound_limits().1 } else { self.cfg.max_pending_bytes };
            if !reserve_counter(&self.pending_bytes, replacement.size - item.pending.size, byte_limit) {
                return false;
            }
        } else {
            self.pending_bytes.fetch_sub(item.pending.size - replacement.size, Ordering::SeqCst);
            self.capacity_notify.notify_one();
        }
        item.pending = replacement;
        if replacement_id != delivery_id {
            self.delivery_states.insert(replacement_id, DeliveryState::Pending);
            self.recovery_parents.insert(replacement_id, delivery_id);
            // This acknowledgment is protected by the still-pending child;
            // mark_completed(child) will eventually register its TTL entry.
            self.delivery_states.insert(delivery_id, DeliveryState::Completed(Instant::now()));
        }
        drop(queue);
        drop(entry);
        self.progress_sequence.fetch_add(1, Ordering::Release);
        true
    }

    /// Called only after cluster-wide absence verification. Recheck ownership
    /// and pins under the queue lock, and retire only the captured DeliveryIds:
    /// a concurrent enqueue for a new session must remain untouched.
    pub(crate) fn retire_unowned_batch(
        &self,
        client_id: &str,
        batch: &[PendingHandoff],
        local_id: NodeId,
    ) -> usize {
        let Some(queue_ref) = self.queues.get(client_id).map(|entry| entry.clone()) else {
            return 0;
        };
        let mut queue = queue_ref.lock();
        let mut retired = 0;
        for item in &mut queue.messages {
            if item.pinned_node_id.is_none()
                && !item.transport_uncertain
                && item.pending.delivery_id.origin_node_id == local_id
                && !item.remotely_completed
                // A shared obligation belongs to the group, not to a retired
                // client. Its current owner must find a replacement instead.
                && !item.pending.is_shared()
                && batch.iter().any(|pending| pending.delivery_id == item.pending.delivery_id)
            {
                item.remotely_completed = true;
                if item.rejected {
                    item.rejected = false;
                    self.failed_messages.fetch_sub(1, Ordering::SeqCst);
                }
                retired += 1;
            }
        }
        let mut finished = Vec::new();
        while queue.messages.front().is_some_and(|item| item.remotely_completed) {
            finished.push(queue.messages.pop_front().expect("queue front exists"));
        }
        drop(queue);
        for item in finished {
            self.finish_item(item);
        }
        if retired > 0 {
            self.progress_sequence.fetch_add(1, Ordering::Release);
        }
        retired
    }

    pub(crate) fn stop_worker(&self, client_id: &str) {
        let Some(queue_ref) = self.queues.get(client_id).map(|entry| entry.clone()) else {
            return;
        };
        let mut queue = queue_ref.lock();
        if queue.worker_running {
            queue.worker_running = false;
            self.active_workers.fetch_sub(1, Ordering::SeqCst);
        }
        drop(queue);
        self.remove_unused_queue(client_id, &queue_ref);
    }

    pub(crate) fn restart_worker_if_pending(&self, client_id: &str) -> bool {
        let Some(queue_ref) = self.queues.get(client_id).map(|entry| entry.clone()) else {
            return false;
        };
        let mut queue = queue_ref.lock();
        if queue.messages.is_empty() || queue.worker_running {
            return false;
        }
        queue.worker_running = true;
        self.active_workers.fetch_add(1, Ordering::SeqCst);
        true
    }

    pub(crate) fn retry_guard(&self, target_node_id: NodeId) -> RetryActivityGuard {
        self.retrying_messages.fetch_add(1, Ordering::SeqCst);
        self.active_by_target
            .entry(target_node_id)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .fetch_add(1, Ordering::SeqCst);
        RetryActivityGuard {
            global: self.retrying_messages.clone(),
            by_target: self.active_by_target.clone(),
            target_node_id,
        }
    }

    pub(crate) fn data_loss_detected(&self) -> bool {
        self.data_loss_detected.load(Ordering::Acquire)
    }

    pub(crate) fn stats(&self) -> HandoffStats {
        let now = Instant::now();
        let admissions_before = self.admissions_in_progress.load(Ordering::SeqCst);
        let progress_before = self.progress_sequence.load(Ordering::Acquire);
        let queue_refs = self.queues.iter().map(|entry| entry.value().clone()).collect::<Vec<_>>();
        let mut actual_pending_messages = 0usize;
        let mut actual_pending_bytes = 0usize;
        let mut actual_active_workers = 0usize;
        let mut actual_clients = HashSet::new();
        let mut oldest_age_millis = 0u64;
        for queue_ref in &queue_refs {
            let queue = queue_ref.lock();
            actual_clients.extend(queue.messages.iter().map(|item| item.pending.target_client_id.clone()));
            actual_active_workers += usize::from(queue.worker_running);
            actual_pending_messages += queue.messages.len();
            actual_pending_bytes += queue.messages.iter().map(|item| item.pending.size).sum::<usize>();
            if let Some(front) = queue.messages.front() {
                oldest_age_millis = oldest_age_millis
                    .max(now.saturating_duration_since(front.pending.created_at).as_millis() as u64);
            }
        }
        let counted_pending_messages = self.pending_messages.load(Ordering::SeqCst);
        let counted_pending_bytes = self.pending_bytes.load(Ordering::SeqCst);
        let counted_active_workers = self.active_workers.load(Ordering::SeqCst);
        let delivery_pending_messages = self
            .delivery_states
            .iter()
            .filter(|entry| matches!(*entry.value(), DeliveryState::Pending))
            .count();
        let admissions_after = self.admissions_in_progress.load(Ordering::SeqCst);
        let progress_after = self.progress_sequence.load(Ordering::Acquire);
        let stable_snapshot = admissions_before == 0
            && admissions_after == 0
            && progress_before == progress_after
            && queue_refs.len() == self.queues.len();
        let counters_match = counted_pending_messages == actual_pending_messages
            && counted_pending_bytes == actual_pending_bytes
            && counted_active_workers == actual_active_workers;
        let invariant_mismatch_observations = if stable_snapshot {
            if counters_match {
                self.invariant_mismatch_observations.store(0, Ordering::Release);
                0
            } else {
                self.invariant_mismatch_observations.fetch_add(1, Ordering::AcqRel) + 1
            }
        } else {
            self.invariant_mismatch_observations.load(Ordering::Acquire)
        };
        if invariant_mismatch_observations >= 3 && stable_snapshot {
            // Stale high counters used to block a rollout forever even though
            // the real queues were empty. Repair them only after three stable
            // queue snapshots and with CAS, so a concurrent enqueue cannot be
            // overwritten. An under-count is treated as unsafe because it may
            // have bypassed configured capacity.
            let repaired_messages = self
                .pending_messages
                .compare_exchange(
                    counted_pending_messages,
                    actual_pending_messages,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
            let repaired_bytes = self
                .pending_bytes
                .compare_exchange(
                    counted_pending_bytes,
                    actual_pending_bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
            let repaired_workers = self
                .active_workers
                .compare_exchange(
                    counted_active_workers,
                    actual_active_workers,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
            if actual_pending_messages > counted_pending_messages
                || actual_pending_bytes > counted_pending_bytes
                || delivery_pending_messages > actual_pending_messages
            {
                self.data_loss_detected.store(true, Ordering::Release);
            }
            log::warn!(
                "reliable handoff accounting reconciled after stable queue audit: counted_messages={}, actual_messages={}, counted_bytes={}, actual_bytes={}, counted_workers={}, actual_workers={}, repaired={}/{}/{}",
                counted_pending_messages,
                actual_pending_messages,
                counted_pending_bytes,
                actual_pending_bytes,
                counted_active_workers,
                actual_active_workers,
                repaired_messages,
                repaired_bytes,
                repaired_workers
            );
            self.invariant_mismatch_observations.store(0, Ordering::Release);
            self.capacity_notify.notify_waiters();
        }
        let (pending_messages, pending_bytes, active_workers, clients) = if stable_snapshot {
            (actual_pending_messages, actual_pending_bytes, actual_active_workers, actual_clients.len())
        } else {
            (counted_pending_messages, counted_pending_bytes, counted_active_workers, self.queues.len())
        };
        let message_capacity = pending_messages
            .saturating_mul(100)
            .checked_div(self.cfg.max_pending_messages.max(1))
            .unwrap_or(100);
        let byte_capacity =
            pending_bytes.saturating_mul(100).checked_div(self.cfg.max_pending_bytes.max(1)).unwrap_or(100);
        HandoffStats {
            pending_messages,
            pending_bytes,
            clients,
            active_workers,
            retrying_messages: self.retrying_messages.load(Ordering::SeqCst),
            failed_messages: self.failed_messages.load(Ordering::SeqCst),
            rejected_messages: self.rejected_messages.load(Ordering::SeqCst),
            oldest_age_millis,
            progress_sequence: self.progress_sequence.load(Ordering::Acquire),
            capacity_percent: message_capacity.max(byte_capacity).min(100) as u8,
            capacity_waiters: self.capacity_waiters.load(Ordering::Acquire),
            invariant_mismatch_observations,
            completed_messages: self.completed_messages.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn pending_for_node(&self, node_id: NodeId) -> usize {
        // This is a drain barrier query, not part of normal forwarding. Read
        // the authoritative queue contents so stale derived counters cannot
        // keep a pod in Terminating forever.
        self.queues
            .iter()
            .map(|entry| {
                entry
                    .value()
                    .lock()
                    .messages
                    .iter()
                    .filter(|item| item.pending.target_node_id == Some(node_id))
                    .count()
            })
            .sum()
    }

    pub(crate) fn active_for_node(&self, node_id: NodeId) -> usize {
        self.active_by_target.get(&node_id).map(|count| count.load(Ordering::SeqCst)).unwrap_or_default()
    }

    pub(crate) fn completed_for_node(&self, node_id: NodeId) -> usize {
        self.completed_by_drain_target
            .get(&node_id)
            .map(|count| count.load(Ordering::Relaxed))
            .unwrap_or_default()
    }

    pub(crate) fn begin_forward(&self, target_node_id: NodeId, operation_active: bool) -> ForwardMode {
        // Normal forwarding must remain the original fast path. Exact
        // per-target accounting is enabled only while an operator-coordinated
        // handoff is active; the caller snapshots that state once per publish,
        // and outside that window this returns a zero-allocation guard.
        if !operation_active || !self.draining_targets.contains_key(&target_node_id) {
            return ForwardMode::Plain(PlainForwardGuard { counter: None });
        }

        let counter = self
            .plain_forwards_by_target
            .entry(target_node_id)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        counter.fetch_add(1, Ordering::SeqCst);
        let guard = PlainForwardGuard { counter: Some(counter) };

        // Prepared only arms accounting while the target is still serving.
        // Do not divert traffic until the operator activates the cutover: a
        // long preparation/EndpointSlice window must not leave a live MQTT
        // connection silently starved of new deliveries. Register the plain
        // forward before rechecking Avoid so activation racing this call
        // either switches it to reliable handoff or leaves a tracked guard
        // for the final drain barrier.
        if self.should_avoid_target(target_node_id) {
            drop(guard);
            ForwardMode::Reliable
        } else {
            ForwardMode::Plain(guard)
        }
    }

    pub(crate) fn mark_ambiguous_plain_failure(&self) {
        self.rejected_messages.fetch_add(1, Ordering::SeqCst);
        self.data_loss_detected.store(true, Ordering::Release);
    }

    pub(crate) fn plain_forwards_for_node(&self, node_id: NodeId) -> usize {
        self.plain_forwards_by_target
            .get(&node_id)
            .map(|count| count.load(Ordering::SeqCst))
            .unwrap_or_default()
    }

    fn finish_item(&self, item: QueuedHandoff) {
        self.pending_messages.fetch_sub(1, Ordering::SeqCst);
        self.pending_bytes.fetch_sub(item.pending.size, Ordering::SeqCst);
        self.capacity_notify.notify_waiters();
        if item.rejected {
            self.failed_messages.fetch_sub(1, Ordering::SeqCst);
        }
        if let Some(target_node_id) = item.pending.target_node_id {
            if let Some(count) = self.pending_by_target.get(&target_node_id) {
                count.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.mark_completed(item.pending.delivery_id);
        self.reset_operation_counters_if_idle();
    }

    fn reset_operation_counters_if_idle(&self) {
        if self.draining_targets.is_empty() && self.pending_messages.load(Ordering::Acquire) == 0 {
            self.failed_messages.store(0, Ordering::Release);
            self.rejected_messages.store(0, Ordering::Release);
            self.data_loss_detected.store(false, Ordering::Release);
        }
    }

    fn cleanup_completed(&self) {
        const MAX_EXPIRED_PER_CLEANUP: usize = 1_024;

        let now = Instant::now();
        let mut completed = self.completed_order.lock();
        for _ in 0..MAX_EXPIRED_PER_CLEANUP {
            if !completed.front().is_some_and(|(completed_at, _)| {
                now.saturating_duration_since(*completed_at) >= self.cfg.dedup_ttl
            }) {
                break;
            }
            let (_, delivery_id) = completed.pop_front().expect("completed handoff entry exists");
            self.delivery_states.remove_if(&delivery_id, |_, state| {
                matches!(
                    state,
                    DeliveryState::Completed(completed_at)
                        if now.saturating_duration_since(*completed_at) >= self.cfg.dedup_ttl
                )
            });
        }
    }

    fn remove_unused_queue(&self, client_id: &str, queue_ref: &Arc<Mutex<ClientQueue>>) {
        let _mutation = self.queue_mutation.lock();
        self.queues.remove_if(client_id, |_, value| {
            if !Arc::ptr_eq(value, queue_ref) {
                return false;
            }
            let queue = value.lock();
            queue.messages.is_empty() && !queue.worker_running
        });
    }
}

fn reserve_counter(counter: &AtomicUsize, amount: usize, limit: usize) -> bool {
    let mut current = counter.load(Ordering::SeqCst);
    loop {
        let Some(next) = current.checked_add(amount).filter(|next| *next <= limit) else {
            return false;
        };
        match counter.compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;
    use bytestring::ByteString;
    use parking_lot::Mutex;
    use rmqtt::types::{CodecPublish, From, Id, Publish, QoS, SubscriptionOptions};

    use super::{ClientQueue, ForwardMode, HandoffManager, HandoffResult, PendingHandoff};
    use crate::config::Handoff;
    use crate::message::ReliableForwardStatus;

    fn manager(max_pending_messages: usize) -> HandoffManager {
        HandoffManager::new(Handoff {
            max_pending_messages,
            max_pending_bytes: 64 * 1024 * 1024,
            retry_timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(1),
            retry_max_interval: Duration::from_millis(10),
            batch_max_messages: 64,
            batch_max_bytes: 1024 * 1024,
            ..Handoff::default()
        })
    }

    fn pending(manager: &HandoffManager, client_id: &str) -> PendingHandoff {
        let publish = Publish::from(CodecPublish {
            dup: false,
            retain: false,
            qos: QoS::ExactlyOnce,
            topic: ByteString::from_static("responses/client"),
            packet_id: None,
            payload: Bytes::from_static(b"message"),
            properties: None,
        });
        let client_id = ByteString::from(client_id.to_owned());
        PendingHandoff::new(
            manager.next_delivery_id(1),
            client_id.clone(),
            Some(2),
            Some(2),
            From::from_custom(Id::from(1, "publisher".into())),
            publish,
            vec![(
                ByteString::from_static("responses/client"),
                client_id,
                SubscriptionOptions::default(),
                None,
                None,
            )],
        )
    }

    #[tokio::test]
    async fn queue_batches_fifo_and_releases_only_confirmed_prefix() {
        let manager = manager(10);
        assert!(manager.prepare_drain(2));
        let first = pending(&manager, "client");
        let second = pending(&manager, "client");
        assert!(manager.enqueue(first.clone()).await.unwrap());
        assert!(!manager.enqueue(second.clone()).await.unwrap());

        let batch = manager.next_batch("client").unwrap();
        assert_eq!(batch.messages.len(), 2);
        manager.apply_batch(
            "client",
            3,
            &[
                HandoffResult { delivery_id: first.delivery_id, status: ReliableForwardStatus::Accepted },
                HandoffResult { delivery_id: second.delivery_id, status: ReliableForwardStatus::Delivered },
            ],
        );
        assert_eq!(manager.stats().pending_messages, 2);
        assert_eq!(manager.stats().completed_messages, 1);
        assert_eq!(manager.completed_for_node(2), 1);
        assert_eq!(manager.next_batch("client").unwrap().pinned_node_id, Some(3));

        manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: first.delivery_id, status: ReliableForwardStatus::Duplicate }],
        );
        assert_eq!(manager.stats().pending_messages, 0);
        assert_eq!(manager.stats().completed_messages, 2);
        assert_eq!(manager.completed_for_node(2), 2);
        assert!(manager.next_batch("client").is_none());
    }

    #[tokio::test]
    async fn duplicate_delivery_id_is_not_enqueued_twice() {
        let manager = manager(10);
        let pending = pending(&manager, "client");
        assert!(manager.enqueue(pending.clone()).await.unwrap());
        assert!(!manager.enqueue(pending).await.unwrap());
        assert_eq!(manager.stats().pending_messages, 1);
    }

    #[test]
    fn enqueue_cannot_be_detached_by_concurrent_empty_queue_cleanup() {
        let manager = manager(10);
        let client_id = ByteString::from_static("racing-client");
        let queue_ref = Arc::new(Mutex::new(ClientQueue::default()));
        manager.queues.insert(client_id, queue_ref.clone());

        let entry_held = Arc::new(Barrier::new(2));
        let release_enqueue = Arc::new(Barrier::new(2));
        let enqueue_manager = manager.clone();
        let enqueue_entry_held = entry_held.clone();
        let enqueue_release = release_enqueue.clone();
        let enqueue_thread = thread::spawn(move || {
            futures::executor::block_on(enqueue_manager.enqueue_with_hook(
                pending(&enqueue_manager, "racing-client"),
                move || {
                    enqueue_entry_held.wait();
                    enqueue_release.wait();
                },
            ))
            .unwrap()
        });

        entry_held.wait();
        let cleanup_manager = manager.clone();
        let cleanup_queue = queue_ref.clone();
        let cleanup_started = Arc::new(Barrier::new(2));
        let cleanup_started_thread = cleanup_started.clone();
        let (cleanup_done_tx, cleanup_done_rx) = mpsc::channel();
        let cleanup_thread = thread::spawn(move || {
            cleanup_started_thread.wait();
            cleanup_manager.remove_unused_queue("racing-client", &cleanup_queue);
            cleanup_done_tx.send(()).unwrap();
        });
        cleanup_started.wait();

        // Cleanup must be blocked by the live DashMap entry guard. If enqueue
        // dropped that guard before taking the queue lock, cleanup would remove
        // the still-empty map entry here and recreate the production orphan.
        assert!(matches!(
            cleanup_done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release_enqueue.wait();

        assert!(enqueue_thread.join().unwrap());
        cleanup_thread.join().unwrap();
        cleanup_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(manager.queues.contains_key("racing-client"));
        let stats = manager.stats();
        assert_eq!(stats.pending_messages, 1);
        assert_eq!(stats.clients, 1);
        assert_eq!(stats.active_workers, 1);
        assert_eq!(stats.invariant_mismatch_observations, 0);

        let batch = manager.next_batch("racing-client").unwrap();
        manager.apply_batch(
            "racing-client",
            3,
            &[HandoffResult {
                delivery_id: batch.messages[0].delivery_id,
                status: ReliableForwardStatus::Delivered,
            }],
        );
        assert!(manager.next_batch("racing-client").is_none());
        let stats = manager.stats();
        assert_eq!(stats.pending_messages, 0);
        assert_eq!(stats.active_workers, 0);
        assert_eq!(stats.clients, 0);
    }

    #[tokio::test]
    async fn persistent_queue_counter_mismatch_is_reported_as_data_loss() {
        let manager = manager(10);
        assert!(manager.enqueue(pending(&manager, "orphaned-client")).await.unwrap());

        // Recreate the production symptom: the queue disappears while its
        // global pending and worker counters remain reserved.
        manager.queues.remove("orphaned-client");
        for expected in 1..=3 {
            let stats = manager.stats();
            assert_eq!(stats.invariant_mismatch_observations, expected);
            assert_eq!(manager.data_loss_detected(), expected >= 3);
        }
    }

    #[tokio::test]
    async fn stale_completed_counters_are_repaired_without_blocking_drain() {
        let manager = manager(10);
        let pending = pending(&manager, "completed-client");
        let size = pending.size;
        assert!(manager.enqueue(pending.clone()).await.unwrap());
        manager.apply_batch(
            "completed-client",
            3,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Delivered }],
        );
        assert!(manager.next_batch("completed-client").is_none());

        // Model an acknowledgement/accounting interruption after the queue
        // item reached Completed. The queue plus delivery state are the
        // authority; stale high gauges must not block pod deletion forever.
        manager.pending_messages.store(1, Ordering::Release);
        manager.pending_bytes.store(size, Ordering::Release);
        for _ in 0..3 {
            let _ = manager.stats();
        }
        let stats = manager.stats();
        assert_eq!(stats.pending_messages, 0);
        assert_eq!(stats.pending_bytes, 0);
        assert!(!manager.data_loss_detected());
    }

    #[tokio::test]
    async fn accepted_target_is_pinned_until_completion() {
        let manager = manager(10);
        let pending = pending(&manager, "client");
        assert!(manager.enqueue(pending.clone()).await.unwrap());
        let first_accept = manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        assert!(first_accept.made_progress());
        let duplicate_accept = manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        assert!(!duplicate_accept.made_progress());
        assert_eq!(manager.next_batch("client").unwrap().pinned_node_id, Some(3));
        assert_eq!(manager.stats().pending_messages, 1);
    }

    #[tokio::test]
    async fn rollback_clears_avoid_but_retains_accepted_target_pin() {
        let manager = manager(10);
        assert!(manager.prepare_drain(2));
        let pending = pending(&manager, "client");
        assert!(manager.enqueue(pending.clone()).await.unwrap());
        manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        manager.clear_drain(2);
        let batch = manager.next_batch("client").unwrap();
        assert_eq!(batch.pinned_node_id, Some(3));
        assert_eq!(batch.avoid_node_id, None);
    }

    #[tokio::test]
    async fn force_discard_removes_only_target_deliveries_and_is_idempotent() {
        let manager = manager(10);
        let target = pending(&manager, "target-client");
        let target_size = target.size;
        assert!(manager.enqueue(target).await.unwrap());

        let mut unrelated = pending(&manager, "unrelated-client");
        unrelated.target_node_id = Some(3);
        unrelated.avoid_node_id = Some(3);
        assert!(manager.enqueue(unrelated).await.unwrap());

        let discarded = manager.discard_for_target(2);
        assert_eq!(discarded.messages, 1);
        assert_eq!(discarded.bytes, target_size);
        assert_eq!(discarded.clients, 1);
        assert!(manager.data_loss_detected());
        assert!(manager.next_batch("target-client").is_none());
        assert!(manager.next_batch("unrelated-client").is_some());
        assert_eq!(manager.stats().pending_messages, 1);

        assert_eq!(manager.discard_for_target(2), super::HandoffDiscardStats::default());
        assert_eq!(manager.stats().pending_messages, 1);
    }

    #[test]
    fn drain_preparation_intercepts_reliably_before_target_is_avoided() {
        let manager = manager(10);
        assert!(manager.prepare_drain(2));
        assert!(manager.is_draining_target(2));
        assert!(!manager.should_avoid_target(2));
        assert!(manager.activate_drain(2));
        assert!(manager.should_avoid_target(2));
        assert!(manager.prepare_drain(2));
        assert!(manager.should_avoid_target(2));
        manager.clear_drain(2);
        assert!(!manager.is_draining_target(2));
    }

    #[test]
    fn normal_forward_fast_path_does_not_allocate_or_count() {
        let manager = manager(10);
        for _ in 0..10_000 {
            let guard = match manager.begin_forward(2, manager.operation_active()) {
                ForwardMode::Plain(guard) => guard,
                ForwardMode::Reliable => panic!("unexpected reliable mode"),
            };
            drop(guard);
        }
        assert!(manager.plain_forwards_by_target.is_empty());
        assert_eq!(manager.plain_forwards_for_node(2), 0);
    }

    #[test]
    fn drain_preparation_tracks_target_without_diverting_traffic() {
        let manager = manager(10);
        assert!(manager.prepare_drain(2));
        let operation_active = manager.operation_active();
        let target = match manager.begin_forward(2, operation_active) {
            ForwardMode::Plain(guard) => guard,
            ForwardMode::Reliable => panic!("prepared target unexpectedly entered reliable mode"),
        };
        assert_eq!(manager.plain_forwards_for_node(2), 1);
        drop(target);
        assert_eq!(manager.plain_forwards_for_node(2), 0);

        let other_target = match manager.begin_forward(3, operation_active) {
            ForwardMode::Plain(guard) => guard,
            ForwardMode::Reliable => panic!("unrelated target unexpectedly entered reliable mode"),
        };
        assert_eq!(manager.plain_forwards_for_node(2), 0);
        assert_eq!(manager.plain_forwards_for_node(3), 0);
        drop(other_target);
        assert_eq!(manager.plain_forwards_for_node(2), 0);
    }

    #[test]
    fn drain_activation_diverts_only_the_target_to_reliable_handoff() {
        let manager = manager(10);
        assert!(manager.prepare_drain(2));
        assert!(manager.activate_drain(2));
        let operation_active = manager.operation_active();
        assert!(matches!(manager.begin_forward(2, operation_active), ForwardMode::Reliable));
        assert!(matches!(manager.begin_forward(3, operation_active), ForwardMode::Plain(_)));
    }

    #[tokio::test]
    async fn disabled_handoff_rejects_prepare_and_enqueue() {
        let manager = HandoffManager::new(Handoff { enabled: false, ..Handoff::default() });
        assert!(!manager.prepare_drain(2));
        assert!(manager.enqueue(pending(&manager, "client")).await.is_err());
    }

    #[tokio::test]
    async fn queue_capacity_applies_backpressure_instead_of_dropping() {
        let manager = manager(1);
        let first = pending(&manager, "client-1");
        assert!(manager.enqueue(first.clone()).await.is_ok());
        let blocked =
            tokio::time::timeout(Duration::from_millis(20), manager.enqueue(pending(&manager, "client-2")))
                .await;
        assert!(blocked.is_err());
        assert_eq!(manager.stats().capacity_waiters, 0);
        assert!(!manager.data_loss_detected());

        manager.apply_batch(
            "client-1",
            3,
            &[HandoffResult { delivery_id: first.delivery_id, status: ReliableForwardStatus::Delivered }],
        );
        assert!(manager.enqueue(pending(&manager, "client-2")).await.is_ok());
    }

    #[tokio::test]
    async fn message_larger_than_total_capacity_is_rejected_explicitly() {
        let manager = HandoffManager::new(Handoff {
            max_pending_messages: 10,
            max_pending_bytes: 1,
            ..Handoff::default()
        });
        assert!(manager.enqueue(pending(&manager, "client")).await.is_err());
        assert!(manager.data_loss_detected());
        assert_eq!(manager.stats().rejected_messages, 1);
    }

    #[tokio::test]
    async fn completed_delivery_cleanup_is_incremental_and_time_ordered() {
        let manager = HandoffManager::new(Handoff {
            dedup_ttl: Duration::from_millis(5),
            retry_timeout: Duration::from_millis(1),
            ..Handoff::default()
        });
        let first = manager.next_delivery_id(1);
        manager.mark_completed(first);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let second = manager.next_delivery_id(1);
        manager.mark_completed(second);
        assert!(!manager.delivery_completed(first));
        assert!(manager.delivery_completed(second));
        assert_eq!(manager.completed_order.lock().len(), 1);
    }

    #[tokio::test]
    async fn rejected_delivery_can_recover_without_sticky_worker_failure() {
        let manager = manager(10);
        let pending = pending(&manager, "client");
        assert!(manager.enqueue(pending.clone()).await.unwrap());
        manager.apply_batch(
            "client",
            3,
            &[HandoffResult {
                delivery_id: pending.delivery_id,
                status: ReliableForwardStatus::Rejected("busy".to_string()),
            }],
        );
        assert_eq!(manager.stats().failed_messages, 1);
        manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        assert_eq!(manager.stats().failed_messages, 0);
    }

    #[tokio::test]
    async fn terminated_client_releases_queued_handoff_capacity() {
        let manager = manager(10);
        let first = pending(&manager, "terminated-client");
        let second = pending(&manager, "terminated-client");
        let live = pending(&manager, "live-client");
        manager.enqueue(first).await.unwrap();
        manager.enqueue(second).await.unwrap();
        manager.enqueue(live).await.unwrap();

        let batch = manager.next_batch("terminated-client").unwrap();
        assert_eq!(manager.retire_unowned_batch("terminated-client", &batch.messages, 1), 2);
        assert!(manager.next_batch("terminated-client").is_none());

        let stats = manager.stats();
        assert_eq!(stats.pending_messages, 1);
        assert_eq!(stats.clients, 1);
        assert_eq!(manager.pending_for_node(2), 1);
        assert!(manager.next_batch("live-client").is_some());
    }

    #[tokio::test]
    async fn retirement_rechecks_remote_pin_and_preserves_concurrent_enqueue() {
        let manager = manager(10);
        let old = pending(&manager, "client");
        manager.enqueue(old.clone()).await.unwrap();
        let captured = manager.next_batch("client").unwrap();
        let new = pending(&manager, "client");
        manager.enqueue(new.clone()).await.unwrap();
        manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: old.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        assert!(manager.has_protected_delivery("client", 1));
        assert_eq!(manager.retire_unowned_batch("client", &captured.messages, 1), 0);
        assert_eq!(manager.stats().pending_messages, 2);
        manager.apply_batch(
            "client",
            3,
            &[HandoffResult { delivery_id: old.delivery_id, status: ReliableForwardStatus::Delivered }],
        );
        assert_eq!(manager.next_batch("client").unwrap().messages[0].delivery_id, new.delivery_id);
    }

    #[tokio::test]
    async fn receiver_queue_cannot_be_retired_by_an_absence_check() {
        let manager = manager(10);
        let inbound = pending(&manager, "client"); // Origin node 1, receiver node 2.
        manager.enqueue(inbound).await.unwrap();
        let captured = manager.next_batch("client").unwrap();
        assert!(manager.has_protected_delivery("client", 2));
        assert_eq!(manager.retire_unowned_batch("client", &captured.messages, 2), 0);
        assert_eq!(manager.stats().pending_messages, 1);
    }

    fn shared_pending(manager: &HandoffManager, client: &str) -> PendingHandoff {
        let mut item = pending(manager, client);
        item.relations[0].4 = Some(("workers".into(), true, vec![client.into()]));
        PendingHandoff::new(
            item.delivery_id,
            item.target_client_id,
            item.target_node_id,
            item.avoid_node_id,
            item.from,
            item.publish,
            item.relations,
        )
    }

    #[tokio::test]
    async fn shared_retirement_reassigns_only_its_obligation_and_releases_accounting() {
        let manager = manager(10);
        let item = shared_pending(&manager, "retired");
        manager.enqueue(item.clone()).await.unwrap();
        let batch = manager.next_batch("retired").unwrap();
        assert_eq!(manager.retire_unowned_batch("retired", &batch.messages, 1), 0);
        assert_eq!(manager.stats().pending_messages, 1);
        let mut relation = item.relations[0].clone();
        relation.1 = "a-longer-replacement".into();
        relation.2.set_qos(QoS::AtLeastOnce);
        assert!(manager.reassign_shared("retired", item.delivery_id, 1, relation));
        let updated = manager.next_batch("retired").unwrap().messages.remove(0);
        assert_eq!(updated.delivery_id, item.delivery_id);
        assert_eq!(updated.target_client_id, "a-longer-replacement");
        assert_eq!(updated.publish.qos, QoS::ExactlyOnce, "keep source QoS for the new subscription");
        assert_eq!(updated.relations[0].2.qos(), QoS::AtLeastOnce);
        assert_eq!(manager.stats().pending_bytes, updated.size);
        assert_eq!(manager.pending_for_node(2), 1);
        manager.apply_batch(
            "retired",
            3,
            &[HandoffResult { delivery_id: updated.delivery_id, status: ReliableForwardStatus::Delivered }],
        );
        assert_eq!(manager.stats().pending_messages, 0);
        assert_eq!(manager.stats().pending_bytes, 0);
        assert_eq!(manager.pending_for_node(2), 0);
    }

    #[tokio::test]
    async fn shared_reassignment_cannot_change_group_filter_or_accepted_receiver() {
        for ambiguous in [false, true] {
            let manager = manager(10);
            let item = shared_pending(&manager, "retired");
            manager.enqueue(item.clone()).await.unwrap();
            let mut replacement = item.relations[0].clone();
            replacement.1 = "new".into();
            let mut other_group = replacement.clone();
            other_group.4.as_mut().unwrap().0 = "other-group".into();
            assert!(!manager.reassign_shared("retired", item.delivery_id, 1, other_group));
            let mut other_filter = replacement.clone();
            other_filter.0 = "other/#".into();
            assert!(!manager.reassign_shared("retired", item.delivery_id, 1, other_filter));
            if ambiguous {
                let batch = manager.next_batch("retired").unwrap();
                manager.pin_uncertain_batch(
                    "retired",
                    &batch.messages,
                    3,
                    super::ForwardAttemptStage::SendCall,
                );
            } else {
                manager.apply_batch(
                    "retired",
                    3,
                    &[HandoffResult {
                        delivery_id: item.delivery_id,
                        status: ReliableForwardStatus::Accepted,
                    }],
                );
            }
            assert!(!manager.shared_reassignment_allowed("retired", item.delivery_id));
            assert!(!manager.reassign_shared("retired", item.delivery_id, 1, replacement));
            assert_eq!(manager.next_batch("retired").unwrap().pinned_node_id, Some(3));
        }
    }

    #[tokio::test]
    async fn receiver_recovery_uses_new_id_without_allocating_another_queue_slot() {
        let manager = manager(1);
        let item = shared_pending(&manager, "retired"); // origin 1, receiver 2
        manager.enqueue(item.clone()).await.unwrap();
        let mut replacement = item.relations[0].clone();
        replacement.1 = "replacement-on-origin".into();
        assert!(manager.reassign_shared("retired", item.delivery_id, 2, replacement));
        let updated = manager.next_batch("retired").unwrap().messages.remove(0);
        assert_ne!(updated.delivery_id, item.delivery_id);
        assert_eq!(updated.delivery_id.origin_node_id, 2);
        assert!(manager.delivery_completed(item.delivery_id), "late old RPCs must remain deduplicated");
        assert!(manager.delivery_pending(updated.delivery_id));
        assert_eq!(manager.stats().pending_messages, 1);
        assert_eq!(manager.stats().pending_bytes, updated.size);
        assert!(!manager.reassign_shared("retired", item.delivery_id, 2, item.relations[0].clone()));
        manager.apply_batch(
            "retired",
            1,
            &[HandoffResult { delivery_id: updated.delivery_id, status: ReliableForwardStatus::Delivered }],
        );
        assert_eq!(manager.stats().pending_messages, 0);
        assert_eq!(manager.stats().pending_bytes, 0);
        assert!(manager.delivery_completed(updated.delivery_id));
    }

    #[tokio::test]
    async fn recovery_parent_remains_deduplicated_until_its_local_child_finishes() {
        let manager = manager(1);
        let item = shared_pending(&manager, "retired"); // origin 1, receiver 2
        manager.enqueue(item.clone()).await.unwrap();
        let mut replacement = item.relations[0].clone();
        replacement.1 = "replacement".into();
        assert!(manager.reassign_shared("retired", item.delivery_id, 2, replacement));
        let child = manager.next_batch("retired").unwrap().messages.remove(0);
        assert_eq!(manager.recovery_parents.len(), 1);
        assert_eq!(manager.stats().pending_messages, 1);

        // Model a replacement which has remained pending beyond dedup_ttl.
        // No wall-clock sleep is needed: protected parents are deliberately
        // absent from the ordinary completion expiration queue.
        let expired_at = std::time::Instant::now() - manager.cfg.dedup_ttl - Duration::from_secs(1);
        manager.delivery_states.insert(item.delivery_id, super::DeliveryState::Completed(expired_at));
        let unrelated = manager.next_delivery_id(2);
        manager.mark_completed(unrelated);
        manager.cleanup_completed();
        assert!(manager.delivery_completed(item.delivery_id));
        assert!(manager.delivery_pending(child.delivery_id));
        assert!(manager.completed_order.lock().iter().all(|(_, id)| *id != item.delivery_id));

        manager.apply_batch(
            "retired",
            3,
            &[HandoffResult { delivery_id: child.delivery_id, status: ReliableForwardStatus::Delivered }],
        );
        assert!(manager.recovery_parents.is_empty());
        assert_eq!(manager.stats().pending_messages, 0);
        assert_eq!(manager.stats().pending_bytes, 0);
        assert!(manager.delivery_completed(item.delivery_id));
        assert!(manager.delivery_completed(child.delivery_id));
        let Some(super::DeliveryState::Completed(refreshed_at)) =
            manager.delivery_states.get(&item.delivery_id).map(|state| *state)
        else {
            panic!("parent acknowledgment must remain available after the child finishes");
        };
        assert!(refreshed_at > expired_at + manager.cfg.dedup_ttl);

        // Both IDs return to the usual bounded TTL cleanup after completion.
        for id in [item.delivery_id, child.delivery_id, unrelated] {
            manager.delivery_states.insert(id, super::DeliveryState::Completed(expired_at));
        }
        for (completed_at, _) in manager.completed_order.lock().iter_mut() {
            *completed_at = expired_at;
        }
        manager.cleanup_completed();
        assert!(!manager.delivery_completed(item.delivery_id));
        assert!(!manager.delivery_completed(child.delivery_id));
        assert!(manager.completed_order.lock().is_empty());
    }

    #[tokio::test]
    async fn diagnostic_snapshots_do_not_reconcile_counters_or_release_work() {
        let manager = manager(10);
        let first = shared_pending(&manager, "retired");
        let second = shared_pending(&manager, "retired");
        manager.enqueue(first.clone()).await.unwrap();
        manager.enqueue(second.clone()).await.unwrap();
        // stats() can repair a persistent accounting mismatch. A diagnostic
        // must leave even a mismatch untouched so observing cannot unblock it.
        manager.pending_messages.fetch_add(1, Ordering::SeqCst);
        let progress = manager.progress_sequence.load(Ordering::Acquire);
        for _ in 0..5 {
            let snapshot = manager.diagnostic_snapshot("retired").unwrap();
            assert_eq!(snapshot.head_delivery_id, first.delivery_id);
            assert_eq!(snapshot.queue_messages, 2);
            assert_eq!(snapshot.shared_messages, 2);
            assert_eq!(snapshot.queue_bytes, first.size + second.size);
            assert_eq!(snapshot.pinned_messages, 0);
            assert_eq!(snapshot.uncertain_messages, 0);
            assert_eq!(snapshot.completed_behind_head, 0);
            assert_eq!(snapshot.recovery_parent, None);
            assert_eq!(snapshot.head_pin_origin, None);
            assert!(!snapshot.head_rejected);
        }
        assert_eq!(manager.pending_messages.load(Ordering::Acquire), 3);
        assert_eq!(manager.progress_sequence.load(Ordering::Acquire), progress);
        assert_eq!(manager.next_batch("retired").unwrap().messages.len(), 2);
        assert_eq!(manager.invariant_mismatch_observations.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn diagnostic_pin_origin_survives_later_errors_and_accepted_replies() {
        for uncertain_first in [false, true] {
            let manager = manager(10);
            let item = shared_pending(&manager, "retired");
            manager.enqueue(item.clone()).await.unwrap();
            let batch = manager.next_batch("retired").unwrap();
            let accept =
                || [HandoffResult { delivery_id: item.delivery_id, status: ReliableForwardStatus::Accepted }];
            if !uncertain_first {
                manager.apply_batch("retired", 3, &accept());
            }
            manager.pin_uncertain_batch(
                "retired",
                &batch.messages,
                3,
                super::ForwardAttemptStage::MissingClient,
            );
            manager.apply_batch("retired", 3, &accept());
            manager.pin_uncertain_batch("retired", &batch.messages, 3, super::ForwardAttemptStage::SendCall);
            manager.apply_batch(
                "retired",
                3,
                &[HandoffResult {
                    delivery_id: item.delivery_id,
                    status: ReliableForwardStatus::Retryable("session moved".into()),
                }],
            );
            let snapshot = manager.diagnostic_snapshot("retired").unwrap();
            assert_eq!(
                snapshot.head_pin_origin,
                Some(if uncertain_first { "transport_error" } else { "accepted" })
            );
            assert!(snapshot.head_accepted_seen);
            assert!(snapshot.head_transport_uncertain);
            assert_eq!(snapshot.head_first_uncertain_stage, Some(super::ForwardAttemptStage::MissingClient));
            assert_eq!(snapshot.head_pinned_node_id, Some(3));
            assert_eq!(snapshot.pinned_messages, 1);
            assert_eq!(snapshot.uncertain_messages, 1);
            assert_eq!(snapshot.queue_messages, 1);
            assert!(!manager.shared_reassignment_allowed("retired", item.delivery_id));
        }
    }

    #[tokio::test]
    async fn uncertain_transport_requires_final_delivery_confirmation() {
        let manager = manager(10);
        let pending = pending(&manager, "client");
        manager.enqueue(pending.clone()).await.unwrap();
        let batch = manager.next_batch("client").unwrap();
        manager.pin_uncertain_batch("client", &batch.messages, 2, super::ForwardAttemptStage::SendCall);
        assert!(manager.has_protected_delivery("client", 1));
        manager.apply_batch(
            "client",
            2,
            &[HandoffResult {
                delivery_id: pending.delivery_id,
                status: ReliableForwardStatus::Retryable("no session".into()),
            }],
        );
        assert_eq!(manager.retire_unowned_batch("client", &batch.messages, 1), 0);
        manager.apply_batch(
            "client",
            2,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Duplicate }],
        );
        assert_eq!(manager.stats().pending_messages, 0);
    }

    #[tokio::test]
    async fn verified_retirement_during_drain_only_releases_captured_deliveries() {
        let manager = manager(10);
        manager.activate_drain(2);
        manager.enqueue(pending(&manager, "client")).await.unwrap();
        let captured = manager.next_batch("client").unwrap();
        let new = pending(&manager, "client");
        manager.enqueue(new.clone()).await.unwrap();
        assert_eq!(manager.retire_unowned_batch("client", &captured.messages, 1), 1);
        assert_eq!(manager.next_batch("client").unwrap().messages[0].delivery_id, new.delivery_id);
        assert_eq!(manager.stats().pending_messages, 1);
        assert_eq!(manager.pending_for_node(2), 1);
        assert!(!manager.data_loss_detected());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_enqueue_and_batched_completion_do_not_stall() {
        const PRODUCERS: usize = 8;
        const MESSAGES_PER_PRODUCER: usize = 1_250;

        let manager = manager(PRODUCERS * MESSAGES_PER_PRODUCER + 1);
        let mut producers = Vec::new();
        for _ in 0..PRODUCERS {
            let manager = manager.clone();
            producers.push(tokio::spawn(async move {
                for _ in 0..MESSAGES_PER_PRODUCER {
                    manager.enqueue(pending(&manager, "busy-client")).await.unwrap();
                }
            }));
        }
        for producer in producers {
            producer.await.unwrap();
        }
        assert_eq!(manager.stats().pending_messages, PRODUCERS * MESSAGES_PER_PRODUCER);

        while let Some(batch) = manager.next_batch("busy-client") {
            let results = batch
                .messages
                .into_iter()
                .map(|pending| HandoffResult {
                    delivery_id: pending.delivery_id,
                    status: ReliableForwardStatus::Delivered,
                })
                .collect::<Vec<_>>();
            assert!(results.len() <= 64);
            manager.apply_batch("busy-client", 3, &results);
        }
        let stats = manager.stats();
        assert_eq!(stats.pending_messages, 0);
        assert_eq!(stats.pending_bytes, 0);
        assert_eq!(stats.active_workers, 0);
    }

    #[tokio::test]
    async fn source_retains_high_volume_batch_until_receiver_completes() {
        const MESSAGES: usize = 4_096;
        let source = manager(MESSAGES + 1);
        let receiver = manager(MESSAGES + 1);
        for _ in 0..MESSAGES {
            source.enqueue(pending(&source, "migrating-client")).await.unwrap();
        }

        while source.stats().pending_messages > 0 {
            let source_batch = source.next_batch("migrating-client").unwrap();
            let mut accepted = Vec::new();
            for message in &source_batch.messages {
                receiver.enqueue(message.clone()).await.unwrap();
                accepted.push(HandoffResult {
                    delivery_id: message.delivery_id,
                    status: ReliableForwardStatus::Accepted,
                });
            }
            source.apply_batch("migrating-client", 3, &accepted);
            assert!(source.stats().pending_messages > 0);

            let receiver_batch = receiver.next_batch("migrating-client").unwrap();
            let delivered = receiver_batch
                .messages
                .into_iter()
                .map(|message| HandoffResult {
                    delivery_id: message.delivery_id,
                    status: ReliableForwardStatus::Delivered,
                })
                .collect::<Vec<_>>();
            receiver.apply_batch("migrating-client", 3, &delivered);

            let completed = source_batch
                .messages
                .into_iter()
                .map(|message| HandoffResult {
                    delivery_id: message.delivery_id,
                    status: ReliableForwardStatus::Duplicate,
                })
                .collect::<Vec<_>>();
            source.apply_batch("migrating-client", 3, &completed);
        }
        assert_eq!(receiver.stats().pending_messages, 0);
        assert_eq!(source.stats().pending_messages, 0);
    }

    #[tokio::test]
    async fn shared_local_delivery_is_not_blocked_by_opposite_recovery_heads() {
        let left = manager(10);
        let right = manager(10);
        let a = shared_pending(&left, "old-a");
        let mut b = shared_pending(&right, "old-b");
        b.delivery_id = right.next_delivery_id(2);
        let left_key = left.enqueue_shared_recovery(a.clone()).await.unwrap();
        let right_key = right.enqueue_shared_recovery(b.clone()).await.unwrap();
        let mut a_target = a.relations[0].clone();
        a_target.1 = "old-b".into();
        let mut b_target = b.relations[0].clone();
        b_target.1 = "old-a".into();
        assert!(left.reassign_shared(&left_key, a.delivery_id, 1, a_target));
        assert!(right.reassign_shared(&right_key, b.delivery_id, 2, b_target));
        let a_forward = left.next_batch(&left_key).unwrap().messages.remove(0);
        let b_forward = right.next_batch(&right_key).unwrap().messages.remove(0);
        let right_local = right.try_enqueue_shared_local(a_forward.clone()).unwrap();
        let left_local = left.try_enqueue_shared_local(b_forward.clone()).unwrap();
        for (source, key, target, id) in
            [(&left, &left_key, 2, a.delivery_id), (&right, &right_key, 1, b.delivery_id)]
        {
            source.apply_batch(
                key,
                target,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Accepted }],
            );
            assert_eq!(source.diagnostic_snapshot(key).unwrap().head_pin_origin, Some("accepted"));
        }
        assert_eq!(left.next_batch(&left_local).unwrap().messages[0].delivery_id, b.delivery_id);
        assert_eq!(right.next_batch(&right_local).unwrap().messages[0].delivery_id, a.delivery_id);
        for (receiver, key, id) in
            [(&left, &left_local, b.delivery_id), (&right, &right_local, a.delivery_id)]
        {
            receiver.apply_batch(
                key,
                1,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Delivered }],
            );
            receiver.stop_worker(key);
        }
        for (source, key, id) in [(&left, &left_key, a.delivery_id), (&right, &right_key, b.delivery_id)] {
            source.apply_batch(
                key,
                2,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Duplicate }],
            );
            source.stop_worker(key);
            assert_eq!(source.stats().pending_messages, 0);
            assert_eq!(source.stats().pending_bytes, 0);
            assert_eq!(source.stats().active_workers, 0);
        }
    }

    #[tokio::test]
    async fn shared_moves_reuse_full_capacity_and_keep_target_accounting() {
        let manager = manager(2);
        let first = shared_pending(&manager, "local");
        let second = shared_pending(&manager, "local");
        let third = shared_pending(&manager, "local");
        let key = manager.try_enqueue_shared_local(first.clone()).unwrap();
        assert_eq!(manager.try_enqueue_shared_local(second.clone()).unwrap(), key);
        assert!(manager.try_enqueue_shared_local(third).is_err());
        assert_eq!(manager.try_enqueue_shared_local(first.clone()).unwrap(), key);
        assert_eq!(manager.stats().pending_messages, 2);
        assert!(manager.restart_worker_if_pending(&key));
        let bytes = manager.stats().pending_bytes;
        let recovery = manager.promote_shared_local_front(&key).unwrap();
        assert_eq!(manager.stats().pending_messages, 2);
        assert_eq!(manager.stats().pending_bytes, bytes);
        assert_eq!(manager.pending_for_node(2), 2);
        assert_eq!(manager.stats().active_workers, 1);
        assert_eq!(manager.next_batch(&key).unwrap().messages[0].delivery_id, second.delivery_id);
        assert_eq!(manager.next_batch(&recovery).unwrap().messages[0].delivery_id, first.delivery_id);
        assert_eq!(manager.move_shared_recovery_to_local(&recovery).unwrap(), key);
        manager.stop_worker(&recovery);
        assert_eq!(manager.stats().active_workers, 1);
        let batch = manager.next_batch(&key).unwrap();
        assert_eq!(
            batch.messages.iter().map(|item| item.delivery_id).collect::<Vec<_>>(),
            vec![second.delivery_id, first.delivery_id]
        );
        manager.apply_batch(
            &key,
            1,
            &batch
                .messages
                .iter()
                .map(|item| HandoffResult {
                    delivery_id: item.delivery_id,
                    status: ReliableForwardStatus::Delivered,
                })
                .collect::<Vec<_>>(),
        );
        manager.stop_worker(&key);
        let stats = manager.stats();
        assert_eq!((stats.pending_messages, stats.pending_bytes, stats.active_workers), (0, 0, 0));
        assert_eq!(manager.pending_for_node(2), 0);
        assert!(!manager.data_loss_detected());
    }

    #[tokio::test]
    async fn outbound_capacity_leaves_an_inbound_progress_slot() {
        let manager = manager(2);
        let outbound = shared_pending(&manager, "outbound");
        let incoming = shared_pending(&manager, "incoming");
        manager.enqueue(outbound).await.unwrap();
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            manager.enqueue_shared_recovery(shared_pending(&manager, "blocked"))
        )
        .await
        .is_err());
        let incoming_key = manager.try_enqueue_shared_local(incoming.clone()).unwrap();
        assert_eq!(manager.stats().pending_messages, 2);
        assert_eq!(manager.try_enqueue_shared_local(incoming).unwrap(), incoming_key);
        assert!(!manager.data_loss_detected());
        assert_eq!(manager.stats().capacity_waiters, 0);
    }

    #[tokio::test]
    async fn unsupported_shared_capacity_is_an_error_not_an_endless_wait() {
        let single = manager(1);
        assert!(single.enqueue_shared_recovery(shared_pending(&single, "single")).await.is_err());
        let manager = HandoffManager::new(Handoff {
            max_pending_messages: 4,
            max_pending_bytes: 128,
            batch_max_bytes: 1,
            ..Handoff::default()
        });
        assert!(manager.enqueue_shared_recovery(shared_pending(&manager, "too-large")).await.is_err());
        assert!(manager.try_enqueue_shared_local(shared_pending(&manager, "too-large")).is_err());
        assert_eq!(manager.stats().pending_messages, 0);
    }

    #[tokio::test]
    async fn protected_shared_work_is_found_by_real_client_across_queue_domains() {
        let manager = manager(10);
        let pending = shared_pending(&manager, "consumer");
        let key = manager.enqueue_shared_recovery(pending.clone()).await.unwrap();
        manager.apply_batch(
            &key,
            2,
            &[HandoffResult { delivery_id: pending.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        assert!(manager.has_protected_delivery("consumer", 1));
        assert!(!manager.has_protected_delivery("unrelated", 1));
    }

    #[tokio::test]
    async fn accepted_peers_can_both_reassign_foreign_children_without_refilling_a_blocked_fifo() {
        let left = manager(10);
        let right = manager(10);
        let a = shared_pending(&left, "right-consumer");
        let mut b = shared_pending(&right, "left-consumer");
        b.delivery_id = right.next_delivery_id(2);
        let a_source = left.enqueue_shared_recovery(a.clone()).await.unwrap();
        let b_source = right.enqueue_shared_recovery(b.clone()).await.unwrap();
        let a_receiver = right.try_enqueue_shared_local(a.clone()).unwrap();
        let b_receiver = left.try_enqueue_shared_local(b.clone()).unwrap();
        left.apply_batch(
            &a_source,
            2,
            &[HandoffResult { delivery_id: a.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        right.apply_batch(
            &b_source,
            1,
            &[HandoffResult { delivery_id: b.delivery_id, status: ReliableForwardStatus::Accepted }],
        );

        // Both selected sessions disappear after accepting their obligation.
        let a_recovery = right.promote_shared_local_front(&a_receiver).unwrap();
        let b_recovery = left.promote_shared_local_front(&b_receiver).unwrap();
        right.stop_worker(&a_receiver);
        left.stop_worker(&b_receiver);
        let mut a_replacement = a.relations[0].clone();
        a_replacement.1 = "left-consumer".into();
        let mut b_replacement = b.relations[0].clone();
        b_replacement.1 = "right-consumer".into();
        assert!(right.reassign_shared(&a_recovery, a.delivery_id, 2, a_replacement));
        assert!(left.reassign_shared(&b_recovery, b.delivery_id, 1, b_replacement));
        let a_child = right.next_batch(&a_recovery).unwrap().messages.remove(0);
        let b_child = left.next_batch(&b_recovery).unwrap().messages.remove(0);
        assert_ne!(a_child.delivery_id, a.delivery_id);
        assert_ne!(b_child.delivery_id, b.delivery_id);
        assert!(right.delivery_completed(a.delivery_id));
        assert!(left.delivery_completed(b.delivery_id));
        let a_local = left.try_enqueue_shared_local(a_child.clone()).unwrap();
        let b_local = right.try_enqueue_shared_local(b_child.clone()).unwrap();
        // Each returned child is executable even while the source's parent
        // and opposite recovery child are still pending in independent lanes.
        assert_eq!(left.next_batch(&a_local).unwrap().messages[0].delivery_id, a_child.delivery_id);
        assert_eq!(right.next_batch(&b_local).unwrap().messages[0].delivery_id, b_child.delivery_id);
        for (manager, key, id) in
            [(&left, &a_local, a_child.delivery_id), (&right, &b_local, b_child.delivery_id)]
        {
            manager.apply_batch(
                key,
                1,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Delivered }],
            );
            manager.stop_worker(key);
        }
        for (manager, key, id) in [
            (&left, &a_source, a.delivery_id),
            (&right, &b_source, b.delivery_id),
            (&right, &a_recovery, a_child.delivery_id),
            (&left, &b_recovery, b_child.delivery_id),
        ] {
            manager.apply_batch(
                key,
                2,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Duplicate }],
            );
            manager.stop_worker(key);
        }
        for manager in [&left, &right] {
            let stats = manager.stats();
            assert_eq!((stats.pending_messages, stats.pending_bytes, stats.active_workers), (0, 0, 0));
            assert_eq!(manager.pending_for_node(2), 0);
            assert!(!manager.data_loss_detected());
        }
    }

    #[test]
    fn shared_move_cannot_be_detached_by_destination_cleanup() {
        for _ in 0..32 {
            let manager = manager(10);
            let pending = shared_pending(&manager, "racing-local");
            let source =
                futures::executor::block_on(manager.enqueue_shared_recovery(pending.clone())).unwrap();
            let local = HandoffManager::shared_local_key("racing-local");
            let empty = Arc::new(Mutex::new(ClientQueue::default()));
            manager.queues.insert(local.clone().into(), empty.clone());
            let start = Arc::new(Barrier::new(2));
            let cleanup_manager = manager.clone();
            let cleanup_key = local.clone();
            let cleanup_start = start.clone();
            let cleanup = thread::spawn(move || {
                cleanup_start.wait();
                cleanup_manager.remove_unused_queue(&cleanup_key, &empty);
            });
            start.wait();
            assert_eq!(manager.move_shared_recovery_to_local(&source).unwrap(), local);
            cleanup.join().unwrap();
            assert_eq!(manager.next_batch(&local).unwrap().messages[0].delivery_id, pending.delivery_id);
            assert_eq!(manager.stats().pending_messages, 1);
            assert_eq!(manager.stats().invariant_mismatch_observations, 0);
        }
    }

    #[tokio::test]
    async fn full_peers_release_inbound_progress_credit_when_owner_changes_without_reselection() {
        let prototype = manager(2);
        let size = shared_pending(&prototype, "same-client-a").size;
        let cfg = Handoff {
            max_pending_messages: 2,
            max_pending_bytes: size * 2,
            batch_max_bytes: size,
            ..prototype.cfg.clone()
        };
        let left = HandoffManager::new(cfg.clone());
        let right = HandoffManager::new(cfg);
        let mut a = shared_pending(&left, "same-client-a");
        a.shared_relation_unbound = true;
        let mut b = shared_pending(&right, "same-client-b");
        b.delivery_id = right.next_delivery_id(2);
        let a_source = left.enqueue_shared_recovery(a.clone()).await.unwrap();
        let b_source = right.enqueue_shared_recovery(b.clone()).await.unwrap();
        let a_local = right.try_enqueue_shared_local(a.clone()).unwrap();
        let b_local = left.try_enqueue_shared_local(b.clone()).unwrap();
        left.apply_batch(
            &a_source,
            2,
            &[HandoffResult { delivery_id: a.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        right.apply_batch(
            &b_source,
            1,
            &[HandoffResult { delivery_id: b.delivery_id, status: ReliableForwardStatus::Accepted }],
        );
        assert_eq!(left.stats().pending_messages, 2);
        assert_eq!(right.stats().pending_messages, 2);
        assert_eq!(left.stats().pending_bytes, size * 2);
        assert_eq!(right.stats().pending_bytes, size * 2);
        let a_recovery = right.promote_shared_local_front(&a_local).unwrap();
        let b_recovery = left.promote_shared_local_front(&b_local).unwrap();
        right.stop_worker(&a_local);
        left.stop_worker(&b_local);
        assert!(right.prepare_shared_recovery_child(&a_recovery, 2));
        assert!(left.prepare_shared_recovery_child(&b_recovery, 1));
        assert!(!right.prepare_shared_recovery_child(&a_recovery, 2), "one fork per receiver ownership");
        let a_child = right.next_batch(&a_recovery).unwrap().messages.remove(0);
        let b_child = left.next_batch(&b_recovery).unwrap().messages.remove(0);
        assert_eq!(a_child.target_client_id, a.target_client_id);
        assert_eq!(b_child.target_client_id, b.target_client_id);
        assert_eq!(a_child.created_at, a.created_at);
        assert_eq!(a_child.size, a.size);
        assert!(a_child.shared_relation_unbound);
        assert_ne!(a_child.delivery_id, a.delivery_id);
        assert_ne!(b_child.delivery_id, b.delivery_id);
        assert!(right.delivery_completed(a.delivery_id));
        assert!(left.delivery_completed(b.delivery_id));
        // A parent reply bypasses full-capacity admission. Each upstream can
        // release its outgoing slot before either recovery child finishes.
        left.apply_batch(
            &a_source,
            2,
            &[HandoffResult { delivery_id: a.delivery_id, status: ReliableForwardStatus::Duplicate }],
        );
        right.apply_batch(
            &b_source,
            1,
            &[HandoffResult { delivery_id: b.delivery_id, status: ReliableForwardStatus::Duplicate }],
        );
        left.stop_worker(&a_source);
        right.stop_worker(&b_source);
        assert_eq!(left.stats().pending_messages, 1);
        assert_eq!(right.stats().pending_messages, 1);
        let a_returned = left.try_enqueue_shared_local(a_child.clone()).unwrap();
        let b_returned = right.try_enqueue_shared_local(b_child.clone()).unwrap();
        for (manager, key, id) in
            [(&left, &a_returned, a_child.delivery_id), (&right, &b_returned, b_child.delivery_id)]
        {
            manager.apply_batch(
                key,
                1,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Delivered }],
            );
            manager.stop_worker(key);
        }
        for (manager, key, id) in
            [(&right, &a_recovery, a_child.delivery_id), (&left, &b_recovery, b_child.delivery_id)]
        {
            manager.apply_batch(
                key,
                2,
                &[HandoffResult { delivery_id: id, status: ReliableForwardStatus::Duplicate }],
            );
            manager.stop_worker(key);
            let stats = manager.stats();
            assert_eq!((stats.pending_messages, stats.pending_bytes, stats.active_workers), (0, 0, 0));
            assert_eq!(manager.pending_for_node(2), 0);
            assert!(!manager.data_loss_detected());
        }
    }

    #[tokio::test]
    async fn foreign_recovery_child_cannot_release_a_remote_pin_or_uncertainty() {
        for uncertain in [false, true] {
            let manager = manager(10);
            let item = shared_pending(&manager, "protected");
            let key = manager.enqueue_shared_recovery(item.clone()).await.unwrap();
            if uncertain {
                let batch = manager.next_batch(&key).unwrap();
                manager.pin_uncertain_batch(&key, &batch.messages, 3, super::ForwardAttemptStage::SendCall);
            } else {
                manager.apply_batch(
                    &key,
                    3,
                    &[HandoffResult {
                        delivery_id: item.delivery_id,
                        status: ReliableForwardStatus::Accepted,
                    }],
                );
            }
            assert!(!manager.prepare_shared_recovery_child(&key, 2));
            let head = manager.next_batch(&key).unwrap().messages.remove(0);
            assert_eq!(head.delivery_id, item.delivery_id);
            assert!(manager.delivery_pending(item.delivery_id));
            assert_eq!(manager.diagnostic_snapshot(&key).unwrap().head_pinned_node_id, Some(3));
        }
    }

    #[tokio::test]
    async fn shared_byte_backpressure_waits_without_waking_its_own_reservation_rollback() {
        let prototype = manager(10);
        let size = shared_pending(&prototype, "peer-1").size;
        let manager = HandoffManager::new(Handoff {
            max_pending_bytes: size * 4,
            batch_max_bytes: size * 2,
            ..prototype.cfg.clone()
        });
        manager.enqueue_shared_recovery(shared_pending(&manager, "peer-1")).await.unwrap();
        manager.enqueue_shared_recovery(shared_pending(&manager, "peer-2")).await.unwrap();
        // Message capacity is still available; only outbound bytes are full.
        // A self-notifying rollback would spin and prevent this timeout from
        // firing on the current-thread Tokio test runtime.
        let blocked = tokio::time::timeout(
            Duration::from_millis(20),
            manager.enqueue_shared_recovery(shared_pending(&manager, "peer-3")),
        )
        .await;
        assert!(blocked.is_err());
        assert_eq!(manager.stats().pending_messages, 2);
        assert_eq!(manager.stats().capacity_waiters, 0);
        assert!(!manager.data_loss_detected());
    }

    #[test]
    fn queue_audit_cannot_repair_capacity_owned_by_an_in_progress_admission() {
        let manager = manager(10);
        let admission =
            super::AdmissionGuard::new(&manager.admissions_in_progress, &manager.progress_sequence);
        // Model the interval after quota reservation and before queue push.
        manager.pending_messages.store(1, Ordering::SeqCst);
        manager.pending_bytes.store(123, Ordering::SeqCst);
        for _ in 0..5 {
            let stats = manager.stats();
            assert_eq!(stats.pending_messages, 1);
            assert_eq!(stats.pending_bytes, 123);
            assert_eq!(stats.invariant_mismatch_observations, 0);
            assert!(!manager.data_loss_detected());
        }
        manager.pending_messages.store(0, Ordering::SeqCst);
        manager.pending_bytes.store(0, Ordering::SeqCst);
        drop(admission);
        assert_eq!(manager.stats().pending_messages, 0);
    }

    #[tokio::test]
    async fn shared_recovery_queue_count_does_not_inflate_real_client_count() {
        let manager = manager(10);
        manager.enqueue_shared_recovery(shared_pending(&manager, "same-consumer")).await.unwrap();
        manager.enqueue_shared_recovery(shared_pending(&manager, "same-consumer")).await.unwrap();
        manager.try_enqueue_shared_local(shared_pending(&manager, "same-consumer")).unwrap();
        assert_eq!(manager.queues.len(), 3);
        let stats = manager.stats();
        assert_eq!(stats.pending_messages, 3);
        assert_eq!(stats.clients, 1);
    }
}
