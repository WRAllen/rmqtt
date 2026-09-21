use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;
use rand::random;
use tokio::sync::Notify;

use rmqtt::types::{ClientId, From, NodeId, Publish, SubRelations};

use super::config::Handoff;
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
    pub created_at: Instant,
    pub size: usize,
}

impl PendingHandoff {
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
}

impl QueuedHandoff {
    fn new(pending: PendingHandoff) -> Self {
        Self {
            pending,
            pinned_node_id: None,
            remotely_completed: false,
            transport_uncertain: false,
            rejected: false,
        }
    }
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
    delivery_states: Arc<DashMap<DeliveryId, DeliveryState>>,
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
            delivery_states: Arc::new(DashMap::new()),
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
        self.completed_order.lock().push_back((completed_at, delivery_id));
        self.progress_sequence.fetch_add(1, Ordering::Release);
        self.cleanup_completed();
    }

    pub(crate) async fn enqueue(&self, pending: PendingHandoff) -> Result<bool, PendingHandoff> {
        self.enqueue_with_hook(pending, || {}).await
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
        if pending.size > self.cfg.max_pending_bytes || self.cfg.max_pending_messages == 0 {
            self.rejected_messages.fetch_add(1, Ordering::SeqCst);
            self.data_loss_detected.store(true, Ordering::Release);
            return Err(pending);
        }

        let size = pending.size;
        loop {
            let notified = self.capacity_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if reserve_counter(&self.pending_messages, 1, self.cfg.max_pending_messages) {
                if reserve_counter(&self.pending_bytes, size, self.cfg.max_pending_bytes) {
                    break;
                }
                self.pending_messages.fetch_sub(1, Ordering::SeqCst);
            }
            let waiter = CapacityWaiterGuard::new(&self.capacity_waiters);
            notified.as_mut().await;
            drop(waiter);
        }

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
        let queue_entry =
            self.queues.entry(client_id).or_insert_with(|| Arc::new(Mutex::new(ClientQueue::default())));
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
        let start_worker = if queue.worker_running {
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
        let mut messages = Vec::with_capacity(self.cfg.batch_max_messages);
        let mut bytes = 0usize;
        for item in &queue.messages {
            if item.remotely_completed || item.pinned_node_id != pinned_node_id {
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
        self.queues.get(client_id).is_some_and(|entry| {
            entry.lock().messages.iter().any(|item| {
                item.pinned_node_id.is_some()
                    || item.transport_uncertain
                    || item.pending.delivery_id.origin_node_id != local_id
            })
        })
    }

    pub(crate) fn mark_transport_uncertain(&self, client_id: &str, batch: &[PendingHandoff]) {
        if let Some(entry) = self.queues.get(client_id) {
            for item in &mut entry.lock().messages {
                if batch.iter().any(|pending| pending.delivery_id == item.pending.delivery_id) {
                    item.transport_uncertain = true;
                }
            }
        }
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
        let progress_before = self.progress_sequence.load(Ordering::Acquire);
        let queue_refs = self.queues.iter().map(|entry| entry.value().clone()).collect::<Vec<_>>();
        let mut actual_pending_messages = 0usize;
        let mut actual_pending_bytes = 0usize;
        let mut actual_active_workers = 0usize;
        let mut actual_clients = 0usize;
        let mut oldest_age_millis = 0u64;
        for queue_ref in &queue_refs {
            let queue = queue_ref.lock();
            if !queue.messages.is_empty() || queue.worker_running {
                actual_clients += 1;
            }
            actual_active_workers += usize::from(queue.worker_running);
            actual_pending_messages += queue.messages.len();
            actual_pending_bytes += queue.messages.iter().map(|item| item.pending.size).sum::<usize>();
            if let Some(front) = queue.messages.front() {
                oldest_age_millis = oldest_age_millis
                    .max(now.saturating_duration_since(front.pending.created_at).as_millis() as u64);
            }
        }
        let progress_after = self.progress_sequence.load(Ordering::Acquire);
        let stable_snapshot = progress_before == progress_after && queue_refs.len() == self.queues.len();
        let counted_pending_messages = self.pending_messages.load(Ordering::SeqCst);
        let counted_pending_bytes = self.pending_bytes.load(Ordering::SeqCst);
        let counted_active_workers = self.active_workers.load(Ordering::SeqCst);
        let delivery_pending_messages = self
            .delivery_states
            .iter()
            .filter(|entry| matches!(*entry.value(), DeliveryState::Pending))
            .count();
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
            (actual_pending_messages, actual_pending_bytes, actual_active_workers, actual_clients)
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

    #[tokio::test]
    async fn uncertain_transport_requires_final_delivery_confirmation() {
        let manager = manager(10);
        let pending = pending(&manager, "client");
        manager.enqueue(pending.clone()).await.unwrap();
        let batch = manager.next_batch("client").unwrap();
        manager.mark_transport_uncertain("client", &batch.messages);
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
}
