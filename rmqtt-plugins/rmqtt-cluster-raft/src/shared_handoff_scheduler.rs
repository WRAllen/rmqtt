//! Bounded-worker scheduling for maintenance-only shared handoff work.
//!
//! The scheduler stores one state value per `(kind, key)`. Waiting work lives in
//! a deadline index rather than occupying a worker or another client's FIFO.
//! Capacity and delivery ownership remain the handoff manager's responsibility.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum WorkKind {
    Recovery,
    LocalDelivery,
}

impl WorkKind {
    fn index(self) -> usize {
        match self {
            Self::Recovery => 0,
            Self::LocalDelivery => 1,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WorkId {
    key: String,
    kind: WorkKind,
}

type Deadline = (Instant, u64);

struct Entry<S> {
    state: Option<S>,
    deadline: Option<Deadline>,
    running: Option<u64>,
    dirty: bool,
}

struct SchedulerState<S> {
    entries: HashMap<WorkId, Entry<S>>,
    deadlines: [BTreeMap<Deadline, WorkId>; 2],
    sequence: u64,
}

impl<S> Default for SchedulerState<S> {
    fn default() -> Self {
        Self { entries: HashMap::new(), deadlines: std::array::from_fn(|_| BTreeMap::new()), sequence: 0 }
    }
}

impl<S> SchedulerState<S> {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.checked_add(1).expect("shared handoff scheduler sequence exhausted");
        self.sequence
    }

    fn queue_at(&mut self, id: WorkId, at: Instant) {
        let deadline = (at, self.next_sequence());
        let entry = self.entries.get_mut(&id).expect("scheduled shared handoff entry exists");
        if let Some(old) = entry.deadline.replace(deadline) {
            self.deadlines[id.kind.index()].remove(&old);
        }
        self.deadlines[id.kind.index()].insert(deadline, id);
    }
}

struct Inner<S> {
    state: Mutex<SchedulerState<S>>,
    notify: [Notify; 2],
}

impl<S: Default> Inner<S> {
    fn finish(&self, id: WorkId, token: u64, state: S, delay: Option<Duration>) {
        let mut scheduler = self.state.lock();
        let Some(entry) = scheduler.entries.get_mut(&id) else {
            return;
        };
        if entry.running != Some(token) {
            return;
        }
        // A concurrent enqueue may have observed an already-running key just
        // after its worker observed an empty queue. Do not retire that wakeup.
        let retry_at = match delay {
            // New messages must not turn an Accepted/retryable head into a
            // polling loop whose rate follows the publisher's message rate.
            Some(delay) => Some(Instant::now() + delay),
            None if entry.dirty => Some(Instant::now()),
            None => None,
        };
        if let Some(at) = retry_at {
            entry.state = Some(state);
            entry.running = None;
            entry.dirty = false;
            scheduler.queue_at(id.clone(), at);
            drop(scheduler);
            self.notify[id.kind.index()].notify_one();
        } else {
            scheduler.entries.remove(&id);
        }
    }
}

/// A worker lease. Dropping it during cancellation or unwinding puts its state
/// back into the same scheduling domain immediately. There is never a second
/// running lease for the same `(kind, key)`.
pub(crate) struct SharedWork<S: Default> {
    pub(crate) key: String,
    pub(crate) kind: WorkKind,
    pub(crate) state: S,
    lease: Option<Lease<S>>,
}

struct Lease<S> {
    inner: Weak<Inner<S>>,
    id: WorkId,
    token: u64,
}

impl<S: Default> SharedWork<S> {
    fn finish(mut self, delay: Option<Duration>) {
        if let Some(lease) = self.lease.take() {
            if let Some(inner) = lease.inner.upgrade() {
                inner.finish(lease.id, lease.token, std::mem::take(&mut self.state), delay);
            }
        }
    }
}

impl<S: Default> Drop for SharedWork<S> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            if let Some(inner) = lease.inner.upgrade() {
                inner.finish(lease.id, lease.token, std::mem::take(&mut self.state), Some(Duration::ZERO));
            }
        }
    }
}

pub(crate) struct SharedHandoffScheduler<S: Default> {
    inner: Arc<Inner<S>>,
}

impl<S: Default> Clone for SharedHandoffScheduler<S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<S: Default> Default for SharedHandoffScheduler<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Default> SharedHandoffScheduler<S> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(SchedulerState::default()),
                notify: std::array::from_fn(|_| Notify::new()),
            }),
        }
    }

    /// Add a new key, or wake an existing one without creating another task.
    /// Returns true only when a new `(kind, key)` was inserted. Fresh local work
    /// may advance a delayed local key; recovery keys retain their retry limit.
    pub(crate) fn schedule(&self, key: String, kind: WorkKind) -> bool {
        let id = WorkId { key, kind };
        let now = Instant::now();
        let mut scheduler = self.inner.state.lock();
        let inserted = match scheduler.entries.get_mut(&id) {
            Some(entry) if entry.running.is_some() => {
                entry.dirty = true;
                return false;
            }
            Some(entry) if entry.deadline.is_some_and(|deadline| deadline.0 <= now) => return false,
            Some(_) if kind == WorkKind::Recovery => return false,
            Some(_) => false,
            None => {
                scheduler.entries.insert(
                    id.clone(),
                    Entry { state: Some(S::default()), deadline: None, running: None, dirty: false },
                );
                true
            }
        };
        scheduler.queue_at(id, now);
        drop(scheduler);
        self.inner.notify[kind.index()].notify_one();
        inserted
    }

    /// Sleep until this domain has due work. Waiting on a deadline does not
    /// reserve a work item, and cancellation of `next` cannot strand a lease.
    pub(crate) async fn next(&self, kind: WorkKind) -> SharedWork<S> {
        loop {
            let notified = self.inner.notify[kind.index()].notified();
            tokio::pin!(notified);
            // Register before inspecting the index: schedule/reschedule can
            // then neither race the empty check nor lose the only wakeup.
            notified.as_mut().enable();
            let deadline = {
                let mut scheduler = self.inner.state.lock();
                let first = scheduler.deadlines[kind.index()].first_key_value().map(|(key, _)| *key);
                if let Some(deadline) = first.filter(|deadline| deadline.0 <= Instant::now()) {
                    let id = scheduler.deadlines[kind.index()]
                        .remove(&deadline)
                        .expect("due shared handoff entry exists");
                    let token = scheduler.next_sequence();
                    let entry = scheduler.entries.get_mut(&id).expect("due shared handoff state exists");
                    entry.deadline = None;
                    entry.running = Some(token);
                    entry.dirty = false;
                    let state = entry.state.take().expect("queued shared handoff state is not leased");
                    return SharedWork {
                        key: id.key.clone(),
                        kind: id.kind,
                        state,
                        lease: Some(Lease { inner: Arc::downgrade(&self.inner), id, token }),
                    };
                }
                first.map(|deadline| deadline.0)
            };
            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = notified.as_mut() => {},
                    _ = tokio::time::sleep_until(deadline) => {},
                }
            } else {
                notified.await;
            }
        }
    }

    /// Retire a completed key, unless a concurrent schedule marked it dirty.
    pub(crate) fn complete(&self, work: SharedWork<S>) {
        work.finish(None);
    }

    /// Release the worker while keeping its state and retry deadline. Unlike
    /// complete, reschedule always honors its delay even after a new enqueue;
    /// retry polling must remain bounded under continuous incoming traffic.
    pub(crate) fn reschedule(&self, work: SharedWork<S>, delay: Duration) {
        work.finish(Some(delay));
    }
}

#[cfg(test)]
mod tests {
    use super::{SharedHandoffScheduler, WorkKind};
    use std::time::Duration;
    use tokio::time::{timeout, Instant};

    const SOON: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn duplicate_key_has_one_worker_and_preserves_state_across_retry() {
        let scheduler = SharedHandoffScheduler::<usize>::new();
        assert!(scheduler.schedule("client".into(), WorkKind::Recovery));
        assert!(!scheduler.schedule("client".into(), WorkKind::Recovery));
        let mut work = scheduler.next(WorkKind::Recovery).await;
        work.state = 17;
        assert!(timeout(Duration::from_millis(10), scheduler.next(WorkKind::Recovery)).await.is_err());
        scheduler.reschedule(work, Duration::ZERO);
        let work = timeout(SOON, scheduler.next(WorkKind::Recovery)).await.unwrap();
        assert_eq!(work.state, 17);
        scheduler.complete(work);
        assert!(scheduler.inner.state.lock().entries.is_empty());
    }

    #[tokio::test]
    async fn retries_wait_until_due_and_do_not_block_other_keys() {
        let scheduler = SharedHandoffScheduler::<()>::new();
        scheduler.schedule("delayed".into(), WorkKind::Recovery);
        let work = scheduler.next(WorkKind::Recovery).await;
        let retry_at = Instant::now() + Duration::from_millis(60);
        scheduler.reschedule(work, Duration::from_millis(60));
        scheduler.schedule("ready".into(), WorkKind::Recovery);
        let ready = scheduler.next(WorkKind::Recovery).await;
        assert_eq!(ready.key, "ready");
        scheduler.complete(ready);
        assert!(timeout(Duration::from_millis(10), scheduler.next(WorkKind::Recovery)).await.is_err());
        let delayed = timeout(SOON, scheduler.next(WorkKind::Recovery)).await.unwrap();
        assert_eq!(delayed.key, "delayed");
        assert!(Instant::now() >= retry_at);
        scheduler.complete(delayed);
    }

    #[tokio::test]
    async fn local_and_recovery_domains_are_independent_for_the_same_client() {
        let scheduler = SharedHandoffScheduler::<()>::new();
        scheduler.schedule("client".into(), WorkKind::Recovery);
        scheduler.schedule("client".into(), WorkKind::LocalDelivery);
        let recovery = scheduler.next(WorkKind::Recovery).await;
        let local = timeout(SOON, scheduler.next(WorkKind::LocalDelivery)).await.unwrap();
        assert_eq!(recovery.key, local.key);
        assert_eq!(local.kind, WorkKind::LocalDelivery);
        scheduler.complete(local);
        scheduler.reschedule(recovery, Duration::from_secs(60));
        scheduler.schedule("another-local".into(), WorkKind::LocalDelivery);
        let local = timeout(SOON, scheduler.next(WorkKind::LocalDelivery)).await.unwrap();
        scheduler.complete(local);
    }

    #[tokio::test]
    async fn enqueue_racing_empty_observation_is_not_lost_by_complete() {
        let scheduler = SharedHandoffScheduler::<usize>::new();
        scheduler.schedule("client".into(), WorkKind::LocalDelivery);
        let mut work = scheduler.next(WorkKind::LocalDelivery).await;
        work.state = 3;
        assert!(!scheduler.schedule("client".into(), WorkKind::LocalDelivery));
        scheduler.complete(work);
        let work = timeout(SOON, scheduler.next(WorkKind::LocalDelivery)).await.unwrap();
        assert_eq!(work.state, 3);
        scheduler.complete(work);
        assert!(scheduler.inner.state.lock().entries.is_empty());
    }

    #[tokio::test]
    async fn enqueue_during_execution_cannot_bypass_retry_backoff() {
        let scheduler = SharedHandoffScheduler::<()>::new();
        for kind in [WorkKind::Recovery, WorkKind::LocalDelivery] {
            scheduler.schedule("client".into(), kind);
            let work = scheduler.next(kind).await;
            scheduler.schedule("client".into(), kind);
            scheduler.reschedule(work, Duration::from_millis(40));
            assert!(timeout(Duration::from_millis(10), scheduler.next(kind)).await.is_err());
            let work = timeout(SOON, scheduler.next(kind)).await.unwrap();
            scheduler.complete(work);
        }
    }

    #[tokio::test]
    async fn fresh_enqueue_wakes_a_worker_waiting_for_a_later_deadline() {
        let scheduler = SharedHandoffScheduler::<()>::new();
        scheduler.schedule("client".into(), WorkKind::LocalDelivery);
        let work = scheduler.next(WorkKind::LocalDelivery).await;
        scheduler.reschedule(work, Duration::from_secs(60));
        let worker = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move { scheduler.next(WorkKind::LocalDelivery).await })
        };
        tokio::task::yield_now().await;
        assert!(!scheduler.schedule("client".into(), WorkKind::LocalDelivery));
        let work = timeout(SOON, worker).await.unwrap().unwrap();
        scheduler.complete(work);
    }

    #[tokio::test]
    async fn recovery_enqueue_cannot_advance_an_existing_deadline() {
        let scheduler = SharedHandoffScheduler::<()>::new();
        scheduler.schedule("client".into(), WorkKind::Recovery);
        let work = scheduler.next(WorkKind::Recovery).await;
        scheduler.reschedule(work, Duration::from_millis(40));
        let deadline =
            *scheduler.inner.state.lock().deadlines[WorkKind::Recovery.index()].first_key_value().unwrap().0;
        for _ in 0..100 {
            assert!(!scheduler.schedule("client".into(), WorkKind::Recovery));
        }
        assert_eq!(
            *scheduler.inner.state.lock().deadlines[WorkKind::Recovery.index()].first_key_value().unwrap().0,
            deadline
        );
        assert!(timeout(Duration::from_millis(10), scheduler.next(WorkKind::Recovery)).await.is_err());
        let work = timeout(SOON, scheduler.next(WorkKind::Recovery)).await.unwrap();
        scheduler.complete(work);
    }

    #[tokio::test]
    async fn dropped_or_panicking_worker_returns_its_lease_and_state() {
        let scheduler = SharedHandoffScheduler::<usize>::new();
        scheduler.schedule("client".into(), WorkKind::Recovery);
        let mut work = scheduler.next(WorkKind::Recovery).await;
        work.state = 41;
        drop(work);
        let mut work = timeout(SOON, scheduler.next(WorkKind::Recovery)).await.unwrap();
        assert_eq!(work.state, 41);
        work.state = 42;
        let panic = tokio::spawn(async move {
            let _work = work;
            panic!("simulated shared handoff worker panic");
        });
        assert!(panic.await.unwrap_err().is_panic());
        let work = timeout(SOON, scheduler.next(WorkKind::Recovery)).await.unwrap();
        assert_eq!(work.state, 42);
        scheduler.complete(work);
    }

    #[tokio::test]
    async fn repeated_wakeups_do_not_accumulate_stale_deadlines() {
        let scheduler = SharedHandoffScheduler::<()>::new();
        scheduler.schedule("client".into(), WorkKind::Recovery);
        let work = scheduler.next(WorkKind::Recovery).await;
        scheduler.reschedule(work, Duration::from_secs(60));
        for _ in 0..10_000 {
            assert!(!scheduler.schedule("client".into(), WorkKind::Recovery));
        }
        let state = scheduler.inner.state.lock();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.deadlines[WorkKind::Recovery.index()].len(), 1);
    }
}
