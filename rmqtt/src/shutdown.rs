//! In-memory process termination coordination.
//!
//! This state is intentionally outside the MQTT publish hot path. It is read
//! only when admitting a session, serving readiness, or handling an operator
//! shutdown request.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct ShutdownCoordinator {
    inner: Arc<ShutdownInner>,
}

#[derive(Debug)]
struct ShutdownInner {
    node_id: u64,
    enabled: bool,
    timeout: Duration,
    finalize_reserve: Duration,
    terminating: AtomicBool,
    pod_name: String,
    pod_uid: String,
    boot_id: String,
    state: Mutex<ShutdownState>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct ShutdownState {
    deadline: Option<Instant>,
    reason: Option<String>,
    phase: Option<String>,
    coordinator_epoch: Option<String>,
    completed: bool,
    outcome: Option<String>,
}

impl ShutdownCoordinator {
    pub fn new(node_id: u64, enabled: bool, timeout: Duration, finalize_reserve: Duration) -> Self {
        let started = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros();
        let boot_id = format!("{node_id}-{}-{started}", std::process::id());
        Self {
            inner: Arc::new(ShutdownInner {
                node_id,
                enabled,
                timeout,
                finalize_reserve: finalize_reserve.min(timeout),
                terminating: AtomicBool::new(false),
                pod_name: std::env::var("RMQTT_POD_NAME").unwrap_or_default(),
                pod_uid: std::env::var("RMQTT_POD_UID").unwrap_or_default(),
                boot_id,
                state: Mutex::new(ShutdownState::default()),
                changed: Notify::new(),
            }),
        }
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    #[inline]
    pub fn is_terminating(&self) -> bool {
        self.inner.terminating.load(Ordering::Acquire)
    }

    #[inline]
    pub fn pod_name(&self) -> &str {
        &self.inner.pod_name
    }

    /// Latches termination once. Repeated calls never extend the deadline.
    pub fn request(&self, reason: impl Into<String>) -> bool {
        if !self.enabled() {
            return false;
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_terminating() {
            return false;
        }
        state.deadline = Some(Instant::now() + self.inner.timeout);
        state.reason = Some(reason.into());
        state.phase = Some("signal_received".to_string());
        // Publish termination only after its deadline is initialized. A
        // concurrent bind must never have its shorter deadline overwritten.
        self.inner.terminating.store(true, Ordering::Release);
        drop(state);
        self.inner.changed.notify_waiters();
        true
    }

    pub fn bind(
        &self,
        expected_pod_uid: Option<&str>,
        expected_boot_id: Option<&str>,
        remaining: Option<Duration>,
        coordinator_epoch: Option<String>,
    ) -> crate::Result<Value> {
        if !self.enabled() {
            return Err(anyhow::anyhow!("graceful shutdown is disabled"));
        }
        self.verify_identity(expected_pod_uid, expected_boot_id)?;
        self.request("operator_external_termination");
        let now = Instant::now();
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(epoch) = coordinator_epoch.as_deref() {
            match state.coordinator_epoch.as_deref() {
                Some(existing) if existing != epoch => {
                    return Err(anyhow::anyhow!(
                        "shutdown is already bound to coordinator epoch {existing}, refusing {epoch}"
                    ));
                }
                Some(_) => {}
                None => state.coordinator_epoch = Some(epoch.to_owned()),
            }
        }
        if coordinator_epoch.is_some() && !state.completed {
            state.phase = Some("operator_bound".to_string());
        }
        if let Some(remaining) = remaining {
            let proposed = now + remaining;
            state.deadline = Some(state.deadline.map_or(proposed, |current| current.min(proposed)));
        }
        drop(state);
        self.inner.changed.notify_waiters();
        Ok(self.status_json())
    }

    pub fn complete(
        &self,
        expected_pod_uid: Option<&str>,
        expected_boot_id: Option<&str>,
        coordinator_epoch: Option<&str>,
        outcome: impl Into<String>,
    ) -> crate::Result<Value> {
        self.verify_identity(expected_pod_uid, expected_boot_id)?;
        if !self.is_terminating() {
            return Err(anyhow::anyhow!("shutdown has not been requested"));
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if coordinator_epoch.is_some_and(|epoch| state.coordinator_epoch.as_deref() != Some(epoch)) {
            return Err(anyhow::anyhow!("shutdown coordinator epoch mismatch"));
        }
        state.completed = true;
        state.phase = Some("completed".to_string());
        if state.outcome.is_none() {
            state.outcome = Some(outcome.into());
        }
        drop(state);
        self.inner.changed.notify_waiters();
        Ok(self.status_json())
    }

    /// Updates the observable shutdown stage. This is only called by control
    /// plane actions and never by the MQTT publish path.
    pub fn set_phase(&self, phase: impl Into<String>) {
        if !self.is_terminating() {
            return;
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.completed {
            return;
        }
        state.phase = Some(phase.into());
        drop(state);
        self.inner.changed.notify_waiters();
    }

    pub fn verify_identity(
        &self,
        expected_pod_uid: Option<&str>,
        expected_boot_id: Option<&str>,
    ) -> crate::Result<()> {
        if expected_pod_uid.is_some_and(|expected| expected != self.inner.pod_uid) {
            return Err(anyhow::anyhow!("pod uid mismatch"));
        }
        if expected_boot_id.is_some_and(|expected| expected != self.inner.boot_id) {
            return Err(anyhow::anyhow!("process boot id mismatch"));
        }
        Ok(())
    }

    pub async fn wait_until_complete_or_deadline(&self) -> Value {
        loop {
            // Register before inspecting state so a completion or shortened
            // deadline between the inspection and await cannot be missed.
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (completed, deadline) = {
                let state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                (state.completed, state.deadline)
            };
            if completed {
                return self.status_json();
            }
            let Some(deadline) = deadline else {
                changed.await;
                continue;
            };
            let now = Instant::now();
            if now >= deadline {
                let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                state.outcome.get_or_insert_with(|| "LocalOnlyDeadlineExceeded".to_string());
                drop(state);
                return self.status_json();
            }
            tokio::select! {
                _ = &mut changed => {}
                _ = tokio::time::sleep(deadline.saturating_duration_since(now)) => {}
            }
        }
    }

    pub fn status_json(&self) -> Value {
        let state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let remaining_millis = state
            .deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()).as_millis() as u64);
        json!({
            "enabled": self.inner.enabled,
            "nodeId": self.inner.node_id,
            "terminating": self.is_terminating(),
            "podName": self.inner.pod_name,
            "podUid": self.inner.pod_uid,
            "bootId": self.inner.boot_id,
            "reason": state.reason,
            "phase": state.phase,
            "remainingMillis": remaining_millis,
            "finalizeReserveMillis": self.inner.finalize_reserve.as_millis() as u64,
            "coordinatorEpoch": state.coordinator_epoch,
            "completed": state.completed,
            "outcome": state.outcome,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ShutdownCoordinator;
    use std::time::Duration;

    #[tokio::test]
    async fn repeated_requests_do_not_extend_deadline() {
        let shutdown = ShutdownCoordinator::new(1, true, Duration::from_millis(40), Duration::from_millis(5));
        assert!(shutdown.request("sigterm"));
        let first = shutdown.status_json()["remainingMillis"].as_u64().unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!shutdown.request("second"));
        let second = shutdown.status_json()["remainingMillis"].as_u64().unwrap();
        assert!(second < first);
    }

    #[tokio::test]
    async fn completion_wakes_waiter() {
        let shutdown = ShutdownCoordinator::new(2, true, Duration::from_secs(30), Duration::from_millis(10));
        shutdown.request("sigterm");
        let wait = shutdown.wait_until_complete_or_deadline();
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        let completer = shutdown.clone();
        let complete = tokio::spawn(async move {
            completer.complete(None, None, None, "Completed").unwrap();
        });
        let status = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("completion must wake the already waiting task");
        complete.await.unwrap();
        assert_eq!(status["outcome"], "Completed");
        assert_eq!(status["phase"], "completed");
    }

    #[tokio::test]
    async fn shortened_deadline_wakes_waiter() {
        let shutdown = ShutdownCoordinator::new(2, true, Duration::from_secs(30), Duration::from_millis(10));
        shutdown.request("sigterm");
        let wait = shutdown.wait_until_complete_or_deadline();
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        let coordinator = shutdown.clone();
        let bind = tokio::spawn(async move {
            coordinator.bind(None, None, Some(Duration::ZERO), Some("pod-a".into())).unwrap();
        });
        let status = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("binding must wake the waiter to use the shortened deadline");
        bind.await.unwrap();
        assert_eq!(status["outcome"], "LocalOnlyDeadlineExceeded");
    }

    #[test]
    fn disabled_shutdown_does_not_latch_termination() {
        let shutdown = ShutdownCoordinator::new(2, false, Duration::from_secs(30), Duration::from_secs(1));
        assert!(!shutdown.request("sigterm"));
        assert!(shutdown.bind(None, None, None, Some("pod-a".into())).is_err());
        assert!(!shutdown.is_terminating());
        let status = shutdown.status_json();
        assert!(status["remainingMillis"].is_null());
        assert!(status["reason"].is_null());
    }

    #[test]
    fn phase_tracks_control_plane_progress_and_stays_completed() {
        let shutdown = ShutdownCoordinator::new(4, true, Duration::from_secs(1), Duration::from_millis(10));
        shutdown.request("sigterm");
        assert_eq!(shutdown.status_json()["phase"], "signal_received");
        shutdown.bind(None, None, None, Some("pod-a".to_string())).unwrap();
        assert_eq!(shutdown.status_json()["phase"], "operator_bound");
        shutdown.set_phase("draining");
        assert_eq!(shutdown.status_json()["phase"], "draining");
        shutdown.complete(None, None, Some("pod-a"), "Completed").unwrap();
        shutdown.set_phase("late_update");
        shutdown.bind(None, None, None, Some("pod-a".to_string())).unwrap();
        assert_eq!(shutdown.status_json()["phase"], "completed");
    }

    #[test]
    fn stale_coordinator_cannot_complete_a_bound_shutdown() {
        let shutdown = ShutdownCoordinator::new(3, true, Duration::from_secs(1), Duration::from_millis(10));
        shutdown.bind(None, None, None, Some("pod-a".to_string())).unwrap();
        assert!(shutdown.complete(None, None, Some("pod-b"), "Completed").is_err());
        assert!(!shutdown.status_json()["completed"].as_bool().unwrap());
    }
}
