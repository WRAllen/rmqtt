//! Handshake Connection Management System
//!
//! Provides controlled execution of MQTT handshake operations with:
//! - Port-specific worker pools
//! - Dynamic busy state detection
//! - Connection rate monitoring
//!
//! ## Core Functionality
//! 1. ​**​Executor Management​**​:
//!    - Creates dedicated TaskExecQueue per listener port
//!    - Auto-scales worker pools based on config limits
//!    - Maintains execution statistics
//!
//! 2. ​**​Busy State Detection​**​:
//!    - Dynamic threshold calculation (35% of max capacity)
//!    - Cross-port busy state aggregation
//!    - Integration with shared server state
//!
//! 3. ​**​Performance Monitoring​**​:
//!    - Per-port execution rate tracking
//!    - Active task count summation
//!    - Async-compatible metrics collection
//!
//! ## Implementation Details
//! - Uses DashMap for concurrent port mapping
//! - Lazy initialization of executor pools
//! - Tokio-based async task spawning
//! - Zero-cost deref to TaskExecQueue
//!
//! Operational Flow:
//! 1. Get executor for specific port (auto-creates if needed)
//! 2. Execute handshake tasks in isolated pool
//! 3. Monitor aggregate system state
//! 4. Trigger busy state when thresholds exceeded

use rust_box::task_exec_queue::{Builder, TaskExecQueue};
use std::ops::Deref;
use std::sync::RwLock;

use crate::context::ServerContext;
use crate::types::{DashMap, ListenerConfig, Port};

type BusyLimit = isize;

pub struct Entry {
    exec: TaskExecQueue,
    busy_limit: BusyLimit,
}

impl Deref for Entry {
    type Target = TaskExecQueue;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.exec
    }
}

pub struct HandshakeExecutor {
    handshake_execs: DashMap<Port, Entry>,
    // Published once per listener creation, never on the per-message path.
    // Monitoring must not iterate the business DashMap or call rate(), whose
    // dependency uses a blocking mutex even though its wrapper is async.
    monitoring_execs: RwLock<Vec<TaskExecQueue>>,
    busy_limit: BusyLimit,
}

impl HandshakeExecutor {
    pub fn new(busy_limit: isize) -> Self {
        Self { handshake_execs: DashMap::default(), monitoring_execs: RwLock::new(Vec::new()), busy_limit }
    }

    #[inline]
    pub fn get(&self, name: Port, listen_cfg: &ListenerConfig) -> TaskExecQueue {
        self.handshake_execs
            .entry(name)
            .or_insert_with(|| {
                let (exec, task_runner) = Builder::default()
                    .workers(listen_cfg.max_handshaking_limit)
                    .queue_max(listen_cfg.max_connections)
                    .build();

                tokio::spawn(async move {
                    task_runner.await;
                });

                let busy_limit = if self.busy_limit == 0 {
                    (listen_cfg.max_handshaking_limit as f64 * 0.35) as isize
                } else {
                    self.busy_limit
                };

                self.monitoring_execs.write().unwrap_or_else(|e| e.into_inner()).push(exec.clone());
                Entry { exec, busy_limit }
            })
            .exec
            .clone()
    }

    #[inline]
    pub fn active_count(&self) -> isize {
        self.handshake_execs.iter().map(|exec| exec.active_count()).sum()
    }

    /// Only read atomic active counters through a monitoring-only registry.
    /// A concurrent listener creation makes this sample unavailable, not zero.
    pub fn metrics_active_count(&self) -> Option<isize> {
        self.monitoring_execs.try_read().ok().map(|execs| execs.iter().map(|exec| exec.active_count()).sum())
    }

    #[inline]
    pub async fn get_rate(&self) -> f64 {
        let mut rate = 0.0;
        for exec in self.handshake_execs.iter() {
            rate += exec.rate().await;
        }
        rate
    }

    #[inline]
    pub async fn is_busy(&self, scx: &ServerContext) -> bool {
        let _is_busy = self
            .handshake_execs
            .iter()
            .filter_map(|exec| if exec.active_count() > exec.busy_limit { Some(1) } else { None })
            .sum::<u32>()
            > 0;
        _is_busy || scx.extends.shared().await.operation_is_busy()
    }
}

#[cfg(test)]
mod monitoring_tests {
    use super::*;

    #[tokio::test]
    async fn monitoring_registry_does_not_touch_business_shards_or_duplicate_listeners() {
        let executor = HandshakeExecutor::new(0);
        let config = ListenerConfig::default();
        executor.get(1883, &config);
        executor.get(1883, &config);
        executor.get(1884, &config);
        assert_eq!(executor.monitoring_execs.read().unwrap().len(), 2);
        assert_eq!(executor.metrics_active_count(), Some(executor.active_count()));

        let _business_writer = executor.handshake_execs.get_mut(&1883).unwrap();
        assert_eq!(executor.metrics_active_count(), Some(0));
        let _monitoring_writer = executor.monitoring_execs.write().unwrap();
        assert_eq!(executor.metrics_active_count(), None);
    }
}
