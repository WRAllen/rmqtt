//! Bounded, read-only local inputs for the metrics cache.
//!
//! This module never performs health/membership requests and never calls the
//! administrative `Stats::clone`/`Node::node_info` collection paths.

use std::time::{Duration, Instant};

use rmqtt::context::ServerContext;
use rmqtt::metrics::Metrics;
use rmqtt::node::{to_uptime, Node, NodeInfo};
use rmqtt::router::MetricsRouteMerge;
use rmqtt::stats::Stats;
use rmqtt::types::NodeId;
use rmqtt::utils::timestamp_millis;
use serde::{Deserialize, Serialize};
use systemstat::Platform;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

pub(crate) const LOCAL_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const SYSTEM_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

// A permit belongs to the actual blocking operation, not its waiter. Even if a
// collector/plugin is restarted while a filesystem call is stuck, no replacement
// worker is started until that operation really finishes.
static SYSTEM_WORKER: Semaphore = Semaphore::const_new(1);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LocalSnapshot {
    pub(crate) node_id: NodeId,
    pub(crate) started_at_millis: i64,
    pub(crate) sampled_at_millis: i64,
    pub(crate) node: NodeInfo,
    #[serde(with = "wire_stats")]
    pub(crate) stats: Stats,
    pub(crate) metrics: Metrics,
    /// Exact rmqtt_stats item labels which were not sampled successfully.
    pub(crate) omitted_stats: Vec<String>,
    /// None means load/memory/disk fields in `node` must not be exported.
    pub(crate) node_sampled_at_millis: Option<i64>,
    /// False means `node.node_status` is a placeholder, not a health result.
    pub(crate) node_running_available: bool,
    /// Captured with the counters; rendering never calls the live router.
    pub(crate) routing_merge: Option<MetricsRouteMerge>,
}

// Keep the V1 wire layout independent of core Stats' debug feature and its
// administrative/executor fields. Append a new protocol version if this schema
// needs to change; do not serialize the evolving core Stats structure directly.
mod wire_stats {
    use rmqtt::stats::Stats;
    use rmqtt::types::{HashMap, NodeId};
    use rmqtt::utils::Counter;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct WireStats {
        handshakings: Counter,
        handshakings_active: Counter,
        handshakings_rate: Counter,
        connections: Counter,
        sessions: Counter,
        subscriptions: Counter,
        subscriptions_shared: Counter,
        message_queues: Counter,
        out_inflights: Counter,
        in_inflights: Counter,
        forwards: Counter,
        message_storages: Counter,
        retaineds: Counter,
        delayed_publishs: Counter,
        topics_map: HashMap<NodeId, Counter>,
        routes_map: HashMap<NodeId, Counter>,
    }

    pub(super) fn serialize<S: Serializer>(stats: &Stats, serializer: S) -> Result<S::Ok, S::Error> {
        WireStats {
            handshakings: stats.handshakings.clone(),
            handshakings_active: stats.handshakings_active.clone(),
            handshakings_rate: stats.handshakings_rate.clone(),
            connections: stats.connections.clone(),
            sessions: stats.sessions.clone(),
            subscriptions: stats.subscriptions.clone(),
            subscriptions_shared: stats.subscriptions_shared.clone(),
            message_queues: stats.message_queues.clone(),
            out_inflights: stats.out_inflights.clone(),
            in_inflights: stats.in_inflights.clone(),
            forwards: stats.forwards.clone(),
            message_storages: stats.message_storages.clone(),
            retaineds: stats.retaineds.clone(),
            delayed_publishs: stats.delayed_publishs.clone(),
            topics_map: stats.topics_map.clone(),
            routes_map: stats.routes_map.clone(),
        }
        .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Stats, D::Error> {
        let wire = WireStats::deserialize(deserializer)?;
        // Assignment keeps private debug-only fields out of this plugin's ABI.
        let mut stats = Stats::new();
        stats.handshakings = wire.handshakings;
        stats.handshakings_active = wire.handshakings_active;
        stats.handshakings_rate = wire.handshakings_rate;
        stats.connections = wire.connections;
        stats.sessions = wire.sessions;
        stats.subscriptions = wire.subscriptions;
        stats.subscriptions_shared = wire.subscriptions_shared;
        stats.message_queues = wire.message_queues;
        stats.out_inflights = wire.out_inflights;
        stats.in_inflights = wire.in_inflights;
        stats.forwards = wire.forwards;
        stats.message_storages = wire.message_storages;
        stats.retaineds = wire.retaineds;
        stats.delayed_publishs = wire.delayed_publishs;
        stats.topics_map = wire.topics_map;
        stats.routes_map = wire.routes_map;
        Ok(stats)
    }
}

pub(crate) struct Sampler {
    scx: ServerContext,
    system: SystemSampler,
}

impl Sampler {
    pub(crate) fn new(scx: ServerContext) -> Self {
        Self { scx, system: SystemSampler::default() }
    }

    /// Call once per LOCAL_SAMPLE_INTERVAL from the single monitoring worker.
    /// Async business locks are never awaited and OS syscalls run separately.
    /// Storage extensions must explicitly provide a nonblocking cached getter;
    /// their live async count/max/len operations are never invoked here.
    pub(crate) async fn sample(&mut self) -> anyhow::Result<LocalSnapshot> {
        self.system.poll().await;
        let (stats, omitted_stats, routing_merge) =
            self.scx.stats.monitoring_snapshot_with_routing(&self.scx);
        let now = timestamp_millis();
        let started_at_millis = self.scx.node.start_time.timestamp_millis();
        let (mut node, node_sampled_at_millis) = match &self.system.cached {
            Some((node, sampled)) => (node.clone(), Some(*sampled)),
            None => (NodeInfo::default(), None),
        };
        node.node_id = self.scx.node.id;
        node.connections = self.scx.connections.count();
        node.uptime = to_uptime((now - started_at_millis).max(0) / 1000);
        node.version = Node::version();
        node.rustc_version = Node::rustc_version();
        Ok(LocalSnapshot {
            node_id: self.scx.node.id,
            started_at_millis,
            sampled_at_millis: now,
            node,
            stats,
            metrics: self.scx.metrics.clone(),
            omitted_stats,
            node_sampled_at_millis,
            node_running_available: false,
            routing_merge,
        })
    }
}

#[derive(Default)]
struct SystemSampler {
    in_flight: Option<JoinHandle<anyhow::Result<(NodeInfo, i64)>>>,
    last_attempt: Option<Instant>,
    cached: Option<(NodeInfo, i64)>,
}

impl SystemSampler {
    async fn poll(&mut self) {
        if self.in_flight.as_ref().is_some_and(JoinHandle::is_finished) {
            if let Some(handle) = self.in_flight.take() {
                match handle.await {
                    Ok(Ok(sample)) => self.cached = Some(sample),
                    Ok(Err(error)) => log::debug!("metrics system sample unavailable: {error}"),
                    Err(error) => log::debug!("metrics system sampler failed: {error}"),
                }
            }
        }
        if self.in_flight.is_some()
            || self.last_attempt.is_some_and(|at| at.elapsed() < SYSTEM_SAMPLE_INTERVAL)
        {
            return;
        }
        self.last_attempt = Some(Instant::now());
        if let Ok(permit) = SYSTEM_WORKER.try_acquire() {
            self.in_flight = Some(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                collect_system()
            }));
        }
    }
}

fn collect_system() -> anyhow::Result<(NodeInfo, i64)> {
    let system = systemstat::System::new();
    let load = system.load_average()?;
    let memory = system.memory()?;
    let mounts = system.mounts()?;
    let node = NodeInfo {
        boottime: system.boot_time().map(|time| time.to_string()).unwrap_or_default(),
        load1: load.one,
        load5: load.five,
        load15: load.fifteen,
        memory_total: memory.total.as_u64(),
        memory_free: memory.free.as_u64(),
        memory_used: systemstat::saturating_sub_bytes(memory.total, memory.free).as_u64(),
        disk_total: mounts.iter().map(|mount| mount.total.as_u64()).sum(),
        disk_free: mounts.iter().map(|mount| mount.free.as_u64()).sum(),
        ..NodeInfo::default()
    };
    Ok((node, timestamp_millis()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct StatsWire(#[serde(with = "wire_stats")] Stats);

    #[test]
    fn v1_stats_wire_has_fixed_layout_without_administrative_fields() {
        let mut stats = Stats::new();
        stats.grpc_server_actives.sets(123);
        stats.message_queue_drops.sets(456);
        stats
            .grpc_clients_actives
            .insert(7, rmqtt::utils::Counter::new_with(9, 10, rmqtt::utils::StatsMergeMode::Max));
        // Fourteen (i64 current, i64 maximum, u32 merge mode) counters,
        // then two empty maps (u64 lengths). This stays identical with debug.
        assert_eq!(bincode::serialize(&StatsWire(stats)).unwrap(), vec![0; 296]);
    }

    #[test]
    fn v1_stats_wire_preserves_rendered_counters_and_merge_modes() {
        use rmqtt::utils::{Counter, StatsMergeMode};

        let mut stats = Stats::new();
        stats.connections.sets(42);
        stats.retaineds = Counter::new_with(3, 9, StatsMergeMode::Max);
        stats.topics_map.insert(7, Counter::new_with(11, 21, StatsMergeMode::Min));
        stats.routes_map.insert(7, Counter::new_with(13, 23, StatsMergeMode::Min));
        let bytes = bincode::serialize(&StatsWire(stats)).unwrap();
        let StatsWire(decoded) = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.connections.count(), 42);
        assert_eq!(decoded.retaineds.count(), 3);
        assert_eq!(decoded.retaineds.max(), 9);
        decoded.retaineds.merge(&Counter::new_with(2, 5, StatsMergeMode::Max));
        assert_eq!(decoded.retaineds.count(), 3, "retain merge must remain max, not sum");
        assert_eq!(decoded.topics_map[&7].count(), 11);
        assert_eq!(decoded.routes_map[&7].max(), 23);
        assert!(decoded.grpc_clients_actives.is_empty());
        assert!(decoded.execs_actives.is_empty());
    }

    #[tokio::test]
    async fn pending_system_worker_does_not_delay_poll_or_start_replacement() {
        let (complete, pending) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = pending.await;
            Ok((NodeInfo::default(), 123))
        });
        let mut sampler = SystemSampler { in_flight: Some(task), ..Default::default() };
        for _ in 0..100 {
            sampler.poll().await;
            assert!(sampler.cached.is_none());
            assert!(sampler.last_attempt.is_none(), "must not enqueue another OS operation");
        }
        complete.send(()).unwrap();
        while !sampler.in_flight.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        // Suppress a new real OS sample so the test is platform-independent.
        sampler.last_attempt = Some(Instant::now());
        sampler.poll().await;
        assert_eq!(sampler.cached.as_ref().unwrap().1, 123);
        assert!(sampler.in_flight.is_none());
    }

    #[tokio::test]
    async fn failed_system_sample_preserves_last_success() {
        let task = tokio::spawn(async { Err(anyhow::anyhow!("system source unavailable")) });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let mut sampler = SystemSampler {
            in_flight: Some(task),
            last_attempt: Some(Instant::now()),
            cached: Some((NodeInfo::default(), 456)),
        };
        sampler.poll().await;
        assert_eq!(sampler.cached.as_ref().unwrap().1, 456);
        assert!(sampler.in_flight.is_none());
    }
}
