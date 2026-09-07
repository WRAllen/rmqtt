//! MQTT Broker Performance Monitoring
//!
//! Provides comprehensive statistical instrumentation for MQTT broker operations, implementing
//! real-time metrics collection and cluster-wide aggregation. The system combines atomic counters
//! with distributed data structures to achieve O(1) metric updates while maintaining thread safety.
//!
//! ## Core Monitoring Dimensions
//! 1. **Connection Lifecycle**:
//!    - Handshake throughput (`handshakings_rate`) and active negotiations (`handshakings_active`)
//!    - Concurrent connections tracking with load shedding detection (`connections`)
//!
//! 2. **Session Management**:
//!    - Persistent session counts with expiry tracking (`sessions`)
//!    - Shared subscription distribution analysis (`subscriptions_shared`)
//!
//! 3. **Message Flow**:
//!    - QoS 1/2 in-flight message windows (`out_inflights`/`in_inflights`)
//!    - Message queue backpressure monitoring (`message_queues`)
//!    - Retained message storage metrics (`retaineds`)
//!
//! ## Architectural Features
//! - **Cluster Awareness**:
//!   ```rust,ignore
//!   topics_map: HashMap<NodeId, Counter>  // Trie-based topic tree metrics per node
//!   routes_map: HashMap<NodeId, Counter>   // Subscription routing table metrics
//!   ```
//!   Enables cross-node aggregation through merge_topics/merge_routes operations
//!
//! - **Performance Optimizations**:
//!   - Lock-free atomic counters for high-frequency updates
//!   - Conditional compilation for production/DEBUG builds
//!   - Zero-copy serialization via Serde integration
//!
//! - **Diagnostics**:
//!   - DEBUG feature exposes:
//!     - Client state machine transitions (`debug_client_states_map`)
//!     - Topic tree memory profiling (`debug_topics_tree_map`)
//!
//! Typical usage includes:
//! - Capacity planning through trend analysis
//! - QoS compliance auditing
//! - Cluster load balancing decisions
//! - Anomaly detection via metric thresholds
//!
//! The implementation leverages Rust's type system to ensure:
//! - Memory safety through ownership model
//! - Linear scalability with cluster size
//! - Seamless integration with Prometheus/Grafana
//!

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::context::ServerContext;
// #[cfg(feature = "debug")]
use crate::context::TaskExecStats;
use crate::types::{HashMap, NodeId};
use crate::utils::Counter;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Stats {
    pub handshakings: Counter,
    pub handshakings_active: Counter,
    pub handshakings_rate: Counter,
    pub connections: Counter,
    pub sessions: Counter,
    pub subscriptions: Counter,
    pub subscriptions_shared: Counter,
    pub message_queues: Counter,
    /// Messages that were still present when their in-memory queue was
    /// destroyed. This is monotonic and must not be treated as a live gauge.
    pub message_queue_drops: Counter,
    pub out_inflights: Counter,
    /// Outbound QoS messages that were still unacknowledged when their
    /// in-flight container was destroyed.
    pub out_inflight_drops: Counter,
    pub in_inflights: Counter,
    pub forwards: Counter,
    pub message_storages: Counter,
    pub retaineds: Counter,
    pub delayed_publishs: Counter,
    pub grpc_server_actives: Counter,
    pub grpc_clients_actives: HashMap<NodeId, Counter>,

    pub topics_map: HashMap<NodeId, Counter>,
    pub routes_map: HashMap<NodeId, Counter>,

    pub execs_actives: HashMap<String, TaskExecStats>,

    #[cfg(feature = "debug")]
    debug_client_states_map: HashMap<NodeId, usize>,
    #[cfg(feature = "debug")]
    debug_topics_tree_map: HashMap<NodeId, usize>,
    #[cfg(feature = "debug")]
    debug_shared_peers: Counter,
    #[cfg(feature = "debug")]
    debug_subscriptions: usize,
    #[cfg(feature = "debug")]
    pub debug_session_channels: Counter,
    // #[cfg(feature = "debug")]
    // debug_server_exec_stats: Option<TaskExecStats>,
    // #[cfg(feature = "debug")]
    // debug_client_exec_stats: Option<TaskExecStats>,
}

impl Stats {
    #[inline]
    pub fn new() -> Self {
        Self {
            handshakings: Counter::new(),
            handshakings_active: Counter::new(),
            handshakings_rate: Counter::new(),
            connections: Counter::new(),
            sessions: Counter::new(),
            subscriptions: Counter::new(),
            subscriptions_shared: Counter::new(),
            message_queues: Counter::new(),
            message_queue_drops: Counter::new(),
            out_inflights: Counter::new(),
            out_inflight_drops: Counter::new(),
            in_inflights: Counter::new(),
            forwards: Counter::new(),
            message_storages: Counter::new(),
            retaineds: Counter::new(),
            delayed_publishs: Counter::new(),
            grpc_server_actives: Counter::new(),
            grpc_clients_actives: HashMap::default(),

            topics_map: HashMap::default(),
            routes_map: HashMap::default(),

            execs_actives: HashMap::default(),

            #[cfg(feature = "debug")]
            debug_client_states_map: HashMap::default(),
            #[cfg(feature = "debug")]
            debug_topics_tree_map: HashMap::default(),
            #[cfg(feature = "debug")]
            debug_shared_peers: Counter::new(),
            #[cfg(feature = "debug")]
            debug_subscriptions: 0,
            #[cfg(feature = "debug")]
            debug_session_channels: Counter::new(),
            // #[cfg(feature = "debug")]
            // debug_server_exec_stats: None,
            // #[cfg(feature = "debug")]
            // debug_client_exec_stats: None,
        }
    }

    /// Best-effort, read-only counters for the background metrics sampler.
    ///
    /// This deliberately does not call `clone`: that API waits for extensions,
    /// writes back derived counters, and can walk business data in debug builds.
    /// Storage extensions must opt in through their synchronous, no-I/O metrics
    /// getters; administrative count/max/len futures are never even polled.
    /// Busy/unsupported sources are omitted, not zero. Honor `omitted`.
    pub fn monitoring_snapshot(&self, scx: &ServerContext) -> (Self, Vec<String>) {
        let (stats, omitted, _) = self.monitoring_snapshot_with_routing(scx);
        (stats, omitted)
    }

    pub fn monitoring_snapshot_with_routing(
        &self,
        scx: &ServerContext,
    ) -> (Self, Vec<String>, Option<crate::router::MetricsRouteMerge>) {
        let mut snapshot = Self {
            handshakings: scx.handshakings.clone(),
            handshakings_active: Counter::new(),
            handshakings_rate: self.handshakings_rate.clone(),
            connections: scx.connections.clone(),
            sessions: scx.sessions.clone(),
            subscriptions: self.subscriptions.clone(),
            subscriptions_shared: self.subscriptions_shared.clone(),
            message_queues: self.message_queues.clone(),
            message_queue_drops: self.message_queue_drops.clone(),
            out_inflights: self.out_inflights.clone(),
            out_inflight_drops: self.out_inflight_drops.clone(),
            in_inflights: self.in_inflights.clone(),
            forwards: self.forwards.clone(),
            message_storages: self.message_storages.clone(),
            retaineds: self.retaineds.clone(),
            delayed_publishs: self.delayed_publishs.clone(),
            grpc_server_actives: self.grpc_server_actives.clone(),
            #[cfg(feature = "debug")]
            debug_session_channels: self.debug_session_channels.clone(),
            ..Self::new()
        };
        let mut omitted = Vec::new();
        let mut omit_counter = |name: &str| {
            omitted.push(format!("{name}.count"));
            omitted.push(format!("{name}.max"));
        };

        // now_or_never cannot prevent the synchronous mutex inside rate().
        // Leave live administrative/limiter semantics alone and omit this input.
        omit_counter("handshakings_rate");

        let routing = scx.extends.router.try_read().ok().and_then(|router| router.metrics_snapshot());
        let routing_merge = if let Some(routing) = routing {
            snapshot.topics_map.insert(scx.node.id, routing.topics);
            snapshot.routes_map.insert(scx.node.id, routing.routes);
            Some(routing.merge)
        } else {
            omit_counter("topics");
            omit_counter("routes");
            None
        };

        #[cfg(feature = "msgstore")]
        {
            let counter =
                scx.extends.message_mgr.try_read().ok().and_then(|manager| manager.metrics_snapshot());
            if let Some(counter) = counter {
                snapshot.message_storages = counter;
            } else {
                omit_counter("message_storages");
            }
        }
        #[cfg(not(feature = "msgstore"))]
        omit_counter("message_storages");

        #[cfg(feature = "retain")]
        {
            let counter = scx.extends.retain.try_read().ok().and_then(|retain| retain.metrics_snapshot());
            if let Some(counter) = counter {
                snapshot.retaineds = counter;
            } else {
                omit_counter("retaineds");
            }
        }
        #[cfg(not(feature = "retain"))]
        omit_counter("retaineds");

        #[cfg(feature = "delayed")]
        {
            let count = scx.extends.delayed_sender.try_read().ok().and_then(|sender| sender.metrics_len());
            if let Some(count) = count {
                snapshot.delayed_publishs.current_set(count as isize);
            } else {
                omit_counter("delayed_publishs");
            }
        }
        #[cfg(not(feature = "delayed"))]
        omit_counter("delayed_publishs");

        // Executor/debug maps are not Prometheus inputs. In particular, never
        // walk clients, subscriptions or topic trees to fill those maps here.
        if let Some(active) = scx.handshake_exec.metrics_active_count() {
            snapshot.handshakings_active.current_set(active);
        } else {
            omitted.push("handshakings_active.count".into());
        }
        (snapshot, omitted, routing_merge)
    }

    #[inline]
    pub async fn clone(&self, scx: &ServerContext) -> Self {
        let node_id = scx.node.id;
        let mut topics_map = HashMap::default();
        let mut routes_map = HashMap::default();
        {
            let router = scx.extends.router().await;
            topics_map.insert(node_id, router.topics());
            routes_map.insert(node_id, router.routes());
        }

        #[allow(unused_mut)]
        let mut grpc_clients_actives = HashMap::default();
        #[cfg(feature = "grpc")]
        {
            let shared = scx.extends.shared().await;
            for (id, (_, grpc_client)) in shared.get_grpc_clients().iter() {
                grpc_clients_actives.insert(*id, grpc_client.active_tasks().clone());
            }
        }

        //execs
        let mut execs_actives = HashMap::default();
        for (key, exec) in scx.execs() {
            execs_actives.insert(key, TaskExecStats::from_exec(&exec).await);
        }

        //@TODO
        self.handshakings.set(&scx.handshakings);
        self.handshakings_active.current_set(scx.handshake_exec.active_count());
        self.handshakings_rate.sets((scx.handshake_exec.get_rate().await * 100.0) as isize);
        self.connections.set(&scx.connections);
        self.sessions.set(&scx.sessions);

        //@TODO
        // let (curr, max) = in_inflights();
        // self.in_inflights.current_set(curr);
        // self.in_inflights.max_max(max);

        #[cfg(feature = "msgstore")]
        {
            let message_mgr = scx.extends.message_mgr().await;
            self.message_storages.current_set(message_mgr.count().await);
            self.message_storages.max_max(message_mgr.max().await);
        }

        #[cfg(feature = "retain")]
        let retaineds = {
            let retain = scx.extends.retain().await;
            Counter::new_with(retain.count().await, retain.max().await, retain.stats_merge_mode())
        };
        #[cfg(not(feature = "retain"))]
        let retaineds = Counter::default();

        #[cfg(feature = "delayed")]
        {
            let delayed_sender = scx.extends.delayed_sender().await;
            self.delayed_publishs.current_set(delayed_sender.len().await as isize);
        }

        #[cfg(feature = "debug")]
        let shared = scx.extends.shared().await;

        #[cfg(feature = "debug")]
        let mut debug_client_states_map = HashMap::default();
        #[cfg(feature = "debug")]
        let mut debug_topics_tree_map = HashMap::default();
        #[cfg(feature = "debug")]
        {
            debug_client_states_map.insert(node_id, shared.client_states_count().await);
            debug_topics_tree_map.insert(node_id, scx.extends.router().await.topics_tree().await);
        }
        #[cfg(feature = "debug")]
        self.debug_shared_peers.current_set(shared.sessions_count() as isize);
        #[cfg(feature = "debug")]
        let debug_subscriptions = shared.subscriptions_count().await;
        // #[cfg(feature = "debug")]
        // let debug_server_exec_stats = Some(TaskExecStats::from_exec(&scx.server_exec).await);
        // #[cfg(feature = "debug")]
        // let debug_client_exec_stats = Some(TaskExecStats::from_exec(&scx.client_exec).await);

        Self {
            handshakings: self.handshakings.clone(),
            handshakings_active: self.handshakings_active.clone(),
            handshakings_rate: self.handshakings_rate.clone(),
            connections: self.connections.clone(),
            sessions: self.sessions.clone(),
            subscriptions: self.subscriptions.clone(),
            subscriptions_shared: self.subscriptions_shared.clone(),
            message_queues: self.message_queues.clone(),
            message_queue_drops: self.message_queue_drops.clone(),
            out_inflights: self.out_inflights.clone(),
            out_inflight_drops: self.out_inflight_drops.clone(),
            in_inflights: self.in_inflights.clone(),
            forwards: self.forwards.clone(),
            message_storages: self.message_storages.clone(),
            delayed_publishs: self.delayed_publishs.clone(),
            grpc_server_actives: self.grpc_server_actives.clone(),
            grpc_clients_actives,

            retaineds,
            topics_map,
            routes_map,

            execs_actives,

            #[cfg(feature = "debug")]
            debug_client_states_map,
            #[cfg(feature = "debug")]
            debug_topics_tree_map,
            #[cfg(feature = "debug")]
            debug_shared_peers: self.debug_shared_peers.clone(),
            #[cfg(feature = "debug")]
            debug_subscriptions,
            #[cfg(feature = "debug")]
            debug_session_channels: self.debug_session_channels.clone(),
            // #[cfg(feature = "debug")]
            // debug_server_exec_stats,
            // #[cfg(feature = "debug")]
            // debug_client_exec_stats,
        }
    }

    #[inline]
    pub fn add(&mut self, other: Self) {
        self.handshakings.add(&other.handshakings);
        self.handshakings_active.add(&other.handshakings_active);
        self.handshakings_rate.add(&other.handshakings_rate);
        self.connections.add(&other.connections);
        self.sessions.add(&other.sessions);
        self.subscriptions.add(&other.subscriptions);
        self.subscriptions_shared.add(&other.subscriptions_shared);
        self.message_queues.add(&other.message_queues);
        self.message_queue_drops.add(&other.message_queue_drops);
        self.out_inflights.add(&other.out_inflights);
        self.out_inflight_drops.add(&other.out_inflight_drops);
        self.in_inflights.add(&other.in_inflights);
        self.forwards.add(&other.forwards);
        self.message_storages.add(&other.message_storages);
        self.retaineds.merge(&other.retaineds);
        self.delayed_publishs.merge(&other.delayed_publishs);
        self.grpc_server_actives.merge(&other.grpc_server_actives);
        self.grpc_clients_actives.extend(other.grpc_clients_actives);

        self.topics_map.extend(other.topics_map);
        self.routes_map.extend(other.routes_map);

        for (k, v) in other.execs_actives {
            self.execs_actives.entry(k).and_modify(|tes| tes.add(&v)).or_insert_with(|| v);
        }

        #[cfg(feature = "debug")]
        {
            self.debug_client_states_map.extend(other.debug_client_states_map);
            self.debug_topics_tree_map.extend(other.debug_topics_tree_map);
            self.debug_shared_peers.add(&other.debug_shared_peers);
            self.debug_subscriptions += other.debug_subscriptions;
            self.debug_session_channels.add(&other.debug_session_channels);
        }
    }

    #[allow(unused_mut)]
    #[inline]
    pub async fn to_json(&self, scx: &ServerContext) -> serde_json::Value {
        let router = scx.extends.router().await;
        let topics = router.merge_topics(&self.topics_map);
        let routes = router.merge_routes(&self.routes_map);

        let grpc_clients_actives = Counter::new();
        for (_, c) in self.grpc_clients_actives.iter() {
            grpc_clients_actives.add(c);
        }

        json!({
            "handshakings.count": self.handshakings.count(),
            "handshakings.max": self.handshakings.max(),
            "handshakings_active.count": self.handshakings_active.count(),
            "handshakings_rate.count": self.handshakings_rate.count() as f64 / 100.0,
            "handshakings_rate.max": self.handshakings_rate.max() as f64 / 100.0,
            "connections.count": self.connections.count(),
            "connections.max": self.connections.max(),
            "sessions.count": self.sessions.count(),
            "sessions.max": self.sessions.max(),
            "subscriptions.count": self.subscriptions.count(),
            "subscriptions.max": self.subscriptions.max(),
            "subscriptions_shared.count": self.subscriptions_shared.count(),
            "subscriptions_shared.max": self.subscriptions_shared.max(),
            "retaineds.count": self.retaineds.count(),
            "retaineds.max": self.retaineds.max(),

            "message_queues.count": self.message_queues.count(),
            "message_queues.max": self.message_queues.max(),
            "message_queue_drops.count": self.message_queue_drops.count(),
            "out_inflights.count": self.out_inflights.count(),
            "out_inflights.max": self.out_inflights.max(),
            "out_inflight_drops.count": self.out_inflight_drops.count(),
            "in_inflights.count": self.in_inflights.count(),
            "in_inflights.max": self.in_inflights.max(),
            "forwards.count": self.forwards.count(),
            "forwards.max": self.forwards.max(),
            "message_storages.count": self.message_storages.count(),
            "message_storages.max": self.message_storages.max(),
            "delayed_publishs.count": self.delayed_publishs.count(),
            "delayed_publishs.max": self.delayed_publishs.max(),

            "topics.count": topics.count(),
            "topics.max": topics.max(),
            "routes.count": routes.count(),
            "routes.max": routes.max(),
        })
    }

    #[allow(unused_mut)]
    #[inline]
    pub async fn to_sys_json(&self, _scx: &ServerContext) -> serde_json::Value {
        let grpc_clients_actives = Counter::new();
        for (_, c) in self.grpc_clients_actives.iter() {
            grpc_clients_actives.add(c);
        }

        let mut json_val = json!({
            "grpc_server_actives.count": self.grpc_server_actives.count(),
            "grpc_server_actives.max": self.grpc_server_actives.max(),
            "grpc_clients_actives.count": grpc_clients_actives.count(),
            "grpc_clients_actives.max": grpc_clients_actives.max(),

            "execs_actives": self.execs_actives,
        });

        #[cfg(feature = "debug")]
        {
            if let Some(obj) = json_val.as_object_mut() {
                obj.insert("debug_grpc_clients_actives".into(), json!(self.grpc_clients_actives));
                obj.insert("debug_client_states_map".into(), json!(self.debug_client_states_map));
                obj.insert("debug_topics_tree_map".into(), json!(self.debug_topics_tree_map));
                obj.insert("debug_shared_peers.count".into(), json!(self.debug_shared_peers.count()));
                obj.insert("debug_subscriptions.count".into(), json!(self.debug_subscriptions));
                obj.insert("debug_session_channels.count".into(), json!(self.debug_session_channels.count()));
                obj.insert("debug_session_channels.max".into(), json!(self.debug_session_channels.max()));
                // obj.insert("debug_server_exec_stats".into(), json!(self.debug_server_exec_stats));
                // obj.insert("debug_client_exec_stats".into(), json!(self.debug_client_exec_stats));
            }
        }

        json_val
    }
}

#[cfg(test)]
mod monitoring_tests {
    use super::*;

    #[tokio::test]
    async fn monitoring_reads_real_counters_without_writing_back() {
        let scx = ServerContext::new().busy_check_enable(false).node_id(7).build().await;
        scx.connections.sets(42);
        scx.sessions.sets(51);
        scx.handshakings.sets(3);
        scx.stats.connections.sets(999);
        scx.stats.sessions.sets(998);
        scx.stats.handshakings.sets(997);
        scx.stats.message_queues.sets(17);

        let (snapshot, _) = scx.stats.monitoring_snapshot(&scx);
        assert_eq!(snapshot.connections.count(), 42);
        assert_eq!(snapshot.sessions.count(), 51);
        assert_eq!(snapshot.handshakings.count(), 3);
        assert_eq!(snapshot.message_queues.count(), 17);
        assert_eq!(scx.stats.connections.count(), 999);
        assert_eq!(scx.stats.sessions.count(), 998);
        assert_eq!(scx.stats.handshakings.count(), 997);
        assert_eq!(snapshot.topics_map.len(), 1);
        assert!(snapshot.topics_map.contains_key(&7));
        assert!(snapshot.execs_actives.is_empty());
        #[cfg(feature = "debug")]
        {
            assert!(snapshot.debug_client_states_map.is_empty());
            assert!(snapshot.debug_topics_tree_map.is_empty());
        }
    }

    #[tokio::test]
    async fn monitoring_omits_blocking_rate_without_changing_business_rate_counters() {
        let scx = ServerContext::new().busy_check_enable(false).build().await;
        scx.stats.handshakings_rate.sets(123);
        let (_, omitted) = scx.stats.monitoring_snapshot(&scx);
        assert!(omitted.iter().any(|item| item == "handshakings_rate.count"));
        assert!(omitted.iter().any(|item| item == "handshakings_rate.max"));
        assert!(!omitted.iter().any(|item| item == "handshakings_active.count"));
        assert_eq!(scx.stats.handshakings_rate.count(), 123);
    }

    #[tokio::test]
    async fn monitoring_omits_busy_router_instead_of_waiting_or_faking_zero() {
        let scx = ServerContext::new().busy_check_enable(false).build().await;
        let _writer = scx.extends.router.write().await;
        let (snapshot, omitted) = scx.stats.monitoring_snapshot(&scx);
        assert!(snapshot.topics_map.is_empty());
        assert!(snapshot.routes_map.is_empty());
        for key in ["topics.count", "topics.max", "routes.count", "routes.max"] {
            assert!(omitted.iter().any(|item| item == key));
        }
    }

    #[cfg(feature = "msgstore")]
    #[tokio::test]
    async fn monitoring_never_polls_storage_futures() {
        struct UnsupportedStorage;

        #[async_trait::async_trait]
        impl crate::message::MessageManager for UnsupportedStorage {
            async fn count(&self) -> isize {
                panic!("monitoring must not start a live storage count query")
            }

            async fn max(&self) -> isize {
                panic!("monitoring must not start a live storage max query")
            }
        }

        let scx = ServerContext::new().busy_check_enable(false).build().await;
        *scx.extends.message_mgr.write().await = Box::new(UnsupportedStorage);
        scx.stats.message_storages.sets(777);
        for _ in 0..100 {
            let (_, omitted) = scx.stats.monitoring_snapshot(&scx);
            assert!(omitted.iter().any(|item| item == "message_storages.count"));
            assert!(omitted.iter().any(|item| item == "message_storages.max"));
            assert!(scx.extends.message_mgr.try_write().is_ok(), "must release extension guard");
            assert_eq!(scx.stats.message_storages.count(), 777);
        }
    }

    #[cfg(feature = "msgstore")]
    #[tokio::test]
    async fn monitoring_uses_opt_in_storage_snapshot_without_live_queries_or_writes() {
        struct SnapshotStorage(Counter);

        #[async_trait::async_trait]
        impl crate::message::MessageManager for SnapshotStorage {
            async fn count(&self) -> isize {
                panic!("monitoring must only read the opt-in snapshot")
            }

            async fn max(&self) -> isize {
                panic!("monitoring must only read the opt-in snapshot")
            }

            fn metrics_snapshot(&self) -> Option<Counter> {
                Some(self.0.clone())
            }
        }

        let scx = ServerContext::new().busy_check_enable(false).build().await;
        *scx.extends.message_mgr.write().await =
            Box::new(SnapshotStorage(Counter::new_with(12, 37, crate::utils::StatsMergeMode::Sum)));
        scx.stats.message_storages.sets(777);
        let (snapshot, omitted) = scx.stats.monitoring_snapshot(&scx);
        assert_eq!(snapshot.message_storages.count(), 12);
        assert_eq!(snapshot.message_storages.max(), 37);
        assert!(!omitted.iter().any(|item| item.starts_with("message_storages.")));
        assert_eq!(scx.stats.message_storages.count(), 777);
    }

    #[cfg(feature = "retain")]
    #[tokio::test]
    async fn monitoring_never_polls_retained_storage_futures() {
        struct UnsupportedRetain;
        #[async_trait::async_trait]
        impl crate::retain::RetainStorage for UnsupportedRetain {
            async fn set(
                &self,
                _: &crate::types::TopicName,
                _: crate::types::Retain,
                _: Option<std::time::Duration>,
            ) -> crate::Result<()> {
                panic!("monitoring must not mutate retained messages")
            }
            async fn get(
                &self,
                _: &crate::types::TopicFilter,
            ) -> crate::Result<Vec<(crate::types::TopicName, crate::types::Retain)>> {
                panic!("monitoring must not scan retained messages")
            }
            async fn count(&self) -> isize {
                panic!("monitoring must not start a retained-storage count query")
            }
            async fn max(&self) -> isize {
                panic!("monitoring must not start a retained-storage max query")
            }
        }
        let scx = ServerContext::new().busy_check_enable(false).build().await;
        *scx.extends.retain.write().await = Box::new(UnsupportedRetain);
        let (_, omitted) = scx.stats.monitoring_snapshot(&scx);
        assert!(omitted.iter().any(|item| item == "retaineds.count"));
        assert!(omitted.iter().any(|item| item == "retaineds.max"));
    }

    #[cfg(feature = "delayed")]
    #[tokio::test]
    async fn monitoring_never_polls_delayed_len_future() {
        struct UnsupportedDelayed;
        #[async_trait::async_trait]
        impl crate::delayed::DelayedSender for UnsupportedDelayed {
            fn parse(&self, _: crate::types::Publish) -> crate::Result<crate::types::Publish> {
                panic!("monitoring must not enter the publish path")
            }
            async fn delay_publish(
                &self,
                _: crate::types::From,
                _: crate::types::Publish,
                _: bool,
                _: Option<std::time::Duration>,
            ) -> crate::Result<Option<(crate::types::From, crate::types::Publish)>> {
                panic!("monitoring must not enter the publish path")
            }
            async fn len(&self) -> usize {
                panic!("monitoring must not poll the delayed length query")
            }
        }
        let scx = ServerContext::new().busy_check_enable(false).build().await;
        *scx.extends.delayed_sender.write().await = Box::new(UnsupportedDelayed);
        let (_, omitted) = scx.stats.monitoring_snapshot(&scx);
        assert!(omitted.iter().any(|item| item == "delayed_publishs.count"));
        assert!(omitted.iter().any(|item| item == "delayed_publishs.max"));
    }
}
