//! In-process regressions for the monitoring cache. These are not a broker load
//! test: no TCP sockets, cluster mutations, or Prometheus server are involved.

use super::*;
use rmqtt::node::NodeStatus;
use rmqtt::utils::{Counter, StatsMergeMode};

fn sample(id: NodeId) -> LocalSnapshot {
    let now = timestamp_millis();
    LocalSnapshot {
        node_id: id,
        started_at_millis: now - 10_000,
        sampled_at_millis: now,
        node: NodeInfo { node_id: id, node_status: NodeStatus::Running(1), ..Default::default() },
        stats: Stats::default(),
        metrics: Metrics::default(),
        omitted_stats: Vec::new(),
        node_sampled_at_millis: Some(now),
        node_running_available: true,
        routing_merge: Some(rmqtt::router::MetricsRouteMerge::Replicated),
    }
}

fn cached(text: &'static str) -> Cached {
    Cached { bytes: Bytes::from_static(text.as_bytes()), expires: Instant::now() + MAX_STALE }
}

#[tokio::test]
async fn rendering_uses_captured_router_policy_even_while_live_router_is_write_locked() {
    use rmqtt::router::{MetricsRouteMerge, Router};
    let scx = ServerContext::new().busy_check_enable(false).build().await;
    let router = rmqtt::router::DefaultRouter::new(None);
    router.topics_count.sets(10);
    router.relations_count.sets(20);
    let saved = router.metrics_snapshot().unwrap();
    *scx.extends.router.write().await = Box::new(router.clone());
    let (stats, omitted_stats, routing_merge) = scx.stats.monitoring_snapshot_with_routing(&scx);
    assert_eq!(routing_merge, Some(MetricsRouteMerge::Replicated));
    let mut snapshot = sample(scx.node.id());
    snapshot.stats = stats;
    snapshot.omitted_stats = omitted_stats;
    snapshot.routing_merge = routing_merge;

    let _writer = scx.extends.router.write().await;
    // Updating live counters must not affect the already captured values.
    router.topics_count.sets(999);
    let renderer = Renderer::new().unwrap();
    renderer.snapshot("1", &snapshot);
    assert_eq!(metric(&renderer, "rmqtt_stats", "1", "topics.count"), Some(saved.topics.count() as f64));
    assert_eq!(metric(&renderer, "rmqtt_stats", "1", "routes.count"), Some(20.0));
}

#[test]
fn broadcast_policy_sums_and_mixed_or_unknown_policies_omit_only_routing() {
    use rmqtt::router::MetricsRouteMerge;
    let make = |id, policy| {
        let mut s = sample(id);
        s.routing_merge = policy;
        s.stats.connections.sets(7);
        s.stats.topics_map.insert(id, Counter::new_with(10, 20, StatsMergeMode::None));
        s.stats.routes_map.insert(id, Counter::new_with(30, 40, StatsMergeMode::None));
        s
    };
    let sum = aggregate(vec![make(1, Some(MetricsRouteMerge::Sum)), make(2, Some(MetricsRouteMerge::Sum))]);
    let renderer = Renderer::new().unwrap();
    renderer.snapshot("all", &sum);
    for (item, value) in
        [("topics.count", 20.0), ("topics.max", 40.0), ("routes.count", 60.0), ("routes.max", 80.0)]
    {
        assert_eq!(metric(&renderer, "rmqtt_stats", "all", item), Some(value));
    }
    for other in [None, Some(MetricsRouteMerge::Replicated)] {
        let sum = aggregate(vec![make(1, Some(MetricsRouteMerge::Sum)), make(2, other)]);
        let renderer = Renderer::new().unwrap();
        renderer.snapshot("all", &sum);
        for item in ["topics.count", "topics.max", "routes.count", "routes.max"] {
            assert!(sum.omitted_stats.iter().any(|omitted| omitted == item));
            assert_eq!(metric(&renderer, "rmqtt_stats", "all", item), None);
        }
        assert_eq!(metric(&renderer, "rmqtt_stats", "all", "connections.count"), Some(14.0));
    }
}

#[test]
fn v2_wire_preserves_router_policy_and_rejects_v1_payload() {
    use rmqtt::router::MetricsRouteMerge;
    for policy in [None, Some(MetricsRouteMerge::Replicated), Some(MetricsRouteMerge::Sum)] {
        let mut snapshot = sample(1);
        snapshot.routing_merge = policy;
        let mut bytes = bincode::serialize(&snapshot).unwrap();
        assert_eq!(decode_snapshot(&bytes, 1).unwrap().routing_merge, policy);
        // V1 ended before the newly appended policy field.
        bytes.truncate(bytes.len() - if policy.is_some() { 5 } else { 1 });
        assert!(decode_snapshot(&bytes, 1).is_err());
    }
}

#[test]
fn refresh_interval_enforces_only_a_minimum_and_preserves_low_frequency_configuration() {
    assert_eq!(refresh_interval(Duration::from_secs(5)), Duration::from_secs(30));
    assert_eq!(refresh_interval(Duration::from_secs(60)), Duration::from_secs(60));
    assert_eq!(refresh_interval(Duration::from_secs(300)), Duration::from_secs(300));
}

fn ready_monitor() -> Monitor {
    let monitor = Monitor::empty(1, 17, Duration::from_secs(5));
    {
        let mut published = monitor.inner.published.write().unwrap();
        published.local_view = Some(cached("local-node"));
        published.local_wire = Some(cached("local-wire"));
        published.views = Arc::new(Views {
            nodes: [(2, cached("remote-node"))].into_iter().collect(),
            all: Some(cached("all-nodes")),
            sum: Some(cached("sum-nodes")),
        });
    }
    monitor
}

fn process_sample(id: NodeId, started: i64, connections: isize) -> LocalSnapshot {
    let mut snapshot = sample(id);
    snapshot.started_at_millis = started;
    snapshot.stats.connections.sets(connections);
    snapshot
}

fn published_sum(monitor: &Monitor) -> Cached {
    monitor.inner.published.read().unwrap().views.sum.clone().unwrap()
}

#[tokio::test]
async fn partial_round_keeps_exact_complete_sum_without_extending_its_expiry() {
    let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
    let monitor = Monitor::empty(1, 99, MIN_REFRESH);
    let expected = BTreeSet::from([1, 2]);
    let mut identities = BTreeMap::new();
    let started = timestamp_millis() - 10_000;
    assert!(publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 10), process_sample(2, started, 20)],
        Vec::new()
    )
    .unwrap()
    .is_none());
    let previous = published_sum(&monitor);

    assert!(publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 999)],
        vec!["node 2 timed out".into()]
    )
    .unwrap()
    .is_some());
    let retained = published_sum(&monitor);
    assert_eq!(retained.bytes.as_ptr(), previous.bytes.as_ptr());
    assert_eq!(retained.bytes, previous.bytes);
    assert_eq!(retained.expires, previous.expires, "partial data must never renew complete data");
    let response = monitor.read(PrometheusDataType::Sum).unwrap();
    assert_eq!(response.body, previous.bytes);
    let metadata = std::str::from_utf8(&response.metadata).unwrap();
    assert!(metadata.contains("rmqtt_metrics_refresh_success 0\n"));
    assert!(metadata.contains("rmqtt_metrics_missing_nodes 1\n"));
}

#[tokio::test]
async fn first_partial_round_publishes_nodes_without_a_fake_complete_sum() {
    let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
    let monitor = Monitor::empty(1, 99, MIN_REFRESH);
    let expected = BTreeSet::from([1, 2, 3]);
    let mut identities = BTreeMap::new();
    let started = timestamp_millis() - 10_000;
    assert!(publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 10), process_sample(2, started, 20)],
        Vec::new()
    )
    .unwrap()
    .is_some());
    assert!(monitor.read(PrometheusDataType::Sum).is_err());
    assert!(monitor.read(PrometheusDataType::All).is_err());
    let node = monitor.read(PrometheusDataType::Node(2)).unwrap();
    assert!(std::str::from_utf8(&node.body).unwrap().contains("item=\"connections.count\",node=\"2\"} 20"));
    assert!(std::str::from_utf8(&node.metadata).unwrap().contains("rmqtt_metrics_missing_nodes 1\n"));
}

#[tokio::test]
async fn restarted_node_in_partial_round_invalidates_pre_restart_aggregate() {
    let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
    let monitor = Monitor::empty(1, 99, MIN_REFRESH);
    let expected = BTreeSet::from([1, 2, 3]);
    let mut identities = BTreeMap::new();
    let started = timestamp_millis() - 10_000;
    publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 10), process_sample(2, started, 20), process_sample(3, started, 30)],
        Vec::new(),
    )
    .unwrap();
    assert!(monitor.read(PrometheusDataType::Sum).is_ok());

    publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 11), process_sample(2, started + 5_000, 1)],
        vec!["node 3 unavailable".into()],
    )
    .unwrap();
    assert!(monitor.read(PrometheusDataType::Sum).is_err());
    assert!(monitor.read(PrometheusDataType::All).is_err());
    assert!(monitor.read(PrometheusDataType::Node(2)).is_ok());
    assert_eq!(identities[&2], started + 5_000);
    // Repeating the partial round must not resurrect the old aggregate.
    publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 12), process_sample(2, started + 5_000, 2)],
        Vec::new(),
    )
    .unwrap();
    assert!(monitor.read(PrometheusDataType::Sum).is_err());
}

#[tokio::test]
async fn roster_change_removes_departed_node_views_identity_and_backoff() {
    let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
    let monitor = Monitor::empty(1, 99, MIN_REFRESH);
    let mut expected = BTreeSet::from([1, 2, 3]);
    let mut identities = BTreeMap::new();
    let started = timestamp_millis() - 10_000;
    publish_cluster(
        &monitor.inner,
        &scx,
        &expected,
        &mut identities,
        vec![process_sample(1, started, 10), process_sample(2, started, 20), process_sample(3, started, 30)],
        Vec::new(),
    )
    .unwrap();
    let mut cooldowns = BTreeMap::from([(3, (Instant::now() + MIN_REFRESH, 1))]);
    update_roster(&monitor.inner, &mut expected, BTreeSet::from([1, 2]), &mut identities, &mut cooldowns);
    assert_eq!(expected, BTreeSet::from([1, 2]));
    assert!(!identities.contains_key(&3));
    assert!(!cooldowns.contains_key(&3));
    assert!(monitor.read(PrometheusDataType::Node(3)).is_err());
    assert!(monitor.read(PrometheusDataType::Node(2)).is_ok());
    assert!(monitor.read(PrometheusDataType::Sum).is_err());
    assert!(monitor.read(PrometheusDataType::All).is_err());
}

fn oversized_candidate(mut views: Views) -> Views {
    // Share one allocation between two logical views; the cache budget counts
    // both encoded outputs, without allocating more than the test requires.
    let bytes = Bytes::from(vec![b'x'; MAX_CACHE_BYTES / 2 + 1]);
    let large = Cached { bytes, expires: Instant::now() + MAX_STALE };
    views.sum = Some(large.clone());
    views.all = Some(large);
    views
}

#[test]
fn oversized_publication_preserves_previous_complete_cache_and_metadata() {
    let monitor = ready_monitor();
    let (previous, metadata) = {
        let state = monitor.inner.published.read().unwrap();
        (state.views.clone(), state.metadata.clone())
    };
    let error = publish_views(&monitor.inner, oversized_candidate((*previous).clone()), 2, 0).unwrap_err();
    assert!(error.to_string().contains("metrics cache exceeds"));
    let state = monitor.inner.published.read().unwrap();
    assert!(Arc::ptr_eq(&state.views, &previous));
    assert_eq!(state.metadata, metadata);
    assert_eq!(state.views.sum.as_ref().unwrap().bytes.as_ref(), b"sum-nodes");
}

#[test]
fn oversized_publication_cannot_restore_a_known_restarted_nodes_old_sum() {
    let monitor = ready_monitor();
    let started = timestamp_millis() - 10_000;
    let mut identities = BTreeMap::from([(1, started), (2, started)]);
    let invalidated = invalidate_snapshot_identities(
        &monitor.inner,
        &mut identities,
        &[process_sample(2, started + 5_000, 1)],
    );
    assert!(invalidated.sum.is_none());
    assert!(invalidated.all.is_none());
    assert!(!invalidated.nodes.contains_key(&2));
    let invalidated_state = monitor.inner.published.read().unwrap().views.clone();
    assert!(publish_views(&monitor.inner, oversized_candidate(invalidated), 2, 1).is_err());
    let state = monitor.inner.published.read().unwrap();
    assert!(Arc::ptr_eq(&state.views, &invalidated_state));
    assert!(state.views.sum.is_none(), "failed new data must not resurrect invalid old totals");
    assert!(state.views.all.is_none());
    assert!(!state.views.nodes.contains_key(&2));
    assert_eq!(identities[&2], started + 5_000);
}

#[tokio::test]
async fn response_admission_lives_until_body_drop_and_is_bounded() {
    let monitor = ready_monitor();
    let mut bodies = Vec::new();
    for _ in 0..MAX_RESPONSES {
        bodies.push(Box::pin(monitor.read(PrometheusDataType::Sum).unwrap().into_stream()));
    }
    assert!(monitor.read(PrometheusDataType::Sum).is_err());
    // Polling only the first chunk still holds the permit for slow consumers.
    assert_eq!(&bodies[0].next().await.unwrap().unwrap()[..], b"sum-nodes");
    assert!(monitor.read(PrometheusDataType::All).is_err());
    bodies.pop();
    assert!(monitor.read(PrometheusDataType::All).is_ok());
    bodies.clear();
    assert_eq!(monitor.inner.responses.available_permits(), MAX_RESPONSES);
}

#[tokio::test]
async fn transport_buffered_bytes_keep_the_response_permit_alive() {
    let monitor = ready_monitor();
    let mut stream = Box::pin(monitor.read(PrometheusDataType::Sum).unwrap().into_stream());
    let body = stream.next().await.unwrap().unwrap();
    let metadata = stream.next().await.unwrap().unwrap();
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(monitor.inner.responses.available_permits(), MAX_RESPONSES - 1);
    let slice = body.slice(0..1);
    drop(body);
    drop(metadata);
    assert_eq!(monitor.inner.responses.available_permits(), MAX_RESPONSES - 1);
    drop(slice);
    assert_eq!(monitor.inner.responses.available_permits(), MAX_RESPONSES);
}

#[test]
fn system_source_age_also_limits_rendered_cache_lifetime() {
    let mut snapshot = sample(1);
    snapshot.node_sampled_at_millis = Some(timestamp_millis() - 110_000);
    let remaining = snapshot_expiry(&snapshot).unwrap().duration_since(Instant::now());
    assert!(remaining <= Duration::from_secs(10));
    assert!(remaining > Duration::from_secs(5));
    // Already expired OS fields are omitted when rendering; cheap counters
    // still get their own full lifetime instead of failing all local metrics.
    snapshot.node_sampled_at_millis = Some(timestamp_millis() - 121_000);
    assert!(snapshot_expiry(&snapshot).unwrap().duration_since(Instant::now()) > Duration::from_secs(110));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedicated_worker_populates_all_views_and_stops_without_joining_business_workers() {
    let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
    scx.connections.sets(23);
    let monitor = Monitor::new(scx.clone(), 99, MIN_REFRESH).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let reply = to_metrics(&scx, monitor.clone(), 99, MIN_REFRESH, PrometheusDataType::Sum).await;
        if let Ok(reply) = reply {
            assert!(std::str::from_utf8(&reply.body)
                .unwrap()
                .contains("item=\"connections.count\",node=\"all\"} 23"));
            drop(reply);
            break;
        }
        if tokio::time::Instant::now() > deadline {
            monitor.shutdown();
            panic!("background cache did not initialize");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(monitor.read(PrometheusDataType::All).is_ok());
    assert!(monitor.read(PrometheusDataType::Node(1)).is_ok());
    assert!(monitor.local_reply().is_ok());
    monitor.shutdown();
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&monitor.inner) > 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(monitor.read(PrometheusDataType::Sum).is_err());
}

fn metric(renderer: &Renderer, family: &str, node: &str, item: &str) -> Option<f64> {
    renderer.reg.gather().into_iter().filter(|value| value.get_name() == family).find_map(|family| {
        family.get_metric().iter().find_map(|value| {
            let has_node =
                value.get_label().iter().any(|label| label.get_name() == "node" && label.get_value() == node);
            let has_item =
                value.get_label().iter().any(|label| label.get_name() == "item" && label.get_value() == item);
            (has_node && has_item).then(|| value.get_gauge().get_value())
        })
    })
}

#[tokio::test]
async fn aggregate_preserves_counter_sums_replicated_router_merge_and_omissions() {
    let mut first = sample(1);
    first.stats.connections.sets(7);
    first.stats.sessions.sets(11);
    first.stats.topics_map.insert(1, Counter::new_with(100, 120, StatsMergeMode::None));
    first.stats.routes_map.insert(1, Counter::new_with(70, 80, StatsMergeMode::None));
    first.stats.retaineds = Counter::new_with(3, 7, StatsMergeMode::Max);
    first.node.memory_total = 100;
    first.omitted_stats = vec!["message_storages.count".into(), "retaineds.count".into()];
    for _ in 0..2 {
        first.metrics.messages_delivered_inc();
    }

    let mut second = sample(2);
    second.sampled_at_millis -= 1_000;
    second.node_sampled_at_millis = Some(second.sampled_at_millis);
    second.stats.connections.sets(13);
    second.stats.sessions.sets(17);
    second.stats.topics_map.insert(2, Counter::new_with(80, 160, StatsMergeMode::None));
    second.stats.routes_map.insert(2, Counter::new_with(60, 100, StatsMergeMode::None));
    second.stats.retaineds = Counter::new_with(5, 9, StatsMergeMode::Max);
    second.node.memory_total = 200;
    second.omitted_stats = vec!["message_storages.count".into()];
    for _ in 0..3 {
        second.metrics.messages_delivered_inc();
    }
    let oldest_sample = second.sampled_at_millis;
    let aggregate = aggregate(vec![first, second]);
    assert_eq!(aggregate.stats.connections.count(), 20);
    assert_eq!(aggregate.stats.sessions.count(), 28);
    assert_eq!(aggregate.stats.topics_map.len(), 2);
    assert_eq!(aggregate.stats.routes_map.len(), 2);
    assert_eq!(aggregate.metrics.messages_delivered(), 5);
    assert_eq!(aggregate.sampled_at_millis, oldest_sample);
    assert_eq!(aggregate.omitted_stats, ["message_storages.count", "retaineds.count"]);

    let renderer = Renderer::new().unwrap();
    renderer.snapshot("all", &aggregate);
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "connections.count"), Some(20.0));
    // Replicated routing counters use min(current)/max(max), not sum.
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "topics.count"), Some(80.0));
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "topics.max"), Some(160.0));
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "routes.count"), Some(60.0));
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "routes.max"), Some(100.0));
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "retaineds.max"), Some(9.0));
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "retaineds.count"), None);
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "message_storages.count"), None);
    assert_eq!(metric(&renderer, "rmqtt_nodes", "all", "memory_total"), Some(300.0));
    assert_eq!(metric(&renderer, "rmqtt_metrics", "all", "messages.delivered"), Some(5.0));
}

#[tokio::test]
async fn missing_or_expired_system_sample_does_not_publish_fake_zero_or_running() {
    let first = sample(1);
    let mut second = sample(2);
    second.node_sampled_at_millis = None;
    second.node_running_available = false;
    let sum = aggregate(vec![first, second]);
    assert_eq!(sum.node_sampled_at_millis, None);
    assert!(!sum.node_running_available);
    let renderer = Renderer::new().unwrap();
    renderer.snapshot("all", &sum);
    assert_eq!(metric(&renderer, "rmqtt_nodes", "all", "memory_total"), None);
    assert_eq!(metric(&renderer, "rmqtt_nodes", "all", "running"), None);
    assert_eq!(metric(&renderer, "rmqtt_stats", "all", "connections.count"), Some(0.0));

    let mut stale = sample(1);
    stale.node_sampled_at_millis = Some(timestamp_millis() - MAX_STALE.as_millis() as i64 - 1);
    let renderer = Renderer::new().unwrap();
    renderer.snapshot("1", &stale);
    assert_eq!(metric(&renderer, "rmqtt_nodes", "1", "load1"), None);
    assert!(metric(&renderer, "rmqtt_metrics_snapshot", "1", "system_sampled_at_seconds").is_some());
}

#[test]
fn wire_rejects_expired_future_oversize_and_mismatched_snapshots() {
    let valid = sample(7);
    let bytes = bincode::serialize(&valid).unwrap();
    assert_eq!(decode_snapshot(&bytes, 7).unwrap().node_id, 7);
    assert!(decode_snapshot(&bytes, 8).is_err());

    let mut wrong_node = sample(7);
    wrong_node.node.node_id = 8;
    assert!(decode_snapshot(&bincode::serialize(&wrong_node).unwrap(), 7).is_err());

    for timestamp in [0, timestamp_millis() - MAX_STALE.as_millis() as i64 - 1, timestamp_millis() + 60_000] {
        let mut invalid = sample(7);
        invalid.sampled_at_millis = timestamp;
        invalid.started_at_millis = timestamp - 1_000;
        assert!(decode_snapshot(&bincode::serialize(&invalid).unwrap(), 7).is_err());
    }
    for started in [0, valid.sampled_at_millis + 60_000] {
        let mut invalid = sample(7);
        invalid.started_at_millis = started;
        assert!(decode_snapshot(&bincode::serialize(&invalid).unwrap(), 7).is_err());
    }

    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode_snapshot(&trailing, 7).is_err());
    assert!(decode_snapshot(&vec![0; MAX_WIRE_BYTES + 1], 7).is_err());
    let mut too_large = sample(7);
    too_large.node.node_name = "x".repeat(MAX_WIRE_BYTES);
    assert!(decode_snapshot(&bincode::serialize(&too_large).unwrap(), 7).is_err());
}

#[test]
fn cache_replacement_does_not_mutate_previously_returned_bytes() {
    let monitor = ready_monitor();
    let old = monitor.read(PrometheusDataType::Sum).unwrap();
    let old_pointer = old.body.as_ptr();
    {
        let mut published = monitor.inner.published.write().unwrap();
        let mut replacement = (*published.views).clone();
        replacement.sum = Some(cached("new-sum"));
        published.views = Arc::new(replacement);
        published.metadata = Bytes::from_static(b"new-metadata");
    }
    let new = monitor.read(PrometheusDataType::Sum).unwrap();
    assert_eq!(&old.body[..], b"sum-nodes");
    assert_eq!(old.body.as_ptr(), old_pointer);
    assert_eq!(&new.body[..], b"new-sum");
    assert_ne!(old.metadata, new.metadata);
}

#[test]
fn busy_cache_returns_errors_without_waiting_for_writer() {
    let monitor = ready_monitor();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer = {
        let monitor = monitor.clone();
        std::thread::spawn(move || {
            let _guard = monitor.inner.published.write().unwrap();
            ready_tx.send(()).unwrap();
            // Bound even a regressed blocking read: do not hang the test suite.
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
        })
    };
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let started = Instant::now();
    for typ in [
        PrometheusDataType::Sum,
        PrometheusDataType::All,
        PrometheusDataType::Node(1),
        PrometheusDataType::Node(2),
    ] {
        assert_eq!(monitor.read(typ).err().unwrap().to_string(), "metrics cache busy");
    }
    assert_eq!(monitor.local_reply().err().unwrap().to_string(), "metrics cache busy");
    assert!(started.elapsed() < Duration::from_secs(1));
    release_tx.send(()).unwrap();
    writer.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_views_leave_two_worker_runtime_heartbeat_running() {
    let monitor = ready_monitor();
    let scx = ServerContext::new().busy_check_enable(false).build().await;
    let beats = Arc::new(AtomicU64::new(0));
    let stopped = Arc::new(AtomicBool::new(false));
    let heartbeat = {
        let beats = beats.clone();
        let stopped = stopped.clone();
        tokio::spawn(async move {
            while !stopped.load(Ordering::Acquire) {
                beats.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };
    let mut readers = Vec::new();
    for reader in 0..64 {
        let monitor = monitor.clone();
        let scx = scx.clone();
        readers.push(tokio::spawn(async move {
            for iteration in 0..128 {
                let typ = match (reader + iteration) % 4 {
                    0 => PrometheusDataType::Sum,
                    1 => PrometheusDataType::All,
                    2 => PrometheusDataType::Node(1),
                    _ => PrometheusDataType::Node(2),
                };
                let response =
                    to_metrics(&scx, monitor.clone(), 17, Duration::from_secs(5), typ).await.unwrap();
                assert!(!response.body.is_empty());
                drop(response);
                tokio::task::yield_now().await;
            }
        }));
    }
    for reader in readers {
        reader.await.unwrap();
    }
    stopped.store(true, Ordering::Release);
    heartbeat.await.unwrap();
    assert!(beats.load(Ordering::Relaxed) > 1, "metrics reads must leave worker time for other tasks");
    assert_eq!(monitor.inner.published.read().unwrap().views.nodes.len(), 1);
    assert!(monitor.inner.requested.load(Ordering::Acquire));
}

#[tokio::test]
async fn unknown_ids_do_not_create_cache_entries_or_refresh_queues() {
    let scx = ServerContext::new().busy_check_enable(false).build().await;
    let monitor = ready_monitor();
    let original = monitor.inner.published.read().unwrap().views.clone();
    for id in 100..1_100 {
        let result = futures::FutureExt::now_or_never(to_metrics(
            &scx,
            monitor.clone(),
            17,
            Duration::from_secs(5),
            PrometheusDataType::Node(id),
        ))
        .expect("unknown-node lookup must complete on its first poll");
        // The API handler maps this cache miss directly to HTTP 503. There is
        // no inline network request, awaited refresh, or cache key insertion.
        assert!(result.is_err());
    }
    let published = monitor.inner.published.read().unwrap();
    assert!(Arc::ptr_eq(&published.views, &original));
    assert_eq!(published.views.nodes.len(), 1);
    assert!(monitor.inner.requested.load(Ordering::Acquire));
    assert_eq!(monitor.inner.refresh_seconds.load(Ordering::Relaxed), 30);
}

#[test]
fn empty_expired_and_stopped_cache_never_returns_success() {
    let empty = Monitor::empty(1, 17, Duration::from_secs(30));
    assert!(empty.read(PrometheusDataType::Sum).is_err());
    assert!(empty.local_reply().is_err());
    let expired =
        Cached { bytes: Bytes::from_static(b"expired"), expires: Instant::now() - Duration::from_secs(1) };
    assert!(expired.get().is_err());
    let monitor = ready_monitor();
    monitor.shutdown();
    assert!(monitor.read(PrometheusDataType::All).is_err());
    assert!(monitor.local_reply().is_err());
}
