//! Best-effort monitoring. HTTP/gRPC only read immutable bytes; one dedicated
//! worker does bounded sampling and cluster collection, never on MQTT workers.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::{
    snapshot::{LocalSnapshot, Sampler, LOCAL_SAMPLE_INTERVAL},
    types::{Message, MessageReply, PrometheusDataType},
};
use anyhow::{anyhow, bail, ensure};
use bincode::Options;
use bytes::Bytes;
use futures::{stream, StreamExt};
use prometheus::{
    register_gauge_vec_with_registry, register_int_gauge_vec_with_registry, Encoder, GaugeVec, IntGaugeVec,
    Registry, TextEncoder,
};
use rmqtt::{
    context::ServerContext,
    grpc::{GrpcClient, Message as GrpcMessage, MessageReply as GrpcMessageReply, MessageType},
    metrics::Metrics,
    node::NodeInfo,
    stats::Stats,
    types::NodeId,
    utils::timestamp_millis,
    Result,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

pub(crate) const PROME_MONITOR: &str = "PROME_MONITOR";
const MIN_REFRESH: Duration = Duration::from_secs(30);
const MAX_STALE: Duration = Duration::from_secs(120);
const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const ROUND_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_NODES: usize = 128;
const MAX_WIRE_BYTES: usize = 256 * 1024;
const MAX_CACHE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RESPONSES: usize = 32;

#[derive(Clone)]
struct Cached {
    bytes: Bytes,
    expires: Instant,
}
impl Cached {
    fn get(&self) -> Result<Bytes> {
        ensure!(Instant::now() < self.expires, "metrics snapshot expired");
        Ok(self.bytes.clone())
    }
}
#[derive(Clone, Default)]
struct Views {
    nodes: BTreeMap<NodeId, Cached>,
    all: Option<Cached>,
    sum: Option<Cached>,
}
struct Published {
    local_wire: Option<Cached>,
    local_view: Option<Cached>,
    views: Arc<Views>,
    metadata: Bytes,
}
struct Inner {
    published: RwLock<Published>,
    requested: AtomicBool,
    stopped: AtomicBool,
    message_type: AtomicU64,
    refresh_seconds: AtomicU64,
    wake: Notify,
    local_id: NodeId,
    responses: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct Monitor {
    inner: Arc<Inner>,
}
pub(crate) struct Response {
    pub body: Bytes,
    pub metadata: Bytes,
    permit: OwnedSemaphorePermit,
}

// Keep admission alive even after the HTTP implementation has polled a body
// chunk into its transport buffers. Cloning/slicing Bytes keeps its owner alive.
struct ResponseBytes {
    bytes: Bytes,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for ResponseBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl Response {
    /// Keep admission attached to the HTTP body, not just the handler call.
    /// Slow consumers can retain at most MAX_RESPONSES old snapshot references.
    pub(crate) fn into_stream(self) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send {
        let Self { body, metadata, permit } = self;
        let permit = Arc::new(permit);
        let body = Bytes::from_owner(ResponseBytes { bytes: body, _permit: permit.clone() });
        let metadata = Bytes::from_owner(ResponseBytes { bytes: metadata, _permit: permit.clone() });
        stream::unfold(
            (Some(body), Some(metadata), Some(permit), Instant::now()),
            |(mut body, mut metadata, mut permit, started)| async move {
                if started.elapsed() > Duration::from_secs(10) {
                    return None;
                }
                let bytes = body.take().or_else(|| metadata.take());
                if bytes.is_none() {
                    permit.take();
                }
                bytes.map(|bytes| (Ok(bytes), (body, metadata, permit, started)))
            },
        )
    }
}

impl Monitor {
    pub fn new(scx: ServerContext, message_type: MessageType, interval: Duration) -> Result<Self> {
        let monitor = Self::empty(scx.node.id(), message_type, interval);
        let inner = monitor.inner.clone();
        std::thread::Builder::new().name("rmqtt-metrics".into()).spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .max_blocking_threads(1)
                    .thread_name("rmqtt-metrics-os")
                    .build()?;
                runtime.block_on(run(scx, inner.clone()));
                // Optional blocked OS syscalls must not block plugin shutdown.
                runtime.shutdown_background();
                Ok(())
            }));
            inner.stopped.store(true, Ordering::Release);
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => log::warn!("metrics collector stopped: {error:#}"),
                Err(_) => log::warn!("metrics collector panicked; no replacement worker spawned"),
            }
        })?;
        Ok(monitor)
    }
    fn empty(local_id: NodeId, message_type: MessageType, interval: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                published: RwLock::new(Published {
                    local_wire: None,
                    local_view: None,
                    views: Arc::new(Views::default()),
                    metadata: refresh_metadata(false, 1, 1),
                }),
                requested: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                message_type: AtomicU64::new(message_type),
                refresh_seconds: AtomicU64::new(refresh_interval(interval).as_secs()),
                wake: Notify::new(),
                local_id,
                responses: Arc::new(Semaphore::new(MAX_RESPONSES)),
            }),
        }
    }
    pub(crate) fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.wake.notify_one();
    }
    /// Pre-encoded MessageReply. Never collect or encode in the receiving hook.
    pub(crate) fn local_reply(&self) -> Result<Bytes> {
        ensure!(!self.inner.stopped.load(Ordering::Acquire), "metrics collector stopped");
        let state = self.inner.published.try_read().map_err(|_| anyhow!("metrics cache busy"))?;
        state.local_wire.as_ref().ok_or_else(|| anyhow!("local metrics not initialized"))?.get()
    }
    fn read(&self, typ: PrometheusDataType) -> Result<Response> {
        ensure!(!self.inner.stopped.load(Ordering::Acquire), "metrics collector stopped");
        let permit = self
            .inner
            .responses
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("too many outstanding metrics responses"))?;
        let state = self.inner.published.try_read().map_err(|_| anyhow!("metrics cache busy"))?;
        let cached = match typ {
            PrometheusDataType::Node(id) if id == self.inner.local_id => state.local_view.as_ref(),
            PrometheusDataType::Node(id) => state.views.nodes.get(&id),
            PrometheusDataType::All => state.views.all.as_ref(),
            PrometheusDataType::Sum => state.views.sum.as_ref(),
        }
        .ok_or_else(|| anyhow!("metrics not initialized or node unknown; refresh is asynchronous"))?;
        Ok(Response { body: cached.get()?, metadata: state.metadata.clone(), permit })
    }
}

// Compatibility wrapper for the existing handlers; there is no awaited work.
pub async fn to_metrics(
    _scx: &ServerContext,
    monitor: Monitor,
    message_type: MessageType,
    cache_interval: Duration,
    typ: PrometheusDataType,
) -> Result<Response> {
    monitor.inner.message_type.store(message_type, Ordering::Relaxed);
    monitor.inner.refresh_seconds.store(refresh_interval(cache_interval).as_secs(), Ordering::Relaxed);
    if typ != PrometheusDataType::Node(monitor.inner.local_id)
        && !monitor.inner.requested.swap(true, Ordering::AcqRel)
    {
        monitor.inner.wake.notify_one();
    }
    monitor.read(typ)
}

#[cfg(test)]
#[path = "prome_tests.rs"]
mod tests;
fn refresh_interval(configured: Duration) -> Duration {
    // Includes deployments explicitly setting the former 5s default.
    configured.max(MIN_REFRESH)
}
fn refresh_metadata(success: bool, expected: usize, missing: usize) -> Bytes {
    format!("# HELP rmqtt_metrics_refresh_success Latest cluster refresh succeeded.\n# TYPE rmqtt_metrics_refresh_success gauge\nrmqtt_metrics_refresh_success {}\n# HELP rmqtt_metrics_expected_nodes Registered monitoring targets.\n# TYPE rmqtt_metrics_expected_nodes gauge\nrmqtt_metrics_expected_nodes {expected}\n# HELP rmqtt_metrics_missing_nodes Missing targets in latest cluster refresh.\n# TYPE rmqtt_metrics_missing_nodes gauge\nrmqtt_metrics_missing_nodes {missing}\n", u8::from(success)).into()
}
fn expiry(sampled_at: i64) -> Result<Instant> {
    let age = timestamp_millis().saturating_sub(sampled_at);
    ensure!(
        sampled_at > 0 && age >= -5_000 && age < MAX_STALE.as_millis() as i64,
        "invalid or stale metrics sample timestamp"
    );
    Ok(Instant::now() + MAX_STALE - Duration::from_millis(age.max(0) as u64))
}
fn snapshot_expiry(snapshot: &LocalSnapshot) -> Result<Instant> {
    let expires = expiry(snapshot.sampled_at_millis)?;
    // System metrics that are still exported must not outlive their own source
    // timestamp just because the cheap counters have been sampled again.
    Ok(snapshot
        .node_sampled_at_millis
        .and_then(|at| expiry(at).ok())
        .map_or(expires, |system| expires.min(system)))
}
fn decode_snapshot(bytes: &[u8], id: NodeId) -> Result<LocalSnapshot> {
    ensure!(bytes.len() <= MAX_WIRE_BYTES, "metrics snapshot exceeds byte limit");
    let snapshot: LocalSnapshot = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_WIRE_BYTES as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)?;
    ensure!(snapshot.node_id == id && snapshot.node.node_id == id, "metrics node identity mismatch");
    ensure!(
        snapshot.started_at_millis > 0 && snapshot.started_at_millis <= snapshot.sampled_at_millis,
        "invalid metrics process identity"
    );
    expiry(snapshot.sampled_at_millis)?;
    Ok(snapshot)
}
async fn fetch_snapshot(id: NodeId, addr: String, message_type: MessageType) -> Result<LocalSnapshot> {
    let reply = GrpcClient::request_once(
        &addr,
        message_type,
        GrpcMessage::Data(Message::MetricsSnapshotV2.encode()?),
        RPC_TIMEOUT,
    )
    .await?;
    match reply {
        GrpcMessageReply::Data(bytes) => {
            ensure!(bytes.len() <= MAX_WIRE_BYTES + 32, "metrics reply exceeds byte limit");
            let reply: MessageReply = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit((MAX_WIRE_BYTES + 32) as u64)
                .reject_trailing_bytes()
                .deserialize(&bytes)?;
            match reply {
                MessageReply::MetricsSnapshotV2(snapshot) => decode_snapshot(&snapshot, id),
                _ => bail!("peer does not support metrics snapshots"),
            }
        }
        GrpcMessageReply::Error(error) => bail!("metrics peer {id}: {error}"),
        _ => bail!("unexpected metrics reply"),
    }
}
async fn run(scx: ServerContext, inner: Arc<Inner>) {
    let mut sampler = Sampler::new(scx.clone());
    let mut next_local = Instant::now();
    let mut next_cluster = Instant::now();
    let mut expected = BTreeSet::new();
    let mut identities = BTreeMap::new();
    let mut cooldowns = BTreeMap::<NodeId, (Instant, u32)>::new();
    let mut last_warning: Option<Instant> = None;
    loop {
        if inner.stopped.load(Ordering::Acquire) {
            break;
        }
        if Instant::now() >= next_local {
            if let Err(error) = sampler.sample().await.and_then(|s| publish_local(&scx, &inner, s)) {
                warn_limited(&mut last_warning, &error);
            }
            next_local = Instant::now() + LOCAL_SAMPLE_INTERVAL;
        }
        if inner.requested.load(Ordering::Acquire) && Instant::now() >= next_cluster {
            inner.requested.store(false, Ordering::Release);
            match refresh_cluster(&scx, &inner, &mut expected, &mut identities, &mut cooldowns).await {
                Ok(Some(error)) => warn_limited(&mut last_warning, &error),
                Ok(None) => {}
                Err(error) => {
                    warn_limited(&mut last_warning, &error);
                    let mut published = inner.published.write().unwrap_or_else(|e| e.into_inner());
                    published.metadata =
                        refresh_metadata(false, expected.len().max(1), expected.len().max(1));
                }
            }
            next_cluster = Instant::now()
                .checked_add(Duration::from_secs(inner.refresh_seconds.load(Ordering::Relaxed)))
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
        }
        let next =
            if inner.requested.load(Ordering::Acquire) { next_local.min(next_cluster) } else { next_local };
        tokio::select! {
            _ = inner.wake.notified() => {},
            _ = tokio::time::sleep_until(next.into()) => {},
        }
    }
}
fn warn_limited(last: &mut Option<Instant>, error: &anyhow::Error) {
    if last.is_none_or(|at| at.elapsed() >= Duration::from_secs(60)) {
        log::warn!("metrics refresh incomplete; cached data may be stale: {error:#}");
        *last = Some(Instant::now());
    }
}
fn publish_local(_scx: &ServerContext, inner: &Inner, snapshot: LocalSnapshot) -> Result<()> {
    let expires = snapshot_expiry(&snapshot)?;
    let renderer = Renderer::new()?;
    renderer.snapshot(&snapshot.node_id.to_string(), &snapshot);
    let view = renderer.encode()?;
    let bytes = bincode::serialize(&snapshot)?;
    ensure!(bytes.len() <= MAX_WIRE_BYTES, "local metrics snapshot exceeds byte limit");
    let wire = MessageReply::MetricsSnapshotV2(bytes).encode()?;
    let mut state = inner.published.write().unwrap_or_else(|e| e.into_inner());
    state.local_wire = Some(Cached { bytes: wire.into(), expires: expiry(snapshot.sampled_at_millis)? });
    state.local_view = Some(Cached { bytes: view, expires });
    Ok(())
}
async fn refresh_cluster(
    scx: &ServerContext,
    inner: &Inner,
    expected: &mut BTreeSet<NodeId>,
    identities: &mut BTreeMap<NodeId, i64>,
    cooldowns: &mut BTreeMap<NodeId, (Instant, u32)>,
) -> Result<Option<anyhow::Error>> {
    // A busy roster is not an empty/singleton cluster.
    let mut addrs = scx
        .extends
        .shared
        .try_read()
        .map_err(|_| anyhow!("monitoring roster busy"))?
        .metrics_node_addrs()
        .ok_or_else(|| anyhow!("monitoring roster unavailable"))?;
    addrs.remove(&inner.local_id);
    ensure!(addrs.len() < MAX_NODES, "metrics target limit exceeded ({MAX_NODES})");
    let current: BTreeSet<_> = addrs.keys().copied().chain([inner.local_id]).collect();
    update_roster(inner, expected, current, identities, cooldowns);
    let local = {
        let state = inner.published.read().unwrap_or_else(|e| e.into_inner());
        state.local_wire.as_ref().ok_or_else(|| anyhow!("local metrics not initialized"))?.get()?
    };
    let local = match MessageReply::decode(&local)? {
        MessageReply::MetricsSnapshotV2(bytes) => decode_snapshot(&bytes, inner.local_id)?,
        _ => bail!("invalid local metrics cache"),
    };
    let mut snapshots = vec![local];
    let now = Instant::now();
    let mut targets: Vec<_> = addrs
        .into_iter()
        .filter(|(id, _)| cooldowns.get(id).is_none_or(|(until, _)| now >= *until))
        .collect();
    targets.sort_by_key(|(id, _)| *id);
    let message_type = inner.message_type.load(Ordering::Relaxed);
    let mut requests = stream::iter(
        targets
            .into_iter()
            .map(|(id, addr)| async move { (id, fetch_snapshot(id, addr.to_string(), message_type).await) }),
    )
    .buffer_unordered(2);
    let deadline = tokio::time::Instant::now() + ROUND_TIMEOUT;
    let mut errors = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, requests.next()).await {
            Ok(Some((id, Ok(snapshot)))) => {
                cooldowns.remove(&id);
                snapshots.push(snapshot);
            }
            Ok(Some((id, Err(error)))) => {
                let failures = cooldowns.get(&id).map_or(1, |(_, n)| n.saturating_add(1));
                let backoff = Duration::from_secs((30u64.saturating_mul(1 << failures.min(3))).min(300));
                cooldowns.insert(id, (Instant::now() + backoff, failures));
                errors.push(format!("node {id}: {error:#}"));
            }
            Ok(None) => break,
            Err(_) => {
                errors.push("cluster metrics deadline exceeded".into());
                break;
            }
        }
    }
    drop(requests); // Unary calls never create a business reconnect queue.
    publish_cluster(inner, scx, expected, identities, snapshots, errors)
}

fn update_roster(
    inner: &Inner,
    expected: &mut BTreeSet<NodeId>,
    current: BTreeSet<NodeId>,
    identities: &mut BTreeMap<NodeId, i64>,
    cooldowns: &mut BTreeMap<NodeId, (Instant, u32)>,
) {
    if *expected == current {
        return;
    }
    let mut views = (*inner.published.read().unwrap_or_else(|e| e.into_inner()).views).clone();
    views.all = None;
    views.sum = None;
    views.nodes.retain(|id, _| current.contains(id));
    identities.retain(|id, _| current.contains(id));
    cooldowns.retain(|id, _| current.contains(id));
    *expected = current;
    inner.published.write().unwrap_or_else(|e| e.into_inner()).views = Arc::new(views);
}

fn invalidate_snapshot_identities(
    inner: &Inner,
    identities: &mut BTreeMap<NodeId, i64>,
    snapshots: &[LocalSnapshot],
) -> Views {
    let mut views = (*inner.published.read().unwrap_or_else(|e| e.into_inner()).views).clone();
    let mut invalidated = false;
    for snapshot in snapshots {
        if identities
            .insert(snapshot.node_id, snapshot.started_at_millis)
            .is_some_and(|previous| previous != snapshot.started_at_millis)
        {
            views.all = None;
            views.sum = None;
            views.nodes.remove(&snapshot.node_id);
            invalidated = true;
        }
    }
    if invalidated {
        // Invalidation survives any later encoding/size error. This pass must
        // precede all rendering so no unvalidated new node views leak through.
        inner.published.write().unwrap_or_else(|e| e.into_inner()).views = Arc::new(views.clone());
    }
    views
}

/// Commit a collection round independently of the transport that produced it.
/// A partial round refreshes node views but never renews the old aggregate TTL.
fn publish_cluster(
    inner: &Inner,
    _scx: &ServerContext,
    expected: &BTreeSet<NodeId>,
    identities: &mut BTreeMap<NodeId, i64>,
    mut snapshots: Vec<LocalSnapshot>,
    errors: Vec<String>,
) -> Result<Option<anyhow::Error>> {
    snapshots.sort_by_key(|s| s.node_id);
    let mut views = invalidate_snapshot_identities(inner, identities, &snapshots);
    for snapshot in &snapshots {
        let renderer = Renderer::new()?;
        renderer.snapshot(&snapshot.node_id.to_string(), snapshot);
        views.nodes.insert(
            snapshot.node_id,
            Cached { bytes: renderer.encode()?, expires: snapshot_expiry(snapshot)? },
        );
    }
    views.nodes.retain(|_, value| Instant::now() < value.expires);
    let missing = expected.len().saturating_sub(snapshots.len());
    if missing == 0 {
        let expires = snapshots
            .iter()
            .map(snapshot_expiry)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .min()
            .ok_or_else(|| anyhow!("no metrics samples"))?;
        let all = Renderer::new()?;
        for snapshot in &snapshots {
            all.snapshot(&snapshot.node_id.to_string(), snapshot);
        }
        let sum = aggregate(snapshots);
        all.snapshot("all", &sum);
        let only_sum = Renderer::new()?;
        only_sum.snapshot("all", &sum);
        views.all = Some(Cached { bytes: all.encode()?, expires });
        views.sum = Some(Cached { bytes: only_sum.encode()?, expires });
    }
    publish_views(inner, views, expected.len(), missing)?;
    Ok((missing > 0).then(|| anyhow!("missing {missing}/{} nodes: {}", expected.len(), errors.join("; "))))
}

fn publish_views(inner: &Inner, views: Views, expected: usize, missing: usize) -> Result<()> {
    let size = views
        .nodes
        .values()
        .chain(views.all.iter())
        .chain(views.sum.iter())
        .map(|v| v.bytes.len())
        .sum::<usize>();
    ensure!(size <= MAX_CACHE_BYTES, "metrics cache exceeds {MAX_CACHE_BYTES} bytes");
    {
        let mut published = inner.published.write().unwrap_or_else(|e| e.into_inner());
        published.views = Arc::new(views);
        published.metadata = refresh_metadata(missing == 0, expected, missing);
    }
    Ok(())
}

fn aggregate(snapshots: Vec<LocalSnapshot>) -> LocalSnapshot {
    // Mixed/unknown policies must not create an apparently valid aggregate.
    let routing_merge = snapshots
        .first()
        .and_then(|s| s.routing_merge)
        .filter(|mode| snapshots.iter().all(|s| s.routing_merge == Some(*mode)));
    let mut sum = LocalSnapshot {
        node_id: 0,
        started_at_millis: 0,
        sampled_at_millis: i64::MAX,
        node: NodeInfo::default(),
        stats: Stats::default(),
        metrics: Metrics::default(),
        omitted_stats: vec![],
        node_sampled_at_millis: Some(i64::MAX),
        node_running_available: true,
        routing_merge,
    };
    let mut omitted = BTreeSet::new();
    if routing_merge.is_none() {
        omitted.extend(["topics.count", "topics.max", "routes.count", "routes.max"].map(str::to_owned));
    }
    let mut stats: Option<Stats> = None;
    for snapshot in snapshots {
        sum.sampled_at_millis = sum.sampled_at_millis.min(snapshot.sampled_at_millis);
        sum.node_sampled_at_millis =
            sum.node_sampled_at_millis.zip(snapshot.node_sampled_at_millis).map(|(a, b)| a.min(b));
        sum.node_running_available &= snapshot.node_running_available;
        sum.node.add(&snapshot.node);
        // Counter::merge uses the destination's merge mode. Starting with
        // Stats::default() (None) would silently zero retained/delayed counters.
        if let Some(stats) = &mut stats {
            stats.add(snapshot.stats);
        } else {
            stats = Some(snapshot.stats);
        }
        sum.metrics.add(&snapshot.metrics);
        omitted.extend(snapshot.omitted_stats);
    }
    sum.omitted_stats = omitted.into_iter().collect();
    sum.stats = stats.unwrap_or_default();
    sum
}
struct Renderer {
    reg: Registry,
    nodes: GaugeVec,
    stats: IntGaugeVec,
    metrics: IntGaugeVec,
    samples: GaugeVec,
}
impl Renderer {
    fn new() -> Result<Self> {
        let reg = Registry::new();
        let nodes = register_gauge_vec_with_registry!("rmqtt_nodes", "nodes status", &["node", "item"], reg)?;
        let stats =
            register_int_gauge_vec_with_registry!("rmqtt_stats", "status data", &["node", "item"], reg)?;
        let metrics =
            register_int_gauge_vec_with_registry!("rmqtt_metrics", "metrics data", &["node", "item"], reg)?;
        let samples = register_gauge_vec_with_registry!(
            "rmqtt_metrics_snapshot",
            "Monitoring sample metadata",
            &["node", "item"],
            reg
        )?;
        Ok(Self { reg, nodes, stats, metrics, samples })
    }
    fn encode(&self) -> Result<Bytes> {
        let mut bytes = Vec::new();
        TextEncoder::new().encode(&self.reg.gather(), &mut bytes)?;
        ensure!(bytes.len() <= MAX_CACHE_BYTES, "encoded metrics exceed byte limit");
        Ok(bytes.into())
    }
    fn snapshot(&self, label: &str, s: &LocalSnapshot) {
        self.samples
            .with_label_values(&[label, "sampled_at_seconds"])
            .set(s.sampled_at_millis as f64 / 1000.0);
        self.samples.with_label_values(&[label, "omitted_stats"]).set(s.omitted_stats.len() as f64);
        if let Some(at) = s.node_sampled_at_millis {
            self.samples.with_label_values(&[label, "system_sampled_at_seconds"]).set(at as f64 / 1000.0);
            if expiry(at).is_ok() {
                let node = &s.node;
                for (key, value) in [
                    ("load1", node.load1 as f64),
                    ("load5", node.load5 as f64),
                    ("load15", node.load15 as f64),
                    ("memory_total", node.memory_total as f64),
                    ("memory_used", node.memory_used as f64),
                    ("memory_free", node.memory_free as f64),
                    ("disk_total", node.disk_total as f64),
                    ("disk_free", node.disk_free as f64),
                ] {
                    self.nodes.with_label_values(&[label, key]).set(value);
                }
            }
        }
        if s.node_running_available {
            self.nodes.with_label_values(&[label, "running"]).set(s.node.node_status.running() as f64);
        }
        let stats = &s.stats;
        for (name, counter) in [
            ("handshakings", &stats.handshakings),
            ("handshakings_rate", &stats.handshakings_rate),
            ("connections", &stats.connections),
            ("sessions", &stats.sessions),
            ("subscriptions", &stats.subscriptions),
            ("subscriptions_shared", &stats.subscriptions_shared),
            ("retaineds", &stats.retaineds),
            ("message_queues", &stats.message_queues),
            ("out_inflights", &stats.out_inflights),
            ("in_inflights", &stats.in_inflights),
            ("forwards", &stats.forwards),
            ("message_storages", &stats.message_storages),
            ("delayed_publishs", &stats.delayed_publishs),
        ] {
            for (suffix, value) in [("count", counter.count()), ("max", counter.max())] {
                self.stat(label, &format!("{name}.{suffix}"), value as i64, &s.omitted_stats);
            }
        }
        self.stat(
            label,
            "handshakings_active.count",
            stats.handshakings_active.count() as i64,
            &s.omitted_stats,
        );
        // All inputs, including semantics, belong to the captured snapshot.
        // No business router lock or callback during encoding or aggregation.
        if let Some(merge) = s.routing_merge {
            for (name, counter) in
                [("topics", merge.merge(&stats.topics_map)), ("routes", merge.merge(&stats.routes_map))]
            {
                self.stat(label, &format!("{name}.count"), counter.count() as i64, &s.omitted_stats);
                self.stat(label, &format!("{name}.max"), counter.max() as i64, &s.omitted_stats);
            }
        }
        s.metrics.build_prometheus_metrics(label, &self.metrics);
    }
    fn stat(&self, label: &str, key: &str, value: i64, omitted: &[String]) {
        if !omitted.iter().any(|k| k == key) {
            self.stats.with_label_values(&[label, key]).set(value);
        }
    }
}
