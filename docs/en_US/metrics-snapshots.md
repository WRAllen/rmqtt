English | [简体中文](../zh_CN/metrics-snapshots.md)

# Prometheus Metrics Snapshots

The Prometheus endpoints in `rmqtt-http-api` serve snapshots prepared in the background. HTTP scrapes and peer snapshot requests read pre-encoded caches; they do not sample, render metrics, or wait for cluster collection during the request. Snapshots provide read-only monitoring and may be temporarily unavailable. They do not participate in MQTT message processing, Raft membership management, or cluster recovery decisions.

## Endpoints and caches

The existing endpoint paths and `node` labels are retained:

| GET path | Response |
| --- | --- |
| `/api/v1/metrics/prometheus/sum` | Complete cluster aggregate with `node="all"` |
| `/api/v1/metrics/prometheus` | Per-node metrics and the complete cluster aggregate |
| `/api/v1/metrics/prometheus/{id}` | Available snapshot for the requested node; requesting the local ID reads the local snapshot directly |

Cluster and remote-node requests only signal a coalesced background refresh and immediately read the current cache. Increasing scrape frequency does not create a collector task for each request. Local snapshots can remain available independently of cluster refreshes.

An uninitialized or expired cache, an unavailable requested node, a temporarily busy cache, or a stopped collector worker results in HTTP `503 Service Unavailable`. An initial scrape may trigger asynchronous initialization, so metrics may be absent for the first one or two scrapes after startup. This is not a guaranteed warm-up duration: mixed versions or failed nodes can prolong the gap.

## Sampling intervals and resource limits

Configure the cluster refresh interval in `plugins/rmqtt-http-api.toml`:

```toml
# Default: 30 seconds; minimum: 30 seconds; recommended: 30 to 60 seconds.
prometheus_metrics_cache_interval = "30s"
```

Existing values below 30 seconds, including `"5s"`, are treated as 30 seconds. Longer configured intervals are honored rather than increasing collection frequency. Intervals exceeding 120 seconds may leave aggregate caches expired (503) between rounds. This setting does not change the local or operating-system sampling intervals below. Requests trigger cluster refreshes; the configured interval must elapse after a refresh finishes before another round starts. Without requests, the collector does not actively poll the cluster.

| Item | Behavior or limit |
| --- | --- |
| Local business-counter snapshot | Sampled approximately every 10 seconds |
| OS load, memory, and disk | Attempted at most once every 60 seconds in a separate blocking task |
| Cluster collection | One dedicated worker per monitor, with at most 2 concurrent remote requests |
| Remote requests | 1-second unary RPC timeout and a 3-second budget for the remote collection round; failed peers use retry backoff |
| Registered monitoring targets | At most 128 nodes, including the local node |
| Per-node wire snapshot | At most 256 KiB of serialized payload, plus a small reply envelope |
| Rendered cluster cache | At most 8 MiB total across per-node, all-node, and aggregate views |
| Served snapshot age | At most 120 seconds from the source sampling time |
| Outstanding metrics response bodies | At most 32; stop sending subsequent chunks after 10 seconds |

The 8 MiB limit applies to rendered cluster caches, not total RMQTT process memory or peak allocations. Exceeding target, size, or time budgets can fail a refresh; existing caches remain subject to their original expiry.

Admission stays attached to each HTTP body until consumption or cancellation, bounding old snapshots retained by slow clients. Under transport backpressure the body might not be polled again, so the 10-second check cannot actively terminate the connection; the 32-permit cap still bounds retained response bodies. Excess scrapes fail quickly with 503.

An unfinished OS call does not cause replacement calls to be queued. Its concurrency permit remains attached to the actual blocking operation until that operation finishes. If the monitoring worker exits or panics, replacement workers are not spawned automatically, and subsequent metrics requests report unavailability.

## Failed nodes and incomplete refreshes

Monitoring targets come from registered monitoring addresses. Live health or membership queries do not remove failed nodes from that set. The aggregate and all-node views are updated only when the current round obtains valid snapshots from every expected node. A partial sum is not exposed as a complete cluster aggregate.

After an incomplete refresh, a previous complete view can still be served until its oldest source snapshot reaches 120 seconds of age. Neither failed refreshes nor repeated scrapes extend that deadline. Without a previous complete view, or after it expires, the aggregate and all-node endpoints return 503. Available per-node views can still be read until they expire. A detected change in the monitoring target set or a node's process start time invalidates old complete views until a new complete set is collected.

A complete refresh means every expected node supplied a valid snapshot. It does not mean every optional field is available or that the cluster or MQTT workload is healthy. HTTP 200 may also represent a previous complete view that has not yet expired.

## Missing fields and freshness

Sampling only tries extension locks without waiting. Busy or unavailable statistics sources can cause individual `rmqtt_stats` fields to be omitted. If any node omits a field, the aggregate omits that field too, so an incomplete value is not presented as a total. Dashboards should not interpret missing fields as zero by default.

Handshake active counts read existing atomic counters through a monitoring-only list updated at listener creation, without iterating the business DashMap. `handshakings_rate.count/max` are omitted: the dependency's rate getter uses a synchronous mutex, which `now_or_never` cannot make nonblocking. Normal handshake scheduling, admission, and administrative rate queries are unchanged.

The Raft monitoring roster is maintained with node registrations, address changes, and removals. Sampling only tries the monitoring copy, never the business node or gRPC client maps. A busy, incomplete, failed, or cancelled restore makes collection unavailable instead of advertising a partial roster as a complete smaller cluster. A subsequent complete successful restore makes the monitoring roster available again.

Topic/route counters carry their aggregation policy in the snapshot. Rendering no longer acquires the Router lock or calls live router methods. Raft/default routing retains minimum current counts and maximum high-water marks; broadcast retains sums. Unsupported routers, unavailable samples, or mixed policies omit routing fields without suppressing unrelated metrics. Routing behavior is unchanged.

Storage statistics use only explicitly provided nonblocking in-memory snapshot getters, never live count/max/len calls that could access a database. Plugins without safe snapshots may omit retaineds/message_storages/delayed_publishs fields. The RAM retainer and built-in delayed queue expose read-only snapshots; message and storage processing are unchanged.

OS data has an independent cache. Load, memory, and disk fields are omitted until a system sample is available and when rendering detects an expired system sample. Caches containing system values also expire 120 seconds after that system sample; newer business-counter samples do not extend its lifetime.

The former `rmqtt_nodes{item="running"}` series is no longer collected or exported by this monitoring path because it requires a live health query. The snapshot mechanism does not change the live semantics of existing health, readiness, liveness, or administrative endpoints. Rules requiring health signals should use the appropriate endpoint or probe.

Successful Prometheus responses include this metadata:

| Metric | Meaning |
| --- | --- |
| `rmqtt_metrics_refresh_success` | Whether the latest cluster refresh obtained every snapshot: 1 for complete, 0 for incomplete |
| `rmqtt_metrics_expected_nodes` | Last known number of registered monitoring targets |
| `rmqtt_metrics_missing_nodes` | Targets missing from the latest cluster refresh |
| `rmqtt_metrics_snapshot{node="…",item="sampled_at_seconds"}` | Business snapshot source Unix time in seconds; the aggregate uses the oldest node timestamp |
| `rmqtt_metrics_snapshot{node="…",item="system_sampled_at_seconds"}` | OS sample source Unix time; omitted if no system sample exists, and the aggregate requires a system sample from every node |
| `rmqtt_metrics_snapshot{node="…",item="omitted_stats"}` | Count of omitted statistics fields recorded in the snapshot; this does not cover every omission during rendering |

Refresh metadata describes cluster refresh state, including when returned with a per-node view. It is not a live health result for that node. These metadata series are not guaranteed to be scrapeable when the endpoint returns 503; also monitor Prometheus `up`.

The timestamp Prometheus attaches to a scrape does not express the freshness of the source data. Use the following PromQL queries for business and OS sample age respectively:

```promql
time() - rmqtt_metrics_snapshot{item="sampled_at_seconds"}
```

```promql
time() - rmqtt_metrics_snapshot{item="system_sampled_at_seconds"}
```

For `node="all"`, the age represents the oldest corresponding sample in the aggregate. Keep node clocks synchronized; snapshots with timestamps too far in the future are rejected. A missing series differs from an old sample, so alert rules should account for both absence and scrape failure.

## Deployment and scope

This mechanism ships with the RMQTT image and its `rmqtt-http-api` plugin. It does not require an Operator upgrade. Existing Prometheus paths, ServiceMonitor configuration, and HTTP plugin authentication settings can be retained.

Peers use `MetricsSnapshotV2` unary requests to read pre-encoded local snapshots carrying the routing aggregation policy. Existing message discriminants are preserved; V1 payloads are not interpreted as V2. Collection does not fall back to legacy live metrics collection or the business gRPC reconnect queue. During a rolling upgrade, peers that lack this protocol count as missing targets. Complete cluster metrics may therefore return 503 until every target supports the protocol and a complete refresh succeeds. An upgraded node's local view can remain available independently.

The design uses a dedicated worker, cache reads, nonblocking lock attempts, and collection budgets to reduce monitoring interference with core processing. Background sampling still reads shared counters and calls extension statistics sources. These boundaries do not establish zero overhead, no effect at arbitrary load, or production throughput guarantees. This document describes the design and endpoint behavior; it does not claim deployment load testing or fault-recovery validation in any environment.
