# RMQTT Raft Operator

First Rust MVP operator for RMQTT raft clusters.

## Current Scope

The operator owns the Kubernetes reconciliation flow for an RMQTT raft cluster:

- renders RMQTT raft config for stable StatefulSet pod identities
- creates ConfigMap, Services, and StatefulSet
- bootstraps a new cluster with one explicitly authorized pod, disables bootstrap in the StatefulSet template, then adds the remaining pods one at a time
- makes every later pod creation and restart join-existing-only, so DNS or network discovery failures cannot create a second raft cluster
- creates a PodDisruptionBudget with `maxUnavailable: 1`
- separates local Kubernetes pod readiness, leader-authoritative raft membership, and client traffic admission
- scales out one node at a time
- scales in one node at a time
- keeps a `status.operation`/`status.confChange` lock so a single `RmqttCluster` only advances one raft membership change at a time
- adds new and rebuilt nodes as non-voting learners, verifies snapshot restoration and leader-side replication progress, and only then promotes them to voters
- completes a rebuilt node's repair from that target node's voter, catch-up, business-membership, and Pod-readiness state, so multiple simultaneous Pod losses are repaired sequentially without a global-readiness deadlock
- checks both raft voters and business-layer gRPC peers before completing a scale step
- drains and removes one voter before reducing StatefulSet replicas
- optionally turns an RMQTT image change into a Raft-aware, one-pod-at-a-time update

The broker readiness probe uses `GET /api/v1/health/ready` and only succeeds when the local node has restored a snapshot, applied its local committed index, is a voter, reports raft `started` and `available`, has a current leader, is present in business peers, and is not draining. Deploy the operator together with a broker image containing this endpoint.

New and recreated pods start with `rmqtt.io/serving=false`, so they are excluded from the client Service. The operator queries every reachable raft node, accepts membership only from the node that reports itself as the current leader, and changes the pod to `rmqtt.io/serving=true` only after local readiness and leader-authoritative voter, raft-peer, and business-peer checks all pass. The headless Service continues to publish not-ready addresses so raft discovery does not depend on client traffic admission.

Controlled bootstrap is intentionally fail-closed. The operator sets `bootstrap=true` only on the first single-node StatefulSet. After that node is healthy, it updates the template to `bootstrap=false` before creating another pod. A restarted or scaled-out node retries leader discovery and exits without calling `raft.lead()` if no existing leader can be found.

`status.confChange` is kept for compatibility with the first MVP. Newer logic also writes `status.operation`, which records the operation type, phase, node id, replica transition, and reconfiguration mode.

Scale-out and restart recovery use the learner flow, represented in status as:

```yaml
status:
  operation:
    type: AddLearnerAndPromote
    phase: CatchingUp
    mode: LearnerCatchUp
  confChange:
    type: AddLearnerAndPromote
```

`rmqtt-cluster-raft` exposes the operations and status needed by this flow:

- learner join and promotion
- local applied/committed/snapshot-restored status
- leader-side matched index, recent activity, and pending snapshot status
- voter/learner membership inspection
- leadership transfer before an allowed leader scale-in
- leader-side voter removal and target-node graceful leave

The add-node flow is:

```text
create pod -> join as learner -> restore snapshot/apply committed log
           -> leader confirms matched >= committed and no pending snapshot
           -> promote learner -> publish business NodeUp -> pod ready -> admit client traffic
```

## Image Update

Automatic broker image rollout is opt-in, so existing `RmqttCluster` resources
keep the original `OnDelete` behavior unless it is enabled:

```yaml
spec:
  image: registry.example.com/rmqtt:next-version
  imagePullPolicy: IfNotPresent
  imageUpdate:
    autoRollout: true
    stabilizationSeconds: 60
```

When `spec.image` or `spec.imagePullPolicy` changes, the operator first updates
the StatefulSet template, then updates one broker at a time. It chooses
non-Leader pods before the current Leader. For every pod it:

```text
prepare reliable handoff on every broker (new forwards receive delivery IDs)
-> exclude from client Service and wait for EndpointSlice removal
-> wait for plain forwards to finish -> activate target avoidance
-> drain local work and wait for handoff/routing barriers
-> leave Raft and business membership -> delete only that Pod
-> StatefulSet recreates it from the target template
-> join as learner -> catch up -> promote -> restore business membership
-> admit client traffic -> observe the stability window -> continue
```

Image replacement composes the same node-drain executor used by scale-in and
the same learner catch-up classifier used by scale-out/restart recovery.

## Rolling Restart

The StatefulSet keeps `updateStrategy: OnDelete`, but the operator recognizes
the standard restart annotation written by:

```bash
kubectl rollout restart sts/ams-rmqtt
```

The command itself does not delete any pod. The operator records
`spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"]` as a
`RollingRestart` target and runs the same one-pod-at-a-time drain, leave,
recreate, learner catch-up, promotion, business readiness, and stabilization
flow used for image rollout. Non-leaders are restarted before the current
leader. A pod counts as updated only after it inherits the target restart
annotation from the StatefulSet template.

The first reconciliation after upgrading an existing operator records the
current StatefulSet restart annotation as `status.observedRestartAt`. This
prevents a historical `kubectl rollout restart` annotation from unexpectedly
restarting a live cluster. Later timestamp changes trigger a new rollout, and
reusing the same timestamp is idempotent. A newer restart request received
during an active rollout becomes the target for the remaining work and is
persisted in `status.operation.targetRestartAt`.
Image-specific orchestration only selects the pod, deletes it after membership
removal, verifies the target template, and enforces the per-pod stability
window.

The operator refuses scale-in or image replacement unless every current
broker acknowledges reliable handoff support. `endpointRemovalStableSeconds`
controls how long the target Pod must remain absent from the client Service
EndpointSlices before membership removal can continue. Session cutover starts
first so EndpointSlice propagation cannot leave an unprotected live session.

Before replacing the Leader, leadership is transferred to a caught-up voter
that is already running the target image. The next pod is not touched unless
all replicas are Ready and leader-authoritative Raft and business membership
are healthy. The handoff queue retains the source copy until the receiver
reports final delivery, batches ordered messages per MQTT client, and applies
backpressure instead of dropping when its hard capacity is reached.
`handoffRollbackThresholdPercent` defaults to `80`. With
`drainTimeoutAction=Rollback`, reaching that watermark, failing a safety
observation, or exceeding the drain timeout causes the operator to undrain the
pod and restore its Service endpoint. The rollout remains blocked for
diagnosis and never deletes that pod.

`drainTimeoutAction` changes only the drain-timeout branch. In either mode, a
failed safety observation, detected data loss, handoff failure/rejection, or
handoff capacity threshold immediately rolls the drain back. `Rollback` is the
default timeout behavior. With `ForceContinue`, only a drain that remains
incomplete until the configured timeout asks every broker to discard handoff
deliveries tied to the draining node, asks the target to
terminate its remaining sessions, removes the target's replicated client and
router state, records all discarded counts in `status.lastForcedDrain`, and
continues the membership change. Raft quorum, leader transfer, caught-up voter,
minimum replica, and serialized ConfChange checks remain mandatory. This policy
can lose the residual messages reported in `lastForcedDrain`; it does not alter
normal forwarding and is not used during scale-out.

Normal forwarding deliberately keeps the original fast path: it does not
allocate delivery IDs, touch the handoff queues, update per-target counters, or
perform deduplication. Once the operator's prepare marker is observed, new
forwards to the selected pod remain on the plain path but are tracked. The
activate marker switches new forwards to reliable handoff, and the target is
activated last immediately before its broker drain disconnects local sessions.
A plain request already in flight at that boundary is included in the final
routing barrier, while a transport failure aborts the rollout. This keeps
normal delivery active throughout preparation without weakening deletion
safety.

The queue is not durable storage. Simultaneous failure of the source broker and
its in-memory queue, QoS 0 delivery, or an application deadline expiring while
backpressure is active are also outside the guarantee. A disk-backed outbox or
an idempotent business protocol is still required for those failure domains.

The plugin defaults allow up to 100,000 queued messages or 512 MiB of encoded
payload per broker, whichever is reached first, and keep completed delivery
IDs for 600 seconds. Size these limits below the broker Pod's available memory;
the operator normally rolls back at 80 percent so the hard limit remains an
emergency backpressure boundary rather than an OOM boundary.

The automatic image setting controls only image rollout. Scale-out, scale-in,
and restart recovery continue to use their existing flows. A replica change
that arrives between completed image-update steps pauses the rollout so
scaling can reconcile first.

Use immutable image tags or digests. Re-pushing different content under the
same image string does not change the `RmqttCluster` spec and therefore cannot
be detected as a new rollout request.

## Build

```bash
PUSH_IMAGE=false ./scripts/build-rmqtt-operator-amd64-image.sh rmqtt-operator:local
```

The operator reuses the local amd64 build image created by the RMQTT broker
build. If it is missing, first run:

```bash
PUSH_IMAGE=false ./scripts/build-rmqtt-amd64-image.sh rmqtt:local
```

Set `RMQTT_AMD64_BUILD_BASE_IMAGE` on both commands to override the default
local build image name.

The RMQTT broker image used by the sample is:

```text
registry.cn-sh-02.sensecore.cn/ccr-ams-public/mirror/docker/rmqtt-test/rmqtt:latest
```

## Deploy

```bash
kubectl apply -f rmqtt-operator/config/crd.yaml
kubectl apply -f rmqtt-operator/config/operator.yaml
kubectl apply -f rmqtt-operator/config/sample.yaml
```

The default namespace is:

```text
rmqtt-test
```

## Logging

The operator uses `RUST_LOG` and defaults to `info`. The deployment manifests
set it explicitly:

```yaml
env:
  - name: RUST_LOG
    value: info
```

INFO logs are state-transition based rather than reconcile-loop based. The
operator logs a new CR generation, bootstrap, scale-out, learner catch-up,
learner promotion, pod readiness, drain, leadership transfer, scale-in,
Running, blocked states and errors only when their status signature changes.
Changing progress text alone does not emit a new line, so the five-second
reconcile interval does not create repeated INFO logs.

Set `RUST_LOG=debug` only while diagnosing a problem. DEBUG includes every
cluster reconcile cycle and is intentionally more verbose.

## Existing Cluster Compatibility

`spec.service` supports the client-facing MQTT, MQTTS, WebSocket, secure
WebSocket and HTTP API ports independently from the headless gRPC/Raft ports.
`spec.resources`, `spec.pod` and `spec.monitoring.serviceMonitor` preserve the
resource guarantees, scheduling constraints, image pull secrets and monitoring
settings of an existing Helm deployment.

An existing RMQTT ConfigMap can be reused through
`spec.config.baseConfigMapName`. The operator copies the base files into a
writable work directory, then overrides only values owned by the dynamic
cluster lifecycle:

- local node ID
- MQTT/MQTTS/WS/WSS, HTTP API, gRPC and Raft listen ports
- Raft and business-layer peer lists
- controlled-bootstrap and learner-join flags

All other broker settings, including packet limits, MQTT inflight and message
queue limits, session expiry, logging and Raft tuning, remain inherited from
the base ConfigMap.

For the `studio-ams-system/ams-rmqtt` dev04 replacement, preserve the current
Helm-generated configuration before uninstalling the old release:

```bash
kubectl -n studio-ams-system get configmap ams-rmqtt-config -o json \
  | jq 'del(.metadata.annotations,.metadata.creationTimestamp,.metadata.labels,.metadata.managedFields,.metadata.ownerReferences,.metadata.resourceVersion,.metadata.uid) |
        .metadata.name = "ams-rmqtt-base-config"' \
  | kubectl apply -f -
```

Then deploy the dev04 operator and cluster manifests:

```bash
kubectl apply -f rmqtt-operator/config/crd.yaml
kubectl apply -f rmqtt-operator/config/ams-operator.yaml
kubectl apply -f rmqtt-operator/config/dev04.yaml
```

The dev04 manifest intentionally differs from the legacy StatefulSet only
where the safe dynamic lifecycle requires it: `OrderedReady`, `OnDelete`, the
Raft-aware readiness endpoint, client traffic admission, learner catch-up,
drain and one-at-a-time membership changes.

## Scale Out

```bash
kubectl -n rmqtt-test patch rmqttcluster rmqtt-optest --type merge -p '{"spec":{"replicas":4}}'
```

The operator advances scale-out one node at a time. If `replicas` changes from `3` to `5`, it first reconciles `3 -> 4`. Node 4 joins as a learner and remains outside the client Service and business peer table. The operator queries the current leader and waits for the learner to be recently active, have no pending snapshot, and reach the leader's committed index. It then promotes node 4. Only after node 4 locally confirms snapshot restoration, voter membership, applied state, and business `NodeUp` does the operator enable `serving=true` and continue with `4 -> 5`.

The new pod receives peers up to the next single-step replica count; existing pods are not restarted because the StatefulSet uses `OnDelete`.

If a Pod object or container is recreated, its in-memory Raft state is empty, so it removes its stale member identity and rejoins as a learner. The operator detects this even when `spec.replicas` did not change, runs the same catch-up and promotion flow, and keeps `serving=false` until recovery completes. Other healthy pods remain Ready and continue serving as long as a Raft quorum is available.

## Scale In

```bash
kubectl -n rmqtt-test patch rmqttcluster rmqtt-optest --type merge -p '{"spec":{"replicas":3}}'
```

The operator records the final survivor and victim node IDs in
`status.operation` and advances scale-in one node at a time. For a `5 -> 3`
change it records `survivors=[1,2,3]` and `victims=[5,4]`, but removes only the
current victim from the client Service. Proactive session handoff uses every
other node in the current single-step topology, so the `5 -> 4` step can hand
off to nodes `[1,2,3,4]` and the `4 -> 3` step can hand off to `[1,2,3]`.
This keeps the handoff destinations aligned with the client Service while
later victims continue serving until their own drain step. Raft membership
still changes strictly as `5 -> 4` and then `4 -> 3`.

For each single-step scale-in, the operator verifies the current Raft membership
is available and that removing the victim leaves a recently-active quorum. It
refuses to remove the current leader unless `allowLeaderScaleIn=true`; when
enabled, it first transfers leadership to a caught-up voter and confirms the
leader changed on a later reconciliation. It arms handoff without diverting
traffic, waits for the current victim's EndpointSlice removal, activates
remote senders and the target last, starts the broker drain, waits for
`safe_to_leave=true`, calls `leave`, confirms Raft voters and business peers no
longer contain the victim, and finally reduces StatefulSet replicas.

`safe_to_leave` requires connections, sessions, handshakes, message queues,
inbound/outbound inflights, forwards, message storages, and delayed publishes
all to reach zero. A safety failure always restores every planned victim to
the Service and leaves the operation in `RolledBack`. With
`drainTimeoutAction=Rollback`, a drain timeout does the same. With
`ForceContinue`, only the timeout branch force-finalizes after every current
broker acknowledges the request, records discarded state in
`status.lastForcedDrain`, and continues only if the refreshed Raft leader,
post-removal quorum, and membership checks remain safe.

After each voter and Pod are removed, the remaining Pods and authoritative
Raft/business membership must stay healthy for
`raft.scaleInStabilizationSeconds` (default `60`) before the next removal.
Any observed unhealthy state resets this window. The existing image rollout
and rolling restart stabilization flow remains unchanged.

## Failure Boundary

This conservative flow preserves service during one-at-a-time restarts and updates as long as a raft quorum remains running. `rmqtt-raft 0.6.0` still stores raft logs and hard state in memory, so a simultaneous loss or restart of every pod cannot be recovered automatically without a future persistent WAL implementation.

## Inspect

```bash
kubectl -n rmqtt-test get rmqttclusters
kubectl -n rmqtt-test get pods
kubectl -n rmqtt-test logs deploy/rmqtt-operator
```
