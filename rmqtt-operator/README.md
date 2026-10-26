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
- accepts `rmqtt.io/manual-restart` on an individual Pod for a controlled single-Pod restart with an explicit timeout policy
- keeps verified in-memory survivors available for best-effort MQTT traffic after lost Raft quorum
- automatically restores a writable Raft quorum from the best surviving state

The broker readiness probe uses `GET /api/v1/health/ready` and only succeeds when the local node has restored a snapshot, applied its local committed index, is a voter, reports raft `started` and `available`, has a current leader, is present in business peers, and is not draining. Deploy the operator together with a broker image containing this endpoint.

New and recreated pods start with `rmqtt.io/serving=false`, so they are excluded from the client Service. During normal operation, the operator queries every reachable raft node, accepts membership only from the node that reports itself as the current leader, and changes the pod to `rmqtt.io/serving=true` only after local readiness and leader-authoritative voter, raft-peer, and business-peer checks all pass. The headless Service continues to publish not-ready addresses so raft discovery does not depend on client traffic admission. A separate, bounded degraded-serving rule applies only after quorum loss has been verified, as described below.

Controlled bootstrap is intentionally fail-closed. The operator sets `bootstrap=true` only on the first single-node StatefulSet. After that node is healthy, it updates the template to `bootstrap=false` before creating another pod. A restarted or scaled-out node retries leader discovery and exits without calling `raft.lead()` if no existing leader can be found.

Operator-managed Pods also render
`health.exit_on_node_unavailable=false`. A quorum failure therefore does not
terminate the last processes holding volatile Raft, session, and router state;
Kubernetes readiness and the client-admission policy decide whether they may
receive traffic. This changes only failure handling and adds no work to the
normal MQTT publish path.

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
-> quiesce new sessions, exclude from client Service and wait for EndpointSlice removal
-> wait for plain forwards to finish -> activate target avoidance
-> notify capable MQTT 5 clients, confirm their FIFO fences and ready replies
-> drain local work and wait for handoff/routing barriers
-> leave Raft and business membership -> delete only that Pod
-> StatefulSet recreates it from the target template
-> join as learner -> catch up -> promote -> restore business membership
-> admit client traffic -> observe the stability window -> continue
```

Clients opt in with MQTT 5 CONNECT user property
`ams_coordinated_drain=1`. The broker sends `$ams/drain/prepare` directly on
each opted-in session. After the client has paused new sends, it publishes
`$ams/drain/fence`; the broker replies on `$ams/drain/fence_ack`. The client
waits for its outbound QoS transactions to complete, then publishes
`$ams/drain/ready` on the same physical connection. The operator proceeds to
the existing session drain only after all capable clients respond. A missing
reply rolls the operation back; `$ams/drain/resume` releases clients whose
connection stays open. Older clients retain the previous drain behavior.

For an externally deleted Pod, the Operator starts this preparation on the
Broker and checks its identity-bound `client_drain_status` in later reconcile
rounds. Each control request stays within the external deletion path's short
request budget. It sends `drain` only after the status is `ready`. External
deletion cannot be rolled back: if clients fail to respond before the Pod's
termination budget runs out, the existing forced-finalization path records
`CompletedWithLoss` rather than claiming a lossless handoff.

Control records use `ams_mqtt_drain` with the `drain_id`, node, physical client
and stage. For a diagnostic rollout, set `RMQTT_AMS_DELIVERY_TRACE=1` on every
Broker process **before it starts**. This enables unsampled
`ams_mqtt_delivery` records for every AMS request and response PUBLISH:
Broker ingress, route match, local or remote queueing, reliable handoff result,
and subscriber socket write. It does not log payloads. Capture stdout from all
Broker Pods, including their final logs before deletion. Leave the switch off
outside the diagnostic window because full request tracing is high volume.

Image replacement composes the same node-drain executor used by scale-in and
the same learner catch-up classifier used by scale-out/restart recovery.
Before replacing a current Leader, the operator confirms the transfer by
observing a different authoritative Leader. An accepted Raft transfer request
is not treated as completion. If the Leader is unchanged after 30 seconds, the
operator revalidates full membership health and retries the request, rotating
through the eligible ready and caught-up voters. It never starts draining or
deletes the current Leader while the transfer is still pending.

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

## Graceful External Pod Deletion

Direct Pod deletion can opt into the same in-memory drain and membership
cleanup path:

```yaml
spec:
  pod:
    terminationGracePeriodSeconds: 120
  gracefulShutdown:
    enabled: true
    timeoutSeconds: 100
    finalizeReserveSeconds: 10
```

The feature is disabled by default. When enabled, SIGTERM immediately makes
the broker unready and rejects new MQTT sessions while keeping its internal
HTTP, gRPC, Raft, existing MQTT connections, and in-memory handoff state alive.
The operator detects `deletionTimestamp`, validates the Pod UID and broker boot
identity, transfers leadership when needed, prepares reliable handoff, drains
the node, removes it from membership, and releases the process. All stages
share `timeoutSeconds`; the deadline cannot be extended by retries.

Multiple external deletions are tracked as one dynamically expanding batch.
Each reconciliation discovers newly terminating Pods, so a second deletion is
merged even when the first Pod is already draining. Data-plane quiesce,
EndpointSlice observation, session/message handoff, and drain-barrier checks
advance independently for every target. Leadership transfer and Raft/business
membership removal remain serialized. The next owner is selected by deadline;
once persisted, a new earlier deadline cannot replace that owner before membership
removal is confirmed. The effective deadline is the minimum of Kubernetes' deadline
and the broker's remaining time, anchored at the start of the status request.
Control RPCs have a two-second budget without inline retries; each external batch
round has a twenty-second budget. Progress is saved per target as it completes.
Terminating targets and an already planned drain target are excluded from handoff
destinations, but still-live terminating senders participate in reliable forwarding
and drain barriers. Destination/participant changes are acknowledged and retried
across rounds, including after leader transfer or operator restart.
Learners confirmed by the authoritative leader to have no business membership
do not participate in HTTP drain/clear barriers: their control API starts only
after promotion. Unready or terminating business senders are not excluded by
this rule. A learner belonging to an earlier scale-out or rollout remains owned
by that operation, which resumes after the external batch finishes.

With graceful shutdown enabled, `rmqtt.io/external-membership-cleanup` is added
to managed Pods before deletion. It keeps the old Pod object (not its process)
until Raft and business removal are confirmed, preventing an id-only cleanup
request from racing a new Pod with the same ordinal. The original grace period
still applies; a stopped process can remain visible as Terminating while cleanup
finishes. Legacy Pods deleted before receiving the finalizer never undergo
identity-blind cleanup. Operator-owned rollouts, scale-in and disaster recovery
release this fence through their existing protected deletion paths.
Normal scale-in and rollout paths also record `rmqtt.io/planned-drain-pod-uid`
on the exact old Pod after their drain/removal workflow. If that Pod remains
Terminating, the external handler binds its shutdown identity and confirms both
memberships are absent, then completes shutdown without repeating drain. Its
local drain result still determines whether completion is clean or with loss.
The marker is not inherited by replacement Pods; disaster-recovery fence release
does not write it. It is an operator-owned annotation, not a user control switch.

External-batch replacement learners are promoted before resuming a saved scale-in/rollout,
without overwriting that operation. Handoff exclusion is cleared only after the
replacement is Ready, caught up, and present in both memberships. Disabling the
feature durably moves an active batch to disabled-cleanup state, clears its
handoff markers on every surviving broker, and identity-signals bound terminating
brokers to finish with a loss outcome. It then updates the OnDelete StatefulSet
template to disable broker coordination and remove the inherited fence before
releasing current Pod finalizers. Failed marker clears are retried without holding
Pod deletion; re-enabling waits for them to finish, including a replacement learner
whose control API is not available until promotion. Deleting the Cluster CR skips
network cleanup and releases its Pod fences immediately. The CR has a cleanup
finalizer for this purpose. Apply the updated operator RBAC (CR metadata patch
permission). Keep the operator running for cleanup; disable this feature and wait
for `status.externalTerminationCleanup` and finalizers to clear before uninstalling
it. Manually removing finalizers bypasses the ordinal-reuse safety guarantee.

For an existing cluster, first deploy broker and operator binaries containing
this capability, then set `gracefulShutdown.enabled=true` and complete a
controlled rollout so every running Pod has the new environment and control
actions. Do not rely on direct deletion until that rollout has finished.

The StatefulSet command already uses `exec rmqttd`, so kubelet signals reach
the broker process directly. No preStop command or persistent storage is
required. External deletion reuses the existing broker drain, reliable handoff,
session relocation, and membership-removal APIs used by planned operations.
It does not change normal QoS 0/1/2 publish processing, inbound packet-ID
tracking, outbound inflight restoration, reconnect/takeover RPCs, or the session
snapshot wire format. It inherits the existing drain's delivery guarantees and
limitations; it does not add a new inbound QoS 2 migration guarantee. Before
drain activation, existing sessions and reliable acknowledgements continue
through their original processing paths. Only new connections and incoming
session relocations are refused once this process starts terminating.
At the deadline the operator may force-finalize only the terminating target
and records the result as `CompletedWithLoss`.

The scope review and regression checks are documented in
[graceful-shutdown-design-review.md](docs/graceful-shutdown-design-review.md).

During shutdown, the broker logs every phase change and emits a progress line
at most every 10 seconds. `message_work_items_remaining` sums live queue,
inflight, delayed, forwarding, and reliable-handoff work; it is an operational
work-item count and can briefly count one MQTT message in more than one stage.
`handoff_messages_completed` counts reliable message handoffs confirmed since
this drain began, while `session_handoffs_completed` counts relocated sessions.
The broker exposes local counters through `drain_status`; each peer exposes its
target-specific handoff count through `routing_status`, and the operator sums
those peer counts in its periodic drain progress log.

## Restart One Pod

After deploying an operator version containing this feature, choose how a drain
timeout should be handled and annotate the existing Pod:

```bash
kubectl --kubeconfig ~/.kube/a800-local.yaml \
  -n studio-ams-system \
  annotate pod ams-rmqtt-2 rmqtt.io/manual-restart=rollback-on-timeout --overwrite
```

Use `rollback-on-timeout` when the original Pod must be retained if the drain
does not finish in time. Use `force-on-timeout` when the operator may perform
controlled force cleanup and continue the replacement after that timeout:

```bash
kubectl --kubeconfig ~/.kube/a800-local.yaml \
  -n studio-ams-system \
  annotate pod ams-rmqtt-2 rmqtt.io/manual-restart=force-on-timeout --overwrite
```

Both types first run the same graceful drain. `force-on-timeout` does not force
an immediate deletion. Replace `ams-rmqtt-2` with the Pod you want to restart.
The command only submits the request; it returns before the restart finishes.
This is a **single-Pod restart**, not scale-in: the StatefulSet recreates a Pod
with the same name. Ordinary `kubectl delete pod` uses the external deletion
workflow only when `spec.gracefulShutdown.enabled=true`; otherwise it retains
the legacy behavior.

The annotation itself submits the restart request and selects its timeout
policy. `manual-restart=rollback-on-timeout` corresponds to `Rollback`, and
`manual-restart=force-on-timeout` corresponds to `ForceContinue`. For this single-Pod
request the annotation takes precedence over `spec.raft.drainTimeoutAction`;
image upgrades, full rolling restarts, and scale-in continue to use that
cluster-wide setting. Both use `spec.raft.drainTimeoutSeconds` (default: 1200 seconds).
Existing clusters with an explicit value retain that value until it is patched.

The operator waits for a healthy cluster, transfers leadership if needed,
quiesces client traffic, drains messages/sessions through the existing reliable
handoff barriers, and removes the target from Raft and business membership.
Only then does it delete the original Pod. The replacement rejoins as a learner,
catches up, becomes a voter, resumes serving, and passes the stability window.
Clients may disconnect and need to reconnect. The replacement uses the
StatefulSet template, with the image pinned for this operation. If
`autoRollout=false`, this can also apply a previously pending image/template
update to that one Pod.

Requests share the existing operation lock with image rollouts, full rolling
restarts, membership repair, and scaling. Those operations take precedence over
pending Pod requests. Multiple Pod annotations are processed one at a time in
ascending Pod ordinal order. Repeating the annotation on the same original Pod
while its request is pending or running coalesces into one request. Once
accepted, the operator freezes the Pod name, original UID, and restart timeout policy in
status, so overwriting the annotation cannot change an in-progress operation.
If another rollout or external deletion already replaces a queued Pod, its
request disappears with that old Pod. Requests must be placed on individual
Pods, never on `spec.pod.annotations` or the StatefulSet template. Any value
other than `rollback-on-timeout` or `force-on-timeout` is rejected and reported
as `InvalidPodRestartRequest`.

Progress and the most recent result are available on the cluster:

```bash
kubectl --kubeconfig ~/.kube/a800-local.yaml -n studio-ams-system \
  get rmqttcluster ams-rmqtt \
  -o jsonpath='{.status.phase}{"\n"}{.status.message}{"\n"}{.status.operation}{"\n"}{.status.lastPodRestart}{"\n"}'
```

While active, `status.operation.type` is `PodRestart`; `targetPodName`,
`targetPodUid`, and `manualRestart` identify the immutable request. The operator
persists the target and phase so a controller restart resumes the same
operation. Deletion uses a UID precondition, protecting a replacement that
already has the same name. After completion, `status.operation` is cleared and
`status.lastPodRestart` records `Succeeded` or `Failed`, the restart timeout policy,
original UID, time, and explanation.

With `rollback-on-timeout`, a drain timeout restores serving and retains the
original Pod. With `force-on-timeout`, only a timeout authorizes the existing
force-finalize protocol: every broker must first advertise support, the
operator records the force intent before sending destructive requests, and it
records discarded counts in `status.lastForcedDrain` before membership removal.
Unsafe handoff, detected data loss, health/quorum failure, and other drain errors
do not enter this force branch. After rollback succeeds, the annotation is
removed and the failed result is recorded. If a destructive cleanup has already
started and a later step cannot be confirmed, the operation stays blocked or
retries instead of pretending the old state was restored. The drain timeout uses
`spec.raft.drainTimeoutSeconds`; stabilization uses
`spec.imageUpdate.stabilizationSeconds` with a minimum of 10 seconds.

To withdraw a request **before the operator accepts it**, remove its annotation:

```bash
kubectl --kubeconfig ~/.kube/a800-local.yaml -n studio-ams-system \
  annotate pod ams-rmqtt-2 rmqtt.io/manual-restart-
```

Removing the annotation after `status.operation` records the request does not
cancel an in-progress drain/replacement. No CRD schema or RBAC changes are
needed; users need permission to annotate the Pod and the operator uses its
existing Pod read/patch/delete permissions.

## Drain Safety for Image Rollouts and Scale-in

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

Stalled handoff deliveries also trigger bounded session reconciliation in the
broker, including queues left after a rollback. The broker checks all known
Raft/business peers for a live or offline session, a connection/install in
progress, a retained migration snapshot, an accepted delivery, or a delivery
whose transport result is uncertain. Only after
every peer confirms absence may it conditionally remove the exact stale owner
through Raft and retire the captured, unaccepted deliveries. A newer connection
is protected by its full connection Id. Retirement is logged with
`reason=confirmed_session_absent`; it is not a claim of delivery to a client.

This requires broker support on every participating node, reported as
`supports.sessionReconciliation` by the membership API. An unavailable or older
peer, an ownership/membership change, or an uncertain acknowledgement defers
cleanup and preserves the existing drain timeout/rollback behavior. No new
Raft log command or normal publish-path check is introduced. Connection and
session-transfer operations maintain a small in-memory activity marker; full
checks run only for maintenance reconciliation, with one queue-initiated
reconciliation and one coordinated peer check per broker at a time.

Session relocation installs the local offline session and its subscriptions
before publishing the destination owner in Raft. A reconnect taking the
snapshot shares the per-client entry lock with installation/extraction, so it
cannot receive `NotExist` during their temporary entry gap. Interrupted
installation retries finish ownership and route registration without replaying
the snapshot's messages. A failed or uncertain metadata update preserves the
installed copy for retry.

The opt-in loopback regression starts three actual brokers and checks orphan
queues, concurrent QoS 1/2 traffic, offline delivery, Clean Start gateway
reconnect, and interrupted installation retries:

```bash
cargo test -p rmqtt-cluster-raft --lib --offline \
  three_node_orphan_reconciliation -- --ignored --nocapture
```

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

The operator also detects drift in the Pod scheduling template, including Pod
annotations, service account, image pull secrets, scheduler, affinity,
tolerations, service-link policy, and termination grace period. These changes
are copied from `RmqttCluster.spec.pod` into the StatefulSet even when the image
is unchanged. Because the StatefulSet uses `OnDelete`, scheduling-only changes
do not implicitly restart a healthy Raft cluster; run `kubectl rollout restart`
after the StatefulSet template contains the expected values to request the
normal operator-managed one-Pod-at-a-time restart.

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

Status-transition logs include the leader's Pod name next to its Raft ID, for
example `leader_id=2 leader_pod_name=ams-rmqtt-1`. Raft IDs start at 1 while Pod
ordinals start at 0. An unknown or invalid leader ID produces `leader_pod_name=-`.

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

## Quorum Loss Recovery

Quorum recovery is an operator availability guarantee and has no per-cluster
feature switch. It is activated only when no available authoritative Raft
leader can be found; it adds no work to the normal MQTT publish path. The
operator first waits a fixed 30-second detection period and a 60-second natural
recovery period. Any authoritative leader observed during that window cancels
recovery without changing Raft membership.

When no authoritative leader exists, the operator reads local Raft status from
reachable Pods. A response is a recoverable survivor when its node identity
matches the queried Pod, its Raft snapshot is restored, and it still reports its
old voter set. Pod readiness, `available`, and `started` are deliberately not
used as evidence that local state is absent: those fields can be false precisely
because quorum was lost. Only these verified old survivors retain
`rmqtt.io/serving=true`; rejected, unreachable, new, and empty-state Pods remain
excluded. The client Service temporarily publishes those survivors despite the
strict Pod readiness probe. Rollout, scale, learner promotion, and all other
control-plane checks continue to use strict readiness, so normal behavior is
unchanged.

Before the window expires, verified survivors can remain published as degraded
endpoints, although operations requiring a Raft commit can still fail. After
expiry, the operator selects the survivor with the most advanced applied state,
using committed/log progress and local client-state count only as tie-breakers.
This handles both cases where the old leader survived and where only followers
remain; stale leader identity never outranks more advanced recoverable state.

The operator removes every non-seed old Pod from the client Service, deletes
those Pods, and verifies their old Kubernetes UIDs are gone before asking the
seed to replace its local Raft runtime with a singleton. The seed keeps its
in-memory router/session ownership snapshot and resumes writable service as a
one-voter quorum. Recreated Pods then join as learners and are promoted one at a
time. Every promoted node must then remain Pod-ready and answer its own local
membership request continuously for 15 seconds before another node is started.
The operator also revalidates every previously rebuilt node before continuing.
If a promoted or previously rebuilt voter is both Pod-NotReady and unable to
answer its local membership check continuously for 30 seconds, the operator can
recycle that one Pod and let it rejoin as a learner. A failure of only one signal
is treated as a readiness or control-path problem and resets the recycle timer.
Before every delete attempt the operator waits for the client EndpointSlice to
exclude the Pod, queries the recovery seed again, verifies the recovery epoch,
leader identity, and current quorum, and confirms that the node is neither the
seed nor the leader. The exact old Pod UID is persisted and supplied as a
Kubernetes delete precondition, so an operator restart or a concurrent
StatefulSet recreation cannot delete the replacement.
Automatic recycling is limited to two attempts per node; when the current
quorum cannot survive the restart or the retry budget is exhausted, recovery
stays in `DegradedServing` and preserves the verified leader instead of risking
an outage loop. These checks and deletes exist only inside an active quorum
recovery and do not change normal rollout, scaling, or MQTT message handling.

If an `OrderedReady` target Pod has not actually been created, the membership
operation is cleared and the verified leader stays admitted while
reconciliation reports the blocking predecessor. The operator never reports
learner catch-up for a nonexistent Pod. Status progresses through
`SelectingRecoverySeed`, `DegradedServing`, `RebuildingQuorum`,
`StabilizingRecoveryNode`, and when necessary `RecyclingRecoveryNode`; it
returns to `Running` only after all desired voters, business peers, and Pods
converge.

If no recoverable survivor remains, the operator automatically performs an
empty bootstrap to restore availability. It first scales the StatefulSet to
zero, removes every old Pod from Service, waits for every old Pod UID to
terminate, and only then creates one bootstrap seed. If an old process cannot
be proven terminated, recovery remains blocked rather than risk two writable
leaders. Empty bootstrap restores service but loses all in-memory cluster
state.

This recovery is availability-first. It can lose messages and sessions that
existed only on discarded nodes, and concurrent client retry can still create
duplicates. QoS 2 does not make volatile multi-node state durable. The feature
adds no disk IO, general-purpose deduplication, or work to the normal publish
fast path.

Transient retries for the read-only `membership` action are logged at `DEBUG`.
Their final results remain visible through an aggregated membership scan record
whenever the reachable node set, generic error kind, or recovery phase changes,
and at least once every 30 seconds while unchanged. The record includes the
original final error text for every failed node. Non-transient membership errors
and all state-changing recovery actions also continue to log their individual
failures at `WARN`.

## Failure Boundary

The conservative lifecycle preserves service during one-at-a-time restarts and
updates while a Raft quorum remains running. `rmqtt-raft 0.6.0` still stores
Raft logs and hard state in memory. Automatic quorum recovery prefers one
survivor and rebuilds availability, but it cannot reconstruct state that no
surviving process holds. Recovering that state requires durable storage or an
external replayable source of truth.

## Inspect

```bash
kubectl -n rmqtt-test get rmqttclusters
kubectl -n rmqtt-test get pods
kubectl -n rmqtt-test logs deploy/rmqtt-operator
```
