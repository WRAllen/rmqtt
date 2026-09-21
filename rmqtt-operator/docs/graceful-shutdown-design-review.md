# Graceful shutdown 范围审计与修订设计

审计基线：`b91117f4`（本次修改前的 HEAD）。审计对象：原暂存区的全部 29 个文件。
目标：给外部 Pod 删除、Node drain 增加协调入口，复用已经存在的 broker drain，
不借此修改正常消息收发、普通重连或已验证的会话迁移语义。

## 审计发现及处理

| 原暂存改动 | 实际影响 | 修订 |
| --- | --- | --- |
| `process_publish` 提前登记、查询入站 QoS 2 packet ID，并改变 PUBREC 失败后的清理 | 每次普通入站 QoS 2 都改变处理行为 | 完整恢复基线 |
| `Message::Kick` 返回 packet ID，`OfflineInfo` 携带、恢复 `in_inflight_packet_ids` | 普通同节点/跨节点会话接管也改变 | 完整恢复基线 |
| 出站 inflight 由原 `reforward` 改为直接回填队列 | 普通重连及已有 drain 的恢复方式改变 | 完整恢复基线 |
| `HandoffCapabilities`、`KickSessionV2`、`SessionHandoffV2` | 普通跨节点接管增加 RPC、失败分支及协议维护成本 | 全部移除，保留原协议 |
| handler 放宽 drain snapshot 缓存重放条件 | 非 drain 的普通接管也进入新增分支 | 完整恢复基线 |
| reliable forward 在 terminating 时先返回 Retryable | 绕过已完成/已接收消息的原去重确认及 drain 判定 | 移除提前拒绝，保留原处理顺序 |
| 关闭功能时清理分支全量 apply StatefulSet | 校验及在途 rollout 目标固定前，可能提前应用 image/资源等变更 | 清理只修改本功能的 template finalizer 和启用开关，保留其余实时模板 |
| 通用 drain 广播重构无条件补回目标节点 | 如 5→4 后 clear_drain 仍请求已移除 node 5，产生无谓重试/超时 | 恢复原广播、prepare、activate、force-finalize 等辅助函数 |
| 终止等待在读状态后才注册通知；退出由 process::exit 改成 runtime 析构 | 完成通知可能错过，或等待阻塞任务而超出退出预算 | 先注册通知再读状态；恢复硬退出方式 |

入站 QoS 2 的独立协议改进如果将来需要，应单独设计、提交和验证；不包含在本功能中。

## 保留的设计

1. Broker 只有在启用功能且收到 SIGTERM 或合法终止绑定请求时才进入 terminating。
   功能关闭时不改变 admission/readiness 状态，保留原进程退出行为。
2. terminating 节点停止接收新连接和新迁入会话，并从 readiness 摘除。
   现存会话的 PUBLISH、ACK、重发、订阅和消息转发继续使用基线代码。
3. Operator 持续收集带 deletionTimestamp 的 Pod，以 Pod UID、boot ID 绑定目标。
   新删除的 Pod 加入现有批次；无论最初删除一个还是多个，都不会只处理第一个。
4. 对每个目标调用原控制接口：prepare_drain → quiesce → EndpointSlice 摘除稳定
   → activate_drain（远端在前，目标在后）→ drain → drain_status/routing_status。
   Broker 的断连、会话迁移、可靠转发和 safe_to_leave 判定不更换实现。
5. 多个目标的数据 drain 可以并行推进，迁移目的地排除正在删除和已有计划 drain 的节点。
   Raft membership 修改由持久化的单个 owner 串行推进，复用原成员变更接口。
6. Finalizer 防止旧 Pod 的成员清理尚未完成时，同名新 Pod 提前占用 ordinal。
   既有缩容、滚动更新、指定 Pod 重启在原 drain/移除屏障通过后释放自身 finalizer；
   同时发生外部删除时允许原等待过程让出协调权，完成后继续既有 operation。
7. 开关关闭/CR 删除仍可清理本功能遗留的 finalizer 与批次状态。
   清理过程不能顺带更新镜像、资源、副本、其它 finalizer 或覆盖固定中的 rollout 模板。
8. 超时沿用显式强制收尾及 `CompletedWithLoss`，不能把超时或数据损失报告成无损完成。
   进度统计仅作观测，不改变原安全退出条件。

## 不变性核验

以下生产文件修订后应与基线逐字一致：

- `rmqtt/src/inflight.rs`
- `rmqtt/src/session.rs`
- `rmqtt/src/shared.rs`
- `rmqtt/src/types.rs`
- `rmqtt-plugins/rmqtt-cluster-raft/src/message.rs`
- `rmqtt-plugins/rmqtt-cluster-raft/src/handler.rs`

以下 Operator 函数也恢复到基线实现：`routing_barrier_status`、
`broadcast_handoff_action`、`force_finalize_node_drain`、`prepare_reliable_handoff`、
`activate_reliable_handoff`、`clear_reliable_handoff`、
`prepare_node_for_drain_with_operation`、`restore_node_after_failed_drain`。
新增的外部删除协调通过同一组 broker 控制 API 工作，不需要重构这些计划操作函数。

## 验证

本地验证结果（测试不访问或修改任何 Kubernetes 集群）：

- `cargo test --locked -p rmqtt -p rmqtt-cluster-raft -p rmqtt-operator --lib --bins`：
  30 + 53 + 123 = 206 项通过；默认忽略的三节点集成另行执行。
- `cargo test --locked -p rmqtt-cluster-raft three_node_orphan_reconciliation -- --ignored --nocapture`：
  通过。启动三个真实本机 Raft/gRPC/MQTT 节点，验证 Clean Start 接管保留原状态、
  60 条 QoS 1/2 消息顺序接收、离线会话/消息恢复及孤立路由清理。
- `cargo check --locked -p rmqttd -p rmqtt-conf -p rmqtt-http-api`：通过。
- 最终复核增加已完成终止阶段不被迟到 bind 改回的保护后，
  `cargo test --locked -p rmqtt shutdown::tests`：6 项通过。
- `cargo fmt --all -- --check`、`git diff --check`：通过。
- 上述六个生产文件、原三节点集成测试文件与基线逐字一致；上述八个 Operator 函数，
  以及 cluster shared 的 `kick`、`accept_reliable_forward`、`send_session_handoff`
  函数与基线逐字一致。

新增回归覆盖：关闭功能不进入 terminating；完成/缩短预算能够唤醒已经等待的任务；
已完成可靠投递在运行、terminating、prepared、draining 阶段仍确认 Duplicate；
默认关闭功能不提前应用新镜像；关闭时保留在途 rollout 模板与其他 finalizer；
5→4 缩容清理只请求节点 1–4。既有批量删除、追加删除及计划操作交错测试保留并通过。

集群回归继续使用 `bench/rmqtt-graceful-shutdown/run.py`，在外部业务压测下覆盖：
逐节点单删、删除 leader、同时删两个（含/不含 leader）、先删一个再追加第二个；
并回归原扩容、缩容、镜像滚动更新、指定 Pod 重启，以及这些操作与外部删除交错。
每轮记录各节点连接数、broker/operator 日志和业务端成功/失败结果。
控制面 PASS 只说明协调完成，不能代替业务端消息完整性验证。
