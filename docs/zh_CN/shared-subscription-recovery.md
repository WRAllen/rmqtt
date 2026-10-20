# 共享订阅维护转发恢复

本次修复针对 `0.19.0-dynamic-peers` 的计划维护：共享消费者退订、退出或会话迁移时，仍未开始 MQTT 发送的消息由同一共享组的其他在线成员接管。维护前已进入会话队列的消息也保留恢复所需的身份。

## 转发责任

源节点尚未把任务交给接收节点时，可以重新选择消费者。一旦远端返回 Accepted，或者 RPC 结果不确定，来源只查询同一接收节点，不会同时改投另一个消费者。

接收节点把消息放入会话 mailbox 后，继续承担其发送、排队和退订恢复责任。会话事件循环在首次网络写入之前二选一：将未发送消息交给恢复队列，或进入 MQTT 发送。进入发送后立即移除恢复上下文；即使首次写入返回错误，也不把它当作“确定没有发送”而改投。QoS 2 已开始的握手仍绑定原会话和 Packet ID。

因此无需让每条发布等待订阅端 ACK，也没有新增逐消息的取消 RPC。`Delivered`、现有计数及 `safe_to_leave` 的完成判定没有升级为端到端送达确认；本次不能据此证明业务处理完成。

## 共享组重选

- 只在原 `SharedGroup + TopicFilter` 中选择仍在线、订阅有效的成员。
- 排除旧消费者、维护禁选节点以及该条任务自身保存的 avoid 节点。
- 使用新成员的 QoS、订阅标识符和订阅选项，并恢复发布时的原始 QoS、retain 属性。
- 不将整条 Publish 再次匹配所有订阅，避免普通订阅或其他共享组多收一份。
- 暂无可用成员时保留任务并退避重试，不再因为旧 Client ID 消失而清理共享义务。

接收节点需要再次转交已接管任务时，会产生新的本地恢复 ID，并保留旧 ID 的去重记录，避免将任务送回原节点时形成相同 ID 的等待环。旧 ID 在本地恢复任务尚未完成时不因 TTL 被清理；后续责任转移完成后仍遵守现有 `handoff.dedup_ttl`，不承诺任意长分区或进程崩溃后的持久去重。

## 队列与会话迁移

普通共享消息携带轻量上下文，保存组、过滤器、原始发布属性和会话、订阅代次。退订后重新订阅不会把已失效的队列项复活。MQTT 5 共享关系独立保留，不再与同客户端的普通关系或其他共享组错误合并。

接管旧会话前先关闭旧 mailbox 的写入口，并处理已经接受的消息，再应答 Kick。正常计划 Drain 保留离线 mailbox。迁移快照明确区分有效与已失效的共享上下文，接收端只重新绑定有效的订阅身份。

`Publish` 的现有序列化布局保持不变；共享上下文通过有版本的快照侧车传输。主动会话移交和客户端跨节点重连的拉取式移交均协商能力。

**部署要求：**新目标支持携带共享上下文的接管协议。旧目标发来的非管理员 legacy Kick 会被新源节点在提取会话之前明确拒绝，避免悄悄丢掉上下文；因此升级期间需要协调客户端落点或使用维护窗口，不能假设新旧节点之间的会话接管完全兼容。新目标拉取旧源时可使用原协议，旧源没有本轮新增上下文。

## 正常性能

普通订阅继续使用原有投递路径。共享订阅增加每会话的代次查询、上下文分配和轻量检查；代次表只为共享过滤器建立条目，没有逐消息 Raft 写入、额外 RPC、全局锁或正文深复制。完整重选、退避和恢复队列只在维护恢复时运行；已开始的恢复不因维护回滚而丢弃责任。

这些措施减少正常消息的额外成本，但不等于零开销。本地功能回归不代替同资源、同落点、同消息大小的吞吐与 p99 对比压测。

## 共享移交的循环等待修复

共享维护任务采用两个独立的调度域。接收端本地投递按真实客户端保持 FIFO，只向本地会话 mailbox 投递，不等待其他 Broker。消费者失效或归属变化时，任务连同原有容量转入独立恢复记录，让本地队尾继续执行。恢复记录按 Delivery ID 独立推进；Accepted 或传输不确定只使该记录等待重试，不再占住接收端客户端 FIFO。

恢复记录和本地投递仍保存在同一个移交记账体系内，迁移不减少 pending messages/bytes 或排空屏障计数。一旦外来责任需要再次转发，即使只改变 owner 而没有更换客户端，也先登记新的本地 child ID 并保护父 ID 的去重记录。父责任可以被上游查询为 Duplicate，但尚未完成的 child 继续占用容量并承担后续恢复，不能将该回复解释为业务处理完成。

每节点最多 8 个恢复 worker 和 4 个本地投递 worker，仅在首次共享维护任务入队后启动。任务使用到期索引与退避重试，不逐消息创建常驻 Tokio task，不扫描全部积压寻找可执行消息。等待期间不占用 worker；本地投递与远端 RPC 使用独立预算。重试中的新消息不会跳过退避间隔，取消或 panic 后仍保留任务责任。

维护出站在总容量内预留一个接收消息槽位及 `min(handoff.batch_max_bytes, handoff.max_pending_bytes / 2)` 字节。共享恢复要求至少两个总消息槽位，单条任务的估算大小（含正文、topic、客户端和关系元数据）不得超过该接收字节预算；不支持的大小明确拒绝并将所有权交还调用者，不能静默清理。入站 RPC 满时立即返回 Retryable，不在 RPC 内等待容量。接收任务因消费者迁移转为恢复任务时复用原槽位，受保护的父回复使上游释放其原出站槽位，恢复接收进展空间；这依赖对端可达和重试能够继续执行，不承诺永久失联或没有可用消费者时自动完成。

恢复任务不再用发布 QoS 和默认 V3 选项假装原订阅。仅在恢复路径绑定同组、同过滤器、同会话代次的真实订阅选项；无法绑定时保留显式未绑定意图，选中有效消费者后才投递。正常 Publish 上下文和序列化布局不增加字段，普通发布和非维护转发不进入新增调度器，Operator 的完成判定保持原样。

## 移交阻塞诊断

诊断版只增加本地观测，不改变转发结果、固定节点、重试、清理、超时及滚动完成判定，也没有新增 RPC 或 Raft 写入。默认可见的 WARN 日志使用以下前缀，不需要将全局日志改为 DEBUG：

- `handoff diagnostic:`：队头等待超过 5 秒后输出；同一状态每 30 秒重复，状态变化时最短间隔 5 秒。包含队列消息数/字节数、共享/pinned/uncertain 数量、队头 Delivery ID、目标客户端、topic、原始 filter/group、owner、avoid、真实尝试节点及当前观察到的下一目标。
- `handoff receiver diagnostic:`：接收端对重复 Pending 返回 Accepted，或因本地会话/归属/维护状态返回 Retryable 时输出。同一客户端每 30 秒最多一条，每节点合计最多 20 条/30 秒。
- `handoff diagnostic resolved/reassigned:`：此前报告过的队头完成或重新选择消费者时输出 INFO。这里的完成沿用现有 Delivered/Duplicate 含义，不代表业务处理完成。

| 字段/现象 | 用途 |
| --- | --- |
| `head_pin_origin=transport_error`、`head_first_uncertain_stage=MissingClient/Encode` | 首次因错误固定节点的那次尝试发生在发送调用之前，可检查这次新增 pin 分支；仍需结合 `head_accepted_seen` 等历史观测判断责任 |
| `head_pin_origin=accepted`、`head_accepted_seen=true` | 首次固定来自 Accepted；后续发生错误不会覆盖这个来源 |
| `head_first_uncertain_stage=SendCall/DecodeReply/...` | 已经调用发送函数，无法据此确认对端是否接管，不把它当作可以直接重选的证明 |
| `reason=retryable` 且队头已固定，`attempt_target_node_id` 与 `owner` 不同 | 固定节点正在拒绝/重试，而当前客户端归属已变化；用相同 Delivery ID 查接收端日志 |
| `reason=shared_no_replacement`、`candidates.same_group/eligible` | 区分原组/过滤器消失与消费者被 offline、handshaking、owner mismatch、avoid、no-local 等条件排除；拒绝计数允许重叠 |
| `selection.outcome` | 记录实际选择过程的退出原因，包括 chooser 无结果、返回离线、无效索引、等待期间路由被移除或归属改变；`selection.candidates` 是该次实际候选数 |
| `recovery_parent` | 接收节点产生本地恢复子任务时，关联旧 Delivery ID 与新的责任链 |
| `completed_behind_head` | 已完成但仍被未完成队头挡住、尚未从 FIFO 释放的条目数 |

`request_may_have_been_sent` 只描述日志中的这次尝试；false 不证明历史从未发送。`snapshot` 中的队列计数只读，未调用可能修正计数的 `stats()`。owner、候选及队列是不同时间点的本地快照，不具备原子一致性，也不能作为释放消息的依据。候选样本最多 6 个；日志不打印消息正文，错误详情限制 512 字符。

下一轮需要从 Broker 启动阶段保留所有 Broker 和 Operator 日志，并覆盖滚动及报错后的重试阶段，按完整 Delivery ID（含 origin/boot/sequence）串联。若重建进程，旧内存队列的缺失字段不能靠新日志追溯；本诊断用于定位重新出现的卡点。一次没有复现也不能证明问题不存在。

## 回归命令

```bash
cargo test --offline --locked -p rmqtt -p rmqtt-cluster-raft --features rmqtt/full --lib
cargo test --offline --locked -p rmqtt-cluster-raft reciprocal_shared_recovery_does_not_block_receiver_fifo -- --ignored --nocapture
cargo test --offline --locked -p rmqtt-cluster-raft shared_subscription_retirement_recovers_queued_delivery -- --ignored --nocapture
cargo test --offline --locked -p rmqtt-cluster-raft three_node_orphan_reconciliation -- --ignored --nocapture
cargo check --offline --locked -p rmqttd
```

三个 ignored 测试启动本机临时 Raft、gRPC 和 MQTT 节点，需要允许回环 TCP 监听，不连接业务集群。共享恢复测试固定选中 A，以一条未确认的 QoS 2 消息占满 A 的在途窗口，再让维护前排队的第二条消息在 A 退订后转交 B；同时核对原握手及其他订阅没有被重复转发。

该测试还在另一节点使用相同 Client ID 重连，实际执行带侧车的拉取式接管；检查队列上下文绑定到新会话，并再次退订验证消息能够交给 B，已经开始的 QoS 2 仍沿原 Packet ID 完成。

诊断增补后的本地验证结果：集群插件 74 项非 ignored 测试全部通过；两项三节点集成测试单独启用后全部通过；`cargo check --offline --locked -p rmqttd` 通过。新增验证覆盖诊断快照不修正计数、不释放消息，首次 pin 来源保留，发送前错误分类，候选选择退出原因及日志限频。吞吐及延迟的 A/B 压测尚未执行。

共享移交循环修复后的本地验证：`cargo test --offline --locked -p rmqtt -p rmqtt-cluster-raft --features rmqtt/full --lib` 共151项通过（rmqtt 53项、集群插件98项，另3项ignored）；三个回环集成测试单独启用后全部通过。新增双向循环回归验证同时重选后各客户端仅收到一份恢复消息、本地FIFO有序、pending/bytes/inflight归零；单元测试另覆盖满消息及字节额度、重复ID、父子责任、订阅选项、并发清理和调度取消。正常吞吐及p99的A/B压测尚未执行。
