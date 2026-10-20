//! Opt-in loopback integration test. No external cluster or credentials.
//! cargo test -p rmqtt-cluster-raft --lib three_node_orphan_reconciliation -- --ignored --nocapture

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;
use rmqtt::net::Builder;
use rmqtt::server::MqttServer;
use rmqtt::types::{ClientId, SubscriptionOptions};

struct GatewayTrace;
static GATEWAY_TRACE: GatewayTrace = GatewayTrace;
static TRACE_LINES: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

impl log::Log for GatewayTrace {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Debug
            && (metadata.target().starts_with("rmqtt_cluster_raft") || metadata.target() == "rmqtt::session")
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            let line = record.args().to_string();
            if line.contains("gateway-clean-start") {
                let mut lines = TRACE_LINES.lock().unwrap();
                if lines.len() == 200 {
                    lines.remove(0);
                }
                lines.push(format!("{} {}", record.target(), line));
            }
        }
    }
    fn flush(&self) {}
}

struct DumpTraceOnFailure;
impl Drop for DumpTraceOnFailure {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for line in TRACE_LINES.lock().unwrap().iter() {
                eprintln!("{line}");
            }
        }
    }
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

fn varint(mut value: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 128;
        }
        bytes.push(byte);
        if value == 0 {
            return bytes;
        }
    }
}

fn utf8(value: &str) -> Vec<u8> {
    let mut out = (value.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(value.as_bytes());
    out
}

async fn send(stream: &mut TcpStream, header: u8, body: &[u8]) {
    let mut packet = vec![header];
    packet.extend(varint(body.len()));
    packet.extend_from_slice(body);
    stream.write_all(&packet).await.unwrap();
}

async fn frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    timeout(Duration::from_secs(10), async {
        let header = stream.read_u8().await.unwrap();
        let mut length = 0;
        let mut shift = 0;
        loop {
            let b = stream.read_u8().await.unwrap();
            length |= ((b & 127) as usize) << shift;
            if b < 128 {
                break;
            }
            shift += 7;
        }
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        (header, body)
    })
    .await
    .expect("MQTT packet timed out")
}

async fn connect(addr: SocketAddr, client_id: &str, clean_start: bool) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = utf8("MQTT");
    body.extend([5, if clean_start { 2 } else { 0 }, 0, 30, 5, 0x11]);
    body.extend(300u32.to_be_bytes());
    body.extend(utf8(client_id));
    send(&mut stream, 0x10, &body).await;
    let (header, ack) = frame(&mut stream).await;
    assert_eq!(header, 0x20, "expected CONNACK: {ack:?}");
    assert_eq!(ack[1], 0, "MQTT connection rejected: {ack:?}");
    stream
}

async fn subscribe(stream: &mut TcpStream, topic: &str) {
    let mut body = vec![0, 1, 0];
    body.extend(utf8(topic));
    body.push(2);
    send(stream, 0x82, &body).await;
    let (header, ack) = frame(stream).await;
    assert_eq!(header, 0x90);
    assert_eq!(ack.last(), Some(&2));
}

async fn publish(stream: &mut TcpStream, topic: &str, payload: &str, id: u16, qos: u8) {
    let mut body = utf8(topic);
    body.extend(id.to_be_bytes());
    body.push(0);
    body.extend(payload.as_bytes());
    send(stream, 0x30 | (qos << 1), &body).await;
    let (header, ack) = frame(stream).await;
    assert_eq!(header, if qos == 1 { 0x40 } else { 0x50 });
    assert_eq!(&ack[..2], &id.to_be_bytes());
    if qos == 2 {
        send(stream, 0x62, &id.to_be_bytes()).await;
        assert_eq!(frame(stream).await.0, 0x70);
    }
}

async fn receive(stream: &mut TcpStream, expected: &str) {
    let (header, body) = frame(stream).await;
    assert_eq!(header >> 4, 3, "expected PUBLISH: {body:?}");
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let qos = (header >> 1) & 3;
    let mut cursor = 2 + topic_len;
    let id = if qos > 0 {
        let id = [body[cursor], body[cursor + 1]];
        cursor += 2;
        Some(id)
    } else {
        None
    };
    let mut property_len = 0;
    let mut shift = 0;
    loop {
        let b = body[cursor];
        cursor += 1;
        property_len |= ((b & 127) as usize) << shift;
        if b < 128 {
            break;
        }
        shift += 7;
    }
    cursor += property_len;
    assert_eq!(&body[cursor..], expected.as_bytes());
    if let Some(id) = id {
        send(stream, if qos == 1 { 0x40 } else { 0x50 }, &id).await;
        if qos == 2 {
            assert_eq!(frame(stream).await.0, 0x62);
            send(stream, 0x70, &id).await;
        }
    }
}

async fn until<F: Fn() -> bool>(description: &str, condition: F) {
    timeout(Duration::from_secs(25), async {
        while !condition() {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {description}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts three real Raft/gRPC/MQTT nodes on loopback"]
async fn three_node_orphan_reconciliation() {
    let _ = log::set_logger(&GATEWAY_TRACE);
    log::set_max_level(log::LevelFilter::Debug);
    let _trace_on_failure = DumpTraceOnFailure;
    let grpc = [free_addr(), free_addr(), free_addr()];
    let raft = [free_addr(), free_addr(), free_addr()];
    let mqtt = [free_addr(), free_addr(), free_addr()];
    let mut plugins = Vec::new();
    for index in 0..3 {
        let id = index + 1;
        let grpc_list =
            (0..=index).map(|i| format!("\"{}@{}\"", i + 1, grpc[i])).collect::<Vec<_>>().join(",");
        let raft_list =
            (0..=index).map(|i| format!("\"{}@{}\"", i + 1, raft[i])).collect::<Vec<_>>().join(",");
        let config = format!(
            r#"
node_grpc_addrs = [{grpc_list}]
raft_peer_addrs = [{raft_list}]
leader_id = 1
bootstrap = {}
worker_threads = 2
node_grpc_client_timeout = "5s"
task_exec_queue_workers = 16
handoff.retry_interval = "20ms"
handoff.retry_max_interval = "100ms"
raft.grpc_timeout = "2s"
raft.proposal_batch_timeout = "10ms"
"#,
            index == 0
        );
        let scx = ServerContext::new()
            .node_id(id as u64)
            .busy_check_enable(false)
            .plugins_config_map_add("rmqtt-cluster-raft", config)
            .build()
            .await;
        scx.node.start_grpc_server(scx.clone(), grpc[index], false, false);
        let mut plugin = ClusterPlugin::new(scx.clone(), "rmqtt-cluster-raft").await.unwrap();
        plugin.init().await.unwrap();
        plugin.start().await.unwrap();
        let listener = Builder::new()
            .name("integration/tcp")
            .laddr(mqtt[index])
            .allow_anonymous(true)
            .max_qos_allowed(rmqtt::types::QoS::ExactlyOnce)
            .message_retry_interval(Duration::from_millis(200))
            .bind()
            .unwrap()
            .tcp()
            .unwrap();
        MqttServer::new(scx).listener_by_id(listener, 1883).build().start();
        plugins.push(plugin);
    }
    eprintln!("three Raft and MQTT nodes started");
    until("all business peers replicated", || plugins.iter().all(|p| p.nodes.snapshot_nodes().len() == 3))
        .await;
    sleep(Duration::from_millis(500)).await;

    let mut publisher = connect(mqtt[0], "publisher", true).await;
    let mut publisher2 = connect(mqtt[1], "publisher-2", true).await;
    // Model cancellation after a destination installed the local snapshot
    // but before ownership/routes were published. Retries must finish the
    // metadata and must not enqueue the snapshot's messages a second time.
    let mut partial = connect(mqtt[1], "partial-install", false).await;
    subscribe(&mut partial, "test/partial").await;
    send(&mut partial, 0xe0, &[]).await;
    drop(partial);
    sleep(Duration::from_millis(200)).await;
    publish(&mut publisher, "test/partial", "one-copy", 90, 1).await;
    sleep(Duration::from_millis(200)).await;
    let partial_id = plugins[1].router.id("partial-install").unwrap();
    let partial_session = plugins[1].shared.inner().entry(partial_id.clone()).session().unwrap();
    assert_eq!(partial_session.deliver_queue().len(), 1);
    let mut options = SubscriptionOptions::default();
    options.set_qos(rmqtt::types::QoS::ExactlyOnce);
    let snapshot = rmqtt::session::OfflineInfo {
        id: partial_id.relocated_to(3),
        subscriptions: vec![("test/partial".into(), options)],
        offline_messages: std::collections::VecDeque::from([(
            From::from_custom(Id::from(1, "publisher".into())),
            Publish::from(rmqtt::types::CodecPublish {
                dup: false,
                retain: false,
                qos: rmqtt::types::QoS::AtLeastOnce,
                topic: "test/partial".into(),
                packet_id: None,
                payload: bytes::Bytes::from_static(b"one-copy"),
                properties: None,
            }),
        )]),
        inflight_messages: Vec::new(),
        created_at: partial_session.created_at().await.unwrap(),
    };
    let handoff = message::SessionHandoff {
        offline: snapshot,
        connect_info: partial_session.connect_info().await.unwrap().relocated_to(3),
        superuser: false,
        session_expiry_millis: 300_000,
    };
    let mailbox = plugins[0].raft_mailbox().await;
    mailbox
        .send_proposal(message::Message::Connected { id: handoff.offline.id.clone() }.encode().unwrap())
        .await
        .unwrap();
    until("partial installation fixture replicated", || {
        plugins[1].router.id("partial-install") == Some(handoff.offline.id.clone())
    })
    .await;
    for _ in 0..2 {
        let reply = plugins[1].shared.accept_session_handoff(handoff.clone()).await.unwrap();
        assert!(reply.installed);
        assert_eq!(reply.destination_id, partial_id);
    }
    let mut partial = connect(mqtt[0], "partial-install", false).await;
    receive(&mut partial, "one-copy").await;
    assert!(
        timeout(Duration::from_millis(300), partial.read_u8()).await.is_err(),
        "snapshot retry duplicated a message"
    );
    drop(partial);
    eprintln!("partial installation retry repaired metadata without replaying messages");
    let mut live = connect(mqtt[1], "live-subscriber", true).await;
    subscribe(&mut live, "test/live").await;
    let mut offline = connect(mqtt[1], "offline-subscriber", false).await;
    subscribe(&mut offline, "test/offline").await;
    send(&mut offline, 0xe0, &[]).await;
    drop(offline);
    sleep(Duration::from_millis(200)).await;
    let offline_id = plugins[1].router.id("offline-subscriber").unwrap();
    assert!(!plugins[1]
        .shared
        .reconcile_handoff_session(&offline_id.client_id, Some(&offline_id))
        .await
        .unwrap());
    publish(&mut publisher, "test/offline", "offline-message", 1, 1).await;
    let mut gateway = connect(mqtt[2], "gateway-clean-start", true).await;
    subscribe(&mut gateway, "test/gateway").await;
    publish(&mut publisher, "test/gateway", "gateway-before-drain", 3, 1).await;
    until("gateway delivery retained until MQTT acknowledgement", || {
        plugins[2].scx.stats.out_inflights.count() > 0
    })
    .await;

    // Reproduce the production failure: route still points at node 3, while
    // the replicated owner is node 2 and no physical MQTT session exists.
    let stale = Id::new(3, 1883, None, None, ClientId::from_static("gone-canary"), None);
    let ghost = stale.relocated_to(2);
    let mailbox = plugins[0].raft_mailbox().await;
    for message in [
        message::Message::Connected { id: stale.clone() },
        message::Message::Add {
            topic_filter: "test/orphan",
            id: stale.clone(),
            opts: SubscriptionOptions::default(),
        },
        message::Message::Connected { id: ghost.clone() },
        message::Message::Disconnected { id: ghost.clone() },
    ] {
        mailbox.send_proposal(message.encode().unwrap()).await.unwrap();
    }
    until("ghost owner replicated", || {
        plugins.iter().all(|p| p.router.id("gone-canary") == Some(ghost.clone()))
    })
    .await;
    for plugin in &plugins {
        plugin.shared.activate_drain(3);
    }
    publish(&mut publisher, "test/orphan", "probe-message", 2, 1).await;
    publish(&mut publisher2, "test/orphan", "second-probe-message", 2, 1).await;
    until("orphan entered both reliable queues", || {
        plugins[0].shared.handoff_pending_for_node(3) == 1
            && plugins[1].shared.handoff_pending_for_node(3) == 1
    })
    .await;
    plugins[2].drain_node(&plugins[2].raft_mailbox().await).await.unwrap();
    drop(gateway);
    until("gateway session relocated", || !plugins[2].shared.inner().exist("gateway-clean-start")).await;
    let mut gateway = connect(mqtt[0], "gateway-clean-start", true).await;
    receive(&mut gateway, "gateway-before-drain").await;
    publish(&mut publisher, "test/gateway", "gateway-after-drain", 4, 2).await;
    receive(&mut gateway, "gateway-after-drain").await;
    eprintln!("Clean Start gateway reconnect preserved unacknowledged message and subscription");
    eprintln!("stale owner and blocked handoff reproduced; exercising live QoS 1/2 traffic");

    for sequence in 0..60 {
        let body = format!("live-{sequence}");
        publish(&mut publisher, "test/live", &body, sequence + 10, if sequence % 2 == 0 { 1 } else { 2 })
            .await;
        receive(&mut live, &body).await;
        sleep(Duration::from_millis(100)).await;
    }
    until("orphan handoff retired during active drain", || {
        plugins[0].shared.handoff_pending_for_node(3) == 0
            && plugins[1].shared.handoff_pending_for_node(3) == 0
    })
    .await;
    assert!(plugins[2].draining.load(Ordering::SeqCst));
    assert!(plugins.iter().all(|p| p.router.id("gone-canary").is_none()));
    assert!(plugins.iter().all(|p| !p.shared.handoff_data_loss_detected()));
    assert!(plugins[1].shared.inner().exist("offline-subscriber"));
    let mut restored = connect(mqtt[0], "offline-subscriber", false).await;
    receive(&mut restored, "offline-message").await;
    eprintln!(
        "PASS: orphan retired, 60 QoS 1/2 messages received in order, offline session/message restored"
    );

    drop((publisher, publisher2, live, restored, gateway));
    for plugin in &plugins {
        plugin.raft_mailbox().await.shutdown().await.unwrap();
    }
}

async fn connect_receive_max(
    addr: SocketAddr,
    client_id: &str,
    clean_start: bool,
    receive_max: u16,
) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = utf8("MQTT");
    // MQTT 5 Clean Start, Session Expiry Interval and Receive Maximum.
    body.extend([5, if clean_start { 2 } else { 0 }, 0, 30, 8, 0x11]);
    body.extend(300u32.to_be_bytes());
    body.push(0x21);
    body.extend(receive_max.to_be_bytes());
    body.extend(utf8(client_id));
    send(&mut stream, 0x10, &body).await;
    let (header, ack) = frame(&mut stream).await;
    assert_eq!(header, 0x20, "expected CONNACK: {ack:?}");
    assert_eq!(ack[1], 0, "MQTT connection rejected: {ack:?}");
    stream
}

async fn receive_qos2_without_ack(stream: &mut TcpStream, expected: &str) -> [u8; 2] {
    let (header, body) = frame(stream).await;
    assert_eq!(header >> 4, 3, "expected PUBLISH: {body:?}");
    assert_eq!((header >> 1) & 3, 2, "expected QoS 2 PUBLISH");
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2 + topic_len;
    let packet_id = [body[cursor], body[cursor + 1]];
    cursor += 2;
    let mut property_len = 0;
    let mut shift = 0;
    loop {
        let byte = body[cursor];
        cursor += 1;
        property_len |= ((byte & 127) as usize) << shift;
        if byte < 128 {
            break;
        }
        shift += 7;
    }
    cursor += property_len;
    assert_eq!(&body[cursor..], expected.as_bytes());
    packet_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts three real Raft/gRPC/MQTT nodes on loopback"]
async fn reciprocal_shared_recovery_does_not_block_receiver_fifo() {
    // Bound the entire fixture, including chooser barriers and shutdown. The
    // broken implementation must fail rather than leave a blocked test worker.
    timeout(Duration::from_secs(90), async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        use rmqtt::router::Router as _;
        use rmqtt::types::{CodecPublish, IsOnline, QoS, SubscriptionIdentifier};
        use tokio::sync::Semaphore;

        struct GateFirstRecovery {
            entered: Arc<AtomicUsize>,
            release: Arc<Semaphore>,
            first: AtomicBool,
        }

        #[async_trait::async_trait]
        impl rmqtt::subscribe::SharedSubscription for GateFirstRecovery {
            async fn choice(
                &self,
                _scx: &ServerContext,
                candidates: &[(
                    NodeId,
                    ClientId,
                    SubscriptionOptions,
                    Option<Vec<SubscriptionIdentifier>>,
                    Option<IsOnline>,
                )],
            ) -> Option<(usize, IsOnline)> {
                if self.first.swap(false, Ordering::SeqCst) {
                    self.entered.fetch_add(1, Ordering::SeqCst);
                    self.release.acquire().await.expect("recovery gate closed").forget();
                }
                // The original consumer is excluded by recovery selection, so
                // the only eligible shared member is on the opposite broker.
                assert_eq!(candidates.len(), 1);
                Some((0, candidates[0].4.unwrap_or(true)))
            }
        }

        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let grpc = [free_addr(), free_addr(), free_addr()];
        let raft = [free_addr(), free_addr(), free_addr()];
        let mqtt = [free_addr(), free_addr(), free_addr()];
        let mut plugins = Vec::new();
        for index in 0..3 {
            let grpc_list =
                (0..=index).map(|i| format!("\"{}@{}\"", i + 1, grpc[i])).collect::<Vec<_>>().join(",");
            let raft_list =
                (0..=index).map(|i| format!("\"{}@{}\"", i + 1, raft[i])).collect::<Vec<_>>().join(",");
            let config = format!(
                r#"
node_grpc_addrs = [{grpc_list}]
raft_peer_addrs = [{raft_list}]
leader_id = 1
bootstrap = {}
worker_threads = 2
node_grpc_client_timeout = "5s"
task_exec_queue_workers = 16
handoff.retry_interval = "20ms"
handoff.retry_max_interval = "100ms"
raft.grpc_timeout = "2s"
raft.proposal_batch_timeout = "10ms"
"#,
                index == 0
            );
            let scx = ServerContext::new()
                .node_id((index + 1) as u64)
                .busy_check_enable(false)
                .plugins_config_map_add("rmqtt-cluster-raft", config)
                .build()
                .await;
            scx.node.start_grpc_server(scx.clone(), grpc[index], false, false);
            let mut plugin = ClusterPlugin::new(scx.clone(), "rmqtt-cluster-raft").await.unwrap();
            plugin.init().await.unwrap();
            plugin.start().await.unwrap();
            *scx.extends.shared_subscription_mut().await = Box::new(GateFirstRecovery {
                entered: entered.clone(),
                release: release.clone(),
                first: AtomicBool::new(true),
            });
            let listener = Builder::new()
                .name("shared-recovery-cycle/tcp")
                .laddr(mqtt[index])
                .allow_anonymous(true)
                .shared_subscription(true)
                .max_qos_allowed(QoS::ExactlyOnce)
                .message_retry_interval(Duration::from_secs(60))
                .bind()
                .unwrap()
                .tcp()
                .unwrap();
            MqttServer::new(scx).listener_by_id(listener, 1883).build().start();
            plugins.push(plugin);
        }
        until("reciprocal recovery business peers replicated", || {
            plugins.iter().all(|plugin| plugin.nodes.snapshot_nodes().len() == 3)
        })
        .await;
        sleep(Duration::from_millis(500)).await;

        const TOPIC: &str = "test/shared-recovery-cycle";
        const SHARED: &str = "$share/workers/test/shared-recovery-cycle";
        const CLIENT_A: &str = "cycle-consumer-a";
        const CLIENT_B: &str = "cycle-consumer-b";
        let mut consumer_a = connect(mqtt[1], CLIENT_A, true).await;
        let mut consumer_b = connect(mqtt[2], CLIENT_B, true).await;
        for consumer in [&mut consumer_a, &mut consumer_b] {
            let mut body = vec![0, 1, 0];
            body.extend(utf8(SHARED));
            body.push(1);
            send(consumer, 0x82, &body).await;
            let (header, ack) = frame(consumer).await;
            assert_eq!(header, 0x90);
            assert_eq!(ack.last(), Some(&1));
        }
        until("reciprocal recovery routes and owners replicated", || {
            plugins.iter().all(|plugin| {
                plugin.router.relations().get(TOPIC).is_some_and(|routes| {
                    routes.len() == 2
                        && plugin.router.get_ready_target_node_id(CLIENT_A) == Some(2)
                        && plugin.router.get_ready_target_node_id(CLIENT_B) == Some(3)
                })
            })
        })
        .await;

        let from = From::from_custom(Id::from(1, "cycle-publisher".into()));
        let make_publish = |payload: &'static str| {
            Publish::from(CodecPublish {
                dup: false,
                retain: false,
                qos: QoS::ExactlyOnce,
                topic: TOPIC.into(),
                packet_id: None,
                payload: bytes::Bytes::from_static(payload.as_bytes()),
                properties: None,
            })
        };
        let make_relation = |index: usize, client: &str, stale: bool| {
            let mut opts =
                plugins[index].router.relations().get(TOPIC).unwrap().get(client).unwrap().1.clone();
            if stale {
                // A live subscriber changed its QoS after this accepted
                // obligation captured its relation. Both original heads must
                // therefore select the other consumer while remaining owed.
                opts.set_qos(QoS::ExactlyOnce);
            }
            (
                TOPIC.into(),
                ClientId::from(client.to_owned()),
                opts,
                None,
                Some(("workers".into(), true, vec![CLIENT_A.into(), CLIENT_B.into()])),
            )
        };
        let recoveries =
            [(1, CLIENT_A, "recovery-from-a", 2, 1001), (2, CLIENT_B, "recovery-from-b", 3, 1002)].map(
                |(index, client, payload, origin_node_id, sequence)| {
                    (
                        index,
                        message::DeliveryId { origin_node_id, boot_id: 0xba22_1e22, sequence },
                        ClientId::from(client),
                        make_publish(payload),
                        vec![make_relation(index, client, true)],
                    )
                },
            );
        for (index, delivery_id, client, publish, relations) in &recoveries {
            assert!(matches!(
                plugins[*index]
                    .shared
                    .accept_reliable_forward(
                        *delivery_id,
                        client.clone(),
                        from.clone(),
                        publish.clone(),
                        relations.clone(),
                    )
                    .await,
                message::ReliableForwardStatus::Accepted
            ));
        }
        until("both reciprocal queue heads reached consumer reselection", || {
            entered.load(Ordering::SeqCst) == 2
        })
        .await;

        // These ordinary local-target obligations share each receiver's FIFO.
        // They must remain ordered even while its independent recovery head
        // waits for a remote Accepted reply. Both recovery heads are held by
        // the gate until these local prefixes have been enqueued.
        for (index, client, payload, sequence) in [
            (1, CLIENT_A, "local-a-first", 1),
            (1, CLIENT_A, "local-a-second", 2),
            (2, CLIENT_B, "local-b-first", 3),
            (2, CLIENT_B, "local-b-second", 4),
        ] {
            assert!(matches!(
                plugins[index]
                    .shared
                    .accept_reliable_forward(
                        message::DeliveryId { origin_node_id: 1, boot_id: 0xfa17_0001, sequence },
                        client.into(),
                        from.clone(),
                        make_publish(payload),
                        vec![make_relation(index, client, false)],
                    )
                    .await,
                message::ReliableForwardStatus::Accepted
            ));
        }
        release.add_permits(2);

        let ((), ()) = tokio::join!(
            async {
                receive(&mut consumer_a, "local-a-first").await;
                receive(&mut consumer_a, "local-a-second").await;
                receive(&mut consumer_a, "recovery-from-b").await;
            },
            async {
                receive(&mut consumer_b, "local-b-first").await;
                receive(&mut consumer_b, "local-b-second").await;
                receive(&mut consumer_b, "recovery-from-a").await;
            },
        );
        until("reciprocal shared obligations and MQTT inflights all completed", || {
            plugins.iter().all(|plugin| {
                let stats = plugin.shared.handoff_stats();
                stats.pending_messages == 0
                    && stats.pending_bytes == 0
                    && plugin.scx.stats.out_inflights.count() == 0
            })
        })
        .await;
        for (index, delivery_id, client, publish, relations) in recoveries {
            assert!(matches!(
                plugins[index]
                    .shared
                    .accept_reliable_forward(delivery_id, client, from.clone(), publish, relations)
                    .await,
                message::ReliableForwardStatus::Duplicate
            ));
        }
        for (name, consumer) in [("A", &mut consumer_a), ("B", &mut consumer_b)] {
            assert!(
                timeout(Duration::from_millis(300), consumer.read_u8()).await.is_err(),
                "unexpected duplicate delivery to reciprocal consumer {name}"
            );
        }
        assert!(plugins.iter().all(|plugin| !plugin.shared.handoff_data_loss_detected()));
        eprintln!("reciprocal shared recovery completed once per consumer; local receiver FIFO retained");
        drop((consumer_a, consumer_b));
        for plugin in &plugins {
            plugin.raft_mailbox().await.shutdown().await.unwrap();
        }
    })
    .await
    .expect("reciprocal shared recovery fixture exceeded 90 seconds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts three loopback nodes"]
async fn shared_subscription_retirement_recovers_queued_delivery() {
    use rmqtt::router::Router as _;
    use rmqtt::types::{IsOnline, QoS, SubscriptionIdentifier};

    struct PreferRetiringConsumer;
    #[async_trait::async_trait]
    impl rmqtt::subscribe::SharedSubscription for PreferRetiringConsumer {
        async fn choice(
            &self,
            _scx: &ServerContext,
            candidates: &[(
                NodeId,
                ClientId,
                SubscriptionOptions,
                Option<Vec<SubscriptionIdentifier>>,
                Option<IsOnline>,
            )],
        ) -> Option<(usize, IsOnline)> {
            let index = candidates
                .iter()
                .position(|(_, client, _, _, _)| client == "retiring-consumer")
                .or_else(|| (!candidates.is_empty()).then_some(0))?;
            Some((index, candidates[index].4.unwrap_or(true)))
        }
    }

    let grpc = [free_addr(), free_addr(), free_addr()];
    let raft = [free_addr(), free_addr(), free_addr()];
    let mqtt = [free_addr(), free_addr(), free_addr()];
    let mut plugins = Vec::new();
    for index in 0..3 {
        let grpc_list =
            (0..=index).map(|i| format!("\"{}@{}\"", i + 1, grpc[i])).collect::<Vec<_>>().join(",");
        let raft_list =
            (0..=index).map(|i| format!("\"{}@{}\"", i + 1, raft[i])).collect::<Vec<_>>().join(",");
        let config = format!(
            r#"
node_grpc_addrs = [{grpc_list}]
raft_peer_addrs = [{raft_list}]
leader_id = 1
bootstrap = {}
worker_threads = 2
node_grpc_client_timeout = "5s"
task_exec_queue_workers = 16
handoff.retry_interval = "20ms"
handoff.retry_max_interval = "100ms"
raft.grpc_timeout = "2s"
raft.proposal_batch_timeout = "10ms"
"#,
            index == 0
        );
        let scx = ServerContext::new()
            .node_id((index + 1) as u64)
            .busy_check_enable(false)
            .plugins_config_map_add("rmqtt-cluster-raft", config)
            .build()
            .await;
        scx.node.start_grpc_server(scx.clone(), grpc[index], false, false);
        let mut plugin = ClusterPlugin::new(scx.clone(), "rmqtt-cluster-raft").await.unwrap();
        plugin.init().await.unwrap();
        plugin.start().await.unwrap();
        *scx.extends.shared_subscription_mut().await = Box::new(PreferRetiringConsumer);
        let listener = Builder::new()
            .name("shared-recovery/tcp")
            .laddr(mqtt[index])
            .allow_anonymous(true)
            .shared_subscription(true)
            .max_qos_allowed(QoS::ExactlyOnce)
            // Keep the intentionally unacknowledged first PUBLISH quiet while
            // checking that only the unsent second message changes consumer.
            .message_retry_interval(Duration::from_secs(60))
            .bind()
            .unwrap()
            .tcp()
            .unwrap();
        MqttServer::new(scx).listener_by_id(listener, 1883).build().start();
        plugins.push(plugin);
    }
    until("shared recovery business peers replicated", || {
        plugins.iter().all(|plugin| plugin.nodes.snapshot_nodes().len() == 3)
    })
    .await;
    sleep(Duration::from_millis(500)).await;

    const TOPIC: &str = "test/shared-retirement";
    const SHARED: &str = "$share/workers/test/shared-retirement";
    let mut publisher = connect(mqtt[0], "shared-recovery-publisher", true).await;
    let mut retiring = connect_receive_max(mqtt[1], "retiring-consumer", true, 1).await;
    let mut replacement = connect(mqtt[2], "replacement-consumer", true).await;
    let mut ordinary = connect(mqtt[2], "ordinary-consumer", true).await;
    let mut other_group = connect(mqtt[2], "other-group-consumer", true).await;
    subscribe(&mut retiring, SHARED).await;
    subscribe(&mut replacement, SHARED).await;
    subscribe(&mut ordinary, TOPIC).await;
    subscribe(&mut other_group, "$share/other-workers/test/shared-retirement").await;
    until("shared subscribers and owners replicated", || {
        plugins.iter().all(|plugin| {
            plugin.router.relations().get(TOPIC).is_some_and(|routes| {
                routes.len() == 4
                    && [
                        "retiring-consumer",
                        "replacement-consumer",
                        "ordinary-consumer",
                        "other-group-consumer",
                    ]
                    .iter()
                    .all(|client| plugin.router.get_ready_target_node_id(client).is_some())
            })
        })
    })
    .await;

    publish(&mut publisher, TOPIC, "already-started", 1, 2).await;
    let first_packet_id = receive_qos2_without_ack(&mut retiring, "already-started").await;
    receive(&mut ordinary, "already-started").await;
    receive(&mut other_group, "already-started").await;

    publish(&mut publisher, TOPIC, "queued-before-maintenance", 2, 2).await;
    receive(&mut ordinary, "queued-before-maintenance").await;
    receive(&mut other_group, "queued-before-maintenance").await;
    let retiring_id = plugins[1].router.id("retiring-consumer").unwrap();
    let session = plugins[1].shared.inner().entry(retiring_id.clone()).session().unwrap();
    until("Receive Maximum holds second message in delivery queue", || session.deliver_queue().len() == 1)
        .await;
    assert_eq!(session.out_inflight().read().await.len(), 1);
    // With credit exhausted, the actor cannot consume this queue item. Inspect
    // and restore the same item to prove pre-maintenance context was retained.
    let pending = session.deliver_queue().pop().unwrap();
    assert_eq!(&pending.1.payload[..], b"queued-before-maintenance");
    assert!(pending.1.shared_delivery_not_started());
    let context = pending.1.shared_delivery.as_ref().expect("shared delivery context before maintenance");
    assert_eq!(context.group, "workers");
    assert_eq!(context.topic_filter, TOPIC);
    assert_eq!(context.target_id, retiring_id);
    session.deliver_queue().push(pending).unwrap();
    assert!(plugins.iter().all(|plugin| !plugin.shared.shared_delivery_recovery_enabled()));

    for plugin in &plugins {
        plugin.shared.prepare_drain(2, Some(vec![1, 3])).await.unwrap();
    }
    assert!(plugins.iter().all(|plugin| plugin.shared.shared_delivery_recovery_enabled()));
    let mut unsubscribe = vec![0, 9, 0];
    unsubscribe.extend(utf8(SHARED));
    send(&mut retiring, 0xa2, &unsubscribe).await;
    let (header, ack) = frame(&mut retiring).await;
    assert_eq!(header, 0xb0, "expected UNSUBACK while old QoS 2 remains inflight: {ack:?}");
    assert_eq!(&ack[..2], &[0, 9]);
    assert_eq!(ack.last(), Some(&0));

    receive(&mut replacement, "queued-before-maintenance").await;
    assert!(session.deliver_queue().is_empty());
    assert_eq!(session.out_inflight().read().await.len(), 1);

    // The first message has started QoS 2 with A and must finish there even
    // after UNSUBSCRIBE. Only the never-sent second message was reassigned.
    send(&mut retiring, 0x50, &first_packet_id).await;
    let (header, release) = frame(&mut retiring).await;
    assert_eq!(header, 0x62);
    assert_eq!(&release[..2], &first_packet_id);
    send(&mut retiring, 0x70, &first_packet_id).await;
    until("all MQTT inflights acknowledged", || {
        plugins.iter().all(|plugin| plugin.scx.stats.out_inflights.count() == 0)
    })
    .await;
    for (name, stream) in [
        ("replacement", &mut replacement),
        ("ordinary", &mut ordinary),
        ("other group", &mut other_group),
        ("retiring", &mut retiring),
    ] {
        assert!(
            timeout(Duration::from_millis(300), stream.read_u8()).await.is_err(),
            "unexpected duplicate delivery to {name}"
        );
    }
    eprintln!("queued shared message recovered to B; started QoS 2 completed on A; fan-out was not replayed");

    // Exercise the separate pull path used by an ordinary cross-node MQTT
    // reconnect, with no maintenance active while the snapshot is extracted.
    for plugin in &plugins {
        plugin.shared.clear_drain(2);
    }
    subscribe(&mut retiring, SHARED).await;
    until("retiring member resubscribed on every router", || {
        plugins.iter().all(|plugin| {
            plugin.router.relations().get(TOPIC).is_some_and(|routes| {
                routes
                    .get("retiring-consumer")
                    .is_some_and(|(_, opts)| opts.shared_group().is_some_and(|group| group == "workers"))
            })
        })
    })
    .await;
    publish(&mut publisher, TOPIC, "pull-already-started", 3, 2).await;
    let pull_first_packet_id = receive_qos2_without_ack(&mut retiring, "pull-already-started").await;
    receive(&mut ordinary, "pull-already-started").await;
    receive(&mut other_group, "pull-already-started").await;
    publish(&mut publisher, TOPIC, "pull-queued-before-maintenance", 4, 2).await;
    receive(&mut ordinary, "pull-queued-before-maintenance").await;
    receive(&mut other_group, "pull-queued-before-maintenance").await;
    until("pull fixture has an unsent shared delivery", || session.deliver_queue().len() == 1).await;
    assert_eq!(session.out_inflight().read().await.len(), 1);
    send(&mut retiring, 0xe0, &[]).await;
    drop(retiring);
    until("old retiring connection became offline", || {
        plugins[1].router.get_ready_target_node_id("retiring-consumer").is_none()
    })
    .await;

    let mut retiring = connect_receive_max(mqtt[0], "retiring-consumer", false, 1).await;
    let resumed_packet_id = receive_qos2_without_ack(&mut retiring, "pull-already-started").await;
    assert_eq!(resumed_packet_id, pull_first_packet_id, "QoS 2 retains its packet identity across pull");
    until("pull installed new session owner", || {
        plugins.iter().all(|plugin| plugin.router.get_ready_target_node_id("retiring-consumer") == Some(1))
    })
    .await;
    let pulled_id = plugins[0].router.id("retiring-consumer").unwrap();
    let pulled_session = plugins[0].shared.inner().entry(pulled_id.clone()).session().unwrap();
    until("pulled shared delivery still waits for MQTT credit", || pulled_session.deliver_queue().len() == 1)
        .await;
    let pulled_pending = pulled_session.deliver_queue().pop().unwrap();
    assert_eq!(&pulled_pending.1.payload[..], b"pull-queued-before-maintenance");
    assert!(pulled_pending.1.shared_delivery_not_started());
    let pulled_context = pulled_pending
        .1
        .shared_delivery
        .as_ref()
        .expect("KickWithShared must preserve queued delivery context");
    assert_eq!(pulled_context.group, "workers");
    assert_eq!(pulled_context.topic_filter, TOPIC);
    assert_eq!(pulled_context.target_id, pulled_id, "pull context must bind to the new connection");
    assert_ne!(pulled_context.subscription_generation, 0);
    assert!(pulled_session.shared_delivery_context_valid(&pulled_pending.1).await);
    pulled_session.deliver_queue().push(pulled_pending).unwrap();

    for plugin in &plugins {
        plugin.shared.prepare_drain(1, Some(vec![2, 3])).await.unwrap();
    }
    let mut unsubscribe = vec![0, 10, 0];
    unsubscribe.extend(utf8(SHARED));
    send(&mut retiring, 0xa2, &unsubscribe).await;
    let (header, ack) = frame(&mut retiring).await;
    assert_eq!(header, 0xb0, "expected UNSUBACK after cross-node pull: {ack:?}");
    assert_eq!(&ack[..2], &[0, 10]);
    assert_eq!(ack.last(), Some(&0));
    receive(&mut replacement, "pull-queued-before-maintenance").await;
    assert!(pulled_session.deliver_queue().is_empty());
    assert_eq!(pulled_session.out_inflight().read().await.len(), 1);
    send(&mut retiring, 0x50, &resumed_packet_id).await;
    let (header, release) = frame(&mut retiring).await;
    assert_eq!(header, 0x62);
    assert_eq!(&release[..2], &resumed_packet_id);
    send(&mut retiring, 0x70, &resumed_packet_id).await;
    until("pulled MQTT inflights acknowledged", || {
        plugins.iter().all(|plugin| plugin.scx.stats.out_inflights.count() == 0)
    })
    .await;
    for (name, stream) in [
        ("replacement after pull", &mut replacement),
        ("ordinary after pull", &mut ordinary),
        ("other group after pull", &mut other_group),
        ("retiring after pull", &mut retiring),
    ] {
        assert!(
            timeout(Duration::from_millis(300), stream.read_u8()).await.is_err(),
            "unexpected duplicate delivery to {name}"
        );
    }
    eprintln!("PASS: local retirement and cross-node pull preserve/recover queued shared deliveries without replaying started QoS 2 or unrelated fan-out");
    drop((publisher, retiring, replacement, ordinary, other_group));
    for plugin in &plugins {
        plugin.raft_mailbox().await.shutdown().await.unwrap();
    }
}
