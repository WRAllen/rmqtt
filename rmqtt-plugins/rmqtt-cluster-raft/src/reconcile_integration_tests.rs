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
