#![deny(unsafe_code)]

use std::time::Duration;

use structopt::StructOpt;

use rmqtt::args::CommandArgs;
use rmqtt::context::ServerContext;
use rmqtt::net::{tls_provider, Builder};
use rmqtt::node::Node;
use rmqtt::server::MqttServer;
use rmqtt::Result;
use rmqtt_conf::{listener::Listener, Options, Settings};

mod logger;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(dead_code)]
mod plugin {
    include!(concat!(env!("OUT_DIR"), "/plugin.rs"));
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Options::from_args();
    if opts.version {
        println!("{} rustc/{}", Node::version(), Node::rustc_version());
        return Ok(());
    }

    // Register the OS signal listener before asynchronous broker/plugin
    // initialization so a deletion signal cannot be missed during startup.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        #[cfg(target_os = "windows")]
        tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
        #[cfg(not(target_os = "windows"))]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term_signal = signal(SignalKind::terminate()).expect("Failed to create signal handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term_signal.recv() => {}
            }
        }
        let _ = shutdown_tx.send(std::time::Instant::now());
    });

    //init config
    let conf = Settings::init(Options::from_args()).expect("settings init failed");

    //rustls crypto install default
    tls_provider::default_provider()
        .install_default()
        .expect("Failed to install the default Rustls crypto backend because it is already installed.");

    //init log
    let (_guard, _logger) = logger::logger_init(&conf.log).expect("logger init failed");

    //node info
    let node = Node::new(
        conf.node.id,
        conf.node.busy.loadavg,
        conf.node.busy.cpuloadavg,
        conf.node.busy.update_interval,
    );

    //print version
    log::info!("Starting rmqtt (version={}, rustc={})", Node::version(), Node::rustc_version());

    let _ = Settings::logs();

    //init ServerContext
    let (shutdown_enabled, shutdown_timeout, shutdown_finalize_reserve) = shutdown_config(conf);
    let scx = ServerContext::new()
        .args(config_args(conf))
        .node(node)
        .task_exec_workers(conf.task.exec_workers)
        .task_exec_queue_max(conf.task.exec_queue_max)
        .busy_check_enable(conf.node.busy.check_enable)
        .busy_handshaking_limit(conf.node.busy.handshaking)
        .mqtt_delayed_publish_max(conf.mqtt.delayed_publish_max)
        .mqtt_delayed_publish_immediate(conf.mqtt.delayed_publish_immediate)
        .mqtt_max_sessions(conf.mqtt.max_sessions)
        .shutdown(shutdown_enabled, shutdown_timeout, shutdown_finalize_reserve)
        .plugins_config_dir(conf.plugins.dir.as_str())
        .build()
        .await;

    //start gRPC server
    scx.node.start_grpc_server(scx.clone(), conf.rpc.server_addr, conf.rpc.reuseaddr, conf.rpc.reuseport);

    //register plugin
    plugin::registers(&scx, conf.plugins.default_startups.clone()).await.expect("register plugin failed");

    let mut builder = MqttServer::new(scx.clone());

    //tcp
    for (_, listen_cfg) in Settings::instance().listeners.tcps.iter() {
        builder = builder.listener(config_builder(listen_cfg).bind()?.tcp()?);
    }

    //tls
    for (_, listen_cfg) in Settings::instance().listeners.tlss.iter() {
        builder = builder.listener(config_builder(listen_cfg).bind()?.tls()?);
    }

    //websocket
    for (_, listen_cfg) in Settings::instance().listeners.wss.iter() {
        builder = builder.listener(config_builder(listen_cfg).bind()?.ws()?);
    }

    //tls-websocket
    for (_, listen_cfg) in Settings::instance().listeners.wsss.iter() {
        builder = builder.listener(config_builder(listen_cfg).bind()?.wss()?);
    }

    //MQTT over QUIC
    for (_, listen_cfg) in Settings::instance().listeners.quics.iter() {
        builder = builder.listener(config_builder(listen_cfg).bind_quic()?);
    }

    builder.build().start();

    let signaled_at = shutdown_rx.await.expect("Failed to receive the shutdown command");
    scx.shutdown.request("process_signal");
    if scx.shutdown.enabled() {
        let remaining = shutdown_timeout.saturating_sub(signaled_at.elapsed());
        let _ = scx.shutdown.bind(None, None, Some(remaining), None);
        log::info!(
            "graceful shutdown entered node_id={} pod_name={} status={}",
            scx.node.id(),
            scx.shutdown.pod_name(),
            scx.shutdown.status_json()
        );
        let wait = scx.shutdown.wait_until_complete_or_deadline();
        tokio::pin!(wait);
        let mut progress = tokio::time::interval(Duration::from_secs(10));
        progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The entry record above is the immediate first observation.
        progress.tick().await;
        let result = loop {
            tokio::select! {
                result = &mut wait => break result,
                _ = progress.tick() => {
                    log::info!(
                        "graceful shutdown waiting node_id={} pod_name={} status={}",
                        scx.node.id(),
                        scx.shutdown.pod_name(),
                        scx.shutdown.status_json()
                    );
                }
            }
        };
        log::info!(
            "graceful shutdown finished node_id={} pod_name={} status={result}",
            scx.node.id(),
            scx.shutdown.pod_name()
        );
        // Give the final control response and log record a bounded chance to
        // flush before exiting the process.
        tokio::time::sleep(Duration::from_millis(100)).await;
    } else {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Preserve the existing hard process-exit behavior. Dropping the Tokio
    // runtime can wait indefinitely for blocking tasks beyond this deadline.
    std::process::exit(0);
}

fn shutdown_config(conf: &Settings) -> (bool, Duration, Duration) {
    let enabled = std::env::var("RMQTT_GRACEFUL_SHUTDOWN_ENABLED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(conf.shutdown.enabled);
    let timeout = std::env::var("RMQTT_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(conf.shutdown.timeout);
    let finalize_reserve = std::env::var("RMQTT_GRACEFUL_SHUTDOWN_FINALIZE_RESERVE_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(conf.shutdown.finalize_reserve)
        .min(timeout);
    (enabled, timeout, finalize_reserve)
}

fn config_builder(cfg: &Listener) -> Builder {
    Builder::new()
        .name(cfg.name.as_str())
        .laddr(cfg.addr)
        .max_connections(cfg.max_connections)
        .max_handshaking_limit(cfg.max_handshaking_limit)
        .max_packet_size(cfg.max_packet_size.as_u32())
        .backlog(cfg.backlog)
        .nodelay(cfg.nodelay)
        .reuseaddr(cfg.reuseaddr)
        .reuseport(cfg.reuseport)
        .allow_anonymous(cfg.allow_anonymous)
        .min_keepalive(cfg.min_keepalive)
        .max_keepalive(cfg.max_keepalive)
        .allow_zero_keepalive(cfg.allow_zero_keepalive)
        .keepalive_backoff(cfg.keepalive_backoff)
        .max_inflight(cfg.max_inflight)
        .handshake_timeout(cfg.handshake_timeout)
        .max_mqueue_len(cfg.max_mqueue_len)
        .mqueue_rate_limit(cfg.mqueue_rate_limit.0, cfg.mqueue_rate_limit.1)
        .max_clientid_len(cfg.max_clientid_len)
        .max_qos_allowed(cfg.max_qos_allowed)
        .max_topic_levels(cfg.max_topic_levels)
        .session_expiry_interval(cfg.session_expiry_interval)
        .max_session_expiry_interval(cfg.max_session_expiry_interval)
        .message_retry_interval(cfg.message_retry_interval)
        .message_expiry_interval(cfg.message_expiry_interval)
        .max_subscriptions(cfg.max_subscriptions)
        .shared_subscription(cfg.shared_subscription)
        .max_topic_aliases(cfg.max_topic_aliases)
        .tls_cross_certificate(cfg.cross_certificate)
        .tls_cert(cfg.cert.clone())
        .tls_key(cfg.key.clone())
        .tls_client_ca_certs(cfg.client_ca_certs.clone())
        .limit_subscription(cfg.limit_subscription)
        .delayed_publish(cfg.delayed_publish)
        .proxy_protocol(cfg.proxy_protocol)
        .proxy_protocol_timeout(cfg.proxy_protocol_timeout)
        .cert_cn_as_username(cfg.cert_cn_as_username)
        .collect_cert_info(cfg.collect_cert_info)
        .idle_timeout(cfg.idle_timeout)
}

fn config_args(cfg: &Settings) -> CommandArgs {
    CommandArgs {
        node_id: cfg.opts.node_id,
        plugins_default_startups: cfg.opts.plugins_default_startups.clone(),
        node_grpc_addrs: cfg.opts.node_grpc_addrs.clone(),
        raft_peer_addrs: cfg.opts.raft_peer_addrs.clone(),
        raft_leader_id: cfg.opts.raft_leader_id,
    }
}
