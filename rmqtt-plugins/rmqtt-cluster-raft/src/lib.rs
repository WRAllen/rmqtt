#![deny(unsafe_code)]
#![recursion_limit = "256"]

use std::collections::HashSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use config::PluginConfig;
use handler::HookHandler;
use rust_box::task_exec_queue::TaskExecQueue;
use serde_json::{self, json};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, timeout};

use nodes::ClusterNodeManager;
use rmqtt::{
    context::ServerContext,
    hook::{Register, Type},
    macros::Plugin,
    plugin::{PackageInfo, Plugin},
    register,
    shared::Shared as _,
    types::{From, Id, Message as SessionMessage, NodeId, Publish, Reason, To},
    Result,
};
use rmqtt_raft::{Mailbox, Raft};
use router::ClusterRouter;
use shared::ClusterShared;

mod config;
mod handler;
mod handoff;
mod message;
mod nodes;
mod raft_admin;
mod router;
mod shared;

type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

#[derive(Default)]
struct OrphanedRouterReconcile {
    state_ids: Vec<Id>,
    routes: Vec<(String, Id)>,
}

impl OrphanedRouterReconcile {
    fn route_details(&self) -> Vec<serde_json::Value> {
        self.routes
            .iter()
            .map(|(topic_filter, id)| {
                json!({
                    "topic": topic_filter,
                    "clientId": id.client_id,
                    "nodeId": id.node_id,
                    "connectionId": id.to_string(),
                })
            })
            .collect()
    }
}

register!(ClusterPlugin::new);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RaftStartMode {
    Configured,
    ForceLeader,
}

struct RaftRuntime {
    mailbox: Mailbox,
    thread: Option<JoinHandle<()>>,
}

#[derive(Plugin)]
struct ClusterPlugin {
    scx: ServerContext,
    register: Box<dyn Register>,
    cfg: Arc<PluginConfig>,
    nodes: ClusterNodeManager,
    shared: ClusterShared,
    router: ClusterRouter,
    raft_runtime: Arc<Mutex<Option<RaftRuntime>>>,
    recovery_active: Arc<AtomicBool>,
    recovery_epoch: Arc<AtomicU64>,
    draining: Arc<AtomicBool>,
    disconnecting: Arc<AtomicBool>,
    force_drain_finalized: Arc<AtomicBool>,
    drain_message_queue_drops_start: Arc<AtomicIsize>,
    drain_out_inflight_drops_start: Arc<AtomicIsize>,
    exec: TaskExecQueue,
    backoff_strategy: ExponentialBackoff,
}

impl ClusterPlugin {
    #[inline]
    async fn new<S: Into<String>>(scx: ServerContext, name: S) -> Result<Self> {
        let name = name.into();
        let env_list_keys = ["node_grpc_addrs", "raft_peer_addrs"];
        let mut cfg = scx.plugins.read_config_with::<PluginConfig>(&name, &env_list_keys)?;
        cfg.merge(&scx.args);
        cfg.handoff.validate()?;
        log::info!("{name} ClusterPlugin cfg: {cfg:?}");

        let exec = scx.get_exec(("RAFT_EXEC", cfg.task_exec_queue_workers, cfg.task_exec_queue_max));

        let register = scx.extends.hook_mgr().register();
        let nodes = ClusterNodeManager::new(scx.clone(), &cfg);
        nodes.seed_from_config(&cfg.node_grpc_addrs, &cfg.raft_peer_addrs).await?;
        let backoff_strategy = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(60)))
            .with_multiplier(2.5)
            .build();
        let router = ClusterRouter::new(
            scx.clone(),
            backoff_strategy.clone(),
            nodes.clone(),
            cfg.try_lock_timeout,
            cfg.compression,
        );
        let draining = Arc::new(AtomicBool::new(false));
        let shared = ClusterShared::new(
            scx.clone(),
            exec.clone(),
            router.clone(),
            nodes.clone(),
            draining.clone(),
            cfg.handoff.clone(),
            cfg.message_type,
            cfg.task_exec_queue_max,
            cfg.task_exec_queue_workers,
            cfg.node_grpc_client_timeout,
        );
        let raft_runtime = Arc::new(Mutex::new(None));
        let recovery_active = Arc::new(AtomicBool::new(false));
        let recovery_epoch = Arc::new(AtomicU64::new(0));
        let disconnecting = Arc::new(AtomicBool::new(false));
        let force_drain_finalized = Arc::new(AtomicBool::new(false));
        let drain_message_queue_drops_start = Arc::new(AtomicIsize::new(0));
        let drain_out_inflight_drops_start = Arc::new(AtomicIsize::new(0));
        let cfg = Arc::new(cfg);
        Ok(Self {
            scx,
            register,
            cfg,
            nodes,
            shared,
            router,
            raft_runtime,
            recovery_active,
            recovery_epoch,
            draining,
            disconnecting,
            force_drain_finalized,
            drain_message_queue_drops_start,
            drain_out_inflight_drops_start,
            exec,
            backoff_strategy,
        })
    }

    //raft init ...
    async fn start_raft(
        scx: &ServerContext,
        cfg: Arc<PluginConfig>,
        router: ClusterRouter,
        mode: RaftStartMode,
    ) -> Result<RaftRuntime> {
        // let logger = Runtime::instance().logger.clone();
        let raft_peer_addrs = cfg.raft_peer_addrs.clone();

        let id = scx.node.id();

        let raft_node_addr = raft_peer_addrs
            .iter()
            .find(|peer| peer.id == id)
            .map(|peer| peer.addr.to_string())
            .ok_or_else(|| anyhow!("raft listening address does not exist"))?;

        let raft_laddr =
            if let Some(laddr) = cfg.laddr.as_ref() { laddr.to_string() } else { raft_node_addr.clone() };

        log::info!("raft_laddr: {raft_laddr:?}, raft_node_addr: {raft_node_addr:?}");

        //verify the listening address
        if cfg.verify_addr {
            parse_addr(&raft_laddr).await?;
        }
        use slog::Drain;
        let logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());

        let raft =
            Raft::new(raft_laddr, router, logger, cfg.raft.to_raft_config()).map_err(|e| anyhow!(e))?;
        let mailbox = raft.mailbox();

        let mut peer_addrs = Vec::new();
        for peer in raft_peer_addrs.iter() {
            if peer.id != id {
                if cfg.verify_addr {
                    peer_addrs.push(parse_addr(&peer.addr).await?.to_string());
                } else {
                    peer_addrs.push(peer.addr.to_string());
                }
            }
        }
        log::info!("peer_addrs: {peer_addrs:?}");

        let leader_info = if mode == RaftStartMode::ForceLeader {
            log::warn!("forcing node {id} to bootstrap a new singleton raft configuration");
            None
        } else {
            match cfg.bootstrap {
                Some(true) => {
                    let bootstrap_node = cfg
                        .leader()?
                        .ok_or_else(|| anyhow!("controlled bootstrap requires a non-zero leader_id"))?;
                    if id != bootstrap_node.id {
                        return Err(anyhow!(
                        "node {id} is not the configured bootstrap node {}; refusing to create a new raft cluster",
                        bootstrap_node.id
                    ));
                    }

                    // An explicitly authorized bootstrap node still checks for an
                    // already-running leader before creating a new cluster. The
                    // operator only enables this mode while the StatefulSet has a
                    // single initial pod.
                    let actual_leader_info = find_actual_leader_retry(&raft, peer_addrs, 3).await?;
                    if actual_leader_info.is_some() {
                        log::info!("Leader already exists, {actual_leader_info:?}");
                    } else {
                        log::info!("No existing leader found; controlled bootstrap is enabled for node {id}");
                    }
                    actual_leader_info
                }
                Some(false) => {
                    // Existing-cluster mode is deliberately fail-closed: DNS,
                    // networking, and leader discovery failures must never be
                    // interpreted as permission to create another raft cluster.
                    log::info!("Controlled bootstrap is disabled; waiting for an existing leader ...");
                    let actual_leader_info = find_actual_leader_retry(&raft, peer_addrs, 60).await?;
                    Some(
                        actual_leader_info.ok_or_else(|| {
                            anyhow!("Leader does not exist; controlled bootstrap is disabled")
                        })?,
                    )
                }
                None => match cfg.leader()? {
                    Some(leader_info) => {
                        log::info!("Specify a leader: {leader_info:?}");
                        if id == leader_info.id {
                            //First, check if the Leader exists.
                            let actual_leader_info = find_actual_leader(&raft, peer_addrs, 3).await?;
                            if actual_leader_info.is_some() {
                                log::info!("Leader already exists, {actual_leader_info:?}");
                            }
                            actual_leader_info
                        } else {
                            //The other nodes are leader.
                            let actual_leader_info = find_actual_leader(&raft, peer_addrs, 60).await?;
                            let (actual_leader_id, actual_leader_addr) =
                                actual_leader_info.ok_or_else(|| anyhow!("Leader does not exist"))?;
                            if actual_leader_id != leader_info.id {
                                return Err(anyhow!(format!(
                                    "Not the expected Leader, the expected one is {:?}",
                                    leader_info
                                )));
                            }
                            Some((actual_leader_id, actual_leader_addr))
                        }
                    }
                    None => {
                        log::info!("Search for the existing leader ... ");
                        let leader_info = raft.find_leader_info(peer_addrs).await.map_err(|e| anyhow!(e))?;
                        log::info!("The information about the located leader: {leader_info:?}");
                        leader_info
                    }
                },
            }
        };

        //let (status_tx, status_rx) = futures::channel::oneshot::channel::<Result<Status>>();
        let child = std::thread::Builder::new().name("cluster-raft".to_string()).spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(cfg.worker_threads)
                .thread_name("cluster-raft-worker")
                .thread_stack_size(4 * 1024 * 1024)
                .build()
                .expect("tokio runtime build failed");

            let runner = async move {
                log::info!("leader_info: {leader_info:?}");
                let raft_handle = match leader_info {
                    Some((leader_id, leader_addr)) => {
                        log::info!(
                            "running in follower mode, leader_id: {leader_id}, leader_addr: {leader_addr}, join_as_learner: {}",
                            cfg.join_as_learner
                        );
                        if cfg.join_as_learner {
                            tokio::spawn(raft.join_as_learner(
                                id,
                                raft_node_addr,
                                Some(leader_id),
                                leader_addr,
                            ))
                            .await
                        } else {
                            tokio::spawn(raft.join(id, raft_node_addr, Some(leader_id), leader_addr)).await
                        }
                    }
                    None => {
                        log::info!("running in leader mode");
                        tokio::spawn(raft.lead(id, raft_node_addr)).await
                    }
                };

                if let Err(_) | Ok(Err(_)) = raft_handle {
                    log::error!("Raft service startup failed, {raft_handle:?}");
                    if mode == RaftStartMode::Configured {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        std::process::exit(-1);
                    }
                }
            };

            rt.block_on(runner);
        })?;
        Ok(RaftRuntime { mailbox, thread: Some(child) })
    }

    #[inline]
    async fn hook_register(&self, typ: Type) {
        self.register
            .add(
                typ,
                Box::new(HookHandler::new(
                    self.scx.clone(),
                    self.cfg.clone(),
                    self.backoff_strategy.clone(),
                    self.shared.clone(),
                    self.router.clone(),
                )),
            )
            .await;
    }

    #[inline]
    async fn raft_mailbox(&self) -> Mailbox {
        self.router.raft_mailbox().await
    }

    fn start_check_health(&self) {
        let exit_on_node_unavailable = self.cfg.health.exit_on_node_unavailable;
        let exit_code = self.cfg.health.exit_code;
        let unavailable_check_interval = self.cfg.health.unavailable_check_interval;
        let max_continuous_unavailable_count = self.cfg.health.max_continuous_unavailable_count;

        let mut continuous_unavailable_count = 0;
        let router = self.router.clone();
        let recovery_active = self.recovery_active.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(unavailable_check_interval).await;
                let raft_mailbox = router.raft_mailbox().await;
                match raft_mailbox.status().await {
                    Err(e) => {
                        log::error!("Error retrieving cluster status, {e:?}");
                    }
                    Ok(s) => {
                        if s.available() {
                            if continuous_unavailable_count > 0 {
                                continuous_unavailable_count = 0;
                            }
                        } else {
                            continuous_unavailable_count += 1;
                            log::error!(
                                "cluster node unavailable({continuous_unavailable_count}), node status: {s:?}"
                            );
                            if exit_on_node_unavailable
                                && !recovery_active.load(Ordering::SeqCst)
                                && continuous_unavailable_count >= max_continuous_unavailable_count
                            {
                                std::thread::sleep(Duration::from_secs(2));
                                std::process::exit(exit_code);
                            }
                        }
                    }
                }
            }
        });
    }

    async fn force_rebootstrap(&self, epoch: u64) -> Result<serde_json::Value> {
        if epoch == 0 {
            return Err(anyhow!("recoveryEpoch must be greater than zero"));
        }

        let current_epoch = self.recovery_epoch.load(Ordering::SeqCst);
        if epoch < current_epoch {
            return Err(anyhow!("stale quorum recovery epoch {epoch}; current epoch is {current_epoch}"));
        }

        let local_id = self.scx.node.id();
        if epoch == current_epoch {
            let mailbox = self.raft_mailbox().await;
            if let Ok(status) = mailbox.status().await {
                if status.is_leader() && status.voters == vec![local_id] {
                    let local_node = self
                        .nodes
                        .local_node_from_config(&self.cfg.node_grpc_addrs, &self.cfg.raft_peer_addrs)?;
                    let node_up = message::Message::NodeUp { node: local_node }.encode()?;
                    mailbox.send_proposal(node_up).await.map_err(anyhow::Error::new)?;
                    self.recovery_active.store(false, Ordering::SeqCst);
                    return Ok(json!({
                        "ok": true,
                        "id": local_id,
                        "recoveryEpoch": epoch,
                        "phase": "DegradedServing",
                        "idempotent": true,
                    }));
                }
            }
        }

        self.recovery_active.store(true, Ordering::SeqCst);
        self.recovery_epoch.store(epoch, Ordering::SeqCst);

        let mut runtime_guard = self.raft_runtime.lock().await;
        let mut old_runtime =
            runtime_guard.take().ok_or_else(|| anyhow!("local raft runtime is not available"))?;

        if let Err(error) = old_runtime.mailbox.shutdown().await {
            log::warn!(
                "raft shutdown acknowledgement failed during quorum recovery epoch {epoch}: {error:?}"
            );
        }

        if let Some(thread) = old_runtime.thread.take() {
            let joined = timeout(Duration::from_secs(15), tokio::task::spawn_blocking(move || thread.join()))
                .await
                .map_err(|_| anyhow!("timed out waiting for old raft runtime to stop"))?
                .map_err(|error| anyhow!("failed to join old raft runtime task: {error}"))?;
            if joined.is_err() {
                return Err(anyhow!("old raft runtime panicked while stopping"));
            }
        }

        let new_runtime =
            Self::start_raft(&self.scx, self.cfg.clone(), self.router.clone(), RaftStartMode::ForceLeader)
                .await?;
        let new_mailbox = new_runtime.mailbox.clone();
        self.router.set_raft_mailbox(new_mailbox.clone()).await;
        runtime_guard.replace(new_runtime);

        let mut ready_status = None;
        for _ in 0..60 {
            match new_mailbox.status().await {
                Ok(status) if status.is_leader() && status.voters == vec![local_id] => {
                    ready_status = Some(status);
                    break;
                }
                Ok(_) | Err(_) => sleep(Duration::from_millis(500)).await,
            }
        }
        let status = ready_status
            .ok_or_else(|| anyhow!("new singleton raft did not become ready within 30 seconds"))?;

        let ping = message::Message::Ping.encode()?;
        new_mailbox.send_proposal(ping).await.map_err(anyhow::Error::new)?;
        let local_node =
            self.nodes.local_node_from_config(&self.cfg.node_grpc_addrs, &self.cfg.raft_peer_addrs)?;
        let node_up = message::Message::NodeUp { node: local_node }.encode()?;
        new_mailbox.send_proposal(node_up).await.map_err(anyhow::Error::new)?;

        self.recovery_active.store(false, Ordering::SeqCst);
        Ok(json!({
            "ok": true,
            "id": local_id,
            "recoveryEpoch": epoch,
            "phase": "DegradedServing",
            "preservedRouterState": true,
            "appliedIndex": status.applied_index,
            "committedIndex": status.committed_index,
            "voters": status.voters,
        }))
    }
}

#[async_trait]
impl Plugin for ClusterPlugin {
    #[inline]
    async fn init(&mut self) -> Result<()> {
        log::info!("{} init", self.name());

        let raft_runtime =
            Self::start_raft(&self.scx, self.cfg.clone(), self.router.clone(), RaftStartMode::Configured)
                .await?;
        let raft_mailbox = raft_runtime.mailbox.clone();

        let mut raft_status = None;
        for i in 0..60 {
            match raft_mailbox.status().await {
                Ok(status) => {
                    if status.is_started() {
                        raft_status = Some(status);
                        break;
                    }
                    log::info!("{} Initializing cluster, raft status({}): {:?}", self.name(), i, status);
                }
                Err(e) => {
                    log::info!("{} init failed, {:?}", self.name(), e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        if raft_status.is_none() {
            return Err(anyhow!("init failed"));
        }

        self.router.set_raft_mailbox(raft_mailbox).await;
        self.raft_runtime.lock().await.replace(raft_runtime);

        self.hook_register(Type::ClientDisconnected).await;
        self.hook_register(Type::SessionTerminated).await;
        self.hook_register(Type::GrpcMessageReceived).await;

        self.start_check_health();

        Ok(())
    }

    #[inline]
    async fn get_config(&self) -> Result<serde_json::Value> {
        self.cfg.to_json()
    }

    #[inline]
    async fn start(&mut self) -> Result<()> {
        log::info!("{} start", self.name());
        let raft_mailbox = self.raft_mailbox().await;
        *self.scx.extends.router_mut().await = Box::new(self.router.clone());
        *self.scx.extends.shared_mut().await = Box::new(self.shared.clone());
        self.register.start().await;
        let status = raft_mailbox.status().await.map_err(anyhow::Error::new)?;
        log::info!("raft status: {status:?}");
        if !status.is_started() {
            return Err(anyhow!("Raft cluster status is abnormal"));
        }

        let ping = message::Message::Ping.encode()?;
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let status = raft_mailbox.status().await.map_err(anyhow::Error::new)?;
            if !status.local_caught_up() || !status.is_voter(status.id) {
                log::info!(
                    "waiting for local raft catch-up before business NodeUp: id={}, snapshot_restored={}, applied={}, committed={}, voters={:?}, learners={:?}",
                    status.id,
                    status.snapshot_restored,
                    status.applied_index,
                    status.committed_index,
                    status.voters,
                    status.learners
                );
                sleep(Duration::from_millis(500)).await;
                continue;
            }
            match raft_mailbox.send_proposal(ping.clone()).await {
                Ok(reply) => match message::MessageReply::decode(&reply)? {
                    message::MessageReply::Ping => {
                        log::info!("ping ok");
                        let local_node = self
                            .nodes
                            .local_node_from_config(&self.cfg.node_grpc_addrs, &self.cfg.raft_peer_addrs)?;
                        let node_up = message::Message::NodeUp { node: local_node }.encode()?;
                        raft_mailbox.send_proposal(node_up).await.map_err(anyhow::Error::new)?;
                        return Ok(());
                    }
                    message::MessageReply::Error(e) => {
                        log::warn!("ping failed, {e:?}");
                    }
                    _ => {
                        log::error!("unreachable!()");
                    }
                },
                Err(e) => {
                    log::warn!("ping failed, {e:?}");
                }
            }
            if !self.cfg.join_as_learner && attempts >= 100 {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!("Raft cluster status is unavailable"))
    }

    #[inline]
    async fn stop(&mut self) -> Result<bool> {
        log::warn!("{} stop, once the cluster is started, it cannot be stopped", self.name());
        Ok(false)
    }

    #[inline]
    async fn attrs(&self) -> serde_json::Value {
        let raft_mailbox = self.raft_mailbox().await;
        let raft_status = raft_mailbox.status().await.ok();

        let mut pears = HashMap::default();
        for (id, p) in raft_mailbox.pears() {
            let stats = json!({
                "active_tasks": p.active_tasks(),
                "grpc_fails": p.grpc_fails(),
            });
            pears.insert(id, stats);
        }

        let mut nodes = HashMap::default();
        let grpc_clients = self.nodes.grpc_clients();
        for (node_id, (_, c)) in grpc_clients.iter() {
            let stats = json!({
                "transfer_queue_len": c.transfer_queue_len(),
                "active_tasks_count": c.active_tasks().count(),
                "active_tasks_max": c.active_tasks().max(),
            });
            nodes.insert(*node_id, stats);
        }

        json!({
            "grpc_clients": nodes,
            "cluster_nodes": self.nodes.to_json(),
            "raft_status": raft_status,
            "raft_pears": pears,
            "client_states": self.router.states_count(),
            "task_exec_queue": {
                "waiting_count": self.exec.waiting_count(),
                "active_count": self.exec.active_count(),
                "completed_count": self.exec.completed_count().await,
            }
        })
    }

    #[inline]
    async fn send(&self, msg: serde_json::Value) -> Result<serde_json::Value> {
        let action = msg.get("action").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("missing action"))?;
        let raft_mailbox = self.raft_mailbox().await;
        match action {
            "prepare_quorum_recovery" => {
                let epoch = msg
                    .get("recoveryEpoch")
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| anyhow!("missing recoveryEpoch"))?;
                let current_epoch = self.recovery_epoch.load(Ordering::SeqCst);
                if epoch < current_epoch {
                    return Err(anyhow!(
                        "stale quorum recovery epoch {epoch}; current epoch is {current_epoch}"
                    ));
                }
                self.recovery_epoch.store(epoch, Ordering::SeqCst);
                self.recovery_active.store(true, Ordering::SeqCst);
                Ok(json!({
                    "ok": true,
                    "id": self.scx.node.id(),
                    "recoveryEpoch": epoch,
                    "phase": "QuorumLost",
                }))
            }
            "cancel_quorum_recovery" => {
                let epoch = msg
                    .get("recoveryEpoch")
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| anyhow!("missing recoveryEpoch"))?;
                if epoch == self.recovery_epoch.load(Ordering::SeqCst) {
                    self.recovery_active.store(false, Ordering::SeqCst);
                }
                Ok(json!({
                    "ok": true,
                    "id": self.scx.node.id(),
                    "recoveryEpoch": epoch,
                    "phase": "Cancelled",
                }))
            }
            "force_rebootstrap" => {
                let epoch = msg
                    .get("recoveryEpoch")
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| anyhow!("missing recoveryEpoch"))?;
                self.force_rebootstrap(epoch).await
            }
            "node_up" => {
                let node: message::ClusterNode =
                    serde_json::from_value(msg.get("node").cloned().unwrap_or_else(|| {
                        json!({
                            "id": msg.get("id").cloned().unwrap_or_default(),
                            "grpc_addr": msg.get("grpc_addr").cloned().unwrap_or_default(),
                            "raft_addr": msg.get("raft_addr").cloned().unwrap_or_default(),
                        })
                    }))?;
                let proposal = message::Message::NodeUp { node }.encode()?;
                raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
                Ok(json!({"ok": true}))
            }
            "node_down" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let proposal = message::Message::NodeDown { id }.encode()?;
                raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
                Ok(json!({"ok": true, "id": id, "raft_membership_changed": false}))
            }
            "node_remove" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).unwrap_or_else(|| self.scx.node.id());
                if id != self.scx.node.id() {
                    return Err(anyhow!(
                        "node_remove must be sent to target node {id}; \
                         use action=force_node_remove for business-only cleanup of an unreachable node"
                    ));
                }
                self.leave_cluster(&raft_mailbox).await
            }
            "force_node_remove" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                if id == self.scx.node.id() {
                    return Err(anyhow!("force_node_remove cannot remove the local node; use action=leave"));
                }
                let proposal = message::Message::NodeRemove { id }.encode()?;
                raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
                Ok(json!({"ok": true, "id": id, "raft_membership_changed": false}))
            }
            "quiesce" | "quiesce_drain" => self.quiesce_node(&raft_mailbox).await,
            "drain" | "node_drain" => self.drain_node(&raft_mailbox).await,
            "undrain" | "resume" | "node_up_local" => self.undrain_node(&raft_mailbox).await,
            "prepare_drain" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let allowed_target_node_ids = msg
                    .get("allowedTargetNodeIds")
                    .and_then(|value| value.as_array())
                    .map(|values| {
                        values
                            .iter()
                            .map(|value| {
                                value.as_u64().ok_or_else(|| {
                                    anyhow!("allowedTargetNodeIds must contain unsigned node ids")
                                })
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                let (reliable_handoff, grpc_client_refreshed) =
                    self.shared.prepare_drain(id, allowed_target_node_ids.clone()).await?;
                Ok(json!({
                    "ok": true,
                    "id": id,
                    "reliable_handoff": reliable_handoff,
                    "grpc_client_refreshed": grpc_client_refreshed,
                    "allowedTargetNodeIds": allowed_target_node_ids,
                    "phase": "prepared"
                }))
            }
            "activate_drain" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let reliable_handoff = self.shared.activate_drain(id);
                Ok(json!({
                    "ok": true,
                    "id": id,
                    "reliable_handoff": reliable_handoff,
                    "phase": "active"
                }))
            }
            "clear_drain" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                self.shared.clear_drain(id);
                if id == self.scx.node.id() {
                    self.force_drain_finalized.store(false, Ordering::SeqCst);
                }
                Ok(json!({"ok": true, "id": id}))
            }
            "force_drain_finalize" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let reason = msg.get("reason").and_then(|v| v.as_str()).unwrap_or("operator_policy");
                self.force_drain_finalize(&raft_mailbox, id, reason).await
            }
            "drain_status" => Ok(self.drain_status(&raft_mailbox).await),
            "routing_status" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).unwrap_or_else(|| self.scx.node.id());
                Ok(self.routing_status(id))
            }
            "leave" | "node_leave" => self.leave_cluster(&raft_mailbox).await,
            "promote_learner" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let status = raft_mailbox.status().await.map_err(anyhow::Error::new)?;
                if !status.is_leader() {
                    return Err(anyhow!("promote_learner must be sent to the current raft leader"));
                }
                if !status.is_learner(id) {
                    return Err(anyhow!("node {id} is not a learner"));
                }
                if !status.peer_caught_up(id) {
                    return Err(anyhow!(
                        "learner {id} has not caught up to leader commit {}",
                        status.committed_index
                    ));
                }
                let addr = status
                    .peers
                    .get(&id)
                    .and_then(|peer| peer.as_ref())
                    .map(|peer| peer.addr.to_string())
                    .ok_or_else(|| anyhow!("raft address for node {id} was not configured"))?;
                raft_mailbox.promote_learner(id, addr).await.map_err(anyhow::Error::new)?;
                Ok(json!({"ok": true, "id": id, "action": "promote_learner"}))
            }
            "transfer_leader" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let status = raft_mailbox.status().await.map_err(anyhow::Error::new)?;
                if !status.is_leader() {
                    return Err(anyhow!("transfer_leader must be sent to the current raft leader"));
                }
                if !status.is_voter(id) || !status.peer_caught_up(id) {
                    return Err(anyhow!("leadership target {id} is not a caught-up voter"));
                }
                raft_mailbox.transfer_leader(id).await.map_err(anyhow::Error::new)?;
                Ok(json!({"ok": true, "id": id, "action": "transfer_leader"}))
            }
            "remove_voter" => {
                let id = msg.get("id").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("missing id"))?;
                let status = raft_mailbox.status().await.map_err(anyhow::Error::new)?;
                if !status.is_leader() {
                    return Err(anyhow!("remove_voter must be sent to the current raft leader"));
                }
                raft_mailbox.remove_node(id).await.map_err(anyhow::Error::new)?;
                Ok(json!({"ok": true, "id": id, "action": "remove_voter"}))
            }
            "nodes" => Ok(self.nodes.to_json()),
            "membership" | "raft_status" => Ok(self.membership_status(&raft_mailbox).await),
            other => Err(anyhow!("unknown action: {other}")),
        }
    }
}

impl ClusterPlugin {
    async fn membership_status(&self, raft_mailbox: &Mailbox) -> serde_json::Value {
        let raft_status = raft_mailbox.status().await.ok();
        let mut raft_peers = raft_status
            .as_ref()
            .map(|status| {
                status
                    .peers
                    .iter()
                    .map(|(id, peer)| {
                        json!({
                            "id": id,
                            "addr": peer.as_ref().map(|p| p.addr.to_string()),
                            "available": peer.as_ref().map(|p| p.available).unwrap_or(false),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        raft_peers.sort_by_key(|peer| peer.get("id").and_then(|id| id.as_u64()).unwrap_or_default());

        if let Some(status) = raft_status.as_ref() {
            for peer in &mut raft_peers {
                let Some(id) = peer.get("id").and_then(|id| id.as_u64()) else {
                    continue;
                };
                if let Some(progress) = status.peer_replication.get(&id) {
                    peer["matchedIndex"] = json!(progress.matched);
                    peer["nextIndex"] = json!(progress.next_index);
                    peer["committedIndex"] = json!(progress.committed_index);
                    peer["pendingSnapshot"] = json!(progress.pending_snapshot);
                    peer["recentActive"] = json!(progress.recent_active);
                    peer["progressState"] = json!(progress.state.clone());
                }
            }
        }
        let business_peers = self.nodes.to_json();

        json!({
            "id": self.scx.node.id(),
            "leaderId": raft_status.as_ref().map(|status| status.leader_id),
            "isLeader": raft_status.as_ref().map(|status| status.is_leader()).unwrap_or(false),
            "available": raft_status.as_ref().map(|status| status.available()).unwrap_or(false),
            "started": raft_status.as_ref().map(|status| status.is_started()).unwrap_or(false),
            "caughtUp": raft_status.as_ref().map(|status| status.local_caught_up()).unwrap_or(false),
            "snapshotRestored": raft_status.as_ref().map(|status| status.snapshot_restored).unwrap_or(false),
            "snapshotIndex": raft_status.as_ref().map(|status| status.snapshot_index).unwrap_or_default(),
            "appliedIndex": raft_status.as_ref().map(|status| status.applied_index).unwrap_or_default(),
            "committedIndex": raft_status.as_ref().map(|status| status.committed_index).unwrap_or_default(),
            "lastIndex": raft_status.as_ref().map(|status| status.last_index).unwrap_or_default(),
            "clientStates": self.router.states_count(),
            "draining": self.draining.load(Ordering::SeqCst),
            "quorumRecoveryActive": self.recovery_active.load(Ordering::SeqCst),
            "quorumRecoveryEpoch": self.recovery_epoch.load(Ordering::SeqCst),
            "role": raft_status.as_ref().map(|status| format!("{:?}", status.role)),
            "uncommitteds": raft_status.as_ref().map(|status| status.uncommitteds).unwrap_or_default(),
            "mergerProposals": raft_status.as_ref().map(|status| status.merger_proposals).unwrap_or_default(),
            "voters": raft_status.as_ref().map(|status| status.voters.clone()).unwrap_or_default(),
            "learners": raft_status.as_ref().map(|status| status.learners.clone()).unwrap_or_default(),
            "raftPeers": raft_peers,
            "businessPeers": business_peers,
            "supports": {
                "addLearner": true,
                "promoteLearner": true,
                "transferLeader": true,
                "removeNodeFromLeader": true,
                "directAddVoter": true,
                "selfLeave": true,
                "reliableHandoff": self.shared.reliable_handoff_enabled(),
                "preDisconnectQuiesce": true,
                "proactiveSessionHandoff": true,
                "forceDrainFinalize": true,
                "forceRebootstrap": true
            }
        })
    }

    async fn reconcile_orphaned_local_router_state(
        &self,
        raft_mailbox: &Mailbox,
    ) -> Result<OrphanedRouterReconcile> {
        let node_id = self.scx.node.id();
        let live_session_ids = self.shared.inner().iter().map(|entry| entry.id()).collect::<HashSet<_>>();
        let orphaned_state_ids = self
            .router
            .node_state_ids(node_id)
            .into_iter()
            .filter(|id| !live_session_ids.contains(id))
            .collect::<Vec<_>>();

        for id in &orphaned_state_ids {
            let proposal = message::Message::SessionTerminated { id: id.clone() }.encode()?;
            raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
        }
        if !orphaned_state_ids.is_empty() {
            log::warn!(
                "drain reconciled {} orphaned local router states on node {} before disconnecting live sessions: {:?}",
                orphaned_state_ids.len(),
                node_id,
                orphaned_state_ids
            );
        }

        // SessionTerminated removes routes owned by the reconciled states.
        // Anything left without an exact state or live session is a route-only
        // orphan and must be removed through Raft so every replica converges.
        let orphaned_routes = self.router.orphaned_node_routes(node_id, &live_session_ids);
        for (topic_filter, id) in &orphaned_routes {
            let proposal = message::Message::Remove { topic_filter, id: id.clone() }.encode()?;
            raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
        }
        if !orphaned_routes.is_empty() {
            log::warn!(
                "drain reconciled {} orphaned routes on node {} through Raft: {:?}",
                orphaned_routes.len(),
                node_id,
                orphaned_routes
            );
        }

        Ok(OrphanedRouterReconcile { state_ids: orphaned_state_ids, routes: orphaned_routes })
    }

    async fn quiesce_node(&self, raft_mailbox: &Mailbox) -> Result<serde_json::Value> {
        let id = self.scx.node.id();
        let reconciled = self.reconcile_orphaned_local_router_state(raft_mailbox).await?;
        let orphaned_route_details = reconciled.route_details();
        let reliable_handoff = self.shared.activate_drain(id);
        let quiesce_started = !self.draining.swap(true, Ordering::SeqCst);
        if quiesce_started {
            self.force_drain_finalized.store(false, Ordering::SeqCst);
            self.drain_message_queue_drops_start
                .store(self.scx.stats.message_queue_drops.count(), Ordering::SeqCst);
            self.drain_out_inflight_drops_start
                .store(self.scx.stats.out_inflight_drops.count(), Ordering::SeqCst);
        }
        Ok(json!({
            "ok": true,
            "id": id,
            "draining": true,
            "disconnecting": self.disconnecting.load(Ordering::SeqCst),
            "reliable_handoff": reliable_handoff,
            "orphanedRouterStatesRemoved": reconciled.state_ids.len(),
            "orphanedRoutesRemoved": reconciled.routes.len(),
            "orphanedRouteDetails": orphaned_route_details,
            "quiesceStarted": quiesce_started,
            "raft_membership_changed": false,
            "note": "new sessions and new local deliveries are quiesced while existing connections remain alive to acknowledge queued and outbound inflight messages"
        }))
    }

    async fn drain_node(&self, raft_mailbox: &Mailbox) -> Result<serde_json::Value> {
        let id = self.scx.node.id();
        let quiesce = self.quiesce_node(raft_mailbox).await?;
        let start_disconnects = !self.disconnecting.swap(true, Ordering::SeqCst);
        let disconnect_requested = self.scx.stats.connections.count();
        if start_disconnects {
            let shared = self.shared.clone();
            let draining = self.draining.clone();
            let disconnecting = self.disconnecting.clone();
            let disconnect_batch_size = self.cfg.handoff.disconnect_batch_size.max(1);
            let disconnect_batch_interval = self.cfg.handoff.disconnect_batch_interval;
            tokio::spawn(async move {
                while draining.load(Ordering::SeqCst) && disconnecting.load(Ordering::SeqCst) {
                    let entries = shared.inner().iter().collect::<Vec<_>>();
                    let mut requested_total = 0usize;
                    for batch in entries.chunks(disconnect_batch_size) {
                        if !draining.load(Ordering::SeqCst) || !disconnecting.load(Ordering::SeqCst) {
                            break;
                        }
                        let mut acknowledgements = Vec::with_capacity(batch.len());
                        for entry in batch {
                            let Some(tx) = entry.tx().filter(|tx| !tx.is_closed()) else {
                                continue;
                            };
                            let client_id = entry.id();
                            let (ack_tx, ack_rx) = oneshot::channel();
                            match tx.unbounded_send(SessionMessage::Drain(ack_tx, client_id.clone())) {
                                Ok(()) => {
                                    requested_total += 1;
                                    acknowledgements.push((client_id, ack_rx));
                                }
                                Err(e) => {
                                    log::warn!("drain failed to request disconnect for {client_id:?}: {e:?}");
                                }
                            }
                        }
                        if !acknowledgements.is_empty() {
                            tokio::spawn(async move {
                                let waits = acknowledgements.into_iter().map(|(client_id, ack_rx)| async move {
                                    if tokio::time::timeout(Duration::from_secs(5), ack_rx).await.is_err() {
                                        log::warn!(
                                            "drain offline transition acknowledgement timed out for {client_id:?}"
                                        );
                                    }
                                });
                                futures::future::join_all(waits).await;
                            });
                        }
                        sleep(disconnect_batch_interval).await;
                    }
                    if requested_total > 0 {
                        log::info!(
                            "drain requested reconnect for {requested_total} local MQTT sessions in batches of {disconnect_batch_size}"
                        );
                    }
                    let migrated = shared.migrate_drained_sessions().await;
                    if migrated > 0 {
                        log::info!(
                            "drain proactively relocated {migrated} offline MQTT sessions to peer brokers"
                        );
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            });
        }
        Ok(json!({
            "ok": true,
            "id": id,
            "draining": true,
            "disconnecting": true,
            "reliable_handoff": quiesce.get("reliable_handoff").cloned().unwrap_or_default(),
            "orphanedRouterStatesRemoved": quiesce
                .get("orphanedRouterStatesRemoved")
                .cloned()
                .unwrap_or_default(),
            "orphanedRoutesRemoved": quiesce
                .get("orphanedRoutesRemoved")
                .cloned()
                .unwrap_or_default(),
            "orphanedRouteDetails": quiesce
                .get("orphanedRouteDetails")
                .cloned()
                .unwrap_or_default(),
            "disconnectWorkerStarted": start_disconnects,
            "disconnectRequested": disconnect_requested,
            "raft_membership_changed": false,
            "note": "external traffic is disabled and connections enter the offline session loop; business routing, offline queues and session handoff remain active until drain_status.safe_to_leave"
        }))
    }

    async fn undrain_node(&self, raft_mailbox: &Mailbox) -> Result<serde_json::Value> {
        let node = self.nodes.local_node_from_config(&self.cfg.node_grpc_addrs, &self.cfg.raft_peer_addrs)?;
        let id = node.id;
        let proposal = message::Message::NodeUp { node }.encode()?;
        raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
        self.disconnecting.store(false, Ordering::SeqCst);
        self.draining.store(false, Ordering::SeqCst);
        self.force_drain_finalized.store(false, Ordering::SeqCst);
        self.shared.clear_drain(id);
        Ok(json!({
            "ok": true,
            "id": id,
            "draining": false,
            "raft_membership_changed": false,
        }))
    }

    async fn drain_status(&self, raft_mailbox: &Mailbox) -> serde_json::Value {
        let stats = &self.scx.stats;
        let id = self.scx.node.id();
        let reconciled = if self.draining.load(Ordering::SeqCst) {
            match self.reconcile_orphaned_local_router_state(raft_mailbox).await {
                Ok(reconciled) => reconciled,
                Err(error) => {
                    log::warn!("drain failed to reconcile orphaned router state on node {id}: {error:?}");
                    OrphanedRouterReconcile::default()
                }
            }
        } else {
            OrphanedRouterReconcile::default()
        };
        let orphaned_route_details = reconciled.route_details();
        let connections = self.scx.connections.count();
        let sessions = self.scx.sessions.count();
        let handshakings_active = self.scx.handshake_exec.active_count();
        let client_states = self.router.node_states_count(id);
        let routes = self.router.node_routes_count(id);
        let message_queues = stats.message_queues.count();
        let message_queue_drops = (stats.message_queue_drops.count()
            - self.drain_message_queue_drops_start.load(Ordering::SeqCst))
        .max(0);
        let out_inflights = stats.out_inflights.count();
        let out_inflight_drops = (stats.out_inflight_drops.count()
            - self.drain_out_inflight_drops_start.load(Ordering::SeqCst))
        .max(0);
        let in_inflights = stats.in_inflights.count();
        let forwards = stats.forwards.count();
        let message_mgr = self.scx.extends.message_mgr().await;
        let message_storage_enabled = message_mgr.enable();
        let message_storages =
            if message_storage_enabled { message_mgr.count().await } else { stats.message_storages.count() };
        let delayed_publishs = stats.delayed_publishs.count();
        let handoff = self.shared.handoff_stats();
        let session_handoff_pending = self.shared.session_handoff_pending();
        let session_handoff_active = self.shared.session_handoff_active();
        let session_handoff_failures = self.shared.session_handoff_failures();
        let handoff_safe_to_leave = handoff.pending_messages == 0
            && handoff.active_workers == 0
            && handoff.retrying_messages == 0
            && handoff.failed_messages == 0
            && handoff.rejected_messages == 0;
        // Kept for API compatibility with older operators. The current
        // operator does not wait for this advisory state: planned drain marks
        // the session as Draining, preserves its subscriptions and MQTT
        // inflights in the offline session, and immediately asks the client to
        // reconnect. Pod removal is still guarded by strict_safe_to_leave.
        let pre_disconnect_safe = handshakings_active == 0
            && message_queues == 0
            && message_queue_drops == 0
            && out_inflights == 0
            && out_inflight_drops == 0
            && in_inflights == 0
            && forwards == 0
            && (!message_storage_enabled || message_storages == 0)
            && delayed_publishs == 0
            && handoff.failed_messages == 0
            && handoff.rejected_messages == 0
            && handoff.capacity_waiters == 0
            && !self.shared.handoff_data_loss_detected();
        let message_safe_to_leave = connections == 0
            && handshakings_active == 0
            && message_queues == 0
            && message_queue_drops == 0
            && out_inflights == 0
            && out_inflight_drops == 0
            && in_inflights == 0
            && forwards == 0
            && (!message_storage_enabled || message_storages == 0)
            && delayed_publishs == 0
            && handoff_safe_to_leave;
        let session_handoff_complete = sessions == 0
            && client_states == 0
            && routes == 0
            && session_handoff_pending == 0
            && session_handoff_active == 0;
        let strict_safe_to_leave = message_safe_to_leave && session_handoff_complete;
        let raft_status = raft_mailbox.status().await.ok();
        let blocking_counts = json!({
            "connections": if connections > 0 { Some(connections) } else { None },
            "sessions": if sessions > 0 { Some(sessions) } else { None },
            "client_states": if client_states > 0 { Some(client_states) } else { None },
            "routes": if routes > 0 { Some(routes) } else { None },
            "handshakings_active": if handshakings_active > 0 { Some(handshakings_active) } else { None },
            "message_queues": if message_queues > 0 { Some(message_queues) } else { None },
            "message_queue_drops": if message_queue_drops > 0 { Some(message_queue_drops) } else { None },
            "out_inflights": if out_inflights > 0 { Some(out_inflights) } else { None },
            "out_inflight_drops": if out_inflight_drops > 0 { Some(out_inflight_drops) } else { None },
            "in_inflights": if in_inflights > 0 { Some(in_inflights) } else { None },
            "forwards": if forwards > 0 { Some(forwards) } else { None },
            "message_storages": if message_storage_enabled && message_storages > 0 { Some(message_storages) } else { None },
            "delayed_publishs": if delayed_publishs > 0 { Some(delayed_publishs) } else { None },
            "handoff_pending_messages": if handoff.pending_messages > 0 { Some(handoff.pending_messages) } else { None },
            "handoff_pending_bytes": if handoff.pending_bytes > 0 { Some(handoff.pending_bytes) } else { None },
            "handoff_clients": if handoff.clients > 0 { Some(handoff.clients) } else { None },
            "handoff_active_workers": if handoff.active_workers > 0 { Some(handoff.active_workers) } else { None },
            "handoff_retrying_messages": if handoff.retrying_messages > 0 { Some(handoff.retrying_messages) } else { None },
            "handoff_failed_messages": if handoff.failed_messages > 0 { Some(handoff.failed_messages) } else { None },
            "handoff_rejected_messages": if handoff.rejected_messages > 0 { Some(handoff.rejected_messages) } else { None },
            "handoff_invariant_mismatch_observations": if handoff.invariant_mismatch_observations > 0 { Some(handoff.invariant_mismatch_observations) } else { None },
            "session_handoff_pending": if session_handoff_pending > 0 { Some(session_handoff_pending) } else { None },
            "session_handoff_active": if session_handoff_active > 0 { Some(session_handoff_active) } else { None },
        });
        let message_blocking_counts = json!({
            "connections": if connections > 0 { Some(connections) } else { None },
            "handshakings_active": if handshakings_active > 0 { Some(handshakings_active) } else { None },
            "message_queues": if message_queues > 0 { Some(message_queues) } else { None },
            "message_queue_drops": if message_queue_drops > 0 { Some(message_queue_drops) } else { None },
            "out_inflights": if out_inflights > 0 { Some(out_inflights) } else { None },
            "out_inflight_drops": if out_inflight_drops > 0 { Some(out_inflight_drops) } else { None },
            "in_inflights": if in_inflights > 0 { Some(in_inflights) } else { None },
            "forwards": if forwards > 0 { Some(forwards) } else { None },
            "message_storages": if message_storage_enabled && message_storages > 0 { Some(message_storages) } else { None },
            "delayed_publishs": if delayed_publishs > 0 { Some(delayed_publishs) } else { None },
            "handoff_pending_messages": if handoff.pending_messages > 0 { Some(handoff.pending_messages) } else { None },
            "handoff_pending_bytes": if handoff.pending_bytes > 0 { Some(handoff.pending_bytes) } else { None },
            "handoff_clients": if handoff.clients > 0 { Some(handoff.clients) } else { None },
            "handoff_active_workers": if handoff.active_workers > 0 { Some(handoff.active_workers) } else { None },
            "handoff_retrying_messages": if handoff.retrying_messages > 0 { Some(handoff.retrying_messages) } else { None },
            "handoff_failed_messages": if handoff.failed_messages > 0 { Some(handoff.failed_messages) } else { None },
            "handoff_rejected_messages": if handoff.rejected_messages > 0 { Some(handoff.rejected_messages) } else { None },
            "handoff_invariant_mismatch_observations": if handoff.invariant_mismatch_observations > 0 { Some(handoff.invariant_mismatch_observations) } else { None },
            "session_handoff_pending": if session_handoff_pending > 0 { Some(session_handoff_pending) } else { None },
            "session_handoff_active": if session_handoff_active > 0 { Some(session_handoff_active) } else { None },
        });
        let pre_disconnect_blocking_counts = json!({
            "handshakings_active": if handshakings_active > 0 { Some(handshakings_active) } else { None },
            "message_queues": if message_queues > 0 { Some(message_queues) } else { None },
            "message_queue_drops": if message_queue_drops > 0 { Some(message_queue_drops) } else { None },
            "out_inflights": if out_inflights > 0 { Some(out_inflights) } else { None },
            "out_inflight_drops": if out_inflight_drops > 0 { Some(out_inflight_drops) } else { None },
            "in_inflights": if in_inflights > 0 { Some(in_inflights) } else { None },
            "forwards": if forwards > 0 { Some(forwards) } else { None },
            "message_storages": if message_storage_enabled && message_storages > 0 { Some(message_storages) } else { None },
            "delayed_publishs": if delayed_publishs > 0 { Some(delayed_publishs) } else { None },
            "handoff_failed_messages": if handoff.failed_messages > 0 { Some(handoff.failed_messages) } else { None },
            "handoff_rejected_messages": if handoff.rejected_messages > 0 { Some(handoff.rejected_messages) } else { None },
            "handoff_capacity_waiters": if handoff.capacity_waiters > 0 { Some(handoff.capacity_waiters) } else { None },
        });
        let blocking_reasons = blocking_counts
            .as_object()
            .map(|counts| {
                counts
                    .iter()
                    .filter_map(|(name, value)| if value.is_null() { None } else { Some(name.as_str()) })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let message_blocking_reasons = message_blocking_counts
            .as_object()
            .map(|counts| {
                counts
                    .iter()
                    .filter_map(|(name, value)| if value.is_null() { None } else { Some(name.as_str()) })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let pre_disconnect_blocking_reasons = pre_disconnect_blocking_counts
            .as_object()
            .map(|counts| {
                counts
                    .iter()
                    .filter_map(|(name, value)| if value.is_null() { None } else { Some(name.as_str()) })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let disconnecting = self.disconnecting.load(Ordering::SeqCst);

        json!({
            "id": id,
            "draining": self.draining.load(Ordering::SeqCst),
            "quiescing": self.draining.load(Ordering::SeqCst) && !disconnecting,
            "disconnecting": disconnecting,
            "force_drain_finalized": self.force_drain_finalized.load(Ordering::SeqCst),
            "pre_disconnect_safe": pre_disconnect_safe,
            "safe_to_leave": strict_safe_to_leave,
            "message_safe_to_leave": message_safe_to_leave,
            "strict_safe_to_leave": strict_safe_to_leave,
            "session_handoff_complete": session_handoff_complete,
            "data_loss_detected": message_queue_drops > 0
                || out_inflight_drops > 0
                || self.shared.handoff_data_loss_detected(),
            "raft_membership_present": raft_status
                .as_ref()
                .map(|status| status.peers.contains_key(&self.scx.node.id()))
                .unwrap_or(false),
            "raft_leader_id": raft_status.as_ref().map(|status| status.leader_id),
            "counts": {
                "connections": connections,
                "sessions": sessions,
                "client_states": client_states,
                "routes": routes,
                "handshakings_active": handshakings_active,
                "message_queues": message_queues,
                "message_queue_drops": message_queue_drops,
                "out_inflights": out_inflights,
                "out_inflight_drops": out_inflight_drops,
                "in_inflights": in_inflights,
                "forwards": forwards,
                "message_storages": message_storages,
                "message_storage_enabled": message_storage_enabled,
                "delayed_publishs": delayed_publishs,
                "handoff_pending_messages": handoff.pending_messages,
                "handoff_pending_bytes": handoff.pending_bytes,
                "handoff_clients": handoff.clients,
                "handoff_active_workers": handoff.active_workers,
                "handoff_retrying_messages": handoff.retrying_messages,
                "handoff_failed_messages": handoff.failed_messages,
                "handoff_rejected_messages": handoff.rejected_messages,
                "handoff_oldest_age_millis": handoff.oldest_age_millis,
                "handoff_progress_sequence": handoff.progress_sequence,
                "handoff_capacity_percent": handoff.capacity_percent,
                "handoff_capacity_waiters": handoff.capacity_waiters,
                "handoff_invariant_mismatch_observations": handoff.invariant_mismatch_observations,
                "session_handoff_pending": session_handoff_pending,
                "session_handoff_active": session_handoff_active,
                "session_handoff_failures": session_handoff_failures,
                "orphaned_router_states_removed": reconciled.state_ids.len(),
                "orphaned_routes_removed": reconciled.routes.len(),
            },
            "orphaned_route_details": orphaned_route_details,
            "blocking_counts": blocking_counts,
            "blocking_reasons": blocking_reasons,
            "message_blocking_counts": message_blocking_counts,
            "message_blocking_reasons": message_blocking_reasons,
            "pre_disconnect_blocking_counts": pre_disconnect_blocking_counts,
            "pre_disconnect_blocking_reasons": pre_disconnect_blocking_reasons,
            "operator_next_step": if message_queue_drops > 0
                || out_inflight_drops > 0
                || self.shared.handoff_data_loss_detected()
                || handoff.failed_messages > 0
                || handoff.rejected_messages > 0
            {
                "abort"
            } else if !disconnecting {
                "disconnect"
            } else if strict_safe_to_leave {
                "leave"
            } else {
                "wait"
            },
            "note": "planned drain first preserves MQTT state, then actively relocates each offline session and its routes to a peer broker; client reconnect is no longer required for the old pod to become safe_to_leave, and any detected queue/inflight loss still blocks pod removal",
        })
    }

    fn routing_status(&self, target_id: NodeId) -> serde_json::Value {
        let client_states = self.router.node_states_count(target_id);
        let routes = self.router.node_routes_count(target_id);
        let active_forwards = self.nodes.grpc_active_tasks(target_id);
        let reliable_forwards = self.shared.handoff_active_for_node(target_id);
        let plain_forwards = self.shared.plain_forwards_for_node(target_id);
        let handoff_pending = self.shared.handoff_pending_for_node(target_id);
        let handoff = self.shared.handoff_stats();
        json!({
            "id": self.scx.node.id(),
            "targetId": target_id,
            "clientStates": client_states,
            "routes": routes,
            "activeForwards": active_forwards,
            "reliableForwards": reliable_forwards,
            "plainForwards": plain_forwards,
            "handoffPending": handoff_pending,
            "handoffPendingBytes": handoff.pending_bytes,
            "handoffOldestAgeMillis": handoff.oldest_age_millis,
            "handoffProgressSequence": handoff.progress_sequence,
            "handoffCapacityPercent": handoff.capacity_percent,
            "handoffCapacityWaiters": handoff.capacity_waiters,
            "handoffFailed": handoff.failed_messages,
            "handoffRejected": handoff.rejected_messages,
            "handoffInvariantMismatchObservations": handoff.invariant_mismatch_observations,
            "dataLossDetected": self.shared.handoff_data_loss_detected(),
            "converged": client_states == 0
                && routes == 0
                && active_forwards == 0
                && reliable_forwards == 0
                && plain_forwards == 0
                && handoff_pending == 0,
        })
    }

    async fn force_drain_finalize(
        &self,
        raft_mailbox: &Mailbox,
        target_id: NodeId,
        reason: &str,
    ) -> Result<serde_json::Value> {
        let local_id = self.scx.node.id();
        if target_id == local_id && !self.draining.load(Ordering::SeqCst) {
            return Err(anyhow!(
                "force_drain_finalize is only allowed while target node {target_id} is draining"
            ));
        }
        let handoff = self.shared.force_discard_handoff_for_target(target_id);
        let mut local_sessions_requested = 0usize;
        let mut local_session_acks = 0usize;
        let mut session_handoffs_discarded = 0usize;
        let mut router_states_removed = 0usize;
        let mut routes_removed = 0usize;

        if target_id == local_id {
            self.disconnecting.store(true, Ordering::SeqCst);

            let entries = self
                .shared
                .inner()
                .iter()
                .filter(|entry| entry.id().node_id == target_id)
                .collect::<Vec<_>>();
            let mut acknowledgements = Vec::with_capacity(entries.len());
            let mut state_ids = self.router.node_state_ids(target_id).into_iter().collect::<HashSet<_>>();
            for entry in entries {
                let id = entry.id();
                state_ids.insert(id.clone());
                let Some(tx) = entry.tx().filter(|tx| !tx.is_closed()) else {
                    continue;
                };
                let (ack_tx, ack_rx) = oneshot::channel();
                match tx.unbounded_send(SessionMessage::Kick(ack_tx, id, true, true)) {
                    Ok(()) => {
                        local_sessions_requested += 1;
                        acknowledgements.push(ack_rx);
                    }
                    Err(error) => {
                        log::warn!("forced drain failed to request local session termination: {error:?}");
                    }
                }
            }
            if !acknowledgements.is_empty() {
                let waits = acknowledgements.into_iter().map(|ack| async move {
                    matches!(tokio::time::timeout(Duration::from_secs(2), ack).await, Ok(Ok(())))
                });
                local_session_acks =
                    futures::future::join_all(waits).await.into_iter().filter(|acked| *acked).count();
            }

            router_states_removed = state_ids.len();
            for id in state_ids {
                let proposal = message::Message::SessionTerminated { id }.encode()?;
                raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
            }
            let routes = self.router.node_routes(target_id);
            routes_removed = routes.len();
            for (topic_filter, id) in routes {
                let proposal = message::Message::Remove { topic_filter: &topic_filter, id }.encode()?;
                raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;
            }
            session_handoffs_discarded = self.shared.force_clear_local_session_handoffs();
            self.force_drain_finalized.store(true, Ordering::SeqCst);
        }

        if handoff.messages > 0
            || local_sessions_requested > 0
            || session_handoffs_discarded > 0
            || router_states_removed > 0
            || routes_removed > 0
        {
            log::warn!(
                "force-finalized drain target_id={} on node_id={} reason={} handoff_messages_discarded={} handoff_bytes_discarded={} handoff_clients_affected={} local_sessions_requested={} local_session_acks={} session_handoffs_discarded={} router_states_removed={} routes_removed={}",
                target_id,
                local_id,
                reason,
                handoff.messages,
                handoff.bytes,
                handoff.clients,
                local_sessions_requested,
                local_session_acks,
                session_handoffs_discarded,
                router_states_removed,
                routes_removed
            );
        }

        Ok(json!({
            "ok": true,
            "nodeId": local_id,
            "targetId": target_id,
            "reason": reason,
            "localTarget": target_id == local_id,
            "forceDrainFinalized": target_id == local_id
                && self.force_drain_finalized.load(Ordering::SeqCst),
            "handoffMessagesDiscarded": handoff.messages,
            "handoffBytesDiscarded": handoff.bytes,
            "handoffClientsAffected": handoff.clients,
            "localSessionsRequested": local_sessions_requested,
            "localSessionAcks": local_session_acks,
            "sessionHandoffsDiscarded": session_handoffs_discarded,
            "routerStatesRemoved": router_states_removed,
            "routesRemoved": routes_removed,
        }))
    }

    async fn leave_cluster(&self, raft_mailbox: &Mailbox) -> Result<serde_json::Value> {
        let id = self.scx.node.id();
        let force_drain_finalized = self.force_drain_finalized.load(Ordering::SeqCst);
        if self.draining.load(Ordering::SeqCst)
            && !force_drain_finalized
            && self
                .drain_status(raft_mailbox)
                .await
                .get("strict_safe_to_leave")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err(anyhow!(
                "node {id} still has active session, connection or message state; refusing raft leave"
            ));
        }
        let local_node =
            self.nodes.local_node_from_config(&self.cfg.node_grpc_addrs, &self.cfg.raft_peer_addrs)?;

        let proposal = message::Message::NodeRemove { id }.encode()?;
        raft_mailbox.send_proposal(proposal).await.map_err(anyhow::Error::new)?;

        if let Err(e) = self.remove_self_from_raft(raft_mailbox).await {
            let restore = message::Message::NodeUp { node: local_node }.encode()?;
            if let Err(restore_err) = raft_mailbox.send_proposal(restore).await {
                log::error!(
                    "raft leave failed and failed to restore business node metadata, id: {id}, \
                     leave_error: {e}, restore_error: {restore_err}"
                );
            }
            return Err(e);
        }

        Ok(json!({
            "ok": true,
            "id": id,
            "raft_membership_changed": true,
            "force_drain_finalized": force_drain_finalized
        }))
    }

    async fn remove_self_from_raft(&self, raft_mailbox: &Mailbox) -> Result<()> {
        let status = raft_mailbox.status().await.map_err(anyhow::Error::new)?;
        if status.is_leader() {
            return raft_mailbox.leave().await.map_err(anyhow::Error::new);
        }

        let leader_id = status.leader_id;
        let leader_addr = status
            .peers
            .get(&leader_id)
            .and_then(|peer| peer.as_ref().map(|peer| peer.addr.to_string()))
            .or_else(|| {
                self.cfg
                    .raft_peer_addrs
                    .iter()
                    .find(|peer| peer.id == leader_id)
                    .map(|peer| peer.addr.to_string())
            })
            .ok_or_else(|| anyhow!("raft leader address not found, leader_id: {leader_id}"))?;

        log::info!(
            "request raft leader to remove local node, node_id: {}, leader_id: {}, leader_addr: {}",
            self.scx.node.id(),
            leader_id,
            leader_addr
        );
        raft_admin::remove_node(
            &leader_addr,
            self.scx.node.id(),
            self.cfg.raft.grpc_timeout.unwrap_or(Duration::from_secs(6)),
            self.cfg.raft.grpc_concurrency_limit.unwrap_or(200),
            self.cfg.raft.grpc_message_size.unwrap_or(50 * 1024 * 1024),
        )
        .await
    }
}

async fn parse_addr(addr: &str) -> Result<SocketAddr> {
    for i in 0..10 {
        match addr.to_socket_addrs() {
            Ok(mut to_socket_addrs) => {
                if let Some(a) = to_socket_addrs.next() {
                    log::info!("Round: {i}, parse_addr({addr:?}), addr is {a:?}");
                    return Ok(a);
                } else {
                    log::warn!("Round: {i}, parse_addr({addr:?}), next is None");
                }
            }
            Err(e) => {
                log::warn!("Round: {i}, {e:?}");
            }
        }
        tokio::time::sleep(Duration::from_millis((rand::random::<u64>() % 300) + 500)).await;
    }
    Err(anyhow!(format!("Parsing address{:?} error", addr)))
}

async fn find_actual_leader(
    raft: &Raft<ClusterRouter>,
    peer_addrs: Vec<String>,
    rounds: usize,
) -> Result<Option<(NodeId, String)>> {
    let mut actual_leader_info = None;
    for i in 0..rounds {
        actual_leader_info = raft.find_leader_info(peer_addrs.clone()).await.map_err(|e| anyhow!(e))?;
        if actual_leader_info.is_some() {
            break;
        }
        log::info!("Leader not found, rounds: {i}");
        sleep(Duration::from_millis(500)).await;
    }
    Ok(actual_leader_info)
}

async fn find_actual_leader_retry(
    raft: &Raft<ClusterRouter>,
    peer_addrs: Vec<String>,
    rounds: usize,
) -> Result<Option<(NodeId, String)>> {
    if peer_addrs.is_empty() {
        return Ok(None);
    }

    let mut actual_leader_info = None;
    for i in 0..rounds {
        match raft.find_leader_info(peer_addrs.clone()).await {
            Ok(Some(leader_info)) => {
                actual_leader_info = Some(leader_info);
                break;
            }
            Ok(None) => log::info!("Leader not found, round: {i}"),
            Err(e) => log::warn!("Leader discovery failed, round: {i}, error: {e}"),
        }
        sleep(Duration::from_millis(500)).await;
    }
    Ok(actual_leader_info)
}

#[inline]
pub(crate) async fn hook_message_dropped(scx: &ServerContext, droppeds: Vec<(To, From, Publish, Reason)>) {
    for (to, from, publish, reason) in droppeds {
        //hook, message_dropped
        scx.extends.hook_mgr().message_dropped(Some(to), from, publish, reason).await;
    }
}
