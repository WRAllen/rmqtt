use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use backoff::{future::retry, ExponentialBackoff};
use rust_box::task_exec_queue::{SpawnExt, TaskExecQueue};

use super::config::PluginConfig;
use super::message::{Message, RaftGrpcMessage, RaftGrpcMessageReply};
use super::router::ClusterRouter;
use super::{
    hook_message_dropped,
    shared::{effective_source_session_policy, ClusterShared},
};
use rmqtt::context::ServerContext;
use rmqtt::{
    grpc::{Message as GrpcMessage, MessageReply},
    hook::{Handler, HookResult, Parameter, ReturnType},
    shared::Shared,
    types::Id,
};

pub(crate) struct HookHandler {
    scx: ServerContext,
    exec: TaskExecQueue,
    backoff_strategy: ExponentialBackoff,
    shared: ClusterShared,
    router: ClusterRouter,
}

impl HookHandler {
    pub(crate) fn new(
        scx: ServerContext,
        cfg: Arc<PluginConfig>,
        backoff_strategy: ExponentialBackoff,
        shared: ClusterShared,
        router: ClusterRouter,
    ) -> Self {
        let exec =
            scx.get_exec(("RAFT_DISCONNECTED_EXEC", cfg.task_exec_queue_workers, cfg.task_exec_queue_max));
        Self { scx, exec, backoff_strategy, shared, router }
    }
}

#[async_trait]
impl Handler for HookHandler {
    async fn hook(&self, param: &Parameter, acc: Option<HookResult>) -> ReturnType {
        log::debug!("hook, Parameter type: {:?}", param.get_type());
        match param {
            Parameter::ClientDisconnected(s, r) => {
                log::debug!("{:?} hook::ClientDisconnected reason: {:?}", s.id, r);
                if !r.is_kicked(false) {
                    let msg = Message::Disconnected { id: s.id.clone() }.encode();
                    match msg {
                        Err(e) => {
                            log::warn!("HookHandler, Message::Disconnected, message encode error, {e:?}");
                        }
                        Ok(msg) => {
                            let router = self.router.clone();
                            let exec = self.exec.clone();
                            let backoff_strategy = self.backoff_strategy.clone();
                            tokio::spawn(async move {
                                if let Err(e) = retry(backoff_strategy, || async {
                                    let msg = msg.clone();
                                    let mailbox = router.raft_mailbox().await;
                                    let res = async move { mailbox.send_proposal(msg).await }
                                        .spawn(&exec)
                                        .result()
                                        .await
                                        .map_err(|_| {
                                             anyhow!(
                                                "Handler::hook(Message::Disconnected), task execution failure",
                                            )
                                        })?
                                        .map_err(|e|  anyhow!(e.to_string()))?;
                                    Ok(res)
                                })
                                    .await
                                {
                                    log::warn!(
                                        "HookHandler, Message::Disconnected, raft mailbox send error, {e:?}"
                                    );
                                }
                            });
                        }
                    };
                }
            }

            Parameter::SessionTerminated(s, _r) => {
                let msg = Message::SessionTerminated { id: s.id.clone() }.encode();
                match msg {
                    Err(e) => {
                        log::warn!("HookHandler, Message::SessionTerminated, message encode error, {e:?}");
                    }
                    Ok(msg) => {
                        let router = self.router.clone();
                        let exec = self.exec.clone();
                        let backoff_strategy = self.backoff_strategy.clone();
                        tokio::spawn(async move {
                            if let Err(e) = retry(backoff_strategy, || async {
                                let msg = msg.clone();
                                let mailbox = router.raft_mailbox().await;
                                let res = async move { mailbox.send_proposal(msg).await }
                                    .spawn(&exec)
                                    .result()
                                    .await
                                    .map_err(|_| {
                                         anyhow!(
                                            "Handler::hook(Message::SessionTerminated), task execution failure",
                                        )
                                    })?
                                    .map_err(|e|  anyhow!(e.to_string()))?;
                                Ok(res)
                            })
                                .await
                            {
                                log::warn!(
                                    "HookHandler, Message::SessionTerminated, raft mailbox send error, {e:?}"
                                );
                            }
                        });
                    }
                }
            }

            Parameter::GrpcMessageReceived(typ, msg) => {
                log::debug!("GrpcMessageReceived, type: {typ}, msg: {msg:?}");
                if self.shared.message_type != *typ {
                    return (true, acc);
                }
                match msg {
                    GrpcMessage::ForwardsTo(from, publish, sub_rels) => {
                        if let Err(droppeds) =
                            self.shared.forwards_to(from.clone(), publish, sub_rels.clone()).await
                        {
                            let reason = droppeds
                                .first()
                                .map(|(_, _, _, reason)| reason.to_string())
                                .unwrap_or_else(|| "local delivery failed".to_string());
                            hook_message_dropped(&self.scx, droppeds).await;
                            return (
                                false,
                                Some(HookResult::GrpcMessageReply(Ok(MessageReply::Error(reason)))),
                            );
                        }
                        return (false, Some(HookResult::GrpcMessageReply(Ok(MessageReply::Success))));
                    }
                    GrpcMessage::Kick(id, clean_start, clear_subscriptions, is_admin)
                    | GrpcMessage::KickWithShared(id, clean_start, clear_subscriptions, is_admin) => {
                        let with_shared = matches!(msg, GrpcMessage::KickWithShared(..));
                        if !with_shared && !*is_admin {
                            // Checking today's queue is insufficient: an older
                            // route can still enqueue another shared delivery
                            // before Kick freezes the actor. Reject before any
                            // lock, extraction or disconnect instead of losing
                            // ownership metadata through a legacy snapshot.
                            return (
                                false,
                                Some(HookResult::GrpcMessageReply(Ok(MessageReply::Error(
                                    "legacy non-admin Kick is unsupported by this source; the destination must negotiate shared-delivery session context".to_string(),
                                )))),
                            );
                        }
                        let _transition = self.shared.begin_session_transition(id.client_id.clone());
                        let mut entry = match self.shared.lock_session_for_takeover(id.clone()).await {
                            Ok(entry) => entry,
                            Err(error) => return (false, Some(HookResult::GrpcMessageReply(Err(error)))),
                        };
                        let source_draining = self.shared.is_draining();
                        let source_session_id = entry.session().map(|session| session.id.clone());
                        let preserve_relocated = source_session_id
                            .as_ref()
                            .is_some_and(|source_id| self.shared.preserve_session_on_takeover(source_id));

                        // The first response may be lost after the source has
                        // already extracted and detached the session.  During
                        // a planned drain, replay the cached snapshot for a
                        // retry instead of reporting NotExist and allowing a
                        // Clean-Start reconnect to discard the old state.
                        if source_draining && !*is_admin && !entry.exist() {
                            if let Some(cached) = self.shared.cached_drain_session_handoff(&id.client_id) {
                                log::warn!(
                                    "{id:?} replaying cached session handoff after the source entry was detached"
                                );
                                self.shared.complete_pull_session_handoff(&id.client_id);
                                return (
                                    false,
                                    Some(HookResult::GrpcMessageReply(Ok(if with_shared {
                                        MessageReply::KickWithShared(
                                            rmqtt::grpc::SharedOfflineSession::from_session(cached),
                                        )
                                    } else {
                                        MessageReply::Kick(cached)
                                    }))),
                                );
                            }
                        }

                        // A client such as LM Gateway reconnects with Clean
                        // Start after the broker intentionally disconnects it.
                        // On a draining source node, return the complete
                        // session state instead of silently discarding it. The
                        // destination recognizes state returned across Clean
                        // Start as a planned handoff and restores subscriptions
                        // before the client performs its normal resubscribe.
                        let (source_clean_start, source_clear_subscriptions) =
                            effective_source_session_policy(
                                *clean_start,
                                *clear_subscriptions,
                                source_draining || preserve_relocated,
                                *is_admin,
                            );
                        if (*clean_start != source_clean_start
                            || *clear_subscriptions != source_clear_subscriptions)
                            && !*is_admin
                        {
                            log::debug!(
                                "{id:?} preserving pending session state and routes for planned drain"
                            );
                        }
                        let new_acc =
                            match entry.kick(source_clean_start, source_clear_subscriptions, *is_admin).await
                            {
                                Ok(o) => {
                                    if source_draining && !*is_admin {
                                        self.shared.cache_drain_session_handoff(id.client_id.clone(), &o);
                                    }
                                    if preserve_relocated && !*is_admin {
                                        if let Some(source_id) = source_session_id.as_ref() {
                                            self.shared.complete_relocated_takeover(source_id);
                                        }
                                    }
                                    HookResult::GrpcMessageReply(Ok(if with_shared {
                                        MessageReply::KickWithShared(
                                            rmqtt::grpc::SharedOfflineSession::from_session(o),
                                        )
                                    } else {
                                        MessageReply::Kick(o)
                                    }))
                                }
                                Err(e) => HookResult::GrpcMessageReply(Err(e)),
                            };
                        return (false, Some(new_acc));
                    }
                    GrpcMessage::GetRetains(topic_filter) => {
                        log::error!("unreachable!(), GrpcMessage::GetRetains({topic_filter})");
                    }
                    GrpcMessage::SubscriptionsGet(clientid) => {
                        let id = Id::from(self.scx.node.id(), clientid.clone());
                        let entry = self.shared.inner().entry(id);
                        let new_acc = HookResult::GrpcMessageReply(Ok(MessageReply::SubscriptionsGet(
                            entry.subscriptions().await,
                        )));
                        return (false, Some(new_acc));
                    }
                    GrpcMessage::Data(data) => {
                        let new_acc = match RaftGrpcMessage::decode(data) {
                            Err(e) => {
                                log::error!("Message::decode, error: {e:?}");
                                HookResult::GrpcMessageReply(Ok(MessageReply::Error(e.to_string())))
                            }
                            Ok(RaftGrpcMessage::GetNodeHealthStatus) => {
                                match self.shared.health_status().await {
                                    Ok(status) => {
                                        match RaftGrpcMessageReply::GetNodeHealthStatus(status).encode() {
                                            Ok(ress) => {
                                                HookResult::GrpcMessageReply(Ok(MessageReply::Data(ress)))
                                            }
                                            Err(e) => HookResult::GrpcMessageReply(Ok(MessageReply::Error(
                                                e.to_string(),
                                            ))),
                                        }
                                    }
                                    Err(e) => {
                                        HookResult::GrpcMessageReply(Ok(MessageReply::Error(e.to_string())))
                                    }
                                }
                            }
                            Ok(RaftGrpcMessage::ReliableForwardBatch(forwards)) => {
                                let statuses = self.shared.accept_reliable_forward_batch(forwards).await;
                                match RaftGrpcMessageReply::ReliableForwardBatch(statuses).encode() {
                                    Ok(response) => {
                                        HookResult::GrpcMessageReply(Ok(MessageReply::Data(response)))
                                    }
                                    Err(e) => {
                                        HookResult::GrpcMessageReply(Ok(MessageReply::Error(e.to_string())))
                                    }
                                }
                            }
                            Ok(RaftGrpcMessage::SessionHandoff(handoff)) => {
                                match self.shared.accept_session_handoff(handoff).await {
                                    Ok(reply) => match RaftGrpcMessageReply::SessionHandoff(reply).encode() {
                                        Ok(response) => {
                                            HookResult::GrpcMessageReply(Ok(MessageReply::Data(response)))
                                        }
                                        Err(e) => HookResult::GrpcMessageReply(Ok(MessageReply::Error(
                                            e.to_string(),
                                        ))),
                                    },
                                    Err(e) => {
                                        HookResult::GrpcMessageReply(Ok(MessageReply::Error(e.to_string())))
                                    }
                                }
                            }
                            Ok(RaftGrpcMessage::GetForwardingCapabilities) => {
                                let result =
                                    RaftGrpcMessageReply::GetForwardingCapabilities { version: 1 }.encode();
                                HookResult::GrpcMessageReply(Ok(match result {
                                    Ok(data) => MessageReply::Data(data),
                                    Err(error) => MessageReply::Error(error.to_string()),
                                }))
                            }
                            Ok(RaftGrpcMessage::SessionHandoffWithShared(snapshot)) => {
                                let result = match snapshot.restore() {
                                    Ok(handoff) => self.shared.accept_session_handoff(handoff).await,
                                    Err(error) => Err(error),
                                }
                                .and_then(|reply| RaftGrpcMessageReply::SessionHandoff(reply).encode());
                                HookResult::GrpcMessageReply(Ok(match result {
                                    Ok(data) => MessageReply::Data(data),
                                    Err(error) => MessageReply::Error(error.to_string()),
                                }))
                            }
                            Ok(RaftGrpcMessage::ProbeHandoffSession { client_id }) => {
                                let result =
                                    self.shared.probe_handoff_session(&client_id).await.and_then(|busy| {
                                        RaftGrpcMessageReply::HandoffSessionProbe {
                                            node_id: self.scx.node.id(),
                                            busy,
                                        }
                                        .encode()
                                    });
                                HookResult::GrpcMessageReply(Ok(match result {
                                    Ok(data) => MessageReply::Data(data),
                                    Err(error) => MessageReply::Error(error.to_string()),
                                }))
                            }
                            Ok(RaftGrpcMessage::ReconcileHandoffSession { client_id, expected_owner }) => {
                                let result = self
                                    .shared
                                    .reconcile_handoff_session(&client_id, expected_owner.as_ref())
                                    .await
                                    .and_then(|gone| {
                                        RaftGrpcMessageReply::HandoffSessionReconciled {
                                            client_id,
                                            expected_owner,
                                            gone,
                                        }
                                        .encode()
                                    });
                                HookResult::GrpcMessageReply(Ok(match result {
                                    Ok(data) => MessageReply::Data(data),
                                    Err(error) => MessageReply::Error(error.to_string()),
                                }))
                            }
                        };
                        return (false, Some(new_acc));
                    }
                    _ => {
                        log::error!("unimplemented, {param:?}")
                    }
                }
            }
            _ => {
                log::error!("unimplemented, {param:?}")
            }
        }
        (true, acc)
    }
}

#[cfg(test)]
mod shared_pull_tests {
    use super::*;

    #[tokio::test]
    async fn legacy_pull_is_rejected_before_locking_and_versioned_pull_is_supported() {
        let scx = ServerContext::new()
            .node_id(1)
            .busy_check_enable(false)
            .plugins_config_map_add(
                "rmqtt-cluster-raft",
                "node_grpc_addrs = []\nraft_peer_addrs = []\ntask_exec_queue_workers = 1",
            )
            .build()
            .await;
        let plugin = crate::ClusterPlugin::new(scx.clone(), "rmqtt-cluster-raft").await.unwrap();
        let handler = HookHandler::new(
            scx,
            plugin.cfg.clone(),
            plugin.backoff_strategy.clone(),
            plugin.shared.clone(),
            plugin.router.clone(),
        );
        let id = Id::from(1, "legacy-pull-worker".into());
        let locked = plugin.shared.inner().entry(id.clone()).try_lock().await.unwrap();
        let (_, result) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handler.hook(
                &Parameter::GrpcMessageReceived(
                    plugin.shared.message_type,
                    GrpcMessage::Kick(id.clone(), false, true, false),
                ),
                None,
            ),
        )
        .await
        .expect("legacy pull must be rejected before waiting for a session lock");
        match result {
            Some(HookResult::GrpcMessageReply(Ok(MessageReply::Error(reason)))) => {
                assert!(reason.contains("destination must negotiate"));
            }
            _ => panic!("expected explicit legacy pull incompatibility"),
        }
        drop(locked);

        let (_, result) = handler
            .hook(
                &Parameter::GrpcMessageReceived(
                    plugin.shared.message_type,
                    GrpcMessage::KickWithShared(id.clone(), false, true, false),
                ),
                None,
            )
            .await;
        match result {
            Some(HookResult::GrpcMessageReply(Ok(MessageReply::KickWithShared(snapshot)))) => {
                assert!(matches!(snapshot.restore().unwrap(), rmqtt::types::OfflineSession::NotExist));
            }
            _ => panic!("expected versioned pull reply"),
        }

        let (_, result) = handler
            .hook(
                &Parameter::GrpcMessageReceived(
                    plugin.shared.message_type,
                    GrpcMessage::Kick(id, false, true, true),
                ),
                None,
            )
            .await;
        assert!(matches!(
            result,
            Some(HookResult::GrpcMessageReply(Ok(MessageReply::Kick(
                rmqtt::types::OfflineSession::NotExist
            ))))
        ));
    }
}
