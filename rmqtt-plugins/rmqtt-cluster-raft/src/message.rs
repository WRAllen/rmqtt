use std::sync::Arc;

use serde::{Deserialize, Serialize};

use rmqtt::session::OfflineInfo;
use rmqtt::types::{
    Addr, ClientId, ConnectInfo, From, Id, NodeHealthStatus, NodeId, Publish, SharedDeliveryContext,
    SubRelations, SubscriptionOptions,
};
use rmqtt::Result;

use super::Mailbox;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message<'a> {
    HandshakeTryLock { id: Id },
    Connected { id: Id },
    Disconnected { id: Id },
    SessionTerminated { id: Id },
    Add { topic_filter: &'a str, id: Id, opts: SubscriptionOptions },
    Remove { topic_filter: &'a str, id: Id },
    //get client node id
    GetClientNodeId { client_id: &'a str },
    Ping,
    NodeUp { node: ClusterNode },
    NodeDown { id: NodeId },
    NodeRemove { id: NodeId },
}

/// Business-layer cluster node metadata replicated through Raft.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClusterNode {
    pub id: NodeId,
    pub grpc_addr: Addr,
    pub raft_addr: Addr,
}

impl<'a> Message<'a> {
    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(anyhow::Error::new)
    }
    #[inline]
    pub fn _decode(data: &'a [u8]) -> Result<Self> {
        bincode::deserialize::<Self>(data).map_err(anyhow::Error::new)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageReply {
    Error(String),
    HandshakeTryLock(Option<Id>),
    Ping,
    // Appended to preserve existing bincode discriminants. No new Raft log
    // command is needed: older leaders simply cannot certify this cleanup.
    SessionTerminated { removed: bool },
}

impl MessageReply {
    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(anyhow::Error::new)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> Result<MessageReply> {
        bincode::deserialize::<MessageReply>(data).map_err(anyhow::Error::new)
    }
}

#[inline]
pub(crate) async fn get_client_node_id(raft_mailbox: Mailbox, client_id: &str) -> Result<Option<NodeId>> {
    let msg = Message::GetClientNodeId { client_id }.encode()?;
    let reply = raft_mailbox.query(msg).await.map_err(anyhow::Error::new)?;
    if !reply.is_empty() {
        Ok(bincode::deserialize(&reply).map_err(anyhow::Error::new)?)
    } else {
        Ok(None)
    }
}

#[allow(dead_code)]
#[inline]
pub async fn ping(raft_mailbox: &Mailbox) -> Result<Option<MessageReply>> {
    let msg = Message::Ping.encode()?;
    let reply = raft_mailbox.send_proposal(msg).await.map_err(anyhow::Error::new)?;
    if !reply.is_empty() {
        Ok(Some(MessageReply::decode(&reply)?))
    } else {
        Ok(None)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftGrpcMessage {
    GetNodeHealthStatus,
    ReliableForwardBatch(Vec<ReliableForward>),
    /// Move an already-disconnected MQTT session to another broker.  This is
    /// deliberately an internal gRPC message rather than a Raft log variant,
    /// so rolling upgrades never ask an older state machine to decode a new
    /// replicated command.
    SessionHandoff(SessionHandoff),
    ProbeHandoffSession {
        client_id: ClientId,
    },
    ReconcileHandoffSession {
        client_id: ClientId,
        expected_owner: Option<Id>,
    },
    // Append only: the legacy session snapshot and Publish layouts stay intact.
    GetForwardingCapabilities,
    SessionHandoffWithShared(SharedSessionHandoff),
}

impl RaftGrpcMessage {
    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(anyhow::Error::new)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        bincode::deserialize::<Self>(data).map_err(anyhow::Error::new)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftGrpcMessageReply {
    GetNodeHealthStatus(NodeHealthStatus),
    ReliableForwardBatch(Vec<ReliableForwardReply>),
    SessionHandoff(SessionHandoffReply),
    HandoffSessionProbe { node_id: NodeId, busy: bool },
    HandoffSessionReconciled { client_id: ClientId, expected_owner: Option<Id>, gone: bool },
    GetForwardingCapabilities { version: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionHandoff {
    pub offline: OfflineInfo,
    pub connect_info: ConnectInfo,
    pub superuser: bool,
    pub session_expiry_millis: u64,
}

impl SessionHandoff {
    pub(crate) fn has_shared_context(&self) -> bool {
        self.offline.offline_messages.iter().any(|(_, publish)| publish.shared_delivery_not_started())
    }
}

/// An opt-in wrapper negotiated before sending a snapshot to a new broker.
/// The sidecar position corresponds exactly to the unsent offline queue; it
/// must never be applied to inflight messages, whose MQTT delivery has started.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SharedSessionHandoff {
    pub handoff: SessionHandoff,
    pub queued_contexts: Vec<Option<SharedDeliveryContext>>,
}

impl SharedSessionHandoff {
    pub(crate) fn from_handoff(mut handoff: SessionHandoff) -> Self {
        let queued_contexts = handoff
            .offline
            .offline_messages
            .iter_mut()
            .map(|(_, publish)| {
                let not_started = publish.shared_delivery_not_started();
                let context = publish.shared_delivery.take();
                if not_started {
                    context.map(|context| context.as_ref().clone())
                } else {
                    None
                }
            })
            .collect();
        for message in &mut handoff.offline.inflight_messages {
            message.publish.shared_delivery = None;
        }
        Self { handoff, queued_contexts }
    }

    pub(crate) fn restore(mut self) -> Result<SessionHandoff> {
        anyhow::ensure!(
            self.queued_contexts.len() == self.handoff.offline.offline_messages.len(),
            "shared handoff sidecar length does not match the offline queue"
        );
        let session_id = &self.handoff.offline.id;
        for ((_, publish), context) in self.handoff.offline.offline_messages.iter().zip(&self.queued_contexts)
        {
            if let Some(context) = context {
                anyhow::ensure!(
                    !publish.dup && publish.packet_id.is_none(),
                    "shared handoff context cannot be attached to a started delivery"
                );
                anyhow::ensure!(
                    context.target_id.client_id == session_id.client_id
                        && context.target_id.create_time == session_id.create_time,
                    "shared handoff context does not belong to the snapshot session"
                );
                let filter = context.topic_filter.parse::<rmqtt::topic::Topic>()?;
                anyhow::ensure!(
                    filter.matches_str(&publish.topic),
                    "shared handoff context does not match the queued publication"
                );
            }
        }
        for ((_, publish), context) in
            self.handoff.offline.offline_messages.iter_mut().zip(self.queued_contexts)
        {
            publish.shared_delivery = context.map(Arc::new);
        }
        for message in &mut self.handoff.offline.inflight_messages {
            message.publish.shared_delivery = None;
        }
        Ok(self.handoff)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionHandoffReply {
    pub destination_id: Id,
    /// False means a newer client connection had already taken ownership; in
    /// that case the source snapshot must not overwrite it and can be retired.
    pub installed: bool,
}

impl RaftGrpcMessageReply {
    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(anyhow::Error::new)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> Result<Self> {
        bincode::deserialize::<Self>(data).map_err(anyhow::Error::new)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryId {
    pub origin_node_id: NodeId,
    pub boot_id: u64,
    pub sequence: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReliableForward {
    pub delivery_id: DeliveryId,
    pub target_client_id: ClientId,
    pub from: From,
    pub publish: Publish,
    pub relations: SubRelations,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReliableForwardReply {
    pub delivery_id: DeliveryId,
    pub status: ReliableForwardStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ReliableForwardStatus {
    Accepted,
    Delivered,
    Duplicate,
    Retryable(String),
    Rejected(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmqtt::inflight::{MomentStatus, OutInflightMessage};
    use rmqtt::types::{CodecPublish, QoS};

    fn shared_handoff() -> SessionHandoff {
        let id = Id::from(4, ClientId::from_static("consumer"));
        let from = From::from_custom(Id::from(1, ClientId::from_static("publisher")));
        let mut publish = Publish::from(CodecPublish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: "sensor/a/temperature".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(b"queued-shared-delivery"),
            properties: None,
        });
        publish.shared_delivery = Some(Arc::new(SharedDeliveryContext {
            group: "workers".into(),
            topic_filter: "sensor/+/temperature".into(),
            source_qos: QoS::ExactlyOnce,
            source_retain: true,
            target_id: id.clone(),
            subscription_generation: 17,
        }));
        let mut ordinary = publish.clone();
        ordinary.shared_delivery = None;
        ordinary.topic = "ordinary/message".into();
        let mut inflight_publish = publish.clone();
        inflight_publish.packet_id = std::num::NonZeroU16::new(2);
        SessionHandoff {
            offline: OfflineInfo {
                id: id.clone(),
                subscriptions: Vec::new(),
                offline_messages: [(from.clone(), publish), (from.clone(), ordinary)].into_iter().collect(),
                inflight_messages: vec![OutInflightMessage::new(
                    MomentStatus::UnReceived,
                    from,
                    inflight_publish,
                )],
                created_at: 123,
            },
            connect_info: id.into(),
            superuser: false,
            session_expiry_millis: 60_000,
        }
    }

    #[test]
    fn existing_rpc_and_raft_discriminants_remain_wire_compatible() {
        assert_eq!(Message::GetClientNodeId { client_id: "x" }.encode().unwrap()[..4], 6u32.to_le_bytes());
        assert_eq!(MessageReply::Ping.encode().unwrap(), 2u32.to_le_bytes());
        assert_eq!(RaftGrpcMessage::GetNodeHealthStatus.encode().unwrap(), 0u32.to_le_bytes());
        let batch = RaftGrpcMessage::ReliableForwardBatch(Vec::new()).encode().unwrap();
        assert_eq!(&batch[..4], &1u32.to_le_bytes());
        assert_eq!(&batch[4..], &0u64.to_le_bytes());
        let ack = MessageReply::SessionTerminated { removed: true }.encode().unwrap();
        assert!(matches!(
            MessageReply::decode(&ack).unwrap(),
            MessageReply::SessionTerminated { removed: true }
        ));

        let id = Id::from(4, "consumer".into());
        for (index, message) in [
            (2u32, RaftGrpcMessage::SessionHandoff(shared_handoff())),
            (3, RaftGrpcMessage::ProbeHandoffSession { client_id: id.client_id.clone() }),
            (
                4,
                RaftGrpcMessage::ReconcileHandoffSession {
                    client_id: id.client_id.clone(),
                    expected_owner: Some(id.clone()),
                },
            ),
        ] {
            assert_eq!(&message.encode().unwrap()[..4], &index.to_le_bytes());
        }
        for (index, reply) in [
            (
                0u32,
                RaftGrpcMessageReply::GetNodeHealthStatus(NodeHealthStatus {
                    node_id: 4,
                    running: true,
                    leader_id: Some(1),
                    descr: None,
                }),
            ),
            (1, RaftGrpcMessageReply::ReliableForwardBatch(Vec::new())),
            (
                2,
                RaftGrpcMessageReply::SessionHandoff(SessionHandoffReply {
                    destination_id: id.clone(),
                    installed: true,
                }),
            ),
            (3, RaftGrpcMessageReply::HandoffSessionProbe { node_id: 4, busy: false }),
            (
                4,
                RaftGrpcMessageReply::HandoffSessionReconciled {
                    client_id: id.client_id.clone(),
                    expected_owner: Some(id),
                    gone: false,
                },
            ),
        ] {
            assert_eq!(&reply.encode().unwrap()[..4], &index.to_le_bytes());
        }
    }

    #[test]
    fn shared_context_keeps_the_legacy_publish_encoding_unchanged() {
        #[derive(Serialize)]
        struct LegacyPublish<'a> {
            inner: &'a CodecPublish,
            target_clientid: &'a Option<ClientId>,
            delay_interval: Option<u32>,
            create_time: Option<i64>,
        }

        let handoff = shared_handoff();
        let publish = &handoff.offline.offline_messages[0].1;
        let expected = bincode::serialize(&LegacyPublish {
            inner: &publish.inner,
            target_clientid: &publish.target_clientid,
            delay_interval: publish.delay_interval,
            create_time: publish.create_time,
        })
        .unwrap();
        let encoded = bincode::serialize(publish).unwrap();
        assert_eq!(encoded, expected);
        let restored = bincode::deserialize::<Publish>(&encoded).unwrap();
        assert!(restored.shared_delivery.is_none());

        let mut without_context = handoff.clone();
        for (_, publish) in &mut without_context.offline.offline_messages {
            publish.shared_delivery = None;
        }
        for message in &mut without_context.offline.inflight_messages {
            message.publish.shared_delivery = None;
        }
        assert_eq!(bincode::serialize(&handoff).unwrap(), bincode::serialize(&without_context).unwrap());
    }

    #[test]
    fn shared_snapshot_roundtrip_restores_only_unsent_delivery_contexts() {
        let handoff = shared_handoff();
        assert!(handoff.has_shared_context());
        let expected_context = handoff.offline.offline_messages[0].1.shared_delivery.clone().unwrap();
        let wrapped = SharedSessionHandoff::from_handoff(handoff);
        assert_eq!(wrapped.queued_contexts.len(), 2);
        assert!(wrapped.queued_contexts[0].is_some());
        assert!(wrapped.queued_contexts[1].is_none());
        assert!(!wrapped.handoff.has_shared_context());
        assert!(wrapped.handoff.offline.inflight_messages[0].publish.shared_delivery.is_none());
        let encoded = RaftGrpcMessage::SessionHandoffWithShared(wrapped).encode().unwrap();
        assert_eq!(&encoded[..4], &6u32.to_le_bytes());
        let RaftGrpcMessage::SessionHandoffWithShared(wrapped) = RaftGrpcMessage::decode(&encoded).unwrap()
        else {
            panic!("unexpected shared session snapshot");
        };
        let restored = wrapped.restore().unwrap();
        let context = restored.offline.offline_messages[0].1.shared_delivery.as_ref().unwrap();
        assert_eq!(context.group, expected_context.group);
        assert_eq!(context.topic_filter, expected_context.topic_filter);
        assert_eq!(context.source_qos, expected_context.source_qos);
        assert_eq!(context.source_retain, expected_context.source_retain);
        assert_eq!(context.target_id, expected_context.target_id);
        assert_eq!(context.subscription_generation, expected_context.subscription_generation);
        assert_eq!(restored.offline.offline_messages[0].1.payload.as_ref(), b"queued-shared-delivery");
        assert!(restored.offline.offline_messages[1].1.shared_delivery.is_none());
        assert!(restored.offline.inflight_messages[0].publish.shared_delivery.is_none());
        assert_eq!(restored.offline.inflight_messages[0].status, MomentStatus::UnReceived);
    }

    #[test]
    fn shared_snapshot_rejects_incomplete_or_extra_sidecar_entries() {
        let wrapped = SharedSessionHandoff::from_handoff(shared_handoff());
        let mut short = wrapped.clone();
        short.queued_contexts.pop();
        assert!(short.restore().is_err());
        let mut long = wrapped;
        long.queued_contexts.push(None);
        assert!(long.restore().is_err());
    }

    #[test]
    fn retired_subscription_context_remains_available_for_receiver_recovery() {
        let mut wrapped = SharedSessionHandoff::from_handoff(shared_handoff());
        // The snapshot intentionally need not contain this subscription: the
        // receiving actor must recover the unsent obligation after retirement.
        assert!(wrapped.handoff.offline.subscriptions.is_empty());
        wrapped.queued_contexts[0].as_mut().unwrap().subscription_generation = 0;
        let encoded = bincode::serialize(&wrapped).unwrap();
        let restored = bincode::deserialize::<SharedSessionHandoff>(&encoded).unwrap().restore().unwrap();
        assert_eq!(
            restored.offline.offline_messages[0].1.shared_delivery.as_ref().unwrap().subscription_generation,
            0
        );
    }

    #[test]
    fn started_context_is_not_exported_as_an_unsent_obligation() {
        let mut handoff = shared_handoff();
        handoff.offline.offline_messages[0].1.dup = true;
        assert!(!handoff.has_shared_context());
        let wrapped = SharedSessionHandoff::from_handoff(handoff);
        assert!(wrapped.queued_contexts.iter().all(Option::is_none));
        assert!(wrapped.handoff.offline.offline_messages[0].1.shared_delivery.is_none());
        assert!(wrapped.handoff.offline.inflight_messages[0].publish.shared_delivery.is_none());
    }

    #[test]
    fn shared_snapshot_rejects_wrong_session_epoch_and_publication() {
        let wrapped = SharedSessionHandoff::from_handoff(shared_handoff());
        let mut wrong_session = wrapped.clone();
        wrong_session.queued_contexts[0].as_mut().unwrap().target_id =
            Id::from(4, ClientId::from_static("other-consumer"));
        assert!(wrong_session.restore().is_err());

        let mut wrong_epoch = wrapped.clone();
        wrong_epoch.handoff.offline.id = serde_json::from_value(serde_json::json!({
            "node_id": 4,
            "lid": 0,
            "local_addr": null,
            "remote_addr": null,
            "client_id": "consumer",
            "username": null,
            "create_time": wrapped.handoff.offline.id.create_time + 1
        }))
        .unwrap();
        assert!(wrong_epoch.restore().is_err());

        let mut wrong_publication = wrapped.clone();
        wrong_publication.queued_contexts.swap(0, 1);
        assert!(wrong_publication.restore().is_err());

        let mut already_started = wrapped.clone();
        already_started.handoff.offline.offline_messages[0].1.packet_id = std::num::NonZeroU16::new(7);
        assert!(already_started.restore().is_err());

        let mut relocated = wrapped;
        relocated.handoff.offline.id = relocated.handoff.offline.id.relocated_to(8);
        assert!(relocated.restore().is_ok());
    }

    #[test]
    fn forwarding_capabilities_are_an_append_only_roundtrip() {
        let encoded = RaftGrpcMessage::GetForwardingCapabilities.encode().unwrap();
        assert_eq!(encoded, 5u32.to_le_bytes());
        assert!(matches!(
            RaftGrpcMessage::decode(&encoded).unwrap(),
            RaftGrpcMessage::GetForwardingCapabilities
        ));
        let encoded = RaftGrpcMessageReply::GetForwardingCapabilities { version: 1 }.encode().unwrap();
        assert_eq!(&encoded[..4], &5u32.to_le_bytes());
        assert!(matches!(
            RaftGrpcMessageReply::decode(&encoded).unwrap(),
            RaftGrpcMessageReply::GetForwardingCapabilities { version: 1 }
        ));
    }

    #[test]
    fn reconciliation_roundtrip_preserves_the_exact_expected_owner() {
        let id = Id::new(4, 0, None, None, ClientId::from_static("old-client"), None);
        let encoded = RaftGrpcMessage::ReconcileHandoffSession {
            client_id: id.client_id.clone(),
            expected_owner: Some(id.clone()),
        }
        .encode()
        .unwrap();
        match RaftGrpcMessage::decode(&encoded).unwrap() {
            RaftGrpcMessage::ReconcileHandoffSession { client_id, expected_owner } => {
                assert_eq!(client_id, id.client_id);
                assert_eq!(expected_owner, Some(id));
            }
            _ => panic!("unexpected reconciliation message"),
        }
    }
}
