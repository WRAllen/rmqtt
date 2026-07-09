use serde::{Deserialize, Serialize};

use rmqtt::session::OfflineInfo;
use rmqtt::types::{
    Addr, ClientId, ConnectInfo, From, Id, NodeHealthStatus, NodeId, Publish, SubRelations,
    SubscriptionOptions,
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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionHandoff {
    pub offline: OfflineInfo,
    pub connect_info: ConnectInfo,
    pub superuser: bool,
    pub session_expiry_millis: u64,
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
