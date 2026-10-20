//! Distributed MQTT Broker gRPC Communication Layer
//!
//! Implements inter-node communication for clustered MQTT deployments with:
//! - High-performance message forwarding
//! - Cluster-wide operations and queries
//! - Fault-tolerant request handling
//!
//! ## Core Functionality
//! 1. ​**​gRPC Server​**​:
//!    - Manages incoming cluster RPC requests
//!    - Handles message forwarding between nodes
//!    - Processes cluster-wide queries (subscriptions, routes, etc.)
//!    - Automatic reconnection on failure
//!
//! 2. ​**​gRPC Client​**​:
//!    - Connection pooling and management
//!    - Request timeout handling
//!    - Concurrent request limiting
//!    - Message queue monitoring
//!
//! 3. ​**​Message Types​**​:
//!    - 22+ predefined message types for cluster operations
//!    - Serialization/deserialization via bincode
//!    - Support for custom binary data payloads
//!
//! ## Key Features
//! - Priority-based message channels
//! - Request/response tracking
//! - Broadcast operations to all nodes
//! - Optimistic response selection
//! - Configurable timeouts and limits:
//!   - `client_timeout`: Per-request timeout
//!   - `client_concurrency_limit`: Max concurrent requests
//!   - `chunk_size`: Message fragmentation threshold (2MB default)
//!
//! ## Implementation Details
//! - Uses Tokio for async I/O
//! - Atomic counters for request tracking
//! - Zero-copy message processing where possible
//! - Connection reuse for performance
//!
//! Typical Usage:
//! 1. Initialize gRPC server with `listen_and_serve()`
//! 2. Create client connections with `GrpcClient::new()`
//! 3. Send messages via:
//!    - Direct `send_message()` for point-to-point
//!    - `MessageBroadcaster` for cluster-wide ops
//! 4. Handle responses via `MessageReply` enum
//!
//! Note: All message types below 1000 are reserved for internal use.
//!
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::StreamExt;
use rust_box::handy_grpc::client::Mailbox;
use rust_box::handy_grpc::{
    client::{Client, DuplexMailbox},
    server::{server, Message as GrpcMessage},
    Priority,
};
use rust_box::mpsc::priority_channel as channel;
use scopeguard::defer;
use serde::{Deserialize, Serialize};

use crate::context::ServerContext;
use crate::types::{
    Addr, CleanStart, ClearSubscriptions, ClientId, From, HashMap, Id, IsAdmin, MsgID, NodeId,
    OfflineSession, Publish, Retain, Route, SessionStatus, SharedDeliveryContext, SharedGroup, SubRelations,
    SubRelationsMap, SubsSearchParams, SubsSearchResult, SubscriptionClientIds, TopicFilter, TopicName,
};
use crate::utils::Counter;
use crate::Result;

#[derive(Clone)]
pub struct GrpcServer {
    scx: ServerContext,
}

impl GrpcServer {
    pub fn new(scx: ServerContext) -> Self {
        Self { scx }
    }

    pub async fn listen_and_serve(
        self,
        server_laddr: SocketAddr,
        reuseaddr: bool,
        reuseport: bool,
    ) -> Result<()> {
        let runner = async move {
            let (tx, mut rx) = channel::<Priority, GrpcMessage>(300_000);
            let recv_data_fut = async move {
                let exec = self.scx.get_exec("GRPC_SERVER_EXEC");
                while let Some((_, (data, reply_tx))) = rx.next().await {
                    #[cfg(feature = "stats")]
                    self.scx.stats.grpc_server_actives.inc();
                    let s = self.clone();
                    let recv_fut = async move {
                        let reply = s.on_recv_message(data).await;
                        if let Some(reply_tx) = reply_tx {
                            if let Err(e) = reply_tx.send(reply.map(|r| r.unwrap_or_default())) {
                                log::warn!("gRPC send result failure, {e:?}");
                            }
                        }
                        #[cfg(feature = "stats")]
                        s.scx.stats.grpc_server_actives.dec();
                    };
                    let _ = exec.spawn(recv_fut).await;
                }
                log::error!("Recv None");
            };

            let run_receiver_fut = async move {
                loop {
                    if let Err(e) = server(server_laddr, tx.clone())
                        .max_decoding_message_size(1024 * 1024 * 4)
                        .max_encoding_message_size(1024 * 1024 * 4)
                        .reuseaddr(reuseaddr)
                        .reuseport(reuseport)
                        .run()
                        .await
                    {
                        log::warn!("Run gRPC receiver error, {e:?}");
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            };
            futures::future::join(recv_data_fut, run_receiver_fut).await;
        };

        runner.await;

        Ok(())
    }

    async fn on_recv_message(&self, req: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let (typ, msg) = Message::decode(&req)?;
        let reply = self.grpc_message_received(typ, msg).await?;
        Ok(Some(reply.encode()?))
    }

    async fn grpc_message_received(&self, typ: MessageType, msg: Message) -> Result<MessageReply> {
        match (typ, msg) {
            (MESSAGE_TYPE_MESSAGE_GET, Message::MessageGet(client_id, topic_filter, group)) => {
                match self
                    .scx
                    .extends
                    .message_mgr()
                    .await
                    .get(&client_id, &topic_filter, group.as_ref())
                    .await
                {
                    Err(e) => Ok(MessageReply::Error(e.to_string())),
                    Ok(msgs) => Ok(MessageReply::MessageGet(msgs)),
                }
            }
            (_, msg) => self.scx.extends.hook_mgr().grpc_message_received(typ, msg).await,
        }
    }
}

pub type GrpcClients = Arc<HashMap<NodeId, (Addr, GrpcClient)>>;

#[derive(Clone)]
pub struct GrpcClient {
    mailbox: Mailbox,
    duplex_mailbox: DuplexMailbox,
    active_tasks: Arc<Counter>,
}

impl GrpcClient {
    /// Send one bounded, low-priority request without entering the business
    /// transfer queues or starting their reconnect loops. Intended for optional
    /// monitoring requests whose result can be discarded when the peer is busy.
    ///
    /// Dropping a timed-out request does not undo work already accepted by the
    /// remote server; callers must still bound request frequency and concurrency.
    pub async fn request_once(
        server_addr: &str,
        typ: MessageType,
        msg: Message,
        timeout: Duration,
    ) -> crate::Result<MessageReply> {
        let request = msg.encode(typ)?;
        let mut client = Client::new(server_addr.into())
            .connect_timeout(timeout)
            .timeout(timeout)
            .concurrency_limit(1)
            .connect_lazy()?;
        let reply = tokio::time::timeout(timeout, client.send(request)).await??;
        MessageReply::decode(&reply)
    }

    fn connect_lazy(
        server_addr: &str,
        client_timeout: Duration,
        client_concurrency_limit: usize,
    ) -> Result<Client> {
        Client::new(server_addr.into())
            .connect_timeout(client_timeout)
            .concurrency_limit(client_concurrency_limit)
            .chunk_size(1024 * 1024 * 2)
            .connect_lazy()
            .map_err(Into::into)
    }

    async fn from_client(mut client: Client) -> Self {
        let duplex_mailbox = client.duplex_transfer_start(100_000).await;
        let mailbox = client.transfer_start(100_000).await;
        let active_tasks = Arc::new(Counter::new());
        Self { mailbox, duplex_mailbox, active_tasks }
    }

    //server_addr - ip:port, 127.0.0.1:6666
    #[inline]
    pub async fn new(
        server_addr: &str,
        client_timeout: Duration,
        client_concurrency_limit: usize,
    ) -> Result<Self> {
        log::info!(
            "GrpcClient::new server_addr: {server_addr}, client_concurrency_limit: {client_concurrency_limit}"
        );
        let client = Self::connect_lazy(server_addr, client_timeout, client_concurrency_limit)?;
        Ok(Self::from_client(client).await)
    }

    /// Verify that the remote application can process a request before
    /// starting the long-lived transfer streams. This is intended for rare
    /// client-repair paths, not normal message delivery.
    pub async fn new_with_probe(
        server_addr: &str,
        client_timeout: Duration,
        client_concurrency_limit: usize,
        typ: MessageType,
        msg: Message,
        probe_timeout: Duration,
    ) -> Result<(Self, MessageReply)> {
        log::info!(
            "GrpcClient::new_with_probe server_addr: {server_addr}, client_concurrency_limit: {client_concurrency_limit}"
        );
        let mut client = Self::connect_lazy(server_addr, client_timeout, client_concurrency_limit)?;
        let request = msg.encode(typ)?;
        let response = tokio::time::timeout(probe_timeout, client.send(request)).await??;
        let reply = MessageReply::decode(&response)?;
        Ok((Self::from_client(client).await, reply))
    }

    #[inline]
    pub fn active_tasks(&self) -> &Counter {
        self.active_tasks.as_ref()
    }

    #[inline]
    pub fn transfer_queue_len(&self) -> usize {
        self.mailbox.req_queue_len()
    }

    #[inline]
    pub async fn send_message(
        &mut self,
        typ: MessageType,
        msg: Message,
        timeout: Option<Duration>,
    ) -> Result<MessageReply> {
        let active_tasks = self.active_tasks.clone();
        active_tasks.inc();
        defer! {
            active_tasks.dec();
        }
        self._send_message(typ, msg, timeout).await
    }

    #[inline]
    async fn _send_message(
        &mut self,
        typ: MessageType,
        msg: Message,
        timeout: Option<Duration>,
    ) -> Result<MessageReply> {
        let req = msg.encode(typ)?;
        let reply = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, self.duplex_mailbox.send(req)).await??
        } else {
            self.duplex_mailbox.send(req).await?
        };
        MessageReply::decode(&reply)
    }

    #[inline]
    pub async fn notify(&mut self, typ: MessageType, msg: Message, timeout: Option<Duration>) -> Result<()> {
        let active_tasks = self.active_tasks.clone();
        active_tasks.inc();
        defer! {
            active_tasks.dec();
        }
        self._notify(typ, msg, timeout).await
    }
    #[inline]
    async fn _notify(&mut self, typ: MessageType, msg: Message, timeout: Option<Duration>) -> Result<()> {
        let req = msg.encode(typ)?;
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, self.mailbox.send(req)).await??;
        } else {
            self.mailbox.send(req).await?;
        };
        Ok(())
    }
}

///Reserved within 1000
pub type MessageType = u64;

pub const MESSAGE_TYPE_MESSAGE_GET: u64 = 22;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message {
    Forwards(From, Publish),
    ForwardsTo(From, Publish, SubRelations),
    Kick(Id, CleanStart, ClearSubscriptions, IsAdmin),
    GetRetains(TopicFilter),
    SubscriptionsSearch(SubsSearchParams),
    SubscriptionsGet(ClientId),
    RoutesGet(usize),
    RoutesGetBy(TopicFilter),
    NumberOfClients,
    NumberOfSessions,
    Online(ClientId),
    SessionStatus(ClientId),
    MessageGet(ClientId, TopicFilter, Option<SharedGroup>),
    Data(Vec<u8>),
    // Append only: negotiated pull snapshots preserve shared queue context
    // without changing the legacy Publish, Kick or OfflineSession wire layout.
    KickWithShared(Id, CleanStart, ClearSubscriptions, IsAdmin),
}

impl Message {
    #[inline]
    pub fn encode(&self, typ: MessageType) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&(typ, self))?)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> Result<(MessageType, Message)> {
        Ok(bincode::deserialize::<(MessageType, Message)>(data)?)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageReply {
    Success,
    Forwards(SubRelationsMap, SubscriptionClientIds),
    Error(String),
    Kick(OfflineSession),
    GetRetains(Vec<(TopicName, Retain)>),
    SubscriptionsSearch(Vec<SubsSearchResult>),
    SubscriptionsGet(Option<Vec<SubsSearchResult>>),
    RoutesGet(Vec<Route>),
    RoutesGetBy(Vec<Route>),
    NumberOfClients(usize),
    NumberOfSessions(usize),
    Online(bool),
    SessionStatus(Option<SessionStatus>),
    MessageGet(Vec<(MsgID, From, Publish)>),
    Data(Vec<u8>),
    KickWithShared(SharedOfflineSession),
}

/// Versioned pull-session wrapper. Contexts correspond to the offline queue,
/// never to MQTT inflight messages whose delivery has already started.
#[derive(Serialize, Deserialize, Debug)]
pub struct SharedOfflineSession {
    pub session: OfflineSession,
    pub queued_contexts: Vec<Option<SharedDeliveryContext>>,
}

impl SharedOfflineSession {
    pub fn from_session(mut session: OfflineSession) -> Self {
        let queued_contexts = match &mut session {
            OfflineSession::Exist(Some(offline)) => {
                for message in &mut offline.inflight_messages {
                    message.publish.shared_delivery = None;
                }
                offline
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
                    .collect()
            }
            _ => Vec::new(),
        };
        Self { session, queued_contexts }
    }

    pub fn restore(mut self) -> Result<OfflineSession> {
        let OfflineSession::Exist(Some(offline)) = &mut self.session else {
            anyhow::ensure!(self.queued_contexts.is_empty(), "shared pull context requires a session");
            return Ok(self.session);
        };
        anyhow::ensure!(
            self.queued_contexts.len() == offline.offline_messages.len(),
            "shared pull context length does not match the offline queue"
        );
        for ((_, publish), context) in offline.offline_messages.iter().zip(&self.queued_contexts) {
            if let Some(context) = context {
                anyhow::ensure!(
                    !publish.dup && publish.packet_id.is_none(),
                    "shared pull context cannot be attached to a started delivery"
                );
                anyhow::ensure!(
                    context.target_id.client_id == offline.id.client_id
                        && context.target_id.create_time == offline.id.create_time,
                    "shared pull context does not belong to the snapshot session"
                );
                let filter = context.topic_filter.parse::<crate::topic::Topic>()?;
                anyhow::ensure!(
                    filter.matches_str(&publish.topic),
                    "shared pull context does not match the queued publication"
                );
            }
        }
        for ((_, publish), context) in offline.offline_messages.iter_mut().zip(self.queued_contexts) {
            publish.shared_delivery = context.map(Arc::new);
        }
        for message in &mut offline.inflight_messages {
            message.publish.shared_delivery = None;
        }
        Ok(self.session)
    }
}

impl MessageReply {
    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }
    #[inline]
    pub fn decode(data: &[u8]) -> Result<MessageReply> {
        Ok(bincode::deserialize::<MessageReply>(data)?)
    }
}

pub struct MessageSender {
    client: GrpcClient,
    msg_type: MessageType,
    msg: Message,
    timeout: Option<Duration>,
}

impl MessageSender {
    #[inline]
    pub fn new(client: GrpcClient, msg_type: MessageType, msg: Message, timeout: Option<Duration>) -> Self {
        Self { client, msg_type, msg, timeout }
    }

    #[inline]
    pub async fn send(mut self) -> Result<MessageReply> {
        match self.client.send_message(self.msg_type, self.msg, self.timeout).await {
            Ok(reply) => Ok(reply),
            Err(e) => {
                log::warn!("error sending message, {e:?}");
                Err(e)
            }
        }
    }

    #[inline]
    pub async fn notify(mut self) -> Result<()> {
        match self.client.notify(self.msg_type, self.msg, self.timeout).await {
            Ok(()) => Ok(()),
            Err(e) => {
                log::warn!("error notify message, {e:?}");
                Err(e)
            }
        }
    }
}

pub struct MessageBroadcaster {
    grpc_clients: GrpcClients,
    msg_type: MessageType,
    msg: Message,
    timeout: Option<Duration>,
}

impl MessageBroadcaster {
    #[inline]
    pub fn new(
        grpc_clients: GrpcClients,
        msg_type: MessageType,
        msg: Message,
        timeout: Option<Duration>,
    ) -> Self {
        assert!(!grpc_clients.is_empty(), "gRPC clients is empty!");
        Self { grpc_clients, msg_type, msg, timeout }
    }

    #[inline]
    pub async fn join_all(self) -> Vec<(NodeId, Result<MessageReply>)> {
        let msg = self.msg;
        let mut senders = Vec::new();
        for (id, (_, grpc_client)) in self.grpc_clients.iter() {
            let msg_type = self.msg_type;
            let msg = msg.clone();
            let fut =
                async move { (*id, grpc_client.clone().send_message(msg_type, msg, self.timeout).await) };
            senders.push(fut.boxed());
        }
        futures::future::join_all(senders).await
    }

    #[inline]
    pub async fn select_ok<R, F>(self, check_fn: F) -> Result<R>
    where
        R: std::any::Any + Send + Sync,
        F: Fn(MessageReply) -> Result<R> + Send + Sync,
    {
        let msg = self.msg;
        let mut senders = Vec::new();
        let max_idx = self.grpc_clients.len() - 1;
        for (i, (_, (_, grpc_client))) in self.grpc_clients.iter().enumerate() {
            if i == max_idx {
                senders.push(Self::send(grpc_client, self.msg_type, msg, self.timeout, &check_fn).boxed());
                break;
            } else {
                senders.push(
                    Self::send(grpc_client, self.msg_type, msg.clone(), self.timeout, &check_fn).boxed(),
                );
            }
        }
        let (reply, _) = futures::future::select_ok(senders).await?;
        Ok(reply)
    }

    #[inline]
    async fn send<R, F>(
        grpc_client: &GrpcClient,
        typ: MessageType,
        msg: Message,
        timeout: Option<Duration>,
        check_fn: &F,
    ) -> Result<R>
    where
        R: std::any::Any + Send + Sync,
        F: Fn(MessageReply) -> Result<R> + Send + Sync,
    {
        match grpc_client.clone().send_message(typ, msg, timeout).await {
            Ok(r) => {
                log::debug!("OK reply: {r:?}");
                check_fn(r)
            }
            Err(e) => {
                log::debug!("ERROR reply: {e:?}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GrpcClient, Message, MessageReply, SharedOfflineSession};
    use crate::{
        inflight::{MomentStatus, OutInflightMessage},
        session::OfflineInfo,
        types::{CodecPublish, From, Id, OfflineSession, Publish, QoS, SharedDeliveryContext},
    };
    use std::num::NonZeroU16;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    fn shared_pull_session() -> OfflineSession {
        let id = Id::from(2, "shared-worker".into());
        let from = From::from_custom(Id::from(1, "publisher".into()));
        let mut queued = Publish::from(CodecPublish {
            dup: false,
            retain: false,
            qos: QoS::ExactlyOnce,
            topic: "work/request".into(),
            packet_id: None,
            payload: bytes::Bytes::from_static(b"queued"),
            properties: None,
        });
        queued.shared_delivery = Some(Arc::new(SharedDeliveryContext {
            group: "workers".into(),
            topic_filter: "work/#".into(),
            source_qos: QoS::ExactlyOnce,
            source_retain: false,
            target_id: id.clone(),
            subscription_generation: 11,
        }));
        let mut started = queued.clone();
        started.packet_id = NonZeroU16::new(7);
        OfflineSession::Exist(Some(OfflineInfo {
            id,
            subscriptions: Vec::new(),
            offline_messages: std::collections::VecDeque::from([(from.clone(), queued)]),
            inflight_messages: vec![OutInflightMessage::new(MomentStatus::UnReceived, from, started)],
            created_at: 1,
        }))
    }

    #[test]
    fn shared_pull_reply_preserves_queued_context_but_never_marks_inflight_unstarted() {
        let wire = MessageReply::KickWithShared(SharedOfflineSession::from_session(shared_pull_session()))
            .encode()
            .unwrap();
        let MessageReply::KickWithShared(snapshot) = MessageReply::decode(&wire).unwrap() else {
            panic!("expected versioned pull snapshot");
        };
        let OfflineSession::Exist(Some(offline)) = snapshot.restore().unwrap() else {
            panic!("expected offline session");
        };
        let context = offline.offline_messages[0].1.shared_delivery.as_ref().unwrap();
        assert_eq!(context.group, "workers");
        assert_eq!(context.topic_filter, "work/#");
        assert_eq!(context.subscription_generation, 11);
        assert_eq!(context.target_id, offline.id);
        assert!(offline.offline_messages[0].1.shared_delivery_not_started());
        assert!(offline.inflight_messages[0].publish.shared_delivery.is_none());
        assert_eq!(offline.inflight_messages[0].publish.packet_id, NonZeroU16::new(7));
    }

    #[test]
    fn shared_pull_reply_rejects_misaligned_or_wrong_session_context() {
        let mut snapshot = SharedOfflineSession::from_session(shared_pull_session());
        snapshot.queued_contexts.clear();
        assert!(snapshot.restore().is_err());

        let mut snapshot = SharedOfflineSession::from_session(shared_pull_session());
        snapshot.queued_contexts[0].as_mut().unwrap().target_id = Id::from(2, "another-worker".into());
        assert!(snapshot.restore().is_err());

        let mut snapshot = SharedOfflineSession::from_session(shared_pull_session());
        snapshot.queued_contexts[0].as_mut().unwrap().topic_filter = "unrelated/#".into();
        assert!(snapshot.restore().is_err());

        let mut snapshot = SharedOfflineSession::from_session(shared_pull_session());
        if let OfflineSession::Exist(Some(offline)) = &mut snapshot.session {
            offline.offline_messages[0].1.packet_id = NonZeroU16::new(3);
        }
        assert!(snapshot.restore().is_err());
    }

    #[test]
    fn shared_pull_variants_leave_legacy_grpc_discriminants_unchanged() {
        let id = Id::from(2, "worker".into());
        let legacy = Message::Kick(id.clone(), false, true, false).encode(99).unwrap();
        assert_eq!(&legacy[8..12], &2u32.to_le_bytes());
        let data = Message::Data(Vec::new()).encode(99).unwrap();
        assert_eq!(&data[8..12], &13u32.to_le_bytes());
        let pull = Message::KickWithShared(id, false, true, false).encode(99).unwrap();
        assert_eq!(&pull[8..12], &14u32.to_le_bytes());
        let legacy_reply = MessageReply::Kick(OfflineSession::NotExist).encode().unwrap();
        assert_eq!(&legacy_reply[..4], &3u32.to_le_bytes());
        let data_reply = MessageReply::Data(Vec::new()).encode().unwrap();
        assert_eq!(&data_reply[..4], &14u32.to_le_bytes());
        let pull_reply =
            MessageReply::KickWithShared(SharedOfflineSession::from_session(OfflineSession::NotExist))
                .encode()
                .unwrap();
        assert_eq!(&pull_reply[..4], &15u32.to_le_bytes());
    }

    #[tokio::test]
    async fn request_once_deadline_includes_an_unresponsive_connection() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        });

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            GrpcClient::request_once(&addr, 99, Message::Data(Vec::new()), Duration::from_millis(100)),
        )
        .await;
        peer.abort();
        assert!(accepted_rx.await.is_ok(), "the request should reach the unresponsive peer");
        assert!(result.expect("a monitoring request must respect its total deadline").is_err());
    }
}
