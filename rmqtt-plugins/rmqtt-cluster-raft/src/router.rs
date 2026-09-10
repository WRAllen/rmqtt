use std::borrow::Cow;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use backoff::{future::retry, ExponentialBackoff};
use rayon::prelude::*;
use rust_box::task_exec_queue::{SpawnExt, TaskExecQueue};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use rmqtt::{
    context::ServerContext,
    router::{DefaultRouter, Router},
    types::{
        AllRelationsMap, ClientId, Id, IsOnline, NodeId, Route, SubRelationsMap, SubsSearchParams,
        SubsSearchResult, SubscriptionOptions, TimestampMillis, Topic, TopicFilter, TopicName,
    },
    utils::{timestamp_millis, Counter},
    Result,
};
use rmqtt_raft::{Error, Mailbox, Result as RaftResult, Store};
use tokio::task::{block_in_place, spawn_blocking};

use super::config::Compression;
use super::message::{ClusterNode, Message, MessageReply};
use super::nodes::ClusterNodeManager;

type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;
type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

fn is_orphaned_node_route(
    node_id: NodeId,
    route_id: &Id,
    current_owner: Option<&Id>,
    live_session_ids: &HashSet<Id>,
) -> bool {
    route_id.node_id == node_id
        && !live_session_ids.contains(route_id)
        && !current_owner.is_some_and(|owner| owner == route_id)
}

fn remove_terminated_owner(states: &DashMap<ClientId, ClientStatus>, id: &Id) -> bool {
    states.remove_if(&id.client_id, |_, status| status.id == *id).is_some()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ClientStatus {
    pub id: Id,
    pub online: IsOnline,
    pub handshaking: bool,
    pub handshak_duration: TimestampMillis,
}

impl ClientStatus {
    fn new(id: Id, online: IsOnline, handshaking: bool) -> Self {
        Self { id, online, handshaking, handshak_duration: timestamp_millis() }
    }

    pub fn handshaking(&self, try_lock_timeout: Duration) -> bool {
        self.handshaking
            && (timestamp_millis()
                < (self.handshak_duration + try_lock_timeout.as_millis() as TimestampMillis))
    }
}

#[derive(Clone)]
pub(crate) struct ClusterRouter {
    inner: DefaultRouter,
    add_exec: TaskExecQueue,
    remove_exec: TaskExecQueue,
    backoff_strategy: ExponentialBackoff,
    nodes: ClusterNodeManager,
    raft_mailbox: Arc<RwLock<Option<Mailbox>>>,
    client_states: Arc<DashMap<ClientId, ClientStatus>>,
    pub try_lock_timeout: Duration,
    compression: Option<Compression>,
}

impl ClusterRouter {
    #[inline]
    pub(crate) fn new(
        scx: ServerContext,
        backoff_strategy: ExponentialBackoff,
        nodes: ClusterNodeManager,
        try_lock_timeout: Duration,
        compression: Option<Compression>,
    ) -> Self {
        let add_exec = scx.get_exec("RAFT_ROUTER_ADD_EXEC");
        let remove_exec = scx.get_exec("RAFT_ROUTER_REMOVE_EXEC");
        Self {
            inner: DefaultRouter::new(Some(scx)),
            add_exec,
            remove_exec,
            backoff_strategy,
            nodes,
            raft_mailbox: Arc::new(RwLock::new(None)),
            client_states: Arc::new(DashMap::default()),
            try_lock_timeout,
            compression,
        }
    }

    #[inline]
    pub(crate) fn _inner(&self) -> &DefaultRouter {
        &self.inner
    }

    #[inline]
    pub(crate) async fn set_raft_mailbox(&self, raft_mailbox: Mailbox) {
        self.raft_mailbox.write().await.replace(raft_mailbox);
    }

    #[inline]
    pub(crate) async fn raft_mailbox(&self) -> Mailbox {
        if let Some(mailbox) = self.raft_mailbox.read().await.as_ref() {
            mailbox.clone()
        } else {
            unreachable!()
        }
    }

    #[inline]
    pub(crate) fn _client_node_id(&self, client_id: &str) -> Option<NodeId> {
        self.client_states.get(client_id).map(|entry| entry.id.node_id)
    }

    #[inline]
    pub(crate) fn id(&self, client_id: &str) -> Option<Id> {
        self.client_states.get(client_id).map(|entry| entry.id.clone())
    }

    #[inline]
    pub(crate) fn states_count(&self) -> usize {
        self.client_states.len()
    }

    #[inline]
    pub(crate) fn node_states_count(&self, node_id: NodeId) -> usize {
        self.client_states.iter().filter(|entry| entry.value().id.node_id == node_id).count()
    }

    #[inline]
    pub(crate) fn node_state_ids(&self, node_id: NodeId) -> Vec<Id> {
        self.client_states
            .iter()
            .filter_map(|entry| {
                let status = entry.value();
                (status.id.node_id == node_id).then(|| status.id.clone())
            })
            .collect()
    }

    #[inline]
    pub(crate) fn node_routes_count(&self, node_id: NodeId) -> usize {
        self.inner
            .relations
            .iter()
            .map(|entry| entry.value().values().filter(|(id, _)| id.node_id == node_id).count())
            .sum()
    }

    pub(crate) fn node_routes(&self, node_id: NodeId) -> Vec<(String, Id)> {
        self.inner
            .relations
            .iter()
            .flat_map(|entry| {
                let topic_filter = entry.key().to_string();
                entry
                    .value()
                    .values()
                    .filter_map(|(route_id, _)| {
                        (route_id.node_id == node_id).then(|| (topic_filter.clone(), route_id.clone()))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(crate) fn orphaned_node_routes(
        &self,
        node_id: NodeId,
        live_session_ids: &HashSet<Id>,
    ) -> Vec<(String, Id)> {
        self.node_routes(node_id)
            .into_iter()
            .filter(|(_, route_id)| {
                let current_owner = self.id(&route_id.client_id);
                is_orphaned_node_route(node_id, route_id, current_owner.as_ref(), live_session_ids)
            })
            .collect()
    }

    #[inline]
    pub(crate) fn status(&self, client_id: &str) -> Option<ClientStatus> {
        self.client_states.get(client_id).map(|entry| entry.value().clone())
    }

    #[inline]
    pub(crate) fn _handshakings(&self) -> usize {
        self.client_states.iter().filter_map(|entry| if entry.handshaking { Some(()) } else { None }).count()
    }

    #[inline]
    pub(crate) fn get_target_node_id(&self, target_clientid: &str) -> Option<NodeId> {
        self.client_states.get(target_clientid).map(|entry| entry.id.node_id)
    }

    #[inline]
    pub(crate) fn get_ready_target_node_id(&self, target_clientid: &str) -> Option<NodeId> {
        self.client_states.get(target_clientid).and_then(|entry| {
            let status = entry.value();
            if status.online && !status.handshaking {
                Some(status.id.node_id)
            } else {
                None
            }
        })
    }

    async fn remove_node_state(&self, node_id: NodeId) {
        let client_ids =
            self.client_states
                .iter()
                .filter_map(|entry| {
                    if entry.value().id.node_id == node_id {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
        for client_id in client_ids {
            self.client_states.remove(&client_id);
        }

        let removals = self
            .inner
            .relations
            .iter()
            .flat_map(|entry| {
                let topic_filter = entry.key().clone();
                entry
                    .value()
                    .iter()
                    .filter_map(move |(_, (id, _))| {
                        if id.node_id == node_id {
                            Some((topic_filter.clone(), id.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for (topic_filter, id) in removals {
            if let Err(e) = self.inner.remove(&topic_filter, id).await {
                log::warn!("remove node route failed, node_id: {node_id}, topic_filter: {topic_filter}, {e}");
            }
        }
    }

    async fn remove_client_routes(&self, id: &Id) {
        let removals = self
            .inner
            .relations
            .iter()
            .flat_map(|entry| {
                let topic_filter = entry.key().clone();
                entry
                    .value()
                    .iter()
                    .filter(move |(_, (route_id, _))| route_id == id)
                    .map(move |(_, (route_id, _))| (topic_filter.clone(), route_id.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for (topic_filter, route_id) in removals {
            if let Err(e) = self.inner.remove(&topic_filter, route_id).await {
                log::warn!(
                    "remove terminated client route failed, id: {id:?}, topic_filter: {topic_filter}, {e}"
                );
            }
        }
    }
}

#[async_trait]
impl Router for ClusterRouter {
    fn metrics_snapshot(&self) -> Option<rmqtt::router::RouterMetricsSnapshot> {
        self.inner.metrics_snapshot()
    }
    #[inline]
    async fn add(&self, topic_filter: &str, id: Id, opts: SubscriptionOptions) -> Result<()> {
        log::debug!("[Router.add] topic_filter: {topic_filter:?}, id: {id:?}, opts: {opts:?}");

        let mut add_res = None;
        for _ in 0..3 {
            let mailbox = self.raft_mailbox().await;
            let msg = Message::Add { topic_filter, id: id.clone(), opts: opts.clone() }.encode()?;
            let res = async move { mailbox.send_proposal(msg).await.map_err(anyhow::Error::new) }
                .spawn(&self.add_exec)
                .result()
                .await
                .map_err(|_| anyhow!("Router::add(..), task execution failure"));
            add_res = Some(res);
            match add_res.as_ref() {
                Some(Ok(Ok(_))) => return Ok(()),
                _ => {
                    tokio::time::sleep(Duration::from_millis(800)).await;
                    continue;
                }
            }
        }
        let _ = add_res.ok_or_else(|| anyhow!("unreachable!()"))???;
        Ok(())
    }

    #[inline]
    async fn remove(&self, topic_filter: &str, id: Id) -> Result<bool> {
        log::debug!("[Router.remove] topic_filter: {topic_filter:?}, id: {id:?}");
        let msg = Message::Remove { topic_filter, id: id.clone() }.encode()?;
        let raft_mailbox = self.raft_mailbox().await;
        let exec = self.remove_exec.clone();
        let backoff_strategy = self.backoff_strategy.clone();
        tokio::spawn(async move {
            if let Err(e) = retry(backoff_strategy, || async {
                let msg = msg.clone();
                let mailbox = raft_mailbox.clone();
                let res = async move { mailbox.send_proposal(msg).await }
                    .spawn(&exec)
                    .result()
                    .await
                    .map_err(|_| anyhow!("Router::remove(..), task execution failure"))?
                    .map_err(|e| anyhow!(e))?;
                Ok(res)
            })
            .await
            {
                log::warn!("[Router.remove] Failed to send Message::Remove, id: {id:?}, {e:?}");
            }
        });
        Ok(true)
    }

    #[inline]
    async fn matches(&self, id: Id, topic: &TopicName) -> Result<SubRelationsMap> {
        self.inner.matches(id, topic).await
    }

    ///Check online or offline
    #[inline]
    async fn is_online(&self, node_id: NodeId, client_id: &str) -> bool {
        log::debug!("[Router.is_online] node_id: {node_id:?}, client_id: {client_id:?}");
        self.client_states.get(client_id).map(|entry| entry.online).unwrap_or(false)
    }

    #[inline]
    async fn gets(&self, limit: usize) -> Vec<Route> {
        self.inner.gets(limit).await
    }

    #[inline]
    async fn get(&self, topic: &str) -> Result<Vec<Route>> {
        self.inner.get(topic).await
    }

    #[inline]
    async fn query_subscriptions(&self, q: &SubsSearchParams) -> Vec<SubsSearchResult> {
        self.inner.query_subscriptions(q).await
    }

    #[inline]
    async fn topics_tree(&self) -> usize {
        self.inner.topics_tree().await
    }

    #[inline]
    fn topics(&self) -> Counter {
        self.inner.topics()
    }

    #[inline]
    fn routes(&self) -> Counter {
        self.inner.routes()
    }

    #[inline]
    fn merge_topics(&self, topics_map: &HashMap<NodeId, Counter>) -> Counter {
        self.inner.merge_topics(topics_map)
    }

    #[inline]
    fn merge_routes(&self, routes_map: &HashMap<NodeId, Counter>) -> Counter {
        self.inner.merge_routes(routes_map)
    }

    #[inline]
    async fn list_topics(&self, top: usize) -> Vec<String> {
        self.inner.list_topics(top).await
    }

    #[inline]
    async fn list_relations(&self, top: usize) -> Vec<serde_json::Value> {
        self.inner.list_relations(top).await
    }

    #[inline]
    fn relations(&self) -> &AllRelationsMap {
        &self.inner.relations
    }
}

#[async_trait]
impl Store for ClusterRouter {
    async fn apply(&mut self, message: &[u8]) -> RaftResult<Vec<u8>> {
        log::debug!("apply, message.len: {:?}", message.len());
        let message: Message = bincode::deserialize(message).map_err(|e| Error::Other(e))?;
        match message {
            Message::HandshakeTryLock { id } => {
                log::debug!("[Router.HandshakeTryLock] id: {id:?}");
                let mut try_lock_ok = false;
                let mut prev_id = None;
                self.client_states
                    .entry(id.client_id.clone())
                    .and_modify(|status| {
                        prev_id = Some(status.id.clone());
                        if !status.handshaking(self.try_lock_timeout) {
                            *status = ClientStatus::new(id.clone(), false, true);
                            try_lock_ok = true;
                        }
                    })
                    .or_insert_with(|| {
                        try_lock_ok = true;
                        ClientStatus::new(id.clone(), false, true)
                    });
                log::debug!(
                    "[Router.HandshakeTryLock] id: {id:?}, try_lock_ok: {try_lock_ok}, prev_id: {prev_id:?}"
                );
                return if try_lock_ok {
                    Ok(MessageReply::HandshakeTryLock(prev_id).encode().map_err(|_e| Error::Unknown)?)
                } else {
                    Ok(MessageReply::Error("Handshake try lock failed".into())
                        .encode()
                        .map_err(|_e| Error::Unknown)?)
                };
            }
            Message::Connected { id } => {
                log::debug!("[Router.Connected] id: {id:?}");
                let mut reply = None;
                self.client_states.entry(id.client_id.clone()).and_modify(|status| {
                    if status.id != id && id.create_time < status.id.create_time {
                        log::info!("[Router.Connected] id not the same, input id: {id:?}, current status: {status:?}");
                        reply = Some(MessageReply::Error("id not the same".into()));
                    } else {
                        if id.create_time > status.id.create_time {
                            log::info!("[Router.Connected] id.create_time > status.id.create_time, input id: {id:?}, current status: {status:?}");
                        }
                        status.id = id.clone();
                        status.online = true;
                        status.handshaking = false;
                    }
                }).or_insert_with(|| {
                    log::debug!("[Router.Connected] id: {id:?}, Not found");
                    ClientStatus::new(id, true, false)
                });
                if let Some(reply) = reply {
                    return reply.encode().map_err(|_e| Error::Unknown);
                }
            }
            Message::Disconnected { id } => {
                log::debug!("[Router.Disconnected] id: {id:?}",);
                if let Some(mut entry) = self.client_states.get_mut(&id.client_id) {
                    let status = entry.value_mut();
                    if status.id != id {
                        log::debug!(
                            "[Router.Disconnected] id not the same, input id: {id:?}, current status: {status:?}"
                        );
                    } else {
                        status.online = false;
                    }
                } else {
                    log::info!("[Router.Disconnected] id: {id:?}, Not found");
                }
            }
            Message::SessionTerminated { id } => {
                log::debug!("[Router.SessionTerminated] id: {id:?}",);
                let removed = remove_terminated_owner(&self.client_states, &id);
                // Route removals normally arrive before SessionTerminated, but
                // a transient proposal failure must not leave a route pointing
                // at a session which no longer exists. The exact connection Id
                // check keeps a newer connection with the same client id intact.
                self.remove_client_routes(&id).await;
                return MessageReply::SessionTerminated { removed }.encode().map_err(|_e| Error::Unknown);
            }
            Message::Add { topic_filter, id, opts } => {
                log::debug!("[Router.add] topic_filter: {topic_filter:?}, id: {id:?}, opts: {opts:?}");
                // A subscription proposal can arrive after a newer client
                // connection has already taken ownership. Never let that
                // stale proposal replace the newer route for the same client
                // id; this is especially important while an offline session
                // snapshot is being relocated during drain.
                let current_owner = self.client_states.get(&id.client_id).map(|status| status.id.clone());
                if current_owner.as_ref() == Some(&id) {
                    self.inner.add(topic_filter, id, opts).await?;
                } else {
                    log::debug!(
                        "[Router.add] ignored stale owner, input id: {id:?}, current owner: {current_owner:?}, topic_filter: {topic_filter:?}"
                    );
                }
            }
            Message::Remove { topic_filter, id } => {
                log::debug!("[Router.remove] topic_filter: {topic_filter:?}, id: {id:?}",);
                self.inner.remove(topic_filter, id).await?;
            }
            Message::NodeUp { node } => {
                log::info!("[Router.NodeUp] node: {node:?}");
                self.nodes.upsert(node).await.map_err(|e| Error::Anyhow(e.into()))?;
            }
            Message::NodeDown { id } => {
                log::info!("[Router.NodeDown] id: {id}");
                self.nodes.remove(id);
            }
            Message::NodeRemove { id } => {
                log::info!("[Router.NodeRemove] id: {id}");
                self.nodes.remove(id);
                self.remove_node_state(id).await;
            }
            Message::GetClientNodeId { client_id } => {
                let node_id = self._client_node_id(client_id);
                let data = bincode::serialize(&node_id).map_err(|e| Error::Other(e))?;
                return Ok(data);
            }
            Message::Ping => return MessageReply::Ping.encode().map_err(|_e| Error::Unknown),
        }

        Ok(Vec::new())
    }

    async fn query(&self, query: &[u8]) -> RaftResult<Vec<u8>> {
        log::debug!("query, message.len: {:?}", query.len());
        let query: Message = bincode::deserialize(query).map_err(|e| Error::Other(e))?;
        match query {
            Message::GetClientNodeId { client_id } => {
                let node_id = self._client_node_id(client_id);
                let data = bincode::serialize(&node_id).map_err(|e| Error::Other(e))?;
                return Ok(data);
            }
            _ => {
                log::error!("unimplemented, query: {query:?}")
            }
        }
        Ok(Vec::new())
    }

    async fn snapshot(&self) -> RaftResult<Vec<u8>> {
        log::debug!("create snapshot ...");
        let now = std::time::Instant::now();

        let compress = |compression: Option<Compression>, data: Vec<u8>| -> RaftResult<Vec<u8>> {
            let compressed = match compression {
                Some(Compression::Zstd) => zstd::encode_all(&*data, 1)?,
                Some(Compression::Lz4) => {
                    use lz4_flex::block::compress_prepend_size;
                    compress_prepend_size(&data)
                }
                Some(Compression::Zlib) => {
                    use flate2::write::ZlibEncoder;
                    let mut e = ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
                    e.write_all(&data)?;
                    e.finish()?
                }
                Some(Compression::Snappy) => {
                    use snap::write;
                    let mut wtr = write::FrameEncoder::new(vec![]);
                    wtr.write_all(&data)?;
                    wtr.into_inner().map_err(|e| Error::Anyhow(e.into()))?
                }
                None => data,
            };
            Ok(compressed)
        };

        let encode_with_length_prefix = |data: &[u8], out: &mut Vec<u8>| {
            let len = data.len();
            out.extend(&len.to_le_bytes());
            out.extend(data);
        };

        let compression = self.compression;
        let relations = self.inner.relations.clone();
        let client_states = self.client_states.clone();
        let topics_count = self.inner.topics_count.clone();
        let relations_count = self.inner.relations_count.clone();
        let cluster_nodes = self.nodes.snapshot_nodes();
        let snapshot: Vec<u8> = spawn_blocking(move || {
            let relations = relations
                .iter()
                .par_bridge()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect::<Vec<_>>();

            let client_states = client_states
                .iter()
                .par_bridge()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect::<Vec<_>>();

            let (relations_bin, client_states_bin) = rayon::join(
                || bincode::serialize(&relations).map(|data| compress(compression, data)),
                || bincode::serialize(&client_states).map(|data| compress(compression, data)),
            );
            let relations_bin = relations_bin??;
            let client_states_bin = client_states_bin??;
            let topics_count_bin = bincode::serialize(&topics_count.as_ref())?;
            let relations_count_bin = bincode::serialize(&relations_count.as_ref())?;
            let cluster_nodes_bin = bincode::serialize(&cluster_nodes)?;

            let mut snapshot = Vec::new();
            encode_with_length_prefix(&relations_bin, &mut snapshot);
            encode_with_length_prefix(&client_states_bin, &mut snapshot);
            encode_with_length_prefix(&topics_count_bin, &mut snapshot);
            encode_with_length_prefix(&relations_count_bin, &mut snapshot);
            encode_with_length_prefix(&cluster_nodes_bin, &mut snapshot);

            Ok(snapshot)
        })
        .await
        .map_err(|e| Error::Anyhow(e.into()))?
        .map_err(|e: Error| Error::Anyhow(e.into()))?;

        log::info!("create snapshot, len: {}, cost time: {:?}", snapshot.len(), now.elapsed());

        Ok(snapshot)
    }

    async fn restore(&mut self, snapshot: &[u8]) -> RaftResult<()> {
        log::info!("restore, snapshot.len: {}", snapshot.len());
        let now = std::time::Instant::now();
        let compression = self.compression;
        fn uncompress(compression: Option<Compression>, data: &[u8]) -> Result<Cow<[u8]>> {
            let uncompr = match compression {
                Some(Compression::Zstd) => Cow::Owned(zstd::decode_all(data)?),
                Some(Compression::Lz4) => {
                    use lz4_flex::block::decompress_size_prepended;
                    let buf = decompress_size_prepended(data).map_err(|e| Error::Other(Box::new(e)))?;
                    Cow::Owned(buf)
                }
                Some(Compression::Zlib) => {
                    use flate2::bufread::ZlibDecoder;
                    let mut z = ZlibDecoder::new(data);
                    let mut buf = Vec::new();
                    z.read_to_end(&mut buf)?;
                    Cow::Owned(buf)
                }
                Some(Compression::Snappy) => {
                    let mut buf = Vec::new();
                    snap::read::FrameDecoder::new(data).read_to_end(&mut buf)?;
                    Cow::Owned(buf)
                }
                None => Cow::Borrowed(data),
            };
            Ok(uncompr)
        }

        fn decode_with_length_prefix<T: serde::de::DeserializeOwned>(
            snapshot: &[u8],
            start: usize,
            len: usize,
            uncompress_enable: bool,
            compression: Option<Compression>,
        ) -> Result<T> {
            let slice = &snapshot[start..start + len];
            if uncompress_enable {
                let slice = uncompress(compression, slice)?;
                Ok(bincode::deserialize(slice.as_ref())?)
            } else {
                Ok(bincode::deserialize(slice)?)
            }
        }

        let read_len = |snapshot: &[u8], start: usize| -> Result<usize> {
            let slice = &snapshot[start..start + size_of::<usize>()];
            Ok(usize::from_le_bytes(slice.try_into()?))
        };

        let res: Result<_> = block_in_place(move || {
            let relations_len = read_len(snapshot, 0)?;
            let relations_start = size_of::<usize>();

            let client_states_len = read_len(snapshot, relations_start + relations_len)?;
            let client_states_start = size_of::<usize>() + relations_start + relations_len;

            //topics_count, relations_count
            let topics_count_len = read_len(snapshot, client_states_start + client_states_len)?;
            let topics_count_start = size_of::<usize>() + client_states_start + client_states_len;

            let relations_count_len = read_len(snapshot, topics_count_start + topics_count_len)?;
            let relations_count_start = size_of::<usize>() + topics_count_start + topics_count_len;
            let cluster_nodes_start_pos = relations_count_start + relations_count_len;

            let (relations, client_states) = rayon::join(
                || decode_with_length_prefix(snapshot, relations_start, relations_len, true, compression),
                || {
                    decode_with_length_prefix(
                        snapshot,
                        client_states_start,
                        client_states_len,
                        true,
                        compression,
                    )
                },
            );
            let relations: Vec<(TopicFilter, HashMap<ClientId, (Id, SubscriptionOptions)>)> = relations?;
            let client_states: Vec<(ClientId, ClientStatus)> = client_states?;
            let topics_count: Counter =
                decode_with_length_prefix(snapshot, topics_count_start, topics_count_len, false, None)?;
            let relations_count: Counter =
                decode_with_length_prefix(snapshot, relations_count_start, relations_count_len, false, None)?;
            let cluster_nodes: Vec<ClusterNode> = if cluster_nodes_start_pos < snapshot.len() {
                let cluster_nodes_len = read_len(snapshot, cluster_nodes_start_pos)?;
                let cluster_nodes_start = size_of::<usize>() + cluster_nodes_start_pos;
                decode_with_length_prefix(snapshot, cluster_nodes_start, cluster_nodes_len, false, None)?
            } else {
                Vec::new()
            };

            log::info!("relations: {:?}", relations.len());
            log::info!("client_states: {:?}", client_states.len());
            log::info!("topics_count: {topics_count:?}",);
            log::info!("relations_count: {relations_count:?}");
            log::info!("cluster_nodes: {:?}", cluster_nodes.len());

            Ok((relations, client_states, topics_count, relations_count, cluster_nodes))
        });

        let (relations, client_states, topics_count, relations_count, cluster_nodes) = res?;

        self.inner.topics_count.set(&topics_count);
        let mut topics = self.inner.topics.write().await;
        self.inner.relations.clear();
        for (topic_filter, relation) in relations {
            let topic = Topic::from_str(&topic_filter).map_err(|e| Error::Msg(format!("{e:?}")))?;
            self.inner.relations.insert(topic_filter, relation);
            topics.insert(&topic, ());
        }

        self.inner.relations_count.set(&relations_count);
        self.client_states.clear();
        for (client_id, content) in client_states {
            self.client_states.insert(client_id, content);
        }
        if !cluster_nodes.is_empty() {
            self.nodes.restore_nodes(cluster_nodes).await.map_err(|e| Error::Anyhow(e.into()))?;
        }

        log::info!(
            "restore, topics_count: {topics_count:?}, relations_count: {relations_count:?}, cost time: {:?}",
            now.elapsed()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(node_id: NodeId, client_id: &'static str) -> Id {
        Id::new(node_id, 0, None, None, ClientId::from_static(client_id), None)
    }

    #[test]
    fn orphaned_route_requires_missing_exact_owner_and_session() {
        let route_id = id(1, "client-a");
        let mut live_session_ids = HashSet::new();

        assert!(!is_orphaned_node_route(2, &route_id, None, &live_session_ids));
        assert!(!is_orphaned_node_route(1, &route_id, Some(&route_id), &live_session_ids));

        live_session_ids.insert(route_id.clone());
        assert!(!is_orphaned_node_route(1, &route_id, None, &live_session_ids));

        live_session_ids.clear();
        assert!(is_orphaned_node_route(1, &route_id, None, &live_session_ids));
    }

    #[test]
    fn stale_route_is_orphaned_after_new_connection_takes_ownership() {
        let old_route_id = id(1, "client-a");
        let new_owner_id = old_route_id.relocated_to(2);
        let live_session_ids = HashSet::new();

        assert!(is_orphaned_node_route(1, &old_route_id, Some(&new_owner_id), &live_session_ids,));
    }

    #[test]
    fn old_route_is_preserved_while_its_session_is_still_live() {
        let old_route_id = id(1, "client-a");
        let new_owner_id = old_route_id.relocated_to(2);
        let live_session_ids = HashSet::from([old_route_id.clone()]);

        assert!(!is_orphaned_node_route(1, &old_route_id, Some(&new_owner_id), &live_session_ids,));
    }

    #[test]
    fn termination_ack_distinguishes_removed_missing_and_reconnected_owner() {
        let states = DashMap::default();
        let old = id(1, "client-a");
        states.insert(old.client_id.clone(), ClientStatus::new(old.clone(), false, false));
        assert!(remove_terminated_owner(&states, &old));
        assert!(!remove_terminated_owner(&states, &old));
        let new = Id::new(1, 0, None, Some("127.0.0.1:30001".parse().unwrap()), old.client_id.clone(), None);
        states.insert(new.client_id.clone(), ClientStatus::new(new.clone(), true, false));
        assert!(!remove_terminated_owner(&states, &old));
        assert_eq!(states.get(&old.client_id).unwrap().id, new);
    }
}
