use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::message::{Message, RaftResponse};
use crate::raft::Store;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::storage::{HeedStorage, LogStore};

use bincode::{deserialize, serialize};
use log::*;
use prost::Message as PMessage;
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message as RaftMessage};
use raft::{prelude::*, raw_node::RawNode, Config};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::channel::Channel;
use tonic::Request;

struct MessageSender {
    message: RaftMessage,
    client: RaftServiceClient<tonic::transport::channel::Channel>,
    client_id: u64,
    chan: mpsc::Sender<Message>,
    max_retries: usize,
    timeout: Duration,
}

impl MessageSender {
    /// attempt to send a message MessageSender::max_retries times at MessageSender::timeout
    /// inteval.
    async fn send(mut self) {
        let mut current_retry = 0usize;
        loop {
            let message_request = Request::new(self.message.clone());
            match self.client.send_message(message_request).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::delay_for(self.timeout).await;
                    } else {
                        debug!(
                            "error sending message after {} retries: {}",
                            self.max_retries, e
                        );
                        let _ = self
                            .chan
                            .send(Message::ReportUnreachable {
                                node_id: self.client_id,
                            })
                            .await;
                        return;
                    }
                }
            }
        }
    }
}

pub struct Peer {
    addr: String,
    client: RaftServiceClient<Channel>,
}

impl Deref for Peer {
    type Target = RaftServiceClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for Peer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Peer {
    pub async fn new(addr: &str) -> Result<Peer> {
        // TODO: clean up this mess
        info!("connecting to node at {}...", addr);
        let client = RaftServiceClient::connect(format!("http://{}", addr)).await?;
        let addr = addr.to_string();
        info!("connected to node.");
        Ok(Peer { addr, client })
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<HeedStorage>,
    // the peer is optional, because an id can be reserved and later populated
    pub peers: HashMap<u64, Option<Peer>>,
    pub rcv: mpsc::Receiver<Message>,
    pub snd: mpsc::Sender<Message>,
    store: S,
    should_quit: bool,
    seq: AtomicU64,
    last_snap_time: Instant,
}

impl<S: Store + 'static> RaftNode<S> {
    pub fn new_leader(
        node_id: u64,
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        store: S,
        logger: &slog::Logger,
    ) -> Self {
        let config = Config {
            id: node_id,
            election_tick: 3,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 2,
            // Just for log
            ..Default::default()
        };

        config.validate().unwrap();

        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![node_id];

        let mut storage = HeedStorage::create(".", node_id).unwrap();
        storage.apply_snapshot(s).unwrap();
        let mut inner = RawNode::new(&config, storage, logger).unwrap();
        let peers = HashMap::new();
        let seq = AtomicU64::new(20);
        let last_snap_time = Instant::now();

        inner.raft.become_candidate();
        inner.raft.become_leader();

        RaftNode {
            inner,
            rcv,
            peers,
            store,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
        }
    }

    pub fn new_follower(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        id: u64,
        store: S,
        logger: &slog::Logger,
    ) -> Result<Self> {
        let config = Config {
            id,
            election_tick: 3,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 2,
            // Just for log
            ..Default::default()
        };

        config.validate().unwrap();

        let storage = HeedStorage::create(".", id)?;
        let inner = RawNode::new(&config, storage, logger)?;
        let peers = HashMap::new();
        let seq = AtomicU64::new(20);
        let last_snap_time = Instant::now()
            .checked_sub(Duration::from_secs(1000))
            .unwrap();

        Ok(RaftNode {
            inner,
            rcv,
            peers,
            store,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
        })
    }

    pub fn peer_mut(&mut self, id: u64) -> Option<&mut Peer> {
        match self.peers.get_mut(&id) {
            None => None,
            Some(v) => v.as_mut(),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    pub fn id(&self) -> u64 {
        self.raft.id
    }

    pub async fn add_peer(&mut self, addr: &str, id: u64) -> Result<()> {
        let peer = Peer::new(addr).await?;
        self.peers.insert(id, Some(peer));
        Ok(())
    }

    fn leader(&self) -> u64 {
        self.raft.leader_id
    }

    fn peer_addrs(&self) -> HashMap<u64, String> {
        self.peers
            .iter()
            .filter_map(|(&id, peer)| {
                peer.as_ref()
                    .map(|Peer { addr, .. }| (id, addr.to_string()))
            })
            .collect()
    }

    // reserve a slot to insert node on next node addition commit
    fn reserve_next_peer_id(&mut self) -> u64 {
        let next_id = self.peers.keys().max().cloned().unwrap_or(1);
        // if assigned id is ourself, return next one
        let next_id = std::cmp::max(next_id + 1, self.id());
        self.peers.insert(next_id, None);
        info!("reserving id {}", next_id);
        next_id
    }

    fn send_wrong_leader(&self, channel: oneshot::Sender<RaftResponse>) {
        let leader_id = self.leader();
        // leader can't be an empty node
        let leader_addr = self.peers[&leader_id].as_ref().unwrap().addr.clone();
        let raft_response = RaftResponse::WrongLeader {
            leader_id,
            leader_addr,
        };
        // TODO handle error here
        let _ = channel.send(raft_response);
    }

    pub async fn run(mut self) -> Result<()> {
        let heartbeat_time = 10 * 1000;
        let mut heartbeat = Duration::from_millis(heartbeat_time);
        let mut now = Instant::now();

        // A map to contain sender to client responses
        let mut client_send = HashMap::new();

        loop {
            if self.should_quit {
                warn!("====== Quitting raft=======");
                return Ok(());
            }
            match timeout(heartbeat, self.rcv.recv()).await {
                Ok(Some(Message::ConfigChange { chan, mut change })) => {
                    let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                    info!(
                        "received ConfigChange request from: {} type:{:?} {}  seq={} ",
                        change.get_node_id(),
                        change.get_change_type(),
                        self.is_leader(),
                        seq,
                    );
                    // whenever a change id is 0, it's a message to self.
                    if change.get_node_id() == 0 {
                        change.set_node_id(self.id());
                    }

                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        // TODO: retry strategy in case of failure
                        self.send_wrong_leader(chan);
                    } else {
                        // leader assign new id to peer
                        client_send.insert(seq, chan);
                        self.propose_conf_change(serialize(&seq).unwrap(), change)?;
                    }
                }
                Ok(Some(Message::Raft(m))) => {
                    info!(
                        "received Raft message: to={} from={} term={} index={} {} msg_type={:?}",
                        self.raft.id,
                        m.from,
                        m.term,
                        m.index,
                        self.is_leader(),
                        m.get_msg_type()
                    );
                    if let Ok(_a) = self.step(*m) {};
                }
                Ok(Some(Message::Propose { proposal, chan })) => {
                    let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                    info!(
                        "received Propose message: raft id={} {} seq={}",
                        self.raft.id,
                        self.is_leader(),
                        seq
                    );
                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        let leader_id = self.leader();
                        // leader can't be an empty node
                        let leader_addr = self.peers[&leader_id].as_ref().unwrap().addr.clone();
                        let raft_response = RaftResponse::WrongLeader {
                            leader_id,
                            leader_addr,
                        };
                        chan.send(raft_response).unwrap();
                    } else {
                        client_send.insert(seq, chan);
                        let seq = serialize(&seq).unwrap();
                        self.propose(seq, proposal).unwrap();
                    }
                }
                Ok(Some(Message::RequestId { chan })) => {
                    info!("received requested Id");
                    if !self.is_leader() {
                        // TODO: retry strategy in case of failure
                        self.send_wrong_leader(chan);
                    } else {
                        let id = self.reserve_next_peer_id();
                        chan.send(RaftResponse::IdReserved { id }).unwrap();
                    }
                }
                Ok(Some(Message::ReportUnreachable { node_id })) => {
                    info!("received ReportUnreachable {}", node_id);
                    self.report_unreachable(node_id);
                }
                Ok(_) => unreachable!(),
                Err(e) => {
                    info!("received message timeout: {}", e);
                }
            }

            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed > heartbeat {
                heartbeat = Duration::from_millis(heartbeat_time);
                self.tick();
            } else {
                heartbeat -= elapsed;
            }

            info!("---------begin on_ready---------- {}", self.is_leader());
            self.on_ready(&mut client_send).await.unwrap();
            info!("---------end on_ready----------\n");
        }
    }

    // 1. 判断 snapshot 是不是空的，如果不是，那么表明当前节点收到了一个 Snapshot，我们需要去应用这个 snapshot。
    // 2. 判断 entries 是不是空的，如果不是，表明现在有新增的 entries，我们需要将其追加到 Raft Log 上面。
    // 3. 判断 hs 是不是空的，如果不是，表明该节点 HardState 状态变更了，
    //    可能是重新给一个新的节点 vote，也可能是 commit index 变了，但无论怎样，我们都需要将变更的 HardState 持续化。
    // 4. 判断是否有 messages，如果有，表明需要给其他 Raft 节点发送消息，具体怎么发，发到哪里，由外面实现者来保证。
    //    这里其实有一个优化，如果我们能知道节点是 Leader，那么这一步可以放到第 1 步前面。
    // 5. 判断 committed_entries 是不是空的，如果不是，表明有新的 Log 已经被提交，我们需要去应用这些 Log 到状态机上面了。
    //    当然，在应用的时候，也需要保存 apply index。
    // 6. 调用 advance 函数，开启下一次的 Ready 处理。
    async fn on_ready(
        &mut self,
        client_send: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) -> Result<()> {
        if !self.has_ready() {
            return Ok(());
        }
        let mut ready = self.ready();

        if !ready.snapshot().is_empty() {
            let snapshot = ready.snapshot();
            self.store
                .restore(self.raft.id, snapshot.get_data())
                .await?;
            let store = self.mut_store();
            store.apply_snapshot(snapshot.clone())?;
            info!("on_ready apply_snapshot={:?}", snapshot);
        }

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            let store = self.mut_store();
            store.append(entries).unwrap();
            for entry in entries.iter() {
                let seq: u64 = deserialize(&entry.get_context()).unwrap_or(0);
                info!(
                    "on_ready entries type={:?} term={} index={} seq={} ",
                    entry.get_entry_type(),
                    entry.term,
                    entry.index,
                    seq
                );
            }
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let store = self.mut_store();
            store.set_hard_state(hs).unwrap();
            info!("on_ready set_hard_state={:?}", hs);
        }

        self.handle_messages(ready.messages.clone()).await;

        if let Some(committed_entries) = ready.committed_entries.take() {
            self.handle_committed_entries(committed_entries, client_send)
                .await;
        }

        self.advance(ready);
        Ok(())
    }

    async fn handle_messages(&mut self, msgs: Vec<RaftMessage>) {
        for message in msgs.iter() {
            info!(
                "on_ready messages type={:?} from {} to {} ",
                message.get_msg_type(),
                message.get_from(),
                message.get_to(),
            );
            let client = match self.peer_mut(message.get_to()) {
                Some(ref peer) => peer.client.clone(),
                None => continue,
            };

            let message_sender = MessageSender {
                client_id: message.get_to(),
                client: client.clone(),
                chan: self.snd.clone(),
                message: message.clone(),
                timeout: Duration::from_millis(100),
                max_retries: 5,
            };
            tokio::spawn(message_sender.send());
        }
    }

    async fn handle_committed_entries(
        &mut self,
        committed_entries: Vec<Entry>,
        client_send: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) {
        let mut last_apply_index;
        for entry in &committed_entries {
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            last_apply_index = entry.get_index();
            let seq: u64 = deserialize(&entry.get_context()).unwrap_or(0);
            info!(
                "on_ready committed_entries type={:?} term={} index={} seq={} last_apply_index={}",
                entry.get_entry_type(),
                entry.term,
                entry.index,
                seq,
                last_apply_index
            );

            if entry.get_data().is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_normal(&entry, client_send).await.unwrap(),
                EntryType::EntryConfChange => self
                    .handle_config_change(&entry, client_send)
                    .await
                    .unwrap(),
                EntryType::EntryConfChangeV2 => unimplemented!(),
            }
        }
    }

    async fn handle_config_change(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context()).unwrap_or(0);
        let change: ConfChange = PMessage::decode(entry.get_data())?;
        let id = change.get_node_id();
        let change_type = change.get_change_type();

        info!("handle_config_change seq={} change={:?}", seq, change);

        match change_type {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context()).unwrap();
                info!("adding {} ({}) to peers", addr, id);
                self.add_peer(&addr, id).await.unwrap();
            }
            ConfChangeType::RemoveNode => {
                if change.get_node_id() == self.id() {
                    self.should_quit = true;
                    warn!("quiting the cluster");
                } else {
                    self.peers.remove(&change.get_node_id());
                }
            }
            _ => unimplemented!(),
        }

        if let Ok(cs) = self.apply_conf_change(&change) {
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot().await?;
            {
                let store = self.mut_store();
                store.set_conf_state(&cs)?;
                store.compact(last_applied)?;
                let _ = store.create_snapshot(snapshot)?;
            }
        }

        if let Some(sender) = senders.remove(&seq) {
            let response = match change_type {
                ConfChangeType::AddNode => RaftResponse::JoinSuccess {
                    assigned_id: id,
                    peer_addrs: self.peer_addrs(),
                },
                ConfChangeType::RemoveNode => RaftResponse::Ok,
                _ => unimplemented!(),
            };
            if sender.send(response).is_err() {
                error!("error sending response")
            }
        }
        Ok(())
    }

    async fn handle_normal(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) -> Result<()> {
        let seq: u64 = deserialize(&entry.get_context()).unwrap_or(0);
        let data = self.store.apply(entry.get_data()).await?;
        if let Some(sender) = senders.remove(&seq) {
            sender.send(RaftResponse::Response { data }).unwrap();
        }

        info!("handle_normal seq={}", seq);

        if Instant::now() > self.last_snap_time + Duration::from_secs(15000000) {
            info!("creating backup..");
            self.last_snap_time = Instant::now();
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot().await?;
            let store = self.mut_store();
            store.compact(last_applied).unwrap();
            let _ = store.create_snapshot(snapshot);
        }
        Ok(())
    }
}

impl<S: Store> Deref for RaftNode<S> {
    type Target = RawNode<HeedStorage>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Store> DerefMut for RaftNode<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
