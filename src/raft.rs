use crate::error::{Error, Result};
use crate::message::{Message, RaftResponse};
use crate::raft_node::RaftNode;
use crate::raft_server::RaftServer;

use async_trait::async_trait;
use bincode::serialize;
use log::{info, warn};
use raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tonic::Request;

use std::time::Duration;

const LEADER_ID: u64 = 11;
#[async_trait]
pub trait Store {
    async fn init(&mut self, id: u64) -> Result<()>;

    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&mut self, id: u64, snapshot: &[u8]) -> Result<()>;
}

/// A mailbox to send messages to a ruung raft node.
#[derive(Clone)]
pub struct Mailbox(mpsc::Sender<Message>);

impl Mailbox {
    /// sends a proposal message to commit to the node. This fails if the current node is not the
    /// leader
    pub async fn send(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let proposal = Message::Propose {
            proposal: message,
            chan: tx,
        };
        let mut sender = self.0.clone();
        // TODO make timeout duration a variable
        match sender.send(proposal).await {
            Ok(_) => match timeout(Duration::from_secs(2), rx).await {
                Ok(Ok(RaftResponse::Response { data })) => Ok(data),
                Ok(Ok(RaftResponse::WrongLeader {
                    leader_id,
                    leader_addr,
                })) => Ok(format!("leader: {}-{}", leader_id, leader_addr)
                    .as_bytes()
                    .to_vec()),
                _ => Err(Error::Unknown),
            },
            Err(e) => Ok(e.to_string().as_bytes().to_vec()),
        }
    }

    pub async fn leave(&self) -> Result<()> {
        let mut change = ConfChange::default();
        // set node id to 0, the node will set it to self when it receives it.
        change.set_node_id(0);
        change.set_change_type(ConfChangeType::RemoveNode);
        let mut sender = self.0.clone();
        let (chan, rx) = oneshot::channel();
        match sender.send(Message::ConfigChange { change, chan }).await {
            Ok(_) => match rx.await {
                Ok(RaftResponse::Ok) => Ok(()),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    addr: String,
    logger: slog::Logger,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// creates a new node with the given address and store.
    pub fn new(addr: String, store: S, logger: slog::Logger) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            store,
            tx,
            rx,
            addr,
            logger,
        }
    }

    /// gets the node's `Mailbox`.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox(self.tx.clone())
    }

    /// Create a new leader for the cluster, with id 1. There has to be exactly one node in the
    /// cluster that is initialised that way
    pub async fn lead(mut self) -> Result<()> {
        self.store.init(LEADER_ID).await.unwrap();
        let addr = self.addr.clone();
        let node = RaftNode::new_leader(
            LEADER_ID,
            self.rx,
            self.tx.clone(),
            self.store,
            &self.logger,
        );
        let server = RaftServer::new(self.tx, addr);
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(node.run());
        let _ = tokio::try_join!(node_handle);
        warn!("leaving leader node");

        Ok(())
    }

    /// Tries to join a new cluster at `addr`, getting an id from the leader, or finding it if
    /// `addr` is not the current leader of the cluster
    pub async fn join(mut self, leader_addr: String, node_id: u64) -> Result<()> {
        self.store.init(node_id).await.unwrap();
        // 1. try to discover the leader and obtain an id from it.
        info!("attempting to join peer cluster at {}", leader_addr);

        info!("obtained ID from leader: {}", node_id);
        // 2. run server and node to prepare for joining
        let mut node =
            RaftNode::new_follower(self.rx, self.tx.clone(), node_id, self.store, &self.logger)?;
        node.add_peer(&leader_addr, LEADER_ID).await?;
        let mut client = node.peer_mut(LEADER_ID).unwrap().clone();
        let server = RaftServer::new(self.tx, self.addr.clone());
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(node.run());

        // 3. Join the cluster
        // TODO: handle wrong leader
        let mut change = ConfChange::default();
        change.set_node_id(node_id);
        change.set_change_type(ConfChangeType::AddNode);
        change.set_context(serialize(&self.addr)?);
        client.change_config(Request::new(change)).await?;
        let _ = tokio::try_join!(node_handle);

        Ok(())
    }
}
