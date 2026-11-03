//! An in-process network that can be cut, delay messages and lose them.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::{NodeId, Raft, TypeConfig};

type RpcError<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

/// Decides what happens to each message. Cuts are one-way, so a partition is a pair of them.
#[derive(Default)]
pub struct Router {
    nodes: RwLock<BTreeMap<NodeId, Raft>>,
    cuts: Mutex<BTreeSet<(NodeId, NodeId)>>,
    loss_percent: AtomicU32,
}

impl Router {
    pub fn add(&self, id: NodeId, raft: Raft) {
        self.nodes.write().unwrap().insert(id, raft);
    }

    pub fn remove(&self, id: NodeId) {
        self.nodes.write().unwrap().remove(&id);
    }

    /// Cuts every link between the two groups, in both directions.
    pub fn partition(&self, a: &[NodeId], b: &[NodeId]) {
        let mut cuts = self.cuts.lock().unwrap();
        for &x in a {
            for &y in b {
                cuts.insert((x, y));
                cuts.insert((y, x));
            }
        }
    }

    /// Cuts one direction only, so `to` stops hearing `from` while `from` still hears `to`.
    pub fn cut_one_way(&self, from: NodeId, to: NodeId) {
        self.cuts.lock().unwrap().insert((from, to));
    }

    pub fn heal(&self) {
        self.cuts.lock().unwrap().clear();
    }

    pub fn set_loss_percent(&self, percent: u32) {
        self.loss_percent.store(percent, Ordering::Relaxed);
    }

    fn passes(&self, from: NodeId, to: NodeId) -> bool {
        !self.cuts.lock().unwrap().contains(&(from, to))
            && fastrand::u32(0..100) >= self.loss_percent.load(Ordering::Relaxed)
    }

    /// Gets the target if the message arrives, after a small delay.
    async fn outbound<E: std::error::Error>(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Result<Raft, RPCError<NodeId, BasicNode, E>> {
        tokio::time::sleep(Duration::from_millis(fastrand::u64(0..4))).await;
        if !self.passes(from, to) {
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "message lost",
            ))));
        }
        self.nodes.read().unwrap().get(&to).cloned().ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other("no such node")))
        })
    }

    /// A reply can be lost after the work is done, which is the case clients can't tell apart from
    /// the work never having happened.
    fn inbound<E: std::error::Error>(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Result<(), RPCError<NodeId, BasicNode, E>> {
        if self.passes(to, from) {
            Ok(())
        } else {
            Err(RPCError::Network(NetworkError::new(&io::Error::other(
                "reply lost",
            ))))
        }
    }
}

#[derive(Clone)]
pub struct Network {
    pub router: Arc<Router>,
    pub from: NodeId,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Connection {
        Connection {
            router: self.router.clone(),
            from: self.from,
            to: target,
        }
    }
}

pub struct Connection {
    router: Arc<Router>,
    from: NodeId,
    to: NodeId,
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        let target = self.router.outbound(self.from, self.to).await?;
        let reply = target
            .append_entries(req)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        self.router.inbound(self.from, self.to)?;
        Ok(reply)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        let target = self.router.outbound(self.from, self.to).await?;
        let reply = target
            .install_snapshot(req)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        self.router.inbound(self.from, self.to)?;
        Ok(reply)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        let target = self.router.outbound(self.from, self.to).await?;
        let reply = target
            .vote(req)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        self.router.inbound(self.from, self.to)?;
        Ok(reply)
    }
}
