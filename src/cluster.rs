//! A cluster of nodes in this process, and the read and write paths clients use.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, ServerState, SnapshotPolicy};

use crate::network::{Network, Router};
use crate::store::{LogStore, StateMachine};
use crate::{NodeId, Raft, Set};

/// How a read reaches the data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadMode {
    /// Confirm with a quorum that this node is still the leader, then read.
    Linearizable,
    /// Read the copy of whichever node believes it's the leader. This is the bug the checker is here
    /// to catch: a partitioned old leader keeps believing it, and keeps serving old data.
    TrustLocalLeader,
}

pub struct Node {
    pub id: NodeId,
    pub raft: Raft,
    pub state_machine: Arc<StateMachine>,
}

pub struct Cluster {
    pub nodes: Vec<Node>,
    pub router: Arc<Router>,
}

impl Cluster {
    pub async fn start(size: u64) -> Self {
        let config = Arc::new(
            Config {
                heartbeat_interval: 30,
                election_timeout_min: 120,
                election_timeout_max: 240,
                // Snapshot and purge often, so a node that fell behind has to catch up by snapshot.
                snapshot_policy: SnapshotPolicy::LogsSinceLast(50),
                max_in_snapshot_log_to_keep: 5,
                purge_batch_size: 5,
                ..Default::default()
            }
            .validate()
            .expect("the config is valid"),
        );
        let router = Arc::new(Router::default());
        let mut nodes = Vec::new();
        for id in 1..=size {
            let state_machine = Arc::new(StateMachine::default());
            let network = Network {
                router: router.clone(),
                from: id,
            };
            let raft = Raft::new(
                id,
                config.clone(),
                network,
                LogStore::default(),
                state_machine.clone(),
            )
            .await
            .expect("the node starts");
            router.add(id, raft.clone());
            nodes.push(Node {
                id,
                raft,
                state_machine,
            });
        }
        let members: BTreeSet<NodeId> = (1..=size).collect();
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("the cluster initializes");
        Cluster { nodes, router }
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    pub fn node(&self, id: NodeId) -> &Node {
        self.nodes.iter().find(|n| n.id == id).expect("known node")
    }

    /// Nodes that believe they're the leader. During a partition there can be more than one.
    pub fn believed_leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.raft.metrics().borrow().state == ServerState::Leader)
            .map(|n| n.id)
            .collect()
    }

    pub async fn wait_for_leader(&self) -> NodeId {
        loop {
            if let Some(&id) = self.believed_leaders().last() {
                return id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits for a leader other than `old`. It needs a quorum, so it's only a real leader.
    pub async fn wait_for_new_leader(&self, old: NodeId) -> NodeId {
        loop {
            if let Some(&id) = self.believed_leaders().iter().find(|&&id| id != old) {
                return id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn write(&self, node: NodeId, key: &str, value: &str) -> Result<(), String> {
        let request = Set {
            key: key.into(),
            value: value.into(),
        };
        self.node(node)
            .raft
            .client_write(request)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub async fn read(
        &self,
        node: NodeId,
        key: &str,
        mode: ReadMode,
    ) -> Result<Option<String>, String> {
        let node = self.node(node);
        if mode == ReadMode::Linearizable {
            node.raft
                .ensure_linearizable()
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(node.state_machine.get(key).await)
    }

    /// Stops the nodes, which also stops their background tasks.
    pub async fn shutdown(&self) {
        for n in &self.nodes {
            let _ = n.raft.shutdown().await;
        }
    }

    /// Every node's copy of the data, by node.
    pub async fn copies(&self) -> BTreeMap<NodeId, BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        for n in &self.nodes {
            out.insert(n.id, n.state_machine.values().await);
        }
        out
    }
}
