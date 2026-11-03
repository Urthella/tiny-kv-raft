//! In-memory log and state machine. Nothing here survives the process, so this is for testing the
//! replication logic, not for keeping data.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::{NodeId, TypeConfig};

#[derive(Default)]
struct LogInner {
    purged: Option<LogId<NodeId>>,
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

#[derive(Clone, Default)]
pub struct LogStore(Arc<Mutex<LogInner>>);

impl LogStore {
    /// The last entry actually stored, which is what a restart will believe.
    pub async fn last_stored_index(&self) -> Option<u64> {
        self.0.lock().await.entries.keys().next_back().copied()
    }

    /// The term of the stored vote, which is what a restart will believe.
    pub async fn stored_term(&self) -> u64 {
        self.0.lock().await.vote.map_or(0, |v| v.leader_id.term)
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.0.lock().await;
        Ok(inner.entries.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.0.lock().await;
        let last = inner.entries.values().next_back().map(|e| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: inner.purged,
            last_log_id: last.or(inner.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.0.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.0.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.0.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.0.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.0.lock().await;
        for entry in entries {
            inner.entries.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.0.lock().await;
        inner.entries.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.0.lock().await;
        inner.purged = Some(log_id);
        inner.entries = inner.entries.split_off(&(log_id.index + 1));
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
struct SmData {
    applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    values: BTreeMap<String, String>,
}

struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

#[derive(Default)]
pub struct StateMachine {
    data: RwLock<SmData>,
    snapshot: RwLock<Option<StoredSnapshot>>,
    next_snapshot: AtomicU64,
}

impl StateMachine {
    /// What a restarted node has: the last snapshot, as if read from disk, and nothing else. The log
    /// replays the rest.
    pub async fn recover(&self) -> Self {
        let snapshot = self.snapshot.read().await;
        let data = match snapshot.as_ref() {
            Some(s) => SmData {
                applied: s.meta.last_log_id,
                membership: s.meta.last_membership.clone(),
                values: serde_json::from_slice(&s.data).expect("we wrote this snapshot"),
            },
            None => SmData::default(),
        };
        StateMachine {
            data: RwLock::new(data),
            snapshot: RwLock::new(snapshot.as_ref().map(|s| StoredSnapshot {
                meta: s.meta.clone(),
                data: s.data.clone(),
            })),
            next_snapshot: AtomicU64::new(self.next_snapshot.load(Ordering::Relaxed)),
        }
    }

    /// Reads this node's copy. On its own this says nothing about what the cluster has committed.
    pub async fn get(&self, key: &str) -> Option<String> {
        self.data.read().await.values.get(key).cloned()
    }

    pub async fn values(&self) -> BTreeMap<String, String> {
        self.data.read().await.values.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (data, last_log_id, last_membership) = {
            let sm = self.data.read().await;
            let data = serde_json::to_vec(&sm.values)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            (data, sm.applied, sm.membership.clone())
        };
        let id = self.next_snapshot.fetch_add(1, Ordering::Relaxed);
        let snapshot_id = match last_log_id {
            Some(log) => format!("{}-{}-{id}", log.leader_id, log.index),
            None => format!("none-{id}"),
        };
        let meta = SnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id,
        };
        *self.snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.data.read().await;
        Ok((sm.applied, sm.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut sm = self.data.write().await;
        let mut replies = Vec::new();
        for entry in entries {
            sm.applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(set) => {
                    sm.values.insert(set.key, set.value);
                }
                EntryPayload::Membership(m) => {
                    sm.membership = StoredMembership::new(Some(entry.log_id), m);
                }
            }
            replies.push(());
        }
        Ok(replies)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let values = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        *self.data.write().await = SmData {
            applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
            values,
        };
        *self.snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self.snapshot.read().await.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
