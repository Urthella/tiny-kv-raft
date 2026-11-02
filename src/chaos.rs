//! Clients write and read while a nemesis breaks the network, then the checker judges the history.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cluster::{Cluster, ReadMode};
use crate::lincheck::{check, Kind, Op, Verdict};
use crate::NodeId;

#[derive(Clone, Debug)]
pub struct Options {
    pub nodes: u64,
    pub clients: u64,
    pub keys: u64,
    pub duration: Duration,
    pub read_mode: ReadMode,
    pub seed: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            nodes: 5,
            clients: 6,
            keys: 4,
            duration: Duration::from_secs(10),
            read_mode: ReadMode::Linearizable,
            seed: 1,
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub verdict: Verdict,
    pub writes_acknowledged: usize,
    pub writes_unknown: usize,
    pub reads: usize,
    pub faults: BTreeMap<&'static str, u64>,
    /// How many times a node became leader during the run. A run with none didn't test failover.
    pub leader_changes: u64,
    /// Whether every node ended with the same data once the network was healed.
    pub converged: bool,
}

#[derive(Default)]
struct History {
    clock: AtomicU64,
    ops: Mutex<Vec<Op>>,
}

impl History {
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    fn record(&self, op: Op) {
        self.ops.lock().unwrap().push(op);
    }
}

const OP_TIMEOUT: Duration = Duration::from_millis(800);

async fn client(
    cluster: Arc<Cluster>,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    o: Options,
    me: u64,
) {
    let mut rng = fastrand::Rng::with_seed(o.seed.wrapping_mul(1000).wrapping_add(me));
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let leaders = cluster.believed_leaders();
        let Some(&node) = rng.choice(leaders.iter()) else {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        let key = format!("k{}", rng.u64(0..o.keys));
        if rng.bool() {
            seq += 1;
            let value = format!("c{me}-{seq}");
            let call = history.tick();
            let result = tokio::time::timeout(OP_TIMEOUT, cluster.write(node, &key, &value)).await;
            // Only a success is certain. An error might come after the entry was replicated, and a
            // timeout tells us nothing, so both may still take effect.
            let ret = matches!(result, Ok(Ok(()))).then(|| history.tick());
            history.record(Op {
                key,
                kind: Kind::Write(value),
                call,
                ret,
            });
        } else {
            let call = history.tick();
            let result =
                tokio::time::timeout(OP_TIMEOUT, cluster.read(node, &key, o.read_mode)).await;
            // A read that failed changed nothing and saw nothing, so it has no place in the history.
            if let Ok(Ok(value)) = result {
                let ret = history.tick();
                history.record(Op {
                    key,
                    kind: Kind::Read(value),
                    call,
                    ret: Some(ret),
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(rng.u64(1..8))).await;
    }
}

async fn nemesis(
    cluster: Arc<Cluster>,
    stop: Arc<AtomicBool>,
    seed: u64,
) -> BTreeMap<&'static str, u64> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut faults = BTreeMap::new();
    let ids = cluster.ids();
    let mut kind = 0; // always start with the leader, so every run tests a failover
    while !stop.load(Ordering::Relaxed) {
        let fault = match kind {
            0 => {
                // The interesting one: the old leader keeps believing it's the leader.
                if let Some(&leader) = cluster.believed_leaders().last() {
                    let rest: Vec<NodeId> = ids.iter().copied().filter(|&i| i != leader).collect();
                    cluster.router.partition(&[leader], &rest);
                }
                "isolate the leader"
            }
            1 => {
                let mut shuffled = ids.clone();
                rng.shuffle(&mut shuffled);
                let (minority, majority) = shuffled.split_at(ids.len() / 2);
                cluster.router.partition(minority, majority);
                "split minority from majority"
            }
            2 => {
                // The leader can still reach everyone, but nobody's replies reach it.
                if let Some(&leader) = cluster.believed_leaders().last() {
                    for &i in ids.iter().filter(|&&i| i != leader) {
                        cluster.router.cut_one_way(i, leader);
                    }
                }
                "cut replies to the leader"
            }
            3 => {
                cluster.router.set_loss_percent(rng.u32(5..30));
                "lose messages"
            }
            _ => {
                // Half the time the leader, since that's the crash that forces a failover.
                let leader = cluster.believed_leaders().last().copied();
                let victim = match leader {
                    Some(id) if rng.bool() => id,
                    _ => ids[rng.usize(0..ids.len())],
                };
                cluster.crash(victim).await;
                "crash a node"
            }
        };
        *faults.entry(fault).or_insert(0) += 1;
        // Long enough for the rest of the cluster to outwait the old leader's lease and elect a new
        // one. Shorter faults mostly heal first, and then nothing interesting has happened.
        tokio::time::sleep(Duration::from_millis(rng.u64(700..1500))).await;
        cluster.router.heal();
        cluster.router.set_loss_percent(0);
        cluster.restart_all().await;
        tokio::time::sleep(Duration::from_millis(rng.u64(200..500))).await;
        kind = rng.u8(0..5);
    }
    cluster.router.heal();
    cluster.router.set_loss_percent(0);
    faults
}

/// Waits for every node to apply everything, or gives up after a generous wait.
async fn wait_for_convergence(cluster: &Cluster) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut written = false;
    while tokio::time::Instant::now() < deadline {
        // A write from the current term commits everything before it. Ask whichever node leads now:
        // leadership can still move just after the network heals.
        if !written {
            if let Some(&leader) = cluster.believed_leaders().last() {
                written =
                    tokio::time::timeout(OP_TIMEOUT, cluster.write(leader, "converge", "done"))
                        .await
                        .is_ok_and(|r| r.is_ok());
            }
        }
        let copies = cluster.copies().await;
        let mut values = copies.values();
        let first = values.next().expect("there are nodes");
        if written && first.contains_key("converge") && values.all(|v| v == first) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for n in cluster.nodes() {
        let m = n.raft.metrics().borrow().clone();
        eprintln!(
            "node {}: {:?}, term {}, leader {:?}, last log {:?}, applied {:?}, snapshot {:?}, purged {:?}, {} keys",
            n.id,
            m.state,
            m.current_term,
            m.current_leader,
            m.last_log_index,
            m.last_applied,
            m.snapshot,
            m.purged,
            n.state_machine.values().await.len()
        );
    }
    false
}

pub async fn run(o: Options) -> Report {
    fastrand::seed(o.seed);
    let cluster = Arc::new(Cluster::start(o.nodes).await);
    cluster.wait_for_leader().await;
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));

    let mut clients = Vec::new();
    for me in 0..o.clients {
        clients.push(tokio::spawn(client(
            cluster.clone(),
            history.clone(),
            stop.clone(),
            o.clone(),
            me,
        )));
    }
    let chaos = tokio::spawn(nemesis(cluster.clone(), stop.clone(), o.seed));
    let watcher = {
        let (cluster, stop) = (cluster.clone(), stop.clone());
        tokio::spawn(async move {
            let mut changes = 0;
            let mut previous = cluster.believed_leaders();
            while !stop.load(Ordering::Relaxed) {
                let current = cluster.believed_leaders();
                changes += current.iter().filter(|id| !previous.contains(id)).count() as u64;
                if !current.is_empty() {
                    previous = current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            changes
        })
    };

    tokio::time::sleep(o.duration).await;
    stop.store(true, Ordering::Relaxed);
    let faults = chaos.await.expect("the nemesis finishes");
    for c in clients {
        c.await.expect("the client finishes");
    }
    let leader_changes = watcher.await.expect("the watcher finishes");
    let converged = wait_for_convergence(&cluster).await;
    cluster.shutdown().await;

    let ops = history.ops.lock().unwrap().clone();
    let is_write = |op: &Op| matches!(op.kind, Kind::Write(_));
    Report {
        verdict: check(&ops),
        writes_acknowledged: ops
            .iter()
            .filter(|o| is_write(o) && o.ret.is_some())
            .count(),
        writes_unknown: ops
            .iter()
            .filter(|o| is_write(o) && o.ret.is_none())
            .count(),
        reads: ops.iter().filter(|o| !is_write(o)).count(),
        faults,
        leader_changes,
        converged,
    }
}
