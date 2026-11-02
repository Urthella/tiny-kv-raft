use std::time::Duration;

use tiny_kv_raft::chaos::{run, Options};
use tiny_kv_raft::cluster::{Cluster, ReadMode};
use tiny_kv_raft::lincheck::{check, Kind, Op, Verdict};

fn seconds() -> Duration {
    let s = std::env::var("CHAOS_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok());
    Duration::from_secs(s.unwrap_or(6))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_history_stays_linearizable_under_faults() {
    for seed in [1, 2] {
        let r = run(Options {
            seed,
            duration: seconds(),
            ..Default::default()
        })
        .await;
        println!("seed {seed}: {r:?}");
        assert_eq!(r.verdict, Verdict::Linearizable, "seed {seed}");
        assert!(
            r.converged,
            "seed {seed}: the nodes ended with different data"
        );
        // A run that did nothing would pass the check, so make sure this one did the work.
        assert!(r.writes_acknowledged > 100, "seed {seed}: {r:?}");
        assert!(r.reads > 100, "seed {seed}: {r:?}");
        assert!(
            r.leader_changes >= 1,
            "seed {seed}: no failover happened: {r:?}"
        );
    }
}

/// Without any randomness: a leader cut off from the rest serves old data, and the checker says so.
/// This is what tells us the checker can actually see the bug it's for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_from_a_partitioned_old_leader_is_caught() {
    let c = Cluster::start(3).await;
    let old = c.wait_for_leader().await;
    let mut ops = Vec::new();
    let mut tick = 0u64;
    let mut next = || {
        tick += 1;
        tick
    };

    let call = next();
    c.write(old, "k", "old")
        .await
        .expect("the leader takes the write");
    ops.push(Op {
        key: "k".into(),
        kind: Kind::Write("old".into()),
        call,
        ret: Some(next()),
    });

    let rest: Vec<u64> = c.ids().into_iter().filter(|&i| i != old).collect();
    c.router.partition(&[old], &rest);
    let new = c.wait_for_new_leader(old).await;
    let call = next();
    c.write(new, "k", "new")
        .await
        .expect("the majority takes the write");
    ops.push(Op {
        key: "k".into(),
        kind: Kind::Write("new".into()),
        call,
        ret: Some(next()),
    });

    let call = next();
    let seen = c
        .read(old, "k", ReadMode::TrustLocalLeader)
        .await
        .expect("a local read can't fail");
    assert_eq!(
        seen.as_deref(),
        Some("old"),
        "the old leader should still have the old value"
    );
    ops.push(Op {
        key: "k".into(),
        kind: Kind::Read(seen),
        call,
        ret: Some(next()),
    });
    assert_eq!(check(&ops), Verdict::Violation { key: "k".into() });

    // The safe read path refuses instead of answering.
    let safe = tokio::time::timeout(
        Duration::from_secs(1),
        c.read(old, "k", ReadMode::Linearizable),
    )
    .await;
    assert!(
        !matches!(safe, Ok(Ok(_))),
        "the old leader confirmed a quorum it can't reach: {safe:?}"
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_fell_far_behind_catches_up_by_snapshot() {
    let c = Cluster::start(3).await;
    let leader = c.wait_for_leader().await;
    let behind = c
        .ids()
        .into_iter()
        .find(|&i| i != leader)
        .expect("three nodes");
    let others: Vec<u64> = c.ids().into_iter().filter(|&i| i != behind).collect();
    c.router.partition(&[behind], &others);
    // This key is only ever written here, so the snapshot is the only way for the node to get it.
    let leader = c.wait_for_new_leader(behind).await;
    c.write(leader, "only-in-snapshot", "yes")
        .await
        .expect("the majority takes the write");
    // Well past the snapshot policy, so the entries it needs get purged from the log.
    for i in 0..300 {
        let leader = c.wait_for_new_leader(behind).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), c.write(leader, "k", &i.to_string()))
            .await;
    }
    let leader = c.wait_for_new_leader(behind).await;
    let purged = c.node(leader).raft.metrics().borrow().purged;
    assert!(
        purged.is_some(),
        "the test needs the log to have been purged"
    );

    c.router.heal();
    let leader = c.wait_for_leader().await;
    c.write(leader, "k", "after")
        .await
        .expect("the write goes through");
    for _ in 0..400 {
        if c.node(behind).state_machine.get("k").await.as_deref() == Some("after") {
            let snapshot = c.node(behind).raft.metrics().borrow().snapshot;
            assert!(snapshot.is_some(), "it should have used a snapshot");
            assert_eq!(
                c.node(behind)
                    .state_machine
                    .get("only-in-snapshot")
                    .await
                    .as_deref(),
                Some("yes"),
                "the snapshot's data didn't arrive"
            );
            c.shutdown().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the node never caught up");
}

async fn wait_until_applied(c: &Cluster, id: u64, key: &str, value: &str) {
    for _ in 0..400 {
        if c.node(id).state_machine.get(key).await.as_deref() == Some(value) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {id} never applied {key}={value}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_node_gets_its_data_back() {
    let c = Cluster::start(3).await;
    let leader = c.wait_for_leader().await;
    let follower = c
        .ids()
        .into_iter()
        .find(|&i| i != leader)
        .expect("three nodes");
    c.write(leader, "before", "1")
        .await
        .expect("the write goes through");
    wait_until_applied(&c, follower, "before", "1").await;

    c.crash(follower).await;
    c.write(leader, "during", "2")
        .await
        .expect("two of three is still a quorum");
    c.restart(follower).await;
    wait_until_applied(&c, follower, "before", "1").await;
    wait_until_applied(&c, follower, "during", "2").await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_leader_loses_nothing_it_acknowledged() {
    let c = Cluster::start(3).await;
    let old = c.wait_for_leader().await;
    c.write(old, "k", "acknowledged")
        .await
        .expect("the write goes through");
    c.crash(old).await;
    let new = c.wait_for_new_leader(old).await;
    let seen = c
        .read(new, "k", ReadMode::Linearizable)
        .await
        .expect("the new leader answers");
    assert_eq!(seen.as_deref(), Some("acknowledged"));
    c.restart(old).await;
    wait_until_applied(&c, old, "k", "acknowledged").await;
    c.shutdown().await;
}

/// An isolated leader appends entries that never commit. Rejoining truncates them, and they must stay
/// gone after a restart, when the node reads its log again. This is why truncate has to really delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_entries_do_not_come_back_after_a_restart() {
    let c = Cluster::start(3).await;
    let old = c.wait_for_leader().await;
    c.write(old, "k", "committed")
        .await
        .expect("the write goes through");
    let rest: Vec<u64> = c.ids().into_iter().filter(|&i| i != old).collect();
    c.router.partition(&[old], &rest);
    for i in 0..30 {
        let value = format!("doomed-{i}");
        let doomed = c.write(old, "k", &value);
        let _ = tokio::time::timeout(Duration::from_millis(10), doomed).await;
    }
    let new = c.wait_for_new_leader(old).await;
    c.write(new, "k", "winner")
        .await
        .expect("the majority takes the write");
    c.router.heal();
    wait_until_applied(&c, old, "k", "winner").await;

    // Compare what's stored, not what the running node believes. A restart reads the store, so a
    // leftover entry there makes the node misreport its last entry, and it could then vote for a
    // candidate that's missing data it holds.
    let node = c.node(old);
    let mut stored = None;
    let mut believed = None;
    for _ in 0..80 {
        believed = node.raft.metrics().borrow().last_log_index;
        stored = node.last_stored_index().await;
        if stored == believed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        stored, believed,
        "the store still holds entries the node has truncated"
    );

    c.crash(old).await;
    c.restart(old).await;
    wait_until_applied(&c, old, "k", "winner").await;
    c.shutdown().await;
}

/// A node that forgets its vote can vote twice in one term, which allows two leaders in that term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_node_remembers_its_term() {
    let c = Cluster::start(3).await;
    let leader = c.wait_for_leader().await;
    let follower = c
        .ids()
        .into_iter()
        .find(|&i| i != leader)
        .expect("three nodes");
    for _ in 0..200 {
        if c.node(follower).raft.metrics().borrow().current_term > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let before = c.node(follower).raft.metrics().borrow().current_term;
    assert!(before > 0, "the follower should have heard from the leader");

    // Cut off, so the term it reports can only come from what it stored.
    let others: Vec<u64> = c.ids().into_iter().filter(|&i| i != follower).collect();
    c.router.partition(&[follower], &others);
    c.crash(follower).await;
    let stored = c.node(follower).stored_term().await;
    assert!(
        stored >= before,
        "it had term {before}, but only term {stored} is stored"
    );
    c.restart(follower).await;
    // Right after starting, the metrics still hold their initial values.
    for _ in 0..200 {
        if c.node(follower).raft.metrics().borrow().current_term >= before {
            c.shutdown().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the node came back without the term it had stored");
}
