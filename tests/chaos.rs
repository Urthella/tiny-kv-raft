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
            c.shutdown().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the node never caught up");
}
