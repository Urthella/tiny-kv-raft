//! Decides whether a history of reads and writes is linearizable: whether every operation can be
//! given a single point in time, between when it started and when it returned, such that each read
//! sees the latest write. This is the Wing and Gong search with Lowe's memoization.
//!
//! Each key is an independent register, and a history is linearizable exactly when each key's is,
//! which keeps the search small.

use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    Write(String),
    Read(Option<String>),
}

#[derive(Clone, Debug)]
pub struct Op {
    pub key: String,
    pub kind: Kind,
    pub call: u64,
    /// When it returned. `None` means the client never learned: a write that timed out may take
    /// effect at any moment after it started, or never.
    pub ret: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    /// No ordering explains this key's history.
    Violation {
        key: String,
    },
    /// The search hit its limit, so this history proves nothing either way.
    Unknown {
        key: String,
    },
}

const SEARCH_LIMIT: usize = 2_000_000;

pub fn check(history: &[Op]) -> Verdict {
    let mut by_key: BTreeMap<&str, Vec<&Op>> = BTreeMap::new();
    for op in history {
        by_key.entry(&op.key).or_default().push(op);
    }
    let mut unknown = None;
    for (key, ops) in by_key {
        match check_register(&ops) {
            Some(true) => {}
            Some(false) => return Verdict::Violation { key: key.into() },
            None => unknown = Some(key.to_string()),
        }
    }
    match unknown {
        Some(key) => Verdict::Unknown { key },
        None => Verdict::Linearizable,
    }
}

fn check_register(ops: &[&Op]) -> Option<bool> {
    // A write with no reply that no read ever saw can't explain anything, and leaving it out is
    // always allowed, since it may never have happened. Each one kept can go anywhere after its start,
    // so these are what make the search explode.
    let seen_values: HashSet<&str> = ops
        .iter()
        .filter_map(|op| match &op.kind {
            Kind::Read(Some(v)) => Some(v.as_str()),
            _ => None,
        })
        .collect();
    let ops: Vec<&Op> = ops
        .iter()
        .copied()
        .filter(|op| match &op.kind {
            Kind::Write(v) => op.ret.is_some() || seen_values.contains(v.as_str()),
            Kind::Read(_) => true,
        })
        .collect();
    let words = ops.len().div_ceil(64);
    let mut start = vec![0u64; words];
    // A read with no outcome tells us nothing, so only reads that returned are in `ops` to check.
    for (i, op) in ops.iter().enumerate() {
        if op.ret.is_none() && matches!(op.kind, Kind::Read(_)) {
            start[i / 64] |= 1 << (i % 64);
        }
    }
    let required: Vec<usize> = (0..ops.len()).filter(|&i| ops[i].ret.is_some()).collect();

    let mut stack: Vec<(Vec<u64>, Option<&str>)> = vec![(start, None)];
    let mut seen: HashSet<(Vec<u64>, Option<&str>)> = HashSet::new();
    let mut steps = 0;
    while let Some((done, value)) = stack.pop() {
        steps += 1;
        if steps > SEARCH_LIMIT {
            return None;
        }
        let is_done = |i: usize| done[i / 64] & (1 << (i % 64)) != 0;
        let Some(deadline) = required
            .iter()
            .filter(|&&i| !is_done(i))
            .map(|&i| ops[i].ret.expect("required ops returned"))
            .min()
        else {
            return Some(true);
        };
        // Anything that started before the earliest unfinished return could be next.
        for (i, op) in ops.iter().enumerate() {
            if is_done(i) || op.call >= deadline {
                continue;
            }
            let next = match &op.kind {
                Kind::Write(v) => Some(v.as_str()),
                Kind::Read(expected) => {
                    if expected.as_deref() != value {
                        continue;
                    }
                    value
                }
            };
            let mut after = done.clone();
            after[i / 64] |= 1 << (i % 64);
            if seen.insert((after.clone(), next)) {
                stack.push((after, next));
            }
        }
    }
    Some(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(key: &str, value: &str, call: u64, ret: u64) -> Op {
        Op {
            key: key.into(),
            kind: Kind::Write(value.into()),
            call,
            ret: Some(ret),
        }
    }

    fn lost_write(key: &str, value: &str, call: u64) -> Op {
        Op {
            key: key.into(),
            kind: Kind::Write(value.into()),
            call,
            ret: None,
        }
    }

    fn read(key: &str, value: Option<&str>, call: u64, ret: u64) -> Op {
        Op {
            key: key.into(),
            kind: Kind::Read(value.map(Into::into)),
            call,
            ret: Some(ret),
        }
    }

    fn violation(key: &str) -> Verdict {
        Verdict::Violation { key: key.into() }
    }

    #[test]
    fn sequential_reads_see_the_latest_write() {
        let h = [write("k", "a", 1, 2), read("k", Some("a"), 3, 4)];
        assert_eq!(check(&h), Verdict::Linearizable);
    }

    #[test]
    fn a_stale_read_after_a_finished_write_is_a_violation() {
        let h = [
            write("k", "a", 1, 2),
            write("k", "b", 3, 4),
            read("k", Some("a"), 5, 6),
        ];
        assert_eq!(check(&h), violation("k"));
    }

    #[test]
    fn a_read_of_a_value_nobody_wrote_is_a_violation() {
        assert_eq!(check(&[read("k", Some("x"), 1, 2)]), violation("k"));
    }

    #[test]
    fn a_read_overlapping_a_write_may_see_either_value() {
        for seen in [Some("a"), Some("b")] {
            let h = [
                write("k", "a", 1, 2),
                write("k", "b", 3, 6),
                read("k", seen, 4, 5),
            ];
            assert_eq!(check(&h), Verdict::Linearizable, "{seen:?}");
        }
    }

    #[test]
    fn values_can_not_go_backwards_between_reads() {
        // Both reads overlap the write, but once one has seen the new value, a later one can't see
        // the old value.
        let h = [
            write("k", "a", 1, 2),
            write("k", "b", 3, 10),
            read("k", Some("b"), 4, 5),
            read("k", Some("a"), 6, 7),
        ];
        assert_eq!(check(&h), violation("k"));
    }

    #[test]
    fn a_write_with_no_reply_may_have_happened_or_not() {
        let happened = [lost_write("k", "a", 1), read("k", Some("a"), 5, 6)];
        assert_eq!(check(&happened), Verdict::Linearizable);
        let never = [lost_write("k", "a", 1), read("k", None, 5, 6)];
        assert_eq!(check(&never), Verdict::Linearizable);
    }

    #[test]
    fn a_write_with_no_reply_can_take_effect_late() {
        let h = [
            lost_write("k", "a", 1),
            write("k", "b", 2, 3),
            read("k", Some("b"), 4, 5),
            read("k", Some("a"), 6, 7),
        ];
        assert_eq!(check(&h), Verdict::Linearizable);
    }

    #[test]
    fn a_write_with_no_reply_can_not_explain_a_value_before_it_started() {
        let h = [read("k", Some("a"), 1, 2), lost_write("k", "a", 3)];
        assert_eq!(check(&h), violation("k"));
    }

    #[test]
    fn keys_are_independent() {
        let h = [
            write("a", "1", 1, 2),
            write("b", "2", 3, 4),
            read("a", Some("1"), 5, 6),
            read("b", Some("2"), 7, 8),
        ];
        assert_eq!(check(&h), Verdict::Linearizable);
    }

    #[test]
    fn many_unobserved_writes_with_no_reply_stay_tractable() {
        let mut h = Vec::new();
        for i in 0..2_000u64 {
            h.push(write("k", &i.to_string(), i * 4 + 100, i * 4 + 101));
            h.push(read("k", Some(&i.to_string()), i * 4 + 102, i * 4 + 103));
        }
        for i in 0..200u64 {
            h.push(lost_write("k", &format!("lost-{i}"), i));
        }
        assert_eq!(check(&h), Verdict::Linearizable);
    }

    #[test]
    fn dropping_unobserved_writes_does_not_hide_a_stale_read() {
        let h = [
            lost_write("k", "never-seen", 0),
            write("k", "a", 1, 2),
            write("k", "b", 3, 4),
            read("k", Some("a"), 5, 6),
        ];
        assert_eq!(check(&h), violation("k"));
    }

    #[test]
    fn long_histories_stay_tractable() {
        let mut h = Vec::new();
        for i in 0..3_000u64 {
            h.push(write("k", &i.to_string(), i * 4, i * 4 + 1));
            h.push(read("k", Some(&i.to_string()), i * 4 + 2, i * 4 + 3));
        }
        assert_eq!(check(&h), Verdict::Linearizable);
    }
}
