# tiny-kv-raft

[![ci](https://github.com/Urthella/tiny-kv-raft/actions/workflows/ci.yml/badge.svg)](https://github.com/Urthella/tiny-kv-raft/actions/workflows/ci.yml)

A replicated key-value store on [openraft](https://github.com/databendlabs/openraft), built to answer
one question: under partitions and lost messages, is what clients see still linearizable?

It runs a cluster in one process over a network that can fail. Clients read and write while a nemesis
breaks that network, and a checker then decides whether one correct copy of the data could have
produced that history. To show the checker can see a real bug, you can turn off the leader check on
reads and watch it catch the stale data.

```bash
cargo run --release
cargo run --release -- --trust-local-leader
```

```
writes acknowledged: 1290, writes with no reply: 57, reads: 1303
leader changes: 4
every node ended with the same data: true
LINEARIZABLE
```

```
NOT LINEARIZABLE: no ordering explains what clients saw on k0
```

This is a test bench, not a database. Data lives in memory, and the nodes share a process. If you want a
Raft key-value store to build on, start with
[openraft's own examples](https://github.com/databendlabs/openraft/tree/main/examples).

## The bug it demonstrates

A leader that gets cut off from the others doesn't know it's been replaced, so it keeps believing it's
the leader. If reads trust that belief, a client can write through the new leader, then read through the
old one and get the value from before its own write.

| Read mode | Before answering | Result over 20 runs |
| --- | --- | --- |
| Default | Confirms with a quorum that this node still leads, using `ensure_linearizable` | 0 violations |
| `--trust-local-leader` | Nothing | 20 runs with a violation |

An earlier batch caught it in 9 of 12 runs, so a single random run can miss it. The test
`reading_from_a_partitioned_old_leader_is_caught` sets up the same bug step by step, so it fails every
time if the checker can't see it.

## Results

Measured on one machine, with 5 nodes and 6 clients for 12 seconds per run:

| Read mode | Runs | Writes acknowledged | Writes with no reply | Reads | Leader changes | Linearizable | Same data at the end |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 40 | 40,600 | 2,222 | 40,396 | 159 | 40 | 40 |
| `--trust-local-leader` | 20 | 26,002 | 1,151 | 27,130 | 87 | 0 | 20 |

Passing runs are evidence, not proof. They cover the faults below for a short time, on a store that
has no disk.

## What the nemesis does

Each fault lasts 0.7 to 1.5 seconds, long enough for a new leader to be elected, and every run starts by
isolating the leader. The others are picked at random:

- isolate the leader
- split a minority from a majority
- cut the replies to the leader, in one direction only
- lose 5 to 30% of messages

Replies can also get lost after the work is done, which is the case a client can't tell apart from the
work never having happened. A seed fixes the sequence of faults, but timing still varies, so a seed
doesn't replay a run exactly.

## How the checker works

`src/lincheck.rs` is the Wing and Gong search with Lowe's memoization. It looks for a single point in time
for each operation, between its start and its return, at which every read sees the latest write.

- **Each key is checked alone,** because a history is linearizable exactly when each key's is.
- **Only success is certain.** A write that errored or timed out may still take effect at any later time,
  or never. Treating it as failed would report violations that aren't real.
- **Unseen writes with no reply are dropped.** No read saw them, so they explain nothing, and leaving them
  in made the search explode: one early run hit the limit and proved nothing.
- **It can say "unknown".** If the search hits its limit, the run counts as a failure instead of a pass.

## Limits

- **No disk and no crashes here.** Nodes keep running through every fault, so this doesn't test recovery.
- **One process.** Delays are small and scheduling is shared, so real networks will behave differently.
- **Registers only.** It has get and set, with no transactions or scans.
- **Testing openraft, mostly.** A clean result is about this store and openraft together, and it's far
  weaker than Jepsen against real nodes.

## Tests

```bash
nix develop
cargo test
```

Planted bugs that the tests caught when this was built: a state machine that loses writes, safe reads
that skip the quorum check, a snapshot that installs without its data, and a checker that ignores order or
accepts any read. One wasn't caught: a `truncate` that does nothing. The leftover entries only matter
when a node restarts and reads its log again, and nothing here restarts.

Claude, Anthropic's AI model, wrote this with Halil Utku Demirtaş directing the work.

## License

MIT
