use std::process::ExitCode;
use std::time::Duration;

use tiny_kv_raft::chaos::{self, Options};
use tiny_kv_raft::cluster::ReadMode;
use tiny_kv_raft::lincheck::Verdict;

const USAGE: &str =
    "Usage: tiny-kv-raft [--seconds N] [--nodes N] [--clients N] [--seed N] [--trust-local-leader]

Runs a cluster in this process, breaks its network while clients read and write, and checks whether
what they saw is linearizable. --trust-local-leader turns off the leader check on reads, which is
the bug the checker should catch.";

fn parse() -> Result<Options, String> {
    let mut o = Options::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut number = |name: &str| -> Result<u64, String> {
            args.next()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| format!("{name} needs a number"))
        };
        match arg.as_str() {
            "--seconds" => o.duration = Duration::from_secs(number("--seconds")?),
            "--nodes" => o.nodes = number("--nodes")?,
            "--clients" => o.clients = number("--clients")?,
            "--seed" => o.seed = number("--seed")?,
            "--trust-local-leader" => o.read_mode = ReadMode::TrustLocalLeader,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(o)
}

#[tokio::main]
async fn main() -> ExitCode {
    let options = match parse() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    println!(
        "{} nodes, {} clients, {:?}, reads: {:?}, seed {}",
        options.nodes, options.clients, options.duration, options.read_mode, options.seed
    );
    let report = chaos::run(options).await;
    println!(
        "writes acknowledged: {}, writes with no reply: {}, reads: {}",
        report.writes_acknowledged, report.writes_unknown, report.reads
    );
    println!("leader changes: {}", report.leader_changes);
    println!("faults: {:?}", report.faults);
    println!("every node ended with the same data: {}", report.converged);
    match report.verdict {
        Verdict::Linearizable if report.converged => {
            println!("LINEARIZABLE");
            ExitCode::SUCCESS
        }
        Verdict::Linearizable => {
            println!("LINEARIZABLE, but the nodes didn't converge");
            ExitCode::FAILURE
        }
        Verdict::Violation { key } => {
            println!("NOT LINEARIZABLE: no ordering explains what clients saw on {key}");
            ExitCode::FAILURE
        }
        Verdict::Unknown { key } => {
            println!("UNKNOWN: the search hit its limit on {key}, so this run proves nothing");
            ExitCode::FAILURE
        }
    }
}
