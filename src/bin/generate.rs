//! Writes a sample transactions file to stdout: `generate > transactions.csv`.
//!
//! The ledger accepts every row, so a run over the file exits 0 with nothing on stderr.
//! The file depends only on the arguments.

use std::io::{self, BufWriter, Write};

use Step::{Chargeback, Deposit, Dispute, Resolve, Withdrawal};
use assignment::amount::format_amount;
use clap::Parser;

/// Writes a sample transactions file to stdout.
#[derive(Parser)]
struct Args {
    /// Number of clients. Ids start at 1.
    #[arg(long, default_value_t = u16::MAX)]
    clients: u16,
}

#[derive(Clone, Copy)]
enum Step {
    Deposit,
    /// Takes half of the client's first deposit.
    Withdrawal,
    /// Disputes the client's first deposit. `Resolve` and `Chargeback` target it too.
    Dispute,
    Resolve,
    Chargeback,
}

const STEPS_PER_CLIENT: usize = 4;

/// What a client does, in order. The ledger accepts every step because:
/// - Each script opens with the deposit that creates the client and funds the rest.
/// - A withdrawal never exceeds that deposit, and always comes before any dispute.
/// - A chargeback freezes the client, so it's only ever the last step.
/// - A chargeback's script has no withdrawal, so `total` always covers the amount.
const SCRIPTS: [[Step; STEPS_PER_CLIENT]; 4] = [
    [Deposit, Deposit, Withdrawal, Deposit],
    [Deposit, Deposit, Dispute, Resolve],
    [Deposit, Deposit, Dispute, Chargeback],
    // Leaves `available` below zero. The repeated dispute is a no-op, not a rejection.
    [Deposit, Withdrawal, Dispute, Dispute],
];

fn main() -> io::Result<()> {
    let args = Args::parse();
    let mut out = BufWriter::new(io::stdout().lock());
    writeln!(out, "type,client,tx,amount")?;
    // One step of every client at a time, so clients interleave as in a live feed while
    // each client's own rows stay in order.
    for step in 0..STEPS_PER_CLIENT {
        for client in 1..=args.clients {
            writeln!(out, "{}", row(client, step))?;
        }
    }
    out.flush()
}

fn row(client: u16, step: usize) -> String {
    let tx = tx_id(client, step);
    let first_deposit = tx_id(client, 0);
    match SCRIPTS[usize::from(client) % SCRIPTS.len()][step] {
        Deposit => format!("deposit,{client},{tx},{}", format_amount(amount(tx))),
        Withdrawal => {
            let half = amount(first_deposit).div_ceil(2);
            format!("withdrawal,{client},{tx},{}", format_amount(half))
        }
        Dispute => format!("dispute,{client},{first_deposit}"),
        Resolve => format!("resolve,{client},{first_deposit}"),
        Chargeback => format!("chargeback,{client},{first_deposit}"),
    }
}

/// Returns an id unique to this client and step. The ledger panics on a repeated id in
/// debug builds.
fn tx_id(client: u16, step: usize) -> u32 {
    (u32::from(client) - 1) * STEPS_PER_CLIENT as u32 + step as u32 + 1
}

/// Spreads amounts between 0.0001 and 100.0000 with a multiplicative hash of `tx`.
fn amount(tx: u32) -> u64 {
    u64::from(tx.wrapping_mul(2_654_435_761) % 1_000_000) + 1
}
