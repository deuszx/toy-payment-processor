//! This implementation includes a trace-level logging of each processed transaction 
//! for accountability and verifiability. Logging is off by default. 
//! Set `RUST_LOG` to log every transaction to stderr:
//!
//! ```text
//! RUST_LOG=assignment=trace cargo run -- transactions.csv > accounts.csv 2> audit.log
//! ```

use std::{io, path::PathBuf, process::ExitCode};

use assignment::{
    codec::{InputError, csv_transactions},
    ledger::Ledger,
    output::{ClientWriter, CsvClientWriter},
    run::RunError,
};
use clap::Parser;
use tokio::{fs::File, io::BufWriter};
use tracing_subscriber::EnvFilter;

/// Applies a CSV file of transactions and prints the client accounts as CSV.
#[derive(Parser)]
struct Args {
    /// Path to the transactions file.
    input: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    // Logs go to stderr, because stdout carries the report.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .with_ansi(false)
        .init();

    let args = Args::parse();
    let file = match File::open(&args.input).await {
        Ok(file) => file,
        Err(error) => {
            eprintln!("{}: {error}", args.input.display());
            return ExitCode::FAILURE;
        }
    };

    let ledger = Ledger::new();
    let mut read_failed = false;
    ledger
        .run(csv_transactions(file), |error| {
            read_failed |= matches!(error, RunError::Input(InputError::Io(_)));
            eprintln!("{error:?}");
        })
        .await;
    // A partial report must not pass for a complete one.
    if read_failed {
        return ExitCode::FAILURE;
    }

    if let Err(error) = write_report(&ledger).await {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn write_report(ledger: &Ledger) -> io::Result<()> {
    let mut writer = CsvClientWriter::new(BufWriter::new(tokio::io::stdout()));
    for client in ledger.snapshot() {
        writer.write(&client).await?;
    }
    writer.finish().await
}
