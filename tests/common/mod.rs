#![allow(dead_code)]

use std::convert::Infallible;

use assignment::{
    client::ClientSnapshot, ledger::Ledger, processor::ProcessorError, run::RunError,
    transaction::Transaction,
};

pub fn deposit(client: u16, tx: u32, amount: u32) -> Transaction {
    Transaction::Deposit { client, tx, amount }
}

pub fn withdrawal(client: u16, tx: u32, amount: u32) -> Transaction {
    Transaction::Withdrawal { client, tx, amount }
}

pub fn dispute(client: u16, tx: u32) -> Transaction {
    Transaction::Dispute { client, tx }
}

pub fn resolve(client: u16, tx: u32) -> Transaction {
    Transaction::Resolve { client, tx }
}

pub fn chargeback(client: u16, tx: u32) -> Transaction {
    Transaction::Chargeback { client, tx }
}

/// Runs `txs` through `Ledger::run` and returns the rejections in order.
pub async fn run_all(ledger: &Ledger, txs: Vec<Transaction>) -> Vec<ProcessorError> {
    let input = tokio_stream::iter(txs.into_iter().map(Ok::<_, Infallible>));
    let mut errors = Vec::new();
    ledger
        .run(input, |error| match error {
            RunError::Rejected(rejected) => errors.push(rejected),
            RunError::Input(never) => match never {},
        })
        .await;
    errors
}

pub fn client(ledger: &Ledger, id: u16) -> Option<ClientSnapshot> {
    ledger.snapshot().find(|client| client.id == id)
}
