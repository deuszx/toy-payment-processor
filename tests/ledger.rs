mod common;

use assignment::{
    amount::{format_amount, format_difference, parse_amount},
    client::ClientSnapshot,
    ledger::Ledger,
    processor::ProcessorError,
    run::RunError,
};
use common::{chargeback, client, deposit, dispute, resolve, run_all, withdrawal};

#[tokio::test]
async fn deposit_creates_client_and_credits() {
    let ledger = Ledger::new();
    let client_id = 1;
    let amount_a = 100;
    let amount_b = 50;
    let errors = run_all(
        &ledger,
        vec![
            deposit(client_id, 1, amount_a),
            deposit(client_id, 2, amount_b),
        ],
    )
    .await;
    assert_eq!(errors, vec![]);
    assert_eq!(
        client(&ledger, 1),
        Some(ClientSnapshot {
            id: client_id,
            total: (amount_a + amount_b) as u64,
            held: 0,
            locked: false
        })
    );
}

#[tokio::test]
async fn withdrawal_reduces_total() {
    let ledger = Ledger::new();
    let client_id = 1;
    let amount_deposit = 100;
    let amount_withdraw = 30;
    let errors = run_all(
        &ledger,
        vec![
            deposit(client_id, 1, amount_deposit),
            withdrawal(client_id, 2, amount_withdraw),
        ],
    )
    .await;
    assert_eq!(errors, vec![]);
    assert_eq!(
        client(&ledger, 1).unwrap().total,
        (amount_deposit - amount_withdraw) as u64
    );
}

#[tokio::test]
async fn withdrawal_from_unknown_client_is_refused_without_creating_it() {
    let ledger = Ledger::new();
    let client_id = 2;
    let errors = run_all(&ledger, vec![withdrawal(client_id, 1, 10)]).await;
    assert_eq!(
        errors,
        vec![ProcessorError::ClientNotFound {
            client: client_id,
            tx: 1
        }]
    );
    assert_eq!(client(&ledger, client_id), None);
}

#[tokio::test]
async fn withdrawal_beyond_available_is_refused() {
    let ledger = Ledger::new();
    let client_id = 1;
    let amount_deposit = 100;
    let amount_withdraw = amount_deposit + 1;
    let errors = run_all(
        &ledger,
        vec![
            deposit(client_id, 1, amount_deposit),
            withdrawal(client_id, 2, amount_withdraw),
        ],
    )
    .await;
    assert_eq!(
        errors,
        vec![ProcessorError::InsufficientFunds {
            client: client_id,
            tx: 2,
            amount: amount_withdraw,
            available: amount_deposit as u64
        }]
    );
    assert_eq!(client(&ledger, 1).unwrap().total, 100);
}

#[tokio::test]
async fn run_continues_past_rejected_transactions_and_input_errors() {
    let ledger = Ledger::new();
    let input = tokio_stream::iter(vec![
        Ok(deposit(1, 1, 100)),
        Err("bad row"),
        Ok(withdrawal(9, 2, 1)),
        Ok(deposit(1, 3, 50)),
    ]);
    let mut errors = Vec::new();
    ledger.run(input, |error| errors.push(error)).await;
    assert_eq!(
        errors,
        vec![
            RunError::Input("bad row"),
            RunError::Rejected(ProcessorError::ClientNotFound { client: 9, tx: 2 }),
        ]
    );
    assert_eq!(client(&ledger, 1).unwrap().total, 150);
}

#[tokio::test]
async fn repeated_dispute_holds_funds_once() {
    let ledger = Ledger::new();
    let client_id = 1;
    let txn_id = 1;
    let amount = 100;
    let errors = run_all(
        &ledger,
        vec![
            deposit(client_id, txn_id, amount),
            dispute(client_id, txn_id),
            dispute(client_id, txn_id),
        ],
    )
    .await;
    assert_eq!(errors, vec![]);
    assert_eq!(
        client(&ledger, client_id),
        Some(ClientSnapshot {
            id: client_id,
            total: amount as u64,
            held: amount as u64,
            locked: false
        })
    );
}

#[tokio::test]
async fn dispute_of_unknown_foreign_or_withdrawal_tx_is_refused() {
    let ledger = Ledger::new();
    let errors = run_all(
        &ledger,
        vec![
            deposit(1, 1, 100),
            deposit(2, 2, 100),
            withdrawal(1, 3, 10),
            dispute(1, 99),
            dispute(1, 2),
            dispute(1, 3),
        ],
    )
    .await;
    assert_eq!(
        errors,
        vec![
            ProcessorError::TxnNotFound { client: 1, tx: 99 },
            ProcessorError::TxnNotFound { client: 1, tx: 2 },
            ProcessorError::TxnNotFound { client: 1, tx: 3 },
        ]
    );
    assert_eq!(client(&ledger, 1).unwrap().held, 0);
}

#[tokio::test]
async fn dispute_can_take_available_below_zero_and_blocks_withdrawals() {
    let ledger = Ledger::new();
    let deposited = parse_amount("100.0").unwrap();
    let withdrawn = parse_amount("80.0").unwrap();
    let errors = run_all(
        &ledger,
        vec![
            deposit(1, 1, deposited),
            withdrawal(1, 2, withdrawn),
            dispute(1, 1),
            withdrawal(1, 3, 1),
        ],
    )
    .await;
    assert_eq!(
        errors,
        vec![ProcessorError::InsufficientFunds {
            client: 1,
            tx: 3,
            amount: 1,
            available: 0
        }]
    );
    let account = client(&ledger, 1).unwrap();
    assert_eq!(format_amount(account.total), "20.0000");
    assert_eq!(format_amount(account.held), "100.0000");
    assert_eq!(format_difference(account.total, account.held), "-80.0000");
}

#[tokio::test]
async fn resolve_releases_funds_and_transaction_can_be_disputed_again() {
    let ledger = Ledger::new();
    let errors = run_all(
        &ledger,
        vec![deposit(1, 1, 100), dispute(1, 1), resolve(1, 1)],
    )
    .await;
    assert_eq!(errors, vec![]);
    assert_eq!(client(&ledger, 1).unwrap().held, 0);

    let errors = run_all(&ledger, vec![resolve(1, 1), dispute(1, 1)]).await;
    assert_eq!(
        errors,
        vec![ProcessorError::DisputeNotFound { client: 1, tx: 1 }]
    );
    assert_eq!(client(&ledger, 1).unwrap().held, 100);
}

#[tokio::test]
async fn wrong_client_cannot_resolve_or_charge_back() {
    let ledger = Ledger::new();
    let errors = run_all(
        &ledger,
        vec![
            deposit(1, 1, 100),
            deposit(2, 2, 100),
            dispute(1, 1),
            resolve(2, 1),
            chargeback(2, 1),
        ],
    )
    .await;
    assert_eq!(
        errors,
        vec![
            ProcessorError::DisputeNotFound { client: 2, tx: 1 },
            ProcessorError::DisputeNotFound { client: 2, tx: 1 },
        ]
    );
    assert_eq!(
        client(&ledger, 1),
        Some(ClientSnapshot {
            id: 1,
            total: 100,
            held: 100,
            locked: false
        })
    );
    assert_eq!(
        client(&ledger, 2),
        Some(ClientSnapshot {
            id: 2,
            total: 100,
            held: 0,
            locked: false
        })
    );
}

#[tokio::test]
async fn chargeback_removes_funds_and_freezes() {
    let ledger = Ledger::new();
    let txs = vec![
        deposit(1, 1, 100),
        deposit(1, 2, 50),
        dispute(1, 1),
        chargeback(1, 1),
    ];
    assert_eq!(run_all(&ledger, txs).await, vec![]);
    assert_eq!(
        client(&ledger, 1),
        Some(ClientSnapshot {
            id: 1,
            total: 50,
            held: 0,
            locked: true
        })
    );
}

#[tokio::test]
async fn chargeback_beyond_total_is_refused_and_dispute_stays_open() {
    let ledger = Ledger::new();
    let errors = run_all(
        &ledger,
        vec![
            deposit(1, 1, 100),
            withdrawal(1, 2, 80),
            dispute(1, 1),
            chargeback(1, 1),
        ],
    )
    .await;
    assert_eq!(
        errors,
        vec![ProcessorError::ChargebackExceedsBalance {
            client: 1,
            tx: 1,
            amount: 100,
            total: 20
        }]
    );
    assert_eq!(
        client(&ledger, 1),
        Some(ClientSnapshot {
            id: 1,
            total: 20,
            held: 100,
            locked: false
        })
    );

    let errors = run_all(&ledger, vec![deposit(1, 3, 80), chargeback(1, 1)]).await;
    assert_eq!(errors, vec![]);
    assert_eq!(
        client(&ledger, 1),
        Some(ClientSnapshot {
            id: 1,
            total: 0,
            held: 0,
            locked: true
        })
    );
}

#[tokio::test]
async fn frozen_client_refuses_all_five_operations() {
    let ledger = Ledger::new();
    run_all(
        &ledger,
        vec![
            deposit(1, 1, 100),
            deposit(1, 2, 50),
            dispute(1, 1),
            dispute(1, 2),
            chargeback(1, 1),
        ],
    )
    .await;
    let before = client(&ledger, 1);
    assert_eq!(
        before,
        Some(ClientSnapshot {
            id: 1,
            total: 50,
            held: 50,
            locked: true
        })
    );

    let errors = run_all(
        &ledger,
        vec![
            deposit(1, 3, 1),
            withdrawal(1, 4, 1),
            dispute(1, 2),
            resolve(1, 2),
            chargeback(1, 2),
        ],
    )
    .await;
    assert_eq!(
        errors,
        vec![
            ProcessorError::ClientFrozen { client: 1, tx: 3 },
            ProcessorError::ClientFrozen { client: 1, tx: 4 },
            ProcessorError::ClientFrozen { client: 1, tx: 2 },
            ProcessorError::ClientFrozen { client: 1, tx: 2 },
            ProcessorError::ClientFrozen { client: 1, tx: 2 },
        ]
    );
    assert_eq!(client(&ledger, 1), before);
}

#[tokio::test]
async fn snapshot_reads_each_client_when_pulled() {
    let ledger = Ledger::new();
    run_all(&ledger, vec![deposit(1, 1, 10), deposit(2, 2, 10)]).await;

    let mut clients = ledger.snapshot();
    assert_eq!(clients.next().map(|client| client.total), Some(10));

    // Later rows see these writes, and the pulled client isn't left locked.
    run_all(&ledger, vec![deposit(1, 3, 5), deposit(2, 4, 5)]).await;
    assert_eq!(clients.next().map(|client| client.total), Some(15));
    assert_eq!(clients.next(), None);
}
