mod common;

use std::{
    convert::Infallible,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
};

use assignment::{
    ledger::Ledger,
    processor::{ProcessorError, TransactionProcessor},
    transaction::Transaction,
};
use common::{client, deposit};
use tokio::task::JoinSet;

const TASKS: u32 = 8;

/// Returns the id of every row a snapshot lists, repeats included, sorted.
fn listed_ids(ledger: &Ledger) -> Vec<u16> {
    let mut ids: Vec<u16> = ledger.snapshot().map(|client| client.id).collect();
    ids.sort_unstable();
    ids
}

/// Spawns one `Ledger::run` per stream on a shared ledger and waits for all of them.
async fn run_concurrently(ledger: &Arc<Ledger>, streams: Vec<Vec<Transaction>>) {
    let mut tasks = JoinSet::new();
    for transactions in streams {
        let ledger = ledger.clone();
        tasks.spawn(async move {
            let input = tokio_stream::iter(transactions.into_iter().map(Ok::<_, Infallible>));
            ledger.run(input, |error| panic!("{error:?}")).await;
        });
    }
    tasks.join_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposits_to_one_client_are_never_lost() {
    let ledger = Arc::new(Ledger::new());
    let streams = (0..TASKS)
        .map(|task| (0..10_000).map(|i| deposit(1, task << 16 | i, 1)).collect())
        .collect();
    run_concurrently(&ledger, streams).await;
    assert_eq!(client(&ledger, 1).unwrap().total, 80_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_withdrawals_never_overspend() {
    let ledger = Arc::new(Ledger::new());
    let client_id = 1;
    let init_amount = 1_000;
    let withdraw_amount = 1;
    ledger
        .deposit(client_id, u32::MAX, init_amount)
        .await
        .unwrap();

    let mut tasks = JoinSet::new();
    for task in 0..TASKS {
        let ledger = ledger.clone();
        tasks.spawn(async move {
            let mut succeeded = 0;
            for i in 0..500 {
                if ledger
                    .withdraw(client_id, task << 16 | i, withdraw_amount)
                    .await
                    .is_ok()
                {
                    succeeded += 1;
                }
            }
            succeeded
        });
    }
    let succeeded: u32 = tasks.join_all().await.into_iter().sum();

    assert_eq!(succeeded, init_amount / withdraw_amount);
    assert_eq!(client(&ledger, client_id).unwrap().total, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leaf_splits_do_not_lose_updates() {
    for _round in 0..5 {
        let ledger = Arc::new(Ledger::new());
        // Each task reaches every client, and ids arrive unordered. The first arrival
        // creates the client while other tasks update it, splitting leaves under load.
        let streams = (0..TASKS)
            .map(|task| {
                (0..20_000u32)
                    .map(|i| deposit((i as u16).wrapping_mul(40503), task << 16 | i, 1))
                    .collect()
            })
            .collect();
        run_concurrently(&ledger, streams).await;

        let snapshot: Vec<_> = ledger.snapshot().collect();
        assert_eq!(snapshot.len(), 20_000);
        assert!(
            snapshot
                .iter()
                .all(|client| client.total == u64::from(TASKS))
        );
    }
}

/// Races `dispute` and `resolve` on one deposit, and checks `held` is only ever 0 or the
/// full amount, even mid-race.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dispute_resolve_cycles_keep_held_consistent() {
    let ledger = Arc::new(Ledger::new());
    let client_id = 1;
    let txn_id = 1;
    ledger.deposit(client_id, txn_id, 100).await.unwrap();

    let mut tasks = JoinSet::new();
    for _ in 0..TASKS {
        let ledger = ledger.clone();
        tasks.spawn(async move {
            for _ in 0..50_000 {
                ledger.dispute(client_id, txn_id).await.unwrap();
                match ledger.resolve(client_id, txn_id).await {
                    Ok(()) | Err(ProcessorError::DisputeNotFound { .. }) => {}
                    Err(error) => panic!("{error:?}"),
                }
            }
        });
    }
    // Watches from outside, because a half-applied change can leave a correct end state.
    while !tasks.is_empty() {
        while let Some(finished) = tasks.try_join_next() {
            finished.unwrap();
        }
        let held = client(&ledger, client_id).unwrap().held;
        assert!(held == 0 || held == 100, "held {held}");
    }
    let _ = ledger.resolve(client_id, txn_id).await;
    assert_eq!(client(&ledger, client_id).unwrap().held, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn chargeback_racing_resolve_has_exactly_one_winner() {
    let (mut resolved, mut charged_back) = (0, 0);
    for _round in 0..4_000 {
        let ledger = Arc::new(Ledger::new());
        ledger.deposit(1, 1, 100).await.unwrap();
        ledger.dispute(1, 1).await.unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let resolve = tokio::spawn({
            let (ledger, barrier) = (ledger.clone(), barrier.clone());
            async move {
                barrier.wait();
                ledger.resolve(1, 1).await
            }
        });
        let chargeback = tokio::spawn({
            let (ledger, barrier) = (ledger.clone(), barrier.clone());
            async move {
                barrier.wait();
                ledger.chargeback(1, 1).await
            }
        });
        let outcome = (resolve.await.unwrap(), chargeback.await.unwrap());

        let account = client(&ledger, 1).unwrap();
        match outcome {
            (Ok(()), Err(ProcessorError::DisputeNotFound { .. })) => {
                resolved += 1;
                assert_eq!(
                    (account.total, account.held, account.locked),
                    (100, 0, false)
                );
            }
            (Err(ProcessorError::ClientFrozen { .. }), Ok(())) => {
                charged_back += 1;
                assert_eq!((account.total, account.held, account.locked), (0, 0, true));
            }
            other => panic!("{other:?} {account:?}"),
        }
    }
    assert!(resolved > 0 && charged_back > 0, "the race never contended");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn snapshot_lists_each_client_once_while_clients_are_created() {
    let ledger = Arc::new(Ledger::new());
    let client_count: u16 = 20_000;
    let ids: Vec<u16> = (0..client_count).map(|i| i.wrapping_mul(40503)).collect();
    let created = Arc::new(AtomicUsize::new(0));
    let creator = tokio::spawn({
        let (ledger, ids, created) = (ledger.clone(), ids.clone(), created.clone());
        async move {
            for (tx, id) in (0u32..).zip(ids) {
                ledger.deposit(id, tx, 1).await.unwrap();
                created.fetch_add(1, Ordering::Release);
            }
        }
    });
    while !creator.is_finished() {
        // Read before the walk, so all these clients exist when it starts.
        let existing = created.load(Ordering::Acquire);
        let listed = listed_ids(&ledger);
        assert!(listed.windows(2).all(|pair| pair[0] != pair[1]));
        assert!(
            ids[..existing]
                .iter()
                .all(|id| listed.binary_search(id).is_ok())
        );
    }
    creator.await.unwrap();

    let mut expected = ids;
    expected.sort_unstable();
    assert_eq!(listed_ids(&ledger), expected);
}
