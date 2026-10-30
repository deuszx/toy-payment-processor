# Transaction Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a concurrent, in-memory transaction ledger that consumes any async stream of transactions and reports client accounts through a pluggable writer, with CSV as the first format on both ends.

**Architecture:** `Ledger` holds three `scc::TreeIndex` maps and implements `TransactionProcessor`. Each client sits behind its own `Arc<parking_lot::Mutex<_>>`, and critical sections are fully synchronous. `Ledger::run` drains a `Stream` in order. CSV lives at the edges: `TransactionCodec` turns any `AsyncRead` into that stream, and `CsvClientWriter` implements `ClientWriter`.

**Tech Stack:** Rust 2024, tokio, tokio-util (`codec`), tokio-stream, parking_lot, scc, clap.

**Spec:** `docs/superpowers/specs/2026-10-30-transaction-processor-design.md`

## Global Constraints

- No lock is held across `.await`. `src/lib.rs` sets `#![deny(clippy::await_holding_lock)]`. Never enable the `parking_lot` feature `send_guard`.
- `clients` value type is exactly `Arc<parking_lot::Mutex<Client>>`. `TreeIndex` leaf splits bitwise-copy values, so clone the `Arc` out of the index, then lock it. Never lock through a reference borrowed from the index.
- Inside a critical section use only `_sync` map operations and `peek_with`. Never hold two client locks at once.
- Amounts are `u32` in units of 0.0001. Balances are `u64`. No floats anywhere.
- Tx ids are guaranteed unique. Do not check for duplicates. No `DuplicateTx` variant exists.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- `.serena/` is not committed.

## Review Focus

- Input with zero clients (empty file or header only): prints the CSV header, exits 0. Pinned in Tasks 5 and 7.
- A row with invalid UTF-8: skipped and reported, every later row still applies. Pinned in Task 6.
- Padded or short rows (`deposit, 1, 1, 1.0`, `dispute,1,1,`, `dispute,1,1`): parse normally. Pinned in Task 6.
- `resolve` or `chargeback` naming the wrong client: refused, and the real client's dispute is untouched. Pinned in Task 3.
- Unreadable input (missing path, or a directory): exits non-zero with nothing on stdout. Pinned in Task 7.

## File Map

| File | Responsibility |
|------|----------------|
| `src/lib.rs` | Crate root, lint, module list |
| `src/amount.rs` | Parse and format fixed-point amounts |
| `src/transaction.rs` | `Transaction` enum |
| `src/client.rs` | `Client` state, `ClientSnapshot` |
| `src/processor.rs` | `TransactionProcessor`, `ProcessorError` |
| `src/ledger.rs` | `Ledger`, renamed from `src/db.rs`: maps, trait impl, `snapshot` |
| `src/run.rs` | `Ledger::run`, `RunError` |
| `src/output.rs` | `ClientWriter`, `CsvClientWriter` |
| `src/codec.rs` | `parse_line`, `TransactionCodec`, `InputError`, `csv_transactions` |
| `src/main.rs` | CLI |
| `tests/common/mod.rs` | Transaction builders, `run_all`, `client` helpers |
| `tests/ledger.rs`, `tests/concurrency.rs`, `tests/cli.rs` | Integration tests |

Tasks 1 to 5 are the spec's build phase 1. Tasks 6 and 7 are phase 2, and work can stop cleanly after Task 5.

---

### Task 1: Crate setup and amounts

**Files:**
- Modify: `Cargo.toml`, `src/main.rs`, `src/transaction.rs`
- Create: `src/lib.rs`, `src/amount.rs`
- Test: inline `#[cfg(test)]` in `src/amount.rs`

**Interfaces:**
- Produces, in `amount`:
  - `pub const AMOUNT_DECIMALS: u8 = 4`
  - `pub fn parse_amount(s: &str) -> Result<u32, AmountError>`
  - `pub fn format_amount(units: u64) -> String`
  - `pub fn format_difference(minuend: u64, subtrahend: u64) -> String`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum AmountError { Malformed, TooManyDecimals, TooLarge }`

- [ ] **Step 1: Create the baseline commit and branch**

The repo has no commits yet, so `main` must exist before a branch can target it. Skip the `git add` and `git commit` if `git log` already shows a baseline.

```bash
git add .gitignore Cargo.toml Cargo.lock src docs
git commit -m "chore: add crate skeleton and design"
git switch -c feat/transaction-processor
```

- [ ] **Step 2: Set dependencies in `Cargo.toml`**

```toml
[dependencies]
clap = { version = "4.6.7", features = ["derive"] }
parking_lot = "0.12.5"
scc = "3.8.8"
tokio = { version = "1.54.1", features = ["rt-multi-thread", "macros", "fs", "io-util", "io-std"] }
tokio-stream = "0.1.19"
tokio-util = { version = "0.7.20", features = ["codec"] }
```

This drops `papaya`, `serde` and `csv`. Rows are simple enough to split by hand.

- [ ] **Step 3: Create `src/lib.rs` and thin `src/main.rs`**

`src/lib.rs` holds `#![deny(clippy::await_holding_lock)]` and `pub mod amount; pub mod client; pub mod db; pub mod processor; pub mod transaction;`. `src/main.rs` loses its `mod` lines. Remove `AMOUNT_DECIMALS` from `src/transaction.rs`, and add `#[derive(Debug, Clone, PartialEq, Eq)]` to `Transaction`.

Run: `cargo check`
Expected: exit 0.

- [ ] **Step 4: Write the failing tests in `src/amount.rs`**

```rust
#[test]
fn parses_up_to_four_decimals() {
    assert_eq!(parse_amount("1"), Ok(10_000));
    assert_eq!(parse_amount("1.5"), Ok(15_000));
    assert_eq!(parse_amount("0.0001"), Ok(1));
    assert_eq!(parse_amount("429496.7295"), Ok(u32::MAX));
}

#[test]
fn rejects_invalid_amounts() {
    assert_eq!(parse_amount("1.23456"), Err(AmountError::TooManyDecimals));
    assert_eq!(parse_amount("1.50000"), Err(AmountError::TooManyDecimals));
    assert_eq!(parse_amount("429496.7296"), Err(AmountError::TooLarge));
    assert_eq!(parse_amount("99999999999999999999"), Err(AmountError::TooLarge));
    for bad in ["-1", "+1", "", ".", "abc", "1.2.3"] {
        assert_eq!(parse_amount(bad), Err(AmountError::Malformed), "{bad:?}");
    }
}

#[test]
fn formats_exactly_four_decimals() {
    assert_eq!(format_amount(15_000), "1.5000");
    assert_eq!(format_amount(1), "0.0001");
    assert_eq!(format_amount(0), "0.0000");
    assert_eq!(format_amount(u64::MAX), "1844674407370955.1615");
}

#[test]
fn formats_signed_difference_without_overflow() {
    assert_eq!(format_difference(200_000, 1_000_000), "-80.0000");
    assert_eq!(format_difference(1_000_000, 200_000), "80.0000");
    assert_eq!(format_difference(5, 5), "0.0000");
    assert_eq!(format_difference(0, u64::MAX), "-1844674407370955.1615");
}
```

- [ ] **Step 5: Run the tests to verify they fail**

Run: `cargo test --lib amount`
Expected: FAIL to compile, `parse_amount` not found.

- [ ] **Step 6: Implement `src/amount.rs`**

`parse_amount`: `split_once('.')`, require ASCII digits only and at least one digit overall (so `-` and `+` are `Malformed`), then check the decimal count. Parse the integer part with `u64`, treating a parse failure as `TooLarge`, and combine with `checked_mul`, `checked_add` and `u32::try_from`.

`format_difference`: pick the sign by comparing the arguments, then `format_amount` the absolute difference. Equal arguments have no sign.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test --lib amount`
Expected: PASS, 4 tests.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src
git commit -m "feat: add crate root and fixed-point amounts"
```

### Task 2: Ledger with deposit, withdraw, snapshot and run

**Files:**
- Modify: `src/client.rs`, `src/processor.rs`, `src/lib.rs`
- Rename: `src/db.rs` to `src/ledger.rs`
- Create: `src/run.rs`, `tests/common/mod.rs`, `tests/ledger.rs`

**Interfaces:**
- Consumes: `format_amount`, `format_difference`, `Transaction`.
- Produces, in `client`:
  - `pub struct Client { id: u16, total: u64, held: u64, locked: bool }`, with `pub fn new(id: u16) -> Self`, `pub fn total(&self) -> u64`, `pub fn is_locked(&self) -> bool`, `pub fn spendable(&self) -> u64` (`total.saturating_sub(held)`), and `pub fn snapshot(&self) -> ClientSnapshot`
  - `pub(crate)` mutators, all taking `&mut self, amount: u32`: `credit`, `debit`, `hold`, `release`, `charge_back`. `charge_back` lowers `held` and `total` and sets `locked`.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct ClientSnapshot { pub id: u16, pub total: u64, pub held: u64, pub locked: bool }`
- Produces, in `processor`:
  - All five trait methods in the form `fn deposit(&self, client: u16, tx: u32, amount: u32) -> impl Future<Output = Result<(), ProcessorError>> + Send;`
  - `#[derive(Debug, PartialEq, Eq)] ProcessorError` with the existing variants plus `ClientFrozen { client: u16, tx: u32 }` and `ChargebackExceedsBalance { client: u16, tx: u32, amount: u32, total: u64 }`. `InsufficientFunds.available` becomes `u64`.
- Produces, in `ledger`: `#[derive(Default)] pub struct Ledger`, `pub fn new() -> Self`, `pub fn snapshot(&self) -> Vec<ClientSnapshot>`.
- Produces, in `run`:
  - `#[derive(Debug, PartialEq, Eq)] pub enum RunError<E> { Input(E), Rejected(ProcessorError) }`
  - `pub async fn run<S, E, F>(&self, input: S, mut on_error: F) where S: Stream<Item = Result<Transaction, E>>, F: FnMut(RunError<E>)`
- Produces, in `tests/common/mod.rs` (with `#![allow(dead_code)]`):
  - `deposit(client, tx, amount)`, `withdrawal(client, tx, amount)`, `dispute(client, tx)`, `resolve(client, tx)`, `chargeback(client, tx)`, each `-> Transaction`
  - `pub async fn run_all(ledger: &Ledger, txs: Vec<Transaction>) -> Vec<ProcessorError>`: runs `tokio_stream::iter` with `E = Infallible` and collects rejections
  - `pub fn client(ledger: &Ledger, id: u16) -> Option<ClientSnapshot>`

- [ ] **Step 1: Write the failing tests in `tests/ledger.rs`, plus the helpers**

```rust
#[tokio::test]
async fn deposit_creates_client_and_credits() {
    let ledger = Ledger::new();
    assert_eq!(run_all(&ledger, vec![deposit(1, 1, 100), deposit(1, 2, 50)]).await, vec![]);
    assert_eq!(client(&ledger, 1), Some(ClientSnapshot { id: 1, total: 150, held: 0, locked: false }));
}

#[tokio::test]
async fn withdrawal_reduces_total() {
    let ledger = Ledger::new();
    assert_eq!(run_all(&ledger, vec![deposit(1, 1, 100), withdrawal(1, 2, 30)]).await, vec![]);
    assert_eq!(client(&ledger, 1).unwrap().total, 70);
}

#[tokio::test]
async fn withdrawal_from_unknown_client_is_refused_without_creating_it() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![withdrawal(2, 1, 10)]).await;
    assert_eq!(errors, vec![ProcessorError::ClientNotFound { client: 2, tx: 1 }]);
    assert_eq!(client(&ledger, 2), None);
}

#[tokio::test]
async fn withdrawal_beyond_available_is_refused() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![deposit(1, 1, 100), withdrawal(1, 2, 150)]).await;
    assert_eq!(errors, vec![ProcessorError::InsufficientFunds { client: 1, tx: 2, amount: 150, available: 100 }]);
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
    ledger.run(input, |e| errors.push(e)).await;
    assert_eq!(errors, vec![
        RunError::Input("bad row"),
        RunError::Rejected(ProcessorError::ClientNotFound { client: 9, tx: 2 }),
    ]);
    assert_eq!(client(&ledger, 1).unwrap().total, 150);
}

#[tokio::test]
async fn snapshot_is_sorted_by_client_id() {
    let ledger = Ledger::new();
    run_all(&ledger, vec![deposit(3, 1, 1), deposit(1, 2, 1), deposit(2, 3, 1)]).await;
    let ids: Vec<u16> = ledger.snapshot().iter().map(|c| c.id).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test ledger`
Expected: FAIL to compile, `Ledger` not found.

- [ ] **Step 3: Update `src/client.rs` and `src/processor.rs`**

Per the Interfaces block. `Display` for `Client` keeps its shape and prints `available` with `format_difference`, and `held` and `total` with `format_amount`. Bring the trait's doc comments in line with the spec's Operation rules, including freezing and refused chargebacks.

- [ ] **Step 4: Implement `src/ledger.rs`**

`git mv src/db.rs src/ledger.rs`, update `src/lib.rs`, and remove the TODOs. `Dispute` becomes `{ client: u16, amount: u32 }`: the key already holds `tx`, and a `tx` field would be dead code.

```rust
#[derive(Default)]
pub struct Ledger {
    clients: scc::TreeIndex<u16, Arc<Mutex<Client>>>,
    transactions: scc::TreeIndex<u32, Transaction>,
    disputes: scc::TreeIndex<u32, Dispute>,
}
```

Private helpers:
- `fn client(&self, id: u16) -> Option<Arc<Mutex<Client>>>`, using `peek_with` to clone the `Arc`
- `fn client_or_create(&self, id: u16) -> Arc<Mutex<Client>>`, where the loser of `insert_sync` falls back to `client`

Implement `deposit` and `withdraw` by the spec's numbered steps. Record each tx with `insert_sync` under the client lock, followed by `debug_assert!` on the result. `dispute`, `resolve` and `chargeback` are `unimplemented!()` for now.

`snapshot`: create `scc::Guard::new()`, iterate `self.clients.iter(&guard)`, lock each client and call `Client::snapshot`.

- [ ] **Step 5: Implement `src/run.rs`**

`std::pin::pin!(input)`, then loop on `tokio_stream::StreamExt::next`. Route `Err(e)` to `on_error(RunError::Input(e))`. Dispatch `Ok(tx)` through a private `async fn apply(&self, tx: Transaction) -> Result<(), ProcessorError>`, routing failures to `RunError::Rejected`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test --test ledger`
Expected: PASS, 6 tests.

- [ ] **Step 7: Commit**

```bash
git add src tests
git commit -m "feat: add ledger with deposit, withdraw and stream runner"
```

### Task 3: Disputes, resolves and chargebacks

**Files:**
- Modify: `src/ledger.rs`
- Test: `tests/ledger.rs`

**Interfaces:**
- Consumes: `Client::hold`, `release`, `charge_back`, `is_locked`, `total`, and the helpers from Task 2.
- Produces: complete `TransactionProcessor` for `Ledger`.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn repeated_dispute_holds_funds_once() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![deposit(1, 1, 100), dispute(1, 1), dispute(1, 1)]).await;
    assert_eq!(errors, vec![]);
    assert_eq!(client(&ledger, 1), Some(ClientSnapshot { id: 1, total: 100, held: 100, locked: false }));
}

#[tokio::test]
async fn dispute_of_unknown_foreign_or_withdrawal_tx_is_refused() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![
        deposit(1, 1, 100), deposit(2, 2, 100), withdrawal(1, 3, 10),
        dispute(1, 99), dispute(1, 2), dispute(1, 3),
    ]).await;
    assert_eq!(errors, vec![
        ProcessorError::TxnNotFound { client: 1, tx: 99 },
        ProcessorError::TxnNotFound { client: 1, tx: 2 },
        ProcessorError::TxnNotFound { client: 1, tx: 3 },
    ]);
    assert_eq!(client(&ledger, 1).unwrap().held, 0);
}

#[tokio::test]
async fn dispute_can_take_available_below_zero_and_blocks_withdrawals() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![
        deposit(1, 1, 100), withdrawal(1, 2, 80), dispute(1, 1), withdrawal(1, 3, 1),
    ]).await;
    assert_eq!(errors, vec![ProcessorError::InsufficientFunds { client: 1, tx: 3, amount: 1, available: 0 }]);
    let c = client(&ledger, 1).unwrap();
    assert_eq!((c.total, c.held), (20, 100));
    assert_eq!(format_difference(c.total, c.held), "-0.0080");
}

#[tokio::test]
async fn resolve_releases_funds_and_transaction_can_be_disputed_again() {
    let ledger = Ledger::new();
    assert_eq!(run_all(&ledger, vec![deposit(1, 1, 100), dispute(1, 1), resolve(1, 1)]).await, vec![]);
    assert_eq!(client(&ledger, 1).unwrap().held, 0);
    let errors = run_all(&ledger, vec![resolve(1, 1), dispute(1, 1)]).await;
    assert_eq!(errors, vec![ProcessorError::DisputeNotFound { client: 1, tx: 1 }]);
    assert_eq!(client(&ledger, 1).unwrap().held, 100);
}

#[tokio::test]
async fn wrong_client_cannot_resolve_or_charge_back() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![
        deposit(1, 1, 100), deposit(2, 2, 100), dispute(1, 1), resolve(2, 1), chargeback(2, 1),
    ]).await;
    assert_eq!(errors, vec![
        ProcessorError::DisputeNotFound { client: 2, tx: 1 },
        ProcessorError::DisputeNotFound { client: 2, tx: 1 },
    ]);
    assert_eq!(client(&ledger, 1), Some(ClientSnapshot { id: 1, total: 100, held: 100, locked: false }));
    assert_eq!(client(&ledger, 2), Some(ClientSnapshot { id: 2, total: 100, held: 0, locked: false }));
}

#[tokio::test]
async fn chargeback_removes_funds_and_freezes() {
    let ledger = Ledger::new();
    let txs = vec![deposit(1, 1, 100), deposit(1, 2, 50), dispute(1, 1), chargeback(1, 1)];
    assert_eq!(run_all(&ledger, txs).await, vec![]);
    assert_eq!(client(&ledger, 1), Some(ClientSnapshot { id: 1, total: 50, held: 0, locked: true }));
}

#[tokio::test]
async fn chargeback_beyond_total_is_refused_and_dispute_stays_open() {
    let ledger = Ledger::new();
    let errors = run_all(&ledger, vec![
        deposit(1, 1, 100), withdrawal(1, 2, 80), dispute(1, 1), chargeback(1, 1),
    ]).await;
    assert_eq!(errors, vec![ProcessorError::ChargebackExceedsBalance { client: 1, tx: 1, amount: 100, total: 20 }]);
    assert_eq!(client(&ledger, 1), Some(ClientSnapshot { id: 1, total: 20, held: 100, locked: false }));

    assert_eq!(run_all(&ledger, vec![deposit(1, 3, 80), chargeback(1, 1)]).await, vec![]);
    assert_eq!(client(&ledger, 1), Some(ClientSnapshot { id: 1, total: 0, held: 0, locked: true }));
}

#[tokio::test]
async fn frozen_client_refuses_all_five_operations() {
    let ledger = Ledger::new();
    run_all(&ledger, vec![
        deposit(1, 1, 100), deposit(1, 2, 50), dispute(1, 1), dispute(1, 2), chargeback(1, 1),
    ]).await;
    let before = client(&ledger, 1);
    assert_eq!(before, Some(ClientSnapshot { id: 1, total: 50, held: 50, locked: true }));

    let errors = run_all(&ledger, vec![
        deposit(1, 3, 1), withdrawal(1, 4, 1), dispute(1, 2), resolve(1, 2), chargeback(1, 2),
    ]).await;
    assert_eq!(errors, vec![
        ProcessorError::ClientFrozen { client: 1, tx: 3 },
        ProcessorError::ClientFrozen { client: 1, tx: 4 },
        ProcessorError::ClientFrozen { client: 1, tx: 2 },
        ProcessorError::ClientFrozen { client: 1, tx: 2 },
        ProcessorError::ClientFrozen { client: 1, tx: 2 },
    ]);
    assert_eq!(client(&ledger, 1), before);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test ledger`
Expected: FAIL, panics with `not implemented`.

- [ ] **Step 3: Implement `dispute`, `resolve` and `chargeback` in `src/ledger.rs`**

Follow the spec's numbered steps. In `dispute`, read the deposit's amount with `transactions.peek_with` before taking the client lock, and match only `Transaction::Deposit` with the same client. Share dispute lookup through a private `fn open_dispute(&self, client: u16, tx: u32) -> Result<u32, ProcessorError>` that returns the amount or `DisputeNotFound`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --test ledger`
Expected: PASS, 14 tests.

- [ ] **Step 5: Commit**

```bash
git add src tests
git commit -m "feat: add dispute, resolve and chargeback"
```

### Task 4: Concurrency guarantees

Implementation should already be correct, so these tests are expected to pass on first run. A failure here is a real bug in Tasks 2 and 3.

**Files:**
- Create: `tests/concurrency.rs`

**Interfaces:**
- Consumes: `Ledger`, `TransactionProcessor`, `run`, and the helpers from Task 2.

- [ ] **Step 1: Write the tests**

All use `#[tokio::test(flavor = "multi_thread", worker_threads = 8)]`, and each worker is a `tokio::spawn`, which also proves the futures are `Send`.

```rust
const TASKS: u32 = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposits_to_one_client_are_never_lost() {
    // Each of TASKS spawned tasks calls ledger.run over 10_000 deposits of 1 unit
    // to client 1, with tx = task << 16 | i.
    assert_eq!(client(&ledger, 1).unwrap().total, 80_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_withdrawals_never_overspend() {
    // Deposit 1_000 to client 1 with tx = u32::MAX. Each of TASKS spawned tasks
    // attempts 500 withdrawals of 1 unit, with tx = task << 16 | i. Count Ok
    // results across all tasks.
    assert_eq!(succeeded, 1_000);
    assert_eq!(client(&ledger, 1).unwrap().total, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leaf_splits_do_not_lose_updates() {
    // 5 rounds, each on a fresh ledger. Each of TASKS spawned tasks deposits 1
    // unit to 20_000 clients, walking ids as (i as u16).wrapping_mul(40503) so
    // they arrive unordered, with tx = task << 16 | i. First arrival creates
    // the client while other tasks update it, which splits leaves under load.
    assert_eq!(ledger.snapshot().len(), 20_000);
    assert!(ledger.snapshot().iter().all(|c| c.total == u64::from(TASKS)));
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --release --test concurrency`
Expected: PASS, 3 tests. A pass is weak evidence for `leaf_splits_do_not_lose_updates`, because the bug it guards against is rare. Reviewers must also confirm the `clients` value type by reading `src/ledger.rs`.

- [ ] **Step 3: Verify the no-await enforcement**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add tests
git commit -m "test: pin concurrency guarantees"
```

### Task 5: Pluggable output with CSV writer

**Files:**
- Create: `src/output.rs`
- Modify: `src/lib.rs`
- Test: inline `#[cfg(test)]` in `src/output.rs`

**Interfaces:**
- Consumes: `ClientSnapshot`, `format_amount`, `format_difference`.
- Produces, in `output`:
  - `pub trait ClientWriter { fn write(&mut self, client: &ClientSnapshot) -> impl Future<Output = io::Result<()>> + Send; fn finish(&mut self) -> impl Future<Output = io::Result<()>> + Send; }`
  - `pub struct CsvClientWriter<W>`, with `pub fn new(writer: W) -> Self` and `pub fn into_inner(self) -> W`, implementing `ClientWriter` for `W: AsyncWrite + Unpin + Send`

- [ ] **Step 1: Write the failing tests**

```rust
const HEADER: &str = "client,available,held,total,locked\n";

#[tokio::test]
async fn no_clients_still_writes_header() {
    let mut w = CsvClientWriter::new(Vec::new());
    w.finish().await.unwrap();
    assert_eq!(w.into_inner(), HEADER.as_bytes());
}

#[tokio::test]
async fn writes_four_decimals_and_negative_available() {
    let mut w = CsvClientWriter::new(Vec::new());
    w.write(&ClientSnapshot { id: 1, total: 15_000, held: 0, locked: false }).await.unwrap();
    w.write(&ClientSnapshot { id: 2, total: 200_000, held: 1_000_000, locked: true }).await.unwrap();
    w.finish().await.unwrap();
    let expected = format!("{HEADER}1,1.5000,0.0000,1.5000,false\n2,-80.0000,100.0000,20.0000,true\n");
    assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib output`
Expected: FAIL to compile, `CsvClientWriter` not found.

- [ ] **Step 3: Implement `src/output.rs`**

Track `header_written: bool`. Both `write` and `finish` emit the header if it is still pending. `finish` flushes the writer. Rows are numeric and boolean, so `format!` needs no CSV quoting.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib output`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat: add client writer with CSV output"
```

### Task 6: CSV input (phase 2)

**Files:**
- Create: `src/codec.rs`
- Modify: `src/lib.rs`
- Test: inline `#[cfg(test)]` in `src/codec.rs`

**Interfaces:**
- Consumes: `Transaction`, `parse_amount`, `AmountError`.
- Produces, in `codec`:
  - `#[derive(Debug, PartialEq, Eq)] pub enum ParseError { InvalidUtf8, UnknownType(String), MissingField(&'static str), InvalidClient(String), InvalidTx(String), InvalidAmount(AmountError) }`
  - `pub fn parse_line(line: &str) -> Result<Transaction, ParseError>`
  - `#[derive(Default)] pub struct TransactionCodec`, implementing `Decoder<Item = Result<Transaction, ParseError>, Error = io::Error>`
  - `#[derive(Debug)] pub enum InputError { Parse(ParseError), Io(io::Error) }`
  - `pub fn csv_transactions<R: AsyncRead>(reader: R) -> impl Stream<Item = Result<Transaction, InputError>>`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn parses_padded_and_short_rows() {
    let d = Transaction::Deposit { client: 1, tx: 2, amount: 10_000 };
    assert_eq!(parse_line("deposit,1,2,1.0"), Ok(d.clone()));
    assert_eq!(parse_line("  deposit , 1, 2, 1.0 "), Ok(d));
    let w = Transaction::Withdrawal { client: 1, tx: 2, amount: 5_000 };
    assert_eq!(parse_line("withdrawal,1,2,0.5"), Ok(w));
    let dispute = Transaction::Dispute { client: 1, tx: 2 };
    for row in ["dispute,1,2", "dispute,1,2,", "dispute,1,2,5.0"] {
        assert_eq!(parse_line(row), Ok(dispute.clone()), "{row:?}");
    }
    assert_eq!(parse_line("resolve,1,2"), Ok(Transaction::Resolve { client: 1, tx: 2 }));
    assert_eq!(parse_line("chargeback,1,2"), Ok(Transaction::Chargeback { client: 1, tx: 2 }));
}

#[test]
fn rejects_bad_rows() {
    assert_eq!(parse_line("refund,1,1,1.0"), Err(ParseError::UnknownType("refund".into())));
    assert_eq!(parse_line("Deposit,1,1,1.0"), Err(ParseError::UnknownType("Deposit".into())));
    assert_eq!(parse_line("deposit"), Err(ParseError::MissingField("client")));
    assert_eq!(parse_line("deposit,1"), Err(ParseError::MissingField("tx")));
    assert_eq!(parse_line("deposit,1,1"), Err(ParseError::MissingField("amount")));
    assert_eq!(parse_line("deposit,1,1,"), Err(ParseError::MissingField("amount")));
    assert_eq!(parse_line("deposit,70000,1,1.0"), Err(ParseError::InvalidClient("70000".into())));
    assert_eq!(parse_line("deposit,1,-1,1.0"), Err(ParseError::InvalidTx("-1".into())));
    assert_eq!(parse_line("deposit,1,1,1.23456"), Err(ParseError::InvalidAmount(AmountError::TooManyDecimals)));
}

#[tokio::test]
async fn skips_header_and_blank_lines_and_handles_crlf_and_missing_final_newline() {
    let input = &b"type,client,tx,amount\r\ndeposit,1,1,1.0\r\n\r\nwithdrawal,1,2,0.5"[..];
    let rows: Vec<_> = csv_transactions(input).collect().await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(Result::is_ok));
}

#[tokio::test]
async fn bad_rows_do_not_stop_the_stream() {
    let input = &b"type,client,tx,amount\ndeposit,1,1,1.0\nnonsense\n\xFF\xFE,1,2\ndeposit,1,3,1.0\n"[..];
    let rows: Vec<_> = csv_transactions(input).collect().await;
    assert_eq!(rows.len(), 4);
    assert!(rows[0].is_ok() && rows[3].is_ok());
    assert!(matches!(rows[1], Err(InputError::Parse(ParseError::UnknownType(_)))));
    assert!(matches!(rows[2], Err(InputError::Parse(ParseError::InvalidUtf8))));
}

#[test]
fn row_split_across_reads_decodes_once_complete() {
    let mut codec = TransactionCodec::default();
    let mut buf = BytesMut::from(&b"type,client,tx,amount\ndeposit,1,"[..]);
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
    buf.extend_from_slice(b"1,1.0\n");
    assert_eq!(codec.decode(&mut buf).unwrap(), Some(Ok(Transaction::Deposit { client: 1, tx: 1, amount: 10_000 })));
}

#[tokio::test]
async fn empty_input_yields_nothing() {
    assert_eq!(csv_transactions(&b""[..]).collect::<Vec<_>>().await.len(), 0);
}
```

Imports: `tokio_stream::StreamExt`, `tokio_util::bytes::BytesMut`, `tokio_util::codec::Decoder`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib codec`
Expected: FAIL to compile, `parse_line` not found.

- [ ] **Step 3: Implement `src/codec.rs`**

`parse_line`: `split(',')`, trim each field, match the type column exactly (case-sensitive). The amount column is read only for deposits and withdrawals.

`Decoder::decode` must loop: take the bytes through the next `\n`, and if the line is the header or blank, continue with the next line instead of returning `Ok(None)`. Returning `Ok(None)` while complete lines remain buffered would stall them.

`decode_eof`: try `decode` first. If it returns `None` and the buffer is non-empty, treat the remainder as a final line. Blank-line skipping must apply there too.

`csv_transactions`: `FramedRead::new(reader, TransactionCodec::default())`, then `.map` to flatten `Result<Result<_, ParseError>, io::Error>` into `Result<Transaction, InputError>`.

Never return a row problem as `Decoder::Error`. `FramedRead` ends the stream after any decoder error.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib codec`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat: add CSV transaction stream"
```

### Task 7: `cargo run` CLI (phase 2)

**Files:**
- Modify: `src/main.rs`
- Create: `tests/cli.rs`

**Interfaces:**
- Consumes: `Ledger`, `Ledger::run`, `csv_transactions`, `InputError`, `RunError`, `CsvClientWriter`, `ClientWriter`.

- [ ] **Step 1: Write the failing tests in `tests/cli.rs`**

Helpers: write the input under `env!("CARGO_TARGET_TMPDIR")` with a per-test file name, and run `Command::new(env!("CARGO_BIN_EXE_assignment"))`.

```rust
const HEADER: &str = "client,available,held,total,locked\n";

#[test]
fn reports_accounts_and_keeps_going_past_rejected_rows() {
    let out = run_on("reports.csv", "type,client,tx,amount\n\
        deposit,1,1,1.0\ndeposit,2,2,2.0\ndeposit,1,3,2.0\n\
        withdrawal,1,4,1.5\nwithdrawal,2,5,3.0\nnonsense\n");
    assert!(out.status.success());
    assert_eq!(stdout(&out), format!("{HEADER}1,1.5000,0.0000,1.5000,false\n2,2.0000,0.0000,2.0000,false\n"));
    assert!(!stderr(&out).is_empty());
}

#[test]
fn header_only_input_prints_header() {
    let out = run_on("header.csv", "type,client,tx,amount\n");
    assert!(out.status.success());
    assert_eq!(stdout(&out), HEADER);
}

#[test]
fn missing_file_exits_nonzero_with_no_report() {
    let out = Command::new(env!("CARGO_BIN_EXE_assignment")).arg("/no/such/file.csv").output().unwrap();
    assert!(!out.status.success());
    assert_eq!(stdout(&out), "");
}

#[test]
fn directory_input_exits_nonzero_with_no_report() {
    let out = Command::new(env!("CARGO_BIN_EXE_assignment")).arg(env!("CARGO_TARGET_TMPDIR")).output().unwrap();
    assert!(!out.status.success());
    assert_eq!(stdout(&out), "");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test cli`
Expected: FAIL, all 4 tests, because `main` still prints `Hello, world!` and exits 0.

- [ ] **Step 3: Implement `src/main.rs`**

`#[derive(clap::Parser)] struct Args { input: PathBuf }` and `#[tokio::main] async fn main() -> ExitCode`.

Open with `tokio::fs::File::open`, pass `csv_transactions` to `Ledger::run`, and print every `RunError` to stderr. `RunError::Input(InputError::Io(_))` sets a failure flag. If the flag is set, return `ExitCode::FAILURE` before writing anything.

Otherwise write `ledger.snapshot()` through `CsvClientWriter::new(BufWriter::new(tokio::io::stdout()))`, call `finish`, and return `ExitCode::FAILURE` on any write error.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --test cli`
Expected: PASS, 4 tests.

- [ ] **Step 5: Verify the whole crate**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: all exit 0.

- [ ] **Step 6: Commit**

```bash
git add src tests
git commit -m "feat: add CSV command line interface"
```
