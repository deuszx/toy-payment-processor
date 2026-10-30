# Toy Transaction Processor: Design

Date: 2026-10-30

Decisions marked **(my call)** were made by the author of this document and not by the project owner. Everything else was decided by the project owner in conversation.

## Intent

A toy transaction processor for an exchange. It applies deposits, withdrawals, disputes, resolves and chargebacks to client accounts and reports the final account state.

The core must not know where transactions come from or how results are presented. CSV is the first input and output format, not a built-in assumption. Later sources (network, channels) and output formats should plug in without changing the core.

## Goals

- Core engine consumes an async stream of transactions from any source.
- Safe when several streams run concurrently against one ledger.
- Output format is pluggable. CSV is the first format.
- Amounts are fixed-point with 4 decimals, parsed and printed without floats.
- `cargo run` reads a CSV file. This is the later phase of the build (see Build order).

## Non-goals

- Persistence. All state is in memory and unbounded, up to 2^32 transaction records.
- Multiple currencies, order matching, authentication.
- Reverting or unfreezing a frozen client.

## Anti-goals

These make the work a failure even if it appears to run.

- A lock held across `.await`.
- Lost balance updates under concurrency.
- A repeated dispute that holds funds twice.
- A malformed input row that stops the rest of the stream from being processed.
- Core code that depends on CSV, files, or bytes.

## Constraints

- Rust edition 2024, on the existing crate.
- tokio is the runtime.
- Concurrent updates to the same client are not allowed. Each client has its own lock, and a caller that finds it taken waits for it to be released, then continues.
- No lock is held across `.await`.
- Tx ids in the input are guaranteed unique, so duplicates are not checked for. If a source broke this guarantee, a repeated deposit would be counted twice.
- Only one stream's order is guaranteed. Each stream is processed in order, one transaction at a time. When separate streams touch the same client, the order is whichever takes the client lock first.

## Approaches

Three ways of getting input into the core were considered.

**A. `Stream` in the core, `AsyncRead` at the edge (chosen).** The core takes `Stream<Item = Result<Transaction, E>>`. `tokio_util::codec::FramedRead<R: AsyncRead, TransactionCodec>` is a `Stream`, so `File`, `TcpStream` and stdin all arrive through one codec, while channels and test vectors plug in directly. A new input format is a new codec. Costs two new dependencies, `tokio-util` and `tokio-stream`.

**B. `AsyncRead` in the core.** Rejected. The core would own framing, and every source would first have to become bytes, so structured sources would be re-encoded.

**C. Own `TransactionSource` trait.** Rejected. It reinvents `Stream`, loses combinators such as `select`, and every adapter is hand-written.

The owner chose A, with the added requirement that no lock is held across `.await`.

## Data model

### Client

```rust
pub struct Client {
    id: u16,
    total: u64,  // gross funds, including held
    held: u64,
    locked: bool,
}
```

The owner decided that `held` is subtracted from `available` rather than `available` being decremented on dispute. So the stored fields are `total` and `held`, and `available` is derived:

```
available = total - held    // may be negative while a dispute is open
```

`total` never goes below 0, because a chargeback that would do so is refused.

Reported `available` may be negative, so it needs a signed representation. It is not stored. `total` and `held` can each be larger than `i64::MAX` in principle, so formatting should work from sign and magnitude rather than casting to `i64` **(my call)**.

### Amounts

Transaction amounts stay `u32`, in units of 0.0001. The largest single transaction is 429,496.7295.

Balances are `u64`. Plain addition on them cannot overflow: tx ids are unique `u32`s, so at most 2^32 transactions exist, each below 2^32, giving a sum below 2^64 **(my call)**.

### Ledger

`State` in `src/db.rs` is renamed `Ledger` **(my call)**.

```rust
pub struct Ledger {
    clients: scc::TreeIndex<u16, Arc<parking_lot::Mutex<Client>>>,
    transactions: scc::TreeIndex<u32, Transaction>,
    disputes: scc::TreeIndex<u32, Dispute>,
}
```

- `clients` entries are never removed.
- `transactions` holds every applied deposit and withdrawal. Tx ids are globally unique.
- `disputes` holds currently open disputes only, keyed by tx.

**The `Arc` around each client's `Mutex` is required.** A `TreeIndex` leaf split moves values with a bitwise copy (`snapshot(v)` in `scc-3.8.8/src/tree_index/leaf_node.rs`). A reference from `peek` stays memory-safe but can point at a dead copy, so with `Mutex<Client>` as the value, two tasks could lock different mutexes for one client. A review probe of `TreeIndex<_, Mutex<u64>>` lost 5 of 3.2M increments this way. The same probe with `Arc<Mutex<u64>>` lost none in 16M.

Rule: any value in a `TreeIndex` is either immutable or reached through an `Arc`. `transactions` and `disputes` values are immutable. To reach a client, clone the `Arc` out of the index, then lock it.

Share the ledger itself as `Arc<Ledger>`.

## Locking

- Use `parking_lot::Mutex`, not `std::sync::Mutex` or `tokio::sync::Mutex`. Critical sections are short and contain no I/O and no `.await`, so a waiter blocks its tokio worker only briefly.
- `parking_lot` over std is the owner's decision. Measured on the development Mac (macOS, 14 cores) with 8 threads, the lookup-and-update path took about 10 ns/op against 15 ns/op for std, uncontended lock about 2 ns against 4 ns, and one hot client about 7 ns against 12 ns. Linux was not measured. It has no lock poisoning, so there is nothing to unwrap.
- Inside a critical section, use only the `_sync` map operations (`insert_sync`, `remove_sync`, `read_sync`, `peek`).
- Never hold two client locks at once. Map operations never take a client lock, so no lock cycle exists.
- A client lock covers all map writes for that client's operation. So a concurrent operation on the same client cannot observe a half-applied change.

The no-await rule is enforced by the compiler, not by review:

- Processor futures are declared `Send`. `parking_lot::MutexGuard` and `scc::Guard` are both `!Send`, so holding either across `.await` fails to compile. This relies on the `parking_lot` feature `send_guard` staying off.
- The crate root sets `#![deny(clippy::await_holding_lock)]`. This lint catches `parking_lot` guards too.

Consequence: `Ledger`'s methods contain no `.await`. The trait stays async because it is the contract for slower backends. The in-memory implementation completes without yielding.

## Processor interface

`TransactionProcessor` keeps its five methods. Each is declared so its future is `Send`, because `async fn` in a trait does not promise that and generic code could not then `tokio::spawn` it:

```rust
fn deposit(&self, client: u16, tx: u32, amount: u32)
    -> impl Future<Output = Result<(), ProcessorError>> + Send;
```

Its doc comments are updated to match the rules below, including freezing.

`ProcessorError` gains these variants **(my call on naming)**:

- `ClientFrozen { client, tx }`
- `ChargebackExceedsBalance { client, tx, amount, total }`

`InsufficientFunds.available` is a `u32` today and becomes a `u64`. It reports 0 when `held` exceeds `total` **(my call)**.

## Operation rules

"Client lock" below always means the per-client mutex, and taking it waits until it is free. It is unrelated to `client.locked`, the permanent frozen flag.

All five operations return `ClientFrozen` for a frozen client. "No more actions" includes `resolve` and `chargeback`, so other open disputes on a frozen client stay held forever.

### deposit

1. Get or create the client. Two concurrent creators are safe: the loser of `insert_sync` uses the existing entry.
2. Take the client lock. If `client.locked`, return `ClientFrozen`.
3. `transactions.insert_sync`. Tx ids are unique, so this cannot collide. `debug_assert!` that it succeeded.
4. `total += amount`.

The tx record is written under the client lock, so a dispute can never act on a deposit that has not been applied **(my call)**.

### withdraw

1. Find the client, or return `ClientNotFound`.
2. Take the client lock. If `client.locked`, return `ClientFrozen`.
3. If `available < amount`, return `InsufficientFunds`.
4. `transactions.insert_sync`, with the same `debug_assert!` as deposit.
5. `total -= amount`.

### dispute

1. Read `tx` from `transactions`. Return `TxnNotFound` if it is missing, belongs to another client, or is not a deposit.
2. Find the client, or return `ClientNotFound`.
3. Take the client lock. If `client.locked`, return `ClientFrozen`.
4. `disputes.insert_sync`. If a dispute is already open, return `Ok(())`. This is the idempotent case and must not hold funds again.
5. `held += amount`.

Disputes may make `available` negative. This is accepted.

### resolve

1. Find the client, or return `ClientNotFound`.
2. Take the client lock. If `client.locked`, return `ClientFrozen`.
3. Read the dispute. If it is missing or belongs to another client, return `DisputeNotFound`.
4. Remove the dispute. `held -= amount`.

After a resolve the transaction can be disputed again **(my call)**. `disputes` tracks open disputes only, and re-opening is money-neutral.

### chargeback

1. Find the client, or return `ClientNotFound`.
2. Take the client lock. If `client.locked`, return `ClientFrozen`.
3. Read the dispute. If it is missing or belongs to another client, return `DisputeNotFound`.
4. If `total < amount`, return `ChargebackExceedsBalance`. The dispute stays open and the client is not frozen, so it can still resolve or be retried after a later deposit **(my call)**.
5. Remove the dispute. `held -= amount`, `total -= amount`, `locked = true`.

## Input

`Ledger` gains a stream runner:

```rust
pub async fn run<S, E, F>(&self, input: S, on_error: F)
where
    S: Stream<Item = Result<Transaction, E>>,
    F: FnMut(RunError<E>);
```

`RunError<E>` is either the stream's `E` or a `ProcessorError`. Both are reported through `on_error` and processing continues until the stream returns `None`. `run` never stops on a rejected transaction, because "ignored" in the trait docs means the caller treats those errors as non-fatal.

Concurrency across sources is `tokio::spawn` of one `run` per stream on a shared `Arc<Ledger>`.

### CSV input

`TransactionCodec` implements `tokio_util::codec::Decoder`.

**Important:** `FramedRead` stops the stream after any `Decoder` error. So a malformed row must not be a `Decoder::Error`. Use `Decoder::Item = Result<Transaction, ParseError>` and `Decoder::Error = io::Error`, then flatten to one error type before passing the stream to `run`.

Rules:

- Columns are `type, client, tx, amount`.
- The first line is a header and is skipped **(my call)**.
- Blank lines are skipped, surrounding whitespace is trimmed, and CRLF is accepted **(my call)**.
- Amounts are parsed with integer arithmetic, using `AMOUNT_DECIMALS`. Never through a float.
- A row with more than 4 decimals, a negative amount, or an amount above `u32` capacity is a `ParseError` **(my call)**.
- A deposit or withdrawal without an amount is a `ParseError` **(my call)**. Dispute, resolve and chargeback rows have no amount, so a value in that column is a `ParseError`.
- Any non-empty field past a row's last column is a `ParseError`, and trailing blank fields are tolerated, so `dispute,1,1,` parses **(my call)**.
- `decode_eof` must handle a final line with no newline.

## Output

```rust
pub trait ClientWriter {
    fn write(&mut self, client: &ClientSnapshot)
        -> impl Future<Output = io::Result<()>> + Send;
    fn finish(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
```

`CsvClientWriter<W: AsyncWrite + Unpin + Send>` is the first implementation. It writes the header `client,available,held,total,locked` **(my call)**, with amounts to exactly 4 decimals, such as `1.5000` and `-80.0000`.

`ClientSnapshot` is a plain copy of `id`, `total`, `held`, `locked`.

`Ledger::snapshot(&self) -> impl Iterator<Item = ClientSnapshot> + Send` replaces the "async walker" TODO. `scc::Guard` is `!Send` and cannot cross an `.await`, so it takes a guard only long enough to clone every client's `Arc` into a `Vec`, bounded at 65,536 entries by the `u16` client id. The rest is lazy: each pull locks one client, copies it and returns the copy. So the iterator can be held across `.await` and interleaved with async writes.

Each row is internally consistent and reflects the moment it was pulled. The set of clients is fixed when `snapshot` is called, each client appears once, and row order is not part of the contract. A walk made while streams are still running is not consistent across clients. The CLI walks after all streams finish.

## CLI

`cargo run -- transactions.csv > accounts.csv` **(my call on shape)**. Opens the file with `tokio::fs`, wraps it in `FramedRead`, runs one stream, snapshots, and writes CSV to stdout.

Row-level failures go to stderr and the exit code stays 0. An I/O failure on input or output exits non-zero, and an input failure writes no report to stdout, so a partial file can't pass for a complete one **(my call)**.

`cargo run --bin generate -- --clients 1000 > transactions.csv` writes a sample input to stdout **(my call on shape)**. `--clients` defaults to `u16::MAX`, with ids from 1. Each client gets 4 rows from one of four fixed scripts, so the file depends only on the arguments. The scripts are built so the ledger accepts every row: tx ids are unique, a withdrawal never exceeds the deposit that funds it, and a chargeback is a client's last row. A run over the file exits 0 with nothing on stderr.

`Cargo.toml` sets `default-run = "assignment"`, because a second binary would otherwise make `cargo run` ambiguous.

## Dependencies

Add `tokio`, `tokio-util` (feature `codec`), `tokio-stream`, `parking_lot`. `tokio-stream` re-exports the same `Stream` trait as `futures-core` and adds the `next` and `map` combinators that `run` and the CSV adapter need, and `iter` for test streams **(my call)**.

Remove `papaya`, which is unused. Keep `clap`, `serde` and `csv` only if the implementation uses them.

## Tests

- Ledger scenarios driven by in-memory streams, for each rule above.
- The chargeback path: deposit 100, withdraw 80, dispute, chargeback is refused, dispute stays open, client not frozen, `available` reports -80.
- Repeated dispute holds funds once.
- Frozen client refuses all five operations.
- Concurrency on a multi-thread runtime: many tasks on one client, final `total` is exact.
- Leaf splits under load: create many clients in shuffled id order while other tasks update existing clients, then check every `total`. One-client tests never split a leaf (a leaf holds 15 entries), and ascending ids dodged the split in the review probe. A green run proves little on its own, because the failure rate without `Arc` is about 1 in 600,000 updates.
- Codec: row split across read chunks, no trailing newline, malformed row does not stop the stream.
- Amount parsing and formatting, including 4 decimals and negative `available`.

## Build order

1. Core: `Client`, `Ledger`, trait, `run`, `ClientWriter`, `CsvClientWriter`, tests over in-memory streams.
2. Later: `TransactionCodec` and `cargo run`.

## Left to the builder

- Whether `src/db.rs` is renamed to match `Ledger`.
- Adding `src/lib.rs` so integration tests can reach the core.
- Whether the row parser uses the `csv` crate or manual splitting.
- Exact shape of `RunError<E>` and `ParseError`.
