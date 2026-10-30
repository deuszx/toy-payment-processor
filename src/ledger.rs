use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    client::{Client, ClientSnapshot},
    processor::{ProcessorError, TransactionProcessor},
    transaction::Transaction,
};

#[derive(Default)]
pub struct Ledger {
    /// Clients' states.
    ///
    /// The `Arc` is required. `TreeIndex` moves values by bitwise copy when a leaf
    /// splits, so a mutex borrowed from the index can be a dead copy. Clone the `Arc`
    /// out of the index, then lock it.
    clients: scc::TreeIndex<u16, Arc<Mutex<Client>>>,
    /// Record of all deposits and withdrawals processed.
    /// For accountability and verification purposes.
    transactions: scc::TreeIndex<u32, Transaction>,
    /// Currently-opened disputes. Keyed by the transaction being disputed.
    disputes: scc::TreeIndex<u32, Dispute>,
}

struct Dispute {
    client: u16,
    amount: u32,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an iterator over every client, in no promised order.
    ///
    /// Each pull locks one client and copies it, so nothing stays locked between pulls.
    /// No `scc::Guard` is kept, so the iterator can be held across `.await`.
    ///
    /// The set of clients is fixed by this call. Each row is consistent. The rows aren't
    /// consistent with each other while streams are still running.
    pub fn snapshot(&self) -> impl Iterator<Item = ClientSnapshot> + Send {
        let guard = scc::Guard::new();
        let clients: Vec<_> = self
            .clients
            .iter(&guard)
            .map(|(_, client)| client.clone())
            .collect();
        clients.into_iter().map(|client| client.lock().snapshot())
    }

    fn client(&self, id: u16) -> Option<Arc<Mutex<Client>>> {
        self.clients.peek_with(&id, |_, client| client.clone())
    }

    fn client_or_create(&self, id: u16) -> Arc<Mutex<Client>> {
        if let Some(client) = self.client(id) {
            return client;
        }
        let client = Arc::new(Mutex::new(Client::new(id)));
        match self.clients.insert_sync(id, client.clone()) {
            Ok(()) => client,
            Err(_) => self.client(id).expect("clients are never removed"),
        }
    }

    /// Records an applied transaction. Must be called under the client lock.
    fn record(&self, tx: u32, transaction: Transaction) {
        let recorded = self.transactions.insert_sync(tx, transaction);
        debug_assert!(recorded.is_ok(), "tx ids are unique");
    }

    /// Returns the amount of the open dispute for `(client, tx)`.
    fn open_dispute(&self, client: u16, tx: u32) -> Result<u32, ProcessorError> {
        self.disputes
            .peek_with(&tx, |_, dispute| {
                (dispute.client == client).then_some(dispute.amount)
            })
            .flatten()
            .ok_or(ProcessorError::DisputeNotFound { client, tx })
    }
}

impl TransactionProcessor for Ledger {
    async fn deposit(&self, client: u16, tx: u32, amount: u32) -> Result<(), ProcessorError> {
        let account = self.client_or_create(client);
        let mut account = account.lock();
        ensure_active(&account, client, tx)?;
        self.record(tx, Transaction::Deposit { client, tx, amount });
        account.credit(amount);
        Ok(())
    }

    async fn withdraw(&self, client: u16, tx: u32, amount: u32) -> Result<(), ProcessorError> {
        let account = self
            .client(client)
            .ok_or(ProcessorError::ClientNotFound { client, tx })?;
        let mut account = account.lock();
        ensure_active(&account, client, tx)?;
        let available = account.spendable();
        if available < u64::from(amount) {
            return Err(ProcessorError::InsufficientFunds {
                client,
                tx,
                amount,
                available,
            });
        }
        self.record(tx, Transaction::Withdrawal { client, tx, amount });
        account.debit(amount);
        Ok(())
    }

    async fn dispute(&self, client: u16, tx: u32) -> Result<(), ProcessorError> {
        let amount = self
            .transactions
            .peek_with(&tx, |_, transaction| match transaction {
                Transaction::Deposit {
                    client: owner,
                    amount,
                    ..
                } if *owner == client => Some(*amount),
                _ => None,
            })
            .flatten()
            .ok_or(ProcessorError::TxnNotFound { client, tx })?;
        let account = self
            .client(client)
            .ok_or(ProcessorError::ClientNotFound { client, tx })?;
        let mut account = account.lock();
        ensure_active(&account, client, tx)?;
        if self
            .disputes
            .insert_sync(tx, Dispute { client, amount })
            .is_ok()
        {
            account.hold(amount);
        }
        Ok(())
    }

    async fn resolve(&self, client: u16, tx: u32) -> Result<(), ProcessorError> {
        let account = self
            .client(client)
            .ok_or(ProcessorError::ClientNotFound { client, tx })?;
        let mut account = account.lock();
        ensure_active(&account, client, tx)?;
        let amount = self.open_dispute(client, tx)?;
        self.disputes.remove_sync(&tx);
        account.release(amount);
        Ok(())
    }

    async fn chargeback(&self, client: u16, tx: u32) -> Result<(), ProcessorError> {
        let account = self
            .client(client)
            .ok_or(ProcessorError::ClientNotFound { client, tx })?;
        let mut account = account.lock();
        ensure_active(&account, client, tx)?;
        let amount = self.open_dispute(client, tx)?;
        let total = account.total();
        if total < u64::from(amount) {
            return Err(ProcessorError::ChargebackExceedsBalance {
                client,
                tx,
                amount,
                total,
            });
        }
        self.disputes.remove_sync(&tx);
        account.charge_back(amount);
        Ok(())
    }
}

fn ensure_active(account: &Client, client: u16, tx: u32) -> Result<(), ProcessorError> {
    if account.is_locked() {
        return Err(ProcessorError::ClientFrozen { client, tx });
    }
    Ok(())
}
