use tokio_stream::{Stream, StreamExt};
use tracing::{field, trace};

use crate::{
    amount::format_amount,
    ledger::Ledger,
    processor::{ProcessorError, TransactionProcessor},
    transaction::Transaction,
};

#[derive(Debug, PartialEq, Eq)]
pub enum RunError<E> {
    /// The stream produced an error instead of a transaction.
    Input(E),
    /// The ledger refused a transaction.
    Rejected(ProcessorError),
}

impl Ledger {
    /// Applies `input` in order, one transaction at a time, until it ends.
    ///
    /// Failures go to `on_error` and never stop the run.
    pub async fn run<S, E, F>(&self, input: S, mut on_error: F)
    where
        S: Stream<Item = Result<Transaction, E>>,
        F: FnMut(RunError<E>),
    {
        let mut input = std::pin::pin!(input);
        while let Some(item) = input.next().await {
            match item {
                Ok(transaction) => {
                    if let Err(error) = self.apply(transaction).await {
                        on_error(RunError::Rejected(error));
                    }
                }
                Err(error) => on_error(RunError::Input(error)),
            }
        }
    }

    /// Applies one transaction and logs its outcome, as the audit trail of the stream.
    ///
    /// The event fires after the client lock is released, because critical sections do no
    /// I/O. So streams racing on one client can log in a different order than they applied.
    async fn apply(&self, transaction: Transaction) -> Result<(), ProcessorError> {
        let result = match transaction {
            Transaction::Deposit { client, tx, amount } => self.deposit(client, tx, amount).await,
            Transaction::Withdrawal { client, tx, amount } => {
                self.withdraw(client, tx, amount).await
            }
            Transaction::Dispute { client, tx } => self.dispute(client, tx).await,
            Transaction::Resolve { client, tx } => self.resolve(client, tx).await,
            Transaction::Chargeback { client, tx } => self.chargeback(client, tx).await,
        };
        let (kind, client, tx, amount) = transaction.parts();
        let outcome = if result.is_ok() {
            "applied"
        } else {
            "rejected"
        };
        trace!(
            kind,
            client,
            tx,
            amount = amount.map(|units| field::display(format_amount(u64::from(units)))),
            outcome,
            error = result.as_ref().err().map(field::debug),
            "transaction processed"
        );
        result
    }
}
