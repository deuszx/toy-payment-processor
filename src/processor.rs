/// All amounts are in units of 0.0001.
///
/// "Ignored" below means the operation changes nothing and returns an error.
/// Callers treat those errors as non-fatal and carry on with the next transaction.
pub trait TransactionProcessor {
    /// Deposits `amount` to `client`.
    ///
    /// Increases client's `available` and `total` balances.
    ///
    /// If client doesn't exist yet – create it.
    /// If client is frozen, error.
    fn deposit(
        &self,
        client: u16,
        tx: u32,
        amount: u32,
    ) -> impl Future<Output = Result<(), ProcessorError>> + Send;

    /// Withdraws `amount` from the `client`.
    ///
    /// Decreases `available` and `total` balances.
    ///
    /// If client doesn't exist, error.
    /// If client is frozen or `available` is less than `amount`, error.
    fn withdraw(
        &self,
        client: u16,
        tx: u32,
        amount: u32,
    ) -> impl Future<Output = Result<(), ProcessorError>> + Send;

    /// Opens a dispute for the `client` regarding transaction id `tx`.
    ///
    /// Decreases `available` balance, increases `held`.
    ///
    /// Ignored if corresponding transaction `tx` or `client` doesn't exist.
    /// Ignored if client is frozen.
    /// A no-op if a dispute for `tx` is already open, so funds are held only once.
    ///
    /// ## Assumptions:
    /// 1. Only `deposit` transactions can be disputed.
    /// 2. If `tx` doesn't exist, it's safe to ignore.
    /// 3. It's acceptable to open a dispute even if disputing the transaction means the available balance becomes negative.
    fn dispute(
        &self,
        client: u16,
        tx: u32,
    ) -> impl Future<Output = Result<(), ProcessorError>> + Send;

    /// Resolves a dispute for the `client` regarding transaction id `tx`.
    ///
    /// Decreases `held`, increases `available` balances.
    ///
    /// Ignored if corresponding transaction `tx` or `client` doesn't exist.
    /// Ignored if there is no dispute for the `(client, tx)`.
    /// Ignored if client is frozen.
    fn resolve(
        &self,
        client: u16,
        tx: u32,
    ) -> impl Future<Output = Result<(), ProcessorError>> + Send;

    /// Issues a chargeback for the `client` for the transaction `tx`.
    ///
    /// Decreases `held` and `total` balances, and freezes the client.
    ///
    /// Ignored if corresponding `tx` or `client` doesn't exist.
    /// Ignored if there is no dispute for the `(client, tx)`.
    /// Ignored if client is frozen.
    /// Refused if `total` is less than the disputed amount. The dispute stays open
    /// and the client isn't frozen, so it can resolve or be retried after a deposit.
    fn chargeback(
        &self,
        client: u16,
        tx: u32,
    ) -> impl Future<Output = Result<(), ProcessorError>> + Send;
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessorError {
    ClientNotFound {
        client: u16,
        tx: u32,
    },
    ClientFrozen {
        client: u16,
        tx: u32,
    },
    InsufficientFunds {
        client: u16,
        tx: u32,
        amount: u32,
        available: u64,
    },
    TxnNotFound {
        client: u16,
        tx: u32,
    },
    DisputeNotFound {
        client: u16,
        tx: u32,
    },
    ChargebackExceedsBalance {
        client: u16,
        tx: u32,
        amount: u32,
        total: u64,
    },
}
