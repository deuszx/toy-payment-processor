#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transaction {
    Deposit { client: u16, tx: u32, amount: u32 },
    Withdrawal { client: u16, tx: u32, amount: u32 },
    Dispute { client: u16, tx: u32 },
    Resolve { client: u16, tx: u32 },
    Chargeback { client: u16, tx: u32 },
}

impl Transaction {
    /// Returns the type name, client, tx and, for deposits and withdrawals, the amount.
    pub(crate) fn parts(&self) -> (&'static str, u16, u32, Option<u32>) {
        match *self {
            Transaction::Deposit { client, tx, amount } => ("deposit", client, tx, Some(amount)),
            Transaction::Withdrawal { client, tx, amount } => {
                ("withdrawal", client, tx, Some(amount))
            }
            Transaction::Dispute { client, tx } => ("dispute", client, tx, None),
            Transaction::Resolve { client, tx } => ("resolve", client, tx, None),
            Transaction::Chargeback { client, tx } => ("chargeback", client, tx, None),
        }
    }
}
