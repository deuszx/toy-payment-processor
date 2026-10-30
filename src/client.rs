use std::fmt::{self, Display, Formatter};

use crate::amount::{format_amount, format_difference};

pub struct Client {
    id: u16,
    // Total amount of funds, including held.
    total: u64,
    // Total amount held for disputes.
    held: u64,
    // Whether account is frozen.
    locked: bool,
}

/// Plain copy of a client's state, used for reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientSnapshot {
    pub id: u16,
    pub total: u64,
    pub held: u64,
    pub locked: bool,
}

impl Client {
    pub fn new(id: u16) -> Self {
        Client {
            id,
            total: 0,
            held: 0,
            locked: false,
        }
    }

    // Returns total amount of funds, available and held.
    pub fn total(&self) -> u64 {
        self.total
    }

    // Returns whether account is frozen.
    pub fn is_locked(&self) -> bool {
        self.locked
    }

    // Returns funds that can be withdrawn. Zero while `held` exceeds `total`.
    pub fn spendable(&self) -> u64 {
        self.total.saturating_sub(self.held)
    }

    pub fn snapshot(&self) -> ClientSnapshot {
        ClientSnapshot {
            id: self.id,
            total: self.total,
            held: self.held,
            locked: self.locked,
        }
    }

    pub(crate) fn credit(&mut self, amount: u32) {
        self.total += u64::from(amount);
    }

    pub(crate) fn debit(&mut self, amount: u32) {
        self.total -= u64::from(amount);
    }

    pub(crate) fn hold(&mut self, amount: u32) {
        self.held += u64::from(amount);
    }

    pub(crate) fn release(&mut self, amount: u32) {
        self.held -= u64::from(amount);
    }

    // Removes disputed funds and freezes the account.
    pub(crate) fn charge_back(&mut self, amount: u32) {
        self.release(amount);
        self.debit(amount);
        self.locked = true;
    }
}

impl Display for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Client(id: {}, locked: {}, available: {}, held: {}, total: {})",
            self.id,
            self.locked,
            format_difference(self.total, self.held),
            format_amount(self.held),
            format_amount(self.total)
        )
    }
}
