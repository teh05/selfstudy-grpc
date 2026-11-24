use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::broadcast;
use tonic::Status;

use crate::pb::TransferEvent;

const BROADCAST_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct Ledger {
    inner: std::sync::Arc<Inner>,
}

struct Inner {
    balances: Mutex<HashMap<String, i64>>,
    balance_tx: broadcast::Sender<(String, i64)>,
    event_tx: broadcast::Sender<TransferEvent>,
}

impl Ledger {
    pub fn with_accounts(accounts: impl IntoIterator<Item = (impl Into<String>, i64)>) -> Self {
        let balances = accounts
            .into_iter()
            .map(|(id, bal)| (id.into(), bal))
            .collect();

        let (balance_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (event_tx, _) = broadcast::channel(BROADCAST_CAPACITY);

        Self {
            inner: std::sync::Arc::new(Inner {
                balances: Mutex::new(balances),
                balance_tx,
                event_tx,
            }),
        }
    }

    pub fn get_balance(&self, account_id: &str) -> Result<i64, Status> {
        self.inner
            .balances
            .lock()
            .expect("ledger lock")
            .get(account_id)
            .copied()
            .ok_or_else(|| Status::not_found(format!("account `{account_id}` not found")))
    }

    pub fn transfer(&self, from: &str, to: &str, amount: i64) -> Result<(), Status> {
        if amount <= 0 {
            return Err(Status::invalid_argument("amount must be > 0"));
        }
        if from == to {
            return Err(Status::invalid_argument("from and to must differ"));
        }

        let mut balances = self.inner.balances.lock().expect("ledger lock");

        let from_bal = balances
            .get(from)
            .copied()
            .ok_or_else(|| Status::not_found(format!("account `{from}` not found")))?;
        if !balances.contains_key(to) {
            return Err(Status::not_found(format!("account `{to}` not found")));
        }
        if from_bal < amount {
            return Err(Status::failed_precondition(format!(
                "insufficient funds on `{from}` (have {from_bal}, need {amount})"
            )));
        }

        let new_from = from_bal - amount;
        let new_to = balances[to] + amount;
        balances.insert(from.to_string(), new_from);
        balances.insert(to.to_string(), new_to);
        drop(balances);

        let _ = self.inner.balance_tx.send((from.to_string(), new_from));
        let _ = self.inner.balance_tx.send((to.to_string(), new_to));
        Ok(())
    }

    pub fn subscribe_balances(&self) -> broadcast::Receiver<(String, i64)> {
        self.inner.balance_tx.subscribe()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<TransferEvent> {
        self.inner.event_tx.subscribe()
    }

    pub fn publish_event(&self, event: TransferEvent) {
        let _ = self.inner.event_tx.send(event);
    }
}
