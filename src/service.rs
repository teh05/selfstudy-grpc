use std::pin::Pin;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::pb::bank_server::Bank;
use crate::pb::{
    BalanceResponse, BalanceUpdate, BatchTransferResponse, GetBalanceRequest, TransferEvent,
    TransferRequest, TransferResult, WatchBalanceRequest,
};
use crate::Ledger;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct BankService {
    ledger: Ledger,
}

impl BankService {
    pub fn new(ledger: Ledger) -> Self {
        Self { ledger }
    }

    fn apply_transfer(&self, from: &str, to: &str, amount: i64) -> TransferResult {
        let (ok, message) = match self.ledger.transfer(from, to, amount) {
            Ok(()) => (true, "ok".into()),
            Err(status) => (false, status.message().into()),
        };
        TransferResult {
            ok,
            message,
            from_account: from.into(),
            to_account: to.into(),
            amount,
        }
    }
}

#[tonic::async_trait]
impl Bank for BankService {
    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<BalanceResponse>, Status> {
        let account_id = request.into_inner().account_id;
        let balance = self.ledger.get_balance(&account_id)?;
        Ok(Response::new(BalanceResponse {
            account_id,
            balance,
        }))
    }

    type WatchBalanceStream = BoxStream<BalanceUpdate>;

    async fn watch_balance(
        &self,
        request: Request<WatchBalanceRequest>,
    ) -> Result<Response<Self::WatchBalanceStream>, Status> {
        let WatchBalanceRequest {
            account_id,
            max_updates,
        } = request.into_inner();

        // Validate account exists before opening the stream.
        let initial = self.ledger.get_balance(&account_id)?;
        let mut balance_rx = self.ledger.subscribe_balances();
        let ledger = self.ledger.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let mut sequence: u64 = 1;
            let send = |seq: u64, bal: i64| BalanceUpdate {
                account_id: account_id.clone(),
                balance: bal,
                sequence: seq,
            };

            if tx.send(Ok(send(sequence, initial))).await.is_err() {
                return;
            }

            let mut sent = 1u32;
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.tick().await; // skip immediate first tick

            loop {
                if max_updates > 0 && sent >= max_updates {
                    break;
                }

                tokio::select! {
                    _ = ticker.tick() => {
                        match ledger.get_balance(&account_id) {
                            Ok(bal) => {
                                sequence += 1;
                                if tx.send(Ok(send(sequence, bal))).await.is_err() {
                                    break;
                                }
                                sent += 1;
                            }
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                break;
                            }
                        }
                    }
                    changed = balance_rx.recv() => {
                        match changed {
                            Ok((id, bal)) if id == account_id => {
                                sequence += 1;
                                if tx.send(Ok(send(sequence, bal))).await.is_err() {
                                    break;
                                }
                                sent += 1;
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn batch_transfer(
        &self,
        request: Request<Streaming<TransferRequest>>,
    ) -> Result<Response<BatchTransferResponse>, Status> {
        let mut stream = request.into_inner();
        let mut results = Vec::new();
        let mut succeeded = 0u32;
        let mut failed = 0u32;

        while let Some(item) = stream.next().await {
            let TransferRequest {
                from_account,
                to_account,
                amount,
            } = item?;

            let result = self.apply_transfer(&from_account, &to_account, amount);
            if result.ok {
                succeeded += 1;
            } else {
                failed += 1;
            }
            results.push(result);
        }

        Ok(Response::new(BatchTransferResponse {
            succeeded,
            failed,
            results,
        }))
    }

    type LiveTransferFeedStream = BoxStream<TransferEvent>;

    async fn live_transfer_feed(
        &self,
        request: Request<Streaming<TransferEvent>>,
    ) -> Result<Response<Self::LiveTransferFeedStream>, Status> {
        let mut inbound = request.into_inner();
        let mut event_rx = self.ledger.subscribe_events();
        let ledger = self.ledger.clone();
        let (tx, rx) = mpsc::channel(32);
        let out = tx.clone();

        // Forward broadcast events to this client.
        tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        if out.send(Ok(event)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // Apply inbound events and publish outcomes.
        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                let event = match item {
                    Ok(e) => e,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                };

                let note = match ledger.transfer(
                    &event.from_account,
                    &event.to_account,
                    event.amount,
                ) {
                    Ok(()) => "applied".into(),
                    Err(status) => format!("rejected: {}", status.message()),
                };

                let outcome = TransferEvent {
                    from_account: event.from_account,
                    to_account: event.to_account,
                    amount: event.amount,
                    note,
                };
                ledger.publish_event(outcome);
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
