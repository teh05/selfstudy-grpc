use selfstudy_grpc::pb::bank_server::BankServer;
use selfstudy_grpc::{seed_accounts, BankService, Ledger, LISTEN_ADDR};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ledger = Ledger::with_accounts(seed_accounts());
    let service = BankService::new(ledger);

    println!("bank-server listening on {LISTEN_ADDR}");
    println!("seed accounts: {:?}", seed_accounts());

    Server::builder()
        .add_service(BankServer::new(service))
        .serve(LISTEN_ADDR.parse()?)
        .await?;

    Ok(())
}
