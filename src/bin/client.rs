use futures::StreamExt;
use selfstudy_grpc::pb::bank_client::BankClient;
use selfstudy_grpc::pb::{
    GetBalanceRequest, TransferEvent, TransferRequest, WatchBalanceRequest,
};
use selfstudy_grpc::ENDPOINT;
use tonic::Request;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = BankClient::connect(ENDPOINT).await?;
    println!("connected to {ENDPOINT}\n");

    demo_unary(&mut client).await?;
    demo_server_stream(&mut client).await?;
    demo_client_stream(&mut client).await?;
    demo_bidi(&mut client).await?;

    println!("all demos finished");
    Ok(())
}

async fn demo_unary(client: &mut BankClient<tonic::transport::Channel>) -> anyhow::Result<()> {
    println!("=== 1) Unary: GetBalance ===");
    for account_id in ["alice", "bob", "nobody"] {
        match client
            .get_balance(Request::new(GetBalanceRequest {
                account_id: account_id.into(),
            }))
            .await
        {
            Ok(resp) => {
                let body = resp.into_inner();
                println!("  {} => {}", body.account_id, body.balance);
            }
            Err(status) => {
                println!(
                    "  {account_id} => ERROR [{:?}] {}",
                    status.code(),
                    status.message()
                )
            }
        }
    }
    println!();
    Ok(())
}

async fn demo_server_stream(
    client: &mut BankClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    println!("=== 2) Server streaming: WatchBalance(alice, max=3) ===");
    let mut stream = client
        .watch_balance(Request::new(WatchBalanceRequest {
            account_id: "alice".into(),
            max_updates: 3,
        }))
        .await?
        .into_inner();

    while let Some(update) = stream.next().await {
        let u = update?;
        println!(
            "  seq={} account={} balance={}",
            u.sequence, u.account_id, u.balance
        );
    }
    println!();
    Ok(())
}

async fn demo_client_stream(
    client: &mut BankClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    println!("=== 3) Client streaming: BatchTransfer ===");
    let transfers = vec![
        transfer("alice", "bob", 100),
        transfer("bob", "carol", 50),
        transfer("alice", "carol", 9_999), // should fail: insufficient funds
    ];

    let response = client
        .batch_transfer(Request::new(tokio_stream::iter(transfers)))
        .await?
        .into_inner();

    println!(
        "  summary: succeeded={} failed={}",
        response.succeeded, response.failed
    );
    for r in response.results {
        println!(
            "  {} {} -> {} amount={} ({})",
            if r.ok { "OK " } else { "ERR" },
            r.from_account,
            r.to_account,
            r.amount,
            r.message
        );
    }
    println!();
    Ok(())
}

async fn demo_bidi(client: &mut BankClient<tonic::transport::Channel>) -> anyhow::Result<()> {
    println!("=== 4) Bidirectional: LiveTransferFeed ===");

    let outbound = tokio_stream::iter(vec![
        event("carol", "alice", 25, "tip"),
        event("alice", "bob", 10, "coffee"),
    ]);

    let mut inbound = client
        .live_transfer_feed(Request::new(outbound))
        .await?
        .into_inner();

    // Read a couple of broadcast outcomes, then stop.
    let mut seen = 0;
    while let Some(item) = inbound.next().await {
        let e = item?;
        println!(
            "  event {} -> {} amount={} note={}",
            e.from_account, e.to_account, e.amount, e.note
        );
        seen += 1;
        if seen >= 2 {
            break;
        }
    }
    println!();
    Ok(())
}

fn transfer(from: &str, to: &str, amount: i64) -> TransferRequest {
    TransferRequest {
        from_account: from.into(),
        to_account: to.into(),
        amount,
    }
}

fn event(from: &str, to: &str, amount: i64, note: &str) -> TransferEvent {
    TransferEvent {
        from_account: from.into(),
        to_account: to.into(),
        amount,
        note: note.into(),
    }
}
