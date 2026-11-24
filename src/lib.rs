pub mod pb {
    tonic::include_proto!("bank");
}

mod service;
mod store;

pub use service::BankService;
pub use store::Ledger;

pub const LISTEN_ADDR: &str = "127.0.0.1:50051";
pub const ENDPOINT: &str = "http://127.0.0.1:50051";

pub fn seed_accounts() -> Vec<(&'static str, i64)> {
    vec![("alice", 1_000), ("bob", 500), ("carol", 250)]
}
