# Architecture

Diagrams below use [Mermaid](https://mermaid.js.org/) and render automatically on GitHub.

## Ledger

In-memory source of truth for balances, plus two broadcast channels used by streaming RPCs.

```mermaid
flowchart TB
  subgraph Ledger["Ledger - Arc Inner"]
    Balances["balances<br/>Mutex HashMap account_id to i64<br/>alice=1000 bob=500 carol=250"]
    BalTx["balance_tx broadcast<br/>account_id + balance"]
    EvTx["event_tx broadcast<br/>TransferEvent"]
  end

  Get["get_balance id"] --> Balances
  Transfer["transfer from to amount"] --> Balances
  Transfer -->|"on success"| BalTx
  Publish["publish_event"] --> EvTx
  SubB["subscribe_balances"] --> BalTx
  SubE["subscribe_events"] --> EvTx
```

### Transfer flow inside the Ledger

```mermaid
flowchart TD
  A["transfer from, to, amount"] --> B{"amount greater than 0<br/>and from not equal to?"}
  B -->|no| E1["Err InvalidArgument"]
  B -->|yes| C{"from and to exist?"}
  C -->|no| E2["Err NotFound"]
  C -->|yes| D{"from balance greater or equal amount?"}
  D -->|no| E3["Err FailedPrecondition"]
  D -->|yes| F["update HashMap<br/>from minus amount<br/>to plus amount"]
  F --> G["broadcast balance_tx<br/>for from and to"]
  G --> H["Ok"]
```

---

## gRPC end-to-end

Contract → codegen → server/client → Ledger.

```mermaid
flowchart LR
  Proto["bank.proto<br/>contract"] -->|"build.rs + prost"| Gen["Generated stubs<br/>Bank trait + BankClient"]
  Gen --> Srv["bank-server<br/>BankService"]
  Gen --> Cli["Postman or bank-client"]

  Cli -->|"HTTP/2 gRPC<br/>127.0.0.1:50051"| Srv
  Srv --> Led["Ledger"]
```

### Four RPC styles mapped to the Ledger

```mermaid
sequenceDiagram
  participant C as Client or Postman
  participant S as BankService
  participant L as Ledger

  Note over C,L: 1 Unary GetBalance
  C->>S: GetBalance alice
  S->>L: get_balance alice
  L-->>S: 1000
  S-->>C: BalanceResponse

  Note over C,L: 2 Server streaming WatchBalance
  C->>S: WatchBalance alice max=3
  S->>L: get_balance + subscribe_balances
  S-->>C: update seq=1
  S-->>C: update seq=2
  S-->>C: update seq=3

  Note over C,L: 3 Client streaming BatchTransfer
  C->>S: TransferRequest 1
  C->>S: TransferRequest 2
  C->>S: TransferRequest 3
  C->>S: End stream
  S->>L: transfer per message
  S-->>C: BatchTransferResponse summary

  Note over C,L: 4 Bidirectional LiveTransferFeed
  C->>S: TransferEvent
  S->>L: transfer
  S->>L: publish_event outcome
  L-->>S: broadcast event_tx
  S-->>C: TransferEvent note applied or rejected
```

### RPC style cheat sheet

| Style | Method | Client sends | Server returns |
|---|---|---|---|
| Unary | `GetBalance` | 1 message | 1 message |
| Server streaming | `WatchBalance` | 1 message | many messages |
| Client streaming | `BatchTransfer` | many messages | 1 summary |
| Bidirectional | `LiveTransferFeed` | stream | stream |

---

## Related source

| Piece | File |
|---|---|
| Contract | [`proto/bank.proto`](../proto/bank.proto) |
| Ledger | [`src/store.rs`](../src/store.rs) |
| gRPC handlers | [`src/service.rs`](../src/service.rs) |
| Server binary | [`src/bin/server.rs`](../src/bin/server.rs) |
| Client demo | [`src/bin/client.rs`](../src/bin/client.rs) |
