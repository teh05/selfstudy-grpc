# selfstudy-grpc

Mini bank simulator to learn **gRPC with Rust (`tonic`)** — all four RPC styles in one clean service.

Architecture diagrams (Mermaid, GitHub-ready): **[docs/architecture.md](docs/architecture.md)**

## RPC coverage

| Style | Method | What you see |
|---|---|---|
| Unary | `GetBalance` | one request → one response |
| Server streaming | `WatchBalance` | one request → many updates |
| Client streaming | `BatchTransfer` | many transfers → one summary |
| Bidirectional | `LiveTransferFeed` | both sides stream events |

## Prerequisites

- Rust ([rustup](https://rustup.rs))
- No system `protoc` needed — build uses `protoc-bin-vendored`

## Run

Terminal 1 — server:

```bash
cargo run --bin bank-server
```

Terminal 2 — client demo (all 4 RPC types):

```bash
cargo run --bin bank-client
```

Server listens on `127.0.0.1:50051` (plaintext). You can also call the same methods manually from **Postman Desktop** (gRPC request + import `proto/bank.proto`, TLS off).

## Layout

```text
proto/bank.proto      # contract (source of truth)
build.rs              # codegen
docs/architecture.md  # Mermaid diagrams (Ledger + gRPC flows)
src/
  lib.rs              # shared constants + seed accounts
  store.rs            # in-memory ledger
  service.rs          # Bank trait implementation
  bin/server.rs       # gRPC server
  bin/client.rs       # demo client
```

## Learning flow

1. Read [`proto/bank.proto`](proto/bank.proto)
2. Skim [`docs/architecture.md`](docs/architecture.md) for Ledger + RPC diagrams
3. `cargo build` and inspect generated stubs under `target/`
4. Trace `Bank` trait impl in [`src/service.rs`](src/service.rs)
5. Run server + call via Postman or `bank-client`

## Docs

| Doc | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Ledger structure, transfer flow, gRPC end-to-end, 4 RPC sequence diagrams |
