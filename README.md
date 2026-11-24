# selfstudy-grpc

Mini bank simulator to learn **gRPC with Rust (`tonic`)** — all four RPC styles in one clean service.

## RPC coverage

| Style | Method | What you see |
|---|---|---|
| Unary | `GetBalance` | one request → one response |
| Server streaming | `WatchBalance` | one request → many updates |
| Client streaming | `BatchTransfer` | many transfers → one summary |
| Bidirectional | `LiveTransferFeed` | both sides stream events |

## Prerequisites

- Rust (rustup)
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

## Layout

```text
proto/bank.proto      # contract (source of truth)
build.rs              # codegen
src/
  lib.rs              # shared constants + seed accounts
  store.rs            # in-memory ledger
  service.rs          # Bank trait implementation
  bin/server.rs       # gRPC server
  bin/client.rs       # demo client
```

## Learning flow

1. Read `proto/bank.proto`
2. `cargo build` and inspect generated stubs under `target/`
3. Trace `Bank` trait impl in `src/service.rs`
4. Run server + client and match each printed section to an RPC type
