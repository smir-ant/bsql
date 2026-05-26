# bsql

**Status: v1.0 architectural redesign in progress.**

The entire repository has been reset (2026-04-15) for a deliberate
rebuild from scratch. The v0.27 codebase is in git history; it will
not ship further.

## Where to look

All design, principles, architecture, feature list, crate layout,
macros ethos, verification strategy, and roadmap live in one place:

**[`reforge.md`](reforge.md)** — the master architectural document.

## Current state

Three crates live in the workspace:

| Crate | Description | Status |
|-------|-------------|--------|
| [`bsql-pg-proto`](crates/bsql-pg-proto/) | Sans-IO PG wire protocol state machine (`no_std + alloc`, `#![forbid(unsafe_code)]`) | Feature-complete for v1 |
| [`bsql-pg-proto-derive`](crates/bsql-pg-proto-derive/) | Proc-macro pair (`#[derive(Pristine)]`) | Shipped |
| [`bsql-postgres`](crates/bsql-postgres/) | Async driver (tokio + rustls) | Alpha — [README](crates/bsql-postgres/README.md) |

### bsql-pg-proto highlights
- PgProtocol<Active>: 296 B per connection
- push_command/ping: 46 ns
- SimpleQuery, Extended Query, COPY, LISTEN/NOTIFY, Describe, Close, Terminate
- SCRAM-SHA-256 + MD5 + Cleartext + Trust authentication
- SSL negotiation typestate (0 B runtime cost)
- No fixed column/param caps (exact-size Box<[u32]>)
- 673 tests, bench-verified 0 regressions

### bsql-postgres highlights
- Connect (Trust + SCRAM + TLS via rustls)
- query / query_one / query_opt / simple_query / execute
- Typed Row access: `row.get::<i32>(0)`
- DSN parsing + env var config
- 29 tests live-tested against real PostgreSQL

The crate is `no_std`, forbids `unsafe`, runs the full clippy forbid
bundle, and passes a hand-rolled 100 000-iteration randomized fuzz of
the frame-header parser. `cargo test -p bsql-pg-proto` runs 19 tests
in < 50 ms.

Remaining Phase 1 sub-phases (1b–1f) land
`bsql-backend`, SCRAM-SHA-256, the full command set, streaming,
COPY / LISTEN / NOTIFY, and the async `run_io` wrapper — in that
order.

## One-line goal

*The Rust SQL driver that defines what absolute safety means in
this domain: if your code compiles, the SQL is correct and the wire
is cancellation-safe — by construction, not by test.*

## License

MIT OR Apache-2.0 at your option.
