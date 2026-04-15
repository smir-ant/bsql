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

Phase 1a has landed: `bsql-pg-proto` crate with the sans-I/O
PostgreSQL wire-protocol state machine (Ping flow only). See
[`crates/bsql-pg-proto/`](crates/bsql-pg-proto/).

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
