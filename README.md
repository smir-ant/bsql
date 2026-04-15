# bsql

**Status: v1.0 architectural redesign in progress.**

The entire repository has been reset (2026-04-15) for a deliberate
rebuild from scratch. The v0.27 codebase is in git history; it will
not ship further.

## Where to look

All design, principles, architecture, feature list, crate layout,
macros ethos, verification strategy, and roadmap live in one place:

**[`reforge.md`](reforge.md)** — the master architectural document.

Nothing else at the top level is prescriptive right now. `Cargo.toml`
is an empty workspace; crates appear as Phase 1..6 of reforge.md
land.

## One-line goal

*The Rust SQL driver that defines what absolute safety means in
this domain: if your code compiles, the SQL is correct and the wire
is cancellation-safe — by construction, not by test.*

## License

MIT OR Apache-2.0 at your option.
