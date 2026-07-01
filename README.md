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

A theoretical-limit rebuild is executing a master plan on the `rebuild`
branch (see `reforge.md` and the in-repo plan). The workspace is:

| Crate | Path | Description |
|-------|------|-------------|
| `bsql` | [`crates/bsql/`](crates/bsql/) | Umbrella facade: re-exports `bsql::pg`, `bsql::pg_sync`, `bsql::sqlite` |
| `bsql-postgres-proto` | [`crates/postgres/proto/`](crates/postgres/proto/) | Sans-IO PG wire protocol state machine (`no_std + alloc`, `#![forbid(unsafe_code)]`) |
| `bsql-postgres-core` | [`crates/postgres/core/`](crates/postgres/core/) | Shared `Session` + types; the `pump_step → PumpAction` loop both drivers share |
| `bsql-postgres-async` | [`crates/postgres/async/`](crates/postgres/async/) | Async driver (tokio + rustls) — [README](crates/postgres/async/README.md) |
| `bsql-postgres-sync` | [`crates/postgres/sync/`](crates/postgres/sync/) | Sync driver (`std::net`, no tokio) |
| `bsql-postgres-derive` | [`crates/postgres/derive/`](crates/postgres/derive/) | Proc-macro for `#[derive(Pristine)]` struct-freshness checks |
| `bsql-sqlite` | [`crates/sqlite/driver/`](crates/sqlite/driver/) | Embedded SQLite driver |

### Tests

Measured @ branch `rebuild` (commit 8eb9276), reproducible by grep:

- 861 `#[test]` / `#[tokio::test]` functions across the workspace.
- 57 of those are `#[ignore]` live tests that need a local PostgreSQL
  (18 async via `--test sq_live`, 19 sync via `--test sync_live`).

```bash
cargo check --workspace                                            # full build
cargo test -p bsql-sqlite                                          # SQLite (no PG needed)
cargo test -p bsql-postgres-async --test sq_live   -- --ignored   # async PG (needs local PG)
cargo test -p bsql-postgres-sync  --test sync_live -- --ignored   # sync PG (needs local PG)
```

Safety floor: every driver crate is `#![forbid(unsafe_code)]` and runs
the clippy forbid bundle (`unwrap_used` / `expect_used` denied).

## Direction

- **Query API = pure SQL text.** SQL lives as text in a future
  compile-checked `query!` macro, validated at `cargo build` against the
  schema replayed from migration DDL — typos, type mismatches, and
  forgotten nullability become compile errors. There are no method
  combinators: the diesel-style Fragment/Col builder was tried and
  reverted. SQL is a language, not an AST-builder.
- **Wire format = binary-uniform.** `ParamsWriter` is the sole format
  authority; `query!` params are binary-encoded uniformly.
- **No CI.** Gates run locally via `cargo` plus a planned `devgates`
  crate. There are no GitHub Actions and none are planned.

## One-line goal

*The Rust SQL driver that defines what absolute safety means in
this domain: if your code compiles, the SQL is correct and the wire
is cancellation-safe — by construction, not by test.*

## License

MIT OR Apache-2.0 at your option.
