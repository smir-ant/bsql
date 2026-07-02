# bsql

*The Rust SQL toolkit that defines what absolute safety means in this
domain: if your code compiles, the SQL is correct and the wire is
cancellation-safe — by construction, not by test.*

`bsql` is a multi-backend SQL toolkit. Its flagship is a compile-checked
`query!` macro: the SQL you write is typed at `cargo build` against the
schema replayed from your migration files, so an unknown table, a wrong
column type, or a forgotten nullability becomes a compile error — never a
runtime surprise. Every parameter is bound in one uniform binary format,
so there is no per-call text/binary drift and no injection surface.

> Pre-release. The umbrella crate is `0.1.0-alpha.0` and the workspace
> crates are `1.0.0-alpha.0`; nothing is on crates.io yet. Depend via git
> or path until the first published alpha.
> *(versions measured: `grep -m1 '^version' crates/bsql/Cargo.toml` and the
> workspace `[workspace.package]` block in the root `Cargo.toml`.)*

## What is shipped today

- **PostgreSQL — async and sync.** A tokio driver (`bsql::pg`) and a
  blocking `std::net` driver (`bsql::pg_sync`), both thin I/O adapters over
  one shared sans-IO protocol engine. Trust / MD5 / SCRAM-SHA-256 auth,
  optional rustls TLS, connection pooling, closure-scoped transactions.
- **Embedded SQLite** (`bsql::sqlite`) over bundled `rusqlite`.
- **The compile-checked `query!` macro** (feature `macros`), reachable
  through the single `bsql` crate. SQL is typed at build time against the
  schema your migration `*.sql` files describe.
- **Binary-uniform wire.** `ParamsWriter` is the sole encoding authority;
  `query!` binds every parameter in binary.
- **A safety floor enforced by the compiler**, not by convention (see
  [Safety floor](#safety-floor)).

Not yet shipped (do not assume these exist): a `COPY` fast path, automatic
N+1 detection, a migration runner, a test kit. The macro validates against
your migration **files**, not a live database — a schema change applied
by hand in `psql` without a migration file is invisible by design (the
committed migration set is the source of truth).

## The one-crate consumer story

A consumer needs exactly one runtime dependency (`bsql`) plus one
build-dependency (`bsql-build`) and a one-line `build.rs`. This is the
shape proven end-to-end by `tools/query_fixture` — its only `[dependency]`
is `bsql`, and every `query!` it writes expands to `::bsql::__rt::…` paths
that resolve through that single crate.

```toml
# Cargo.toml
[dependencies]
# `macros` pulls the query! toolchain; add the backend(s) you use.
bsql = { git = "https://github.com/smir-ant/bsql", features = ["macros", "postgres-async"] }

[build-dependencies]
bsql-build = { git = "https://github.com/smir-ant/bsql" }
```

```rust
// build.rs — replays migrations/ into the schema catalog query! reads.
fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")
}
```

```sql
-- migrations/0001_init.sql
CREATE TABLE users (id int PRIMARY KEY, email text NOT NULL);
```

```rust
// src/lib.rs — SQL typed at build time against the catalog above.
bsql::query!(UsersById, "SELECT id, email FROM users WHERE id = $1");
```

`query!(Name, "<SQL>")` emits two typed-record types (`Name` and its
row-shape) plus the carrier `NameQuery`, which implements the umbrella
crate's re-exported `bsql::TypedQuery`. Referencing a column the
migrations never created — e.g. `SELECT nope FROM users` — is a
`compile_error!`, not a failed query at runtime. The typed result of
executing a carrier is a `bsql::Rows<NameQuery>` (2 allocations per result,
0 per row); the user-facing query types (`bsql::TypedQuery`,
`bsql::PreparedQuery`, `bsql::QueryFingerprint`, `bsql::DecodeError`,
`bsql::Rows`, `bsql::RowsBuilder`) are all nameable with no dependency in
scope but `bsql`.

*(consumer shape verified by reading `crates/bsql/src/lib.rs`,
`tools/query_fixture/{Cargo.toml,build.rs,src/lib.rs}`, and the `emit` /
`emit_catalog` doc comments in `crates/build/src/lib.rs`. `emit` is the
one-line entry point; it also emits the SQLite conformance template when
the build-dep's `sqlite` feature is on. A deliberately PostgreSQL-only
build can call `bsql_build::emit_catalog("migrations")` instead.)*

To target SQLite for compile-checked queries as well, enable the
umbrella's `macros-sqlite` feature and `bsql-build`'s `sqlite` feature in
`[build-dependencies]`; `emit` then cross-checks each `query!` against a
real SQLite replay of the same migrations.

### Runtime queries

The typed `query!` path is the flagship, but every driver also exposes a
runtime-SQL surface (`query_sql`, `query_params_one`, `prepare` /
`execute_prepared`, `transaction`, …) and a dynamic 16-byte `Row` backed
by an `Arc`-shared arena (4 heap allocations per whole result, regardless
of row count). See the crate-root docs of `bsql` / `bsql-postgres-async` /
`bsql-postgres-sync` / `bsql-sqlite` for runnable examples.

## Crate layout

Nine publishable crates and four never-published (`publish = false`) dev
tools. *(members: root `Cargo.toml` `[workspace] members`; package names /
publish flags: `grep -m1 '^name\|^publish' <member>/Cargo.toml`.)*

| Crate | Path | Role |
|-------|------|------|
| `bsql` | [`crates/bsql/`](crates/bsql/) | Umbrella facade. Re-exports `bsql::pg` / `bsql::pg_sync` / `bsql::sqlite` per feature; behind `macros`, re-exports `query!` + the typed-query surface. The one crate a consumer needs. |
| `bsql-postgres-proto` | [`crates/postgres/proto/`](crates/postgres/proto/) | Sans-IO PostgreSQL wire protocol + session engine (`no_std + alloc`). Holds the typed-query decode primitives the `query!` expansion names. |
| `bsql-postgres-core` | [`crates/postgres/core/`](crates/postgres/core/) | Shared across both drivers: result materializer, dynamic `Row` / `QueryResult` types, `ConnectConfig`, TLS config, and `Rows` / `RowsBuilder`. |
| `bsql-postgres-async` | [`crates/postgres/async/`](crates/postgres/async/) | tokio async driver — a thin I/O adapter over the engine. |
| `bsql-postgres-sync` | [`crates/postgres/sync/`](crates/postgres/sync/) | `std::net` blocking driver — a thin I/O adapter over the engine. |
| `bsql-postgres-derive` | [`crates/postgres/derive/`](crates/postgres/derive/) | Internal proc-macro for `#[derive(Pristine)]` struct-freshness checks. A build helper, not a consumer-facing feature. |
| `bsql-sqlite` | [`crates/sqlite/driver/`](crates/sqlite/driver/) | Embedded SQLite driver over bundled `rusqlite`. |
| `bsql-build` | [`crates/build/`](crates/build/) | **Build-time only.** Replays migration DDL into the schema catalog (and, under its `sqlite` feature, a SQLite conformance template). A `[build-dependencies]` helper — never a runtime dependency. |
| `bsql-query-macros` | [`crates/query-macros/`](crates/query-macros/) | **Host-only proc-macro.** Reads the build catalog and types / validates each `query!`, emitting the typed records. Runs in the compiler; never linked into a consumer's runtime binary. |

`bsql-build` and `bsql-query-macros` are the build-time query toolchain.
Neither (nor `sqlparser`, which `bsql-build` reaches) enters any shipped
crate's runtime graph — the `runtime_graph_pin` gate proves this.

Dev-only tools (`publish = false`, not shipped): `bsql-devgates`
(the local gates + counting allocator, the workspace's only `unsafe`),
`bsql-query-fixture` and `bsql-query-sqlite-fixture` (real consumers that
exercise the migrations → catalog → `query!` chain end-to-end), and
`bsql-corpus` (a replay corpus pinning engine behaviour against goldens).

## Safety floor

- `#![forbid(unsafe_code)]` at the root of every shipped crate — all
  production code is unsafe-free and un-bypassable. The only `unsafe` in
  the workspace lives in the never-published `bsql-devgates`.
- A two-tier lint wall inherited by every shipped crate (root
  `Cargo.toml` `[workspace.lints]`): a `forbid` floor plus a `deny` band
  that bans `panic!` / `unwrap` / `expect` and a silent-fallback ledger in
  production while allowing the loud-failure forms inside tests.
- NULL is `Option<NonZeroU32>` — the compiler enforces handling, no
  sentinel value.
- A `PreparedStatement` is consumed by `close_statement` — use after close
  is a compile error.
- Transactions are closure-scoped — no forgotten commits.
- Passwords are zeroized on drop and redacted in `Debug`.

## Verification

There is **no CI** — by design. All gates run locally via `cargo` and the
`bsql-devgates` crate.

```bash
cargo check  --workspace                                   # full build
cargo clippy --workspace --all-targets                     # lint wall — 0 warnings
cargo test   --workspace                                   # unit + integration (offline)
cargo test   --workspace --doc                             # doctests
cargo test -p bsql-devgates --test deps_pin                # pinned dependency frontier
cargo test -p bsql-devgates --test runtime_graph_pin       # build-time-only boundary
cargo test -p bsql-devgates --test doc_links               # intra-doc-link wall
cargo bench  --workspace                                   # perf evidence (criterion)
```

The live suites are `#[ignore]` and need a local PostgreSQL / SQLite:

```bash
cargo test -p bsql-sqlite                                          # SQLite (no PG needed)
cargo test -p bsql-postgres-async --test sq_live   -- --ignored   # async PG
cargo test -p bsql-postgres-sync  --test sync_live -- --ignored   # sync PG
cargo test -p bsql-query-fixture  --test query_live_async -- --ignored  # live query! (async)
cargo test -p bsql-query-fixture  --test query_live_sync  -- --ignored  # live query! (sync)
```

### Measured facts (reproduce them)

All measured at commit `67882617` in this worktree:

- **1573 test functions** — `1516` `#[test]` + `57` `#[tokio::test]`
  (including `tokio::test(flavor = …)` variants). Of these, **125 are
  `#[ignore]` live suites** that require a running database.
  ```bash
  find . -path ./target -prune -o -name '*.rs' -print0 \
    | xargs -0 grep -hE '^[[:space:]]*#\[(tokio::)?test' | wc -l   # 1573
  find . -path ./target -prune -o -name '*.rs' -print0 \
    | xargs -0 grep -hE '^[[:space:]]*#\[ignore'        | wc -l   # 125
  ```
- **Source LoC** (per shipped crate `src/`; the largest, `bsql-build`, is
  dominated by an inline `#[cfg(test)]` inference test module):
  ```bash
  for d in crates/bsql crates/postgres/{proto,core,async,sync,derive} \
           crates/sqlite/driver crates/build crates/query-macros; do
    printf '%-28s %s\n' "$d" \
      "$(find "$d/src" -name '*.rs' -exec cat {} + | wc -l)"
  done
  # bsql 173 · proto 24813 · core 3776 · async 1711 · sync 1534
  # derive 416 · sqlite/driver 1010 · build 32862 · query-macros 1285
  ```

## Sources of truth

- **[`CLAUDE.md`](CLAUDE.md)** — the live conventions, workspace layout,
  build/test commands, and safety invariants. Read this first.
- **The code** — the authoritative behaviour.
- **[`reforge.md`](reforge.md)** and **[`CREDO.md`](CREDO.md)** are
  **historical** design records (the original v1.0 blueprint and its
  principles). They are superseded by `CLAUDE.md` + the code; consult them
  only for design rationale, never as current instructions.
- **[`BENCHMARKING.md`](BENCHMARKING.md)** — the perf/codegen measurement
  playbook. **[`deferred.md`](deferred.md)** is a frozen pre-rebuild
  registry.

## License

MIT OR Apache-2.0 at your option.
