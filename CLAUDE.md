# bsql — Multi-backend SQL toolkit in Rust

## Direction (2026 rebuild)

A theoretical-limit rebuild is underway on branch `rebuild` (master plan
`reforge.md` + the rebuild plan). Load-bearing decisions for any new session:

- **Query API = PURE SQL TEXT.** SQL lives as text in a future compile-checked
  `query!` macro, validated at build time against the schema replayed from
  migration DDL. There are **NO** method combinators — the diesel-style
  Fragment/Col combinator paradigm was tried and **reverted** (owner rejected
  `.filter().eq()` builders). Do not reintroduce a runtime SQL builder.
- **Wire format = binary-uniform.** The `prepared!` path encodes every param in
  binary; `ParamsWriter` is the **sole** format authority (no per-call
  text/binary drift).
- **NO CI.** There are no GitHub Actions and the owner mandates none. Gates run
  locally via `cargo` + the `tools/devgates` crate (`deps_pin`,
  `runtime_graph_pin`). Treat any `reforge.md` CI prescription (nightly CI,
  cargo-deny CI) as superseded by local devgates.
- **Compile-checked query API.** SQL references are validated at build time
  against the schema replayed from the consumer's migration `*.sql` by
  `bsql-build` (a `[build-dependencies]` helper) into a catalog the
  `bsql-query-macros` proc-macro reads. **Guarantee boundary:** the catalog
  matches the migration FILES, not the live database. A migration applied
  out-of-band (e.g. by hand in `psql`) without a corresponding file is invisible
  by design — but this is strictly stronger than a live-introspection cache that
  can silently go stale relative to the migrations under version control: the
  source of truth is the committed migration set, and any change to it (add,
  edit, delete, rename — at any directory depth) recompiles and re-validates. A
  DDL form the replay cannot model faithfully is a loud build error, never a
  silently-wrong catalog.
- **Toolchain pinned** to rustc 1.96.0 (`rust-toolchain.toml`); trybuild/clippy
  goldens capture diagnostics verbatim, so the patch version is fixed.
- **Converge, don't drift.** A prescriptive doc that contradicts the current
  direction silently misleads every future session — fix it, don't append.

## Workspace layout

```
crates/
  bsql/              — umbrella re-export (bsql::pg, bsql::pg_sync, bsql::sqlite)  — 113 LoC
  postgres/
    proto/           — sans-IO wire protocol state machine (no_std, ~49.3K LoC)
    core/            — shared Session + types (2450 LoC)
    async/           — tokio async driver (661 LoC)
    sync/            — std::net sync driver (760 LoC)
    derive/          — proc-macro for prepared! statements (2472 LoC)
  sqlite/
    driver/          — embedded SQLite driver (423 LoC)
```

(src LoC measured @ branch `work/s2b` base 8eb9276 via `find … -name '*.rs' -exec cat {} + | wc -l`, post-Fragment-revert. Package names: `bsql`, `bsql-postgres-{proto,core,async,sync,derive}`, `bsql-sqlite`.)

## Build & test

```bash
cargo check --workspace              # full build
cargo clippy --workspace --all-targets   # lint wall — must be 0 warnings
cargo test --workspace               # unit + integration (non-ignored)
cargo test --workspace --doc         # doctests
cargo test -p bsql-devgates --test deps_pin            # dependency-frontier gate
cargo test -p bsql-devgates --test runtime_graph_pin   # build-time-only boundary gate
cargo test -p bsql-devgates --test doc_links           # intra-doc-link wall (broken-link deny)
cargo test -p bsql-sqlite            # SQLite (no PG needed)
cargo test -p bsql-postgres-async --test sq_live -- --ignored    # async PG (needs local PG)
cargo test -p bsql-postgres-sync --test sync_live -- --ignored   # sync PG (needs local PG)
```

The `deps_pin` gate (`tools/devgates/tests/deps_pin.rs`) pins the resolved
dependency set (parsed from `Cargo.lock`) to a committed golden, and bans any
NEW crate resolving to two versions. An accidental dependency addition or a
version drift fails it. A deliberate dependency change is a reviewed golden
diff, regenerated with `BSQL_DEPS_PIN=overwrite cargo test -p bsql-devgates
--test deps_pin` (mirroring `TRYBUILD=overwrite`), and must be justified in the
root `Cargo.toml` `[workspace.dependencies]` policy block. `deps_pin` pins only
the package SET and is dependency-kind-blind — it cannot tell a runtime edge
from a build edge.

The `runtime_graph_pin` gate (`tools/devgates/tests/runtime_graph_pin.rs`)
covers that blind spot: it parses each shipped crate's `cargo tree -e normal`
(runtime) graph and asserts the build-time-only query toolchain — `sqlparser`,
`bsql-build`, `bsql-query-macros` — is absent. Moving `bsql-build` from
`[build-dependencies]` into a shipped crate's `[dependencies]` would leak
`sqlparser` into the runtime/forbid closure; `deps_pin` stays green on that, but
`runtime_graph_pin` turns red.

The `doc_links` gate (`tools/devgates/tests/doc_links.rs`) is the intra-doc-link
wall: it runs `cargo doc --workspace --no-deps` with
`RUSTDOCFLAGS=-D rustdoc::broken_intra_doc_links` (in a dedicated
`CARGO_TARGET_DIR` to avoid contending for the parent build lock) and asserts
the doc build succeeds. A doc comment that links to a symbol a deletion removed
(`cargo build`/`clippy`/`cargo test` never resolve intra-doc links) fails this
gate the moment it lands. Scope: the PUBLIC documented surface of every member
(a `--document-private-items` tightening is a follow-up — it currently surfaces
unrelated pre-existing private-doc rot in the build-time inference crate + an
engine `super::flush` ambiguity).

PG tests require: PostgreSQL on localhost:5432, user `smir-ant`, database `postgres`, trust auth.
SCRAM test requires: user `bsql_test_scram` with password `test_password_123` in pg_hba.conf.

## Architecture

- **Sans-IO core** (`bsql-postgres-proto`): wire protocol with zero I/O dependencies. State machine driven by `feed_bytes`/`advance_one_frame`.
- **Session** (`bsql-postgres-core`): pump state machine wrapping PgProtocol. Both drivers use identical `pump_step() → PumpAction` loop.
- **Drivers** are thin I/O adapters (~15-line pump loop each). Difference: `.await` on async, blocking on sync.
- **Row** uses Arc-shared arena: 4 heap allocations per entire QueryResult (not per row). Row is 16 bytes, `'static + Clone + Send + Sync`.

## Safety invariants

- `#![forbid(unsafe_code)]` on all driver crates
- Every SHIPPED crate is unsafe-free (`#![forbid(unsafe_code)]` at its own root).
  `unsafe` exists only in `publish = false` (never-shipped) places: the
  `tools/devgates` building blocks (the counting allocator + the post-drop memory
  probe), and one justified `std::env::set_var` in each of the two consumer-fixture
  trybuild tests (`tools/query_fixture/tests/compile_fail.rs` and
  `tools/query_sqlite_fixture/tests/sqlite_gate.rs`, which forward the build-emitted
  catalog / SQLite-template rustc-env channels into the spawned trybuild children) —
  `set_var` is `unsafe` in edition 2024, used once, serially, before any trybuild
  child is spawned, with a `SAFETY` comment. None is in a shipped artifact.
- `#![deny(clippy::unwrap_used, clippy::expect_used)]` on all driver crates
- Static assertions: `Connection: Send`, `Row: Send + Sync + 'static`, `Pool: Send + Sync`
- NULL = `Option<NonZeroU32>` (compiler-enforced, no sentinel)
- `PreparedStatement` consumed by `close_statement(stmt)` — use-after-close is compile error
- Transactions via closure scope — no forgotten commits
- Passwords `Zeroizing<String>` — scrubbed on drop, redacted in Debug

## Conventions

- No `expect()` or `unwrap()` in production code
- Error types: `DriverError::Db(DbError)` for server errors with SQLSTATE, `DriverError::Config` for pre-connect validation, `DriverError::NoRows` for empty results
- `ConnectConfig` is `#[non_exhaustive]` — construct via `new()` + builder methods
- `SslMode::Prefer` warns in debug builds about SSL downgrade risk
