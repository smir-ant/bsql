# bsql — Multi-backend SQL toolkit in Rust

## Direction (2026 rebuild)

The theoretical-limit rebuild's core has landed: the compile-checked
`query!` flagship, the PostgreSQL async + sync drivers, and the embedded
SQLite backend all ship today, reachable through the single `bsql` umbrella
crate. `reforge.md` (the original blueprint) and `CREDO.md` (its principles)
are now **historical** — this file and the code are the source of truth.
Load-bearing decisions for any new session:

- **Query API = PURE SQL TEXT.** SQL lives as text in the compile-checked
  `query!` macro, validated at build time against the schema replayed from
  migration DDL. There are **NO** method combinators — the diesel-style
  Fragment/Col combinator paradigm was tried and **reverted** (owner rejected
  `.filter().eq()` builders). Do not reintroduce a runtime SQL builder.
- **Wire format = binary-uniform.** The compile-checked `query!` path encodes every param in
  binary; `ParamsWriter` is the **sole** format authority (no per-call
  text/binary drift).
- **NO CI.** There are no GitHub Actions and the owner mandates none. Gates run
  locally via `cargo` + the `tools/devgates` crate (`deps_pin`,
  `runtime_graph_pin`). Treat any `reforge.md` CI prescription (nightly CI,
  cargo-deny CI) as superseded by local devgates.
- **Compile-checked query API.** SQL references are validated at build time
  against the schema replayed from the consumer's migration `*.sql` by
  `bsql-build` (a `[build-dependencies]` helper) into a catalog the
  `bsql-query-macros` proc-macro reads. The whole flagship is reachable
  through the single `bsql` crate with `features = ["macros"]` — a consumer
  hand-wires nothing (see Build & test). **Guarantee boundary:** the catalog
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
  bsql/              — umbrella facade + query! re-export (bsql::pg, ::pg_sync, ::sqlite)  — 197 LoC
  postgres/
    proto/           — sans-IO wire protocol + session engine (no_std + alloc)  — 24245 LoC
    core/            — shared engine materializer + types + config + TLS + Rows  — 4169 LoC
    async/           — tokio async driver (thin adapter over the engine)  — 1705 LoC
    sync/            — std::net sync driver (thin adapter over the engine)  — 1529 LoC
  sqlite/
    driver/          — embedded SQLite driver (bundled rusqlite)  — 1010 LoC
  build/             — BUILD-DEP: migration DDL → schema catalog (+ SQLite template)  — 32862 LoC
  query-macros/      — PROC-MACRO: reads the catalog, types/validates query!  — 1285 LoC
```

(src LoC measured per crate via `find <crate>/src -name '*.rs' -exec cat {} + | wc -l` — counts inline `#[cfg(test)]` modules, so `build/`'s total is dominated by ~13K lines of inference tests in `src/infer.rs`. Publishable package names: `bsql`, `bsql-postgres-{proto,core,async,sync}`, `bsql-sqlite`, `bsql-build`, `bsql-query-macros`. Non-shipped `publish = false` tools under `tools/`: `bsql-devgates`, `bsql-query-fixture`, `bsql-query-bridge-fixture`, `bsql-query-sqlite-fixture`, `bsql-corpus`.)

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
cargo test -p bsql-query-fixture --test query_live_async -- --ignored  # live query! (async, needs PG)
cargo test -p bsql-query-fixture --test query_live_sync  -- --ignored  # live query! (sync, needs PG)
```

**Consumer wiring — the `query!` flagship.** A consumer reaches the whole
compile-checked query API through the single `bsql` crate: add it with
`features = ["macros", "postgres-async"]` (or `postgres-sync`) to
`[dependencies]`, add `bsql-build` to `[build-dependencies]`, and add a
one-line `build.rs`:

```rust
fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")   // replays migrations/ into the catalog
}
```

Then `bsql::query!(Name, "<SQL>")` types the SQL at build time against that
catalog and emits the `Name` record + the `NameQuery` carrier (which
implements the umbrella's re-exported `bsql::TypedQuery`); an unknown
column is a `compile_error!`. The macro's expansion names only
`::bsql::__rt::…` paths (a `#[doc(hidden)]` internal module), so no other
dependency is needed at compile time — `bsql-query-macros` is a host-only
proc-macro and never enters the runtime binary. `emit` also emits the
SQLite conformance template when the build-dep's `sqlite` feature is on; a
PostgreSQL-only build can call `bsql_build::emit_catalog("migrations")`
instead. `tools/query_fixture` is the end-to-end proof of this shape.

**External-type bridges (optional).** A consumer can make `query!` decode a
column directly into a chosen EXTERNAL crate type (`chrono::DateTime`,
`uuid::Uuid`, `serde_json::Value`, …) with bsql depending on and forcing
NOTHING. The build.rs uses the richer builder — `Catalog::from_migrations(dir)
.bridge(pg_type, target_type_path, converter_fn_path).emit()` (or `.emit_catalog()`
for a PostgreSQL-only build) — keyed on the canonical PG type; the consumer
supplies one INFALLIBLE converter free function per bridged type
(`fn(bsql::Timestamptz) -> chrono::DateTime<Utc>`). The target type and
converter travel as STRINGS, so `bsql-build` / `bsql-query-macros` gain no
external dependency. The free function is the orphan-proof seam: a consumer
cannot `impl bsql::Cell for chrono::DateTime` (E0117 — both foreign), but a free
fn compiles for any foreign target. The bridge reshapes ONLY the record field
value; the row OID list and the const validator ride the native pivot, so the
compile-time OID-drift guarantee (E0080) is untouched.
`tools/query_bridge_fixture` is the end-to-end proof.

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
covers that blind spot: it parses each shipped crate's
`cargo tree --all-features -e normal,no-proc-macro` (runtime) graph and asserts
the build-time-only SQL-parsing libraries — `sqlparser` and `bsql-build` — are
absent. `--all-features` makes the check exhaustive (it activates the umbrella
crate's non-default `macros` feature, which pulls the `bsql-query-macros`
proc-macro); `no-proc-macro` models runtime LINKAGE faithfully (a proc-macro
runs in the compiler and is never linked into the consumer's runtime binary, so
the host-only `bsql -> bsql-query-macros` edge — and `bsql-build` / `sqlparser`
reached only through it — is correctly excluded). Moving `bsql-build` from
`[build-dependencies]` into a shipped crate's `[dependencies]` would leak
`sqlparser` into the runtime/forbid closure via a NORMAL-library edge that
`no-proc-macro` does NOT prune; `deps_pin` stays green on that, but
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

- **Sans-IO engine** (`bsql-postgres-proto`, `engine` module): the session engine with zero I/O dependencies (`no_std + alloc`). Its seams: `Transport` (the driver-facing I/O seam, RPITIT + `Send`), `Live` (a branded, non-`Clone`, linear liveness token minted by `engine::session` / `session_with`), an `Observer` policy seam, and the `Never` carrier for phase-impossible frames. Protocol logic lives here; the driver only supplies bytes.
- **Core** (`bsql-postgres-core`): the shared result materializer, the dynamic `Row` / `QueryResult` types, `ConnectConfig`, TLS config, and the typed `Rows` / `RowsBuilder` containers. Both drivers build on it.
- **Drivers** are thin I/O adapters implementing the `Transport` seam over the one engine. Difference: `.await` on async, blocking on sync.
- **Rows.** The dynamic `Row` (from `query_sql` etc.) is 16 bytes (`'static + Clone + Send + Sync`) over an `Arc`-shared arena: 4 heap allocations per whole `QueryResult`, regardless of row count. The typed `Rows<Q>` (from the `query!` flagship) is 2 allocations per result and 0 per row (borrowed, zero-copy decode).

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
