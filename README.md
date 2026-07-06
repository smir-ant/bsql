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
  blocking `std::net` driver (`bsql::pg_sync`), both thin I/O adapters that
  plug their socket into ONE transport-generic driver core (`Core<S>`), so
  async/sync parity is a compiler guarantee, not hand-maintained twins. Trust
  / MD5 / SCRAM-SHA-256 auth, rustls TLS, connection pooling, closure-scoped
  transactions. Runtime parameterized queries run in a single round trip (a
  fused `Parse`+`Bind`+`Describe`+`Execute`+`Sync`).
- **Embedded SQLite** (`bsql::sqlite`) over bundled `rusqlite` — a full peer of
  the PostgreSQL path, not a text-only wrapper. The compile-checked `query!`
  flagship RUNS against it: the same `query::<Q>` / `query_one::<Q>` /
  `query_opt::<Q>` / `query_each::<Q>` verbs decode into the same typed records,
  verifying each value's storage class at runtime (a mismatch or an unexpected
  `NULL` is a classified error, never a silent coercion). Typed `&[ValueRef]`
  parameters bind in their true storage class (so `NULL` / `BLOB` are bindable
  and integers escape the affinity trap), a closure-scoped transaction guard
  makes a nested/manual-commit desync a compile error, and a default
  `busy_timeout` turns WAL contention into a classified busy error, never a hang.
- **Local unix-domain sockets.** An absolute-path host (`/tmp`,
  `PGHOST=/var/run/postgresql`, or a `host=` DSN parameter) connects over a
  local `AF_UNIX` socket instead of TCP — libpq's rule, centralized once —
  measured ~2.4–2.9× faster than loopback TCP on a single round trip. A unix
  socket is always plaintext, so `SslMode::Require` over one is a loud
  `DriverError::Config`, never a silent downgrade.
- **Threat-scoped TLS default.** When you set no `SslMode`, the effective mode
  is resolved at connect against the endpoint: a LOCAL endpoint (unix socket, or
  a loopback host — `localhost`, `127.0.0.0/8`, `::1`) defaults to `Prefer`, and
  a REMOTE endpoint (any other host, private ranges included) defaults to
  `Require` — so a remote server that refuses TLS is a loud error naming the
  fix, never a silent plaintext connect an on-path attacker could have forced.
  An explicit `SslMode` (builder / DSN `sslmode=` / `PGSSLMODE`) always wins.
- **TLS with custom or private CA roots.** `SslMode::Require` verifies against
  the baked Mozilla root bundle by default; a private CA is supplied with
  `ConnectConfig::with_ca_roots(pem)` (or the `sslrootcert=<path>` DSN key /
  `PGSSLROOTCERT` env), and a bad or empty PEM fails CLOSED — never a fallback
  to baked roots or plaintext. `SslMode::Prefer` warns on stderr (debug AND
  release) when the server refuses SSL and it falls back to plain TCP, and
  `Connection::is_encrypted()` (both drivers) lets a consumer reject a
  plaintext / downgraded connection outright.
- **The compile-checked `query!` macro** (feature `macros`), reachable
  through the single `bsql` crate. SQL is typed at build time against the
  schema your migration `*.sql` files describe.
- **Binary-uniform wire.** `ParamsWriter` is the sole encoding authority;
  `query!` binds every parameter in binary.
- **Streaming `COPY`.** `copy_in` / `copy_in_with` bulk-load a table in
  constant memory (the send buffer stays bounded under 2× a 64 KiB flush
  threshold) and batch streamed rows into large flushes — a megarow load costs
  roughly `total_bytes / 64 KiB` write syscalls instead of one per row.
  `copy_out` streams a table back the same way. Both drivers, PostgreSQL text
  copy format, the table name validated as a SQL identifier (`SafeTable`).
- **Typed `LISTEN` / `NOTIFY`.** `listen` subscribes; `recv_notification`
  (or `recv_notification_as::<T>` for a `FromStr`-parsed payload) delivers
  the notifications. A `NOTIFY` that arrives *during* a query is captured in
  a counted, no-drop notification ledger — never silently dropped. Both
  drivers.
- **External-type bridges.** `query!` can decode a column straight into a
  consumer-chosen external crate type (`chrono::DateTime`, `uuid::Uuid`,
  `serde_json::Value`, …) with `bsql` depending on and forcing nothing: the
  `build.rs` registers `.bridge(pg_type, target_path, converter_fn_path)` and
  the consumer supplies one infallible free-function converter — the
  orphan-proof seam. (CLAUDE.md documents it under *External-type bridges*.)
- **N+1 query detection** (feature `n1-detect`). Detects the classic
  anti-pattern — the same `query!` executed once per row of a prior result,
  from the same source line — and surfaces it through `conn.n1_report()` with
  the offending SQL, file, line, and count. Diagnostics-only (it never
  batches, blocks, errors, or alters a result) and zero-cost when off
  (default): a production build compiles no detector field, no query-path
  branch, and no `#[track_caller]` cost. Both drivers.
- **Destructive-migration acknowledgement gate.** A migration that
  irreversibly destroys data — `DROP TABLE`, `ALTER TABLE … DROP COLUMN`,
  `DROP SCHEMA … CASCADE`, `TRUNCATE`, or `DROP DATABASE` — is a **build
  error** unless a co-located `-- bsql:ack-destructive` comment
  acknowledges it, so accidental data loss is caught at compile time.
- **An in-memory fake PostgreSQL test kit** (`bsql-testkit`). A consumer
  tests real driver code — `query_sql` *and* the compile-checked `query!`
  flagship — against a deterministic in-memory fake, with no network and no
  server, over either the async or the sync driver.
- **Cargo feature slimming.** TLS (`tls`, default-on) and SCRAM-SHA-256 auth
  (`scram`, default-on) are droppable: a `default-features = false` build for a
  common localhost / unix-socket / trust-auth deployment omits — and never
  compiles — the whole ring/rustls subtree and the SCRAM crypto crates
  (measured async runtime graph 41 → 27 crates with both off). Dropping a
  capability FAILS LOUD at connect: `SslMode::Require` with `tls` off, or a
  password with `scram` off, is a classified `DriverError::Config`, never a
  silent plaintext connect or auth failure. The baked Mozilla CA bundle is
  itself behind the default-on `webpki-roots` feature (drop it for a
  pinned-/private-CA-only build).
- **A safety floor enforced by the compiler**, not by convention (see
  [Safety floor](#safety-floor)).

Not yet shipped (do not assume it exists): a live migration **runner**. The
build-time schema validation and the destructive-migration acknowledgement gate
ship, but there is no
`bsql migrate` command that applies migrations to a running database. The
macro validates against your migration **files**, not a live database — a
schema change applied by hand in `psql` without a migration file is
invisible by design (the committed migration set is the source of truth).

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
`bsql::Rows`) are all nameable with no dependency in scope but `bsql`. (The
prebuffer `Rows` is built from, `RowsBuilder`, is a `#[doc(hidden)]` internal
decode seam — reachable for the fixture's offline tests, but not a consumer
API.)

*(consumer shape verified by reading `crates/bsql/src/lib.rs`,
`tools/query_fixture/{Cargo.toml,build.rs,src/lib.rs}`, and the `emit` /
`emit_catalog` doc comments in `crates/build/src/lib.rs`. `emit` is the
one-line entry point; it also emits the SQLite conformance template when
the build-dep's `sqlite` feature is on. A deliberately PostgreSQL-only
build can call `bsql_build::emit_catalog("migrations")` instead.)*

The same `query!` runs against **SQLite** too. With the umbrella's `sqlite`
feature on, a carrier for a SQLite-decodable query (every projected column a
SQLite storage class, no PostgreSQL-only dynamic sugar) also implements
`bsql::sqlite::SqliteTypedQuery`, and `bsql::sqlite::Connection` gains the
same typed verbs — `query::<Q>` / `query_one::<Q>` / `query_opt::<Q>` /
`query_each::<Q>` (on the connection and its transaction guard) — that return
the SAME typed records. Because SQLite is dynamically typed, decoding
VERIFIES each value's actual storage class against the record's declared
field type: a mismatch (the catalog declared `INTEGER`, a `TEXT` arrives) is
a classified `TypeMismatch`, a `NULL` in a non-`Option` field is
`UnexpectedNull` — never a silent coercion. A carrier for a PostgreSQL-only
query (a `uuid` column, an `OPTIONAL(...)` toggle) simply does not implement
`SqliteTypedQuery`, so running it on the SQLite driver is a located compile
error, not a runtime surprise.

**Recommended: enable `macros-sqlite` alongside.** The runtime `sqlite`
feature and the build-time `macros-sqlite` conformance oracle are
orthogonal: with `sqlite` + `macros` but WITHOUT `macros-sqlite` you get the
typed runtime (still fail-loud — a storage-class mismatch is a classified
runtime error, never silent) but NO build-time proof that real SQLite
resolves the same row shape the lattice inferred. Enabling `macros-sqlite`
(and `bsql-build`'s `sqlite` feature in `[build-dependencies]`) closes that
gap — `emit` then cross-checks each `query!` against a real SQLite replay of
the migrations at build time. The only cost is a second bundled-`rusqlite`
build-dependency compile; a SQLite-targeting consumer should enable both.

### Runtime queries

The typed `query!` path is the flagship, but every driver also exposes a
runtime-SQL surface (`query_sql`, `query_params_one`, `prepare` /
`execute_prepared`, `transaction`, …) and a dynamic 16-byte `Row` backed
by an `Arc`-shared arena (3 heap allocations per whole result — the arena's
data + slots vectors + the shared `Arc`; the result mints `Row` handles
lazily, never an eager `Vec<Row>` — regardless of row count). See the
crate-root docs of `bsql` / `bsql-postgres-async` /
`bsql-postgres-sync` / `bsql-sqlite` for runnable examples.

## Crate layout

Nine publishable crates and six never-published (`publish = false`) dev
tools. *(members: root `Cargo.toml` `[workspace] members`; package names /
publish flags: `grep -m1 '^name\|^publish' <member>/Cargo.toml`.)*

| Crate | Path | Role |
|-------|------|------|
| `bsql` | [`crates/bsql/`](crates/bsql/) | Umbrella facade. Re-exports `bsql::pg` / `bsql::pg_sync` / `bsql::sqlite` per feature; behind `macros`, re-exports `query!` + the typed-query surface. The one crate a consumer needs. |
| `bsql-postgres-proto` | [`crates/postgres/proto/`](crates/postgres/proto/) | Sans-IO PostgreSQL wire protocol + session engine (`no_std + alloc`). Holds the typed-query decode primitives the `query!` expansion names. |
| `bsql-postgres-core` | [`crates/postgres/core/`](crates/postgres/core/) | Shared across both drivers: the transport-generic driver engine `Core<S>` (verbs defined once), the result materializer, dynamic `Row` / `QueryResult` types, `ConnectConfig`, TLS config, the typed `Rows` container (built from an internal `RowsBuilder` prebuffer), and the `SafeIdent` guard. |
| `bsql-postgres-async` | [`crates/postgres/async/`](crates/postgres/async/) | tokio async driver — plugs a `TokioSocket` into the shared `Core<S>`. |
| `bsql-postgres-sync` | [`crates/postgres/sync/`](crates/postgres/sync/) | `std::net` blocking driver — plugs a `SyncSocket` into the shared `Core<S>`. |
| `bsql-sqlite` | [`crates/sqlite/driver/`](crates/sqlite/driver/) | Embedded SQLite driver over bundled `rusqlite`. |
| `bsql-testkit` | [`crates/testkit/`](crates/testkit/) | Deterministic in-memory fake PostgreSQL. Tests real driver code (`query_sql` and the compile-checked `query!` path) against scripted replies over both drivers — no network, no server. Enables the drivers' + core's off-by-default `testkit` feature (the `Wire::Fake` transport arm). |
| `bsql-build` | [`crates/build/`](crates/build/) | **Build-time only.** Replays migration DDL into the schema catalog (and, under its `sqlite` feature, a SQLite conformance template). A `[build-dependencies]` helper — never a runtime dependency. |
| `bsql-query-macros` | [`crates/query-macros/`](crates/query-macros/) | **Host-only proc-macro.** Reads the build catalog and types / validates each `query!`, emitting the typed records. Runs in the compiler; never linked into a consumer's runtime binary. |

`bsql-build` and `bsql-query-macros` are the build-time query toolchain.
Neither (nor `sqlparser`, which `bsql-build` reaches) enters any shipped
crate's runtime graph — the `runtime_graph_pin` gate proves this.

Dev-only tools (`publish = false`, not shipped): `bsql-devgates`
(the local gates + counting allocator), `bsql-query-fixture` and
`bsql-query-sqlite-fixture` (real consumers that exercise the
migrations → catalog → `query!` chain end-to-end), `bsql-query-bridge-fixture`
(a real consumer proving the external-type bridge — `query!` decoding a column
into a consumer-chosen type), `bsql-test-harness-fixture` (the live
`#[bsql::test]` schema-isolation witness over both drivers), and `bsql-corpus`
(a replay corpus pinning engine behaviour against goldens).

## Safety floor

- `#![forbid(unsafe_code)]` at the root of every shipped crate — all
  production code is unsafe-free and un-bypassable. The only `unsafe` lives
  in never-published (`publish = false`) places: the `bsql-devgates` building
  blocks and one justified `std::env::set_var` in each of two consumer-fixture
  trybuild tests — none ships.
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
- SQL identifiers spliced into DDL / COPY (table + schema names) pass through
  `SafeIdent` / `SafeTable` — a private-field newtype with a validate-only
  constructor, so an un-validated identifier cannot be spliced. Identifier
  injection-safety is structural, not a runtime escape pass.

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
cargo test -p bsql-devgates --test test_count              # README test-count doc-vs-reality wall
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

### Measured facts

The two test counts below are **gate-enforced**: the `test_count` devgate
(`tools/devgates/tests/test_count.rs`) runs the exact commands shown and asserts
the README numbers match the live workspace, so a test added or removed without
updating this section fails `cargo test --workspace`. A deliberate change
regenerates them in place with
`BSQL_TEST_COUNT_PIN=overwrite cargo test -p bsql-devgates --test test_count`.
The numbers therefore cannot silently rot.

- **Test functions: 1974** — every `#[test]` / `#[tokio::test]` attribute:
  ```bash
  find . -path ./target -prune -o -path ./.claude -prune -o -name '*.rs' -print0 \
    | xargs -0 grep -hE '^[[:space:]]*#\[(tokio::)?test' | wc -l
  ```
- **`#[ignore]` live suites (need a running database): 218**:
  ```bash
  find . -path ./target -prune -o -path ./.claude -prune -o -name '*.rs' -print0 \
    | xargs -0 grep -hE '^[[:space:]]*#\[ignore' | wc -l
  ```
- **Source LoC** (per shipped crate `src/`; the largest, `bsql-build`, is
  dominated by `src/infer.rs` — 29563 lines, the schema/type-inference engine
  plus a ~13K-line inline `#[cfg(test)]` test module):
  ```bash
  for d in crates/bsql crates/postgres/{proto,core,async,sync} \
           crates/sqlite/driver crates/testkit crates/build crates/query-macros; do
    printf '%-28s %s\n' "$d" \
      "$(find "$d/src" -name '*.rs' -exec cat {} + | wc -l)"
  done
  # bsql 947 · proto 27886 · core 9236 · async 1743 · sync 1575
  # sqlite/driver 2179 · testkit 1005 · build 35036 · query-macros 2252
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
