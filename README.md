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
- **Migration runner** — `conn.run_migrations(source)` applies your migration
  set to a LIVE database, on all three drivers (async PG, sync PG, SQLite).
  **Exactly once, in order** (the same lexicographic-by-name order the build-time
  catalog replay uses), tracked in a `_bsql_migrations` ledger; a re-run is a
  no-op. **Atomic per migration** (the DDL and its ledger row are one
  transaction), and a migration that fails **rolls back and STOPS** the run with
  a classified error naming it. An already-applied migration whose file **changed**
  (checksum drift — exact-bytes, so pin line endings in `.gitattributes` to avoid a
  spurious CRLF-vs-LF drift), or a migration inserted before / deleted from the
  applied set, or a duplicate name, is a **classified error**, never silently
  re-run or skipped. Concurrent runners **serialize** — PostgreSQL via a
  non-blocking `pg_try_advisory_lock` poll (deadlock-free even with `CREATE INDEX
  CONCURRENTLY` migrations), SQLite via `BEGIN IMMEDIATE` + an in-transaction
  ledger re-check — so two instances booting together never double-apply. A
  `-- bsql:no-transaction` migration runs outside a transaction (for `CREATE INDEX
  CONCURRENTLY IF NOT EXISTS` etc.). The migration set is either EMBEDDED in the
  binary (`bsql::embed_migrations!()`, baked by `bsql_build::emit_migrations` — the
  destructive-ack gate above runs on it too, and a migration that manages its own
  transaction (`BEGIN`/`COMMIT`) is a build error) or read from a runtime
  DIRECTORY. `conn.migration_status(source)` and `conn.dry_run_migrations(source)`
  report applied-vs-pending without applying anything.
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

A note on the guarantee boundary: the compile-checked `query!` macro validates
against your migration **files**, not a live database — a schema change applied
by hand in `psql` without a migration file is invisible to the macro by design
(the committed migration set is the source of truth). The migration **runner**
above applies that SAME committed set to the live database, so the files stay the
one source of truth for both the compile-time types AND the applied schema.

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

### Applying migrations

Add `bsql_build::emit_migrations("migrations")` to your `build.rs` to bake the
set into the binary, then apply it at startup:

```rust
// build.rs
fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")?;             // query! catalog (if you use it)
    bsql_build::emit_migrations("migrations")     // the embedded runner set
}
```

```rust
// startup — no filesystem access at run time
const MIGRATIONS: &[(&str, &str)] = bsql::embed_migrations!();

let report = conn.run_migrations(MIGRATIONS)?;    // exactly-once, in order
println!("applied {} new migration(s)", report.applied.len());
```

Or read them from a runtime directory (the ops-friendly path):

```rust
use bsql::pg_sync::MigrationSource;
conn.run_migrations(MigrationSource::directory("migrations"))?;
// Inspect first, without applying:
let status = conn.migration_status(MigrationSource::directory("migrations"))?;
let pending = conn.dry_run_migrations(MigrationSource::directory("migrations"))?;
```

The runner reads identically on all three drivers (`bsql::pg`, `bsql::pg_sync`,
`bsql::sqlite`). See the *Migration runner* bullet above for the full ledger /
atomicity / drift / concurrency guarantees.

### Runtime queries

The typed `query!` path is the flagship, but every driver also exposes a
runtime-SQL surface (`query_sql`, `query_params_one`, `prepare` /
`execute_prepared`, `transaction`, …) and a dynamic 16-byte `Row` backed
by an `Arc`-shared arena (3 heap allocations per whole result — the arena's
data + slots vectors + the shared `Arc`; the result mints `Row` handles
lazily, never an eager `Vec<Row>` — regardless of row count). See the
crate-root docs of `bsql` / `bsql-postgres-async` /
`bsql-postgres-sync` / `bsql-sqlite` for runnable examples.

For a colossal runtime result, `query_each_sql(sql, on_row)` /
`query_each_params(sql, params, on_row)` stream the dynamic row to a callback
ONE AT A TIME in constant memory (the escape from eager `query_sql`), lending
each row as a zero-copy `BorrowedRow` with zero per-row allocation and a
`ControlFlow` early break that drains the remainder so the connection stays
reusable. Available on both PostgreSQL and SQLite with the SAME signature — a
dynamic stream reads identically across backends.

### Error handling — reconnect vs. retry

A resilient consumer needs to tell two failures apart: "the server REJECTED my
query but the connection is FINE" (fix the query, retry on the same connection)
vs. "the connection DIED mid-operation" (reconnect / get a fresh pooled
connection). `DriverError::is_disconnect()` (both PostgreSQL drivers) draws that
line EXACTLY — by construction, never a string-match heuristic:

```rust
match conn.query_sql(sql).await {
    Ok(rows) => rows,
    Err(e) if e.is_disconnect() => reconnect_and_retry().await?, // socket died / backend terminated
    Err(e) => return Err(e),                                     // a query error — fix the query
}
```

It is `true` for a dropped socket / EOF / reset (`Io`), a not-ready connection
whose token a prior fatal error took (`NotReady`), a fatal liveness-deadline
(`Timeout` — a silently vanished peer), and a connection-broken server error
(the `08` class, or the whole `57P` termination subclass — `57P01`/`57P02`
admin/crash shutdown, `57P03` cannot-connect, `57P04` database-dropped, `57P05`
idle-session-timeout). It is `false`
for every per-query server error the connection survives — INCLUDING `57014`
`query_canceled` (a `statement_timeout` or `CancelToken` cancel leaves the
connection reusable), a syntax error, and a constraint violation.

The same decision is CROSS-BACKEND via `BackendError::is_disconnect()`: SQLite
runs in-process, so it never network-disconnects, but the analogue — a broken
handle/file (`SQLITE_IOERR` / `SQLITE_CORRUPT` / `SQLITE_CANTOPEN` /
`SQLITE_NOTADB`, whose recovery is a fresh handle) — reads identically, so a
generic consumer's reconnect/reopen logic is one decision on both backends.

The SERVER-side guardrail against a runaway query is
`ConnectConfig::with_statement_timeout(Duration)` — the complement to the client
`CancelToken`. PostgreSQL aborts any statement exceeding the budget with `57014`
`query_canceled`, and the connection is left reusable (so the abort is NOT a
disconnect — it sheds a slow query without killing the pooled connection). It
rides the existing startup-parameter map (footprint-neutral — no new
`ConnectConfig` field), applies from before the first query, and survives a
pooled connection's `RESET ALL`. `Duration::ZERO` maps to PG's `statement_timeout
= 0` (disabled); a sub-millisecond request rounds up to 1 ms (never down to
0/disabled).

```rust
let config = ConnectConfig::new("db.example.com", "app")
    .with_statement_timeout(Duration::from_secs(5)); // abort any query over 5s
```

### Observability — the `DiagEvent` sink

bsql emits structured operational events through ONE **dep-free** seam: a
consumer-installed callback that receives a borrowed `DiagEvent`. No
`tracing` / `log` / `metrics` in the runtime graph — a consumer chooses their
own logging stack and bsql forces none (an optional `tracing` adapter can wrap
the callback later).

```rust
use bsql::pg::{DiagEvent, Diagnostics, Pool};
use std::time::Duration;

let pool = Pool::builder(config, 16)
    .on_diagnostic(|ev: &DiagEvent<'_>| match ev {
        DiagEvent::SlowQuery { sql, elapsed } => eprintln!("SLOW {elapsed:?}: {sql}"),
        DiagEvent::ServerNotice { severity, message, .. } => eprintln!("{severity}: {message}"),
        other => eprintln!("{other:?}"),
    })
    .slow_query_threshold(Duration::from_millis(100))
    .build();
```

A standalone connection opts in with `Connection::connect_with(config, &diag)`
(or `set_diagnostics`); a pool installs the same configuration on every
connection it mints. Diagnostics is **NOT** a `ConnectConfig` field — the
152-byte config footprint is untouched.

- **Zero-cost when off.** An unset sink is a single never-taken `if let Some`
  branch at each COLD lifecycle boundary — no event built, no wire parsed, no
  `Instant::now`, no allocation. The events fire only at cold boundaries (a
  completed query's timing, a received `NOTICE`, a pool checkout, a connect), so
  the per-row hot path is untouched — the `next_event` codegen ceiling is
  byte-identical, the deliberate distinction from a per-row observation seam.
- **No PII by default.** A `DiagEvent` never carries a bound parameter VALUE — a
  slow-query event carries the SQL TEXT (or a digest a consumer computes from
  it), never the values a placeholder stood for.
- **A sink can do anything — the driver absorbs it.** Every sink call is wrapped
  in `catch_unwind`, so a panicking callback is contained (never poisons a
  connection or aborts the process); pool events run outside the pool lock, so a
  sink may safely call `pool.stats()`; and a diagnostic emitted from WITHIN a sink
  (a self-slow query, a re-entrant `pool.get()`) is silently suppressed, so a
  self-emitting sink can never recurse into a stack overflow.
- **Slow-query covers the flagship.** `SlowQuery` fires for the compile-checked
  `query!` verbs (`query`/`query_one`/`query_opt`/typed `execute`) AND the dynamic
  SQL verbs, only for a query that COMPLETED (an errored/cancelled one is not
  reported); streaming verbs are excluded by design (a stream's duration is
  consumer iteration time).
- **Events** (`#[non_exhaustive]`): `ServerNotice` (a `RAISE NOTICE`/`WARNING` —
  the primary PL/pgSQL log channel, formerly silently dropped), `SslDowngrade`,
  `SlowQuery` (timing gated behind a threshold so the off path reads no clock),
  `PoolAcquireTimeout` / `PoolConnectionEvicted` (plus counters via
  `Pool::stats()`), `ParameterStatus`, `MigrationLockWaiting` /
  `MigrationApplying` / `MigrationApplied`, and `PreparedCacheSelfHeal`.

### Connection-pool robustness

The pool (both drivers) is bounded, self-resetting, and FIFO-fair; beyond that it
hardens the connection LIFECYCLE:

- **Graceful shutdown — `Pool::close(self)`.** Dropping a pool drops each pooled
  connection's socket bare (an RST/FIN with no protocol `Terminate`), and
  PostgreSQL logs an "unexpected EOF on client connection" per connection — an
  error-log flood at shutdown for a large pool. `pool.close()` instead sends a
  protocol `Terminate` to every currently-IDLE connection so the server sees a
  CLEAN disconnect, then closes each socket. It CONSUMES the pool, so
  use-after-close is a compile error, and each `Terminate` is BOUNDED by the
  connection's `connect_timeout` (a black-hole peer cannot hang the drain for the
  kernel's `tcp_retries2` budget, ~15 min) — best-effort, so a single dead peer
  never stalls the rest of the drain.
- **TCP keepalive by default.** Every TCP connection enables `SO_KEEPALIVE` (idle
  ~60 s, interval ~10 s) right after connect, so a silently-vanished peer on an
  IDLE connection is eventually detected by the kernel — matching libpq, which
  enables keepalives by default. No config knob (a dead-peer probe is near-free on
  a healthy connection and near-universally wanted); TCP-only (a unix socket has
  no keepalive). Set through `socket2`'s SAFE borrowed-fd API, so the driver crates
  stay `#![forbid(unsafe_code)]`.
- **`max_lifetime` + `idle_timeout` (a LAZY reaper).**
  `Pool::builder(config, max).max_lifetime(Some(dur)).idle_timeout(Some(dur))`
  bounds how long a pooled connection lives (age since it was established) and how
  long it may sit idle (since it last returned). At CHECKOUT, before a reused
  connection is handed out, one that exceeds either bound is gracefully closed
  (the bounded `Terminate` above) and REPLACED with a fresh connection — so a
  long-lived pool rotates connections (bounding per-backend memory growth, letting
  a rolling credential/DNS change take effect) and a quiet pool sheds connections
  the server may have already timed out. The reaper is LAZY (checked at checkout),
  so it adds NO background timer thread/task — chosen over a background reaper
  (extra runtime dependency + lifecycle, and it cannot even spawn from a pool
  built outside a runtime) and over reap-on-return (which cannot catch a
  connection that ages past a bound while sitting idle). Both bounds default to
  `None` (disabled) — no behaviour change for an existing pool, and ZERO timing
  work per checkout when disabled (the clock read is gated behind
  `max_lifetime.is_some() || idle_timeout.is_some()` via `&&` short-circuit, and
  the return-time restamp reads the clock only when `idle_timeout` is set). A reap
  emits `DiagEvent::PoolConnectionEvicted` and increments the `Pool::stats()`
  eviction counter.

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

## Platform support

- **64-bit Linux, macOS, and Windows** (`x86_64` and `aarch64`). The footprint
  pins assert exact `size_of` / `align_of` for 64-bit pointers, so a 64-bit
  target is required — a non-64-bit build (i686 / wasm32 / 32-bit ARM) is a loud
  `compile_error!`, never a silently-wrong layout.
- **TCP transport works on every platform.** The **unix-domain-socket** transport
  (an absolute-path host) is **unix-only** — it is gated behind `#[cfg(unix)]`,
  because `std::os::unix::net::UnixStream` does not exist on Windows. A
  unix-socket host requested on a non-unix target is a classified
  `DriverError::Config` at connect (`use a TCP host`), never a silent fallback or
  a panic. So a Windows deployment uses TCP; a unix-socket deployment is Linux /
  macOS.
- **TLS on Windows / when cross-compiling needs a C toolchain.** The default-on
  `tls` feature pulls `ring`, which compiles C — cross-compiling it (e.g. a
  Windows target built on a macOS/Linux host) needs the *target's* C toolchain,
  which `rustup target add <triple>` does NOT install. A `default-features =
  false` (TLS-off) build is pure Rust and cross-compiles with only the target's
  prebuilt `std`; add TLS back with the target's C cross-toolchain in place. A
  native build on each platform (with its own C compiler) builds `ring` normally.
- **Local regression guard.** With no CI, the `cross_platform` devgate
  (`tools/devgates/tests/cross_platform.rs`) runs `cargo check` for the Windows
  and Linux targets (pure-Rust, `--no-default-features`) whenever those targets
  are installed, so an accidental unconditional `use std::os::unix::…` is caught
  on the dev's own machine. It skips (passes) any target not installed, so a
  developer who has not run `rustup target add …` never gets a false red.

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
cargo test -p bsql-devgates --test cross_platform          # Windows/Linux cross-target regression wall (skips absent targets)
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

- **Test functions: 2297** — every `#[test]` / `#[tokio::test]` attribute:
  ```bash
  find . -path ./target -prune -o -name .claude -prune -o -name '*.rs' -print0 \
    | xargs -0 grep -hE '^[[:space:]]*#\[(tokio::)?test' | wc -l
  ```
- **`#[ignore]` live suites (need a running database): 303**:
  ```bash
  find . -path ./target -prune -o -name .claude -prune -o -name '*.rs' -print0 \
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
