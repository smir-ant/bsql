# bsql

**Compile-time-safe SQL for Rust — PostgreSQL and SQLite, async and sync.**

Write real SQL. It is checked against your actual schema during `cargo build` —
table names, column names, types, nullability. **If it compiles, the query is
correct.** No DSL, no method chains, no runtime "column not found".

> **1.0.0-alpha** — stable in shape, early in life; expect a few more alpha
> iterations before a full 1.0. Built with [Claude Code](https://claude.com/claude-code)
> (design first, review second, implementation third).

```rust
// migrations/0001_init.sql →  CREATE TABLE users (id int PRIMARY KEY, email text NOT NULL);

// Typed at build time against the migration above.
// `SELECT nope FROM users` would be a compile error, not a runtime surprise.
bsql::query!(UserById, "SELECT id, email FROM users WHERE id = $1");

let user = conn.query_one::<UserById>((42_i32,)).await?;
// user.id: i32   user.email: String     (a nullable column would be Option<T>)
```

## Why bsql

- **If it compiles, the SQL is correct.** Every query is validated at build time
  against the schema your migration `*.sql` files describe — names, types, and
  nullability. Wrong column, wrong type, typo → the build fails.
- **One query function, and it always checks.** There is no unchecked escape
  hatch. In sqlx a missing `!` (`query()` vs `query!()`) silently skips
  validation; in bsql the unchecked version *does not exist*, so you cannot write
  it by accident.
- **Pure SQL text — no builder.** CTEs, JOINs, window functions, subqueries. The
  `.filter().eq()` combinator paradigm was tried during the rebuild and
  deliberately reverted. If PostgreSQL or SQLite supports it, you just write it.
- **Async and sync are both first-class.** Both drivers plug the same socket into
  ONE transport-generic core, so parity is a *compiler guarantee*, not
  hand-maintained twins. The sync driver drops tokio entirely — pure `fn`, no
  async runtime. Switch backends with one line in `Cargo.toml`.
- **PostgreSQL and SQLite, same macro.** SQLite is a full peer, not a text
  wrapper: the same `query!` carrier runs on both, decoding into the same typed
  records — and SQLite verifies each value's storage class at runtime (a mismatch
  is a classified error, never a silent coercion).
- **Tiny footprint.** ~1.6–1.8 MB peak memory for a real workload — **the leanest
  client measured**, leaner than a C/libpq client (~7.5×) and Go/pgx (~10×), and
  ~3.6× under the Rust field — and the whole TLS/SCRAM stack is feature-gated, so a
  localhost / trust-auth build is a handful of crates.
- **`#![forbid(unsafe_code)]` on every shipped crate.** No `unwrap`/`expect` in
  production code. NULL is `Option<NonZeroU32>`, not a sentinel. The hot decode
  path is proven panic-free and byte-stable by a machine-checked codegen gate.
- **Things nobody else does** — automatic N+1 detection, a build-time
  destructive-migration gate, Rust types generated *from your migrations*, typed
  binary `COPY`, and sub-millisecond schema-per-test isolation. See
  [**What makes it different**](#what-makes-it-different).

## Performance

[**You need to see this** 🫢](https://github.com/smir-ant/bsql/blob/bench/README.md)
— **seven clients across four languages** (bsql, C/libpq, Go/pgx, tokio-postgres,
sqlx, diesel), PostgreSQL over loopback TCP, full methodology and captured logs so
you don't have to trust the table.

The short version, measured on an Apple-silicon laptop over the same PostgreSQL:
the **blocking** driver `bsql::pg_sync` is the fastest of the whole field — it
**beats C/libpq** on every read (the true apples-to-apples comparison, both
synchronous). The **async** driver is on par with C on point reads (within the
~1 µs an async runtime spends parking a would-block read, which a blocking client
does not) and faster on larger results — and it is by a wide margin the fastest
*async* driver (tokio-postgres ~1.5×, sqlx, diesel, Go/pgx behind it). On memory
bsql is the **leanest of all** — ~1.6–1.8 MB peak, ~3.6× under the Rust field,
~7.5× under a C/libpq client, ~10× under Go/pgx. Don't take our word for it —
`git switch bench && cargo bench`.

## Quick start

<details>
<summary><b>PostgreSQL</b></summary>

```toml
# Cargo.toml
[dependencies]
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

```rust
use bsql::pg;

bsql::query!(UserById, "SELECT id, email FROM users WHERE id = $1");

#[tokio::main]
async fn main() -> Result<(), bsql::pg::DriverError> {
    let cfg = pg::ConnectConfig::from_dsn("postgres://user@localhost/mydb")?;
    let mut conn = pg::Connection::connect(cfg).await?;

    let user = conn.query_one::<UserById>((42_i32,)).await?;
    println!("{} <{}>", user.id, user.email);
    Ok(())
}
```

Blocking driver? Swap the feature to `postgres-sync`, use `bsql::pg_sync`, and
drop the `async`/`.await` — the `query!` carriers and record types are identical.
</details>

<details>
<summary><b>SQLite</b></summary>

Enable `features = ["macros", "sqlite", "macros-sqlite"]` (the last one is the
build-time conformance oracle — it re-checks each `query!` against a real SQLite
replay of your migrations). The same carrier runs on `bsql::sqlite::Connection`
with the same `query` / `query_one` / `query_opt` / `query_each` verbs and the
same typed records. A PostgreSQL-only query (a `uuid` column, say) simply does
not implement the SQLite trait, so running it on SQLite is a *compile* error.
</details>

## What makes it different

<details>
<summary><b>N+1 detection, for free</b> (feature <code>n1-detect</code>)</summary>

The classic anti-pattern — the same `query!` run once per row of a prior result,
from the same source line — is detected and surfaced through `conn.n1_report()`
with the offending SQL, file, line, and count. Diagnostics-only (never batches,
blocks, or alters a result) and **zero-cost when off**: a production build
compiles no detector field, no branch, no `#[track_caller]` overhead. Same on
PostgreSQL and SQLite.
</details>

<details>
<summary><b>A migration that destroys data won't compile silently</b></summary>

`DROP TABLE`, `ALTER TABLE … DROP COLUMN`, `DROP SCHEMA … CASCADE`, `TRUNCATE`,
`DROP DATABASE` in a migration are a **build error** unless a co-located
`-- bsql:ack-destructive` comment acknowledges the loss. Accidental data
destruction is caught at compile time, not in production.
</details>

<details>
<summary><b>Rust types generated from your migrations</b></summary>

`CREATE TYPE mood AS ENUM ('happy','sad')` in a migration plus `bsql::user_types!()`
generates `enum Mood { Happy, Sad }` — no derives, no hand-maintained name. Rename
or delete a variant in a later migration and any code that named the old one stops
compiling. Composites (`CREATE TYPE addr AS (...)`) generate a struct; domains are
transparent to their base type. Drift is a build error, by construction — a
capability no other Rust SQL library offers, because only bsql parses your
migration set at build time.
</details>

<details>
<summary><b>Safe-by-construction bulk load — typed binary <code>COPY</code></b></summary>

`copy!(Ins, "table", (cols))` validates the target table + columns + types against
the same catalog `query!` reads, then `copy_in_typed::<Ins>(rows)` streams each row
as a *binary* `COPY` in constant memory. No text to mis-escape (the classic `COPY`
footgun), injection-safe by construction, and faster than the text path on both
sides. The raw text `copy_in` stays as the escape hatch.
</details>

<details>
<summary><b>Schema-per-test isolation in sub-millisecond</b> (feature <code>test-harness</code>)</summary>

```rust
#[bsql::test]
async fn creates_a_user(conn: &mut bsql::pg::Connection) {
    conn.query_sql("CREATE TABLE users (id int)").await.unwrap();  // in an ISOLATED schema
}   // schema auto-dropped, even if the test panics
```

Each test runs in its own freshly-created PostgreSQL schema (a `CREATE SCHEMA`, not
a whole database), so `cargo test`'s default parallelism never leaks state — and
teardown runs even on panic. Works over the async *and* the blocking driver (the
attribute picks by `async`-ness).
</details>

<details>
<summary><b>Migrations, applied at runtime — atomic, ordered, exactly-once</b></summary>

`conn.run_migrations(source)` applies your set to a live database on all three
drivers, exactly once and in order, tracked in a `_bsql_migrations` ledger. Each
migration + its ledger row is one transaction; a failure rolls back and stops with
a classified error. An edited-after-apply migration (checksum drift), a reorder, or
a delete is a classified error — never silently re-run. Concurrent boots serialize
(PostgreSQL via a non-blocking advisory-lock poll that stays deadlock-free even with
`CREATE INDEX CONCURRENTLY`; SQLite via `BEGIN IMMEDIATE` + a re-check).
</details>

<details>
<summary><b>Production-grade connection lifecycle</b></summary>

Pooling with health-gated checkout, graceful shutdown (`Pool::close`), TCP
keepalive, `max_lifetime` / `idle_timeout` reaping, `is_disconnect()`
reconnect-vs-retry classification, server-side `statement_timeout`, and a
client-side liveness window so a black-holed in-flight query is *bounded*, not a
15-minute hang. Structured diagnostics via a dep-free `DiagEvent` sink (surface
`RAISE NOTICE`, slow queries, pool stats, migration progress) — zero-cost when no
sink is installed.
</details>

<details>
<summary><b>Bring your own types — external-crate bridges</b></summary>

`query!` can decode a column straight into `chrono::DateTime`, `uuid::Uuid`,
`serde_json::Value`, or any type you like, with bsql depending on and forcing
*nothing*: your `build.rs` registers `.bridge(pg_type, target, converter)` and you
supply one infallible free-function converter. The orphan-proof seam.
</details>

## How it compares

Speed is half the story; the other half is what the driver lets you *do*. An honest
side-by-side with the Rust field — competitors get credit for what they have (✅ full,
◐ partial, ❌ none).

<details>
<summary><b>Capability matrix — bsql vs tokio-postgres / sqlx / diesel</b></summary>

| capability | bsql | tokio-postgres | sqlx | diesel |
|---|:---:|:---:|:---:|:---:|
| Compile-time SQL check vs real schema | ✅ `query!` replays migrations | ❌ unchecked string | ✅ `query!` | ◐ typed DSL only |
| …with **no live DB / cache** at build | ✅ offline from migration files | — | ❌ needs DB or committed cache | ◐ `schema.rs` usually from a DB |
| Plain SQL text (not a DSL) | ✅ | ✅ (unchecked) | ✅ | ❌ query-builder DSL |
| **N+1 query detection** | ✅ `conn.n1_report()`, zero-cost off | ❌ | ❌ | ❌ |
| Typed/safe **binary COPY** | ✅ `copy_in_typed`, compile-checked | ◐ hand-wired binary | ◐ text COPY only | ❌ no COPY |
| Build-time **migration safety** gate | ✅ destructive-op ack + checksum drift | ❌ | ◐ checksum drift | ◐ up/down by version |
| First-class **sync AND async**, one API | ✅ shared `Core<S>` | ❌ async only | ❌ async only | ❌ sync only |
| **Zero-per-row-alloc** streaming | ✅ `query_each`, O(1) RAM, 0 alloc/row | ◐ streams, allocs/row | ◐ streams, allocs/row | ◐ default materializes |
| Out-of-band query cancellation | ✅ detached `CancelToken` | ✅ `cancel_token()` | ◐ cancel-on-drop | ❌ |
| `#![forbid(unsafe_code)]` (shipped crates) | ✅ every crate | ◐ some `unsafe` | ◐ some `unsafe` | ❌ links libpq (C FFI) |
| Unix-domain socket transport | ✅ (~2.4–2.9× faster than TCP locally) | ✅ | ✅ | ✅ (libpq) |
| Same `query!` on **PostgreSQL and SQLite** | ✅ one carrier, both backends | ❌ PG only | ◐ separate `Sqlite`/`Pg` types | ◐ separate backends |

</details>

## Safety floor

- `#![forbid(unsafe_code)]` on every shipped crate; `deny(unwrap_used, expect_used)`.
- The compile-checked path pins each column's PostgreSQL OID at build time — a
  wrong Rust type for a column is a compile error, and a wrong parameter type is
  rejected loudly on **every** binding surface (typed at compile, dynamic at the
  server, prepared at the client, SQLite by storage class) — never a silent
  byte-for-byte reinterpretation.
- The wire decoders are proven total (no panic on *any* input) by a dep-free
  fuzz gate; the inbound hot dispatch is proven panic-free and byte-stable by a
  codegen gate. NULL is `Option<NonZeroU32>`; SQL identifiers spliced into DDL go
  through a validate-only `SafeIdent` newtype.
- 64-bit Linux / macOS / Windows. TCP everywhere; unix sockets on unix.

## Verification

No CI — by design. All gates run locally via `cargo` and a `bsql-devgates` crate
(dependency-frontier pin, runtime-vs-build boundary pin, intra-doc-link wall,
cross-platform check, hot-path codegen ceiling, decoder fuzz):

```bash
cargo clippy --workspace --all-targets    # lint wall — 0 warnings
cargo test   --workspace                  # unit + integration (offline)
cargo test -p bsql-devgates               # the pins + walls
# live suites are #[ignore] and need a local PostgreSQL:
cargo test -p bsql-postgres-async --test sq_live -- --ignored
```

Benchmarks + their methodology live on the **`bench` branch**
(`git switch bench && cargo bench`), kept off the code branch so a
normal clone stays lean.

## About

Built with [Claude Code](https://claude.com/claude-code). Design first,
architectural review second, implementation third — every slice reviewed
adversarially before it landed.

~2,400 tests across the workspace — unit, integration, compile-fail (`trybuild`),
live-database, dependency-free fuzz, and machine-checked codegen gates. Not just
tests that the code works, but tests that *broken* code is rejected at compile
time. The whole thing went through nine successive deep audits and a real-load
fault-injection pass (TLS byte-fragmentation, mid-stream faults, million-row
streams, concurrency) — the sort of thing that catches what green unit tests hide.

Don't follow the author's name. Don't assume an older library is automatically the
safer bet. Run the benchmarks yourself, read the tests, check the code.

`CLAUDE.md` is the exhaustive engineering reference (conventions, layout, every
gate and safety invariant) — read it if you want the full picture.

## License

MIT OR Apache-2.0, at your option.
