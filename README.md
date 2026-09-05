# bsql

**Compile-time-safe SQL for Rust — PostgreSQL and SQLite, async and sync.**

Write real SQL. It is checked at `cargo build` against the schema your migration
files define — table names, column names, types, nullability. **If it compiles,
the query is correct.** No DSL, no method chains, no runtime "column not found".

> **1.0.0-alpha.4** — stable in shape, early in life; expect a few more alpha
> iterations before a full 1.0. On crates.io as a **pre-release**: pin
> `bsql = "1.0.0-alpha.4"` (a bare `cargo add bsql` resolves the older stable
> `0.27` — a different, unrelated library). Built with
> Claude and Gemini in adversarial dual-LLM pair engineering (collaborating across
> architecture synthesis, red-team auditing, competitive benchmarking, and
> zero-overhead implementation).

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
- **One query function, and it always checks — no *accidental* unchecked path.**
  In sqlx a missing `!` (`query()` vs `query!()`) silently skips validation. bsql
  has a raw-SQL escape hatch too, but it is deliberate and named apart: the
  unchecked verbs carry a distinct suffix (`query_raw` for raw text, `query_params`
  for raw text plus runtime parameters), so you opt into unchecked SQL on purpose —
  never by forgetting a `!`.
- **Pure SQL text — no builder.** CTEs, JOINs, window functions, subqueries — no
  `.filter().eq()` method chains to learn. If PostgreSQL or SQLite supports it, you
  just write it.
- **Async and sync are both first-class.** Both drivers plug the same socket into
  ONE transport-generic core, so parity is a *compiler guarantee*, not
  hand-maintained twins. The sync driver drops tokio entirely — pure `fn`, no
  async runtime. Switch async↔sync by swapping one feature line (and dropping
  `.await`).
- **PostgreSQL and SQLite, same macro.** The same `query!` carrier runs on both
  for typed *reads*, decoding into the same records — and SQLite verifies each
  value's storage class at runtime (a mismatch is a classified error, never a
  silent coercion). Typed *writes* through `query!` (`execute` / `execute_batch` /
  `query_batch`) are a PostgreSQL capability; on SQLite the typed flagship is
  read-only, and dynamic writes (`execute_raw` / `execute_params`) work on both.
- **Tiny footprint.** ~1.7–1.8 MB peak memory for a real PostgreSQL workload —
  **the leanest client measured there** (and a near-tie with raw C on SQLite), ~8×
  under a C/libpq client, ~10× under Go/pgx, and ~3.6× under the nearest Rust
  client — and the whole TLS/SCRAM stack is feature-gated, so a localhost /
  trust-auth build is a handful of crates.
- **`#![forbid(unsafe_code)]` on every shipped crate.** No `unwrap`/`expect` in
  production code. NULL is `Option<NonZeroU32>`, not a sentinel. The hot decode
  path is proven panic-free and byte-stable by a machine-checked codegen gate.
- **Things nobody else does** — automatic N+1 detection, a build-time
  destructive-migration gate, Rust types generated *from your migrations*, typed
  binary `COPY`, and sub-millisecond schema-per-test isolation. See
  [**What makes it different**](#what-makes-it-different).

## Performance

> **[Explore the Full Cross-Language Benchmark Suite & Methodology 🫢](https://github.com/smir-ant/bsql/blob/bench/README.md)**
> Seven clients across four languages (bsql, C/libpq, C/sqlite3, Go/pgx, Go/mattn, tokio-postgres, sqlx, diesel).

### 1. PostgreSQL — Latency & Memory Footprint

Point read latency (`SELECT by-PK`, 1 connection, lower is better):

```text
bsql (sync)      [████                    ] 24.6 µs   (1.0x)  1.69 MB
bsql (async)     [████▌                   ] 26.2 µs   (1.1x)  1.80 MB
C / libpq        [████                    ] 25.5 µs   (1.0x) 13.25 MB
sqlx             [████▌                   ] 27.9 µs   (1.1x)  6.73 MB
tokio-postgres   [██████▌                 ] 39.8 µs   (1.6x)  6.50 MB
diesel           [███████                 ] 41.3 µs   (1.7x)  7.01 MB
Go / pgx         [█████████               ] 52.1 µs   (2.1x) 16.81 MB
```

### 2. SQLite — Pure C-Level Speed with Type Safety

Point read latency (`SELECT by-PK prepared`, lower is better):

```text
C / sqlite3      [██                      ] 1.51 µs  (1.0x)   3.83 MB
bsql             [██                      ] 1.58 µs  (1.0x)   3.86 MB
diesel           [██▌                     ] 1.87 µs  (1.2x)   4.08 MB
Go / mattn       [████                    ] 3.25 µs  (2.2x)  16.94 MB
sqlx             [████████                ] 6.16 µs  (4.1x)   4.59 MB
```

- On 10–100 row queries and batch inserts (`751 µs` vs `909 µs`), `bsql` is **15–20% faster than raw C**.
- On 10,000 rows, `bsql` (**1.02 ms**) is **14× faster than sqlx** (**14.19 ms**), which hops threads on every row.
- Peak RSS is a virtual tie with raw C (**3.86 MB** vs **3.83 MB**).

<details>
<summary><b>Where the Real Multipliers Are (Orders of Magnitude, Not Fractions of a Percent)</b></summary>

In microbenchmarks (single point reads on loopback TCP), modern OS kernel TCP stacks and DB query planning consume ~85–90% of the ~25 µs round trip. Driver micro-optimizations shave ~200–300 ns (~1–2% on loopback, invisible over a 1 ms network ping). Anyone promising 10× on a single SELECT is selling snake oil.

**Where `bsql` delivers 10× to 1,000× architectural multipliers:**

1. **Aggregated Binary `COPY IN` (~1,250× fewer wire frames & syscalls)**:
   Rows are packed into adaptive 64 KiB chunks: **5,000 rows stream in 4 TCP frames** instead of 5,000 separate frames.
2. **Constant-Memory Streaming (~524× less RAM than tokio-postgres, ~254× less than libpq)**:
   5,000,000 rows stream in a flat **1.75 MB peak RSS** with **5 total heap allocations** across the entire stream (0.000001 allocs/row). Materializing competitors consume **918 MB** (tokio-postgres) and **445 MB** (libpq).
3. **Zero-Allocation Column Slots (`SlotsScratch`)**:
   Stack-allocated slots (`[ColSlot; 16]`) mean queries with $\le 16$ columns perform **0 heap allocations** for slot management.
4. **Instant Early-Break Recovery (>30,000× faster client reclaim)**:
   Terminating early from a 10M-row stream drops the socket via TCP RST after 128 frames, recovering the connection in **<1 ms** instead of hanging for 30+ seconds.
5. **High-Concurrency Tail Latency (p99 / p99.9)**:
   At 128 concurrent connections, `bsql` delivers **129,219 QPS** with lowest p99 (**1.58 ms**) and **~32% lower extreme tail latency** (p99.9 = 2.25 ms vs tokio-pg 3.33 ms).
6. **Connection Pool Thundering Herd Defense**:
   Rate-limited dials (`max_concurrent_handshakes`) and double-checked idle reuse prevent DB CPU collapse on fleet reconnection.
7. **Machine-Checked Hardware & Allocator Invariants**:
   Hot path pinned at **761 instructions** (0 panics, 0 unwinds), eager query allocations pinned at **14**, session reset at **11**.
</details>

<details>
<summary><b>Full PostgreSQL & SQLite Benchmark Tables</b></summary>

#### PostgreSQL (Latency in µs, lower is better)
| scenario | bsql (sync) | bsql | C/libpq | sqlx | tokio-pg | diesel | Go/pgx |
|---|---|---|---|---|---|---|---|
| SELECT by-PK (1) | **24.6** <kbd>x1</kbd> | 26.2 <kbd>x1.1</kbd> | 25.5 <kbd>x1.0</kbd> | 27.9 <kbd>x1.1</kbd> | 39.8 <kbd>x1.6</kbd> | 41.3 <kbd>x1.7</kbd> | 52.1 <kbd>x2.1</kbd> |
| SELECT 10 rows | **37.2** <kbd>x1</kbd> | 40.0 <kbd>x1.1</kbd> | 39.7 <kbd>x1.1</kbd> | 42.6 <kbd>x1.1</kbd> | 53.0 <kbd>x1.4</kbd> | 55.4 <kbd>x1.5</kbd> | 74.8 <kbd>x2.0</kbd> |
| SELECT 100 rows | **49.7** <kbd>x1</kbd> | 52.6 <kbd>x1.1</kbd> | 54.7 <kbd>x1.1</kbd> | 70.1 <kbd>x1.4</kbd> | 72.0 <kbd>x1.4</kbd> | 78.6 <kbd>x1.6</kbd> | 78.5 <kbd>x1.6</kbd> |
| SELECT 1000 rows | **232.7** <kbd>x1</kbd> | 234.0 <kbd>x1.0</kbd> | 250.3 <kbd>x1.1</kbd> | 287.2 <kbd>x1.2</kbd> | 279.9 <kbd>x1.2</kbd> | 295.5 <kbd>x1.3</kbd> | 259.5 <kbd>x1.1</kbd> |
| INSERT single | 37.5 <kbd>x1.0</kbd> | 43.1 <kbd>x1.2</kbd> | **37.4** <kbd>x1</kbd> | 45.3 <kbd>x1.2</kbd> | 43.2 <kbd>x1.2</kbd> | 44.0 <kbd>x1.2</kbd> | 58.3 <kbd>x1.6</kbd> |
| JOIN + GROUP BY | **2.99 ms** <kbd>x1</kbd> | 3.01 ms <kbd>x1.0</kbd> | 3.04 ms <kbd>x1.0</kbd> | 3.03 ms <kbd>x1.0</kbd> | 3.05 ms <kbd>x1.0</kbd> | 3.03 ms <kbd>x1.0</kbd> | 3.05 ms <kbd>x1.0</kbd> |

#### SQLite (Latency in µs, lower is better)
| scenario | bsql | C/sqlite3 | diesel | Go/mattn | sqlx |
|---|---|---|---|---|---|
| by-PK (prepared) | 1.58 <kbd>x1.0</kbd> | **1.51** <kbd>x1</kbd> | 1.87 <kbd>x1.2</kbd> | 3.25 <kbd>x2.2</kbd> | 6.16 <kbd>x4.1</kbd> |
| 10 rows (prepared) | **4.69** <kbd>x1</kbd> | 5.87 <kbd>x1.3</kbd> | 9.91 <kbd>x2.1</kbd> | 9.96 <kbd>x2.1</kbd> | 14.05 <kbd>x3.0</kbd> |
| 100 rows (prepared) | **13.48** <kbd>x1</kbd> | 14.41 <kbd>x1.1</kbd> | 29.70 <kbd>x2.2</kbd> | 71.81 <kbd>x5.3</kbd> | 112.37 <kbd>x8.3</kbd> |
| 1000 rows (prepared) | 100.3 <kbd>x1.0</kbd> | **97.7** <kbd>x1</kbd> | 226.7 <kbd>x2.3</kbd> | 671.2 <kbd>x6.9</kbd> | 1.39 ms <kbd>x14.2</kbd> |
| 10000 rows (prepared) | 1.02 ms <kbd>x1.1</kbd> | **943.9** <kbd>x1</kbd> | 2.19 ms <kbd>x2.3</kbd> | 6.82 ms <kbd>x7.2</kbd> | 14.19 ms <kbd>x15.0</kbd> |
| INSERT single (prepared) | **18.43** <kbd>x1</kbd> | 18.65 <kbd>x1.0</kbd> | 21.01 <kbd>x1.1</kbd> | 23.65 <kbd>x1.3</kbd> | 27.89 <kbd>x1.5</kbd> |
| INSERT batch (100) | **751** <kbd>x1</kbd> | 909 <kbd>x1.2</kbd> | 1.07 ms <kbd>x1.4</kbd> | 1.07 ms <kbd>x1.4</kbd> | 1.54 ms <kbd>x2.1</kbd> |
| Subquery (prepared) | 28.58 <kbd>x1.0</kbd> | **27.48** <kbd>x1</kbd> | 42.87 <kbd>x1.6</kbd> | 69.22 <kbd>x2.5</kbd> | 113.97 <kbd>x4.1</kbd> |
| JOIN + aggregate | 34.64 ms <kbd>x1.0</kbd> | 34.77 ms <kbd>x1.0</kbd> | 34.36 ms <kbd>x1.0</kbd> | **34.12 ms** <kbd>x1</kbd> | 34.52 ms <kbd>x1.0</kbd> |

</details>

## Quick start

<details>
<summary><b>PostgreSQL</b></summary>

```toml
# Cargo.toml  (alpha is a pre-release, so pin the exact version)
[dependencies]
bsql = { version = "1.0.0-alpha.4", features = ["macros", "postgres-async"] }

[build-dependencies]
bsql-build = "1.0.0-alpha.4"
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

**Runnable examples.** The [`examples/`](examples/) directory is a heavily-commented
tour that doubles as a usage guide — one focused program per feature (basic CRUD,
cross-backend, `params` vs `raw`, joins/aggregates, migrations, generated types,
external-type bridges, N+1 detection, typed COPY, pipelining/batch, streaming,
transactions, pooling, schema-per-test). The SQLite ones run with zero setup:
`cargo run -p bsql-examples --bin basic_sqlite`.

## What makes it different

<details>
<summary><b>N+1 detection, for free</b> (feature <code>n1-detect</code>)</summary>

The bug ORMs quietly encourage — one query per row of a previous result. bsql
catches it *for* you:

```rust
let authors = conn.query::<AllAuthors>(()).await?;
for a in authors.iter() {
    // one query per author, from the same source line — the anti-pattern
    let books = conn.query::<BooksByAuthor>((a.id,)).await?;
}

if let Some(n1) = conn.n1_report() {
    // N1Report { sql: "SELECT … FROM books WHERE author_id = $1",
    //            file: "src/catalog.rs", line: 42, count: 250 }
    eprintln!("N+1 at {}:{} ran {}×", n1.file, n1.line, n1.count);
}
```

The same `query!` run 25+ times from the same call site *within one logical
operation* is flagged and attributed to the exact source line (via
`#[track_caller]`), with the SQL and repeat count. **Diagnostics-only** — it never
batches, blocks, or alters a result, so a false positive is at most a spurious log.
**Zero-cost when off** (the default): a production build compiles no detector field,
no branch, no `#[track_caller]` cost — the query verbs stay byte-identical. One
shared detector across PostgreSQL *and* SQLite.
</details>

<details>
<summary><b>A migration that destroys data won't compile silently</b></summary>

A `DROP TABLE`, `ALTER TABLE … DROP COLUMN`, `DROP SCHEMA … CASCADE`, `TRUNCATE`, or
`DROP DATABASE` in a migration is a **build error** — refused unless you acknowledge
the data loss on the line above it:

```sql
-- migrations/0007_drop_legacy.sql
-- bsql:ack-destructive
DROP TABLE legacy_sessions;   -- compiles ONLY because of the ack line above
```

Without the `-- bsql:ack-destructive` line, `cargo build` fails and names the
offending statement. So a `DROP` that slipped in during a rebase can't reach
production silently — you either meant it (and said so) or you find out at build
time. The check runs on the same migration AST the schema catalog is built from, and
covers both directory migrations and a set baked into the binary with
`embed_migrations!()`.
</details>

<details>
<summary><b>Rust types generated from your migrations</b></summary>

Declare a type in SQL, get a Rust type for it — no derive, no hand-written name, no
drift:

```sql
-- migrations/0003_types.sql
CREATE TYPE mood AS ENUM ('happy', 'sad');
```
```rust
bsql::user_types!();   // generates:  pub enum Mood { Happy, Sad }

bsql::query!(GetMood, "SELECT mood FROM users WHERE id = $1");
let m: Mood = conn.query_one::<GetMood>((1_i32,)).await?.mood;
```

Rename or delete a variant in a later migration and **every use of the old name
stops compiling** — drift is a build error, by construction. Enum evolution
(`ALTER TYPE … ADD VALUE` / `RENAME VALUE`) is replayed so the generated enum always
matches the files, and variant order mirrors PostgreSQL's, so the derived `Ord`
matches the server's sort. Composites (`CREATE TYPE addr AS (...)`) generate a
`struct`; domains are transparent to their base type. No other Rust SQL library does
this — only bsql parses your migration set at build time.
</details>

<details>
<summary><b>Safe-by-construction bulk load — typed binary <code>COPY</code></b></summary>

The raw text `COPY` path is the classic footgun: you hand-format the data, and one
mis-escaped tab or newline silently corrupts a row. `copy!` removes the text
entirely:

```rust
copy!(LoadUsers, "users", (id, email));    // validated against the catalog

conn.copy_in_typed::<LoadUsers>(
    people.iter().map(|p| (p.id, p.email.as_str()))    // one typed tuple per row
).await?;
```

`copy!` checks the target table, columns and their types against the same catalog
`query!` reads; `copy_in_typed` then streams each row as a *binary* `COPY` in
constant memory. A wrong column type or arity is a **compile error**; an embedded
tab / newline / quote rides the binary field verbatim (nothing to mis-escape); and
it is faster than the text path on both client and server. The raw `copy_in` stays
as the escape hatch for pre-formatted data.
</details>

<details>
<summary><b>Schema-per-test isolation in sub-millisecond</b> (feature <code>test-harness</code>)</summary>

```rust
#[bsql::test]
async fn creates_a_user(conn: &mut bsql::pg::Connection) {
    conn.query_raw("CREATE TABLE users (id int)").await.unwrap();  // in an ISOLATED schema
}   // schema auto-dropped, even if the test panics
```

Each test runs in its own freshly-created PostgreSQL schema (a `CREATE SCHEMA`, not a
whole database — so it's sub-millisecond, not a per-test database spin-up), so
`cargo test`'s default parallelism never leaks state between tests. The isolation
rides the connect-time `search_path`, which survives a pool's `RESET ALL`, so a
pooled connection can't escape its schema. Teardown runs even if the test panics
(the schema is dropped in a `catch_unwind`, then the panic re-raised, so
`#[should_panic]` still works). Write a plain `fn` taking `&mut bsql::pg_sync::Connection`
and the same attribute gives you the blocking driver — it picks by `async`-ness.
</details>

<details>
<summary><b>Migrations, applied at runtime — atomic, ordered, exactly-once</b></summary>

```rust
let report = conn.run_migrations(
    bsql::MigrationSource::directory("migrations")
).await?;
// report.applied — the migrations newly applied this run, in order
```

Applies your set to a live database on all three drivers, exactly once and in
lexicographic order, tracked in a `_bsql_migrations` ledger. Each migration + its
ledger row is **one transaction** — a failure rolls back and stops with a classified
error naming it, later migrations untouched. An edited-after-apply migration
(checksum drift), a reorder, or a deletion is a classified error, never silently
re-run. Concurrent boots serialize: PostgreSQL via a non-blocking advisory-lock poll
that stays deadlock-free even with `CREATE INDEX CONCURRENTLY`; SQLite via
`BEGIN IMMEDIATE` + an in-transaction re-check. Or bake the set into the binary with
`embed_migrations!()` — no filesystem at run time.
</details>

<details>
<summary><b>Production-grade connection lifecycle</b></summary>

```rust
let pool = pg::Pool::builder(cfg, 16)
    .max_lifetime(Some(Duration::from_secs(1800)))
    .idle_timeout(Some(Duration::from_secs(300)))
    .slow_query_threshold(Some(Duration::from_millis(100)))
    .on_diagnostic(|ev| tracing::info!(?ev))   // RAISE NOTICE, slow queries, pool stats…
    .build();

match conn.query_one::<GetUser>((id,)).await {
    Err(e) if e.is_disconnect() => reconnect(),  // connection died → reconnect
    Err(e) => return Err(e),                      // query error → the connection is fine
    Ok(u)  => u,
}
```

Health-gated checkout (a peer that died while idle is swapped transparently, and
`get()` stays *bounded* even on a half-open socket, never the ~15-minute kernel
hang), graceful shutdown (`Pool::close` sends a clean `Terminate` — no server
error-log flood), TCP keepalive, `max_lifetime` / `idle_timeout` reaping,
`is_disconnect()` reconnect-vs-retry classification, server-side `statement_timeout`,
and a client-side liveness window so a black-holed in-flight query is bounded too.
Diagnostics ride a dep-free `DiagEvent` sink — **zero-cost when none is installed**
(no clock read, no event built, hot path untouched).
</details>

<details>
<summary><b>Bring your own types — external-crate bridges</b></summary>

Decode a column straight into a foreign crate's type, with bsql depending on and
forcing *nothing*:

```rust
// build.rs
bsql_build::Catalog::from_migrations("migrations")
    .bridge("timestamptz", "chrono::DateTime<chrono::Utc>", "crate::to_chrono")
    .emit()?;
```
```rust
// your one infallible converter — the orphan-proof seam
pub fn to_chrono(ts: bsql::Timestamptz) -> chrono::DateTime<chrono::Utc> { /* … */ }
```

Now a `timestamptz` column in any `query!` decodes as `chrono::DateTime<Utc>`. The
target type and converter travel as *strings*, so `bsql-build` gains no dependency on
`chrono`. You can't `impl bsql::Cell for chrono::DateTime` (both are foreign — E0117),
but a free function compiles for any foreign target — so this works for `uuid::Uuid`,
`serde_json::Value`, `rust_decimal`, anything.
</details>

## How it compares

Speed is half the story; the other half is what the driver lets you *do*. An honest
side-by-side with the Rust field — competitors get credit for what they have.

<details>
<summary><b>Capability matrix — bsql vs tokio-postgres / sqlx / diesel</b></summary>

Legend: ✅ full · ◐ partial · ❌ none.

| capability | bsql | tokio-postgres | sqlx | diesel |
|---|:---:|:---:|:---:|:---:|
| Compile-time SQL check vs real schema | ✅ `query!` replays migrations | ❌ unchecked string | ✅ `query!` | ◐ typed DSL only |
| …with **no live DB / cache** at build | ✅ offline from migration files | — | ❌ needs DB or committed cache | ◐ `schema.rs` usually from a DB |
| Plain SQL text (not a DSL) | ✅ | ✅ (unchecked) | ✅ | ❌ query-builder DSL |
| **N+1 query detection** | ✅ `conn.n1_report()`, zero-cost off | ❌ | ❌ | ❌ |
| Typed/safe **binary COPY** | ✅ `copy_in_typed`, compile-checked | ◐ hand-wired binary | ◐ text COPY only | ◐ `copy_from`, not compile-checked |
| Atomic typed **pipelining / bulk batch** | ✅ `pipeline` / `execute_batch`, all-or-nothing, ~1 RTT | ◐ untyped pipelining | ❌ | ❌ |
| Build-time **migration safety** gate | ✅ destructive-op ack + checksum drift | ❌ | ◐ checksum drift | ◐ up/down by version |
| First-class **sync AND async**, one API | ✅ shared `Core<S>` | ❌ async only | ❌ async only | ◐ separate `diesel` + `diesel-async` |
| **Zero-per-row-alloc** streaming | ✅ `query_each`, O(1) RAM, 0 alloc/row | ◐ streams, allocs/row | ◐ streams, allocs/row | ◐ default materializes |
| Out-of-band query cancellation | ✅ detached `CancelToken` | ✅ `cancel_token()` | ◐ cancel-on-drop | ❌ |
| `#![forbid(unsafe_code)]` (shipped crates) | ✅ every crate | ◐ some `unsafe` | ◐ some `unsafe` | ❌ links libpq (C FFI) |
| Unix-domain socket — connect to a same-machine DB without the network layer (faster than `localhost`) | ✅ ~2.4–2.9× vs loopback TCP | ✅ | ✅ | ✅ (libpq) |
| Same `query!` on **PostgreSQL and SQLite** | ✅ one carrier, both backends | ❌ PG only | ◐ separate `Sqlite`/`Pg` types | ◐ separate backends |

</details>

## Safety floor

- `#![forbid(unsafe_code)]` on every shipped crate; `deny(unwrap_used, expect_used)`.
- The compile-checked path pins each column's PostgreSQL OID at build time — a
  wrong Rust type for a column is a compile error, and a wrong parameter type is
  rejected loudly on **every** binding surface (typed at compile, dynamic at the
  server, prepared at the client, SQLite by storage class) — never a silent
  byte-for-byte reinterpretation.
- **The build-time check is against your committed migration files** (the
  version-controlled source of truth), not a live-DB introspection that can go
  stale. An out-of-band `ALTER TABLE` applied by hand with no migration file is
  invisible at build time — but a runtime OID guard catches such drift as a
  classified error on the wire, never a silent wrong value.
- **What the checker asks of you in return:** name your result columns (`SELECT *`
  is rejected — the shape must be explicit), and add a cast where an expression's
  type is genuinely ambiguous (a bare `SUM(x)` → `SUM(x)::int8`, an uninferable
  expression → `expr::type`). This is the ergonomic price of typing every column;
  a plain column, a join, `COALESCE`, and a cast-annotated aggregate all infer
  with no annotation.
- **What "correct" means, precisely.** The compile check proves the query's
  *shape*: relation and column names, result-column types, nullability, and the
  parameter type OIDs. It does **not** type-check the SQL's operand / cast / value
  semantics — a `WHERE bool_col = 5`, a nonsensical `CAST`, or a value-domain
  violation is caught by **PostgreSQL itself at runtime**, loudly and classified
  (`42883` / `42846` / `22P02`), never a silent wrong result. So "if it compiles,
  the query is correct" means its *shape and types* are correct against your
  schema; the server stays the authority on operand semantics.
- **Non-UTF-8 text is a classified error, never a panic.** A `text` / `varchar`
  column carrying non-UTF-8 bytes (e.g. a `SQL_ASCII` database) decodes to a loud
  `DecodeError` (`NonUtf8`), not a panic and not a lossy substitution — read such
  a column as `bytea` if it actually holds binary data.
- **Large results are bounded and streamable.** An eager `query` / `query_raw`
  reads the whole result into one arena bounded at 4 GiB (a larger one is the loud
  `RowTooLarge`, never an unbounded OOM); for a colossal result, stream it
  row-by-row in constant memory with `query_each` / `query_each_raw`.
- The wire decoders are proven total (no panic on *any* input) by a dep-free
  fuzz gate; the inbound hot dispatch is proven panic-free and byte-stable by a
  codegen gate. NULL is `Option<NonZeroU32>`; SQL identifiers spliced into DDL go
  through a validate-only `SafeIdent` newtype.
- Over TLS, SCRAM-SHA-256-**PLUS** channel binding (opt-in-strict) anchors auth to
  the server's certificate, closing the valid-cert relay/MITM gap that cert +
  hostname verification alone leaves.
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

Built with Claude and Gemini in adversarial dual-LLM pair engineering. Both
systems collaborated continuously across architectural synthesis, red-team
chaos auditing, adversarial judging, and zero-overhead implementation — every
slice challenged and verified before it landed.

2,640+ tests across the workspace — unit, integration, compile-fail (`trybuild`),
live-database, dependency-free fuzz, and machine-checked codegen gates. Not just
tests that the code works, but tests that *broken* code is rejected at compile
time. Hardened by a real-load fault-injection pass (TLS byte-fragmentation,
mid-stream faults, million-row streams, concurrency) — the sort of thing that
catches what green unit tests hide.

Don't follow the author's name. Don't assume an older library is automatically the
safer bet. Run the benchmarks yourself, read the tests, check the code.

`CLAUDE.md` is the exhaustive engineering reference (conventions, layout, every
gate and safety invariant) — read it if you want the full picture.

## License

MIT OR Apache-2.0, at your option.
