bsql

Compile-time safe SQL for Rust. PostgreSQL and SQLite.

## Why bsql

- **If it compiles, the SQL is correct** -- every query is validated against your real database during `cargo build`. Table names, column names, types, nullability -- all checked before your code can run.
- **Always checked** -- there is no unchecked SQL function. In sqlx, one missing `!` (`query()` vs `query!()`) silently skips compile-time validation. In bsql, there is only one function, and it always checks. You cannot accidentally write unchecked SQL because the unchecked version does not exist.
- **Pure SQL** -- write real SQL. CTEs, JOINs, window functions, subqueries. No DSL, no method chains, no `.filter().select().join()` (hi, diesel). If PostgreSQL or SQLite supports it, bsql validates it.
- **C-level performance** -- matches raw C (libpq) on single-row queries, 10-20% faster on multi-row fetches, 42% faster on pipelined batch INSERT. See [benchmarks](https://github.com/smir-ant/bsql/blob/main/bench/README.md).
- **Minimal footprint** -- 1.59 MB peak memory — 4.3x less than C (libpq), 4.4x less than sqlx, 10.9x less than Go. See [memory benchmarks](https://github.com/smir-ant/bsql/blob/main/bench/README.md#memory-peak-rss).
- **Async and sync — both first-class** -- same `query!` macro, same performance, same features. Async uses true cooperative scheduling (RPITIT, no `block_in_place` hacks). Sync removes tokio entirely — pure `fn`, zero async runtime overhead. Switch by changing one line in `Cargo.toml`. Most Rust SQL libraries are async-first with sync as an afterthought, or sync-only. bsql is both, equally.
- **PostgreSQL and SQLite** -- same `query!` macro, same compile-time safety, both databases. SQLite is not a second-class citizen.
- **Test isolation in 2ms** -- `#[bsql::test]` gives each test its own PostgreSQL schema. Fixtures applied at compile time. 25x faster than sqlx's per-database approach. [Details below](#test-isolation).
- **Things nobody else does** -- [automatic N+1 detection](#n1-query-detection), [compile-time query plan analysis](#compile-time-query-plan-analysis), [migration safety checking](#migration-safety-check), [request coalescing](#singleflight-request-coalescing), [SQLite parameter type checking](#sqlite-parameter-type-checking), [smart NULL inference](#smart-null-inference). Details below.

```rust
let id = 42i32;

// This query is validated at compile time against your real database.
// If the `users` table doesn't exist, or `login` isn't a column,
// or `id` isn't an i32 -- this won't compile.
let users = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch(&pool).await?;
let user = &users[0];

// user.id: i32, user.login: String, user.active: bool
// Types are inferred from the database schema. Nullable columns become Option<T>.
```

---

## Performance & Memory

[**You need to see this** 🫢](https://github.com/smir-ant/bsql/blob/main/bench/README.md) — bsql vs C vs Go vs diesel vs sqlx, PostgreSQL and SQLite, full methodology and how to reproduce.

---

## Quick Start

<details><summary>PostgreSQL</summary>

**Cargo.toml:**

```toml
[dependencies]
bsql = { version = "0.22", features = ["time", "uuid"] }
```

**Set the database URL** (used by `query!` at compile time):

```bash
export BSQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

**src/main.rs:**

```rust
use bsql::Pool;

#[tokio::main]
async fn main() -> Result<(), bsql::BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    let id = 1i32;
    let users = bsql::query!(
        "SELECT id, login, first_name FROM users WHERE id = $id: i32"
    ).fetch(&pool).await?;
    let user = &users[0];

    println!("{} ({})", user.first_name, user.login);
    Ok(())
}
```

</details>

<details><summary>SQLite</summary>

**Cargo.toml:**

```toml
[dependencies]
bsql = { version = "0.22", features = ["sqlite"] }
```

**Set the database URL** (used by `query!` at compile time):

```bash
export BSQL_DATABASE_URL="sqlite:./myapp.db"
```

If you commit the `.bsql/` cache directory to your repo, teammates and CI can compile without a live database -- the cache contains the schema snapshot.

**src/main.rs:**

```rust
use bsql::SqlitePool;

#[tokio::main]
async fn main() -> Result<(), bsql::BsqlError> {
    let pool = SqlitePool::open("./myapp.db").await?;

    let id = 1i64;
    let users = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i64"
    ).fetch(&pool).await?;
    let user = &users[0];

    println!("{}: active={}", user.login, user.active);
    Ok(())
}
```

URL formats: `sqlite:./relative/path`, `sqlite:///absolute/path`, `sqlite::memory:`

</details>

See [examples/](examples/) for more complete, runnable programs.

---

## Safety

- **PostgreSQL driver**: `#![forbid(unsafe_code)]` -- zero unsafe
- **SQLite driver**: unsafe confined to FFI boundary calls (`ffi.rs`) -- every other file is safe Rust
- **5 of 6 crates** enforce `#![forbid(unsafe_code)]` at compile time
- **1,900+ tests** (unit, integration, compile-fail, and property-based)

<details>
<summary>Requirements</summary>

**Rust 1.75+** (MSRV). Required for RPITIT — `impl Future + Send` in trait return position. This is how bsql provides true async without `block_in_place` or `BoxFuture`. Rust 1.75 was released December 2023.

**PostgreSQL 10+**. bsql uses prepared statements with binary protocol, `pg_catalog` introspection, SCRAM-SHA-256 authentication (PG 10+), and `CREATE SCHEMA` for test isolation. PG 15-18 are tested in CI matrix.

**SQLite 3.37+** (for STRICT tables). bsql uses WAL mode, mmap, foreign keys, and STRICT tables by default. SQLite 3.37 (2021) added STRICT.

</details>

<details>
<summary>Why does the SQLite driver use unsafe?</summary>

SQLite is a C library, not a network protocol. Talking to it means calling C functions from Rust, which requires `unsafe` at the FFI boundary. This is the same constraint every Rust SQLite library faces (including rusqlite, diesel, and sqlx).

In bsql, all unsafe code is confined to one file: `crates/bsql-driver-sqlite/src/ffi.rs`. Every other module in the SQLite driver is safe Rust. The PostgreSQL driver has zero unsafe -- it speaks the PostgreSQL wire protocol in pure Rust.

When a pure-Rust SQLite engine like [Limbo](https://github.com/penberg/limbo) reaches production readiness, this FFI layer can be replaced entirely.

</details>

---

## Compile-Time Checks

| Your mistake                 | What happens                                                            |
| ---------------------------- | ----------------------------------------------------------------------- |
| Table name typo              | `table "tcikets" not found -- did you mean "tickets"?`                |
| Column doesn't exist         | `column "naem" not found in table "users"`                            |
| Wrong parameter type         | `expected i32, found &str for column "users.id"`                      |
| Nullable column              | Automatically becomes `Option<T>` -- you cannot forget to handle NULL |
| `UPDATE` without `WHERE` | Compile error -- flags accidental full-table updates                    |
| `DELETE` without `WHERE` | Compile error -- same protection                                        |
| SQL syntax error             | PostgreSQL's own parser error message, at compile time                  |
| Typo in any identifier       | Levenshtein-based "did you mean?" suggestions                           |

---

## Features

<details>
<summary>Optional type support (timestamps, UUIDs, decimals)</summary>

Out of the box, bsql works with basic types: integers, floats, booleans, strings, byte arrays. Enable features for specialized types:

```toml
bsql = { version = "0.22", features = ["time", "uuid", "decimal"] }
```

| Feature     | PostgreSQL types                   | Rust types                                   |
| ----------- | ---------------------------------- | -------------------------------------------- |
| `time`    | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `time::OffsetDateTime`, `Date`, `Time` |
| `chrono`  | Same (alternative to `time`)     | `chrono::DateTime<Utc>`, `NaiveDateTime` |
| `uuid`    | UUID                               | `uuid::Uuid`                               |
| `decimal` | NUMERIC, DECIMAL                   | `rust_decimal::Decimal`                    |

If your query touches a column that needs a feature you haven't enabled, you get a compile error naming the exact feature to add.

</details>

<details>
<summary>Dynamic queries (optional WHERE clauses)</summary>

Optional clauses expand to every combination at compile time. Each combination is validated against the database.

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets WHERE deleted_at IS NULL
     [AND department_id = $dept: Option<i64>]
     [AND assignee_id = $assignee: Option<i64>]"
).fetch(&pool).await?;
```

No string concatenation. No runtime SQL assembly. 2 optional clauses = 4 variants, all validated at compile time.

**Compile time**: N optional clauses generate 2^N SQL variants, each validated via PREPARE. For 6+ clauses, compile time may increase noticeably. Maximum: 10 clauses (1024 variants).

</details>

<details>
<summary>Execution methods</summary>

| Method            | Returns      | Use                    |
| ----------------- | ------------ | ---------------------- |
| `.fetch(&pool).await` | `Vec<Row>` | SELECT queries         |
| `.run(&pool).await`   | `u64`      | INSERT, UPDATE, DELETE |
| `.defer(&mut tx).await`   | `()`       | Buffer in transaction  |

Power users: `fetch_one`, `fetch_optional`, `fetch_stream`, `for_each` also available.

</details>

<details>
<summary>Transactions and batching</summary>

```rust
let mut tx = pool.begin().await?;

// .defer() buffers writes -- nothing hits the network yet
bsql::query!("INSERT INTO audit_log (msg) VALUES ($msg: &str)")
    .defer(&mut tx).await?;
bsql::query!("UPDATE accounts SET balance = balance - $amt: i32 WHERE id = $id: i32")
    .defer(&mut tx).await?;

// commit() flushes all deferred operations in a single pipeline, then commits
tx.commit().await?;
```

Savepoints are also supported: `tx.savepoint("sp1")`, `tx.rollback_to("sp1")`.

If the transaction is dropped without calling `commit()`, it automatically rolls back.

</details>

<details>
<summary>Streaming large result sets</summary>

```rust
let mut stream = bsql::query!(
    "SELECT id, login FROM users"
).fetch_stream(&pool).await?;

while stream.advance()? {
    let row = stream.next_row().unwrap();
    println!("{}: {}", row.get_i32(0).unwrap(), row.get_str(1).unwrap());
}
```

True PostgreSQL-level streaming. Rows are fetched in batches and yielded one at a time. Memory usage stays constant regardless of result set size.

</details>

<details>
<summary>LISTEN/NOTIFY (PostgreSQL)</summary>

```rust
let mut listener = Listener::connect("postgres://...")?;
listener.listen("events")?;

loop {
    let n = listener.recv()?;
    println!("channel={}, payload={}", n.channel(), n.payload());
}
```

Real-time notifications for cache invalidation, job queues, live updates.

</details>

<details>
<summary>Compile-time EXPLAIN plans</summary>

```toml
bsql = { version = "0.22", features = ["explain"] }
```

Runs `EXPLAIN` on every query during compilation. The plan is embedded as a doc comment (hover in your IDE to see it), and bsql actively warns about sequential scans and missing indexes. See [compile-time query plan analysis](#compile-time-query-plan-analysis) for details. Development-only -- disable in CI and release builds.

</details>

<details>
<summary>PostgreSQL enums</summary>

```rust
#[bsql::pg_enum]
enum TicketStatus {
    #[sql("new")]         New,
    #[sql("in_progress")] InProgress,
    #[sql("resolved")]    Resolved,
    #[sql("closed")]      Closed,
}
```

Type-safe mapping between Rust enums and PostgreSQL enum types.

</details>

<details>
<summary>Sort enums</summary>

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets ORDER BY $[sort: TicketSort] LIMIT $limit: i64"
).fetch(&pool).await?;
```

Each sort variant's SQL is validated at compile time. The enum is exhaustive -- no default case, no fallback.

</details>

<details>
<summary>Connection pool</summary>

Full-featured LIFO connection pool with health checks and configurable behavior.

```rust
let pool = Pool::builder()
    .url("postgres://user:pass@localhost/mydb")
    .max_size(20)                                    // max connections (default: 10)
    .acquire_timeout(Some(Duration::from_millis(50))) // wait for free connection (default: None = fail-fast)
    .max_lifetime(Some(Duration::from_secs(1800)))    // recycle connections after 30 min
    .stale_timeout(Duration::from_secs(30))           // discard idle connections after 30s
    .min_idle(2)                                      // keep at least 2 idle connections
    .max_stmt_cache_size(256)                         // prepared statement cache per connection
    .warmup(&["SELECT 1", "SELECT id FROM users WHERE id = $1"])  // pre-PREPARE on new connections
    .build()?;
```

**LIFO ordering** -- most recently returned connection is reused first (warmest PostgreSQL backend caches).

**Fail-fast by default** -- when `acquire_timeout` is `None`, a pool-exhausted condition returns an error immediately. No silent queuing, no unbounded waits. Set `acquire_timeout` when your workload has predictable bursts.

**Health checks** -- connections idle > 5 seconds are health-checked with an empty query before reuse. Stale connections (idle > `stale_timeout`) are silently discarded.

**Statement warmup** -- new connections pre-PREPARE your hot queries. First real execution hits the statement cache instead of doing a Parse+Describe round-trip.

</details>

<details>
<summary>Read/write splitting</summary>

Route SELECT queries to a read replica, writes to the primary -- transparently.

```rust
let pool = Pool::builder()
    .url("postgres://primary/mydb")
    .replica_url("postgres://replica/mydb")  // optional read replica
    .replica_max_size(10)                    // replica pool size (default: same as primary)
    .build()?;

// SELECT queries automatically route to the replica:
let users = bsql::query!("SELECT id, login FROM users").fetch(&pool).await?;

// INSERT/UPDATE/DELETE always route to the primary:
bsql::query!("INSERT INTO users (login) VALUES ($login: &str)").run(&pool).await?;
```

The proc macro knows which queries are read-only (SELECT) at compile time and generates code that routes through `query_raw_readonly`, which the pool sends to the replica. No user code changes needed -- just add `replica_url` to the builder.

</details>

<details>
<summary>Singleflight (request coalescing)</summary>

When multiple threads issue the same query with the same parameters simultaneously, only one executes against PostgreSQL. The others wait and receive a shared copy of the result.

```rust
let pool = Pool::builder()
    .url("postgres://localhost/mydb")
    .singleflight(true)   // opt-in
    .build()?;
```

100 concurrent requests for `SELECT * FROM config WHERE key = 'theme'` become 1 database query. The other 99 threads block on a condvar and receive an `Arc`-shared copy of the result.

- Only coalesces read-only queries (SELECT). Writes are never coalesced.
- Key = `rapidhash(sql_hash, encoded parameter bytes)` -- same query + same params = same key.
- 30-second timeout on waiting. If the leader panics, followers get an error (not a deadlock).

</details>

<details>
<summary>Async and sync modes</summary>

Default: async (`#[tokio::main]` + `.await` on all methods).

```toml
# Async (default)
bsql = { version = "0.22" }

# Sync -- removes tokio dependency entirely
bsql = { version = "0.22", default-features = false }
```

Same `query!` macro, same zero-copy fetch. Sync mode is pure `fn` -- no async runtime, no `.await`, no tokio in your dependency tree.

When async is enabled, TCP connections use true async I/O via tokio — the scheduler can run other tasks while waiting for PostgreSQL. Unix domain socket connections use sync I/O (sub-millisecond, no benefit from async). No `block_in_place`, no `Handle::current().block_on()` — the `QueryTarget` enum dispatch uses genuine cooperative scheduling.

</details>

<details>
<summary>Offline mode</summary>

Build without a live database. The `.bsql/queries/` directory caches validation results from your last online build.

```bash
# Online: validate queries and populate cache
cargo build   # with BSQL_DATABASE_URL set

# Offline: use cached validation
BSQL_OFFLINE=true cargo build   # no database needed
```

Auto-fallback: if no `BSQL_DATABASE_URL` is set but `.bsql/` exists, bsql uses the cache automatically.

Cache is version-gated: upgrading bsql invalidates the cache. Commit `.bsql/` to your repo so CI and teammates can build offline.

Format: bitcode-serialized (50x faster than JSON for schema cache loading).

</details>

<details>
<summary>Zero-copy architecture</summary>

bsql's hot path allocates nothing on the heap for most queries.

- **Binary protocol** -- `i32` is `i32::from_be_bytes()`, not parsed from ASCII text
- **Pipelined messages** -- Parse+Bind+Execute+Sync in one `write_all()` syscall
- **Bind templates** -- re-execution patches parameter data in-place, no message rebuild
- **Thread-local buffer recycling** -- response buffers, column offset vectors, and arenas are recycled via thread-local pools. Second query on the same thread: zero malloc
- **Zero UTF-8 validation on hot path** -- statement names are `[u8; 18]` passed directly to the wire protocol. No `&str` conversion, no validation overhead
- **Monolithic execute path** -- entire send+receive inlined in one function for global compiler optimization
- **SIMD UTF-8 validation** -- `simdutf8` for bulk string validation on result data
- **Statement cache** -- Vec-based O(n) with u64 hash keys. Faster than HashMap for < 30 entries due to cache locality

</details>

<details>
<summary>What SQLite settings are used</summary>

bsql automatically configures SQLite for optimal performance:

- **WAL mode** -- concurrent readers, non-blocking reads
- **256 MB mmap** -- memory-mapped I/O for fast reads
- **64 MB cache** -- large page cache
- **STRICT tables** -- recommended for type safety
- **`busy_timeout = 0`** -- fail-fast, no silent waiting
- **Foreign keys ON** -- enforced by default

The pool uses a single writer + N reader connections (default 4) behind `Mutex`, fully synchronous.

</details>

<details>
<summary>What bsql is not</summary>

- **Not an ORM.** You write SQL, not method chains.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`.
- **Not database-agnostic.** PostgreSQL and SQLite only. No MySQL, no MSSQL.
- **Not a migration tool.** Use dbmate, sqitch, refinery, or whatever you prefer. bsql can [validate your migrations](#migration-safety-check) before you deploy them, but it does not write or apply them.

</details>

<details>
<summary>Protocol and feature limitations</summary>

bsql implements the PostgreSQL extended query protocol. The following PG features are **not supported**:

- **GSSAPI / SSPI / LDAP / certificate authentication** — only cleartext, MD5, SCRAM-SHA-256, and SCRAM-SHA-256-PLUS
- **Logical replication protocol** — use pg_recvlogical or a dedicated replication tool
- **Large Objects** (lo_read, lo_write) — use BYTEA columns instead
- **SSL_KEY_LOG** for TLS debugging — not exposed

Supported authentication: cleartext password, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS (channel binding).
Supported transports: TCP, Unix domain sockets, TLS (via rustls).

</details>

---

## One more thing

These are features that no other Rust SQL library offers. They exist because bsql sees every query at compile time and every query execution at runtime -- that visibility makes things possible that are architecturally impossible in other libraries.

### N+1 query detection

The most common database performance bug: your code fetches a list, then queries once per item. 100 users = 100 queries instead of 1. Frameworks like Rails have third-party gems to detect this. bsql detects it at the driver level -- no middleware, no configuration, no code changes.

```toml
bsql = { version = "0.22", features = ["detect-n-plus-one"] }
```

When the same query fires more than 10 times in a row on a single connection, bsql logs a warning with the query hash. The threshold is configurable via `Pool::builder().n_plus_one_threshold(5)`. When the feature is disabled, zero code exists in the binary -- full compile-time exclusion.

### Compile-time query plan analysis

When you enable the `explain` feature, bsql runs `EXPLAIN` on every query during `cargo build` and analyzes the result. If PostgreSQL would use a sequential scan on a table with more than 1,000 rows, you get a compile-time warning:

```
warning: [bsql] Seq Scan on "orders" (est. 50000 rows) — consider adding an index
```

This catches missing indexes before your code reaches production. The threshold is configurable via the `BSQL_EXPLAIN_THRESHOLD` environment variable. When the `explain` feature is disabled, this analysis does not run.

### Migration safety check

You write a migration. Will it break any of your existing queries? Find out before deploying:

```bash
bsql migrate --check add_column.sql
```

bsql reads every validated query from its compile-time cache, creates a temporary copy of your schema, applies the migration, and tests each query against the new schema. If any query would break, it tells you which ones and why -- before the migration touches production.

This works because bsql's offline cache (`.bsql/queries/`) contains every SQL statement your application uses. No other library has this cache, so no other library can offer this check.

### Singleflight (request coalescing)

When 100 requests hit the same endpoint at the same time and each one runs the same query with the same parameters, bsql can execute it once and share the result. The other 99 requests wait (not poll) and receive a shared copy.

```rust
let pool = Pool::builder()
    .url("postgres://localhost/mydb")
    .singleflight(true)
    .build()?;
```

Only read queries are coalesced. Writes always execute independently. The deduplication key is the query hash combined with the encoded parameter bytes -- same query + same parameters = one database round-trip.

### SQLite parameter type checking

Every Rust SQLite library checks parameter types at runtime — pass a string where an integer is expected, and you get a runtime error. bsql checks at compile time.

```rust
// Column "id" is INTEGER in the schema.
// This won't compile — &str is incompatible with INTEGER:
bsql::query!("SELECT name FROM users WHERE id = $id: &str")
// error: parameter $id declared as &str but column "id" is INTEGER (expected i64)
```

bsql parses the SQL, finds which column each parameter is compared against, looks up the column's declared type via `PRAGMA table_info`, and verifies compatibility. Works for `WHERE`, `INSERT VALUES`, `UPDATE SET`, and comparison operators (`=`, `>`, `<`, `LIKE`, `IN`, etc.). No other Rust SQL library does this for SQLite.

### Smart NULL inference

Most SQL libraries treat all computed expressions as nullable. `SELECT COUNT(*) as cnt` returns `Option<i64>` — even though `COUNT(*)` can never be NULL. You end up writing `.unwrap()` everywhere for values that are guaranteed to exist.

bsql analyzes the SQL and infers NOT NULL for expressions that are guaranteed by the SQL standard:

| Expression | Other libraries | bsql |
|---|---|---|
| `COUNT(*)` | `Option<i64>` | `i64` |
| `COALESCE(name, 'unknown')` | `Option<String>` | `String` |
| `EXISTS(subquery)` | `Option<bool>` | `bool` |
| `CURRENT_TIMESTAMP` | `Option<...>` | `OffsetDateTime` |
| `42` (literal) | `Option<i64>` | `i64` |

No `!` override syntax, no user hints, no runtime panics. If the macro can prove NOT NULL — you get the bare type. If it can't — you get `Option<T>` (safe default).

### Test isolation

Every test gets its own PostgreSQL schema. No shared state, no flaky tests, full parallelism.

```rust
#[bsql::test(fixtures("schema", "seed"))]
async fn test_get_user(pool: bsql::Pool) {
    let user = bsql::query!("SELECT name FROM users WHERE id = $id: i32")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(user.name, "Alice");
}
```

Each test: `CREATE SCHEMA` (~1-2ms) → apply fixtures → run test → `DROP SCHEMA CASCADE`. Fixtures are SQL files embedded at compile time via `include_str!` — zero runtime file I/O. Cleanup runs even on panic (Drop guard). Extensions are database-global and shared across schemas.

sqlx creates a temporary DATABASE per test (~50ms). bsql creates a SCHEMA (~2ms). Same isolation for tables, data, indexes, views — 25x faster setup.

---

## About

Built with [Claude Code](https://claude.ai/code). Seventeen design principles written before the first line of code. Specifications first, then implementation, then multiple rounds of architectural audit. 1,900+ tests proving not just that the code works, but that broken code is rejected.

Don't follow the author's name. Don't assume a library that's been around for 2 years is 12 times better than one that's been around for 2 months. Run the benchmarks yourself, read the tests, check the code.

## License

MIT OR Apache-2.0
