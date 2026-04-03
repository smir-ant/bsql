# bsql

Compile-time safe SQL for Rust. PostgreSQL and SQLite.

## Why bsql

- **If it compiles, the SQL is correct** -- every query is validated against your real database during `cargo build`. Table names, column names, types, nullability -- all checked before your code can run.

- **Always checked** -- there is no unchecked SQL function. In sqlx, one missing `!` (`query()` vs `query!()`) silently skips compile-time validation. In bsql, there is only one function, and it always checks. You cannot accidentally write unchecked SQL because the unchecked version does not exist.

- **Pure SQL** -- write real SQL. CTEs, JOINs, window functions, subqueries. No DSL, no method chains, no `.filter().select().join()`. If PostgreSQL or SQLite supports it, bsql validates it.

- **Faster than C** -- arena allocation, binary protocol, zero-copy decode. bsql beats raw C (libpq) on single-row PostgreSQL fetches and beats raw C (sqlite3) on SQLite reads. See [benchmarks](bench/README.md).

- **PostgreSQL and SQLite** -- same `query!` macro, same compile-time safety, both databases. SQLite is not a second-class citizen.

```rust
let id = 42i32;

// This query is validated at compile time against your real database.
// If the `users` table doesn't exist, or `login` isn't a column,
// or `id` isn't an i32 -- this won't compile.
let user = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_one(&pool).await?;

// user.id: i32, user.login: String, user.active: bool
// Types are inferred from the database schema. Nullable columns become Option<T>.
```

## Performance

### PostgreSQL

| Operation | bsql | C | Go (pgx) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | **15.6 us** | 19.3 us | 29.8 us | 30.1 us | 61.3 us |
| 100 rows | **48.3 us** | 50.2 us | 63.1 us | 68.7 us | 116 us |
| 1,000 rows | **307 us** | 351 us | 378 us | 475 us | 537 us |
| 10,000 rows | **2.72 ms** | 3.14 ms | 2.86 ms | 4.53 ms | 4.32 ms |

### SQLite

| Operation | bsql | C | Go (go-sqlite3) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | **1.76 us** | 2.96 us | 3.76 us | 3.56 us | 32.0 us |
| 100 rows | **37.8 us** | 15.7 us | 77.6 us | 33.2 us | 215 us |
| 1,000 rows | **92.6 us** | 112 us | 707 us | 256 us | 1.85 ms |
| 10,000 rows | **934 us** | 1.11 ms | 7.13 ms | 2.85 ms | 20.6 ms |

Apple M1 Pro, macOS, PostgreSQL 15.14, SQLite 3.51.0. All times are median.

> Run these yourself: `cd bench && cargo bench` -- [full methodology and instructions](bench/README.md)

## Quick Start

<details open><summary><h3>PostgreSQL</h3></summary>

**Cargo.toml:**
```toml
[dependencies]
bsql = { version = "0.14", features = ["time", "uuid"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
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
    let user = bsql::query!(
        "SELECT id, login, first_name FROM users WHERE id = $id: i32"
    ).fetch_one(&pool).await?;

    println!("{} ({})", user.first_name, user.login);
    Ok(())
}
```

</details>

<details><summary><h3>SQLite</h3></summary>

**Cargo.toml:**
```toml
[dependencies]
bsql = { version = "0.14", features = ["sqlite"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
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
    let pool = SqlitePool::connect("./myapp.db")?;

    let id = 1i64;
    let user = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i64"
    ).fetch_one(&pool).await?;

    println!("{}: active={}", user.login, user.active);
    Ok(())
}
```

URL formats: `sqlite:./relative/path`, `sqlite:///absolute/path`, `sqlite::memory:`

</details>

See [examples/](examples/) for more complete, runnable programs.

## Compile-Time Checks

| Your mistake | What happens |
|---|---|
| Table name typo | `table "tcikets" not found -- did you mean "tickets"?` |
| Column doesn't exist | `column "naem" not found in table "users"` |
| Wrong parameter type | `expected i32, found &str for column "users.id"` |
| Nullable column | Automatically becomes `Option<T>` -- you cannot forget to handle NULL |
| `UPDATE` without `WHERE` | Compile error -- flags accidental full-table updates |
| `DELETE` without `WHERE` | Compile error -- same protection |
| SQL syntax error | PostgreSQL's own parser error message, at compile time |
| Typo in any identifier | Levenshtein-based "did you mean?" suggestions |

## Safety

- **PostgreSQL driver**: `#![forbid(unsafe_code)]` -- zero unsafe
- **SQLite driver**: unsafe confined to FFI boundary calls (`ffi.rs`) -- every other file is safe Rust
- **5 of 6 crates** enforce `#![forbid(unsafe_code)]` at compile time
- **1,600+ tests** (unit, integration, and compile-fail)

<details>
<summary>Why does the SQLite driver use unsafe?</summary>

SQLite is a C library, not a network protocol. Talking to it means calling C functions from Rust, which requires `unsafe` at the FFI boundary. This is the same constraint every Rust SQLite library faces (including rusqlite, diesel, and sqlx).

In bsql, all unsafe code is confined to one file: `crates/bsql-driver-sqlite/src/ffi.rs`. Every other module in the SQLite driver is safe Rust. The PostgreSQL driver has zero unsafe -- it speaks the PostgreSQL wire protocol in pure Rust.

When a pure-Rust SQLite engine like [Limbo](https://github.com/penberg/limbo) reaches production readiness, this FFI layer can be replaced entirely.

</details>

## Features

<details>
<summary>Optional type support (timestamps, UUIDs, decimals)</summary>

Out of the box, bsql works with basic types: integers, floats, booleans, strings, byte arrays. Enable features for specialized types:

```toml
bsql = { version = "0.14", features = ["time", "uuid", "decimal"] }
```

| Feature | PostgreSQL types | Rust types |
|---|---|---|
| `time` | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `time::OffsetDateTime`, `Date`, `Time` |
| `chrono` | Same (alternative to `time`) | `chrono::DateTime<Utc>`, `NaiveDateTime` |
| `uuid` | UUID | `uuid::Uuid` |
| `decimal` | NUMERIC, DECIMAL | `rust_decimal::Decimal` |

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
).fetch_all(&pool).await?;
```

No string concatenation. No runtime SQL assembly. 2 optional clauses = 4 variants, all validated at compile time.

</details>

<details>
<summary>Execution methods</summary>

| Method | Returns | Use when |
|---|---|---|
| `.fetch_one(&pool)` | `T` | Exactly one row expected |
| `.fetch_all(&pool)` | `Vec<T>` | All matching rows |
| `.fetch_optional(&pool)` | `Option<T>` | Row might not exist |
| `.fetch_stream(&pool)` | `impl Stream<Item = Result<T>>` | Large result sets, row-by-row processing |
| `.execute(&pool)` | `u64` (affected rows) | INSERT/UPDATE/DELETE without RETURNING |

</details>

<details>
<summary>Transactions and savepoints</summary>

```rust
let tx = pool.begin().await?;

// Queries within the transaction...
tx.savepoint("sp1").await?;
// More queries...
tx.rollback_to("sp1").await?;
tx.commit().await?;
```

If the transaction is dropped without calling `commit()`, it automatically rolls back.

</details>

<details>
<summary>Streaming large result sets</summary>

```rust
let mut stream = bsql::query!(
    "SELECT id, login FROM users"
).fetch_stream(&pool);

while let Some(row) = stream.next().await {
    let user = row?;
    println!("{}: {}", user.id, user.login);
}
```

True PostgreSQL-level streaming. Rows are fetched in batches and yielded one at a time. Memory usage stays constant regardless of result set size.

</details>

<details>
<summary>LISTEN/NOTIFY (PostgreSQL)</summary>

```rust
let mut listener = Listener::connect("postgres://...").await?;
listener.listen("events").await?;

while let Some(notification) = listener.next().await {
    let n = notification?;
    println!("channel={}, payload={}", n.channel, n.payload);
}
```

Real-time notifications for cache invalidation, job queues, live updates.

</details>

<details>
<summary>Compile-time EXPLAIN plans</summary>

```toml
bsql = { version = "0.14", features = ["explain"] }
```

Runs `EXPLAIN` on every query during compilation and embeds the plan as a doc comment. Hover over any query result type in your IDE to see the query plan. Development-only -- disable in CI and release builds.

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
).fetch_all(&pool).await?;
```

Each sort variant's SQL is validated at compile time. The enum is exhaustive -- no default case, no fallback.

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

The pool uses a single writer thread + N reader threads (default 4), communicating via crossbeam channels. No tokio dependency in the driver layer.

</details>

<details>
<summary>What bsql is not</summary>

- **Not an ORM.** You write SQL, not method chains.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`.
- **Not database-agnostic.** PostgreSQL and SQLite only. No MySQL, no MSSQL.
- **Not a migration tool.** Use dbmate, sqitch, refinery, or whatever you prefer.

</details>

## Examples

See [examples/](examples/) for complete, runnable programs:

- [pg_basic.rs](examples/pg_basic.rs) -- CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- [pg_dynamic.rs](examples/pg_dynamic.rs) -- Optional WHERE clauses, sort enums, pagination
- [pg_transactions.rs](examples/pg_transactions.rs) -- Transactions, savepoints, rollback
- [pg_streaming.rs](examples/pg_streaming.rs) -- Streaming large result sets row-by-row
- [pg_listener.rs](examples/pg_listener.rs) -- Real-time LISTEN/NOTIFY
- [sqlite_basic.rs](examples/sqlite_basic.rs) -- SQLite CRUD operations
- [sqlite_dynamic.rs](examples/sqlite_dynamic.rs) -- Dynamic queries with SQLite

Setup instructions: [examples/README.md](examples/README.md)

## Benchmarks

See [bench/README.md](bench/README.md) for the full methodology, all numbers (including INSERT, JOIN, subquery, TCP vs UDS), and step-by-step instructions to reproduce everything on your own machine.

## About

Built with [Claude Code](https://claude.ai/code). Seventeen design principles written before the first line of code. Specifications first, then implementation, then multiple rounds of architectural audit. 1,600+ tests proving not just that the code works, but that broken code is rejected.

Run the benchmarks yourself, read the tests, check the code.

## License

MIT OR Apache-2.0
