# bsql

Compile-time safe SQL for PostgreSQL and SQLite. If it compiles, every query is correct.

## Why bsql

- **If it compiles, the SQL is correct.** Every query validated against a real database at `cargo build`.
- **No escape hatch.** No `query()` next to `query!()`. One API. Always checked.
- **Pure SQL.** CTEs, JOINs, window functions — write real SQL, not a DSL.
- **Faster than C.** Arena allocation, binary protocol, zero-copy decode. [See benchmarks.](bench/README.md)
- **PostgreSQL + SQLite.** Same `query!` macro, same safety, both databases.

```rust
let id = 42i32;
let user = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_one(&pool).await?;
// user.id: i32, user.login: String, user.active: bool
```

## Performance

| | bsql | C (-O3) | diesel | sqlx |
|---|---|---|---|---|
| PG fetch_one (UDS) | **15.6 us** | 19.3 us | 30.1 us | 61.3 us |
| PG fetch_1K (UDS) | **307 us** | 351 us | 475 us | 537 us |
| SQLite fetch_one | **1.76 us** | 2.96 us | 3.56 us | 32.0 us |
| SQLite fetch_1K | **92.6 us** | 112 us | 256 us | 1.85 ms |

[See full benchmarks](bench/README.md)

## Quick Start

`Cargo.toml`:
```toml
[dependencies]
bsql = { version = "0.14", features = ["time", "uuid"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Terminal — set the database URL for compile-time validation:
```bash
export BSQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

`src/main.rs`:
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

## Safety

- PostgreSQL driver: `#![forbid(unsafe_code)]` — zero unsafe
- SQLite driver: unsafe confined to a single FFI module (`ffi.rs`) — every other file is safe
- 5 of 6 crates enforce `#![forbid(unsafe_code)]` at compile time
- 1,375+ unit tests

## What Gets Checked at Compile Time

| Your mistake | What happens |
|-------------|-------------|
| Table name typo | `table "tcikets" not found` |
| Column doesn't exist | `column "naem" not found in table "users"` |
| Wrong parameter type | `expected i32, found &str for column "users.id"` |
| Nullable column | Automatically becomes `Option<T>` — you can't forget to handle NULL |
| `UPDATE` without `WHERE` | Compile error — flags accidental full-table updates |
| `DELETE` without `WHERE` | Compile error — same protection |
| SQL syntax error | PostgreSQL's own error message, at compile time |
| Typo in table/column name | Levenshtein-based "did you mean?" suggestions at compile time |

## SQLite Support

Same `query!` macro, same compile-time validation. The database URL determines which backend is used.

```toml
[dependencies]
bsql = { version = "0.14", features = ["sqlite"] }
```

```rust
// BSQL_DATABASE_URL=sqlite:./myapp.db

let pool = bsql_core::SqlitePool::connect("./myapp.db")?;

let user = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i64"
).fetch_one(&pool).await?;
// user.id: i64, user.login: String, user.active: bool
```

URL formats: `sqlite:./relative/path`, `sqlite:///absolute/path`, `sqlite::memory:`

## Features

<details>
<summary>Optional type support</summary>

Out of the box, bsql works with basic types: integers, floats, booleans, strings, byte arrays. For specialized PostgreSQL types, enable the corresponding feature:

```toml
bsql = { version = "0.14", features = ["time", "uuid", "decimal"] }
```

| Feature | PostgreSQL types | Rust types |
|---------|-----------------|------------|
| `time` | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `time::OffsetDateTime`, `Date`, `Time` |
| `chrono` | Same (alternative to `time`) | `chrono::DateTime<Utc>`, `NaiveDateTime` |
| `uuid` | UUID | `uuid::Uuid` |
| `decimal` | NUMERIC, DECIMAL | `rust_decimal::Decimal` |

If your query touches a column that needs a feature you haven't enabled, you get a compile error naming the exact feature to add.

</details>

<details>
<summary>Compile-time EXPLAIN plans</summary>

```toml
bsql = { version = "0.14", features = ["explain"] }
```

Runs `EXPLAIN` on every query during compilation and embeds the plan as a doc comment on the generated result struct. Hover over any query result type in your IDE to see the plan.

Development-only. Disable in CI and release builds.

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

Type-safe PG enum mapping. Only accepts the specific PostgreSQL enum type it was defined for.

</details>

<details>
<summary>Execution methods</summary>

| Method | Returns | Use when |
|--------|---------|----------|
| `.fetch_one(&pool)` | `T` | Exactly one row expected |
| `.fetch_all(&pool)` | `Vec<T>` | All matching rows |
| `.fetch_optional(&pool)` | `Option<T>` | Row might not exist |
| `.fetch_stream(&pool)` | `impl Stream<Item = Result<T>>` | Large result sets, row-by-row processing |
| `.execute(&pool)` | `u64` (affected rows) | INSERT/UPDATE/DELETE without RETURNING |

</details>

<details>
<summary>Dynamic queries</summary>

Optional clauses expand to every combination at compile time. Each combination is validated against the database.

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets WHERE deleted_at IS NULL
     [AND department_id = $dept: Option<i64>]
     [AND assignee_id = $assignee: Option<i64>]"
).fetch_all(&pool).await?;
```

No string concatenation. No runtime SQL assembly.

</details>

<details>
<summary>Sort enums</summary>

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets ORDER BY $[sort: TicketSort] LIMIT $limit: i64"
).fetch_all(&pool).await?;
```

</details>

<details>
<summary>Transactions</summary>

```rust
let tx = pool.begin().await?;
// Execute queries within the transaction...
tx.savepoint("sp1").await?;
// More queries...
tx.rollback_to("sp1").await?;
tx.commit().await?;
```

</details>

<details>
<summary>Streaming</summary>

```rust
let mut stream = bsql::query!(
    "SELECT id, login FROM users"
).fetch_stream(&pool);

while let Some(row) = stream.next().await {
    let user = row?;
    println!("{}: {}", user.id, user.login);
}
```

True PG-level streaming with row-by-row processing.

</details>

<details>
<summary>LISTEN/NOTIFY</summary>

```rust
let mut listener = pool.listen("events").await?;

while let Some(notification) = listener.next().await {
    let n = notification?;
    println!("channel={}, payload={}", n.channel, n.payload);
}
```

</details>

<details>
<summary>SQLite configuration</summary>

bsql automatically configures SQLite for optimal performance:

- **WAL mode** — concurrent readers, non-blocking reads
- **256 MB mmap** — memory-mapped I/O for fast reads
- **64 MB cache** — large page cache
- **STRICT tables** — recommended for type safety
- **`busy_timeout = 0`** — fail-fast, no silent waiting
- **Foreign keys ON** — enforced by default

The pool uses a single writer thread + N reader threads (default 4), communicating via crossbeam channels. No tokio dependency in the driver layer.

</details>

<details>
<summary>What bsql is not</summary>

- **Not an ORM.** You write SQL, not method chains.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`.
- **Not database-agnostic.** PostgreSQL and SQLite only.
- **Not a migration tool.** Use dbmate, sqitch, refinery, or whatever you prefer.

</details>

## Examples

See [examples/](examples/) for complete, runnable usage:

- [pg_basic.rs](examples/pg_basic.rs) — PostgreSQL CRUD operations
- [pg_dynamic.rs](examples/pg_dynamic.rs) — Dynamic queries with optional clauses
- [pg_transactions.rs](examples/pg_transactions.rs) — Transactions with savepoints
- [pg_streaming.rs](examples/pg_streaming.rs) — Streaming large result sets
- [pg_listener.rs](examples/pg_listener.rs) — LISTEN/NOTIFY
- [sqlite_basic.rs](examples/sqlite_basic.rs) — SQLite CRUD
- [sqlite_dynamic.rs](examples/sqlite_dynamic.rs) — Dynamic queries with SQLite

Each example is a standalone `fn main()` with comments explaining every step. See [examples/README.md](examples/README.md) for setup instructions.

## Development

Built with [Claude Code](https://claude.ai/code). Specifications and 17 design principles written before the first line of code. Multiple rounds of architectural audit. Unit, integration, and compile-fail tests proving not just that the code works, but that broken code is rejected.

Judge this project by the evidence: 1,375+ tests, [benchmark numbers](bench/README.md), and the code itself.

## License

MIT OR Apache-2.0
