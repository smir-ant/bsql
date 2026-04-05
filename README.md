bsql

Compile-time safe SQL for Rust. PostgreSQL and SQLite.

## Why bsql

- **If it compiles, the SQL is correct** -- every query is validated against your real database during `cargo build`. Table names, column names, types, nullability -- all checked before your code can run.
- **Always checked** -- there is no unchecked SQL function. In sqlx, one missing `!` (`query()` vs `query!()`) silently skips compile-time validation. In bsql, there is only one function, and it always checks. You cannot accidentally write unchecked SQL because the unchecked version does not exist.
- **Pure SQL** -- write real SQL. CTEs, JOINs, window functions, subqueries. No DSL, no method chains, no `.filter().select().join()` (hi, diesel). If PostgreSQL or SQLite supports it, bsql validates it.
- **Faster than C** -- 1.05–2.5x faster than raw C (libpq, sqlite3) on every benchmark. Not synthetic tuning — the same code paths run in benchmarks and in your production app. See [benchmarks](bench/README.md).
- **Minimal footprint** -- 2.2 MB peak memory (RSS) — 3–7x less than sqlx, diesel, C (libpq), and Go. See [memory benchmarks](bench/README.md#memory-peak-rss).
- **PostgreSQL and SQLite** -- same `query!` macro, same compile-time safety, both databases. SQLite is not a second-class citizen.

```rust
let id = 42i32;

// This query is validated at compile time against your real database.
// If the `users` table doesn't exist, or `login` isn't a column,
// or `id` isn't an i32 -- this won't compile.
let users = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch(&pool)?;
let user = &users[0];

// user.id: i32, user.login: String, user.active: bool
// Types are inferred from the database schema. Nullable columns become Option<T>.
```

---

## Performance & Memory

[**You need to see this** 🫢](bench/README.md) — bsql vs C vs Go vs diesel vs sqlx, PostgreSQL and SQLite, full methodology and how to reproduce.

---

## Quick Start

<details><summary>PostgreSQL</summary>

**Cargo.toml:**

```toml
[dependencies]
bsql = { version = "0.17", features = ["time", "uuid"] }
```

**Set the database URL** (used by `query!` at compile time):

```bash
export BSQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

**src/main.rs:**

```rust
use bsql::Pool;

fn main() -> Result<(), bsql::BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    let id = 1i32;
    let users = bsql::query!(
        "SELECT id, login, first_name FROM users WHERE id = $id: i32"
    ).fetch(&pool)?;
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
bsql = { version = "0.17", features = ["sqlite"] }
```

**Set the database URL** (used by `query!` at compile time):

```bash
export BSQL_DATABASE_URL="sqlite:./myapp.db"
```

If you commit the `.bsql/` cache directory to your repo, teammates and CI can compile without a live database -- the cache contains the schema snapshot.

**src/main.rs:**

```rust
use bsql::SqlitePool;

fn main() -> Result<(), bsql::BsqlError> {
    let pool = SqlitePool::open("./myapp.db")?;

    let id = 1i64;
    let users = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i64"
    ).fetch(&pool)?;
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
- **1,600+ tests** (unit, integration, and compile-fail)

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
bsql = { version = "0.17", features = ["time", "uuid", "decimal"] }
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
).fetch(&pool)?;
```

No string concatenation. No runtime SQL assembly. 2 optional clauses = 4 variants, all validated at compile time.

</details>

<details>
<summary>Execution methods</summary>

| Method            | Returns      | Use                    |
| ----------------- | ------------ | ---------------------- |
| `.fetch(&pool)` | `Vec<Row>` | SELECT queries         |
| `.run(&pool)`   | `u64`      | INSERT, UPDATE, DELETE |
| `.defer(&tx)`   | `()`       | Buffer in transaction  |

Power users: `fetch_one`, `fetch_optional`, `fetch_stream`, `for_each` also available.

</details>

<details>
<summary>Transactions and batching</summary>

```rust
let tx = pool.begin()?;

// .defer() buffers writes -- nothing hits the network yet
bsql::query!("INSERT INTO audit_log (msg) VALUES ($msg: &str)")
    .defer(&tx)?;
bsql::query!("UPDATE accounts SET balance = balance - $amt: i32 WHERE id = $id: i32")
    .defer(&tx)?;

// commit() flushes all deferred operations in a single pipeline, then commits
tx.commit()?;
```

Savepoints are also supported: `tx.savepoint("sp1")`, `tx.rollback_to("sp1")`.

If the transaction is dropped without calling `commit()`, it automatically rolls back.

</details>

<details>
<summary>Streaming large result sets</summary>

```rust
let mut stream = bsql::query!(
    "SELECT id, login FROM users"
).fetch_stream(&pool)?;

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
bsql = { version = "0.17", features = ["explain"] }
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
).fetch(&pool)?;
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

The pool uses a single writer + N reader connections (default 4) behind `Mutex`, fully synchronous.

</details>

<details>
<summary>What bsql is not</summary>

- **Not an ORM.** You write SQL, not method chains.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`.
- **Not database-agnostic.** PostgreSQL and SQLite only. No MySQL, no MSSQL.
- **Not a migration tool.** Use dbmate, sqitch, refinery, or whatever you prefer.

</details>

---

## About

Built with [Claude Code](https://claude.ai/code). Seventeen design principles written before the first line of code. Specifications first, then implementation, then multiple rounds of architectural audit. 1,600+ tests proving not just that the code works, but that broken code is rejected.

Don't follow the author's name. Don't assume a library that's been around for 2 years is 12 times better than one that's been around for 2 months. Run the benchmarks yourself, read the tests, check the code.

## License

MIT OR Apache-2.0
