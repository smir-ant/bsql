# bsql

- **If it compiles, every SQL query is correct.** Validated against a real PostgreSQL instance during `cargo build`. Not at runtime. Not "if you use the right function". Always.
- **No escape hatch exists.** There is no function that accepts unchecked SQL. Not deprecated, not hidden — it does not exist.
- **Pure SQL, not a DSL.** Write real PostgreSQL — CTEs, JOINs, window functions, subqueries. If you know SQL, you know bsql.
- **100% unsafe-free.** Guaranteed by the Rust compiler. No exceptions, no opt-outs.
- **Fail-fast, not fail-silent.** No timeouts. No "wait and hope". Every failure is immediate and explicit.
- **Dangerous SQL won't compile.** Wrong column type, nonexistent table, SQL injection attempts — all caught before the binary is produced.

```rust
let id = 42i32;
let user = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_one(&pool).await?;
// user.id: i32, user.login: String, user.active: bool
```

---

## Why?

| Library | What's missing |
|---------|---------------|
| **sqlx** | `query()` and `query!()` live side by side. One missing `!` — no compile-time check, runtime crash. You won't see it in code review. |
| **Diesel** | Complex SQL (CTEs, window functions, `LATERAL`) can't be expressed in the DSL. You end up calling `sql_query()` — raw strings, zero validation. |
| **SeaORM** | No compile-time SQL checking at all. Every error is discovered when the query hits PostgreSQL in production. |
| **Cornucopia / Clorinde** | SQL in separate `.sql` files — either one unreadable giant file or dozens of scattered ones. File-hopping hell. No dynamic queries. |

What bsql does differently:

- **Inline SQL** — the query is where it's used. No jumping between files. Code review sees SQL and Rust in the same diff.
- **No unchecked path** — not "be disciplined and use the safe function". There is only one function. It is safe.
- **Dynamic queries** — optional clauses `[AND col = $param]` expand to every combination at compile time. Each combination is validated. No string concatenation.
- **Built for performance** — optimized connection pooling, prepared statement caching, fail-fast error handling. Architecture designed for arena allocation, binary protocol, and SIMD (planned).

## What Gets Checked at Compile Time

| Your mistake | What happens |
|-------------|-------------|
| Table name typo | `table "tcikets" not found` |
| Column doesn't exist | `column "naem" not found in table "users"` |
| Wrong parameter type | `expected i32, found &str for column "users.id"` |
| Nullable column | Automatically becomes `Option<T>` — you can't forget to handle NULL |
| `UPDATE` without `WHERE` | Compile warning — flags accidental full-table updates (planned) |
| `DELETE` without `WHERE` | Compile warning — same protection (planned) |
| SQL syntax error | PostgreSQL's own error message, at compile time |

## Quick Start

`Cargo.toml`:
```toml
[dependencies]
bsql = { version = "0.9", features = ["time", "uuid"] }
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

## Optional Type Support

Out of the box, bsql works with basic types: integers, floats, booleans, strings, byte arrays. This is enough for most queries. For specialized PostgreSQL types like timestamps or UUIDs, enable the corresponding feature:

```toml
bsql = { version = "0.9", features = ["time", "uuid", "decimal"] }
```

| Feature | PostgreSQL types | Rust types |
|---------|-----------------|------------|
| `time` | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `time::OffsetDateTime`, `Date`, `Time` |
| `chrono` | Same (alternative to `time`) | `chrono::DateTime<Utc>`, `NaiveDateTime` |
| `uuid` | UUID | `uuid::Uuid` |
| `decimal` | NUMERIC, DECIMAL | `rust_decimal::Decimal` |

If your query touches a column that needs a feature you haven't enabled, you get a compile error naming the exact feature to add.

## Compile-Time EXPLAIN Plans

Enable the `explain` feature to see the query plan at compile time:

```toml
bsql = { version = "0.9", features = ["explain"] }
```

When enabled, bsql runs `EXPLAIN` on every query during compilation and embeds the plan as a doc comment on the generated result struct. Hover over any query result type in your IDE to see the plan — no round-trip to `psql` needed.

This is a development-only tool. Disable it in CI and release builds to avoid the extra PG round-trip per query.

## PostgreSQL Enums

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

## Execution Methods

| Method | Returns | Use when |
|--------|---------|----------|
| `.fetch_one(&pool)` | `T` | Exactly one row expected |
| `.fetch_all(&pool)` | `Vec<T>` | All matching rows |
| `.fetch_optional(&pool)` | `Option<T>` | Row might not exist |
| `.fetch_stream(&pool)` | `impl Stream<Item = Result<T>>` | Large result sets, row-by-row processing |
| `.execute(&pool)` | `u64` (number of affected rows) | INSERT/UPDATE/DELETE without RETURNING |

## What bsql Is Not

- **Not an ORM.** You write SQL, not method chains.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`.
- **Not database-agnostic.** PostgreSQL only.
- **Not a migration tool.** Use dbmate, sqitch, refinery, or whatever you prefer.

## What bsql Doesn't Cover (and Why)

95%+ of application code is regular SELECT/INSERT/UPDATE/DELETE with fixed table names. bsql covers that with absolute compile-time safety. The remaining cases are intentionally outside bsql's scope:

**DDL / Migrations.** `CREATE TABLE`, `DROP INDEX`, `ALTER TABLE`, `GRANT` — PostgreSQL's `PREPARE` does not accept DDL statements, so compile-time validation is impossible. More importantly, migrations *change* the schema that bsql validates against — checking them against the current schema is meaningless. Use a dedicated migration tool (dbmate, sqitch, refinery, sqlx-cli).

**Dynamic table/column names.** `SELECT * FROM {runtime_table_name}` — `PREPARE` does not accept parameters in identifier positions (`$1` works for values, not for table or column names). Multi-tenant applications with per-tenant tables (`tenant_123_orders`) or admin panels browsing arbitrary tables fall into this category. Solution: a fixed set of `query!()` calls per known table, or use `tokio-postgres` directly for these specific admin/infrastructure queries.

**Server-side dynamic SQL.** `DO $$ BEGIN EXECUTE format(...); END $$` — PostgreSQL validates the outer `DO` block at `PREPARE` time but does not validate the dynamically constructed SQL inside `EXECUTE format(...)`. It runs only at execution time.

Migrations, admin panels, and multi-tenant dynamic tables are infrastructure — they don't belong in application business logic. bsql secures the 95% that does. For the remaining 5%, use `tokio-postgres` alongside bsql — two tools, each for its purpose.

## Roadmap

| Version | Status | What |
|---------|--------|------|
| v0.1 | Released | `query!` macro, compile-time validation, base types, pool |
| v0.2 | Released | `time`, `uuid`, `decimal`, `chrono`, PG enums, CI on PG 15-18 |
| v0.3 | Released | Dynamic queries: `[optional clauses]`, sort enums |
| v0.4 | Released | Offline mode: bitcode cache, auto-populated during build |
| v0.5 | Released | Transactions: `begin()`, `commit()`, `rollback()`, auto-rollback on drop |
| v0.6 | Released | Streaming results, LISTEN/NOTIFY |
| v0.7 | Released | Singleflight request coalescing, read/write splitting, EXPLAIN at compile time |
| v0.8 | Released | TLS support, SmallVec optimizations |
| v0.9 | **Current** | Connection warmup, safety gates (UPDATE/DELETE without WHERE) |
| v0.10 | Planned | Custom PG wire protocol driver (bsql-driver): binary protocol, arena allocation, zero-copy row decoding, built-in connection pool, pipelining. Replaces tokio-postgres + deadpool. |

## About the Development Process

Built with [Claude Code](https://claude.ai/code). Specifications and 17 design principles written before the first line of code. Six rounds of architectural audit. 166 tests — unit, integration, and compile-fail — proving not just that the code works, but that broken code is rejected.

Without this process, I would not have discovered bitcode for serialization, rapidhash over FNV-1a, or the fail-fast pool pattern. I would have shipped UTF-8 bugs because I would have tested with ASCII only.

The value is in the discipline: constant audits, clear specifications, and test coverage that treats every untested path as a bug.

## License

MIT OR Apache-2.0
