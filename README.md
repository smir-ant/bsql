# sasql

- **Your SQL is checked before your program even runs.** Every query is validated against a real PostgreSQL database during compilation. If your code compiles, every SQL query in it is guaranteed to be correct.
- **There is no way to write unchecked SQL.** Other libraries give you a "safe" function and an "unsafe" function side by side. sasql only has the safe one. The unsafe one doesn't exist.
- **You write real SQL, not a substitute.** No custom query language to learn. If you know PostgreSQL, you know sasql. CTEs, JOINs, window functions, subqueries — all work on day one.
- **Dangerous patterns are caught at compile time.** `UPDATE` without `WHERE`? Won't compile. Wrong column type? Won't compile. Table doesn't exist? Won't compile.
- **No unsafe memory operations anywhere.** The entire codebase is proven free of unsafe code by the Rust compiler itself.
- **Your app never hangs waiting for a database connection.** If connections are exhausted, you get an error instantly — not after a timeout.

```rust
let id = 42i32;
let user = sasql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_one(&pool).await?;
// user.id: i32, user.login: String, user.active: bool
```

---

## Why Does This Matter?

Imagine you have 500 SQL queries in your project. All of them are validated at compile time. Your codebase is "safe."

Then someone adds one query using an unchecked function. Maybe they were in a hurry. Maybe the safe macro didn't support their use case. That one query bypasses all safety checks. It has a typo in a column name. It deploys to production. It crashes at 3 AM.

Every existing Rust SQL library makes this possible:

- **sqlx** provides both `query!()` (safe) and `query()` (unchecked) in the same module. Nothing prevents using the wrong one.
- **Diesel** provides `sql_query()` for writing raw SQL strings when the DSL can't express your query. No validation.
- **SeaORM** doesn't check any SQL at compile time. All errors are discovered at runtime.
- **Cornucopia** is fully safe, but SQL lives in separate files. You constantly jump between `.sql` files and Rust code. No dynamic queries.

sasql makes the unchecked path impossible. There is no function to misuse. If your project uses sasql, every SQL query is validated — not because developers are disciplined, but because there is no alternative.

## What Gets Checked

| What | What happens |
|------|-------------|
| Table name has a typo | Compile error: `table "tcikets" not found` |
| Column doesn't exist | Compile error: `column "naem" not found in table "users"` |
| Parameter type is wrong | Compile error: `expected i32, found &str for column "users.id"` |
| Column can be NULL | Automatically becomes `Option<T>` in your Rust struct |
| `UPDATE` without `WHERE` | Compile error: refuses to compile a full-table update |
| `DELETE` without `WHERE` | Compile error: refuses to compile a full-table delete |
| SQL syntax error | Compile error with PostgreSQL's own error message |

## Quick Start

```toml
[dependencies]
sasql = { version = "0.2", features = ["time", "uuid"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Tell sasql where your database is (for compile-time validation):

```bash
export SASQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

```rust
use sasql::Pool;

#[tokio::main]
async fn main() -> Result<(), sasql::SasqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // This query is validated at compile time.
    // If the table, columns, or types are wrong — it won't compile.
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, login, first_name FROM users WHERE id = $id: i32"
    ).fetch_one(&pool).await?;

    // The result is a typed struct — IDE autocomplete works.
    println!("{} ({})", user.first_name, user.login);

    // INSERT with RETURNING — also compile-time validated
    let title = "Fix the bug";
    let creator = 1i32;
    let ticket = sasql::query!(
        "INSERT INTO tickets (title, created_by_user_id)
         VALUES ($title: &str, $creator: i32)
         RETURNING id"
    ).fetch_one(&pool).await?;

    println!("Created ticket #{}", ticket.id);

    Ok(())
}
```

## Optional Type Support

By default, sasql supports basic types (integers, floats, booleans, strings, byte arrays). For dates, UUIDs, or decimals, enable the feature you need:

```toml
sasql = { version = "0.2", features = ["time", "uuid", "decimal"] }
```

| Feature | What it adds | PostgreSQL types |
|---------|-------------|-----------------|
| `time` | Dates and timestamps | TIMESTAMPTZ, TIMESTAMP, DATE, TIME |
| `chrono` | Dates and timestamps (alternative) | Same as `time` |
| `uuid` | Universally unique identifiers | UUID |
| `decimal` | Exact decimal numbers | NUMERIC, DECIMAL |

If your query returns a column that needs a feature you haven't enabled, you get a clear compile error telling you exactly which feature to add.

## PostgreSQL Enums

Map PostgreSQL enum types to Rust enums with compile-time safety:

```rust
#[sasql::pg_enum]
enum TicketStatus {
    #[sql("new")]         New,
    #[sql("in_progress")] InProgress,
    #[sql("resolved")]    Resolved,
    #[sql("closed")]      Closed,
}
```

The generated code only accepts the specific PostgreSQL enum type it was designed for — it won't silently deserialize a different enum type with overlapping labels.

## How It Works

When you run `cargo build`:

1. The `query!()` macro extracts your SQL and parameter declarations
2. It connects to PostgreSQL (once per build, shared across all queries)
3. It runs `PREPARE` — PostgreSQL validates the SQL syntax, table names, column names, and types
4. It introspects `pg_catalog` to determine which columns are nullable
5. It generates a Rust struct with correctly typed fields
6. If anything is wrong, compilation stops with a clear error message

The compiled binary contains only validated SQL. There is no runtime SQL parsing, no string concatenation, no chance of a query failing because of a typo.

## Execution Methods

| Method | Returns | When to use |
|--------|---------|-------------|
| `.fetch_one(&pool)` | One row (`T`) | When exactly one row is expected |
| `.fetch_all(&pool)` | All rows (`Vec<T>`) | When you want all matching rows |
| `.fetch_optional(&pool)` | Maybe one row (`Option<T>`) | When the row might not exist |
| `.execute(&pool)` | Affected row count (`u64`) | For INSERT/UPDATE/DELETE without RETURNING |

## What sasql Is Not

- **Not an ORM.** No `User::find(42)`, no `user.save()`, no `belongs_to`. You write SQL.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()` method chains. You write SQL.
- **Not database-agnostic.** Built for PostgreSQL. Not MySQL. Not SQLite. PostgreSQL.
- **Not a migration tool.** Use whatever migration tool you prefer — sasql validates against whatever schema exists.

## Roadmap

See the full roadmap on GitHub: [Projects](https://github.com/smir-ant/sasql/milestones)

| Version | Status | What |
|---------|--------|------|
| v0.1 | Released | `query!` macro, compile-time validation, base types, connection pool |
| v0.2 | **Current** | Feature-gated types (`time`, `uuid`, `decimal`), PG enums, CI pipeline |
| v0.3 | Planned | Dynamic queries with compile-time verified optional clauses |
| v0.4 | Planned | Offline mode — validate without a live database |
| v0.5 | Planned | Transactions with automatic rollback on drop |
| v0.6 | Planned | Request coalescing, streaming results, LISTEN/NOTIFY |
| v0.7 | Planned | Cross-query analysis, query plan insights, automatic read/write splitting |
| v1.0 | Planned | Stable release with arena allocation, binary protocol, SIMD optimizations |

## About the Development Process

This project was built with [Claude Code](https://claude.ai/code). I could have hidden that. But ask yourself: would you trust a strong solo developer more, or a strong solo developer backed by an advisor with the collective knowledge of the entire software engineering field?

What matters is not who wrote the code. What matters is the process: specifications written before the first line of code. 17 non-negotiable design principles. Six rounds of architectural audit before implementation began. 166 tests — unit, integration, and compile-fail — that prove not just that the code works, but that broken code is rejected.

Without this process, I would not have considered bitcode for serialization, arena allocation for result sets, or rapidhash over FNV-1a. I would have written tests that confirm the code handles specific inputs, not tests that prove the system rejects invalid ones. I would have shipped UTF-8 bugs in the SQL parser because I would have tested with ASCII and called it done.

The value is in the discipline: constant audits, clear specifications, and test coverage that treats every untested path as a bug.

## License

MIT OR Apache-2.0
