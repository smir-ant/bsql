# bsql-postgres

Async PostgreSQL driver for Rust, built on the [`bsql-pg-proto`](../bsql-pg-proto) sans-IO state machine.

## Features

- **Async/await** via tokio
- **TLS** via rustls (Disable/Prefer/Require)
- **SCRAM-SHA-256** + Trust authentication
- **Typed row access** — `row.get::<i32>(0)`
- **Connection timeout**
- **Error recovery** — bad SQL doesn't kill the connection
- **DSN parsing** — `postgres://user:pass@host/db?sslmode=require`
- **Environment variables** — `PGHOST`, `PGUSER`, `PGPASSWORD`, etc.
- **Zero unsafe** in the protocol layer (`#![forbid(unsafe_code)]`)

## Quick Start

```rust
use bsql_postgres::{ConnectConfig, Connection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect via DSN
    let config = ConnectConfig::from_dsn(
        "postgres://myuser:mypass@localhost:5432/mydb?sslmode=prefer"
    )?;
    let mut conn = Connection::connect(&config).await?;

    // Execute DDL/DML
    conn.execute_raw("CREATE TABLE users (id serial, name text, score float8)").await?;
    let inserted = conn.execute(
        "INSERT INTO users (name, score) VALUES ('alice', 9.5), ('bob', 7.2)"
    ).await?;
    println!("inserted {inserted} rows");

    // Query with typed access
    let result = conn.query("SELECT id, name, score FROM users ORDER BY id").await?;
    for row in &result.rows {
        let id: i32 = row.get(0).unwrap();
        let name: String = row.get(1).unwrap();
        let score: f64 = row.get(2).unwrap();
        println!("{id}: {name} (score {score})");
    }

    // Scalar query
    let row = conn.query_one("SELECT count(*) FROM users").await?;
    let count: i64 = row.get(0).unwrap();
    println!("total: {count}");

    // Transactions
    conn.execute_raw("BEGIN").await?;
    conn.execute_raw("UPDATE users SET score = score + 1").await?;
    conn.execute_raw("COMMIT").await?;

    conn.close().await?;
    Ok(())
}
```

## Configuration

Three ways to configure:

```rust
// Builder
let config = ConnectConfig::new("localhost", "myuser")
    .database("mydb")
    .password("secret")
    .ssl_mode(SslMode::Require)
    .connect_timeout(5);

// DSN string
let config = ConnectConfig::from_dsn("postgres://user:pass@host/db")?;

// Environment variables (PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGSSLMODE)
let config = ConnectConfig::from_env();
```

## API

| Method | Description |
|--------|-------------|
| `Connection::connect(&config)` | TCP + optional TLS + auth handshake |
| `conn.query(sql)` | SELECT → `QueryResult { rows, command_tag }` |
| `conn.query_one(sql)` | SELECT → single `Row` (error if empty) |
| `conn.query_opt(sql)` | SELECT → `Option<Row>` |
| `conn.simple_query(sql)` | Any SQL → command tag string |
| `conn.execute(sql)` | DML → affected row count (`u64`) |
| `conn.ping()` | Liveness check |
| `conn.close()` | Graceful Terminate |
| `conn.server_version()` | PG version string |
| `conn.backend_pid()` | Server process ID |

### Row access

Every typed getter returns `Result<Option<T>, ColumnError>`, keeping each
outcome distinct: `Ok(Some(v))` = value, `Ok(None)` = SQL `NULL`, and `Err(..)`
= out-of-range or a classified decode failure (never a silently-swallowed
`None`).

```rust
// Generic — decodes through the classified Cell<TextFmt> matrix
let id: Option<i32> = row.get::<i32>(0)?;      // Ok(None) = SQL NULL
let name: Option<&str> = row.get::<&str>(1)?;  // zero-copy &str

// Named accessors (same classified return shape)
row.get_i32(0)?   row.get_i64(0)?   row.get_f64(0)?
row.get_str(0)?   row.get_bool(0)?  row.get_raw(0)?
row.is_null(0)    row.len()
```

Generic `get::<T>` covers `i16`, `i32`, `i64`, `u32`, `bool`, `&str` via the
classified `Cell<TextFmt>` decoder; `get_f64` adds the text-float path.

## Architecture

```
bsql-postgres (this crate)
  └─ bsql-pg-proto (sans-IO state machine, #![forbid(unsafe_code)])
       └─ arrayvec, simdutf8, sha2, hmac, pbkdf2, md-5, zeroize, getrandom
```

The sans-IO protocol handles all wire-format parsing, state transitions,
and cryptographic operations. The driver wraps it with tokio TCP/TLS I/O.
Cancellation-safe by construction — dropped futures cannot corrupt wire state.
