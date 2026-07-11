//! A realistic small web-service data layer, end-to-end.
//!
//! This is the "what does real usage look like?" example: a connection POOL, a
//! couple of compile-checked `query!` typed queries, an atomic TRANSACTION,
//! reconnect-on-disconnect error handling via [`DriverError::is_disconnect`], and
//! the structured DIAGNOSTICS sink wired to stderr. Everything a request handler
//! in a small service would touch, in ~150 lines.
//!
//! # It compiles offline; it runs against a live PostgreSQL
//!
//! `cargo build --examples` (or `cargo check --examples`) compiles this WITHOUT a
//! database: the `query!` macros are typed at build time against the schema
//! `tools/query_fixture/build.rs` replays from `migrations/` (the `users` /
//! `orders` tables below), and `main` is only COMPILED, never run, by a build.
//! To actually run it, point `DATABASE_URL` at a PostgreSQL and
//! `cargo run -p bsql-query-fixture --example web_service`.
//!
//! # Wiring note (this fixture vs. a real consumer)
//!
//! This example lives in `tools/query_fixture`, which already has the `build.rs`
//! and `migrations/` a compile-checked consumer needs, so it reaches `query!`
//! through `bsql` (the `macros` feature) and the async driver through the
//! separate `bsql_postgres_async` crate. A real consumer instead depends on the
//! ONE umbrella crate with `bsql = { features = ["macros", "postgres-async"] }`,
//! reaching the identical types as `bsql::pg::{Pool, Connection, DiagEvent}` —
//! the `query!` carriers below are byte-identical either way.

#![forbid(unsafe_code)]
// An example inherits the crate's forbid floor, so it is held to production
// discipline: no `unwrap`/`expect`/`panic`, every fallible call handled.

use core::time::Duration;

use bsql_postgres_async::{ConnectConfig, DiagEvent, DriverError, Pool};

// ── Compile-checked queries ────────────────────────────────────────────────
//
// Each `query!` is typed at build time against the migration-replayed catalog.
// `SELECT nope FROM users` — or a wrong column type, or a forgotten nullability —
// would be a `compile_error!` here, not a runtime surprise. Every `$N` parameter
// binds in one uniform binary format (no injection surface).
//
// The schema (from `tools/query_fixture/migrations/`):
//   users(id BIGINT PK, email TEXT NOT NULL, bio TEXT NOT NULL, name TEXT NOT NULL)
//   orders(id BIGINT PK, user_id BIGINT NOT NULL, total INTEGER, status TEXT)

// A by-key lookup: `id`/`name` are NOT NULL → `i64`/`&str` in the record.
bsql::query!(UserByEmail, "SELECT id, name FROM users WHERE email = $1");
// A one-to-many read: `total`/`status` are nullable → `Option<i32>`/`Option<&str>`.
bsql::query!(OrdersForUser, "SELECT id, total, status FROM orders WHERE user_id = $1");
// Writes with `RETURNING` so the transaction can read the generated keys back.
bsql::query!(
    InsertUser,
    "INSERT INTO users (id, email, bio, name) VALUES ($1, $2, $3, $4) RETURNING id"
);
bsql::query!(InsertOrder, "INSERT INTO orders (id, user_id) VALUES ($1, $2) RETURNING id");

/// A new user plus their first order, the input to one atomic write.
struct NewUser {
    id: i64,
    email: String,
    bio: String,
    name: String,
    first_order_id: i64,
}

/// Build the service's connection pool with structured diagnostics and a bounded
/// connection lifecycle — the shape a production service uses.
fn build_pool(config: ConnectConfig) -> Pool {
    Pool::builder(config, 16)
        // Structured observability through ONE dep-free callback. A real service
        // forwards these to its logging/metrics stack; here they go to stderr.
        // `DiagEvent` is `#[non_exhaustive]`, so the catch-all arm is required.
        .on_diagnostic(|ev: &DiagEvent<'_>| match ev {
            DiagEvent::SlowQuery { sql, elapsed } => eprintln!("[bsql] slow {elapsed:?}: {sql}"),
            DiagEvent::ServerNotice { severity, message, .. } => {
                eprintln!("[bsql] {severity}: {message}");
            }
            DiagEvent::PoolAcquireTimeout { waited } => {
                eprintln!("[bsql] pool exhausted; waited {waited:?}");
            }
            other => eprintln!("[bsql] {other:?}"),
        })
        // Any query slower than this reports a `SlowQuery` event (off-path reads
        // no clock, so this is free when no threshold is set).
        .slow_query_threshold(Duration::from_millis(100))
        // A lazy reaper rotates connections older than 30 min and sheds ones idle
        // past 5 min — bounding per-backend memory and letting rolling
        // credential/DNS changes take effect. Both default to `None` (disabled).
        .max_lifetime(Some(Duration::from_secs(1800)))
        .idle_timeout(Some(Duration::from_secs(300)))
        .build()
}

/// Read a user by email, RETRYING ONCE on a dead connection.
///
/// This is the load-bearing reconnect pattern: [`DriverError::is_disconnect`]
/// draws the exact line between "the connection DIED mid-query, get a FRESH one"
/// and "the server rejected the query but the connection is FINE, surface it" —
/// by construction, never a string-match. A silently-vanished pooled peer (a NAT
/// idle-drop, an AZ partition) is caught, dropped, and the read is retried on a
/// fresh checkout; a genuine query error propagates unchanged.
async fn find_user_by_email(pool: &Pool, email: &str) -> Result<Option<(i64, String)>, DriverError> {
    let mut pooled = pool.get().await?;
    let first = pooled.conn_mut()?.query_opt::<UserByEmailQuery>((email,)).await;
    let found = match first {
        Ok(found) => found,
        // Connection dead mid-query → drop it (the pool evicts it) and retry once
        // on a fresh connection. A second failure propagates.
        Err(e) if e.is_disconnect() => {
            drop(pooled);
            let mut fresh = pool.get().await?;
            fresh.conn_mut()?.query_opt::<UserByEmailQuery>((email,)).await?
        }
        // A per-query error the connection SURVIVED (bad param, etc.) — surface it.
        Err(e) => return Err(e),
    };
    // `query_opt` yields the OWNED record (or `None`); extract the domain fields.
    Ok(found.map(|u| (u.id, u.name)))
}

/// List a user's orders. `query::<Q>` returns the typed rows; each borrowed
/// record is decoded lazily, and a per-row decode error propagates as a
/// `DriverError` via `?` (never a silent skip).
async fn orders_for_user(pool: &Pool, user_id: i64) -> Result<Vec<(i64, Option<i32>)>, DriverError> {
    let mut pooled = pool.get().await?;
    let rows = pooled.conn_mut()?.query::<OrdersForUserQuery>((user_id,)).await?;
    let mut out = Vec::new();
    for row in rows.iter() {
        let row = row?; // classified DecodeError -> DriverError
        out.push((row.id, row.total));
    }
    Ok(out)
}

/// Create a user AND their first order ATOMICALLY. The closure IS the transaction
/// boundary: returning `Ok` commits, any `Err` (or an early `?`) rolls back — a
/// forgotten commit is impossible by construction.
async fn create_user_with_order(pool: &Pool, new_user: &NewUser) -> Result<i64, DriverError> {
    let mut pooled = pool.get().await?;
    pooled
        .conn_mut()?
        .transaction(async |tx| {
            let user = tx
                .query_one::<InsertUserQuery>((
                    new_user.id,
                    new_user.email.as_str(),
                    new_user.bio.as_str(),
                    new_user.name.as_str(),
                ))
                .await?;
            tx.query_one::<InsertOrderQuery>((new_user.first_order_id, new_user.id)).await?;
            Ok(user.id) // -> COMMIT (the returned user id); any Err above -> ROLLBACK
        })
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A real service reads its DSN from the environment; a bare `cargo run` with
    // no `DATABASE_URL` falls back to a localhost config (an explicit match, not a
    // hidden default). `from_dsn` parses the libpq URL and fails loud on a bad one.
    let config = match std::env::var("DATABASE_URL") {
        Ok(dsn) => ConnectConfig::from_dsn(&dsn)?,
        Err(_) => ConnectConfig::new("127.0.0.1", "postgres").database("postgres"),
    };

    let pool = build_pool(config);

    // One atomic write, then two reads — a typical request lifecycle.
    let new_user = NewUser {
        id: 1,
        email: "alice@example.com".to_string(),
        bio: "first user".to_string(),
        name: "Alice".to_string(),
        first_order_id: 100,
    };
    let user_id = create_user_with_order(&pool, &new_user).await?;
    println!("created user {user_id} with their first order");

    if let Some((id, name)) = find_user_by_email(&pool, "alice@example.com").await? {
        println!("looked up user {id}: {name}");
    }

    for (order_id, total) in orders_for_user(&pool, user_id).await? {
        println!("  order {order_id}: total = {total:?}");
    }

    // Graceful shutdown: a clean protocol `Terminate` to every idle backend
    // (bounded, best-effort), instead of dropping sockets bare.
    pool.close().await;
    Ok(())
}
