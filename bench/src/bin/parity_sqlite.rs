//! PARITY perf runner — the REBUILD's SQLite driver over the ORIGINAL benchmark
//! matrix (`bench_users` / `bench_orders`, original SQL, original iteration
//! counts), printed as `ns/op` in the SAME shape as `bench/c/sqlite_bench.c`.
//!
//! Read path: mostly the STREAMING `query_each_*` verbs (constant-memory,
//! zero-copy borrowed rows), reading EVERY column via `value_ref` — the closest
//! analogue to C's `sqlite3_step` + per-column read loop.
//!
//! STRUCTURAL NOTE on prepared-statement caching: the rebuild caches compiled
//! statements per connection for the EAGER, EXECUTE and typed-single-row verbs
//! (a by-key lookup or insert re-run in a loop pays NO per-call
//! `sqlite3_prepare_v2`), but NOT for the zero-copy STREAMING `query_each_*`
//! verbs. rusqlite's statement cache forces `SQLITE_PREPARE_PERSISTENT`, which
//! bypasses SQLite's lookaside pool and measurably slows multi-row stepping;
//! streaming has no per-row materialization to hide that cost, so caching it
//! would REGRESS large-N streaming. Hence the streaming `sqlite_fetch_*` cells
//! below still pay a per-call `prepare` (matching the pre-cache behavior — never
//! regressed), while `sqlite_fetch_one_eager` (the idiomatic by-PK verb,
//! `query_params_one`) and the `sqlite_insert_*` cells ride the cache.
//!
//! rusqlite opens with `SQLITE_OPEN_NO_MUTEX` by default (serialized externally
//! by `&mut`/`&`), matching the C runner's NOMUTEX. `open()` sets WAL; this
//! runner adds `synchronous=NORMAL` to match the C PRAGMAs.
//!
//! Env:
//!   BENCH_SQLITE_PATH   path to the bench database file (REQUIRED)

use std::hint::black_box;
use std::ops::ControlFlow;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use bsql::sqlite::{BorrowedRow, Connection, SqliteError, ValueRef};

const ITERS_DEFAULT: u64 = 10_000;
const ITERS_BIG: u64 = 1_000; // 10k-row fetch, batch insert (matches sqlite_bench.c)
const ITERS_JOIN: u64 = 300; // 22 ms/op — a mean is stable well below C's 1000
const ITERS_SUBQUERY: u64 = 5_000;

fn report(label: &str, elapsed: Duration, iters: u64) {
    let ns = elapsed.as_nanos() / u128::from(iters);
    println!("{label}: {ns} ns/op  ({iters} iters)\tKV\t{label}\t{ns}\t{iters}");
}

/// Touch every column of a streamed row zero-copy (the borrowed `ValueRef`
/// view), mirroring C's per-column read. Always `Continue` (drain the row set).
fn touch_all(row: &BorrowedRow<'_>) -> ControlFlow<SqliteError> {
    let n = row.column_count();
    for col in 0..n {
        match row.value_ref(col) {
            Ok(v) => {
                black_box(v);
            }
            Err(e) => return ControlFlow::Break(e),
        }
    }
    ControlFlow::Continue(())
}

fn bench_fetch_one(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email FROM bench_users WHERE id = ?1";
    let p = [ValueRef::Integer(42)];
    conn.query_each_params(sql, &p, |r| touch_all(&r))?; // warm up
    let start = Instant::now();
    for _ in 0..ITERS_DEFAULT {
        if let Some(e) = conn.query_each_params(black_box(sql), &p, |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report("sqlite_fetch_one", start.elapsed(), ITERS_DEFAULT);
    Ok(())
}

/// By-PK via the EAGER at-most-one verb (`query_params_one`) — the idiomatic
/// single-row fetch, and the path the prepared-statement cache accelerates
/// (eager materialization of one row masks the cache's `PERSISTENT`-flag cost,
/// so caching is a clean win here, unlike the zero-copy streaming path above).
/// Reads every column of the one row, mirroring `bench_fetch_one`'s per-column
/// touch, so the two by-PK cells are directly comparable (streaming-uncached vs
/// eager-cached).
fn bench_fetch_one_eager(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email FROM bench_users WHERE id = ?1";
    let p = [ValueRef::Integer(42)];
    let cols = conn.query_params_one(sql, &p)?.column_count(); // warm up + width
    let start = Instant::now();
    for _ in 0..ITERS_DEFAULT {
        let row = conn.query_params_one(black_box(sql), &p)?;
        for col in 0..cols {
            black_box(row.value_ref(col)?);
        }
    }
    report("sqlite_fetch_one_eager", start.elapsed(), ITERS_DEFAULT);
    Ok(())
}

fn bench_fetch_many(conn: &Connection, limit: i64) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1";
    let p = [ValueRef::Integer(limit)];
    conn.query_each_params(sql, &p, |r| touch_all(&r))?; // warm up
    let iters = if limit >= 10_000 { ITERS_BIG } else { ITERS_DEFAULT };
    let start = Instant::now();
    for _ in 0..iters {
        if let Some(e) = conn.query_each_params(black_box(sql), &p, |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report(&format!("sqlite_fetch_many/{limit}"), start.elapsed(), iters);
    Ok(())
}

fn bench_insert_single(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "INSERT INTO bench_users (name, email, active, score) \
               VALUES (?1, ?2, 1, 0.0) RETURNING id";
    let p = [
        ValueRef::Text(b"bench_insert"),
        ValueRef::Text(b"bench@example.com"),
    ];
    conn.query_each_params(sql, &p, |r| touch_all(&r))?; // warm up (reads id)
    let start = Instant::now();
    for _ in 0..ITERS_DEFAULT {
        if let Some(e) = conn.query_each_params(black_box(sql), &p, |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report("sqlite_insert_single", start.elapsed(), ITERS_DEFAULT);
    Ok(())
}

/// 100 DISCRETE INSERTs inside one transaction — the honest comparable to C's
/// `bench_insert_batch`. Each `execute_params` re-prepares (see the module note),
/// unlike C's one-prepared-statement reuse.
fn bench_insert_batch(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)";
    let run_batch = || -> Result<(), SqliteError> {
        conn.transaction(|tx| {
            for j in 0..100_i32 {
                let name = format!("batch_{j}");
                let email = format!("batch_{j}@example.com");
                let p = [
                    ValueRef::Text(name.as_bytes()),
                    ValueRef::Text(email.as_bytes()),
                ];
                tx.execute_params(sql, &p)?;
            }
            Ok(())
        })
    };
    run_batch()?; // warm up
    let start = Instant::now();
    for _ in 0..ITERS_BIG {
        run_batch()?;
    }
    report("sqlite_insert_batch/100", start.elapsed(), ITERS_BIG);
    Ok(())
}

fn bench_join_aggregate(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
               FROM bench_users u \
               JOIN bench_orders o ON u.id = o.user_id \
               WHERE u.active = 1 \
               GROUP BY u.name \
               ORDER BY SUM(o.amount) DESC \
               LIMIT 100";
    conn.query_each_sql(sql, |r| touch_all(&r))?; // warm up
    let start = Instant::now();
    for _ in 0..ITERS_JOIN {
        if let Some(e) = conn.query_each_sql(black_box(sql), |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report("sqlite_join_aggregate", start.elapsed(), ITERS_JOIN);
    Ok(())
}

fn bench_subquery(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email FROM bench_users \
               WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)";
    conn.query_each_sql(sql, |r| touch_all(&r))?; // warm up
    let start = Instant::now();
    for _ in 0..ITERS_SUBQUERY {
        if let Some(e) = conn.query_each_sql(black_box(sql), |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report("sqlite_subquery", start.elapsed(), ITERS_SUBQUERY);
    Ok(())
}

fn run() -> Result<(), SqliteError> {
    let path = std::env::var("BENCH_SQLITE_PATH")
        .map_err(|_| SqliteError::Open("BENCH_SQLITE_PATH must be set".to_owned()))?;
    let conn = Connection::open(&path)?;
    conn.execute_sql("PRAGMA synchronous=NORMAL")?;
    println!("=== rebuild bsql (bundled SQLite) Benchmarks ===");
    println!("path={path}\n");

    // READS FIRST on the pristine table (the JOIN scan is sensitive to
    // `bench_users` size, so measuring it before the INSERT scenarios grow the
    // table removes the bloat confound — same rationale as the PG runner).
    bench_fetch_one(&conn)?;
    bench_fetch_one_eager(&conn)?;
    bench_fetch_many(&conn, 10)?;
    bench_fetch_many(&conn, 100)?;
    bench_fetch_many(&conn, 1_000)?;
    bench_fetch_many(&conn, 10_000)?;
    bench_join_aggregate(&conn)?;
    bench_subquery(&conn)?;
    bench_insert_single(&conn)?;
    bench_insert_batch(&conn)?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("parity_sqlite: {e:?}");
            ExitCode::FAILURE
        }
    }
}
