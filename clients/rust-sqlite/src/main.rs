//! Cross-language SQLite benchmark client — Rust / sqlx (sqlite backend).
//!
//! Async (tokio current-thread — the single-connection choice applied equally to
//! every async Rust client). NB sqlx-sqlite runs SQLite on a dedicated
//! background worker thread + channel regardless of the runtime flavour (its
//! architecture), so each query still crosses a thread boundary — the honest
//! sqlx cost, reported as-is.
//!
//! Scenarios / output shape mirror the C + Go SQLite clients: VERSION,
//! LAT <scenario> <ns>, SKIP <scenario> <reason>, RSS <bytes>, ERR. Latency is a
//! 2000-warmup, 7-rep MEDIAN ns/op; every column of every row is decoded. Idiom:
//! sqlx's transparent per-connection statement cache reuses one prepared
//! statement per SQL string (the universal competitor prepared-reuse shape) —
//! mapping to bsql's `parity_sqlite` PREPARED cells; bsql's per-call-prepare /
//! eager API variants have no distinct competitor analogue and are SKIPped.
//!
//! Env:  BENCH_SQLITE_PATH   path to the seeded bench.db (REQUIRED)

use std::hint::black_box;
use std::time::Instant;

use futures::TryStreamExt;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqliteSynchronous,
};
use sqlx::{ConnectOptions, Connection, Row};

const SQL_BY_PK: &str = "SELECT id, name, email FROM bench_users WHERE id = ?";
const SQL_MANY: &str =
    "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?";
const SQL_JOIN: &str = "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
     FROM bench_users u JOIN bench_orders o ON u.id = o.user_id \
     WHERE u.active = 1 GROUP BY u.name ORDER BY SUM(o.amount) DESC LIMIT 100";
const SQL_SUBQ: &str = "SELECT id, name, email FROM bench_users \
     WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)";
const SQL_INS1: &str =
    "INSERT INTO bench_users (name, email, active, score) VALUES (?, ?, 1, 0.0) RETURNING id";
const SQL_INSB: &str =
    "INSERT INTO bench_users (name, email, active, score) VALUES (?, ?, 1, 0.0)";
const SQL_CLEAN: &str = "DELETE FROM bench_users WHERE name = 'bench_insert' OR name LIKE 'batch_%'";

async fn q_by_pk(conn: &mut SqliteConnection, id: i64) -> Result<(), sqlx::Error> {
    let row = sqlx::query(SQL_BY_PK).bind(id).fetch_one(&mut *conn).await?;
    black_box(row.try_get::<i64, _>(0)?);
    black_box(row.try_get::<&str, _>(1)?);
    black_box(row.try_get::<&str, _>(2)?);
    Ok(())
}

async fn q_many(conn: &mut SqliteConnection, limit: i64) -> Result<(), sqlx::Error> {
    let mut stream = sqlx::query(SQL_MANY).bind(limit).fetch(&mut *conn);
    while let Some(row) = stream.try_next().await? {
        black_box(row.try_get::<i64, _>(0)?);
        black_box(row.try_get::<&str, _>(1)?);
        black_box(row.try_get::<&str, _>(2)?);
        black_box(row.try_get::<i64, _>(3)?);
        black_box(row.try_get::<f64, _>(4)?);
    }
    Ok(())
}

async fn q_agg(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut stream = sqlx::query(SQL_JOIN).fetch(&mut *conn);
    while let Some(row) = stream.try_next().await? {
        black_box(row.try_get::<&str, _>(0)?);
        black_box(row.try_get::<i64, _>(1)?);
        black_box(row.try_get::<f64, _>(2)?);
    }
    Ok(())
}

async fn q_subq(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut stream = sqlx::query(SQL_SUBQ).fetch(&mut *conn);
    while let Some(row) = stream.try_next().await? {
        black_box(row.try_get::<i64, _>(0)?);
        black_box(row.try_get::<&str, _>(1)?);
        black_box(row.try_get::<&str, _>(2)?);
    }
    Ok(())
}

async fn q_ins1(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let row = sqlx::query(SQL_INS1)
        .bind("bench_insert")
        .bind("bench@example.com")
        .fetch_one(&mut *conn)
        .await?;
    black_box(row.try_get::<i64, _>(0)?);
    Ok(())
}

async fn q_batch(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut tx = conn.begin().await?;
    for j in 0..100_i32 {
        let name = format!("batch_{j}");
        let email = format!("batch_{j}@example.com");
        sqlx::query(SQL_INSB)
            .bind(&name)
            .bind(&email)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn clean_inserts(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    sqlx::query(SQL_CLEAN).execute(&mut *conn).await?;
    Ok(())
}

/// warmup + 7-rep median ns/op over an async body. `$i` is the loop counter the
/// body may reference (e.g. to cycle the by-PK id).
macro_rules! bench {
    ($label:expr, $warmup:expr, $n:expr, $i:ident, $body:block) => {{
        for $i in 0usize..$warmup {
            $body
        }
        let mut reps = [0u128; 7];
        for __r in 0usize..7 {
            let __t0 = Instant::now();
            for $i in 0usize..$n {
                $body
            }
            reps[__r] = __t0.elapsed().as_nanos() / ($n as u128);
        }
        reps.sort_unstable();
        println!("LAT {} {}", $label, reps[3]);
    }};
}

async fn verify(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let row = sqlx::query(SQL_BY_PK).bind(42_i64).fetch_one(&mut *conn).await?;
    let id: i64 = row.try_get(0)?;
    let name: &str = row.try_get(1)?;
    let email: &str = row.try_get(2)?;
    if id != 42 || name != "user_42" || email != "user_42@example.com" {
        return Err(sqlx::Error::Protocol(format!(
            "verify by_pk id=42 got id={id} name={name} email={email}"
        )));
    }
    let n = sqlx::query(SQL_MANY)
        .bind(10_i64)
        .fetch_all(&mut *conn)
        .await?
        .len();
    if n != 10 {
        return Err(sqlx::Error::Protocol(format!("verify fetch_many/10 got {n} rows")));
    }
    Ok(())
}

async fn latency(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    println!("SKIP sqlite_fetch_one bsql_streaming_per-call-prepare_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared");
    println!("SKIP sqlite_fetch_one_eager bsql_eager-cached_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared");
    println!("SKIP sqlite_fetch_many/10 bsql_per-call-prepare_streaming_10-row;competitor_prepared-reuse=10row_prepared");

    bench!("by_pk_prepared", 2000, 20000, i, { q_by_pk(conn, ((i % 10000) + 1) as i64).await?; });
    bench!("10row_prepared", 2000, 10000, i, { let _ = i; q_many(conn, 10).await?; });
    bench!("sqlite_fetch_many/100", 2000, 5000, i, { let _ = i; q_many(conn, 100).await?; });
    bench!("sqlite_fetch_many/1000", 500, 2000, i, { let _ = i; q_many(conn, 1000).await?; });
    bench!("sqlite_fetch_many/10000", 100, 300, i, { let _ = i; q_many(conn, 10000).await?; });
    bench!("sqlite_join_aggregate", 10, 100, i, { let _ = i; q_agg(conn).await?; });
    bench!("sqlite_subquery", 500, 2000, i, { let _ = i; q_subq(conn).await?; });

    clean_inserts(conn).await?;
    bench!("sqlite_insert_single", 2000, 10000, i, { let _ = i; q_ins1(conn).await?; });
    clean_inserts(conn).await?;
    bench!("sqlite_insert_batch/100", 30, 300, i, { let _ = i; q_batch(conn).await?; });
    clean_inserts(conn).await?;
    Ok(())
}

async fn rss(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    for i in 0..10000_i64 {
        q_by_pk(conn, (i % 10000) + 1).await?;
    }
    clean_inserts(conn).await?;
    for _ in 0..1000 {
        q_ins1(conn).await?;
    }
    clean_inserts(conn).await?;

    // getrusage(RUSAGE_SELF).ru_maxrss — macOS: BYTES (Linux: KiB, divide PEAK by 1024).
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `ru` is a valid, zeroed rusage; RUSAGE_SELF is a valid `who`.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    if rc != 0 {
        return Err(sqlx::Error::Protocol("getrusage failed".into()));
    }
    println!("RSS {}", ru.ru_maxrss);
    println!("PEAK_RSS {:.2}", ru.ru_maxrss as f64 / 1048576.0);
    Ok(())
}

async fn run() -> Result<(), sqlx::Error> {
    let mode = std::env::args()
        .nth(1)
        .ok_or_else(|| sqlx::Error::Protocol("usage: sqlx_sqlite_bench latency|rss".into()))?;
    let path = std::env::var("BENCH_SQLITE_PATH")
        .map_err(|_| sqlx::Error::Protocol("BENCH_SQLITE_PATH must be set".into()))?;

    let opts = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(false)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal);
    let mut conn = opts.connect().await?;

    let ver: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&mut conn)
        .await?;
    println!("VERSION sqlx-sqlite 0.8");
    println!("VERSION sqlite {ver}");

    verify(&mut conn).await?;

    match mode.as_str() {
        "latency" => latency(&mut conn).await?,
        "rss" => rss(&mut conn).await?,
        other => return Err(sqlx::Error::Protocol(format!("unknown mode: {other}"))),
    }
    conn.close().await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        println!("ERR sqlx {e}");
        std::process::exit(1);
    }
}
