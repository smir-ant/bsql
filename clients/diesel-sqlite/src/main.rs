//! Cross-language SQLite benchmark client — Rust / diesel (sqlite backend).
//!
//! diesel's SQLite backend is SYNCHRONOUS (no async peer), so this is a plain
//! blocking client — the honest diesel model. Idiomatic diesel is the TYPED
//! query builder (its statement-cached fast path), used here for by-PK / N-row /
//! insert; the aggregate + subquery are expressed via `sql_query` (the builder
//! cannot cleanly express GROUP BY SUM ORDER-BY-agg / IN(subselect)) — matching
//! the PG diesel client's hybrid shape.
//!
//! Output shape mirrors the C/Go/sqlx SQLite clients: VERSION,
//! LAT <scenario> <ns>, SKIP <scenario> <reason>, RSS <bytes>, ERR. Latency is a
//! 2000-warmup, 7-rep MEDIAN ns/op; every column of every row is decoded. The
//! typed DSL reuses one cached prepared statement per query (the universal
//! competitor prepared-reuse shape) → bsql's `parity_sqlite` PREPARED cells;
//! bsql's per-call-prepare / eager API variants have no distinct diesel analogue
//! and are SKIPped.
//!
//! Env:  BENCH_SQLITE_PATH   path to the seeded bench.db (REQUIRED)

use std::hint::black_box;
use std::time::Instant;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Double, Integer, Text};

// ── schema (tables already seeded; only the benchmarked columns are declared) ──
diesel::table! {
    bench_users (id) {
        id -> Integer,
        name -> Text,
        email -> Text,
        active -> Integer,
        score -> Double,
    }
}
diesel::table! {
    bench_orders (id) {
        id -> Integer,
        user_id -> Integer,
        amount -> Double,
        status -> Text,
    }
}

// sql_query result rows (aggregate + subquery + version).
#[derive(QueryableByName)]
struct AggRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = BigInt)]
    order_count: i64,
    #[diesel(sql_type = Double)]
    total_amount: f64,
}
#[derive(QueryableByName)]
struct R3 {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    email: String,
}
#[derive(QueryableByName)]
struct Ver {
    #[diesel(sql_type = Text)]
    v: String,
}

fn die(scenario: &str, msg: &str) -> ! {
    println!("ERR {scenario} {msg}");
    std::process::exit(1);
}

fn peak_rss_bytes() -> i64 {
    // getrusage(RUSAGE_SELF).ru_maxrss — macOS: BYTES (Linux: KiB).
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `usage` is a valid zeroed rusage; RUSAGE_SELF is a valid `who`.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0;
    }
    usage.ru_maxrss as i64
}

// ── per-scenario single-iteration ops (read every column of every row) ──

fn op_by_pk(conn: &mut SqliteConnection, pk: i32) -> QueryResult<()> {
    let rows: Vec<(i32, String, String)> = bench_users::table
        .filter(bench_users::id.eq(pk))
        .select((bench_users::id, bench_users::name, bench_users::email))
        .load(conn)?;
    for (id, name, email) in &rows {
        black_box(*id);
        black_box(name.len());
        black_box(email.len());
    }
    Ok(())
}

fn op_many(conn: &mut SqliteConnection, limit: i64) -> QueryResult<()> {
    let rows: Vec<(i32, String, String, i32, f64)> = bench_users::table
        .order(bench_users::id.asc())
        .limit(limit)
        .select((
            bench_users::id,
            bench_users::name,
            bench_users::email,
            bench_users::active,
            bench_users::score,
        ))
        .load(conn)?;
    for (id, name, email, active, score) in &rows {
        black_box(*id);
        black_box(name.len());
        black_box(email.len());
        black_box(*active);
        black_box(*score);
    }
    Ok(())
}

fn op_agg(conn: &mut SqliteConnection) -> QueryResult<()> {
    let rows: Vec<AggRow> = diesel::sql_query(
        "SELECT u.name AS name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
         FROM bench_users u JOIN bench_orders o ON u.id = o.user_id \
         WHERE u.active = 1 GROUP BY u.name ORDER BY SUM(o.amount) DESC LIMIT 100",
    )
    .load(conn)?;
    for r in &rows {
        black_box(r.name.len());
        black_box(r.order_count);
        black_box(r.total_amount);
    }
    Ok(())
}

fn op_subq(conn: &mut SqliteConnection) -> QueryResult<()> {
    let rows: Vec<R3> = diesel::sql_query(
        "SELECT id, name, email FROM bench_users \
         WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)",
    )
    .load(conn)?;
    for r in &rows {
        black_box(r.id);
        black_box(r.name.len());
        black_box(r.email.len());
    }
    Ok(())
}

fn op_ins1(conn: &mut SqliteConnection) -> QueryResult<()> {
    // Typed INSERT ... RETURNING id (a real diesel-sqlite capability, >=3.35).
    let id: i32 = diesel::insert_into(bench_users::table)
        .values((
            bench_users::name.eq("bench_insert"),
            bench_users::email.eq("bench@example.com"),
            bench_users::active.eq(1),
            bench_users::score.eq(0.0),
        ))
        .returning(bench_users::id)
        .get_result(conn)?;
    black_box(id);
    Ok(())
}

fn op_batch(conn: &mut SqliteConnection) -> QueryResult<()> {
    conn.transaction(|conn| {
        for j in 0..100_i32 {
            let name = format!("batch_{j}");
            let email = format!("batch_{j}@example.com");
            diesel::insert_into(bench_users::table)
                .values((
                    bench_users::name.eq(&name),
                    bench_users::email.eq(&email),
                    bench_users::active.eq(1),
                    bench_users::score.eq(0.0),
                ))
                .execute(conn)?;
        }
        Ok(())
    })
}

fn clean_inserts(conn: &mut SqliteConnection) -> QueryResult<()> {
    diesel::sql_query("DELETE FROM bench_users WHERE name = 'bench_insert' OR name LIKE 'batch_%'")
        .execute(conn)?;
    Ok(())
}

fn verify(conn: &mut SqliteConnection) {
    let rows: Vec<(i32, String, String)> = bench_users::table
        .filter(bench_users::id.eq(42))
        .select((bench_users::id, bench_users::name, bench_users::email))
        .load(conn)
        .unwrap_or_else(|e| die("verify", &e.to_string()));
    if rows.len() != 1 || rows[0].0 != 42 || rows[0].1 != "user_42" || rows[0].2 != "user_42@example.com"
    {
        die("verify", "by_pk id=42 mismatch");
    }
    let ten: Vec<(i32, String, String, i32, f64)> = bench_users::table
        .order(bench_users::id.asc())
        .limit(10)
        .select((
            bench_users::id,
            bench_users::name,
            bench_users::email,
            bench_users::active,
            bench_users::score,
        ))
        .load(conn)
        .unwrap_or_else(|e| die("verify", &e.to_string()));
    if ten.len() != 10 || ten[9].0 != 10 {
        die("verify", "fetch_many/10 mismatch");
    }
}

fn median(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    v[v.len() / 2]
}

/// warmup + 7-rep median ns/op. `$i` is the loop counter the body may reference.
macro_rules! bench {
    ($name:expr, $warm:expr, $n:expr, $i:ident, $body:block) => {{
        for $i in 0..$warm {
            $body
        }
        let mut reps: Vec<u64> = Vec::with_capacity(7);
        for _rep in 0..7 {
            let start = Instant::now();
            for $i in 0..$n {
                $body
            }
            reps.push(start.elapsed().as_nanos() as u64 / ($n as u64));
        }
        println!("LAT {} {}", $name, median(reps));
    }};
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let path = match std::env::var("BENCH_SQLITE_PATH") {
        Ok(p) => p,
        Err(_) => die("open", "BENCH_SQLITE_PATH must be set"),
    };

    let mut conn = SqliteConnection::establish(&path)
        .unwrap_or_else(|e| die("open", &e.to_string()));
    conn.batch_execute("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .unwrap_or_else(|e| die("pragma", &e.to_string()));

    let ver: Ver = diesel::sql_query("SELECT sqlite_version() AS v")
        .get_result(&mut conn)
        .unwrap_or_else(|e| die("version", &e.to_string()));
    println!("VERSION diesel-sqlite 2.2");
    println!("VERSION sqlite {}", ver.v);

    verify(&mut conn);

    match mode.as_str() {
        "latency" => {
            println!("SKIP sqlite_fetch_one bsql_streaming_per-call-prepare_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared");
            println!("SKIP sqlite_fetch_one_eager bsql_eager-cached_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared");
            println!("SKIP sqlite_fetch_many/10 bsql_per-call-prepare_streaming_10-row;competitor_prepared-reuse=10row_prepared");

            bench!("by_pk_prepared", 2000usize, 20000usize, i, {
                op_by_pk(&mut conn, ((i % 10000) + 1) as i32).unwrap_or_else(|e| die("by_pk_prepared", &e.to_string()));
            });
            bench!("10row_prepared", 2000usize, 10000usize, i, {
                let _ = i;
                op_many(&mut conn, 10).unwrap_or_else(|e| die("10row_prepared", &e.to_string()));
            });
            bench!("sqlite_fetch_many/100", 2000usize, 5000usize, i, {
                let _ = i;
                op_many(&mut conn, 100).unwrap_or_else(|e| die("sqlite_fetch_many/100", &e.to_string()));
            });
            bench!("sqlite_fetch_many/1000", 500usize, 2000usize, i, {
                let _ = i;
                op_many(&mut conn, 1000).unwrap_or_else(|e| die("sqlite_fetch_many/1000", &e.to_string()));
            });
            bench!("sqlite_fetch_many/10000", 100usize, 300usize, i, {
                let _ = i;
                op_many(&mut conn, 10000).unwrap_or_else(|e| die("sqlite_fetch_many/10000", &e.to_string()));
            });
            bench!("sqlite_join_aggregate", 10usize, 100usize, i, {
                let _ = i;
                op_agg(&mut conn).unwrap_or_else(|e| die("sqlite_join_aggregate", &e.to_string()));
            });
            bench!("sqlite_subquery", 500usize, 2000usize, i, {
                let _ = i;
                op_subq(&mut conn).unwrap_or_else(|e| die("sqlite_subquery", &e.to_string()));
            });

            clean_inserts(&mut conn).unwrap_or_else(|e| die("clean", &e.to_string()));
            bench!("sqlite_insert_single", 2000usize, 10000usize, i, {
                let _ = i;
                op_ins1(&mut conn).unwrap_or_else(|e| die("sqlite_insert_single", &e.to_string()));
            });
            clean_inserts(&mut conn).unwrap_or_else(|e| die("clean", &e.to_string()));
            bench!("sqlite_insert_batch/100", 30usize, 300usize, i, {
                let _ = i;
                op_batch(&mut conn).unwrap_or_else(|e| die("sqlite_insert_batch/100", &e.to_string()));
            });
            clean_inserts(&mut conn).unwrap_or_else(|e| die("clean", &e.to_string()));
        }
        "rss" => {
            for i in 0..10000_i32 {
                op_by_pk(&mut conn, (i % 10000) + 1).unwrap_or_else(|e| die("by_pk", &e.to_string()));
            }
            clean_inserts(&mut conn).unwrap_or_else(|e| die("clean", &e.to_string()));
            for _ in 0..1000 {
                op_ins1(&mut conn).unwrap_or_else(|e| die("insert", &e.to_string()));
            }
            clean_inserts(&mut conn).unwrap_or_else(|e| die("clean", &e.to_string()));
            let rss = peak_rss_bytes();
            println!("RSS {rss}");
            println!("PEAK_RSS {:.2}", rss as f64 / 1048576.0);
        }
        other => die("args", &format!("unknown mode: {other}")),
    }
}
