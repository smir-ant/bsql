//! Multi-client PostgreSQL benchmark harness.
//!
//! Usage: pg_bench <client> <mode>
//!   client ∈ { bsql, bsql_sync, tokio_postgres, sqlx }
//!   mode   ∈ { latency, rss }
//!
//! One identical harness (scenarios, iteration counts, warmup, median-of-7,
//! read-every-column) drives all four clients so their numbers are comparable.

use std::hint::black_box;
use std::process::exit;
use std::time::Instant;

// ─── Connection facts (trust auth, no password) ───
const HOST: &str = "127.0.0.1";
const PORT: u16 = 5432;
const USER: &str = "smir-ant";
const DBNAME: &str = "postgres";

// bsql version (from the resolved git dependency; verified against Cargo.lock).
const BSQL_VERSION: &str = "1.0.0-alpha.0";

// ─── Harness constants ───
const WARMUP: u64 = 2000;
const REPS: usize = 7;

// SQL — identical text for every client.
const SQL_BY_PK: &str = "SELECT id, name, val FROM bench_items WHERE id = $1";
const SQL_ROWS: &str = "SELECT id, name, val FROM bench_items WHERE id <= $1 ORDER BY id";
const SQL_INSERT: &str = "INSERT INTO bench_ins (id, name, val) VALUES ($1, $2, $3)";
const SQL_JOIN_AGG: &str = "SELECT c.label, count(*)::int8, sum(i.val)::int8 \
     FROM bench_items i JOIN bench_cat c ON i.val = c.val \
     WHERE i.id <= $1 GROUP BY c.label ORDER BY c.label";

fn n_for(scen: &str) -> u64 {
    match scen {
        "by_pk" => 20000,
        "rows_10" => 10000,
        "rows_100" => 5000,
        "rows_1000" => 2000,
        "insert" => 10000,
        "join_agg" => 500,
        _ => unreachable!(),
    }
}

const SCENARIOS: [&str; 6] = ["by_pk", "rows_10", "rows_100", "rows_1000", "insert", "join_agg"];

/// Median of the 7 per-op rep results (sorted middle element).
fn median7(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    v[REPS / 2]
}

/// Peak resident memory. On macOS `ru_maxrss` is BYTES; on Linux it is KiB.
fn peak_rss_bytes() -> u64 {
    // SAFETY: getrusage into a zeroed rusage is the documented libc contract.
    let ru = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru
    };
    #[cfg(target_os = "macos")]
    {
        ru.ru_maxrss as u64
    }
    #[cfg(not(target_os = "macos"))]
    {
        (ru.ru_maxrss as u64) * 1024
    }
}

fn err_exit(scen: &str, msg: impl std::fmt::Display) -> ! {
    println!("ERR {scen} {msg}");
    exit(1);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let client = args.get(1).map(String::as_str).unwrap_or("");
    let mode = args.get(2).map(String::as_str).unwrap_or("");
    if !matches!(mode, "latency" | "rss") {
        eprintln!("usage: pg_bench <bsql|bsql_sync|tokio_postgres|sqlx> <latency|rss>");
        exit(2);
    }
    match client {
        "bsql" => bsql_async::run(mode),
        "bsql_sync" => bsql_sync_client::run(mode),
        "tokio_postgres" => tokio_pg::run(mode),
        "sqlx" => sqlx_client::run(mode),
        _ => {
            eprintln!("unknown client `{client}`");
            exit(2);
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  bsql — compile-checked query! flagship (binary), ASYNC driver
// ═══════════════════════════════════════════════════════════════

// query! carriers, validated at build time against migrations/. Shared by the
// async AND sync bsql clients (both drivers implement TypedQuery for them).
bsql::query!(ByPk, "SELECT id, name, val FROM bench_items WHERE id = $1");
bsql::query!(RowsN, "SELECT id, name, val FROM bench_items WHERE id <= $1 ORDER BY id");
bsql::query!(
    JoinAgg,
    "SELECT c.label AS label, count(*)::int8 AS cnt, sum(i.val)::int8 AS total \
     FROM bench_items i JOIN bench_cat c ON i.val = c.val \
     WHERE i.id <= $1 GROUP BY c.label ORDER BY c.label"
);

mod bsql_async {
    use super::*;
    use bsql::pg::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new(HOST, USER)
            .port(PORT)
            .database(DBNAME)
            .ssl_mode(SslMode::Disable)
    }

    pub fn run(mode: &str) {
        println!("VERSION bsql-postgres-async {}", BSQL_VERSION);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap_or_else(|e| err_exit("connect", e));
        rt.block_on(async {
            let mut c = Connection::connect(&config())
                .await
                .unwrap_or_else(|e| err_exit("connect", e));
            // Correctness spot-check: by_pk id=1 → name_1, val=2.
            {
                let rows = c
                    .query::<ByPkQuery>((1i32,))
                    .await
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                let rec = rows
                    .iter()
                    .next()
                    .unwrap_or_else(|| err_exit("by_pk", "no row for id=1"))
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                if rec.name != "name_1" || rec.val != 2 {
                    err_exit("by_pk", format!("wrong decode: {} {}", rec.name, rec.val));
                }
            }

            let ins_stmt = c
                .prepare(SQL_INSERT)
                .await
                .unwrap_or_else(|e| err_exit("insert", e));

            if mode == "rss" {
                // reference workload: 10000 by_pk + 1000 inserts
                let mut sink: u64 = 0;
                for id in 1..=10_000i32 {
                    let rows = c
                        .query::<ByPkQuery>((id,))
                        .await
                        .unwrap_or_else(|e| err_exit("by_pk", e));
                    for r in rows.iter() {
                        let r = r.unwrap_or_else(|e| err_exit("by_pk", e));
                        sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                    }
                }
                c.execute_sql("TRUNCATE bench_ins")
                    .await
                    .unwrap_or_else(|e| err_exit("insert", e));
                for id in 1..=1000i64 {
                    c.execute_prepared::<(i64, &str, i32)>(&ins_stmt, &(id, "x", 1))
                        .await
                        .unwrap_or_else(|e| err_exit("insert", e));
                }
                black_box(sink);
                println!("RSS {}", peak_rss_bytes());
                return;
            }

            // latency mode
            for scen in SCENARIOS {
                let n = n_for(scen);
                let mut reps = Vec::with_capacity(REPS);
                let mut sink: u64 = 0;
                let mut pk: i32 = 0;
                let mut ins_id: i64 = 0;
                if scen == "insert" {
                    c.execute_sql("TRUNCATE bench_ins")
                        .await
                        .unwrap_or_else(|e| err_exit("insert", e));
                }
                // warmup
                for _ in 0..WARMUP {
                    sink = sink.wrapping_add(
                        run_one_async(&mut c, &ins_stmt, scen, &mut pk, &mut ins_id).await,
                    );
                }
                for _ in 0..REPS {
                    let t = Instant::now();
                    for _ in 0..n {
                        sink = sink.wrapping_add(
                            run_one_async(&mut c, &ins_stmt, scen, &mut pk, &mut ins_id).await,
                        );
                    }
                    let ns = t.elapsed().as_nanos() as u64 / n;
                    reps.push(ns);
                }
                black_box(sink);
                println!("LAT {scen} {}", median7(reps));
            }
            let _ = c.close().await;
        });
    }

    async fn run_one_async(
        c: &mut Connection,
        ins_stmt: &bsql::pg::PreparedStatement,
        scen: &str,
        pk: &mut i32,
        ins_id: &mut i64,
    ) -> u64 {
        let mut sink: u64 = 0;
        match scen {
            "by_pk" => {
                *pk = (*pk % 10_000) + 1;
                let rows = c
                    .query::<ByPkQuery>((*pk,))
                    .await
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit("by_pk", e));
                    sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                }
            }
            "rows_10" | "rows_100" | "rows_1000" => {
                let lim: i32 = match scen {
                    "rows_10" => 10,
                    "rows_100" => 100,
                    _ => 1000,
                };
                let rows = c
                    .query::<RowsNQuery>((lim,))
                    .await
                    .unwrap_or_else(|e| err_exit(scen, e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit(scen, e));
                    sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                }
            }
            "insert" => {
                *ins_id += 1;
                c.execute_prepared::<(i64, &str, i32)>(ins_stmt, &(*ins_id, "x", 1))
                    .await
                    .unwrap_or_else(|e| err_exit("insert", e));
                sink = sink.wrapping_add(*ins_id as u64);
            }
            "join_agg" => {
                let rows = c
                    .query::<JoinAggQuery>((10_000i32,))
                    .await
                    .unwrap_or_else(|e| err_exit("join_agg", e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit("join_agg", e));
                    sink = sink
                        .wrapping_add(r.label.len() as u64 + total_u64(r.cnt) + total_u64(r.total));
                }
            }
            _ => unreachable!(),
        }
        sink
    }
}

// join_agg `sum` is nullable in PG's type system; accept either shape.
#[inline]
fn total_u64(t: Option<i64>) -> u64 {
    t.unwrap_or(0) as u64
}

// ═══════════════════════════════════════════════════════════════
//  bsql — compile-checked query! flagship (binary), SYNC driver
// ═══════════════════════════════════════════════════════════════
mod bsql_sync_client {
    use super::*;
    use bsql::pg_sync::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new(HOST, USER)
            .port(PORT)
            .database(DBNAME)
            .ssl_mode(SslMode::Disable)
    }

    pub fn run(mode: &str) {
        println!("VERSION bsql-postgres-sync {}", BSQL_VERSION);
        let mut c = Connection::connect(&config()).unwrap_or_else(|e| err_exit("connect", e));
        {
            let rows = c
                .query::<ByPkQuery>((1i32,))
                .unwrap_or_else(|e| err_exit("by_pk", e));
            let rec = rows
                .iter()
                .next()
                .unwrap_or_else(|| err_exit("by_pk", "no row for id=1"))
                .unwrap_or_else(|e| err_exit("by_pk", e));
            if rec.name != "name_1" || rec.val != 2 {
                err_exit("by_pk", format!("wrong decode: {} {}", rec.name, rec.val));
            }
        }
        let ins_stmt = c.prepare(SQL_INSERT).unwrap_or_else(|e| err_exit("insert", e));

        if mode == "rss" {
            let mut sink: u64 = 0;
            for id in 1..=10_000i32 {
                let rows = c
                    .query::<ByPkQuery>((id,))
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit("by_pk", e));
                    sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                }
            }
            c.execute_sql("TRUNCATE bench_ins")
                .unwrap_or_else(|e| err_exit("insert", e));
            for id in 1..=1000i64 {
                c.execute_prepared::<(i64, &str, i32)>(&ins_stmt, &(id, "x", 1))
                    .unwrap_or_else(|e| err_exit("insert", e));
            }
            black_box(sink);
            println!("RSS {}", peak_rss_bytes());
            return;
        }

        for scen in SCENARIOS {
            let n = n_for(scen);
            let mut reps = Vec::with_capacity(REPS);
            let mut sink: u64 = 0;
            let mut pk: i32 = 0;
            let mut ins_id: i64 = 0;
            if scen == "insert" {
                c.execute_sql("TRUNCATE bench_ins")
                    .unwrap_or_else(|e| err_exit("insert", e));
            }
            for _ in 0..WARMUP {
                sink = sink.wrapping_add(run_one_sync(&mut c, &ins_stmt, scen, &mut pk, &mut ins_id));
            }
            for _ in 0..REPS {
                let t = Instant::now();
                for _ in 0..n {
                    sink =
                        sink.wrapping_add(run_one_sync(&mut c, &ins_stmt, scen, &mut pk, &mut ins_id));
                }
                let ns = t.elapsed().as_nanos() as u64 / n;
                reps.push(ns);
            }
            black_box(sink);
            println!("LAT {scen} {}", median7(reps));
        }
        let _ = c.close();
    }

    fn run_one_sync(
        c: &mut Connection,
        ins_stmt: &bsql::pg_sync::PreparedStatement,
        scen: &str,
        pk: &mut i32,
        ins_id: &mut i64,
    ) -> u64 {
        let mut sink: u64 = 0;
        match scen {
            "by_pk" => {
                *pk = (*pk % 10_000) + 1;
                let rows = c
                    .query::<ByPkQuery>((*pk,))
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit("by_pk", e));
                    sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                }
            }
            "rows_10" | "rows_100" | "rows_1000" => {
                let lim: i32 = match scen {
                    "rows_10" => 10,
                    "rows_100" => 100,
                    _ => 1000,
                };
                let rows = c
                    .query::<RowsNQuery>((lim,))
                    .unwrap_or_else(|e| err_exit(scen, e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit(scen, e));
                    sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                }
            }
            "insert" => {
                *ins_id += 1;
                c.execute_prepared::<(i64, &str, i32)>(ins_stmt, &(*ins_id, "x", 1))
                    .unwrap_or_else(|e| err_exit("insert", e));
                sink = sink.wrapping_add(*ins_id as u64);
            }
            "join_agg" => {
                let rows = c
                    .query::<JoinAggQuery>((10_000i32,))
                    .unwrap_or_else(|e| err_exit("join_agg", e));
                for r in rows.iter() {
                    let r = r.unwrap_or_else(|e| err_exit("join_agg", e));
                    sink =
                        sink.wrapping_add(r.label.len() as u64 + total_u64(r.cnt) + total_u64(r.total));
                }
            }
            _ => unreachable!(),
        }
        sink
    }
}

// ═══════════════════════════════════════════════════════════════
//  tokio-postgres 0.7 (binary prepared)
// ═══════════════════════════════════════════════════════════════
mod tokio_pg {
    use super::*;
    use tokio_postgres::{NoTls, Statement};

    pub fn run(mode: &str) {
        println!("VERSION tokio-postgres 0.7.18");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap_or_else(|e| err_exit("connect", e));
        rt.block_on(async {
            let conninfo = format!(
                "host={HOST} port={PORT} user={USER} dbname={DBNAME} sslmode=disable"
            );
            let (client, connection) = tokio_postgres::connect(&conninfo, NoTls)
                .await
                .unwrap_or_else(|e| err_exit("connect", e));
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let by_pk = client.prepare(SQL_BY_PK).await.unwrap_or_else(|e| err_exit("by_pk", e));
            let rows_stmt = client.prepare(SQL_ROWS).await.unwrap_or_else(|e| err_exit("rows", e));
            let ins = client.prepare(SQL_INSERT).await.unwrap_or_else(|e| err_exit("insert", e));
            let jagg = client.prepare(SQL_JOIN_AGG).await.unwrap_or_else(|e| err_exit("join_agg", e));

            // spot-check
            {
                let row = client
                    .query_one(&by_pk, &[&1i32])
                    .await
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                let name: &str = row.get(1);
                let val: i32 = row.get(2);
                if name != "name_1" || val != 2 {
                    err_exit("by_pk", format!("wrong decode: {name} {val}"));
                }
            }

            if mode == "rss" {
                let mut sink: u64 = 0;
                for id in 1..=10_000i32 {
                    let rs = client
                        .query(&by_pk, &[&id])
                        .await
                        .unwrap_or_else(|e| err_exit("by_pk", e));
                    for row in &rs {
                        let rid: i32 = row.get(0);
                        let name: &str = row.get(1);
                        let val: i32 = row.get(2);
                        sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                    }
                }
                client
                    .execute("TRUNCATE bench_ins", &[])
                    .await
                    .unwrap_or_else(|e| err_exit("insert", e));
                for id in 1..=1000i64 {
                    client
                        .execute(&ins, &[&id, &"x", &1i32])
                        .await
                        .unwrap_or_else(|e| err_exit("insert", e));
                }
                black_box(sink);
                println!("RSS {}", peak_rss_bytes());
                return;
            }

            for scen in SCENARIOS {
                let n = n_for(scen);
                let mut reps = Vec::with_capacity(REPS);
                let mut sink: u64 = 0;
                let mut pk: i32 = 0;
                let mut ins_id: i64 = 0;
                if scen == "insert" {
                    client
                        .execute("TRUNCATE bench_ins", &[])
                        .await
                        .unwrap_or_else(|e| err_exit("insert", e));
                }
                for _ in 0..WARMUP {
                    sink = sink.wrapping_add(
                        one(&client, &by_pk, &rows_stmt, &ins, &jagg, scen, &mut pk, &mut ins_id)
                            .await,
                    );
                }
                for _ in 0..REPS {
                    let t = Instant::now();
                    for _ in 0..n {
                        sink = sink.wrapping_add(
                            one(&client, &by_pk, &rows_stmt, &ins, &jagg, scen, &mut pk, &mut ins_id)
                                .await,
                        );
                    }
                    let ns = t.elapsed().as_nanos() as u64 / n;
                    reps.push(ns);
                }
                black_box(sink);
                println!("LAT {scen} {}", median7(reps));
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn one(
        client: &tokio_postgres::Client,
        by_pk: &Statement,
        rows_stmt: &Statement,
        ins: &Statement,
        jagg: &Statement,
        scen: &str,
        pk: &mut i32,
        ins_id: &mut i64,
    ) -> u64 {
        let mut sink: u64 = 0;
        match scen {
            "by_pk" => {
                *pk = (*pk % 10_000) + 1;
                let rs = client
                    .query(by_pk, &[&*pk])
                    .await
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                for row in &rs {
                    let rid: i32 = row.get(0);
                    let name: &str = row.get(1);
                    let val: i32 = row.get(2);
                    sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                }
            }
            "rows_10" | "rows_100" | "rows_1000" => {
                let lim: i32 = match scen {
                    "rows_10" => 10,
                    "rows_100" => 100,
                    _ => 1000,
                };
                let rs = client
                    .query(rows_stmt, &[&lim])
                    .await
                    .unwrap_or_else(|e| err_exit(scen, e));
                for row in &rs {
                    let rid: i32 = row.get(0);
                    let name: &str = row.get(1);
                    let val: i32 = row.get(2);
                    sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                }
            }
            "insert" => {
                *ins_id += 1;
                client
                    .execute(ins, &[&*ins_id, &"x", &1i32])
                    .await
                    .unwrap_or_else(|e| err_exit("insert", e));
                sink = sink.wrapping_add(*ins_id as u64);
            }
            "join_agg" => {
                let rs = client
                    .query(jagg, &[&10_000i32])
                    .await
                    .unwrap_or_else(|e| err_exit("join_agg", e));
                for row in &rs {
                    let label: &str = row.get(0);
                    let cnt: i64 = row.get(1);
                    let total: Option<i64> = row.get(2);
                    sink = sink.wrapping_add(label.len() as u64 + cnt as u64 + total_u64(total));
                }
            }
            _ => unreachable!(),
        }
        sink
    }
}

// ═══════════════════════════════════════════════════════════════
//  sqlx 0.8 (postgres, runtime-tokio; per-connection prepared cache)
// ═══════════════════════════════════════════════════════════════
mod sqlx_client {
    use super::*;
    use futures_util::TryStreamExt;
    use sqlx::{Connection as _, Executor, Row};

    pub fn run(mode: &str) {
        println!("VERSION sqlx 0.8.6");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap_or_else(|e| err_exit("connect", e));
        rt.block_on(async {
            let url = format!("postgres://{USER}@{HOST}:{PORT}/{DBNAME}?sslmode=disable");
            let mut conn = sqlx::postgres::PgConnection::connect(&url)
                .await
                .unwrap_or_else(|e| err_exit("connect", e));

            // spot-check (also warms the statement cache for by_pk)
            {
                let row = sqlx::query(SQL_BY_PK)
                    .bind(1i32)
                    .fetch_one(&mut conn)
                    .await
                    .unwrap_or_else(|e| err_exit("by_pk", e));
                let name: &str = row.get(1);
                let val: i32 = row.get(2);
                if name != "name_1" || val != 2 {
                    err_exit("by_pk", format!("wrong decode: {name} {val}"));
                }
            }

            if mode == "rss" {
                let mut sink: u64 = 0;
                for id in 1..=10_000i32 {
                    let mut s = sqlx::query(SQL_BY_PK).bind(id).fetch(&mut conn);
                    while let Some(row) =
                        s.try_next().await.unwrap_or_else(|e| err_exit("by_pk", e))
                    {
                        let rid: i32 = row.get(0);
                        let name: &str = row.get(1);
                        let val: i32 = row.get(2);
                        sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                    }
                }
                conn.execute("TRUNCATE bench_ins")
                    .await
                    .unwrap_or_else(|e| err_exit("insert", e));
                for id in 1..=1000i64 {
                    sqlx::query(SQL_INSERT)
                        .bind(id)
                        .bind("x")
                        .bind(1i32)
                        .execute(&mut conn)
                        .await
                        .unwrap_or_else(|e| err_exit("insert", e));
                }
                black_box(sink);
                println!("RSS {}", peak_rss_bytes());
                return;
            }

            for scen in SCENARIOS {
                let n = n_for(scen);
                let mut reps = Vec::with_capacity(REPS);
                let mut sink: u64 = 0;
                let mut pk: i32 = 0;
                let mut ins_id: i64 = 0;
                if scen == "insert" {
                    conn.execute("TRUNCATE bench_ins")
                        .await
                        .unwrap_or_else(|e| err_exit("insert", e));
                }
                for _ in 0..WARMUP {
                    sink = sink.wrapping_add(one(&mut conn, scen, &mut pk, &mut ins_id).await);
                }
                for _ in 0..REPS {
                    let t = Instant::now();
                    for _ in 0..n {
                        sink = sink.wrapping_add(one(&mut conn, scen, &mut pk, &mut ins_id).await);
                    }
                    let ns = t.elapsed().as_nanos() as u64 / n;
                    reps.push(ns);
                }
                black_box(sink);
                println!("LAT {scen} {}", median7(reps));
            }
            let _ = conn.close().await;
        });
    }

    async fn one(
        conn: &mut sqlx::postgres::PgConnection,
        scen: &str,
        pk: &mut i32,
        ins_id: &mut i64,
    ) -> u64 {
        let mut sink: u64 = 0;
        match scen {
            "by_pk" => {
                *pk = (*pk % 10_000) + 1;
                let mut s = sqlx::query(SQL_BY_PK).bind(*pk).fetch(&mut *conn);
                while let Some(row) = s.try_next().await.unwrap_or_else(|e| err_exit("by_pk", e)) {
                    let rid: i32 = row.get(0);
                    let name: &str = row.get(1);
                    let val: i32 = row.get(2);
                    sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                }
            }
            "rows_10" | "rows_100" | "rows_1000" => {
                let lim: i32 = match scen {
                    "rows_10" => 10,
                    "rows_100" => 100,
                    _ => 1000,
                };
                let mut s = sqlx::query(SQL_ROWS).bind(lim).fetch(&mut *conn);
                while let Some(row) = s.try_next().await.unwrap_or_else(|e| err_exit(scen, e)) {
                    let rid: i32 = row.get(0);
                    let name: &str = row.get(1);
                    let val: i32 = row.get(2);
                    sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                }
            }
            "insert" => {
                *ins_id += 1;
                sqlx::query(SQL_INSERT)
                    .bind(*ins_id)
                    .bind("x")
                    .bind(1i32)
                    .execute(&mut *conn)
                    .await
                    .unwrap_or_else(|e| err_exit("insert", e));
                sink = sink.wrapping_add(*ins_id as u64);
            }
            "join_agg" => {
                let mut s = sqlx::query(SQL_JOIN_AGG).bind(10_000i32).fetch(&mut *conn);
                while let Some(row) =
                    s.try_next().await.unwrap_or_else(|e| err_exit("join_agg", e))
                {
                    let label: &str = row.get(0);
                    let cnt: i64 = row.get(1);
                    let total: Option<i64> = row.get(2);
                    sink = sink.wrapping_add(label.len() as u64 + cnt as u64 + total_u64(total));
                }
            }
            _ => unreachable!(),
        }
        sink
    }
}
