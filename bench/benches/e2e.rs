//! End-to-end latency benchmarks: the rebuild's bsql (async + sync) vs the Rust
//! competitors (`tokio-postgres`, `sqlx`), side by side, over the SAME server,
//! the SAME transport (loopback TCP), and the SAME SQL — so a group's four bars
//! are apples-to-apples.
//!
//! # Methodology (matches the original bsql harness)
//!
//! Every iteration does IDENTICAL work: bind a pre-prepared statement, send it,
//! receive ALL rows, and read EVERY column of every row (the read is what forces
//! the driver to actually decode, not just frame). Statements are prepared ONCE
//! before the timed loop — the cache-HIT path a real workload spends its life on
//! (tokio-postgres prepares explicitly; sqlx caches per connection; bsql holds a
//! `PreparedStatement`). A single direct connection per client (no pool) so the
//! number is the driver's per-round-trip cost, not pool bookkeeping.
//!
//! # Noise control
//!
//! - Loopback TCP: no wire, no switch, in-kernel — the network is removed as a
//!   noise source. (bsql's drivers are TCP-only today, so every client uses TCP
//!   for fairness; see the README for the unix-socket follow-up.)
//! - `bench/setup/pg_setup.sql` disables autovacuum on the bench tables and
//!   CHECKPOINTs before measuring, so no background vacuum/checkpoint fires
//!   mid-sample.
//! - criterion's warm-up + resampling reports a 95% confidence interval per
//!   number; run on a quiet machine (`scripts/bench-stable.sh` gates load).

use std::cell::RefCell;
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use bsql_bench as h;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sqlx::{Connection as _, Row as _};
use tokio::runtime::Runtime;

// ---- fixture helpers -------------------------------------------------------

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Read every column of a bsql `(id int4, name text, val int4)` result.
fn read_select_bsql(qr: &bsql::pg::QueryResult) -> i64 {
    let mut acc = 0_i64;
    for row in &qr.rows {
        let id = row.get_i32(0).expect("id decodes");
        let name = row.get_str(1).expect("name decodes");
        let val = row.get_i32(2).expect("val decodes");
        acc = acc
            .wrapping_add(id.map_or(0, i64::from))
            .wrapping_add(name.map_or(0, |s| s.len() as i64))
            .wrapping_add(val.map_or(0, i64::from));
    }
    acc
}

/// Read every column of a bsql `(label text, count int8, sum int8)` result.
fn read_agg_bsql(qr: &bsql::pg::QueryResult) -> i64 {
    let mut acc = 0_i64;
    for row in &qr.rows {
        let label = row.get_str(0).expect("label decodes");
        let cnt = row.get_i64(1).expect("count decodes");
        let sm = row.get_i64(2).expect("sum decodes");
        acc = acc
            .wrapping_add(label.map_or(0, |s| s.len() as i64))
            .wrapping_add(cnt.unwrap_or(0))
            .wrapping_add(sm.unwrap_or(0));
    }
    acc
}

fn read_select_tp(rows: &[tokio_postgres::Row]) -> i64 {
    let mut acc = 0_i64;
    for row in rows {
        let id: i32 = row.get(0);
        let name: &str = row.get(1);
        let val: i32 = row.get(2);
        acc = acc
            .wrapping_add(i64::from(id))
            .wrapping_add(name.len() as i64)
            .wrapping_add(i64::from(val));
    }
    acc
}

fn read_agg_tp(rows: &[tokio_postgres::Row]) -> i64 {
    let mut acc = 0_i64;
    for row in rows {
        let label: &str = row.get(0);
        let cnt: i64 = row.get(1);
        let sm: i64 = row.get(2);
        acc = acc
            .wrapping_add(label.len() as i64)
            .wrapping_add(cnt)
            .wrapping_add(sm);
    }
    acc
}

fn read_select_sqlx(rows: &[sqlx::postgres::PgRow]) -> i64 {
    let mut acc = 0_i64;
    for row in rows {
        let id: i32 = row.try_get(0).expect("id decodes");
        let name: &str = row.try_get(1).expect("name decodes");
        let val: i32 = row.try_get(2).expect("val decodes");
        acc = acc
            .wrapping_add(i64::from(id))
            .wrapping_add(name.len() as i64)
            .wrapping_add(i64::from(val));
    }
    acc
}

fn read_agg_sqlx(rows: &[sqlx::postgres::PgRow]) -> i64 {
    let mut acc = 0_i64;
    for row in rows {
        let label: &str = row.try_get(0).expect("label decodes");
        let cnt: i64 = row.try_get(1).expect("count decodes");
        let sm: i64 = row.try_get(2).expect("sum decodes");
        acc = acc
            .wrapping_add(label.len() as i64)
            .wrapping_add(cnt)
            .wrapping_add(sm);
    }
    acc
}

// ---- SELECT scenarios ------------------------------------------------------

/// One group per row-count: `by_pk` (1 row, `= $1`) and range fetches
/// (`<= $1`) at 10 / 100 / 1000 rows. Each group holds all four clients.
fn bench_select(c: &mut Criterion) {
    // (label, sql, param, expected_rows)
    let scenarios: &[(&str, &str, i32, u64)] = &[
        ("by_pk", h::SQL_SELECT_BY_PK, 5000, 1),
        ("rows_10", h::SQL_SELECT_RANGE, 10, 10),
        ("rows_100", h::SQL_SELECT_RANGE, 100, 100),
        ("rows_1000", h::SQL_SELECT_RANGE, 1000, 1000),
    ];
    let rt = runtime();

    for &(label, sql, param, n_rows) in scenarios {
        let mut g = c.benchmark_group(format!("select/{label}"));
        g.throughput(Throughput::Elements(n_rows));

        // bsql sync
        {
            let mut conn =
                bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
            let stmt = conn.prepare(sql).expect("bsql sync prepare");
            g.bench_function(BenchmarkId::from_parameter("bsql_sync"), |b| {
                b.iter(|| {
                    let qr = conn
                        .query_prepared(&stmt, &(black_box(param),))
                        .expect("bsql sync query");
                    black_box(read_select_bsql(&qr))
                });
            });
            let _ = conn.close_statement(stmt);
        }

        // bsql async
        {
            let mut conn = rt
                .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
                .expect("bsql async connect");
            let stmt = rt.block_on(conn.prepare(sql)).expect("bsql async prepare");
            let conn = RefCell::new(conn);
            g.bench_function(BenchmarkId::from_parameter("bsql_async"), |b| {
                b.to_async(&rt).iter(|| async {
                    let qr = conn
                        .borrow_mut()
                        .query_prepared(&stmt, &(black_box(param),))
                        .await
                        .expect("bsql async query");
                    black_box(read_select_bsql(&qr))
                });
            });
        }

        // tokio-postgres
        {
            let (client, connection) = rt
                .block_on(tokio_postgres::connect(
                    &h::pg_conn_string(),
                    tokio_postgres::NoTls,
                ))
                .expect("tokio-postgres connect");
            let conn_task = rt.spawn(async move { connection.await });
            let stmt = rt.block_on(client.prepare(sql)).expect("tp prepare");
            g.bench_function(BenchmarkId::from_parameter("tokio_postgres"), |b| {
                b.to_async(&rt).iter(|| async {
                    let rows = client
                        .query(&stmt, &[&black_box(param)])
                        .await
                        .expect("tp query");
                    black_box(read_select_tp(&rows))
                });
            });
            drop(client);
            rt.block_on(async { conn_task.await.ok() });
        }

        // sqlx
        {
            let conn = rt
                .block_on(sqlx::postgres::PgConnection::connect(&h::pg_url()))
                .expect("sqlx connect");
            let conn = RefCell::new(conn);
            g.bench_function(BenchmarkId::from_parameter("sqlx"), |b| {
                b.to_async(&rt).iter(|| async {
                    let rows = sqlx::query(sql)
                        .bind(black_box(param))
                        .fetch_all(&mut *conn.borrow_mut())
                        .await
                        .expect("sqlx query");
                    black_box(read_select_sqlx(&rows))
                });
            });
        }

        g.finish();
    }
}

// ---- INSERT scenario -------------------------------------------------------

/// Single-row prepared INSERT into the unlogged sink. Each client writes into a
/// disjoint id range (a per-client atomic counter) so the shared `bench_ins`
/// primary key never collides across clients.
fn bench_insert(c: &mut Criterion) {
    let rt = runtime();
    // Reset the sink once so the run starts from an empty table.
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("connect for truncate");
        conn.execute_sql("TRUNCATE bench_ins").expect("truncate");
    }

    let mut g = c.benchmark_group("insert/single");

    // bsql sync — id base 0
    {
        let ctr = AtomicI64::new(0);
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
        let stmt = conn.prepare(h::SQL_INSERT_ONE).expect("bsql sync prepare");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync"), |b| {
            b.iter(|| {
                let id = ctr.fetch_add(1, Ordering::Relaxed);
                let n = conn
                    .execute_prepared(&stmt, &(id, "ins", 7_i32))
                    .expect("bsql sync insert");
                black_box(n)
            });
        });
        let _ = conn.close_statement(stmt);
    }

    // bsql async — id base 1e15
    {
        let ctr = AtomicI64::new(1_000_000_000_000_000);
        let mut conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async connect");
        let stmt = rt
            .block_on(conn.prepare(h::SQL_INSERT_ONE))
            .expect("bsql async prepare");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async"), |b| {
            b.to_async(&rt).iter(|| async {
                let id = ctr.fetch_add(1, Ordering::Relaxed);
                let n = conn
                    .borrow_mut()
                    .execute_prepared(&stmt, &(id, "ins", 7_i32))
                    .await
                    .expect("bsql async insert");
                black_box(n)
            });
        });
    }

    // tokio-postgres — id base 2e15
    {
        let ctr = AtomicI64::new(2_000_000_000_000_000);
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &h::pg_conn_string(),
                tokio_postgres::NoTls,
            ))
            .expect("tokio-postgres connect");
        let conn_task = rt.spawn(async move { connection.await });
        let stmt = rt.block_on(client.prepare(h::SQL_INSERT_ONE)).expect("tp prepare");
        g.bench_function(BenchmarkId::from_parameter("tokio_postgres"), |b| {
            b.to_async(&rt).iter(|| async {
                let id = ctr.fetch_add(1, Ordering::Relaxed);
                let n = client
                    .execute(&stmt, &[&id, &"ins", &7_i32])
                    .await
                    .expect("tp insert");
                black_box(n)
            });
        });
        drop(client);
        rt.block_on(async { conn_task.await.ok() });
    }

    // sqlx — id base 3e15
    {
        let ctr = AtomicI64::new(3_000_000_000_000_000);
        let conn = rt
            .block_on(sqlx::postgres::PgConnection::connect(&h::pg_url()))
            .expect("sqlx connect");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("sqlx"), |b| {
            b.to_async(&rt).iter(|| async {
                let id = ctr.fetch_add(1, Ordering::Relaxed);
                let n = sqlx::query(h::SQL_INSERT_ONE)
                    .bind(id)
                    .bind("ins")
                    .bind(7_i32)
                    .execute(&mut *conn.borrow_mut())
                    .await
                    .expect("sqlx insert")
                    .rows_affected();
                black_box(n)
            });
        });
    }

    g.finish();
}

// ---- complex query scenario ------------------------------------------------

/// JOIN + GROUP BY aggregation over the first 1000 items. Reads every column of
/// every grouped row.
fn bench_complex(c: &mut Criterion) {
    let rt = runtime();
    let sql = h::SQL_JOIN_AGG;
    let param = 1000_i32;
    let mut g = c.benchmark_group("complex/join_agg");

    // bsql sync
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
        let stmt = conn.prepare(sql).expect("bsql sync prepare");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync"), |b| {
            b.iter(|| {
                let qr = conn
                    .query_prepared(&stmt, &(black_box(param),))
                    .expect("bsql sync query");
                black_box(read_agg_bsql(&qr))
            });
        });
        let _ = conn.close_statement(stmt);
    }

    // bsql async
    {
        let mut conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async connect");
        let stmt = rt.block_on(conn.prepare(sql)).expect("bsql async prepare");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async"), |b| {
            b.to_async(&rt).iter(|| async {
                let qr = conn
                    .borrow_mut()
                    .query_prepared(&stmt, &(black_box(param),))
                    .await
                    .expect("bsql async query");
                black_box(read_agg_bsql(&qr))
            });
        });
    }

    // tokio-postgres
    {
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &h::pg_conn_string(),
                tokio_postgres::NoTls,
            ))
            .expect("tokio-postgres connect");
        let conn_task = rt.spawn(async move { connection.await });
        let stmt = rt.block_on(client.prepare(sql)).expect("tp prepare");
        g.bench_function(BenchmarkId::from_parameter("tokio_postgres"), |b| {
            b.to_async(&rt).iter(|| async {
                let rows = client
                    .query(&stmt, &[&black_box(param)])
                    .await
                    .expect("tp query");
                black_box(read_agg_tp(&rows))
            });
        });
        drop(client);
        rt.block_on(async { conn_task.await.ok() });
    }

    // sqlx
    {
        let conn = rt
            .block_on(sqlx::postgres::PgConnection::connect(&h::pg_url()))
            .expect("sqlx connect");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("sqlx"), |b| {
            b.to_async(&rt).iter(|| async {
                let rows = sqlx::query(sql)
                    .bind(black_box(param))
                    .fetch_all(&mut *conn.borrow_mut())
                    .await
                    .expect("sqlx query");
                black_box(read_agg_sqlx(&rows))
            });
        });
    }

    g.finish();
}

fn config() -> Criterion {
    // Enough samples for a tight CI while keeping the whole 24-function sweep to
    // a few minutes. Reported CIs let the reader judge the residual noise.
    Criterion::default()
        .warm_up_time(Duration::from_millis(1500))
        .measurement_time(Duration::from_secs(4))
        .sample_size(60)
}

criterion_group! {
    name = e2e;
    config = config();
    targets = bench_select, bench_insert, bench_complex
}
criterion_main!(e2e);
