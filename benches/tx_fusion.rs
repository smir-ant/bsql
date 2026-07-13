//! bsql round-trip delta on the TRANSACTION path: the deferred-`BEGIN` fusion
//! (D2) vs the explicit `BEGIN; <stmt>; COMMIT` three-flush sequence.
//!
//! This is a bsql-ONLY bench (no competitors): both bars in a group are the same
//! bsql driver running the SAME logical work — open a transaction, run ONE
//! statement, commit — against the same server and SQL. The only variable is HOW
//! the `BEGIN` is framed on the wire:
//!
//! - `fused` — `conn.transaction(|c| c.query_params(sql, &params))`:
//!   the closure API arms a DEFERRED `BEGIN` that fuses into the first statement's
//!   flush, so `BEGIN` + `Parse`+`Bind`+`Describe`+`Execute`+`Sync` ride ONE round
//!   trip, then `COMMIT` is a second round trip = TWO round trips for the whole
//!   transaction.
//! - `three_flush` — the manual `BEGIN; <stmt>; COMMIT` sequence a driver without
//!   the fusion must send: `conn.execute_sql("BEGIN")` (1 RTT) +
//!   `conn.query_params(sql, &params)` (1 RTT) + `conn.execute_sql("COMMIT")`
//!   (1 RTT) = THREE round trips. The per-statement work is byte-identical to the
//!   fused bar (the SAME `query_params`), so the delta is EXACTLY the separate
//!   `BEGIN` round trip the fusion removes.
//!
//! The `by_pk` fetch (one row, three columns, every column read) is the
//! statement, so per-statement cost is small and constant and the round-trip
//! count dominates the delta.
//!
//! Run from inside `bench/`:
//! ```text
//!   cargo bench --bench tx_fusion
//! ```
//! Needs a local PostgreSQL reachable over loopback TCP (`127.0.0.1:5432`),
//! seeded by `bench/setup/pg_setup.sql`.

use std::cell::RefCell;
use std::hint::black_box;
use std::time::Duration;

use bsql_bench as h;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

/// Read every column of a bsql `(id int4, name text, val int4)` result — the
/// read is what forces the driver to actually decode, not just frame.
fn read_select_bsql(qr: &bsql::pg::QueryResult) -> i64 {
    let mut acc = 0_i64;
    for row in qr.iter() {
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// One group; four bars: {sync, async} × {fused 2-RTT, three_flush 3-RTT}. Same
/// SQL, same param, same read — only the `BEGIN` framing differs.
fn bench_tx(c: &mut Criterion) {
    let rt = runtime();
    let sql = h::SQL_SELECT_BY_PK;
    let param: i32 = 5000;

    let mut g = c.benchmark_group("tx_fusion/one_stmt");

    // ── bsql sync — FUSED deferred BEGIN (2 round trips) ────────────────────
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync_fused"), |b| {
            b.iter(|| {
                let acc = conn
                    .transaction(|c| {
                        let qr = c.query_params(sql, &(black_box(param),))?;
                        Ok(read_select_bsql(&qr))
                    })
                    .expect("fused tx");
                black_box(acc)
            });
        });
    }

    // ── bsql sync — manual BEGIN; stmt; COMMIT (3 round trips) ───────────────
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync_three_flush"), |b| {
            b.iter(|| {
                conn.execute_sql("BEGIN").expect("begin");
                let qr = conn
                    .query_params(sql, &(black_box(param),))
                    .expect("query");
                let acc = read_select_bsql(&qr);
                conn.execute_sql("COMMIT").expect("commit");
                black_box(acc)
            });
        });
    }

    // ── bsql async — FUSED deferred BEGIN (2 round trips) ───────────────────
    {
        let conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async connect");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async_fused"), |b| {
            b.to_async(&rt).iter(|| async {
                let acc = conn
                    .borrow_mut()
                    .transaction(async |c| {
                        let qr = c.query_params(sql, &(black_box(param),)).await?;
                        Ok(read_select_bsql(&qr))
                    })
                    .await
                    .expect("fused tx");
                black_box(acc)
            });
        });
    }

    // ── bsql async — manual BEGIN; stmt; COMMIT (3 round trips) ──────────────
    {
        let conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async connect");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async_three_flush"), |b| {
            b.to_async(&rt).iter(|| async {
                let mut c = conn.borrow_mut();
                c.execute_sql("BEGIN").await.expect("begin");
                let qr = c
                    .query_params(sql, &(black_box(param),))
                    .await
                    .expect("query");
                let acc = read_select_bsql(&qr);
                c.execute_sql("COMMIT").await.expect("commit");
                black_box(acc)
            });
        });
    }

    g.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(1500))
        .measurement_time(Duration::from_secs(4))
        .sample_size(60)
}

criterion_group! {
    name = tx_fusion;
    config = config();
    targets = bench_tx
}
criterion_main!(tx_fusion);
