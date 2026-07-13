//! bsql round-trip delta on the DYNAMIC (runtime-untyped) one-shot query path:
//! the fused ONE-round-trip `query_params` vs the explicit THREE-round-trip
//! prepare / bind+execute / close sequence it replaced.
//!
//! This is a bsql-ONLY bench (no competitors): both bars in a group are the same
//! bsql driver running byte-identical work against the same server and SQL; the
//! only variable is HOW the extended-protocol exchange is framed —
//!
//! - `fused` — `conn.query_params(sql, &params)`:
//!   `Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` + `Sync` in ONE
//!   flush = ONE round trip. The result schema (OIDs + names) arrives INLINE from
//!   the `Describe`(portal), so the runtime consumer recovers it with no separate
//!   `prepare` round trip.
//! - `three_rtt` — the explicit sequence `query_params` USED to compose:
//!   `conn.prepare(sql)` (`Parse`+`Describe`+`Sync` = 1 RTT), then
//!   `conn.query_prepared(&stmt, &params)` (`Bind`+`Execute`+`Sync` = 1 RTT),
//!   then `conn.close_statement(stmt)` (`Close`+`Sync` = 1 RTT) = THREE round
//!   trips. This is the pre-fusion baseline, measured side by side so the delta is
//!   apples-to-apples on the SAME binary — no old-code rebuild needed.
//!
//! The `by_pk` fetch (one row, three columns, every column read) isolates the
//! per-round-trip cost — a one-shot dynamic query is exactly where the round-trip
//! count dominates, since NOTHING is amortized (a real one-shot query prepares,
//! binds, and discards each time).
//!
//! Run from inside `bench/`:
//! ```text
//!   cargo bench --bench dynamic_oneshot
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

/// One group; four bars: {sync, async} × {fused 1-RTT, three_rtt}. Same SQL, same
/// param, same read — only the round-trip framing differs.
fn bench_oneshot(c: &mut Criterion) {
    let rt = runtime();
    let sql = h::SQL_SELECT_BY_PK;
    let param: i32 = 5000;

    let mut g = c.benchmark_group("dynamic_oneshot/by_pk");

    // ── bsql sync — FUSED one round trip ────────────────────────────────────
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync_fused"), |b| {
            b.iter(|| {
                let qr = conn
                    .query_params(sql, &(black_box(param),))
                    .expect("fused query_params");
                black_box(read_select_bsql(&qr))
            });
        });
    }

    // ── bsql sync — explicit THREE round trips (pre-fusion baseline) ─────────
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync connect");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync_three_rtt"), |b| {
            b.iter(|| {
                let stmt = conn.prepare(sql).expect("prepare");
                let qr = conn
                    .query_prepared(&stmt, &(black_box(param),))
                    .expect("query_prepared");
                let acc = read_select_bsql(&qr);
                conn.close_statement(stmt).expect("close");
                black_box(acc)
            });
        });
    }

    // ── bsql async — FUSED one round trip ───────────────────────────────────
    {
        let conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async connect");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async_fused"), |b| {
            b.to_async(&rt).iter(|| async {
                let qr = conn
                    .borrow_mut()
                    .query_params(sql, &(black_box(param),))
                    .await
                    .expect("fused query_params");
                black_box(read_select_bsql(&qr))
            });
        });
    }

    // ── bsql async — explicit THREE round trips (pre-fusion baseline) ────────
    {
        let conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async connect");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async_three_rtt"), |b| {
            b.to_async(&rt).iter(|| async {
                let mut c = conn.borrow_mut();
                let stmt = c.prepare(sql).await.expect("prepare");
                let qr = c
                    .query_prepared(&stmt, &(black_box(param),))
                    .await
                    .expect("query_prepared");
                let acc = read_select_bsql(&qr);
                c.close_statement(stmt).await.expect("close");
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
    name = dynamic_oneshot;
    config = config();
    targets = bench_oneshot
}
criterion_main!(dynamic_oneshot);
