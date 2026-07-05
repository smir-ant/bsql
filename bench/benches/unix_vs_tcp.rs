//! bsql transport delta: the SAME query, the SAME server, over loopback TCP vs
//! the LOCAL UNIX-DOMAIN socket — the transport the original bsql used for local
//! connections and the one the bench baseline assumes.
//!
//! This is a bsql-ONLY bench (no competitors): both bars in a group are the same
//! bsql driver running byte-identical work; the only variable is the transport.
//! It isolates the win the owner wants — a unix socket skips the loopback
//! TCP/IP stack (no Nagle, no delayed-ACK, no per-packet checksum/framing), so a
//! single round-trip should be measurably cheaper than loopback TCP.
//!
//! # What each iteration does
//!
//! The single-round-trip `by_pk` fetch (one row, three columns, every column
//! read) — the scenario where per-round-trip transport overhead dominates and a
//! multi-row decode does not drown it. The statement is prepared ONCE before the
//! timed loop (the cache-HIT steady state), then bound + sent + fully read per
//! iteration, exactly like the cross-client `e2e` bench.
//!
//! Run from inside `bench/`:
//! ```text
//!   cargo bench --bench unix_vs_tcp
//! ```
//! Needs a local PostgreSQL reachable over BOTH TCP (`127.0.0.1:5432`) and its
//! unix socket (`/tmp/.s.PGSQL.5432`, or set `BSQL_BENCH_SOCKET_DIR`), seeded by
//! `bench/setup/pg_setup.sql`.

use std::cell::RefCell;
use std::hint::black_box;
use std::time::Duration;

use bsql_bench as h;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

/// Read every column of a bsql `(id int4, name text, val int4)` result — the
/// read is what forces the driver to actually decode, not just frame.
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// One group; four bars: {sync, async} × {tcp, unix}. Same SQL, same param, same
/// read — only the transport differs.
fn bench_transport(c: &mut Criterion) {
    let rt = runtime();
    let sql = h::SQL_SELECT_BY_PK;
    let param: i32 = 5000;

    let mut g = c.benchmark_group("transport/by_pk");

    // ── bsql sync over loopback TCP ─────────────────────────────────────────
    {
        let mut conn =
            bsql::pg_sync::Connection::connect(&h::bsql_config()).expect("bsql sync TCP connect");
        let stmt = conn.prepare(sql).expect("prepare");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync_tcp"), |b| {
            b.iter(|| {
                let qr = conn
                    .query_prepared(&stmt, &(black_box(param),))
                    .expect("query");
                black_box(read_select_bsql(&qr))
            });
        });
        let _ = conn.close_statement(stmt);
    }

    // ── bsql sync over the LOCAL UNIX SOCKET ────────────────────────────────
    {
        let mut conn = bsql::pg_sync::Connection::connect(&h::bsql_config_unix())
            .expect("bsql sync unix connect");
        assert!(!conn.is_encrypted(), "unix socket is plaintext");
        let stmt = conn.prepare(sql).expect("prepare");
        g.bench_function(BenchmarkId::from_parameter("bsql_sync_unix"), |b| {
            b.iter(|| {
                let qr = conn
                    .query_prepared(&stmt, &(black_box(param),))
                    .expect("query");
                black_box(read_select_bsql(&qr))
            });
        });
        let _ = conn.close_statement(stmt);
    }

    // ── bsql async over loopback TCP ────────────────────────────────────────
    {
        let mut conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config()))
            .expect("bsql async TCP connect");
        let stmt = rt.block_on(conn.prepare(sql)).expect("prepare");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async_tcp"), |b| {
            b.to_async(&rt).iter(|| async {
                let qr = conn
                    .borrow_mut()
                    .query_prepared(&stmt, &(black_box(param),))
                    .await
                    .expect("query");
                black_box(read_select_bsql(&qr))
            });
        });
    }

    // ── bsql async over the LOCAL UNIX SOCKET ───────────────────────────────
    {
        let mut conn = rt
            .block_on(bsql::pg::Connection::connect(&h::bsql_config_unix()))
            .expect("bsql async unix connect");
        assert!(!conn.is_encrypted(), "unix socket is plaintext");
        let stmt = rt.block_on(conn.prepare(sql)).expect("prepare");
        let conn = RefCell::new(conn);
        g.bench_function(BenchmarkId::from_parameter("bsql_async_unix"), |b| {
            b.to_async(&rt).iter(|| async {
                let qr = conn
                    .borrow_mut()
                    .query_prepared(&stmt, &(black_box(param),))
                    .await
                    .expect("query");
                black_box(read_select_bsql(&qr))
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
    name = unix_vs_tcp;
    config = config();
    targets = bench_transport
}
criterion_main!(unix_vs_tcp);
