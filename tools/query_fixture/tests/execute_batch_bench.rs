//! Measured wall-clock: `execute_batch::<Q>(N sets)` vs N serial `execute::<Q>`.
//! Loopback PG; a rough single-run timing (not a criterion harness) to size the
//! round-trip win the batch buys. Run:
//!   cargo test -p bsql-query-fixture --test execute_batch_bench -- --ignored --nocapture
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    clippy::panic,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    reason = "bench harness — expect/unwrap/panic surface failures loudly"
)]

use std::time::Instant;

use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

bsql::query!(
    EbbUpd,
    "UPDATE eb_rows SET balance = balance + $2::int8 WHERE id = $1 RETURNING id"
);

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

#[tokio::test]
#[ignore = "requires local PG; timing bench"]
async fn batch_vs_serial() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    c.execute_raw("DROP TABLE IF EXISTS eb_rows").await.expect("drop");
    c.execute_raw("CREATE TABLE eb_rows (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .await
        .expect("create");

    println!("\n  N | execute_batch |  N×serial execute | speedup");
    println!("----+---------------+-------------------+--------");
    for &n in &[2_usize, 8, 32, 128, 512, 2000] {
        // Seed n rows.
        c.execute_raw("TRUNCATE eb_rows").await.expect("truncate");
        c.execute_batch::<EbbUpd, _>((0..n as i64).map(|_| (0_i64, 0_i64)))
            .await
            .ok(); // ignore (rows absent) — just to warm
        for i in 0..n as i64 {
            c.execute_raw(&format!("INSERT INTO eb_rows VALUES ({i}, 0)"))
                .await
                .expect("seed");
        }

        // Warm both paths once.
        let sets: Vec<(i64, i64)> = (0..n as i64).map(|i| (i, 1)).collect();
        c.execute_batch::<EbbUpd, _>(sets.clone()).await.expect("warm batch");

        // Time execute_batch (best of 5).
        let mut batch_ns = u128::MAX;
        for _ in 0..5 {
            let t = Instant::now();
            c.execute_batch::<EbbUpd, _>(sets.clone()).await.expect("batch");
            batch_ns = batch_ns.min(t.elapsed().as_nanos());
        }

        // Time N serial single-command batches (each exactly 1 round trip — the
        // apples-to-apples "N sequential typed writes" the batch replaces).
        let mut serial_ns = u128::MAX;
        for _ in 0..5 {
            let t = Instant::now();
            for &(id, inc) in &sets {
                c.execute_batch::<EbbUpd, _>(vec![(id, inc)]).await.expect("serial");
            }
            serial_ns = serial_ns.min(t.elapsed().as_nanos());
        }

        let speedup = serial_ns as f64 / batch_ns as f64;
        println!(
            "{n:4} | {:>10.1}µs | {:>14.1}µs | {speedup:>5.1}×",
            batch_ns as f64 / 1000.0,
            serial_ns as f64 / 1000.0,
        );
    }
    println!();
    c.close().await.expect("close");
}
