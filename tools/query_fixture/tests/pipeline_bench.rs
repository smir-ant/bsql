//! Wall-clock measurement: a K-command `pipeline` (ONE round trip) vs K sequential
//! `query_one` calls (K round trips), over the async driver on loopback PG.
//!
//! Prints a table of median wall-clock per operation for K = 2 / 4 / 8 / 16 both
//! ways, plus the ratio (expected ~K× on the round-trip term for cheap SELECTs).
//! The batch is a repeated cheap `SELECT 1::int4` so the ROUND TRIP dominates —
//! the heterogeneous capability is proven separately by `pipeline_live_async.rs`.
//!
//! Run: `cargo test -p bsql-query-fixture --test pipeline_bench -- --ignored --nocapture`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::as_conversions,
    reason = "measurement harness — expect/unwrap surface failures loudly; timing arithmetic on small counts"
)]

use std::time::{Duration, Instant};

use bsql::BindExt;
use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

bsql::query!(B, "SELECT 1::int4 AS n");

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Median of a sorted-in-place vector of durations.
fn median(mut v: Vec<Duration>) -> Duration {
    v.sort_unstable();
    v[v.len().midpoint(0)]
}

/// Time `iters` runs of `f`, returning the median per-run wall-clock.
async fn time_it<F: AsyncFnMut()>(iters: usize, mut f: F) -> Duration {
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f().await;
        samples.push(t.elapsed());
    }
    median(samples)
}

#[tokio::test]
#[ignore = "requires local PG — measurement, run with --nocapture"]
async fn pipeline_vs_sequential_wall_clock() {
    let iters = 300usize;
    let mut c = Connection::connect(&cfg()).await.expect("connect");

    // Warm the per-connection statement cache so both sides measure the steady
    // state (one Parse, then plan reuse) — the fair comparison.
    for _ in 0..3 {
        c.query_one::<BQuery>(()).await.expect("warm");
        drop(c.pipeline((BQuery::bind(()), BQuery::bind(()))).await.expect("warm pipe"));
    }

    println!("\n  K | sequential (K× query_one) | pipeline (1 round trip) | speedup");
    println!("----+---------------------------+-------------------------+--------");

    // K = 2
    let seq = time_it(iters, async || {
        for _ in 0..2 {
            c.query_one::<BQuery>(()).await.expect("seq");
        }
    })
    .await;
    let pipe = time_it(iters, async || {
        drop(c.pipeline((BQuery::bind(()), BQuery::bind(()))).await.expect("pipe"));
    })
    .await;
    report(2, seq, pipe);

    // K = 4
    let seq = time_it(iters, async || {
        for _ in 0..4 {
            c.query_one::<BQuery>(()).await.expect("seq");
        }
    })
    .await;
    let pipe = time_it(iters, async || {
        drop(
            c.pipeline((
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
            ))
            .await
            .expect("pipe"),
        );
    })
    .await;
    report(4, seq, pipe);

    // K = 8
    let seq = time_it(iters, async || {
        for _ in 0..8 {
            c.query_one::<BQuery>(()).await.expect("seq");
        }
    })
    .await;
    let pipe = time_it(iters, async || {
        drop(
            c.pipeline((
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
            ))
            .await
            .expect("pipe"),
        );
    })
    .await;
    report(8, seq, pipe);

    // K = 16
    let seq = time_it(iters, async || {
        for _ in 0..16 {
            c.query_one::<BQuery>(()).await.expect("seq");
        }
    })
    .await;
    let pipe = time_it(iters, async || {
        drop(
            c.pipeline((
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
                BQuery::bind(()), BQuery::bind(()), BQuery::bind(()), BQuery::bind(()),
            ))
            .await
            .expect("pipe"),
        );
    })
    .await;
    report(16, seq, pipe);

    println!();
    c.close().await.expect("close");
}

fn report(k: usize, seq: Duration, pipe: Duration) {
    let ratio = seq.as_secs_f64() / pipe.as_secs_f64();
    println!(
        " {k:2} | {:>21.1?} | {:>19.1?} | {ratio:>5.2}×",
        seq, pipe
    );
}
