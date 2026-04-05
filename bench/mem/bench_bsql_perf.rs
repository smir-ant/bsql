//! Raw performance benchmark — same methodology as C and Go.
//!
//! N iterations, total time, mean per-op.
//! No Criterion, no adaptive sampling — direct comparison with C/Go numbers.
//!
//! Run: BENCH_DATABASE_URL=... BSQL_DATABASE_URL=... cargo run --release --bin bench_bsql_perf

use std::time::Instant;

use bsql::{Pool, BsqlError};

const ITERATIONS: usize = 10000;
const ITERATIONS_SLOW: usize = 1000;
const ITERATIONS_SUB: usize = 5000;

fn main() -> Result<(), BsqlError> {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let pool = Pool::connect(&url)?;

    println!("=== bsql (Rust) PostgreSQL Benchmarks ===\n");

    // fetch_one
    {
        let id = 42i32;
        // warm up
        let _ = bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i32")
            .fetch_one(&pool)?;

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let _ = bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i32")
                .fetch_one(&pool)?;
        }
        let elapsed = start.elapsed();
        println!(
            "pg_fetch_one:       {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS as u128,
            ITERATIONS
        );
    }

    // fetch_many
    for limit in [10i64, 100, 1000, 10000] {
        let iters = if limit >= 10000 { ITERATIONS_SLOW } else { ITERATIONS };
        // warm up
        let _ = bsql::query!(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
        )
        .fetch(&pool)?;

        let start = Instant::now();
        for _ in 0..iters {
            let _ = bsql::query!(
                "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
            )
            .fetch(&pool)?;
        }
        let elapsed = start.elapsed();
        println!(
            "pg_fetch_many/{:<5} {} ns/op  ({} iters)",
            limit,
            elapsed.as_nanos() / iters as u128,
            iters
        );
    }

    // insert_single
    {
        let name = "bench_insert";
        let email = "bench@example.com";
        // warm up
        let _ = bsql::query!(
            "INSERT INTO bench_users (name, email, active, score) VALUES ($name: &str, $email: &str, true, 0.0) RETURNING id"
        )
        .fetch_one(&pool)?;

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let _ = bsql::query!(
                "INSERT INTO bench_users (name, email, active, score) VALUES ($name: &str, $email: &str, true, 0.0) RETURNING id"
            )
            .fetch_one(&pool)?;
        }
        let elapsed = start.elapsed();
        println!(
            "pg_insert_single:   {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS as u128,
            ITERATIONS
        );
    }

    // insert_batch (100 in transaction)
    {
        let start = Instant::now();
        for _ in 0..ITERATIONS_SLOW {
            let tx = pool.begin()?;
            for j in 0..100 {
                let name = format!("batch_{j}");
                let email = format!("batch{j}@test.com");
                bsql::query!(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($name: String, $email: String, true, 0.0)"
                )
                .defer(&tx)?;
            }
            tx.commit()?;
        }
        let elapsed = start.elapsed();
        println!(
            "pg_insert_batch/100: {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_SLOW as u128,
            ITERATIONS_SLOW
        );
    }

    // join_aggregate
    {
        // warm up
        let _ = bsql::query!(
            "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
             FROM bench_users u JOIN bench_orders o ON u.id = o.user_id \
             WHERE u.active = true GROUP BY u.name ORDER BY total_amount DESC LIMIT 10"
        )
        .fetch(&pool)?;

        let start = Instant::now();
        for _ in 0..ITERATIONS_SLOW {
            let _ = bsql::query!(
                "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
                 FROM bench_users u JOIN bench_orders o ON u.id = o.user_id \
                 WHERE u.active = true GROUP BY u.name ORDER BY total_amount DESC LIMIT 10"
            )
            .fetch(&pool)?;
        }
        let elapsed = start.elapsed();
        println!(
            "pg_join_aggregate:  {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_SLOW as u128,
            ITERATIONS_SLOW
        );
    }

    // subquery
    {
        // warm up
        bsql::query!(
            "SELECT id, name, email FROM bench_users \
             WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)"
        )
        .for_each(&pool, |_row| Ok(()))?;

        let start = Instant::now();
        for _ in 0..ITERATIONS_SUB {
            bsql::query!(
                "SELECT id, name, email FROM bench_users \
                 WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)"
            )
            .for_each(&pool, |_row| Ok(()))?;
        }
        let elapsed = start.elapsed();
        println!(
            "pg_subquery:        {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_SUB as u128,
            ITERATIONS_SUB
        );
    }

    Ok(())
}
