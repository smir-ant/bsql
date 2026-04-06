//! Raw performance benchmark for sqlx — same methodology as C/Go/bsql/diesel.
//! N iterations, total time, mean per-op. No Criterion.

use std::time::Instant;

const ITERATIONS: usize = 10000;
const ITERATIONS_SLOW: usize = 1000;
const ITERATIONS_JOIN: usize = 3000;
const ITERATIONS_SUB: usize = 5000;

fn main() {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let pool = rt.block_on(async { sqlx::PgPool::connect(&url).await.unwrap() });

    println!("=== sqlx (Rust) PostgreSQL Benchmarks ===\n");

    // fetch_one
    {
        rt.block_on(async {
            let _: (i32, String, String) =
                sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = $1")
                    .bind(42i32)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
        });

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            rt.block_on(async {
                let _: (i32, String, String) =
                    sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = $1")
                        .bind(42i32)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
            });
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
        let iters = if limit >= 10000 {
            ITERATIONS_SLOW
        } else {
            ITERATIONS
        };
        rt.block_on(async {
            let _: Vec<(i32, String, String, bool, f64)> = sqlx::query_as(
                "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&pool)
            .await
            .unwrap();
        });

        let start = Instant::now();
        for _ in 0..iters {
            rt.block_on(async {
                let _: Vec<(i32, String, String, bool, f64)> = sqlx::query_as(
                    "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
                )
                .bind(limit)
                .fetch_all(&pool)
                .await
                .unwrap();
            });
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
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            rt.block_on(async {
                let _: (i32,) = sqlx::query_as(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0) RETURNING id",
                )
                .bind("bench_insert")
                .bind("bench@example.com")
                .fetch_one(&pool)
                .await
                .unwrap();
            });
        }
        let elapsed = start.elapsed();
        println!(
            "pg_insert_single:   {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS as u128,
            ITERATIONS
        );
    }

    // insert_batch
    {
        let start = Instant::now();
        for _ in 0..ITERATIONS_SLOW {
            rt.block_on(async {
                let mut tx = pool.begin().await.unwrap();
                for j in 0..100 {
                    let name = format!("batch_{j}");
                    let email = format!("batch{j}@test.com");
                    sqlx::query("INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)")
                        .bind(&name)
                        .bind(&email)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                }
                tx.commit().await.unwrap();
            });
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
        let sql = "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
                   FROM bench_users u JOIN bench_orders o ON u.id = o.user_id \
                   WHERE u.active = true GROUP BY u.name ORDER BY total_amount DESC LIMIT 10";
        rt.block_on(async {
            let _: Vec<(String, i64, f64)> = sqlx::query_as(sql).fetch_all(&pool).await.unwrap();
        });

        let start = Instant::now();
        for _ in 0..ITERATIONS_JOIN {
            rt.block_on(async {
                let _: Vec<(String, i64, f64)> =
                    sqlx::query_as(sql).fetch_all(&pool).await.unwrap();
            });
        }
        let elapsed = start.elapsed();
        println!(
            "pg_join_aggregate:  {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_JOIN as u128,
            ITERATIONS_JOIN
        );
    }

    // subquery
    {
        let sql = "SELECT id, name, email FROM bench_users \
                   WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)";
        rt.block_on(async {
            let _: Vec<(i32, String, String)> = sqlx::query_as(sql).fetch_all(&pool).await.unwrap();
        });

        let start = Instant::now();
        for _ in 0..ITERATIONS_SUB {
            rt.block_on(async {
                let _: Vec<(i32, String, String)> =
                    sqlx::query_as(sql).fetch_all(&pool).await.unwrap();
            });
        }
        let elapsed = start.elapsed();
        println!(
            "pg_subquery:        {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_SUB as u128,
            ITERATIONS_SUB
        );
    }
}
