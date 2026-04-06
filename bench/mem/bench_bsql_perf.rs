//! Raw performance benchmark — same methodology as C and Go.
//!
//! N iterations, total time, mean per-op.
//! No Criterion, no adaptive sampling — direct comparison with C/Go numbers.
//!
//! Run: BENCH_DATABASE_URL=... BSQL_DATABASE_URL=... cargo run --release --bin bench_bsql_perf

use std::time::Instant;

use bsql::{BsqlError, Pool};

const ITERATIONS: usize = 10000;
const ITERATIONS_JOIN: usize = 3000;
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

    // fetch_many — uses fetch (borrowed &str, like C's PQgetvalue returns char*)
    // Both C and bsql fetch() return pointers/references without heap allocation.
    for limit in [10i64, 100, 1000, 10000] {
        let iters = if limit >= 10000 {
            ITERATIONS_SLOW
        } else {
            ITERATIONS
        };
        // warm up
        let _ = bsql::query!(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
        )
        .fetch(&pool)?;

        let start = Instant::now();
        for _ in 0..iters {
            let rows = bsql::query!(
                "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
            )
            .fetch(&pool)?;
            // Read all columns to match C's PQgetvalue loop (prevent dead-code elimination)
            for row in rows.iter() {
                let r = row.unwrap();
                std::hint::black_box((&r.id, &r.name, &r.email, &r.active, &r.score));
            }
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
        for _ in 0..ITERATIONS_JOIN {
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
            elapsed.as_nanos() / ITERATIONS_JOIN as u128,
            ITERATIONS_JOIN
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

    // === fetch (zero-copy) vs fetch_all (owned) on 10K rows ===
    // Isolates the allocation overhead: fetch returns borrowed &str, fetch_all returns String.
    {
        let limit = 10000i64;
        // warm up
        let _ = bsql::query!(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
        )
        .fetch_all(&pool)?;

        let iters = 200;

        let start = Instant::now();
        for _ in 0..iters {
            let rows = bsql::query!(
                "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
            )
            .fetch_all(&pool)?;
            std::hint::black_box(&rows);
        }
        let fetch_all_elapsed = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            let rows = bsql::query!(
                "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $limit: i64"
            )
            .fetch(&pool)?;
            std::hint::black_box(&rows);
        }
        let fetch_elapsed = start.elapsed();

        println!("\n--- fetch_all vs fetch (10K rows, {} iters) ---", iters);
        println!(
            "fetch_all(): {} us/op",
            fetch_all_elapsed.as_micros() / iters as u128
        );
        println!(
            "fetch():     {} us/op",
            fetch_elapsed.as_micros() / iters as u128
        );
        let pct = if fetch_all_elapsed > fetch_elapsed {
            let saved = (fetch_all_elapsed - fetch_elapsed).as_nanos() as f64
                / fetch_all_elapsed.as_nanos() as f64
                * 100.0;
            format!("{:.1}% faster", saved)
        } else {
            "no improvement".to_string()
        };
        println!("fetch is {pct}");
    }

    // === Raw driver-level comparison: query vs for_each on 10K rows ===
    // This isolates the data copy overhead from Pool/codegen overhead.
    {
        use bsql_driver_postgres::{Config, Connection, Encode, hash_sql};

        let config = Config::from_url(&url).unwrap();
        let mut conn = Connection::connect(&config).unwrap();

        let sql = "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1";
        let hash = hash_sql(sql);
        let limit = 10000i64;
        let params: &[&(dyn Encode + Sync)] = &[&limit];

        // warm
        let _ = conn.query(sql, hash, params).unwrap();
        conn.for_each(sql, hash, params, |_| Ok(())).unwrap();

        // measure query (materializes Vec via resp_buf)
        let n = 200;
        let start = Instant::now();
        for _ in 0..n {
            let _ = conn.query(sql, hash, params).unwrap();
        }
        let elapsed = start.elapsed();
        println!("\n--- Raw driver 10K rows ---");
        println!(
            "query():    {} us/op  ({n} iters)",
            elapsed.as_micros() / n as u128
        );

        // measure for_each (zero-copy, processes in-place)
        let start = Instant::now();
        for _ in 0..n {
            conn.for_each(sql, hash, params, |_| Ok(())).unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "for_each(): {} us/op  ({n} iters)",
            elapsed.as_micros() / n as u128
        );
    }

    // === Raw driver single-row: isolate pool/wrapper overhead ===
    {
        use bsql_driver_postgres::{Config, Connection, Encode, hash_sql};

        let config = Config::from_url(&url)?;
        let mut conn = Connection::connect(&config)?;

        let sql_one = "SELECT id, name, email FROM bench_users WHERE id = $1";
        let hash_one = hash_sql(sql_one);
        let id = 42i32;
        let params_one: &[&(dyn Encode + Sync)] = &[&id];

        // warm
        let _ = conn.query(sql_one, hash_one, params_one);

        let n = 10000;
        let start = Instant::now();
        for _ in 0..n {
            let _ = conn.query(sql_one, hash_one, params_one);
        }
        let elapsed = start.elapsed();
        println!(
            "\n--- Raw driver single row ---\nquery():    {} ns/op  ({n} iters)",
            elapsed.as_nanos() / n as u128
        );
    }

    Ok(())
}
