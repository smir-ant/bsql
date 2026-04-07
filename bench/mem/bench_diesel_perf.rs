//! Raw performance benchmark for diesel — same methodology as C/Go/bsql.
//! N iterations, total time, mean per-op. No Criterion.

use std::time::Instant;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Bool, Double, Integer, Text};

const ITERATIONS: usize = 10000;
const ITERATIONS_SLOW: usize = 1000;
const ITERATIONS_JOIN: usize = 3000;
const ITERATIONS_SUB: usize = 5000;

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
struct User3 {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    email: String,
}

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
struct User5 {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    email: String,
    #[diesel(sql_type = Bool)]
    active: bool,
    #[diesel(sql_type = Double)]
    score: f64,
}

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
struct InsertRet {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
struct JoinRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = BigInt)]
    order_count: i64,
    #[diesel(sql_type = Double)]
    total_amount: f64,
}

fn main() {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let mut conn = PgConnection::establish(&url).unwrap();

    println!("=== diesel (Rust) PostgreSQL Benchmarks ===\n");

    // fetch_one
    {
        let _ = diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = $1")
            .bind::<Integer, _>(42i32)
            .load::<User3>(&mut conn)
            .unwrap();

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let _ = diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = $1")
                .bind::<Integer, _>(42i32)
                .load::<User3>(&mut conn)
                .unwrap();
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
        let _ = diesel::sql_query(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
        )
        .bind::<BigInt, _>(limit)
        .load::<User5>(&mut conn)
        .unwrap();

        let start = Instant::now();
        for _ in 0..iters {
            let _ = diesel::sql_query(
                "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
            )
            .bind::<BigInt, _>(limit)
            .load::<User5>(&mut conn)
            .unwrap();
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
            let _ = diesel::sql_query("INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0) RETURNING id")
                .bind::<Text, _>("bench_insert")
                .bind::<Text, _>("bench@example.com")
                .load::<InsertRet>(&mut conn)
                .unwrap();
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
            conn.transaction::<_, diesel::result::Error, _>(|tc| {
                for j in 0..100 {
                    let name = format!("batch_{j}");
                    let email = format!("batch{j}@test.com");
                    diesel::sql_query("INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)")
                        .bind::<Text, _>(&name)
                        .bind::<Text, _>(&email)
                        .execute(tc)
                        .unwrap();
                }
                Ok(())
            }).unwrap();
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
        let _ = diesel::sql_query(sql).load::<JoinRow>(&mut conn).unwrap();

        let start = Instant::now();
        for _ in 0..ITERATIONS_JOIN {
            let _ = diesel::sql_query(sql).load::<JoinRow>(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "pg_join_aggregate:  {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_JOIN as u128,
            ITERATIONS_JOIN
        );
    }

    // dynamic query (4 optional clauses via string concatenation)
    //
    // Note: diesel's sql_query does not support .filter() on BoxedSqlQuery.
    // The idiomatic diesel approach for raw SQL with dynamic clauses is
    // string concatenation — same as C's sprintf approach. This is NOT
    // compile-time validated (unlike bsql's optional clauses).
    {
        let active_filter: Option<bool> = Some(true);
        let min_score: Option<i16> = Some(5);
        let name_filter: Option<&str> = None;
        let email_filter: Option<&str> = None;

        // Warm up
        {
            let mut sql =
                String::from("SELECT id, name, email, active, score FROM bench_users WHERE 1=1");
            if active_filter.is_some() {
                sql.push_str(" AND active = true");
            }
            if min_score.is_some() {
                sql.push_str(" AND score > 5");
            }
            sql.push_str(" ORDER BY id LIMIT 100");
            let _ = diesel::sql_query(&sql).load::<User5>(&mut conn).unwrap();
        }

        let start = Instant::now();
        for i in 0..ITERATIONS {
            // Vary parameters each iteration to prevent PG query plan caching
            // on identical SQL text. This matches real-world usage where
            // filter values change per request.
            let mut sql =
                String::from("SELECT id, name, email, active, score FROM bench_users WHERE 1=1");
            if active_filter.is_some() {
                sql.push_str(if i % 2 == 0 {
                    " AND active = true"
                } else {
                    " AND active = false"
                });
            }
            if min_score.is_some() {
                sql.push_str(&format!(" AND score > {}", (i % 10) as i16));
            }
            if name_filter.is_some() {
                sql.push_str(" AND name LIKE '%bench%'");
            }
            if email_filter.is_some() {
                sql.push_str(" AND email LIKE '%bench%'");
            }
            sql.push_str(" ORDER BY id LIMIT 100");
            let _ = diesel::sql_query(&sql).load::<User5>(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "pg_dynamic_4clauses: {} ns/op  ({} iters)  [string concat, not validated]",
            elapsed.as_nanos() / ITERATIONS as u128,
            ITERATIONS
        );
    }

    // subquery
    {
        let sql = "SELECT id, name, email FROM bench_users \
                   WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)";
        let _ = diesel::sql_query(sql).load::<User3>(&mut conn).unwrap();

        let start = Instant::now();
        for _ in 0..ITERATIONS_SUB {
            let _ = diesel::sql_query(sql).load::<User3>(&mut conn).unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "pg_subquery:        {} ns/op  ({} iters)",
            elapsed.as_nanos() / ITERATIONS_SUB as u128,
            ITERATIONS_SUB
        );
    }
}
