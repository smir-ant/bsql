//! Benchmark: INSERT operations (PostgreSQL).
//!
//! Tests single INSERT RETURNING and batch INSERT (100 rows in a transaction).
//!
//! Requires:
//!   BENCH_DATABASE_URL  — PostgreSQL connection string (runtime)
//!   BSQL_DATABASE_URL   — same URL (compile-time, for bsql::query!)

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_database_url() -> String {
    std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL must be set")
}

fn bench_pg_insert_single(c: &mut Criterion) {
    let url = bench_database_url();

    // sqlx is still async — it needs a runtime for its pool
    let rt = tokio::runtime::Runtime::new().unwrap();

    let bsql_pool = bsql::Pool::connect(&url).unwrap();
    let sqlx_pool = rt.block_on(async { sqlx::PgPool::connect(&url).await.unwrap() });

    use diesel::prelude::*;
    let mut diesel_conn = PgConnection::establish(&url).unwrap();

    // Clean up INSERT accumulation and force WAL checkpoint.
    // Without this, previous bench runs leave rows that slow down autovacuum
    // (even if disabled) and bloat table pages, degrading INSERT throughput.
    bsql_pool
        .raw_execute("DELETE FROM bench_users WHERE name = 'bench_insert'; CHECKPOINT")
        .ok();

    let mut group = c.benchmark_group("pg_insert_single");

    // -- bsql: single INSERT RETURNING (sync) --
    group.bench_function("bsql", |b| {
        b.iter(|| {
            let name = "bench_insert";
            let email = "bench@example.com";
            let _row = bsql::query!(
                "INSERT INTO bench_users (name, email, active, score) VALUES ($name: &str, $email: &str, true, 0.0) RETURNING id"
            )
            .fetch_one(&bsql_pool)
            .unwrap();
        });
    });

    // -- sqlx: single INSERT RETURNING (async — needs runtime) --
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _row: (i32,) = sqlx::query_as(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0) RETURNING id",
                )
                .bind("bench_insert")
                .bind("bench@example.com")
                .fetch_one(&sqlx_pool)
                .await
                .unwrap();
            });
        });
    });

    // -- diesel: single INSERT RETURNING --
    {
        use diesel::sql_types::{Integer, Text};

        #[derive(diesel::QueryableByName, Debug)]
        #[allow(dead_code)]
        struct Returning {
            #[diesel(sql_type = Integer)]
            id: i32,
        }

        group.bench_function("diesel", |b| {
            b.iter(|| {
                let _rows = diesel::sql_query(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0) RETURNING id",
                )
                .bind::<Text, _>("bench_insert")
                .bind::<Text, _>("bench@example.com")
                .load::<Returning>(&mut diesel_conn)
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_pg_insert_batch(c: &mut Criterion) {
    let url = bench_database_url();

    // sqlx is still async — it needs a runtime for its pool
    let rt = tokio::runtime::Runtime::new().unwrap();

    let bsql_pool = bsql::Pool::connect(&url).unwrap();
    let sqlx_pool = rt.block_on(async { sqlx::PgPool::connect(&url).await.unwrap() });

    use diesel::prelude::*;
    let mut diesel_conn = PgConnection::establish(&url).unwrap();

    let mut group = c.benchmark_group("pg_insert_batch_100");

    // -- bsql: 100 INSERTs in a transaction (sync) --
    group.bench_function("bsql", |b| {
        b.iter(|| {
            let tx = bsql_pool.begin().unwrap();
            for i in 0..100i32 {
                let name = format!("batch_{i}");
                let email = format!("batch_{i}@example.com");
                bsql::query!(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($name: String, $email: String, true, 0.0)"
                )
                .execute(&tx)
                .unwrap();
            }
            tx.commit().unwrap();
        });
    });

    // -- sqlx: 100 INSERTs in a transaction (async — needs runtime) --
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut tx = sqlx_pool.begin().await.unwrap();
                for i in 0..100i32 {
                    let name = format!("batch_{i}");
                    let email = format!("batch_{i}@example.com");
                    sqlx::query(
                        "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)",
                    )
                    .bind(&name)
                    .bind(&email)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
                tx.commit().await.unwrap();
            });
        });
    });

    // -- diesel: 100 INSERTs in a transaction --
    {
        use diesel::sql_types::Text;

        group.bench_function("diesel", |b| {
            b.iter(|| {
                diesel_conn
                    .transaction::<_, diesel::result::Error, _>(|conn| {
                        for i in 0..100i32 {
                            let name = format!("batch_{i}");
                            let email = format!("batch_{i}@example.com");
                            diesel::sql_query(
                                "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)",
                            )
                            .bind::<Text, _>(&name)
                            .bind::<Text, _>(&email)
                            .execute(conn)?;
                        }
                        Ok(())
                    })
                    .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_pg_insert_batch_pipeline(c: &mut Criterion) {
    let url = bench_database_url();

    let pool = bsql_driver_postgres::Pool::connect(&url).unwrap();

    let mut group = c.benchmark_group("pg_insert_batch_100_pipeline");

    let sql = "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)";
    let sql_hash = bsql_driver_postgres::hash_sql(sql);

    // -- bsql pipelined: 100 INSERTs in one round-trip (sync) --
    group.bench_function("bsql_pipeline", |b| {
        b.iter(|| {
            let mut tx = pool.begin().unwrap();

            // Pre-build parameter sets
            let names: Vec<String> = (0..100).map(|i| format!("batch_{i}")).collect();
            let emails: Vec<String> = (0..100).map(|i| format!("batch_{i}@example.com")).collect();

            let param_sets: Vec<[&(dyn bsql_driver_postgres::Encode + Sync); 2]> = names
                .iter()
                .zip(emails.iter())
                .map(|(n, e)| [n as &(dyn bsql_driver_postgres::Encode + Sync), e as _])
                .collect();

            let param_refs: Vec<&[&(dyn bsql_driver_postgres::Encode + Sync)]> =
                param_sets.iter().map(|p| p.as_slice()).collect();

            tx.execute_pipeline(sql, sql_hash, &param_refs).unwrap();

            tx.commit().unwrap();
        });
    });

    group.finish();
}

fn bench_pg_insert_batch_deferred(c: &mut Criterion) {
    let url = bench_database_url();

    let pool = bsql_driver_postgres::Pool::connect(&url).unwrap();

    let mut group = c.benchmark_group("pg_insert_batch_100_deferred");

    let sql = "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)";
    let sql_hash = bsql_driver_postgres::hash_sql(sql);

    // -- bsql deferred pipeline: 100 defer_execute + commit auto-flush (sync) --
    group.bench_function("bsql_deferred", |b| {
        b.iter(|| {
            let mut tx = pool.begin().unwrap();

            for i in 0..100i32 {
                let name = format!("batch_{i}");
                let email = format!("batch_{i}@example.com");
                tx.defer_execute(
                    sql,
                    sql_hash,
                    &[
                        &name as &(dyn bsql_driver_postgres::Encode + Sync),
                        &email as _,
                    ],
                )
                .unwrap();
            }

            // commit() auto-flushes all 100 as one pipeline + COMMIT
            tx.commit().unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pg_insert_single,
    bench_pg_insert_batch,
    bench_pg_insert_batch_pipeline,
    bench_pg_insert_batch_deferred
);
criterion_main!(benches);
