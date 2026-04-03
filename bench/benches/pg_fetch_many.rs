//! Benchmark: multi-row SELECT with varying row counts (PostgreSQL).
//!
//! Measures throughput scaling across 10, 100, 1000, and 10000 rows.
//!
//! Requires:
//!   BENCH_DATABASE_URL  — PostgreSQL connection string (runtime)
//!   BSQL_DATABASE_URL   — same URL (compile-time, for bsql::query!)

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn bench_database_url() -> String {
    std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL must be set")
}

fn bench_pg_fetch_many(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let url = bench_database_url();

    // -- bsql pool --
    let bsql_pool = rt.block_on(async { bsql::Pool::connect(&url).await.unwrap() });

    // -- bsql direct connection (no pool, same as C/Go) --
    let sql_direct = "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1";
    let sql_hash = bsql_driver_postgres::hash_sql(sql_direct);
    let mut bsql_conn = rt.block_on(async {
        let config = bsql_driver_postgres::Config::from_url(&url).unwrap();
        let mut conn = bsql_driver_postgres::Connection::connect(&config)
            .await
            .unwrap();
        // Pre-prepare statement so benchmark iterations only send Bind+Execute+Sync
        conn.prepare_only(sql_direct, sql_hash).await.unwrap();
        conn
    });

    // -- bsql sync connection (UDS only, zero async overhead) --
    #[cfg(unix)]
    let mut bsql_sync = {
        let config = bsql_driver_postgres::Config::from_url(&url).unwrap();
        let mut sc = bsql_driver_postgres::SyncConnection::connect(&config).unwrap();
        sc.prepare_only(sql_direct, sql_hash).unwrap();
        sc
    };

    // -- sqlx pool --
    let sqlx_pool = rt.block_on(async { sqlx::PgPool::connect(&url).await.unwrap() });

    // -- diesel connection --
    use diesel::prelude::*;
    let mut diesel_conn = PgConnection::establish(&url).unwrap();

    // Warm up: run a small query on each backend
    rt.block_on(async {
        let n = 10i64;
        let _rows = bsql::query!(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $n: i64"
        )
        .fetch_all(&bsql_pool)
        .await
        .unwrap();
    });
    rt.block_on(async {
        let n_param: i64 = 10;
        let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&n_param];
        bsql_conn
            .for_each_raw(sql_direct, sql_hash, params, |_data| Ok(()))
            .await
            .unwrap();
    });
    rt.block_on(async {
        let _rows: Vec<(i32, String, String, bool, f64)> = sqlx::query_as(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
        )
        .bind(10i64)
        .fetch_all(&sqlx_pool)
        .await
        .unwrap();
    });

    let row_counts: &[i64] = &[10, 100, 1_000, 10_000];

    let mut group = c.benchmark_group("pg_fetch_many");

    for &n in row_counts {
        // -- bsql (for_each via pool — measures pool + mutex + query) --
        group.bench_with_input(BenchmarkId::new("bsql", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async {
                let n = n;
                bsql::query!(
                    "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $n: i64"
                )
                .for_each(&bsql_pool, |_row| Ok(()))
                .await
                .unwrap();
            });
        });

        // -- bsql_direct (no pool, same as C/Go — raw connection) --
        group.bench_with_input(BenchmarkId::new("bsql_direct", n), &n, |b, &n| {
            let n_param: i64 = n;
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&n_param];
                        bsql_conn
                            .for_each_raw(sql_direct, sql_hash, params, |_data| Ok(()))
                            .await
                            .unwrap();
                    }
                    start.elapsed()
                })
            });
        });

        // -- bsql_sync (UDS, zero async overhead) --
        #[cfg(unix)]
        group.bench_with_input(BenchmarkId::new("bsql_sync", n), &n, |b, &n| {
            let n_param: i64 = n;
            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&n_param];
                    bsql_sync
                        .for_each_raw(sql_direct, sql_hash, params, |_data| Ok(()))
                        .unwrap();
                }
                start.elapsed()
            });
        });

        // -- sqlx --
        group.bench_with_input(BenchmarkId::new("sqlx", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async {
                let _rows: Vec<(i32, String, String, bool, f64)> = sqlx::query_as(
                    "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
                )
                .bind(n)
                .fetch_all(&sqlx_pool)
                .await
                .unwrap();
            });
        });

        // -- diesel (sync) --
        {
            use diesel::sql_types::{BigInt, Bool, Double, Integer, Text};

            #[derive(diesel::QueryableByName, Debug)]
            #[allow(dead_code)]
            struct User {
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

            group.bench_with_input(BenchmarkId::new("diesel", n), &n, |b, &n| {
                b.iter(|| {
                    let _rows = diesel::sql_query(
                        "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
                    )
                    .bind::<BigInt, _>(n)
                    .load::<User>(&mut diesel_conn)
                    .unwrap();
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_pg_fetch_many);
criterion_main!(benches);
