//! Benchmark: multi-row SELECT with varying row counts (SQLite).
//!
//! Measures throughput scaling across 10, 100, 1000, and 10000 rows.
//!
//! Requires:
//!   BENCH_SQLITE_PATH     — path to the SQLite database file (runtime)
//!   BSQL_DATABASE_URL     — sqlite://<same path> (compile-time, for bsql::query!)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_sqlite_path() -> String {
    std::env::var("BENCH_SQLITE_PATH").expect("BENCH_SQLITE_PATH must be set")
}

fn bench_sqlite_fetch_many(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let path = bench_sqlite_path();

    // -- bsql SQLite pool --
    let bsql_pool = bsql::SqlitePool::connect(&path).unwrap();

    // -- sqlx SQLite pool --
    let sqlx_pool = rt.block_on(async {
        sqlx::SqlitePool::connect(&format!("sqlite:{path}"))
            .await
            .unwrap()
    });

    // -- diesel SQLite connection --
    use diesel::prelude::*;
    let mut diesel_conn = SqliteConnection::establish(&path).unwrap();

    // Warm up
    {
        let n = 10i64;
        let _rows = bsql::query!(
            "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $n: i64"
        )
        .fetch_all(&bsql_pool)
        .unwrap();
    }

    let row_counts: &[i64] = &[10, 100, 1_000, 10_000];

    let mut group = c.benchmark_group("sqlite_fetch_many");

    for &n in row_counts {
        // -- bsql (sync) --
        group.bench_with_input(BenchmarkId::new("bsql", n), &n, |b, &n| {
            b.iter(|| {
                let _rows = bsql::query!(
                    "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $n: i64"
                )
                .fetch_all(&bsql_pool)
                .unwrap();
            });
        });

        // -- sqlx --
        group.bench_with_input(BenchmarkId::new("sqlx", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async {
                let _rows: Vec<(i64, String, String, bool, f64)> = sqlx::query_as(
                    "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1",
                )
                .bind(n)
                .fetch_all(&sqlx_pool)
                .await
                .unwrap();
            });
        });

        // -- diesel (sync) --
        {
            use diesel::sql_types::{BigInt, Bool, Double, Text};

            #[derive(diesel::QueryableByName, Debug)]
            #[allow(dead_code)]
            struct User {
                #[diesel(sql_type = BigInt)]
                id: i64,
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
                        "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1",
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

criterion_group!(benches, bench_sqlite_fetch_many);
criterion_main!(benches);
