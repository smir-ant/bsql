//! Benchmark: single-row SELECT by primary key (PostgreSQL).
//!
//! Compares bsql, sqlx, and diesel fetching a single row by PK.
//!
//! Requires:
//!   BENCH_DATABASE_URL  — PostgreSQL connection string (runtime)
//!   BSQL_DATABASE_URL   — same URL (compile-time, for bsql::query!)

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_database_url() -> String {
    std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL must be set")
}

fn bench_pg_fetch_one(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let url = bench_database_url();

    // -- bsql pool --
    let bsql_pool = rt.block_on(async { bsql::Pool::connect(&url).await.unwrap() });

    // -- sqlx pool --
    let sqlx_pool = rt.block_on(async { sqlx::PgPool::connect(&url).await.unwrap() });

    // -- diesel connection --
    use diesel::prelude::*;
    let mut diesel_conn = PgConnection::establish(&url).unwrap();

    // Warm up: run the query once on each backend
    rt.block_on(async {
        let _row = bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i32")
            .fetch_one(&bsql_pool)
            .await
            .unwrap();
    });
    rt.block_on(async {
        let _row: (i32, String, String) =
            sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = $1")
                .bind(42i32)
                .fetch_one(&sqlx_pool)
                .await
                .unwrap();
    });
    {
        use diesel::sql_types::{Integer, Text};

        #[derive(diesel::QueryableByName, Debug)]
        #[allow(dead_code)]
        struct User {
            #[diesel(sql_type = Integer)]
            id: i32,
            #[diesel(sql_type = Text)]
            name: String,
            #[diesel(sql_type = Text)]
            email: String,
        }

        let _rows = diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = $1")
            .bind::<Integer, _>(42i32)
            .load::<User>(&mut diesel_conn)
            .unwrap();
    }

    let mut group = c.benchmark_group("pg_fetch_one");

    // -- bsql --
    group.bench_function("bsql", |b| {
        b.to_async(&rt).iter(|| async {
            let id = 42i32;
            let _user =
                bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i32")
                    .fetch_one(&bsql_pool)
                    .await
                    .unwrap();
        });
    });

    // -- sqlx --
    group.bench_function("sqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let _user: (i32, String, String) =
                sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = $1")
                    .bind(42i32)
                    .fetch_one(&sqlx_pool)
                    .await
                    .unwrap();
        });
    });

    // -- diesel (sync) --
    {
        use diesel::sql_types::{Integer, Text};

        #[derive(diesel::QueryableByName, Debug)]
        #[allow(dead_code)]
        struct User {
            #[diesel(sql_type = Integer)]
            id: i32,
            #[diesel(sql_type = Text)]
            name: String,
            #[diesel(sql_type = Text)]
            email: String,
        }

        group.bench_function("diesel", |b| {
            b.iter(|| {
                let _rows =
                    diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = $1")
                        .bind::<Integer, _>(42i32)
                        .load::<User>(&mut diesel_conn)
                        .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pg_fetch_one);
criterion_main!(benches);
