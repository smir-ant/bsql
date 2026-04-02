//! Benchmark: single-row SELECT by primary key (SQLite).
//!
//! Compares bsql, sqlx, and diesel fetching a single row by PK.
//!
//! Requires:
//!   BENCH_SQLITE_PATH     — path to the SQLite database file (runtime)
//!   BSQL_DATABASE_URL     — sqlite://<same path> (compile-time, for bsql::query!)

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_sqlite_path() -> String {
    std::env::var("BENCH_SQLITE_PATH").expect("BENCH_SQLITE_PATH must be set")
}

fn bench_sqlite_fetch_one(c: &mut Criterion) {
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
    rt.block_on(async {
        let _row =
            bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i64")
                .fetch_one(&bsql_pool)
                .await
                .unwrap();
    });
    rt.block_on(async {
        let _row: (i64, String, String) =
            sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = ?1")
                .bind(42i64)
                .fetch_one(&sqlx_pool)
                .await
                .unwrap();
    });
    {
        use diesel::sql_types::{BigInt, Text};

        #[derive(diesel::QueryableByName, Debug)]
        #[allow(dead_code)]
        struct User {
            #[diesel(sql_type = BigInt)]
            id: i64,
            #[diesel(sql_type = Text)]
            name: String,
            #[diesel(sql_type = Text)]
            email: String,
        }

        let _rows = diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = ?1")
            .bind::<BigInt, _>(42i64)
            .load::<User>(&mut diesel_conn)
            .unwrap();
    }

    let mut group = c.benchmark_group("sqlite_fetch_one");

    // -- bsql --
    group.bench_function("bsql", |b| {
        b.to_async(&rt).iter(|| async {
            let id = 42i64;
            let _user =
                bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i64")
                    .fetch_one(&bsql_pool)
                    .await
                    .unwrap();
        });
    });

    // -- sqlx --
    group.bench_function("sqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let _user: (i64, String, String) =
                sqlx::query_as("SELECT id, name, email FROM bench_users WHERE id = ?1")
                    .bind(42i64)
                    .fetch_one(&sqlx_pool)
                    .await
                    .unwrap();
        });
    });

    // -- diesel (sync) --
    {
        use diesel::sql_types::{BigInt, Text};

        #[derive(diesel::QueryableByName, Debug)]
        #[allow(dead_code)]
        struct User {
            #[diesel(sql_type = BigInt)]
            id: i64,
            #[diesel(sql_type = Text)]
            name: String,
            #[diesel(sql_type = Text)]
            email: String,
        }

        group.bench_function("diesel", |b| {
            b.iter(|| {
                let _rows =
                    diesel::sql_query("SELECT id, name, email FROM bench_users WHERE id = ?1")
                        .bind::<BigInt, _>(42i64)
                        .load::<User>(&mut diesel_conn)
                        .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sqlite_fetch_one);
criterion_main!(benches);
