//! Benchmark: INSERT operations (SQLite).
//!
//! Tests single INSERT RETURNING and batch INSERT (100 rows in a transaction).
//!
//! Requires:
//!   BENCH_SQLITE_PATH     — path to the SQLite database file (runtime)
//!   BSQL_DATABASE_URL     — sqlite://<same path> (compile-time, for bsql::query!)

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_sqlite_path() -> String {
    std::env::var("BENCH_SQLITE_PATH").expect("BENCH_SQLITE_PATH must be set")
}

fn bench_sqlite_insert_single(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let path = bench_sqlite_path();

    let bsql_pool = bsql::SqlitePool::connect(&path).unwrap();

    let sqlx_pool = rt.block_on(async {
        sqlx::SqlitePool::connect(&format!("sqlite:{path}"))
            .await
            .unwrap()
    });

    use diesel::prelude::*;
    let mut diesel_conn = SqliteConnection::establish(&path).unwrap();

    let mut group = c.benchmark_group("sqlite_insert_single");

    // -- bsql: single INSERT RETURNING --
    group.bench_function("bsql", |b| {
        b.to_async(&rt).iter(|| async {
            let name = "bench_insert";
            let email = "bench@example.com";
            let _row = bsql::query!(
                "INSERT INTO bench_users (name, email, active, score) VALUES ($name: &str, $email: &str, 1, 0.0) RETURNING id"
            )
            .fetch_one(&bsql_pool)
            .await
            .unwrap();
        });
    });

    // -- sqlx: single INSERT RETURNING --
    group.bench_function("sqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let _row: (i64,) = sqlx::query_as(
                "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0) RETURNING id",
            )
            .bind("bench_insert")
            .bind("bench@example.com")
            .fetch_one(&sqlx_pool)
            .await
            .unwrap();
        });
    });

    // -- diesel: single INSERT --
    // Note: diesel SQLite does not support RETURNING in sql_query for older SQLite versions.
    // We use execute() and last_insert_rowid pattern instead.
    {
        use diesel::sql_types::Text;

        group.bench_function("diesel", |b| {
            b.iter(|| {
                diesel::sql_query(
                    "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)",
                )
                .bind::<Text, _>("bench_insert")
                .bind::<Text, _>("bench@example.com")
                .execute(&mut diesel_conn)
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_sqlite_insert_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let path = bench_sqlite_path();

    let bsql_pool = bsql::SqlitePool::connect(&path).unwrap();

    let sqlx_pool = rt.block_on(async {
        sqlx::SqlitePool::connect(&format!("sqlite:{path}"))
            .await
            .unwrap()
    });

    use diesel::prelude::*;
    let mut diesel_conn = SqliteConnection::establish(&path).unwrap();

    let mut group = c.benchmark_group("sqlite_insert_batch_100");

    // -- bsql: 100 INSERTs in a transaction --
    group.bench_function("bsql", |b| {
        b.to_async(&rt).iter(|| async {
            let tx = bsql_pool.begin().await.unwrap();
            for i in 0..100i32 {
                let name = format!("batch_{i}");
                let email = format!("batch_{i}@example.com");
                bsql::query!(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($name: String, $email: String, 1, 0.0)"
                )
                .execute(&bsql_pool)
                .await
                .unwrap();
            }
            tx.commit().await.unwrap();
        });
    });

    // -- sqlx: 100 INSERTs in a transaction --
    group.bench_function("sqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tx = sqlx_pool.begin().await.unwrap();
            for i in 0..100i32 {
                let name = format!("batch_{i}");
                let email = format!("batch_{i}@example.com");
                sqlx::query(
                    "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)",
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
                                "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)",
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

criterion_group!(
    benches,
    bench_sqlite_insert_single,
    bench_sqlite_insert_batch
);
criterion_main!(benches);
