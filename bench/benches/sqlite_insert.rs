//! Benchmark: INSERT operations (SQLite).
//!
//! Tests single INSERT RETURNING and batch INSERT (100 rows in a transaction).
//!
//! Requires:
//!   BENCH_SQLITE_PATH     — path to the SQLite database file (runtime)
//!   BSQL_DATABASE_URL     — sqlite://<same path> (compile-time, for bsql::query!)

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_sqlite_path() -> String {
    std::env::var("BENCH_SQLITE_PATH").expect("BENCH_SQLITE_PATH must be set")
}

fn bench_sqlite_insert_single(c: &mut Criterion) {
    let path = bench_sqlite_path();

    // sqlx is still async — it needs a runtime for its pool
    let rt = tokio::runtime::Runtime::new().unwrap();

    let bsql_pool = bsql::SqlitePool::connect(&path).unwrap();

    let sqlx_pool = rt.block_on(async {
        sqlx::SqlitePool::connect(&format!("sqlite:{path}"))
            .await
            .unwrap()
    });

    use diesel::prelude::*;
    let mut diesel_conn = SqliteConnection::establish(&path).unwrap();

    let mut group = c.benchmark_group("sqlite_insert_single");

    // -- bsql: single INSERT RETURNING (sync) --
    group.bench_function("bsql", |b| {
        b.iter(|| {
            let name = "bench_insert";
            let email = "bench@example.com";
            let _row = bsql::query!(
                "INSERT INTO bench_users (name, email, active, score) VALUES ($name: &str, $email: &str, 1, 0.0) RETURNING id"
            )
            .fetch_one(&bsql_pool)
            .unwrap();
        });
    });

    // -- sqlx: single INSERT RETURNING (async — needs runtime) --
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
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
    let path = bench_sqlite_path();

    // sqlx is still async — it needs a runtime for its pool
    let rt = tokio::runtime::Runtime::new().unwrap();

    let bsql_pool = bsql::SqlitePool::connect(&path).unwrap();

    let sqlx_pool = rt.block_on(async {
        sqlx::SqlitePool::connect(&format!("sqlite:{path}"))
            .await
            .unwrap()
    });

    use diesel::prelude::*;
    let mut diesel_conn = SqliteConnection::establish(&path).unwrap();

    let mut group = c.benchmark_group("sqlite_insert_batch_100");

    // -- bsql: 100 INSERTs in a transaction (sync) --
    group.bench_function("bsql", |b| {
        b.iter(|| {
            let tx = bsql_pool.begin().unwrap();
            for i in 0..100i32 {
                let name = format!("batch_{i}");
                let email = format!("batch_{i}@example.com");
                bsql::query!(
                    "INSERT INTO bench_users (name, email, active, score) VALUES ($name: String, $email: String, 1, 0.0)"
                )
                .execute(&bsql_pool)
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

fn bench_sqlite_insert_batch_optimized(c: &mut Criterion) {
    let path = bench_sqlite_path();

    let bsql_pool = bsql::SqlitePool::connect(&path).unwrap();

    let mut group = c.benchmark_group("sqlite_insert_batch_100_optimized");

    // -- bsql: 100 INSERTs using execute_batch (one acquire, one prepare) --
    group.bench_function("bsql_batch", |b| {
        b.iter(|| {
            use bsql_driver_sqlite::codec::SqliteEncode;

            let tx = bsql_pool.begin().unwrap();

            let sql =
                "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)";
            let sql_hash = bsql_driver_sqlite::conn::hash_sql(sql);

            let names: Vec<String> = (0..100).map(|i| format!("batch_{i}")).collect();
            let emails: Vec<String> = (0..100).map(|i| format!("batch_{i}@example.com")).collect();

            let param_sets: Vec<[&dyn SqliteEncode; 2]> = names
                .iter()
                .zip(emails.iter())
                .map(|(n, e)| [n as &dyn SqliteEncode, e as &dyn SqliteEncode])
                .collect();
            let param_refs: Vec<&[&dyn SqliteEncode]> =
                param_sets.iter().map(|p| p.as_slice()).collect();

            bsql_pool
                .__inner()
                .execute_batch(sql, sql_hash, &param_refs)
                .unwrap();

            tx.commit().unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sqlite_insert_single,
    bench_sqlite_insert_batch,
    bench_sqlite_insert_batch_optimized
);
criterion_main!(benches);
