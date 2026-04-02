//! Benchmark: complex queries — JOIN + aggregation and subquery (SQLite).
//!
//! Requires:
//!   BENCH_SQLITE_PATH     — path to the SQLite database file (runtime)
//!   BSQL_DATABASE_URL     — sqlite://<same path> (compile-time, for bsql::query!)

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_sqlite_path() -> String {
    std::env::var("BENCH_SQLITE_PATH").expect("BENCH_SQLITE_PATH must be set")
}

fn bench_sqlite_join_aggregate(c: &mut Criterion) {
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

    let sql_text = "\
        SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount \
        FROM bench_users u \
        JOIN bench_orders o ON u.id = o.user_id \
        WHERE u.active = 1 \
        GROUP BY u.name \
        ORDER BY SUM(o.amount) DESC \
        LIMIT 100";

    // Warm up
    rt.block_on(async {
        let _rows = bsql::query!(
            "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount
             FROM bench_users u
             JOIN bench_orders o ON u.id = o.user_id
             WHERE u.active = 1
             GROUP BY u.name
             ORDER BY SUM(o.amount) DESC
             LIMIT 100"
        )
        .fetch_all(&bsql_pool)
        .await
        .unwrap();
    });

    let mut group = c.benchmark_group("sqlite_join_aggregate");

    // -- bsql --
    group.bench_function("bsql", |b| {
        b.to_async(&rt).iter(|| async {
            let _rows = bsql::query!(
                "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount
                 FROM bench_users u
                 JOIN bench_orders o ON u.id = o.user_id
                 WHERE u.active = 1
                 GROUP BY u.name
                 ORDER BY SUM(o.amount) DESC
                 LIMIT 100"
            )
            .fetch_all(&bsql_pool)
            .await
            .unwrap();
        });
    });

    // -- sqlx --
    group.bench_function("sqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let _rows: Vec<(String, i32, f64)> = sqlx::query_as(sql_text)
                .fetch_all(&sqlx_pool)
                .await
                .unwrap();
        });
    });

    // -- diesel --
    {
        use diesel::sql_types::{Double, Integer, Text};

        #[derive(diesel::QueryableByName, Debug)]
        #[allow(dead_code)]
        struct AggRow {
            #[diesel(sql_type = Text)]
            name: String,
            #[diesel(sql_type = Integer)]
            order_count: i32,
            #[diesel(sql_type = Double)]
            total_amount: f64,
        }

        group.bench_function("diesel", |b| {
            b.iter(|| {
                let _rows = diesel::sql_query(sql_text)
                    .load::<AggRow>(&mut diesel_conn)
                    .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_sqlite_subquery(c: &mut Criterion) {
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

    let sql_text = "\
        SELECT id, name, email \
        FROM bench_users \
        WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)";

    let mut group = c.benchmark_group("sqlite_subquery");

    // -- bsql --
    group.bench_function("bsql", |b| {
        b.to_async(&rt).iter(|| async {
            let _rows = bsql::query!(
                "SELECT id, name, email FROM bench_users
                 WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)"
            )
            .fetch_all(&bsql_pool)
            .await
            .unwrap();
        });
    });

    // -- sqlx --
    group.bench_function("sqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let _rows: Vec<(i64, String, String)> = sqlx::query_as(sql_text)
                .fetch_all(&sqlx_pool)
                .await
                .unwrap();
        });
    });

    // -- diesel --
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
                let _rows = diesel::sql_query(sql_text)
                    .load::<User>(&mut diesel_conn)
                    .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sqlite_join_aggregate, bench_sqlite_subquery);
criterion_main!(benches);
