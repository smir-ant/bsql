//! Memory benchmark for bsql (PostgreSQL)
//! Run: BENCH_DATABASE_URL=... BSQL_DATABASE_URL=... /usr/bin/time -l cargo run --release --bin mem_bsql_pg

use bsql::{BsqlError, Pool};

fn main() -> Result<(), BsqlError> {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let pool = Pool::connect(&url)?;

    // 10K SELECT queries
    for i in 0..10_000 {
        let id = (i % 10000 + 1) as i32;
        let _row = bsql::query!("SELECT id, name, email FROM bench_users WHERE id = $id: i32")
            .fetch_one(&pool)?;
    }

    // 1K INSERT queries
    for i in 0..1_000 {
        let name = format!("memtest_{i}");
        let email = format!("mem{i}@test.com");
        bsql::query!(
            "INSERT INTO bench_users (name, email, active, score) VALUES ($name: String, $email: String, true, 0.0)"
        )
        .execute(&pool)?;
    }

    Ok(())
}
