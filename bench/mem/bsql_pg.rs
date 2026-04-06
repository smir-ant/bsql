//! Memory benchmark for bsql (PostgreSQL)
//!
//! Uses the driver Connection directly (sync, no tokio) for minimum RSS.
//! Same workload as C/Go/sqlx/diesel: connect, 10K SELECTs, 1K INSERTs, exit.
//!
//! Run: BENCH_DATABASE_URL=... BSQL_DATABASE_URL=... /usr/bin/time -l cargo run --release --bin mem_bsql_pg

use bsql_driver_postgres::{Connection, Config, hash_sql, Encode};

fn main() {
    let url = std::env::var("BENCH_DATABASE_URL").expect("BENCH_DATABASE_URL");
    let config = Config::from_url(&url).expect("invalid URL");
    let mut conn = Connection::connect(&config).expect("connect failed");

    // 10K SELECT queries
    let sql = "SELECT id, name, email FROM bench_users WHERE id = $1";
    let hash = hash_sql(sql);
    for i in 0..10_000 {
        let id = (i % 10000 + 1) as i32;
        let params: &[&(dyn Encode + Sync)] = &[&id];
        let _result = conn.query(sql, hash, params).expect("query failed");
    }

    // 1K INSERT queries
    let sql_ins = "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)";
    let hash_ins = hash_sql(sql_ins);
    for i in 0..1_000 {
        let name = format!("memtest_{i}");
        let email = format!("mem{i}@test.com");
        let params: &[&(dyn Encode + Sync)] = &[&name as &(dyn Encode + Sync), &email];
        conn.execute(sql_ins, hash_ins, params).expect("insert failed");
    }
}
