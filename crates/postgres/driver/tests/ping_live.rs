//! Live integration test — requires PG on localhost:5432 with Trust auth.
//!
//! Run: `cargo test -p bsql-driver-postgres --test ping_live`
//! Skip if no PG: tests are `#[ignore]` by default.

use bsql_postgres::{ConnectConfig, Connection};

#[tokio::test]
#[ignore = "requires local PG with Trust auth"]
async fn connect_and_ping() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());

    let mut conn = Connection::connect(&config)
        .await
        .expect("connect failed");

    conn.ping().await.expect("ping failed");
    conn.close().await.expect("close failed");
}

#[tokio::test]
#[ignore = "requires local PG with Trust auth"]
async fn connect_ping_twice() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());

    let mut conn = Connection::connect(&config)
        .await
        .expect("connect failed");

    conn.ping().await.expect("first ping failed");
    conn.ping().await.expect("second ping failed");
    conn.close().await.expect("close failed");
}
