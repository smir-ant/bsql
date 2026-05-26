#![forbid(unsafe_code)]
use bsql_driver_postgres::{ConnectConfig, Connection};

#[tokio::test]
#[ignore = "requires local PG"]
async fn dml_create() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let tag = conn.simple_query("CREATE TEMP TABLE bsql_sq_test(i int)")
        .await.expect("query");
    eprintln!("tag: [{tag}]");
    assert!(tag.contains("CREATE"), "got: {tag}");
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn dml_insert_and_drop() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let t = conn.simple_query("CREATE TEMP TABLE sq2(v text)").await.expect("create");
    assert!(t.contains("CREATE"), "got: {t}");

    let t = conn.simple_query("INSERT INTO sq2 VALUES ('hello')").await.expect("insert");
    assert!(t.contains("INSERT"), "got: {t}");

    let t = conn.simple_query("DROP TABLE sq2").await.expect("drop");
    assert!(t.contains("DROP"), "got: {t}");

    conn.close().await.expect("close");
}
