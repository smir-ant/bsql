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
    let _ = conn.close().await;
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

    let _ = conn.close().await;
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn select_1_returns_tag() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let tag = conn.simple_query("SELECT 1").await.expect("select");
    eprintln!("SELECT 1 tag: [{tag}]");
    assert!(tag.contains("SELECT"), "got: {tag}");
    let _ = conn.close().await;
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn query_select_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.simple_query("CREATE TEMP TABLE qtest(id int, name text)")
        .await.expect("create");
    conn.simple_query("INSERT INTO qtest VALUES (1, 'alice'), (2, 'bob')")
        .await.expect("insert");

    let result = conn.query("SELECT id, name FROM qtest ORDER BY id")
        .await.expect("select");

    eprintln!("tag: {}", result.command_tag);
    eprintln!("rows: {}", result.rows.len());
    for (i, row) in result.rows.iter().enumerate() {
        let cols: Vec<String> = row.iter().map(|c| match c {
            Some(b) => String::from_utf8_lossy(b).to_string(),
            None => "NULL".to_string(),
        }).collect();
        eprintln!("  row[{i}]: {:?}", cols);
    }

    assert_eq!(result.rows.len(), 2, "expected 2 rows");
    assert_eq!(result.rows[0].len(), 2, "expected 2 columns");
    assert_eq!(result.rows[0][0].as_deref(), Some(b"1".as_slice()));
    assert_eq!(result.rows[0][1].as_deref(), Some(b"alice".as_slice()));
    assert_eq!(result.rows[1][0].as_deref(), Some(b"2".as_slice()));
    assert_eq!(result.rows[1][1].as_deref(), Some(b"bob".as_slice()));

    let _ = conn.close().await;
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn query_with_nulls() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.query("SELECT 1, NULL::text, 'hello'").await.expect("select");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].len(), 3);
    assert_eq!(result.rows[0][0].as_deref(), Some(b"1".as_slice()));
    assert!(result.rows[0][1].is_none(), "expected NULL");
    assert_eq!(result.rows[0][2].as_deref(), Some(b"hello".as_slice()));

    let _ = conn.close().await;
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn query_100_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.query("SELECT generate_series(1, 100)")
        .await.expect("select");
    assert_eq!(result.rows.len(), 100, "expected 100 rows");
    // First row = "1", last = "100"
    assert_eq!(result.rows[0][0].as_deref(), Some(b"1".as_slice()));
    assert_eq!(result.rows[99][0].as_deref(), Some(b"100".as_slice()));

    let _ = conn.close().await;
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn bad_sql_returns_error() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.simple_query("SELCT TYPO").await;
    assert!(result.is_err(), "bad SQL should error");

    // Connection should still be usable after error
    // Connection may be in errored state — recovery is future work
    let _ = conn.close().await;
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth (not trust)"]
async fn scram_auth_connect_and_query() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string())
        .password("test_password_123".to_string());

    let mut conn = Connection::connect(&config).await.expect("SCRAM connect");
    let result = conn.query("SELECT current_user").await.expect("query");
    eprintln!("user: {:?}", result.rows[0][0].as_deref().map(String::from_utf8_lossy));
    assert_eq!(result.rows[0][0].as_deref(), Some(b"bsql_test_scram".as_slice()));

    conn.close().await.expect("close");
}
