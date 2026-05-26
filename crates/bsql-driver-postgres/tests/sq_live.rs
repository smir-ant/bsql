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
    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
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

    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
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
    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
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
        let c0 = row.get_str(0).unwrap_or("NULL");
        let c1 = row.get_str(1).unwrap_or("NULL");
        eprintln!("  row[{i}]: [{c0}, {c1}]");
    }

    assert_eq!(result.rows.len(), 2, "expected 2 rows");
    assert_eq!(result.rows[0].len(), 2, "expected 2 columns");
    assert_eq!(result.rows[0].get_raw(0), Some(b"1".as_slice()));
    assert_eq!(result.rows[0].get_raw(1), Some(b"alice".as_slice()));
    assert_eq!(result.rows[1].get_raw(0), Some(b"2".as_slice()));
    assert_eq!(result.rows[1].get_raw(1), Some(b"bob".as_slice()));

    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
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
    assert_eq!(result.rows[0].get_raw(0), Some(b"1".as_slice()));
    assert!(result.rows[0].is_null(1), "expected NULL");
    assert_eq!(result.rows[0].get_raw(2), Some(b"hello".as_slice()));

    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
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
    assert_eq!(result.rows[0].get_raw(0), Some(b"1".as_slice()));
    assert_eq!(result.rows[99].get_raw(0), Some(b"100".as_slice()));

    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
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
    // Connection recovers — ping after error works
    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth (not trust)"]
async fn scram_auth_connect_and_query() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string())
        .password("test_password_123".to_string());

    let mut conn = Connection::connect(&config).await.expect("SCRAM connect");
    let result = conn.query("SELECT current_user").await.expect("query");
    eprintln!("user: {:?}", result.rows[0].get_raw(0).map(String::from_utf8_lossy));
    assert_eq!(result.rows[0].get_raw(0), Some(b"bsql_test_scram".as_slice()));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_row_access() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.query("SELECT 42::int, 'hello'::text, true::bool, NULL::int")
        .await.expect("query");
    let row = &result.rows[0];

    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.get_str(1), Some("hello"));
    assert_eq!(row.get_bool(2), Some(true));
    assert!(row.is_null(3));
    assert_eq!(row.get_i32(3), None); // NULL → None
    assert_eq!(row.len(), 4);

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn sequential_queries() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.simple_query("CREATE TEMP TABLE seq_test(v int)").await.expect("create");
    for i in 1..=5 {
        conn.simple_query(&format!("INSERT INTO seq_test VALUES ({i})"))
            .await.expect("insert");
    }
    let result = conn.query("SELECT v FROM seq_test ORDER BY v").await.expect("select");
    assert_eq!(result.rows.len(), 5);
    for (i, row) in result.rows.iter().enumerate() {
        assert_eq!(row.get_i32(0), Some((i as i32) + 1));
    }

    // Second SELECT on same connection
    let r2 = conn.query("SELECT count(*) FROM seq_test").await.expect("count");
    assert_eq!(r2.rows[0].get_i64(0), Some(5));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn transaction_commit() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.simple_query("CREATE TEMP TABLE tx_test(v int)").await.expect("create");
    conn.simple_query("BEGIN").await.expect("begin");
    conn.simple_query("INSERT INTO tx_test VALUES (1)").await.expect("insert");
    conn.simple_query("COMMIT").await.expect("commit");

    let result = conn.query("SELECT v FROM tx_test").await.expect("select");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get_i32(0), Some(1));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn query_one_convenience() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let row = conn.query_one("SELECT 42::int, 3.14::float8, 'bsql'::text")
        .await.expect("query_one");
    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.get_f64(1), Some(3.14));
    assert_eq!(row.get_str(2), Some("bsql"));

    conn.close().await.expect("close");
}
