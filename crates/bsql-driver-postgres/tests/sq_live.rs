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

#[tokio::test]
#[ignore = "requires local PG"]
async fn select_1_returns_tag() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let tag = conn.simple_query("SELECT 1").await.expect("select");
    eprintln!("SELECT 1 tag: [{tag}]");
    assert!(tag.contains("SELECT"), "got: {tag}");
    conn.close().await.expect("close");
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

    conn.close().await.expect("close");
}
