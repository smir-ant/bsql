#![forbid(unsafe_code)]
use bsql_postgres_async::{ConnectConfig, Connection};

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
async fn column_names() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.query("SELECT 1 AS id, 'hello' AS greeting")
        .await.expect("select");
    assert_eq!(&*result.column_names, &["id", "greeting"]);

    let row = &result.rows[0];
    assert_eq!(row.get_by_name("id", &result.column_names), Some(b"1".as_slice()));
    assert_eq!(row.get_by_name("greeting", &result.column_names), Some(b"hello".as_slice()));
    assert_eq!(row.get_by_name("missing", &result.column_names), None);

    let result2 = conn.query_params(
        "SELECT $1::int AS val",
        &(42i32,),
    ).await.expect("query_params");
    assert_eq!(&*result2.column_names, &["val"]);
    assert_eq!(result2.rows[0].get_by_name("val", &result2.column_names), Some(b"42".as_slice()));

    conn.close().await.expect("close");
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

    conn.ping().await.expect("ping after error"); conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn db_error_sqlstate() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    // Syntax error → SQLSTATE 42601
    let err = conn.simple_query("SELCT TYPO").await.unwrap_err();
    if let bsql_postgres_async::DriverError::Db(ref db_err) = err {
        eprintln!("code={} severity={} msg={}", db_err.code, db_err.severity, db_err.message);
        assert_eq!(&db_err.code, "42601", "expected syntax_error SQLSTATE");
        assert_eq!(&db_err.severity, "ERROR");
        assert!(!db_err.message.is_empty());
    } else {
        panic!("expected DbError, got: {err:?}");
    }

    // Unique violation → SQLSTATE 23505
    conn.execute("CREATE TEMP TABLE uk_test(id int PRIMARY KEY)").await.expect("create");
    conn.execute("INSERT INTO uk_test VALUES (1)").await.expect("insert");
    let err = conn.execute("INSERT INTO uk_test VALUES (1)").await.unwrap_err();
    if let bsql_postgres_async::DriverError::Db(ref db_err) = err {
        eprintln!("code={} severity={} msg={}", db_err.code, db_err.severity, db_err.message);
        assert!(db_err.is_unique_violation(), "expected 23505, got {}", db_err.code);
    } else {
        panic!("expected DbError for unique violation, got: {err:?}");
    }

    conn.ping().await.expect("ping after errors");
    conn.close().await.expect("close");
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

#[tokio::test]
#[ignore = "requires local PG"]
async fn unicode_round_trip() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let row = conn.query_one("SELECT 'Привет мир 🌍'::text").await.expect("query");
    assert_eq!(row.get_str(0), Some("Привет мир 🌍"));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn connect_wrong_port_errors() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").port(19999);
    let result = Connection::connect(&config).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth (not trust)"]
async fn connect_wrong_password_errors() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string())
        .password("WRONG_PASSWORD".to_string());
    let result = Connection::connect(&config).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn large_result_1000_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let result = conn.query("SELECT generate_series(1, 1000)").await.expect("query");
    assert_eq!(result.rows.len(), 1000);
    assert_eq!(result.rows[0].get_i32(0), Some(1));
    assert_eq!(result.rows[999].get_i32(0), Some(1000));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn large_result_10k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let result = conn.query("SELECT generate_series(1, 10000)").await.expect("query");
    assert_eq!(result.rows.len(), 10000);
    assert_eq!(result.rows[0].get_i32(0), Some(1));
    assert_eq!(result.rows[9999].get_i32(0), Some(10000));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn large_result_100k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    let result = conn.query("SELECT generate_series(1, 100000)").await.expect("query");
    assert_eq!(result.rows.len(), 100000);
    assert_eq!(result.rows[0].get_i32(0), Some(1));
    assert_eq!(result.rows[99999].get_i32(0), Some(100000));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn execute_returns_row_count() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE exec_test(v int)").await.expect("create");

    let n = conn.execute("INSERT INTO exec_test VALUES (1), (2), (3)")
        .await.expect("insert");
    assert_eq!(n, 3, "expected 3 rows inserted");

    let n = conn.execute("UPDATE exec_test SET v = v + 10 WHERE v > 1")
        .await.expect("update");
    assert_eq!(n, 2, "expected 2 rows updated");

    let n = conn.execute("DELETE FROM exec_test WHERE v = 1")
        .await.expect("delete");
    assert_eq!(n, 1, "expected 1 row deleted");

    conn.close().await.expect("close");
}


#[tokio::test]
#[ignore = "requires local PG"]
async fn server_version_and_pid() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let version = conn.server_version();
    eprintln!("server version: {:?}", version);
    assert!(version.is_some(), "expected server version");

    let pid = conn.backend_pid();
    eprintln!("backend pid: {pid}");
    assert!(pid > 0, "expected positive pid");

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn query_opt_found_and_not_found() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let found = conn.query_opt("SELECT 42").await.expect("found");
    assert!(found.is_some());
    assert_eq!(found.unwrap().get_i32(0), Some(42));

    let empty = conn.query_opt("SELECT 1 WHERE false").await.expect("empty");
    assert!(empty.is_none());

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn generic_get_typed() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let row = conn.query_one("SELECT 42::int, 3.14::float8, true::bool, 'hello'::text")
        .await.expect("query");

    let i: i32 = row.get(0).unwrap();
    let f: f64 = row.get(1).unwrap();
    let b: bool = row.get(2).unwrap();
    let s: String = row.get(3).unwrap();

    assert_eq!(i, 42);
    assert!((f - 3.14).abs() < 0.001);
    assert!(b);
    assert_eq!(s, "hello");

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires unreachable host"]
async fn connect_timeout() {
    // 192.0.2.1 is TEST-NET-1 — packets are dropped, never refused
    let config = ConnectConfig::new("192.0.2.1", "test")
        .connect_timeout(1);
    let start = std::time::Instant::now();
    let result = Connection::connect(&config).await;
    let elapsed = start.elapsed();
    assert!(result.is_err());
    assert!(elapsed.as_secs() <= 3, "timeout took too long: {:?}", elapsed);
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn ssl_prefer_falls_back_to_plain() {
    use bsql_postgres_async::SslMode;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Prefer);
    let mut conn = Connection::connect(&config).await.expect("prefer should fallback");
    conn.ping().await.expect("ping");
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG without SSL"]
async fn ssl_require_fails_on_non_ssl_server() {
    use bsql_postgres_async::SslMode;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Require);
    let result = Connection::connect(&config).await;
    assert!(result.is_err(), "require should fail on non-SSL server");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn ssl_disable_works() {
    use bsql_postgres_async::SslMode;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable);
    let mut conn = Connection::connect(&config).await.expect("disable should work");
    conn.ping().await.expect("ping");
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn connect_via_dsn() {
    let config = ConnectConfig::from_dsn(
        "postgres://smir-ant@127.0.0.1:5432/postgres?sslmode=disable"
    ).expect("parse DSN");
    let mut conn = Connection::connect(&config).await.expect("connect");
    let row = conn.query_one("SELECT 1").await.expect("query");
    assert_eq!(row.get_i32(0), Some(1));
    conn.close().await.expect("close");
}

#[test]
fn dsn_parsing_unit_tests() {
    let c = ConnectConfig::from_dsn("postgres://alice:secret@db.example.com:5433/mydb?sslmode=require").unwrap();
    assert_eq!(c.user, "alice");
    assert_eq!(c.password_str(), Some("secret"));
    assert_eq!(c.host, "db.example.com");
    assert_eq!(c.port, 5433);
    assert_eq!(c.database.as_deref(), Some("mydb"));
    assert_eq!(c.ssl_mode, bsql_postgres_async::SslMode::Require);

    let c2 = ConnectConfig::from_dsn("postgres://bob@localhost").unwrap();
    assert_eq!(c2.user, "bob");
    assert!(c2.password_str().is_none());
    assert_eq!(c2.host, "localhost");
    assert_eq!(c2.port, 5432);
    assert!(c2.database.is_none());

    assert!(ConnectConfig::from_dsn("http://bad").is_err());
    assert!(ConnectConfig::from_dsn("postgres://").is_err());
}


#[test]
fn from_env_creates_config() {
    // Just verify from_env doesn't panic and returns sane defaults
    let config = ConnectConfig::from_env();
    assert!(!config.host.is_empty());
    assert!(config.port > 0);
    assert!(!config.user.is_empty());
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn execute_params_insert() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE param_test(id int, name text)").await.expect("create");

    let n = conn.execute_params(
        "INSERT INTO param_test VALUES ($1, $2)",
        &(42i32, "alice"),
    ).await.expect("insert");
    assert_eq!(n, 1, "expected 1 row inserted");

    let row = conn.query_one("SELECT id, name FROM param_test").await.expect("select");
    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.get_str(1), Some("alice"));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_query_reuse() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE prep_test(id int, name text)").await.expect("create");
    conn.execute("INSERT INTO prep_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .await.expect("insert");

    let stmt = conn.prepare("SELECT id, name FROM prep_test WHERE id = $1")
        .await.expect("prepare");

    let r1 = conn.query_prepared(&stmt, &(1i32,)).await.expect("q1");
    assert_eq!(r1.rows.len(), 1);
    assert_eq!(r1.rows[0].get_str(1), Some("alice"));

    let r2 = conn.query_prepared(&stmt, &(2i32,)).await.expect("q2");
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(r2.rows[0].get_str(1), Some("bob"));

    let r3 = conn.query_prepared(&stmt, &(3i32,)).await.expect("q3");
    assert_eq!(r3.rows.len(), 1);
    assert_eq!(r3.rows[0].get_str(1), Some("charlie"));

    conn.close_statement(stmt).await.expect("close stmt");
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_execute_dml() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE prep_dml(v int)").await.expect("create");

    let stmt = conn.prepare("INSERT INTO prep_dml VALUES ($1)")
        .await.expect("prepare");

    for i in 1..=5 {
        let n = conn.execute_prepared(&stmt, &(i as i32,)).await.expect("exec");
        assert_eq!(n, 1);
    }

    let result = conn.query("SELECT count(*) FROM prep_dml").await.expect("count");
    assert_eq!(result.rows[0].get_i64(0), Some(5));

    conn.close_statement(stmt).await.expect("close stmt");
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_empty_result() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE prep_empty(id int)").await.expect("create");

    let stmt = conn.prepare("SELECT id FROM prep_empty WHERE id = $1")
        .await.expect("prepare");

    let result = conn.query_prepared(&stmt, &(999i32,)).await.expect("query");
    assert_eq!(result.rows.len(), 0);

    conn.close_statement(stmt).await.expect("close stmt");
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn query_params_select() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE qp_test(id int, name text)").await.expect("create");
    conn.execute("INSERT INTO qp_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .await.expect("insert");

    let result = conn.query_params(
        "SELECT name FROM qp_test WHERE id > $1 ORDER BY id",
        &(1i32,),
    ).await.expect("query_params");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get_str(0), Some("bob"));
    assert_eq!(result.rows[1].get_str(0), Some("charlie"));

    let row = conn.query_params_one(
        "SELECT name FROM qp_test WHERE id = $1",
        &(2i32,),
    ).await.expect("query_params_one");
    assert_eq!(row.get_str(0), Some("bob"));

    let none = conn.query_params_opt(
        "SELECT name FROM qp_test WHERE id = $1",
        &(999i32,),
    ).await.expect("query_params_opt");
    assert!(none.is_none());

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn transaction_helpers() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE tx_h(v int)").await.expect("create");

    conn.begin().await.expect("begin");
    conn.execute("INSERT INTO tx_h VALUES (1)").await.expect("insert");
    conn.commit().await.expect("commit");
    let r = conn.query("SELECT count(*) FROM tx_h").await.expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(1));

    conn.begin().await.expect("begin2");
    conn.execute("INSERT INTO tx_h VALUES (2)").await.expect("insert2");
    conn.rollback().await.expect("rollback");
    let r = conn.query("SELECT count(*) FROM tx_h").await.expect("count2");
    assert_eq!(r.rows[0].get_i64(0), Some(1));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn listen_notify() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut listener = Connection::connect(&config).await.expect("listener");
    let mut notifier = Connection::connect(&config).await.expect("notifier");

    listener.listen("bsql_test_ch").await.expect("listen");

    notifier.simple_query("NOTIFY bsql_test_ch, 'hello from bsql'")
        .await.expect("notify");

    let notif = listener.recv_notification(std::time::Duration::from_secs(5))
        .await.expect("recv");
    let notif = notif.expect("should have notification");
    assert_eq!(notif.channel, "bsql_test_ch");
    assert_eq!(notif.payload, "hello from bsql");
    assert!(notif.pid > 0);

    listener.unlisten("bsql_test_ch").await.expect("unlisten");

    let none = listener.recv_notification(std::time::Duration::from_millis(100))
        .await.expect("recv timeout");
    assert!(none.is_none(), "no notification after unlisten");

    listener.close().await.expect("close listener");
    notifier.close().await.expect("close notifier");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_bulk_insert() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE cp_test(id int, name text)").await.expect("create");

    let rows = vec![
        "1\talice",
        "2\tbob",
        "3\tcharlie",
    ];
    let n = conn.copy_in("cp_test", rows).await.expect("copy_in");
    assert_eq!(n, 3, "expected 3 rows copied");

    let result = conn.query("SELECT id, name FROM cp_test ORDER BY id")
        .await.expect("select");
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get_str(0), Some("1"));
    assert_eq!(result.rows[0].get_str(1), Some("alice"));
    assert_eq!(result.rows[2].get_str(1), Some("charlie"));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_large() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE cp_large(i int)").await.expect("create");

    let rows: Vec<String> = (0..10000).map(|i| i.to_string()).collect();
    let n = conn.copy_in("cp_large", &rows).await.expect("copy_in");
    assert_eq!(n, 10000);

    let result = conn.query("SELECT count(*) FROM cp_large").await.expect("count");
    assert_eq!(result.rows[0].get_i64(0), Some(10000));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_empty() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");
    conn.execute("CREATE TEMP TABLE cp_empty(v int)").await.expect("create");

    let n = conn.copy_in("cp_empty", Vec::<&str>::new()).await.expect("copy_in empty");
    assert_eq!(n, 0);

    let r = conn.query("SELECT count(*) FROM cp_empty").await.expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(0));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_reuse_after_error() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE pr_err(id int PRIMARY KEY)").await.expect("create");
    let stmt = conn.prepare("INSERT INTO pr_err VALUES ($1)").await.expect("prepare");

    conn.execute_prepared(&stmt, &(1i32,)).await.expect("insert 1");
    let err = conn.execute_prepared(&stmt, &(1i32,)).await;
    assert!(err.is_err(), "duplicate should fail");

    conn.execute_prepared(&stmt, &(2i32,)).await.expect("insert 2 after error");

    let r = conn.query("SELECT count(*) FROM pr_err").await.expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(2));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn unicode_values() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.query("SELECT 'Привет мир'::text, '日本語'::text, '🦀🐘'::text")
        .await.expect("query");
    assert_eq!(result.rows[0].get_str(0), Some("Привет мир"));
    assert_eq!(result.rows[0].get_str(1), Some("日本語"));
    assert_eq!(result.rows[0].get_str(2), Some("🦀🐘"));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn null_heavy_result() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let result = conn.query(
        "SELECT NULL::int, NULL::text, NULL::bool FROM generate_series(1, 500)"
    ).await.expect("query");
    assert_eq!(result.rows.len(), 500);
    for row in &result.rows {
        assert!(row.is_null(0));
        assert!(row.is_null(1));
        assert!(row.is_null(2));
        assert_eq!(row.len(), 3);
    }
    conn.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires local PG"]
async fn pool_concurrent_contention() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let pool = Pool::new(config, 3).await.expect("pool");

    let mut handles = Vec::new();
    for i in 0..20u32 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = p.get().await.expect("get");
            let r = conn.query(&format!("SELECT {i}::int")).await.expect("query");
            assert_eq!(r.rows[0].get_i32(0), Some(i as i32));
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn sequential_errors_recovery() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    for _ in 0..10 {
        let err = conn.simple_query("INVALID SQL").await;
        assert!(err.is_err());
    }

    let result = conn.query("SELECT 42::int").await.expect("query after 10 errors");
    assert_eq!(result.rows[0].get_i32(0), Some(42));
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn wide_row_many_columns() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    let cols: Vec<String> = (0..50).map(|i| format!("{i}::int AS c{i}")).collect();
    let sql = format!("SELECT {}", cols.join(", "));
    let result = conn.query(&sql).await.expect("query");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].len(), 50);
    assert_eq!(result.column_names.len(), 50);
    assert_eq!(result.column_names[0], "c0");
    assert_eq!(result.column_names[49], "c49");
    assert_eq!(result.rows[0].get_i32(0), Some(0));
    assert_eq!(result.rows[0].get_i32(49), Some(49));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_basic() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let pool = Pool::new(config, 3).await.expect("pool");

    // Get two connections
    let mut c1 = pool.get().await.expect("c1");
    let mut c2 = pool.get().await.expect("c2");

    c1.ping().await.expect("c1 ping");
    c2.ping().await.expect("c2 ping");

    let r1 = c1.query_one("SELECT 1").await.expect("q1");
    assert_eq!(r1.get_i32(0), Some(1));

    let r2 = c2.query_one("SELECT 2").await.expect("q2");
    assert_eq!(r2.get_i32(0), Some(2));

    // Return c1 to pool
    drop(c1);
    tokio::task::yield_now().await;

    assert_eq!(pool.idle_count(), 1);

    // Get c3 — should reuse c1's connection
    let mut c3 = pool.get().await.expect("c3");
    c3.ping().await.expect("c3 ping");

    drop(c2);
    drop(c3);
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_concurrent_queries() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let pool = Pool::new(config, 5).await.expect("pool");

    let mut handles = vec![];
    for i in 0..5u32 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = p.get().await.expect("get");
            let row = conn.query_one(&format!("SELECT {i}")).await.expect("q");
            assert_eq!(row.get_i32(0), Some(i as i32));
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn full_lifecycle_integration() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string());
    let mut conn = Connection::connect(&config).await.expect("connect");

    conn.execute("CREATE TEMP TABLE lc(id serial PRIMARY KEY, name text, val int)").await.expect("create");

    conn.begin().await.expect("begin");
    conn.execute("INSERT INTO lc(name, val) VALUES ('a', 10)").await.expect("insert a");
    conn.execute("INSERT INTO lc(name, val) VALUES ('b', 20)").await.expect("insert b");
    conn.commit().await.expect("commit");

    let stmt = conn.prepare("SELECT name FROM lc WHERE val = $1").await.expect("prepare");
    let r1 = conn.query_prepared(&stmt, &(10i32,)).await.expect("q1");
    assert_eq!(r1.rows[0].get_str(0), Some("a"));
    let r2 = conn.query_prepared(&stmt, &(20i32,)).await.expect("q2");
    assert_eq!(r2.rows[0].get_str(0), Some("b"));
    conn.close_statement(stmt).await.expect("close stmt");

    let err = conn.execute("INSERT INTO lc(id) VALUES (1)").await;
    assert!(err.is_err());
    conn.ping().await.expect("ping after error");

    // Row is Send + 'static — can cross .await
    let result = conn.query("SELECT name FROM lc ORDER BY val").await.expect("query");
    let row = result.rows[0].clone();
    let name = tokio::task::spawn(async move {
        row.get_str(0).map(String::from)
    }).await.expect("spawn");
    assert_eq!(name, Some("a".to_string()));

    conn.close().await.expect("close");
}
