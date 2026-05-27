#![forbid(unsafe_code)]
use bsql_postgres_sync::{ConnectConfig, Connection};

#[test]
#[ignore = "requires local PG"]
fn raw_tcp_sanity() {
    use std::io::{Read, Write};
    let mut tcp = std::net::TcpStream::connect("127.0.0.1:5432").unwrap();
    tcp.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    let startup: &[u8] = &[0,0,0,41, 0,3,0,0, 117,115,101,114,0, 115,109,105,114,45,97,110,116,0, 100,97,116,97,98,97,115,101,0, 112,111,115,116,103,114,101,115,0, 0];
    tcp.write_all(startup).unwrap();
    let mut buf = [0u8; 1024];
    let n = tcp.read(&mut buf).unwrap();
    assert!(n > 0, "PG should respond");
    assert_eq!(buf[0], b'R', "first byte should be R (AuthenticationOk)");
}

#[test]
#[ignore = "requires local PG"]
fn connect_and_ping() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    conn.ping().expect("ping");
    assert!(conn.is_healthy());
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn simple_query_ddl() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let tag = conn.simple_query("CREATE TEMP TABLE sync_test(i int)").expect("create");
    assert!(tag.contains("CREATE"));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn query_select_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");

    conn.simple_query("CREATE TEMP TABLE sq(id int, name text)").expect("create");
    conn.simple_query("INSERT INTO sq VALUES (1, 'alice'), (2, 'bob')").expect("insert");

    let result = conn.query("SELECT id, name FROM sq ORDER BY id").expect("select");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(&*result.column_names, &["id", "name"]);
    assert_eq!(result.rows[0].get_i32(0), Some(1));
    assert_eq!(result.rows[0].get_str(1), Some("alice"));
    assert_eq!(result.rows[1].get_by_name("name", &result.column_names), Some(b"bob".as_slice()));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_1000_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let result = conn.query("SELECT generate_series(1, 1000)").expect("query");
    assert_eq!(result.rows.len(), 1000);
    assert_eq!(result.rows[0].get_i32(0), Some(1));
    assert_eq!(result.rows[999].get_i32(0), Some(1000));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn prepared_statement_reuse() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");

    conn.execute("CREATE TEMP TABLE ps(id int, v text)").expect("create");
    conn.execute("INSERT INTO ps VALUES (1, 'a'), (2, 'b')").expect("insert");

    let stmt = conn.prepare("SELECT v FROM ps WHERE id = $1").expect("prepare");
    let r1 = conn.query_prepared(&stmt, &(1i32,)).expect("q1");
    assert_eq!(r1.rows[0].get_str(0), Some("a"));
    let r2 = conn.query_prepared(&stmt, &(2i32,)).expect("q2");
    assert_eq!(r2.rows[0].get_str(0), Some("b"));
    conn.close_statement(stmt).expect("close stmt");
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn query_params_one_shot() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let row = conn.query_params_one("SELECT $1::int + $2::int AS sum", &(10i32, 32i32))
        .expect("query_params_one");
    assert_eq!(row.get_i32(0), Some(42));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn transactions() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    conn.execute("CREATE TEMP TABLE tx(v int)").expect("create");

    conn.begin().expect("begin");
    conn.execute("INSERT INTO tx VALUES (1)").expect("insert");
    conn.commit().expect("commit");
    let r = conn.query("SELECT count(*) FROM tx").expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(1));

    conn.begin().expect("begin2");
    conn.execute("INSERT INTO tx VALUES (2)").expect("insert2");
    conn.rollback().expect("rollback");
    let r = conn.query("SELECT count(*) FROM tx").expect("count2");
    assert_eq!(r.rows[0].get_i64(0), Some(1));

    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn error_recovery() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");

    let err = conn.simple_query("INVALID SQL").unwrap_err();
    assert!(matches!(err, bsql_postgres_sync::DriverError::Db(_)));

    let result = conn.query("SELECT 42::int").expect("query after error");
    assert_eq!(result.rows[0].get_i32(0), Some(42));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn unicode_and_nulls() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let r = conn.query("SELECT 'Привет'::text, NULL::int, '🦀'::text").expect("query");
    assert_eq!(r.rows[0].get_str(0), Some("Привет"));
    assert!(r.rows[0].is_null(1));
    assert_eq!(r.rows[0].get_str(2), Some("🦀"));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn execute_params_insert() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    conn.execute("CREATE TEMP TABLE ep_test(id int, name text)").expect("create");
    let n = conn.execute_params("INSERT INTO ep_test VALUES ($1, $2)", &(42i32, "alice")).expect("insert");
    assert_eq!(n, 1);
    let row = conn.query_one("SELECT id, name FROM ep_test").expect("select");
    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.get_str(1), Some("alice"));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn column_names_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let result = conn.query("SELECT 1 AS id, 'hello' AS greeting").expect("query");
    assert_eq!(&*result.column_names, &["id", "greeting"]);
    assert_eq!(result.rows[0].get_by_name("id", &result.column_names), Some(b"1".as_slice()));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn db_error_sqlstate_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let err = conn.simple_query("SELCT TYPO").unwrap_err();
    if let bsql_postgres_sync::DriverError::Db(ref db_err) = err {
        assert_eq!(&db_err.code, "42601");
    } else { panic!("expected DbError"); }
    conn.ping().expect("ping after error");
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sequential_errors_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    for _ in 0..10 { let _ = conn.simple_query("INVALID SQL"); }
    let result = conn.query("SELECT 42::int").expect("query after errors");
    assert_eq!(result.rows[0].get_i32(0), Some(42));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn wide_row_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let cols: Vec<String> = (0..50).map(|i| format!("{i}::int AS c{i}")).collect();
    let result = conn.query(&format!("SELECT {}", cols.join(", "))).expect("query");
    assert_eq!(result.rows[0].len(), 50);
    assert_eq!(result.column_names.len(), 50);
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn null_heavy_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let result = conn.query("SELECT NULL::int, NULL::text FROM generate_series(1, 100)").expect("query");
    assert_eq!(result.rows.len(), 100);
    for row in &result.rows { assert!(row.is_null(0)); assert!(row.is_null(1)); }
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn query_100_rows_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    let result = conn.query("SELECT generate_series(1, 100)").expect("query");
    assert_eq!(result.rows.len(), 100);
    assert_eq!(result.rows[0].get_raw(0), Some(b"1".as_slice()));
    assert_eq!(result.rows[99].get_raw(0), Some(b"100".as_slice()));
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn execute_returns_count_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    conn.execute("CREATE TEMP TABLE erc(v int)").expect("create");
    let n = conn.execute("INSERT INTO erc VALUES (1), (2), (3)").expect("insert");
    assert_eq!(n, 3);
    conn.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn server_version_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let conn = Connection::connect(&config).expect("connect");
    assert!(conn.server_version().is_some());
}

#[test]
#[ignore = "requires local PG"]
fn prepared_reuse_after_error_sync() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect");
    conn.execute("CREATE TEMP TABLE pr_err(id int PRIMARY KEY)").expect("create");
    let stmt = conn.prepare("INSERT INTO pr_err VALUES ($1)").expect("prepare");
    conn.execute_prepared(&stmt, &(1i32,)).expect("insert 1");
    let err = conn.execute_prepared(&stmt, &(1i32,));
    assert!(err.is_err());
    conn.execute_prepared(&stmt, &(2i32,)).expect("insert 2 after error");
    let r = conn.query("SELECT count(*) FROM pr_err").expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(2));
    conn.close().expect("close");
}
