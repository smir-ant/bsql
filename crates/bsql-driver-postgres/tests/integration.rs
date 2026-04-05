//! Integration tests for bsql-driver-postgres.
//!
//! These tests require a running PostgreSQL instance. Set `BSQL_DATABASE_URL`
//! to a connection URL, e.g.:
//!
//! ```sh
//! BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test -p bsql-driver
//! ```
//!
//! Tests are skipped (not failed) if the environment variable is not set.

use bsql_driver_postgres::{Arena, Config, Connection, DriverError, Pool, hash_sql};

fn db_url() -> Option<String> {
    std::env::var("BSQL_DATABASE_URL").ok()
}

/// Skip the test if no database URL is configured.
macro_rules! require_db {
    () => {
        match db_url() {
            Some(url) => url,
            None => {
                eprintln!("BSQL_DATABASE_URL not set — skipping integration test");
                return;
            }
        }
    };
}

// --- Connection tests ---

#[test]
fn connect_and_simple_query() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn connect_wrong_port() {
    let result = Connection::connect(&Config {
        host: "127.0.0.1".into(),
        port: 1, // no server here
        user: "nobody".into(),
        password: "".into(),
        database: "nonexistent".into(),
        ssl: bsql_driver_postgres::SslMode::Disable,
        statement_timeout_secs: 30,
    });

    assert!(result.is_err());
    assert!(matches!(result, Err(DriverError::Io(_))));
}

#[test]
fn connect_wrong_password() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.password = "definitely_wrong_password_12345".into();

    let result = Connection::connect(&config);
    // If PG is configured with `trust` auth, this will succeed — that's fine.
    // We only assert the error type if it fails.
    if let Err(ref e) = result {
        match e {
            DriverError::Auth(_) => {}
            DriverError::Server { .. } => {}
            _ => panic!("expected Auth or Server error, got: {e}"),
        }
    }
}

// --- Simple query tests ---

#[test]
fn simple_query_begin_commit() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("BEGIN").unwrap();
    assert!(conn.is_in_transaction());

    conn.simple_query("COMMIT").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn simple_query_begin_rollback() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("BEGIN").unwrap();
    conn.simple_query("ROLLBACK").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn simple_query_set() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("SET statement_timeout = '5s'").unwrap();
}

// --- Prepared query tests ---

#[test]
fn query_select_int() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::int4 AS val";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&42i32], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.column_name(0), "val");
}

#[test]
fn query_all_base_types() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::bool, $2::int2, $3::int4, $4::int8, $5::float4, $6::float8, $7::text, $8::bytea";
    let hash = hash_sql(sql);
    let bytea_val: &[u8] = &[0xDE, 0xAD];
    let result = conn
        .query(
            sql,
            hash,
            &[
                &true,
                &42i16,
                &12345i32,
                &9876543210i64,
                &3.15f32,
                &2.71f64,
                &"hello",
                &bytea_val,
            ],
            &mut arena,
        )
        .unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_bool(0), Some(true));
    assert_eq!(row.get_i16(1), Some(42));
    assert_eq!(row.get_i32(2), Some(12345));
    assert_eq!(row.get_i64(3), Some(9876543210));
    assert!((row.get_f32(4).unwrap() - 3.15).abs() < 0.001);
    assert!((row.get_f64(5).unwrap() - 2.71).abs() < 1e-9);
    assert_eq!(row.get_str(6), Some("hello"));
    assert_eq!(row.get_bytes(7), Some([0xDE, 0xAD].as_slice()));
}

#[test]
fn query_nullable_columns() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT NULL::int4, NULL::text, 42::int4";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert!(row.is_null(0));
    assert!(row.is_null(1));
    assert!(!row.is_null(2));
    assert_eq!(row.get_i32(0), None);
    assert_eq!(row.get_str(1), None);
    assert_eq!(row.get_i32(2), Some(42));
}

#[test]
fn query_empty_result() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 1 WHERE false";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();
    assert!(result.is_empty());
    assert_eq!(result.len(), 0);
}

#[test]
fn query_multiple_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 100) AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();
    assert_eq!(result.len(), 100);

    for (i, row) in result.rows(&arena).enumerate() {
        assert_eq!(row.get_i32(0), Some((i + 1) as i32));
    }
}

#[test]
fn query_statement_cache_hit() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::int4 + $2::int4 AS sum";
    let hash = hash_sql(sql);

    // First call: Parse+Bind+Execute
    let r1 = conn.query(sql, hash, &[&1i32, &2i32], &mut arena).unwrap();
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(3));

    // Second call: cache hit, only Bind+Execute
    arena.reset();
    let r2 = conn
        .query(sql, hash, &[&10i32, &20i32], &mut arena)
        .unwrap();
    assert_eq!(r2.row(0, &arena).get_i32(0), Some(30));
}

#[test]
fn execute_returns_affected_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // Create a temp table
    conn.simple_query("CREATE TEMP TABLE _driver_test_exec (id int)")
        .unwrap();

    let sql = "INSERT INTO _driver_test_exec VALUES ($1::int4)";
    let hash = hash_sql(sql);
    let affected = conn.execute(sql, hash, &[&1i32]).unwrap();
    assert_eq!(affected, 1);

    let sql2 = "DELETE FROM _driver_test_exec WHERE id = $1::int4";
    let hash2 = hash_sql(sql2);
    let affected = conn.execute(sql2, hash2, &[&1i32]).unwrap();
    assert_eq!(affected, 1);
}

#[test]
fn query_insert_returning() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    conn.simple_query(
        "CREATE TEMP TABLE _driver_test_ret (id serial PRIMARY KEY, name text NOT NULL)",
    )
    .unwrap();

    let sql = "INSERT INTO _driver_test_ret (name) VALUES ($1::text) RETURNING id, name";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&"alice"], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(1)); // serial starts at 1
    assert_eq!(row.get_str(1), Some("alice"));
}

#[test]
fn query_invalid_sql() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECTT INVALID SYNTAX";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena);

    match result {
        Err(DriverError::Server { code, message, .. }) => {
            assert!(!code.is_empty(), "should have a SQLSTATE code");
            assert!(!message.is_empty(), "should have an error message");
        }
        Err(e) => panic!("expected Server error, got: {e}"),
        Ok(_) => panic!("expected error for invalid SQL"),
    }
}

#[test]
fn query_large_text() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    // 1MB text
    let big = "x".repeat(1_000_000);
    let sql = "SELECT $1::text AS big";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&big.as_str()], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    let val = row.get_str(0).unwrap();
    assert_eq!(val.len(), 1_000_000);
    assert!(val.chars().all(|c| c == 'x'));
}

#[test]
fn query_long_sql() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    // Build a very long SQL query (>100KB) using repeated UNION ALL
    let mut sql = String::from("SELECT 1 AS n");
    for i in 2..=500 {
        sql.push_str(&format!(" UNION ALL SELECT {i}"));
    }
    let hash = hash_sql(&sql);
    let result = conn.query(&sql, hash, &[], &mut arena).unwrap();
    assert_eq!(result.len(), 500);
}

// --- Arena tests (with real data) ---

#[test]
fn arena_100_rows_single_chunk() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 100)::int4 AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();
    assert_eq!(result.len(), 100);

    // 100 int4 values = 400 bytes, should fit in initial 8KB chunk
    assert!(arena.allocated() < 8192);
}

#[test]
fn arena_reset_reuse() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 50)::int4";
    let hash = hash_sql(sql);

    // First query — data goes to QueryResult's inline data_buf, not arena.
    let r1 = conn.query(sql, hash, &[], &mut arena).unwrap();
    assert_eq!(r1.len(), 50);

    // Verify rows are accessible (data_buf path works)
    let row = r1.row(0, &arena);
    assert!(row.get_i32(0).is_some());

    // Reset arena (no-op for data_buf queries, but must not crash)
    arena.reset();

    let r2 = conn.query(sql, hash, &[], &mut arena).unwrap();
    assert_eq!(r2.len(), 50);
    let row2 = r2.row(49, &arena);
    assert!(row2.get_i32(0).is_some());
}

// --- Pool tests ---

#[test]
fn pool_acquire_release() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    {
        let mut conn = pool.acquire().unwrap();
        conn.simple_query("SELECT 1").unwrap();
    }
    // conn returned to pool

    // Acquire again — should get the same connection back (LIFO)
    // Give the spawned task a moment to return the connection
    std::thread::sleep(std::time::Duration::from_millis(10));

    let mut conn2 = pool.acquire().unwrap();
    conn2.simple_query("SELECT 2").unwrap();
}

#[test]
fn pool_fail_fast_exhaustion() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(1).build().unwrap();

    let _conn1 = pool.acquire().unwrap();

    // Pool has 1 connection, it's borrowed — next acquire should fail
    let result = pool.acquire();
    assert!(result.is_err());
    match result {
        Err(DriverError::Pool(msg)) => assert!(msg.contains("exhausted")),
        Err(e) => panic!("expected Pool error, got: {e}"),
        Ok(_) => panic!("expected exhaustion error"),
    }
}

// --- Transaction tests ---

#[test]
fn transaction_commit() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let mut tx = pool.begin().unwrap();
    tx.simple_query("CREATE TEMP TABLE _driver_test_tx_commit (val int)")
        .unwrap();
    tx.simple_query("INSERT INTO _driver_test_tx_commit VALUES (1)")
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn transaction_rollback() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut tx = pool.begin().unwrap();
    tx.simple_query("SELECT 1").unwrap();
    tx.rollback().unwrap();
}

#[test]
fn transaction_drop_without_commit() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(2).build().unwrap();

    {
        let mut tx = pool.begin().unwrap();
        tx.simple_query("SELECT 1").unwrap();
        // Drop without commit — connection should be discarded
    }

    // The connection was discarded; open_count was decremented.
    // We should be able to acquire a new connection.
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut conn = pool.acquire().unwrap();
    conn.simple_query("SELECT 1").unwrap();
}

// --- Binary round-trip tests ---

#[test]
fn binary_roundtrip_bool() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::bool AS val";
    let hash = hash_sql(sql);

    let r = conn.query(sql, hash, &[&true], &mut arena).unwrap();
    assert_eq!(r.row(0, &arena).get_bool(0), Some(true));

    arena.reset();
    let r = conn.query(sql, hash, &[&false], &mut arena).unwrap();
    assert_eq!(r.row(0, &arena).get_bool(0), Some(false));
}

#[test]
fn binary_roundtrip_i16() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::int2 AS val";
    let hash = hash_sql(sql);

    for val in [0i16, 1, -1, i16::MIN, i16::MAX] {
        arena.reset();
        let r = conn.query(sql, hash, &[&val], &mut arena).unwrap();
        assert_eq!(r.row(0, &arena).get_i16(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_i32() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::int4 AS val";
    let hash = hash_sql(sql);

    for val in [0i32, 1, -1, i32::MIN, i32::MAX, 42, 1234567] {
        arena.reset();
        let r = conn.query(sql, hash, &[&val], &mut arena).unwrap();
        assert_eq!(r.row(0, &arena).get_i32(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_i64() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::int8 AS val";
    let hash = hash_sql(sql);

    for val in [0i64, 1, -1, i64::MIN, i64::MAX, 9876543210] {
        arena.reset();
        let r = conn.query(sql, hash, &[&val], &mut arena).unwrap();
        assert_eq!(r.row(0, &arena).get_i64(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_f32() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::float4 AS val";
    let hash = hash_sql(sql);

    for val in [0.0f32, 1.0, -1.0, 3.15, f32::MIN, f32::MAX] {
        arena.reset();
        let r = conn.query(sql, hash, &[&val], &mut arena).unwrap();
        let got = r.row(0, &arena).get_f32(0).unwrap();
        assert!((got - val).abs() < f32::EPSILON || got == val);
    }
}

#[test]
fn binary_roundtrip_f64() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::float8 AS val";
    let hash = hash_sql(sql);

    for val in [0.0f64, 1.0, -1.0, std::f64::consts::PI] {
        arena.reset();
        let r = conn.query(sql, hash, &[&val], &mut arena).unwrap();
        let got = r.row(0, &arena).get_f64(0).unwrap();
        assert!((got - val).abs() < f64::EPSILON || got == val);
    }
}

#[test]
fn binary_roundtrip_text() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);

    for val in ["", "hello", "unicode: \u{1F600}", "with\nnewlines\ttabs"] {
        arena.reset();
        let r = conn.query(sql, hash, &[&val], &mut arena).unwrap();
        assert_eq!(r.row(0, &arena).get_str(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_bytea() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::bytea AS val";
    let hash = hash_sql(sql);
    let data: &[u8] = &[0, 1, 2, 255, 128, 64];
    let result = conn.query(sql, hash, &[&data], &mut arena).unwrap();
    assert_eq!(result.row(0, &arena).get_bytes(0), Some(data));
}

#[test]
fn null_handling_all_types() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT NULL::bool, NULL::int2, NULL::int4, NULL::int8, NULL::float4, NULL::float8, NULL::text, NULL::bytea";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    for i in 0..row.column_count() {
        assert!(row.is_null(i), "column {i} should be NULL");
    }
    assert_eq!(row.get_bool(0), None);
    assert_eq!(row.get_i16(1), None);
    assert_eq!(row.get_i32(2), None);
    assert_eq!(row.get_i64(3), None);
    assert_eq!(row.get_f32(4), None);
    assert_eq!(row.get_f64(5), None);
    assert_eq!(row.get_str(6), None);
    assert_eq!(row.get_bytes(7), None);
}

// --- Connection parameter tests ---

#[test]
fn connection_reports_server_version() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();

    let version = conn.parameter("server_version");
    assert!(version.is_some(), "server_version should be reported");
    assert!(!version.unwrap().is_empty());
}

#[test]
fn connection_has_pid() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();
    assert!(conn.pid() > 0);
}

// --- Multiple queries on same connection ---

#[test]
fn multiple_queries_same_connection() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    // Different queries
    let sql1 = "SELECT 1::int4 AS a";
    let sql2 = "SELECT 'hello'::text AS b";
    let sql3 = "SELECT 3.15::float8 AS c";

    let h1 = hash_sql(sql1);
    let h2 = hash_sql(sql2);
    let h3 = hash_sql(sql3);

    let r1 = conn.query(sql1, h1, &[], &mut arena).unwrap();
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(1));

    arena.reset();
    let r2 = conn.query(sql2, h2, &[], &mut arena).unwrap();
    assert_eq!(r2.row(0, &arena).get_str(0), Some("hello"));

    arena.reset();
    let r3 = conn.query(sql3, h3, &[], &mut arena).unwrap();
    let val = r3.row(0, &arena).get_f64(0).unwrap();
    assert!((val - 3.15).abs() < 1e-10);
}

// --- Column metadata ---

#[test]
fn query_result_columns() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 1::int4 AS id, 'test'::text AS name";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();

    let cols = result.columns();
    assert_eq!(cols.len(), 2);
    assert_eq!(&*cols[0].name, "id");
    assert_eq!(cols[0].type_oid, 23); // int4
    assert_eq!(&*cols[1].name, "name");
    assert_eq!(cols[1].type_oid, 25); // text
}

// --- Error handling ---

#[test]
fn error_invalid_sql_has_code() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT * FROM _definitely_nonexistent_table_12345";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena);

    match result {
        Err(DriverError::Server { code, message, .. }) => {
            assert_eq!(&*code, "42P01", "should be undefined_table error");
            assert!(
                message.contains("does not exist"),
                "message should mention nonexistence: {message}"
            );
        }
        Err(e) => panic!("expected Server error, got: {e}"),
        Ok(_) => panic!("expected error for nonexistent table"),
    }

    // Connection should still be usable after error
    arena.reset();
    let sql2 = "SELECT 1::int4";
    let hash2 = hash_sql(sql2);
    let result = conn.query(sql2, hash2, &[], &mut arena).unwrap();
    assert_eq!(result.row(0, &arena).get_i32(0), Some(1));
}

#[test]
fn error_simple_query_reports_server_error() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let result = conn.simple_query("SELECT * FROM _nonexistent_table_xyz");

    match result {
        Err(DriverError::Server { code, .. }) => {
            assert_eq!(&*code, "42P01");
        }
        Err(e) => panic!("expected Server error, got: {e}"),
        Ok(_) => panic!("expected error"),
    }
}

// --- Query with zero columns ---

#[test]
fn query_zero_columns() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // A DO block returns no columns and no rows
    conn.simple_query("DO $$ BEGIN END $$").unwrap();
}

// --- Pool race condition test ---

#[test]
fn pool_concurrent_acquire_race() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(5).build().unwrap();

    // Spawn 20 concurrent tasks all racing to acquire from a pool of 5.
    // With the CAS loop fix, open_count must never exceed max_size.
    let pool = std::sync::Arc::new(pool);
    let mut handles = Vec::new();

    for _ in 0..20 {
        let pool = pool.clone();
        handles.push(std::thread::spawn(move || {
            match pool.acquire() {
                Ok(mut conn) => {
                    let _ = conn.simple_query("SELECT 1");
                    // Hold briefly
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    drop(conn);
                    Ok(())
                }
                Err(DriverError::Pool(_)) => {
                    // Expected when pool is exhausted
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    // open_count must never exceed max_size
    assert!(pool.open_count() <= pool.max_size());
}

// --- Pool LIFO ordering test ---

#[test]
fn pool_lifo_ordering() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(3).build().unwrap();

    // Acquire 3 connections, record their PIDs
    let mut conn1 = pool.acquire().unwrap();
    conn1.simple_query("SELECT 1").unwrap();
    let _pid1 = conn1.pid();

    let mut conn2 = pool.acquire().unwrap();
    conn2.simple_query("SELECT 1").unwrap();
    let pid2 = conn2.pid();

    let mut conn3 = pool.acquire().unwrap();
    conn3.simple_query("SELECT 1").unwrap();
    let pid3 = conn3.pid();

    // Return in order: 1, 2, 3
    drop(conn1);
    drop(conn2);
    drop(conn3);

    // LIFO: next acquire should get conn3 (last returned = top of stack)
    let conn = pool.acquire().unwrap();
    assert_eq!(conn.pid(), pid3);
    drop(conn);

    // Next should get conn3 again (just returned it)
    let conn = pool.acquire().unwrap();
    assert_eq!(conn.pid(), pid3);
    drop(conn);

    // Drain two: should get conn3 then conn2
    let c_a = pool.acquire().unwrap();
    let c_b = pool.acquire().unwrap();
    assert_eq!(c_a.pid(), pid3);
    assert_eq!(c_b.pid(), pid2);
    drop(c_a);
    drop(c_b);
}

// --- Codec edge cases ---

#[test]
fn codec_nan_and_infinity() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    // NaN
    let sql = "SELECT $1::float4 AS f4, $2::float8 AS f8";
    let hash = hash_sql(sql);
    let result = conn
        .query(sql, hash, &[&f32::NAN, &f64::NAN], &mut arena)
        .unwrap();
    let row = result.row(0, &arena);
    assert!(row.get_f32(0).unwrap().is_nan());
    assert!(row.get_f64(1).unwrap().is_nan());

    // Positive infinity
    arena.reset();
    let result = conn
        .query(sql, hash, &[&f32::INFINITY, &f64::INFINITY], &mut arena)
        .unwrap();
    let row = result.row(0, &arena);
    assert!(row.get_f32(0).unwrap().is_infinite());
    assert!(row.get_f64(1).unwrap().is_infinite());

    // Negative infinity
    arena.reset();
    let result = conn
        .query(
            sql,
            hash,
            &[&f32::NEG_INFINITY, &f64::NEG_INFINITY],
            &mut arena,
        )
        .unwrap();
    let row = result.row(0, &arena);
    assert!(row.get_f32(0).unwrap().is_infinite());
    assert!(row.get_f64(1).unwrap().is_infinite());
}

#[test]
fn codec_empty_string_and_max_i64() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    // Empty string
    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);
    let empty = "";
    let result = conn.query(sql, hash, &[&empty], &mut arena).unwrap();
    assert_eq!(result.row(0, &arena).get_str(0), Some(""));

    // Max i64
    arena.reset();
    let sql2 = "SELECT $1::int8 AS val";
    let hash2 = hash_sql(sql2);
    let result = conn.query(sql2, hash2, &[&i64::MAX], &mut arena).unwrap();
    assert_eq!(result.row(0, &arena).get_i64(0), Some(i64::MAX));

    // Min i64
    arena.reset();
    let result = conn.query(sql2, hash2, &[&i64::MIN], &mut arena).unwrap();
    assert_eq!(result.row(0, &arena).get_i64(0), Some(i64::MIN));
}

// --- Config validation tests ---

#[test]
fn config_rejects_empty_host() {
    let result = Config::from_url("postgres://user:pass@/db");
    assert!(result.is_err());
}

#[test]
fn config_rejects_empty_user() {
    let result = Config::from_url("postgres://:pass@localhost/db");
    assert!(result.is_err());
}

#[test]
fn config_url_decodes_host() {
    let cfg = Config::from_url("postgres://user:pass@local%2Dhost/db").unwrap();
    assert_eq!(cfg.host, "local-host");
}

#[test]
fn config_statement_timeout_default() {
    let cfg = Config::from_url("postgres://user:pass@localhost/db").unwrap();
    assert_eq!(cfg.statement_timeout_secs, 30);
}

#[test]
fn config_statement_timeout_custom() {
    let cfg = Config::from_url("postgres://user:pass@localhost/db?statement_timeout=60").unwrap();
    assert_eq!(cfg.statement_timeout_secs, 60);
}

#[test]
fn config_statement_timeout_zero() {
    let cfg = Config::from_url("postgres://user:pass@localhost/db?statement_timeout=0").unwrap();
    assert_eq!(cfg.statement_timeout_secs, 0);
}

// --- NoticeResponse handling ---

#[test]
fn notice_response_does_not_break_query() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // RAISE WARNING produces a NoticeResponse — the query should succeed.
    conn.simple_query("DO $$ BEGIN RAISE WARNING 'test warning from bsql'; END $$")
        .unwrap();

    // Connection should still be usable afterward.
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());
}

// --- Large result set ---

#[test]
fn query_100k_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 100000)::int4 AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();

    assert_eq!(result.len(), 100_000);
    // Spot-check first and last rows
    assert_eq!(result.row(0, &arena).get_i32(0), Some(1));
    assert_eq!(result.row(99_999, &arena).get_i32(0), Some(100_000));
}

// --- Wide query (many columns) ---

#[test]
fn query_wide_50_columns() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    // Build SELECT 1 AS c01, 2 AS c02, ... , 50 AS c50
    let mut sql = String::with_capacity(512);
    sql.push_str("SELECT ");
    for i in 1..=50 {
        if i > 1 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("{i}::int4 AS c{i:02}"));
    }
    let hash = hash_sql(&sql);
    let result = conn.query(&sql, hash, &[], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.column_count(), 50);
    for i in 0..50 {
        assert_eq!(row.get_i32(i), Some((i + 1) as i32));
    }
}

// --- Unicode column name ---

#[test]
fn query_unicode_column_name() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 1 AS \"colonn\u{00e9}\u{00e9}\"";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.column_name(0), "colonn\u{00e9}\u{00e9}");
    assert_eq!(row.get_i32(0), Some(1));
}

// --- True streaming tests ---

#[test]
fn streaming_1000_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 1000) AS n";
    let hash = hash_sql(sql);

    let (columns, _) = conn.query_streaming_start(sql, hash, &[], 64).unwrap();
    assert_eq!(columns.len(), 1);

    let mut total_rows = 0;
    let mut all_values = Vec::new();
    let mut first_chunk = true;

    loop {
        let num_cols = columns.len();
        let mut col_offsets: Vec<(usize, i32)> = Vec::new();

        if !first_chunk {
            conn.streaming_send_execute(64).unwrap();
        }
        first_chunk = false;

        let more = conn
            .streaming_next_chunk(&mut arena, &mut col_offsets)
            .unwrap();

        let row_count = col_offsets.len().checked_div(num_cols).unwrap_or(0);

        for i in 0..row_count {
            let (offset, len) = col_offsets[i * num_cols];
            if len >= 0 {
                let data = arena.get(offset, len as usize);
                let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                all_values.push(val);
            }
        }

        total_rows += row_count;

        if !more {
            break;
        }
        arena.reset();
    }

    assert_eq!(total_rows, 1000);
    assert_eq!(all_values.len(), 1000);
    // Verify values are 1..=1000
    for (i, &val) in all_values.iter().enumerate() {
        assert_eq!(val, (i + 1) as i32, "mismatch at index {i}");
    }
}

#[test]
fn streaming_chunk_boundary_exact() {
    // 64 rows exactly — should get one chunk with PortalSuspended, then a
    // second empty chunk with CommandComplete.
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 64) AS n";
    let hash = hash_sql(sql);

    let (columns, _) = conn.query_streaming_start(sql, hash, &[], 64).unwrap();

    let num_cols = columns.len();
    let mut col_offsets: Vec<(usize, i32)> = Vec::new();
    let more = conn
        .streaming_next_chunk(&mut arena, &mut col_offsets)
        .unwrap();

    let first_chunk_rows = col_offsets.len() / num_cols;

    if more {
        // PG may return 64 rows + PortalSuspended. Next chunk should be empty + CommandComplete.
        arena.reset();
        col_offsets.clear();
        conn.streaming_send_execute(64).unwrap();
        let more2 = conn
            .streaming_next_chunk(&mut arena, &mut col_offsets)
            .unwrap();
        let second_chunk_rows = if num_cols > 0 && !col_offsets.is_empty() {
            col_offsets.len() / num_cols
        } else {
            0
        };
        assert!(!more2, "should be done after second chunk");
        assert_eq!(first_chunk_rows + second_chunk_rows, 64);
    } else {
        // PG returned all 64 in one chunk with CommandComplete
        assert_eq!(first_chunk_rows, 64);
    }
}

#[test]
fn streaming_zero_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 1 AS n WHERE false";
    let hash = hash_sql(sql);

    let (columns, _) = conn.query_streaming_start(sql, hash, &[], 64).unwrap();

    let num_cols = columns.len();
    let mut col_offsets: Vec<(usize, i32)> = Vec::new();
    let more = conn
        .streaming_next_chunk(&mut arena, &mut col_offsets)
        .unwrap();

    assert!(!more, "zero-row query should not have more chunks");
    let rows = if num_cols > 0 && !col_offsets.is_empty() {
        col_offsets.len() / num_cols
    } else {
        0
    };
    assert_eq!(rows, 0);
}

#[test]
fn streaming_single_row() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 42::int4 AS n";
    let hash = hash_sql(sql);

    let (columns, _) = conn.query_streaming_start(sql, hash, &[], 64).unwrap();

    let num_cols = columns.len();
    let mut col_offsets: Vec<(usize, i32)> = Vec::new();
    let more = conn
        .streaming_next_chunk(&mut arena, &mut col_offsets)
        .unwrap();

    assert!(!more, "single-row query should not have more chunks");
    let rows = col_offsets.len() / num_cols;
    assert_eq!(rows, 1);

    let (offset, len) = col_offsets[0];
    assert_eq!(len, 4);
    let data = arena.get(offset, len as usize);
    let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    assert_eq!(val, 42);
}

#[test]
fn streaming_early_drop() {
    // Consume only the first chunk, then drop. The connection should remain
    // usable (protocol state is clean after ReadyForQuery).
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut guard = pool.acquire().unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 200) AS n";
    let hash = hash_sql(sql);

    let (_, _) = guard.query_streaming_start(sql, hash, &[], 64).unwrap();

    let mut col_offsets: Vec<(usize, i32)> = Vec::new();
    let more = guard
        .streaming_next_chunk(&mut arena, &mut col_offsets)
        .unwrap();
    assert!(more, "200 rows with chunk_size=64 should have more");

    // Drop guard WITHOUT consuming remaining chunks. This returns the
    // connection to the pool.
    drop(guard);

    // Acquire again — the connection should be reusable.
    let mut guard2 = pool.acquire().unwrap();
    // The unnamed portal is auto-cleaned on next Bind. Run a normal query.
    arena.reset();
    let sql2 = "SELECT 99::int4 AS n";
    let hash2 = hash_sql(sql2);
    let result = guard2.query(sql2, hash2, &[], &mut arena).unwrap();
    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(99));
}

// --- SIMD UTF-8 validation tests ---

#[test]
fn simd_utf8_text_column() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);
    let text = "Hello, world! Rust + PG";
    let result = conn.query(sql, hash, &[&text], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_str(0), Some("Hello, world! Rust + PG"));
}

#[test]
fn simd_utf8_multibyte() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);
    // Japanese, emoji, accented Latin — exercises multi-byte UTF-8 paths
    let text = "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}\u{4e16}\u{754c} \u{1f600} caf\u{00e9}";
    let result = conn.query(sql, hash, &[&text], &mut arena).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_str(0), Some(text));
}

#[test]
fn simd_utf8_rejects_invalid() {
    use bsql_driver_postgres::codec::decode_str;
    assert!(decode_str(&[0xFF, 0xFE]).is_err());
    assert!(decode_str(&[0xC0, 0xAF]).is_err()); // overlong encoding
    assert!(decode_str(&[0xED, 0xA0, 0x80]).is_err()); // surrogate half
}

#[test]
fn simd_utf8_accepts_valid() {
    use bsql_driver_postgres::codec::decode_str;
    assert_eq!(decode_str(b"hello").unwrap(), "hello");
    assert_eq!(decode_str(b"").unwrap(), "");
    assert_eq!(decode_str("\u{1f600}".as_bytes()).unwrap(), "\u{1f600}");
}

// ---------------------------------------------------------------------------
// Deferred pipeline (defer_execute / flush_deferred)
// ---------------------------------------------------------------------------

#[test]
fn defer_execute_commit_auto_flushes() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();
    for i in 0..5i32 {
        let login = format!("defer_commit_{i}");
        let first_name = format!("first_commit_{i}");
        let last_name = "test".to_string();
        let email = format!("{}@test.com", login);
        tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
            .unwrap();
    }
    assert_eq!(tx.deferred_count(), 5);
    tx.commit().unwrap();

    // Verify all 5 rows were inserted
    let mut conn = pool.acquire().unwrap();
    let mut arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login LIKE 'defer_commit_%'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[], &mut arena).unwrap();
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(5));

    // Clean up
    conn.simple_query("DELETE FROM users WHERE login LIKE 'defer_commit_%'")
        .unwrap();
}

#[test]
fn defer_execute_flush_returns_affected_rows() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();
    for i in 0..3i32 {
        let login = format!("defer_flush_{i}");
        let first_name = format!("first_flush_{i}");
        let last_name = "test".to_string();
        let email = format!("{}@test.com", login);
        tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
            .unwrap();
    }

    let results = tx.flush_deferred().unwrap();
    assert_eq!(results.len(), 3);
    for &r in &results {
        assert_eq!(r, 1); // each INSERT affects 1 row
    }
    assert_eq!(tx.deferred_count(), 0);

    tx.commit().unwrap();

    // Clean up
    let mut conn = pool.acquire().unwrap();
    conn.simple_query("DELETE FROM users WHERE login LIKE 'defer_flush_%'")
        .unwrap();
}

#[test]
fn defer_execute_auto_flushes_before_query() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();

    let login = "defer_before_query".to_string();
    let first_name = "first_before_query".to_string();
    let last_name = "test".to_string();
    let email = format!("{}@test.com", login);
    tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
        .unwrap();
    assert_eq!(tx.deferred_count(), 1);

    // Query should auto-flush the deferred insert first
    let mut arena = Arena::new();
    let q_sql = "SELECT count(*)::int4 AS c FROM users WHERE login = 'defer_before_query'";
    let q_hash = hash_sql(q_sql);
    let result = tx.query(q_sql, q_hash, &[], &mut arena).unwrap();
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(1));
    assert_eq!(tx.deferred_count(), 0);

    tx.rollback().unwrap();
}

#[test]
fn defer_execute_empty_commit_is_noop() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    // No deferred operations — commit should succeed without pipeline flush
    let tx = pool.begin().unwrap();
    assert_eq!(tx.deferred_count(), 0);
    tx.commit().unwrap();
}

#[test]
fn defer_execute_100_inserts() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();
    for i in 0..100i32 {
        let login = format!("defer_100_{i}");
        let first_name = format!("first_100_{i}");
        let last_name = "test".to_string();
        let email = format!("{}@test.com", login);
        tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
            .unwrap();
    }
    assert_eq!(tx.deferred_count(), 100);
    tx.commit().unwrap();

    // Verify all 100 rows
    let mut conn = pool.acquire().unwrap();
    let mut arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login LIKE 'defer_100_%'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[], &mut arena).unwrap();
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(100));

    // Clean up
    conn.simple_query("DELETE FROM users WHERE login LIKE 'defer_100_%'")
        .unwrap();
}

#[test]
fn defer_execute_mixed_with_regular_execute() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();

    // Deferred
    let login = "defer_mixed_d1".to_string();
    let first_name = "first_mixed_d1".to_string();
    let last_name = "test".to_string();
    let email = format!("{}@test.com", login);
    tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
        .unwrap();

    // Regular execute (does NOT flush deferred)
    let login2 = "defer_mixed_r1".to_string();
    let first_name2 = "first_mixed_r1".to_string();
    let last_name2 = "test".to_string();
    let email2 = format!("{}@test.com", login2);
    let affected = tx
        .execute(sql, hash, &[&login2, &first_name2, &last_name2, &email2])
        .unwrap();
    assert_eq!(affected, 1);

    // Another deferred
    let login3 = "defer_mixed_d2".to_string();
    let first_name3 = "first_mixed_d2".to_string();
    let last_name3 = "test".to_string();
    let email3 = format!("{}@test.com", login3);
    tx.defer_execute(sql, hash, &[&login3, &first_name3, &last_name3, &email3])
        .unwrap();
    assert_eq!(tx.deferred_count(), 2);

    tx.commit().unwrap();

    // All 3 rows should exist
    let mut conn = pool.acquire().unwrap();
    let mut arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login LIKE 'defer_mixed_%'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[], &mut arena).unwrap();
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(3));

    conn.simple_query("DELETE FROM users WHERE login LIKE 'defer_mixed_%'")
        .unwrap();
}

#[test]
fn defer_execute_rollback_discards_deferred() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();
    let login = "defer_rollback".to_string();
    let first_name = "first_rollback".to_string();
    let last_name = "test".to_string();
    let email = format!("{}@test.com", login);
    tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
        .unwrap();
    assert_eq!(tx.deferred_count(), 1);

    // Rollback discards deferred ops without sending them
    tx.rollback().unwrap();

    // Verify nothing was inserted
    let mut conn = pool.acquire().unwrap();
    let mut arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login = 'defer_rollback'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[], &mut arena).unwrap();
    let row = result.row(0, &arena);
    assert_eq!(row.get_i32(0), Some(0));
}

#[test]
fn defer_execute_auto_flushes_before_for_each() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();
    let login = "defer_before_foreach".to_string();
    let first_name = "first_before_foreach".to_string();
    let last_name = "test".to_string();
    let email = format!("{}@test.com", login);
    tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
        .unwrap();

    // for_each should auto-flush first
    let q_sql = "SELECT login FROM users WHERE login = 'defer_before_foreach'";
    let q_hash = hash_sql(q_sql);
    let mut found = false;
    tx.for_each(q_sql, q_hash, &[], |_row| {
        found = true;
        Ok(())
    })
    .unwrap();
    assert!(found, "for_each should see the deferred insert");

    tx.rollback().unwrap();
}

#[test]
fn defer_execute_auto_flushes_before_simple_query() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();
    let login = "defer_before_simple".to_string();
    let first_name = "first_before_simple".to_string();
    let last_name = "test".to_string();
    let email = format!("{}@test.com", login);
    tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
        .unwrap();
    assert_eq!(tx.deferred_count(), 1);

    // simple_query should auto-flush first
    tx.simple_query("SELECT 1").unwrap();
    assert_eq!(tx.deferred_count(), 0);

    tx.rollback().unwrap();
}

#[test]
fn defer_execute_param_count_exceeds_max() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let mut tx = pool.begin().unwrap();

    // Build a param list that exceeds i16::MAX
    let too_many: Vec<&(dyn bsql_driver_postgres::Encode + Sync)> =
        vec![&1i32 as &(dyn bsql_driver_postgres::Encode + Sync); 32768];
    let result = tx.defer_execute("SELECT 1", hash_sql("SELECT 1"), &too_many);
    assert!(result.is_err());
    match result.unwrap_err() {
        DriverError::Protocol(msg) => {
            assert!(msg.contains("parameter count"), "msg: {msg}");
        }
        other => panic!("expected Protocol error, got: {other:?}"),
    }

    tx.rollback().unwrap();
}
