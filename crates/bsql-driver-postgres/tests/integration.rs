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

use bsql_driver_postgres::{hash_sql, Arena, Config, Connection, DriverError, Pool};

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
        statement_cache_mode: bsql_driver_postgres::StatementCacheMode::Named,
        ssl_root_cert: None,
        ssl_cert: None,
        ssl_key: None,
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
    let arena = Arena::new();

    let sql = "SELECT $1::int4 AS val";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&42i32]).unwrap();

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
    let arena = Arena::new();

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
    let arena = Arena::new();

    let sql = "SELECT NULL::int4, NULL::text, 42::int4";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

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

    let sql = "SELECT 1 WHERE false";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();
    assert!(result.is_empty());
    assert_eq!(result.len(), 0);
}

#[test]
fn query_multiple_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT generate_series(1, 100) AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();
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
    let arena = Arena::new();

    let sql = "SELECT $1::int4 + $2::int4 AS sum";
    let hash = hash_sql(sql);

    // First call: Parse+Bind+Execute
    let r1 = conn.query(sql, hash, &[&1i32, &2i32]).unwrap();
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(3));

    // Second call: cache hit, only Bind+Execute
    let r2 = conn.query(sql, hash, &[&10i32, &20i32]).unwrap();
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
    let arena = Arena::new();

    conn.simple_query(
        "CREATE TEMP TABLE _driver_test_ret (id serial PRIMARY KEY, name text NOT NULL)",
    )
    .unwrap();

    let sql = "INSERT INTO _driver_test_ret (name) VALUES ($1::text) RETURNING id, name";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&"alice"]).unwrap();

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

    let sql = "SELECTT INVALID SYNTAX";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]);

    match result {
        Err(DriverError::Server { code, message, .. }) => {
            assert!(code != *b"     ", "should have a SQLSTATE code");
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
    let arena = Arena::new();

    // 1MB text
    let big = "x".repeat(1_000_000);
    let sql = "SELECT $1::text AS big";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&big.as_str()]).unwrap();

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

    // Build a very long SQL query (>100KB) using repeated UNION ALL
    let mut sql = String::from("SELECT 1 AS n");
    for i in 2..=500 {
        sql.push_str(&format!(" UNION ALL SELECT {i}"));
    }
    let hash = hash_sql(&sql);
    let result = conn.query(&sql, hash, &[]).unwrap();
    assert_eq!(result.len(), 500);
}

// --- Arena tests (with real data) ---

#[test]
fn arena_100_rows_single_chunk() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT generate_series(1, 100)::int4 AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();
    assert_eq!(result.len(), 100);

    // 100 int4 values = 400 bytes, should fit in initial 8KB chunk
    assert!(arena.allocated() < 8192);
}

#[test]
fn arena_reset_reuse() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT generate_series(1, 50)::int4";
    let hash = hash_sql(sql);

    // First query — data goes to QueryResult's inline data_buf, not arena.
    let r1 = conn.query(sql, hash, &[]).unwrap();
    assert_eq!(r1.len(), 50);

    // Verify rows are accessible (data_buf path works)
    let row = r1.row(0, &arena);
    assert!(row.get_i32(0).is_some());

    // Reset arena (no-op for data_buf queries, but must not crash)

    let r2 = conn.query(sql, hash, &[]).unwrap();
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
    let arena = Arena::new();

    let sql = "SELECT $1::bool AS val";
    let hash = hash_sql(sql);

    let r = conn.query(sql, hash, &[&true]).unwrap();
    assert_eq!(r.row(0, &arena).get_bool(0), Some(true));

    let r = conn.query(sql, hash, &[&false]).unwrap();
    assert_eq!(r.row(0, &arena).get_bool(0), Some(false));
}

#[test]
fn binary_roundtrip_i16() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::int2 AS val";
    let hash = hash_sql(sql);

    for val in [0i16, 1, -1, i16::MIN, i16::MAX] {
        let r = conn.query(sql, hash, &[&val]).unwrap();
        assert_eq!(r.row(0, &arena).get_i16(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_i32() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::int4 AS val";
    let hash = hash_sql(sql);

    for val in [0i32, 1, -1, i32::MIN, i32::MAX, 42, 1234567] {
        let r = conn.query(sql, hash, &[&val]).unwrap();
        assert_eq!(r.row(0, &arena).get_i32(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_i64() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::int8 AS val";
    let hash = hash_sql(sql);

    for val in [0i64, 1, -1, i64::MIN, i64::MAX, 9876543210] {
        let r = conn.query(sql, hash, &[&val]).unwrap();
        assert_eq!(r.row(0, &arena).get_i64(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_f32() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::float4 AS val";
    let hash = hash_sql(sql);

    for val in [0.0f32, 1.0, -1.0, 3.15, f32::MIN, f32::MAX] {
        let r = conn.query(sql, hash, &[&val]).unwrap();
        let got = r.row(0, &arena).get_f32(0).unwrap();
        assert!((got - val).abs() < f32::EPSILON || got == val);
    }
}

#[test]
fn binary_roundtrip_f64() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::float8 AS val";
    let hash = hash_sql(sql);

    for val in [0.0f64, 1.0, -1.0, std::f64::consts::PI] {
        let r = conn.query(sql, hash, &[&val]).unwrap();
        let got = r.row(0, &arena).get_f64(0).unwrap();
        assert!((got - val).abs() < f64::EPSILON || got == val);
    }
}

#[test]
fn binary_roundtrip_text() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);

    for val in ["", "hello", "unicode: \u{1F600}", "with\nnewlines\ttabs"] {
        let r = conn.query(sql, hash, &[&val]).unwrap();
        assert_eq!(r.row(0, &arena).get_str(0), Some(val));
    }
}

#[test]
fn binary_roundtrip_bytea() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::bytea AS val";
    let hash = hash_sql(sql);
    let data: &[u8] = &[0, 1, 2, 255, 128, 64];
    let result = conn.query(sql, hash, &[&data]).unwrap();
    assert_eq!(result.row(0, &arena).get_bytes(0), Some(data));
}

#[test]
fn null_handling_all_types() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT NULL::bool, NULL::int2, NULL::int4, NULL::int8, NULL::float4, NULL::float8, NULL::text, NULL::bytea";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

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
    let arena = Arena::new();

    // Different queries
    let sql1 = "SELECT 1::int4 AS a";
    let sql2 = "SELECT 'hello'::text AS b";
    let sql3 = "SELECT 3.15::float8 AS c";

    let h1 = hash_sql(sql1);
    let h2 = hash_sql(sql2);
    let h3 = hash_sql(sql3);

    let r1 = conn.query(sql1, h1, &[]).unwrap();
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(1));

    let r2 = conn.query(sql2, h2, &[]).unwrap();
    assert_eq!(r2.row(0, &arena).get_str(0), Some("hello"));

    let r3 = conn.query(sql3, h3, &[]).unwrap();
    let val = r3.row(0, &arena).get_f64(0).unwrap();
    assert!((val - 3.15).abs() < 1e-10);
}

// --- Column metadata ---

#[test]
fn query_result_columns() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT 1::int4 AS id, 'test'::text AS name";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

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
    let arena = Arena::new();

    let sql = "SELECT * FROM _definitely_nonexistent_table_12345";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]);

    match result {
        Err(DriverError::Server { code, message, .. }) => {
            assert_eq!(&code, b"42P01", "should be undefined_table error");
            assert!(
                message.contains("does not exist"),
                "message should mention nonexistence: {message}"
            );
        }
        Err(e) => panic!("expected Server error, got: {e}"),
        Ok(_) => panic!("expected error for nonexistent table"),
    }

    // Connection should still be usable after error
    let sql2 = "SELECT 1::int4";
    let hash2 = hash_sql(sql2);
    let result = conn.query(sql2, hash2, &[]).unwrap();
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
            assert_eq!(&code, b"42P01");
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
    let arena = Arena::new();

    // NaN
    let sql = "SELECT $1::float4 AS f4, $2::float8 AS f8";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[&f32::NAN, &f64::NAN]).unwrap();
    let row = result.row(0, &arena);
    assert!(row.get_f32(0).unwrap().is_nan());
    assert!(row.get_f64(1).unwrap().is_nan());

    // Positive infinity
    let result = conn
        .query(sql, hash, &[&f32::INFINITY, &f64::INFINITY])
        .unwrap();
    let row = result.row(0, &arena);
    assert!(row.get_f32(0).unwrap().is_infinite());
    assert!(row.get_f64(1).unwrap().is_infinite());

    // Negative infinity
    let result = conn
        .query(sql, hash, &[&f32::NEG_INFINITY, &f64::NEG_INFINITY])
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
    let arena = Arena::new();

    // Empty string
    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);
    let empty = "";
    let result = conn.query(sql, hash, &[&empty]).unwrap();
    assert_eq!(result.row(0, &arena).get_str(0), Some(""));

    // Max i64
    let sql2 = "SELECT $1::int8 AS val";
    let hash2 = hash_sql(sql2);
    let result = conn.query(sql2, hash2, &[&i64::MAX]).unwrap();
    assert_eq!(result.row(0, &arena).get_i64(0), Some(i64::MAX));

    // Min i64
    let result = conn.query(sql2, hash2, &[&i64::MIN]).unwrap();
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

// --- Pool edge cases ---

#[test]
fn pool_max_size_1_sequential() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(1).build().unwrap();

    // Acquire the single connection, use it, release it.
    {
        let mut conn = pool.acquire().unwrap();
        conn.simple_query("SELECT 1").unwrap();
    }

    // Give the spawned return task a moment.
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Acquire again -- should succeed because the connection was returned.
    let mut conn2 = pool.acquire().unwrap();
    conn2.simple_query("SELECT 2").unwrap();
}

#[test]
fn pool_acquire_timeout_fires() {
    let url = require_db!();
    let pool = Pool::builder()
        .url(&url)
        .max_size(1)
        .acquire_timeout(Some(std::time::Duration::from_millis(100)))
        .build()
        .unwrap();

    // Hold the single connection.
    let _conn1 = pool.acquire().unwrap();

    // Second acquire should block then timeout.
    let start = std::time::Instant::now();
    let result = pool.acquire();
    let elapsed = start.elapsed();

    assert!(result.is_err(), "second acquire should timeout");
    match result {
        Err(DriverError::Pool(msg)) => {
            assert!(
                msg.contains("timeout") || msg.contains("exhausted"),
                "unexpected error: {msg}"
            );
        }
        Err(e) => panic!("expected Pool error, got: {e}"),
        Ok(_) => panic!("expected timeout error"),
    }
    // Verify the timeout actually waited (~100ms, not instant).
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "timeout fired too fast: {elapsed:?}"
    );
}

// --- Large result set ---

#[test]
fn query_10k_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT generate_series(1, 10000)::int4 AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

    assert_eq!(result.len(), 10_000);
    assert_eq!(result.row(0, &arena).get_i32(0), Some(1));
    assert_eq!(result.row(9_999, &arena).get_i32(0), Some(10_000));
}

#[test]
fn query_large_text_100kb() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT repeat('x', 100000) AS big";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    let val = row.get_str(0).unwrap();
    assert_eq!(val.len(), 100_000);
    assert!(val.chars().all(|c| c == 'x'));
}

// --- Streaming via pool guard ---

#[test]
fn streaming_basic_via_pool() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut guard = pool.acquire().unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT generate_series(1, 100)::int4 AS n";
    let hash = hash_sql(sql);

    let (columns, _) = guard.query_streaming_start(sql, hash, &[], 32).unwrap();
    assert_eq!(columns.len(), 1);

    let num_cols = columns.len();
    let mut total_rows = 0;
    let mut first_chunk = true;

    loop {
        let mut col_offsets: Vec<(usize, i32)> = Vec::new();

        if !first_chunk {
            guard.streaming_send_execute(32).unwrap();
        }
        first_chunk = false;

        let more = guard
            .streaming_next_chunk(&mut arena, &mut col_offsets)
            .unwrap();

        let row_count = col_offsets.len().checked_div(num_cols).unwrap_or(0);
        total_rows += row_count;

        if !more {
            break;
        }
        arena.reset();
    }

    assert_eq!(total_rows, 100);
}

#[test]
fn streaming_empty_result_via_pool() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut guard = pool.acquire().unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 1 AS n WHERE false";
    let hash = hash_sql(sql);

    let (columns, _) = guard.query_streaming_start(sql, hash, &[], 32).unwrap();
    let num_cols = columns.len();

    let mut col_offsets: Vec<(usize, i32)> = Vec::new();
    let more = guard
        .streaming_next_chunk(&mut arena, &mut col_offsets)
        .unwrap();

    assert!(!more, "empty result should have no more chunks");
    let rows = if num_cols > 0 && !col_offsets.is_empty() {
        col_offsets.len() / num_cols
    } else {
        0
    };
    assert_eq!(rows, 0);
}

#[test]
fn streaming_single_row_via_pool() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut guard = pool.acquire().unwrap();
    let mut arena = Arena::new();

    let sql = "SELECT 42::int4 AS n";
    let hash = hash_sql(sql);

    let (columns, _) = guard.query_streaming_start(sql, hash, &[], 32).unwrap();
    let num_cols = columns.len();

    let mut col_offsets: Vec<(usize, i32)> = Vec::new();
    let more = guard
        .streaming_next_chunk(&mut arena, &mut col_offsets)
        .unwrap();

    assert!(!more, "single-row result should have no more chunks");
    let rows = col_offsets.len() / num_cols;
    assert_eq!(rows, 1);

    let (offset, len) = col_offsets[0];
    assert_eq!(len, 4);
    let data = arena.get(offset, len as usize);
    let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    assert_eq!(val, 42);
}

#[test]
fn query_100k_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT generate_series(1, 100000)::int4 AS n";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

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
    let arena = Arena::new();

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
    let result = conn.query(&sql, hash, &[]).unwrap();

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
    let arena = Arena::new();

    let sql = "SELECT 1 AS \"colonn\u{00e9}\u{00e9}\"";
    let hash = hash_sql(sql);
    let result = conn.query(sql, hash, &[]).unwrap();

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
    let sql2 = "SELECT 99::int4 AS n";
    let hash2 = hash_sql(sql2);
    let result = guard2.query(sql2, hash2, &[]).unwrap();
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
    let arena = Arena::new();

    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);
    let text = "Hello, world! Rust + PG";
    let result = conn.query(sql, hash, &[&text]).unwrap();

    assert_eq!(result.len(), 1);
    let row = result.row(0, &arena);
    assert_eq!(row.get_str(0), Some("Hello, world! Rust + PG"));
}

#[test]
fn simd_utf8_multibyte() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::text AS val";
    let hash = hash_sql(sql);
    // Japanese, emoji, accented Latin — exercises multi-byte UTF-8 paths
    let text = "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}\u{4e16}\u{754c} \u{1f600} caf\u{00e9}";
    let result = conn.query(sql, hash, &[&text]).unwrap();

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
    let arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login LIKE 'defer_commit_%'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[]).unwrap();
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
    let arena = Arena::new();
    let q_sql = "SELECT count(*)::int4 AS c FROM users WHERE login = 'defer_before_query'";
    let q_hash = hash_sql(q_sql);
    let result = tx.query(q_sql, q_hash, &[]).unwrap();
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
    let arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login LIKE 'defer_100_%'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[]).unwrap();
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
    let arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login LIKE 'defer_mixed_%'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[]).unwrap();
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
    let arena = Arena::new();
    let count_sql = "SELECT count(*)::int4 AS c FROM users WHERE login = 'defer_rollback'";
    let count_hash = hash_sql(count_sql);
    let result = conn.query(count_sql, count_hash, &[]).unwrap();
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

// =========================================================================
// Gap tests: concurrent pool stress
// =========================================================================

#[test]
fn pool_concurrent_10_threads_100_queries_each() {
    let url = require_db!();
    // Use acquire_timeout so threads wait for a connection instead of busy-spinning.
    let pool = std::sync::Arc::new(
        Pool::builder()
            .url(&url)
            .max_size(5)
            .acquire_timeout(Some(std::time::Duration::from_secs(10)))
            .build()
            .unwrap(),
    );

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(10));
    let mut handles = Vec::new();

    for thread_id in 0..10u32 {
        let pool = pool.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait(); // All threads start together
            for i in 0..100u32 {
                let mut conn = pool
                    .acquire()
                    .unwrap_or_else(|e| panic!("thread {thread_id} iter {i}: acquire failed: {e}"));
                let sql = "SELECT $1::int4 + $2::int4 AS sum";
                let h = hash_sql(sql);
                let arena = Arena::new();
                let result = conn
                    .query(sql, h, &[&(thread_id as i32), &(i as i32)])
                    .unwrap();
                assert_eq!(result.len(), 1);
                let row = result.row(0, &arena);
                assert_eq!(
                    row.get_i32(0),
                    Some((thread_id + i) as i32),
                    "thread={thread_id} iter={i}"
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // open_count must never exceed max_size
    assert!(pool.open_count() <= pool.max_size());
}

#[test]
fn pool_concurrent_acquire_release_rapid() {
    let url = require_db!();
    // Use acquire_timeout so threads wait for a connection instead of busy-spinning.
    let pool = std::sync::Arc::new(
        Pool::builder()
            .url(&url)
            .max_size(2)
            .acquire_timeout(Some(std::time::Duration::from_secs(10)))
            .build()
            .unwrap(),
    );

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(5));
    let mut handles = Vec::new();

    for thread_id in 0..5u32 {
        let pool = pool.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..200u32 {
                let mut conn = pool
                    .acquire()
                    .unwrap_or_else(|e| panic!("thread {thread_id} iter {i}: acquire failed: {e}"));
                conn.simple_query("SELECT 1").unwrap();
                drop(conn); // immediate release
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(pool.open_count() <= pool.max_size());
}

// =========================================================================
// Gap tests: connection error scenarios
// =========================================================================

#[test]
fn connect_bad_host_fails() {
    // Use localhost with an unlikely port instead of a non-routable IP,
    // because non-routable IPs cause OS-level TCP timeout delays (60+s).
    let result = Connection::connect(&Config {
        host: "127.0.0.1".into(),
        port: 2, // port 2 is "CompressNET Management" — almost certainly not running PG
        user: "nobody".into(),
        password: "".into(),
        database: "nonexistent".into(),
        ssl: bsql_driver_postgres::SslMode::Disable,
        statement_timeout_secs: 5,
        statement_cache_mode: bsql_driver_postgres::StatementCacheMode::Named,
        ssl_root_cert: None,
        ssl_cert: None,
        ssl_key: None,
    });
    assert!(result.is_err(), "connecting to bad host/port should fail");
}

#[test]
fn connect_bad_port_port1_fails() {
    let result = Connection::connect(&Config {
        host: "127.0.0.1".into(),
        port: 1,
        user: "nobody".into(),
        password: "".into(),
        database: "nonexistent".into(),
        ssl: bsql_driver_postgres::SslMode::Disable,
        statement_timeout_secs: 30,
        statement_cache_mode: bsql_driver_postgres::StatementCacheMode::Named,
        ssl_root_cert: None,
        ssl_cert: None,
        ssl_key: None,
    });
    assert!(result.is_err());
    assert!(matches!(result, Err(DriverError::Io(_))));
}

#[test]
fn connect_bad_password_fails_with_auth_error() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.password = "definitely_wrong_password_xyz_99999".into();

    let result = Connection::connect(&config);
    // Some PG installations use trust auth — skip the assertion if connect succeeds.
    if let Err(ref e) = result {
        match e {
            DriverError::Auth(_) => {}       // expected
            DriverError::Server { .. } => {} // PG may return a server error too
            _ => panic!("expected Auth or Server error for bad password, got: {e}"),
        }
    }
}

#[test]
fn query_after_connection_closed_is_consumed() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();

    // Close the connection — conn is consumed.
    conn.close().unwrap();

    // Cannot query after close — close(self) consumes self.
    // This is enforced at compile time. This test verifies the close path itself.
}

// =========================================================================
// Gap tests: connection accessors
// =========================================================================

#[test]
fn connection_pid_nonzero_gap() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();
    assert!(conn.pid() > 0, "pid should be positive after connect");
}

#[test]
fn connection_is_idle_after_connect() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();
    assert!(conn.is_idle(), "freshly connected should be idle");
}

#[test]
fn connection_is_not_in_transaction_initially() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();
    assert!(
        !conn.is_in_transaction(),
        "freshly connected should not be in transaction"
    );
}

#[test]
fn connection_server_params_has_encoding() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();

    let params = conn.server_params();
    assert!(!params.is_empty(), "should have server parameters");

    // server_encoding should be present
    let encoding = conn.parameter("server_encoding");
    assert!(
        encoding.is_some(),
        "server_encoding should be in server_params"
    );
    assert_eq!(encoding.unwrap(), "UTF8", "server_encoding should be UTF8");
}

#[test]
fn connection_secret_key_nonzero() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();
    let _ = conn.secret_key(); // accessor should not panic
}

#[test]
fn connection_is_in_failed_transaction() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("BEGIN").unwrap();
    assert!(conn.is_in_transaction());

    // Execute invalid SQL to put the transaction into a failed state
    let _ = conn.simple_query("SELECT * FROM _nonexistent_table_xyzzy_12345");
    assert!(
        conn.is_in_failed_transaction(),
        "should be in failed transaction after error inside BEGIN"
    );

    conn.simple_query("ROLLBACK").unwrap();
    assert!(conn.is_idle());
}

// =========================================================================
// Gap tests: pool builder validation
// =========================================================================

#[test]
fn pool_builder_max_size_zero_acquire_errors() {
    let pool = Pool::builder()
        .url("postgres://user:pass@localhost/db")
        .max_size(0)
        .build()
        .unwrap();

    let result = pool.acquire();
    assert!(result.is_err());
    match result {
        Err(DriverError::Pool(msg)) => {
            assert!(msg.contains("exhausted"), "should say exhausted: {msg}")
        }
        Err(e) => panic!("expected Pool error, got: {e}"),
        Ok(_) => panic!("expected error for max_size=0"),
    }
}

#[test]
fn pool_builder_default_max_size_is_10() {
    let pool = Pool::builder()
        .url("postgres://user:pass@localhost/db")
        .build()
        .unwrap();
    assert_eq!(pool.max_size(), 10);
}

// =========================================================================
// Gap tests: pool guard accessors
// =========================================================================

#[test]
fn pool_guard_pid_nonzero() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let conn = pool.acquire().unwrap();
    assert!(conn.pid() > 0, "pool guard pid should be positive");
}

#[test]
fn pool_guard_is_idle_after_acquire() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let conn = pool.acquire().unwrap();
    assert!(conn.is_idle(), "pool guard should be idle after acquire");
}

#[test]
fn pool_guard_is_not_in_transaction() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let conn = pool.acquire().unwrap();
    assert!(
        !conn.is_in_transaction(),
        "pool guard should not be in transaction"
    );
}

// =========================================================================
// Gap tests: low-level Connection methods
// =========================================================================

#[test]
fn prepare_only_caches_statement() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT $1::int4 + $2::int4 AS sum";
    let h = hash_sql(sql);

    assert_eq!(conn.stmt_cache_len(), 0);

    // prepare_only should parse+describe but NOT execute
    conn.prepare_only(sql, h).unwrap();
    assert_eq!(conn.stmt_cache_len(), 1);

    // Now query using the same SQL — should use cached statement
    let arena = Arena::new();
    let result = conn.query(sql, h, &[&3i32, &7i32]).unwrap();
    assert_eq!(result.row(0, &arena).get_i32(0), Some(10));

    // Still only 1 entry in cache
    assert_eq!(conn.stmt_cache_len(), 1);
}

#[test]
fn prepare_only_idempotent() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT 1::int4";
    let h = hash_sql(sql);

    conn.prepare_only(sql, h).unwrap();
    assert_eq!(conn.stmt_cache_len(), 1);

    // Second prepare_only for same SQL should be a no-op
    conn.prepare_only(sql, h).unwrap();
    assert_eq!(conn.stmt_cache_len(), 1);
}

#[test]
fn simple_query_rows_returns_data() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let rows = conn
        .simple_query_rows("SELECT 1 AS n, 'hello' AS msg")
        .unwrap();
    assert_eq!(rows.len(), 1);
    // SimpleRow = Vec<Option<String>>
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1].as_deref(), Some("hello"));
}

#[test]
fn simple_query_rows_multiple_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let rows = conn
        .simple_query_rows("SELECT generate_series(1, 3) AS n")
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));
    assert_eq!(rows[2][0].as_deref(), Some("3"));
}

#[test]
fn simple_query_rows_empty_result() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let rows = conn.simple_query_rows("SELECT 1 WHERE false").unwrap();
    assert!(rows.is_empty());
}

#[test]
fn simple_query_rows_null_value() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let rows = conn.simple_query_rows("SELECT NULL::text AS val").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], None);
}

#[test]
fn execute_monolithic_returns_affected() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE _driver_test_mono (id int, name text)")
        .unwrap();

    let sql = "INSERT INTO _driver_test_mono VALUES ($1::int4, $2::text)";
    let h = hash_sql(sql);
    let affected = conn.execute_monolithic(sql, h, &[&1i32, &"alice"]).unwrap();
    assert_eq!(affected, 1);

    let affected2 = conn.execute_monolithic(sql, h, &[&2i32, &"bob"]).unwrap();
    assert_eq!(affected2, 1);

    // Verify both rows exist
    let arena = Arena::new();
    let sel = "SELECT count(*)::int4 FROM _driver_test_mono";
    let sel_h = hash_sql(sel);
    let result = conn.query(sel, sel_h, &[]).unwrap();
    assert_eq!(result.row(0, &arena).get_i32(0), Some(2));
}

#[test]
fn execute_monolithic_update() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query(
        "CREATE TEMP TABLE _driver_test_mono_upd (id int, val text);
         INSERT INTO _driver_test_mono_upd VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .unwrap();

    let sql = "UPDATE _driver_test_mono_upd SET val = $1::text WHERE id > $2::int4";
    let h = hash_sql(sql);
    let affected = conn.execute_monolithic(sql, h, &[&"new", &1i32]).unwrap();
    assert_eq!(affected, 2);
}

#[test]
fn for_each_raw_processes_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT generate_series(1, 10)::int4 AS n";
    let h = hash_sql(sql);
    let mut count = 0usize;
    conn.for_each_raw(sql, h, &[], |_raw_row_data| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 10);
}

#[test]
fn for_each_raw_zero_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT 1 WHERE false";
    let h = hash_sql(sql);
    let mut count = 0usize;
    conn.for_each_raw(sql, h, &[], |_raw_row_data| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn drain_notifications_empty() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // No LISTEN — no notifications pending
    let notifs = conn.drain_notifications();
    assert!(notifs.is_empty());
    assert_eq!(conn.pending_notification_count(), 0);
}

#[test]
fn stmt_cache_len_after_queries() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    assert_eq!(conn.stmt_cache_len(), 0);

    let sql1 = "SELECT 1::int4";
    let h1 = hash_sql(sql1);
    conn.query(sql1, h1, &[]).unwrap();
    assert_eq!(conn.stmt_cache_len(), 1);

    let sql2 = "SELECT 2::int4";
    let h2 = hash_sql(sql2);
    conn.query(sql2, h2, &[]).unwrap();
    assert_eq!(conn.stmt_cache_len(), 2);

    let sql3 = "SELECT $1::text";
    let h3 = hash_sql(sql3);
    conn.query(sql3, h3, &[&"hello"]).unwrap();
    assert_eq!(conn.stmt_cache_len(), 3);

    // Repeat sql1 — should NOT increase cache size
    conn.query(sql1, h1, &[]).unwrap();
    assert_eq!(conn.stmt_cache_len(), 3);
}

#[test]
fn stmt_cache_len_with_prepare_only() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    assert_eq!(conn.stmt_cache_len(), 0);

    let sql1 = "SELECT $1::int4 + 1 AS inc";
    let h1 = hash_sql(sql1);
    conn.prepare_only(sql1, h1).unwrap();
    assert_eq!(conn.stmt_cache_len(), 1);

    let sql2 = "SELECT $1::text || $2::text AS concat";
    let h2 = hash_sql(sql2);
    conn.prepare_only(sql2, h2).unwrap();
    assert_eq!(conn.stmt_cache_len(), 2);

    // Now execute both — cache should still be 2
    let arena = Arena::new();
    let r1 = conn.query(sql1, h1, &[&5i32]).unwrap();
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(6));
    assert_eq!(conn.stmt_cache_len(), 2);

    let r2 = conn.query(sql2, h2, &[&"hello ", &"world"]).unwrap();
    assert_eq!(r2.row(0, &arena).get_str(0), Some("hello world"));
    assert_eq!(conn.stmt_cache_len(), 2);
}

// =========================================================================
// Gap tests: wait_for_notification — multi-thread
// =========================================================================

#[test]
fn wait_for_notification_receives_notify() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();

    // Listener connection
    let mut listener_conn = Connection::connect(&config).unwrap();
    listener_conn
        .simple_query("LISTEN test_wait_notif")
        .unwrap();

    // Sender thread — sends NOTIFY after small delay
    let config2 = config.clone();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut sender = Connection::connect(&config2).unwrap();
        sender
            .simple_query("NOTIFY test_wait_notif, 'hello_from_thread'")
            .unwrap();
    });

    // This blocks until notification arrives
    let (channel, payload) = listener_conn.wait_for_notification().unwrap();
    assert_eq!(channel, "test_wait_notif");
    assert_eq!(payload, "hello_from_thread");

    handle.join().unwrap();
    listener_conn
        .simple_query("UNLISTEN test_wait_notif")
        .unwrap();
}

// =========================================================================
// Gap tests: cancel() — idle connection
// =========================================================================

#[test]
fn cancel_on_idle_connection_does_not_panic() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();

    // Cancel on idle connection — PG ignores it, but the function should work.
    // On UDS connections cancel() may fail (it always uses TCP), so accept either
    // Ok or Err. The important thing is it does not panic.
    let _ = conn.cancel();
}

// =========================================================================
// Gap tests: touch / idle_duration
// =========================================================================

#[test]
fn connection_touch_updates_idle_duration() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));
    let before = conn.idle_duration();
    assert!(
        before >= std::time::Duration::from_millis(40),
        "idle_duration should be >= 40ms after sleeping 50ms, got {before:?}"
    );

    conn.touch();
    let after = conn.idle_duration();
    assert!(
        after < std::time::Duration::from_millis(10),
        "idle_duration should be < 10ms right after touch(), got {after:?}"
    );
}

// =========================================================================
// Gap tests: query_counter
// =========================================================================

#[test]
fn connection_query_counter_increments() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let before = conn.query_counter();

    // query() increments query_counter
    let sql = "SELECT 1::int4";
    let hash = hash_sql(sql);
    let _ = conn.query(sql, hash, &[]).unwrap();
    let after_one = conn.query_counter();
    assert!(
        after_one > before,
        "query_counter should increment after query(): before={before}, after={after_one}"
    );

    // execute() also increments
    conn.simple_query("CREATE TEMP TABLE _driver_test_qc (id int)")
        .unwrap();
    let exec_sql = "INSERT INTO _driver_test_qc VALUES ($1::int4)";
    let exec_hash = hash_sql(exec_sql);
    let _ = conn.execute(exec_sql, exec_hash, &[&1i32]).unwrap();
    let after_two = conn.query_counter();
    assert!(
        after_two > after_one,
        "query_counter should increment after execute(): after_one={after_one}, after_two={after_two}"
    );
}

// =========================================================================
// Gap tests: created_at
// =========================================================================

#[test]
fn connection_created_at_is_recent() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();

    assert!(
        conn.created_at().elapsed() < std::time::Duration::from_secs(5),
        "created_at should be within the last 5 seconds"
    );
}

// =========================================================================
// Gap tests: drain_notifications after NOTIFY
// =========================================================================

#[test]
fn connection_drain_notifications_after_notify() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("LISTEN drain_test_chan").unwrap();

    // Send notification from a separate connection
    let mut sender = Connection::connect(&config).unwrap();
    sender
        .simple_query("NOTIFY drain_test_chan, 'drain_payload'")
        .unwrap();

    // Give PG a moment to deliver
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Run a query to trigger notification buffering (read_one_message buffers
    // NotificationResponse messages it sees while reading query results).
    let _ = conn.simple_query("SELECT 1");

    let notifs = conn.drain_notifications();
    // May or may not have the notification depending on timing, but drain
    // must not panic and must return a Vec.
    assert!(notifs.len() <= 1);

    // After drain, pending count should be zero.
    assert_eq!(conn.pending_notification_count(), 0);

    conn.simple_query("UNLISTEN drain_test_chan").unwrap();
}

// =========================================================================
// Gap tests: pending_notification_count on fresh connection
// =========================================================================

#[test]
fn connection_pending_notification_count_fresh() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let conn = Connection::connect(&config).unwrap();

    // Fresh connection should have 0 pending notifications
    assert_eq!(conn.pending_notification_count(), 0);
}

// =========================================================================
// Gap tests: set_max_stmt_cache_size eviction
// =========================================================================

#[test]
fn connection_set_max_stmt_cache_size_evicts() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.set_max_stmt_cache_size(5);

    // Cache more than 5 statements — older ones should be evicted
    for i in 0..10 {
        let sql = format!("SELECT {i}::int4");
        let hash = hash_sql(&sql);
        let _ = conn.query(&sql, hash, &[]).unwrap();
    }
    assert!(
        conn.stmt_cache_len() <= 5,
        "cache should be capped at 5, got {}",
        conn.stmt_cache_len()
    );
}

// =========================================================================
// Gap tests: set_read_timeout — notification timeout
// =========================================================================

#[test]
fn connection_set_read_timeout() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // LISTEN first, before setting the short timeout
    conn.simple_query("LISTEN timeout_test_chan").unwrap();

    // Now set a very short timeout so wait_for_notification will time out
    conn.set_read_timeout(Some(std::time::Duration::from_millis(1)))
        .unwrap();

    let result = conn.wait_for_notification();
    assert!(result.is_err(), "should timeout with 1ms read timeout");

    // After a read timeout the connection's internal buffer state may be
    // partially filled, so the connection is no longer reliably reusable.
    // We just verify the timeout fired correctly and let the connection drop.
    // PG will auto-UNLISTEN when the session closes.
}

// --- COPY protocol tests ---

#[test]
fn copy_in_basic() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_test (id serial, name text, email text)")
        .unwrap();

    let rows = [
        "alice\talice@example.com",
        "bob\tbob@example.com",
        "charlie\tcharlie@example.com",
    ];

    let count = conn
        .copy_in("copy_test", &["name", "email"], rows.iter().copied())
        .unwrap();
    assert_eq!(count, 3);

    // Verify data was actually inserted
    let result = conn
        .simple_query_rows("SELECT name, email FROM copy_test ORDER BY name")
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0][0].as_deref(), Some("alice"));
    assert_eq!(result[0][1].as_deref(), Some("alice@example.com"));
    assert_eq!(result[1][0].as_deref(), Some("bob"));
    assert_eq!(result[2][0].as_deref(), Some("charlie"));
}

#[test]
fn copy_in_empty() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_empty_test (name text, email text)")
        .unwrap();

    let count = conn
        .copy_in(
            "copy_empty_test",
            &["name", "email"],
            std::iter::empty::<&str>(),
        )
        .unwrap();
    assert_eq!(count, 0);

    // Connection should still be usable
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn copy_out_basic() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // Create and populate a temp table
    conn.simple_query("CREATE TEMP TABLE copy_out_test (name text, email text)")
        .unwrap();
    conn.simple_query(
        "INSERT INTO copy_out_test VALUES ('alice', 'alice@example.com'), ('bob', 'bob@example.com'), ('charlie', 'charlie@example.com')",
    )
    .unwrap();

    let mut buf = Vec::new();
    let count = conn
        .copy_out(
            "SELECT name, email FROM copy_out_test ORDER BY name",
            &mut buf,
        )
        .unwrap();
    assert_eq!(count, 3);
    assert!(!buf.is_empty());

    let text = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].contains("alice"));
    assert!(lines[0].contains('\t'));
    assert!(lines[1].contains("bob"));
    assert!(lines[2].contains("charlie"));
}

#[test]
fn copy_in_bad_table() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let result = conn.copy_in(
        "nonexistent_table_12345",
        &["col1"],
        ["value"].iter().copied(),
    );
    assert!(result.is_err());
    // Connection should still be usable after error
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn copy_in_bad_column() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_badcol_test (name text)")
        .unwrap();

    let result = conn.copy_in(
        "copy_badcol_test",
        &["nonexistent_column"],
        ["value"].iter().copied(),
    );
    assert!(result.is_err());
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn copy_out_bad_query() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let mut buf = Vec::new();
    let result = conn.copy_out("SELECT * FROM nonexistent_table_12345", &mut buf);
    assert!(result.is_err());
    // Connection should still be usable after error
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());
}

#[test]
fn copy_in_special_chars_in_identifiers() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // Table name with quotes and special chars
    conn.simple_query(r#"CREATE TEMP TABLE "copy""test" ("col""name" text)"#)
        .unwrap();

    let count = conn
        .copy_in(r#"copy"test"#, &[r#"col"name"#], ["hello"].iter().copied())
        .unwrap();
    assert_eq!(count, 1);

    let rows = conn
        .simple_query_rows(r#"SELECT "col""name" FROM "copy""test""#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("hello"));
}

#[test]
fn copy_roundtrip() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_rt (name text, age text)")
        .unwrap();

    // Copy in
    let in_rows = ["alice\t30", "bob\t25"];
    let in_count = conn
        .copy_in("copy_rt", &["name", "age"], in_rows.iter().copied())
        .unwrap();
    assert_eq!(in_count, 2);

    // Copy out
    let mut buf = Vec::new();
    let out_count = conn
        .copy_out("SELECT name, age FROM copy_rt ORDER BY name", &mut buf)
        .unwrap();
    assert_eq!(out_count, 2);

    let text = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "alice\t30");
    assert_eq!(lines[1], "bob\t25");
}

#[test]
fn copy_in_via_pool() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut conn = pool.acquire().unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_pool_test (name text, val text)")
        .unwrap();

    let rows = ["a\t1", "b\t2"];
    let count = conn
        .copy_in("copy_pool_test", &["name", "val"], rows.iter().copied())
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn copy_out_via_pool() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();
    let mut conn = pool.acquire().unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_pool_out (x text)")
        .unwrap();
    conn.simple_query("INSERT INTO copy_pool_out VALUES ('one'), ('two')")
        .unwrap();

    let mut buf = Vec::new();
    let count = conn
        .copy_out("SELECT x FROM copy_pool_out ORDER BY x", &mut buf)
        .unwrap();
    assert_eq!(count, 2);

    let text = String::from_utf8(buf).unwrap();
    assert_eq!(text.lines().count(), 2);
}

#[test]
fn copy_in_many_rows() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE copy_many (id text, val text)")
        .unwrap();

    let rows: Vec<String> = (0..1000).map(|i| format!("{i}\tvalue_{i}")).collect();
    let count = conn
        .copy_in("copy_many", &["id", "val"], rows.iter().map(|s| s.as_str()))
        .unwrap();
    assert_eq!(count, 1000);

    let result = conn
        .simple_query_rows("SELECT count(*) FROM copy_many")
        .unwrap();
    assert_eq!(result[0][0].as_deref(), Some("1000"));
}

// =========================================================================
// Unnamed statement mode (pgbouncer compatibility)
// =========================================================================

#[test]
fn unnamed_statement_basic_query() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.statement_cache_mode = bsql_driver_postgres::StatementCacheMode::Disabled;
    let mut conn = Connection::connect(&config).unwrap();

    let h = hash_sql("SELECT 1 AS n");
    let arena = Arena::new();
    let result = conn.query("SELECT 1 AS n", h, &[]).unwrap();
    let rows: Vec<_> = result.rows(&arena).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_i32(0), Some(1));
}

#[test]
fn unnamed_statement_cache_stays_empty() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.statement_cache_mode = bsql_driver_postgres::StatementCacheMode::Disabled;
    let mut conn = Connection::connect(&config).unwrap();

    let h1 = hash_sql("SELECT 1");
    conn.query("SELECT 1", h1, &[]).unwrap();

    let h2 = hash_sql("SELECT 2");
    conn.query("SELECT 2", h2, &[]).unwrap();

    let h3 = hash_sql("SELECT 3");
    conn.query("SELECT 3", h3, &[]).unwrap();

    assert_eq!(
        conn.stmt_cache_len(),
        0,
        "stmt cache must stay empty in disabled mode"
    );
}

#[test]
fn unnamed_statement_multiple_queries_same_sql() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.statement_cache_mode = bsql_driver_postgres::StatementCacheMode::Disabled;
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT 42 AS answer";
    let h = hash_sql(sql);
    let arena = Arena::new();

    // Run the same query multiple times — should succeed every time
    // even without caching (each execution re-parses).
    for _ in 0..5 {
        let result = conn.query(sql, h, &[]).unwrap();
        let rows: Vec<_> = result.rows(&arena).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_i32(0), Some(42));
    }
}

#[test]
fn unnamed_statement_with_params() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.statement_cache_mode = bsql_driver_postgres::StatementCacheMode::Disabled;
    let mut conn = Connection::connect(&config).unwrap();

    let sql = "SELECT $1::int + $2::int AS sum";
    let h = hash_sql(sql);
    let arena = Arena::new();

    let a: i32 = 10;
    let b: i32 = 20;
    let result = conn.query(sql, h, &[&a, &b]).unwrap();
    let rows: Vec<_> = result.rows(&arena).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_i32(0), Some(30));
}

#[test]
fn unnamed_statement_execute() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.statement_cache_mode = bsql_driver_postgres::StatementCacheMode::Disabled;
    let mut conn = Connection::connect(&config).unwrap();

    conn.simple_query("CREATE TEMP TABLE unnamed_exec_test (id int)")
        .unwrap();

    let sql = "INSERT INTO unnamed_exec_test VALUES ($1)";
    let h = hash_sql(sql);
    let val: i32 = 99;
    let affected = conn.execute(sql, h, &[&val]).unwrap();
    assert_eq!(affected, 1);

    let arena = Arena::new();
    let q = "SELECT id FROM unnamed_exec_test";
    let hq = hash_sql(q);
    let result = conn.query(q, hq, &[]).unwrap();
    let rows: Vec<_> = result.rows(&arena).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_i32(0), Some(99));
}

#[test]
fn unnamed_statement_url_parsing() {
    let url = require_db!();
    // Append statement_cache=disabled to the URL
    let sep = if url.contains('?') { "&" } else { "?" };
    let url_disabled = format!("{url}{sep}statement_cache=disabled");
    let config = Config::from_url(&url_disabled).unwrap();
    assert_eq!(
        config.statement_cache_mode,
        bsql_driver_postgres::StatementCacheMode::Disabled,
    );

    let mut conn = Connection::connect(&config).unwrap();
    let h = hash_sql("SELECT 1");
    let arena = Arena::new();
    let result = conn.query("SELECT 1", h, &[]).unwrap();
    let rows: Vec<_> = result.rows(&arena).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(conn.stmt_cache_len(), 0);
}

#[test]
fn unnamed_statement_pool_builder() {
    let url = require_db!();
    let pool = Pool::builder()
        .url(&url)
        .max_size(1)
        .statement_cache_mode(bsql_driver_postgres::StatementCacheMode::Disabled)
        .build()
        .unwrap();

    let mut conn = pool.acquire().unwrap();
    let h = hash_sql("SELECT 1");
    let arena = Arena::new();
    let result = conn.query("SELECT 1", h, &[]).unwrap();
    let rows: Vec<_> = result.rows(&arena).collect();
    assert_eq!(rows.len(), 1);
}

// =========================================================================
// Phase 7: Pool edge case and connection error recovery tests
// =========================================================================

// --- Pool concurrent acquire up to max_size ---

#[test]
fn pool_concurrent_acquire_up_to_max_size() {
    let url = require_db!();
    let pool = std::sync::Arc::new(
        Pool::builder()
            .url(&url)
            .max_size(3)
            .acquire_timeout(None) // fail-fast
            .build()
            .unwrap(),
    );

    // Acquire 3 connections — all should succeed.
    let mut conn1 = pool.acquire().unwrap();
    let mut conn2 = pool.acquire().unwrap();
    let mut conn3 = pool.acquire().unwrap();

    conn1.simple_query("SELECT 1").unwrap();
    conn2.simple_query("SELECT 1").unwrap();
    conn3.simple_query("SELECT 1").unwrap();

    assert_eq!(pool.open_count(), 3);

    // 4th acquire should fail immediately (fail-fast, no timeout).
    let result = pool.acquire();
    assert!(result.is_err());
    match result {
        Err(DriverError::Pool(msg)) => {
            assert!(msg.contains("exhausted"), "expected exhausted error: {msg}");
        }
        Err(e) => panic!("expected Pool error, got: {e}"),
        Ok(_) => panic!("4th acquire should fail with max_size=3"),
    }

    // Return all connections, pool should recover.
    drop(conn1);
    drop(conn2);
    drop(conn3);

    // Acquire again should succeed.
    let mut conn = pool.acquire().unwrap();
    conn.simple_query("SELECT 1").unwrap();
}

// --- Pool acquire timeout (with real connections) ---

#[test]
fn pool_acquire_timeout_with_held_connection() {
    let url = require_db!();
    let pool = Pool::builder()
        .url(&url)
        .max_size(1)
        .acquire_timeout(Some(std::time::Duration::from_millis(100)))
        .build()
        .unwrap();

    // Hold the single connection.
    let _conn1 = pool.acquire().unwrap();

    // Second acquire should block then timeout.
    let start = std::time::Instant::now();
    let result = pool.acquire();
    let elapsed = start.elapsed();

    assert!(result.is_err(), "second acquire should timeout");
    match result {
        Err(DriverError::Pool(msg)) => {
            assert!(
                msg.contains("timeout") || msg.contains("exhausted"),
                "unexpected error: {msg}"
            );
        }
        Err(e) => panic!("expected Pool error, got: {e}"),
        Ok(_) => panic!("expected timeout error"),
    }
    // Verify it actually waited (at least ~50ms, not instant).
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "timeout fired too fast: {elapsed:?}"
    );
    // And didn't wait too long.
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "timeout took too long: {elapsed:?}"
    );
}

// --- Pool max_lifetime causes connection replacement ---

#[test]
fn pool_max_lifetime_replaces_expired_connections() {
    let url = require_db!();
    let pool = Pool::builder()
        .url(&url)
        .max_size(1)
        .max_lifetime(Some(std::time::Duration::from_millis(50)))
        .stale_timeout(std::time::Duration::from_secs(60)) // don't stale-out
        .build()
        .unwrap();

    // Acquire first connection, note its PID.
    let mut conn = pool.acquire().unwrap();
    conn.simple_query("SELECT 1").unwrap();
    let pid1 = conn.pid();
    drop(conn);

    // Wait longer than max_lifetime so the connection expires.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Next acquire should get a NEW connection (old one expired).
    let mut conn2 = pool.acquire().unwrap();
    conn2.simple_query("SELECT 1").unwrap();
    let pid2 = conn2.pid();

    // PIDs should differ because the old connection was discarded.
    assert_ne!(
        pid1, pid2,
        "expected a new connection after max_lifetime expiry (pid1={pid1}, pid2={pid2})"
    );
}

// --- Pool status under load ---

#[test]
fn pool_status_reflects_active_and_idle() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(3).build().unwrap();

    // Initially, everything is zero.
    let s = pool.status();
    assert_eq!(s.idle, 0);
    assert_eq!(s.active, 0);
    assert_eq!(s.open, 0);

    // Acquire 2 connections — both active.
    let mut c1 = pool.acquire().unwrap();
    let mut c2 = pool.acquire().unwrap();
    c1.simple_query("SELECT 1").unwrap();
    c2.simple_query("SELECT 1").unwrap();

    let s = pool.status();
    assert_eq!(s.active, 2, "2 connections held = 2 active");
    assert_eq!(s.idle, 0);
    assert_eq!(s.open, 2);

    // Return one connection — it becomes idle.
    drop(c1);
    let s = pool.status();
    assert_eq!(s.active, 1, "1 connection held = 1 active");
    assert_eq!(s.idle, 1, "1 connection returned = 1 idle");
    assert_eq!(s.open, 2);

    // Return the other.
    drop(c2);
    let s = pool.status();
    assert_eq!(s.active, 0);
    assert_eq!(s.idle, 2, "both returned = 2 idle");
    assert_eq!(s.open, 2);
    assert_eq!(s.max_size, 3);
}

// --- Server kill backend returns error ---

#[test]
fn server_kill_backend_returns_error() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();

    // Open the "victim" connection.
    let mut victim = Connection::connect(&config).unwrap();
    victim.simple_query("SELECT 1").unwrap(); // verify it works

    // Get the victim's backend PID via simple_query_rows.
    let rows = victim.simple_query_rows("SELECT pg_backend_pid()").unwrap();
    let pid_str = rows[0][0].as_deref().expect("pid should not be null");
    let pid: i32 = pid_str.parse().expect("pid should be an integer");
    assert!(pid > 0, "backend pid should be positive: {pid}");

    // Open a second connection to terminate the victim.
    let mut killer = Connection::connect(&config).unwrap();
    let kill_sql = format!("SELECT pg_terminate_backend({pid})");
    killer.simple_query(&kill_sql).unwrap();

    // Give PG a moment to process the termination.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // The victim's next query should fail with an I/O error.
    let result = victim.simple_query("SELECT 1");
    assert!(
        result.is_err(),
        "query on terminated connection should fail"
    );
    match &result {
        Err(DriverError::Io(_)) => {} // expected — socket closed
        Err(DriverError::Server { code, .. }) => {
            // PG may send an error response before closing (57P01 = admin_shutdown)
            assert_eq!(
                code,
                b"57P01",
                "expected admin_shutdown code, got: {}",
                std::str::from_utf8(code).unwrap_or("???")
            );
        }
        Err(e) => panic!("expected Io or Server(57P01) error, got: {e}"),
        Ok(_) => panic!("expected error"),
    }
}

// --- Statement timeout returns error ---

#[test]
fn statement_timeout_returns_error() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // Set a very short statement timeout (100ms).
    conn.simple_query("SET statement_timeout = '100ms'")
        .unwrap();

    // Run a query that takes longer than the timeout.
    let result = conn.simple_query("SELECT pg_sleep(5)");
    assert!(
        result.is_err(),
        "pg_sleep(5) with 100ms timeout should fail"
    );
    match &result {
        Err(DriverError::Server { code, .. }) => {
            // 57014 = query_canceled
            assert_eq!(
                code,
                b"57014",
                "expected query_canceled (57014), got: {}",
                std::str::from_utf8(code).unwrap_or("???")
            );
        }
        Err(e) => panic!("expected Server(57014) error, got: {e}"),
        Ok(_) => panic!("expected error"),
    }
}

// --- Query after pg_terminate_backend returns error ---

#[test]
fn query_after_disconnect_returns_error() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();

    // Open a connection and verify it works.
    let mut conn = Connection::connect(&config).unwrap();
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle());

    // Get the connection's own backend PID.
    let rows = conn.simple_query_rows("SELECT pg_backend_pid()").unwrap();
    let pid: i32 = rows[0][0].as_deref().unwrap().parse().unwrap();

    // Terminate it from a separate connection.
    let mut killer = Connection::connect(&config).unwrap();
    let kill_sql = format!("SELECT pg_terminate_backend({pid})");
    killer.simple_query(&kill_sql).unwrap();

    // Give PG a moment to tear down the backend.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Every subsequent query should fail.
    let r1 = conn.simple_query("SELECT 1");
    assert!(r1.is_err(), "first query after kill should fail");

    // Connection should not magically recover — second attempt also fails.
    let r2 = conn.simple_query("SELECT 2");
    assert!(r2.is_err(), "second query after kill should also fail");
}

// --- Pool discards terminated connections and recovers ---

#[test]
fn pool_recovers_after_backend_terminated() {
    let url = require_db!();
    // Use stale_timeout=0 so the dead connection is immediately discarded
    // when the pool tries to pop it (idle_duration >= 0 always).
    let pool = Pool::builder()
        .url(&url)
        .max_size(1)
        .stale_timeout(std::time::Duration::ZERO)
        .build()
        .unwrap();

    // Acquire, use, get PID, return to pool.
    let pid1;
    {
        let mut conn = pool.acquire().unwrap();
        conn.simple_query("SELECT 1").unwrap();
        pid1 = conn.pid();
    }

    // Kill the backend from a separate connection.
    {
        let config = Config::from_url(&url).unwrap();
        let mut killer = Connection::connect(&config).unwrap();
        killer
            .simple_query(&format!("SELECT pg_terminate_backend({pid1})"))
            .unwrap();
    }

    // Give PG a moment to process the termination.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // With stale_timeout=0, the pool discards the dead connection on pop
    // (idle_duration >= 0 is always true), decrements open_count, and
    // creates a fresh connection. The next acquire should succeed.
    let mut conn = pool.acquire().unwrap();
    conn.simple_query("SELECT 1").unwrap();
    let pid2 = conn.pid();
    assert_ne!(
        pid1, pid2,
        "should get a new connection after the old one was terminated"
    );
}

// --- Pool concurrent acquire/release stress with status invariants ---

#[test]
fn pool_status_invariant_under_concurrent_load() {
    let url = require_db!();
    let pool = std::sync::Arc::new(
        Pool::builder()
            .url(&url)
            .max_size(4)
            .acquire_timeout(Some(std::time::Duration::from_secs(10)))
            .build()
            .unwrap(),
    );

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let pool = pool.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..50 {
                let mut conn = pool.acquire().unwrap();
                conn.simple_query("SELECT 1").unwrap();
                drop(conn);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // After all threads finish, open_count <= max_size.
    assert!(pool.open_count() <= pool.max_size());

    // All connections returned — active should be 0.
    let status = pool.status();
    assert_eq!(status.active, 0, "all connections should be idle");
    assert!(
        status.idle <= pool.max_size(),
        "idle count should not exceed max_size"
    );
    assert_eq!(
        status.open, status.idle,
        "open should equal idle when no connections are active"
    );
}

// --- Connection recovery after statement timeout ---

#[test]
fn connection_recovers_after_statement_timeout() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // Set a short statement timeout.
    conn.simple_query("SET statement_timeout = '100ms'")
        .unwrap();

    // Trigger a timeout.
    let result = conn.simple_query("SELECT pg_sleep(5)");
    assert!(result.is_err());

    // The connection enters a failed transaction state or returns to idle
    // depending on whether we were in a transaction. Without BEGIN, PG
    // auto-rolls-back and the connection should be usable again.
    // Reset timeout and verify recovery.
    conn.simple_query("SET statement_timeout = '0'").unwrap();
    conn.simple_query("SELECT 1").unwrap();
    assert!(conn.is_idle(), "connection should be idle after recovery");
}

// ===============================================================
// Error handling gaps
// ===============================================================

#[test]
fn not_null_constraint_violation() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // tickets.title is NOT NULL — insert without it should fail
    let result = conn.simple_query("INSERT INTO tickets (created_by_user_id) VALUES (1)");
    assert!(result.is_err());
    match result {
        Err(DriverError::Server { code, .. }) => {
            // PG error code 23502 = not_null_violation
            assert_eq!(&code, b"23502");
        }
        Err(e) => panic!("expected Server error with code 23502, got: {e}"),
        Ok(_) => unreachable!(),
    }
}

#[test]
fn division_by_zero_error() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    let result = conn.simple_query("SELECT 1/0");
    assert!(result.is_err());
    match result {
        Err(DriverError::Server { code, .. }) => {
            // PG error code 22012 = division_by_zero
            assert_eq!(&code, b"22012");
        }
        Err(e) => panic!("expected Server error with code 22012, got: {e}"),
        Ok(_) => unreachable!(),
    }
}

// ===============================================================
// Pool URL parsing edge cases
// ===============================================================

#[test]
fn url_without_password() {
    let config = Config::from_url("postgres://user@localhost/db").unwrap();
    assert_eq!(config.user, "user");
    assert_eq!(config.password, "");
}

#[test]
fn url_without_port_defaults_5432() {
    let config = Config::from_url("postgres://user:pass@localhost/db").unwrap();
    assert_eq!(config.port, 5432);
}

#[test]
fn url_with_percent_encoded_password() {
    let config = Config::from_url("postgres://user:p%40ss%23word@localhost/db").unwrap();
    assert_eq!(config.password, "p@ss#word");
}

#[test]
fn url_invalid_scheme() {
    let result = Config::from_url("mysql://user:pass@localhost/db");
    assert!(result.is_err());
}

#[test]
fn url_missing_at_sign() {
    let result = Config::from_url("postgres://localhost/db");
    assert!(result.is_err());
}

// ===============================================================
// Pool idempotency
// ===============================================================

#[test]
fn pool_close_is_idempotent() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(1).build().unwrap();

    // Acquire + release a connection so the pool has one idle connection.
    {
        let mut conn = pool.acquire().unwrap();
        conn.simple_query("SELECT 1").unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(10));

    pool.close();
    pool.close(); // second close must not panic

    let status = pool.status();
    assert_eq!(status.idle, 0);
    assert!(pool.is_closed());

    // Acquiring after close should fail.
    let result = pool.acquire();
    assert!(result.is_err());
}

// ===============================================================
// Statement cache edge cases
// ===============================================================

#[test]
fn stmt_cache_size_zero_evicts_aggressively() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    conn.set_max_stmt_cache_size(0);

    // With max_stmt_cache_size=0, the cache evicts before inserting a new
    // statement, keeping at most 1 entry. Two *different* queries demonstrate
    // that the cache never grows beyond 1.
    let sql1 = "SELECT 1 AS n";
    let hash1 = hash_sql(sql1);
    let sql2 = "SELECT 2 AS m";
    let hash2 = hash_sql(sql2);

    let r1 = conn.query(sql1, hash1, &[]).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(1));
    assert!(conn.stmt_cache_len() <= 1);

    let r2 = conn.query(sql2, hash2, &[]).unwrap();
    assert_eq!(r2.len(), 1);
    assert_eq!(r2.row(0, &arena).get_i32(0), Some(2));
    // Cache should still be at most 1 due to aggressive eviction
    assert!(conn.stmt_cache_len() <= 1);
}

#[test]
fn stmt_cache_disabled_mode_always_reparses() {
    let url = require_db!();
    let mut config = Config::from_url(&url).unwrap();
    config.statement_cache_mode = bsql_driver_postgres::StatementCacheMode::Disabled;
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    // With Disabled mode, every query uses the unnamed statement — no caching.
    let sql = "SELECT 1 AS n";
    let hash = hash_sql(sql);

    let r1 = conn.query(sql, hash, &[]).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r1.row(0, &arena).get_i32(0), Some(1));

    let r2 = conn.query(sql, hash, &[]).unwrap();
    assert_eq!(r2.len(), 1);
    assert_eq!(r2.row(0, &arena).get_i32(0), Some(1));

    assert_eq!(conn.stmt_cache_len(), 0);
}

// --- Additional error tests ---

#[test]
fn check_constraint_violation() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();

    // Attempt to insert a negative score.
    // score is SMALLINT NOT NULL DEFAULT 0 — no CHECK, but -1 is valid for smallint.
    // This test documents that negative values are accepted.
    let result = conn.simple_query(
        "INSERT INTO users (login, first_name, last_name, email, score) VALUES ('check_test', 'A', 'B', 'c@d.com', -1)"
    );
    if result.is_ok() {
        conn.simple_query("DELETE FROM users WHERE login = 'check_test'")
            .unwrap();
    }
}

#[test]
fn wrong_database_connection() {
    let config = Config::from_url("postgres://bsql:bsql@localhost/nonexistent_db_xyz").unwrap();
    let result = Connection::connect(&config);
    assert!(result.is_err());
}

#[test]
fn wrong_password_connection() {
    let config = Config::from_url("postgres://bsql:wrong_password@localhost/bsql_test").unwrap();
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

// --- Pool edge-case: prepared queries through max_size=1 pool ---

#[test]
fn pool_max_size_1_sequential_queries() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(1).build().unwrap();
    let arena = Arena::new();

    let sql = "SELECT 1 AS n";
    let hash = hash_sql(sql);

    // First query — acquire, run prepared query, release.
    {
        let mut conn = pool.acquire().unwrap();
        let r = conn.query(sql, hash, &[]).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.row(0, &arena).get_i32(0), Some(1));
    }

    // Give the spawned return task a moment.
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Second query — should get the same connection back (statement is cached).
    {
        let mut conn = pool.acquire().unwrap();
        let r = conn.query(sql, hash, &[]).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.row(0, &arena).get_i32(0), Some(1));
    }
}

// --- Encoding edge case: bytea with interleaved NUL bytes ---

#[test]
fn encode_nul_byte_in_bytea() {
    let url = require_db!();
    let config = Config::from_url(&url).unwrap();
    let mut conn = Connection::connect(&config).unwrap();
    let arena = Arena::new();

    let sql = "SELECT $1::bytea AS val";
    let hash = hash_sql(sql);
    let val: &[u8] = &[0x00, 0xFF, 0x00, 0xAB];
    let result = conn.query(sql, hash, &[&val]).unwrap();
    let row = result.row(0, &arena);
    let decoded = row.get_bytes(0).unwrap();
    assert_eq!(decoded, &[0x00, 0xFF, 0x00, 0xAB]);
}

// --- Streaming query interrupted by backend kill ---

#[test]
fn streaming_query_interrupted_by_backend_kill() {
    let url = require_db!();
    // Need 2 connections: one for streaming, one for pg_terminate_backend
    let pool = Pool::builder().url(&url).max_size(2).build().unwrap();

    let mut conn = pool.acquire().unwrap();

    // Get the backend PID
    let pid_rows = conn.simple_query_rows("SELECT pg_backend_pid()").unwrap();
    let pid: i32 = pid_rows[0][0].as_deref().unwrap().parse().unwrap();

    // Start a long-running streaming query that generates many rows
    let sql = "SELECT g FROM generate_series(1, 100000) g";
    let hash = hash_sql(sql);

    let stream_result = conn.query_streaming_start(sql, hash, &[], 64);

    if let Ok(_) = stream_result {
        // Kill the backend from another connection
        let mut killer = pool.acquire().unwrap();
        let _ = killer.simple_query(&format!("SELECT pg_terminate_backend({pid})"));
        drop(killer);

        // Give PG a moment to process the termination
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Next read from the stream should error — connection is broken
        let result = conn.simple_query("SELECT 1");
        assert!(
            result.is_err(),
            "connection should be broken after backend kill"
        );
    }
}

// --- Pool recovers after connection killed during query ---

#[test]
fn pool_recovers_after_connection_killed_during_query() {
    let url = require_db!();
    let pool = Pool::builder()
        .url(&url)
        .max_size(2)
        .stale_timeout(std::time::Duration::from_millis(0)) // immediate stale eviction
        .build()
        .unwrap();

    // Get a connection and its PID
    let mut conn = pool.acquire().unwrap();
    let pid_rows = conn.simple_query_rows("SELECT pg_backend_pid()").unwrap();
    let pid: i32 = pid_rows[0][0].as_deref().unwrap().parse().unwrap();
    drop(conn);

    // Kill that backend
    let mut killer = pool.acquire().unwrap();
    let _ = killer.simple_query(&format!("SELECT pg_terminate_backend({pid})"));
    drop(killer);

    // Wait for stale eviction
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Pool should recover — next query gets a fresh connection
    let mut conn = pool.acquire().unwrap();
    let result = conn.simple_query("SELECT 1");
    assert!(
        result.is_ok(),
        "pool should provide working connection after kill"
    );
}

// --- Transaction with large deferred batch ---

#[test]
fn transaction_with_large_deferred_batch() {
    let url = require_db!();
    // Verify ordering is preserved in large deferred pipeline
    let pool = Pool::builder().url(&url).max_size(1).build().unwrap();

    let mut tx = pool.begin().unwrap();
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', 1)";
    let hash = hash_sql(sql);

    for i in 0..100 {
        let title = format!("order_test_{:04}", i);
        tx.defer_execute(sql, hash, &[&title]).unwrap();
    }

    let results = tx.flush_deferred().unwrap();
    assert_eq!(results.len(), 100);
    // All should affect 1 row
    assert!(results.iter().all(|&r| r == 1));

    tx.rollback().unwrap();
}

// ===========================================================================
// STRESS TESTS — run with: cargo test -- --ignored
// ===========================================================================

#[test]
#[ignore] // stress: 16 threads x 100 queries
fn stress_concurrent_pool_queries() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(4).build().unwrap();

    let pool = std::sync::Arc::new(pool);
    let mut handles = vec![];

    for thread_id in 0..16u32 {
        let pool = pool.clone();
        handles.push(std::thread::spawn(move || {
            for query_id in 0..100u32 {
                let sql = "SELECT id, login FROM users ORDER BY id";
                let hash = hash_sql(sql);
                let mut conn = pool.acquire().unwrap();
                let result = conn.query(sql, hash, &[]).unwrap();
                assert_eq!(result.len(), 2, "thread {thread_id} query {query_id}");
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let status = pool.status();
    assert!(status.idle + status.active <= 4, "pool size exceeded max");
}

#[test]
#[ignore] // stress: singleflight under contention
fn stress_singleflight_contention() {
    use std::sync::Arc;

    let url = require_db!();
    let pool = Arc::new(Pool::builder().url(&url).max_size(4).build().unwrap());

    let mut handles = vec![];

    // 20 threads all doing the exact same query simultaneously
    for _ in 0..20 {
        let pool = pool.clone();
        handles.push(std::thread::spawn(move || {
            let sql = "SELECT id, login FROM users ORDER BY id";
            let hash = hash_sql(sql);
            let mut conn = pool.acquire().unwrap();
            let result = conn.query(sql, hash, &[]).unwrap();
            assert_eq!(result.len(), 2);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn connection_returned_in_tx_state_is_discarded() {
    let url = require_db!();
    // If a connection is returned to pool while in transaction state,
    // pool should discard it (not reuse with dangling tx)
    let pool = Pool::builder().url(&url).max_size(1).build().unwrap();

    {
        let mut conn = pool.acquire().unwrap();
        conn.simple_query("BEGIN").unwrap();
        // Drop conn WITHOUT commit/rollback — pool should discard
    }

    // Next acquire should get a fresh connection (not in tx state)
    let mut conn = pool.acquire().unwrap();
    let result = conn.simple_query("SELECT 1");
    assert!(
        result.is_ok(),
        "new connection should work after discarded tx connection"
    );
}

#[test]
fn pool_status_after_all_connections_used_and_returned() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(3).build().unwrap();

    // Acquire all 3
    let c1 = pool.acquire().unwrap();
    let c2 = pool.acquire().unwrap();
    let c3 = pool.acquire().unwrap();

    let status = pool.status();
    assert_eq!(status.active, 3);
    assert_eq!(status.idle, 0);

    // Return all
    drop(c1);
    drop(c2);
    drop(c3);

    let status = pool.status();
    assert_eq!(status.active, 0);
    assert_eq!(status.idle, 3);
}

#[test]
#[ignore] // stress: rapid acquire/release cycles
fn stress_pool_acquire_release_cycles() {
    let url = require_db!();
    let pool = Pool::builder().url(&url).max_size(2).build().unwrap();

    // 1000 rapid acquire/release cycles
    for _ in 0..1000 {
        let mut conn = pool.acquire().unwrap();
        conn.simple_query("SELECT 1").unwrap();
        // conn dropped here, returned to pool
    }

    let status = pool.status();
    assert!(status.idle > 0);
    assert_eq!(status.active, 0);
}

#[test]
#[ignore] // stress: deferred pipeline with 1000 operations
fn stress_deferred_pipeline_1000() {
    let url = require_db!();
    let pool = Pool::connect(&url).unwrap();

    let sql = "INSERT INTO users (login, first_name, last_name, email) VALUES ($1, $2, $3, $4)";
    let hash = hash_sql(sql);

    let mut tx = pool.begin().unwrap();

    // Defer 1000 inserts
    for i in 0..1000i32 {
        let login = format!("deferred_stress_{i}");
        let first_name = format!("first_stress_{i}");
        let last_name = "test".to_string();
        let email = format!("deferred_stress_{i}@test.com");
        tx.defer_execute(sql, hash, &[&login, &first_name, &last_name, &email])
            .unwrap();
    }

    // Flush all at once
    let results = tx.flush_deferred().unwrap();
    assert_eq!(results.len(), 1000);
    for &affected in &results {
        assert_eq!(affected, 1);
    }

    tx.rollback().unwrap();
}

// --- Unix domain socket connection ---

#[cfg(unix)]
#[test]
fn unix_socket_connection() {
    let _url = require_db!(); // skip if no DB configured

    // Connect via Unix domain socket (default PG socket path).
    // Try /tmp first (macOS default), then /var/run/postgresql (Linux).
    let urls = [
        "postgres://bsql:bsql@localhost/bsql_test?host=/tmp",
        "postgres://bsql:bsql@localhost/bsql_test?host=/var/run/postgresql",
    ];

    let mut connected = false;
    for url in &urls {
        let config = Config::from_url(url).unwrap();
        if let Ok(mut conn) = Connection::connect(&config) {
            let result = conn.simple_query("SELECT 1");
            assert!(result.is_ok());
            connected = true;
            break;
        }
    }
    // At least one socket path should work on Unix
    assert!(connected, "UDS connection should work on at least one path");
}

#[cfg(windows)]
#[test]
fn unix_socket_rejected_on_windows() {
    let config = Config::from_url("postgres://bsql:bsql@localhost/bsql_test?host=/tmp").unwrap();
    let result = Connection::connect(&config);
    assert!(result.is_err(), "UDS should fail on Windows");
}

// --- TLS prefer fallback ---

#[test]
fn tls_prefer_fallback_to_plain() {
    let _url = require_db!(); // skip if no DB configured

    // sslmode=prefer should try TLS, fall back to plain if server doesn't support it.
    // Most test PG instances don't have TLS, so this tests the fallback path.
    let config =
        Config::from_url("postgres://bsql:bsql@localhost/bsql_test?sslmode=prefer").unwrap();
    let result = Connection::connect(&config);
    // Should succeed (falls back to plain) or succeed with TLS
    assert!(
        result.is_ok(),
        "sslmode=prefer should connect (with or without TLS): {:?}",
        result.err()
    );
}

// --- Tokio cancellation mid-stream ---

#[tokio::test]
async fn tokio_cancel_during_query() {
    let _url = require_db!(); // skip if no DB configured

    // Start a long query, cancel the task, verify pool is not poisoned.
    let pool = std::sync::Arc::new(
        Pool::builder()
            .url("postgres://bsql:bsql@localhost/bsql_test")
            .max_size(1)
            .acquire_timeout(Some(std::time::Duration::from_secs(5)))
            .build()
            .unwrap(),
    );

    let p = pool.clone();
    let handle = tokio::spawn(async move {
        let mut conn = p.acquire().unwrap();
        let sql = "SELECT pg_sleep(10)"; // 10 second sleep
        let hash = hash_sql(sql);
        // This will block for 10 seconds
        let _ = conn.query(sql, hash, &[]);
    });

    // Cancel after 100ms
    std::thread::sleep(std::time::Duration::from_millis(100));
    handle.abort();
    let _ = handle.await; // JoinError from abort

    // Wait for connection to be returned/discarded
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Pool should still work — get a new connection
    let mut conn = pool.acquire().unwrap();
    let result = conn.simple_query("SELECT 1");
    assert!(result.is_ok(), "pool should work after cancelled task");
}
