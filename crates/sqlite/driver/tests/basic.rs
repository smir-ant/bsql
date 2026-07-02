#![forbid(unsafe_code)]
//! Functional + tier-1 read-path tests for the SQLite driver.
//!
//! The read path is *classified*: a typed read of a column whose storage class
//! does not match the requested Rust type is an `Err`, never a silent `None`,
//! and a real SQL `NULL` is distinct from a type mismatch. The streaming
//! `query_each` path borrows each row zero-copy and cannot leak a borrow.

use core::ops::ControlFlow;

use bsql_sqlite::{Connection, SqliteError, Type, ValueRef};

#[test]
fn open_in_memory() {
    let conn = Connection::open_in_memory().expect("open");
    conn.close().expect("close");
}

#[test]
fn create_table_and_insert() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)").expect("create");
    let changed = conn.execute("INSERT INTO t VALUES (1, 'alice')").expect("insert");
    assert_eq!(changed, 1);
    conn.close().expect("close");
}

#[test]
fn query_rows() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER, name TEXT)").expect("create");
    conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')").expect("insert");

    let result = conn.query("SELECT id, name FROM t ORDER BY id").expect("select");
    assert_eq!(result.column_count, 2);
    assert_eq!(result.rows.len(), 2);

    assert_eq!(result.rows[0].get::<i32>(0).expect("id0"), 1);
    assert_eq!(result.rows[0].get::<&str>(1).expect("name0"), "alice");
    assert_eq!(result.rows[1].get::<i32>(0).expect("id1"), 2);
    assert_eq!(result.rows[1].get::<&str>(1).expect("name1"), "bob");
}

#[test]
fn query_with_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").expect("insert");

    let result = conn.query_params("SELECT v FROM t WHERE id > ?", &["1"]).expect("select");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get::<&str>(0).expect("b"), "b");
    assert_eq!(result.rows[1].get::<&str>(0).expect("c"), "c");
}

#[test]
fn execute_with_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");

    let changed = conn.execute_params("INSERT INTO t VALUES (?, ?)", &["42", "hello"]).expect("insert");
    assert_eq!(changed, 1);

    let result = conn.query("SELECT id, v FROM t").expect("select");
    assert_eq!(result.rows[0].get::<i32>(0).expect("id"), 42);
    assert_eq!(result.rows[0].get::<&str>(1).expect("v"), "hello");
}

#[test]
fn null_handling() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELECT 1, NULL, 'hello'").expect("select");
    assert_eq!(result.rows.len(), 1);

    let row = &result.rows[0];
    assert_eq!(row.column_count(), 3);
    assert_eq!(row.get::<i32>(0).expect("i32"), 1);
    assert!(row.is_null(1).expect("is_null"));
    // A nullable read of the NULL column yields Ok(None) — distinct from a
    // type mismatch (which would be Err).
    assert_eq!(row.get_opt::<&str>(1).expect("opt"), None);
    assert_eq!(row.get::<&str>(2).expect("hello"), "hello");
}

#[test]
fn typed_access() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELECT 42, 2.5, 'text', 1, 0").expect("select");
    let row = &result.rows[0];

    assert_eq!(row.get::<i32>(0).expect("i32"), 42);
    assert_eq!(row.get::<i64>(0).expect("i64"), 42);
    assert!((row.get::<f64>(1).expect("f64") - 2.5).abs() < f64::EPSILON);
    assert_eq!(row.get::<&str>(2).expect("str"), "text");
    assert!(row.get::<bool>(3).expect("bool true"));
    assert!(!row.get::<bool>(4).expect("bool false"));
}

#[test]
fn empty_result() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER)").expect("create");
    let result = conn.query("SELECT id FROM t").expect("select");
    assert_eq!(result.rows.len(), 0);
    assert_eq!(result.column_count, 1);
}

#[test]
fn many_rows() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(i INTEGER)").expect("create");
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})")).expect("insert");
    }
    let result = conn.query("SELECT i FROM t ORDER BY i").expect("select");
    assert_eq!(result.rows.len(), 1000);
    assert_eq!(result.rows[0].get::<i32>(0).expect("first"), 0);
    assert_eq!(result.rows[999].get::<i32>(0).expect("last"), 999);
}

#[test]
fn bad_sql_returns_error() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELCT TYPO");
    assert!(result.is_err());
}

#[test]
fn foreign_keys_enforced() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)").expect("create parent");
    conn.execute("CREATE TABLE child(id INTEGER, pid INTEGER REFERENCES parent(id))")
        .expect("create child");
    let result = conn.execute("INSERT INTO child VALUES (1, 999)");
    assert!(result.is_err(), "FK violation should fail");
}

#[test]
fn transaction_commit() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v INTEGER)").expect("create");
    conn.execute("BEGIN").expect("begin");
    conn.execute("INSERT INTO t VALUES (1)").expect("insert");
    conn.execute("COMMIT").expect("commit");

    let result = conn.query("SELECT v FROM t").expect("select");
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn transaction_rollback() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v INTEGER)").expect("create");
    conn.execute("INSERT INTO t VALUES (1)").expect("seed");
    conn.execute("BEGIN").expect("begin");
    conn.execute("INSERT INTO t VALUES (2)").expect("insert");
    conn.execute("ROLLBACK").expect("rollback");

    let result = conn.query("SELECT v FROM t").expect("select");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<i32>(0).expect("v"), 1);
}

#[test]
fn blob_roundtrip() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(data BLOB)").expect("create");
    conn.execute("INSERT INTO t VALUES (X'DEADBEEF')").expect("insert");

    let result = conn.query("SELECT data FROM t").expect("select");
    assert_eq!(result.rows[0].get::<&[u8]>(0).expect("blob"), [0xDE, 0xAD, 0xBE, 0xEF].as_slice());
    // Owned copy variant.
    assert_eq!(result.rows[0].get::<Vec<u8>>(0).expect("owned blob"), vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn open_file_and_reopen() {
    let dir = std::env::temp_dir().join("bsql_sqlite_test.db");
    let _ = std::fs::remove_file(&dir);

    {
        let conn = Connection::open(&dir).expect("open");
        conn.execute("CREATE TABLE t(v INTEGER)").expect("create");
        conn.execute("INSERT INTO t VALUES (42)").expect("insert");
        conn.close().expect("close");
    }
    {
        let conn = Connection::open(&dir).expect("reopen");
        let result = conn.query("SELECT v FROM t").expect("select");
        assert_eq!(result.rows[0].get::<i32>(0).expect("v"), 42);
        conn.close().expect("close");
    }

    let _ = std::fs::remove_file(&dir);
}

#[test]
fn column_names() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELECT 1 AS id, 'hello' AS greeting").expect("query");
    assert_eq!(result.column_names, vec!["id", "greeting"]);
    assert_eq!(
        result.rows[0].get_by_name::<&str>("greeting", &result.column_names).expect("greeting"),
        "hello"
    );
    // A missing name is a classified error, not a silent None.
    match result.rows[0].get_by_name::<&str>("missing", &result.column_names) {
        Err(SqliteError::UnknownColumn { name }) => assert_eq!(name, "missing"),
        other => panic!("expected UnknownColumn, got {other:?}"),
    }
    assert_eq!(result.rows[0].get::<i32>(0).expect("id"), 1);
}

#[test]
fn query_one_and_opt() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    conn.execute("INSERT INTO t VALUES (42)").expect("insert");

    let row = conn.query_one("SELECT v FROM t").expect("query_one");
    assert_eq!(row.get::<i32>(0).expect("v"), 42);

    let opt = conn.query_opt("SELECT v FROM t WHERE v = 999").expect("query_opt");
    assert!(opt.is_none());
}

#[test]
fn transaction_helpers() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");

    conn.begin().expect("begin");
    conn.execute("INSERT INTO t VALUES (1)").expect("insert");
    conn.commit().expect("commit");
    let r = conn.query("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.rows[0].get::<i64>(0).expect("count"), 1);

    conn.begin().expect("begin2");
    conn.execute("INSERT INTO t VALUES (2)").expect("insert2");
    conn.rollback().expect("rollback");
    let r = conn.query("SELECT count(*) FROM t").expect("count2");
    assert_eq!(r.rows[0].get::<i64>(0).expect("count2"), 1);
}

#[test]
fn unicode_values() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v text)").expect("create");
    conn.execute("INSERT INTO t VALUES ('Привет мир')").expect("insert");
    let result = conn.query("SELECT v FROM t").expect("query");
    assert_eq!(result.rows[0].get::<&str>(0).expect("v"), "Привет мир");
    assert_eq!(result.rows[0].get::<String>(0).expect("owned"), "Привет мир".to_string());
}

#[test]
fn transaction_closure_commit() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    conn.transaction(|c| {
        c.execute("INSERT INTO t VALUES (1)")?;
        Ok(())
    })
    .expect("tx");
    let r = conn.query("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.rows[0].get::<i64>(0).expect("count"), 1);
}

#[test]
fn transaction_closure_rollback_on_err() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    conn.execute("INSERT INTO t VALUES (1)").expect("seed");
    let err: Result<(), _> = conn.transaction(|c| {
        c.execute("INSERT INTO t VALUES (2)")?;
        Err(SqliteError::Query("forced".to_string()))
    });
    assert!(err.is_err());
    let r = conn.query("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.rows[0].get::<i64>(0).expect("count"), 1, "should have rolled back");
}

#[test]
fn transaction_closure_return_value() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    let count = conn
        .transaction(|c| {
            c.execute("INSERT INTO t VALUES (1)")?;
            c.execute("INSERT INTO t VALUES (2)")?;
            let r = c.query("SELECT count(*) FROM t")?;
            r.rows[0].get::<i64>(0)
        })
        .expect("tx");
    assert_eq!(count, 2);
}

#[test]
fn typed_get_generic() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 42, 2.5, 'hello'").expect("query");
    let row = &r.rows[0];
    assert_eq!(row.get::<i32>(0).expect("i32"), 42);
    assert!((row.get::<f64>(1).expect("f64") - 2.5).abs() < f64::EPSILON);
    assert_eq!(row.get::<String>(2).expect("string"), "hello".to_string());
}

#[test]
fn execute_batch_multi_statement() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch(
        "
        CREATE TABLE a(v int);
        CREATE TABLE b(v int);
        INSERT INTO a VALUES (1);
        INSERT INTO b VALUES (2);
    ",
    )
    .expect("batch");
    let r1 = conn.query("SELECT v FROM a").expect("a");
    let r2 = conn.query("SELECT v FROM b").expect("b");
    assert_eq!(r1.rows[0].get::<i64>(0).expect("a"), 1);
    assert_eq!(r2.rows[0].get::<i64>(0).expect("b"), 2);
}

#[test]
fn native_integer_access() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 42, 2.5, NULL").expect("query");
    let row = &r.rows[0];
    assert_eq!(row.get::<i64>(0).expect("i64"), 42);
    assert!((row.get::<f64>(1).expect("f64") - 2.5).abs() < f64::EPSILON);
    // Integer -> f64 lossless coercion.
    assert!((row.get::<f64>(0).expect("coerce") - 42.0).abs() < f64::EPSILON);
    assert!(row.is_null(2).expect("null"));
}

// ─────────────────────── tier-1: classified reads ───────────────────────

#[test]
fn type_mismatch_is_classified_not_silent_none() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 42, 'text', X'BEEF'").expect("query");
    let row = &r.rows[0];

    // Integer read as &str: a classified TypeMismatch, NOT a silent None.
    match row.get::<&str>(0) {
        Err(SqliteError::TypeMismatch { column, expected, found }) => {
            assert_eq!(column, 0);
            assert_eq!(expected, Type::Text);
            assert_eq!(found, Type::Integer);
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }

    // Text read as i64: classified mismatch (no hopeful str::parse).
    match row.get::<i64>(1) {
        Err(SqliteError::TypeMismatch { expected, found, .. }) => {
            assert_eq!(expected, Type::Integer);
            assert_eq!(found, Type::Text);
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }

    // Blob read as &str: classified mismatch.
    assert!(matches!(row.get::<&str>(2), Err(SqliteError::TypeMismatch { .. })));

    // get_opt on a type mismatch is STILL an Err — it distinguishes NULL from
    // mismatch, it does not swallow a mismatch as None.
    assert!(matches!(row.get_opt::<&str>(0), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn null_is_distinct_from_mismatch() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT NULL, 'x'").expect("query");
    let row = &r.rows[0];

    // A non-nullable get of a NULL column is UnexpectedNull, not TypeMismatch.
    match row.get::<i64>(0) {
        Err(SqliteError::UnexpectedNull { column }) => assert_eq!(column, 0),
        other => panic!("expected UnexpectedNull, got {other:?}"),
    }
    // The nullable read of the same NULL column is Ok(None).
    assert_eq!(row.get_opt::<i64>(0).expect("opt null"), None);

    // A mismatch on a non-NULL column is TypeMismatch, never UnexpectedNull.
    assert!(matches!(row.get::<i64>(1), Err(SqliteError::TypeMismatch { .. })));
    // And a nullable read that finds a present-but-wrong value is Err, not None.
    assert!(matches!(row.get_opt::<i64>(1), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn integer_out_of_range_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 5000000000").expect("query"); // > i32::MAX
    let row = &r.rows[0];
    // Fits i64, overflows i32 — a classified error, not a truncated value.
    assert_eq!(row.get::<i64>(0).expect("i64"), 5_000_000_000);
    match row.get::<i32>(0) {
        Err(SqliteError::IntegerOutOfRange { column, value }) => {
            assert_eq!(column, 0);
            assert_eq!(value, 5_000_000_000);
        }
        other => panic!("expected IntegerOutOfRange, got {other:?}"),
    }
}

#[test]
fn f64_rejects_inexact_integer() {
    let conn = Connection::open_in_memory().expect("open");
    // 2^53 + 1 is the first integer f64 cannot represent exactly.
    let r = conn.query("SELECT 9007199254740993").expect("query");
    let row = &r.rows[0];
    assert_eq!(row.get::<i64>(0).expect("i64"), 9_007_199_254_740_993);
    match row.get::<f64>(0) {
        Err(SqliteError::InexactFloat { value, .. }) => assert_eq!(value, 9_007_199_254_740_993),
        other => panic!("expected InexactFloat, got {other:?}"),
    }
}

#[test]
fn f64_accepts_two_pow_53_exactly() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 9007199254740992").expect("query"); // exactly 2^53
    let v = r.rows[0].get::<f64>(0).expect("2^53 must convert");
    assert!((v - 9_007_199_254_740_992.0).abs() < f64::EPSILON);
}

#[test]
fn non_boolean_integer_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 5, 'true'").expect("query");
    let row = &r.rows[0];
    // Integer 5 read as bool: SQLite bool is 0/1, so 5 is classified, not truthy.
    match row.get::<bool>(0) {
        Err(SqliteError::NotABoolean { value, .. }) => assert_eq!(value, 5),
        other => panic!("expected NotABoolean, got {other:?}"),
    }
    // Text 'true' read as bool is a type mismatch — text-bool parsing is gone.
    assert!(matches!(row.get::<bool>(1), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn invalid_utf8_text_is_classified_on_eager_materialize() {
    let conn = Connection::open_in_memory().expect("open");
    // CAST a non-UTF-8 blob to TEXT: the eager path validates UTF-8 up front
    // and fails the whole query with a classified error, never lossily.
    match conn.query("SELECT CAST(X'FF' AS TEXT)") {
        Err(SqliteError::InvalidUtf8 { column }) => assert_eq!(column, 0),
        other => panic!("expected InvalidUtf8, got {other:?}"),
    }
}

#[test]
fn column_index_out_of_bounds_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 1").expect("query");
    let row = &r.rows[0];
    match row.get::<i64>(5) {
        Err(SqliteError::ColumnIndexOutOfBounds { index, count }) => {
            assert_eq!(index, 5);
            assert_eq!(count, 1);
        }
        other => panic!("expected ColumnIndexOutOfBounds, got {other:?}"),
    }
}

// ─────────────────────── streaming lending path ───────────────────────

#[test]
fn query_each_streams_all_rows() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(i INTEGER)").expect("create");
    conn.execute("INSERT INTO t VALUES (0),(1),(2),(3),(4)").expect("insert");

    let mut sum = 0i64;
    let mut n = 0usize;
    let outcome = conn
        .query_each("SELECT i FROM t ORDER BY i", |row| {
            sum += row.get::<i64>(0).expect("i64");
            n += 1;
            ControlFlow::<()>::Continue(())
        })
        .expect("stream");
    assert_eq!(outcome, None, "full stream reaches exhaustion");
    assert_eq!(n, 5);
    assert_eq!(sum, 10); // 0 + 1 + 2 + 3 + 4
}

#[test]
fn query_each_breaks_early() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(i INTEGER)").expect("create");
    conn.execute("INSERT INTO t VALUES (0),(1),(2),(3),(4)").expect("insert");

    let mut seen = 0usize;
    let outcome = conn
        .query_each("SELECT i FROM t ORDER BY i", |row| {
            let v = row.get::<i64>(0).expect("i64");
            seen += 1;
            if v == 2 {
                ControlFlow::Break("stopped at 2")
            } else {
                ControlFlow::Continue(())
            }
        })
        .expect("stream");
    assert_eq!(outcome, Some("stopped at 2"));
    assert_eq!(seen, 3, "stopped after seeing 0,1,2");
}

#[test]
fn query_each_zero_copy_text_and_blob_borrow() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(s TEXT, b BLOB)").expect("create");
    conn.execute("INSERT INTO t VALUES ('hello', X'DEADBEEF')").expect("insert");

    let outcome = conn
        .query_each("SELECT s, b FROM t", |row| {
            // The &str / &[u8] borrow SQLite's own column buffer for the row
            // step — zero copy. Assert the borrowed values directly.
            let s: &str = row.get::<&str>(0).expect("str borrow");
            let b: &[u8] = row.get::<&[u8]>(1).expect("blob borrow");
            assert_eq!(s, "hello");
            assert_eq!(b, [0xDE, 0xAD, 0xBE, 0xEF].as_slice());
            // The raw storage class is inspectable via value_ref too.
            assert!(matches!(row.value_ref(0).expect("vref"), ValueRef::Text(_)));
            assert!(matches!(row.value_ref(1).expect("vref"), ValueRef::Blob(_)));
            ControlFlow::<()>::Continue(())
        })
        .expect("stream");
    assert_eq!(outcome, None);
}

#[test]
fn query_each_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");
    conn.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").expect("insert");

    let mut collected: Vec<String> = Vec::new();
    let outcome = conn
        .query_each_params("SELECT v FROM t WHERE id > ? ORDER BY id", &["1"], |row| {
            // Copy out explicitly when accumulation is wanted.
            collected.push(row.get::<String>(0).expect("owned"));
            ControlFlow::<()>::Continue(())
        })
        .expect("stream");
    assert_eq!(outcome, None);
    assert_eq!(collected, vec!["b".to_string(), "c".to_string()]);
}

#[test]
fn query_each_classified_error_mid_stream() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 'x'").expect("query");
    // Reading text as i64 inside the stream returns a classified Err from the
    // accessor — the driver never silently substitutes.
    assert!(matches!(r.rows[0].get::<i64>(0), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn full_lifecycle_integration() {
    let conn = Connection::open_in_memory().expect("open");

    conn.execute_batch("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score REAL);")
        .expect("create");

    conn.transaction(|tx| {
        tx.execute("INSERT INTO users(name, score) VALUES ('alice', 95.5)")?;
        tx.execute("INSERT INTO users(name, score) VALUES ('bob', 88.0)")?;
        Ok(())
    })
    .expect("tx");

    let result = conn.query("SELECT id, name, score FROM users ORDER BY id").expect("query");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.column_names, vec!["id", "name", "score"]);
    assert_eq!(result.rows[0].get::<&str>(1).expect("name"), "alice");
    assert!((result.rows[0].get::<f64>(2).expect("score") - 95.5).abs() < f64::EPSILON);
    assert_eq!(result.rows[1].get::<i64>(0).expect("id"), 2);

    // Native type access — no double-conversion.
    assert_eq!(result.rows[0].get::<i64>(0).expect("id int"), 1);
    assert!((result.rows[0].get::<f64>(0).expect("id as f64") - 1.0).abs() < f64::EPSILON);

    // Error in transaction → rollback.
    let err: Result<(), _> = conn.transaction(|tx| {
        tx.execute("INSERT INTO users(name) VALUES ('charlie')")?;
        Err(SqliteError::Query("abort".to_string()))
    });
    assert!(err.is_err());
    let count = conn.query("SELECT count(*) FROM users").expect("count");
    assert_eq!(count.rows[0].get::<i64>(0).expect("count"), 2); // charlie rolled back

    conn.close().expect("close");
}
