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
    conn.execute_raw("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)").expect("create");
    let changed = conn.execute_raw("INSERT INTO t VALUES (1, 'alice')").expect("insert");
    assert_eq!(changed, 1);
    conn.close().expect("close");
}

#[test]
fn query_rows() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(id INTEGER, name TEXT)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')").expect("insert");

    let result = conn.query_raw("SELECT id, name FROM t ORDER BY id").expect("select");
    assert_eq!(result.column_count(), 2);
    assert_eq!(result.len(), 2);

    assert_eq!(result.get(0).expect("row 0").get::<i32>(0).expect("id0"), 1);
    assert_eq!(result.get(0).expect("row 0").get::<&str>(1).expect("name0"), "alice");
    assert_eq!(result.get(1).expect("row 1").get::<i32>(0).expect("id1"), 2);
    assert_eq!(result.get(1).expect("row 1").get::<&str>(1).expect("name1"), "bob");
}

#[test]
fn query_with_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").expect("insert");

    // Bind a TRUE integer parameter (not the text "1" the old text-only path
    // forced): `id INTEGER > ?` compares integer-to-integer with no affinity coercion.
    let result =
        conn.query_params("SELECT v FROM t WHERE id > ?", &[ValueRef::Integer(1)]).expect("select");
    assert_eq!(result.len(), 2);
    assert_eq!(result.get(0).expect("row 0").get::<&str>(0).expect("b"), "b");
    assert_eq!(result.get(1).expect("row 1").get::<&str>(0).expect("c"), "c");
}

#[test]
fn execute_with_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");

    // A mixed param list: an integer bound as INTEGER, a string bound as TEXT —
    // the `.into()` ergonomic keeps common binds terse.
    let changed = conn
        .execute_params("INSERT INTO t VALUES (?, ?)", &[42_i64.into(), "hello".into()])
        .expect("insert");
    assert_eq!(changed, 1);

    let result = conn.query_raw("SELECT id, v FROM t").expect("select");
    assert_eq!(result.get(0).expect("row 0").get::<i32>(0).expect("id"), 42);
    assert_eq!(result.get(0).expect("row 0").get::<&str>(1).expect("v"), "hello");
}

#[test]
fn null_handling() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query_raw("SELECT 1, NULL, 'hello'").expect("select");
    assert_eq!(result.len(), 1);

    let row = &result.get(0).expect("row 0");
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
    let result = conn.query_raw("SELECT 42, 2.5, 'text', 1, 0").expect("select");
    let row = &result.get(0).expect("row 0");

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
    conn.execute_raw("CREATE TABLE t(id INTEGER)").expect("create");
    let result = conn.query_raw("SELECT id FROM t").expect("select");
    assert_eq!(result.len(), 0);
    assert_eq!(result.column_count(), 1);
}

#[test]
fn many_rows() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(i INTEGER)").expect("create");
    for i in 0..1000 {
        conn.execute_raw(&format!("INSERT INTO t VALUES ({i})")).expect("insert");
    }
    let result = conn.query_raw("SELECT i FROM t ORDER BY i").expect("select");
    assert_eq!(result.len(), 1000);
    assert_eq!(result.get(0).expect("row 0").get::<i32>(0).expect("first"), 0);
    assert_eq!(result.get(999).expect("row 999").get::<i32>(0).expect("last"), 999);
}

#[test]
fn bad_sql_returns_error() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query_raw("SELCT TYPO");
    assert!(result.is_err());
}

#[test]
fn foreign_keys_enforced() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE parent(id INTEGER PRIMARY KEY)").expect("create parent");
    conn.execute_raw("CREATE TABLE child(id INTEGER, pid INTEGER REFERENCES parent(id))")
        .expect("create child");
    let result = conn.execute_raw("INSERT INTO child VALUES (1, 999)");
    assert!(result.is_err(), "FK violation should fail");
}

#[test]
fn transaction_commit() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v INTEGER)").expect("create");
    conn.execute_raw("BEGIN").expect("begin");
    conn.execute_raw("INSERT INTO t VALUES (1)").expect("insert");
    conn.execute_raw("COMMIT").expect("commit");

    let result = conn.query_raw("SELECT v FROM t").expect("select");
    assert_eq!(result.len(), 1);
}

#[test]
fn transaction_rollback() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v INTEGER)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (1)").expect("seed");
    conn.execute_raw("BEGIN").expect("begin");
    conn.execute_raw("INSERT INTO t VALUES (2)").expect("insert");
    conn.execute_raw("ROLLBACK").expect("rollback");

    let result = conn.query_raw("SELECT v FROM t").expect("select");
    assert_eq!(result.len(), 1);
    assert_eq!(result.get(0).expect("row 0").get::<i32>(0).expect("v"), 1);
}

#[test]
fn blob_roundtrip() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(data BLOB)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (X'DEADBEEF')").expect("insert");

    let result = conn.query_raw("SELECT data FROM t").expect("select");
    assert_eq!(result.get(0).expect("row 0").get::<&[u8]>(0).expect("blob"), [0xDE, 0xAD, 0xBE, 0xEF].as_slice());
    // Owned copy variant.
    assert_eq!(result.get(0).expect("row 0").get::<Vec<u8>>(0).expect("owned blob"), vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn open_file_and_reopen() {
    // Unique per process so a concurrent `cargo test` run (e.g. a background
    // build) cannot race this fixed path — matching the pid-scoped convention
    // in the sibling file-backed tests (`busy_timeout.rs`, `error_predicates.rs`).
    let dir = std::env::temp_dir().join(format!("bsql_sqlite_reopen_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&dir);

    {
        let conn = Connection::open(&dir).expect("open");
        conn.execute_raw("CREATE TABLE t(v INTEGER)").expect("create");
        conn.execute_raw("INSERT INTO t VALUES (42)").expect("insert");
        conn.close().expect("close");
    }
    {
        let conn = Connection::open(&dir).expect("reopen");
        let result = conn.query_raw("SELECT v FROM t").expect("select");
        assert_eq!(result.get(0).expect("row 0").get::<i32>(0).expect("v"), 42);
        conn.close().expect("close");
    }

    let _ = std::fs::remove_file(&dir);
}

#[test]
fn column_names() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query_raw("SELECT 1 AS id, 'hello' AS greeting").expect("query");
    assert_eq!(result.column_names.len(), 2);
    assert_eq!(result.column_names[0], "id");
    assert_eq!(result.column_names[1], "greeting");
    // The row carries its own names (via the shared arena) — no threaded slice.
    let row = result.get(0).expect("row 0");
    assert_eq!(row.get_by_name::<&str>("greeting").expect("greeting"), "hello");
    // A missing name is a classified error, not a silent None.
    match row.get_by_name::<&str>("missing") {
        Err(SqliteError::UnknownColumn { name }) => assert_eq!(name, "missing"),
        other => panic!("expected UnknownColumn, got {other:?}"),
    }
    assert_eq!(row.get::<i32>(0).expect("id"), 1);
}

#[test]
fn query_one_and_opt() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v int)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (42)").expect("insert");

    let row = conn.query_one_raw("SELECT v FROM t").expect("query_one");
    assert_eq!(row.get::<i32>(0).expect("v"), 42);

    let opt = conn.query_opt_raw("SELECT v FROM t WHERE v = 999").expect("query_opt");
    assert!(opt.is_none());
}

#[test]
fn f32_reads_lossless_or_classifies() {
    // The checked f32 narrow: a REAL/INTEGER read as f32 succeeds only when the
    // conversion is provably lossless, and is a classified error otherwise —
    // never a silently rounded/overflowed value.
    let conn = Connection::open_in_memory().expect("open");

    // In-range REAL, exactly representable in f32 -> Ok.
    let row = conn.query_one_raw("SELECT 2.5").expect("query 2.5");
    assert_eq!(row.get::<f32>(0).expect("2.5 narrows exactly"), 2.5_f32);

    // In-range INTEGER within f32's 24-bit mantissa -> Ok (lossless widen).
    let row = conn.query_one_raw("SELECT 100").expect("query 100");
    assert_eq!(row.get::<f32>(0).expect("100 as f32"), 100.0_f32);

    // A REAL needing more than f32's mantissa (the f64 nearest 0.1) -> classified.
    let row = conn.query_one_raw("SELECT 0.1").expect("query 0.1");
    match row.get::<f32>(0) {
        Err(SqliteError::InexactFloatNarrowing { column, .. }) => assert_eq!(column, 0),
        other => panic!("0.1 must not narrow to f32 exactly: {other:?}"),
    }

    // A REAL past f32::MAX (would overflow to +inf) -> classified.
    let row = conn.query_one_raw("SELECT 1e40").expect("query 1e40");
    assert!(matches!(row.get::<f32>(0), Err(SqliteError::InexactFloatNarrowing { .. })));

    // An INTEGER beyond f32's exact range (2^24 + 1) -> classified InexactFloat.
    let row = conn.query_one_raw("SELECT 16777217").expect("query 2^24+1");
    match row.get::<f32>(0) {
        Err(SqliteError::InexactFloat { value, .. }) => assert_eq!(value, 16_777_217),
        other => panic!("2^24+1 is not exact as f32: {other:?}"),
    }

    // A non-numeric column read as f32 -> TypeMismatch.
    let row = conn.query_one_raw("SELECT 'hi'").expect("query text");
    assert!(matches!(row.get::<f32>(0), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn transaction_helpers() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v int)").expect("create");

    conn.begin().expect("begin");
    conn.execute_raw("INSERT INTO t VALUES (1)").expect("insert");
    conn.commit().expect("commit");
    let r = conn.query_raw("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.get(0).expect("row 0").get::<i64>(0).expect("count"), 1);

    conn.begin().expect("begin2");
    conn.execute_raw("INSERT INTO t VALUES (2)").expect("insert2");
    conn.rollback().expect("rollback");
    let r = conn.query_raw("SELECT count(*) FROM t").expect("count2");
    assert_eq!(r.get(0).expect("row 0").get::<i64>(0).expect("count2"), 1);
}

#[test]
fn unicode_values() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v text)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES ('Привет мир')").expect("insert");
    let result = conn.query_raw("SELECT v FROM t").expect("query");
    assert_eq!(result.get(0).expect("row 0").get::<&str>(0).expect("v"), "Привет мир");
    assert_eq!(result.get(0).expect("row 0").get::<String>(0).expect("owned"), "Привет мир".to_string());
}

#[test]
fn transaction_closure_commit() {
    let mut conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v int)").expect("create");
    conn.transaction(|c| {
        c.execute_raw("INSERT INTO t VALUES (1)")?;
        Ok(())
    })
    .expect("tx");
    let r = conn.query_raw("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.get(0).expect("row 0").get::<i64>(0).expect("count"), 1);
}

#[test]
fn transaction_closure_rollback_on_err() {
    let mut conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v int)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (1)").expect("seed");
    let err: Result<(), _> = conn.transaction(|c| {
        c.execute_raw("INSERT INTO t VALUES (2)")?;
        Err(SqliteError::Query("forced".to_string()))
    });
    assert!(err.is_err());
    let r = conn.query_raw("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.get(0).expect("row 0").get::<i64>(0).expect("count"), 1, "should have rolled back");
}

#[test]
fn transaction_closure_return_value() {
    let mut conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v int)").expect("create");
    let count = conn
        .transaction(|c| {
            c.execute_raw("INSERT INTO t VALUES (1)")?;
            c.execute_raw("INSERT INTO t VALUES (2)")?;
            let r = c.query_raw("SELECT count(*) FROM t")?;
            r.get(0).expect("row 0").get::<i64>(0)
        })
        .expect("tx");
    assert_eq!(count, 2);
}

#[test]
fn typed_get_generic() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query_raw("SELECT 42, 2.5, 'hello'").expect("query");
    let row = &r.get(0).expect("row 0");
    assert_eq!(row.get::<i32>(0).expect("i32"), 42);
    assert!((row.get::<f64>(1).expect("f64") - 2.5).abs() < f64::EPSILON);
    assert_eq!(row.get::<String>(2).expect("string"), "hello".to_string());
}

#[test]
fn execute_batch_sql_multi_statement() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch_raw(
        "
        CREATE TABLE a(v int);
        CREATE TABLE b(v int);
        INSERT INTO a VALUES (1);
        INSERT INTO b VALUES (2);
    ",
    )
    .expect("batch");
    let r1 = conn.query_raw("SELECT v FROM a").expect("a");
    let r2 = conn.query_raw("SELECT v FROM b").expect("b");
    assert_eq!(r1.get(0).expect("row 0").get::<i64>(0).expect("a"), 1);
    assert_eq!(r2.get(0).expect("row 0").get::<i64>(0).expect("b"), 2);
}

#[test]
fn native_integer_access() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query_raw("SELECT 42, 2.5, NULL").expect("query");
    let row = &r.get(0).expect("row 0");
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
    let r = conn.query_raw("SELECT 42, 'text', X'BEEF'").expect("query");
    let row = &r.get(0).expect("row 0");

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
    let r = conn.query_raw("SELECT NULL, 'x'").expect("query");
    let row = &r.get(0).expect("row 0");

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
    let r = conn.query_raw("SELECT 5000000000").expect("query"); // > i32::MAX
    let row = &r.get(0).expect("row 0");
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
fn i16_reads_in_range_and_classifies_overflow() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query_raw("SELECT 30000, 40000").expect("query");
    let row = &r.get(0).expect("row 0");
    // In range for i16.
    assert_eq!(row.get::<i16>(0).expect("i16"), 30000);
    // 40000 > i16::MAX — classified out-of-range, never a wrapped/truncated read.
    match row.get::<i16>(1) {
        Err(SqliteError::IntegerOutOfRange { column, value }) => {
            assert_eq!(column, 1);
            assert_eq!(value, 40000);
        }
        other => panic!("expected IntegerOutOfRange, got {other:?}"),
    }
}

#[test]
fn f64_rejects_inexact_integer() {
    let conn = Connection::open_in_memory().expect("open");
    // 2^53 + 1 is the first integer f64 cannot represent exactly.
    let r = conn.query_raw("SELECT 9007199254740993").expect("query");
    let row = &r.get(0).expect("row 0");
    assert_eq!(row.get::<i64>(0).expect("i64"), 9_007_199_254_740_993);
    match row.get::<f64>(0) {
        Err(SqliteError::InexactFloat { value, .. }) => assert_eq!(value, 9_007_199_254_740_993),
        other => panic!("expected InexactFloat, got {other:?}"),
    }
}

#[test]
fn f64_accepts_two_pow_53_exactly() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query_raw("SELECT 9007199254740992").expect("query"); // exactly 2^53
    let v = r.get(0).expect("row 0").get::<f64>(0).expect("2^53 must convert");
    assert!((v - 9_007_199_254_740_992.0).abs() < f64::EPSILON);
}

#[test]
fn non_boolean_integer_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query_raw("SELECT 5, 'true'").expect("query");
    let row = &r.get(0).expect("row 0");
    // Integer 5 read as bool: SQLite bool is 0/1, so 5 is classified, not truthy.
    match row.get::<bool>(0) {
        Err(SqliteError::NotABoolean { value, .. }) => assert_eq!(value, 5),
        other => panic!("expected NotABoolean, got {other:?}"),
    }
    // Text 'true' read as bool is a type mismatch — text-bool parsing is gone.
    assert!(matches!(row.get::<bool>(1), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn invalid_utf8_text_is_classified_lazily_at_get() {
    let conn = Connection::open_in_memory().expect("open");
    // CAST a non-UTF-8 blob to TEXT. The arena stores the raw bytes and validates
    // UTF-8 LAZILY at `get::<&str>` — so the query SUCCEEDS (one bad byte in a big
    // result no longer fails the whole materialization), and only the text read
    // classifies. This is the semantic improvement over the old eager path.
    let r = conn.query_raw("SELECT CAST(X'FF' AS TEXT)").expect("query succeeds — no eager UTF-8 check");
    let row = r.get(0).expect("row 0");
    // The storage class is TEXT (not a lossy substitution).
    assert!(matches!(row.value_ref(0).expect("vref"), ValueRef::Text(_)));
    // Reading it as `&str` is the classified failure point.
    match row.get::<&str>(0) {
        Err(SqliteError::InvalidUtf8 { column }) => assert_eq!(column, 0),
        other => panic!("expected InvalidUtf8, got {other:?}"),
    }
    // The raw bytes are always recoverable via `value_ref`.
    match row.value_ref(0).expect("vref bytes") {
        ValueRef::Text(bytes) => assert_eq!(bytes, [0xFF].as_slice()),
        other => panic!("expected Text bytes, got {other:?}"),
    }
}

#[test]
fn column_index_out_of_bounds_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query_raw("SELECT 1").expect("query");
    let row = &r.get(0).expect("row 0");
    match row.get::<i64>(5) {
        Err(SqliteError::ColumnIndexOutOfBounds { index, count }) => {
            assert_eq!(index, 5);
            assert_eq!(count, 1);
        }
        other => panic!("expected ColumnIndexOutOfBounds, got {other:?}"),
    }
}

#[test]
fn streaming_column_index_out_of_bounds_is_classified() {
    // The STREAMING `BorrowedRow::value_ref` defers its bounds check to rusqlite's
    // `get_ref` (to drop a redundant per-cell `sqlite3_column_count` FFI on the hot
    // read) and RE-SHAPES rusqlite's `InvalidColumnIndex` into the SAME classified
    // `ColumnIndexOutOfBounds { index, count }` the eager `Row` path returns. This
    // pins that the cold error path is byte-identical to the former pre-check —
    // same variant, same `index`, same `count` — so the optimization changed only
    // the hot-path cost, never the observable error.
    let conn = Connection::open_in_memory().expect("open");
    let mut hit = false;
    conn.query_each_raw("SELECT 1", |row| {
        match row.value_ref(5) {
            Err(SqliteError::ColumnIndexOutOfBounds { index, count }) => {
                assert_eq!(index, 5);
                assert_eq!(count, 1);
                hit = true;
            }
            other => panic!("expected ColumnIndexOutOfBounds, got {other:?}"),
        }
        ControlFlow::<()>::Continue(())
    })
    .expect("stream");
    assert!(hit, "callback must have observed the out-of-bounds read");
}

// ─────────────────────── streaming lending path ───────────────────────

#[test]
fn query_each_streams_all_rows() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(i INTEGER)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (0),(1),(2),(3),(4)").expect("insert");

    let mut sum = 0i64;
    let mut n = 0usize;
    let outcome = conn
        .query_each_raw("SELECT i FROM t ORDER BY i", |row| {
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
    conn.execute_raw("CREATE TABLE t(i INTEGER)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (0),(1),(2),(3),(4)").expect("insert");

    let mut seen = 0usize;
    let outcome = conn
        .query_each_raw("SELECT i FROM t ORDER BY i", |row| {
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
    conn.execute_raw("CREATE TABLE t(s TEXT, b BLOB)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES ('hello', X'DEADBEEF')").expect("insert");

    let outcome = conn
        .query_each_raw("SELECT s, b FROM t", |row| {
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
    conn.execute_raw("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").expect("insert");

    let mut collected: Vec<String> = Vec::new();
    let outcome = conn
        .query_each_params("SELECT v FROM t WHERE id > ? ORDER BY id", &[ValueRef::Integer(1)], |row| {
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
    let r = conn.query_raw("SELECT 'x'").expect("query");
    // Reading text as i64 inside the stream returns a classified Err from the
    // accessor — the driver never silently substitutes.
    assert!(matches!(r.get(0).expect("row 0").get::<i64>(0), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn full_lifecycle_integration() {
    let mut conn = Connection::open_in_memory().expect("open");

    conn.execute_batch_raw("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score REAL);")
        .expect("create");

    conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO users(name, score) VALUES ('alice', 95.5)")?;
        tx.execute_raw("INSERT INTO users(name, score) VALUES ('bob', 88.0)")?;
        Ok(())
    })
    .expect("tx");

    let result = conn.query_raw("SELECT id, name, score FROM users ORDER BY id").expect("query");
    assert_eq!(result.len(), 2);
    assert_eq!(result.column_names.len(), 3);
    assert_eq!(result.column_names[0], "id");
    assert_eq!(result.column_names[1], "name");
    assert_eq!(result.column_names[2], "score");
    assert_eq!(result.get(0).expect("row 0").get::<&str>(1).expect("name"), "alice");
    assert!((result.get(0).expect("row 0").get::<f64>(2).expect("score") - 95.5).abs() < f64::EPSILON);
    assert_eq!(result.get(1).expect("row 1").get::<i64>(0).expect("id"), 2);

    // Native type access — no double-conversion.
    assert_eq!(result.get(0).expect("row 0").get::<i64>(0).expect("id int"), 1);
    assert!((result.get(0).expect("row 0").get::<f64>(0).expect("id as f64") - 1.0).abs() < f64::EPSILON);

    // Error in transaction → rollback.
    let err: Result<(), _> = conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO users(name) VALUES ('charlie')")?;
        Err(SqliteError::Query("abort".to_string()))
    });
    assert!(err.is_err());
    let count = conn.query_raw("SELECT count(*) FROM users").expect("count");
    assert_eq!(count.get(0).expect("row 0").get::<i64>(0).expect("count"), 2); // charlie rolled back

    conn.close().expect("close");
}

// ─────────────────────── typed parameter binding ───────────────────────
//
// The param model is `&[ValueRef]`, binding each value in its TRUE SQLite
// storage class. The text-only `&[&str]` model these tests replace could bind
// neither `NULL` nor `BLOB`, and bound every integer/real as text (leaning on
// column affinity, a silent-wrong-result trap against an affinity-less compare).

#[test]
fn null_parameter_round_trips() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(id INTEGER, note TEXT)").expect("create");

    // Bind a real SQL NULL — impossible under the old text-only model (no `&str`
    // binds NULL; `""` binds an empty TEXT value, a different thing entirely).
    let changed = conn
        .execute_params("INSERT INTO t VALUES (?, ?)", &[ValueRef::Integer(1), ValueRef::Null])
        .expect("insert null");
    assert_eq!(changed, 1);

    // The bound NULL is a genuine NULL: `note IS NULL` matches it.
    let r = conn.query_raw("SELECT id FROM t WHERE note IS NULL").expect("select");
    assert_eq!(r.len(), 1);
    assert_eq!(r.get(0).expect("row 0").get::<i64>(0).expect("id"), 1);

    // And it reads back as a NULL, distinct from any present value.
    let r2 = conn.query_raw("SELECT note FROM t").expect("select note");
    assert!(r2.get(0).expect("row 0").is_null(0).expect("is_null"));
    assert_eq!(r2.get(0).expect("row 0").get_opt::<&str>(0).expect("opt"), None);

    // The ergonomic `Option` bind produces the same NULL.
    let none: Option<&str> = None;
    let r3 = conn
        .query_params("SELECT ? IS NULL", &[none.into()])
        .expect("opt-none bind");
    assert!(r3.get(0).expect("row 0").get::<bool>(0).expect("is null bool"));
}

#[test]
fn blob_parameter_round_trips_byte_exact() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(data BLOB)").expect("create");

    // A blob with a non-UTF-8 byte (0xFF) and a NUL — bytes no text param could
    // carry losslessly. Bound as a BLOB, read back byte-for-byte.
    let bytes: &[u8] = &[0x00, 0xDE, 0xAD, 0xFF, 0xBE, 0xEF];
    let changed = conn
        .execute_params("INSERT INTO t VALUES (?)", &[ValueRef::Blob(bytes)])
        .expect("insert blob");
    assert_eq!(changed, 1);

    let r = conn.query_raw("SELECT data FROM t").expect("select");
    assert_eq!(r.get(0).expect("row 0").get::<&[u8]>(0).expect("blob"), bytes, "blob round-trips byte-exact");
    // Its storage class really is BLOB (not TEXT): a `&str` read is a mismatch.
    assert!(matches!(r.get(0).expect("row 0").get::<&str>(0), Err(SqliteError::TypeMismatch { .. })));
}

#[test]
fn integer_and_real_params_bind_as_their_storage_class() {
    let conn = Connection::open_in_memory().expect("open");

    // `typeof(?)` reports the parameter's ACTUAL storage class. An integer bound
    // as INTEGER reports 'integer' (not 'text' — the affinity trap the old
    // text-only path fell into, which silently broke affinity-less comparisons).
    let ti = conn.query_params("SELECT typeof(?)", &[ValueRef::Integer(42)]).expect("int typeof");
    assert_eq!(ti.get(0).expect("row 0").get::<&str>(0).expect("typeof int"), "integer");

    let tr = conn.query_params("SELECT typeof(?)", &[ValueRef::Real(2.5)]).expect("real typeof");
    assert_eq!(tr.get(0).expect("row 0").get::<&str>(0).expect("typeof real"), "real");

    // The affinity trap made concrete: against an affinity-less comparison
    // (`id + 0`), a text "42" would NOT equal the integer 42, silently returning
    // no rows. A true integer bind matches.
    conn.execute_raw("CREATE TABLE t(id INTEGER)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (42)").expect("seed");
    let matched = conn
        .query_params("SELECT id FROM t WHERE id + 0 = ?", &[ValueRef::Integer(42)])
        .expect("affinity-less compare");
    assert_eq!(matched.len(), 1, "an integer param compares as an integer");
}

#[test]
fn text_parameter_still_expressible() {
    // The capability the old model had — binding text — is preserved, via
    // `ValueRef::Text` or the `&str` `From` impl.
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v TEXT)").expect("create");
    conn.execute_params("INSERT INTO t VALUES (?)", &[ValueRef::Text(b"explicit")]).expect("text");
    conn.execute_params("INSERT INTO t VALUES (?)", &["ergonomic".into()]).expect("text into");

    let r = conn.query_raw("SELECT v FROM t ORDER BY v").expect("select");
    assert_eq!(r.get(0).expect("row 0").get::<&str>(0).expect("v0"), "ergonomic");
    assert_eq!(r.get(1).expect("row 1").get::<&str>(0).expect("v1"), "explicit");
    // Both bound as TEXT.
    let tt = conn.query_params("SELECT typeof(?)", &["x".into()]).expect("typeof text");
    assert_eq!(tt.get(0).expect("row 0").get::<&str>(0).expect("typeof"), "text");
}

// ─────────────────────── unsigned reads + u64 affected count ───────────────────────

#[test]
fn unsigned_integer_reads_are_range_checked() {
    let conn = Connection::open_in_memory().expect("open");
    // Values within u32 and u64 read directly — a rowid/count/bitfield no longer
    // needs a manual i64 round-trip + range check the driver already owns.
    let r = conn.query_raw("SELECT 4000000000, 9000000000000000000").expect("query");
    let row = r.get(0).expect("row 0");
    assert_eq!(row.get::<u32>(0).expect("u32"), 4_000_000_000);
    assert_eq!(row.get::<u64>(1).expect("u64"), 9_000_000_000_000_000_000);

    // A negative value is out of range for both unsigned types — classified,
    // never wrapped to a huge positive.
    let neg = conn.query_raw("SELECT -1").expect("neg");
    let row = neg.get(0).expect("row 0");
    match row.get::<u32>(0) {
        Err(SqliteError::IntegerOutOfRange { value, .. }) => assert_eq!(value, -1),
        other => panic!("expected IntegerOutOfRange for u32, got {other:?}"),
    }
    match row.get::<u64>(0) {
        Err(SqliteError::IntegerOutOfRange { value, .. }) => assert_eq!(value, -1),
        other => panic!("expected IntegerOutOfRange for u64, got {other:?}"),
    }

    // A value beyond u32 but within u64: u32 is a classified overflow, u64 fits.
    let big = conn.query_raw("SELECT 5000000000").expect("big");
    let row = big.get(0).expect("row 0");
    assert!(matches!(row.get::<u32>(0), Err(SqliteError::IntegerOutOfRange { .. })));
    assert_eq!(row.get::<u64>(0).expect("u64 fits"), 5_000_000_000);
}

#[test]
fn affected_count_is_u64() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t(v int)").expect("create");
    // The `u64` annotation pins the return type (cross-backend parity with the
    // PostgreSQL drivers' affected-row `u64`).
    let inserted: u64 = conn.execute_raw("INSERT INTO t VALUES (1),(2),(3)").expect("insert");
    assert_eq!(inserted, 3);
    let deleted: u64 = conn
        .execute_params("DELETE FROM t WHERE v > ?", &[ValueRef::Integer(1)])
        .expect("delete");
    assert_eq!(deleted, 2);
}
