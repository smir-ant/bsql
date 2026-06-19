#![forbid(unsafe_code)]
#![allow(clippy::disallowed_methods, reason = "test/bench harness — fixtures use the sanctioned try_from(..).unwrap_or(SAT) / slice.get(..).unwrap_or(&[]) dead-arm shape, not production data fallbacks")]
use bsql_sqlite::Connection;

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

    assert_eq!(result.rows[0].get_i32(0), Some(1));
    assert_eq!(result.rows[0].get_str(1), Some("alice"));
    assert_eq!(result.rows[1].get_i32(0), Some(2));
    assert_eq!(result.rows[1].get_str(1), Some("bob"));
}

#[test]
fn query_with_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").expect("insert");

    let result = conn.query_params("SELECT v FROM t WHERE id > ?", &["1"]).expect("select");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get_str(0), Some("b"));
    assert_eq!(result.rows[1].get_str(0), Some("c"));
}

#[test]
fn execute_with_params() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(id INTEGER, v TEXT)").expect("create");

    let changed = conn.execute_params(
        "INSERT INTO t VALUES (?, ?)",
        &["42", "hello"],
    ).expect("insert");
    assert_eq!(changed, 1);

    let result = conn.query("SELECT id, v FROM t").expect("select");
    assert_eq!(result.rows[0].get_i32(0), Some(42));
    assert_eq!(result.rows[0].get_str(1), Some("hello"));
}

#[test]
fn null_handling() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELECT 1, NULL, 'hello'").expect("select");
    assert_eq!(result.rows.len(), 1);

    let row = &result.rows[0];
    assert_eq!(row.len(), 3);
    assert_eq!(row.get_i32(0), Some(1));
    assert!(row.is_null(1));
    assert_eq!(row.get_raw(1), None);
    assert_eq!(row.get_str(2), Some("hello"));
}

#[test]
fn typed_access() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELECT 42, 2.5, 'text', 1, 0").expect("select");
    let row = &result.rows[0];

    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.get_i64(0), Some(42));
    assert_eq!(row.get_f64(1), Some(2.5));
    assert_eq!(row.get_str(2), Some("text"));
    assert_eq!(row.get_bool(3), Some(true));
    assert_eq!(row.get_bool(4), Some(false));
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
    assert_eq!(result.rows[0].get_i32(0), Some(0));
    assert_eq!(result.rows[999].get_i32(0), Some(999));
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
    assert_eq!(result.rows[0].get_i32(0), Some(1));
}

#[test]
fn blob_roundtrip() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(data BLOB)").expect("create");
    conn.execute("INSERT INTO t VALUES (X'DEADBEEF')").expect("insert");

    let result = conn.query("SELECT data FROM t").expect("select");
    assert_eq!(result.rows[0].get_raw(0), Some([0xDE, 0xAD, 0xBE, 0xEF].as_slice()));
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
        assert_eq!(result.rows[0].get_i32(0), Some(42));
        conn.close().expect("close");
    }

    let _ = std::fs::remove_file(&dir);
}

#[test]
fn column_names() {
    let conn = Connection::open_in_memory().expect("open");
    let result = conn.query("SELECT 1 AS id, 'hello' AS greeting").expect("query");
    assert_eq!(result.column_names, vec!["id", "greeting"]);
    assert_eq!(result.rows[0].get_by_name("greeting", &result.column_names), Some(b"hello".as_slice()));
    assert_eq!(result.rows[0].get_by_name("missing", &result.column_names), None);
    assert_eq!(result.rows[0].get_i32(0), Some(1));
}

#[test]
fn query_one_and_opt() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    conn.execute("INSERT INTO t VALUES (42)").expect("insert");

    let row = conn.query_one("SELECT v FROM t").expect("query_one");
    assert_eq!(row.get_i32(0), Some(42));

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
    assert_eq!(r.rows[0].get_i64(0), Some(1));

    conn.begin().expect("begin2");
    conn.execute("INSERT INTO t VALUES (2)").expect("insert2");
    conn.rollback().expect("rollback");
    let r = conn.query("SELECT count(*) FROM t").expect("count2");
    assert_eq!(r.rows[0].get_i64(0), Some(1));
}

#[test]
fn unicode_values() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v text)").expect("create");
    conn.execute("INSERT INTO t VALUES ('Привет мир')").expect("insert");
    let result = conn.query("SELECT v FROM t").expect("query");
    assert_eq!(result.rows[0].get_str(0), Some("Привет мир"));
}

#[test]
fn transaction_closure_commit() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    conn.transaction(|c| {
        c.execute("INSERT INTO t VALUES (1)")?;
        Ok(())
    }).expect("tx");
    let r = conn.query("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(1));
}

#[test]
fn transaction_closure_rollback_on_err() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    conn.execute("INSERT INTO t VALUES (1)").expect("seed");
    let err: Result<(), _> = conn.transaction(|c| {
        c.execute("INSERT INTO t VALUES (2)")?;
        Err(bsql_sqlite::SqliteError::Query("forced".to_string()))
    });
    assert!(err.is_err());
    let r = conn.query("SELECT count(*) FROM t").expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(1), "should have rolled back");
}

#[test]
fn transaction_closure_return_value() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t(v int)").expect("create");
    let count = conn.transaction(|c| {
        c.execute("INSERT INTO t VALUES (1)")?;
        c.execute("INSERT INTO t VALUES (2)")?;
        let r = c.query("SELECT count(*) FROM t")?;
        Ok(r.rows[0].get_i64(0).unwrap_or(0))
    }).expect("tx");
    assert_eq!(count, 2);
}

#[test]
fn typed_get_from_text() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 42, 2.5, 'hello'").expect("query");
    let row = &r.rows[0];
    assert_eq!(row.get::<i32>(0), Some(42));
    assert_eq!(row.get::<f64>(1), Some(2.5));
    assert_eq!(row.get::<String>(2), Some("hello".to_string()));
}

#[test]
fn execute_batch_multi_statement() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch("
        CREATE TABLE a(v int);
        CREATE TABLE b(v int);
        INSERT INTO a VALUES (1);
        INSERT INTO b VALUES (2);
    ").expect("batch");
    let r1 = conn.query("SELECT v FROM a").expect("a");
    let r2 = conn.query("SELECT v FROM b").expect("b");
    assert_eq!(r1.rows[0].get_i64(0), Some(1));
    assert_eq!(r2.rows[0].get_i64(0), Some(2));
}

#[test]
fn native_integer_access() {
    let conn = Connection::open_in_memory().expect("open");
    let r = conn.query("SELECT 42, 2.5, NULL").expect("query");
    let row = &r.rows[0];
    assert_eq!(row.get_i64(0), Some(42));
    assert_eq!(row.get_f64(1), Some(2.5));
    assert_eq!(row.get_f64(0), Some(42.0)); // Integer → f64 coercion
    assert!(row.is_null(2));
}

#[test]
fn get_f64_rejects_inexact_integer() {
    let conn = Connection::open_in_memory().expect("open");
    // An integer beyond f64's lossless integer range must not be returned as a
    // rounded approximation: get_f64 yields None ("read it as an integer").
    let r = conn.query("SELECT 9007199254740993").expect("query");
    let row = &r.rows[0];
    assert_eq!(row.get_i64(0), Some(9_007_199_254_740_993));
    assert_eq!(row.get_f64(0), None);
}

#[test]
fn full_lifecycle_integration() {
    let conn = Connection::open_in_memory().expect("open");

    conn.execute_batch("
        CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score REAL);
    ").expect("create");

    conn.transaction(|tx| {
        tx.execute("INSERT INTO users(name, score) VALUES ('alice', 95.5)")?;
        tx.execute("INSERT INTO users(name, score) VALUES ('bob', 88.0)")?;
        Ok(())
    }).expect("tx");

    let result = conn.query("SELECT id, name, score FROM users ORDER BY id").expect("query");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.column_names, vec!["id", "name", "score"]);
    assert_eq!(result.rows[0].get_str(1), Some("alice"));
    assert_eq!(result.rows[0].get_f64(2), Some(95.5));
    assert_eq!(result.rows[1].get_i64(0), Some(2));

    // Native type access — no double-conversion
    assert_eq!(result.rows[0].get_i64(0), Some(1)); // Integer direct
    assert_eq!(result.rows[0].get_f64(0), Some(1.0)); // Integer → f64 coercion

    // Error in transaction → rollback
    let err: Result<(), _> = conn.transaction(|tx| {
        tx.execute("INSERT INTO users(name) VALUES ('charlie')")?;
        Err(bsql_sqlite::SqliteError::Query("abort".to_string()))
    });
    assert!(err.is_err());
    let count = conn.query("SELECT count(*) FROM users").expect("count");
    assert_eq!(count.rows[0].get_i64(0), Some(2)); // charlie rolled back

    conn.close().expect("close");
}
