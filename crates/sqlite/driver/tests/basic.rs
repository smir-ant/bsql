#![forbid(unsafe_code)]
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
    let result = conn.query("SELECT 42, 3.14, 'text', 1, 0").expect("select");
    let row = &result.rows[0];

    assert_eq!(row.get_i32(0), Some(42));
    assert_eq!(row.get_i64(0), Some(42));
    assert_eq!(row.get_f64(1), Some(3.14));
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
