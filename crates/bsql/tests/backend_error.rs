//! Cross-backend error classification witness for [`bsql::BackendError`].
//!
//! Proves the trait reads a constraint class IDENTICALLY on PostgreSQL and
//! SQLite even though the two encode it differently (a 5-char SQLSTATE vs a
//! numeric extended result code): `is_unique_violation()` is true on a real
//! PostgreSQL `23505` AND a real `SQLITE_CONSTRAINT_UNIQUE`, and false on an
//! unrelated error on both. The SQLite half runs in-process (no server); the
//! PostgreSQL half has an offline half (a synthesised `DbError`, exercising the
//! SQLSTATE mapping without a database) plus an `#[ignore]` live half against a
//! real server.

#![forbid(unsafe_code)]

use bsql::BackendError;

// ─── PostgreSQL (offline: synthesised server error) ──────────────────────────

/// Build a `DriverError::Db` carrying SQLSTATE `code` — exercises the SQLSTATE
/// classification mapping without a live server. Not a `#[test]` fn, but it only
/// constructs values (no `unwrap`/`expect`/`panic`), so the floor is satisfied.
fn pg_db_error(code: &str) -> bsql::pg::DriverError {
    bsql::pg::DriverError::from(bsql::pg::DbError::new(
        code,
        Some("ERROR".to_string()),
        "synthetic classification fixture".to_string(),
        None,
        None,
    ))
}

#[test]
fn pg_driver_error_classifies_offline() {
    let unique = pg_db_error("23505");
    assert!(BackendError::is_unique_violation(&unique));
    assert!(!BackendError::is_not_null_violation(&unique));
    assert!(!BackendError::is_too_many_rows(&unique));
    assert_eq!(BackendError::sqlstate(&unique), Some("23505"));

    assert!(BackendError::is_not_null_violation(&pg_db_error("23502")));
    assert!(BackendError::is_foreign_key_violation(&pg_db_error("23503")));
    assert!(BackendError::is_check_violation(&pg_db_error("23514")));

    // An unrelated server error (undefined_table) is no constraint class, but its
    // SQLSTATE is still surfaced.
    let other = pg_db_error("42P01");
    assert!(!BackendError::is_unique_violation(&other));
    assert!(!BackendError::is_not_null_violation(&other));
    assert!(!BackendError::is_foreign_key_violation(&other));
    assert!(!BackendError::is_check_violation(&other));
    assert_eq!(BackendError::sqlstate(&other), Some("42P01"));

    // Cross-backend disconnect classification: a connection-broken SQLSTATE (the
    // `08` class, `57P01` admin shutdown) is a disconnect; a `57014` cancel and an
    // ordinary server error are not.
    assert!(BackendError::is_disconnect(&pg_db_error("08006")));
    assert!(BackendError::is_disconnect(&pg_db_error("57P01")));
    assert!(!BackendError::is_disconnect(&pg_db_error("57014")));
    assert!(!BackendError::is_disconnect(&unique));
    assert!(!BackendError::is_disconnect(&other));
}

// ─── SQLite (in-process: real engine errors) ─────────────────────────────────

#[test]
fn sqlite_error_classifies_in_process() {
    let conn = bsql::sqlite::Connection::open_in_memory().expect("open in-memory sqlite");
    conn.execute_raw("PRAGMA foreign_keys = ON").expect("enable fk enforcement");
    conn.execute_raw(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL, \
         qty INTEGER CHECK (qty > 0))",
    )
    .expect("create t");
    conn.execute_raw("CREATE TABLE parent (id INTEGER PRIMARY KEY)").expect("create parent");
    conn.execute_raw("CREATE TABLE child (pid INTEGER REFERENCES parent(id))")
        .expect("create child");
    conn.execute_raw("INSERT INTO t VALUES (1, 'a@x', 'alice', 5)").expect("seed row");

    // UNIQUE (duplicate email) — the real SQLITE_CONSTRAINT_UNIQUE.
    let uniq = conn
        .execute_raw("INSERT INTO t VALUES (2, 'a@x', 'bob', 5)")
        .expect_err("duplicate email must violate UNIQUE");
    assert!(BackendError::is_unique_violation(&uniq));
    assert!(!BackendError::is_not_null_violation(&uniq));
    assert_eq!(BackendError::sqlstate(&uniq), None);

    // PRIMARY KEY duplicate ALSO classifies as unique (PG's 23505 spans both).
    let pk = conn
        .execute_raw("INSERT INTO t VALUES (1, 'c@x', 'carol', 5)")
        .expect_err("duplicate PK must violate PRIMARY KEY");
    assert!(BackendError::is_unique_violation(&pk));

    // NOT NULL.
    let nn = conn
        .execute_raw("INSERT INTO t VALUES (3, 'd@x', NULL, 5)")
        .expect_err("NULL name must violate NOT NULL");
    assert!(BackendError::is_not_null_violation(&nn));
    assert!(!BackendError::is_unique_violation(&nn));

    // CHECK.
    let chk = conn
        .execute_raw("INSERT INTO t VALUES (4, 'e@x', 'dave', 0)")
        .expect_err("qty 0 must violate CHECK (qty > 0)");
    assert!(BackendError::is_check_violation(&chk));

    // FOREIGN KEY.
    let fk = conn
        .execute_raw("INSERT INTO child VALUES (999)")
        .expect_err("orphan child must violate FOREIGN KEY");
    assert!(BackendError::is_foreign_key_violation(&fk));

    // An unrelated error (syntax) is no constraint class at all.
    let syntax = conn.execute_raw("NOT VALID SQL").expect_err("syntax error");
    assert!(!BackendError::is_unique_violation(&syntax));
    assert!(!BackendError::is_not_null_violation(&syntax));
    assert!(!BackendError::is_foreign_key_violation(&syntax));
    assert!(!BackendError::is_check_violation(&syntax));
    assert!(!BackendError::is_too_many_rows(&syntax));

    // A constraint / syntax error is NOT a disconnect on SQLite either — the
    // handle stays usable (the in-process analogue of "the connection is fine").
    // (The broken-handle codes IOERR/CORRUPT are unit-tested in the sqlite crate;
    // they cannot be provoked against a healthy in-memory database here.)
    assert!(!BackendError::is_disconnect(&uniq));
    assert!(!BackendError::is_disconnect(&syntax));
    // The handle still answers a query — proving the errors above were not
    // connection-fatal.
    assert_eq!(
        conn.query_one_raw("SELECT 1").and_then(|r| r.get::<i64>(0)).ok(),
        Some(1_i64),
    );
}

// ─── PostgreSQL (live: real server error) ────────────────────────────────────

#[test]
#[ignore = "requires local PostgreSQL (127.0.0.1, user smir-ant, db postgres, trust) — see CLAUDE.md"]
fn pg_unique_violation_live() {
    use bsql::pg_sync::{ConnectConfig, Connection, SslMode};

    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable);
    let mut conn = Connection::connect(&cfg).expect("connect to local PostgreSQL");

    conn.execute_raw("DROP TABLE IF EXISTS bsql_backend_error_witness").expect("drop stale");
    conn.execute_raw(
        "CREATE TABLE bsql_backend_error_witness (id int PRIMARY KEY, email text UNIQUE NOT NULL)",
    )
    .expect("create witness table");
    conn.execute_raw("INSERT INTO bsql_backend_error_witness VALUES (1, 'a@x')")
        .expect("first insert");

    // A real 23505 from the server.
    let dup = conn
        .execute_raw("INSERT INTO bsql_backend_error_witness VALUES (2, 'a@x')")
        .expect_err("duplicate email must violate the UNIQUE constraint");
    assert!(
        BackendError::is_unique_violation(&dup),
        "a real 23505 must classify as unique: {dup:?}"
    );
    assert_eq!(BackendError::sqlstate(&dup), Some("23505"));

    // A real 23502 from the server (NULL into a NOT NULL column).
    let nn = conn
        .execute_raw("INSERT INTO bsql_backend_error_witness VALUES (3, NULL)")
        .expect_err("NULL email must violate NOT NULL");
    assert!(
        BackendError::is_not_null_violation(&nn),
        "a real 23502 must classify as not-null: {nn:?}"
    );

    conn.execute_raw("DROP TABLE bsql_backend_error_witness").expect("cleanup");
}
