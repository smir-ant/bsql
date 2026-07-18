#![forbid(unsafe_code)]

//! Mixed witness: an ASYNC and a SYNC `#[bsql::test]` in the SAME test file,
//! each over its own driver. Proves the attribute dispatches on `async`-ness
//! per function (no whole-file flavor), that both run isolated (same table
//! name, distinct rows, each sees only its own), and that both clean up. Needs
//! a real PostgreSQL at `BSQL_TEST_DSN`; `#[ignore]`, run with `--ignored`.
//!
//! Run with, e.g.:
//! ```text
//! BSQL_TEST_DSN=postgres://smir-ant@localhost/postgres \
//!   cargo test -p bsql-test-harness-fixture -- --ignored
//! ```

// An async fn takes the async connection; a sync fn takes the sync connection.
// Both drivers are reached through `bsql` under the `test-harness` feature.

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn mixed_async_side(conn: &mut bsql::pg::Connection) {
    conn.execute_raw("CREATE TABLE mixed_shared (v int)").await.unwrap();
    conn.execute_raw("INSERT INTO mixed_shared (v) VALUES (1)").await.unwrap();

    let only = conn.query_one_raw("SELECT v FROM mixed_shared").await.unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(1)), "async side sees only its own row");

    let count = conn.query_one_raw("SELECT count(*) FROM mixed_shared").await.unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");

    let schema = conn.query_one_raw("SELECT current_schema()::text").await.unwrap();
    let name = schema.get_str(0).unwrap().unwrap();
    assert!(name.starts_with("bsql_t_"), "async side runs in a harness schema, got {name:?}");
}

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn mixed_sync_side(conn: &mut bsql::pg_sync::Connection) {
    conn.execute_raw("CREATE TABLE mixed_shared (v int)").unwrap();
    conn.execute_raw("INSERT INTO mixed_shared (v) VALUES (2)").unwrap();

    let only = conn.query_one_raw("SELECT v FROM mixed_shared").unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(2)), "sync side sees only its own row");

    let count = conn.query_one_raw("SELECT count(*) FROM mixed_shared").unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");

    let schema = conn.query_one_raw("SELECT current_schema()::text").unwrap();
    let name = schema.get_str(0).unwrap().unwrap();
    assert!(name.starts_with("bsql_t_"), "sync side runs in a harness schema, got {name:?}");
}
