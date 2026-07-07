//! The happy path COMPILES (trybuild only compiles it — never run): a valid
//! `copy!` carrier and a `copy_in_typed` call with a matching row tuple
//! type-check on the blocking driver. The GREEN peer of the `copy_wrong_*`
//! compile-fail goldens.

use bsql_postgres_sync::Connection;

bsql::copy!(BulkRow, "copy_bulk", (id, label, note, amount));

fn ok(conn: &mut Connection) {
    // A matching row tuple: (i64, &str, Option<&str>, Option<i32>). Borrowed text
    // takes a non-'static lifetime (the rows are dropped at end of scope).
    let rows = vec![(1i64, "a", Some("n"), Some(10i32)), (2, "b", None, None)];
    let _ = conn.copy_in_typed::<BulkRow, _>(rows);
}

fn main() {}
