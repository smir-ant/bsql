//! A row with the wrong NUMBER of fields is a compile error at the
//! `copy_in_typed` call. `copy_bulk` has four columns in the carrier, so a
//! two-field row does not match `BulkRow::Row<'q>` — arity is pinned by the
//! catalog column list.

use bsql_postgres_sync::Connection;

bsql::copy!(BulkRow, "copy_bulk", (id, label, note, amount));

fn wrong(conn: &mut Connection) {
    // Two fields where the carrier's row has four.
    let _ = conn.copy_in_typed::<BulkRow, _>([(1i64, "x")]);
}

fn main() {}
