//! A row whose column TYPE does not match the catalog is a compile error at the
//! `copy_in_typed` call. `copy_bulk.label` is `text` (`&str`), so supplying an
//! `i64` for it makes the row tuple not match `BulkRow::Row<'q>` — the
//! compile-time half of "a typed binary COPY row cannot carry a wrong-typed
//! value" (the whole point of the flagship over the raw `&[u8]` COPY).

use bsql_postgres_sync::Connection;

bsql::copy!(BulkRow, "copy_bulk", (id, label, note, amount));

fn wrong(conn: &mut Connection) {
    // `label` is `&str`; here it is an `i64`. The row tuple does not match the
    // carrier's `Row<'q>`, so the `IntoIterator<Item = Row<'q>>` bound fails.
    let _ = conn.copy_in_typed::<BulkRow, _>([(1i64, 2i64, None::<&str>, None::<i32>)]);
}

fn main() {}
