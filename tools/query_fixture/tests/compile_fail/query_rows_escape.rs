//! Escape-wall: a borrowed record from `Rows::iter()` borrows the `Rows`
//! buffer it was decoded from, so it CANNOT outlive that buffer. Dropping the
//! `Rows` while a borrowed record is still held is an `E0505` move-while-borrowed
//! error — the compile-time half of the "a borrowed record cannot escape its
//! prebuffer" invariant. A row that must outlive the buffer goes through
//! `Rows::into_owned()`.
//!
//! The query projects a TEXT column, so the borrowed record `Escape<'q>` aliases
//! the prebuffer (`s: &'q str`); for a no-text query the record would be owned
//! and there would be nothing to escape.

use bsql_postgres_sync::Rows;

bsql::query!(Escape, "SELECT 'x'::text AS s");

fn escape(rows: Rows<Escape>) {
    // `held` borrows `rows` (its `s` field aliases the prebuffer).
    let held = rows.iter().next();
    // Moving `rows` out (here, dropping it) while `held` still borrows it is the
    // borrow violation.
    drop(rows);
    // `held` is used after the move — forces the borrow to span the drop.
    let _ = held;
}

fn main() {}
