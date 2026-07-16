//! Escape-wall for the STREAMING typed path: a borrowed record handed to a
//! `query_each` closure CANNOT escape it. The `for<'q>` HRTB on `on_row` forces
//! the closure to accept a record at ANY lifetime, so stashing one in a `Vec`
//! that outlives the closure — fixing the record's borrow to a longer lifetime —
//! is a borrowed-data-escapes error. This is the compile-time half of "a
//! streamed record is valid only inside the `on_row` call".
//!
//! The query projects a TEXT column, so the record `Escape<'q>` aliases the
//! transient ingest buffer (`s: &'q str`); a no-text record would be owned and
//! there would be nothing to escape.

use core::ops::ControlFlow;

use bsql_postgres_sync::Connection;

bsql::query!(Escape, "SELECT 'x'::text AS s");

fn escape(conn: &mut Connection) {
    // `v` outlives the closure. Pushing the borrowed record into it would let the
    // record escape the `for<'q>` bound — the borrow violation.
    let mut v = Vec::new();
    let _ = conn.query_each::<Escape, _, _>((), |row| {
        v.push(row);
        ControlFlow::<()>::Continue(())
    });
    let _ = v;
}

fn main() {}
