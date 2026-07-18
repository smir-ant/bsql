//! Escape-wall for the streaming read path: a borrow lent by a `BorrowedRow`
//! inside a `query_each` callback CANNOT escape it. The `for<'r>` bound on the
//! callback forces it to accept a row at ANY lifetime, so stashing a `&str`
//! pulled from the row into a `Vec` that outlives the callback — which would
//! fix the borrow to a longer lifetime — is a borrow-checker error. This is
//! the compile-time half of "a streamed borrow is valid only inside the call".

use core::ops::ControlFlow;

use bsql_sqlite::Connection;

fn escape(conn: &Connection) {
    // `stash` outlives the callback. Pushing a borrow taken from the row into
    // it would let that borrow escape the `for<'r>` bound — the violation.
    let mut stash: Vec<&str> = Vec::new();
    let _ = conn.query_each_raw("SELECT 'x'", |row| {
        let s = row.get::<&str>(0).expect("str");
        stash.push(s);
        ControlFlow::<()>::Continue(())
    });
    let _ = stash;
}

fn main() {}
