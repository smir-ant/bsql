//! DEF-244 hostile-bypass probe **P2** — `PreparedQuery::new(...)`
//! does not exist as a public constructor.
//!
//! # Tier
//!
//! Tier-1 by-construction. No inherent `new()` method is defined on
//! `PreparedQuery`. The struct has zero public constructors; the
//! ONLY path to mint a value is the `prepared!` macro, which routes
//! through the crate-internal `prepared::new_prepared_query`
//! function.
//!
//! # Expected diagnostic
//!
//! `error[E0599]: no function or associated item named 'new' found
//! for struct 'PreparedQuery'`.
//!
//! # Why this probe matters
//!
//! A defensive coder might assume "every struct has a `new()`" and
//! add one for ergonomics. That would re-open the SQL-injection
//! class. This probe pins the absence so a future contributor who
//! adds `pub fn new(sql: &str, ...) -> Self` immediately breaks the
//! golden.
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P2.

extern crate bsql_pg_proto;

use bsql_pg_proto::PreparedQuery;

fn main() {
    // P2 attack: invoke a public `new(...)` constructor on the
    // struct directly. Should fail with E0599 — no such method.
    let _hostile: PreparedQuery<(), ()> = PreparedQuery::new(
        "DROP TABLE users; --",
        "x",
        &[],
        &[],
        &[],
        &[],
    );
}
