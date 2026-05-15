//! DEF-244 hostile-bypass probe **P1** — direct struct construction
//! with hostile SQL.
//!
//! # Tier
//!
//! Tier-1 by-construction. All fields of [`PreparedQuery`] are
//! `pub(crate)`; external crates cannot mint the struct literal.
//!
//! # Expected diagnostic
//!
//! `error[E0451]: field 'sql' of struct 'PreparedQuery' is private`
//! (plus the same E0451 for every other field — rustc reports all
//! private-field violations in one shot).
//!
//! # Why this probe matters
//!
//! Without `pub(crate)`, a hostile caller could bypass the
//! `prepared!` macro entirely and mint a `PreparedQuery` carrying
//! arbitrary SQL — including SQL with embedded user data. That
//! would defeat the entire memo §7 closure.
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P1.

extern crate bsql_pg_proto;

use bsql_pg_proto::PreparedQuery;
use core::marker::PhantomData;

fn main() {
    // P1 attack: synthesise a PreparedQuery from outside the crate
    // with hostile SQL. Should fail with E0451 (private fields).
    let _hostile: PreparedQuery<(), ()> = PreparedQuery {
        sql: "DROP TABLE users; --",
        stmt_name: "x",
        param_oids: &[],
        row_oids: &[],
        parse_template: &[],
        bind_execute_prefix: &[],
        _phantom: PhantomData,
    };
}
