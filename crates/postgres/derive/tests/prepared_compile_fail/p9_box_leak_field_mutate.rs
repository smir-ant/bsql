//! Hostile-bypass probe **P9** — `Box::leak` + field mutate
//! to obtain `&'static mut PreparedQuery` and overwrite `sql`.
//!
//! # Tier
//!
//! Tier-1 by-construction. Same mechanism as P1: `Box::new(...)` of
//! a `PreparedQuery` literal requires writing the struct fields,
//! all of which are `pub(crate)`. External crates cannot mint the
//! literal, so `Box::leak` has nothing to leak. The mutation step
//! never gets reached.
//!
//! # Expected diagnostic
//!
//! `error[E0451]: field 'sql' of struct 'PreparedQuery' is private`
//! (plus the same E0451 for the other private fields).
//!
//! # Why this probe matters
//!
//! `Box::leak` is a common trick to convert owned data to `&'static
//! mut`. A naive defence ("we use immutable `&'static` references")
//! could be bypassed if construction were allowed. The `pub(crate)`
//! visibility on fields closes this at the construction step,
//! before `Box::leak` even enters the picture.
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P9.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;
use core::marker::PhantomData;

fn main() {
    // P9 attack: Box::leak + mutate. Should fail with E0451 at the
    // struct literal step (Box::new sees the private fields).
    let _leaked: &'static mut PreparedQuery<(), ()> = Box::leak(Box::new(PreparedQuery {
        sql: "DROP TABLE users; --",
        stmt_name: "x",
        param_oids: &[],
        row_oids: &[],
        parse_template: &[],
        bind_execute_prefix: &[],
        _phantom: PhantomData,
    }));
}
