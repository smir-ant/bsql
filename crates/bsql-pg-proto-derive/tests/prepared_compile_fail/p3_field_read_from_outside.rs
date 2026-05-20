//! Hostile-bypass probe **P3** — read `Q.sql` directly from
//! outside the crate.
//!
//! # Tier
//!
//! Tier-1 by-construction. Field `sql` has `pub(crate)` visibility;
//! external code cannot project the field directly. The public
//! accessor `q.sql()` exists but returns `&'static str` of the
//! already-validated macro-emitted SQL — NOT a SQL-injection vector
//! because the returned string cannot be routed to a fresh
//! `Parse`/`SimpleQuery` that the macro hasn't already sanitised.
//!
//! # Expected diagnostic
//!
//! `error[E0616]: field 'sql' of struct 'PreparedQuery' is private`.
//!
//! # Why this probe matters
//!
//! Direct field reads might tempt a caller to splice the raw SQL
//! into a new query string ("SELECT ... WHERE id = " + q.sql), which
//! defeats the prepared-statement boundary. By making `sql` private,
//! the language rejects this at compile time. The accessor method
//! exists for diagnostic/debug use (returning a `&'static str` that
//! is already known-static-`.rodata`).
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P3.

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(i32,), (i32,)> = prepared!("SELECT id::int4 WHERE id = $1::int4");

fn main() {
    // P3 attack: read Q.sql to splice into a new query. Should fail
    // with E0616 (private field).
    let _hostile: &str = Q.sql;
    let _ = _hostile;
}
