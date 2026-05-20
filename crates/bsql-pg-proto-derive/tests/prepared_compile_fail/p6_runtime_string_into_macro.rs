//! Hostile-bypass probe **P6** — feed a runtime string
//! (variable, expression, or macro invocation other than the
//! literal itself) into `prepared!`.
//!
//! # Tier
//!
//! Tier-1 by-construction. The macro's `prepared_impl` first action
//! is `syn::parse2::<LitStr>(input)`; any non-string-literal token
//! produces `Err(syn::Error)` that the public entry point converts
//! to `compile_error!` with the offending span.
//!
//! # Expected diagnostic
//!
//! `error: prepared!: SQL must be a single string literal (not an
//! identifier, expression, or macro invocation). Use
//! prepared!("SELECT ..."). Note: concat!("a", "b") is NOT accepted
//! at the proc-macro level - proc-macros see their arguments as raw
//! token-streams before other macros expand. Use one literal.`
//!
//! # Why this probe matters
//!
//! Without this rejection, a hostile caller could route untrusted
//! string data into the macro: `prepared!(user_input)`. That would
//! defeat the macro's compile-time SQL validation and re-open the
//! injection class entirely. Proc-macro receives its argument as a
//! raw `TokenStream` BEFORE other macros expand, so `concat!`
//! interpolation is also rejected at the LitStr parse step (correct
//! tier-1 behaviour: the macro accepts ONE syn::LitStr token).
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P6.

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

fn main() {
    // P6 attack: pass an identifier instead of a string literal.
    let hostile: &str = "DROP TABLE users; --";
    let _q: PreparedQuery<(), ()> = prepared!(hostile);
}
