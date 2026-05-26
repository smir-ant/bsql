//! Hostile-bypass probe **P4** — mutate `Q.sql` through an
//! `unsafe` raw pointer.
//!
//! # Tier
//!
//! - **Language-level (this trybuild file)**: tier-1 by-construction.
//!   With `#![forbid(unsafe_code)]` at probe-file scope, the
//!   compile_fail outcome is mechanical drift detection — a future
//!   change that relaxed the forbid in a probe context would surface
//!   here.
//! - **OS-level (production runtime)**: `.rodata` is read-only at
//!   the OS level (segment protection via `mprotect(PROT_READ)`).
//!   Writing through a raw pointer to a `const` segfaults at
//!   runtime — stronger than the language guarantee. The same
//!   OS-boundary framing applies to the crate-wide
//!   `panic = "abort"` policy for cases the language alone cannot
//!   reject.
//!
//! # What this probe pins
//!
//! The language-level half: a probe that engages `unsafe` is
//! mechanically rejected at the probe-file boundary (forbid). The
//! OS-level half is architectural — `bsql-pg-proto` itself carries
//! `#![forbid(unsafe_code)]` at its root and consumer-side `unsafe`
//! is the consumer's signed contract (CREDO §1).
//!
//! # Expected diagnostic
//!
//! `error: usage of an 'unsafe' block` (from `#[forbid(unsafe_code)]`
//! at this file's scope). The exact wording can vary across rustc
//! versions; the golden pins the current diagnostic.
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P4 + §12 OS-boundary framing.

#![forbid(unsafe_code)]

extern crate bsql_postgres_proto;

use bsql_postgres_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(i32,), (i32,)> = prepared!("SELECT id::int4 WHERE id = $1::int4");

fn main() {
    // P4 attack: mutate Q.sql via a raw pointer cast. Compile-fail
    // at the `unsafe` block under `#![forbid(unsafe_code)]`.
    unsafe {
        let sql_ptr = &Q.sql as *const &str as *mut &str;
        sql_ptr.write("DROP TABLE users; --");
    }
}
