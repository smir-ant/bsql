//! Hostile-bypass probe **P10** — `core::mem::transmute`
//! a raw byte array into a `PreparedQuery`.
//!
//! # Tier
//!
//! - **Language-level (this trybuild file)**: tier-1 by-construction.
//!   `#![forbid(unsafe_code)]` at probe-file scope mechanically
//!   rejects the `unsafe { transmute(...) }` block.
//! - **OS-level**: same OS-boundary class as P4/P5. Even without
//!   forbid, a transmuted struct points into stack or `.rodata`
//!   that the OS protects; mutation would segfault.
//!
//! # What this probe pins
//!
//! `mem::transmute` is `unsafe`-only by definition; forbid bans the
//! `unsafe` block at the file scope. This is the same architectural
//! statement as P4/P5 but for a different specific UB pattern.
//!
//! # Expected diagnostic
//!
//! `error: usage of an 'unsafe' block` (from `#[forbid(unsafe_code)]`).
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P10 + §12 OS-boundary framing.

#![forbid(unsafe_code)]

extern crate bsql_pg_proto;

use bsql_pg_proto::PreparedQuery;

fn main() {
    // P10 attack: mem::transmute a byte array into a PreparedQuery.
    // Forbid-bundle at this file's scope rejects the unsafe block.
    let bytes: [u8; 96] = [0; 96];
    let _hostile: PreparedQuery<(), ()> = unsafe { core::mem::transmute(bytes) };
}
