//! Hostile-bypass probe **P12** — mutate the macro-baked
//! wire-template bytes (`parse_template` or `bind_execute_prefix`).
//!
//! # Tier
//!
//! - **Language-level (this trybuild file)**: tier-1 by-construction.
//!   `#![forbid(unsafe_code)]` at probe-file scope rejects the
//!   `unsafe` block needed for raw-pointer-write into `.rodata`.
//!   Additionally, `parse_template` is `pub(crate)` so external
//!   direct field access is E0616 (no `unsafe` even needed at the
//!   first attempt).
//! - **OS-level**: `.rodata` is read-only at the OS level. Writes
//!   through a raw pointer segfault. Same OS-boundary class as
//!   P4/P5/P10.
//!
//! # What this probe pins
//!
//! Field privacy + forbid-unsafe at probe-file scope. The macro
//! emits `parse_template: &'static [u8]` referencing a const-item
//! that LLVM places in `.rodata`; even with the field private, the
//! `static []` byte sequence is in read-only memory.
//!
//! # Expected diagnostic
//!
//! `error[E0616]: field 'parse_template' of struct 'PreparedQuery'
//! is private` (the visibility check fires before the unsafe-block
//! diagnostic in source order — rustc reports both, but the field
//! visibility is the first error encountered).
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P12 + §12 OS-boundary framing.

#![forbid(unsafe_code)]

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(i32,), (i32,)> = prepared!("SELECT id::int4 WHERE id = $1::int4");

fn main() {
    // P12 attack: read parse_template directly to harvest the bytes
    // for splicing, OR mutate via unsafe. Both fail compile:
    // - E0616 on the field read (pub(crate) field, not accessible
    //   from this crate);
    // - the unsafe block is rejected by the forbid bundle.
    let _hostile_bytes: &[u8] = Q.parse_template;
    let _ = _hostile_bytes;
    unsafe {
        let ptr = &Q.parse_template as *const &[u8] as *mut &[u8];
        ptr.write(b"hostile bytes");
    }
}
