//! Hostile-bypass probe **P5** — fabricate a hostile `&str`
//! via `unsafe` and route it through a fake `PreparedQuery`.
//!
//! # Tier
//!
//! - **Language-level (this trybuild file)**: tier-1 by-construction.
//!   `#![forbid(unsafe_code)]` at probe-file scope mechanically
//!   rejects any `unsafe` block.
//! - **OS-level**: see P4. Even without forbid, the resulting
//!   pointer write into `.rodata` would segfault.
//!
//! # What this probe pins
//!
//! Same OS-boundary class as P4. The architecture-level statement
//! ("consumer-side `unsafe` is the consumer's signed contract; our
//! crate carries `#![forbid(unsafe_code)]`") doesn't change with
//! the specific UB pattern; this file pins it for `mem::transmute`-
//! style fabrication.
//!
//! # Expected diagnostic
//!
//! `error: usage of an 'unsafe' block` (from `#[forbid(unsafe_code)]`).
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P5 + §12 OS-boundary framing.

#![forbid(unsafe_code)]

extern crate bsql_pg_proto;

use bsql_pg_proto::PreparedQuery;
use core::marker::PhantomData;

fn main() {
    // P5 attack: fabricate a PreparedQuery via unsafe pointer cast.
    // Forbid-bundle at this file's scope mechanically rejects the
    // unsafe block.
    #[repr(C)]
    struct Mirror {
        sql: &'static str,
        stmt_name: &'static str,
        param_oids: &'static [u32],
        row_oids: &'static [u32],
        parse_template: &'static [u8],
        bind_execute_prefix: &'static [u8],
        _phantom: PhantomData<fn(()) -> ()>,
    }
    let mirror = Mirror {
        sql: "DROP TABLE users; --",
        stmt_name: "x",
        param_oids: &[],
        row_oids: &[],
        parse_template: &[],
        bind_execute_prefix: &[],
        _phantom: PhantomData,
    };
    let _hostile: &PreparedQuery<(), ()> = unsafe {
        &*(&mirror as *const Mirror as *const PreparedQuery<(), ()>)
    };
}
