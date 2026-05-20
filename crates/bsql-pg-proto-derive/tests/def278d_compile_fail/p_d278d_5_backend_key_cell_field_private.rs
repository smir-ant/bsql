//! Probe **P-D278D-5** — `CancelRequestCredentials` is not in
//! the public surface. External code cannot import it (E0432
//! unresolved import).
//!
//! Tier-1 by construction: there is no publicly-exported struct
//! carrying cancel-request material. The closure-scoped
//! `with_cancel_request` lends a `&[u8; 16]` directly; there is no
//! externally-nameable type carrying the `secret_key`. Removing
//! the type from the public surface forecloses an entire class of
//! misuse (struct-literal construction via destructured fields,
//! direct `Debug`, `Clone` via `derive`-after-the-fact, etc.).
//!
//! The cell-level type `BackendKey` is `pub(crate)`, unreachable
//! from external crates.

extern crate bsql_pg_proto;

// E0432 expected — `CancelRequestCredentials` is not in
// `bsql_pg_proto`'s public surface post-Bundle-D'.
use bsql_pg_proto::CancelRequestCredentials;

fn _force_use() {
    // Force the import to actually be used so the compiler emits
    // the E0432 diagnostic on `use` rather than a "unused import"
    // warning that would not fail the build under the trybuild
    // golden contract.
    let _ = core::mem::size_of::<CancelRequestCredentials>();
}

fn main() {}
