//! DEF-278 Bundle D' probe **P-D278D-5** — the
//! `CancelRequestCredentials` type from Bundle D is no longer
//! publicly exported. External code cannot import it (E0432
//! unresolved import).
//!
//! Tier-1 by construction: Bundle D' eliminated the public struct
//! entirely. The closure-scoped `with_cancel_request` lends a
//! `&[u8; 16]` directly; there is no longer any externally-nameable
//! type carrying the secret_key. Removing the type from the public
//! surface forecloses an entire class of misuse (struct-literal
//! construction via destructured fields, direct Debug, Clone via
//! `derive`-after-the-fact, etc.).
//!
//! The two cell-level types `BackendKey` and `BackendKeyCell` are
//! `pub(crate)` — even before Bundle D' they were unreachable from
//! external crates. This probe pins the Bundle-D' delta: the ONE
//! type that WAS publicly exported is gone.
//!
//! Filename is preserved from the Bundle-D revision for git-blame
//! continuity even though the probe target shifted from field
//! privacy (E0616) to import resolution (E0432). Comments updated.

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
