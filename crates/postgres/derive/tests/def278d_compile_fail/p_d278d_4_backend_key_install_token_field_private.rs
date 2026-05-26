//! Probe **P-D278D-4** — the
//! `BackendKeyInstallToken` cannot be minted from outside
//! the `_backend_key_install_leaf` submodule (which is itself
//! `pub(crate)`, so unreachable from external crates).
//!
//! Tier-1 by construction:
//! - `_backend_key_install_leaf` module is `pub(crate)` — external
//!   code cannot name the module path (E0603).
//! - Even if reachable, `BackendKeyInstallToken(())` literal would
//!   be rejected by the field-private `()` payload (E0451 / E0603
//!   on the field).
//!
//! This probe exercises the outer gate (module-private) since the
//! inner gate (field-private) is unreachable through an external
//! crate's import. The combination ensures the token cannot be
//! minted by any code path that does not live inside
//! `bsql_postgres_proto::protocol::_backend_key_install_leaf`.

extern crate bsql_postgres_proto;

fn main() {
    // The module `_backend_key_install_leaf` is `pub(crate)` —
    // external code cannot import it. E0603 expected.
    let _token = bsql_postgres_proto::protocol::_backend_key_install_leaf::BackendKeyInstallToken(());
    let _ = _token;
}
