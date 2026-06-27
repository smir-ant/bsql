//! Connecting/active phase-separation — `trybuild` golden harness.
//!
//! The new engine splits the handshake into two handles: [`ConnectingEngine`]
//! (drives the auth flow, exposes `next_auth_event` / `into_active`) and the
//! [`ActiveEngine`] it produces (exposes the active-phase verbs `backend_pid` /
//! `tx_status` / `with_secret_key`). Those active verbs are *absent* on the
//! connecting handle, so issuing one before the handshake completes is a
//! method-not-found compile error, not a runtime guard.
//!
//! [`ConnectingEngine`]: bsql_postgres_proto::engine::ConnectingEngine
//! [`ActiveEngine`]: bsql_postgres_proto::engine::ActiveEngine
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto \
//!     --test engine_connecting_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// Calling the active-phase `backend_pid` verb on a connecting-phase handle is
/// E0599 (method not found) — a query/cancel cannot be issued before the
/// handshake completes.
#[test]
fn active_verb_on_connecting_is_e0599() {
    trybuild::TestCases::new()
        .compile_fail("tests/engine_connecting/fail_active_verb_on_connecting.rs");
}
