//! Probe **P-D278D-1** — `with_cancel_request()`
//! is method-absent on `<DisconnectedPhase>`.
//!
//! Tier-1 by construction: the method lives ONLY on
//! `impl PgProtocol<ActivePhase>` (see `src/protocol.rs`). Calling it
//! on `<DisconnectedPhase>` returns E0599 — phase has no such method.
//!
//! Rationale: before handshake, the backend has not emitted
//! `BackendKeyData`, so there is no wire frame to materialise. A
//! method that always returned `None` (without invoking the closure)
//! would be a tier-3 by-runtime-discriminate API; method-absence is
//! tier-1 by-construction.

extern crate bsql_pg_proto;

use bsql_pg_proto::PgProtocol;

fn main() {
    let proto = PgProtocol::new();
    // proto is <DisconnectedPhase>; with_cancel_request does NOT exist.
    let _ = proto.with_cancel_request(|_bytes, _pid| ());
}
