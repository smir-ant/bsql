//! DEF-278 Bundle D probe **P-D278D-3** — `cancel_request_credentials()`
//! is method-absent on `<ClosedPhase>`.
//!
//! Tier-1 by construction: the method lives ONLY on
//! `impl PgProtocol<ActivePhase>` (see `src/protocol.rs`). Calling it
//! on `<ClosedPhase>` returns E0599 — phase has no such method.
//!
//! Rationale: a terminally-closed connection cannot be cancelled
//! (the backend may have already torn down). The driver should
//! observe `<ClosedPhase>::cause()` for the typed error and discard
//! the wrapper.

extern crate bsql_pg_proto;

use bsql_pg_proto::{Credentials, Ident, IntoActiveError, PgProtocol, WriteBuf};

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let startup_reply = proto.next_reply_id();
    let user = match Ident::try_from_str("u") {
        Ok(u) => u,
        Err(_) => return,
    };
    let (_, mut connecting) = match proto.push_startup(
        user,
        None,
        None,
        Credentials::Trust,
        startup_reply,
        &mut wb,
    ) {
        Ok(p) => p,
        Err(_) => return,
    };
    // Drive into Errored via an unexpected frame.
    if connecting.feed_inbound(&[b'X', 0xFF, 0xFF, 0xFF, 0xFF]).is_err() {
        return;
    }
    let _ = connecting.advance_one_frame(&mut wb);
    let closed = match connecting.into_active() {
        Ok(_) => return,
        Err(IntoActiveError::Closed(c)) => c,
        Err(_) => return,
    };
    // closed is <ClosedPhase>; cancel_request_credentials does NOT exist.
    let _ = closed.cancel_request_credentials();
}
