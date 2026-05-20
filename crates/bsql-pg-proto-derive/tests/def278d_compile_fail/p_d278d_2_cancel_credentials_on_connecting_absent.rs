//! Probe **P-D278D-2** — `with_cancel_request()`
//! is method-absent on `<ConnectingPhase>`.
//!
//! Tier-1 by construction: the method lives ONLY on
//! `impl PgProtocol<ActivePhase>` (see `src/protocol.rs`). Calling it
//! on `<ConnectingPhase>` returns E0599 — phase has no such method.
//!
//! A driver wanting to cancel mid-handshake must drop the
//! connection; there is no production scenario where a pool cancels
//! a mid-handshake connection (cost of opening a new connection <
//! cost of debugging the cancel semantics). Method-absence
//! eliminates the runtime-classify ambiguity ("sometimes returns
//! Some, sometimes None based on whether K arrived").

extern crate bsql_pg_proto;

use bsql_pg_proto::{Credentials, Ident, PgProtocol, WriteBuf};

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let startup_reply = proto.next_reply_id();
    let user = match Ident::try_from_str("u") {
        Ok(u) => u,
        Err(_) => return,
    };
    let (_, proto) = match proto.push_startup(
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
    // proto is now <ConnectingPhase>; with_cancel_request does NOT exist.
    let _ = proto.with_cancel_request(|_bytes, _pid| ());
}
