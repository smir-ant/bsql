//! Probe **P-E-6** — pushing a regular
//! command on `<ClosedPhase>` is method-absent.
//!
//! Tier-1 by construction: `<ClosedPhase>` exposes ONLY `cause()` —
//! no `push_command`, no `feed_inbound`, no `feed_bytes`, no
//! `advance_one_frame`. The protocol is terminal.

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
    let mut closed = match connecting.into_active() {
        Ok(_) => return,
        Err(IntoActiveError::Closed(c)) => c,
        Err(_) => return,
    };
    // closed is <ClosedPhase>; `push_command` does NOT exist on
    // `<ClosedPhase>` — calling it (or any push surface) is E0599.
    let _ = closed.push_command();
}
