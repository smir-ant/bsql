//! DEF-246 Phase 4 elevation #3 probe **P-E-7** — `feed_inbound` is
//! method-absent on `<ClosedPhase>`. Tier-1 by construction: the
//! terminal phase absorbs no input.

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
    if connecting.feed_inbound(&[b'X', 0xFF, 0xFF, 0xFF, 0xFF]).is_err() {
        return;
    }
    let _ = connecting.advance_one_frame(&mut wb);
    let mut closed = match connecting.into_active() {
        Ok(_) => return,
        Err(IntoActiveError::Closed(c)) => c,
        Err(_) => return,
    };
    // closed is <ClosedPhase>; `feed_inbound` does NOT exist.
    let _ = closed.feed_inbound(&[]);
}
