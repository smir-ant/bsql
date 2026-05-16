//! DEF-246 Phase 3 elevation #2 probe **P-E-5** — pushing a regular
//! command on `<ConnectingPhase>` is method-absent. Tier-1 by
//! construction: handshake phase has only `feed_inbound` /
//! `advance_one_frame` / `into_active`; no `push_command` /
//! `push_*` surface.

extern crate bsql_pg_proto;

use bsql_pg_proto::{Credentials, Ident, PgProtocol, WriteBuf};
use bsql_pg_proto::push_command::Ping;

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let startup_reply = proto.next_reply_id();
    let user = match Ident::try_from_str("u") {
        Ok(u) => u,
        Err(_) => return,
    };
    let (_, mut proto) = match proto.push_startup(
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
    // proto is now <ConnectingPhase>; `push_command` does NOT exist.
    // Direct call (no `as_ready` indirection) — the method does not
    // exist on `<ConnectingPhase>`.
    let reply = proto.next_reply_id();
    let _ = proto.push_command(Ping { reply }, &mut wb);
}
