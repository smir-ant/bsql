//! DEF-246 Phase 2 elevation #1 probe **P-E-4** — pushing a regular
//! command from `<DisconnectedPhase>` is method-absent.
//!
//! Tier-1 by construction: `push_command` lives on
//! `<ActivePhase>::push_command_internal` (called through
//! `ReadyGuard::push_command`); `<DisconnectedPhase>` exposes only
//! `push_startup`. Calling `push_command(Ping)` here returns E0599.

extern crate bsql_pg_proto;

use bsql_pg_proto::{PgProtocol, WriteBuf};
use bsql_pg_proto::push_command::Ping;

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply = proto.next_reply_id();
    // proto is <DisconnectedPhase>; `push_command` does NOT exist.
    // Direct call — the method is method-absent on <DisconnectedPhase>.
    let _ = proto.push_command(Ping { reply }, &mut wb);
}
