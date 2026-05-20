//! Probe **P-E-2** — `__test_bypass_into_active()`
//! does not exist on `PgProtocol<DisconnectedPhase>`. The
//! consume-self path is exclusively `push_startup → ConnectingPhase
//! → into_active` (the latter is a real classifier, not a bypass).

extern crate bsql_pg_proto;

use bsql_pg_proto::PgProtocol;

fn main() {
    let proto = PgProtocol::new();
    let _active = proto.__test_bypass_into_active();
}
