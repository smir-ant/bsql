//! Probe **P-E-1** — `new_active_for_test()` does
//! not exist anywhere on `PgProtocol`. A naive transitional
//! shape `pub #[doc(hidden)] fn new_active_for_test()` is deleted in
//! the same commit; the only public constructor is `PgProtocol::new`
//! producing `<DisconnectedPhase>`.

extern crate bsql_pg_proto;

use bsql_pg_proto::PgProtocol;

fn main() {
    let _proto = PgProtocol::new_active_for_test();
}
