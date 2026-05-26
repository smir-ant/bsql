//! Probe **P-E-3** — `push_command::Startup` is
//! deleted; the only path is `<DisconnectedPhase>::push_startup`.
//! External code that imports the struct fails with E0432.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::push_command::Startup;

fn main() {
    let _: Option<Startup> = None;
}
