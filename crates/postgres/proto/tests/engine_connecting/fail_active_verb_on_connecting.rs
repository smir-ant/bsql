// EXPECT: E0599 — `backend_pid` is an active-phase verb living on
// `ActiveEngine` (produced by `ConnectingEngine::into_active`). Calling it on a
// connecting-phase handle is method-not-found: a query/cancel cannot be issued
// before the handshake completes.
use bsql_postgres_proto::engine::ConnectingEngine;

fn use_active_verb_on_connecting(engine: &ConnectingEngine) -> i32 {
    engine.backend_pid()
}

fn main() {}
