// EXPECT: E0308 — the connecting and active phases have DISTINCT event
// vocabularies (`AuthEvent` vs `Event`). An active-phase frame classified into
// `Event` cannot masquerade as a connecting-phase `AuthEvent`: the impossible
// cross-phase pairing is a type mismatch by construction, not a runtime guard.
use bsql_postgres_proto::engine::{ActiveEngine, AuthEvent};

fn want_auth_event(_: AuthEvent<'_>) {}

fn feed_active_event_to_connecting_consumer(engine: &mut ActiveEngine) {
    // `next_event` yields `Event<'_>`; the connecting consumer wants
    // `AuthEvent<'_>` — E0308.
    want_auth_event(engine.next_event());
}

fn main() {}
