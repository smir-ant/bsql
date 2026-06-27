// EXPECT: E0499 — the `Event` returned by `next_event` borrows `&mut self`;
// calling `read_slot` while that event is still live is a second mutable borrow
// of the engine at the same time. The no-escape wall: a borrow-through active
// event cannot outlive the next mutating call.
use bsql_postgres_proto::engine::{ActiveEngine, Event};

fn hold_event_across_mutation(engine: &mut ActiveEngine) {
    let ev = engine.next_event();
    // Next mutating call while the borrow-through event is still live:
    let _slot = engine.read_slot(64);
    // Use `ev` after the mutating call so the borrow checker must keep it alive
    // across that call — E0499.
    match ev {
        Event::Row(body) => {
            let _ = body.len();
        }
        _ => {}
    }
}

fn main() {}
