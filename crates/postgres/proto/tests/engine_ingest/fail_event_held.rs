// EXPECT: E0499 — the `Event` returned by `next_event` borrows `&mut buf`;
// calling `read_slot` while that event is still live is a second mutable
// borrow of `buf` at the same time. The no-escape wall: a borrow-through
// event cannot outlive the next mutating call.
use bsql_postgres_proto::engine::{Event, IngestBuf};

fn main() {
    let mut buf = IngestBuf::new();
    let ev = buf.next_event();
    // Next mutating call while the borrow-through event is still live:
    let _slot = buf.read_slot(64).expect("slot");
    // Use `ev` after the mutating call so the borrow checker must keep it
    // alive across that call — E0499.
    match ev {
        Event::Row(body) => {
            let _ = body.len();
        }
        _ => {}
    }
}
