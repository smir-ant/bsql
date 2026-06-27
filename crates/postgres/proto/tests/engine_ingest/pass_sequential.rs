// EXPECT: compiles — the sanctioned path. Each borrow (a lent slot, a
// borrow-through event) ends before the next mutating call, so the
// sequential read_slot/commit/next_event reuse the no-escape wall is built
// to permit is accepted.
use bsql_postgres_proto::engine::{Event, IngestBuf};

fn main() {
    let mut buf = IngestBuf::new();

    // Lend a slot, write into it, drop the slot borrow, then commit.
    {
        let slot = buf.read_slot(64).expect("slot");
        slot[0] = b'D';
        slot[1] = 0;
        slot[2] = 0;
        slot[3] = 0;
        slot[4] = 5;
        slot[5] = b'x';
    }
    buf.commit(6).expect("commit");

    // Pull the borrow-through event; the borrow ends at the end of the match.
    match buf.next_event() {
        Event::Row(_) | Event::NeedMore => {}
        _ => {}
    }

    // Another full cycle compiles fine — sequential reuse.
    {
        let _slot = buf.read_slot(64).expect("slot2");
    }
}
