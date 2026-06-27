// EXPECT: E0499 — the `&mut [u8]` lent by `read_slot` borrows `&mut buf`;
// calling `read_slot` again while that slot is still live is a second
// mutable borrow of `buf` at the same time. The no-escape wall: a lent slot
// cannot outlive the next mutating call.
use bsql_postgres_proto::engine::IngestBuf;

fn main() {
    let mut buf = IngestBuf::new();
    let slot = buf.read_slot(64).expect("first slot");
    // Second mutating call while the first lent slot is still live:
    let _second = buf.read_slot(64).expect("second slot");
    // Use `slot` after the second call so the borrow checker must keep it
    // alive across that call — E0499.
    slot[0] = 1;
}
