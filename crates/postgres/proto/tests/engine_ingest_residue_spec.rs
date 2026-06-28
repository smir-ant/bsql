//! Residue gate for the single-residence ingest buffer.
//!
//! The retired `buf_compact_staleness_spec` pinned a POSITIVE residue-scrub:
//! the old `ReadBuf::compact` zeroized the abandoned tail so a stale byte could
//! never be read. The sans-IO [`IngestBuf`] replaces that with a stronger,
//! scrub-free design — zero-once-at-construction + a `filled` watermark — under
//! which the protocol-read side ([`IngestBuf::unread`]) only ever sees
//! `active[cursor..filled]`, while [`IngestBuf::read_slot`] lends the spare
//! `active[filled..]` purely as an overwrite destination. Stale bytes left in
//! the spare by an earlier write are therefore never published as protocol
//! data — not because they are scrubbed, but because the watermark never
//! advances over un-rewritten bytes.
//!
//! This gate proves that residue property behaviourally: it deliberately
//! leaves stale bytes in the spare and asserts they never surface through
//! `unread()` / `unread_len()`, including across a compaction.
//!
//! [`IngestBuf`]: bsql_postgres_proto::engine::IngestBuf
//! [`IngestBuf::unread`]: bsql_postgres_proto::engine::IngestBuf::unread
//! [`IngestBuf::read_slot`]: bsql_postgres_proto::engine::IngestBuf::read_slot

#![allow(
    clippy::expect_used,
    reason = "integration test — expect() is the loud failure signal; the crate-internal forbid bundle does not extend to tests/."
)]

use bsql_postgres_proto::engine::IngestBuf;

/// Write `bytes` through one `read_slot` + `commit` cycle (a "socket" write).
fn push(buf: &mut IngestBuf, bytes: &[u8]) {
    let slot = buf.read_slot(bytes.len()).expect("slot lent");
    let n = slot.len().min(bytes.len());
    slot[..n].copy_from_slice(&bytes[..n]);
    buf.commit(n).expect("commit");
    assert_eq!(n, bytes.len(), "the test pushes fit the buffer in one slot");
}

/// After a large committed-and-fully-consumed write, a smaller follow-up write
/// surfaces EXACTLY its own bytes — the stale tail of the first write that
/// physically remains in the spare is never published through `unread()`.
#[test]
fn committed_watermark_never_surfaces_stale_spare() {
    let mut buf = IngestBuf::new();

    // First write: a block of 0xAA, committed, then fully consumed.
    push(&mut buf, &[0xAA; 100]);
    assert_eq!(buf.unread_len(), 100);
    let consumed = buf.take_chunk(100).expect("chunk");
    assert_eq!(consumed, (0, 100));
    assert_eq!(buf.unread_len(), 0, "all consumed");

    // Second write: a SMALLER block of 0xBB. The buffer still physically holds
    // the 0xAA bytes in its spare, but the watermark must publish only the 10
    // freshly-committed 0xBB.
    push(&mut buf, &[0xBB; 10]);
    assert_eq!(buf.unread_len(), 10, "watermark publishes only the new write");
    assert_eq!(
        buf.unread(),
        &[0xBB; 10],
        "unread() must surface exactly the new bytes, never the stale 0xAA tail",
    );
    assert!(
        !buf.unread().contains(&0xAA),
        "no stale byte from the consumed first write may leak into the read side",
    );
}

/// A partial consume followed by another write keeps `unread()` equal to
/// (the unconsumed live bytes ++ the new bytes) with no stale prefix/tail,
/// whether or not the second `read_slot` triggered an internal compaction.
#[test]
fn live_bytes_survive_without_stale_tail_across_reuse() {
    let mut buf = IngestBuf::new();

    push(&mut buf, &[0xAA; 30]);
    // Consume the first 20; 10 live 0xAA remain unread.
    assert_eq!(buf.take_chunk(20).expect("chunk"), (0, 20));
    assert_eq!(buf.unread_len(), 10);

    // Append 5 fresh 0xBB. Regardless of whether the buffer compacted the live
    // [20..30] region down to the front or appended in place, the read side
    // must be exactly the 10 live 0xAA followed by the 5 new 0xBB.
    push(&mut buf, &[0xBB; 5]);
    assert_eq!(buf.unread_len(), 15);
    let mut expected = [0u8; 15];
    expected[..10].fill(0xAA);
    expected[10..].fill(0xBB);
    assert_eq!(
        buf.unread(),
        &expected,
        "unread() = live 0xAA tail ++ new 0xBB, with no stale residue from the consumed prefix",
    );
}

/// A frame parsed after a consume reflects only its own bytes — the borrow-out
/// `frame_body` never includes stale spare bytes beyond the frame's length.
#[test]
fn frame_body_excludes_stale_spare() {
    let mut buf = IngestBuf::new();

    // Prime the spare with 0xAA, then consume it all.
    push(&mut buf, &[0xAA; 64]);
    assert_eq!(buf.take_chunk(64).expect("chunk"), (0, 64));

    // Write one well-formed 5-byte-header frame with a 3-byte body. The frame's
    // declared length is 4 (length field) + 3 (body) = 7; tag 'D'.
    let mut framed = vec![b'D'];
    framed.extend_from_slice(&7u32.to_be_bytes());
    framed.extend_from_slice(&[0x01, 0x02, 0x03]);
    push(&mut buf, &framed);

    let (tag, start, end) = buf.take_frame().expect("one complete frame");
    assert_eq!(tag, b'D');
    assert_eq!(
        buf.frame_body(start, end),
        &[0x01, 0x02, 0x03],
        "frame body is exactly the 3 committed bytes — no stale 0xAA spillover",
    );
}
