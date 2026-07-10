//! Behavioural + footprint gates for the engine-owned send buffer and its
//! drain loop.
//!
//! Covers, from outside the crate boundary: the [`SendBuf`] cursor API
//! (enqueue / pending / advance / drain / lossless reset), the happy-path
//! drain to completion across both one-shot and partial-write transports, the
//! two classified flush failure modes (`WriteZero`, `SendOverrun`), the
//! pinned [`SendBuf`] footprint, and the `Send`-ness of the flush future. The
//! cancellation-unrolling proof and the zero-alloc proof live in sibling test
//! files (`engine_flush_cancel`, `engine_flush_alloc`).

#![forbid(unsafe_code)]

use bsql_postgres_proto::engine::{flush, EngineError, SendBuf, SendOverrun};
use core::convert::Infallible;
use core::future::{ready, Future};
use core::task::{Context, Poll};

// ---------------------------------------------------------------------------
// Dependency-free poll driver. The transports below are all immediately ready
// (no `Pending`), so this never spins.
// ---------------------------------------------------------------------------

fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = core::pin::pin!(f);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

// ---------------------------------------------------------------------------
// Transports exercising each flush outcome.
// ---------------------------------------------------------------------------

/// Accepts the whole offered tail in one attempt — drains any buffer in a
/// single loop iteration.
struct AcceptAll;

impl bsql_postgres_proto::engine::Transport for AcceptAll {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Accepts at most `CHUNK` bytes per attempt and records every accepted byte —
/// forces a multi-iteration drain and lets the test prove the reassembled
/// stream is byte-identical to the enqueued bytes.
struct ChunkRecorder {
    socket: Vec<u8>,
}

impl ChunkRecorder {
    const CHUNK: usize = 3;
    fn new() -> Self {
        Self { socket: Vec::new() }
    }
}

impl bsql_postgres_proto::engine::Transport for ChunkRecorder {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = buf.len().min(Self::CHUNK);
        self.socket.extend_from_slice(&buf[..n]);
        ready(Ok(n))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Accepts zero bytes from a non-empty buffer — the stalled/broken write side.
struct WriteZeroSink;

impl bsql_postgres_proto::engine::Transport for WriteZeroSink {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        _buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Reports accepting one more byte than it was offered — a contract violation
/// the send cursor must reject rather than overrun.
struct OverAccept;

impl bsql_postgres_proto::engine::Transport for OverAccept {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len() + 1))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// SendBuf cursor API.
// ---------------------------------------------------------------------------

#[test]
fn enqueue_then_advance_walks_the_cursor_to_drained() {
    let mut sb = SendBuf::new();
    assert!(sb.is_drained(), "a fresh buffer is drained");
    assert_eq!(sb.pending(), b"");
    assert_eq!(sb.pending_len(), 0);

    sb.enqueue(b"hello world");
    assert!(!sb.is_drained());
    assert_eq!(sb.pending(), b"hello world");
    assert_eq!(sb.pending_len(), 11);

    sb.advance(6).expect("6 <= pending");
    assert_eq!(sb.pending(), b"world");
    assert_eq!(sb.pending_len(), 5);
    assert!(!sb.is_drained());

    sb.advance(5).expect("5 == pending");
    assert_eq!(sb.pending(), b"");
    assert!(sb.is_drained());
}

#[test]
fn enqueue_appends_across_calls() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"abc");
    sb.enqueue(b"def");
    assert_eq!(sb.pending(), b"abcdef");
}

#[test]
fn advance_past_pending_is_a_classified_overrun_not_a_wrap() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"four");
    let err = sb.advance(5).expect_err("5 > pending(4) must be rejected");
    assert_eq!(
        err,
        SendOverrun {
            committed: 5,
            pending: 4
        }
    );
    // The cursor did not move — the buffer is unchanged and re-drainable.
    assert_eq!(sb.pending(), b"four");
    assert_eq!(sb.pending_len(), 4);
}

#[test]
fn reset_is_lossless_and_retains_capacity() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"abcdefgh");
    sb.advance(3).expect("3 <= 8");
    // Reset before a full drain: the unsent tail survives at the front.
    sb.reset();
    assert_eq!(sb.pending(), b"defgh", "unsent tail preserved across reset");
    assert_eq!(sb.pending_len(), 5);

    // Drain fully, then reset: the buffer empties and stays drained.
    sb.advance(5).expect("5 == pending");
    assert!(sb.is_drained());
    sb.reset();
    assert!(sb.is_drained());
    assert_eq!(sb.pending(), b"");
}

// ---------------------------------------------------------------------------
// flush() outcomes.
// ---------------------------------------------------------------------------

#[test]
fn flush_on_empty_buffer_is_ok_and_never_writes() {
    let mut sb = SendBuf::new();
    // WriteZeroSink would error if `write` were called; an empty buffer must
    // end the loop before any write attempt.
    let mut t = WriteZeroSink;
    let r: Result<(), EngineError<Infallible>> = block_on(flush(&mut sb, &mut t));
    assert!(r.is_ok());
    assert!(sb.is_drained());
}

#[test]
fn flush_drains_in_one_shot() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"a complete pipeline batch");
    let mut t = AcceptAll;
    block_on(flush(&mut sb, &mut t)).expect("flush drains");
    assert!(sb.is_drained());
}

#[test]
fn flush_drains_across_partial_writes_byte_identically() {
    let payload: &[u8] = b"P\0\0\0\x10stmt\0\0\0B\0\0\0\x0e\0stmt\0\0\0S\0\0\0\x04";
    let mut sb = SendBuf::new();
    sb.enqueue(payload);
    let mut t = ChunkRecorder::new();
    block_on(flush(&mut sb, &mut t)).expect("flush drains across partials");
    assert!(sb.is_drained());
    assert_eq!(
        t.socket.as_slice(),
        payload,
        "multi-write drain delivers the batch exactly once, in order"
    );
}

#[test]
fn flush_classifies_a_stalled_transport_as_write_zero() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"unsendable");
    let mut t = WriteZeroSink;
    let r: Result<(), EngineError<Infallible>> = block_on(flush(&mut sb, &mut t));
    assert!(
        matches!(r, Err(EngineError::WriteZero)),
        "Ok(0) on a non-empty buffer must be EngineError::WriteZero, got {r:?}"
    );
    // The cursor never advanced — nothing was reported sent.
    assert_eq!(sb.pending(), b"unsendable");
}

#[test]
fn flush_classifies_an_over_accepting_transport_as_send_overrun() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"five!");
    let mut t = OverAccept;
    let r: Result<(), EngineError<Infallible>> = block_on(flush(&mut sb, &mut t));
    assert!(
        matches!(
            r,
            Err(EngineError::SendOverrun(SendOverrun {
                committed: 6,
                pending: 5
            }))
        ),
        "accepting more than offered must be EngineError::SendOverrun, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Footprint + Send.
// ---------------------------------------------------------------------------

#[test]
fn flush_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}
    let mut sb = SendBuf::new();
    let mut t = AcceptAll;
    let fut = flush(&mut sb, &mut t);
    assert_send(&fut);
    // Drive it to completion too, so the assertion is on a real, usable future.
    block_on(fut).expect("flush");
}

// ---------------------------------------------------------------------------
// Transport-error + transport-flush coverage (a real, non-Infallible Error).
// ---------------------------------------------------------------------------

/// A real transport error type (not `Infallible`), so the
/// `map_err(EngineError::Transport)` arm and the `EngineError::Transport`
/// variant are actually exercised.
#[derive(Debug, PartialEq, Eq)]
struct IoFault(&'static str);

/// Records the ordered op log so a test can prove `flush` is called exactly
/// once and AFTER the last write. Accepts a fixed chunk per write (so a batch
/// takes several writes). `flush_err` makes the post-drain flush fail.
struct OpLog {
    ops: Vec<&'static str>,
    flush_err: bool,
}

impl OpLog {
    const CHUNK: usize = 4;
    fn new(flush_err: bool) -> Self {
        Self {
            ops: Vec::new(),
            flush_err,
        }
    }
}

impl bsql_postgres_proto::engine::Transport for OpLog {
    type Error = IoFault;
    fn is_would_block(_err: &Self::Error) -> bool {
        // `IoFault` models a hard write/IO fault, never a read deadline.
        false
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, IoFault>> + Send + 'a {
        ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, IoFault>> + Send + 'a {
        self.ops.push("write");
        ready(Ok(buf.len().min(Self::CHUNK)))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), IoFault>> + Send + 'a {
        self.ops.push("flush");
        if self.flush_err {
            ready(Err(IoFault("flush failed")))
        } else {
            ready(Ok(()))
        }
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), IoFault>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Accepts a bounded partial on its first write, then fails every subsequent
/// write — to drive the transport-error-mid-drain path and prove the cursor
/// reflects exactly the accepted partial.
struct FailAfterPartial {
    first: bool,
    partial: usize,
    socket: Vec<u8>,
}

impl bsql_postgres_proto::engine::Transport for FailAfterPartial {
    type Error = IoFault;
    fn is_would_block(_err: &Self::Error) -> bool {
        false
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, IoFault>> + Send + 'a {
        ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, IoFault>> + Send + 'a {
        if self.first {
            self.first = false;
            let n = self.partial.min(buf.len());
            self.socket.extend_from_slice(&buf[..n]);
            ready(Ok(n))
        } else {
            ready(Err(IoFault("write failed mid-drain")))
        }
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), IoFault>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), IoFault>> + Send + 'a {
        ready(Ok(()))
    }
}

#[test]
fn flush_drives_transport_flush_exactly_once_after_the_last_write() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"a batch large enough to take several chunked writes");
    let mut t = OpLog::new(false);
    block_on(flush(&mut sb, &mut t)).expect("flush drains");
    assert!(sb.is_drained());
    let flushes = t.ops.iter().filter(|o| **o == "flush").count();
    assert_eq!(flushes, 1, "transport flush must be driven exactly once");
    assert_eq!(
        t.ops.last(),
        Some(&"flush"),
        "the flush must come AFTER the last write"
    );
    assert!(
        t.ops.contains(&"write"),
        "writes must have happened before the flush"
    );
}

#[test]
fn flush_propagates_a_transport_flush_error() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"bytes that drain, then the flush fails");
    let mut t = OpLog::new(true);
    let r = block_on(flush(&mut sb, &mut t));
    assert!(
        matches!(r, Err(EngineError::Transport(IoFault(_)))),
        "a flush() failure must surface as EngineError::Transport, got {r:?}"
    );
    // The buffer drained (all writes completed) before the flush failed.
    assert!(sb.is_drained());
}

#[test]
fn transport_error_mid_drain_leaves_cursor_at_the_partial_then_resumes_byte_identically() {
    let full: &[u8] = b"a multi-write payload that fails partway through";
    let mut sb = SendBuf::new();
    sb.enqueue(full);
    let mut t = FailAfterPartial {
        first: true,
        partial: 10,
        socket: Vec::new(),
    };
    let r = block_on(flush(&mut sb, &mut t));
    assert!(
        matches!(r, Err(EngineError::Transport(IoFault(_)))),
        "a write() failure must surface as EngineError::Transport, got {r:?}"
    );
    // The cursor reflects EXACTLY the accepted partial — advanced once, not
    // torn, not reset. (A mutation advancing the cursor before the result
    // check would leave a different pending tail here.)
    assert_eq!(sb.pending(), &full[10..], "cursor sits at the partial boundary");
    assert_eq!(t.socket.as_slice(), &full[..10], "socket holds exactly the partial");

    // Resume over the SAME SendBuf with a healthy transport: it sends ONLY the
    // unsent tail, and the reassembled stream is byte-identical to `full`.
    let mut recorder = ChunkRecorder::new();
    block_on(flush(&mut sb, &mut recorder)).expect("resume drains");
    assert!(sb.is_drained());
    assert_eq!(
        recorder.socket.as_slice(),
        &full[10..],
        "resume sends only the not-yet-sent tail — no replay, no loss"
    );
}

#[test]
fn enqueue_after_a_partial_advance_appends_to_the_unsent_tail() {
    let mut sb = SendBuf::new();
    sb.enqueue(b"abcdef");
    sb.advance(2).expect("2 <= 6"); // sent = 2, pending = "cdef"
    // Append with the cursor already advanced (sent > 0), no reset in between.
    sb.enqueue(b"GHI");
    assert_eq!(
        sb.pending(),
        b"cdefGHI",
        "enqueue appends to the unsent tail, preserving order at sent > 0"
    );
    assert_eq!(sb.pending_len(), 7);

    // And it still drains end to end.
    let mut t = ChunkRecorder::new();
    block_on(flush(&mut sb, &mut t)).expect("drain");
    assert_eq!(t.socket.as_slice(), b"cdefGHI");
}
