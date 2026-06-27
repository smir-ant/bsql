//! Cancellation-unrolling gate for the engine-owned send cursor.
//!
//! Proves that dropping a [`flush`] future mid-drain — an `async` task
//! cancelled at its only suspension point — leaves the engine-owned
//! [`SendBuf`] consistent, so the next flush over the same buffer resumes
//! exactly where the socket left off: no double-send, no lost byte, even when
//! the drop lands in the middle of a wire frame.
//!
//! The proof is a DETERMINISTIC FULL SWEEP: the flush is dropped at every
//! single byte boundary of a multi-frame buffer (not a handful of hand-picked
//! points), re-driven over the same buffer, and the reassembled socket stream
//! is asserted byte-identical to the enqueued bytes.
//!
//! A NEGATIVE CONTROL puts the cursor *inside* the flush future (the naive
//! `write_all`-loop shape) and shows the same drop-and-resume DOUBLE-SENDS —
//! proving the harness has teeth and the engine-owned cursor placement is
//! load-bearing.
//!
//! No executor: the futures are driven by hand with a no-op waker and dropped
//! to model cancellation (`tokio`/`select!`/`timeout` are not available in
//! this `no_std`-style crate).

#![forbid(unsafe_code)]

use bsql_postgres_proto::engine::{flush, EngineError, SendBuf, Transport};
use core::convert::Infallible;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;

// ---------------------------------------------------------------------------
// A scripted socket. Each `Step` controls one `poll` of the write future.
// `socket` is the ground-truth record of every byte the kernel accepted.
// ---------------------------------------------------------------------------

struct ScriptedTransport {
    socket: Vec<u8>,
    script: VecDeque<Step>,
}

#[derive(Clone, Copy, Debug)]
enum Step {
    /// Accept `min(k, offered)` bytes this poll; return `Ready(Ok(accepted))`.
    Accept(usize),
    /// Would-block: return `Pending`, accept ZERO bytes (`poll_write`
    /// atomicity). An exhausted script also parks.
    Pending,
}

impl ScriptedTransport {
    fn new() -> Self {
        Self {
            socket: Vec::new(),
            script: VecDeque::new(),
        }
    }
    fn load(&mut self, steps: impl IntoIterator<Item = Step>) {
        self.script.extend(steps);
    }
}

/// Concrete future for `ScriptedTransport::write`, modelling `Pending`
/// precisely. Holds only references, so it is `Unpin` and `Pin::get_mut` is
/// the safe accessor (no `unsafe`).
struct WriteFut<'a> {
    t: &'a mut ScriptedTransport,
    buf: &'a [u8],
}

impl Future for WriteFut<'_> {
    type Output = Result<usize, Infallible>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = Pin::get_mut(self); // safe: WriteFut: Unpin
        match me.t.script.pop_front() {
            Some(Step::Accept(k)) => {
                let n = k.min(me.buf.len());
                me.t.socket.extend_from_slice(&me.buf[..n]);
                Poll::Ready(Ok(n))
            }
            Some(Step::Pending) | None => Poll::Pending,
        }
    }
}

impl Transport for ScriptedTransport {
    type Error = Infallible;
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        core::future::ready(Ok(0))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        WriteFut { t: self, buf }
    }
    // flush/shutdown are no-ops here: this transport has no internal buffer,
    // and flush must NOT consume from the write script (it is a distinct
    // operation from a write attempt).
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// NEGATIVE CONTROL: cursor lives in the FUTURE (the naive write_all shape).
// On drop the local `sent` is lost; resume restarts from 0 -> double-send.
// This is NOT the real flush — it exists only to prove the harness detects
// corruption.
// ---------------------------------------------------------------------------

async fn flush_future_local_cursor<T: Transport>(
    buf: &[u8],
    transport: &mut T,
) -> Result<(), EngineError<T::Error>> {
    let mut sent = 0usize; // cursor in the FUTURE, not the engine
    while sent < buf.len() {
        let n = transport
            .write(&buf[sent..])
            .await
            .map_err(EngineError::Transport)?;
        if n == 0 {
            return Err(EngineError::WriteZero);
        }
        sent += n;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

/// A deliberately multi-frame buffer so cancellation can land MID-FRAME.
/// Each frame = 1-byte tag + 4-byte big-endian length + payload, mimicking a
/// Parse/Bind/Describe/Execute/Sync pipeline at the byte level. The exact
/// contents are irrelevant to the unroll proof; only that it is >1 frame.
fn pipeline_bytes() -> Vec<u8> {
    let mut v = Vec::new();
    let mut frame = |tag: u8, payload: &[u8]| {
        v.push(tag);
        let len = (payload.len() as u32 + 4).to_be_bytes();
        v.extend_from_slice(&len);
        v.extend_from_slice(payload);
    };
    frame(b'P', b"stmt\0SELECT $1::int4\0\0\x01\x00\x00\x00\x17");
    frame(b'B', b"\0stmt\0\0\x01\x00\x01\0\x01\x00\x00\x00\x04\x00\x00\x00\x2a");
    frame(b'D', b"P\0");
    frame(b'E', b"\0\x00\x00\x00\x00");
    frame(b'S', b"");
    v
}

/// Script that accepts bytes in `chunk`-sized partials up to `target`, then
/// parks. Models a slow socket that would-blocks mid-drain at `target`.
fn accept_then_park(target: usize, chunk: usize) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut acc = 0;
    while acc < target {
        let c = chunk.min(target - acc);
        steps.push(Step::Accept(c));
        acc += c;
    }
    steps.push(Step::Pending); // cancellation lands here
    steps
}

// ---------------------------------------------------------------------------
// THE GATE — drop at EVERY byte boundary; verify the unroll invariant + resume.
// ---------------------------------------------------------------------------

#[test]
fn flush_unrolls_at_every_drop_point_with_byte_identical_resume() {
    let full = pipeline_bytes();
    let len = full.len();
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    // Drop at EVERY cursor position 0..=len (every byte boundary, including
    // the start, every mid-frame offset, and the natural end).
    for target in 0..=len {
        let mut send_buf = SendBuf::new();
        send_buf.enqueue(&full);
        let mut transport = ScriptedTransport::new();

        // --- Drain partially to `target` in many small writes, then cancel;
        // the same engine-owned buffer is re-driven afterwards. Chunk size 3
        // forces multiple cursor advances per poll. ---
        transport.load(accept_then_park(target, 3));
        let mut fut = Box::pin(flush(&mut send_buf, &mut transport));
        let p = fut.as_mut().poll(&mut cx);
        if target == len {
            assert!(
                matches!(p, Poll::Ready(Ok(()))),
                "target=len: the whole buffer is accepted, flush completes"
            );
        } else {
            assert!(p.is_pending(), "target={target}: flush parks at the write await");
        }
        // CANCELLATION: drop the flush future mid-drain.
        drop(fut);

        // --- Invariant after cancellation (accessor-free) ---
        let k = transport.socket.len();
        assert_eq!(k, target, "target={target}: socket accepted exactly target bytes");
        assert_eq!(
            transport.socket.as_slice(),
            &full[..k],
            "target={target}: socket is the buffer prefix (no replay, no skip)"
        );
        // The cursor sits EXACTLY past the accepted bytes: the pending tail
        // begins where the socket stopped. This rules out a torn cursor in
        // both directions at a possibly mid-frame offset.
        assert_eq!(
            send_buf.pending(),
            &full[k..],
            "target={target}: cursor unrolled to exactly the accepted count"
        );

        // --- Resume on the SAME send_buf + SAME socket. ---
        transport.script.clear();
        transport.load([Step::Accept(usize::MAX)]); // accept the whole tail
        let mut fut2 = Box::pin(flush(&mut send_buf, &mut transport));
        let p2 = fut2.as_mut().poll(&mut cx);
        assert!(matches!(p2, Poll::Ready(Ok(()))), "target={target}: resume completes");
        drop(fut2);

        // --- Final invariant: full stream delivered EXACTLY once, in order. ---
        assert!(send_buf.is_drained(), "target={target}: drained after resume");
        assert_eq!(
            transport.socket.len(),
            len,
            "target={target}: no double-send (length matches)"
        );
        assert_eq!(
            transport.socket.as_slice(),
            &full[..],
            "target={target}: byte-identical reassembly"
        );
    }
}

/// Repeated cancellation: cancel, resume-a-bit, cancel again, across the whole
/// buffer. Models a future cancelled on many consecutive polls. The cursor
/// must advance monotonically and never replay.
#[test]
fn flush_survives_repeated_cancellation_without_replay() {
    let full = pipeline_bytes();
    let len = full.len();
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let mut send_buf = SendBuf::new();
    send_buf.enqueue(&full);
    let mut transport = ScriptedTransport::new();

    let mut last_sent = 0usize;
    let mut rounds = 0usize;

    // Each round: accept exactly 2 bytes then park; poll once; cancel.
    while !send_buf.is_drained() {
        rounds += 1;
        transport.script.clear();
        transport.load([Step::Accept(2), Step::Pending]);

        let mut fut = Box::pin(flush(&mut send_buf, &mut transport));
        // The poll parks after accepting 2 bytes, EXCEPT on the final round
        // where those 2 bytes complete the drain and the poll returns Ready —
        // so the post-drop invariants below, not this poll's variant, are the
        // assertion.
        let _outcome = fut.as_mut().poll(&mut cx);
        drop(fut); // cancel every round

        let sent = transport.socket.len();
        assert!(sent >= last_sent, "cursor went backwards");
        assert!(sent <= last_sent + 2, "advanced more than the socket accepted");
        assert_eq!(
            transport.socket.as_slice(),
            &full[..sent],
            "no replay across {rounds} cancels"
        );
        assert_eq!(send_buf.pending(), &full[sent..], "cursor tracks the socket");
        last_sent = sent;
    }

    assert_eq!(transport.socket.as_slice(), &full[..], "byte-identical after {rounds} cancels");
    assert_eq!(transport.socket.len(), len, "no double-send across repeated cancellation");
}

/// NEGATIVE CONTROL. A future-local cursor double-sends under the same
/// drop-and-resume. If this PASSED (no corruption), the sweep above would be
/// blind.
#[test]
fn negative_control_future_local_cursor_double_sends() {
    let full = pipeline_bytes();
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let mut transport = ScriptedTransport::new();

    // Accept 6 bytes, then park; poll; cancel. The future-local
    // `sent` (=6) is DESTROYED by the drop. The engine has no record.
    transport.load([Step::Accept(6), Step::Pending]);
    let mut fut = Box::pin(flush_future_local_cursor(&full, &mut transport));
    assert!(fut.as_mut().poll(&mut cx).is_pending(), "parks after 6 bytes");
    drop(fut);
    assert_eq!(transport.socket.len(), 6, "first drive accepted 6");

    // Resume with a fresh future. It restarts from sent=0 and
    // re-sends the first 6 bytes -> the socket holds buf[0..6] TWICE.
    transport.script.clear();
    transport.load([Step::Accept(usize::MAX)]);
    let mut fut2 = Box::pin(flush_future_local_cursor(&full, &mut transport));
    let _outcome = fut2.as_mut().poll(&mut cx);
    drop(fut2);

    let corrupted =
        transport.socket.len() != full.len() || transport.socket.as_slice() != full.as_slice();
    assert!(
        corrupted,
        "NEGATIVE CONTROL FAILED: a future-local cursor did NOT corrupt on cancel"
    );
    // Specifically: a double-send of exactly the first 6 bytes.
    assert_eq!(
        transport.socket.len(),
        full.len() + 6,
        "control double-sent exactly the first 6 bytes"
    );
    assert_eq!(&transport.socket[..6], &full[..6]);
    assert_eq!(&transport.socket[6..12], &full[..6], "first 6 bytes appear twice");
}
