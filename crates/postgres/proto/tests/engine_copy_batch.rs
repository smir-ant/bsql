//! Syscall-reduction witness for the BATCHED COPY IN write path.
//!
//! Drives a COPY IN of many small chunks against an in-process transport that
//! COUNTS every `write` (one `write` ≙ one socket write syscall) and RECORDS the
//! bytes it received. It pins the win the batching exists for:
//!
//! **N small chunks cost ~`total_bytes / THRESHOLD` writes, not N.** The
//! unbatched path (flush every chunk) does exactly N writes for N chunks; the
//! batched path accumulates framed `CopyData` and flushes only at the threshold,
//! so the measured write count during the loop is a tiny fraction of N — the
//! 100–1000× syscall reduction reported by this test.
//!
//! It ALSO proves the batching is byte-exact: the recorded stream is parsed back
//! into its `CopyData` frames and their concatenated bodies must equal every
//! streamed chunk in order, with exactly N frames and one closing `CopyDone` —
//! coalescing WRITES never merges, drops, or reorders FRAMES.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "measurement harness — expect/panic are the loud test-failure signal, and the \
              recorded-stream parser walks a self-produced byte buffer, not untrusted input"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bsql_postgres_proto::engine::{open_owned, poll_once, Never, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};

// The engine's batched-flush threshold (mirrors `COPY_IN_FLUSH_THRESHOLD` in the
// proto engine's `verbs.rs`). Used only to size the analytical upper bound on the
// measured write count; the headline `write_count << N` claim holds for any
// threshold and does not couple to this exact value.
const THRESHOLD: usize = 64 * 1024;

// ─────────────────────────── counting transport ───────────────────────────

/// Feeds scripted backend bytes on `read`; on `write` it COUNTS the call (a
/// socket write syscall) and RECORDS the bytes, then accepts them whole.
struct CountingServer {
    inbound: Vec<u8>,
    cursor: usize,
    writes: Arc<AtomicUsize>,
    recorded: Arc<Mutex<Vec<u8>>>,
}

impl Transport for CountingServer {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        let end = self.cursor + n;
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.recorded.lock().expect("recorder lock").extend_from_slice(buf);
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        // A flush drives a transport-internal buffer (a TLS record) to the socket;
        // this plaintext fake buffers nothing, so it is NOT a data write and is
        // deliberately NOT counted.
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn param_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    for (k, v) in [
        ("client_encoding", "UTF8"),
        ("server_version", "16.2"),
        ("TimeZone", "UTC"),
    ] {
        out.extend_from_slice(&param_status(k, v));
    }
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A COPY-in reply cycle: `CopyInResponse` + `CommandComplete("COPY n")` + RFQ.
fn copy_in_cycle(rows: usize) -> Vec<u8> {
    let mut out = frame(b'G', &[0, 0, 0]); // CopyInResponse: text, 0 columns
    let mut cc = format!("COPY {rows}").into_bytes();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn no_op_sink(surface: Surface<'_>) -> ControlFlow<Never> {
    let _ = core::hint::black_box(surface);
    ControlFlow::Continue(())
}

/// Walk a recorded outbound byte stream (tagged frames: `tag | u32 len | body`)
/// and return every `CopyData` (`'d'`) body plus whether a `CopyDone` (`'c'`)
/// frame was seen. Non-`d`/`c` frames (the `Q` COPY command) are stepped over.
fn parse_copy_frames(stream: &[u8]) -> (Vec<Vec<u8>>, bool) {
    let mut bodies = Vec::new();
    let mut saw_done = false;
    let mut i = 0usize;
    while i + 5 <= stream.len() {
        let tag = stream[i];
        let len = u32::from_be_bytes([stream[i + 1], stream[i + 2], stream[i + 3], stream[i + 4]])
            as usize;
        // `len` is self-inclusive of the 4 length bytes; the body is `len - 4`.
        let body_start = i + 5;
        let body_end = i + 1 + len;
        assert!(body_end <= stream.len(), "recorded frame overruns the stream");
        if tag == b'd' {
            bodies.push(stream[body_start..body_end].to_vec());
        } else if tag == b'c' {
            saw_done = true;
        }
        i = body_end;
    }
    assert_eq!(i, stream.len(), "recorded stream did not frame-align");
    (bodies, saw_done)
}

// ─────────────────────────── the witness ───────────────────────────

#[test]
fn copy_in_batches_writes_and_preserves_every_frame() {
    let user = Ident::try_from_str("gate").expect("valid ident");

    const N: usize = 10_000;
    let chunk: &[u8] = b"42\tpayload-row\n"; // 14 bytes → 19-byte CopyData frame
    let frame_len = chunk.len() + 5;
    let total_bytes = N * frame_len;

    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(N));

    let writes = Arc::new(AtomicUsize::new(0));
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let server = CountingServer {
        inbound,
        cursor: 0,
        writes: Arc::clone(&writes),
        recorded: Arc::clone(&recorded),
    };

    let (mut engine, live) =
        open_owned(server, &user, None, &[], Credentials::Trust).expect("session assembles");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // Drop the handshake's outbound bytes so the recorder holds only the COPY
    // exchange (a tagged-frame stream the parser can walk).
    recorded.lock().expect("recorder lock").clear();

    poll_once(engine.copy_in_begin("COPY t FROM STDIN", |_s| ControlFlow::Continue(())))
        .expect("poll")
        .expect("begin");

    // Measure ONLY the write loop (exclude begin's Q-frame write and finish).
    let writes_before_loop = writes.load(Ordering::Relaxed);
    for _ in 0..N {
        poll_once(engine.copy_in_write(chunk))
            .expect("poll")
            .expect("write");
    }
    let batched_writes = writes.load(Ordering::Relaxed) - writes_before_loop;

    match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("finish must complete, got {other:?}"),
    }

    // ---- Report the measured syscall reduction. ----
    // Unbatched, the driver flushes every chunk: exactly N writes for N chunks.
    let unbatched_writes = N;
    let reduction = unbatched_writes as f64 / batched_writes.max(1) as f64;
    println!(
        "COPY-in {N} chunks ({total_bytes} bytes, {frame_len}B/frame): \
         unbatched writes = {unbatched_writes}, batched writes = {batched_writes}, \
         reduction = {reduction:.0}x (threshold {THRESHOLD}B)"
    );

    // ---- The win: writes ≪ N, bounded by the analytical ceil(total/threshold). ----
    assert!(
        batched_writes >= 1,
        "the buffer must cross the threshold and flush mid-stream at least once for {N} chunks",
    );
    // Analytical ceiling `batched_writes <= floor(total/threshold) + 1`, stated
    // via multiplication so it needs no (workspace-forbidden) integer division:
    // `(batched_writes - 1) * threshold <= total_bytes`.
    assert!(
        batched_writes.saturating_sub(1) * THRESHOLD <= total_bytes,
        "batched writes {batched_writes} exceeded the analytical ceiling \
         floor(total_bytes/threshold) + 1 (total {total_bytes}B, threshold {THRESHOLD}B)",
    );
    assert!(
        batched_writes * 100 < N,
        "batching must cut writes by >100x — got {batched_writes} for {N} chunks",
    );

    // ---- Byte-exactness: every chunk survives as its own CopyData frame. ----
    let stream = recorded.lock().expect("recorder lock").clone();
    let (bodies, saw_done) = parse_copy_frames(&stream);
    assert_eq!(
        bodies.len(),
        N,
        "coalescing WRITES must not merge/drop FRAMES — expected {N} CopyData frames",
    );
    assert!(saw_done, "the CopyDone frame must close the stream");
    for (idx, body) in bodies.iter().enumerate() {
        assert_eq!(body.as_slice(), chunk, "CopyData body {idx} corrupted by batching");
    }
}
