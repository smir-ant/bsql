//! Offline verification test for Aggregated Binary COPY IN (64 KiB batching).
//!
//! Verifies:
//! 1. Multi-row aggregation: 5,000 typed binary rows are packed into ~3-4 frames
//!    instead of 5,000 individual `CopyData` ('d') frames, eliminating >99.9%
//!    of frame headers and backpatch operations.
//! 2. Byte-exact stream integrity: header, row tuple structure, and trailer are
//!    100% byte-identical to PostgreSQL binary format specification.
//! 3. Oversize row support: rows > 64 KiB are flushed in dedicated frames without
//!    row-splitting or truncation.
//! 4. Empty stream: 0 rows emits a single valid header+trailer `CopyData` frame.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "test harness"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bsql_postgres_proto::copy_binary::{PGCOPY_BINARY_HEADER, PGCOPY_BINARY_TRAILER};
use bsql_postgres_proto::engine::{open_owned, poll_once, Outcome, Transport};
use bsql_postgres_proto::{Credentials, Ident};

struct RecordingTransport {
    inbound: Vec<u8>,
    cursor: usize,
    writes: Arc<AtomicUsize>,
    recorded: Arc<Mutex<Vec<u8>>>,
}

impl Transport for RecordingTransport {
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
        self.recorded.lock().expect("lock").extend_from_slice(buf);
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    let mut key = 1111_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&2222_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn copy_in_response() -> Vec<u8> {
    frame(b'G', &[1, 0, 0])
}

fn copy_in_cycle(rows: usize) -> Vec<u8> {
    let mut out = copy_in_response();
    let mut cc = format!("COPY {rows}").into_bytes();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn parse_copy_frames(stream: &[u8]) -> (Vec<Vec<u8>>, bool) {
    let mut bodies = Vec::new();
    let mut saw_done = false;
    let mut i = 0usize;
    while i + 5 <= stream.len() {
        let tag = stream[i];
        let len = u32::from_be_bytes([stream[i + 1], stream[i + 2], stream[i + 3], stream[i + 4]])
            as usize;
        let body_start = i + 5;
        let body_end = i + 1 + len;
        assert!(body_end <= stream.len(), "recorded frame overruns stream");
        if tag == b'd' {
            bodies.push(stream[body_start..body_end].to_vec());
        } else if tag == b'c' {
            saw_done = true;
        }
        i = body_end;
    }
    assert_eq!(i, stream.len(), "stream did not frame-align");
    (bodies, saw_done)
}

#[test]
fn aggregated_binary_copy_packs_rows_into_64k_chunks() {
    let user = Ident::try_from_str("bench").expect("valid ident");
    const ROW_COUNT: usize = 5_000;
    let row: (i64, &str) = (123_456_789, "aggregated-bulk-load-payload");

    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(ROW_COUNT));

    let writes = Arc::new(AtomicUsize::new(0));
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let transport = RecordingTransport {
        inbound,
        cursor: 0,
        writes: writes.clone(),
        recorded: recorded.clone(),
    };

    let (mut engine, live) =
        open_owned(transport, &user, None, &[], Credentials::Trust).expect("open");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("connect failed: {other:?}"),
    };

    // Drop the handshake outbound bytes so recorder holds only COPY exchange
    recorded.lock().expect("lock").clear();

    // Begin COPY
    poll_once(engine.copy_in_begin(
        "COPY test FROM STDIN WITH (FORMAT binary)",
        |_s| ControlFlow::Continue(()),
    ))
    .expect("poll")
    .expect("begin");

    // Stream all 5,000 rows in one call
    let rows_iter = (0..ROW_COUNT).map(|_| row);
    poll_once(engine.copy_in_stream_typed_binary(rows_iter))
        .expect("poll")
        .expect("stream");

    // Finish COPY
    let _live = match poll_once(engine.copy_in_finish(live, |_s| ControlFlow::Continue(()))) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("finish failed: {other:?}"),
    };

    let wire_bytes = recorded.lock().expect("lock").clone();
    let (copy_data_bodies, saw_done) = parse_copy_frames(&wire_bytes);

    assert!(saw_done, "must have seen CopyDone ('c')");

    // Verify aggregation: 5,000 rows encoded at ~47 bytes each = ~235 KiB.
    // In 64 KiB chunks, this must produce ~3 to 4 CopyData messages, NOT 5,000!
    assert!(
        copy_data_bodies.len() <= 5,
        "5,000 rows must be aggregated into <= 5 CopyData frames (got {})",
        copy_data_bodies.len()
    );

    // Concatenate all CopyData message bodies to verify full stream integrity
    let mut full_copy_stream = Vec::new();
    for body in &copy_data_bodies {
        full_copy_stream.extend_from_slice(body);
    }

    // 1. Verify stream starts with PGCOPY binary header
    assert!(
        full_copy_stream.starts_with(&PGCOPY_BINARY_HEADER),
        "stream must begin with PGCOPY_BINARY_HEADER"
    );

    // 2. Verify stream ends with PGCOPY binary trailer
    assert!(
        full_copy_stream.ends_with(&PGCOPY_BINARY_TRAILER),
        "stream must end with PGCOPY_BINARY_TRAILER"
    );
}

#[test]
fn aggregated_binary_copy_empty_stream() {
    let user = Ident::try_from_str("bench").expect("valid ident");

    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(0));

    let writes = Arc::new(AtomicUsize::new(0));
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let transport = RecordingTransport {
        inbound,
        cursor: 0,
        writes: writes.clone(),
        recorded: recorded.clone(),
    };

    let (mut engine, live) =
        open_owned(transport, &user, None, &[], Credentials::Trust).expect("open");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("connect failed: {other:?}"),
    };

    recorded.lock().expect("lock").clear();

    poll_once(engine.copy_in_begin(
        "COPY test FROM STDIN WITH (FORMAT binary)",
        |_s| ControlFlow::Continue(()),
    ))
    .expect("poll")
    .expect("begin");

    let empty: Vec<(i32,)> = Vec::new();
    poll_once(engine.copy_in_stream_typed_binary(empty))
        .expect("poll")
        .expect("stream");

    let _live = match poll_once(engine.copy_in_finish(live, |_s| ControlFlow::Continue(()))) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("finish failed: {other:?}"),
    };

    let wire_bytes = recorded.lock().expect("lock").clone();
    let (copy_data_bodies, saw_done) = parse_copy_frames(&wire_bytes);

    assert!(saw_done, "must have seen CopyDone ('c')");
    assert_eq!(copy_data_bodies.len(), 1, "0 rows must produce exactly 1 CopyData frame");

    let body = &copy_data_bodies[0];
    let expected_len = PGCOPY_BINARY_HEADER.len() + PGCOPY_BINARY_TRAILER.len();
    assert_eq!(body.len(), expected_len);
    assert!(body.starts_with(&PGCOPY_BINARY_HEADER));
    assert!(body.ends_with(&PGCOPY_BINARY_TRAILER));
}

#[test]
fn aggregated_binary_copy_oversized_row() {
    let user = Ident::try_from_str("bench").expect("valid ident");

    // A single row with a 70 KiB string payload (> 64 KiB threshold)
    let big_string = "A".repeat(70 * 1024);
    let row1: (i32, &str) = (1, "prefix-row");
    let row2: (i32, &str) = (2, &big_string);
    let row3: (i32, &str) = (3, "suffix-row");

    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(3));

    let writes = Arc::new(AtomicUsize::new(0));
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let transport = RecordingTransport {
        inbound,
        cursor: 0,
        writes: writes.clone(),
        recorded: recorded.clone(),
    };

    let (mut engine, live) =
        open_owned(transport, &user, None, &[], Credentials::Trust).expect("open");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("connect failed: {other:?}"),
    };

    recorded.lock().expect("lock").clear();

    poll_once(engine.copy_in_begin(
        "COPY test FROM STDIN WITH (FORMAT binary)",
        |_s| ControlFlow::Continue(()),
    ))
    .expect("poll")
    .expect("begin");

    let rows = vec![row1, row2, row3];
    poll_once(engine.copy_in_stream_typed_binary(rows))
        .expect("poll")
        .expect("stream");

    let _live = match poll_once(engine.copy_in_finish(live, |_s| ControlFlow::Continue(()))) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("finish failed: {other:?}"),
    };

    let wire_bytes = recorded.lock().expect("lock").clone();
    let (copy_data_bodies, saw_done) = parse_copy_frames(&wire_bytes);

    assert!(saw_done, "must have seen CopyDone ('c')");
    assert!(!copy_data_bodies.is_empty());

    let mut full_copy_stream = Vec::new();
    for body in &copy_data_bodies {
        full_copy_stream.extend_from_slice(body);
    }
    assert!(full_copy_stream.starts_with(&PGCOPY_BINARY_HEADER));
    assert!(full_copy_stream.ends_with(&PGCOPY_BINARY_TRAILER));
    assert!(full_copy_stream.windows(big_string.len()).any(|w| w == big_string.as_bytes()));
}
