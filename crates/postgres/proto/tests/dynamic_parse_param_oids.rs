//! OFFLINE wire-shape gate for D1: the DYNAMIC `query_params` family declares its
//! parameter-type OIDs in the `Parse` frame.
//!
//! The silent-reinterpret hole was that the dynamic path sent `n_param_types = 0`
//! yet Bound in binary, so PostgreSQL inferred each `$N` from the SQL context and
//! read the client's binary bytes AS the inferred type with no check. The fix
//! declares `<P as ParamsWriter>::OIDS` in the `Parse`. This gate drives the REAL
//! `query_params_fused` verb over an in-process transport that CAPTURES every
//! outbound byte, then decodes the emitted `Parse` frame and asserts it carries
//! `n_param_types == P::COUNT` and the exact `P::OIDS` — so a future refactor that
//! dropped the OIDs (back to the zero trailer) turns this red OFFLINE, no server.
//!
//! Complements the in-module `parse_frame_twin` (which pins the `build_parse`
//! BUILDER's byte layout): this pins the whole VERB → wire path — that the verb
//! actually threads `P::OIDS` into the builder.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "wire-shape gate harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use bsql_postgres_proto::decode::oids::{BOOL, INT4, TEXT};
use bsql_postgres_proto::engine::{open_owned, poll_once, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident, ParamsWriter};

/// A transport that serves a scripted inbound stream AND records every outbound
/// byte the engine writes, so the test can inspect the flushed request frames.
struct Capturing {
    inbound: Vec<u8>,
    cursor: usize,
    captured: Arc<Mutex<Vec<u8>>>,
}

impl Transport for Capturing {
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
        // Record synchronously before returning the (Send) ready future.
        self.captured
            .lock()
            .expect("capture mutex not poisoned")
            .extend_from_slice(buf);
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── scripted server frames ───────────────────────────

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
    let mut out = frame(b'R', &0_i32.to_be_bytes()); // AuthenticationOk
    out.extend_from_slice(&param_status("server_version", "16.2"));
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A one-column `RowDescription` (`'T'`) — the fused `Describe`(portal) reply.
fn row_description_1() -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    body.extend_from_slice(b"v");
    body.push(0);
    body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
    body.extend_from_slice(&0_i16.to_be_bytes()); // column attr
    body.extend_from_slice(&INT4.to_be_bytes()); // int4
    body.extend_from_slice(&4_i16.to_be_bytes()); // typlen
    body.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
    body.extend_from_slice(&0_i16.to_be_bytes()); // text format
    frame(b'T', &body)
}

/// The fused reply that lets `query_params_fused` complete cleanly: ParseComplete,
/// BindComplete, RowDescription, CommandComplete, ReadyForQuery (zero rows — the
/// gate cares only about the OUTBOUND Parse).
fn fused_reply() -> Vec<u8> {
    let mut out = frame(b'1', &[]); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    out.extend_from_slice(&row_description_1());
    let mut cc = b"SELECT 0".to_vec();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

// ─────────────────────────── captured-frame decode ───────────────────────────

/// Skip the untagged `StartupMessage` (its 4-byte self-inclusive length prefix),
/// then walk tagged request frames and return the FIRST `Parse` (`'P'`) body.
fn first_parse_body(captured: &[u8]) -> Vec<u8> {
    let startup_len = u32::from_be_bytes(
        captured.get(0..4).expect("startup length").try_into().expect("4 bytes"),
    );
    let mut pos = usize::try_from(startup_len).expect("fits usize");
    while pos < captured.len() {
        let tag = *captured.get(pos).expect("frame tag");
        let flen = u32::from_be_bytes(
            captured.get(pos + 1..pos + 5).expect("frame length").try_into().expect("4 bytes"),
        );
        let flen = usize::try_from(flen).expect("fits usize");
        // Body excludes the tag but the length field counts itself, so the body is
        // `[pos+5 .. pos+1+flen)`.
        let body_end = pos + 1 + flen;
        let body = captured.get(pos + 5..body_end).expect("frame body").to_vec();
        if tag == b'P' {
            return body;
        }
        pos = body_end;
    }
    panic!("no Parse ('P') frame found in the captured outbound stream");
}

/// Decode a `Parse` body (`stmt_name NUL | sql NUL | n_param_types i16 | oid u32*`)
/// into its declared parameter-type OID list.
fn parse_param_oids(body: &[u8]) -> Vec<u32> {
    let stmt_nul = body.iter().position(|&b| b == 0).expect("stmt_name NUL");
    let after_stmt = stmt_nul + 1;
    let rest = body.get(after_stmt..).expect("after stmt name");
    let sql_nul = rest.iter().position(|&b| b == 0).expect("sql NUL");
    let mut pos = after_stmt + sql_nul + 1;
    let count = i16::from_be_bytes(
        body.get(pos..pos + 2).expect("n_param_types").try_into().expect("2 bytes"),
    );
    pos += 2;
    let mut oids = Vec::new();
    for _ in 0..count {
        let oid = u32::from_be_bytes(
            body.get(pos..pos + 4).expect("param OID").try_into().expect("4 bytes"),
        );
        oids.push(oid);
        pos += 4;
    }
    oids
}

// ─────────────────────────── the gate ───────────────────────────

#[test]
fn dynamic_fused_parse_declares_the_param_type_oids() {
    let user = Ident::try_from_str("oids").expect("valid ident");
    let captured = Arc::new(Mutex::new(Vec::new()));

    let mut inbound = handshake();
    inbound.extend_from_slice(&fused_reply());

    let transport = Capturing {
        inbound,
        cursor: 0,
        captured: Arc::clone(&captured),
    };
    let (mut engine, live) =
        open_owned(transport, &user, None, &[], Credentials::Trust).expect("session assembles");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // A three-parameter tuple: OIDS = [int4, text, bool]. `text` (25) is the exact
    // shape of the repro — a `&str` param whose OID the old zero-trailer dropped.
    let params = (7i32, "hi", true);
    let sink = |_: Surface<'_>| ControlFlow::Continue(());
    let outcome = poll_once(engine.query_params_fused(live, "SELECT $1, $2, $3", &params, sink));
    match outcome {
        Ok(Ok(_)) => {}
        other => panic!("query_params_fused must complete, got {other:?}"),
    }

    let body = first_parse_body(&captured.lock().expect("capture not poisoned"));
    let oids = parse_param_oids(&body);
    assert_eq!(
        oids,
        <(i32, &str, bool) as ParamsWriter>::OIDS,
        "the dynamic fused Parse must declare P::OIDS ([int4, text, bool]), not the zero trailer",
    );
    assert_eq!(oids, vec![INT4, TEXT, BOOL], "declared OIDs must be the encoded types");
    assert_eq!(oids.len(), 3, "n_param_types must equal the parameter arity, not 0");
}
