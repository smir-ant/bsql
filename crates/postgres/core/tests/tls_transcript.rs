// The whole gate targets the rustls-backed `TlsTransport`, which exists only
// under the `tls` feature; with TLS off this test binary is empty (no tests).
#![cfg(feature = "tls")]
//! Scripted-TLS transcript gate for [`bsql_postgres_core::tls::TlsTransport`].
//!
//! Drives the production `TlsTransport` (a real `rustls::unbuffered` client)
//! through a genuine TLS exchange against an in-memory `rustls` server peer
//! (real key exchange + AEAD; only the cert-chain check is stubbed). Asserts
//! the five transport behaviours: the handshake completes inside the
//! driver-side connect step; `write`+`flush` delivers plaintext the server
//! decrypts identically; a back-pressured (partial-accept) write is completed
//! by `flush`; a record split across socket reads decrypts cleanly; and
//! `shutdown` emits `close_notify` the server observes as a clean close.

// Helper fns and impl methods in the shared harness are not in `#[test]`
// context, so the in-tests carve-out does not reach them; these scoped allows
// (keystone-required reason) cover the harness's loud-failure expects/panics.
#![allow(
    clippy::expect_used,
    reason = "test harness — expect() is the loud failure signal; the in-tests carve-out reaches #[test] fns but not free helper fns / impl methods"
)]
#![allow(
    clippy::panic,
    reason = "test harness — the loopback panics on an impossible state as the loud failure signal; the in-tests carve-out does not reach free helper fns"
)]

mod tls_common;

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tls_common::{block_on, test_client_config, test_server_name, LoopbackInner, MockInner};

use bsql_postgres_core::tls::TlsTransport;
use bsql_postgres_proto::engine::Transport;

/// Connect a `TlsTransport` over a fresh loopback and settle the server so it
/// reaches the data phase (consuming the client's Finished). Returns the ready
/// transport and a shared handle to the server-side state.
fn connect_pair() -> (TlsTransport<MockInner>, Arc<Mutex<LoopbackInner>>) {
    let (inner, state) = MockInner::new();
    let transport = block_on(TlsTransport::connect(
        inner,
        test_client_config(),
        test_server_name(),
    ))
    .expect("TLS handshake completes inside connect()");
    state.lock().expect("lock").pump_server();
    (transport, state)
}

#[test]
fn shared_client_config_builds_with_explicit_provider() {
    // The bare `ClientConfig::builder()` has no process-default provider in a
    // ring-only workspace and would fail; the explicit
    // `builder_with_provider(ring)` path succeeds — and is cached.
    let a = bsql_postgres_core::tls::shared_client_config().expect("config builds");
    let b = bsql_postgres_core::tls::shared_client_config().expect("config builds (cached)");
    assert!(
        Arc::ptr_eq(&a, &b),
        "the client config is built once and shared across calls"
    );
}

#[test]
fn handshake_completes_through_connect() {
    // Reaching the assert means `connect()` returned `Ok` — the full handshake
    // ran inside the driver-side connect step (there is no handshake method on
    // the Transport quartet).
    let (_transport, _state) = connect_pair();
}

#[test]
fn write_flush_delivers_plaintext_to_server() {
    let (mut transport, state) = connect_pair();
    let msg = b"startup: the engine's bytes ride inside TLS records".as_slice();

    let n = block_on(transport.write(msg)).expect("write");
    assert_eq!(n, msg.len(), "write consumes the full plaintext");

    block_on(transport.flush()).expect("flush");
    state.lock().expect("lock").pump_server();

    let recv = state.lock().expect("lock").server_recv.clone();
    assert_eq!(recv, msg, "server decrypted the plaintext identically");
}

#[test]
fn backpressured_write_completed_by_flush() {
    let (mut transport, state) = connect_pair();
    // 32 KiB plaintext spans several TLS records of ciphertext.
    let big: Vec<u8> = (0..32_768usize).map(|i| (i % 251) as u8).collect();

    // The socket accepts at most 1000 ciphertext bytes per write attempt.
    state.lock().expect("lock").send_cap = 1000;

    let n = block_on(transport.write(&big)).expect("write");
    assert_eq!(
        n,
        big.len(),
        "write consumes the full plaintext despite back-pressure"
    );
    // write performed NO socket I/O: the ciphertext is buffered internally and
    // the wire is still empty (the handshake bytes were drained at connect).
    assert_eq!(
        state.lock().expect("lock").c2s_len(),
        0,
        "write did not touch the socket — the ciphertext is buffered for flush"
    );

    block_on(transport.flush()).expect("flush");

    // Drain the server until it has reassembled the whole 32 KiB.
    {
        let mut g = state.lock().expect("lock");
        loop {
            if g.server_recv.len() >= big.len() {
                break;
            }
            let before = g.server_recv.len();
            g.pump_server();
            assert!(
                g.server_recv.len() > before,
                "server made no progress draining the flushed ciphertext"
            );
        }
    }
    let recv = state.lock().expect("lock").server_recv.clone();
    assert_eq!(
        recv, big,
        "server reassembled the full 32 KiB after the back-pressured flush"
    );
}

#[test]
fn split_inbound_record_decrypts() {
    let (mut transport, state) = connect_pair();
    let msg: Vec<u8> = (0..200usize).map(|i| (i % 251) as u8).collect();

    {
        let mut g = state.lock().expect("lock");
        g.server_send_app(&msg); // stage one app-data record on the wire
        // Deliver only 8 ciphertext bytes per socket read, forcing the record
        // to arrive across many reads.
        g.recv_cap = 8;
    }

    let mut buf = vec![0u8; 16 * 1024];
    let n = block_on(transport.read(&mut buf)).expect("read");
    assert_eq!(
        &buf[..n],
        &msg[..],
        "the record split across many socket reads reassembled and decrypted exactly"
    );
}

#[test]
fn read_serves_a_record_larger_than_the_caller_buffer() {
    // A caller buffer smaller than the decrypted record must lose no bytes:
    // the plaintext buffer serves the record across several reads.
    let (mut transport, state) = connect_pair();
    let msg: Vec<u8> = (0..500usize).map(|i| (i % 251) as u8).collect();
    state.lock().expect("lock").server_send_app(&msg);

    let mut got = Vec::new();
    let mut small = [0u8; 64];
    while got.len() < msg.len() {
        let n = block_on(transport.read(&mut small)).expect("read");
        assert!(n > 0, "read made no progress before the record was drained");
        got.extend_from_slice(&small[..n]);
    }
    assert_eq!(got, msg, "a record larger than the caller buffer is delivered whole");
}

#[test]
fn shutdown_emits_close_notify() {
    let (mut transport, state) = connect_pair();

    block_on(transport.shutdown()).expect("shutdown");

    // The close_notify record is now on the wire; the server observes it.
    state.lock().expect("lock").pump_server();
    assert!(
        state.lock().expect("lock").server_closed,
        "server observed a clean close_notify (not a truncation)"
    );
}

// ===========================================================================
// Would-block read must not corrupt the inbound ciphertext staging buffer.
// ===========================================================================

/// A [`MockInner`] wrapper that returns ONE would-block read on demand (the
/// recv_notification deadline analog), then delegates to the real loopback. Its
/// `wb` flag is shared so a test can arm it AFTER the handshake (the inner is
/// moved into the `TlsTransport` by `connect`, out of the test's reach otherwise).
#[derive(Clone)]
struct WouldBlockOnceInner {
    inner: MockInner,
    wb: Arc<AtomicBool>,
}

impl Transport for WouldBlockOnceInner {
    type Error = std::io::Error;

    fn is_would_block(err: &std::io::Error) -> bool {
        matches!(
            err.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    }

    async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, std::io::Error> {
        if self.wb.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
        }
        // `MockInner` is `Infallible`, so the error arm is unreachable.
        match self.inner.read(buf).await {
            Ok(n) => Ok(n),
            Err(e) => match e {},
        }
    }

    async fn write<'a>(&'a mut self, buf: &'a [u8]) -> Result<usize, std::io::Error> {
        match self.inner.write(buf).await {
            Ok(n) => Ok(n),
            Err(e) => match e {},
        }
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'a {
        std::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'a {
        std::future::ready(Ok(()))
    }
}

/// Connect a `TlsTransport` over a would-block-once loopback and settle the
/// server to the data phase. Returns the ready transport, the server-state
/// handle, and the shared would-block arm.
fn connect_wb_pair() -> (
    TlsTransport<WouldBlockOnceInner>,
    Arc<Mutex<LoopbackInner>>,
    Arc<AtomicBool>,
) {
    let (mock, state) = MockInner::new();
    let wb = Arc::new(AtomicBool::new(false));
    let inner = WouldBlockOnceInner {
        inner: mock,
        wb: Arc::clone(&wb),
    };
    let transport = block_on(TlsTransport::connect(
        inner,
        test_client_config(),
        test_server_name(),
    ))
    .expect("TLS handshake completes inside connect()");
    state.lock().expect("lock").pump_server();
    (transport, state, wb)
}

#[test]
fn would_block_read_does_not_corrupt_staging() {
    // The recv_notification deadline path: a would-block read returns the
    // classified error, and the NEXT read must still decrypt a real record — i.e.
    // the would-block left the inbound ciphertext staging buffer uncorrupted (no
    // zero-padding from the abandoned read window). On the pre-fix code the second
    // read fails with a TLS error (the 0x00-content-type zeros poison the record
    // stream); this test FAILS before the fix and PASSES after it.
    let (mut transport, state, wb) = connect_wb_pair();
    let msg: Vec<u8> = (0..200usize).map(|i| (i % 251) as u8).collect();
    state.lock().expect("lock").server_send_app(&msg); // a real encrypted record on the wire

    // Arm a single would-block read.
    wb.store(true, Ordering::SeqCst);
    let mut buf = vec![0u8; 16 * 1024];
    match block_on(transport.read(&mut buf)) {
        Err(e) => assert!(
            TlsTransport::<WouldBlockOnceInner>::is_would_block(&e),
            "the armed deadline must surface as a would-block read, got {e:?}"
        ),
        Ok(n) => panic!("expected a would-block read, got Ok({n})"),
    }

    // The would-block must not have polluted the ciphertext staging buffer: the
    // next read decrypts the real record cleanly.
    let n = block_on(transport.read(&mut buf))
        .expect("a read after a would-block must decrypt the real record (staging uncorrupted)");
    assert_eq!(
        &buf[..n],
        &msg[..],
        "the real record decrypts after the would-block — staging was not corrupted"
    );
}

/// A [`MockInner`] wrapper that returns ONE would-block read AFTER a set number
/// of successful reads (a mid-stream deadline), then delegates. Unlike
/// [`WouldBlockOnceInner`] (which fires on the very first read, with an empty
/// staging buffer), this places the deadline once a PARTIAL record has already
/// accumulated past the watermark — the watermark model's load-bearing case.
#[derive(Clone)]
struct WouldBlockAfterInner {
    inner: MockInner,
    /// Reads left before the single would-block fires; `0` = disarmed. The
    /// fire-read stores `0`, so exactly one would-block is produced.
    countdown: Arc<AtomicUsize>,
}

impl Transport for WouldBlockAfterInner {
    type Error = std::io::Error;

    fn is_would_block(err: &std::io::Error) -> bool {
        matches!(
            err.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    }

    async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, std::io::Error> {
        let c = self.countdown.load(Ordering::SeqCst);
        if c == 1 {
            self.countdown.store(0, Ordering::SeqCst); // disarm after firing once
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
        }
        if c > 1 {
            self.countdown.store(c - 1, Ordering::SeqCst);
        }
        // `MockInner` is `Infallible`, so the error arm is unreachable.
        match self.inner.read(buf).await {
            Ok(n) => Ok(n),
            Err(e) => match e {},
        }
    }

    async fn write<'a>(&'a mut self, buf: &'a [u8]) -> Result<usize, std::io::Error> {
        match self.inner.write(buf).await {
            Ok(n) => Ok(n),
            Err(e) => match e {},
        }
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'a {
        std::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'a {
        std::future::ready(Ok(()))
    }
}

#[test]
fn would_block_mid_record_preserves_partial_residue() {
    // The watermark model's load-bearing cancel-safety case: a deadline that
    // elapses AFTER a partial record has accumulated in staging
    // (staging_filled > 0). The abandoned read window sits past that non-zero
    // watermark; advancing the watermark only on a successful read is what keeps
    // the accumulated partial residue byte-identical across the would-block. If
    // the watermark were corrupted (residue truncated, or the abandoned window's
    // bytes admitted into the valid range), the record would fail to reassemble.
    let (mock, state) = MockInner::new();
    let countdown = Arc::new(AtomicUsize::new(0));
    let inner = WouldBlockAfterInner {
        inner: mock,
        countdown: Arc::clone(&countdown),
    };
    let mut transport = block_on(TlsTransport::connect(
        inner,
        test_client_config(),
        test_server_name(),
    ))
    .expect("TLS handshake completes inside connect()");
    state.lock().expect("lock").pump_server();

    // Stage one ~200-byte app record and force it to arrive 8 ciphertext bytes
    // per socket read, so several reads accumulate a partial record before the
    // whole record is present.
    let msg: Vec<u8> = (0..200usize).map(|i| (i % 251) as u8).collect();
    {
        let mut g = state.lock().expect("lock");
        g.server_send_app(&msg);
        g.recv_cap = 8;
    }

    // Fire the would-block on the 6th socket read: 5 successful 8-byte reads
    // (40 bytes of partial record accumulated at staging_filled) precede it.
    countdown.store(6, Ordering::SeqCst);
    let mut buf = vec![0u8; 16 * 1024];
    match block_on(transport.read(&mut buf)) {
        Err(e) => assert!(
            TlsTransport::<WouldBlockAfterInner>::is_would_block(&e),
            "the mid-record deadline must surface as a would-block read, got {e:?}"
        ),
        Ok(n) => panic!("expected a mid-record would-block, got Ok({n})"),
    }

    // Resume: the accumulated partial residue survived the would-block, so the
    // remaining bytes complete the record and it decrypts exactly.
    let mut got = Vec::new();
    while got.len() < msg.len() {
        let n = block_on(transport.read(&mut buf))
            .expect("reads after the mid-record would-block must reassemble the record");
        assert!(n > 0, "read made no progress after the would-block");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(
        got, msg,
        "the record reassembled and decrypted after a would-block that hit mid-record — \
         the partial residue past the watermark was preserved byte-identically"
    );
}
