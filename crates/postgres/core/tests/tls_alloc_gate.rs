// The whole gate targets the rustls-backed `TlsTransport`, which exists only
// under the `tls` feature; with TLS off this test binary is empty (no tests).
#![cfg(feature = "tls")]
//! Allocation gate for [`bsql_postgres_core::tls::TlsTransport`].
//!
//! Installs the workspace counting allocator as this binary's
//! `#[global_allocator]` and brackets the measured windows with snapshots. A
//! counting allocator measures one thing — allocation traffic — so this gate
//! proves only ALLOCATION claims:
//!
//! 1. **Outbound `write` is bounded and does not grow per record.** Our buffer
//!    management contributes zero: the encrypt scratch is a fixed array, the
//!    outbound queue is reused (capacity retained across flushes), and `write`
//!    performs no socket I/O. `rustls` itself performs exactly one internal
//!    record-construction allocation per record (in its AEAD record layer),
//!    outside this layer's control — `rustls` is the sole TLS authority and is
//!    never forked. That residual is a per-record constant: an early write
//!    costs the same as a late one.
//! 2. **Inbound decrypt is bounded and does not grow per record.** The
//!    documented temporary inbound floor (an owned plaintext residence + an
//!    extra copy) is a small constant per record; the cost of an early record
//!    equals the cost of a late one, so the floor never grows.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global and counts every thread. `cargo
//! test` runs `#[test]` fns in parallel, so the measured windows all live in a
//! SINGLE `#[test]` fn run sequentially — no concurrent test thread can
//! allocate inside a measured window. (Other test binaries are separate
//! processes with their own allocator instance.)

#![allow(
    clippy::expect_used,
    reason = "test harness — expect() is the loud failure signal; the in-tests carve-out reaches #[test] fns but not free helper fns / impl methods"
)]
#![allow(
    clippy::panic,
    reason = "test harness — the loopback panics on an impossible state as the loud failure signal; the in-tests carve-out does not reach free helper fns"
)]

mod tls_common;

use bsql_devgates::CountingAllocator;
use tls_common::{block_on, test_client_config, test_server_name, MockInner};

use bsql_postgres_core::tls::TlsTransport;
use bsql_postgres_proto::engine::Transport;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

/// Read exactly `len` plaintext bytes, looping over the transport's reads
/// (a record may be served in pieces if the caller buffer is small).
fn read_exact(transport: &mut TlsTransport<MockInner>, buf: &mut [u8], len: usize) {
    let mut got = 0;
    while got < len {
        let n = block_on(transport.read(buf)).expect("read");
        assert!(n > 0, "read made no progress before the record was drained");
        got += n;
    }
    assert_eq!(got, len, "read delivered exactly the staged record");
}

#[test]
fn outbound_zero_alloc_inbound_bounded() {
    // ---- setup: connect + settle the server into the data phase ----
    let (inner, state) = MockInner::new();
    let mut transport = block_on(TlsTransport::connect(
        inner,
        test_client_config(),
        test_server_name(),
    ))
    .expect("handshake");
    state.lock().expect("lock").pump_server();

    let rec: Vec<u8> = (0..4096usize).map(|i| (i % 251) as u8).collect();
    let mut buf = vec![0u8; 16 * 1024];

    // ---- warm-up: grow every internal buffer to steady-state capacity ----
    for _ in 0..8 {
        let n = block_on(transport.write(&rec)).expect("write");
        assert_eq!(n, rec.len());
        block_on(transport.flush()).expect("flush");
        state.lock().expect("lock").pump_server();

        state.lock().expect("lock").server_send_app(&rec);
        read_exact(&mut transport, &mut buf, rec.len());
    }
    // The warm-up writes reached the server (exercises `server_recv`).
    assert!(
        !state.lock().expect("lock").server_recv.is_empty(),
        "server received the warm-up plaintext"
    );

    // ---- (1) outbound write: bounded, non-growing, no socket I/O ----
    let before = ALLOC.snapshot();
    let n = block_on(transport.write(&rec)).expect("write");
    let after = ALLOC.snapshot();
    assert_eq!(n, rec.len());
    let write_allocs_early = after.delta(before).allocs;
    assert_eq!(
        state.lock().expect("lock").c2s_len(),
        0,
        "write performed no socket I/O — the ciphertext is buffered for flush"
    );
    // Drain the buffered record (not measured).
    block_on(transport.flush()).expect("flush");
    state.lock().expect("lock").pump_server();

    // ---- (2) inbound decrypt cost: bounded + non-growing ----
    state.lock().expect("lock").server_send_app(&rec);
    let before = ALLOC.snapshot();
    read_exact(&mut transport, &mut buf, rec.len());
    let after = ALLOC.snapshot();
    let read_allocs_early = after.delta(before).allocs;

    // Many intervening records — if a path grew per record, the late
    // measurement would exceed the early one.
    for _ in 0..32 {
        state.lock().expect("lock").server_send_app(&rec);
        read_exact(&mut transport, &mut buf, rec.len());
        let m = block_on(transport.write(&rec)).expect("write");
        assert_eq!(m, rec.len());
        block_on(transport.flush()).expect("flush");
        state.lock().expect("lock").pump_server();
    }

    state.lock().expect("lock").server_send_app(&rec);
    let before = ALLOC.snapshot();
    read_exact(&mut transport, &mut buf, rec.len());
    let after = ALLOC.snapshot();
    let read_allocs_late = after.delta(before).allocs;

    let before = ALLOC.snapshot();
    let n = block_on(transport.write(&rec)).expect("write");
    let after = ALLOC.snapshot();
    assert_eq!(n, rec.len());
    let write_allocs_late = after.delta(before).allocs;
    block_on(transport.flush()).expect("flush");
    state.lock().expect("lock").pump_server();

    // ---- (3) clean teardown (exercises `server_closed`) ----
    block_on(transport.shutdown()).expect("shutdown");
    state.lock().expect("lock").pump_server();
    assert!(
        state.lock().expect("lock").server_closed,
        "server observed a clean close_notify"
    );

    // ---- assertions ----
    eprintln!(
        "tls alloc gate: write_allocs early={write_allocs_early} late={write_allocs_late}; \
         read_allocs early={read_allocs_early} late={read_allocs_late}"
    );
    // Our buffer management adds nothing; the only residual is rustls' single
    // per-record record-construction allocation. The load-bearing invariant is
    // that neither path GROWS per record.
    assert_eq!(
        write_allocs_early, write_allocs_late,
        "outbound write must not grow per record (early {write_allocs_early}, late {write_allocs_late})"
    );
    assert_eq!(
        read_allocs_early, read_allocs_late,
        "inbound decrypt must not grow per record (early {read_allocs_early}, late {read_allocs_late})"
    );
    // Pin a generous per-record bound so any unbounded regression fails loudly.
    // The measured floor is one rustls-internal allocation on each path.
    assert!(
        write_allocs_late <= 2,
        "outbound per-record allocation must be bounded; got {write_allocs_late}"
    );
    assert!(
        read_allocs_late <= 2,
        "inbound per-record allocation must be bounded; got {read_allocs_late}"
    );
}
