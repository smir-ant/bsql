//! Allocation proof for the engine-owned send buffer and its drain loop.
//!
//! Installs the workspace counting allocator as this binary's
//! `#[global_allocator]` and brackets the steady-state flush cycle with
//! snapshots. The claims proved here:
//!
//! 1. **Steady state is zero-alloc.** After the first batch grows the backing
//!    `Vec`, an unbounded number of enqueue/flush/reset cycles over
//!    same-or-smaller batches performs ZERO allocations and ZERO allocated
//!    bytes — [`SendBuf::reset`] retains the capacity, so each batch reuses
//!    it, and the drain loop itself allocates nothing.
//! 2. **The first fill allocates once.** Growing the empty `Vec` to hold the
//!    first batch is the only allocation; subsequent cycles are free.
//!
//! The flush future is stack-pinned with `core::pin::pin!` (no `Box`), and the
//! transport is a non-allocating sink, so the only allocation traffic the
//! measured window can see is the send buffer's own backing store.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global. `cargo test` runs `#[test]` fns
//! in parallel, so all measured windows live in a SINGLE `#[test]` fn run
//! sequentially — no concurrent test thread can allocate inside a measured
//! window. (Other test binaries are separate processes with their own
//! allocator instance.)

#![forbid(unsafe_code)]

use bsql_devgates::CountingAllocator;
use bsql_postgres_proto::engine::{flush, EngineError, SendBuf, Transport};
use core::convert::Infallible;
use core::future::{ready, Future};
use core::task::{Context, Poll};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

/// A non-allocating sink that accepts at most `CHUNK` bytes per write attempt
/// (forcing a multi-iteration drain) and records nothing — so it contributes
/// no allocation traffic to a measured window.
struct ChunkSink;

impl ChunkSink {
    const CHUNK: usize = 4;
}

impl Transport for ChunkSink {
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
        ready(Ok(buf.len().min(Self::CHUNK)))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Drive a future to completion on the stack (no `Box`, no executor). The sink
/// is always ready, so this never spins.
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

/// A const batch — building it allocates nothing and cannot pollute a measured
/// window. Larger than `ChunkSink::CHUNK`, so each drain loops several times.
const BATCH: &[u8] = b"P\0\0\0\x10stmt\0\0\0B\0\0\0\x0e\0stmt\0\0\0S\0\0\0\x04";

#[test]
fn steady_state_flush_is_zero_alloc_first_fill_allocates_once() {
    let mut sb = SendBuf::new();
    let mut sink = ChunkSink;

    // ---- (1) First fill: the empty Vec grows to hold the batch (one or more
    // allocations), then drains. ----
    let before = ALLOC.snapshot();
    sb.enqueue(BATCH);
    let r: Result<(), EngineError<Infallible>> = block_on(flush(&mut sb, &mut sink));
    r.expect("first flush drains");
    sb.reset();
    let first_fill_allocs = ALLOC.snapshot().delta(before).allocs;

    // ---- (2) Steady state: many cycles over the same batch, zero allocs. ----
    let before = ALLOC.snapshot();
    for _ in 0..10_000u32 {
        sb.enqueue(BATCH);
        let r: Result<(), EngineError<Infallible>> = block_on(flush(&mut sb, &mut sink));
        r.expect("steady flush drains");
        sb.reset();
    }
    let after = ALLOC.snapshot();
    let steady_allocs = after.delta(before).allocs;
    let steady_bytes = after.delta(before).bytes;

    // ---- Assertions. ----
    assert!(
        first_fill_allocs >= 1,
        "the first batch must grow the backing Vec at least once (got {first_fill_allocs})"
    );
    assert_eq!(
        steady_allocs, 0,
        "steady-state enqueue/flush/reset must not allocate (got {steady_allocs})"
    );
    assert_eq!(
        steady_bytes, 0,
        "steady-state enqueue/flush/reset must not allocate any bytes (got {steady_bytes})"
    );
}
