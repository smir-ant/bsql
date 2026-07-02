//! Criterion ns/op benches for the sans-IO engine's hot paths.
//!
//! # Scope — the four paths a SELECT-heavy workload spends its time on
//!
//! 1. **`query_params` cache HIT** — the flagship. A statement already Parsed on
//!    the connection, so the verb skips Close+Parse and sends only
//!    Bind+Execute+Sync; the reply (BindComplete, DataRow, CommandComplete,
//!    ReadyForQuery) is framed by the ingest buffer and each row surfaced to the
//!    sink. Driven end to end over an in-process scripted transport, so the
//!    number is the pure wire-build + framing + surface cost with the socket
//!    removed. `iter_batched` primes a fresh engine per iteration OUTSIDE the
//!    timed window, so only the single HIT round-trip is measured.
//! 2. **typed DataRow decode (per-cell)** — parse one `DataRow`, split its
//!    columns, and decode the `(int4, text)` cells. This is the per-cell decode
//!    path the `query!` carrier's non-fixed branch drives; the all-fixed
//!    const-offset fast path is benched against it in the `query_fixture`
//!    crate's `typed_decode` bench (it needs the macro's build catalog).
//! 3. **flush drain loop** — enqueue a request batch and drain it to a ready
//!    sink; the outbound framing loop every verb runs before its first read.
//! 4. **ingest framing loop** — the `read_slot` → `commit` → `next_event` cycle
//!    that turns socket bytes into wire frames; the primary inbound hot loop.
//!
//! # Methodology
//!
//! `black_box` wraps inputs and outputs so LLVM cannot const-fold the fixtures
//! into no-ops. The engine is no_std; this bench target compiles with std (the
//! `[[bench]]` target's own std), exactly as the lib's dev/test targets do.
//!
//! Post-LTO codegen for these paths is inspected with `scripts/asm-linked-diff.sh`
//! (it dumps this bench's fully-optimized binary); ns/op regressions are tracked
//! with `scripts/bench-stable.sh`. See `BENCHMARKING.md`.

#![allow(
    missing_docs,
    reason = "bench harness — criterion's macro-generated wrappers don't take doc comments uniformly; the module docstring and descriptive bench-fn names cover intent"
)]
#![allow(
    clippy::expect_used,
    reason = "bench harness — expect() is the loud fixture-failure signal; a bench is never a #[test] context, so the floor's allow-in-tests carve-out cannot reach it"
)]
#![allow(
    clippy::panic,
    reason = "bench harness — panic on an impossible fixture state is the loud failure signal; the in-tests carve-out cannot reach a bench"
)]

use std::hint::black_box;

use bsql_postgres_proto::engine::{flush, poll_once, IngestBuf, Outcome, SendBuf};
use bsql_postgres_proto::{DataRowRef, FormatCode};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

mod common;
use common::{demo_row, frame, primed_engine, sink, Script, DEMO_QUERY};

/// The flagship: one cache-HIT `query_params` round-trip. The primed engine
/// (handshake + priming MISS) is built in the untimed `setup`; the timed
/// `routine` runs exactly one Bind+Execute+Sync HIT and surfaces its row.
fn bench_query_params_hit(c: &mut Criterion) {
    c.bench_function("query_params/cache_hit", |b| {
        b.iter_batched(
            || primed_engine(1),
            |(mut engine, live)| {
                let out = poll_once(engine.query_params(live, &DEMO_QUERY, (black_box(0),), sink));
                match out {
                    // `Live` is a ZST linear token; black-box then drop it so the
                    // consumed round-trip is not optimized away.
                    Ok(Ok(Outcome { live, .. })) => {
                        black_box(live);
                    }
                    other => panic!("cache-HIT query_params must complete, got {other:?}"),
                }
            },
            BatchSize::SmallInput,
        );
    });
}

/// Per-cell typed decode: parse one `DataRow`, split its columns, decode the
/// `(int4, text)` cells. Mirrors the `query!` carrier's per-cell decode branch.
fn bench_datarow_decode(c: &mut Criterion) {
    let row = demo_row(0x0102_0304, "benchmark-name");
    // Strip the tag + length prefix — `DataRowRef::parse` takes the frame body.
    let body = row[5..].to_vec();
    c.bench_function("decode/datarow_percell", |b| {
        b.iter(|| {
            let dr = DataRowRef::parse(black_box(&body)).expect("well-formed demo row");
            let mut id: i32 = 0;
            let mut name_len = 0usize;
            for (idx, col) in dr.columns().enumerate() {
                let bytes = col.expect("column decodes").expect("column is non-null");
                if idx == 0 {
                    let chunk: [u8; 4] = bytes.try_into().expect("int4 is four bytes");
                    id = i32::from_be_bytes(chunk);
                } else {
                    let s = core::str::from_utf8(bytes).expect("text column is utf-8");
                    name_len = s.len();
                }
            }
            black_box((id, name_len, FormatCode::Binary))
        });
    });
}

/// The outbound framing loop: enqueue a request batch and drain it to a ready
/// sink. `reset` retains the backing capacity, so steady state is alloc-free.
fn bench_flush_drain(c: &mut Criterion) {
    // A representative Bind+Execute+Sync request batch (the HIT wire shape).
    let batch = {
        let mut v = frame(b'B', b"\0bsql_bench_demo\0\0\0\0\0\0\0\x01");
        v.extend_from_slice(&frame(b'E', b"\0\0\0\0\0"));
        v.extend_from_slice(&frame(b'S', &[]));
        v
    };
    let mut sb = SendBuf::new();
    let mut ready_sink = Script::new(Vec::new()); // write accepts everything
    c.bench_function("flush/drain_loop", |b| {
        b.iter(|| {
            sb.enqueue(black_box(&batch));
            let r = poll_once(flush(&mut sb, &mut ready_sink));
            match r {
                Ok(Ok(())) => {}
                other => panic!("flush must drain, got {other:?}"),
            }
            sb.reset();
        });
    });
}

/// The inbound framing loop: `read_slot` → write the frame bytes → `commit` →
/// `next_event`. The steady-state per-frame cost of turning socket bytes into a
/// wire event, with no per-read allocation.
fn bench_ingest_framing(c: &mut Criterion) {
    let row = demo_row(42, "bench");
    let mut ib = IngestBuf::new();
    c.bench_function("ingest/framing_loop", |b| {
        b.iter(|| {
            let slot = ib.read_slot(black_box(row.len())).expect("slot available");
            let n = row.len().min(slot.len());
            slot[..n].copy_from_slice(&row[..n]);
            ib.commit(n).expect("commit fits");
            black_box(ib.next_event());
        });
    });
}

criterion_group!(
    hot_paths,
    bench_query_params_hit,
    bench_datarow_decode,
    bench_flush_drain,
    bench_ingest_framing
);
criterion_main!(hot_paths);
