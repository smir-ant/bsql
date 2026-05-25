//! Criterion bench harness for `bsql-pg-proto` hot paths.
//!
//! # Scope
//!
//! Targets the four hot paths on the wire-frame critical path:
//!
//! 1. **`parse_header`** — single-frame header parse. Runs once per
//!    inbound frame; the only constant-work lookup on every byte of
//!    inbound traffic.
//! 2. **`feed_bytes` Ping round-trip** — push `Ping` command,
//!    feed matching `ReadyForQuery` frame, observe `Pong` action.
//!    Cycles through `push_command` → wire build → `feed_bytes`
//!    → `dispatch()` → `OutActions` materialise. Covers the
//!    B21/C6 `dispatch()` by-ref refactor's real cost.
//! 3. **`feed_bytes` 1000-row DataRow stream** — synthetic
//!    `RowDescription` + N×`DataRow` + `CommandComplete` + `RFQ`
//!    fed as a single block. Exercises `row_stream::fast_path_data_row`
//!    (bypasses dispatch), the primary SELECT hot loop.
//! 4. **`push_command` Startup** — startup-message build path
//!    (encodes user/database/app_name into a frame). Tests the
//!    `compute_push_startup` + `write_buf` emit cost.
//!
//! # Methodology
//!
//! Criterion's default 3-second warmup + 5-second measurement
//! window. `black_box` wraps all inputs/outputs to prevent LLVM
//! from const-folding fixtures into no-ops. Each iteration starts
//! from a fresh `PgProtocol::new()` so prior-call caches don't
//! skew the measurement.
//!
//! # Interpreting output
//!
//! Criterion emits `target/criterion/<bench-name>/report/index.html`
//! with box plots + distribution curves. Regression detection
//! compares against the last saved baseline (`--save-baseline` /
//! `--baseline` flags).

#![allow(missing_docs, reason = "benchmark harness — criterion's \
    macro-generated test wrappers don't tolerate doc comments \
    uniformly, and every bench fn has a descriptive name + the \
    module docstring above covers intent.")]

use bsql_pg_proto::{
    frame::parse_header,
    reply_id::{PingKind, QueryKind},
    PgProtocol, PushFailure, WriteBuf,
};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput,
};

mod common;
use common::fresh_active_via_trust_handshake;

// Bench-side extension trait for the witness-guard typestate.
//
// Routes through `proto.as_ready().push_command(cmd, wb)` —
// returning `Option<ReadyGuard>`, then `Result<(), PushFailure>`
// from the typed push. The helper preserves both guard-acquisition
// + Result discipline so the bench timing reflects the production
// caller's cost.
//
// Benches always start from a fresh `PgProtocol::new()` (Idle state)
// via `iter_batched`-style setup-per-iter or before-loop hoisting —
// the `None` guard arm is unreachable in correctly-built benches,
// so `panic!` surfaces a fixture bug as a loud bench failure rather
// than silent wrong-data. A naive `reset_for_bench` cfg(bench) hook
// would be a tier-3 by-discipline gap — `iter_batched` is the
// idiomatic stable-Rust pattern for stateful per-iter setup.
//
// Cost: one branch on `state.push_class()` per call. On Idle the branch
// is well-predicted, ~1 ns added to the timed path. Same overhead the
// production caller pays for the tier-1 closure check.
trait BenchPushOrPanic {
    fn bench_push_or_panic<C: bsql_pg_proto::push_command::PushCommand>(
        &mut self,
        cmd: C,
        wb: &mut WriteBuf,
    ) -> Result<(), PushFailure>;
}

impl BenchPushOrPanic for PgProtocol<bsql_pg_proto::ActivePhase> {
    #[inline]
    fn bench_push_or_panic<C: bsql_pg_proto::push_command::PushCommand>(
        &mut self,
        cmd: C,
        wb: &mut WriteBuf,
    ) -> Result<(), PushFailure> {
        // Capture status BEFORE the mutable borrow `as_ready` takes —
        // otherwise the panic-arm message would conflict with the
        // mutable borrow's lifetime extending over the whole match.
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!("bench fixture: proto must be Idle for push (status = {status:?})");
        };
        // `push_command` returns `OutActions` to surface
        // borrowed-SQL chunks. The bench drops the iterator
        // immediately — production drains it via `writev` to the
        // socket, which the bench excludes (push path is the
        // measurement target, not the kernel `writev` syscall).
        // Drop is alloc-neutral.
        g.push_command(cmd, wb).map(|_actions| ())
    }
}

// ---------------------------------------------------------------
// Fixture builders — synthetic wire frames with exact PG layout.
// ---------------------------------------------------------------

/// Build a synthetic `ReadyForQuery` frame.
///
/// Wire shape: tag 'Z' (1B) + length 5 BE (4B, includes-self) +
/// tx_status byte 'I' (idle). Total: 6 bytes.
fn rfq_frame() -> [u8; 6] {
    [b'Z', 0x00, 0x00, 0x00, 0x05, b'I']
}

/// Build a synthetic `DataRow` frame with one column of `len`
/// bytes. Wire shape: tag 'D' + length BE (includes-self,
/// includes column count + all column data) + u16 BE column
/// count + i32 BE col length + col data bytes.
fn data_row_frame(len: u16) -> alloc::vec::Vec<u8> {
    let mut out = alloc::vec::Vec::with_capacity(usize::from(len) + 11);
    out.push(b'D');
    // length field (includes-self): 4 (length) + 2 (n_cols) + 4 (col_len) + len
    let total: u32 = u32::from(len).saturating_add(10);
    out.extend_from_slice(&total.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // n_cols = 1
    out.extend_from_slice(&u32::from(len).to_be_bytes()); // col length
    out.extend(core::iter::repeat_n(b'x', usize::from(len)));
    out
}

// `ReplyId::from_raw` is `pub(crate)`. Benches mint via
// `proto.next_reply_id::<K>()` directly inside each iteration.

// ---------------------------------------------------------------
// Bench: parse_header single-frame.
// ---------------------------------------------------------------

fn bench_parse_header(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_header");
    group.throughput(Throughput::Elements(1));

    // Header-only input: dispatch reads only the 5-byte header
    // + the length field to classify the frame.
    let rfq = rfq_frame();

    group.bench_function("rfq_header", |b| {
        b.iter(|| {
            let result = parse_header(black_box(&rfq));
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------
// Bench: feed_bytes Ping round-trip.
// ---------------------------------------------------------------
//
// Full cycle: push_command(Ping) emits Sync frame into write_buf,
// then feed_bytes consumes a synthetic RFQ to drive the state
// Idle → PingAwaitingRfq → Idle. Measures the combined:
//   - compute_push_ping + write_buf.build_sync_message
//   - feed_bytes read_buf append + header parse + dispatch
//   - B21/C6 DispatchOutcome return path
//   - materialise pending Action into OutActions slice

fn bench_ping_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("ping_round_trip");
    group.throughput(Throughput::Elements(1));

    let rfq = rfq_frame();

    group.bench_function("push_then_feed", |b| {
        // `iter_batched_ref` is the correct primitive — Drop on
        // `(PgProtocol, WriteBuf)` falls OUTSIDE the timed window.
        // Per-iter `setup` is also untimed.
        //
        // Quiet-system floor (cargo bench --warm-up 3 --measurement
        // 5): ~114-130 ns. The honest cost of `push + feed_bytes(rfq)`
        // on a post-handshake `<Active>` proto.
        //
        // Note on bench-stable.sh: that wrapper adds `taskpolicy -c
        // utility` (lower QoS) + 30s/10s measurement window. On a
        // moderately-busy system the QoS demotion picks up background
        // contention and reports 200+ ns floor — that is measurement
        // noise from the QoS demotion, NOT a protocol regression.
        // Always cross-check with direct `cargo bench` at normal QoS.
        b.iter_batched_ref(
            || (fresh_active_via_trust_handshake(), WriteBuf::new()),
            |(proto, wb)| {
                // Push Ping — emits Sync frame bytes into write_buf.
                // Mint reply via the public counter API.
                let reply = proto.next_reply_id::<PingKind>();
                let push_out = proto.bench_push_or_panic(
                    bsql_pg_proto::push_command::Ping { reply },
                    wb,
                );
                let _ = black_box(push_out);
                // Feed RFQ — transitions PingAwaitingRfq → Idle + Pong.
                let feed_out = proto.feed_bytes(black_box(&rfq), wb);
                black_box(feed_out);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------
// Bench: iter_rows per-row next_event fast-path.
// ---------------------------------------------------------------
//
// Measures the cost of emitting a single row via
// `row_stream::fast_path_data_row` — the primary SELECT hot loop.
// Setup (not timed): enter StreamingRows state with one DataRow
// frame pre-loaded in read_buf. Timed: pull next_event() once.
//
// Why not a multi-row stream? `feed_bytes` is capped at
// `MAX_STAGED_PER_CALL` = ~9 frames per call, so a "1000-row"
// stream bench via `feed_bytes` actually processes only 9 rows
// then early-returns (measured: 258 ns regardless of stream size).
// A proper multi-row measurement needs an `iter_rows` loop that
// pulls frame-by-frame — but the RowStream API requires read_buf
// to stay populated across pulls, which means chunked feeds. The
// per-row single-pull bench below captures the architectural
// cost unit (one DataRow → one StreamItem::Row emission) without
// the chunking complexity.

// ---------------------------------------------------------------
// Bench: iter_rows per-row amortised throughput.
// ---------------------------------------------------------------
//
// Measures the true per-row cost of the `row_stream` fast-path
// in a hot SELECT loop. Setup (not timed) uses the public
// [`PgProtocol::feed_inbound`] API to pre-populate `read_buf`
// with N DataRow frames — raw append, no dispatch. Timed body
// loops `next_event()` N times, consuming all rows via fast-path.
//
// Throughput reports per-row amortised ns.
//
// # Why feed_inbound is the right setup primitive
//
// Public `feed_bytes` correctly rejects DataRow in
// `SimpleQueryStreamingRows` state — that's production behaviour
// ("caller should use `iter_rows`, not `feed_bytes`" catch-all
// arm). Feeding 100 DataRows after RowDescription via `feed_bytes`
// lands in `Errored(Framing)`, 0 rows pullable. `feed_inbound` is
// the dispatch-bypass primitive for pipelining forward-compat —
// appends bytes without triggering dispatch classification,
// exactly what bench setup needs. A naive `bench_append_read_buf`
// cfg(bench) hook would be a strict duplicate of `feed_inbound`
// and a tier-3 by-discipline gap.
//
// # Row size vs READ_BUF_CAP
//
// READ_BUF_CAP = 4096 B. Using 16-byte DataRow payload:
// 11 bytes (header + col metadata) + 16 (payload) = 27 B per
// row. RowDescription ~27 B. Budget: 4096 - 27 - safety =
// ~3900 B for rows / 27 = ~145 rows max. N_ROWS = 100 fits.

fn bench_iter_rows_per_row_throughput(c: &mut Criterion) {
    use bsql_pg_proto::ColEvent;

    let mut group = c.benchmark_group("iter_rows_via_col_next");

    const N_ROWS: u32 = 100;
    group.throughput(Throughput::Elements(u64::from(N_ROWS)));

    let rowdesc = {
        let mut out = alloc::vec::Vec::new();
        out.push(b'T');
        let name = b"col\0";
        let body_len = 2 + name.len() + 18;
        let total = 4 + body_len;
        out.extend_from_slice(&(u32::try_from(total).unwrap_or(0)).to_be_bytes());
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&25u32.to_be_bytes());
        out.extend_from_slice(&(-1_i16).to_be_bytes());
        out.extend_from_slice(&(-1_i32).to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out
    };
    let single_row = data_row_frame(16);

    group.bench_function("pull_100_rows", |b| {
        b.iter_batched(
            // Setup (not timed): push query, feed RowDesc
            // (legal — dispatch transitions state to
            // StreamingRows), then RAW APPEND 100 DataRow
            // frames to read_buf via the bench hook. The
            // hook bypasses dispatch; iter_rows fast-path
            // will consume them row-by-row.
            || {
                let mut proto = fresh_active_via_trust_handshake();
                let mut wb = WriteBuf::new();
                let reply = proto.next_reply_id::<QueryKind>();
                let push_out = proto.bench_push_or_panic(
                    bsql_pg_proto::push_command::SimpleQuery {
                        sql: "SELECT x",
                        reply,
                    },
                    &mut wb,
                );
                let _ = black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                // Raw-append DataRow bytes into read_buf.
                // feed_inbound returns Result<(), ReadBufFull>;
                // assert on Ok so a setup misconfig (e.g., READ_BUF_CAP
                // shrunk below N_ROWS × row_size) fails loud rather than
                // producing silent garbage numbers. Setup is not timed.
                for _ in 0..N_ROWS {
                    let append_res = proto.feed_inbound(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: feed_inbound must succeed for N_ROWS={N_ROWS}",
                    );
                }
                (proto, wb)
            },
            // Timed: pull rows via iter_rows closure-scoped API
            // until all 100 consumed or stream drains. Each row
            // emits Got × col_count + EndRow events.
            |(mut proto, mut wb)| {
                let rows_seen = proto.iter_rows(&mut wb, |stream| {
                    let mut rows: u32 = 0;
                    loop {
                        match stream.col_next() {
                            ColEvent::Got { .. } | ColEvent::Null { .. } => {}
                            ColEvent::EndRow => {
                                rows = rows.saturating_add(1);
                            }
                            ColEvent::Chunk { .. } | ColEvent::ChunkEnd { .. } => {}
                            ColEvent::NeedMore
                            | ColEvent::EndQuery { .. } => break,
                            _ => break,
                        }
                    }
                    rows
                });
                assert!(
                    rows_seen >= N_ROWS,
                    "per-row bench broken: expected {N_ROWS} rows, pulled {rows_seen}",
                );
                black_box(rows_seen);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------
// The `iter_rows_via_col_next` group above is the canonical per-row
// throughput probe; the three below target
//   - multi-column large-row throughput (`iter_10cols_large_5kb_row`),
//   - partial-frame chunked-body streaming (`iter_jsonb_1mb_streaming`),
//   - per-event dispatch overhead (`col_next_per_event_cost`).
// ---------------------------------------------------------------

/// 10-column row totalling ~5 KB. Exercises:
/// - many Got events per row (col_count = 10);
/// - row body 1 + length-field 4 + col_count 2 + 10 × (4 + 500) =
///   5047 B > READ_BUF_CAP, so partial-frame mode activates and
///   chunked-column events fire.
fn bench_iter_10cols_large_5kb_row(c: &mut Criterion) {
    use bsql_pg_proto::ColEvent;
    let mut group = c.benchmark_group("iter_10cols_large_5kb_row");
    group.throughput(Throughput::Elements(1));

    // Build a 10-column RowDescription.
    let rowdesc: alloc::vec::Vec<u8> = {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&10u16.to_be_bytes());
        for i in 0..10u16 {
            body.extend_from_slice(b"c");
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&i.to_be_bytes());
            body.extend_from_slice(&25u32.to_be_bytes());
            body.extend_from_slice(&(-1_i16).to_be_bytes());
            body.extend_from_slice(&(-1_i32).to_be_bytes());
            body.extend_from_slice(&0u16.to_be_bytes());
        }
        let mut out = alloc::vec::Vec::new();
        out.push(b'T');
        let Ok(total) = u32::try_from(body.len().saturating_add(4)) else {
            unreachable!()
        };
        out.extend_from_slice(&total.to_be_bytes());
        out.extend(body);
        out
    };

    // Build the large DataRow frame: 10 columns × 500 B each.
    let big_row: alloc::vec::Vec<u8> = {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&10i16.to_be_bytes());
        for _ in 0..10 {
            body.extend_from_slice(&500i32.to_be_bytes());
            body.extend(core::iter::repeat_n(b'x', 500));
        }
        let mut out = alloc::vec::Vec::new();
        out.push(b'D');
        let Ok(total) = u32::try_from(body.len().saturating_add(4)) else {
            unreachable!()
        };
        out.extend_from_slice(&total.to_be_bytes());
        out.extend(body);
        out
    };
    let cc_frame: alloc::vec::Vec<u8> = {
        let mut body = alloc::vec::Vec::from(b"SELECT 1".as_slice());
        body.push(0);
        let mut out = alloc::vec::Vec::new();
        out.push(b'C');
        let Ok(total) = u32::try_from(body.len().saturating_add(4)) else {
            unreachable!()
        };
        out.extend_from_slice(&total.to_be_bytes());
        out.extend(body);
        out
    };
    let rfq = rfq_frame();

    group.bench_function("pull_one_big_row", |b| {
        b.iter_batched(
            || {
                let mut proto = fresh_active_via_trust_handshake();
                let mut wb = WriteBuf::new();
                let reply = proto.next_reply_id::<QueryKind>();
                let push_out = proto.bench_push_or_panic(
                    bsql_pg_proto::push_command::SimpleQuery {
                        sql: "SELECT x",
                        reply,
                    },
                    &mut wb,
                );
                let _ = black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let total_chunks = proto.iter_rows(&mut wb, |stream| {
                    // Feed the big-row body in slices ≤ READ_BUF_CAP
                    // so partial-frame mode activates.
                    let mut fed = 0usize;
                    let bytes = &big_row;
                    let chunk_size = 2048usize;
                    let mut chunk_count = 0u32;
                    while fed < bytes.len() {
                        let end = core::cmp::min(fed.saturating_add(chunk_size), bytes.len());
                        let Some(s) = bytes.get(fed..end) else { break };
                        let _ = stream.feed(s);
                        fed = end;
                        // Drain events for this feed slice.
                        loop {
                            match stream.col_next() {
                                ColEvent::Got { .. }
                                | ColEvent::Null { .. }
                                | ColEvent::Chunk { .. }
                                | ColEvent::ChunkEnd { .. } => {
                                    chunk_count = chunk_count.saturating_add(1);
                                }
                                ColEvent::EndRow => {}
                                ColEvent::EndQuery { .. } => return chunk_count,
                                ColEvent::NeedMore => break,
                                _ => break,
                            }
                        }
                    }
                    // Drain trailing CC + RFQ.
                    let _ = stream.feed(&cc_frame);
                    let _ = stream.feed(&rfq);
                    loop {
                        match stream.col_next() {
                            ColEvent::EndQuery { .. } => break,
                            ColEvent::NeedMore => break,
                            _ => {}
                        }
                    }
                    chunk_count
                });
                black_box(total_chunks);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// 1 MB BYTEA / JSONB-style single-column streaming.
/// The headline scenario for partial-frame mode: a single column body
/// of 1 MiB is streamed as a sequence of `Chunk` events bounded by
/// READ_BUF_CAP. Pre-Sub-A this tore down the connection with
/// `FrameTooLarge`; Sub-A streams it as `Chunk × N → ChunkEnd`.
fn bench_iter_jsonb_1mb_streaming(c: &mut Criterion) {
    use bsql_pg_proto::ColEvent;
    let mut group = c.benchmark_group("iter_jsonb_1mb_streaming");
    group.throughput(Throughput::Bytes(1 << 20));

    let rowdesc = {
        let mut out = alloc::vec::Vec::new();
        out.push(b'T');
        let name = b"col\0";
        let body_len = 2 + name.len() + 18;
        let total = 4 + body_len;
        out.extend_from_slice(&(u32::try_from(total).unwrap_or(0)).to_be_bytes());
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&25u32.to_be_bytes());
        out.extend_from_slice(&(-1_i16).to_be_bytes());
        out.extend_from_slice(&(-1_i32).to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out
    };

    // 1 MiB single-column DataRow frame.
    let huge_row: alloc::vec::Vec<u8> = {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        let payload_len: i32 = 1 << 20;
        body.extend_from_slice(&payload_len.to_be_bytes());
        let Ok(payload_len_usize) = usize::try_from(payload_len) else {
            unreachable!()
        };
        body.extend(core::iter::repeat_n(b'x', payload_len_usize));
        let mut out = alloc::vec::Vec::new();
        out.push(b'D');
        let Ok(total) = u32::try_from(body.len().saturating_add(4)) else {
            unreachable!()
        };
        out.extend_from_slice(&total.to_be_bytes());
        out.extend(body);
        out
    };
    let cc_frame: alloc::vec::Vec<u8> = {
        let mut body = alloc::vec::Vec::from(b"SELECT 1".as_slice());
        body.push(0);
        let mut out = alloc::vec::Vec::new();
        out.push(b'C');
        let Ok(total) = u32::try_from(body.len().saturating_add(4)) else {
            unreachable!()
        };
        out.extend_from_slice(&total.to_be_bytes());
        out.extend(body);
        out
    };
    let rfq = rfq_frame();

    group.bench_function("stream_1mb_chunked", |b| {
        b.iter_batched(
            || {
                let mut proto = fresh_active_via_trust_handshake();
                let mut wb = WriteBuf::new();
                let reply = proto.next_reply_id::<QueryKind>();
                let push_out = proto.bench_push_or_panic(
                    bsql_pg_proto::push_command::SimpleQuery {
                        sql: "SELECT x",
                        reply,
                    },
                    &mut wb,
                );
                let _ = black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let total_bytes = proto.iter_rows(&mut wb, |stream| {
                    let chunk_feed_size = 3500usize; // < READ_BUF_CAP
                    let mut fed = 0usize;
                    let mut bytes_collected: u64 = 0;
                    while fed < huge_row.len() {
                        let end = core::cmp::min(
                            fed.saturating_add(chunk_feed_size),
                            huge_row.len(),
                        );
                        let Some(s) = huge_row.get(fed..end) else { break };
                        let _ = stream.feed(s);
                        fed = end;
                        loop {
                            match stream.col_next() {
                                ColEvent::Got { bytes, .. }
                                | ColEvent::Chunk { bytes, .. }
                                | ColEvent::ChunkEnd { bytes, .. } => {
                                    let Ok(n) = u64::try_from(bytes.len()) else {
                                        break;
                                    };
                                    bytes_collected =
                                        bytes_collected.saturating_add(n);
                                }
                                ColEvent::EndRow => {}
                                ColEvent::EndQuery { .. } => return bytes_collected,
                                ColEvent::NeedMore => break,
                                _ => break,
                            }
                        }
                    }
                    let _ = stream.feed(&cc_frame);
                    let _ = stream.feed(&rfq);
                    loop {
                        match stream.col_next() {
                            ColEvent::EndQuery { .. } => break,
                            ColEvent::NeedMore => break,
                            _ => {}
                        }
                    }
                    bytes_collected
                });
                black_box(total_bytes);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Per-event overhead measurement. 100 small
/// rows (single 4-B column each) pre-loaded; each col_next yields
/// one Got + one EndRow (200 events). Isolates the dispatch-loop
/// cost from wire setup.
fn bench_col_next_per_event_cost(c: &mut Criterion) {
    use bsql_pg_proto::ColEvent;
    let mut group = c.benchmark_group("col_next_per_event_cost");
    const N_ROWS: u32 = 100;
    // Each row emits 2 events (Got + EndRow).
    group.throughput(Throughput::Elements(u64::from(N_ROWS).saturating_mul(2)));

    let rowdesc = {
        let mut out = alloc::vec::Vec::new();
        out.push(b'T');
        let name = b"col\0";
        let body_len = 2 + name.len() + 18;
        let total = 4 + body_len;
        out.extend_from_slice(&(u32::try_from(total).unwrap_or(0)).to_be_bytes());
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&25u32.to_be_bytes());
        out.extend_from_slice(&(-1_i16).to_be_bytes());
        out.extend_from_slice(&(-1_i32).to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out
    };
    let single_row = data_row_frame(4);

    group.bench_function("200_events_tight_loop", |b| {
        b.iter_batched(
            || {
                let mut proto = fresh_active_via_trust_handshake();
                let mut wb = WriteBuf::new();
                let reply = proto.next_reply_id::<QueryKind>();
                let push_out = proto.bench_push_or_panic(
                    bsql_pg_proto::push_command::SimpleQuery {
                        sql: "SELECT x",
                        reply,
                    },
                    &mut wb,
                );
                let _ = black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                for _ in 0..N_ROWS {
                    let append_res = proto.feed_inbound(&single_row);
                    assert!(append_res.is_ok());
                }
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let events_seen = proto.iter_rows(&mut wb, |stream| {
                    let mut events: u32 = 0;
                    loop {
                        match stream.col_next() {
                            ColEvent::Got { .. } | ColEvent::Null { .. } => {
                                events = events.saturating_add(1);
                            }
                            ColEvent::EndRow => {
                                events = events.saturating_add(1);
                            }
                            ColEvent::Chunk { .. } | ColEvent::ChunkEnd { .. } => {
                                events = events.saturating_add(1);
                            }
                            ColEvent::NeedMore | ColEvent::EndQuery { .. } => break,
                            _ => break,
                        }
                    }
                    events
                });
                assert!(events_seen >= N_ROWS.saturating_mul(2));
                black_box(events_seen);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------
// Bench: push_command Ping isolated.
// ---------------------------------------------------------------
//
// Isolates the push path — compute_push_ping + write_buf Sync
// frame emission + OutActions materialisation. No inbound traffic.

fn bench_push_ping(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_command");
    group.throughput(Throughput::Elements(1));

    // Full-cycle push (allocate proto + wb, push, drop). Includes
    // zeroize-on-Drop cost (`WriteBuf` zeroize 4 KB + `ReadBuf`
    // zeroize 4 KB on Drop). Production parallel: connection
    // lifetime.
    group.bench_function("ping", |b| {
        // `iter_batched_ref` — see `ping_round_trip/push_then_feed`
        // above for the rationale.
        //
        // Quiet-system floor (cargo bench --warm-up 3
        // --measurement 5): ~24-30 ns. The number is bench-harness
        // shape, NOT pure protocol cost — `black_box` on
        // `Result<OutActions<'w,'r>, PushFailure>` (~88 B return
        // slot) materialises ~80 `ldrh` instructions inside the
        // timed window (asm-verified at offsets after the
        // `bl bench_push_or_panic` call). Production callers iterate
        // the `OutActions` once via `for action in out { writev(...) }`
        // — they do not pay the `black_box` reads.
        //
        // The "pure push" cost on a post-handshake proto is
        // ~10-15 ns, recoverable from the asm decomposition (~5 ns
        // write_buf clear + ~3 ns residue clear + ~5 ns state
        // transition + ~2 ns single-pass materialise). Bench number
        // ≈ push cost + ~10 ns harness overhead.
        b.iter_batched_ref(
            || (fresh_active_via_trust_handshake(), WriteBuf::new()),
            |(proto, wb)| {
                // Use the per-command `Ping` struct directly —
                // 16 B by-value vs a naive `PgCommand`-enum shape
                // (sized to `Parse`) that would force a 2176 B
                // value on each push. Mint via the public counter
                // API.
                let reply = proto.next_reply_id::<PingKind>();
                let out = proto.bench_push_or_panic(
                    bsql_pg_proto::push_command::Ping::new(reply),
                    wb,
                );
                let _ = black_box(out);
            },
            BatchSize::SmallInput,
        );
    });

    // A naive `ping_amortised` sub-bench using a `reset_for_bench`
    // hook to reuse the same `PgProtocol` across criterion
    // iterations (cache-warm, ~10 ns timing — production-relevant
    // since real connections reuse proto across thousands of
    // queries) is out: the only safe-Rust replacement (criterion's
    // `iter_batched_ref` with fresh proto per iter) reports a
    // ~47 ns floor (~30 ns criterion batch-management overhead +
    // ~17 ns first-touch cache cost on each fresh proto), which
    // **misleads readers** about the actual push cost — production
    // push remains ~10 ns (cache-warm).
    //
    // Decision: no `ping_amortised` rather than ship a misleading
    // metric. Coverage closed by:
    //   - `ping_round_trip/push_then_feed` (~113 ns full cycle —
    //     push + feed_bytes(rfq) — fresh proto each iter, honest
    //     cold-path cycle measurement). Regression on push WILL
    //     show up here as cycle-time growth; signal preserved.
    //   - `push_command/ping` (above, fresh proto per iter) — same
    //     methodology, cold-cycle push-only number. Honest metric.
    //
    // If a future audit demands precise sub-ns per-push measurement
    // for regression hunt, options: (a) hand-rolled `black_box` loop
    // with `cargo asm` verification, (b) custom criterion harness,
    // (c) a tier-1 reset mechanism (non-feature-gated public API
    // such as `proto.reset()` with proper scrub semantics — would
    // be a real production API, not bench-only).

    group.finish();
}

// ---------------------------------------------------------------
// Per-column decode hot path.
// ---------------------------------------------------------------
//
// Covers what happens AFTER `RowStream::next_row_bytes` hands raw
// row bytes to the caller: `DataRowRef::parse` (column-count header
// parse), `ColumnsIter` per-column length-prefix walk, and
// `FromPgText::from_pg_text` typed decoding.
//
// Real per-row latency on row-bearing responses is dominated by
// per-column work (parse + iterate + decode), not frame dispatch.
// The measurement infrastructure below makes perf claims on
// decoder optimisations (LUT dispatch, `simdutf8` text validation,
// niche tightening) evidence-backed per CREDO §4.12.
//
// # Bench shape
//
// Each bench parses a synthetic DataRow body (post-frame-header,
// post-frame-length-prefix bytes) and walks all columns. Bodies are
// pre-built in setup so the bench measures only the parse +
// iteration + decode work, not the body-construction cost.
//
// Production analog: each iter measures the work the user pays per
// row on a SELECT response. RowStream's frame-dispatch + emission
// cost is covered by `iter_rows_via_next_row_bytes` (separate bench);
// these benches focus on what happens AFTER `next_row_bytes` returns
// raw bytes and the user calls `DataRowRef::parse(bytes)`.

/// Build a synthetic DataRow body with `n_cols` columns of i32 values.
///
/// Wire shape (post-header, what `RowStream::next_row_bytes` returns):
///   int16 n_cols (BE)
///   for each col: int32 col_len (BE) + col_len bytes (text "value\0"
///                 — actually no NUL, just the digits as ASCII)
fn data_row_body_int4(n_cols: u16, value: i32) -> alloc::vec::Vec<u8> {
    let value_str = alloc::format!("{value}");
    let value_bytes = value_str.as_bytes();
    let col_len: i32 = i32::try_from(value_bytes.len()).unwrap_or(0);
    let mut body = alloc::vec::Vec::with_capacity(
        2 + usize::from(n_cols) * (4 + value_bytes.len()),
    );
    body.extend_from_slice(&n_cols.to_be_bytes());
    for _ in 0..n_cols {
        body.extend_from_slice(&col_len.to_be_bytes());
        body.extend_from_slice(value_bytes);
    }
    body
}

/// Build a DataRow body with one column
/// per supplied i32 value (text-format ASCII digits, no NUL).
///
/// Used by the `iter_5cols_decode_i32_common_values` bench to feed
/// the common-literal fast-paths (`0`, `1`, `-1`) deliberately.
/// Per-column len varies with the digit count of each value (`-1`
/// is 2 bytes, `0` / `1` are 1 byte each); the column-count header
/// plus length-prefix layout matches `data_row_body_int4`'s shape
/// so the per-row parse cost is comparable.
fn data_row_body_int4_mixed(values: &[i32]) -> alloc::vec::Vec<u8> {
    let n_cols: u16 = u16::try_from(values.len()).unwrap_or(0);
    let mut body = alloc::vec::Vec::with_capacity(2 + values.len() * 8);
    body.extend_from_slice(&n_cols.to_be_bytes());
    for v in values {
        let s = alloc::format!("{v}");
        let bytes = s.as_bytes();
        let col_len: i32 = i32::try_from(bytes.len()).unwrap_or(0);
        body.extend_from_slice(&col_len.to_be_bytes());
        body.extend_from_slice(bytes);
    }
    body
}

/// Build a synthetic DataRow body with `n_cols` columns of fixed-len text.
fn data_row_body_text(n_cols: u16, text: &str) -> alloc::vec::Vec<u8> {
    let bytes = text.as_bytes();
    let col_len: i32 = i32::try_from(bytes.len()).unwrap_or(0);
    let mut body = alloc::vec::Vec::with_capacity(
        2 + usize::from(n_cols) * (4 + bytes.len()),
    );
    body.extend_from_slice(&n_cols.to_be_bytes());
    for _ in 0..n_cols {
        body.extend_from_slice(&col_len.to_be_bytes());
        body.extend_from_slice(bytes);
    }
    body
}

/// Build a DataRow body alternating non-null and SQL NULL columns.
/// `n_value_cols` value columns interleaved with `n_value_cols` NULL
/// columns, totalling `2 * n_value_cols` columns. Tests the NULL
/// fast-path in ColumnsIter (col_len = -1 signals NULL, no data
/// bytes).
fn data_row_body_alternating_null(n_value_cols: u16, value: i32) -> alloc::vec::Vec<u8> {
    let value_str = alloc::format!("{value}");
    let value_bytes = value_str.as_bytes();
    let col_len: i32 = i32::try_from(value_bytes.len()).unwrap_or(0);
    let total_cols: u16 = n_value_cols.saturating_mul(2);
    let mut body = alloc::vec::Vec::with_capacity(
        2 + usize::from(n_value_cols) * (4 + value_bytes.len()) + usize::from(n_value_cols) * 4,
    );
    body.extend_from_slice(&total_cols.to_be_bytes());
    for _ in 0..n_value_cols {
        // Value column
        body.extend_from_slice(&col_len.to_be_bytes());
        body.extend_from_slice(value_bytes);
        // NULL column (col_len = -1, no data)
        body.extend_from_slice(&(-1_i32).to_be_bytes());
    }
    body
}

/// Bench: pure `DataRowRef::parse` cost — parse the column-count
/// header from the body bytes. No iteration, no decode. Lower bound
/// on per-row parse cost.
fn bench_data_row_parse(c: &mut Criterion) {
    use bsql_pg_proto::decode::DataRowRef;

    let mut group = c.benchmark_group("column_decode");
    group.throughput(Throughput::Elements(1));

    let body = data_row_body_int4(5, 42);

    group.bench_function("data_row_parse_5cols", |b| {
        b.iter(|| {
            // Both arms `black_box`'d — measures the parse call path,
            // not post-parse use. Fixture is well-formed (controlled
            // by `data_row_body_int4`); the Err arm is dead in this
            // bench but exists to satisfy `Result`'s `#[must_use]`
            // without a silent `let _ =` discard (CREDO §0 — even in
            // bench harness, no silent fallbacks).
            match DataRowRef::parse(black_box(&body)) {
                Ok(row) => {
                    black_box(row);
                }
                Err(e) => {
                    black_box(e);
                }
            }
        });
    });

    group.finish();
}

/// Bench: `DataRowRef::parse` + `ColumnsIter` walk WITHOUT typed
/// decode. Measures the per-column length-prefix walk + slice
/// returns. Subtract this from the typed-decode benches below to
/// isolate the FromPgText decode cost.
fn bench_iter_columns_raw(c: &mut Criterion) {
    use bsql_pg_proto::decode::DataRowRef;

    let mut group = c.benchmark_group("column_decode");
    const N_COLS: u16 = 5;
    group.throughput(Throughput::Elements(u64::from(N_COLS)));

    let body = data_row_body_int4(N_COLS, 42);

    group.bench_function("iter_5cols_raw_no_decode", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut sum_len: usize = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col {
                    sum_len = sum_len.saturating_add(bytes.len());
                }
            }
            black_box(sum_len);
        });
    });

    group.finish();
}

/// Bench: full per-column decode for 5 i32 columns via
/// `FromPgText`. Production-relevant cost on `SELECT id, ... FROM ...`
/// queries with integer columns.
fn bench_iter_columns_5x_int4_decode(c: &mut Criterion) {
    use bsql_pg_proto::decode::{DataRowRef, FromPgText};

    let mut group = c.benchmark_group("column_decode");
    const N_COLS: u16 = 5;
    group.throughput(Throughput::Elements(u64::from(N_COLS)));

    let body = data_row_body_int4(N_COLS, 42_000_000);

    group.bench_function("iter_5cols_decode_i32", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut sum: i64 = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col
                    && let Ok(v) = i32::from_pg_text(bytes)
                {
                    sum = sum.saturating_add(i64::from(v));
                }
            }
            black_box(sum);
        });
    });

    // Common-value cache hit measurement.
    // The 5 columns are populated with {0, 1, -1, 0, 1} — the three
    // literals the fast-path branches exist for. On hit, the digit
    // loop is bypassed entirely; the bench measures the byte-equality
    // match cost in isolation.
    //
    // Compare with `iter_5cols_decode_i32` (above, 8-digit literal
    // 42_000_000) for the cache-miss baseline. Hit-vs-miss delta
    // should be 1-2 ns/col with the common-value cache active.
    let body_common = data_row_body_int4_mixed(&[0, 1, -1, 0, 1]);
    group.bench_function("iter_5cols_decode_i32_common_values", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body_common)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut sum: i64 = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col
                    && let Ok(v) = i32::from_pg_text(bytes)
                {
                    sum = sum.saturating_add(i64::from(v));
                }
            }
            black_box(sum);
        });
    });

    group.finish();
}

/// Caller-routed SWAR fast-path
/// for short unsigned integers (ASCII-decimal, 0..=9999).
///
/// `parse_short_uint_swar` is exposed as an opt-in helper at the
/// `decode` module surface, NOT integrated into
/// `<i32 as FromPgText>::from_pg_text`. Two prior attempts to
/// embed the SWAR inside `from_pg_text` regressed adjacent benches
/// (Attempt 1: text +4-7%, 8-digit +5.2% from icache pressure;
/// Attempt 2: `iter_5cols_decode_i32_common_values` +31% from
/// `SimplifyCFG` merging dispatch with the common-value
/// match — forensics at `/tmp/asm-attempt{1,2}-i32.s`).
///
/// This bench measures the realistic call shape: the caller knows
/// (via SQL type info) the column is a short unsigned integer
/// and tries the fast-path first, falling back to the generic
/// `from_pg_text` on miss. The 4-digit body shape (`1234` ×5
/// columns) exercises the SWAR path. Hit-case target: ~17-20 ns
/// per row vs ~30+ ns for the generic 4-digit decode.
///
/// Compare with `iter_5cols_decode_i32` (8-digit literal
/// `42_000_000`, generic decode) and `iter_5cols_decode_i32_common_values`
/// (common-value fast-path on `0`/`1`/`-1`). The SWAR helper is
/// orthogonal to both — caller-routed dispatch decoupled from the
/// `from_pg_text` body.
fn bench_iter_columns_5x_int4_swar_short(c: &mut Criterion) {
    use bsql_pg_proto::decode::{DataRowRef, FromPgText, parse_short_uint_swar};

    let mut group = c.benchmark_group("column_decode");
    const N_COLS: u16 = 5;
    group.throughput(Throughput::Elements(u64::from(N_COLS)));

    // 4-digit values exercise the SWAR helper at its full-cap
    // shape (the slowest valid input for the helper). Real-world
    // analogues: HTTP status codes, port numbers, small counts
    // with leading-digit variance.
    let body = data_row_body_int4(N_COLS, 1234);

    group.bench_function("iter_5cols_decode_i32_short_4digit_via_swar", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut sum: i64 = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col {
                    // Caller-routed fast-path: try SWAR first, fall
                    // back to generic decode on miss. This is the
                    // realistic call shape — caller has type info,
                    // SWAR covers the unsigned 0..=9999 subset, the
                    // generic path covers everything else.
                    let v = parse_short_uint_swar(bytes)
                        .and_then(|u| i32::try_from(u).ok())
                        .or_else(|| i32::from_pg_text(bytes).ok());
                    if let Some(v) = v {
                        sum = sum.saturating_add(i64::from(v));
                    }
                }
            }
            black_box(sum);
        });
    });

    group.finish();
}

/// β SWAR extension: caller-routed fast-path
/// for unsigned i64 text decoding on 5-19 digit values.
///
/// `parse_long_uint_swar` extends the SWAR-fast-path precedent to
/// the i64-representable range. Bench shape: 5 columns of an 8-digit
/// literal (`42_000_000`) — the same body as
/// `iter_5cols_decode_i32` for direct comparison. The 8-digit shape
/// puts the helper in the mid-band (between len-5 minimum and
/// len-19 maximum). Realistic analogues: unix timestamps, customer
/// IDs, transaction IDs.
fn bench_iter_columns_5x_int4_swar_long(c: &mut Criterion) {
    use bsql_pg_proto::decode::{DataRowRef, FromPgText, parse_long_uint_swar};

    let mut group = c.benchmark_group("column_decode");
    const N_COLS: u16 = 5;
    group.throughput(Throughput::Elements(u64::from(N_COLS)));

    // 8-digit value — middle of the helper's 5-19 acceptance window.
    // Same body shape as `iter_5cols_decode_i32` for compare.
    let body = data_row_body_int4(N_COLS, 42_000_000);

    group.bench_function("iter_5cols_decode_i32_long_8digit_via_swar", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut sum: i64 = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col {
                    // Caller knows the column is unsigned-ish i32;
                    // SWAR long-uint covers the 5-19 digit unsigned
                    // mid-band. Sign handling falls back to generic.
                    let v = parse_long_uint_swar(bytes)
                        .and_then(|u| i32::try_from(u).ok())
                        .or_else(|| i32::from_pg_text(bytes).ok());
                    if let Some(v) = v {
                        sum = sum.saturating_add(i64::from(v));
                    }
                }
            }
            black_box(sum);
        });
    });

    group.finish();
}

/// β SWAR extension: micro-bench for the
/// all-ASCII fast-path UTF-8 validator.
///
/// `validate_utf8_swar` returns `Some(())` on pure ASCII input
/// (skipping the full `simdutf8` validator's setup overhead).
/// Three shapes pin the per-length cost curve:
///
/// 1. **Short ASCII** (17 B `alice@example.com`) — typical column
///    name / short identifier. The helper's win is largest here
///    because `simdutf8` setup cost dominates short inputs.
/// 2. **Long ASCII** (200 B descriptive text) — log lines, free
///    text. Helper still wins (scanning is one masked-OR per 8 B)
///    but `simdutf8` amortises better.
/// 3. **Multi-byte UTF-8** (Cyrillic ~80 B) — helper returns `None`
///    (fast-path miss); pin the miss-arm cost as ≤ helper-hit cost
///    minus the SIMD setup cost.
fn bench_validate_utf8_swar(c: &mut Criterion) {
    use bsql_pg_proto::decode::validate_utf8_swar;

    let mut group = c.benchmark_group("column_decode");
    group.throughput(Throughput::Elements(1));

    let short_ascii: &[u8] = b"alice@example.com"; // 17 B
    let long_ascii: alloc::vec::Vec<u8> = b"The quick brown fox jumps over the lazy dog. \
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. \
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. \
        Ut enim ad minim veniam, quis nostrud exercitation."
        .to_vec();
    let cyrillic: alloc::vec::Vec<u8> = "Привет, мир! Это многобайтный UTF-8 текст для проверки.".as_bytes().to_vec();

    group.bench_function("validate_utf8_swar_short_ascii_17b", |b| {
        b.iter(|| {
            let r = validate_utf8_swar(black_box(short_ascii));
            black_box(r);
        });
    });

    group.bench_function("validate_utf8_swar_long_ascii_200b", |b| {
        b.iter(|| {
            let r = validate_utf8_swar(black_box(&long_ascii));
            black_box(r);
        });
    });

    group.bench_function("validate_utf8_swar_multibyte_miss", |b| {
        b.iter(|| {
            let r = validate_utf8_swar(black_box(&cyrillic));
            black_box(r);
        });
    });

    group.finish();
}

/// β SWAR extension: micro-bench for the
/// PG boolean text-literal cache-hit parser.
///
/// `parse_pg_bool_swar` recognises the four PG-wire-legal forms
/// (`b"t"` / `b"f"` / `b"true"` / `b"false"`) and returns `None`
/// on every other byte slice. Measures each accepted shape so
/// LLVM jump-table layout is visible; a miss-case measures the
/// fall-through cost.
fn bench_parse_pg_bool_swar(c: &mut Criterion) {
    use bsql_pg_proto::decode::parse_pg_bool_swar;

    let mut group = c.benchmark_group("column_decode");
    group.throughput(Throughput::Elements(1));

    group.bench_function("parse_pg_bool_swar_t", |b| {
        b.iter(|| {
            let r = parse_pg_bool_swar(black_box(b"t"));
            black_box(r);
        });
    });

    group.bench_function("parse_pg_bool_swar_f", |b| {
        b.iter(|| {
            let r = parse_pg_bool_swar(black_box(b"f"));
            black_box(r);
        });
    });

    group.bench_function("parse_pg_bool_swar_true", |b| {
        b.iter(|| {
            let r = parse_pg_bool_swar(black_box(b"true"));
            black_box(r);
        });
    });

    group.bench_function("parse_pg_bool_swar_false", |b| {
        b.iter(|| {
            let r = parse_pg_bool_swar(black_box(b"false"));
            black_box(r);
        });
    });

    group.bench_function("parse_pg_bool_swar_miss", |b| {
        b.iter(|| {
            let r = parse_pg_bool_swar(black_box(b"yes"));
            black_box(r);
        });
    });

    group.finish();
}

/// Bench: per-column decode for 5 text columns of varying shapes.
///
/// Three shapes cover the realistic Postgres text-column distribution:
///
/// 1. **Short ASCII** (`alice@example.com`, 17 B) — typical OLTP
///    columns: usernames, emails, short identifiers.
/// 2. **Long ASCII** (~200 B) — descriptions, log lines, SQL queries.
///    SIMD UTF-8 validators win most clearly here.
/// 3. **Multi-byte UTF-8** (Cyrillic, ~80 B) — non-ASCII real text.
///    Forces the validator off the ASCII fast-path; SIMD vs scalar
///    delta is largest on this shape.
fn bench_iter_columns_5x_text_decode(c: &mut Criterion) {
    use bsql_pg_proto::decode::{DataRowRef, FromPgText};

    let mut group = c.benchmark_group("column_decode");
    const N_COLS: u16 = 5;
    group.throughput(Throughput::Elements(u64::from(N_COLS)));

    // Shape (1): short ASCII (17 B per col).
    let body_short = data_row_body_text(N_COLS, "alice@example.com");

    // Shape (2): long ASCII (~200 B per col) — typical
    // log-line / description column.
    let body_long_ascii = data_row_body_text(
        N_COLS,
        "the quick brown fox jumps over the lazy dog while the spectacled \
         platypus paddles upstream past the misty mountains where ancient \
         dragons coil amid silver birch trees and forgotten lore",
    );

    // Shape (3): multi-byte UTF-8 — Cyrillic, ~78 B per col,
    // forces the validator off the ASCII fast-path.
    let body_cyrillic = data_row_body_text(
        N_COLS,
        "Съешь же ещё этих мягких французских булок, да выпей чаю",
    );

    group.bench_function("iter_5cols_decode_text_short_ascii", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body_short)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut total_chars: usize = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col
                    && let Ok(s) = <&str>::from_pg_text(bytes)
                {
                    total_chars = total_chars.saturating_add(s.len());
                }
            }
            black_box(total_chars);
        });
    });

    group.bench_function("iter_5cols_decode_text_long_ascii", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body_long_ascii)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut total_chars: usize = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col
                    && let Ok(s) = <&str>::from_pg_text(bytes)
                {
                    total_chars = total_chars.saturating_add(s.len());
                }
            }
            black_box(total_chars);
        });
    });

    group.bench_function("iter_5cols_decode_text_cyrillic", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body_cyrillic)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut total_chars: usize = 0;
            for col in row.columns() {
                if let Ok(Some(bytes)) = col
                    && let Ok(s) = <&str>::from_pg_text(bytes)
                {
                    total_chars = total_chars.saturating_add(s.len());
                }
            }
            black_box(total_chars);
        });
    });

    group.finish();
}

/// Bench: alternating NULL / non-NULL columns. Exercises the
/// `col_len == -1` shortcut path in `ColumnsIter::next` (
/// A5/B10 sign-path collapse).
fn bench_iter_columns_with_nulls(c: &mut Criterion) {
    use bsql_pg_proto::decode::{DataRowRef, FromPgText};

    let mut group = c.benchmark_group("column_decode");
    // 5 value cols + 5 null cols = 10 total
    const N_VALUE: u16 = 5;
    const N_TOTAL: u16 = N_VALUE * 2;
    group.throughput(Throughput::Elements(u64::from(N_TOTAL)));

    let body = data_row_body_alternating_null(N_VALUE, 42_000_000);

    group.bench_function("iter_10cols_alternating_null_i32", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body)) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut sum: i64 = 0;
            let mut nulls: u32 = 0;
            for col in row.columns() {
                match col {
                    Ok(Some(bytes)) => {
                        if let Ok(v) = i32::from_pg_text(bytes) {
                            sum = sum.saturating_add(i64::from(v));
                        }
                    }
                    Ok(None) => {
                        nulls = nulls.saturating_add(1);
                    }
                    Err(_) => break,
                }
            }
            black_box((sum, nulls));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------
// `prepared!` macro path benches.
// ---------------------------------------------------------------
//
// Comparison anchor: `push_command/ping` and the
// `push_bind_execute_one_int_param` bench below —
// both measure the same shape (push one command, get OutActions).
// The deferred.md claim was "-25 ns per push" relative to a hand-
// constructed Parse + BindExecute pipeline; the prepared path skips
// per-call header construction by baking Parse + Bind-prefix at
// compile time. The paired bench (`push_bind_execute_one_int_param`)
// is the parity anchor for the DML-shape sister
// (`BENCH_PREPARED_DML_Q`): one i32 param, no result rows (DML),
// fetch-all, push-only fresh PgProtocol per iter. Routes through
// the runtime `BindExecute` path that constructs the Bind frame
// (portal NUL + stmt_name NUL + format codes + n_params header)
// per call. Delta = prepared's per-push advantage.

use bsql_pg_proto::{
    prepared, push_command::BindExecute, FetchRows, PortalName, PreparedQuery, StmtName,
};

const BENCH_PREPARED_Q: PreparedQuery<(i32,), (i32, &'static str)> = prepared!(
    "SELECT id::int4, name::text FROM users WHERE id = $1::int4"
);

/// DML-shape sister to
/// [`BENCH_PREPARED_Q`] for the paired bench. One i32 param, zero
/// result rows (DELETE with WHERE), fetch-all (vacuous since the
/// statement returns 0 rows). Same wire-frame shape as the paired
/// [`bench_push_bind_execute_one_int_param`]'s `BindExecute` with
/// `row_desc = None`.
const BENCH_PREPARED_DML_Q: PreparedQuery<(i32,), ()> = prepared!(
    "DELETE FROM users WHERE id = $1::int4"
);

fn bench_prepared_query_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepared_query_push");
    group.throughput(Throughput::Elements(1));
    group.bench_function("execute_prepared", |b| {
        b.iter_batched_ref(
            || (fresh_active_via_trust_handshake(), WriteBuf::new()),
            |(proto, wb)| {
                let reply = proto.next_reply_id::<QueryKind>();
                let g = match proto.as_ready() {
                    Some(g) => g,
                    None => panic!("bench fixture: proto must be Idle"),
                };
                let out = g.execute_prepared(
                    &BENCH_PREPARED_Q,
                    (42_i32,),
                    FetchRows::All,
                    reply,
                    wb,
                );
                let _ = black_box(out);
            },
            BatchSize::SmallInput,
        );
    });
    // DML-shape variant — matches the
    // paired `push_bind_execute_one_int_param/bind_execute` bench's
    // shape exactly (1 i32 param, no result rows, no RowDesc parking).
    // The delta between this sub-bench and the paired bench is the
    // pure prepared-vs-non-prepared per-push cost, isolating the
    // header-construction saving.
    group.bench_function("execute_prepared_dml", |b| {
        b.iter_batched_ref(
            || (fresh_active_via_trust_handshake(), WriteBuf::new()),
            |(proto, wb)| {
                let reply = proto.next_reply_id::<QueryKind>();
                let g = match proto.as_ready() {
                    Some(g) => g,
                    None => panic!("bench fixture: proto must be Idle"),
                };
                let out = g.execute_prepared(
                    &BENCH_PREPARED_DML_Q,
                    (42_i32,),
                    FetchRows::All,
                    reply,
                    wb,
                );
                let _ = black_box(out);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------
// Paired bench for the «-25 ns»
// verification claim.
// ---------------------------------------------------------------
//
// **Parity anchor** for `prepared_query_push/execute_prepared`:
// same input shape (one i32 param, 2-column schema, fetch-all,
// push-only fresh PgProtocol per iter) but routes through the
// runtime `BindExecute` path that constructs the Bind frame body
// (portal NUL + stmt_name NUL + compact format-code block +
// n_params header + per-param values) on every call. The prepared
// path bakes that prefix at macro-expand time.
//
// Delta interpretation:
//   prepared `execute_prepared` time − this bench's time
//   = the saving per push. Reported as the verification of
//     prepared-macro path -25 ns claim.
//
// Notes on parity precision:
//   - Both benches push 1 i32 param. The PAIRED variant uses
//     `BENCH_PREPARED_DML_Q` (no result rows; matches the
//     `BindExecute { row_desc: None, ... }` shape exactly).
//   - Prepared path stages 4 wire frames (Parse + Bind + Execute +
//     Sync), paired path stages 3 (Bind + Execute + Sync; Parse is
//     out of scope here — in production you Parse once and reuse
//     the stmt_name many times, which is why the prepared path
//     bakes the Parse template).
//   - The comparison is fair: the prepared path does Parse-template
//     ferry (one `SendBytesBorrowed` of static bytes — essentially
//     free) plus pre-baked Bind prefix copy + per-param values +
//     pre-baked Execute + pre-baked Sync. The paired path does
//     Bind-build (runtime prefix construction in-place) +
//     Execute-build (runtime per-call frame) + Sync.
//   - The cost the paired path pays that the prepared path does
//     NOT pay: the runtime Bind frame builder
//     (`build_bind_message`) formats portal_name + stmt_name +
//     format-code block + n_params bytes, AND `build_execute_message`
//     formats the Execute frame's portal name + max_rows bytes.
//     The prepared macro baked both into static bytes. THIS is the
//     "-25 ns" headline.

/// Stmt name allocator for the paired bench — content is irrelevant
/// since push-only mode (no server reply); a single small static
/// portal/stmt pair amortises across iterations. Build-time-fallible
/// `try_from_str` is consumed inside the bench setup; failure aborts
/// the benchmark fixture (acceptable: bench fixture bugs surface
/// loud, not silently).
#[inline]
fn make_stmt_name() -> StmtName {
    match StmtName::try_from_str("s") {
        Ok(n) => n,
        Err(_) => panic!("bench fixture: 's' must be a valid StmtName"),
    }
}

#[inline]
fn make_portal_name() -> PortalName {
    match PortalName::try_from_str("p") {
        Ok(n) => n,
        Err(_) => panic!("bench fixture: 'p' must be a valid PortalName"),
    }
}

fn bench_push_bind_execute_one_int_param(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_bind_execute_one_int_param");
    group.throughput(Throughput::Elements(1));
    // Pair anchor for
    // `prepared_query_push/execute_prepared_dml`. Same exact shape —
    // 1 i32 param, no RowDesc (DML), fetch-all. The two benches form
    // the verification couple for the prepared-macro -25 ns per
    // push" claim: prepared time minus this time = per-push saving.
    group.bench_function("bind_execute", |b| {
        // Build the names once; criterion's iter_batched_ref gives
        // us fresh proto + wb per iter (same fixture shape as the
        // prepared bench).
        let stmt = make_stmt_name();
        let portal = make_portal_name();
        b.iter_batched_ref(
            || (fresh_active_via_trust_handshake(), WriteBuf::new()),
            |(proto, wb)| {
                let reply = proto.next_reply_id::<QueryKind>();
                let g = match proto.as_ready() {
                    Some(g) => g,
                    None => panic!("bench fixture: proto must be Idle"),
                };
                let out = g.push_command(
                    BindExecute {
                        portal_name: &portal,
                        stmt_name: &stmt,
                        params: &(42_i32,),
                        row_desc: None, // DML — matches BENCH_PREPARED_DML_Q's `()` row tuple
                        fetch: FetchRows::All,
                        reply,
                    },
                    wb,
                );
                let _ = black_box(out);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// `prepared_iter_rows_typed` — end-to-end push + feed-bytes for a
// 10-row reply + `collect_tuple` per row. Mirrors
// `bench_iter_rows_per_row_throughput`'s shape but for the prepared
// macro path.
fn bench_prepared_iter_rows_typed(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepared_iter_rows_typed");
    group.throughput(Throughput::Elements(10));

    // Pre-build the server reply: ParseComplete + BindComplete +
    // 10 × DataRow + CommandComplete + RFQ.
    let mut server_bytes: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
    server_bytes.extend_from_slice(&[b'1', 0, 0, 0, 4]);
    server_bytes.extend_from_slice(&[b'2', 0, 0, 0, 4]);
    for i in 0..10_u32 {
        let id_text = alloc::format!("{i}");
        let name_text = alloc::format!("user_{i}");
        let id_b = id_text.as_bytes();
        let nm_b = name_text.as_bytes();
        let body_len = 2_usize + 4 + id_b.len() + 4 + nm_b.len();
        let total_len = 4 + body_len;
        let mut frame = alloc::vec::Vec::new();
        frame.push(b'D');
        if let Ok(tl) = u32::try_from(total_len) {
            frame.extend_from_slice(&tl.to_be_bytes());
        }
        frame.extend_from_slice(&2i16.to_be_bytes());
        if let Ok(l) = i32::try_from(id_b.len()) {
            frame.extend_from_slice(&l.to_be_bytes());
        }
        frame.extend_from_slice(id_b);
        if let Ok(l) = i32::try_from(nm_b.len()) {
            frame.extend_from_slice(&l.to_be_bytes());
        }
        frame.extend_from_slice(nm_b);
        server_bytes.extend_from_slice(&frame);
    }
    // CommandComplete body: NUL-terminated ASCII "SELECT 10"
    let cc_body = b"SELECT 10\0";
    let cc_total = 4 + cc_body.len();
    let mut cc = alloc::vec::Vec::new();
    cc.push(b'C');
    if let Ok(tl) = u32::try_from(cc_total) {
        cc.extend_from_slice(&tl.to_be_bytes());
    }
    cc.extend_from_slice(cc_body);
    server_bytes.extend_from_slice(&cc);
    server_bytes.extend_from_slice(&rfq_frame());

    group.bench_function("push_feed_collect_10rows", |b| {
        b.iter_batched_ref(
            || (fresh_active_via_trust_handshake(), WriteBuf::new()),
            |(proto, wb)| {
                let reply = proto.next_reply_id::<QueryKind>();
                {
                    let g = match proto.as_ready() {
                        Some(g) => g,
                        None => panic!("bench fixture: proto must be Idle"),
                    };
                    let actions_result = g.execute_prepared(
                        &BENCH_PREPARED_Q,
                        (42_i32,),
                        FetchRows::All,
                        reply,
                        wb,
                    );
                    if actions_result.is_err() {
                        panic!("bench fixture: execute_prepared errored");
                    }
                }
                let mut rows_count: u32 = 0;
                let _result: Result<(), bsql_pg_proto::ProtocolError> = proto.iter_rows(wb, |stream| {
                    if stream.feed(&server_bytes).is_err() {
                        return Err(bsql_pg_proto::ProtocolError::InternalCrateBug {
                            locus: bsql_pg_proto::CrateBugLocus::ReadCursorAdvance,
                        });
                    }
                    for _ in 0..32 {
                        match stream.collect_tuple::<(i32, &'static str)>() {
                            Ok(Some((id, name))) => {
                                rows_count = rows_count.saturating_add(1);
                                let _ = black_box((id, name));
                            }
                            Ok(None) => return Ok(()),
                            Err(c) => return Err(c),
                        }
                    }
                    Ok(())
                });
                let _ = black_box(rows_count);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------
// `with_cancel_request` closure-extract bench
//
// Measures the cost of `<ActivePhase>::with_cancel_request(...)` on
// a post-handshake protocol. The accessor path is:
//   1. Project `&self.inner.backend_key` — 0 cycles (struct field).
//   2. `as_inner() -> Option<&BackendKey>` — 1 branch on Option niche.
//   3. Pull `pid: i32` + `secret: i32` from the cell (2 i32 reads).
//   4. `cancel_request_bytes(pid, secret) -> [u8; 16]` — pure const
//      fn; LLVM inlines + writes the 16-byte array on the stack.
//   5. Move into `Zeroizing<[u8; 16]>` — NRVO writes directly into
//      the guard's inline storage (no copy).
//   6. Invoke closure with `&bytes`, return `Some(R)`.
//   7. Guard's Drop fires on scope exit — `zeroize::Zeroize` writes
//      16 zero bytes (cheap; cache-hot single store).
//
// Bundle-D' delta vs Bundle-D: the secret-scrub mechanism shifted
// from `Sensitive<i32>::ZeroizeOnDrop` on the credentials struct's
// 4-byte secret field to `Zeroizing<[u8; 16]>::ZeroizeOnDrop` on the
// 16-byte wire-frame array. The 16-byte zeroize is ~4x the work of
// a 4-byte zeroize but still <2 ns; the closure-scope tier
// elevation closes a retention-bypass class. Expected floor: ≤ 8 ns
// (per principal sign-off in spec). Setup
// (fresh_active_via_trust_handshake) is NOT timed —
// `iter_batched_ref` hoists it outside the measurement window.
// ---------------------------------------------------------------

fn bench_cancel_credentials_extract(c: &mut Criterion) {
    let mut group = c.benchmark_group("cancel_credentials_extract");
    group.throughput(Throughput::Elements(1));

    group.bench_function("active_some_arm", |b| {
        b.iter_batched_ref(
            fresh_active_via_trust_handshake,
            |active| {
                // The accessor takes &self — no consume. Each call
                // builds the wire frame on the stack, lends `&bytes`
                // + `pid` into the closure, scrubs on closure return.
                let r = active.with_cancel_request(|bytes, pid| {
                    // black_box both so LLVM cannot constant-fold
                    // the closure body away. Returning a
                    // `(*bytes, pid)` tuple measures the full lend +
                    // memcpy-out path the driver pattern uses.
                    black_box((*bytes, pid))
                });
                black_box(r);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_header,
    bench_ping_round_trip,
    bench_iter_rows_per_row_throughput,
    // Pull-API per-row throughput bench surface.
    bench_iter_10cols_large_5kb_row,
    bench_iter_jsonb_1mb_streaming,
    bench_col_next_per_event_cost,
    bench_push_ping,
    // Column decode measurement infra.
    bench_data_row_parse,
    bench_iter_columns_raw,
    bench_iter_columns_5x_int4_decode,
    // Caller-routed SWAR fast-path for 4-digit
    // unsigned integers; opt-in helper, decoupled from `from_pg_text`.
    bench_iter_columns_5x_int4_swar_short,
    bench_iter_columns_5x_text_decode,
    bench_iter_columns_with_nulls,
    // β SWAR extension: three additional opt-in helpers.
    bench_iter_columns_5x_int4_swar_long,
    bench_validate_utf8_swar,
    bench_parse_pg_bool_swar,
    // `prepared!` macro path.
    bench_prepared_query_push,
    bench_prepared_iter_rows_typed,
    // Paired bench for «-25 ns» claim
    // verification — runtime BindExecute path, same shape as the
    // prepared DML variant.
    bench_push_bind_execute_one_int_param,
    //
    // `with_cancel_request` closure-scoped path.
    bench_cancel_credentials_extract,
);
criterion_main!(benches);

extern crate alloc;
