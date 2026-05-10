//! DEF-143 — criterion bench harness for `bsql-pg-proto` hot paths.
//!
//! # Scope
//!
//! Targets the four hot paths identified in deferred.md §24 DEF-143:
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

// DEF-198 + DEF-212 — bench-side extension trait for the witness-guard
// typestate.
//
// Pre-DEF-198 benches called `proto.push_command(cmd, wb)` directly,
// returning OutActions. DEF-198 routed via
// `proto.as_ready().push_command(cmd, wb)` (Option<ReadyGuard>). DEF-212
// (Alt Y') changed the typed-push return from `OutActions<'w, 'p>` (800 B)
// to `Result<(), PushFailure>` (~80 B). The bench helper preserves both
// guard-acquisition + Result discipline so the bench timing reflects the
// production caller's cost.
//
// Benches always start from a fresh `PgProtocol::new()` (Idle state)
// — either via `iter_batched`-style setup-per-iter or before-loop
// hoisting. Post-DEF-211 FAKE-19 (audit 2026-05-04) the
// `reset_for_bench` shortcut was eliminated; criterion's
// `iter_batched` is the idiomatic pattern for stateful per-iter
// setup, so the `None` guard arm is unreachable in correctly-built
// benches — `panic!` surfaces a fixture bug as a loud bench failure
// rather than silent wrong-data.
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

impl BenchPushOrPanic for PgProtocol {
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
        // DEF-160 Z2 (2026-05-11): `push_command` returns `OutActions` to
        // surface borrowed-SQL chunks. The bench drops the iterator
        // immediately — production drains it via `writev` to the socket,
        // which the bench excludes (push path is the measurement target,
        // not the kernel `writev` syscall). Drop is alloc-neutral.
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

// DEF-270: ReplyId::from_raw is now pub(crate). Benches mint via
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
        b.iter(|| {
            let mut proto = PgProtocol::new();
            let mut wb = WriteBuf::new();
            // Push Ping — emits Sync frame bytes into write_buf.
            // DEF-270: mint reply via the public counter API.
            let reply = proto.next_reply_id::<PingKind>();
            let push_out = proto.bench_push_or_panic(
                bsql_pg_proto::push_command::Ping { reply },
                &mut wb,
            );
            let _ = black_box(push_out);
            // Feed RFQ — transitions PingAwaitingRfq → Idle + Pong.
            let feed_out = proto.feed_bytes(black_box(&rfq), &mut wb);
            black_box(feed_out);
        });
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
// [`PgProtocol::feed_inbound`] API (DEF-212 Phase 2 commit
// 201f86a) to pre-populate `read_buf` with N DataRow frames —
// raw append, no dispatch. Timed body loops `next_event()` N
// times, consuming all rows via fast-path.
//
// Throughput reports per-row amortised ns.
//
// # Why feed_inbound is the right setup primitive
//
// Public `feed_bytes` correctly rejects DataRow in
// `SimpleQueryStreamingRows` state — that's production
// behavior ("caller should use iter_rows, not feed_bytes"
// catch-all arm). Verified 2026-04-24: feeding 100 DataRows
// after RowDescription via `feed_bytes` lands in
// Errored(Framing), 0 rows pullable. `feed_inbound` is the
// dispatch-bypass primitive shipped with DEF-212 Phase 2 for
// 1c-5 pipelining forward-compat — appends bytes without
// triggering dispatch classification, exactly what bench setup
// needs. Pre-DEF-211 FAKE-19 (audit + ship 2026-05-04) the
// bench used a `bench_append_read_buf` hook that was a strict
// duplicate of `feed_inbound` — replaced.
//
// # Row size vs READ_BUF_CAP
//
// READ_BUF_CAP = 4096 B. Using 16-byte DataRow payload:
// 11 bytes (header + col metadata) + 16 (payload) = 27 B per
// row. RowDescription ~27 B. Budget: 4096 - 27 - safety =
// ~3900 B for rows / 27 = ~145 rows max. N_ROWS = 100 fits.

fn bench_iter_rows_per_row_throughput(c: &mut Criterion) {
    use bsql_pg_proto::row_stream::StreamItem;

    let mut group = c.benchmark_group("iter_rows_via_next_event");

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
                let mut proto = PgProtocol::new();
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
            // Timed: pull rows via iter_rows fast-path until
            // all 100 consumed or stream drains.
            |(mut proto, mut wb)| {
                let mut stream = proto.iter_rows(&mut wb);
                let mut rows_seen: u32 = 0;
                loop {
                    match stream.next_event() {
                        StreamItem::Row { .. } => {
                            rows_seen = rows_seen.saturating_add(1);
                        }
                        StreamItem::NeedMore
                        | StreamItem::CloseSocket
                        | StreamItem::Complete { .. } => break,
                        _other => break,
                    }
                }
                // Sanity: ensure the bench actually pulled N_ROWS,
                // not 0 (which would indicate setup failure).
                // assert! is permitted in bench harness (separate
                // crate target; forbid-bundle doesn't apply).
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
// DEF-190: per-row throughput via `next_row` API (compact Row
// struct, no StreamItem enum allocation per row).
// ---------------------------------------------------------------

fn bench_iter_rows_per_row_via_next_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_rows_via_next_row");
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

    group.bench_function("pull_100_rows_via_next_row", |b| {
        b.iter_batched(
            || {
                let mut proto = PgProtocol::new();
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
                    // feed_inbound returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.feed_inbound(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: feed_inbound must succeed for N_ROWS={N_ROWS}",
                    );
                }
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let mut stream = proto.iter_rows(&mut wb);
                let mut rows_seen: u32 = 0;
                while let Some(_row) = stream.next_row() {
                    rows_seen = rows_seen.saturating_add(1);
                }
                assert!(
                    rows_seen >= N_ROWS,
                    "next_row bench: expected {N_ROWS}, pulled {rows_seen}",
                );
                black_box(rows_seen);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------
// DEF-190: per-row throughput via `next_row_bytes` ULTRA-hot API.
// 24 B return (id, &[u8]); desc projected once before loop.
// ---------------------------------------------------------------

fn bench_iter_rows_per_row_via_next_row_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_rows_via_next_row_bytes");
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

    group.bench_function("pull_100_rows_via_next_row_bytes", |b| {
        b.iter_batched(
            || {
                let mut proto = PgProtocol::new();
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
                    // feed_inbound returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.feed_inbound(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: feed_inbound must succeed for N_ROWS={N_ROWS}",
                    );
                }
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let mut stream = proto.iter_rows(&mut wb);
                let mut rows_seen: u32 = 0;
                while let Some((_id, _bytes)) = stream.next_row_bytes() {
                    rows_seen = rows_seen.saturating_add(1);
                }
                assert!(
                    rows_seen >= N_ROWS,
                    "next_row_bytes bench: expected {N_ROWS}, pulled {rows_seen}",
                );
                black_box(rows_seen);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------
// DEF-191: per-row throughput via `consume_rows::<8>` batch API.
// Single cursor advance amortized across 8 rows; LLVM unrolls the
// validation loop. Stack array, zero alloc, zero copy.
// ---------------------------------------------------------------

fn bench_iter_rows_via_consume_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_rows_via_consume_batch");
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

    group.bench_function("pull_100_rows_via_consume_batch_8", |b| {
        b.iter_batched(
            || {
                let mut proto = PgProtocol::new();
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
                    // feed_inbound returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.feed_inbound(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: feed_inbound must succeed for N_ROWS={N_ROWS}",
                    );
                }
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let mut stream = proto.iter_rows(&mut wb);
                let mut rows_seen: u32 = 0;
                loop {
                    let batch: [Option<bsql_pg_proto::row_stream::Row<'_>>; 8] =
                        stream.consume_rows::<8>();
                    let mut yielded = 0u32;
                    for row in batch.iter() {
                        if row.is_some() {
                            yielded = yielded.saturating_add(1);
                        }
                    }
                    if yielded == 0 {
                        break;
                    }
                    rows_seen = rows_seen.saturating_add(yielded);
                }
                assert!(
                    rows_seen >= N_ROWS,
                    "consume_batch bench: expected {N_ROWS}, pulled {rows_seen}",
                );
                black_box(rows_seen);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------
// DEF-190: per-row throughput via `for_each_row` closure API.
// LLVM inlines the closure into the internal pull loop —
// eliminates per-row function-call boundary.
// ---------------------------------------------------------------

fn bench_iter_rows_per_row_via_for_each(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_rows_via_for_each");
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

    group.bench_function("pull_100_rows_via_for_each", |b| {
        b.iter_batched(
            || {
                let mut proto = PgProtocol::new();
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
                    // feed_inbound returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.feed_inbound(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: feed_inbound must succeed for N_ROWS={N_ROWS}",
                    );
                }
                (proto, wb)
            },
            |(mut proto, mut wb)| {
                let mut stream = proto.iter_rows(&mut wb);
                let mut rows_seen: u32 = 0;
                stream.for_each_row(|_row| {
                    rows_seen = rows_seen.saturating_add(1);
                });
                assert!(
                    rows_seen >= N_ROWS,
                    "for_each_row bench: expected {N_ROWS}, pulled {rows_seen}",
                );
                black_box(rows_seen);
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

    // Original: full-cycle push (allocate proto + wb, push, drop).
    // Includes zeroize-on-Drop cost from DEF-185 P0-B/P0-C
    // (WriteBuf zeroize 4 KB + ReadBuf zeroize 4 KB on Drop).
    // Production parallel: connection lifetime.
    group.bench_function("ping", |b| {
        b.iter(|| {
            let mut proto = PgProtocol::new();
            let mut wb = WriteBuf::new();
            // DEF-269 v2 (T): use the per-command `Ping` struct directly.
            // 16 B by-value vs 2176 B for the legacy `bsql_pg_proto::push_command::Ping`
            // (sized to Parse). This is the bench probe for T's claimed
            // -18..-22% gain.
            // DEF-270: mint via the public counter API.
            let reply = proto.next_reply_id::<PingKind>();
            let out = proto.bench_push_or_panic(
                bsql_pg_proto::push_command::Ping::new(reply),
                &mut wb,
            );
            let _ = black_box(out);
        });
    });

    // DEF-194 follow-up (2026-04-27): `ping_amortised` sub-bench —
    // DROPPED 2026-05-05 along with DEF-211 FAKE-19's bench-hooks
    // elimination. Pre-FAKE-19 the sub-bench used a `reset_for_bench`
    // hook to reuse the same `PgProtocol` across criterion iterations
    // (cache-warm, ~10 ns timing — production-relevant since real
    // connections reuse proto across thousands of queries).
    //
    // Post-FAKE-19 the only safe-Rust replacement (criterion's
    // `iter_batched_ref` with fresh proto per iter) reports a ~47 ns
    // floor: ~30 ns criterion batch-management overhead + ~17 ns
    // first-touch cache cost on each fresh proto. That number
    // **misleads readers** about the actual push cost — production
    // push remains ~10 ns (cache-warm).
    //
    // Decision: drop `ping_amortised` rather than ship a misleading
    // metric. Coverage gap closed by:
    //   - `ping_round_trip/push_then_feed` (~113 ns full cycle —
    //     push + feed_bytes(rfq) — fresh proto each iter, honest
    //     cold-path cycle measurement). Regression on push WILL
    //     show up here as cycle-time growth; signal preserved.
    //   - `push_command/ping` (above, fresh proto per iter) — same
    //     methodology, cold-cycle push-only number. Honest metric.
    //
    // If a future audit demands precise sub-ns per-push measurement
    // for regression hunt, options: (a) hand-rolled black_box loop
    // with cargo asm verification, (b) custom criterion harness,
    // (c) re-introduce a tier-1 reset mechanism (non-feature-gated
    // public API such as `proto.reset()` with proper scrub semantics
    // — would be a real production API, not bench-only).

    group.finish();
}

// ---------------------------------------------------------------
// DEF-197: per-column decode hot path.
// ---------------------------------------------------------------
//
// Closes the largest measurement blind spot in the crate: the
// per-column decode cost on row consumption. Pre-DEF-197 the bench
// suite measured frame parse, dispatch, RowStream emission, and
// `push_command` paths — but ZERO benches covered what happens
// AFTER `RowStream::next_row_bytes` hands raw row bytes to the
// caller: `DataRowRef::parse` (column-count header parse),
// `ColumnsIter` per-column length-prefix walk, and
// `FromPgText::from_pg_text` typed decoding.
//
// User context: real per-row latency on row-bearing responses is
// dominated by per-column work (parse + iterate + decode), not
// frame dispatch. DEF-197 lands the measurement infrastructure so
// subsequent perf claims on decoder optimisations (DEF-200 LUT
// dispatch, DEF-202 `simdutf8` text validation, DEF-203 niche
// tightening) are evidence-backed per CREDO §4.12.
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

/// DEF-251 (audit 2026-05-08): build a DataRow body with one column
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

    // DEF-251 (audit 2026-05-08): common-value cache hit measurement.
    // The 5 columns are populated with {0, 1, -1, 0, 1} — the three
    // literals the fast-path branches exist for. On hit, the digit
    // loop is bypassed entirely; the bench measures the byte-equality
    // match cost in isolation.
    //
    // Compare with `iter_5cols_decode_i32` (above, 8-digit literal
    // 42_000_000) for the cache-miss baseline. Hit-vs-miss delta
    // should be 1-2 ns/col post-DEF-251.
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

/// DEF-250 Phase B (2026-05-08): caller-routed SWAR fast-path
/// for short unsigned integers (ASCII-decimal, 0..=9999).
///
/// `parse_short_uint_swar` is exposed as an opt-in helper at the
/// `decode` module surface, NOT integrated into
/// `<i32 as FromPgText>::from_pg_text`. Two prior attempts to
/// embed the SWAR inside `from_pg_text` regressed adjacent benches
/// (Attempt 1: text +4-7%, 8-digit +5.2% from icache pressure;
/// Attempt 2: `iter_5cols_decode_i32_common_values` +31% from
/// `SimplifyCFG` merging dispatch with the DEF-251 common-value
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
/// (DEF-251 fast-path on `0`/`1`/`-1`). The SWAR helper is
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

/// Bench: per-column decode for 5 text columns of varying shapes.
///
/// Three shapes cover the realistic Postgres text-column distribution:
///
/// 1. **Short ASCII** (`alice@example.com`, 17 B) — typical OLTP
///    columns: usernames, emails, short identifiers.
/// 2. **Long ASCII** (~200 B) — descriptions, log lines, SQL queries.
///    SIMD UTF-8 validators (DEF-202) win most clearly here.
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
/// `col_len == -1` shortcut path in `ColumnsIter::next` (DEF-184
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

criterion_group!(
    benches,
    bench_parse_header,
    bench_ping_round_trip,
    bench_iter_rows_per_row_throughput,
    bench_iter_rows_per_row_via_next_row,
    bench_iter_rows_per_row_via_next_row_bytes,
    bench_iter_rows_via_consume_batch,
    bench_iter_rows_per_row_via_for_each,
    bench_push_ping,
    // DEF-197: column decode measurement infra.
    bench_data_row_parse,
    bench_iter_columns_raw,
    bench_iter_columns_5x_int4_decode,
    // DEF-250 Phase B: caller-routed SWAR fast-path for 4-digit
    // unsigned integers; opt-in helper, decoupled from `from_pg_text`.
    bench_iter_columns_5x_int4_swar_short,
    bench_iter_columns_5x_text_decode,
    bench_iter_columns_with_nulls,
);
criterion_main!(benches);

extern crate alloc;
