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
    command::PgCommand,
    frame::parse_header,
    ident::Sql,
    reply_id::{PingKind, QueryKind, ReplyId},
    PgProtocol, WriteBuf,
};
use core::num::NonZeroU64;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput,
};

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

fn reply_id_ping(raw: u64) -> ReplyId<PingKind> {
    ReplyId::from_raw(NonZeroU64::new(raw).unwrap_or(NonZeroU64::MIN))
}

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
            let push_out = proto.push_command(
                PgCommand::Ping {
                    reply: reply_id_ping(1),
                },
                &mut wb,
            );
            black_box(push_out);
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
// in a hot SELECT loop. Setup (not timed) uses the
// `PgProtocol::bench_append_read_buf` hook to pre-populate
// `read_buf` with N DataRow frames (raw append, bypasses
// dispatch). Timed body loops `next_event()` N times,
// consuming all rows via fast-path.
//
// Throughput reports per-row amortised ns.
//
// # Why the bench hook is necessary
//
// Public `feed_bytes` correctly rejects DataRow in
// `SimpleQueryStreamingRows` state — that's production
// behavior ("caller should use iter_rows, not feed_bytes"
// catch-all arm). Verified 2026-04-24: feeding 100 DataRows
// after RowDescription lands in Errored(Framing), 0 rows
// pullable. The `bench_append_read_buf` hook is a
// `#[doc(hidden)] pub fn` for bench-only use — appends bytes
// without triggering dispatch classification.
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
                let push_out = proto.push_command(
                    PgCommand::SimpleQuery {
                        sql: Sql::from_str_truncating("SELECT x"),
                        reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
                    },
                    &mut wb,
                );
                black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                // Raw-append DataRow bytes into read_buf.
                // bench_append_read_buf returns Result<(), ReadBufFull>;
                // assert on Ok so a setup misconfig (e.g., READ_BUF_CAP
                // shrunk below N_ROWS × row_size) fails loud rather than
                // producing silent garbage numbers. Setup is not timed.
                for _ in 0..N_ROWS {
                    let append_res = proto.bench_append_read_buf(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: bench_append_read_buf must succeed for N_ROWS={N_ROWS}",
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
                let push_out = proto.push_command(
                    PgCommand::SimpleQuery {
                        sql: Sql::from_str_truncating("SELECT x"),
                        reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
                    },
                    &mut wb,
                );
                black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                for _ in 0..N_ROWS {
                    // bench_append_read_buf returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.bench_append_read_buf(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: bench_append_read_buf must succeed for N_ROWS={N_ROWS}",
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
                let push_out = proto.push_command(
                    PgCommand::SimpleQuery {
                        sql: Sql::from_str_truncating("SELECT x"),
                        reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
                    },
                    &mut wb,
                );
                black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                for _ in 0..N_ROWS {
                    // bench_append_read_buf returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.bench_append_read_buf(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: bench_append_read_buf must succeed for N_ROWS={N_ROWS}",
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
                let push_out = proto.push_command(
                    PgCommand::SimpleQuery {
                        sql: Sql::from_str_truncating("SELECT x"),
                        reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
                    },
                    &mut wb,
                );
                black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                for _ in 0..N_ROWS {
                    // bench_append_read_buf returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.bench_append_read_buf(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: bench_append_read_buf must succeed for N_ROWS={N_ROWS}",
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
                let push_out = proto.push_command(
                    PgCommand::SimpleQuery {
                        sql: Sql::from_str_truncating("SELECT x"),
                        reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
                    },
                    &mut wb,
                );
                black_box(push_out);
                let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
                black_box(feed_out);
                for _ in 0..N_ROWS {
                    // bench_append_read_buf returns Result<(), ReadBufFull>.
                    // Silent discard would mask setup misconfiguration
                    // (e.g., READ_BUF_CAP shrunk below N_ROWS × row_size)
                    // — assert success so bench breakage is loud, not
                    // silent garbage numbers. Setup path is not timed.
                    let append_res = proto.bench_append_read_buf(&single_row);
                    assert!(
                        append_res.is_ok(),
                        "bench setup: bench_append_read_buf must succeed for N_ROWS={N_ROWS}",
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
            let out = proto.push_command(
                PgCommand::Ping {
                    reply: reply_id_ping(1),
                },
                &mut wb,
            );
            black_box(out);
        });
    });

    // DEF-194 follow-up (2026-04-27): amortised push_command —
    // measures the PUSH PATH ONLY, excluding PgProtocol::new() +
    // WriteBuf::new() construction and the matched Drop sequence
    // (which under DEF-185 P0-B/P0-C zeroize 8 KB of buffers on
    // every iter exit — ~10-20 ns of pure memset cost on the
    // measurement path that doesn't reflect production hot-path
    // economics, where PgProtocol lives a connection lifetime).
    //
    // Setup (not timed): create one PgProtocol + WriteBuf.
    // Timed: push_command + the matching state reset (write_buf
    // clear + protocol reset to Idle) so subsequent iters start
    // from a clean state. The reset is cheap (small clear + state
    // overwrite); the dominating cost is the push path itself.
    //
    // Expected: ~98-105 ns (close to DEF-189 baseline 98.6 ns
    // for push_command/ping post-DEF-189). Confirms whether
    // perceived "+10% regression vs def184-complete" is
    // (a) DEF-185 zeroize-on-Drop bench harness artefact, or
    // (b) real push-path regression.
    group.bench_function("ping_amortised", |b| {
        let mut proto = PgProtocol::new();
        let mut wb = WriteBuf::new();
        b.iter(|| {
            let out = proto.push_command(
                PgCommand::Ping {
                    reply: reply_id_ping(1),
                },
                &mut wb,
            );
            black_box(out);
            // Reset between iters so push_command sees a clean
            // Idle state. push_command on PingAwaitingRfq fails
            // with FailReply — would skew the measurement.
            // `reset_for_bench` is a `#[cfg(feature = "bench-hooks")]`
            // helper that drops the in-flight state without firing
            // FailReply (the bench is measuring the push path, not
            // the failure path).
            proto.reset_for_bench();
            wb.clear();
        });
    });

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

    group.finish();
}

/// Bench: per-column decode for 5 text columns. UTF-8 validation
/// dominates — the production bottleneck future `simdutf8` (DEF-202)
/// will target.
fn bench_iter_columns_5x_text_decode(c: &mut Criterion) {
    use bsql_pg_proto::decode::{DataRowRef, FromPgText};

    let mut group = c.benchmark_group("column_decode");
    const N_COLS: u16 = 5;
    group.throughput(Throughput::Elements(u64::from(N_COLS)));

    // Realistic-length text: typical name / short description.
    let body = data_row_body_text(N_COLS, "alice@example.com");

    group.bench_function("iter_5cols_decode_text", |b| {
        b.iter(|| {
            let row = match DataRowRef::parse(black_box(&body)) {
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
    bench_iter_columns_5x_text_decode,
    bench_iter_columns_with_nulls,
);
criterion_main!(benches);

extern crate alloc;
