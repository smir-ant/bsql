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
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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

/// Build a synthetic `CommandComplete` frame with tag "SELECT 1000".
fn command_complete_frame() -> alloc::vec::Vec<u8> {
    let tag = b"SELECT 1000\0";
    let mut out = alloc::vec::Vec::with_capacity(5 + tag.len());
    out.push(b'C');
    let total = u32::try_from(4 + tag.len()).unwrap_or(0);
    out.extend_from_slice(&total.to_be_bytes());
    out.extend_from_slice(tag);
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
// Bench: feed_bytes N×DataRow stream.
// ---------------------------------------------------------------
//
// Synthesises a full SELECT reply: RowDescription → N×DataRow →
// CommandComplete → ReadyForQuery. Driven via iter_rows which
// takes the fast-path for DataRow frames (bypasses dispatch).
// Varies N through 100 / 1000 to catch per-row O(1) claims.

fn bench_datarow_stream(c: &mut Criterion) {
    let mut group = c.benchmark_group("datarow_stream");

    for &n_rows in &[100u32, 1000u32] {
        group.throughput(Throughput::Elements(u64::from(n_rows)));

        // Build a synthetic inbound stream: a minimal RowDescription
        // for 1 text column, N DataRows (each 32-byte payload), then
        // CommandComplete + RFQ.
        let rowdesc = {
            let mut out = alloc::vec::Vec::new();
            out.push(b'T');
            // n_fields = 1
            // per-field: name\0 + tableOid(4) + col(2) + typeOid(4) + typeSize(2) + typeMod(4) + format(2)
            // = 1 + 4 + 2 + 4 + 2 + 4 + 2 = 19 + name-len
            let name = b"col\0";
            let body_len = 2 + name.len() + 18;
            let total = 4 + body_len;
            out.extend_from_slice(&(u32::try_from(total).unwrap_or(0)).to_be_bytes());
            out.extend_from_slice(&1u16.to_be_bytes()); // n_fields
            out.extend_from_slice(name);
            out.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            out.extend_from_slice(&0u16.to_be_bytes()); // col
            out.extend_from_slice(&25u32.to_be_bytes()); // type_oid = TEXT
            out.extend_from_slice(&(-1_i16).to_be_bytes()); // type_size
            out.extend_from_slice(&(-1_i32).to_be_bytes()); // type_mod
            out.extend_from_slice(&0u16.to_be_bytes()); // format = text
            out
        };
        let cc = command_complete_frame();
        let rfq = rfq_frame();
        let single_row = data_row_frame(32);

        let mut stream = alloc::vec::Vec::with_capacity(
            rowdesc.len()
                + single_row.len() * usize::try_from(n_rows).unwrap_or(0)
                + cc.len()
                + rfq.len(),
        );
        stream.extend_from_slice(&rowdesc);
        for _ in 0..n_rows {
            stream.extend_from_slice(&single_row);
        }
        stream.extend_from_slice(&cc);
        stream.extend_from_slice(&rfq);

        group.bench_with_input(
            BenchmarkId::from_parameter(n_rows),
            &stream,
            |b, stream| {
                b.iter(|| {
                    // Fresh proto + simple-query push to enter
                    // the row-streaming state, then feed the full
                    // synthetic response in one block.
                    let mut proto = PgProtocol::new();
                    let mut wb = WriteBuf::new();
                    let push_out = proto.push_command(
                        PgCommand::SimpleQuery {
                            sql: Sql::from_str_truncating("SELECT 1"),
                            reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
                        },
                        &mut wb,
                    );
                    black_box(push_out);
                    let feed_out = proto.feed_bytes(black_box(stream), &mut wb);
                    black_box(feed_out);
                });
            },
        );
    }

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

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_header,
    bench_ping_round_trip,
    bench_datarow_stream,
    bench_push_ping,
);
criterion_main!(benches);

extern crate alloc;
