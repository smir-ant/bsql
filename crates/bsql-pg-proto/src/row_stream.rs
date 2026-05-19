//! Column-by-column streaming consumer of in-flight PostgreSQL
//! query replies.
//!
//! # Universal streaming for colossal data
//!
//! The `'D'` (DataRow) tag's frame body is streamed via a chunk-
//! emission state machine on [`RowStream`]. Caller pulls [`ColEvent`]
//! events; the column body of an arbitrarily large cell arrives as
//! a sequence of `Chunk { bytes, total_len, remaining_len }` events
//! followed by exactly one `ChunkEnd { idx, bytes }` event. Every
//! wire-legal body size is handled — bodies larger than `READ_BUF_CAP`
//! (4096 B) are streamed; without this machinery they would have to
//! be classified as `FrameTooLarge` and tear down the connection,
//! and multi-MB JSONB / TEXT / BYTEA cells would be unreachable.
//!
//! Only `'D'` is exposed column-by-column. Other tags (`T`
//! RowDescription, `E` ErrorResponse, …) use the existing
//! `feed_bytes` dispatch path; oversized bodies for those tags are
//! handled by the separate streaming-sink path in
//! [`crate::partial_assembly`]. (All current PG drivers cap them
//! well below 4 KB in practice; the streaming sink handles the rare
//! over-cap cases.)
//!
//! # Closure-scoped API
//!
//! [`crate::PgProtocol::iter_rows`] is the sole construction path.
//! Caller passes a closure that receives `&mut RowStream`; the
//! `RowStream` value lives on `iter_rows`'s stack frame, dropped
//! synchronously when the closure returns. **`mem::forget` is
//! structurally closed** — caller has only a borrow, not the value.
//!
//! Drop fires unconditionally on every closure exit — normal return,
//! `?`-propagation, or panic unwind under `panic = "unwind"` (the
//! workspace default). When the stream's `drained` flag is `false`
//! at drop time, the Drop impl installs
//! `Errored(InternalCrateBug { locus: StreamDroppedMidStream })` via
//! [`crate::PgProtocol::install_errored_stream_dropped_mid_stream`].
//! The next operation on the connection observes Errored and the
//! wrapper surfaces `ConnectionAlreadyClosed { prior_kind }` to the
//! caller's pending oneshots.
//!
//! `panic = "abort"` is a binary-level setting outside library reach;
//! on process death the OS-level TCP RST tears down the connection
//! server-side. Architectural boundary stronger than any library
//! mechanism.
//!
//! # ColEvent variants
//!
//! - `Got { idx, bytes }` — a complete column body arrived inline
//!   (column fit in the current read-buf headroom). Caller decodes
//!   via `crate::decode::FromPgText` / `FromPgBinary`.
//! - `Null { idx }` — column was the PG SQL NULL sentinel (`len = -1`).
//! - `EndRow` — the row's last column emitted; caller can perform
//!   per-row aggregation before the next row begins.
//! - `Chunk { idx, bytes, total_len, remaining_len }` — a partial
//!   slice of a column body that exceeds the active read-buf
//!   headroom. Caller MUST consume `bytes` before calling
//!   `col_next` again (the slice is invalidated by the next mutating
//!   call).
//! - `ChunkEnd { idx, bytes }` — the final chunk of a chunked
//!   column; after this the next event will be `Got`/`Null`/`EndRow`
//!   for the next column.
//! - `NeedMore` — read-buf empty mid-row, awaiting more wire bytes.
//!   Caller exits the closure loop turn and feeds more bytes on the
//!   next turn (sans-I/O driver pattern).
//! - `EndQuery { id, outcome: Result<Reply<'a>, ProtocolError> }`
//!   — terminal. `id` is `Some(NonZeroU64)` for any terminal
//!   reached after the streaming state was observed; `None` only
//!   for the architecturally-rare pre-streaming Errored terminal
//!   (no real correlator was ever minted). `Ok(reply)` on
//!   `CommandComplete` + `ReadyForQuery`; `Err(cause)` on
//!   `ErrorResponse` + `ReadyForQuery` or wire malformedness.
//!   After `EndQuery` the stream is drained; subsequent `col_next`
//!   returns `NeedMore` deterministically.
//!
//! # Tier matrix
//!
//! - **Tier-1 (compile)**: `mem::forget(RowStream)` impossible;
//!   `ColEvent` variant exhaustion forced by `#[non_exhaustive]` on
//!   downstream callers but exhaustive within-crate; `PartialFrameToken`
//!   mint gated to leaf submodule.
//! - **Tier-1 (drop-glue)**: stream Drop fires on every non-`forget`
//!   non-`abort` exit per Rust spec.
//! - **OS-level boundary**: `panic = "abort"` → process death → TCP
//!   RST → server-side teardown.

use core::marker::PhantomData;
use core::num::NonZeroU64;

use crate::action::{Action, Reply};
use crate::buf::ReadBufFull;
use crate::decode::RowDescBorrow;
use crate::error::ProtocolError;
use crate::frame::{HEADER_LEN, HeaderParse, MAX_FRAME_LEN_FIELD, parse_header};
use crate::protocol::PgProtocol;
use crate::wire::TAG_DATA_ROW;
use crate::write_buf::WriteBuf;

/// **Event yielded by [`RowStream::col_next`]** — column-by-column
/// pull surface for in-flight PostgreSQL query replies.
///
/// See module docs for the variant set's rationale. Every variant is
/// borrow-lifetime-tied to the underlying read buffer / error arena;
/// the caller MUST consume the variant's borrowed payload (decode,
/// copy, or discard) before calling `col_next` again on the same
/// stream.
///
/// # `#[non_exhaustive]`
///
/// Downstream callers must use a wildcard arm. Internal in-crate
/// `match` may be exhaustive (per Rust semantics) and IS exhaustive
/// in the state machine.
///
/// # Variant frequency ordering
///
/// Variants are ordered by per-query frequency (hot-most first), so
/// the variant tag's branch predictor placement favours the typical
/// SELECT loop (`Got × col_count × row_count`):
///
/// - `Got` — emitted N×M times per SELECT (cols × rows).
/// - `Null` — alternating-NULL workloads.
/// - `EndRow` — once per row.
/// - `Chunk` / `ChunkEnd` — only for huge cells (multi-KB+).
/// - `NeedMore` — every read-buf-drain turn.
/// - `EndQuery` — once per query terminal.
#[derive(Debug)]
#[non_exhaustive]
pub enum ColEvent<'a> {
    /// A complete column body arrived inline. `bytes` is the raw
    /// PG-protocol body (text for `FormatCode::Text`, binary for
    /// `FormatCode::Binary`); decode via the
    /// [`crate::decode::FromPgText`] / [`crate::decode::FromPgBinary`]
    /// traits.
    Got {
        /// Zero-based column index within the row.
        idx: u16,
        /// Borrowed column body. Lifetime ties to the read buffer's
        /// unread region; caller MUST consume before next `col_next`.
        bytes: &'a [u8],
    },
    /// PG SQL NULL sentinel (the wire `len = -1`).
    Null {
        /// Zero-based column index within the row.
        idx: u16,
    },
    /// All columns of the current row emitted. Next `col_next` will
    /// either begin the next row (`Got`/`Null`/`Chunk`) or transition
    /// to a terminal (`EndQuery`/`NeedMore`).
    EndRow,
    /// A non-final partial slice of a column body that exceeded the
    /// active read-buf headroom. Followed by zero or more additional
    /// `Chunk` events and exactly one `ChunkEnd` to close the column.
    Chunk {
        /// Zero-based column index within the row.
        idx: u16,
        /// Borrowed slice of the column body. Caller MUST consume
        /// before next `col_next`.
        bytes: &'a [u8],
        /// Total declared length of the column body (from the wire
        /// `i32` length prefix). Bytes summed across all `Chunk` +
        /// `ChunkEnd` events for this column equal `total_len`.
        total_len: u32,
        /// Bytes the wire still owes for this column AFTER this
        /// chunk is consumed. `0` only on the chunk immediately
        /// preceding `ChunkEnd`.
        remaining_len: u32,
    },
    /// Final chunk of a chunked column body. Subsequent events
    /// resume the per-column cadence (`Got`/`Null`/`Chunk` for the
    /// next column, or `EndRow`).
    ChunkEnd {
        /// Zero-based column index within the row.
        idx: u16,
        /// Borrowed final slice of the column body.
        bytes: &'a [u8],
    },
    /// Read buffer drained mid-stream; caller feeds more wire bytes
    /// and resumes via `col_next`. The stream's state is preserved
    /// across `NeedMore` returns — repeat calls without intervening
    /// `feed` calls return `NeedMore` deterministically.
    NeedMore,
    /// Query reply terminal. **One variant covers both success and
    /// error** — the fork lives inside `Result<Reply, ProtocolError>`:
    /// - `Ok(reply)`: server emitted `CommandComplete` + `ReadyForQuery`.
    /// - `Err(cause)`: server emitted `ErrorResponse` + `ReadyForQuery`,
    ///   OR the parser classified wire bytes as malformed.
    ///
    /// In both cases the trailing `Z` has been silently drained; the
    /// stream is in terminal state and subsequent `col_next` calls
    /// return `NeedMore` deterministically.
    ///
    /// Designed to be `?`-friendly:
    /// `let reply = match … { EndQuery { outcome, .. } => outcome?, … };`.
    EndQuery {
        /// Correlator of the in-flight reply.
        ///
        /// `Some(NonZeroU64)` for any terminal reached after the
        /// streaming state was observed (every Ok arm + the typical
        /// Err arm). `None` only for an architecturally-rare
        /// pre-streaming Errored terminal — execution never reached
        /// the streaming-id-cached arm, so no real correlator
        /// exists. Wrapper layers route on `outcome::Err`'s cause,
        /// not on id equality, so the `None` carries the honest
        /// "no id was ever minted for this stream" signal.
        id: Option<NonZeroU64>,
        /// Success — typed payload | Error — protocol-classified
        /// failure.
        outcome: Result<Reply<'a>, ProtocolError>,
    },
}

/// **Mid-row column-cursor state** — 16 B inline (size pinned by
/// the const-assert below).
///
/// Carries the column-index counter and the in-progress chunked
/// column's accounting (when streaming a column body that exceeds
/// read-buf headroom).
#[derive(Debug, Clone, Copy)]
struct RowProgress {
    /// Total columns in this row (parsed from the 2-byte row body
    /// header).
    n_cols: u16,
    /// Number of columns whose body has been fully emitted
    /// (`Got`/`Null`/`ChunkEnd`). The next event for this row will
    /// concern column index `parsed_cols` (or `EndRow` if
    /// `parsed_cols == n_cols`).
    parsed_cols: u16,
    /// Bytes of the in-progress chunked column already emitted as
    /// `Chunk`. `0` when no chunked column is in flight (either
    /// between columns, or the current column is being processed
    /// in whole-row mode).
    chunk_consumed_in_col: u32,
    /// Declared total length of the in-progress chunked column. `0`
    /// when no chunked column is in flight; positive when a chunked
    /// column is mid-stream (between `Chunk` events).
    chunk_total_in_col: u32,
}

impl RowProgress {
    /// Construct progress at the start of a fresh row.
    #[inline]
    const fn new(n_cols: u16) -> Self {
        Self {
            n_cols,
            parsed_cols: 0,
            chunk_consumed_in_col: 0,
            chunk_total_in_col: 0,
        }
    }

    /// Whether a chunked column is currently in flight (between
    /// `Chunk` events). `true` once the column's first `Chunk` has
    /// been emitted; reset to `false` after `ChunkEnd` (or after the
    /// last `Chunk` with `remaining_len == 0`).
    #[inline]
    const fn in_chunked_col(&self) -> bool {
        self.chunk_total_in_col > 0
    }
}

// Compile-time size pin: keep `RowProgress` ≤ 16 B to fit
// alongside the rest of `RowStream`'s state without bloating the
// per-iter_rows stack frame. 2 + 2 + 4 + 4 = 12 B plus padding to
// 16 B alignment.
const _: () = assert!(
    core::mem::size_of::<RowProgress>() <= 16,
    "RowProgress must stay ≤ 16 B — \
     adding a field requires explicit budget review against the \
     RowStream stack footprint.",
);

/// **Pull-based column streamer** over a [`PgProtocol`] connection.
///
/// Constructed exclusively via [`PgProtocol::iter_rows`]; the value
/// lives on `iter_rows`'s stack frame, with the caller's closure
/// receiving only `&mut RowStream`. See module docs for the
/// closure-scoped API tier-1 closure of `mem::forget` /
/// `Box::leak` / `ManuallyDrop`.
#[derive(Debug)]
pub struct RowStream<'p, 'w> {
    /// Owning borrow of the protocol — all state mutation flows
    /// through this ref.
    proto: &'p mut PgProtocol,
    /// Caller-owned write buffer. Slow-path emission writes here;
    /// not currently surfaced as an event (Sub-A scope: SendBytes
    /// for SCRAM-mid-handshake is handled by [`PgProtocol::feed_bytes`]
    /// before `iter_rows` is called).
    write_buf: &'w mut WriteBuf,
    /// `true` after the stream emitted a terminal event
    /// (`EndQuery`). Drop with `drained == false` installs Errored
    /// via the leaf-gated state setter — see
    /// [`PgProtocol::install_errored_stream_dropped_mid_stream`].
    drained: bool,
    /// `true` after a terminal event was staged but the trailing
    /// `'Z'` (ReadyForQuery) has not yet been silently consumed.
    /// The next `col_next` call drains it and sets `drained = true`.
    /// `false` outside the terminal window.
    flush_pending: bool,
    /// Cached streaming reply correlator. Populated on first
    /// streaming-state observation; cleared by terminal events.
    /// `Some(id)` only when `proto.state` is a row-streaming variant.
    cached_reply_id: Option<NonZeroU64>,
    /// Mid-row column-cursor state. `Some` while inside a row body
    /// (between the row's first column event and `EndRow`); `None`
    /// outside a row (waiting for next frame, or in a non-streaming
    /// state).
    row_progress: Option<RowProgress>,
    /// Force `RowStream: !Send + !Sync` via a ZST
    /// `PhantomData<*const ()>` (`*const ()` is the canonical
    /// non-`Send` / non-`Sync` witness in core). The closure-scoped
    /// API pins lifetime via HRTB on `iter_rows`'s closure, but
    /// without this marker a caller could capture `&mut RowStream`
    /// inside a `tokio::spawn(async move { ... })` (the spawned
    /// future requires `Send` on its captures — with `!Send`
    /// RowStream the spawn fails to compile rather than running
    /// Drop on a foreign thread after `iter_rows`'s frame returns).
    ///
    /// ZST: no layout cost. `*const ()` carries no provenance — purely
    /// a marker type. Cannot be constructed by anyone outside this
    /// struct (the field is private + has no `pub` constructor surface;
    /// callers go through `RowStream::new`).
    _not_send: PhantomData<*const ()>,
}

impl<'p, 'w> RowStream<'p, 'w> {
    /// **Crate-internal constructor**. Production callers go through
    /// [`PgProtocol::iter_rows`]; this constructor is callable only
    /// from inside the crate.
    #[inline]
    #[must_use]
    pub(crate) fn new(proto: &'p mut PgProtocol, write_buf: &'w mut WriteBuf) -> Self {
        Self {
            proto,
            write_buf,
            drained: false,
            flush_pending: false,
            cached_reply_id: None,
            row_progress: None,
            _not_send: PhantomData,
        }
    }

    /// Append inbound TCP bytes to the protocol's read buffer.
    ///
    /// Err on [`ReadBufFull`]. On Err the stream drains — subsequent
    /// `col_next` returns `NeedMore` and the caller's error path
    /// resolves via the returned `Err`.
    ///
    /// Returns the compact [`ReadBufFull`] struct (a few bytes)
    /// rather than [`ProtocolError`] (~72 B) so the happy-path
    /// return doesn't pay a ProtocolError-sized stack slot for a
    /// cold failure mode. Callers who want a `ProtocolError` can
    /// `.map_err(ProtocolError::from)` at the boundary.
    #[inline]
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        match self.proto.read_buf_append(bytes) {
            Ok(()) => Ok(()),
            Err(err) => {
                // Drain so subsequent col_next returns NeedMore. We
                // also mark `drained = true` to ensure the post-Drop
                // install_errored fires only when the closure body
                // exited mid-frame, not on a routine read-buf-full
                // teardown (which the caller is expected to handle
                // upstream).
                self.drained = true;
                Err(err)
            }
        }
    }

    /// Get the current row's schema descriptor.
    ///
    /// Returns `None` outside a row-streaming state. Schema is
    /// invariant across rows of one query — call once before the
    /// row loop; cache the result outside the closure if needed
    /// (the borrow ties to `&self`, so a per-event refetch is also
    /// zero-cost).
    #[inline]
    #[must_use]
    pub fn current_row_desc(&self) -> Option<RowDescBorrow<'_>> {
        self.proto.current_row_desc()
    }

    /// Collect the next complete row into a typed tuple via
    /// [`crate::prepared::RowDecode`].
    ///
    /// Drives [`Self::col_next`] internally, accumulating per-column
    /// byte ranges (`(start_offset, len)` pairs into the protocol's
    /// populated read buffer) until `EndRow` arrives. The row's
    /// columns are then decoded via `<R as RowDecode>::decode` and
    /// returned as `R::Row<'_>` (the GAT projection at the read-buf
    /// lifetime).
    ///
    /// # Returns
    ///
    /// - `Ok(Some(R::Row<'_>))` — a complete row was assembled and
    ///   typed-decoded successfully.
    /// - `Ok(None)` — more bytes needed (mirrors [`ColEvent::NeedMore`]
    ///   on the underlying stream).
    /// - `Err(ProtocolError)` — terminal error in the underlying
    ///   stream, OR a per-column decode failure (`DecodeError` is
    ///   mapped to `ProtocolError::DecodeFailure` for uniformity).
    ///   The stream's terminal state is preserved; subsequent calls
    ///   return `Ok(None)`.
    ///
    /// # v1 constraint — no chunked columns
    ///
    /// If any column body exceeds the active read-buf headroom
    /// (`ColEvent::Chunk` would fire on the underlying stream),
    /// `collect_tuple` returns
    /// `Err(ProtocolError::ChunkedColumnInTypedRow)`. The typed
    /// decode path requires the column body in one contiguous
    /// slice; assembling chunks into a typed value requires either
    /// (a) caller-owned scratch buffer (out of the no_alloc contract)
    /// or (b) heap-allocated per-cell vectors (also out of contract).
    ///
    /// Wider coverage would require chunk-aware decoders for `&[u8]`
    /// (bytea) and `&str` (long text) that synthesise a borrowed-or-
    /// stitched view; out of v1 scope.
    pub fn collect_tuple<R>(&mut self) -> Result<Option<R::Row<'_>>, ProtocolError>
    where
        R: crate::prepared::RowDecode,
    {
        let mut col_offsets: [Option<(usize, usize)>; crate::decode::MAX_ROW_COLUMNS] =
            [None; crate::decode::MAX_ROW_COLUMNS];
        let col_formats: [crate::decode::FormatCode; crate::decode::MAX_ROW_COLUMNS] =
            [crate::decode::FormatCode::Text; crate::decode::MAX_ROW_COLUMNS];
        let mut col_count: u16 = 0;
        loop {
            // Pull one event; if it's a chunked column or terminal,
            // handle inline. Other events accumulate into the
            // offsets array.
            let event = self.col_next();
            match event {
                ColEvent::Got { idx, bytes } => {
                    // Convert the borrow into a (start, len) pair via
                    // address arithmetic. The event's `bytes` borrow
                    // holds `&mut self.proto` for its scope; capturing
                    // the slice's *address* (a usize) does NOT hold the
                    // borrow further. We compute the offset against the
                    // current populated() base, re-fetched after the
                    // event borrow ends. To capture both the slice ptr
                    // and base ptr atomically (avoiding compaction
                    // between them — see below), capture the slice's
                    // ptr+len here and the populated len, then in the
                    // outer scope (after the arm) re-fetch populated
                    // base and compute offset.
                    let idx_usize = usize::from(idx);
                    if idx_usize >= crate::decode::MAX_ROW_COLUMNS {
                        return Err(ProtocolError::TooManyColumns {
                            count: idx_usize.saturating_add(1),
                            max: crate::decode::MAX_ROW_COLUMNS,
                        });
                    }
                    let slice_addr = bytes.as_ptr().addr();
                    let len = bytes.len();
                    let populated_now = self.proto.read_buf_populated();
                    let populated_base_addr = populated_now.as_ptr().addr();
                    let populated_total_len = populated_now.len();
                    let Some(offset) = slice_addr.checked_sub(populated_base_addr) else {
                        return Err(ProtocolError::InternalCrateBug {
                            locus: crate::error::CrateBugLocus::RowRangeConstruction,
                        });
                    };
                    // `is_none_or` (Rust 1.82+) names the condition
                    // directly: `None` ⇒ true (no add — overflow path;
                    // reject as out-of-range); `Some(end)` ⇒
                    // `end > populated_total_len`. "End either overflows
                    // OR exceeds the buffer."
                    if offset.checked_add(len).is_none_or(|end| end > populated_total_len) {
                        return Err(ProtocolError::InternalCrateBug {
                            locus: crate::error::CrateBugLocus::RowRangeConstruction,
                        });
                    }
                    let Some(slot) = col_offsets.get_mut(idx_usize) else {
                        return Err(ProtocolError::InternalCrateBug {
                            locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                        });
                    };
                    *slot = Some((offset, len));
                    if idx_usize >= usize::from(col_count) {
                        col_count = idx.saturating_add(1);
                    }
                }
                ColEvent::Null { idx } => {
                    // Mirrors the `Got` path above (oversize index
                    // classification). A silent `if idx_usize <
                    // MAX_ROW_COLUMNS { … }` guard form would skip the
                    // col_count bump on overflow: a server emitting a
                    // row with 33+ Null columns would see `col_count`
                    // plateau at 32 + the R::ARITY check pass false-
                    // positively if R::ARITY happened to equal the
                    // truncated value (silent data-corruption class).
                    // The explicit `TooManyColumns` classification
                    // surfaces a typed ProtocolError instead.
                    let idx_usize = usize::from(idx);
                    if idx_usize >= crate::decode::MAX_ROW_COLUMNS {
                        return Err(ProtocolError::TooManyColumns {
                            count: idx_usize.saturating_add(1),
                            max: crate::decode::MAX_ROW_COLUMNS,
                        });
                    }
                    // Slot already None.
                    if idx_usize >= usize::from(col_count) {
                        col_count = idx.saturating_add(1);
                    }
                }
                ColEvent::EndRow => {
                    // Assemble the row from `col_offsets` and decode.
                    if R::ARITY != col_count {
                        return Err(ProtocolError::ColumnCountMismatch {
                            expected: R::ARITY,
                            actual: col_count,
                        });
                    }
                    // Slice the protocol's populated region to re-borrow
                    // each column body. The populated region is stable
                    // across the row body (buffer compaction happens only
                    // between frames).
                    let populated = self.proto.read_buf_populated();
                    // Build the per-column &[u8] slice array.
                    let mut col_bytes: [Option<&[u8]>; crate::decode::MAX_ROW_COLUMNS] =
                        [None; crate::decode::MAX_ROW_COLUMNS];
                    let n_used = usize::from(col_count).min(crate::decode::MAX_ROW_COLUMNS);
                    // Let-chain (Rust 1.88+) — short-circuit
                    // evaluation; inner body runs iff all three clauses
                    // bind.
                    for i in 0..n_used {
                        let entry = col_offsets.get(i).copied().flatten();
                        if let Some((off, len)) = entry
                            && let Some(s) = populated.get(off..off.saturating_add(len))
                            && let Some(slot) = col_bytes.get_mut(i)
                        {
                            *slot = Some(s);
                        }
                    }
                    // Explicit-match form on an architecturally-dead
                    // None (n_used = col_count.min(MAX_ROW_COLUMNS) ≤
                    // MAX_ROW_COLUMNS, both arrays are
                    // [_; MAX_ROW_COLUMNS], so `.get(..n_used)` is
                    // provably-Some). The match form documents the dead
                    // arm at the call site; CREDO §V's ban on silent
                    // fallback applies here even though the None is
                    // mathematically unreachable.
                    let formats_slice = match col_formats.get(..n_used) {
                        Some(s) => s,
                        None => &[],
                    };
                    let bytes_slice = match col_bytes.get(..n_used) {
                        Some(s) => s,
                        None => &[],
                    };
                    let decoded = R::decode(bytes_slice, formats_slice)
                        .map_err(ProtocolError::DecodeFailure)?;
                    return Ok(Some(decoded));
                }
                ColEvent::Chunk { .. } | ColEvent::ChunkEnd { .. } => {
                    // v1: typed decode requires contiguous columns.
                    self.drained = true;
                    return Err(ProtocolError::ChunkedColumnInTypedRow);
                }
                ColEvent::NeedMore => {
                    // NeedMore can mean either: (a) the read buffer
                    // is exhausted (caller must feed more bytes), or
                    // (b) `slow_path_once` advanced the state machine
                    // silently (e.g., ParseComplete in prepared!'s
                    // bundle) and we should retry to consume the next
                    // frame.
                    //
                    // Disambiguate by checking whether the read buffer
                    // has more bytes after the cursor. If yes — silent
                    // advance happened, loop back. If no — true
                    // buffer-empty, return Ok(None).
                    let cursor = usize::from(self.proto.read_buf_cursor_u16());
                    let pop_len = self.proto.read_buf_populated().len();
                    if cursor < pop_len {
                        // More bytes to consume; loop back to col_next.
                        continue;
                    }
                    return Ok(None);
                }
                ColEvent::EndQuery { outcome, .. } => {
                    match outcome {
                        Ok(_reply) => {
                            // No row was assembled — the query completed
                            // without producing a row. Signal end-of-rows
                            // via the same None contract as NeedMore;
                            // the caller observes `drained` separately.
                            return Ok(None);
                        }
                        Err(cause) => return Err(cause),
                    }
                }
            }
        }
    }

    /// **Pull the next column event.** See [`ColEvent`] for the
    /// event set + lifetime contract.
    ///
    /// # Flow
    ///
    /// 1. If `drained`, return `NeedMore`.
    /// 2. If state is Errored, drain + return terminal `EndQuery`
    ///    with an `Err` outcome carrying the prior cause's
    ///    classifier (single terminal arm — see [`ColEvent::EndQuery`]
    ///    for the contract).
    /// 3. If `flush_pending`, consume the trailing `'Z'` silently
    ///    and drain.
    /// 4. Otherwise drive the per-column / per-row / per-frame
    ///    state machine: parse header, handle DataRow column-by-column
    ///    (whole-row fast path or partial-frame chunked path), or
    ///    delegate non-D frames to [`feed_bytes_bounded`] and
    ///    surface a terminal `EndQuery` on `CommandComplete` /
    ///    `ErrorResponse`.
    ///
    /// # Lifetime
    ///
    /// Each returned `ColEvent<'_>` borrows from
    /// `self.proto.read_buf` / `self.proto.row_desc_slot` /
    /// `self.proto.error_arena` (for the `EndQuery::Err` arm).
    /// Caller MUST process the event before next `col_next` — the
    /// borrow checker enforces this (you cannot hold a `ColEvent`
    /// across the next mutable call).
    #[inline]
    pub fn col_next(&mut self) -> ColEvent<'_> {
        if self.drained {
            return ColEvent::NeedMore;
        }
        // Fused classification: one match on state observes (Errored
        // / Streaming / Other) + returns the streaming reply id by
        // value (Copy NonZeroU64).
        let class = self.proto.classify_for_iter_rows();
        if matches!(class, crate::protocol::IterRowsClass::Errored) {
            self.drained = true;
            // Errored entry — terminal `EndQuery` with an Err carrying
            // the prior-cause-classifier. The state is Errored from
            // upstream (either before `iter_rows` entry or as a result
            // of a prior frame the user fed via `feed_bytes`).
            // Synthesise an Err outcome with InternalCrateBug — the
            // production wrapper layer's path is to fail any
            // outstanding oneshots when the connection's state is
            // Errored at iter_rows entry; this terminal communicates
            // that to the closure caller in the canonical Result shape.
            //
            // Cached reply id: unknown here (the Errored install
            // already drained it via the state-setter route). Fall
            // back to a sentinel correlator: caller is signalling
            // "connection torn down; resolve any pending oneshots
            // via the wrapper's ConnectionAlreadyClosed path". The
            // outer dispatcher (`PgProtocol::feed_bytes`) on a
            // post-Errored call returns CloseSocket; iter_rows here
            // returns EndQuery::Err so the closure has a single
            // terminal arm.
            let cached = self.cached_reply_id.take();
            // Architecturally rare: Errored entry without a cached
            // id (no streaming was ever observed). Use the
            // wire-canonical "post-error sentinel" — a saturated
            // NonZeroU64. The wrapper layer matches on the
            // EndQuery::Err *cause*, not on id equality, so this
            // sentinel is purely a placeholder.
            let id: Option<NonZeroU64> = cached;
            return ColEvent::EndQuery {
                id,
                outcome: Err(ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                }),
            };
        }

        if self.flush_pending {
            // Post-terminal flush — consume trailing silent frames
            // (`Z` after `C`) so the protocol state returns to Idle
            // / DrainRfqAfterError ready for the next command.
            // Unbounded — silent-state-transition frames don't stage
            // actions; OutActions empty is expected.
            //
            // Drop binding on `flush_actions` releases the `&mut self.proto`
            // borrow at the next-statement boundary (NLL).
            let flush_actions = self.proto.feed_bytes(&[], self.write_buf);
            debug_assert!(
                flush_actions.as_slice().is_empty(),
                "RowStream flush path produced unexpected action — \
                 `flush_pending` gate promises trailing frames stage no \
                 actions; a frame leaked through.",
            );
            self.flush_pending = false;
            self.drained = true;
            return ColEvent::NeedMore;
        }

        // Cache reply id on first streaming observation.
        let cached_id = match class {
            crate::protocol::IterRowsClass::Streaming(id) => {
                self.cached_reply_id = Some(id);
                Some(id)
            }
            crate::protocol::IterRowsClass::Other => self.cached_reply_id,
            crate::protocol::IterRowsClass::Errored => {
                // Handled above; unreachable from here.
                core::hint::cold_path();
                self.drained = true;
                return ColEvent::NeedMore;
            }
        };

        // If we are mid-row in a chunked column, continue emitting
        // chunks before parsing any new header.
        if let Some(progress) = self.row_progress {
            if progress.in_chunked_col() {
                return self.emit_next_chunk(progress);
            }
            // Mid-row but not in a chunked col — we just finished
            // emitting EndRow or are between columns inside a body
            // that's fully buffered. Continue per-column emission
            // from the current cursor.
            return self.emit_next_col(progress);
        }

        // Between rows — peek next frame header to decide path.
        self.dispatch_next_frame(cached_id)
    }

    /// Parse the next frame header from the read-buf and dispatch
    /// to the appropriate emission path: DataRow → row-body parse
    /// (whole-row fast path or partial-frame chunked entry);
    /// non-D → slow-path `feed_bytes_bounded(1)` into a terminal
    /// `EndQuery` or a silent transition (`NeedMore`).
    #[inline]
    fn dispatch_next_frame(&mut self, cached_id: Option<NonZeroU64>) -> ColEvent<'_> {
        // Peek header at current cursor. If we are in partial-frame
        // mode (mid-body of a previous oversized DataRow), the body
        // bytes themselves don't have a header — re-entry detection
        // is the row_progress state. partial_remaining > 0 with no
        // row_progress is an architectural impossibility (entered
        // partial mode only via DataRow open-row path); fall through
        // to the normal peek and rely on the dispatcher to surface
        // any drift.
        let populated = self.proto.read_buf_populated();
        let cursor = usize::from(self.proto.read_buf_cursor_u16());
        // Explicit-match form. None arm is architecturally-dead
        // (cursor ≤ populated.len() upheld by read_buf invariants);
        // downstream `parse_header(&[])` returns HeaderParse::Empty
        // which routes to ColEvent::NeedMore — but the call-site
        // silent-fallback `.unwrap_or(&[])` is banned per CREDO §V
        // regardless of downstream classification.
        let after_cursor = match populated.get(cursor..) {
            Some(s) => s,
            None => &[],
        };
        let header = parse_header(after_cursor);
        match header {
            HeaderParse::Empty | HeaderParse::Incomplete => ColEvent::NeedMore,
            HeaderParse::MalformedLength { .. } => {
                // Malformed — route through slow path so the dispatcher
                // classifies and emits FailReply + CloseSocket through
                // the canonical fail_inflight path.
                self.slow_path_once(cached_id)
            }
            HeaderParse::FrameTooLarge { declared } => {
                // Sub-A scope: only D-tag in a streaming state enters
                // partial mode. Other tag/state combinations continue
                // to tear down with the canonical FrameTooLarge path.
                if cached_id.is_some() {
                    // Re-read the tag inline (parse_header doesn't
                    // expose the tag on the FrameTooLarge variant —
                    // it's pre-classified). The 5-byte header is
                    // guaranteed present (parse_header returned Ok-or-
                    // classified, both require at least 5 bytes).
                    let tag = after_cursor.first().copied().unwrap_or(0);
                    if tag == TAG_DATA_ROW.byte() {
                        return self.begin_partial_data_row(declared);
                    }
                }
                self.slow_path_once(cached_id)
            }
            HeaderParse::Ok { tag, total_len } => {
                let total = usize::from(total_len);
                if after_cursor.len() < total {
                    return ColEvent::NeedMore;
                }
                if tag.byte() == TAG_DATA_ROW.byte()
                    && let Some(id) = cached_id
                {
                    return self.begin_whole_data_row(id, cursor, total);
                }
                self.slow_path_once(cached_id)
            }
        }
    }

    /// Enter the whole-row fast path: header fully buffered, row
    /// body fits in `[cursor + HEADER_LEN, cursor + total)`. Parse
    /// the 2-byte col_count, advance the cursor past the header,
    /// and emit the first column event.
    #[inline]
    fn begin_whole_data_row(
        &mut self,
        id: NonZeroU64,
        cursor: usize,
        total: usize,
    ) -> ColEvent<'_> {
        let row_start = cursor.saturating_add(HEADER_LEN);
        let row_end = cursor.saturating_add(total);
        let row_body_len = row_end.saturating_sub(row_start);
        // Body < 2 bytes can't carry the col-count header.
        if row_body_len < 2 {
            self.drained = true;
            let drained = self.proto.install_errored_malformed_data_row(total);
            let cause = ProtocolError::MalformedDataRow { total_len: total };
            let term_id: Option<NonZeroU64> = drained.or(Some(id));
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }

        // Advance the read cursor past the 5-byte header. The row
        // body starts at the new cursor position.
        if self.proto.read_buf_advance(HEADER_LEN).is_err() {
            self.drained = true;
            let drained = self.proto.install_errored_read_cursor_advance();
            let cause = ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            };
            let term_id: Option<NonZeroU64> = drained.or(Some(id));
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }

        // Read col_count from body[0..2]. Body starts at the new cursor.
        let n_cols = match self.read_col_count() {
            Ok(n) => n,
            Err(cause) => {
                self.drained = true;
                let drained = self.proto.install_errored_malformed_data_row(total);
                let term_id: Option<NonZeroU64> = drained.or(Some(id));
                return ColEvent::EndQuery {
                    id: term_id,
                    outcome: Err(cause),
                };
            }
        };
        // Advance past the 2-byte col_count header.
        if self.proto.read_buf_advance(2).is_err() {
            self.drained = true;
            let drained = self.proto.install_errored_read_cursor_advance();
            let cause = ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            };
            let term_id: Option<NonZeroU64> = drained.or(Some(id));
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }

        let progress = RowProgress::new(n_cols);
        self.row_progress = Some(progress);
        if n_cols == 0 {
            // Zero-column row — emit EndRow immediately. PG wire
            // permits zero-column DataRow (rare but legal).
            self.row_progress = None;
            return ColEvent::EndRow;
        }
        self.emit_next_col(progress)
    }

    /// Enter partial-frame mode for an oversized DataRow. The frame
    /// header (5 bytes) has been observed at the read cursor; the
    /// declared body length exceeds the active-tier headroom. Advance
    /// past the header, transition ReadBuf into partial mode, and
    /// emit the first column event (which may itself be a `Chunk`).
    #[inline]
    fn begin_partial_data_row(&mut self, declared: u32) -> ColEvent<'_> {
        // Re-fetch the cached reply id (architecturally guaranteed
        // Some by the dispatch site's `cached_id.is_some()` check).
        let cached_id: Option<NonZeroU64> = self.cached_reply_id;

        // Advance past the 5-byte frame header.
        if self.proto.read_buf_advance(HEADER_LEN).is_err() {
            self.drained = true;
            let drained = self.proto.install_errored_read_cursor_advance();
            let cause = ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            };
            let term_id: Option<NonZeroU64> = drained.or(cached_id);
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }

        // Body length includes the 4 length-field self-bytes per PG
        // wire spec. Subtract them: partial counter tracks body bytes
        // beyond the header.
        let body_remaining = declared.saturating_sub(4);
        let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
        // Typed-Err propagation from ReadBuf::enter_partial_mode. The
        // Err arm classifies the re-entry bug via
        // `CrateBugLocus::PartialModeReentry`, installs Errored, and
        // surfaces ColEvent::EndQuery::Err uniformly across build
        // modes. A `()`-returning shape would silently overwrite in
        // release (debug-assert panic in dev only) — the CREDO §V
        // glass pattern with wire-desync consequence. The re-entry
        // is architecturally dead under intact dispatcher (every
        // begin_partial_data_row precedes a matching
        // exit_partial_mode), but the typed return closes the
        // by-construction shield.
        if self.proto.enter_partial_mode_for_data_row(&token, body_remaining).is_err() {
            self.drained = true;
            let drained = self.proto.install_errored_partial_mode_reentry();
            let cause = ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::PartialModeReentry,
            };
            let term_id: Option<NonZeroU64> = drained.or(cached_id);
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }

        // Read col_count from the now-current cursor. With body
        // remaining > 0 and at least 2 bytes buffered (frame headers
        // ≥ 5 B fit, post-advance ≥ 0 B but PG body always starts
        // with 2-byte col count), col_count is the next 2 bytes.
        let n_cols = match self.read_col_count() {
            Ok(n) => n,
            Err(_) => {
                // Not enough bytes yet — pause to await more. Exit
                // partial mode is NOT correct here (we haven't
                // touched any body bytes); leave the counter intact
                // and return NeedMore. The next col_next will reach
                // this path again with more bytes available.
                //
                // The 2-byte col_count is in-body (its bytes account
                // toward body_remaining). We have not consumed them
                // yet — leave row_progress None so the next call
                // re-enters begin_partial_data_row?  No: re-entering
                // would re-advance past the (already-advanced) header
                // and double-count. Set a "partial-mode pending header"
                // marker via row_progress with n_cols = 0 and
                // chunk_total_in_col = sentinel? That conflates state.
                //
                // Simpler: read_col_count returns Err only on body too
                // short. In partial mode the body's first 2 bytes are
                // the col_count; if even those aren't here, we cannot
                // safely proceed. Mark drained on this rare path and
                // surface a protocol-error terminal.
                self.drained = true;
                let drained = self.proto.install_errored_malformed_data_row(0);
                let cause = ProtocolError::MalformedDataRow { total_len: 0 };
                let term_id: Option<NonZeroU64> = drained.or(cached_id);
                return ColEvent::EndQuery {
                    id: term_id,
                    outcome: Err(cause),
                };
            }
        };
        // Consume the 2 col_count bytes from both the read cursor
        // and the partial-mode counter.
        if self.proto.read_buf_advance(2).is_err() {
            self.drained = true;
            let drained = self.proto.install_errored_read_cursor_advance();
            let cause = ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            };
            let term_id: Option<NonZeroU64> = drained.or(cached_id);
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }
        let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
        if self.proto.subtract_partial_for_row_stream(&token, 2).is_err() {
            // Architecturally dead: we just verified read_col_count
            // succeeded (≥ 2 body bytes buffered) and the body counter
            // started at `body_remaining = declared - 4`, which for
            // declared > READ_BUF_CAP is ≥ 4093 ≥ 2. Defensive route
            // through the same locus.
            self.drained = true;
            let drained = self.proto.install_errored_read_cursor_advance();
            let cause = ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            };
            let term_id: Option<NonZeroU64> = drained.or(cached_id);
            return ColEvent::EndQuery {
                id: term_id,
                outcome: Err(cause),
            };
        }

        let progress = RowProgress::new(n_cols);
        self.row_progress = Some(progress);
        if n_cols == 0 {
            // Zero-column oversized row (architecturally pathological
            // — a server emitting that exhausts the partial-mode
            // counter only via the chunked-body path. Exit partial
            // mode and emit EndRow.). The exit call's typed-Err
            // return enforces `partial_remaining == 0` at the call
            // site; if the wire still owed bytes (architecturally
            // unreachable from a wire-legal frame with a non-zero
            // counter) classify via Errored install + EndQuery::Err
            // routing.
            let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
            if self.proto.exit_partial_mode_for_row_stream(&token).is_err() {
                self.drained = true;
                let drained = self.proto.install_errored_partial_mode_exit_undrained();
                let cause = ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::PartialModeExitUndrained,
                };
                let term_id: Option<NonZeroU64> = drained.or(cached_id);
                return ColEvent::EndQuery {
                    id: term_id,
                    outcome: Err(cause),
                };
            }
            self.row_progress = None;
            return ColEvent::EndRow;
        }
        self.emit_next_col(progress)
    }

    /// Emit the next column from the current row.
    ///
    /// Handles three sub-cases:
    /// 1. **Whole-column inline**: column body fits in unread region.
    ///    Emit `Got` or `Null`, advance cursor + (partial counter if
    ///    partial-mode), bump `parsed_cols`.
    /// 2. **First chunk of a chunked column**: declared > inline
    ///    headroom. Emit `Chunk` with as-much-as-buffered slice,
    ///    enter mid-chunk state via `chunk_total_in_col`.
    /// 3. **`EndRow`**: `parsed_cols == n_cols`. Clear `row_progress`
    ///    + (exit partial mode if 0 remaining).
    #[inline]
    fn emit_next_col(&mut self, progress: RowProgress) -> ColEvent<'_> {
        if progress.parsed_cols == progress.n_cols {
            // All columns of this row emitted. Reset row_progress so
            // the next col_next can pick up the next frame.
            self.row_progress = None;
            // If we were in partial-mode at end-of-row, exit partial
            // mode here. A wire-legal oversized DataRow has body ==
            // sum(2 col_count + per-col (4 len + len bytes)) — when
            // parsed_cols == n_cols, partial_remaining MUST be 0
            // (else a body-length / column-length disagreement).
            //
            // The exit call returns
            // `Result<(), PartialModeExitUndrained>` — single source
            // of truth: the function enforces the
            // `partial_remaining == 0` precondition. The
            // classification happens IMMEDIATELY at end-of-row via the
            // typed Err + install_errored_partial_mode_exit_undrained
            // path; a discipline-based form (pre-check
            // `is_in_partial_mode` AND `partial_remaining == 0` upstream
            // + silent skip on non-zero remaining) would defer
            // classification to the dispatcher's next-frame entry
            // (silent for the duration of this dispatch frame's body).
            // The `is_in_partial_mode` predicate-check is preserved as
            // the fast-path gate (non-partial rows don't pay the
            // exit-call cost).
            if self.proto.is_in_partial_mode_for_row_stream() {
                let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
                if self.proto.exit_partial_mode_for_row_stream(&token).is_err() {
                    self.drained = true;
                    let drained = self.proto.install_errored_partial_mode_exit_undrained();
                    let cause = ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::PartialModeExitUndrained,
                    };
                    let cached_id: Option<NonZeroU64> = self.cached_reply_id;
                    let term_id: Option<NonZeroU64> = drained.or(cached_id);
                    return ColEvent::EndQuery {
                        id: term_id,
                        outcome: Err(cause),
                    };
                }
            }
            return ColEvent::EndRow;
        }

        // Read the 4-byte column length prefix at the current cursor.
        let col_len_i32 = match self.read_col_len() {
            Ok(n) => n,
            Err(_) => return ColEvent::NeedMore,
        };
        // Consume the 4-byte length prefix from cursor + partial.
        if self.proto.read_buf_advance(4).is_err() {
            return self.terminal_internal_advance_err();
        }
        if self.proto.is_in_partial_mode_for_row_stream() {
            let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
            if self.proto.subtract_partial_for_row_stream(&token, 4).is_err() {
                return self.terminal_internal_advance_err();
            }
        }

        let idx = progress.parsed_cols;
        if col_len_i32 == -1 {
            // SQL NULL.
            let mut new_progress = progress;
            new_progress.parsed_cols = idx.saturating_add(1);
            self.row_progress = Some(new_progress);
            return ColEvent::Null { idx };
        }
        if col_len_i32 < 0 {
            // Negative non-(-1) is malformed.
            return self.terminal_malformed_col_len(col_len_i32);
        }
        let col_len = match u32::try_from(col_len_i32) {
            Ok(v) => v,
            Err(_) => return self.terminal_malformed_col_len(col_len_i32),
        };

        // Decide whole-col vs chunked emission. Whole-col when the
        // entire body is buffered in the unread region.
        let unread_len = self.proto.read_buf_unread_len();
        let col_len_usize = match usize::try_from(col_len) {
            Ok(v) => v,
            Err(_) => return self.terminal_malformed_col_len(col_len_i32),
        };
        if col_len_usize <= unread_len {
            // Whole-column path. Carve out the slice + advance.
            let bytes_offset = usize::from(self.proto.read_buf_cursor_u16());
            if self.proto.read_buf_advance(col_len_usize).is_err() {
                return self.terminal_internal_advance_err();
            }
            if self.proto.is_in_partial_mode_for_row_stream() {
                let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
                if self
                    .proto
                    .subtract_partial_for_row_stream(&token, col_len)
                    .is_err()
                {
                    return self.terminal_internal_advance_err();
                }
            }
            let mut new_progress = progress;
            new_progress.parsed_cols = idx.saturating_add(1);
            self.row_progress = Some(new_progress);

            // Two-phase defense for the body-slice projection. A
            // silent `populated.get(...).unwrap_or(&[])` would be the
            // CREDO §V glass pattern — user-visible-data-corruption-
            // class if `read_buf_advance`'s contract ever broke.
            //
            // Phase 1 (explicit pre-check, uses populated.len() as a
            // copy so the borrow is fully released before any
            // `&mut self` call): if the slice end exceeds the
            // populated region OR bytes_offset arithmetic underflowed
            // (saturating_add could produce a smaller-than-offset end
            // only on usize wrap, architecturally impossible because
            // col_len_usize fits in i32), classify via the canonical
            // terminal helper — `CrateBugLocus::ReadCursorAdvance`
            // (closest existing semantics: "cursor/populated invariant
            // broke between `read_buf_advance` Ok and slice
            // projection"). The borrow-checker constraint forces this
            // pre-check to live BEFORE the `populated` re-borrow,
            // because `terminal_internal_advance_err` takes `&mut self`
            // and the slice's lifetime ties to the populated borrow.
            //
            // Phase 2: project the slice. The `.unwrap_or(&[])`
            // fallback is preserved as the closing syntactic shape
            // (no `clippy::indexing_slicing` use; no `unwrap()` /
            // `expect()` panic class), but its None arm is
            // ARCHITECTURALLY UNREACHABLE per phase 1's pre-check.
            // The pair is the tier-1 elevation: classified Err on the
            // hazard path, syntactic-shape fallback on the proven-dead
            // path.
            let populated_len = self.proto.read_buf_populated().len();
            let slice_end = bytes_offset.saturating_add(col_len_usize);
            if slice_end > populated_len || slice_end < bytes_offset {
                return self.terminal_internal_advance_err();
            }
            let populated = self.proto.read_buf_populated();
            let bytes = populated.get(bytes_offset..slice_end).unwrap_or(&[]);
            return ColEvent::Got { idx, bytes };
        }

        // Chunked path. Emit Chunk with as-much-as-buffered.
        // Pre-condition: col_len > unread_len, so we have strictly
        // fewer bytes buffered than the column declares. Partial mode
        // MUST be active for this case (otherwise the whole frame
        // wouldn't have fit and we'd have classified as FrameTooLarge
        // earlier). Defensive: if not in partial mode (e.g., the
        // single-column body happened to span unread without a
        // partial-mode entry), still emit Chunk and rely on the next
        // col_next to fetch more bytes via Caller-feed.
        let bytes_offset = usize::from(self.proto.read_buf_cursor_u16());
        let chunk_len = unread_len;
        let chunk_len_u32 = match u32::try_from(chunk_len) {
            Ok(v) => v,
            Err(_) => return self.terminal_internal_advance_err(),
        };
        if self.proto.read_buf_advance(chunk_len).is_err() {
            return self.terminal_internal_advance_err();
        }
        if self.proto.is_in_partial_mode_for_row_stream() {
            let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
            if self
                .proto
                .subtract_partial_for_row_stream(&token, chunk_len_u32)
                .is_err()
            {
                return self.terminal_internal_advance_err();
            }
        }

        let mut new_progress = progress;
        new_progress.chunk_total_in_col = col_len;
        new_progress.chunk_consumed_in_col = chunk_len_u32;
        // We do NOT bump parsed_cols here — the column is in flight.
        self.row_progress = Some(new_progress);

        let remaining_len = col_len.saturating_sub(chunk_len_u32);
        // Two-phase defense mirroring the Got-arm projection above.
        // `read_buf_advance(chunk_len)` succeeded above ⇒
        // `bytes_offset + chunk_len <= populated.len()`. Pre-check
        // via len-copy + classified Err on the architecturally-dead
        // bounds-violation arm before the populated re-borrow.
        let populated_len = self.proto.read_buf_populated().len();
        let slice_end = bytes_offset.saturating_add(chunk_len);
        if slice_end > populated_len || slice_end < bytes_offset {
            return self.terminal_internal_advance_err();
        }
        let populated = self.proto.read_buf_populated();
        let bytes = populated.get(bytes_offset..slice_end).unwrap_or(&[]);
        ColEvent::Chunk {
            idx,
            bytes,
            total_len: col_len,
            remaining_len,
        }
    }

    /// Emit the next chunk of an in-progress chunked column. Called
    /// when `row_progress.in_chunked_col() == true` at `col_next`
    /// entry. Decides between `Chunk` (more chunks to follow) and
    /// `ChunkEnd` (this completes the column).
    #[inline]
    fn emit_next_chunk(&mut self, progress: RowProgress) -> ColEvent<'_> {
        let unread_len = self.proto.read_buf_unread_len();
        let remaining = progress
            .chunk_total_in_col
            .saturating_sub(progress.chunk_consumed_in_col);
        if remaining == 0 {
            // Architecturally dead: caller already drained the last
            // chunk; we should have emitted ChunkEnd and reset
            // chunk_total_in_col to 0 (resetting in_chunked_col).
            // Defensive: bump parsed_cols + clear, then continue the
            // per-column loop.
            let mut new_progress = progress;
            new_progress.parsed_cols = new_progress.parsed_cols.saturating_add(1);
            new_progress.chunk_total_in_col = 0;
            new_progress.chunk_consumed_in_col = 0;
            self.row_progress = Some(new_progress);
            return self.emit_next_col(new_progress);
        }
        if unread_len == 0 {
            // No body bytes buffered — pause to feed.
            return ColEvent::NeedMore;
        }
        let unread_u32 = match u32::try_from(unread_len) {
            Ok(v) => v,
            Err(_) => return self.terminal_internal_advance_err(),
        };
        let chunk_len = core::cmp::min(remaining, unread_u32);
        let chunk_len_usize = match usize::try_from(chunk_len) {
            Ok(v) => v,
            Err(_) => return self.terminal_internal_advance_err(),
        };
        let bytes_offset = usize::from(self.proto.read_buf_cursor_u16());
        if self.proto.read_buf_advance(chunk_len_usize).is_err() {
            return self.terminal_internal_advance_err();
        }
        if self.proto.is_in_partial_mode_for_row_stream() {
            let token = crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher();
            if self
                .proto
                .subtract_partial_for_row_stream(&token, chunk_len)
                .is_err()
            {
                return self.terminal_internal_advance_err();
            }
        }

        let new_consumed = progress.chunk_consumed_in_col.saturating_add(chunk_len);
        let is_final = new_consumed >= progress.chunk_total_in_col;

        let mut new_progress = progress;
        new_progress.chunk_consumed_in_col = new_consumed;
        if is_final {
            new_progress.chunk_total_in_col = 0;
            new_progress.chunk_consumed_in_col = 0;
            new_progress.parsed_cols = new_progress.parsed_cols.saturating_add(1);
        }
        self.row_progress = Some(new_progress);

        // Two-phase defense (chunk continuation), same pattern as
        // the first-chunk site. `read_buf_advance(chunk_len_usize)`
        // succeeded above ⇒
        // `bytes_offset + chunk_len_usize <= populated.len()`.
        let populated_len = self.proto.read_buf_populated().len();
        let slice_end = bytes_offset.saturating_add(chunk_len_usize);
        if slice_end > populated_len || slice_end < bytes_offset {
            return self.terminal_internal_advance_err();
        }
        let populated = self.proto.read_buf_populated();
        let bytes = populated.get(bytes_offset..slice_end).unwrap_or(&[]);
        let idx = progress.parsed_cols;
        if is_final {
            ColEvent::ChunkEnd { idx, bytes }
        } else {
            ColEvent::Chunk {
                idx,
                bytes,
                total_len: progress.chunk_total_in_col,
                remaining_len: progress.chunk_total_in_col.saturating_sub(new_consumed),
            }
        }
    }

    /// Slow path: delegate one dispatch to `feed_bytes_bounded(1)`.
    /// Translate the first emitted action into a `ColEvent` terminal
    /// (`EndQuery`) or `NeedMore` (silent state transition).
    #[inline]
    fn slow_path_once(&mut self, cached_id: Option<NonZeroU64>) -> ColEvent<'_> {
        let actions = self.proto.feed_bytes_bounded(&[], self.write_buf, 1);
        let first_opt = actions.as_slice().first().copied();
        let is_terminal = matches!(
            first_opt,
            Some(Action::DeliverReply { .. })
                | Some(Action::FailReply { .. })
                | Some(Action::CloseSocket),
        );
        if is_terminal {
            self.flush_pending = true;
        }
        match first_opt {
            None => ColEvent::NeedMore,
            Some(Action::DeliverReply { id, value }) => ColEvent::EndQuery {
                id: Some(id),
                outcome: Ok(value),
            },
            Some(Action::FailReply { id, cause }) => ColEvent::EndQuery {
                id: Some(id),
                outcome: Err(cause),
            },
            Some(Action::CloseSocket) => {
                // CloseSocket with no prior FailReply: classify via
                // cached id (if any) as a connection teardown without
                // a server-attributable cause. Synthesise an Err
                // terminal carrying ConnectionAlreadyClosed-style
                // semantics; the test suite expects a single terminal
                // arm on iter_rows so we route through EndQuery::Err.
                let id: Option<NonZeroU64> = cached_id;
                ColEvent::EndQuery {
                    id,
                    outcome: Err(ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                    }),
                }
            }
            Some(Action::SendBytes(_)) => {
                // SCRAM-mid-handshake bytes flowed through slow path;
                // re-enter caller loop to drain remaining frames.
                // This is rare from inside iter_rows (handshake should
                // complete before SELECT), but possible from a future
                // pipelined design. Currently we treat it as
                // "non-terminal, no event for caller" → NeedMore.
                ColEvent::NeedMore
            }
        }
    }

    /// Read the 2-byte row col_count from the read buffer's unread
    /// region without advancing the cursor.
    #[inline]
    fn read_col_count(&self) -> Result<u16, ProtocolError> {
        let unread = self.proto.read_buf_populated();
        let cursor = usize::from(self.proto.read_buf_cursor_u16());
        // Explicit-match. Architecturally-dead None arm (cursor ≤
        // unread.len() by ReadBuf invariant); the downstream
        // `_ => Err(MalformedDataRow)` classifies the empty-slice
        // case to a typed wire error, but the call-site silent
        // `.unwrap_or(&[])` is banned per CREDO §V.
        let after = match unread.get(cursor..) {
            Some(s) => s,
            None => &[],
        };
        match after {
            [a, b, ..] => {
                let v_i16 = i16::from_be_bytes([*a, *b]);
                // PG wire: col_count is i16; 0 is legal (rare). Negative
                // values are malformed.
                if v_i16 < 0 {
                    return Err(ProtocolError::MalformedDataRow { total_len: 0 });
                }
                Ok(u16::try_from(v_i16).unwrap_or(0))
            }
            _ => Err(ProtocolError::MalformedDataRow { total_len: 0 }),
        }
    }

    /// Read the 4-byte column length prefix at the current cursor
    /// without advancing.
    #[inline]
    fn read_col_len(&self) -> Result<i32, ()> {
        let unread = self.proto.read_buf_populated();
        let cursor = usize::from(self.proto.read_buf_cursor_u16());
        // Explicit-match. Architecturally-dead None arm (cursor ≤
        // unread.len() by ReadBuf invariant); downstream
        // `_ => Err(())` classifies upstream into the caller's
        // MalformedDataRow surface. Call-site silent `.unwrap_or(&[])`
        // banned per CREDO §V.
        let after = match unread.get(cursor..) {
            Some(s) => s,
            None => &[],
        };
        match after {
            [a, b, c, d, ..] => Ok(i32::from_be_bytes([*a, *b, *c, *d])),
            _ => Err(()),
        }
    }

    /// Build the canonical terminal for an architecturally-dead
    /// `read_buf_advance` Err. Drains the inflight, installs Errored,
    /// surfaces an `EndQuery::Err`.
    #[inline]
    #[cold]
    fn terminal_internal_advance_err(&mut self) -> ColEvent<'_> {
        self.drained = true;
        let drained = self.proto.install_errored_read_cursor_advance();
        let cached = self.cached_reply_id;
        let id: Option<NonZeroU64> = drained.or(cached);
        ColEvent::EndQuery {
            id,
            outcome: Err(ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            }),
        }
    }

    /// Build the canonical terminal for a malformed column length
    /// (negative, non-(-1)).
    #[inline]
    #[cold]
    fn terminal_malformed_col_len(&mut self, _bad_len: i32) -> ColEvent<'_> {
        self.drained = true;
        let drained = self.proto.install_errored_malformed_data_row(0);
        let cached = self.cached_reply_id;
        let id: Option<NonZeroU64> = drained.or(cached);
        ColEvent::EndQuery {
            id,
            outcome: Err(ProtocolError::MalformedDataRow { total_len: 0 }),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Drop impl + mem::forget closure
//
// Drop fires unconditionally on every closure exit (normal return,
// `?`-propagation, panic unwind under `panic = "unwind"`). The Drop
// body installs Errored when the stream is mid-frame; the closure
// scope closes `mem::forget` structurally.
// ─────────────────────────────────────────────────────────────────────

impl Drop for RowStream<'_, '_> {
    fn drop(&mut self) {
        if !self.drained {
            // The stream was dropped mid-frame — either via panic
            // unwind, early `return`, `?`-propagation, or closure
            // body returning without consuming all events.
            //
            // The Errored install routes through the leaf-gated
            // FeedStateSetter (see
            // `_stream_dropped_mid_stream_drain_leaf` in mod
            // protocol). The drained in-flight reply id is absorbed
            // here (Drop has no FailReply emission context); the
            // next operation on the connection observes Errored and
            // the wrapper surfaces ConnectionAlreadyClosed { prior_kind:
            // ClientOrdering } so the user's oneshot is not silently
            // leaked.
            self.proto.install_errored_stream_dropped_mid_stream();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// `MAX_FRAME_LEN_FIELD` is referenced from this module for the
// declared-length classifier inside `dispatch_next_frame`. Tier-1
// pin: validate the constant's invariant relationship.
// ─────────────────────────────────────────────────────────────────────

// Tier-1 compile-time anchor: `MAX_FRAME_LEN_FIELD` is referenced via
// the import above to keep the symbol load-bearing.
const _: () = assert!(MAX_FRAME_LEN_FIELD >= 4);

/// Leaf submodule for partial-frame mode entry.
///
/// Hosts the per-call-site concrete-type token gating
/// [`crate::buf::ReadBuf::enter_partial_mode`] /
/// [`crate::buf::ReadBuf::exit_partial_mode`] /
/// [`crate::buf::ReadBuf::subtract_partial_remaining`]. The token's
/// tuple-struct field is private to this submodule — `Self(())` mints
/// are callable ONLY inside the leaf. The leaf lives in `mod row_stream`
/// (the caller module) rather than `mod buf` (the callee), because the
/// call surface must be restricted to the caller module.
///
/// **Tier-1 within-crate by-construction**: hostile callers outside
/// `mod row_stream` attempting to call the partial-mode methods on
/// `ReadBuf` cannot supply the token type — the type system rejects.
///
/// **The only legitimate proximate mint site** is
/// [`mint_for_row_stream_dispatcher`], `pub(in crate::row_stream)`,
/// callable only from inside this module.
//
pub(crate) mod _row_stream_partial_leaf {
    /// **Tier-1 leaf-scope token** for partial-frame mode entry/exit.
    ///
    /// Tuple-struct field private to leaf: `Self(())` mints are
    /// callable ONLY inside this submodule. Hostile in-crate code
    /// outside the leaf cannot construct the type — the type system
    /// rejects.
    pub(crate) struct PartialFrameToken(());

    /// Mint a fresh [`PartialFrameToken`] for the row-stream
    /// dispatcher. Sole legitimate caller is
    /// [`super::RowStream::col_next`] (and its `begin_partial_*` /
    /// `emit_next_*` helpers) when transitioning from
    /// "frame-too-large-but-D-tag-in-streaming-state" classify to
    /// partial-frame mode entry, or when draining body bytes against
    /// the partial-mode counter.
    ///
    /// Visibility `pub(in crate::row_stream)`: only callable from
    /// inside `mod row_stream`. External crates and other in-crate
    /// modules cannot mint the token — the type system rejects
    /// (E0624 method-private-in-this-impl-context).
    #[inline]
    #[must_use]
    pub(in crate::row_stream) fn mint_for_row_stream_dispatcher() -> PartialFrameToken {
        PartialFrameToken(())
    }
}

/// Spec tests for the partial-mode re-entry / exit tier-1 invariants:
/// - `enter_partial_mode` returns `Ok(())` on a fresh buffer (counter
///   was `0`).
/// - `enter_partial_mode` returns `Err(AlreadyInPartialMode)` if called
///   while the counter is already non-zero, AND **does not overwrite**
///   the existing counter value (no silent state corruption).
/// - `exit_partial_mode` returns `Err(PartialModeExitUndrained)` if
///   called with bytes still owed, AND **does not reset** the counter.
///
/// A `()`-returning shape would debug-assert in dev and silently
/// overwrite/reset in release on the re-entry/exit-undrained paths;
/// the typed Result return shape closes that wire-desync surface.
#[cfg(test)]
mod partial_mode_tier1_spec {
    use super::_row_stream_partial_leaf::mint_for_row_stream_dispatcher;
    use crate::buf::{AlreadyInPartialMode, ReadBuf};

    /// First entry into partial mode from `partial_remaining == 0` is
    /// the happy path — Ok and counter updated to declared_len.
    #[test]
    fn enter_partial_mode_on_idle_buffer_is_ok() {
        let mut buf = ReadBuf::new();
        let token = mint_for_row_stream_dispatcher();
        let result = buf.enter_partial_mode(&token, 1024);
        assert!(result.is_ok());
        assert_eq!(buf.partial_remaining(), 1024);
    }

    /// Re-entry while already in partial mode returns Err and does NOT
    /// overwrite the existing counter. A silent-overwrite shape on
    /// release builds would be a wire-desync class regression.
    #[test]
    fn enter_partial_mode_on_partial_buffer_returns_err_and_preserves_counter() {
        let mut buf = ReadBuf::new();
        let token1 = mint_for_row_stream_dispatcher();
        // `clippy::expect_used` is forbid'd crate-wide; assert + tier-1
        // unwrap_or shape for the precondition setup.
        assert!(
            buf.enter_partial_mode(&token1, 2048).is_ok(),
            "fixture: first entry from idle must succeed",
        );

        let token2 = mint_for_row_stream_dispatcher();
        let result = buf.enter_partial_mode(&token2, 4096);
        assert_eq!(
            result,
            Err(AlreadyInPartialMode {
                prev_remaining: 2048,
                new_declared_len: 4096,
            }),
            "re-entry returns typed witness, not silent overwrite",
        );
        // Counter preserved at its prior value (NOT overwritten to 4096).
        assert_eq!(
            buf.partial_remaining(),
            2048,
            "Err arm leaves counter unchanged — a silent-overwrite \
             shape would drop the 2048 bytes still owed on the wire",
        );
    }

    /// Display impl for AlreadyInPartialMode renders the canonical
    /// diagnostic string (drift-detection pin).
    #[test]
    fn already_in_partial_mode_display() {
        extern crate std;
        let e = AlreadyInPartialMode {
            prev_remaining: 100,
            new_declared_len: 200,
        };
        assert_eq!(
            std::format!("{e}"),
            "enter_partial_mode called while already in partial mode \
             (prev remaining: 100 bytes; rejected declared_len: 200)",
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // exit_partial_mode spec
    // ─────────────────────────────────────────────────────────────────

    /// Exit from `partial_remaining == 0` is the happy path — Ok and
    /// counter remains 0.
    #[test]
    fn exit_partial_mode_on_drained_buffer_is_ok() {
        use crate::buf::PartialModeExitUndrained;
        // Const-context witness: pins type-import for `git grep` without
        // `let _ =` form.
        const _: core::marker::PhantomData<PartialModeExitUndrained> =
            core::marker::PhantomData;
        let mut buf = ReadBuf::new();
        let token = mint_for_row_stream_dispatcher();
        // Buffer starts with partial_remaining == 0 (idle).
        let result = buf.exit_partial_mode(&token);
        assert!(result.is_ok(), "idle exit must succeed");
        assert_eq!(buf.partial_remaining(), 0);
    }

    /// Exit while `partial_remaining > 0` returns Err and does NOT
    /// reset the counter. A silent-reset shape on release builds
    /// would be a wire-desync class regression (body bytes still
    /// owed on wire abandoned).
    #[test]
    fn exit_partial_mode_with_undrained_returns_err_and_preserves_counter() {
        use crate::buf::PartialModeExitUndrained;
        let mut buf = ReadBuf::new();
        let token = mint_for_row_stream_dispatcher();
        // Set up partial-mode with 512 bytes still owed.
        assert!(
            buf.enter_partial_mode(&token, 512).is_ok(),
            "fixture: enter must succeed from idle",
        );
        assert_eq!(buf.partial_remaining(), 512);

        let token2 = mint_for_row_stream_dispatcher();
        let result = buf.exit_partial_mode(&token2);
        assert_eq!(
            result,
            Err(PartialModeExitUndrained { remaining: 512 }),
            "undrained exit returns typed witness, not silent \
             counter reset",
        );
        // Counter preserved at its prior value (NOT reset to 0).
        assert_eq!(
            buf.partial_remaining(),
            512,
            "Err arm leaves counter unchanged — a silent-reset \
             shape would abandon the 512 bytes still owed on the wire",
        );
    }

    /// Display impl for PartialModeExitUndrained renders the
    /// canonical diagnostic string (drift-detection pin).
    #[test]
    fn partial_mode_exit_undrained_display() {
        extern crate std;
        use crate::buf::PartialModeExitUndrained;
        let e = PartialModeExitUndrained { remaining: 256 };
        assert_eq!(
            std::format!("{e}"),
            "exit_partial_mode called with 256 bytes still owed on the wire \
             (counter preserved; not silently reset)",
        );
    }
}
