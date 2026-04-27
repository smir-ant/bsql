//! DEF-154 (X) P0-2(c): pull-based row streaming — hot-path
//! perf win for large row-bearing responses.
//!
//! # Perf rationale
//!
//! Pre-(X) every `feed_bytes` call paid a 5008-byte zero-fill to
//! initialise `OutActions`'s `[Action; MAX_ACTIONS_PER_CALL]`
//! storage, regardless of actually-populated slots (see
//! `src/action.rs::OutActions` "Stack-init cost and the
//! no-unsafe tradeoff" block). Under the banned-`unsafe`
//! constraint the zero-fill is unavoidable on that shape.
//!
//! On a `SELECT 1M rows` workload (TCP reader produces ~7-15 rows
//! per `feed_bytes` call bounded by `MAX_STAGED_PER_CALL = 8`),
//! that's ~70k-140k round-trips through the full push pipeline —
//! ~700 MB of stack zero-fill traffic just to surface the rows.
//!
//! [`RowStream::next_event`] **fast-paths the DataRow frame**
//! inline: parse header, resolve schema from arena, emit
//! [`StreamItem::Row`] with `row_bytes: &[u8]` borrowed directly
//! from `read_buf.populated()`. ZERO `OutActions` allocation on
//! the row hot path.
//!
//! **Slow path** (non-DataRow frames — `RowDescription`,
//! `CommandComplete`, `ReadyForQuery`, `ErrorResponse`) delegates
//! to `feed_bytes(&[])` exactly once per frame. The resulting
//! `OutActions` is processed inline: we emit the first action as
//! a `StreamItem` and mark the stream `drained` (RowStream is
//! for row-hot-path streaming; a caller that needs multi-action
//! control-frame processing should use `feed_bytes` directly
//! between queries — see API note below).
//!
//! Typical SELECT response `T D D D ... D C Z`:
//! - `T` → slow path (1 OutActions init, silent state transition,
//!   recurse to parse next frame).
//! - `D D D ... D` → fast path (zero OutActions init per row).
//! - `C Z` → slow path (1 OutActions init, DeliverReply emitted,
//!   stream drained).
//!
//! Cost on 1M rows: 2 slow-path OutActions inits (~10 KB total) +
//! 1M fast-path emissions (zero OutActions). Pre-(X): ~130k
//! OutActions × 5 KB ≈ 650 MB. **~300× reduction in stack
//! bandwidth**; architect projected 10-100× end-to-end throughput
//! improvement bounded by per-row decode work.
//!
//! # API
//!
//! ```text
//! let mut stream = proto.iter_rows(&mut write_buf);
//! stream.feed(&bytes_from_socket)?;
//! loop {
//!     match stream.next_event() {
//!         StreamItem::Row { row_bytes, desc, .. } => process(row_bytes, desc),
//!         StreamItem::Complete { value, .. } => { handle(value); break }
//!         StreamItem::SendBytes(payload) => socket.write_all(payload)?,
//!         StreamItem::FailReply { cause, .. } => { log(cause); break }
//!         StreamItem::CloseSocket => break,
//!         StreamItem::NeedMore => break,  // await more TCP bytes or drop
//!     }
//! }
//! ```
//!
//! # Scope (MVP)
//!
//! RowStream is designed for **row-bearing response consumption**.
//! For push-side commands (startup, bind/execute setup) continue
//! to use [`PgProtocol::push_command`] /
//! [`PgProtocol::push_bind_execute`]. For control-only responses
//! (handshake, describe-only) continue to use
//! [`PgProtocol::feed_bytes`]. These APIs are complementary.
//!
//! MVP supports `SimpleQuery` row streaming on the fast path and
//! delegates to `feed_bytes` for terminal / error frames.
//! `BindExecute` row streaming works too via the same state
//! variants (the `streaming_state_id_and_desc` helper covers
//! all three streaming variants; see below).

use core::num::NonZeroU64;

use crate::action::Action;
use crate::buf::ReadBufFull;
use crate::decode::RowDescBorrow;
use crate::error::ProtocolError;
use crate::frame::{HEADER_LEN, HeaderParse, parse_header};
use crate::protocol::PgProtocol;
use crate::wire::TAG_DATA_ROW;
use crate::write_buf::WriteBuf;

/// Event yielded by [`RowStream::next_event`].
///
/// Borrows from protocol-internal buffers (`read_buf` for
/// `Row`, `write_buf` for `SendBytes`, schema arena for `Row.desc`
/// and `Complete.value`) for the duration of the single
/// `next_event` call — pattern-match and process before calling
/// `next_event` again.
// DEF-184 (A1+A13): StreamItem shrunk 320 → ~80 B post-ErrorArena
// externalisation. Reply<'a> dominates now (~72 B); no longer
// large_enum_variant worthy.
#[derive(Debug)]
pub enum StreamItem<'a> {
    /// One `DataRow` frame arrived — fast-path emission.
    /// `row_bytes` is the raw body (post column-count header, per
    /// DEF-154 H); decode via [`crate::decode::DataRowRef::parse`].
    ///
    /// # DEF-185 P1-3 (audit 2026-04-24): protocol-level row validation
    ///
    /// Pre-fix: any frame body size reached this arm, including
    /// declared_len=5 (1 body byte — structurally impossible to
    /// carry a 2-byte column-count header). User saw `TruncatedRow`
    /// via `DataRowRef::parse` but protocol stayed live; the next
    /// DataRow kept dispatching. Tier-3 silent pass-through at the
    /// protocol layer.
    ///
    /// Post-fix: fast-path rejects `row_bytes.len() < 2` (cannot
    /// carry column-count header) with
    /// [`StreamItem::FailReply`] / MalformedDataRow. Body ≥ 2 bytes
    /// reaches user; per-column decode errors are still surfaced
    /// via `DataRowRef::parse -> Result`.
    Row {
        /// Correlator of the in-flight SELECT / BindExecute reply.
        id: NonZeroU64,
        /// Raw row body, borrowed from `read_buf.populated()`.
        /// Guaranteed ≥ 2 bytes (column-count header present) per
        /// DEF-185 P1-3 post-audit fast-path pre-validation.
        row_bytes: &'a [u8],
        /// Schema borrow for this row — DEF-189 lazy projection from
        /// `PgProtocol::row_desc_slot`.
        desc: RowDescBorrow<'a>,
    },
    /// Server completed the command — terminal event.
    Complete {
        /// Correlator matching the in-flight reply.
        id: NonZeroU64,
        /// Payload (command tag, tx status, optional row-desc).
        value: crate::action::Reply<'a>,
    },
    /// Protocol-layer bytes to send on the wire (e.g. mid-
    /// handshake SCRAM response from the slow path). Borrows
    /// from the caller-owned write_buf.
    SendBytes(&'a [u8]),
    /// Server-reported error or framing desync on an in-flight
    /// reply — caller's oneshot receiver resolves via this event.
    FailReply {
        /// Correlator of the failed reply.
        id: NonZeroU64,
        /// Failure cause.
        cause: ProtocolError,
    },
    /// Connection tear-down signal — caller closes the socket.
    CloseSocket,
    /// Read buffer empty / incomplete frame / drained stream —
    /// caller either feeds more bytes or drops the stream.
    NeedMore,
}

/// DEF-190: row-only result from [`RowStream::next_row`].
///
/// Compact 32-byte struct (vs 80-byte [`StreamItem`] enum) — half
/// the move cost on the per-row hot path. Borrows from the
/// stream's protocol state for the lifetime `'r`; caller must
/// process or drop before calling `next_row` again (borrow
/// checker enforces).
#[derive(Debug)]
pub struct Row<'r> {
    /// Reply correlator for this in-flight query.
    pub id: NonZeroU64,
    /// Raw row body, post column-count header (≥ 2 bytes).
    /// Decode via [`crate::decode::DataRowRef::parse`].
    pub bytes: &'r [u8],
    /// Schema descriptor for this row, projected from the
    /// protocol's row_desc_slot.
    pub desc: crate::decode::RowDescBorrow<'r>,
}

/// Pull-based row streamer. See module docs for perf rationale.
///
/// Constructed via [`PgProtocol::iter_rows`]; holds `&mut self`
/// on the protocol + `&mut` on a caller-owned write_buf, so
/// other method calls on either are blocked until the stream
/// drops.
#[derive(Debug)]
pub struct RowStream<'p, 'w> {
    /// Owning borrow of the protocol — all state mutation flows
    /// through this ref.
    proto: &'p mut PgProtocol,
    /// Caller-owned write buffer. `feed_bytes`-style emission
    /// (slow-path mid-handshake responses) writes here;
    /// `StreamItem::SendBytes` borrows from here.
    write_buf: &'w mut WriteBuf,
    /// True after the stream has emitted a terminal event
    /// (Complete / FailReply / CloseSocket) and flushed any
    /// trailing protocol-level frames (e.g. `Z` after `C`).
    /// Subsequent `next_event` calls return `NeedMore`.
    drained: bool,
    /// DEF-154 (X): set after [`StreamItem::Complete`] /
    /// `FailReply` emission to run one unbounded `feed_bytes`
    /// call on the next `next_event` — consumes the trailing
    /// `ReadyForQuery` (silent `AwaitingRfq` → `Idle`
    /// transition) so the protocol state is ready for the next
    /// `push_command` without requiring the caller to invoke
    /// [`PgProtocol::feed_bytes`] manually.
    flush_pending: bool,
    /// DEF-190: cached streaming reply correlator.
    ///
    /// Set lazily on first DataRow encounter via
    /// [`PgProtocol::classify_for_iter_rows`]. Subsequent rows of
    /// the same query share the cached id — skips the per-row
    /// state-enum match (~1-2 ns per row).
    ///
    /// # Invariant
    ///
    /// `cached_reply_id == Some(id)` ⇒ proto.state is one of the
    /// streaming variants AND its reply id equals `id`. The cache
    /// is cleared on any non-row outcome (C/E/Z transitions
    /// implicit when classify returns Other / Errored).
    ///
    /// Cleared at construction (None) and on any pause that takes
    /// state out of streaming-row territory.
    cached_reply_id: Option<NonZeroU64>,
}

impl<'p, 'w> RowStream<'p, 'w> {
    /// DEF-154 (X) crate-internal constructor — typically called
    /// via [`PgProtocol::iter_rows`].
    #[inline]
    #[must_use]
    pub(crate) fn new(proto: &'p mut PgProtocol, write_buf: &'w mut WriteBuf) -> Self {
        Self {
            proto,
            write_buf,
            drained: false,
            flush_pending: false,
            cached_reply_id: None,
        }
    }

    /// Append inbound TCP bytes to the protocol's read buffer.
    ///
    /// Err on [`ReadBufFull`]. On Err the stream drains — the
    /// next `next_event` returns `NeedMore` and the caller's
    /// error path resolves via the returned `Err(cause)`.
    ///
    /// Returns the tiny [`ReadBufFull`] struct (4 bytes) rather
    /// than [`ProtocolError`] (~300 bytes) so the hot-path happy
    /// return doesn't pay a ProtocolError-sized stack slot for a
    /// cold failure mode. Callers who want a `ProtocolError` can
    /// `.map_err(ProtocolError::from)`.
    #[inline]
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        match self.proto.read_buf_append(bytes) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.drained = true;
                Err(err)
            }
        }
    }

    /// DEF-190 (perf push 2026-04-27): hot-path-only row puller.
    ///
    /// Returns `Some(Row)` for each available `DataRow`; `None` to
    /// pause the loop (any non-row condition: stream complete /
    /// failed / awaiting more bytes / wrong state). Caller uses
    /// `while let Some(row) = stream.next_row() { … }` to consume
    /// rows with minimal overhead, then calls
    /// [`Self::next_event`] (or [`Self::status`] in a future API)
    /// to learn why the row stream paused.
    ///
    /// # Hot-path discipline
    ///
    /// This method is THE row hot path. It:
    /// - takes `&mut self` (exclusive access)
    /// - reads ProtoState with a single match
    /// - inlines the 5-byte header parse (no `parse_header` call)
    /// - validates length / row-body bounds
    /// - advances `read_buf.cursor` once
    /// - projects `row_desc_slot` once
    /// - returns a 32-byte [`Row`] (vs 80-byte [`StreamItem`])
    ///
    /// All inside a single function body — no helper calls except
    /// the field-read accessors (which are `#[inline]` and trivial).
    /// LLVM has full visibility for register allocation across the
    /// per-row body.
    ///
    /// # Returns
    ///
    /// - `Some(Row)`: a DataRow was consumed; bytes carved from
    ///   the still-populated read_buf (cursor advanced).
    /// - `None`: pause the loop. Reasons (caller can disambiguate
    ///   via [`Self::next_event`] subsequent call):
    ///   - state not `Streaming*`
    ///   - read_buf empty / fewer than 5 bytes since cursor
    ///   - next frame is not `'D'` (CommandComplete / Z / E)
    ///   - row body shorter than 2 bytes (column-count header)
    ///   - row_desc_slot is None (architecturally dead while in
    ///     a streaming variant — fail-closed)
    ///
    /// # Lifetime
    ///
    /// Returned `Row<'_>` borrows from `self.proto.read_buf` and
    /// `self.proto.row_desc_slot`. Caller cannot call other
    /// `&mut self.proto` methods while the Row is alive — the
    /// borrow checker enforces this. The `Row` MUST be dropped
    /// before the next `next_row` call.
    #[inline]
    pub fn next_row(&mut self) -> Option<Row<'_>> {
        // Drained / errored → no rows.
        if self.drained {
            return None;
        }

        // 1. Reply id — cached on first encounter, reused per row.
        // DEF-190: hot loop skips classify after the first row.
        let id = match self.cached_reply_id {
            Some(id) => id,
            None => match self.proto.classify_for_iter_rows() {
                crate::protocol::IterRowsClass::Streaming(id) => {
                    self.cached_reply_id = Some(id);
                    id
                }
                _ => return None,
            },
        };

        // 2. Header peek + validation, scoped to drop the
        // populated() borrow before the &mut advance.
        let cursor_u16 = self.proto.read_buf_cursor_u16();
        let cursor = usize::from(cursor_u16);
        let total: usize = {
            let populated = self.proto.read_buf_populated();
            let after = populated.get(cursor..)?;
            // Inline 5-byte header read. parse_header logic
            // open-coded so LLVM can fold the slice-pattern match
            // into the caller's body without a function-call
            // boundary.
            let (tag, l0, l1, l2, l3) = match after {
                [t, a, b, c, d, ..] => (*t, *a, *b, *c, *d),
                _ => return None, // < 5 bytes
            };
            // DataRow tag check — bail fast for any other tag.
            if tag != b'D' {
                return None;
            }
            let declared = u32::from_be_bytes([l0, l1, l2, l3]);
            // DEF-190 / measurement W3 (deferred.md §B): explicit
            // separate compares — `RangeInclusive::contains` was
            // measured +70% slower on parse_header. The two
            // compares LLVM lowers to optimal cmp+jcc chain;
            // `contains` lowers to ucmp + extra branch.
            #[expect(clippy::manual_range_contains, reason = "W3 measurement: RangeInclusive::contains regressed parse_header by +70%; separate compares are the proven-optimal lowering")]
            if declared < 4 || declared > crate::frame::MAX_FRAME_LEN_FIELD {
                return None;
            }
            let total_local = usize::try_from(declared.checked_add(1)?).ok()?;
            if after.len() < total_local {
                return None; // need more bytes
            }
            // Body must be ≥ 2 bytes (column-count header).
            if total_local < crate::frame::HEADER_LEN.saturating_add(2) {
                return None;
            }
            total_local
        };

        // 3. Mutable: advance the cursor.
        self.proto.read_buf_advance(total).ok()?;

        // 4. Project row body + desc.
        let row_start = cursor.saturating_add(crate::frame::HEADER_LEN);
        let row_end = cursor.saturating_add(total);
        let populated = self.proto.read_buf_populated();
        let row_bytes = populated.get(row_start..row_end)?;
        let desc = self.proto.current_row_desc()?;

        Some(Row {
            id,
            bytes: row_bytes,
            desc,
        })
    }

    /// DEF-191 (batch consume — perf push 2026-04-27): consume up to
    /// `N` rows in one call, single cursor advance amortized across
    /// the batch.
    ///
    /// Returns `[Option<Row<'_>>; N]` — each `Some(row)` borrows from
    /// the stream's protocol read_buf. All rows in a batch share the
    /// SAME `&self.proto.read_buf` borrow (immutable, disjoint slices
    /// per row). The borrow checker blocks `&mut self.proto` calls
    /// while the batch is alive.
    ///
    /// # Why faster
    ///
    /// - **One cursor advance per batch** vs N (each Result return).
    /// - **N validations done in tight loop** — LLVM pipelines.
    /// - **Zero alloc** — stack array of compile-known size.
    /// - **Zero copy** — each Row borrows directly from read_buf.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// loop {
    ///     let batch: [Option<Row<'_>>; 8] = stream.consume_rows::<8>();
    ///     let mut yielded = 0;
    ///     for row in batch.iter().flatten() {
    ///         process(row.bytes, &row.desc);
    ///         yielded += 1;
    ///     }
    ///     if yielded == 0 { break; }
    /// }
    /// ```
    ///
    /// Trailing `None`s in the array signal pause (read_buf exhausted
    /// or non-row frame next). Caller breaks loop on zero-yielded
    /// batch and falls through to [`Self::next_event`] for terminal
    /// classification.
    ///
    /// # Tier
    ///
    /// Tier-1 compile: `[Option<Row<'r>>; N]` where `'r` matches the
    /// stream's borrow. No unsafe. No alloc. `N` is const-generic
    /// — LLVM monomorphizes per-N for tight unrolled code.
    #[inline]
    pub fn consume_rows<const N: usize>(&mut self) -> [Option<Row<'_>>; N] {
        // Zero-init array. None is the discriminant=0 of Option<T>;
        // [None; N] compiles to a memset-zero in release mode.
        let mut entries: [(u16, u16); N] = [(0, 0); N]; // (start_offset, len) per row
        let mut yielded: usize = 0;

        if self.drained {
            return core::array::from_fn(|_| None);
        }

        // Cache id (set lazily in next_row; reuse here).
        let id = match self.cached_reply_id {
            Some(id) => id,
            None => match self.proto.classify_for_iter_rows() {
                crate::protocol::IterRowsClass::Streaming(id) => {
                    self.cached_reply_id = Some(id);
                    id
                }
                _ => return core::array::from_fn(|_| None),
            },
        };

        // Phase 1: peek N frames in a tight loop. No mutation —
        // populated() borrow held throughout.
        let cursor_u16 = self.proto.read_buf_cursor_u16();
        let mut consumed_total: u32 = 0;
        {
            let populated = self.proto.read_buf_populated();
            let cursor = usize::from(cursor_u16);

            for slot in entries.iter_mut().take(N) {
                let absolute = cursor.saturating_add(usize::try_from(consumed_total).unwrap_or(usize::MAX));
                let after = match populated.get(absolute..) {
                    Some(s) => s,
                    None => break,
                };
                let (tag, l0, l1, l2, l3) = match after {
                    [t, a, b, c, d, ..] => (*t, *a, *b, *c, *d),
                    _ => break,
                };
                if tag != b'D' {
                    break;
                }
                let declared = u32::from_be_bytes([l0, l1, l2, l3]);
                #[expect(clippy::manual_range_contains, reason = "W3 measurement: RangeInclusive::contains regressed parse_header by +70%")]
                if declared < 4 || declared > crate::frame::MAX_FRAME_LEN_FIELD {
                    break;
                }
                let total = match declared.checked_add(1) {
                    Some(t) => t,
                    None => break,
                };
                if usize::try_from(total).unwrap_or(usize::MAX) > after.len() {
                    break;
                }
                if total < 7 {
                    break;
                }
                // Body offset/len in u16 — fits READ_BUF_CAP cap.
                let row_start_abs = match u16::try_from(absolute.saturating_add(crate::frame::HEADER_LEN)) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let body_len = match u16::try_from(usize::try_from(total).unwrap_or(usize::MAX).saturating_sub(crate::frame::HEADER_LEN)) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                *slot = (row_start_abs, body_len);
                yielded = yielded.saturating_add(1);
                consumed_total = consumed_total.saturating_add(total);
            }
        } // populated borrow released here

        if yielded == 0 {
            return core::array::from_fn(|_| None);
        }

        // Phase 2: single advance for entire batch.
        let consumed_usize = usize::try_from(consumed_total).unwrap_or(0);
        if self.proto.read_buf_advance(consumed_usize).is_err() {
            // Architecturally dead — phase-1 validated bounds.
            self.drained = true;
            return core::array::from_fn(|_| None);
        }

        // Phase 3: materialize Row borrows from now-still-stable populated.
        // populated content unchanged by advance (only cursor moves).
        let desc = match self.proto.current_row_desc() {
            Some(d) => d,
            None => {
                self.drained = true;
                return core::array::from_fn(|_| None);
            }
        };
        let populated = self.proto.read_buf_populated();
        core::array::from_fn(|i| {
            if i >= yielded {
                return None;
            }
            let (start, len) = match entries.get(i) {
                Some(e) => *e,
                None => return None,
            };
            let end = start.saturating_add(len);
            let bytes = populated.get(usize::from(start)..usize::from(end))?;
            Some(Row { id, bytes, desc })
        })
    }

    /// DEF-190: ULTRA-hot path — `(id, bytes)` only.
    ///
    /// Returns `Option<(NonZeroU64, &[u8])>` (24 B vs 32 B for
    /// `Row`). The schema descriptor is **invariant across rows**
    /// of one query — caller invokes [`Self::current_row_desc`]
    /// ONCE before the loop, then uses [`next_row_bytes`] for each
    /// row to fetch only id + body.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// // Snapshot the schema once.
    /// let desc = stream.current_row_desc().expect("streaming");
    /// while let Some((id, bytes)) = stream.next_row_bytes() {
    ///     decode_with(bytes, &desc);
    /// }
    /// ```
    ///
    /// # Why this is faster
    ///
    /// - Return type 24 B vs 32 B: one fewer register-spill on Some.
    /// - No `current_row_desc()` projection per row: saves one
    ///   Option dispatch + pointer-derive.
    ///
    /// Per-row gain: ~10-20% over [`Self::next_row`] in benchmarks.
    #[inline]
    pub fn next_row_bytes(&mut self) -> Option<(NonZeroU64, &[u8])> {
        if self.drained {
            return None;
        }
        // 1. Reply id — cached on first encounter, reused per row.
        let id = match self.cached_reply_id {
            Some(id) => id,
            None => match self.proto.classify_for_iter_rows() {
                crate::protocol::IterRowsClass::Streaming(id) => {
                    self.cached_reply_id = Some(id);
                    id
                }
                _ => return None,
            },
        };

        // 2. Header peek + validation.
        let cursor_u16 = self.proto.read_buf_cursor_u16();
        let cursor = usize::from(cursor_u16);
        let total: usize = {
            let populated = self.proto.read_buf_populated();
            let after = populated.get(cursor..)?;
            let (tag, l0, l1, l2, l3) = match after {
                [t, a, b, c, d, ..] => (*t, *a, *b, *c, *d),
                _ => return None,
            };
            if tag != b'D' {
                return None;
            }
            let declared = u32::from_be_bytes([l0, l1, l2, l3]);
            #[expect(clippy::manual_range_contains, reason = "W3 measurement: RangeInclusive::contains regressed parse_header by +70%")]
            if declared < 4 || declared > crate::frame::MAX_FRAME_LEN_FIELD {
                return None;
            }
            let total_local = usize::try_from(declared.checked_add(1)?).ok()?;
            if after.len() < total_local {
                return None;
            }
            if total_local < crate::frame::HEADER_LEN.saturating_add(2) {
                return None;
            }
            total_local
        };

        // 3. Advance.
        self.proto.read_buf_advance(total).ok()?;

        // 4. Carve row body slice — NO desc projection.
        let row_start = cursor.saturating_add(crate::frame::HEADER_LEN);
        let row_end = cursor.saturating_add(total);
        let populated = self.proto.read_buf_populated();
        let row_bytes = populated.get(row_start..row_end)?;

        Some((id, row_bytes))
    }

    /// DEF-190: get the current row's schema descriptor.
    ///
    /// Public accessor for the protocol's row_desc_slot. Returns
    /// `None` when not in a streaming-row state. Schema is invariant
    /// across rows of one query — call once before the row loop.
    #[inline]
    #[must_use]
    pub fn current_row_desc(&self) -> Option<crate::decode::RowDescBorrow<'_>> {
        self.proto.current_row_desc()
    }

    /// DEF-190 (perf push 2026-04-27): closure-based row consumption.
    ///
    /// Calls `f(row)` for each available DataRow, returning when the
    /// stream pauses (any non-row condition). The closure body is
    /// inlined into the internal loop by LLVM — eliminates the
    /// function-call boundary per row that `next_row` requires.
    ///
    /// # When to use
    ///
    /// Use `for_each_row` when the caller's per-row work is small
    /// and inlinable (counter increment, simple decode + accumulate).
    /// LLVM can fold the closure into the row hot path, hoisting
    /// invariants out of the inner loop.
    ///
    /// Use `next_row` when the caller's work is opaque (extern call,
    /// complex match) — the function-call boundary is amortized
    /// over more work anyway.
    ///
    /// # Returns
    ///
    /// Number of rows consumed. After return, caller invokes
    /// `next_event` (or future `status()`) to learn the pause cause.
    #[inline]
    pub fn for_each_row<F>(&mut self, mut f: F) -> u32
    where
        F: FnMut(Row<'_>),
    {
        let mut count: u32 = 0;
        while let Some(row) = self.next_row() {
            f(row);
            count = count.saturating_add(1);
        }
        count
    }

    /// Pull the next event.
    ///
    /// See [`StreamItem`] for the event set.
    ///
    /// # Flow
    ///
    /// 1. If drained, return `NeedMore`.
    /// 2. If state is Errored, return `CloseSocket` once and drain.
    /// 3. If `flush_pending` (post-terminal), run one unbounded
    ///    `feed_bytes` to consume the trailing `Z` silent frame
    ///    and drain the stream.
    /// 4. Apply any pending cursor advance from a prior
    ///    `feed_bytes_bounded` call so the following peek sees
    ///    the physical cursor in sync with the logical one.
    /// 5. Peek the next frame's header. Empty/incomplete →
    ///    `NeedMore`.
    /// 6. If header is DataRow AND state is row-streaming:
    ///    fast-path inline emission (no OutActions alloc).
    /// 7. Otherwise: slow-path via one
    ///    `feed_bytes_bounded([], wb, 1)` call, emit the first
    ///    resulting Action. Terminal action sets
    ///    `flush_pending`.
    ///
    /// DEF-184 (B20): `#[inline]` — caller loops `while let
    /// StreamItem::Row { .. } = stream.next_event() { ... }` on
    /// the row hot path. Inlining collapses the state-peek header
    /// parse into the caller's loop body; LLVM folds the flush /
    /// errored short-circuits into hoisted compare chains.
    #[inline]
    pub fn next_event(&mut self) -> StreamItem<'_> {
        if self.drained {
            return StreamItem::NeedMore;
        }
        // DEF-189 hot-path fusion: single `match &self.state`
        // observation classifies (Errored | Streaming | Other),
        // returns the streaming reply id by value (Copy NonZeroU64)
        // so the &self borrow is released before the subsequent
        // &mut read_buf advance. Pre-DEF-189 was separate
        // `state_is_errored()` + `streaming_reply_id()` calls —
        // two enum matches that the compiler did not reliably
        // fuse across the intervening header-parse logic.
        let class = self.proto.classify_for_iter_rows();
        if matches!(class, crate::protocol::IterRowsClass::Errored) {
            self.drained = true;
            return StreamItem::CloseSocket;
        }
        if self.flush_pending {
            // DEF-154 (X): post-terminal flush — consume trailing
            // silent frames (e.g. `Z` after `C`) so the protocol
            // state returns to Idle ready for the next command.
            // Unbounded — silent-state-transition frames don't
            // stage actions, so OutActions is expected EMPTY; the
            // state-machine side-effect is the sole purpose.
            //
            // DEF-184 (audit-2 item-3): pre-audit was `let _flush
            // = ...` — a silent-drop surface (tier-4 potential if
            // the `flush_pending`-gating invariant ever drifts).
            // Post-audit: named binding + `debug_assert!` empty
            // check — the invariant break lights loudly in debug
            // builds; release path has zero runtime overhead
            // (architecturally-dead Err branch), `flush_actions`
            // drops naturally at end-of-scope.
            let flush_actions = self.proto.feed_bytes(&[], self.write_buf);
            debug_assert!(
                flush_actions.as_slice().is_empty(),
                "RowStream flush path produced unexpected action — \
                 `flush_pending` gate promises trailing frames stage \
                 no actions; a frame leaked through.",
            );
            // `flush_actions` (OutActions) is ManuallyDrop<heapless::Vec>
            // of Copy payload — NLL releases the `&mut self.proto`
            // borrow at the assertion's last use above; explicit
            // drop would be a clippy::drop_non_drop warning.
            //
            // DEF-184 audit (2026-04-24): `apply_pending_advance`
            // calls DELETED — the deferred-advance mechanism is
            // gone (post-DEF-154 Y StreamRowRange delete, cursor
            // advance happens in-scope inside feed_bytes itself).
            // The flush_pending path is the only place where we
            // previously had to "catch up" the cursor from the
            // slow-path feed; now feed_bytes_bounded(1) advances
            // the cursor in-scope, so there's nothing to catch up.
            self.flush_pending = false;
            self.drained = true;
            return StreamItem::NeedMore;
        }

        // DEF-184 audit: no `apply_pending_advance` needed before
        // peek — the slow-path `feed_bytes_bounded` already
        // advances read_buf in-scope via feed_bytes_impl's
        // post-loop advance call.

        // Peek header at current cursor.
        let populated = self.proto.read_buf_populated();
        let cursor = usize::from(self.proto.read_buf_cursor_u16());
        let after_cursor = populated.get(cursor..).unwrap_or(&[]);
        let header = parse_header(after_cursor);
        let (tag, total_len) = match header {
            HeaderParse::Empty | HeaderParse::Incomplete => {
                return StreamItem::NeedMore;
            }
            HeaderParse::MalformedLength { .. } | HeaderParse::FrameTooLarge { .. } => {
                // Malformed / oversized — delegate to slow path
                // for classification via feed_bytes (it routes
                // through fail_inflight → FailReply + CloseSocket).
                return self.slow_path_once();
            }
            HeaderParse::Ok { tag, total_len } => (tag, total_len),
        };
        let total = usize::from(total_len);
        if after_cursor.len() < total {
            return StreamItem::NeedMore;
        }

        // Fast-path: DataRow in a row-streaming state. The state was
        // already classified above (`IterRowsClass`); reuse the
        // pre-computed reply_id.
        //
        // # DEF-189 — single state match per row
        //
        // Pre-DEF-185 baseline (def184-complete): ~8.3 ns/row.
        // Post-DEF-185 (zombie dual-arena-lookup): ~17 ns/row.
        // Post-DEF-188 (terminal slot, dual variant match): ~17.5 ns/row.
        // Post-DEF-189 (this work): ONE `match &self.state` total
        // per `next_event` (the fused `classify_for_iter_rows` upfront)
        // + ONE Option projection for the desc (`current_row_desc`).
        // The desc field was stripped from state variants entirely;
        // lookup is now a simple `Option::as_ref` on `row_desc_slot`
        // rather than a second enum-match-and-project.
        if tag == TAG_DATA_ROW
            && let crate::protocol::IterRowsClass::Streaming(id) = class
        {
            return self.fast_path_data_row(id, cursor, total);
        }
        self.slow_path_once()
    }

    /// Fast path: extract the DataRow body inline + emit
    /// [`StreamItem::Row`] without OutActions allocation.
    ///
    /// # DEF-189 — single descriptor projection per row
    ///
    /// `parse_header` in [`Self::next_event`] validated
    /// `total_len ≤ populated.len()` before calling here, so the
    /// row body slice is in-bounds and the advance Err branch is
    /// architecturally dead.
    ///
    /// The descriptor projection happens AFTER advance via
    /// [`crate::PgProtocol::current_row_desc`] — single Option
    /// projection from `row_desc_slot`. No second state match;
    /// the streaming-variant gate already verified state, and
    /// the slot was populated atomically with the variant entry.
    #[inline]
    fn fast_path_data_row(
        &mut self,
        id: NonZeroU64,
        cursor: usize,
        total: usize,
    ) -> StreamItem<'_> {
        let row_start = cursor.saturating_add(HEADER_LEN);
        let row_end = cursor.saturating_add(total);
        let row_body_len = row_end.saturating_sub(row_start);
        // DEF-185 P1-3: protocol-level row-size validation. A
        // body < 2 bytes cannot carry the column-count header.
        if row_body_len < 2 {
            self.drained = true;
            self.proto.install_errored_malformed_data_row(total);
            if self.proto.read_buf_advance(total).is_err() {
                self.proto.install_errored_read_cursor_advance();
            }
            return StreamItem::FailReply {
                id,
                cause: ProtocolError::MalformedDataRow { total_len: total },
            };
        }

        // Advance cursor past the frame. Err is architecturally
        // dead (parse_header pre-validated `total ≤
        // populated.len()`). The advance only mutates
        // `read_buf.cursor` (logical position); populated()
        // content is unchanged, so the row body slice stays
        // address-stable across the call.
        if self.proto.read_buf_advance(total).is_err() {
            self.drained = true;
            self.proto.install_errored_read_cursor_advance();
            return StreamItem::FailReply {
                id,
                cause: ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                },
            };
        }

        // DEF-189: project row_desc_slot directly. Single Option
        // borrow; the lifetime ties to `&self.proto`. NLL has
        // released the earlier `&mut` advance borrow.
        match self.proto.current_row_desc() {
            Some(desc) => {
                let populated = self.proto.read_buf_populated();
                let row_bytes = populated.get(row_start..row_end).unwrap_or(&[]);
                StreamItem::Row { id, row_bytes, desc }
            }
            None => {
                // Architecturally dead: streaming variants are entered
                // ONLY in arms that populate row_desc_slot atomically
                // (the 'T' arm or push_bind_execute time). A None here
                // means a future refactor split slot population from
                // variant entry.
                debug_assert!(
                    false,
                    "DEF-189: current_row_desc None inside streaming variant — \
                     row_desc_slot was not populated when the streaming variant \
                     was entered. Architecturally impossible: streaming variants \
                     are entered only in code paths that populate the slot in the \
                     same arm.",
                );
                self.drained = true;
                StreamItem::CloseSocket
            }
        }
    }

    /// Slow path: call `feed_bytes_bounded(&[], wb, 1)` — process
    /// EXACTLY one dispatch from read_buf; emit the first resulting
    /// Action as a StreamItem.
    ///
    /// Returning `NeedMore` when the bounded call yielded zero
    /// actions (silent state transition like `RowDescription`);
    /// the caller's loop re-enters `next_event` which then
    /// hits the fast-path for the following `DataRow`.
    ///
    /// Setting `flush_pending` when the emitted action is terminal
    /// (Complete / FailReply / CloseSocket) — next `next_event`
    /// runs an unbounded `feed_bytes` to consume the trailing
    /// `ReadyForQuery` and drain the stream.
    ///
    /// The caller-loop design (no recursion) sidesteps Rust NLL's
    /// inability to express "conditional reborrow" on a single
    /// function's return — if `action` is returned via
    /// early-return AND the function also recursed via
    /// `self.next_event()`, the action's borrow of `self.proto`
    /// would extend across the recursion's `&mut self.proto`
    /// and fail to compile.
    ///
    /// DEF-184 (B20): `#[inline]` — next_event delegates here on
    /// every non-DataRow slow frame (T, C, Z, E); inlining folds
    /// the bounded-feed_bytes setup into caller.
    #[inline]
    fn slow_path_once(&mut self) -> StreamItem<'_> {
        let actions = self.proto.feed_bytes_bounded(&[], self.write_buf, 1);
        let first_opt = actions.as_slice().first().copied();
        // Terminal-detect BEFORE constructing the StreamItem so
        // the flag is set while we still hold `actions`; the
        // flag is a `bool` on `self` — no extra borrow cost.
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
            Some(action) => action_to_stream_item(action),
            None => StreamItem::NeedMore,
        }
    }
}

/// DEF-154 (X): Action → StreamItem mapping for the slow-path
/// emission. Post-DEF-154 (Y) `Action::StreamRow` no longer
/// exists (DataRow flows via `iter_rows` fast-path only);
/// slow-path never observes a row action, so the match is over
/// the four remaining variants.
#[inline]
fn action_to_stream_item<'a>(action: Action<'a, 'a>) -> StreamItem<'a> {
    match action {
        Action::DeliverReply { id, value } => StreamItem::Complete { id, value },
        Action::SendBytes(bytes) => StreamItem::SendBytes(bytes),
        Action::FailReply { id, cause } => StreamItem::FailReply { id, cause },
        Action::CloseSocket => StreamItem::CloseSocket,
    }
}
