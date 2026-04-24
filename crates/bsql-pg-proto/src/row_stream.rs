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
//! variants (the `streaming_reply_id_and_schema` helper covers
//! all three variants; see below).

use core::num::NonZeroU64;

use crate::action::Action;
use crate::buf::ReadBufFull;
use crate::decode::RowDesc;
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
    Row {
        /// Correlator of the in-flight SELECT / BindExecute reply.
        id: NonZeroU64,
        /// Raw row body, borrowed from `read_buf.populated()`.
        row_bytes: &'a [u8],
        /// Schema arena ref for this row.
        desc: &'a RowDesc,
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
        if self.proto.state_is_errored() {
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

        // Fast-path: DataRow in a row-streaming state.
        if tag == TAG_DATA_ROW
            && let Some((id, schema_ref)) = self.proto.streaming_reply_id_and_schema()
        {
            return self.fast_path_data_row(id, schema_ref, cursor, total);
        }

        // Slow path: any other frame OR DataRow in non-streaming
        // state (which dispatch will classify as UnexpectedFrame).
        self.slow_path_once()
    }

    /// Fast path: extract the DataRow body inline + emit
    /// [`StreamItem::Row`] without OutActions allocation.
    ///
    /// DEF-184 (B25): `read_buf_advance(total)` Err paths
    /// previously silently discarded via `let _ = ...` — tier-4
    /// fallback violating CREDO §1. `total` is parse_header-
    /// validated against `after_cursor.len()` in [`next_event`]
    /// before calling into this fast-path, so advance is
    /// architecturally infallible here. Elevated tier-3 trust to
    /// tier-2 structural via classified `InternalCrateBug` emission
    /// on the dead Err branch.
    ///
    /// DEF-184 (B20): `#[inline]` — hot path per-DataRow emission.
    #[inline]
    fn fast_path_data_row(
        &mut self,
        id: NonZeroU64,
        schema_ref: crate::schema_arena::SchemaRef,
        cursor: usize,
        total: usize,
    ) -> StreamItem<'_> {
        let row_start = cursor.saturating_add(HEADER_LEN);
        let row_end = cursor.saturating_add(total);
        if row_start >= row_end {
            // Empty body — malformed per DEF-154 K (server-side
            // framing desync: total_len == HEADER_LEN).
            self.drained = true;
            self.proto.install_errored_malformed_data_row();
            // Advance Err architecturally dead (total was validated
            // against populated slice length by caller); on dead-
            // branch trip we're already Errored, so classified
            // InternalCrateBug would be double-tagging the same
            // connection-terminal state. Swallow is safe HERE
            // because state is ALREADY Errored — but for audit
            // discipline, verify advance result and replace the
            // cause if it trips (most-recent wins — framing
            // classification preserved above).
            if self.proto.read_buf_advance(total).is_err() {
                self.proto.install_errored_read_cursor_advance();
            }
            return StreamItem::FailReply {
                id,
                cause: ProtocolError::MalformedDataRow { total_len: total },
            };
        }

        // Advance cursor past the frame — captures indices
        // remain valid while populated is borrowed. Err is
        // architecturally dead (total pre-validated); classify
        // into Errored + FailReply instead of silent-skip.
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

        let populated = self.proto.read_buf_populated();
        let row_bytes = populated.get(row_start..row_end).unwrap_or(&[]);
        let arena = self.proto.schema_arena_reader();
        match arena.get(schema_ref) {
            Some(desc) => StreamItem::Row { id, row_bytes, desc },
            None => {
                self.drained = true;
                StreamItem::FailReply {
                    id,
                    cause: ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::StaleSchemaRef,
                    },
                }
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
