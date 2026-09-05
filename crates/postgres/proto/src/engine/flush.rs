//! Engine-owned outbound send buffer and its cancellation-safe drain loop.
//!
//! # The send cursor lives here, never in the flush future
//!
//! [`SendBuf`] holds already-encoded outbound bytes plus a `sent` cursor —
//! the number of bytes the transport has confirmed accepted. The buffer is
//! owned by the engine (the caller), so it **survives the drop of any
//! [`flush`] future that borrows it**. That survival is the whole point: a
//! flush future dropped mid-drain (an `async` task cancelled at its only
//! suspension point) leaves the cursor exactly reflecting the bytes the
//! socket took, so the next flush over the same buffer resumes from
//! `pending()` — no double-send, no lost byte, even when the cancellation
//! lands in the middle of a wire frame.
//!
//! The corrupting shape is a `sent` counter living *inside* the flush future
//! (the shape a naive `write_all` loop has): dropping the future discards
//! the counter, and the resumed flush restarts from zero and re-sends the
//! prefix the socket already received. Keeping the cursor in [`SendBuf`]
//! makes that class of bug unrepresentable.
//!
//! # One write attempt per loop step; commit is synchronous
//!
//! [`Transport::write`](super::seams::Transport::write) is a single write
//! attempt mirroring one `poll_write`: it returns the count the socket
//! accepted (possibly partial), or stays `Pending` having accepted zero. The
//! drain loop performs exactly one such attempt per iteration and advances
//! the cursor **synchronously**, immediately after the write resolves
//! `Ready(Ok(n))`, with no `.await` between the resolved write and the
//! advance. That adjacency is what makes a drop at the suspension point
//! unroll cleanly, and it is machine-enforced by a static
//! no-`.await`-between-write-and-advance source scan in the test suite, not
//! merely by this prose.
//!
//! # Draining the transport's own buffer
//!
//! Moving every byte out of [`SendBuf`] is not the same as putting them on
//! the wire: a buffering transport (a TLS layer encrypting plaintext into an
//! internal record) still holds bytes the loop's writes handed it. After the
//! buffer is drained, [`flush`] calls
//! [`Transport::flush`](super::seams::Transport::flush) once to drive that
//! transport-internal buffer to the socket, so a partial wire frame cannot be
//! left dangling. A plaintext transport flushes nothing. A buffering
//! transport must own its internal buffer (as [`SendBuf`] owns its bytes) for
//! that flush to be cancellation-safe in turn.
//!
//! # Scrub-on-drop
//!
//! [`SendBuf`] holds outbound wire bytes; once a connection's authentication
//! flows through it those include the SCRAM client proof and the password
//! message. `Drop` zeroizes the **full backing capacity**, not just the
//! populated prefix: [`reset`](SendBuf::reset) truncates after compacting the
//! unsent tail, leaving already-sent secret bytes resident in the spare
//! capacity, so a length-only scrub would leak them. The scrub is watched by
//! the crate's zeroize-coverage manifest gate, so dropping it is a build
//! failure, not a silent regression.
//!
//! # Why the drain is a free function over disjoint `&mut`
//!
//! The drain couples a [`SendBuf`] to a [`Transport`]; the sans-I/O phase
//! engines that frame and dispatch hold no transport, so the I/O-bearing loop
//! cannot be one of their methods. It lives at the layer that owns both the
//! buffer and the transport — the session pump — as a free function over
//! `(&mut SendBuf, &mut T)`, the disjoint split-borrow that layer threads.
//! Keeping [`SendBuf`] transport-free also keeps it directly testable: a test
//! owns the buffer and the transport separately and inspects both between
//! cancellations.
//!
//! # Constraint: one outbound buffer after the cutover
//!
//! The connecting phase currently emits via the bounded outbound frame
//! *builder* elsewhere in the crate, which has no cancellation-safe drain;
//! [`SendBuf`] is that drain. A later pump/cutover must unify outbound on
//! [`SendBuf`] — which routes the SCRAM client proof through it, which is why
//! the scrub above must already be in place.
//!
//! # Bounding is a verb-layer concern (recorded, not silent)
//!
//! [`SendBuf`] grows on demand: [`enqueue`](SendBuf::enqueue) appends without
//! a hard ceiling. A bound on a single pipelined batch — how many frames a
//! verb encoder may queue before it must drain — belongs to that verb layer,
//! which knows the protocol-level batch semantics; it is not a property of
//! this byte cursor. The slow-peer / zero-window case (a peer that stops
//! reading, so a write would block indefinitely) is likewise a verb-layer
//! bound, not a buffer concern: [`SendBuf`] holds only locally-produced bytes,
//! so no peer can force its growth.
//!
//! Reaching the zero-allocation steady state is a constraint on the layer above:
//! every capacity-reclaiming method ([`reset`](SendBuf::reset),
//! [`scrub_drained`](SendBuf::scrub_drained)) retains the backing allocation, so
//! a follow-on batch that fits reuses it with no reallocation — but that holds
//! only once a compaction point actually runs between batches.
//! [`scrub_drained`](SendBuf::scrub_drained) is that point at handshake
//! completion (it also erases the handshake's secret-bearing wire, see below);
//! a per-command compaction in the active phase is that layer's responsibility.

use alloc::vec::Vec;
use core::fmt;

use super::error::EngineError;
use super::seams::Transport;
use crate::write_buf::WriteBufFull;

/// High-water capacity, in bytes, above which
/// [`SendBuf::reclaim_if_oversized`] releases the outbound backing allocation
/// at a cold pool-checkout settle point.
///
/// A single large `Bind` parameter block (a multi-megabyte `bytea` / `jsonb` /
/// `text` parameter) streams UNCAPPED onto [`SendBuf`] (see the module docs),
/// and that grown capacity is otherwise retained for the whole life of the
/// connection — a steady-state memory bloat for a long-lived pooled connection
/// that once sent a big payload. `reclaim_if_oversized` reclaims it.
///
/// The mark is `64 KiB`, matching the COPY-in flush threshold
/// (`COPY_IN_FLUSH_THRESHOLD` in `engine::verbs`): a NORMAL query's outbound
/// batch — a `≤ MAX_SQL_LEN` (2 KiB) simple query, or a `Parse`+`Bind`+…+`Sync`
/// pipeline with modest scalar params — never crosses it, so a steady
/// small-query workload NEVER triggers a shrink (no thrash, zero realloc on the
/// common path). Only a genuinely oversized buffer is reclaimed. The two
/// constants are independent policies that happen to share `64 KiB`; a drift
/// between them is not a correctness issue.
pub(crate) const SEND_BUF_HIGH_WATER: usize = 64 * 1024;

/// The baseline capacity, in bytes, [`SendBuf::reclaim_if_oversized`] shrinks an
/// oversized backing down to.
///
/// Large enough that a following normal query — and the reset round trip's own
/// `RESET ALL` simple query — reuses the retained allocation without a realloc
/// (so a shrink is never immediately undone by the next small batch); small
/// enough that reclaiming a multi-megabyte buffer returns almost all of it.
pub(crate) const SEND_BUF_BASELINE: usize = 8 * 1024;

/// Engine-owned outbound send buffer: already-encoded bytes plus a send
/// cursor.
///
/// The cursor (`sent`) records how many leading bytes the transport has
/// accepted. The not-yet-accepted tail is [`pending`](Self::pending); the
/// buffer is [`is_drained`](Self::is_drained) when the cursor reaches the
/// end. Because the buffer is owned by the caller and not by the flush
/// future, the cursor survives a mid-drain cancellation — see the module
/// docs.
///
/// # Invariants
///
/// - `sent <= buf.len()` at all times. [`advance`](Self::advance) is the only
///   method that moves the cursor, and it rejects any step that would pass
///   the end with a classified [`SendOverrun`] rather than wrapping or
///   panicking.
pub struct SendBuf {
    /// Already-encoded outbound bytes awaiting the socket. Growable on
    /// purpose: a hard batch ceiling is a verb-layer concern (see module
    /// docs), not a property of this cursor.
    buf: Vec<u8>,
    /// Send cursor: bytes in `buf[..sent]` the transport has confirmed
    /// accepted. `buf[sent..]` is the pending tail.
    sent: usize,
}

impl Default for SendBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl SendBuf {
    /// Construct an empty send buffer.
    ///
    /// `const fn` and allocation-free: the backing `Vec` is created empty and
    /// first allocates only when bytes are [`enqueue`](Self::enqueue)d.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buf: Vec::new(),
            sent: 0,
        }
    }

    /// Append already-encoded frame bytes to the outbound queue.
    ///
    /// The bytes join the pending tail in order. Growable: the queue has no
    /// hard ceiling here (see the module docs — bounding is a verb-layer
    /// concern). After a [`reset`](Self::reset) the backing capacity is
    /// retained, so a follow-on batch that fits reuses it with no
    /// reallocation.
    #[inline]
    pub fn enqueue(&mut self, frame: &[u8]) {
        self.buf.extend_from_slice(frame);
    }

    /// Borrow the not-yet-accepted tail — the bytes a [`flush`] still has to
    /// hand to the transport.
    ///
    /// Equal to `buf[sent..]`. Empty exactly when [`is_drained`](Self::is_drained).
    #[inline]
    #[must_use]
    pub fn pending(&self) -> &[u8] {
        // `sent <= buf.len()` by the type invariant, so `Some` is the live
        // arm; the `None` arm is structurally dead and present only to keep
        // the accessor free of an unchecked slice index.
        match self.buf.get(self.sent..) {
            Some(tail) => tail,
            None => &[],
        }
    }

    /// Number of pending (not-yet-accepted) bytes.
    #[inline]
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.buf.len().saturating_sub(self.sent)
    }

    /// Whether the whole queued batch has been accepted by the transport.
    #[inline]
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.sent == self.buf.len()
    }

    /// Begin a new `CopyData` ('d') frame: appends the `'d'` message tag followed by
    /// a 4-byte placeholder for the self-inclusive length.
    /// Returns the buffer offset where the 4-byte length prefix begins.
    #[inline]
    pub fn begin_copy_frame(&mut self) -> usize {
        self.buf.push(crate::wire::TAG_COPY_DATA.byte());
        let offset = self.buf.len();
        self.buf.extend_from_slice(&[0, 0, 0, 0]);
        offset
    }

    /// Seal an active `CopyData` frame started at `len_offset`: back-patches the
    /// self-inclusive 4-byte length (counting the 4 length bytes plus all body bytes).
    #[inline]
    pub fn seal_copy_frame(&mut self, len_offset: usize) -> Result<(), WriteBufFull> {
        let len = self.buf.len().saturating_sub(len_offset);
        let len_u32 = u32::try_from(len).map_err(|_| WriteBufFull)?;
        let end = len_offset.checked_add(4).ok_or(WriteBufFull)?;
        let slot = self.buf.get_mut(len_offset..end).ok_or(WriteBufFull)?;
        slot.copy_from_slice(&len_u32.to_be_bytes());
        Ok(())
    }

    /// The whole queued region (`buf[..len]`), independent of the send cursor —
    /// the bytes physically resident in the live part of the backing store.
    ///
    /// Test-only probe: it exposes the live queued bytes (where a flushed-but-
    /// not-scrubbed handshake wire — incl. the SCRAM proof — would still sit,
    /// since [`pending`](Self::pending) skips the already-sent prefix) so a test
    /// can assert the secret is gone after [`scrub_drained`](Self::scrub_drained).
    /// The Vec's zeroized spare beyond `len` is not safely readable (it is
    /// formally uninitialized to `Vec`), so the probe covers the live region; the
    /// spare scrub is covered by the zeroize-coverage manifest.
    #[cfg(test)]
    #[inline]
    pub(crate) fn queued(&self) -> &[u8] {
        &self.buf
    }

    /// The backing allocation's capacity — test-only, to assert the
    /// capacity-retaining property of [`scrub_drained`](Self::scrub_drained) /
    /// [`reset`](Self::reset).
    #[cfg(test)]
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Commit `n` bytes the transport just accepted, advancing the send
    /// cursor.
    ///
    /// `n` is the count returned by one
    /// [`Transport::write`](super::seams::Transport::write); by that seam's
    /// contract `n <= pending().len()`. A larger `n` (a transport that
    /// claimed to accept more than it was offered) would push the cursor past
    /// the end of the buffer — a contract violation, reported as a classified
    /// [`SendOverrun`] rather than silently wrapping or saturating.
    #[inline]
    pub fn advance(&mut self, n: usize) -> Result<(), SendOverrun> {
        let pending = self.pending_len();
        if n > pending {
            core::hint::cold_path();
            return Err(SendOverrun {
                committed: n,
                pending,
            });
        }
        // `sent + n <= buf.len() <= isize::MAX`, so the add cannot overflow;
        // the dead arm avoids unchecked arithmetic for the forbid wall.
        self.sent = match self.sent.checked_add(n) {
            Some(v) => v,
            None => {
                core::hint::cold_path();
                return Err(SendOverrun {
                    committed: n,
                    pending,
                });
            }
        };
        Ok(())
    }

    /// Scrub the **entire backing store** and empty the buffer, retaining the
    /// allocated capacity for the next batch.
    ///
    /// Zeroizes the full capacity (the queued bytes — which after a handshake
    /// include the SCRAM client proof and the password message — plus any spare),
    /// then truncates the length to zero and rewinds the cursor. Unlike the
    /// teardown [`Drop`] scrub, this runs *mid-lifetime*: it is the prompt scrub
    /// that erases the handshake's secret-bearing wire at handshake completion so
    /// it does not linger in the buffer for the rest of the connection, while
    /// keeping the allocation so the active phase reuses it with no realloc.
    ///
    /// # Precondition
    ///
    /// Only valid when the buffer is [`is_drained`](Self::is_drained): it scrubs
    /// the whole backing, so any **unsent** tail would be lost. The sole caller
    /// invokes it at the handshake-ready boundary, where the last outbound frame
    /// has already been flushed; a `debug_assert` pins that invariant in debug
    /// builds (it is a programming-error check, not a runtime fallback — there is
    /// no recovery branch).
    #[inline]
    pub(crate) fn scrub_drained(&mut self) {
        debug_assert!(
            self.is_drained(),
            "scrub_drained called with an unsent tail; it scrubs the whole backing and would lose those bytes",
        );
        use zeroize::Zeroize;
        // Zero the full capacity (queued secret bytes + spare), then drop the
        // length and rewind the cursor. `clear` keeps the allocation.
        self.buf.zeroize();
        self.buf.clear();
        self.sent = 0;
    }

    /// Drop the already-sent prefix and rewind the cursor, retaining the
    /// backing capacity for the next batch.
    ///
    /// Lossless: any not-yet-sent tail is preserved at the front of the
    /// buffer (a bounded memmove, never a per-call zero-fill). After a full
    /// drain the tail is empty and this empties the buffer. The backing
    /// allocation is kept, so the next [`enqueue`](Self::enqueue) of a batch
    /// that fits reuses it with no reallocation — the steady-state zero-alloc
    /// property the flush path relies on.
    ///
    /// Note the truncated bytes (the just-sent prefix) remain resident in the
    /// spare capacity until `Drop` scrubs the full capacity; mid-life they are
    /// owned, not freed.
    #[inline]
    pub fn reset(&mut self) {
        if self.sent == 0 {
            return;
        }
        let len = self.buf.len();
        let keep = len.saturating_sub(self.sent);
        // Move the unsent tail `buf[sent..len]` to the front. `copy_within`
        // is a bounded memmove (relocates bytes; never writes a constant
        // across a region), so this is no realloc and no zero-fill.
        self.buf.copy_within(self.sent..len, 0);
        // Discard the now-stale trailing bytes; `truncate` shrinks the length
        // but never the capacity.
        self.buf.truncate(keep);
        self.sent = 0;
    }

    /// Split the LAST `n` pending (staged-but-unsent) bytes off the tail,
    /// returning them and leaving them un-staged — the un-stage primitive the
    /// windowed batch drive uses to ISOLATE an oversize command from a co-window
    /// prefix (see the driver's `isolate_prefix`).
    ///
    /// The batch drive stages one command, then measures whether that command's
    /// own frame (`n` bytes) would co-window with a large-result prefix into a
    /// write-path deadlock. When it would, the command's bytes are the tail `n`
    /// pending bytes (staging appends contiguously, so the just-staged command is
    /// exactly the pending tail): this lifts them OUT of the buffer so the prefix
    /// can be flushed + drained alone, then the returned bytes are re-staged (via
    /// [`enqueue`](Self::enqueue)) into a FRESH window where the command is
    /// isolated. The command's engine-side state (a pipeline MISS's guard-OID FIFO
    /// push, its seat) is UNTOUCHED — only its already-encoded WIRE BYTES move —
    /// so the receive FSM still guards / decodes it correctly when the isolated
    /// window is drained.
    ///
    /// # Precondition
    ///
    /// `n <= pending_len()`: only UNSENT bytes may be lifted (splitting into the
    /// already-`sent` prefix would push `sent` past the buffer end and corrupt the
    /// cursor). A `debug_assert` pins it; in release the split point is clamped to
    /// the send cursor so the invariant `sent <= buf.len()` holds unconditionally
    /// (the sole caller always passes a just-measured `n <= pending_len()`, so the
    /// clamp is never reached — a fail-safe, not a fallback path).
    #[inline]
    #[must_use]
    pub fn split_off_pending(&mut self, n: usize) -> Vec<u8> {
        debug_assert!(
            n <= self.pending_len(),
            "split_off_pending would lift already-sent bytes (n > pending_len)",
        );
        // `buf.len() - n`, clamped to never dip below `sent` (fail-safe for the
        // debug-asserted precondition). `saturating_sub` keeps the forbid wall
        // (`arithmetic_side_effects`) satisfied; `.max(sent)` guarantees the split
        // point never lifts a sent byte, so `sent <= buf.len()` still holds.
        let at = self.buf.len().saturating_sub(n).max(self.sent);
        // `Vec::split_off` returns `buf[at..]` as a fresh Vec and truncates `buf`
        // to `at` (never below `sent`, so the cursor stays valid).
        self.buf.split_off(at)
    }

    /// Empty the buffer, DISCARDING any not-yet-sent bytes, retaining the
    /// backing capacity for the next batch.
    ///
    /// Unlike [`reset`](Self::reset) — which PRESERVES the unsent tail (moving it
    /// to the front) — this drops it. The sole caller is COPY-in abort: a
    /// `CopyFail` aborts the whole COPY, so any accumulated-but-unflushed
    /// `CopyData` is moot and must NOT be sent — the server would only discard it,
    /// and sending it risks the server erroring on a stale buffered row instead of
    /// echoing the caller's abort reason. The discarded bytes are the
    /// application's own COPY payload, never auth material; like
    /// [`reset`](Self::reset) this does not scrub mid-life (the teardown [`Drop`]
    /// scrubs the full capacity). `clear` keeps the allocation, so the next batch
    /// reuses it with no reallocation.
    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear();
        self.sent = 0;
    }

    /// Release an OVERSIZED backing allocation down to a small baseline,
    /// scrubbing the freed bytes first — the bounded-pool-memory reclaim.
    ///
    /// A large `Bind` parameter block grows [`SendBuf`] uncapped and that
    /// capacity is otherwise retained for the connection's whole life (see the
    /// module docs). This reclaims it: when the backing capacity exceeds
    /// [`SEND_BUF_HIGH_WATER`] it is dropped to [`SEND_BUF_BASELINE`].
    ///
    /// # Thrash-free by construction
    ///
    /// The reclaim is a COLD, once-per-checkout op (its sole caller is the
    /// driver's `reset_session`, run when a pooled connection is handed back
    /// out) — NEVER the hot per-query / per-flush path, so a per-query
    /// shrink-then-regrow cannot happen. And a normal small-query workload never
    /// crosses [`SEND_BUF_HIGH_WATER`], so the gate below returns after a single
    /// capacity compare — no scrub, no realloc, no shrink at all. Only a
    /// connection that actually sent an oversized payload pays the reclaim, once,
    /// at a boundary that already costs a reset round trip.
    ///
    /// # Scrub before shrink
    ///
    /// [`Vec::shrink_to`] reallocates to a smaller block and FREES the old,
    /// oversized one, which held prior outbound wire bytes (the SQL text, and
    /// after the handshake the SCRAM client proof / password message). The
    /// teardown [`Drop`] only scrubs whatever backing is live AT drop, so this
    /// mid-life shrink must zeroize its OWN freed block — a shrink without the
    /// prior scrub would leak those bytes into freed heap. The full capacity is
    /// zeroized before the realloc.
    ///
    /// # Precondition
    ///
    /// Only valid when the buffer is [`is_drained`](Self::is_drained): it scrubs
    /// and reallocates the whole backing, so any UNSENT tail would be lost. The
    /// sole caller invokes it after its reset round trip has fully drained the
    /// buffer; a `debug_assert` pins the invariant (a programming-error check, no
    /// runtime recovery branch).
    #[inline]
    pub(crate) fn reclaim_if_oversized(&mut self) {
        // Fast gate: a buffer that never grew past the high-water mark is not
        // worth reclaiming. This is the ONLY work a normal small-query workload
        // ever does here — one capacity compare, then return — so the reclaim is
        // zero-realloc / zero-thrash on the common path.
        if self.buf.capacity() <= SEND_BUF_HIGH_WATER {
            return;
        }
        debug_assert!(
            self.is_drained(),
            "reclaim_if_oversized called with an unsent tail; it scrubs and \
             reallocates the whole backing and would lose those bytes",
        );
        use zeroize::Zeroize;
        // Scrub the FULL capacity (queued bytes + spare) BEFORE `shrink_to`
        // frees the oversized allocation — see "Scrub before shrink" above.
        self.buf.zeroize();
        self.buf.clear();
        self.sent = 0;
        // Drop the oversized allocation to the baseline the next batch reuses
        // without a realloc. `shrink_to` never grows and never panics; with the
        // length now `0`, the new capacity settles at `>= SEND_BUF_BASELINE`.
        self.buf.shrink_to(SEND_BUF_BASELINE);
    }

    /// Borrow the backing store as a GROWABLE frame builder — the streaming
    /// peer of the bounded [`WriteBuf`](crate::write_buf::WriteBuf).
    ///
    /// The returned [`SendFrame`] appends onto the same buffer
    /// [`enqueue`](Self::enqueue) does (so the built frame joins the pending
    /// tail in order), but exposes the PG-wire push primitives with BACK-PATCHED
    /// length prefixes, so a frame whose total length is only known after its
    /// body is encoded — the `Bind`, whose parameter block is unbounded — can be
    /// assembled in place without an intermediate bounded buffer or a size
    /// pre-pass. The view holds no cursor of its own; it only appends, exactly
    /// like `enqueue`, so a mid-build drop leaves the buffer's send cursor
    /// untouched (the partial frame is simply unsent bytes a later `reset`
    /// reclaims). Growable on purpose: bounding a single pipelined frame is the
    /// verb layer's concern, not this cursor's (see the module docs).
    #[inline]
    pub(crate) fn frame(&mut self) -> SendFrame<'_> {
        SendFrame { buf: &mut self.buf }
    }
}

/// A GROWABLE [`crate::write_buf::FrameSink`] over a [`SendBuf`]'s backing
/// store — the streaming counterpart of the bounded
/// [`WriteBuf`](crate::write_buf::WriteBuf).
///
/// Minted by [`SendBuf::frame`]. Every push appends to the borrowed buffer
/// (reusing its warm capacity — a small-parameter Bind on a warm buffer
/// allocates nothing), and the two length-prefix helpers reserve a 4-byte
/// placeholder, run the body, then back-patch it with the final count. Because
/// the placeholder is patched by ABSOLUTE index (not a raw pointer), a body
/// that reallocates the buffer mid-build leaves the offset valid — the reason a
/// growable back-patch is sound where a pointer-based one would dangle.
///
/// The only failure is a frame body exceeding the `u32` / `i32` wire length
/// field (> 2 GiB — architecturally dead, a process would exhaust memory
/// first), surfaced as the crate's [`WriteBufFull`](crate::write_buf::WriteBufFull)
/// "outbound frame too long" sentinel, classified never panicked — the same one
/// the streamed simple-query / Parse / `CopyData` headers already return.
#[derive(Debug)]
pub(crate) struct SendFrame<'a> {
    /// The [`SendBuf`]'s backing store, borrowed for in-place frame assembly.
    buf: &'a mut Vec<u8>,
}

impl crate::write_buf::frame_sink_sealed::Sealed for SendFrame<'_> {}

impl crate::write_buf::FrameSink for SendFrame<'_> {
    #[inline]
    fn push_u8(&mut self, byte: u8) -> Result<(), WriteBufFull> {
        self.buf.push(byte);
        Ok(())
    }
    #[inline]
    fn push_bytes(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(data);
        Ok(())
    }
    #[inline]
    fn push_i16_be(&mut self, val: i16) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }
    #[inline]
    fn push_u16_be(&mut self, val: u16) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }
    #[inline]
    fn push_i32_be(&mut self, val: i32) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }
    #[inline]
    fn push_u32_be(&mut self, val: u32) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }
    #[inline]
    fn push_i64_be(&mut self, val: i64) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }
    #[inline]
    fn push_nul_terminated(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.buf.extend_from_slice(data);
        self.buf.push(0);
        Ok(())
    }
    #[inline]
    fn with_length_prefix<F>(&mut self, body: F) -> Result<(), WriteBufFull>
    where
        F: FnOnce(&mut Self) -> Result<(), WriteBufFull>,
    {
        // Self-inclusive length (PG convention: the 4-byte field counts itself).
        let start = self.buf.len();
        self.buf.extend_from_slice(&[0, 0, 0, 0]); // placeholder
        body(self)?;
        let body_len = self.buf.len().saturating_sub(start);
        let len = u32::try_from(body_len).map_err(|_| WriteBufFull)?;
        // Patch by ABSOLUTE index via `get_mut` + `first_chunk_mut` — no
        // unchecked slice index (the forbid wall), and valid even if `body`
        // reallocated the buffer. The `push_u32_be(0)` placeholder guarantees
        // `buf.len() >= start + 4`, so the `None` arm is structurally dead;
        // converting it to an explicit `Err` means a future refactor that
        // dropped the placeholder fails loudly rather than emitting a zero
        // length field.
        let Some(slot) = self.buf.get_mut(start..).and_then(|s| s.first_chunk_mut::<4>()) else {
            return Err(WriteBufFull);
        };
        *slot = len.to_be_bytes();
        Ok(())
    }
    #[inline]
    fn with_i32_length_prefixed_body<F>(&mut self, body: F) -> Result<(), WriteBufFull>
    where
        F: FnOnce(&mut Self) -> Result<(), WriteBufFull>,
    {
        // Body-only length (PG Bind per-param: the i32 counts only the value
        // bytes, not the length field itself).
        let len_offset = self.buf.len();
        self.buf.extend_from_slice(&[0, 0, 0, 0]); // placeholder
        let body_start = self.buf.len();
        body(self)?;
        let body_len = self.buf.len().saturating_sub(body_start);
        let body_len_i32 = i32::try_from(body_len).map_err(|_| WriteBufFull)?;
        let Some(slot) = self
            .buf
            .get_mut(len_offset..)
            .and_then(|s| s.first_chunk_mut::<4>())
        else {
            return Err(WriteBufFull);
        };
        *slot = body_len_i32.to_be_bytes();
        Ok(())
    }
}

/// Scrub the backing store on teardown.
///
/// Zeroizes the **full capacity** (live elements plus spare), because
/// [`reset`](SendBuf::reset) leaves already-sent secret bytes resident in the
/// spare capacity that a length-only scrub would miss. `Vec::zeroize` zeroes
/// the spare via `spare_capacity_mut`. This is the only scrub on the buffer's
/// lifetime — there is no per-batch scrub, since the cursor, not
/// re-initialisation, tracks what is live.
impl Drop for SendBuf {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.buf.zeroize();
    }
}

impl fmt::Debug for SendBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately never prints the byte contents (outbound payloads can
        // be large and carry auth material) — only the cursor geometry.
        f.debug_struct("SendBuf")
            .field("sent", &self.sent)
            .field("queued", &self.buf.len())
            .field("pending", &self.pending_len())
            .field("capacity", &self.buf.capacity())
            .finish()
    }
}

crate::wire_pin!(SendBuf, size = 32, align = 8);

/// Returned by [`SendBuf::advance`] when the committed count would push the
/// send cursor past the end of the queued bytes — a transport that reported
/// accepting more bytes than it was offered.
///
/// Surfaced to a [`flush`] caller as
/// [`EngineError::SendOverrun`](super::EngineError::SendOverrun).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendOverrun {
    /// Bytes the caller tried to commit.
    pub committed: usize,
    /// Bytes that were actually pending (the maximum a single commit may
    /// advance).
    pub pending: usize,
}

impl core::error::Error for SendOverrun {}

impl fmt::Display for SendOverrun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "send cursor overrun: tried to commit {} bytes, only {} were pending",
            self.committed, self.pending,
        )
    }
}

// Two `usize` detail fields (committed, pending) → 16 B.
crate::wire_pin!(SendOverrun, size = 16, align = 8);

/// Drain `send_buf` to the transport: the engine-owned, cancellation-safe
/// flush loop.
///
/// Runs one [`Transport::write`](super::seams::Transport::write) attempt per
/// iteration over the buffer's [`pending`](SendBuf::pending) tail and commits
/// the accepted count synchronously, until the buffer
/// [`is_drained`](SendBuf::is_drained); then drives the transport's own
/// internal buffer to the socket with a single
/// [`Transport::flush`](super::seams::Transport::flush). The only suspension
/// point in the drain loop is inside `write`; the cursor advance has no
/// `.await` before it, so dropping this future at the loop's suspension point
/// leaves `send_buf` consistent and a re-entered `flush` resumes exactly where
/// the socket left off.
///
/// # Errors
///
/// - [`EngineError::Transport`](super::EngineError::Transport) — the
///   transport reported a write or flush failure (its own error, carried
///   verbatim).
/// - [`EngineError::WriteZero`](super::EngineError::WriteZero) — the
///   transport accepted zero bytes from a non-empty buffer. The loop only
///   ever calls `write` with a non-empty tail (a drained buffer ends the
///   loop), so `Ok(0)` can only mean a stalled/broken transport; classifying
///   it as an error is the only no-fallback choice — looping would spin
///   forever and skipping would silently drop bytes.
/// - [`EngineError::SendOverrun`](super::EngineError::SendOverrun) — the
///   transport reported accepting more bytes than it was offered (a contract
///   violation), caught by [`SendBuf::advance`].
pub async fn flush<T: Transport>(
    send_buf: &mut SendBuf,
    transport: &mut T,
) -> Result<(), EngineError<T::Error>> {
    while !send_buf.is_drained() {
        // The loop's sole suspension point. `pending()` is non-empty here
        // (the loop guard rules out a drained buffer). A single
        // `Transport::write` is atomic by cancellation: `Ready(Ok(n))` means
        // exactly `n` bytes committed to the socket, `Pending` means zero.
        let n = transport
            .write(send_buf.pending())
            .await
            .map_err(EngineError::Transport)?;
        if n == 0 {
            // Non-empty tail, zero accepted: the transport is stalled. Never
            // loop on it and never skip the bytes.
            core::hint::cold_path();
            return Err(EngineError::WriteZero);
        }
        // Synchronous commit, immediately after the write resolves — there is
        // NO suspension between the resolved write and this advance, which is
        // what makes a mid-drain cancellation unroll cleanly.
        send_buf.advance(n).map_err(EngineError::SendOverrun)?;
    }
    // Drained into the transport; drive any transport-internal buffered bytes
    // (a TLS record) to the socket so a partial wire frame is not left
    // dangling. Reached only after the buffer is drained, so it is irrelevant
    // to the mid-drain cursor unroll.
    transport.flush().await.map_err(EngineError::Transport)?;
    Ok(())
}

/// Stream a borrowed byte slice straight to the transport, bypassing
/// [`SendBuf`] entirely — the COPY-in large-chunk passthrough.
///
/// A COPY-in chunk at or above the batching threshold is written DIRECTLY from
/// the caller's borrowed slice rather than copied into [`SendBuf`], so even a
/// gigabyte chunk costs no buffer growth: the constant-memory invariant holds
/// for arbitrarily large chunks, not just small rows. One
/// [`Transport::write`](super::seams::Transport::write) attempt runs per
/// iteration and advances a LOCAL cursor (a sub-slice reborrow) SYNCHRONOUSLY —
/// no `.await` between the resolved write and the advance — then a single
/// [`Transport::flush`](super::seams::Transport::flush) drives any
/// transport-internal buffer to the socket. The same drain shape as [`flush`],
/// over a borrowed slice instead of the owned buffer.
///
/// # Cancellation
///
/// Unlike [`flush`], the cursor is LOCAL to this future — a borrowed per-call
/// slice cannot carry an engine-owned resume cursor across calls. A drop
/// mid-write therefore does not resume; but the only caller (`copy_in_write`,
/// invoked inside `copy_in_with`) sends NO terminal frame on a drop, abandoning
/// the connection, so there is nothing to resume onto: the whole-copy drop
/// contract already leaves a mid-stream drop's connection dead. Within a single
/// uncancelled call the synchronous advance still guarantees exactly-once byte
/// accounting.
///
/// # Errors
///
/// As [`flush`]: [`EngineError::Transport`](super::EngineError::Transport) on a
/// write/flush fault, [`EngineError::WriteZero`](super::EngineError::WriteZero)
/// if the transport accepts zero from a non-empty slice, and
/// [`EngineError::SendOverrun`](super::EngineError::SendOverrun) if it claims to
/// accept more than it was offered.
pub async fn write_all<T: Transport>(
    transport: &mut T,
    mut bytes: &[u8],
) -> Result<(), EngineError<T::Error>> {
    while !bytes.is_empty() {
        // The sole suspension point, mirroring `flush`'s loop: a single
        // `Transport::write` attempt over the remaining borrowed tail.
        let n = transport
            .write(bytes)
            .await
            .map_err(EngineError::Transport)?;
        if n == 0 {
            // Non-empty slice, zero accepted: a stalled transport. Never loop on
            // it and never skip the bytes.
            core::hint::cold_path();
            return Err(EngineError::WriteZero);
        }
        // Synchronous advance immediately after the write resolves — the same
        // no-`.await`-between-write-and-advance adjacency `flush` keeps.
        let pending = bytes.len();
        bytes = match bytes.get(n..) {
            Some(rest) => rest,
            None => {
                // The transport claimed more than it was offered — a contract
                // violation, classified rather than allowed to wrap/panic.
                core::hint::cold_path();
                return Err(EngineError::SendOverrun(SendOverrun {
                    committed: n,
                    pending,
                }));
            }
        };
    }
    // Drive any transport-internal buffered bytes (a TLS record) to the socket,
    // exactly as `flush` does after draining the owned buffer.
    transport.flush().await.map_err(EngineError::Transport)?;
    Ok(())
}

#[cfg(test)]
mod scrub_drained_tests {
    //! Mid-lifetime scrub contract for [`SendBuf::scrub_drained`]: it empties the
    //! queued region (so a flushed secret-bearing wire does not linger) and
    //! retains the backing allocation (so the next phase reuses it). The
    //! byte-level full-capacity zeroize is the same `Vec::zeroize` the teardown
    //! `Drop` runs (covered by the zeroize-coverage manifest); the Vec's spare
    //! beyond `len` is not safely readable, so this asserts the live region is
    //! cleared and the allocation kept. The connect call-site teeth (the scrub
    //! actually runs at handshake completion) live in `engine::connect_scrub_tests`.

    use super::SendBuf;

    #[test]
    fn scrub_drained_empties_queued_region_and_retains_capacity() {
        let mut sb = SendBuf::new();
        let secret = b"scram-client-proof-and-password-message-bytes";
        sb.enqueue(secret);
        // The queued region holds the secret before the scrub.
        assert_eq!(sb.queued(), secret);
        // Mark fully sent so the drained precondition holds.
        let pending = sb.pending_len();
        assert!(sb.advance(pending).is_ok());
        assert!(sb.is_drained());
        let cap = sb.capacity();

        sb.scrub_drained();

        // The secret-bearing queued region is gone, the buffer is empty and
        // drained, and the allocation is retained for reuse.
        assert!(sb.queued().is_empty(), "queued region not cleared");
        assert!(sb.pending().is_empty());
        assert!(sb.is_drained());
        assert_eq!(sb.capacity(), cap, "capacity not retained across scrub");

        // The retained allocation accepts a follow-on batch.
        sb.enqueue(b"next-phase-frame");
        assert_eq!(sb.queued(), b"next-phase-frame");
    }
}

#[cfg(test)]
mod split_off_pending_tests {
    //! Un-stage contract for [`SendBuf::split_off_pending`]: it lifts the LAST `n`
    //! pending bytes off the tail (the oversize command's just-staged frame), leaves
    //! the prefix pending, and NEVER touches the already-`sent` cursor — the driver's
    //! `isolate_prefix` relies on all three to relocate an oversize command's bytes
    //! into a fresh window while flushing the prefix alone.

    use super::SendBuf;

    #[test]
    fn splits_the_pending_tail_and_leaves_the_prefix() {
        let mut sb = SendBuf::new();
        sb.enqueue(b"PREFIX"); // the co-window prefix (6 B)
        sb.enqueue(b"OVERSIZE-COMMAND-FRAME"); // the just-staged oversize command (22 B)
        assert_eq!(sb.pending_len(), 28);

        // Lift the oversize command's 22 tail bytes back out (un-stage it).
        let lifted = sb.split_off_pending(22);

        assert_eq!(lifted, b"OVERSIZE-COMMAND-FRAME", "the tail bytes are returned");
        assert_eq!(sb.pending(), b"PREFIX", "only the prefix remains pending");
        assert_eq!(sb.pending_len(), 6);

        // Re-stage the lifted bytes (the isolate's fresh window) — byte-identical.
        sb.enqueue(&lifted);
        assert_eq!(sb.pending(), b"PREFIXOVERSIZE-COMMAND-FRAME");
    }

    #[test]
    fn never_lifts_already_sent_bytes_across_a_prior_window() {
        // Mimic a MULTI-WINDOW batch: a prior window's bytes were flushed (sent), then
        // this window staged a prefix + an oversize command onto the SAME buffer.
        let mut sb = SendBuf::new();
        sb.enqueue(b"WINDOW0"); // 7 B, already flushed below
        assert!(sb.advance(7).is_ok(), "flush window 0");
        assert!(sb.is_drained());
        sb.enqueue(b"P1"); // window 1 prefix (2 B, unsent)
        sb.enqueue(b"OVERSIZE1"); // window 1 oversize command (9 B, unsent)
        assert_eq!(sb.pending_len(), 11, "only window 1 is pending");

        let lifted = sb.split_off_pending(9);

        assert_eq!(lifted, b"OVERSIZE1");
        // The prefix is the ONLY pending remainder; the already-sent WINDOW0 prefix is
        // untouched, so `pending()` (buf[sent..]) is exactly the window-1 prefix.
        assert_eq!(sb.pending(), b"P1", "sent cursor preserved — no sent byte lifted");
        assert_eq!(sb.pending_len(), 2);
    }
}

#[cfg(test)]
mod reclaim_tests {
    //! Bounded-pool-memory witness for [`SendBuf::reclaim_if_oversized`]: a
    //! backing that grew past [`SEND_BUF_HIGH_WATER`] is dropped to
    //! [`SEND_BUF_BASELINE`] (memory reclaimed), while a normal small-query
    //! buffer is left UNTOUCHED across any number of reclaims (no thrash). This
    //! is the deterministic, in-process witness for the P2 shrink-on-checkin
    //! reclaim — the buffer capacity is directly observable via the `cfg(test)`
    //! [`SendBuf::capacity`] probe, so no live server is needed. The wiring —
    //! that `reset_session` calls the reclaim — is witnessed at the engine level
    //! by `super::super::reclaim_send_buffer_engine_tests`.

    use super::{SendBuf, SEND_BUF_BASELINE, SEND_BUF_HIGH_WATER};

    /// An oversized buffer (a big `Bind` param block) is reclaimed to the
    /// baseline on a drained settle point — the memory returns to the allocator.
    /// A 4 MiB stand-in for a large `Bind` parameter payload.
    const BIG_PAYLOAD_LEN: usize = 4 * 1024 * 1024;

    #[test]
    fn oversized_buffer_is_reclaimed_to_baseline() {
        let mut sb = SendBuf::new();
        // Simulate a multi-megabyte parameter payload growing the backing.
        let mut big = alloc::vec::Vec::new();
        big.resize(BIG_PAYLOAD_LEN, 0xABu8);
        sb.enqueue(&big);
        // Fully "send" it so the drained precondition holds (a completed verb
        // leaves `sent == len`).
        let pending = sb.pending_len();
        assert!(sb.advance(pending).is_ok());
        assert!(sb.is_drained());
        let grown = sb.capacity();
        assert!(
            grown > SEND_BUF_HIGH_WATER,
            "precondition: the 4 MiB payload must have grown the backing past the high-water mark (was {grown})",
        );

        sb.reclaim_if_oversized();

        let reclaimed = sb.capacity();
        assert!(
            reclaimed < grown,
            "reclaim must shrink the oversized backing ({grown} -> {reclaimed})",
        );
        assert!(
            reclaimed <= SEND_BUF_HIGH_WATER,
            "reclaimed capacity {reclaimed} must be back under the high-water mark",
        );
        // The buffer is empty + drained + reusable after the reclaim.
        assert!(sb.is_drained());
        assert!(sb.pending().is_empty());
        // A normal follow-on batch reuses the baseline allocation without a
        // realloc (the baseline is sized to hold it).
        let after = sb.capacity();
        sb.enqueue(b"SELECT 1");
        assert_eq!(sb.capacity(), after, "a small batch must reuse the baseline, not realloc");
    }

    /// A normal small-query buffer never crosses the high-water mark, so the
    /// reclaim leaves it UNTOUCHED — the no-thrash guarantee — even when called
    /// repeatedly (a small-query loop returning to the pool each cycle).
    #[test]
    fn small_query_buffer_never_shrinks_no_thrash() {
        let mut sb = SendBuf::new();
        // A representative small request (a typical parameterized query batch).
        sb.enqueue(b"SELECT id, name FROM users WHERE id = $1");
        let pending = sb.pending_len();
        assert!(sb.advance(pending).is_ok());
        assert!(sb.is_drained());
        let cap = sb.capacity();
        assert!(
            cap <= SEND_BUF_HIGH_WATER,
            "a normal small-query buffer must stay under the high-water mark (was {cap})",
        );

        // Repeated reclaims (each pool checkout) must be a no-op on capacity —
        // never a shrink-then-regrow cycle.
        for _ in 0..16 {
            sb.reclaim_if_oversized();
            assert_eq!(
                sb.capacity(),
                cap,
                "reclaim shrank a small buffer — a thrash the gate must prevent",
            );
        }
    }

    /// The reclaim baseline is at least large enough for the reset round trip's
    /// own `RESET ALL` simple query, so the reclaim never forces the very next
    /// (reset-internal) frame to realloc.
    #[test]
    fn baseline_holds_a_reset_all_simple_query() {
        const RESET: &[u8] = b"ROLLBACK; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; \
             CLOSE ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        assert!(
            RESET.len() < SEND_BUF_BASELINE,
            "the reclaim baseline ({SEND_BUF_BASELINE}) must comfortably hold the RESET simple query ({} bytes)",
            RESET.len(),
        );
    }
}

#[cfg(test)]
mod drop_witness_tests {
    //! Drop-fire witness for [`SendBuf`] via the crate-internal
    //! [`crate::drop_witness::DropCounter`]. `DropCounter` / `DropProbe` are
    //! `pub(crate)`, so this witness lives in `src`; behavioural coverage of
    //! the send API lives in the `engine_flush_spec` integration test.

    use super::SendBuf;
    use crate::drop_witness::{DropCounter, DropProbe};

    /// `SendBuf::drop` fires its zeroize scrub exactly once — the counterpart
    /// to its manifest registration in `drop_witness.rs`.
    #[test]
    fn send_buf_drop_fires_zeroize() {
        let probe = DropProbe::new();
        let mut buf = SendBuf::new();
        // Enqueue bytes so the scrub has live contents (and, after a partial
        // advance + reset elsewhere, spare-capacity residue) to clear.
        buf.enqueue(b"outbound secret bytes");
        DropCounter::scoped(buf, probe.clone(), || {
            assert_eq!(probe.fired(), 0);
        });
        assert_eq!(probe.fired(), 1, "SendBuf drop must fire exactly once");
    }
}
