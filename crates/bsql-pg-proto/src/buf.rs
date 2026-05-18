//! Sealed, bounded read buffer.
//!
//! `ReadBuf` wraps `heapless::Vec<u8, READ_BUF_CAP>` and exposes only
//! four operations: [`append`], [`unread`], [`advance`], [`clear`].
//! Methods that could panic on misuse (`insert`, `resize`, `drain`,
//! indexing) are physically absent from the API. This makes the
//! "buffer-OOB" class of bugs **STRUCTURALLY UNREACHABLE** (reforge.md
//! §3.2 / §16).
//!
//! The two error paths exposed:
//!
//! - [`append`] returns [`ReadBufFull`] when the bounded capacity is
//!   exceeded. The caller (the protocol's `feed_bytes`) classifies this
//!   as a fatal connection error and emits `CloseSocket`.
//! - [`advance`] returns [`AdvancePastEnd`] when asked to advance beyond
//!   the unread region. Inside the protocol the dispatcher only calls
//!   `advance(header.total_len)` after `parse_header` has confirmed the
//!   bytes are present, so this Err is dead in our code paths — but the
//!   public signature forces any future caller to handle it. Tier-1
//!   against silent corruption.
//!
//! [`append`]: ReadBuf::append
//! [`unread`]: ReadBuf::unread
//! [`advance`]: ReadBuf::advance
//! [`clear`]: ReadBuf::clear

use crate::frame::READ_BUF_CAP;
use core::fmt;
use heapless::{CapacityError, Vec};

/// Bounded byte buffer for inbound wire data — generic-form.
///
/// **DEF-199**: const-generic over capacity `N`. The default
/// [`ReadBuf`] type alias picks `N = READ_BUF_CAP` (4096) for backward
/// compat; bumping `N` lets callers handle larger frames (analytics
/// workloads with wide `RowDescription`, large `DataRow` payloads).
///
/// Beyond capacity, [`append`] returns [`ReadBufFull`] — the protocol
/// classifies this as a fatal connection error.
///
/// [`append`]: ReadBufN::append
pub struct ReadBufN<const N: usize> {
    /// Backing storage. Private — every public method preserves the
    /// invariants below.
    inner: Vec<u8, N>,
    /// Read cursor. Bytes in `inner[..cursor]` are consumed and may be
    /// reclaimed on the next [`compact`] call.
    ///
    /// Invariant: `cursor <= inner.len() <= N <= 65_535`
    /// (enforced by every mutator path + the const-block assert below).
    ///
    /// DEF-120: `u16` (not `usize`) — current N values fit
    /// with headroom; narrower type saves bytes per `ReadBuf`
    /// on 64-bit. The const-block in `new()` rejects `N > 65_535`
    /// at monomorph time.
    cursor: u16,
}

/// Inline-mode capacity for the two-tier [`ReadBuf`] introduced by
/// DEF-265 Idea-38 (2026-05-08). Frames ≤ 256 B stay in stack-inline
/// storage with full cache-locality (state + small_buf adjacent).
/// Frames > 256 B trigger a one-time lazy escape: the inline contents
/// copy into a heap-allocated 4096-byte storage, and subsequent
/// operations work against the heap. Once escaped, the buffer stays
/// in heap mode for the connection's lifetime (downgrading would
/// require copying back, no perf benefit).
///
/// **Why 256 B**: Postgres protocol frames break down by typical size:
/// - `Sync` (5 B), `Flush` (5 B), `Terminate` (5 B): always inline ✓
/// - `ReadyForQuery` (6 B), `ParseComplete` (5 B), `BindComplete` (5 B): ✓
/// - `RowDescription` (~14-200 B for typical column lists): usually ✓
/// - small `DataRow` (≤ 5 i32 columns ≈ 30-67 B): ✓
/// - `CommandComplete` ('SELECT N\0' tags): ✓
///
/// Workloads where inline-mode-stays-resident:
/// - PING / RFQ round-trip benches (`ping_round_trip`)
/// - small OLTP queries with single-row or small-row results
/// - SCRAM handshake frames (~500 B nonce — may escape on first frame)
///
/// Workloads that escape:
/// - analytics queries with multi-KB JSON / TEXT cells
/// - `iter_rows` benches feeding 100×row batch (~6700 B total)
/// - large `RowDescription` (wide tables)
const INLINE_BUF_CAP: usize = 256;

impl<const N: usize> Default for ReadBufN<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> ReadBufN<N> {
    /// Construct an empty buffer.
    ///
    /// **Tier-1 cap invariant** (DEF-199): the const-block fires at
    /// monomorphisation time. `ReadBufN<70_000>::new()` would compile
    /// without this assertion but break the `cursor: u16` invariant
    /// — any `advance()` past 65_535 bytes would trip a dead Err
    /// branch silently. The assertion makes any `N > u16::MAX` a
    /// **build failure**, not a runtime tier regression.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                N <= 65_535,
                "ReadBufN<N>: N must be ≤ u16::MAX (cursor is u16). \
                 Widen the cursor type before bumping N past 65_535.",
            );
        }
        Self {
            inner: Vec::new(),
            cursor: 0,
        }
    }

    /// Append `bytes` to the unread region.
    ///
    /// Returns [`ReadBufFull`] if the resulting length would exceed
    /// [`READ_BUF_CAP`] even after reclaiming the consumed prefix.
    ///
    /// **Lazy compaction (DEF-058).** We try to fit the incoming bytes
    /// into the tail first — `heapless::Vec::extend_from_slice` checks
    /// capacity before copying and returns `Err` without mutation, so
    /// we can safely retry after compacting. On the typical workload
    /// (8 KiB chunks on a 4 KiB buffer where previous frames have been
    /// consumed) this saves one `memmove` per `append` call whenever
    /// the tail already has room. Only when the tail is insufficient
    /// do we reclaim `[0..cursor)` and try again.
    #[inline]
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // Fast path: tail has room, no need to compact.
        if self.inner.extend_from_slice(bytes).is_ok() {
            return Ok(());
        }
        // Slow path: reclaim the consumed prefix and retry. If it
        // still does not fit, classify as a fatal buffer-full error.
        self.compact();
        self.inner
            .extend_from_slice(bytes)
            .map_err(|CapacityError { .. }| ReadBufFull {
                attempted: bytes.len(),
                available: N.saturating_sub(self.inner.len()),
                cap: N,
            })
    }

    /// Borrow the unread region.
    ///
    /// The returned slice is valid until the next `&mut self` method
    /// call on this buffer (`append`, `advance`, `clear`). The borrow
    /// checker enforces this — there is no way for the caller to
    /// invalidate the slice while still holding it.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        // SAFETY-style note (no `unsafe` involved): `self.cursor` is
        // maintained `<= self.inner.len()` by every mutator, so the
        // slice expression cannot index out of bounds. We use `get`
        // rather than `[..]` because the forbid-bundle bans
        // `indexing_slicing`. DEF-120: `u16 → usize` via infallible
        // widening `From` impl (no `as` cast).
        //
        // F-014 (pass-#8): debug-builds actively assert the invariant
        // `cursor <= inner.len()` so a mutator regression would fail
        // the test suite before the dead unwrap_or(&[]) fallback
        // masked it silently.
        debug_assert!(
            usize::from(self.cursor) <= self.inner.len(),
            "ReadBuf invariant: cursor ({}) must not exceed inner.len() ({})",
            self.cursor,
            self.inner.len(),
        );
        self.inner.get(usize::from(self.cursor)..).unwrap_or(&[])
    }

    /// Borrow the full populated region, including bytes already
    /// advanced past the cursor. Used by the `StreamRow` materialiser
    /// (1c-1b) which needs absolute-position slices into rows whose
    /// frames were advanced-past during the dispatch loop but whose
    /// bytes must remain valid until `OutActions` drops.
    ///
    /// # Lifetime invariant
    ///
    /// The returned slice is valid until the next `&mut self` method
    /// call on this buffer (`append`, `advance`, `clear`) — same as
    /// [`unread`]. Compaction happens lazily on the next `append`;
    /// by then no outstanding borrow can be alive (the borrow
    /// checker refuses the `&mut` call otherwise). Callers emit
    /// [`crate::action::Action::StreamRow`] with slices carved out
    /// of this region during [`crate::PgProtocol::feed_bytes`]; the
    /// `'r` lifetime on `OutActions<'w, 'r>` ties those slices back
    /// to the `&'r mut self` borrow on `PgProtocol`, which blocks
    /// the next `feed_bytes` call while they are alive.
    ///
    /// [`unread`]: ReadBuf::unread
    ///
    /// F-016 (pass-#8): visibility narrowed `pub` → `pub(crate)`.
    /// Only `materialise` / dispatch resolution need this view; an
    /// external caller reading `populated()` gets access to bytes
    /// already consumed past the cursor with no user benefit. Surface
    /// shrink closes a latent access hole.
    ///
    /// DEF-265 Idea-38 (2026-05-08): retained for completeness on the
    /// `ReadBufN<N>` primitive type even though no production
    /// callsite uses it (the wrapping `ReadBuf` two-tier struct is
    /// the production read buffer). Marked `#[allow(dead_code)]`
    /// rather than removed — `ReadBufN<N>` is a stable primitive
    /// that may serve future wire-buffer types.
    #[inline]
    #[must_use]
    #[expect(dead_code, reason = "DEF-265 Idea-38: ReadBufN<N> is a stable \
        primitive retained for future wire-buffer designs; production read \
        buffer is the wrapping ReadBuf struct. Migrated #[allow]→#[expect] \
        (Rust 1.81): if a future caller starts using this method, the \
        attribute fires (the lint no longer triggers), forcing the contributor \
        to remove the now-dead attribute — drift-detection.")]
    pub(crate) fn populated(&self) -> &[u8] {
        self.inner.as_slice()
    }

    /// Absolute position of the read cursor, in bytes from the start
    /// of [`populated`]. Used by the dispatch loop to compute absolute
    /// row-range coordinates (1c-1b).
    ///
    /// DEF-154 (G): u16 cursor accessor — storage type itself.
    /// Used by dispatch cursor math that wants to stay in u16 all
    /// the way through `AbsFrameStart::new(u16)` without ever
    /// widening to usize. Pre-(G) there was also a
    /// `cursor_position() -> usize` widening accessor, deleted
    /// because the only production callsite
    /// (`BrandedReadBuf::cursor_position_scope_local`) now returns
    /// u16, and no other callers remain.
    ///
    /// DEF-265 Idea-38 (2026-05-08): same dead-code allowance as
    /// `populated()` above.
    #[inline]
    #[must_use]
    #[expect(dead_code, reason = "DEF-265 Idea-38: ReadBufN<N> is a stable \
        primitive retained for future wire-buffer designs. Migrated \
        #[allow]→#[expect] (Rust 1.81) — fires when a caller is added, \
        prompting attribute removal.")]
    pub(crate) const fn cursor_position_u16(&self) -> u16 {
        self.cursor
    }

    /// Advance the read cursor by `n` bytes.
    ///
    /// Returns [`AdvancePastEnd`] if `n` exceeds the unread length.
    /// In the dispatcher this Err is dead (we only advance after
    /// `parse_header` confirmed the bytes are present); exposing it
    /// publicly is a tier-1 belt-and-braces.
    #[inline]
    pub fn advance(&mut self, n: usize) -> Result<(), AdvancePastEnd> {
        let available = self.unread_len();
        if n > available {
            return Err(AdvancePastEnd {
                requested: n,
                available,
            });
        }
        // DEF-120: cursor is `u16`. Widen to usize for the
        // add-check, then narrow via `u16::try_from`. Both steps
        // preserved for forbid-bundle safety (no `as`). Arithmetic
        // is architecturally bounded: cursor + n <= inner.len() <=
        // READ_BUF_CAP <= 65_535, so the `try_from` Err branch is
        // dead — kept as belt-and-braces.
        //
        // # LLVM codegen
        //
        // Under `opt-level >= 1` LLVM propagates the `n <= available`
        // bound through the checked_add and folds the `u16::try_from`
        // Err arm out of the emitted code entirely. Release builds
        // carry ZERO instructions for the Err path — it's purely a
        // type-level match-exhaustion concern. Verified by pass-#8
        // audit (F-015). Do NOT replace with an `unsafe` cast.
        let new_cursor_usize = usize::from(self.cursor).checked_add(n).ok_or(AdvancePastEnd {
            requested: n,
            available,
        })?;
        let new_cursor = u16::try_from(new_cursor_usize).map_err(|_| AdvancePastEnd {
            requested: n,
            available,
        })?;
        self.cursor = new_cursor;
        Ok(())
    }

    /// Reset the buffer to empty.
    ///
    /// Used on connection teardown / errored state transitions.
    ///
    /// # DEF-185 P0-C (audit 2026-04-24): zero-on-clear discipline
    ///
    /// `heapless::Vec::clear()` by itself only resets length to 0 —
    /// the backing bytes persist in the 4096-byte array until a later
    /// `append()` overwrites them. For SCRAM handshakes this kept the
    /// server-final message on the connection's stack: the bytes
    /// `"v=<base64_signature>"` where `signature = HMAC(ServerKey,
    /// AuthMessage)` and `ServerKey = HMAC(SaltedPassword, "Server Key")`
    /// are **password-correlated** (though a passive wire attacker
    /// already sees them, a core-dump attacker reads them directly
    /// from client memory with one less network hop). More importantly,
    /// long-lived connections accumulate arbitrary SQL statement
    /// history — `INSERT INTO users (password) VALUES ('...')`,
    /// `SELECT secret FROM vault WHERE id=...`, session tokens, API
    /// keys — all in the backing array.
    ///
    /// Post-fix: overwrite the occupied prefix with zeros before
    /// truncating the length. Cost: O(len) memset; on READ_BUF_CAP =
    /// 4 KiB and L1-cache resident, negligible vs. the typical syscall
    /// that preceded the clear.
    ///
    /// Pairs with manual `Drop` below: clear() handles reuse;
    /// `Drop` handles stack-frame teardown.
    #[inline]
    pub fn clear(&mut self) {
        use zeroize::Zeroize;
        self.inner.as_mut_slice().zeroize();
        self.inner.clear();
        self.cursor = 0;
    }

    /// Number of bytes currently unread.
    #[inline]
    #[must_use]
    pub fn unread_len(&self) -> usize {
        // DEF-120: cursor is u16; widen via `usize::from` for the
        // subtraction (infallible, no `as`).
        self.inner.len().saturating_sub(usize::from(self.cursor))
    }

    /// Reclaim the consumed prefix `[0..cursor)`.
    ///
    /// Internal helper called from [`append`]. Cheap when `cursor == 0`
    /// (no-op); otherwise a `copy_within` of the unread tail followed
    /// by an in-place zeroize of the abandoned tail.
    ///
    /// # DEF-204 (2026-04-27): staleness leak closure
    ///
    /// Pre-(204) sequence was `copy_within → truncate`. After truncate,
    /// `inner.len()` shrinks to `unread_len`, but bytes physically at
    /// positions `[unread_len..len_before)` retain their pre-compact
    /// content — `heapless::Vec::truncate` only adjusts the length
    /// counter for `Copy` types, it does NOT scrub the abandoned
    /// storage. Future `clear()`/`Drop` zeroize only
    /// `[0..current_len)` (= `[0..unread_len)` post-compact), MISSING
    /// the stale tail.
    ///
    /// Concrete leak vector (pre-fix): a 2 KB
    /// `AuthenticationSASLContinue` frame containing server salt +
    /// nonce reaches `ReadBuf`; the dispatcher consumes it
    /// (`cursor → 2048`); a small `ReadyForQuery` arrives; `append()`
    /// triggers `compact()` with `unread_len=0`; `truncate(0)` leaves
    /// bytes `[0..2048)` physically present in the array — including
    /// password-correlated salt + nonce bytes. The leak persists for
    /// the connection lifetime until either a future response of
    /// equal-or-larger size overwrites position-by-position OR `Drop`
    /// fires (which only scrubs the post-compact `inner.len() = 0`,
    /// missing the tail entirely).
    ///
    /// Post-(204): the abandoned range is zeroized in place BEFORE
    /// truncate. `inner.as_mut_slice()` at that point still returns
    /// `[0..len_before)`; the slice view starting at `unread_len`
    /// covers exactly the abandoned bytes (the consumed prefix's
    /// physical content + the source side of the `copy_within` above,
    /// which `copy_within` does NOT zero on the source side).
    ///
    /// **Tier**: tier-3 by-audit ("future push overwrites eventually")
    /// → **tier-2 structural** (every compact is a scrub by
    /// construction; no audit dependency on call patterns).
    fn compact(&mut self) {
        if self.cursor == 0 {
            return;
        }
        // DEF-120: cursor is u16; widen once for all uses.
        let cursor = usize::from(self.cursor);
        let len = self.inner.len();
        // `cursor <= len` invariant; subtraction safe.
        let unread_len = len.saturating_sub(cursor);
        // `copy_within` accepts a Range; the source range is
        // `cursor..len` and dest is `0`. Both inside `len`.
        self.inner.copy_within(cursor..len, 0);
        // DEF-204: zeroize the abandoned tail BEFORE truncate. The
        // slice `[unread_len..len_before)` covers the bytes that
        // (a) were the consumed prefix `[unread_len..cursor)` (if
        // any), and (b) were the source-side of the copy_within
        // `[cursor..len_before)`. Both ranges retain pre-compact
        // physical content unless explicitly scrubbed here.
        //
        // The Some-arm always fires under the `cursor != 0` guard
        // above (=> `unread_len < len_before`). The None-arm is
        // architecturally dead but classified explicitly as a no-op
        // rather than `unwrap_or` silent fallback (CREDO §5).
        {
            use zeroize::Zeroize;
            let inner_mut = self.inner.as_mut_slice();
            if let Some(stale_tail) = inner_mut.get_mut(unread_len..) {
                stale_tail.zeroize();
            }
        }
        // truncate to the new (compacted) length; `Vec::truncate`
        // never panics, only shortens.
        self.inner.truncate(unread_len);
        self.cursor = 0;
    }
}

/// DEF-185 P0-C (audit 2026-04-24): manual Drop impl zeroizes the
/// occupied prefix on scope teardown.
///
/// Rationale: `heapless::Vec` does NOT implement `zeroize::Zeroize`
/// (upstream bound requires `Default + Copy` which Vec lacks). Scrub
/// manually via `Zeroize` on the mut slice (impl'd on `[u8]`). When a
/// wrapper's connection handle goes out of scope the backing 4096-byte
/// array is scrubbed — protocol-level SCRAM signatures and any SQL
/// history from prior frames vanish from memory.
///
/// Caveat per DEF-185 P0-A: under `panic = "abort"` Drop does NOT run
/// on panic paths. This claim holds for the normal-control-flow path
/// only; true memory hygiene under panic requires either `panic =
/// "unwind"` or `mlock`+explicit scrub — flagged for separate design
/// discussion.
impl<const N: usize> Drop for ReadBufN<N> {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.inner.as_mut_slice().zeroize();
    }
}

impl<const N: usize> fmt::Debug for ReadBufN<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBufN")
            .field("unread_len", &self.unread_len())
            .field("cap", &N)
            .finish()
    }
}

/// Two-tier inbound read buffer with stack-inline fast path and
/// lazy heap escape (DEF-265 Idea-38, 2026-05-08).
///
/// # Design rationale
///
/// `PgProtocol` previously embedded a 4096-byte inline buffer
/// (`ReadBufN<4096>`), making `PgProtocol` 4352 B inline. This was
/// cache-locality-friendly (state + buffer co-located on the same
/// struct) but cost 4352 B per connection in pool scenarios.
///
/// Two prior DEF-265 attempts (commits-then-revert, see
/// `deferred.md` DEF-265 entry) tried `Box<ReadBuf>` and
/// `&'buf mut ReadBuf` with lifetime parameter. Both regressed
/// `ping_round_trip` (+54.65% and +18.01%) — the former from
/// heap-alloc cost on every fresh `PgProtocol::new()`, the latter
/// from cache-locality split + pointer-chase cost.
///
/// **Idea-38 design**: keep buffer storage *inline* for tiny frames
/// (≤ 256 B), lazy-escape to heap only when inline overflows.
///
/// - Frames that fit in 256 B: zero alloc, full cache locality
///   (inline storage adjacent to PgProtocol's other fields).
/// - Frames > 256 B: one-time escape (Box::new + memcpy of inline
///   contents to heap), subsequent operations work against heap.
///
/// PgProtocol with this two-tier ReadBuf shrinks from 4352 B inline
/// to ~528 B inline (88% reduction) — without paying alloc cost on
/// the common path.
///
/// # Tier-1 invariant
///
/// At any moment, **exactly one** of `inline` / `heap` holds the
/// populated bytes:
/// - Pre-escape: `heap == None`, `inline.len()` = populated_len.
/// - Post-escape: `heap == Some`, `inline.len() == 0`, `heap.len()` =
///   populated_len.
///
/// `cursor` is the read position into the active storage. Invariant:
/// `cursor <= populated_len <= active_capacity <= 65_535`.
///
/// # `#[forbid(unsafe_code)]` preserved
///
/// All operations use `heapless::Vec`'s safe API. No `MaybeUninit`,
/// no raw pointers, no transmute.
///
/// # Drop semantics (DEF-185 P0-C, DEF-204, DEF-259)
///
/// On Drop, both `inline` and `heap` (if Some) are zeroized. The
/// `Box<heapless::Vec<…>>` heap allocation is then released by Box's
/// own Drop. DEF-259 manifest registers `ReadBuf` as a
/// secret-bearing type; DropCounter test verifies Drop fires on every
/// teardown path.
pub struct ReadBuf {
    /// Inline storage for tiny frames. Always present in the
    /// PgProtocol struct's stack/heap allocation; no extra alloc
    /// cost vs current single-field inline.
    inline: Vec<u8, INLINE_BUF_CAP>,
    /// Lazily-allocated heap storage for frames > `INLINE_BUF_CAP`.
    /// `None` until first append that exceeds inline capacity;
    /// `Some` thereafter for the buffer's lifetime.
    heap: Option<alloc::boxed::Box<Vec<u8, READ_BUF_CAP>>>,
    /// Read cursor into the active storage. Tier-1 invariant:
    /// `cursor <= active_storage.len() <= cap <= 65_535`.
    cursor: u16,
    /// **DEF-248 Sub-A (2026-05-12)** — partial-frame mode tracker.
    ///
    /// `0` outside partial-frame mode (the common case: every frame
    /// either fits whole in the active storage, or the dispatcher
    /// classifies it as `FrameTooLarge` and tears down).
    ///
    /// Non-zero inside partial-frame mode: the count of body bytes
    /// the wire still owes us before the in-flight frame body is
    /// complete. Decremented as bytes are consumed (drained via
    /// `subtract_partial_remaining`) by the streaming consumer
    /// ([`crate::row_stream::RowStream::col_next`] in Sub-A scope).
    ///
    /// Tier-1 by-construction: the field is `pub(crate)`-visible
    /// only via the `partial_*` accessors below, and the mutators
    /// require a [`_row_stream_partial_leaf::PartialFrameToken`]
    /// whose tuple-struct field is private to the leaf submodule.
    /// External callers cannot toggle partial mode; the
    /// leaf-submodule is the single proximate mint site.
    ///
    /// Sub-A scope: partial mode is entered ONLY for D-tag
    /// (`DataRow`) frames in row-streaming protocol state. Non-D
    /// tags > `READ_BUF_CAP` continue to tear down with
    /// `HeaderParse::FrameTooLarge` (memo §1 third bullet — Sub-B's
    /// concern). The partial-mode counter itself is frame-agnostic;
    /// only the policy that gates entry is tag-restricted.
    partial_remaining: u32,
}

impl Default for ReadBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadBuf {
    /// Construct an empty buffer in inline mode.
    ///
    /// `const fn` — caller-side stack allocation is zero-cost
    /// (heapless::Vec::new is const, no memset).
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inline: Vec::new(),
            heap: None,
            cursor: 0,
            // DEF-248 Sub-A: `0` is the canonical "not in partial
            // mode" sentinel. Entered only via the leaf-gated
            // `enter_partial_mode` below.
            partial_remaining: 0,
        }
    }

    /// Append `bytes` to the unread region.
    ///
    /// Inline-mode fast path: try inline `extend_from_slice`. If the
    /// inline storage is exhausted, escape to heap (one-time alloc +
    /// memcpy of inline contents) and retry.
    #[inline]
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // Heap-mode: append to heap with compact-on-overflow (mirrors
        // the pre-DEF-265 single-mode compact behaviour).
        if self.heap.is_some() {
            return self.append_heap(bytes);
        }
        // Inline-mode fast path: try inline append.
        if self.inline.extend_from_slice(bytes).is_ok() {
            return Ok(());
        }
        // Inline-mode SLOW PATH: inline overflow. Compact inline
        // first (might fit after reclaiming consumed prefix); if
        // still doesn't fit, escape to heap.
        self.compact_inline();
        if self.inline.extend_from_slice(bytes).is_ok() {
            return Ok(());
        }
        // Escape: copy inline contents to a fresh heap-allocated
        // 4096-byte buffer, then append the new bytes.
        let mut heap_box: alloc::boxed::Box<Vec<u8, READ_BUF_CAP>> =
            alloc::boxed::Box::new(Vec::new());
        heap_box
            .extend_from_slice(self.inline.as_slice())
            .map_err(|CapacityError { .. }| ReadBufFull {
                attempted: bytes.len(),
                available: 0,
                cap: READ_BUF_CAP,
            })?;
        heap_box
            .extend_from_slice(bytes)
            .map_err(|CapacityError { .. }| {
                let len = heap_box.len();
                ReadBufFull {
                    attempted: bytes.len(),
                    available: READ_BUF_CAP.saturating_sub(len),
                    cap: READ_BUF_CAP,
                }
            })?;
        // Zeroize inline before clearing — the bytes were copied to
        // heap; the inline storage now holds stale duplicates that
        // should not persist (CREDO §11 zeroize-on-clear discipline).
        {
            use zeroize::Zeroize;
            self.inline.as_mut_slice().zeroize();
        }
        self.inline.clear();
        self.heap = Some(heap_box);
        Ok(())
    }

    /// Borrow the unread region.
    ///
    /// The returned slice is valid until the next `&mut self` method
    /// call on this buffer.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        let pop = self.populated();
        debug_assert!(
            usize::from(self.cursor) <= pop.len(),
            "ReadBuf invariant: cursor ({}) must not exceed populated len ({})",
            self.cursor,
            pop.len(),
        );
        pop.get(usize::from(self.cursor)..).unwrap_or(&[])
    }

    /// Borrow the full populated region (used by 1c-1b
    /// `StreamRow` materialiser for absolute-position slices).
    #[inline]
    #[must_use]
    pub(crate) fn populated(&self) -> &[u8] {
        match &self.heap {
            None => self.inline.as_slice(),
            Some(heap) => heap.as_slice(),
        }
    }

    /// Absolute cursor position in u16 (DEF-154 G).
    #[inline]
    #[must_use]
    pub(crate) const fn cursor_position_u16(&self) -> u16 {
        self.cursor
    }

    /// Advance the read cursor by `n` bytes.
    #[inline]
    pub fn advance(&mut self, n: usize) -> Result<(), AdvancePastEnd> {
        let available = self.unread_len();
        if n > available {
            return Err(AdvancePastEnd {
                requested: n,
                available,
            });
        }
        let new_cursor_usize =
            usize::from(self.cursor)
                .checked_add(n)
                .ok_or(AdvancePastEnd {
                    requested: n,
                    available,
                })?;
        let new_cursor = u16::try_from(new_cursor_usize).map_err(|_| AdvancePastEnd {
            requested: n,
            available,
        })?;
        self.cursor = new_cursor;
        Ok(())
    }

    /// Reset the buffer to empty. Zeroizes active storage; releases
    /// heap allocation if escaped.
    #[inline]
    pub fn clear(&mut self) {
        use zeroize::Zeroize;
        self.inline.as_mut_slice().zeroize();
        self.inline.clear();
        if let Some(heap) = &mut self.heap {
            heap.as_mut_slice().zeroize();
        }
        // Drop the heap allocation entirely — return to inline mode.
        // Subsequent appends will reuse inline; if they overflow again,
        // a new heap will be allocated. Per-connection-lifetime this is
        // not a concern (clear runs at most a few times per conn).
        self.heap = None;
        self.cursor = 0;
        // DEF-248 Sub-A: `clear` is the canonical "reset to fresh"
        // operation (called on connection teardown, errored-state
        // entry, post-fatal cleanup). Resetting partial_remaining
        // here keeps the post-clear invariant tight: any subsequent
        // header parse starts in non-partial mode regardless of
        // whether the cleared state had been mid-frame.
        self.partial_remaining = 0;
    }

    /// Number of bytes currently unread.
    #[inline]
    #[must_use]
    pub fn unread_len(&self) -> usize {
        self.populated()
            .len()
            .saturating_sub(usize::from(self.cursor))
    }

    /// Heap-mode append helper: try direct extend; on capacity
    /// failure, compact heap and retry (mirrors pre-DEF-265
    /// `ReadBufN<N>::append` lazy-compact discipline).
    #[inline]
    fn append_heap(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        let Some(heap) = self.heap.as_mut() else {
            // Caller bug — branch unreachable per `append` precondition.
            return Err(ReadBufFull {
                attempted: bytes.len(),
                available: READ_BUF_CAP,
                cap: READ_BUF_CAP,
            });
        };
        if heap.extend_from_slice(bytes).is_ok() {
            return Ok(());
        }
        // Slow path: heap full. Compact (reclaim consumed prefix) and
        // retry. The cursor advances during dispatch — the heap may
        // be physically full while the unread region is small.
        self.compact_heap();
        let Some(heap) = self.heap.as_mut() else {
            return Err(ReadBufFull {
                attempted: bytes.len(),
                available: READ_BUF_CAP,
                cap: READ_BUF_CAP,
            });
        };
        heap.extend_from_slice(bytes)
            .map_err(|CapacityError { .. }| {
                let len = heap.len();
                ReadBufFull {
                    attempted: bytes.len(),
                    available: READ_BUF_CAP.saturating_sub(len),
                    cap: READ_BUF_CAP,
                }
            })
    }

    /// Reclaim the consumed prefix of inline storage.
    ///
    /// Internal helper called from [`append`] when the inline tail
    /// runs out of room. Cheap when `cursor == 0` (no-op).
    fn compact_inline(&mut self) {
        if self.cursor == 0 {
            return;
        }
        debug_assert!(
            self.heap.is_none(),
            "compact_inline called in heap mode — caller bug",
        );
        let cursor = usize::from(self.cursor);
        let len = self.inline.len();
        let unread_len = len.saturating_sub(cursor);
        self.inline.copy_within(cursor..len, 0);
        // Zeroize abandoned tail (DEF-204 staleness leak closure).
        {
            use zeroize::Zeroize;
            if let Some(stale_tail) = self.inline.as_mut_slice().get_mut(unread_len..) {
                stale_tail.zeroize();
            }
        }
        self.inline.truncate(unread_len);
        self.cursor = 0;
    }

    /// Reclaim the consumed prefix of heap storage. Mirrors
    /// `compact_inline` but operates on the heap-mode buffer.
    fn compact_heap(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let Some(heap) = self.heap.as_mut() else {
            return; // caller bug; defensive no-op
        };
        let cursor = usize::from(self.cursor);
        let len = heap.len();
        let unread_len = len.saturating_sub(cursor);
        heap.copy_within(cursor..len, 0);
        // Zeroize abandoned tail (DEF-204 staleness leak closure).
        {
            use zeroize::Zeroize;
            if let Some(stale_tail) = heap.as_mut_slice().get_mut(unread_len..) {
                stale_tail.zeroize();
            }
        }
        heap.truncate(unread_len);
        self.cursor = 0;
    }

    // ─────────────────────────────────────────────────────────────────
    // DEF-248 Sub-A (2026-05-12) — partial-frame mode substrate.
    //
    // The counter `partial_remaining: u32` tracks bytes the wire still
    // owes for the in-flight frame body. Entered via
    // `enter_partial_mode`, exited via `exit_partial_mode`, queried via
    // `is_in_partial_mode` / `partial_remaining`. Bytes drain via
    // `subtract_partial_remaining`.
    //
    // All mutators require a [`_row_stream_partial_leaf::PartialFrameToken`]
    // whose tuple-struct field is private to the leaf submodule. Tier-1
    // within-crate by-construction: hostile in-crate callers cannot
    // mint the token outside the leaf.
    // ─────────────────────────────────────────────────────────────────

    /// Whether the buffer is currently in partial-frame mode.
    ///
    /// `false` outside partial mode (the common case); `true` while a
    /// frame body is being streamed in chunks larger than the
    /// active-tier headroom.
    #[inline]
    #[must_use]
    pub(crate) const fn is_in_partial_mode(&self) -> bool {
        self.partial_remaining > 0
    }

    /// Bytes the wire still owes for the in-flight frame body.
    ///
    /// `0` outside partial mode. Inside partial mode, this is the
    /// count returned by `enter_partial_mode(declared_len)` minus the
    /// sum of all `subtract_partial_remaining` since entry.
    ///
    /// # DEF-280 Bundle K-mirror (2026-05-18) — `#[cfg(test)]`-gated
    ///
    /// Pre-Bundle-K-mirror this accessor was a `pub(crate)` predicate
    /// for upstream callers in `row_stream.rs` that pre-checked
    /// `partial_remaining == 0` before calling `exit_partial_mode`
    /// (tier-2 by-discipline). Bundle K-mirror moved that precondition
    /// INTO `exit_partial_mode` itself (Approach B); the upstream
    /// predicate became dead and the wrapper
    /// `PgProtocol::partial_remaining_for_row_stream` was deleted.
    ///
    /// The accessor is retained as `#[cfg(test)]` because Bundle K's
    /// spec tests (`row_stream::bundle_k_spec_tests`) assert the
    /// counter value directly on a `ReadBuf` fixture — pinning the
    /// no-overwrite-on-Err invariant. Production code uses only the
    /// Result return shape; there is no legitimate production read
    /// path on the counter field.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) const fn partial_remaining(&self) -> u32 {
        self.partial_remaining
    }

    /// Enter partial-frame mode. The leaf-minted token gates this
    /// transition.
    ///
    /// # Caller contract
    ///
    /// - `declared_len` is the frame's wire-declared body length
    ///   **including** the 4 length-field self-bytes (i.e., the raw
    ///   `i32` value from the header). The streaming consumer
    ///   subtracts these accounted-for bytes as it advances.
    /// - The caller has already observed the 5-byte frame header
    ///   (tag + length) and is responsible for advancing the read
    ///   cursor past the header before chunked-body consumption
    ///   begins. The counter tracks body bytes, not header bytes.
    ///
    /// # Re-entry protection (DEF-280 Bundle K, 2026-05-18)
    ///
    /// Pre-Bundle K re-entry while already in partial mode was a
    /// tier-2 caller-bug condition: `debug_assert!(partial_remaining
    /// == 0, ...)` panicked loudly in dev builds, while release
    /// builds silently overwrote `partial_remaining` and dropped the
    /// previously-pending body-byte count — wire-level desync once
    /// the next inbound bytes were classified as a fresh frame
    /// header. The CREDO §V glass pattern (dev loud + release silent).
    ///
    /// Post-Bundle K `enter_partial_mode` returns
    /// `Result<(), AlreadyInPartialMode>`. On Err the counter is
    /// left unchanged (no overwrite); the caller is expected to
    /// classify the bug via `CrateBugLocus::PartialModeReentry` and
    /// transition the connection to Errored. Both dev and release
    /// route through the same path.
    #[inline]
    pub(crate) fn enter_partial_mode(
        &mut self,
        _token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
        declared_len: u32,
    ) -> Result<(), AlreadyInPartialMode> {
        if self.partial_remaining != 0 {
            return Err(AlreadyInPartialMode {
                prev_remaining: self.partial_remaining,
                new_declared_len: declared_len,
            });
        }
        self.partial_remaining = declared_len;
        Ok(())
    }

    /// Exit partial-frame mode. The leaf-minted token gates this
    /// transition.
    ///
    /// # Caller contract — DEF-280 Bundle K-mirror (2026-05-18)
    ///
    /// Exit is only legal once the body has been fully drained
    /// (`partial_remaining == 0`).
    ///
    /// Pre-Bundle-K-mirror: `debug_assert!(partial_remaining == 0)` +
    /// silent reset of the counter to `0` on release builds. The
    /// docstring claimed «tier-1 by-construction: counter is non-
    /// negative regardless of caller drift» — but that argument only
    /// covers counter-value correctness, NOT **wire-synchronisation
    /// correctness**. A silent reset with `partial_remaining > 0`
    /// means body bytes that the wire still owes are never drained;
    /// the next inbound bytes get classified as a fresh frame header
    /// instead of body continuation — wire-desync class (same hazard
    /// class as Bundle K's enter-side silent overwrite, mirror
    /// direction).
    ///
    /// Post-Bundle-K-mirror: returns `Result<(), PartialModeExitUndrained>`.
    /// On Err the counter is **left unchanged** (preserves the
    /// caller's view of body bytes still owed); the caller classifies
    /// via `CrateBugLocus::PartialModeExitUndrained` and transitions
    /// to Errored. Both dev and release route through the same path.
    ///
    /// The pre-Bundle-K-mirror upstream `if partial_remaining == 0`
    /// caller-side checks (defense by-discipline) were removed when
    /// migrating callers to the Err-propagation shape (Approach B —
    /// single source of truth: the function itself enforces the
    /// precondition, caller handles the typed Err).
    #[inline]
    pub(crate) fn exit_partial_mode(
        &mut self,
        _token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
    ) -> Result<(), PartialModeExitUndrained> {
        if self.partial_remaining != 0 {
            return Err(PartialModeExitUndrained {
                remaining: self.partial_remaining,
            });
        }
        // Already 0; the explicit write makes the intent visible and
        // mirrors the enter-side's `self.partial_remaining = declared_len`
        // shape symmetry.
        self.partial_remaining = 0;
        Ok(())
    }

    /// Subtract `n` bytes from the partial-mode counter. Caller drains
    /// these bytes from the buffer's unread region (typically via
    /// [`Self::advance`]) before or after this call.
    ///
    /// Returns `Err(AdvancePastEnd)` if `n` exceeds the current
    /// remaining; the counter is left unchanged on Err. Architecturally
    /// dead under intact callers (the streaming consumer subtracts only
    /// what it actually consumed from the unread region), but the
    /// signature forces every future caller to handle it. Tier-1
    /// belt-and-braces vs silent decrement past zero.
    #[inline]
    pub(crate) fn subtract_partial_remaining(
        &mut self,
        _token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
        n: u32,
    ) -> Result<(), AdvancePastEnd> {
        if n > self.partial_remaining {
            return Err(AdvancePastEnd {
                requested: usize::try_from(n).unwrap_or(usize::MAX),
                available: usize::try_from(self.partial_remaining).unwrap_or(usize::MAX),
            });
        }
        self.partial_remaining = self.partial_remaining.saturating_sub(n);
        Ok(())
    }
}

// DEF-248 Sub-A (2026-05-12) — the partial-frame token TYPE lives in
// `mod crate::row_stream::_row_stream_partial_leaf` so the mint is
// `pub(in crate::row_stream)` (call surface restricted to row_stream).
// `ReadBuf` imports the type and gates the partial_* methods above on
// `&PartialFrameToken`. Tuple-struct field private to its leaf — no
// in-crate caller outside the leaf can mint the token.
//
// Mirror of DEF-272 cluster δ pattern but inverted: the token type is
// declared in the *caller* module (row_stream), not the *callee*
// (buf), because we needed to restrict the call surface to a single
// caller module that lives *outside* `mod buf`. The cluster δ pattern
// declares tokens inside `mod protocol` for the same reason — same
// shape, different module placement.
//
// See [`crate::row_stream::_row_stream_partial_leaf`] for the type
// + mint function.

/// DEF-185 P0-C + DEF-265 Idea-38: zeroize both inline and heap
/// storage on Drop. `heapless::Vec` doesn't implement Zeroize
/// natively (upstream bound requires `Default + Copy`); manual
/// scrub via slice's `Zeroize` impl. The Box's own Drop releases
/// the heap allocation after our zeroize completes.
impl Drop for ReadBuf {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.inline.as_mut_slice().zeroize();
        if let Some(heap) = &mut self.heap {
            heap.as_mut_slice().zeroize();
        }
        // Box::drop runs next, releasing the heap allocation.
    }
}

impl fmt::Debug for ReadBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = if self.heap.is_some() { "heap" } else { "inline" };
        f.debug_struct("ReadBuf")
            .field("mode", &mode)
            .field("unread_len", &self.unread_len())
            .field("inline_cap", &INLINE_BUF_CAP)
            .field("heap_cap", &READ_BUF_CAP)
            .finish()
    }
}

/// Returned by [`ReadBuf::append`] when the bounded buffer cannot
/// accept the inbound bytes.
///
/// The protocol treats this as a fatal connection error: emits
/// `CloseSocket` and fails the in-flight reply (if any). Pool
/// discards the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBufFull {
    /// How many bytes the caller tried to append.
    pub attempted: usize,
    /// How much room was actually available in the buffer.
    pub available: usize,
    /// Configured cap of the buffer (= the const-generic `N` of the
    /// `ReadBuf<N>` instance that produced this error). DEF-199:
    /// carried in the error so the Display impl reports the cap of
    /// the specific protocol instance, not a hard-coded constant.
    pub cap: usize,
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the read-buf-overflow sentinel.
impl core::error::Error for ReadBufFull {}

impl fmt::Display for ReadBufFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read buffer full: tried to append {} bytes, only {} available (cap {})",
            self.attempted, self.available, self.cap,
        )
    }
}

/// Returned by [`ReadBuf::advance`] when asked to advance beyond the
/// unread region.
///
/// In bsql-pg-proto's own dispatcher this Err is unreachable (we only
/// advance after `parse_header` has confirmed the bytes), but the
/// public signature forces every future caller to handle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvancePastEnd {
    /// How far the caller asked to advance.
    pub requested: usize,
    /// How much was actually unread.
    pub available: usize,
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the advance-past-end sentinel.
impl core::error::Error for AdvancePastEnd {}

impl fmt::Display for AdvancePastEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "advance past end: requested {} bytes, only {} unread",
            self.requested, self.available,
        )
    }
}

/// DEF-280 Bundle K (2026-05-18): returned by
/// [`ReadBuf::enter_partial_mode`] when called while the buffer is
/// already in partial-frame mode (`partial_remaining > 0`).
///
/// Pre-Bundle K the same condition was a `debug_assert!` that
/// panicked in dev builds and silently overwrote the prior
/// `partial_remaining` value in release — the CREDO §V glass pattern
/// (loud dev + silent release). Post-Bundle K the counter is left
/// unchanged on detection and a typed witness is returned for caller
/// classification via `CrateBugLocus::PartialModeReentry`. Both dev
/// and release route through the same path.
///
/// Architecturally dead under intact callers — the streaming
/// dispatcher in `row_stream.rs` guarantees `exit_partial_mode` runs
/// before `enter_partial_mode` recurrence — but the typed return is
/// the by-construction shield against future refactor drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyInPartialMode {
    /// Bytes still outstanding for the IN-FLIGHT partial frame at
    /// the moment of the rejected re-entry attempt.
    pub prev_remaining: u32,
    /// `declared_len` that the re-entry attempt would have written
    /// (provided here for diagnostic logs; the counter is NOT
    /// overwritten by Err).
    pub new_declared_len: u32,
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the partial-mode re-entry sentinel.
impl core::error::Error for AlreadyInPartialMode {}

impl fmt::Display for AlreadyInPartialMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "enter_partial_mode called while already in partial mode \
             (prev remaining: {} bytes; rejected declared_len: {})",
            self.prev_remaining, self.new_declared_len,
        )
    }
}

/// DEF-280 Bundle K-mirror (2026-05-18): returned by
/// [`ReadBuf::exit_partial_mode`] when called while the buffer still
/// owes wire body bytes (`partial_remaining > 0`).
///
/// Pre-Bundle-K-mirror the same condition was a `debug_assert!`
/// (panic in dev) plus a silent counter reset to `0` in release —
/// the CREDO §V glass pattern (loud dev + silent release). The
/// release-mode reset's hazard: the previously-pending body bytes
/// are never drained from the wire; the next inbound bytes get
/// mis-classified as a fresh frame header. Wire-desync class —
/// mirror of [`AlreadyInPartialMode`]'s entry-side hazard.
///
/// Post-Bundle-K-mirror the counter is **left unchanged** on
/// detection and a typed witness is returned for caller
/// classification via `CrateBugLocus::PartialModeExitUndrained`.
/// Both dev and release route through the same path.
///
/// Architecturally dead under intact callers — pre-Bundle-K-mirror
/// the two callers in `row_stream.rs` did `if partial_remaining == 0
/// { exit_partial_mode(...) }` upstream-defense; Bundle K-mirror
/// moved that check INTO the function (Approach B — single source
/// of truth) and the caller now routes Err through Errored install
/// + `ColEvent::EndQuery::Err`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialModeExitUndrained {
    /// Body bytes still owed on the wire at the moment of the
    /// rejected exit attempt (provided here for diagnostic logs;
    /// the counter is NOT reset by Err).
    pub remaining: u32,
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the partial-mode exit-undrained
// sentinel.
impl core::error::Error for PartialModeExitUndrained {}

impl fmt::Display for PartialModeExitUndrained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "exit_partial_mode called with {} bytes still owed on the wire \
             (counter preserved; not silently reset)",
            self.remaining,
        )
    }
}

// DEF-154 (H): read-side `BrandedReadBuf<'brand, 'a>` + its
// `populated_branded` / `unread_branded` / `into_populated_branded`
// / `advance_scope_local` / `clear_scope_local` / `cursor_position_scope_local`
// methods + `ReadBuf::with_branded` HRTB entry — ALL DELETED.
//
// Read-side brand was introduced in (B) Phase B2 to prove
// buffer-identity for `ReadRange<'brand>::apply` bounds-safety.
// (H) deleted `ReadRange` entirely — `StagedAction::StreamRowRange`
// now carries `row_bytes: &'r [u8]` slices of populated() directly,
// with lifetime-enforced borrow safety (tier-1). The brand became
// dead scaffolding and is removed per user directive:
// "всё четко и однозначно должно быть; может что-то пригодится
// потом, а может что-то просто избыточно" — избыточно, deleted.

// DEF-154 (H): `BrandedReadBuf<'brand, 'a>` type + its impl +
// `ReadBuf::with_branded` method + `phase_b2_tests` module —
// ALL DELETED. See block comment above.

#[cfg(test)]
mod drop_witness_tests {
    //! DEF-259 (2026-05-08): tier-1-by-construction Drop-fire witness
    //! for [`ReadBufN<N>`] via [`crate::drop_witness::DropCounter`].
    //!
    //! Pre-DEF-259: `ReadBufN<N>::drop` had no per-type witness — it
    //! is a manual `impl Drop` (`buf.rs:382`) that calls
    //! `inner.as_mut_slice().zeroize()` (no `ZeroizeOnDrop` derive
    //! because `heapless::Vec` doesn't impl `Default + Copy`,
    //! upstream's `Zeroize` bound). Verified only transitively via
    //! `dropping_proto_mid_scram_handshake_runs_drop_glue` (which
    //! drops a `PgProtocol` containing a `ReadBuf` and checks no
    //! panic).
    //!
    //! Post-DEF-259: the witness fires the same Drop chain
    //! production runs and the counter increments deterministically
    //! on every `cargo test`. Catches a regression that removes the
    //! manual Drop impl (which would silently leak buffer content
    //! across stack-frame teardown — passwords / SCRAM proofs).

    use super::ReadBufN;
    use crate::drop_witness::{DropCounter, DropProbe};

    /// `ReadBufN<N>::drop` fires its manual `inner.as_mut_slice().zeroize()`
    /// body. Counter increments iff Drop was reached.
    #[test]
    fn read_buf_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let buf: ReadBufN<256> = ReadBufN::<256>::new();
        DropCounter::scoped(buf, probe.clone(), || {
            assert_eq!(probe.fired(), 0);
        });
        assert_eq!(
            probe.fired(),
            1,
            "ReadBufN<N> drop must fire exactly once",
        );
    }

    /// Drop fires for the production-cap instantiation (`READ_BUF_CAP`
    /// = 4096). Pins the production-shape variant.
    #[test]
    fn read_buf_drop_fires_at_production_capacity() {
        let probe = DropProbe::new();
        let buf: ReadBufN<{ crate::frame::READ_BUF_CAP }> =
            ReadBufN::<{ crate::frame::READ_BUF_CAP }>::new();
        DropCounter::scoped(buf, probe.clone(), || {});
        assert_eq!(probe.fired(), 1);
    }
}
