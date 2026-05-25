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
use core::num::NonZeroU32;
use heapless::{CapacityError, Vec};

/// Bounded byte buffer for inbound wire data — generic-form.
///
/// Const-generic over capacity `N`. The default [`ReadBuf`] type
/// alias picks `N = READ_BUF_CAP` (4096); bumping `N` lets callers
/// handle larger frames (analytics workloads with wide
/// `RowDescription`, large `DataRow` payloads).
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
    /// `u16` (not `usize`) — current N values fit with headroom;
    /// narrower type saves bytes per `ReadBuf` on 64-bit. The
    /// const-block in `new()` rejects `N > 65_535` at monomorph time.
    cursor: u16,
}

/// Inline-mode capacity for the two-tier [`ReadBuf`]. Frames ≤
/// 256 B stay in stack-inline storage with full cache-locality
/// (state + small_buf adjacent). Frames > 256 B trigger a one-time
/// lazy escape: the inline contents copy into a heap-allocated
/// 4096-byte storage, and subsequent operations work against the
/// heap. Once escaped, the buffer stays in heap mode for the
/// connection's lifetime (downgrading would require copying back,
/// no perf benefit).
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
    /// **Tier-1 cap invariant**: the const-block fires at
    /// monomorphisation time. Without it, `ReadBufN<70_000>::new()`
    /// would compile but break the `cursor: u16` invariant — any
    /// `advance()` past 65_535 bytes would trip a dead Err branch
    /// silently. The assertion makes any `N > u16::MAX` a **build
    /// failure**, not a runtime tier regression.
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
    /// capacity even after reclaiming the consumed prefix.
    ///
    /// **Eager cursor-reset + lazy compact fallback (DEF-058).**
    /// When the buffer is fully consumed (`cursor == inner.len()`),
    /// zeroizes the consumed region and resets to empty before
    /// extending. The `extend_from_slice` then sees an empty buffer
    /// with full capacity — no `compact()` needed. Only when a
    /// partial frame residue exists (cursor < len) does the lazy
    /// compact fallback fire.
    #[inline]
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // DEF-058 eager cursor-reset: when the buffer is fully
        // consumed (cursor == inner.len() and cursor > 0), zeroize
        // and reset before extending. The extend then sees an empty
        // buffer with full capacity — no compact() needed. The
        // `cursor > 0` guard skips the no-op case (fresh buffer).
        if self.cursor > 0 && usize::from(self.cursor) == self.inner.len() {
            use zeroize::Zeroize;
            self.inner.as_mut_slice().zeroize();
            self.inner.clear();
            self.cursor = 0;
        }
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
        // `indexing_slicing`. `u16 → usize` via infallible
        // widening `From` impl (no `as` cast).
        //
        // Debug-builds actively assert the invariant
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
    /// which needs absolute-position slices into rows whose frames
    /// were advanced-past during the dispatch loop but whose bytes
    /// must remain valid until `OutActions` drops.
    ///
    /// # Lifetime invariant
    ///
    /// The returned slice is valid until the next `&mut self` method
    /// call on this buffer (`append`, `advance`, `clear`) — same as
    /// [`unread`]. Compaction happens lazily on the next `append`;
    /// by then no outstanding borrow can be alive (the borrow
    /// checker refuses the `&mut` call otherwise). Callers emit
    /// the `ColEvent` row-streaming pull API with slices carved out
    /// of this region during [`crate::PgProtocol::feed_bytes`]; the
    /// `'r` lifetime on `OutActions<'w, 'r>` ties those slices back
    /// to the `&'r mut self` borrow on `PgProtocol`, which blocks
    /// the next `feed_bytes` call while they are alive.
    ///
    /// [`unread`]: ReadBuf::unread
    ///
    /// Visibility is `pub(crate)`. Only `materialise` / dispatch
    /// resolution need this view; an external caller reading
    /// `populated()` would get access to bytes already consumed
    /// past the cursor with no user benefit.
    ///
    /// Retained on the `ReadBufN<N>` primitive type even though no
    /// production callsite uses it (the wrapping `ReadBuf` two-tier
    /// struct is the production read buffer). `ReadBufN<N>` is a
    /// stable primitive that may serve future wire-buffer types.
    #[inline]
    #[must_use]
    #[expect(dead_code, reason = "`ReadBufN<N>` is a stable primitive retained for future \
        wire-buffer designs; production read buffer is the wrapping `ReadBuf` struct. \
        `#[expect]` triggers the lint if a caller is added — prompting attribute removal.")]
    pub(crate) fn populated(&self) -> &[u8] {
        self.inner.as_slice()
    }

    /// Absolute position of the read cursor, in bytes from the start
    /// of [`populated`]. Used by the dispatch loop to compute
    /// absolute row-range coordinates. `u16` accessor — storage type
    /// itself, so dispatch cursor math stays in `u16` all the way
    /// through `AbsFrameStart::new(u16)` without ever widening to
    /// `usize`.
    ///
    /// Same dead-code allowance as `populated()` above.
    #[inline]
    #[must_use]
    #[expect(dead_code, reason = "`ReadBufN<N>` is a stable primitive retained for future \
        wire-buffer designs. `#[expect]` triggers the lint when a caller is added, \
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
        // `cursor` is `u16`. Widen to usize for the add-check, then
        // narrow via `u16::try_from`. Both steps preserved for
        // forbid-bundle safety (no `as`). Arithmetic is
        // architecturally bounded: cursor + n <= inner.len() <=
        // READ_BUF_CAP <= 65_535, so the `try_from` Err branch is
        // dead — kept as belt-and-braces.
        //
        // # LLVM codegen
        //
        // Under `opt-level >= 1` LLVM propagates the `n <= available`
        // bound through the checked_add and folds the `u16::try_from`
        // Err arm out of the emitted code entirely. Release builds
        // carry ZERO instructions for the Err path — it's purely a
        // type-level match-exhaustion concern. Do NOT replace with an
        // `unsafe` cast.
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
    /// # Zero-on-clear discipline
    ///
    /// `heapless::Vec::clear()` by itself only resets length to 0 —
    /// the backing bytes persist in the 4096-byte array until a later
    /// `append()` overwrites them. SCRAM server-final bytes
    /// (`"v=<base64_signature>"` where `signature = HMAC(ServerKey,
    /// AuthMessage)` and `ServerKey = HMAC(SaltedPassword, "Server
    /// Key")`) are **password-correlated**: though a passive wire
    /// attacker already sees them, a core-dump attacker reads them
    /// directly from client memory with one less network hop. More
    /// importantly, long-lived connections accumulate arbitrary SQL
    /// statement history — `INSERT INTO users (password) VALUES
    /// ('...')`, `SELECT secret FROM vault WHERE id=...`, session
    /// tokens, API keys — all in the backing array.
    ///
    /// Mitigation: overwrite the occupied prefix with zeros before
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
        // `cursor` is u16; widen via `usize::from` for the
        // subtraction (infallible, no `as`).
        self.inner.len().saturating_sub(usize::from(self.cursor))
    }

    /// Reclaim the consumed prefix `[0..cursor)`.
    ///
    /// Internal helper called from [`append`]. Cheap when `cursor == 0`
    /// (no-op); otherwise a `copy_within` of the unread tail followed
    /// by an in-place zeroize of the abandoned tail.
    ///
    /// # Staleness leak closure
    ///
    /// A naive `copy_within → truncate` sequence leaves the bytes at
    /// positions `[unread_len..len_before)` physically present in the
    /// array — `heapless::Vec::truncate` only adjusts the length
    /// counter for `Copy` types, it does NOT scrub the abandoned
    /// storage. Future `clear()`/`Drop` zeroize only
    /// `[0..current_len)` (= `[0..unread_len)` post-compact), missing
    /// the stale tail.
    ///
    /// Concrete leak vector this closure prevents: a 2 KB
    /// `AuthenticationSASLContinue` frame containing server salt +
    /// nonce reaches `ReadBuf`; the dispatcher consumes it
    /// (`cursor → 2048`); a small `ReadyForQuery` arrives; `append()`
    /// triggers `compact()` with `unread_len=0`; a naive `truncate(0)`
    /// would leave bytes `[0..2048)` physically present in the array
    /// — including password-correlated salt + nonce bytes. The leak
    /// would persist for the connection lifetime until either a
    /// future response of equal-or-larger size overwrote
    /// position-by-position OR `Drop` fired (which only scrubs the
    /// post-compact `inner.len() = 0`, missing the tail entirely).
    ///
    /// Closure: the abandoned range is zeroized in place BEFORE
    /// truncate. `inner.as_mut_slice()` at that point still returns
    /// `[0..len_before)`; the slice view starting at `unread_len`
    /// covers exactly the abandoned bytes (the consumed prefix's
    /// physical content + the source side of the `copy_within` above,
    /// which `copy_within` does NOT zero on the source side).
    ///
    /// **Tier**: tier-2 structural — every compact is a scrub by
    /// construction; no call-pattern audit dependency.
    fn compact(&mut self) {
        if self.cursor == 0 {
            return;
        }
        // `cursor` is u16; widen once for all uses.
        let cursor = usize::from(self.cursor);
        let len = self.inner.len();
        // `cursor <= len` invariant; subtraction safe.
        let unread_len = len.saturating_sub(cursor);
        // `copy_within` accepts a Range; the source range is
        // `cursor..len` and dest is `0`. Both inside `len`.
        self.inner.copy_within(cursor..len, 0);
        // Zeroize the abandoned tail BEFORE truncate. The slice
        // `[unread_len..len_before)` covers the bytes that (a) were
        // the consumed prefix `[unread_len..cursor)` (if any), and
        // (b) were the source-side of the copy_within
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

/// Manual Drop impl zeroizes the occupied prefix on scope teardown.
///
/// Rationale: `heapless::Vec` does NOT implement `zeroize::Zeroize`
/// (upstream bound requires `Default + Copy` which Vec lacks). Scrub
/// manually via `Zeroize` on the mut slice (impl'd on `[u8]`). When a
/// wrapper's connection handle goes out of scope the backing 4096-byte
/// array is scrubbed — protocol-level SCRAM signatures and any SQL
/// history from prior frames vanish from memory.
///
/// Caveat: under `panic = "abort"` Drop does NOT run on panic paths.
/// This claim holds for the normal-control-flow path only; true
/// memory hygiene under panic requires either `panic = "unwind"` or
/// `mlock` + explicit scrub — handled by the driver-side panic hook
/// for secret-bearing types.
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
/// lazy heap escape.
///
/// # Design rationale
///
/// A single-tier 4096-byte inline buffer (`ReadBufN<4096>`) makes
/// `PgProtocol` 4352 B inline. That is cache-locality-friendly (state +
/// buffer co-located on the same struct) but costs 4352 B per
/// connection in pool scenarios. Alternative single-tier shapes
/// (`Box<ReadBuf>` and `&'buf mut ReadBuf` with lifetime parameter)
/// either pay heap-alloc cost on every fresh `PgProtocol::new()` or
/// split cache locality and add a pointer-chase per access — both
/// measurably regress `ping_round_trip`.
///
/// **Two-tier design**: keep buffer storage *inline* for tiny frames
/// (≤ 256 B), lazy-escape to heap only when inline overflows.
///
/// - Frames that fit in 256 B: zero alloc, full cache locality
///   (inline storage adjacent to PgProtocol's other fields).
/// - Frames > 256 B: one-time escape (Box::new + memcpy of inline
///   contents to heap), subsequent operations work against heap.
///
/// PgProtocol with this two-tier ReadBuf is ~528 B inline (88%
/// reduction vs single-tier 4352 B) — without paying alloc cost on
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
/// # Drop semantics
///
/// On Drop, both `inline` and `heap` (if Some) are zeroized. The
/// `Box<heapless::Vec<…>>` heap allocation is then released by Box's
/// own Drop. The crate-level `SecretZeroize` manifest registers
/// `ReadBuf` as a secret-bearing type; a `DropCounter` test verifies
/// Drop fires on every teardown path.
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
    /// Partial-frame mode tracker.
    ///
    /// **Type-level state encoding**: `Option<NonZeroU32>` where
    /// `None` = not in partial mode (the common case: every frame
    /// either fits whole in the active storage, or the dispatcher
    /// classifies it as `FrameTooLarge` and tears down).
    ///
    /// `Some(remaining)` = in partial mode; `remaining.get()` is the
    /// count of body bytes the wire still owes us before the
    /// in-flight frame body is complete. Decremented as bytes are
    /// consumed via [`Self::subtract_partial_remaining`]; transitions
    /// to `None` when the counter reaches `0`.
    ///
    /// # Tier-1 closure
    ///
    /// A naive `partial_remaining: u32` shape with the convention
    /// `0 ⟺ not in partial mode` is a value-level invariant
    /// (tier-2-by-discipline). A future bug
    /// `self.partial_remaining = 0` while intending to stay in
    /// partial mode would be a compile-clean silent desync. The
    /// `Option<NonZeroU32>` encoding captures the "in partial
    /// mode" state at type level — writing `partial_remaining = 0`
    /// no longer typechecks; only explicit `partial_remaining =
    /// None` (exit) or `partial_remaining = NonZeroU32::new(_)`
    /// (which is itself a Result-returning shape).
    ///
    /// Tier-1 by-construction: the field is `pub(crate)`-visible
    /// only via the `partial_*` accessors below, and the mutators
    /// require a [`_row_stream_partial_leaf::PartialFrameToken`]
    /// whose tuple-struct field is private to the leaf submodule.
    /// External callers cannot toggle partial mode; the
    /// leaf-submodule is the single proximate mint site.
    ///
    /// Scope: partial mode is entered ONLY for D-tag (`DataRow`)
    /// frames in row-streaming protocol state. Non-D tags >
    /// `READ_BUF_CAP` tear down with `HeaderParse::FrameTooLarge`.
    /// The partial-mode counter itself is frame-agnostic; only the
    /// policy that gates entry is tag-restricted.
    partial_remaining: Option<NonZeroU32>,
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
            // `None` = not in partial mode; entered only via the
            // leaf-gated `enter_partial_mode` below.
            partial_remaining: None,
        }
    }

    /// Append `bytes` to the unread region.
    ///
    /// Inline-mode fast path: try inline `extend_from_slice`. If the
    /// inline storage is exhausted, escape to heap (one-time alloc +
    /// memcpy of inline contents) and retry.
    ///
    /// # Eager cursor-reset (DEF-058)
    ///
    /// Before attempting `extend_from_slice`, checks whether the
    /// active storage is fully consumed (`cursor == populated.len()`
    /// and `cursor > 0`). If so, zeroizes the consumed region and
    /// resets the storage to empty (len = 0, cursor = 0). The
    /// subsequent `extend_from_slice` then finds an empty buffer with
    /// full capacity — no `compact()` needed.
    ///
    /// **Why this eliminates compact on the hot path**: the Postgres
    /// wire protocol has a consume-then-append pattern — the driver
    /// reads a TCP chunk, parses all complete frames (advancing past
    /// each), then waits for the next chunk. After parsing all frames,
    /// cursor == len (the common case). With eager reset, the next
    /// `append()` sees room = full capacity and succeeds on the first
    /// `extend_from_slice` attempt. Without it, the buffer has
    /// cursor = len = N, room = cap - N, which may be insufficient
    /// for the incoming bytes — triggering `compact()` with its
    /// `copy_within` memmove + zeroize + truncate + retry overhead.
    ///
    /// **Residual compacts**: the `compact_inline()` / `compact_heap()`
    /// fallbacks remain for the case where advance left a partial
    /// residue (cursor < len) and the residue + incoming bytes exceed
    /// tail capacity. The fallback fires only on that path — with
    /// eager reset covering the fully-consumed case, the residual
    /// compacts run strictly fewer times.
    ///
    /// # Zeroize discipline
    ///
    /// The eager reset path zeroizes `[0..len)` of the active storage
    /// before truncating — same staleness-leak-closure guarantee as
    /// `compact()`. Every consumed byte is scrubbed before the buffer
    /// re-enters the empty state; no stale frame data (SQL history,
    /// SCRAM proofs) persists past the reset point.
    ///
    /// Tier-2 structural: zeroize-before-truncate is unconditional on
    /// the reset path — no call-pattern audit dependency.
    ///
    /// # Safety of reset timing
    ///
    /// The reset runs inside `append()`, which takes `&mut self`.
    /// The borrow checker guarantees that no outstanding `&populated`
    /// / `&unread` borrows can be alive at this point — the caller
    /// must have released all shared borrows before calling `append`.
    /// This is stricter than resetting inside `advance()`, where
    /// callers (e.g., `emit_next_col`) may still project slices from
    /// `populated()` after advancing the cursor.
    #[inline]
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // Heap-mode fast path.
        if let Some(heap) = self.heap.as_mut() {
            // DEF-058 eager cursor-reset: when the heap buffer is
            // fully consumed (cursor == heap.len()), zeroize and
            // reset before extending. The extend then sees an empty
            // buffer with full capacity — no compact_heap() needed.
            if self.cursor > 0 && usize::from(self.cursor) == heap.len() {
                use zeroize::Zeroize;
                heap.as_mut_slice().zeroize();
                heap.clear();
                self.cursor = 0;
            }
            // Try extend. Fast path: room in tail → single memcpy.
            if heap.extend_from_slice(bytes).is_ok() {
                return Ok(());
            }
            // Slow path: tail exhausted with unread residue.
            // compact_heap moves the residue to position 0, frees
            // tail, and retries. This fires only when advance left a
            // partial frame — rare under the eager-reset gate above.
            self.compact_heap();
            let Some(heap) = self.heap.as_mut() else {
                return Err(ReadBufFull {
                    attempted: bytes.len(),
                    available: READ_BUF_CAP,
                    cap: READ_BUF_CAP,
                });
            };
            return heap
                .extend_from_slice(bytes)
                .map_err(|CapacityError { .. }| {
                    let len = heap.len();
                    ReadBufFull {
                        attempted: bytes.len(),
                        available: READ_BUF_CAP.saturating_sub(len),
                        cap: READ_BUF_CAP,
                    }
                });
        }

        // Inline-mode: DEF-058 eager cursor-reset for inline tier.
        if self.cursor > 0 && usize::from(self.cursor) == self.inline.len() {
            use zeroize::Zeroize;
            self.inline.as_mut_slice().zeroize();
            self.inline.clear();
            self.cursor = 0;
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

    /// Borrow the full populated region (used by the `StreamRow`
    /// materialiser for absolute-position slices).
    #[inline]
    #[must_use]
    pub(crate) fn populated(&self) -> &[u8] {
        match &self.heap {
            None => self.inline.as_slice(),
            Some(heap) => heap.as_slice(),
        }
    }

    /// Absolute cursor position in u16.
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
        // `clear` is the canonical "reset to fresh" operation (called
        // on connection teardown, errored-state entry, post-fatal
        // cleanup). Resetting partial_remaining here keeps the
        // post-clear invariant tight: any subsequent header parse
        // starts in non-partial mode regardless of whether the
        // cleared state had been mid-frame.
        self.partial_remaining = None;
    }

    /// Number of bytes currently unread.
    #[inline]
    #[must_use]
    pub fn unread_len(&self) -> usize {
        self.populated()
            .len()
            .saturating_sub(usize::from(self.cursor))
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
        // Zeroize abandoned tail (staleness leak closure: see
        // `ReadBufN::compact` docstring for the leak vector).
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
        // Zeroize abandoned tail (staleness leak closure: see
        // `ReadBufN::compact` docstring for the leak vector).
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
    // Partial-frame mode substrate.
    //
    // The counter `partial_remaining: Option<NonZeroU32>` tracks bytes
    // the wire still owes for the in-flight frame body. Entered via
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
        self.partial_remaining.is_some()
    }

    /// Bytes the wire still owes for the in-flight frame body.
    ///
    /// `0` outside partial mode. Inside partial mode, this is the
    /// count returned by `enter_partial_mode(declared_len)` minus the
    /// sum of all `subtract_partial_remaining` since entry.
    ///
    /// # `#[cfg(test)]`-gated
    ///
    /// `exit_partial_mode` itself enforces the
    /// `partial_remaining == 0` precondition via the typed Err
    /// return; production code never reads the raw counter
    /// (single-source-of-truth: the function enforces, callers
    /// handle the Result).
    ///
    /// The accessor is retained as `#[cfg(test)]` because the
    /// row-stream spec tests assert the counter value directly on a
    /// `ReadBuf` fixture — pinning the no-overwrite-on-Err invariant
    /// against future refactor drift.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) const fn partial_remaining(&self) -> u32 {
        match self.partial_remaining {
            Some(n) => n.get(),
            None => 0,
        }
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
    /// # Re-entry protection
    ///
    /// Re-entry while already in partial mode would be a wire-desync
    /// hazard: silently overwriting `partial_remaining` drops the
    /// previously-pending body-byte count, and the next inbound
    /// bytes get classified as a fresh frame header. A naive
    /// `debug_assert!(partial_remaining == 0, ...)` matches the
    /// CREDO §V glass pattern (loud dev + silent release) which
    /// still leaves the release-mode hazard in place.
    ///
    /// Instead `enter_partial_mode` returns
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
        if let Some(prev) = self.partial_remaining {
            return Err(AlreadyInPartialMode {
                prev_remaining: prev.get(),
                new_declared_len: declared_len,
            });
        }
        // Typed transition. The `Some` arm of the type-state means
        // "in partial mode"; the None-to-Some flip happens here
        // exactly once per partial frame.
        // `NonZeroU32::new(declared_len)` returns None for the
        // architecturally-dead `declared_len == 0` case — a PG wire
        // frame with body length < 5 (header is 5 bytes inclusive) is
        // pre-rejected by `parse_header`, so the upstream caller never
        // hands us 0. The None path keeps `partial_remaining` at None
        // (silent failure to enter partial mode); that branch is dead
        // but the typed Result return doesn't currently surface it
        // (the u32 representation also did not surface
        // `declared_len == 0`).
        self.partial_remaining = NonZeroU32::new(declared_len);
        Ok(())
    }

    /// Exit partial-frame mode. The leaf-minted token gates this
    /// transition.
    ///
    /// # Caller contract
    ///
    /// Exit is only legal once the body has been fully drained
    /// (`partial_remaining == 0`).
    ///
    /// A naive `debug_assert!(partial_remaining == 0)` + silent reset
    /// of the counter to `0` on release builds only covers counter-
    /// value correctness, NOT **wire-synchronisation correctness**.
    /// A silent reset with `partial_remaining > 0` means body bytes
    /// that the wire still owes are never drained; the next inbound
    /// bytes get classified as a fresh frame header instead of body
    /// continuation — wire-desync class (mirror of the enter-side
    /// silent-overwrite hazard documented on
    /// [`AlreadyInPartialMode`]).
    ///
    /// Instead `exit_partial_mode` returns
    /// `Result<(), PartialModeExitUndrained>`. On Err the counter is
    /// **left unchanged** (preserves the caller's view of body bytes
    /// still owed); the caller classifies via
    /// `CrateBugLocus::PartialModeExitUndrained` and transitions to
    /// Errored. Both dev and release route through the same path.
    ///
    /// Caller-side `if partial_remaining == 0` defense-by-discipline
    /// guards are unnecessary — the function itself enforces the
    /// precondition (single source of truth) and the caller handles
    /// the typed Err.
    #[inline]
    pub(crate) fn exit_partial_mode(
        &mut self,
        _token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
    ) -> Result<(), PartialModeExitUndrained> {
        if let Some(remaining) = self.partial_remaining {
            return Err(PartialModeExitUndrained {
                remaining: remaining.get(),
            });
        }
        // Explicit type-state exit transition. The assignment
        // mirrors the enter-side's `self.partial_remaining =
        // NonZeroU32::new(declared_len)` symmetry. Already `None`
        // at this point — the explicit write makes the intent
        // visible.
        self.partial_remaining = None;
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
        // Map the type-state into the u32 representation for the
        // subtract arithmetic. The `None` arm (not in partial mode)
        // is treated as "0 remaining" for diagnostic purposes — a
        // caller subtracting while not in partial mode trips the
        // `n > current` check immediately.
        let current = match self.partial_remaining {
            Some(remaining) => remaining.get(),
            None => 0,
        };
        if n > current {
            // Widen `u32 → usize` for the diagnostic-display fields.
            // The conversion is infallible under the crate-root
            // `usize::BITS >= 32` const-assert; routed through the
            // single-audit-point `narrow::usize_from_u32` helper —
            // the dead-arm landing pad lives there, not at this call
            // site. (A prior audit verdict tagged this as
            // "display-only tier-3 saturation"; in practice the
            // architectural-dead status is identical to compute
            // paths and the same helper applies.)
            return Err(AdvancePastEnd {
                requested: crate::narrow::usize_from_u32(n),
                available: crate::narrow::usize_from_u32(current),
            });
        }
        // Type-state transition: when `current - n` reaches 0 we exit
        // partial mode automatically (Some(_) → None). Otherwise we
        // stay in partial mode with the decremented count.
        self.partial_remaining = NonZeroU32::new(current.saturating_sub(n));
        Ok(())
    }
}

// The partial-frame token TYPE lives in
// `mod crate::row_stream::_row_stream_partial_leaf` so the mint is
// `pub(in crate::row_stream)` (call surface restricted to row_stream).
// `ReadBuf` imports the type and gates the partial_* methods above on
// `&PartialFrameToken`. Tuple-struct field private to its leaf — no
// in-crate caller outside the leaf can mint the token.
//
// Mirror of the protocol-state token-leaf pattern but inverted: the
// token type is declared in the *caller* module (row_stream), not
// the *callee* (buf), because the call surface must be restricted to
// a single caller module that lives *outside* `mod buf`. The
// protocol-state pattern declares tokens inside `mod protocol` for
// the same reason — same shape, different module placement.
//
// See [`crate::row_stream::_row_stream_partial_leaf`] for the type
// + mint function.

/// Zeroize both inline and heap storage on Drop. `heapless::Vec`
/// doesn't implement Zeroize natively (upstream bound requires
/// `Default + Copy`); manual scrub via slice's `Zeroize` impl. The
/// Box's own Drop releases the heap allocation after our zeroize
/// completes.
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
    /// `ReadBuf<N>` instance that produced this error). Carried in
    /// the error so the Display impl reports the cap of the specific
    /// protocol instance, not a hard-coded constant.
    pub cap: usize,
}

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

/// Returned by [`ReadBuf::enter_partial_mode`] when called while the
/// buffer is already in partial-frame mode (`partial_remaining > 0`).
///
/// A naive `debug_assert!` here would panic in dev builds and
/// silently overwrite the prior `partial_remaining` value in release
/// — the CREDO §V glass pattern (loud dev + silent release). Instead
/// the counter is left unchanged on detection and a typed witness is
/// returned for caller classification via
/// `CrateBugLocus::PartialModeReentry`. Both dev and release route
/// through the same path.
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

/// Returned by [`ReadBuf::exit_partial_mode`] when called while the
/// buffer still owes wire body bytes (`partial_remaining > 0`).
///
/// A naive `debug_assert!` (panic in dev) plus a silent counter
/// reset to `0` in release is the CREDO §V glass pattern (loud dev,
/// silent release). The release-mode reset's hazard: the
/// previously-pending body bytes are never drained from the wire;
/// the next inbound bytes get mis-classified as a fresh frame
/// header. Wire-desync class — mirror of [`AlreadyInPartialMode`]'s
/// entry-side hazard.
///
/// Instead the counter is **left unchanged** on detection and a
/// typed witness is returned for caller classification via
/// `CrateBugLocus::PartialModeExitUndrained`. Both dev and release
/// route through the same path.
///
/// Architecturally dead under intact callers — the function itself
/// enforces the `partial_remaining == 0` precondition (single source
/// of truth) and the caller routes Err through Errored install +
/// `ColEvent::EndQuery::Err`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialModeExitUndrained {
    /// Body bytes still owed on the wire at the moment of the
    /// rejected exit attempt (provided here for diagnostic logs;
    /// the counter is NOT reset by Err).
    pub remaining: u32,
}

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

#[cfg(test)]
mod drop_witness_tests {
    //! Tier-1-by-construction Drop-fire witness for [`ReadBufN<N>`]
    //! via [`crate::drop_witness::DropCounter`].
    //!
    //! `ReadBufN<N>::drop` is a manual `impl Drop` that calls
    //! `inner.as_mut_slice().zeroize()` — `ZeroizeOnDrop` derive is
    //! unavailable because `heapless::Vec` doesn't impl
    //! `Default + Copy` (upstream's `Zeroize` bound). The witness
    //! fires the same Drop chain production runs and the counter
    //! increments deterministically on every `cargo test`. Catches
    //! a regression that removes the manual Drop impl (which would
    //! silently leak buffer content across stack-frame teardown —
    //! passwords / SCRAM proofs).

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
