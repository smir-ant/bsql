//! Server-error payload arena — externalised storage for the
//! `ProtocolError::ServerErrorResponse` bounded strings.
//!
//! # DEF-184 (A1+A13) rationale
//!
//! Pre-(184) `ProtocolError::ServerErrorResponse` carried `message:
//! BoundedStr<128> + detail: BoundedStr<96> + hint: BoundedStr<64>`
//! inline (~288 B). Because `ProtocolError` is the `.cause` field of
//! `Action::FailReply` / `StreamItem::FailReply` /
//! `DispatchOutcome::Errored`, the 288 B payload cascaded through:
//!
//! - `Action<'w, 'r>` — 312 B dominated variant.
//! - `OutActions = [Action; 9]` — 9 × 312 = 2808 B stack frame.
//! - `StreamItem<'a>` — 320 B per `next_event()` return-by-value.
//!
//! Post-(184): the three bounded strings move into a single-slot
//! [`ErrorArena`] on `PgProtocol`. The `ServerErrorResponse` variant
//! carries an [`ErrorRef`] handle (8 B post-(audit #3 A-04)) instead
//! of inline strings; callers resolve via
//! [`crate::PgProtocol::get_server_error`] to get
//! `Result<&ErrorPayload, ArenaError>` (A-06 tier-3 elevation).
//!
//! **Cascade result:** `ProtocolError` shrinks 312 B → 72 B (A-12
//! exact pin); `Action` shrinks 312 B → 88 B (Reply-bounded);
//! `OutActions = [Action; 9]` shrinks 2808 B → 800 B; `StreamItem`
//! shrinks ~4×.
//!
//! # Single-slot design
//!
//! The arena holds a **single** `Option<ErrorPayload>` — not a multi-
//! slot slab (mirror of the pre-DEF-188 `SchemaArena` design,
//! deleted alongside the schema arena). Rationale:
//!
//! 1. **Single-inflight semantics (pre-1c-5).** Per feed_bytes /
//!    push_command cycle, at most ONE server error can reach the
//!    client (the state machine transitions to `Errored` on first
//!    ErrorResponse frame, blocking further dispatch). One slot
//!    suffices.
//! 2. **Simpler stale-ref model.** One `u32 gen` counter (A-04:
//!    widened u8→u32 for wrap-safety on long-running connections);
//!    alloc bumps gen + overwrites slot; get compares gen.
//! 3. **Smaller PgProtocol footprint.** One slot approx 289 B + 4 B
//!    gen + padding. Multi-slot slab of size 2 would be approx 576 B.
//!    Defer multi-slot until 1c-5 pipelining actually needs it.
//!
//! # Alloc / clear discipline (mirror of schema_arena.rs)
//!
//! - **Alloc** happens in dispatch.rs when parsing an `ErrorResponse`
//!   frame (`parse_and_alloc_server_error`): parsed bounded strings
//!   get stored in the arena; the returned [`ErrorRef`] threads into
//!   `ProtocolError::ServerErrorResponse { details_ref, ... }`.
//! - **Clear** happens at entry-point boundaries when prior state is
//!   `Idle` or `Errored` — alongside `SchemaArena::clear()` in
//!   [`crate::PgProtocol::clear_terminal_row_desc_if_idle_or_errored`]. The next
//!   feed_bytes call starts with a fresh arena; any ErrorRef held
//!   past that boundary becomes stale (classified via generation).
//!
//! # Staleness classification
//!
//! [`ErrorArena::get`] returns `Result<&ErrorPayload, ArenaError>`
//! with two classified error variants:
//!
//! - [`ArenaError::Empty`] — arena was never populated for this
//!   generation. Happens when a caller holds an `ErrorRef` forged
//!   or corrupted into `{slot=OCCUPIED, generation=current}` but
//!   the slot is `None` (architecturally unreachable under
//!   `#[forbid(unsafe_code)]` since `ErrorRef` construction is
//!   confined to `ErrorArena::alloc` which always populates the
//!   slot — but classified explicitly here rather than left as
//!   a silent tier-4 fallback).
//! - [`ArenaError::Stale`] — generation mismatch. The `ErrorRef`
//!   was issued in an earlier allocation cycle; the arena has
//!   since been cleared or a fresh payload was alloc'd that bumped
//!   generation. This is the expected "consumed" signal for
//!   callers who deferred resolution past an entry-point boundary.
//!
//! Pre-(audit #3 A-06) this was `Option<&ErrorPayload>` with the
//! two failure modes collapsed into `None`. Tier-3 elevation via
//! Result classifies the two cases so callers (tests, wrapper
//! crate, operator diagnostics) can distinguish "I held the ref
//! too long" (Stale) from "crate bug, shouldn't happen" (Empty).

use crate::ident::SecretBoundedStr;

/// Full per-server-error payload — the three bounded strings that
/// used to live inline in `ProtocolError::ServerErrorResponse`.
///
/// # DEF-205 (2026-04-27): tier-1 staleness closure
///
/// Pre-DEF-205 this struct was `#[derive(Copy)]` with `BoundedStr<N>`
/// fields — `ErrorArena::clear` flipped `slot = None` which only
/// changed the discriminant; the `Some(ErrorPayload)` data bytes
/// physically persisted in the `Option`'s storage region (~288 B
/// of server error message / detail / hint, possibly containing
/// query details echoed in syntax errors with embedded password
/// literals).
///
/// Post-DEF-205: fields use [`SecretBoundedStr<N>`] which is
/// non-Copy with `Drop` that scrubs the buffer. By Rust language
/// semantics, `slot = None` MUST drop the old `Some(ErrorPayload)`
/// before flipping the discriminant, firing the Drop chain that
/// scrubs each field's bytes. **Tier-1 by compiler-enforced Drop**
/// — no audit dependency, no callsite to forget.
///
/// Trade-off: `ErrorPayload` is no longer `Copy`. Callers using by
/// value still work (move semantics), but `Result::copied()` calls
/// must change to `Result::cloned()`. Production cost: cold-path
/// only (error frames are rare); cloning is one struct memcpy.
///
/// Users access via [`crate::PgProtocol::get_server_error`] →
/// `Result<&ErrorPayload, ArenaError>`.
///
/// # Tier-1 ZeroizeOnDrop enforcement
///
/// DEF-205 audit (2026-05-07): the struct derives
/// [`zeroize::ZeroizeOnDrop`] explicitly. Every field MUST
/// implement [`zeroize::Zeroize`] or carry `#[zeroize(skip)]` —
/// adding a new non-zeroize-aware field is a build error. Pre-
/// elevation, the three `SecretBoundedStr<N>` fields each had
/// individual `Drop` impls (so the auto-derived struct Drop
/// chained through them correctly), but a future contributor
/// adding e.g. `pub server_session_id: BoundedStr<32>` (without
/// the `Secret` prefix) would silently bypass scrubbing — server
/// error context can carry SQL fragments and other forensic
/// material that operators do NOT want lingering in freed memory
/// after error-arena reuse. Post-elevation, the contributor must
/// explicitly choose: zeroize-aware type or `#[zeroize(skip)]`
/// annotation with rationale.
#[derive(Debug, Clone, PartialEq, Eq, zeroize::ZeroizeOnDrop)]
pub struct ErrorPayload {
    /// Server-provided human-readable error message (M field per
    /// PG §55.7 ErrorResponse). Truncated at 128 bytes with `"…"`
    /// marker if longer.
    pub message: SecretBoundedStr<128>,
    /// Optional detail string (D field). Often empty.
    pub detail: SecretBoundedStr<96>,
    /// Optional hint string (H field). Often empty.
    pub hint: SecretBoundedStr<64>,
}

// DEF-184 (audit #3 A-15): `ErrorPayload::empty()` and its `Default`
// impl DELETED. Post-A-06 `get()` returns `Result<&ErrorPayload,
// ArenaError>` with classified `Empty` / `Stale` variants; the
// "empty payload" PRODUCTION sentinel no longer has a call site
// (was previously used by the test helper `parse_and_resolve` via
// `.copied().unwrap_or_default()` — the exact silent-fallback
// pattern banned per CREDO §5 + user feedback_no_underscore_vars.md).
//
// A test-only named fallback `ErrorPayload::dead_for_test()` is
// provided below — mirror of `SchemaRef::dead_for_test` in
// schema_arena.rs. The name makes intent explicit: only exists to
// satisfy the `assert!(is_ok) + unwrap_or(fallback)` idiom demanded
// by the crate's `#[forbid(clippy::unwrap_used, clippy::panic)]`
// bundle. Production code `match`es the Result exhaustively.

#[cfg(test)]
impl ErrorPayload {
    /// Test-only dead-code fallback. Used exclusively inside
    /// `assert!(r.is_ok(), ...) + r.unwrap_or(dead_for_test())`
    /// — the assert fires loudly if the precondition breaks; the
    /// fallback is defensive dead code keeping the helper compiling
    /// under the crate-root `#[forbid(clippy::panic, ...)]` bundle.
    ///
    /// Mirrors `SchemaRef::dead_for_test` in schema_arena.rs.
    /// NOT production code — explicitly-named, `#[cfg(test)]`-gated.
    #[must_use]
    pub(crate) fn dead_for_test() -> Self {
        Self {
            message: SecretBoundedStr::<128>::new(),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        }
    }
}

/// Opaque handle into [`ErrorArena`]. 8 bytes (u8 slot-marker +
/// u32 generation + padding), niche-packed for `Option<ErrorRef>`
/// at the same size.
///
/// # Invariants
///
/// An `ErrorRef` is **only** constructed via [`ErrorArena::alloc`];
/// its `slot` is the constant [`SLOT_OCCUPIED_MARKER`] and its
/// `generation` matches the arena's counter at the moment of
/// allocation.
///
/// # Niche note
///
/// `slot: core::num::NonZeroU8` ensures `Option<ErrorRef>` niches to
/// 8 bytes via the 0 byte-pattern of the outer `None`. Single-slot
/// design doesn't need multi-slot indexing, but the NonZeroU8 field
/// preserves the niche invariant for `Option<ErrorRef>` storage.
///
/// # Generation width (DEF-184 A1+A13, audit #3 A-04)
///
/// `generation: u32` (not `u8`) — the u8 would wrap after 256
/// alloc/clear cycles, risking a stashed `ErrorRef` collision with
/// a new-alloc payload in a long-running connection (architect
/// audit A-04 — "silent wrong-payload read" class).
///
/// u32 pushes the wrap to 2³² (~4.3 G cycles) — architecturally
/// unreachable under any realistic connection lifetime. Cost:
/// `ErrorRef` grows from 2 B to 8 B (with padding), but `ErrorRef`
/// only lives inside `ProtocolError::ServerErrorResponse` (72 B
/// total), so the 6 B growth is absorbed by the existing
/// ProtocolError discriminant padding.
///
/// # `#[must_use]` (audit #3 A-07)
///
/// An `ErrorRef` obtained from a pattern destructure must either
/// be resolved (via `PgProtocol::get_server_error`) or explicitly
/// discarded with `let _ =`. Silent drop loses the only handle to
/// server message/detail/hint.
// DEF-184 (audit #4 P2-11): `Hash` removed. Pre-(P2-11) it was
// present via copy-paste from multi-slot SchemaRef where hashing
// matters for dedup sets. ErrorRef is single-slot and never used as
// a HashMap key internally or externally. Re-add on demand — with
// a concrete consumer landing in the same commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "ErrorRef is the sole handle to server error message/detail/hint; \
              resolve via PgProtocol::get_server_error before drop, or discard \
              explicitly with `let _ = ref;`"]
pub struct ErrorRef {
    /// Fixed marker = 1 (single slot). `NonZeroU8` for niche.
    slot: core::num::NonZeroU8,
    /// Arena generation at alloc time. Mismatch = stale.
    ///
    /// DEF-184 (audit #3 A-04): widened u8 → u32 to eliminate
    /// 256-cycle wrap collision risk in long-running connections.
    generation: u32,
}

/// Classified failure from [`ErrorArena::get`] / [`crate::PgProtocol::get_server_error`].
///
/// DEF-184 (audit #3 A-06): tier-3 elevation over the pre-audit
/// `Option<&ErrorPayload>` return. The two failure modes — empty
/// slot vs stale generation — were previously collapsed into
/// `None`; callers receiving `None` had no signal to distinguish
/// "I deferred resolution too long" (Stale, expected) from "arena
/// state is inconsistent" (Empty, architecturally unreachable
/// outside of `unsafe`).
///
/// # Variant shape
///
/// `#[non_exhaustive]` reserved for future failure classes (e.g.
/// poisoning on a multi-slot refactor). Users must carry a `_ =>`
/// catch-all in match expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum ArenaError {
    /// Arena's slot is `None` while the ref's generation matches the
    /// arena's current generation. Architecturally unreachable under
    /// `#[forbid(unsafe_code)]` because `ErrorRef` construction is
    /// confined to `ErrorArena::alloc` which always sets
    /// `slot = Some(payload)`. Classified explicitly rather than
    /// silently papered over.
    Empty = 0,
    /// `ErrorRef.generation != ErrorArena.generation` — the ref was
    /// issued in an earlier cycle. Expected signal for callers that
    /// deferred resolution past an entry-point boundary (post-
    /// `clear_terminal_row_desc_if_idle_or_errored`) or past a subsequent `alloc`
    /// in a refactor that violates single-inflight invariant.
    Stale = 1,
}

impl core::fmt::Display for ArenaError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => f.write_str("error arena slot is empty (architecturally unreachable)"),
            Self::Stale => f.write_str("error arena ref is stale (generation mismatch — arena cleared or reallocated)"),
        }
    }
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the arena-lookup error sentinel.
impl core::error::Error for ArenaError {}

// Niche-pack pin: Option<ArenaError> stays 1 byte via the 254 unused
// u8 discriminants (`#[repr(u8)]` with 2 C-like variants).
const _: () = assert!(
    core::mem::size_of::<ArenaError>() == 1,
    "ArenaError must stay 1 byte (#[repr(u8)] + C-like variants). \
     If a future variant carries a payload, Result<&T, ArenaError> \
     layout changes cascade into the public get_server_error API \
     surface — audit first.",
);
const _: () = assert!(
    core::mem::size_of::<Option<ArenaError>>() == 1,
    "Option<ArenaError> must niche-pack via unused discriminant range",
);

#[cfg(test)]
impl ErrorRef {
    /// DEF-184 (audit #4 P1-6): test-only forgery hook.
    ///
    /// Constructs an `ErrorRef` with an arbitrary generation and
    /// the SLOT_OCCUPIED_MARKER slot — exclusively for exercising
    /// the [`ArenaError::Empty`] arm in [`ErrorArena::get`], which
    /// is architecturally unreachable via public API (alloc() is
    /// the only constructor and always populates `slot`).
    ///
    /// Without this hook, the `Empty` arm has zero arm-body shield
    /// coverage — a swap `None => Err(ArenaError::Stale)` for
    /// `None => Err(ArenaError::Empty)` compiles silent, and
    /// operators wondering why their arena says "Stale" when the
    /// gen matches would have no test pinning the classification.
    ///
    /// Return type `ErrorRef` already carries `#[must_use]` at the
    /// type level (A-07) — no redundant fn-level attribute needed.
    pub(crate) const fn forge_for_test(generation: u32) -> Self {
        Self {
            slot: SLOT_OCCUPIED_MARKER,
            generation,
        }
    }
}

/// Fixed marker for the single-slot arena. Used as the `slot` field
/// value on every [`ErrorRef`] issued by this arena.
///
/// `NonZeroU8::MIN == 1` by type definition — no match-fallback
/// needed (contrast schema_arena.rs which uses `NonZeroU8::new(idx +
/// 1)` for multi-slot indexing). Single-slot arena doesn't index
/// by slot, so this constant is purely for niche preservation of
/// `Option<ErrorRef>`.
const SLOT_OCCUPIED_MARKER: core::num::NonZeroU8 = core::num::NonZeroU8::MIN;

/// Single-slot error-payload arena on `PgProtocol`.
///
/// See module docstring for full design. One `Option<ErrorPayload>`
/// slot + one `u32 gen` counter + padding ≈ 293 B per arena. Cleared at each
/// entry-point when state is Idle/Errored.
#[derive(Debug)]
pub(crate) struct ErrorArena {
    /// DEF-185 P2-G (audit 2026-04-24): counter of
    /// `alloc_while_occupied` events — incremented each time `alloc`
    /// overwrites a previously-occupied slot. Architecturally dead
    /// under current single-inflight state machine (`parse_error_response`
    /// fires at most once per feed_bytes call, and the
    /// arena is cleared at entry-point boundaries before the next
    /// cycle). Documented 1c-5 blocker for pipelining support.
    ///
    /// Until pipelining lands this counter is monotonically zero;
    /// operators investigating anomalies can use a non-zero value as
    /// a protocol-layer canary. `saturating_add` keeps the counter at
    /// `u16::MAX` rather than wrapping. `u16` rather than `u32`
    /// because overflow would require 65k+ classified-dead events per
    /// connection — a clear protocol break not diluted by pin
    /// widening.
    overwrite_count: u16,
    /// `None` = free, `Some(payload)` = occupied. Populated only
    /// by [`alloc`]; reset to `None` by [`clear`].
    slot: Option<ErrorPayload>,
    /// Monotonically-bumped counter. Incremented on EVERY
    /// [`alloc`] and on [`clear`] when the slot was occupied —
    /// defence-in-depth (DEF-184 audit #3 A-13): even if a future
    /// refactor accidentally violates the "at most one alloc per
    /// feed_bytes" invariant maintained by the dispatch state
    /// machine, any prior-issued `ErrorRef` resolves via
    /// generation mismatch (tier-2 classified None) rather than
    /// silent wrong-payload read.
    ///
    /// Width u32 (not u8, DEF-184 audit #3 A-04): wrap at 2³² is
    /// architecturally unreachable under any realistic connection
    /// lifetime.
    generation: u32,
}

impl ErrorArena {
    /// Construct an empty arena (free slot, gen 0).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slot: None,
            generation: 0,
            overwrite_count: 0,
        }
    }

    /// DEF-185 P2-G: count of architecturally-dead "alloc while slot
    /// was occupied" events. Zero under correct dispatch-layer
    /// invariants (single-inflight state machine clears the arena
    /// before each new cycle). A non-zero value signals a protocol-
    /// layer invariant break — operator-facing canary.
    ///
    /// Surfaced via [`crate::PgProtocol::error_arena_overwrite_count`]
    /// so wrappers can expose the canary in their health checks.
    #[inline]
    #[must_use]
    pub(crate) const fn overwrite_count(&self) -> u16 {
        self.overwrite_count
    }

    /// Allocate the slot for `payload`, returning a handle capturing
    /// the current generation.
    ///
    /// DEF-184 (audit #3 A-13): bumps generation on EVERY call
    /// (not just when replacing an occupied slot). Defence-in-
    /// depth against any future dispatch refactor that might
    /// accidentally fire `parse_error_response` twice in one
    /// feed_bytes cycle — a prior-issued ErrorRef then resolves
    /// to None via gen mismatch instead of matching the new
    /// payload silently.
    ///
    /// `wrapping_add` permitted by forbid-bundle (no panic).
    /// Width u32 (A-04) makes wrap at 2³² architecturally
    /// unreachable under realistic connection lifetimes.
    ///
    /// Return type `ErrorRef` already carries `#[must_use = "..."]`
    /// at the type level (audit #3 A-07) — no redundant fn-level
    /// `#[must_use]` needed (clippy::double_must_use).
    #[inline]
    pub(crate) fn alloc(&mut self, payload: ErrorPayload) -> ErrorRef {
        // DEF-185 P2-G (audit 2026-04-24): bump overwrite_count if
        // slot was occupied. Architecturally dead under current
        // single-inflight invariants, but surface as canary.
        if self.slot.is_some() {
            self.overwrite_count = self.overwrite_count.saturating_add(1);
        }
        self.generation = self.generation.wrapping_add(1);
        self.slot = Some(payload);
        ErrorRef {
            slot: SLOT_OCCUPIED_MARKER,
            generation: self.generation,
        }
    }

    /// Read the payload at `r`.
    ///
    /// DEF-184 (audit #3 A-06): tier-3 classified Result replaces
    /// the pre-audit `Option<&ErrorPayload>`.
    ///
    /// Returns:
    /// - `Ok(&ErrorPayload)` — ref resolves; generation matches and
    ///   slot is populated.
    /// - `Err(ArenaError::Stale)` — ref was issued before a clear
    ///   or a subsequent alloc bumped the generation.
    /// - `Err(ArenaError::Empty)` — generation matches but slot is
    ///   `None`. Architecturally unreachable outside of `unsafe`
    ///   (ErrorRef construction requires a populated slot); surfaced
    ///   explicitly so callers never silently fabricate a payload.
    #[inline]
    pub(crate) fn get(&self, r: ErrorRef) -> Result<&ErrorPayload, ArenaError> {
        if r.generation != self.generation {
            return Err(ArenaError::Stale);
        }
        match self.slot.as_ref() {
            Some(payload) => Ok(payload),
            None => Err(ArenaError::Empty),
        }
    }

    /// Release the slot. Bumps generation unconditionally so
    /// subsequent [`get`] on any outstanding ref classifies as
    /// [`ArenaError::Stale`].
    ///
    /// Called by [`crate::PgProtocol::clear_terminal_row_desc_if_idle_or_errored`]
    /// at entry-point boundaries when the prior state is Idle or
    /// Errored.
    ///
    /// DEF-184 (audit #4 P1-3): bump is unconditional for symmetry
    /// with `alloc()` (A-13 defence-in-depth). Pre-(P1-3) the bump
    /// was guarded by `if self.slot.is_some()` — a seam that a
    /// future test-swap (`if self.slot.is_none()`) would silently
    /// invert, letting a stashed ErrorRef resolve across a
    /// no-op-clear boundary. Post-(P1-3) both alloc and clear bump
    /// every call; the single `wrapping_add` on u32 is 1 cycle,
    /// negligible on a cold error-entry-point path.
    ///
    /// `wrapping_add` permitted by forbid-bundle (no panic);
    /// u32 wrap at 2³² (A-04) architecturally unreachable.
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.slot = None;
    }

    // DEF-184 (audit #3 A-09): `is_occupied` deleted — was dead
    // code with deferred-justification `#[expect(dead_code)]`,
    // banned per CREDO §5 "сделаем потом" + no-dead-code feedback.
    // If a future diagnostic / telemetry site lands, reintroduce
    // the method in the same commit as the caller.
}

impl Default for ErrorArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------
// DEF-184 (audit #3 A-02) — Display-with-arena adapter.
// ---------------------------------------------------------------------

/// Display wrapper that resolves a [`ProtocolError::ServerErrorResponse`]'s
/// arena-backed strings inline.
///
/// Constructed via [`crate::PgProtocol::display_error`]. The adapter
/// borrows both the error and the arena; the borrow lifetime is the
/// shorter of the two (always `&self` of `PgProtocol`).
///
/// # Rendering
///
/// - `ServerErrorResponse` with resolvable arena ref:
///   `"server error: SEVERITY (SQLSTATE) — message; detail: <detail>;
///   hint: <hint>"`. Empty detail/hint sections are suppressed.
/// - `ServerErrorResponse` with unresolvable ref:
///   `"server error: SEVERITY (SQLSTATE) [arena ref unresolved:
///   <ArenaError>]"` — honest diagnostic over silent empty-string.
/// - Other variants: delegates to [`ProtocolError`]'s built-in
///   `Display` impl verbatim (no UX change).
///
/// # Lifetime budget
///
/// Single `'a` = `&self` of `PgProtocol`. The error pointer is
/// separately borrowed but typically lives at the same scope
/// (operator formats `proto.display_error(&err)` synchronously).
///
/// # DEF-184 (audit #4 P2-7, P2-8) shape choices
///
/// - `#[non_exhaustive]`: future fields (e.g. locale / tz /
///   redaction flags) can land without a breaking change.
/// - No `Clone` / `Copy`: single-use adapter — the intended pattern
///   is `log::error!("{}", proto.display_error(&err));`. Allowing
///   `Copy` would tempt stashing adapters that outlive the borrow
///   (architecturally impossible here thanks to the `'a` lifetime,
///   but removing the derive removes the temptation).
#[derive(Debug)]
#[non_exhaustive]
pub struct DisplayError<'a> {
    err: &'a crate::error::ProtocolError,
    arena: &'a ErrorArena,
}

impl<'a> DisplayError<'a> {
    #[inline]
    #[must_use]
    pub(crate) fn new(err: &'a crate::error::ProtocolError, arena: &'a ErrorArena) -> Self {
        Self { err, arena }
    }
}

impl core::fmt::Display for DisplayError<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        use crate::error::ProtocolError;
        match self.err {
            ProtocolError::ServerErrorResponse {
                severity,
                code,
                details_ref,
            } => {
                write!(f, "server error: {severity} ({code})")?;
                match self.arena.get(*details_ref) {
                    Ok(payload) => {
                        let message = payload.message.as_str();
                        if !message.is_empty() {
                            write!(f, " — {message}")?;
                        }
                        let detail = payload.detail.as_str();
                        if !detail.is_empty() {
                            write!(f, "; detail: {detail}")?;
                        }
                        let hint = payload.hint.as_str();
                        if !hint.is_empty() {
                            write!(f, "; hint: {hint}")?;
                        }
                        Ok(())
                    }
                    Err(e) => {
                        // Arena miss — classified diagnostic rather
                        // than silent empty-string. Stale is
                        // expected after clear_arena boundary; Empty
                        // signals crate bug (architecturally
                        // unreachable outside unsafe).
                        write!(f, " [arena ref unresolved: {e}]")
                    }
                }
            }
            // Other variants: delegate verbatim to the built-in
            // Display impl. No regression class here — only
            // ServerErrorResponse had arena-backed strings.
            other => core::fmt::Display::fmt(other, f),
        }
    }
}

// ---------------------------------------------------------------------
// Drift pins — DEF-184 A1 invariant guardrails.
// ---------------------------------------------------------------------

// Size pin: ErrorRef is 8 bytes post-(audit #3 A-04): u32 generation +
// NonZeroU8 slot marker + 3 B struct padding (u32 alignment).
// Pre-audit was 2 B (NonZeroU8 + u8); widened for wrap-safety.
const _: () = assert!(
    core::mem::size_of::<ErrorRef>() == 8,
    "ErrorRef should be 8 bytes (NonZeroU8 slot + u32 generation + \
     padding). If changed, update ServerErrorResponse.details_ref \
     budget + PgProtocol.error_arena footprint estimate in \
     error_arena.rs docs.",
);

// DEF-184 (audit #4 P0-2): exact-equality pin on Option<ErrorRef>.
//
// Pre-(P0-2) this was a relative pin (`size_of::<Option<ErrorRef>>()
// == size_of::<ErrorRef>()`), which is a weak shield — a non-niche
// field added to ErrorRef can coincidentally keep the relation while
// regressing footprint (both sides grow in lockstep). Exact pin
// catches absolute-size drift independent of the relative invariant.
//
// The two pins together shield:
//   • Absolute size drift on ErrorRef itself (pin above): 8 B exact.
//   • Absolute size drift on Option<ErrorRef>: 8 B exact (this pin).
//   • Niche collapse (if Option ever stops niche-packing, this would
//     trip via the 8 vs 16 byte budget — Option<{u32,u8,padding}>
//     without niche would be 12..16 B).
const _: () = assert!(
    core::mem::size_of::<Option<ErrorRef>>() == 8,
    "Option<ErrorRef> must be exactly 8 bytes — niche-packed via the \
     NonZeroU8 slot field. If this trips: (a) a non-niche field was \
     added to ErrorRef and the Option discriminant no longer niches, \
     or (b) the NonZeroU8 slot was converted to a plain u8. Restore \
     single-NonZero or add explicit repr(C) + manual discriminant.",
);

#[cfg(test)]
mod drop_witness_tests {
    //! DEF-259 (2026-05-08): tier-1-by-construction Drop-fire witness
    //! for [`ErrorPayload`] via [`crate::drop_witness::DropCounter`].
    //!
    //! Pre-DEF-259: `ErrorPayload` Drop was verified by the
    //! `#[ignore]`-gated memory-probe tests in
    //! `tests/error_arena_staleness_spec.rs::def205_error_payload_drop_zeroizes_all_fields`
    //! and `def205_error_payload_overwrite_zeroizes_old_value`. Both
    //! run only via `cargo test -- --ignored` or `cargo miri test`.
    //!
    //! Post-DEF-259: this test runs deterministically on every
    //! `cargo test` invocation. The `DropCounter<ErrorPayload>` wrapper
    //! observes that the `ZeroizeOnDrop` derive's body fires,
    //! transitively scrubbing each `SecretBoundedStr<N>` field (whose
    //! own Drop impl runs `.zeroize_in_place()` on the inner buffer).

    use super::ErrorPayload;
    use crate::drop_witness::{DropCounter, DropProbe};
    use crate::ident::SecretBoundedStr;

    /// `ErrorPayload::drop` fires the `ZeroizeOnDrop`-derived body.
    /// Counter increments iff the drop body was reached.
    #[test]
    fn error_payload_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let payload = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("error-witness"),
            detail: SecretBoundedStr::<96>::from_str_truncating("detail"),
            hint: SecretBoundedStr::<64>::from_str_truncating("hint"),
        };
        {
            let _w = DropCounter::new(payload, probe.clone());
            assert_eq!(probe.fired(), 0);
        }
        assert_eq!(
            probe.fired(),
            1,
            "ErrorPayload drop must fire exactly once on scope exit",
        );
    }

    /// `ErrorPayload` overwrite fires Drop on the old payload — the
    /// `ErrorArena::alloc()` reuse path. Counter increments equal the
    /// number of overwrites + the final scope-exit drop.
    #[test]
    fn error_payload_overwrite_fires_drop_on_old_value() {
        let probe = DropProbe::new();
        let mut wrapper = DropCounter::new(
            ErrorPayload {
                message: SecretBoundedStr::<128>::from_str_truncating("first"),
                detail: SecretBoundedStr::<96>::new(),
                hint: SecretBoundedStr::<64>::new(),
            },
            probe.clone(),
        );
        assert_eq!(probe.fired(), 0, "wrapper alive — counter is 0");

        // Overwrite the wrapper. Rust drops the OLD wrapper before
        // moving the new one in; counter increments by 1 for the
        // OLD wrapper's Drop. `core::mem::replace` is the explicit
        // form — gives us back the OLD value (which we drop
        // immediately by binding to `_old`) and pins that the
        // assignment is observable. Plain `wrapper = ...` triggers
        // `unused_assignments` because the LATER drop(wrapper) is
        // the only read of the new value.
        let _old = core::mem::replace(
            &mut wrapper,
            DropCounter::new(
                ErrorPayload {
                    message: SecretBoundedStr::<128>::from_str_truncating("second"),
                    detail: SecretBoundedStr::<96>::new(),
                    hint: SecretBoundedStr::<64>::new(),
                },
                probe.clone(),
            ),
        );
        // `_old` is the OLD wrapper; it drops at end of statement
        // (or here on the next line via explicit drop). Counter
        // should increment to 1.
        drop(_old);
        assert_eq!(
            probe.fired(),
            1,
            "overwrite (via mem::replace + drop _old) must drop the \
             old wrapper (counter == 1)",
        );

        drop(wrapper);
        assert_eq!(
            probe.fired(),
            2,
            "final drop must increment counter to 2",
        );
    }
}

#[cfg(test)]
mod tests {
    //! Forbid-bundle compliance: `panic!`, `.unwrap()`, `.expect()`,
    //! `unreachable!()`, and `assert!(false)` are banned crate-wide
    //! (including unit tests). Tests below use the
    //! `assert!(is_ok/matches!, ...) + if-let-Ok` idiom or the
    //! `.unwrap_or(ErrorPayload::dead_for_test())` fallback — the
    //! `assert!` fires loudly if the precondition breaks; the
    //! `if let` / `unwrap_or` landing pad is defensive dead code
    //! keeping the test compiling. Same pattern as
    //! `schema_arena::tests::must_alloc` + `decode::data_row_ref_tests`.
    use super::*;

    #[test]
    fn alloc_then_get_returns_payload() {
        let mut arena = ErrorArena::new();
        let payload = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("boom"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r = arena.alloc(payload);
        let got = arena.get(r);
        assert!(got.is_ok(), "alloc'd ref must resolve, got {got:?}");
        if let Ok(payload_ref) = got {
            assert_eq!(payload_ref.message.as_str(), "boom");
        }
    }

    #[test]
    fn get_after_clear_classifies_as_stale() {
        let mut arena = ErrorArena::new();
        let payload = ErrorPayload {
            message: SecretBoundedStr::<128>::new(),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r = arena.alloc(payload);
        arena.clear();
        assert!(
            matches!(arena.get(r), Err(ArenaError::Stale)),
            "post-clear ref must classify as Stale, got {:?}",
            arena.get(r),
        );
    }

    #[test]
    fn overwrite_count_tracks_double_alloc() {
        // DEF-185 P2-G (audit 2026-04-24): the `overwrite_count`
        // canary counts "alloc while slot was occupied" events.
        // Architecturally dead under single-inflight invariants; a
        // non-zero value in production would signal a dispatch-layer
        // break.
        let mut arena = ErrorArena::new();
        assert_eq!(arena.overwrite_count(), 0, "pristine arena starts at zero");

        let p1 = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("first"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r1 = arena.alloc(p1);
        // Validate the ref resolves before the second alloc.
        assert!(arena.get(r1).is_ok(), "r1 must resolve on fresh alloc");
        assert_eq!(
            arena.overwrite_count(),
            0,
            "first alloc on empty slot must not bump overwrite_count",
        );

        let p2 = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("second"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r2 = arena.alloc(p2);
        // r1 is now stale (generation bumped); r2 resolves.
        assert!(arena.get(r1).is_err(), "r1 must be Stale after r2 alloc");
        assert!(arena.get(r2).is_ok(), "r2 must resolve on post-overwrite alloc");
        assert_eq!(
            arena.overwrite_count(),
            1,
            "second alloc while slot occupied must bump overwrite_count to 1",
        );

        // Return-type pin: accessor is u16 (not usize / u32) — keeps
        // the canary at single-byte ABI after niche packing.
        let count: u16 = arena.overwrite_count();
        assert_eq!(count, 1, "second read must still return 1");
    }

    #[test]
    fn alloc_overwrites_previous_and_bumps_generation() {
        let mut arena = ErrorArena::new();
        let p1 = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("first"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r1 = arena.alloc(p1);
        let p2 = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("second"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r2 = arena.alloc(p2);
        // r1 should be stale (generation mismatch).
        assert!(
            matches!(arena.get(r1), Err(ArenaError::Stale)),
            "old ref must be Stale, got {:?}",
            arena.get(r1),
        );
        // r2 resolves the new payload.
        let got = arena.get(r2);
        assert!(got.is_ok(), "fresh ref must resolve, got {got:?}");
        if let Ok(payload_ref) = got {
            assert_eq!(payload_ref.message.as_str(), "second");
        }
    }

    #[test]
    fn clear_bumps_generation_unconditionally() {
        // DEF-184 (audit #4 P1-3): clear() bumps generation whether
        // or not the slot was occupied. Alloc + clear on a pristine
        // arena reaches generation=2; a ref issued after the clear
        // cycle is distinguishable from a ref issued before it.
        //
        // Without unconditional bump, a rare idle-clear sequence
        // (clear without preceding alloc) leaves generation at 0,
        // and a later alloc's ref (generation=1) could collide with
        // a hypothetically-stashed ref issued during the crate's
        // bootstrap. Zero-cost defence-in-depth.
        let mut arena = ErrorArena::new();
        // Empty clear — bumps to 1.
        arena.clear();
        let payload = ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("after"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        // Alloc — bumps to 2.
        let r = arena.alloc(payload);
        // Second clear — bumps to 3.
        arena.clear();
        // r was issued at gen=2, arena now at gen=3: classify Stale.
        assert!(
            matches!(arena.get(r), Err(ArenaError::Stale)),
            "post-clear ref must be Stale (unconditional bump), got {:?}",
            arena.get(r),
        );
    }

    #[test]
    fn prior_arena_ref_classifies_as_stale_on_fresh_arena() {
        // Cross-arena collision class: arena A emits ref at its
        // generation; fresh arena B resolves the ref → Stale
        // (gen mismatch), never Empty. Pins the gen-mismatch arm
        // against a would-be body swap.
        let mut a = ErrorArena::new();
        let p = ErrorPayload {
            message: SecretBoundedStr::<128>::new(),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        };
        let r = a.alloc(p);
        let b = ErrorArena::new();
        // Fresh arena has generation=0; r has generation=1 — mismatch.
        assert!(matches!(b.get(r), Err(ArenaError::Stale)));
    }

    #[test]
    fn forged_ref_with_matching_gen_but_empty_slot_classifies_as_empty() {
        // DEF-184 (audit #4 P1-6): exercise ArenaError::Empty arm
        // directly. Without this test, swapping
        //   `None => Err(ArenaError::Empty)` ↔ `None => Err(ArenaError::Stale)`
        // in `get()` compiles silent and operators see "Stale" on a
        // gen-match arena — misdirecting diagnostics to "I cleared
        // this" when the actual condition is "slot was never populated".
        //
        // Forges an ErrorRef at the arena's current generation via
        // the #[cfg(test)] hook `ErrorRef::forge_for_test`, then
        // asserts the empty-slot path returns Err(Empty), not
        // Err(Stale). Architecturally unreachable via public API
        // (ErrorArena::alloc always populates slot); the forgery
        // hook is the only way to reach the arm — same pattern as
        // SchemaRef::dead_for_test in schema_arena.rs.
        let arena = ErrorArena::new();
        // Fresh arena: generation=0, slot=None. Forge a matching-
        // generation ref pointing at the empty slot.
        let r = ErrorRef::forge_for_test(0);
        assert!(
            matches!(arena.get(r), Err(ArenaError::Empty)),
            "forged matching-gen ref on empty slot must classify Empty, got {:?}",
            arena.get(r),
        );
    }

    #[test]
    fn forged_ref_with_stale_gen_classifies_stale_not_empty() {
        // Symmetric shield: if slot is empty AND gen mismatches,
        // the gen-mismatch check MUST precede the slot check.
        // This pins the check order: `r.generation != self.generation`
        // returns Stale before the `self.slot.as_ref()` path.
        let arena = ErrorArena::new();
        // Fresh arena: generation=0. Forge stale-gen ref.
        let r = ErrorRef::forge_for_test(99);
        assert!(
            matches!(arena.get(r), Err(ArenaError::Stale)),
            "forged stale-gen ref (even on empty slot) must classify Stale, got {:?}",
            arena.get(r),
        );
    }

    #[test]
    fn option_errorref_niche_packed() {
        assert_eq!(
            core::mem::size_of::<Option<ErrorRef>>(),
            core::mem::size_of::<ErrorRef>(),
        );
    }

    #[test]
    fn arena_error_is_one_byte() {
        // Colocated with const-assert above — runtime test gives a
        // second witness in the test report.
        assert_eq!(core::mem::size_of::<ArenaError>(), 1);
        assert_eq!(core::mem::size_of::<Option<ArenaError>>(), 1);
    }

    // DEF-184 (audit #3 A-02): DisplayError adapter behavioural
    // coverage. Closes the "Display arm order swap" shield gap:
    // reversing message / detail / hint in the fmt body compiles
    // silently and operators see the wrong field in logs.

    extern crate alloc;
    use alloc::format;

    #[test]
    fn display_error_renders_full_text_when_ref_resolves() {
        use crate::error::{ProtocolError, Severity, SqlStateCode};
        let mut arena = ErrorArena::new();
        let details_ref = arena.alloc(ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("password authentication failed"),
            detail: SecretBoundedStr::<96>::from_str_truncating("user 'alice' not found"),
            hint: SecretBoundedStr::<64>::from_str_truncating("check pg_hba.conf"),
        });
        let err = ProtocolError::ServerErrorResponse {
            severity: Severity::Fatal,
            code: SqlStateCode::from_bytes(b"28P01"),
            details_ref,
        };
        let adapter = DisplayError::new(&err, &arena);
        let rendered = format!("{adapter}");
        assert_eq!(
            rendered,
            "server error: FATAL (28P01) — password authentication failed; \
             detail: user 'alice' not found; hint: check pg_hba.conf",
        );
    }

    #[test]
    fn display_error_suppresses_empty_detail_and_hint() {
        use crate::error::{ProtocolError, Severity, SqlStateCode};
        let mut arena = ErrorArena::new();
        let details_ref = arena.alloc(ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("boom"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        });
        let err = ProtocolError::ServerErrorResponse {
            severity: Severity::Error,
            code: SqlStateCode::from_bytes(b"42000"),
            details_ref,
        };
        let adapter = DisplayError::new(&err, &arena);
        let rendered = format!("{adapter}");
        assert_eq!(rendered, "server error: ERROR (42000) — boom");
    }

    #[test]
    fn display_error_emits_arena_miss_diagnostic_when_ref_stale() {
        use crate::error::{ProtocolError, Severity, SqlStateCode};
        let mut arena = ErrorArena::new();
        let details_ref = arena.alloc(ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("orig"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        });
        // Clear the arena — ref becomes stale. (Operator scenario:
        // deferred diagnostic log past an entry-point boundary.)
        arena.clear();
        let err = ProtocolError::ServerErrorResponse {
            severity: Severity::Error,
            code: SqlStateCode::from_bytes(b"XX000"),
            details_ref,
        };
        let adapter = DisplayError::new(&err, &arena);
        let rendered = format!("{adapter}");
        assert!(
            rendered.contains("server error: ERROR (XX000) [arena ref unresolved:"),
            "stale ref must produce classified-miss diagnostic, got: {rendered}",
        );
        assert!(
            rendered.contains("stale"),
            "miss diagnostic must identify Stale class, got: {rendered}",
        );
    }

    #[test]
    fn built_in_display_advisory_points_to_adapter() {
        // DEF-184 (audit #4 P0-1): the base `ProtocolError::Display`
        // impl MUST emit the grep-able advisory tag pointing operators
        // at `PgProtocol::display_error` — not silently emit an empty
        // string or fake "details" content. This is the tier-2 shield
        // for non-adapter log / panic / thiserror-source sites.
        //
        // Pre-(P0-1) Display emitted `"[details in ErrorArena]"`
        // which mimicked presence of details while silently hiding
        // them — tier-4 silent-degradation. Post-(P0-1) the advisory
        // is explicit.
        use crate::error::{ProtocolError, Severity, SqlStateCode};
        let mut arena = ErrorArena::new();
        let details_ref = arena.alloc(ErrorPayload {
            message: SecretBoundedStr::<128>::from_str_truncating("boom"),
            detail: SecretBoundedStr::<96>::new(),
            hint: SecretBoundedStr::<64>::new(),
        });
        let err = ProtocolError::ServerErrorResponse {
            severity: Severity::Fatal,
            code: SqlStateCode::from_bytes(b"28P01"),
            details_ref,
        };
        // Built-in Display — deliberately NOT going through the
        // adapter. Must contain severity, code, and advisory tag.
        let rendered = format!("{err}");
        assert!(
            rendered.contains("FATAL"),
            "built-in Display must retain severity, got: {rendered}",
        );
        assert!(
            rendered.contains("28P01"),
            "built-in Display must retain SQLSTATE, got: {rendered}",
        );
        assert!(
            rendered.contains("PgProtocol::display_error"),
            "built-in Display must advise operators to use the adapter for full text, \
             got: {rendered}",
        );
        // Critically: must NOT contain the message — that's the
        // adapter's job. If Display ever started resolving the arena
        // inline, it would need the arena borrow which isn't available
        // from `&self`.
        assert!(
            !rendered.contains("boom"),
            "built-in Display must NOT resolve arena strings (no arena in scope), \
             got: {rendered}",
        );
    }

    #[test]
    fn display_error_delegates_for_non_server_variants() {
        use crate::error::{CrateBugLocus, ProtocolError};
        // Any non-ServerErrorResponse variant exercises the
        // fall-through branch. InternalCrateBug is convenient — its
        // Display has a stable contract pinned in other tests.
        // DEF-188: switched from `StaleSchemaRef` (deleted with the
        // schema arena) to `ReadCursorAdvance` (still extant). The
        // test only exercises the fallthrough branch — locus identity
        // is incidental.
        let err = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::ReadCursorAdvance,
        };
        let arena = ErrorArena::new();
        let adapter = DisplayError::new(&err, &arena);
        // Must match the plain Display exactly (no arena-specific
        // prefix / suffix) — adapter is a no-op for non-server variants.
        let rendered = format!("{adapter}");
        let plain = format!("{err}");
        assert_eq!(rendered, plain);
    }
}
