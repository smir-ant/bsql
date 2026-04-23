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
//! carries an [`ErrorRef`] handle (~3 B) instead of inline strings;
//! callers resolve via [`crate::PgProtocol::get_server_error`] to
//! get `&ErrorPayload`.
//!
//! **Cascade result:** `ProtocolError` shrinks 312 B → ~32 B;
//! `Action` shrinks ~5-8× (now Reply-bounded); `OutActions` stack
//! frame shrinks ~3.5-4×; `StreamItem` shrinks ~4×.
//!
//! # Single-slot design
//!
//! The arena holds a **single** `Option<ErrorPayload>` — not a multi-
//! slot slab like [`crate::schema_arena::SchemaSlab`]. Rationale:
//!
//! 1. **Single-inflight semantics (pre-1c-5).** Per feed_bytes /
//!    push_command cycle, at most ONE server error can reach the
//!    client (the state machine transitions to `Errored` on first
//!    ErrorResponse frame, blocking further dispatch). One slot
//!    suffices.
//! 2. **Simpler stale-ref model.** One `u8 gen` counter; alloc bumps
//!    gen + overwrites slot; get compares gen.
//! 3. **Smaller PgProtocol footprint.** One slot = ~288 B + 1 B gen.
//!    Multi-slot slab of size 2 would be ~576 B. Defer multi-slot
//!    until 1c-5 pipelining actually needs it.
//!
//! # Alloc / clear discipline (mirror of schema_arena.rs)
//!
//! - **Alloc** happens in dispatch.rs when parsing an `ErrorResponse`
//!   frame (`parse_and_alloc_server_error`): parsed bounded strings
//!   get stored in the arena; the returned [`ErrorRef`] threads into
//!   `ProtocolError::ServerErrorResponse { details_ref, ... }`.
//! - **Clear** happens at entry-point boundaries when prior state is
//!   `Idle` or `Errored` — alongside `SchemaSlab::clear()` in
//!   [`crate::PgProtocol::clear_arena_if_idle_or_errored`]. The next
//!   feed_bytes call starts with a fresh arena; any ErrorRef held
//!   past that boundary becomes stale (classified via generation).
//!
//! # Staleness classification
//!
//! [`ErrorArena::get`] returns `None` on gen mismatch (stale ref).
//! Per CREDO §1 tier elevation: stale is tier-2 structural (bounded
//! detection via typed generation counter), not tier-4 silent.
//! Callers (tests / wrapper crate) that see `None` from
//! `get_server_error` distinguish "no server error payload" (never
//! happened) from "stale ref" (consumed or cleared) by context.

use crate::ident::BoundedStr;

/// Full per-server-error payload — the three bounded strings that
/// used to live inline in `ProtocolError::ServerErrorResponse`.
///
/// Copy-POD: all fields are Copy; no heap indirection. The total
/// size is approximately 288 B (three `BoundedStr<N>` instances).
///
/// Users access via [`crate::PgProtocol::get_server_error`] →
/// `Option<&ErrorPayload>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorPayload {
    /// Server-provided human-readable error message (M field per
    /// PG §55.7 ErrorResponse). Truncated at 128 bytes with `"…"`
    /// marker if longer.
    pub message: BoundedStr<128>,
    /// Optional detail string (D field). Often empty.
    pub detail: BoundedStr<96>,
    /// Optional hint string (H field). Often empty.
    pub hint: BoundedStr<64>,
}

impl ErrorPayload {
    /// Construct an empty payload (all three strings empty). Used
    /// as the arena's uninitialised-slot sentinel and as the
    /// fallback for stale-ref resolution in the wrapper crate.
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            message: BoundedStr::<128>::new(),
            detail: BoundedStr::<96>::new(),
            hint: BoundedStr::<64>::new(),
        }
    }
}

impl Default for ErrorPayload {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

/// Opaque handle into [`ErrorArena`]. 2 bytes (u8 slot-marker +
/// u8 generation), niche-packed for `Option<ErrorRef>` at the same
/// size.
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
/// 2 bytes via the 0 byte-pattern of the outer `None`. Single-slot
/// design doesn't need multi-slot indexing, but the NonZeroU8 field
/// preserves the niche invariant for `Option<ErrorRef>` storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorRef {
    /// Fixed marker = 1 (single slot). `NonZeroU8` for niche.
    slot: core::num::NonZeroU8,
    /// Arena generation at alloc time. Mismatch = stale.
    generation: u8,
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
/// slot + one `u8 gen` counter = ~289 B per arena. Cleared at each
/// entry-point when state is Idle/Errored.
#[derive(Debug)]
pub(crate) struct ErrorArena {
    /// `None` = free, `Some(payload)` = occupied. Populated only
    /// by [`alloc`]; reset to `None` by [`clear`].
    slot: Option<ErrorPayload>,
    /// Bumped on [`alloc`] when overwriting a previously-occupied
    /// slot, and on [`clear`] when the slot was occupied. `u8` is
    /// safe in pre-1c-5 single-inflight (ErrorRef lives only while
    /// the Action/StreamItem carrying it is alive, which cannot
    /// survive across 256 clear cycles).
    generation: u8,
}

impl ErrorArena {
    /// Construct an empty arena (free slot, gen 0).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slot: None,
            generation: 0,
        }
    }

    /// Allocate the slot for `payload`, returning a handle capturing
    /// the current generation.
    ///
    /// Overwrites any prior payload (single-slot design). If the
    /// prior slot was occupied, the generation bumps so any
    /// out-of-date [`ErrorRef`] resolves to `None` via mismatch.
    #[inline]
    #[must_use]
    pub(crate) fn alloc(&mut self, payload: ErrorPayload) -> ErrorRef {
        if self.slot.is_some() {
            // Generation bump is only needed when we're REPLACING an
            // existing payload — an outstanding ErrorRef into the old
            // payload would otherwise match the new payload's gen.
            // `wrapping_add` permitted by forbid-bundle (no panic).
            self.generation = self.generation.wrapping_add(1);
        }
        self.slot = Some(payload);
        ErrorRef {
            slot: SLOT_OCCUPIED_MARKER,
            generation: self.generation,
        }
    }

    /// Read the payload at `r`, or `None` if the slot is free /
    /// the generation no longer matches (stale ref).
    #[inline]
    #[must_use]
    pub(crate) fn get(&self, r: ErrorRef) -> Option<&ErrorPayload> {
        if r.generation != self.generation {
            return None;
        }
        self.slot.as_ref()
    }

    /// Release the slot. Bumps generation if slot was occupied so
    /// subsequent [`get`] on any outstanding ref returns `None`.
    ///
    /// Called by [`crate::PgProtocol::clear_arena_if_idle_or_errored`]
    /// at entry-point boundaries when the prior state is Idle or
    /// Errored.
    #[inline]
    pub(crate) fn clear(&mut self) {
        if self.slot.is_some() {
            self.generation = self.generation.wrapping_add(1);
        }
        self.slot = None;
    }

    /// Whether the slot is currently occupied. Debug helper.
    ///
    /// Reserved for future diagnostic use (e.g. assertions in
    /// `PgProtocol::get_server_error` that surface pre-Errored
    /// ref issuance for wrapper-crate telemetry). Kept `pub(crate)`
    /// but temporarily `#[expect(dead_code)]` until such a site
    /// lands.
    #[cfg(any(test, debug_assertions))]
    #[inline]
    #[must_use]
    #[expect(dead_code, reason = "reserved for future diagnostic / telemetry use")]
    pub(crate) fn is_occupied(&self) -> bool {
        self.slot.is_some()
    }
}

impl Default for ErrorArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------
// Drift pins — DEF-184 A1 invariant guardrails.
// ---------------------------------------------------------------------

// Niche-pack invariant: Option<ErrorRef> must fit in the same byte
// count as ErrorRef itself, via the NonZeroU8 slot field niche.
const _: () = assert!(
    core::mem::size_of::<Option<ErrorRef>>()
        == core::mem::size_of::<ErrorRef>(),
    "Option<ErrorRef> must niche-pack into ErrorRef's size via the \
     NonZeroU8 slot niche. If this trips, a field was added to ErrorRef \
     that broke the niche; restore single-NonZero or add explicit \
     repr(C) + manual discriminant.",
);

// Size pin: ErrorRef is 2 bytes (NonZeroU8 + u8). Bumping to u16
// generation or multi-slot u8 index would double this; the drift-pin
// forces a deliberate review.
const _: () = assert!(
    core::mem::size_of::<ErrorRef>() == 2,
    "ErrorRef should be 2 bytes (NonZeroU8 slot + u8 generation). If \
     changed, update ServerErrorResponse.details_ref size budget + \
     PgProtocol.error_arena footprint estimate in error_arena.rs docs.",
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_then_get_returns_payload() {
        let mut arena = ErrorArena::new();
        let payload = ErrorPayload {
            message: BoundedStr::<128>::from_str_truncating("boom"),
            detail: BoundedStr::<96>::new(),
            hint: BoundedStr::<64>::new(),
        };
        let r = arena.alloc(payload);
        let got = arena.get(r);
        assert!(got.is_some(), "alloc'd ref must resolve");
        if let Some(got) = got {
            assert_eq!(got.message.as_str(), "boom");
        }
    }

    #[test]
    fn get_after_clear_returns_none_via_generation_mismatch() {
        let mut arena = ErrorArena::new();
        let payload = ErrorPayload::empty();
        let r = arena.alloc(payload);
        arena.clear();
        assert!(arena.get(r).is_none());
    }

    #[test]
    fn alloc_overwrites_previous_and_bumps_generation() {
        let mut arena = ErrorArena::new();
        let p1 = ErrorPayload {
            message: BoundedStr::<128>::from_str_truncating("first"),
            detail: BoundedStr::<96>::new(),
            hint: BoundedStr::<64>::new(),
        };
        let r1 = arena.alloc(p1);
        let p2 = ErrorPayload {
            message: BoundedStr::<128>::from_str_truncating("second"),
            detail: BoundedStr::<96>::new(),
            hint: BoundedStr::<64>::new(),
        };
        let r2 = arena.alloc(p2);
        // r1 should be stale (generation mismatch).
        assert!(arena.get(r1).is_none(), "old ref must be stale");
        // r2 resolves the new payload.
        assert!(arena.get(r2).is_some(), "fresh ref must resolve");
        if let Some(got) = arena.get(r2) {
            assert_eq!(got.message.as_str(), "second");
        }
    }

    #[test]
    fn option_errorref_niche_packed() {
        assert_eq!(
            core::mem::size_of::<Option<ErrorRef>>(),
            core::mem::size_of::<ErrorRef>(),
        );
    }
}
