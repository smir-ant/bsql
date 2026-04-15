//! Opaque correlator for in-flight commands.
//!
//! `bsql-pg-proto` is `no_std` and oblivious to async runtimes; it cannot
//! own `tokio::sync::oneshot::Sender`s itself. Instead, each
//! [`crate::PgCommand`] carries a `ReplyId` that the upstream wrapper
//! crate (`bsql-driver-postgres`, Phase 1e) uses as the key in a
//! `HashMap<ReplyId, oneshot::Sender<Reply>>`.
//!
//! # ID provenance
//!
//! `ReplyId` wraps a [`NonZeroU64`] for two reasons:
//!
//! 1. **Niche optimization.** `Option<ReplyId>` is 8 bytes, not 16.
//! 2. **No sentinel collision.** Zero is reserved as "no ID"; the
//!    constructor refuses it.
//!
//! `bsql-pg-proto` does **not** mint IDs. The wrapper crate runs a
//! per-connection monotonic counter starting at 1; collision-freedom is
//! the wrapper's responsibility. Per reforge.md §7.5, this is
//! **tier-3 by audit**: the cross-crate seal is not expressible in
//! stable Rust today. Mitigations:
//!
//! - The constructor takes `NonZeroU64` (zero impossible at the type
//!   level).
//! - Production wrappers must use a single fetch-add counter per
//!   connection. Reusing IDs across the same `PgProtocol` instance is
//!   undefined at the spec level (the protocol can deliver to the wrong
//!   sender), but cannot violate memory safety.
//! - `cargo-vet` / commit review flag any new constructor outside the
//!   sanctioned wrapper.

use core::fmt;
use core::num::NonZeroU64;

/// Opaque handle correlating a pushed command with its eventual reply.
///
/// Constructed by the wrapper crate from a per-connection monotonic
/// counter. The protocol crate ferries the value through; it never
/// inspects it, never compares it, never mints one of its own.
///
/// # Non-`Copy` by design
///
/// `ReplyId` wraps an 8-byte `NonZeroU64` — trivially copyable at the
/// machine-word level — but the type is deliberately *not* `Copy`.
/// Marking it `Copy` would make `match prev { AwaitingPingReply(id) =>
/// { /* forget id */ } }` a silent bit-copy; non-`Copy` downgrades
/// that misuse to a move-out-of-match, which an unused binding still
/// passes through but makes reviewer-visible.
///
/// Honest tier classification of the state-as-data invariant: moving
/// out of [`crate::ProtoState::AwaitingPingReply`] naming the `id` is
/// forced by the pattern, but the compiler does not enforce that the
/// named id flows into a [`crate::Action::DeliverReply`] /
/// [`crate::Action::FailReply`]. That step is **tier-2** — enforced by
/// the dispatcher's documented match arms and by review, not by the
/// type system. Making `ReplyId` non-`Copy` just keeps the reviewer
/// honest (a discarded move-out is visually apparent).
///
/// `Clone` is implemented because legitimate protocol paths in later
/// sub-phases occasionally echo an id back while retaining it (e.g.
/// error recovery that both reports failure and restores the pending
/// state). Phase 1a does not call `.clone()`.
#[expect(
    missing_copy_implementations,
    reason = "deliberately non-Copy to keep move-out-of-state-variant reviewer-visible; see docstring",
)]
#[derive(Clone, PartialEq, Eq, Hash)]
#[must_use = "a ReplyId without a registered sender will silently drop the reply"]
pub struct ReplyId(NonZeroU64);

impl ReplyId {
    /// Construct a `ReplyId` from a non-zero monotonic counter value.
    ///
    /// **Caller contract** (tier-3, audit-enforced): `value` must not
    /// have been used previously on the same `PgProtocol` instance.
    /// Reuse causes the protocol to deliver future replies to whichever
    /// sender is still registered under that ID — a logic error, not a
    /// memory-safety issue.
    ///
    /// The standard wrapper (`bsql-driver-postgres`) uses an
    /// `AtomicU64` initialised to 1 with `fetch_add(1, Relaxed)`. At one
    /// pull per nanosecond that lasts ~584 years, so wraparound is
    /// outside any realistic horizon.
    #[inline]
    pub const fn from_raw(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Extract the underlying counter value.
    ///
    /// Used by the wrapper to look up the matching `oneshot::Sender` in
    /// its pending-replies map.
    #[inline]
    #[must_use]
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl fmt::Debug for ReplyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplyId({})", self.0.get())
    }
}
