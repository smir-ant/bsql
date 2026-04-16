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
/// # Non-`Copy`, non-`Clone` by design — tier-1 non-duplication
///
/// `ReplyId` wraps an 8-byte `NonZeroU64` — trivially copyable at the
/// machine-word level — but the type deliberately implements **neither
/// `Copy` nor `Clone`**. Any attempt to duplicate it is a compile error.
///
/// Consequence, combined with the crate-root `#[deny(unused_variables)]`
/// and the architect.txt Part V ban on `let _ = expr;` and `_varname`
/// suppression: extracting an `id` from a [`crate::ProtoState`] match arm
/// forces the arm to *use* it (pass it into an
/// [`crate::Action::DeliverReply`] / [`crate::Action::FailReply`] payload,
/// or bind it to a further variable that is itself used). A match arm
/// that silently drops the id is a build failure, not an audit finding —
/// this is **tier-1 compile** for the "no silent reply loss" invariant.
///
/// The one path that *does* legitimately drop an unfinished id is
/// transport teardown (wrapper crash, connection error before reply):
/// the wrapper crate converts the dropped id into a classified
/// `TransportClosed` failure delivered to the caller's oneshot, one
/// layer above the protocol core. This crate never needs to clone.
#[expect(
    missing_copy_implementations,
    reason = "deliberately non-Copy + non-Clone: duplicating an id is a compile error — the tier-1 mechanism for reply-loss prevention",
)]
#[derive(PartialEq, Eq, Hash)]
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

    /// Extract the underlying counter value by reference.
    ///
    /// Takes `&self` — the contained `NonZeroU64` is `Copy`, so
    /// extracting the raw value does not consume the id. The
    /// non-duplication guarantee (see type-level docstring) comes from
    /// `ReplyId` itself being non-`Copy` / non-`Clone`; you still
    /// cannot reproduce a whole `ReplyId` from a raw value outside of
    /// this module's constructor.
    ///
    /// Used by the wrapper to look up the matching `oneshot::Sender` in
    /// its pending-replies map without moving the id out of the match
    /// arm that carries it.
    #[inline]
    #[must_use]
    pub const fn get(&self) -> NonZeroU64 {
        self.0
    }
}

impl fmt::Debug for ReplyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplyId({})", self.0.get())
    }
}
