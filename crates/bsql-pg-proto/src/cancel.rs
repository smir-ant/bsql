//! PostgreSQL §55.2.7 CancelRequest mechanism.
//!
//! Surfaces the `(pid, secret_key)` material captured at
//! `BackendKeyData` ('K') receipt during the handshake. The public
//! API is [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`],
//! a closure-scoped accessor that materialises the 16-byte wire
//! frame inside a [`zeroize::Zeroizing`] stack guard and lends a
//! `&[u8; 16]` to the caller. On closure return (Ok / Err / unwind
//! panic), the guard's `Drop` scrubs the bytes via `zeroize::Zeroize`.
//!
//! # Tier-1 by closure scope
//!
//! The closure-scoped API materialises the wire bytes on
//! `with_cancel_request`'s STACK inside a `Zeroizing<[u8; 16]>` guard.
//! The closure receives `&[u8; 16]` borrowed from that guard. The
//! guard is OWNED by the function frame — neither the closure nor
//! any outer scope can reach it. `mem::forget(guard)`,
//! `Box::leak(guard)`, `ManuallyDrop::new(guard)` are all unreachable
//! because the guard is not in scope where the caller writes code.
//! Retention of the `&[u8; 16]` itself is rejected at compile time:
//! the HRTB on `FnOnce(&[u8; 16], i32) -> R` quantifies over `'a`, so
//! the borrow cannot escape the call.
//!
//! A by-value `pub struct CancelRequestCredentials { pid, secret_key:
//! Sensitive<i32> }` alternative would fire
//! `Sensitive<i32>::ZeroizeOnDrop` on the secret — tier-1 by-Drop,
//! but retention is possible via `mem::forget`, `Box::leak`,
//! `ManuallyDrop::new`, all of which bypass `Drop`. The closure
//! shape moves the invariant from tier-1-by-Drop-fire (suppressible)
//! to tier-1-by-construction.
//!
//! What the caller CAN do: copy bytes contents into their own memory
//! (e.g. `bytes.to_vec()`). That copy lives in caller-controlled
//! storage and is the caller's responsibility to scrub. The ORIGINAL
//! bytes (in the Zeroizing guard) are unaffected by the caller's copy
//! and get scrubbed on closure return regardless. Documented in
//! [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`].
//!
//! # Sans-I/O contract
//!
//! This module **does not** open the side TCP connection. The driver
//! crate is responsible for opening a SEPARATE TCP connection to
//! the same backend, writing the 16-byte payload lent through the
//! closure, and closing the socket. The server does not respond on
//! this socket.
//!
//! # Lifecycle of the captured `(pid, secret_key)` pair
//!
//! | Phase                                              | Cell value           |
//! |----------------------------------------------------|----------------------|
//! | `<DisconnectedPhase>` (fresh)                      | empty                |
//! | `<ConnectingPhase>` (mid-handshake, pre-`K`)       | empty                |
//! | dispatch arm `(ConnectingPostAuthHaveKey, 'Z')`    | install (token-gate) |
//! | `<ActivePhase>` (steady)                           | populated, read-only |
//! | `<ClosedPhase>` (post-handshake teardown)          | populated, opaque    |
//!
//! Install happens **exactly once** per connection lifetime — at the
//! dispatch arm that processes the first `ReadyForQuery` ('Z') frame
//! after the server emits `BackendKeyData` ('K'). Mutation is
//! token-gated via the leaf submodule
//! [`crate::protocol::_backend_key_install_leaf::BackendKeyInstallToken`]
//! (private tuple-struct field — mintable only inside the leaf).
//!
//! # `BackendKey` cell payload
//!
//! [`BackendKey`] is the `pub(crate)` cell payload — the storage
//! shape that lives on `PgProtocolInner`. Carries `Sensitive<i32>`
//! for the secret_key so the cell's drop chain scrubs the secret
//! when the connection terminates. Bundle D' eliminated the public
//! `CancelRequestCredentials` struct; the wire-frame materialisation
//! happens inline inside `with_cancel_request` against a stack-local
//! `Zeroizing<[u8; 16]>` guard.
//!
//! # Wire format (PG §55.2.7)
//!
//! ```text
//! [length BE u32 = 16] [magic BE u32 = 80877102] [pid BE i32] [secret BE i32]
//! ```
//!
//! The const [`CANCEL_REQUEST_LEN`] pins the length-field value;
//! [`crate::wire::CANCEL_REQUEST_VERSION`] pins the magic. The
//! existing crate-level [`crate::cancel_request_bytes`] free function
//! is the single source of truth for the byte composition;
//! `with_cancel_request` calls it to build the array, then moves the
//! result into the `Zeroizing` guard.
//!
//! # Panic semantics
//!
//! - Under `panic = "unwind"` (workspace default for `cargo test`):
//!   closure panic propagates, `with_cancel_request`'s frame unwinds,
//!   the `Zeroizing<[u8; 16]>` guard's `Drop` fires during unwind,
//!   bytes are scrubbed.
//! - Under `panic = "abort"` (workspace `release` profile): process
//!   terminates without running `Drop` glue. The OS reclaims the
//!   stack frame on exit; the bytes are not explicitly scrubbed.
//!   Aligned with the crate-wide panic-abort zeroize policy.

use crate::sensitive::Sensitive;

/// Cell-level storage for the `(pid, secret_key)` pair captured at
/// `BackendKeyData` receipt.
///
/// Lives on [`crate::protocol::PgProtocolInner::backend_key`] wrapped
/// in [`BackendKeyCell`]; installed exactly once per connection via
/// the token-gated path inside the dispatch arm that processes
/// `(ConnectingPostAuthHaveKey, 'Z')`. Read on-demand by
/// [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`]
/// to build the 16-byte wire frame on the function's stack inside a
/// `Zeroizing<[u8; 16]>` guard.
///
/// `secret_key` wrapped in [`Sensitive<i32>`] — the cell's drop fires
/// `ZeroizeOnDrop` on the inner `i32` when the connection terminates,
/// scrubbing the secret. `pid` carries no Sensitive wrapper because
/// it is wire-public (the server emits it to anyone who can read the
/// `K` frame; equivalent to "what backend pid handles my queries").
///
/// # Layout
///
/// Inline `{ pid: i32, secret_key: Sensitive<i32> }` = 8 B. The cell's
/// `Option<BackendKey>` adds a 1 B discriminant + alignment padding —
/// total cell footprint ≈ 12 B with the discriminant niche.
///
/// # Debug
///
/// Manual `Debug` impl redacts `secret_key` (delegates to
/// `Sensitive<i32>`'s `Debug` which prints `<REDACTED>`). Every type
/// containing a `Sensitive<T>` field needs a manual Debug or no
/// Debug; here we provide a manual one that prints the pid for
/// diagnostic value and redacts the secret.
pub(crate) struct BackendKey {
    pub(crate) pid: i32,
    pub(crate) secret_key: Sensitive<i32>,
}

impl core::fmt::Debug for BackendKey {
    /// Debug prints the pid plain (wire-public) and redacts the
    /// secret_key via [`Sensitive<i32>`]'s Debug.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BackendKey")
            .field("pid", &self.pid)
            .field("secret_key", &self.secret_key)
            .finish()
    }
}

/// `#[repr(transparent)]` newtype wrapper over `Option<BackendKey>`
/// enforcing tier-1 within-crate write provenance.
///
/// Mirror of [`crate::schema_slot::RowDescSlotCell`] /
/// [`crate::session_params_slot::SessionParamsCell`] /
/// [`crate::partial_assembly::PartialAssemblyCell`] (token-gated
/// cell family). The inner `Option<BackendKey>` is private to
/// `mod cancel`; mutation routes through the token-gated
/// [`Self::install_at_handshake`] method on a per-leaf concrete token
/// minted inside
/// [`crate::protocol::_backend_key_install_leaf`].
///
/// # Tier-1 closure
///
/// External crates cannot:
/// - Name the inner `Option` (field privacy — `E0616` on direct
///   field access).
/// - Mint a [`crate::protocol::_backend_key_install_leaf::BackendKeyInstallToken`]
///   (private tuple-struct field — `E0451` on struct literal).
///
/// Within-crate, mutations are confined to the dispatch arm that
/// processes `(ConnectingPostAuthHaveKey, 'Z')`. Other dispatch arms
/// cannot accidentally install or mutate the key without minting a
/// token from the leaf.
///
/// # Layout
///
/// `#[repr(transparent)]` over `Option<BackendKey>`. Byte-identical
/// to the bare Option; zero-cost wrapping.
#[repr(transparent)]
pub(crate) struct BackendKeyCell {
    inner: Option<BackendKey>,
}

impl BackendKeyCell {
    /// Construct an empty cell. Token-gated to the proto-init leaf
    /// so a future caller cannot bypass the install-on-handshake
    /// invariant by minting a pre-populated cell.
    ///
    /// Takes a [`crate::protocol::_proto_init_leaf::ProtoInitToken`]
    /// — the same construction-gate used by all token-gated cells.
    /// The token is field-private to its leaf submodule, so the only
    /// call site is
    /// [`crate::protocol::PgProtocol::<crate::DisconnectedPhase>::new`].
    #[inline]
    #[must_use]
    pub(crate) const fn empty(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: None }
    }

    /// Install `(pid, secret_key)` at the dispatch arm that processes
    /// the first `ReadyForQuery` after `BackendKeyData`. Token-gated to
    /// [`crate::protocol::_backend_key_install_leaf::BackendKeyInstallToken`]
    /// so the install site is structurally confined to one leaf.
    ///
    /// # Pre-condition (caller-asserted via the token)
    ///
    /// The cell SHOULD be empty at install time — the handshake
    /// passes through `BackendKeyData` exactly once per connection.
    /// A second install (architecturally impossible) silently
    /// overwrites; per CREDO §7 axis 4, we do not panic on this
    /// architecturally-distant case.
    ///
    /// # Tier impact
    ///
    /// Tier-1 within-crate by token-gating: the only legal caller is
    /// inside the leaf, which holds the sole token mint expression.
    #[inline]
    pub(crate) fn install_via_token(
        &mut self,
        _token: &crate::protocol::_backend_key_install_leaf::BackendKeyInstallToken,
        key: BackendKey,
    ) {
        self.inner = Some(key);
    }

    /// Read-only access to the inner payload for the closure-scoped
    /// public API
    /// [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`].
    ///
    /// Returns `None` before the handshake's `BackendKeyData`/`RFQ`
    /// pair is processed; `Some` once the dispatch arm installed the
    /// payload. On `<ActivePhase>`, the runtime invariant is "key is
    /// always Some" because the only path from `<ConnectingPhase>` to
    /// `<ActivePhase>` is `into_active(Ok)`, which requires the state
    /// to be `Idle` — and that state is reached only after the
    /// dispatch arm at `(ConnectingPostAuthHaveKey, 'Z')` runs the
    /// install.
    ///
    /// **`None` returns** on `<ActivePhase>` are architecturally
    /// distant: a non-standard PG fork that skipped the `K` frame
    /// would land in `Idle` without an install. The public API
    /// surface returns `Option<R>` from `with_cancel_request` so the
    /// caller models this honestly without a panic.
    #[inline]
    #[must_use]
    pub(crate) fn as_inner(&self) -> Option<&BackendKey> {
        self.inner.as_ref()
    }
}

impl core::fmt::Debug for BackendKeyCell {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("BackendKeyCell").field(&self.inner).finish()
    }
}

/// `pub(crate)` re-export of the wire-length constant for the
/// CancelRequest packet. The single source of truth lives in
/// [`crate::wire::CANCEL_REQUEST_LEN`] (next to
/// [`crate::wire::CANCEL_REQUEST_VERSION`] for proximity to the
/// magic-version constant family). The drift-pin block below
/// cross-checks it against the
/// [`crate::wire::cancel_request_bytes`] return shape.
///
/// `pub(crate)` because the constant is an internal composition
/// primitive — the user-facing surface is
/// [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`]
/// which lends `&[u8; 16]` directly (size implied by the borrow type).
pub(crate) use crate::wire::CANCEL_REQUEST_LEN;

const _CANCEL_LEN_CROSS_MODULE_PIN: () = {
    assert!(
        CANCEL_REQUEST_LEN == 16,
        "CancelRequest length must be exactly 16 bytes per PG §55.2.7. \
         The single source of truth lives in `wire.rs`; this re-export \
         must agree.",
    );
    // Cross-pin: `cancel_request_bytes` returns `[u8; 16]`. The
    // closure-scoped `with_cancel_request` borrows from a
    // `Zeroizing<[u8; 16]>` guard built from this function — if the
    // return-type ever drifts from 16, the cross-module type-check
    // catches it AND this pin fires. The crate forbids
    // `clippy::as_conversions`, so a direct `CANCEL_REQUEST_LEN as
    // usize` would not type-check; instead pin both sides against
    // the literal 16 and rely on the cross-pin in `wire.rs`
    // (`assert!(CANCEL_REQUEST_LEN == 16, ...)`) for the equality.
    assert!(
        crate::wire::cancel_request_bytes(0, 0).len() == 16,
        "cancel_request_bytes return slice length must equal \
         CANCEL_REQUEST_LEN — drift here breaks the with_cancel_request \
         lend slot.",
    );
};

#[cfg(test)]
mod cell_tests {
    //! Crate-internal smoke tests for [`BackendKey`]'s Debug
    //! redaction. Cell write/read provenance is exercised end-to-end
    //! via `tests/cancel_request_spec.rs` through the public
    //! [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`]
    //! closure-scoped accessor.

    use super::*;
    use crate::sensitive::Sensitive;
    use alloc::format;

    /// Verify the Debug impl on `BackendKey` redacts the secret_key
    /// via Sensitive's `<REDACTED>` while keeping pid plain.
    #[test]
    fn backend_key_debug_redacts_secret() {
        // 0xdead_beef as a positive i32 — use `i32::from_be_bytes` to
        // avoid the forbidden `as` cast and the
        // `clippy::cast_possible_wrap` lint that would catch
        // `0xdead_beef_u32 as i32`.
        let secret_bytes: i32 = i32::from_be_bytes([0xde, 0xad, 0xbe, 0xef]);
        let key = BackendKey {
            pid: 42,
            secret_key: Sensitive::new(secret_bytes),
        };
        let s = format!("{key:?}");
        assert!(
            s.contains("pid: 42"),
            "BackendKey Debug must show pid plain — got {s:?}",
        );
        assert!(
            s.contains("REDACTED"),
            "BackendKey Debug must redact secret_key via Sensitive's \
             `<REDACTED>` placeholder — got {s:?}",
        );
        // Defensive: the literal secret value must not leak into
        // Debug. `secret_bytes` is negative (sign bit set), so check
        // for its decimal representation as well.
        let secret_decimal = format!("{secret_bytes}");
        assert!(
            !s.contains(&secret_decimal),
            "BackendKey Debug must not leak secret_key bytes — got {s:?}",
        );
    }
}
