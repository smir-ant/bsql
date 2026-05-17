//! DEF-278 Bundle D (2026-05-17) — PostgreSQL §55.2.7 CancelRequest
//! mechanism.
//!
//! Surfaces the `(pid, secret_key)` material captured at
//! `BackendKeyData` ('K') receipt during the handshake, plus the
//! 16-byte wire encoding required by the PG cancel side-channel.
//!
//! # Sans-I/O contract
//!
//! This module **does not** open the side TCP connection. The driver
//! (Phase 1e `bsql-driver-postgres`) is responsible for opening a
//! SEPARATE TCP connection to the same backend, writing the 16-byte
//! payload returned by [`CancelRequestCredentials::encode`], and
//! closing the socket. The server does not respond on this socket.
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
//! # `BackendKey` vs `CancelRequestCredentials`
//!
//! - [`BackendKey`] is the `pub(crate)` cell payload — the storage
//!   shape that lives on `PgProtocolInner`. Carries `Sensitive<i32>`
//!   for the secret_key so the cell's drop chain scrubs the secret
//!   when the connection terminates.
//! - [`CancelRequestCredentials`] is the **public** by-value shape
//!   returned to driver code. Constructed on-demand by
//!   [`crate::PgProtocol::<crate::ActivePhase>::cancel_request_credentials`].
//!   Carries a plain `i32` secret_key wrapped in [`crate::Sensitive`]
//!   so the public type's drop chain scrubs the secret too — the
//!   driver's copy is short-lived (single-use up to `encode()`) and
//!   gets zeroed on drop regardless of caller discipline.
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
//! is the construction primitive; [`CancelRequestCredentials::encode`]
//! is a thin typed wrapper over it so the typed flow does not
//! duplicate the byte layout.

use crate::sensitive::Sensitive;

/// DEF-278 Bundle D — cell-level storage for the `(pid, secret_key)`
/// pair captured at `BackendKeyData` receipt.
///
/// Lives on [`crate::protocol::PgProtocolInner::backend_key`] wrapped
/// in [`BackendKeyCell`]; installed exactly once per connection via
/// the token-gated path inside the dispatch arm that processes
/// `(ConnectingPostAuthHaveKey, 'Z')`.
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
/// `Sensitive<i32>`'s `Debug` which prints `<REDACTED>`). Per DEF-048,
/// every type containing a `Sensitive<T>` field needs a manual Debug
/// or no Debug — here we provide a manual one that prints the pid
/// for diagnostic value and redacts the secret.
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

/// DEF-278 Bundle D — `#[repr(transparent)]` newtype wrapper over
/// `Option<BackendKey>` enforcing tier-1 within-crate write
/// provenance.
///
/// Mirror of [`crate::schema_slot::RowDescSlotCell`] /
/// [`crate::session_params_slot::SessionParamsCell`] /
/// [`crate::partial_assembly::PartialAssemblyCell`] (DEF-272 cluster
/// α/β/Sub-B family). The inner `Option<BackendKey>` is private to
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
    /// DEF-278 Bundle D — construct an empty cell. Token-gated to the
    /// proto-init leaf so a future caller cannot bypass the
    /// install-on-handshake invariant by minting a pre-populated cell.
    ///
    /// Takes a [`crate::protocol::_proto_init_leaf::ProtoInitToken`]
    /// — the same construction-gate used by all DEF-272 cluster
    /// cells. The token is field-private to its leaf submodule, so
    /// the only call site is
    /// [`crate::protocol::PgProtocol::<crate::DisconnectedPhase>::new`].
    #[inline]
    #[must_use]
    pub(crate) const fn empty(_t: crate::protocol::_proto_init_leaf::ProtoInitToken) -> Self {
        Self { inner: None }
    }

    /// DEF-278 Bundle D — install `(pid, secret_key)` at the dispatch
    /// arm that processes the first `ReadyForQuery` after
    /// `BackendKeyData`. Token-gated to
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

    /// DEF-278 Bundle D — read-only access to the inner payload for
    /// public-API construction of [`CancelRequestCredentials`].
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
    /// would land in `Idle` without an install. Public API surface
    /// returns `Option<CancelRequestCredentials>` to model this
    /// honestly.
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

/// DEF-278 Bundle D — re-export of the wire-length constant for
/// the CancelRequest packet. The single source of truth lives in
/// [`crate::wire::CANCEL_REQUEST_LEN`] (next to
/// [`crate::wire::CANCEL_REQUEST_VERSION`] for proximity to the
/// magic-version constant family). This module's const-pins below
/// cross-check it against
/// [`CancelRequestCredentials::encode`]'s return-type shape.
///
/// `pub(crate)` because the constant is an internal composition
/// primitive — the user-facing surface is [`CancelRequestCredentials::encode`]
/// (returns `[u8; 16]`, size implied by the return type).
pub(crate) use crate::wire::CANCEL_REQUEST_LEN;

const _CANCEL_LEN_CROSS_MODULE_PIN: () = {
    assert!(
        CANCEL_REQUEST_LEN == 16,
        "CancelRequest length must be exactly 16 bytes per PG §55.2.7. \
         The single source of truth lives in `wire.rs`; this re-export \
         must agree.",
    );
    // Cross-pin: `CancelRequestCredentials::encode` returns
    // `[u8; 16]`. If the return-type ever drifts from the constant,
    // either this pin fires or the function signature fails to type-
    // check (the array literal in `cancel_request_bytes`'s return
    // is structurally tied to the constant by way of the
    // `crate::wire::CANCEL_REQUEST_LEN` import). Both are tier-1
    // build-time guards.
    // Use `CANCEL_REQUEST_LEN` value-equality at u32 then compare
    // to the literal slice length (16). The crate forbids
    // `clippy::as_conversions`, so a direct `CANCEL_REQUEST_LEN as
    // usize` would not type-check; instead pin both sides against
    // the literal 16 and rely on the cross-pin in `wire.rs`
    // (`assert!(CANCEL_REQUEST_LEN == 16, ...)`) for the equality.
    assert!(
        crate::wire::cancel_request_bytes(0, 0).len() == 16,
        "cancel_request_bytes return slice length must equal \
         CANCEL_REQUEST_LEN — drift here breaks the typed-encode \
         return slot.",
    );
};

/// DEF-278 Bundle D — credentials for the PostgreSQL §55.2.7
/// CancelRequest side-channel mechanism.
///
/// Returned by-value from
/// [`crate::PgProtocol::<crate::ActivePhase>::cancel_request_credentials`].
/// Holds the backend pid (wire-public diagnostic field) and the
/// secret_key (sensitive — wrapped in [`Sensitive<i32>`] so a
/// caller's dropped credentials scrub the secret bytes).
///
/// # Why not `Copy` / `Clone`
///
/// `secret_key` is a capability-token-class secret. A leaked secret
/// enables impersonated cancellation of the target query (same
/// threat model as [`crate::StartupCompletePayload`]'s docstring).
/// `Copy` / `Clone` would double the scrub surface for zero benefit;
/// `Sensitive<T>` itself is `!Copy + !Clone` (see `sensitive.rs`).
///
/// # Drop scrubs the secret
///
/// `secret_key: Sensitive<i32>` implements
/// [`zeroize::ZeroizeOnDrop`]; on drop the inner `i32` bytes are
/// overwritten with zeros via the
/// [`zeroize::Zeroize`] chain. Tier-1 by-construction: a future
/// caller forgetting to `drop()` early still gets a scrubbed
/// credential whenever the value goes out of scope.
///
/// # Debug redacts the secret
///
/// Manual [`Debug`] impl prints the pid plain and redacts
/// `secret_key` as `<REDACTED>` — matches the
/// [`crate::StartupCompletePayload`] precedent. Pinned by spec test
/// `cancel_credentials_debug_redacts_secret_key`.
///
/// # Wire shape — handled by [`Self::encode`]
///
/// 16 bytes per PG §55.2.7. The encode method delegates to
/// [`crate::cancel_request_bytes`] (the crate's pre-existing free
/// function) so the typed flow does not duplicate the byte layout.
/// See [`CANCEL_REQUEST_LEN`] for the length-field drift pin.
///
/// # Sans-I/O reminder
///
/// The protocol crate does not open the side socket. The driver
/// (Phase 1e `bsql-driver-postgres`) calls [`Self::encode`], opens a
/// SEPARATE TCP connection to the same backend, writes the bytes,
/// closes the socket. No reply is expected. See module-level docs
/// for the full driver pattern.
pub struct CancelRequestCredentials {
    pid: i32,
    secret_key: Sensitive<i32>,
}

impl CancelRequestCredentials {
    /// DEF-278 Bundle D — construct from the cell-level payload.
    /// `pub(crate)` so the construction surface is confined to
    /// [`crate::PgProtocol::<crate::ActivePhase>::cancel_request_credentials`].
    ///
    /// External callers cannot construct `CancelRequestCredentials`
    /// directly — `pid` and `secret_key` fields are field-private to
    /// `mod cancel`, blocking struct-literal construction outside.
    #[inline]
    #[must_use]
    pub(crate) fn from_backend_key(key: &BackendKey) -> Self {
        // Copy the i32 out of the cell's Sensitive and wrap in a
        // fresh Sensitive for the credentials struct. Both wrappers
        // implement ZeroizeOnDrop independently — the cell's wrapper
        // scrubs the cell's slot on connection teardown; the
        // credentials' wrapper scrubs the public struct's slot on
        // user-side drop. Two drop sites, two zeroes — defense-in-depth.
        let secret_inner: i32 = *key.secret_key.get();
        Self {
            pid: key.pid,
            secret_key: Sensitive::new(secret_inner),
        }
    }

    /// DEF-278 Bundle D — encode to the 16-byte CancelRequest wire
    /// frame per PG §55.2.7.
    ///
    /// Layout (BE):
    ///
    /// ```text
    /// [length=16] [magic=80877102] [pid] [secret_key]
    /// ```
    ///
    /// Delegates to [`crate::cancel_request_bytes`] (the crate's
    /// pre-existing const-fn wire builder) so the byte layout has
    /// exactly one source of truth. The const-assert drift-pin in
    /// [`crate::wire`] catches any future divergence at build time.
    ///
    /// # Tier impact
    ///
    /// Pure function returning `[u8; 16]`. No allocation, no panic,
    /// no I/O. Tier-1 by-return-shape (the size is type-fixed) and
    /// tier-1 by-const-pin (the byte layout is build-time verified).
    #[inline]
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        crate::wire::cancel_request_bytes(self.pid, *self.secret_key.get())
    }

    /// DEF-278 Bundle D — pid accessor (non-sensitive, wire-public).
    ///
    /// The PG backend's process id is announced via the
    /// `BackendKeyData` ('K') frame in plaintext alongside the
    /// secret_key; the pid alone is not a capability and is safe to
    /// log for operator diagnostics ("cancelling pid 12345 on
    /// backend.internal:5432").
    ///
    /// # Option α §8.3 (DEF-278 Bundle D principal sign-off)
    ///
    /// Decision recorded: expose `pid()` for diagnostic value. Match
    /// the existing precedent on [`crate::StartupCompletePayload`]
    /// where `pid: i32` is a plain field (no Sensitive wrapper).
    #[inline]
    #[must_use]
    pub fn pid(&self) -> i32 {
        self.pid
    }
}

impl core::fmt::Debug for CancelRequestCredentials {
    /// DEF-278 Bundle D — prints pid plain (wire-public) and redacts
    /// `secret_key` via [`Sensitive<i32>`]'s `<REDACTED>` Debug. Tier
    /// impact: tier-1 by-impl (the entire impl is one
    /// `debug_struct` call; no formatting branches that could leak).
    /// Pinned by `tests/cancel_request_spec.rs::cancel_credentials_debug_redacts_secret_key`.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CancelRequestCredentials")
            .field("pid", &self.pid)
            .field("secret_key", &self.secret_key)
            .finish()
    }
}

#[cfg(test)]
mod cell_tests {
    //! Crate-internal smoke tests for [`BackendKey`]'s Debug
    //! redaction. Cell write/read provenance is exercised end-to-end
    //! via `tests/cancel_request_spec.rs` through the public
    //! `cancel_request_credentials` accessor.

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
