//! SCRAM-SHA-256 text-protocol message construction and parsing.
//!
//! This module handles the four SCRAM messages exchanged during
//! authentication (RFC 5802 §7):
//!
//! 1. **client-first-message** — `<gs2-header>n=<user>,r=<nonce>` (we build it)
//! 2. **server-first-message** — `r=<nonce>,s=<salt>,i=<iters>` (we parse it)
//! 3. **client-final-message** — `c=<cbind_b64>,r=<nonce>,p=<proof>` (we build it)
//! 4. **server-final-message** — `v=<verifier>` or `e=<error>` (we parse it)
//!
//! This is protocol text, not crypto — it is allowed to be hand-written.
//! All cryptographic operations are in [`super::crypto`].
//!
//! # Channel binding
//!
//! The gs2 header and the client-final `c=` value are parametrized by the
//! resolved [`SaslChoice`](crate::scram::channel_binding::SaslChoice): `n,,`
//! (no binding), `y,,` (capable-but-unused), or `p=tls-server-end-point,,`
//! for SCRAM-SHA-256-PLUS — in which case `c=` carries the base64 of that
//! header plus the server certificate hash. See
//! [`super::channel_binding`].

use crate::scram::types::{CappedServerNonce, SecretDigest};
use core::fmt;

/// Maximum byte length for the client-first-message-bare.
///
/// `n=<user>,r=<nonce>` where user <= 63 bytes and nonce is ~24 bytes
/// base64. 128 bytes is generous.
pub const MAX_CLIENT_FIRST_BARE_LEN: usize = 128;

/// Maximum byte length for a base64-encoded SCRAM nonce.
///
/// 18 random bytes base64-encode to 24 chars. We allow up to 48 for
/// configurability in tests.
pub const MAX_CLIENT_NONCE_B64_LEN: usize = 48;

/// Maximum byte length for the full client-first-message (gs2 header + bare).
///
/// The gs2 header is up to
/// [`MAX_GS2_HEADER_LEN`](crate::scram::channel_binding::MAX_GS2_HEADER_LEN)
/// bytes (`p=tls-server-end-point,,`), so this is that plus
/// [`MAX_CLIENT_FIRST_BARE_LEN`].
pub const MAX_CLIENT_FIRST_MSG_LEN: usize =
    crate::scram::channel_binding::MAX_GS2_HEADER_LEN + MAX_CLIENT_FIRST_BARE_LEN;

/// Maximum byte length for the client-final-message.
///
/// `c=<cbind_b64>,r=<server_nonce>,p=<proof_b64>` where `cbind_b64` is the
/// base64 cbind-input (up to
/// [`MAX_CBIND_B64_LEN`](crate::scram::channel_binding::MAX_CBIND_B64_LEN) —
/// widest for the `-PLUS` `p=tls-server-end-point,,` header plus a SHA-512 cert
/// hash), the server nonce is up to
/// [`MAX_SERVER_NONCE_LEN`](crate::scram::types::MAX_SERVER_NONCE_LEN), and
/// `proof_b64` is 44 chars.
pub const MAX_CLIENT_FINAL_MSG_LEN: usize = 448;

/// Minimum SCRAM iteration count per RFC 7677 section 4.2.
pub const MIN_SCRAM_ITERATIONS: u32 = 4096;

/// Maximum SCRAM iteration count this client will execute.
///
/// RFC 7677 does not mandate a maximum, but PBKDF2 work is linear
/// in iteration count. A malicious or mis-configured server sending
/// `iterations = u32::MAX` (~4 billion) would stall the client for
/// minutes to hours per connection attempt — a client-side DoS
/// surface.
///
/// # Why `100_000`
///
/// `100_000` is roughly 20ms per handshake on modern x86 — 100
/// concurrent adversarial servers ≈ 2 CPU-seconds total, below the
/// DoS-sensitivity threshold. Real PG deployments use
/// `iterations ≤ 10_000` per `pg_hba.conf` defaults, so 10× headroom
/// covers every plausible production configuration while preserving
/// RFC 7677 §4.2 "legitimate upward scaling" (the RFC's recommended
/// baseline was 4096; `100_000` supports every future iteration bump
/// for the next decade of hardware).
///
/// A naive `10_000_000` cap would permit ~2 seconds of PBKDF2-SHA-256
/// per connection attempt on modern x86 — 100 concurrent malicious
/// servers = 100 CPU-seconds of work before teardown (legitimate DoS
/// surface, bounded but real).
///
/// A value above this classifies as [`ScramError::IterationsTooHigh`]
/// — connection is torn down with a typed diagnostic, not a stuck
/// handshake.
pub const MAX_SCRAM_ITERATIONS: u32 = 100_000;

/// Maximum base64-decoded salt length.
pub const MAX_SALT_LEN: usize = 64;

/// Base64-encoded length (with RFC 4648 padding) of a SHA-256 digest.
///
/// SHA-256 produces 32 bytes. Base64 encoding with padding is
/// `ceil(N / 3) * 4`; for `N = 32` that is `ceil(32/3) * 4 = 11 * 4 = 44`.
///
/// Derived via `usize::div_ceil` (a method call, not the `/`
/// operator, so `clippy::integer_division` does not flag it).
/// Formula: base64 encodes 3 bytes into 4 chars, unpadded length =
/// `ceil(n/3) * 4`. For SHA-256 (32 bytes):
/// `ceil(32/3) * 4 = 11 * 4 = 44` chars. Drift pin: if
/// `SHA256_DIGEST_LEN` ever changes (it won't — SHA-256 is forever
/// 32 bytes per RFC 6234) or the formula is off, the `static_assert`
/// below catches it.
const SHA256_DIGEST_LEN: usize = 32;
const SHA256_PROOF_B64_LEN: usize = SHA256_DIGEST_LEN
    .div_ceil(3)
    .saturating_mul(4);

// Drift-pin: derived value must equal the wire-verified magic
// number (44). `base64ct` runtime encoding in scram::crypto::tests
// independently checks the proof's padding length on the RFC 7677
// test vector; the const-assert here makes the formula → value
// pairing explicit at compile time.
const _: () = assert!(
    SHA256_PROOF_B64_LEN == 44,
    "SHA256_PROOF_B64_LEN drift — base64(sha256_digest) is always \
     44 chars for 32-byte input (RFC 4648 §4). If this assert \
     trips, either SHA256_DIGEST_LEN changed (unlikely per \
     RFC 6234) or the div_ceil-based formula is wrong.",
);

/// Byte size of a SASL `Initial Response` frame sent by the client.
///
/// Wire format: tag `'p'` (1) + length field (4) + mechanism name +
/// NUL (1) + body-length field (4) + client-first-message body.
///
/// The result is used in a drift-guard against
/// [`crate::write_buf::MAX_OWNED_SEND_LEN`] — if the worst-case SASL
/// initial response outgrows the outbound buffer, build fails.
pub(crate) const fn sasl_initial_response_frame_size() -> usize {
    1usize // tag 'p'
        .saturating_add(4) // length field
        // Widest mechanism name: `SCRAM-SHA-256-PLUS` (the -PLUS variant).
        .saturating_add(crate::wire::SCRAM_SHA_256_PLUS_MECHANISM.len())
        .saturating_add(1) // mechanism NUL terminator
        .saturating_add(4) // body-length field
        .saturating_add(MAX_CLIENT_FIRST_MSG_LEN)
}

/// Byte size of a SASL `Response` frame sent by the client (step 2).
///
/// Wire format: tag `'p'` (1) + length field (4) + client-final-message
/// body.
pub(crate) const fn sasl_response_frame_size() -> usize {
    1usize // tag 'p'
        .saturating_add(4) // length field
        .saturating_add(MAX_CLIENT_FINAL_MSG_LEN)
}

// -------------------------------------------------------------------
// Drift guards
//
// Each expected-size const fn below computes a worst-case size from
// the underlying inputs (`MAX_IDENT_LEN`, `MAX_SERVER_NONCE_LEN`, ...).
// The accompanying `const _` assert ties the declared bound to its
// computed worst case. Bump any input without growing the bound →
// build fails here. This converts a class of silent-runtime-truncation
// regressions (tier 2) into compile errors (tier 1).
// -------------------------------------------------------------------

/// Worst-case `client-first-message-bare`: `n=<user>,r=<nonce_b64>`.
const fn expected_client_first_bare_size() -> usize {
    2usize // "n="
        .saturating_add(crate::ident::MAX_IDENT_LEN)
        .saturating_add(3) // ",r="
        .saturating_add(MAX_CLIENT_NONCE_B64_LEN)
}
const _: () = assert!(
    MAX_CLIENT_FIRST_BARE_LEN >= expected_client_first_bare_size(),
    "MAX_CLIENT_FIRST_BARE_LEN below worst-case n=<user>,r=<nonce>",
);

/// Worst-case full `client-first-message`: the widest gs2 header
/// (`p=tls-server-end-point,,`) plus the bare message.
const fn expected_client_first_msg_size() -> usize {
    crate::scram::channel_binding::MAX_GS2_HEADER_LEN
        .saturating_add(MAX_CLIENT_FIRST_BARE_LEN)
}
const _: () = assert!(
    MAX_CLIENT_FIRST_MSG_LEN >= expected_client_first_msg_size(),
    "MAX_CLIENT_FIRST_MSG_LEN below GS2 header + client-first-bare",
);

/// Worst-case `client-final-message`: `c=<cbind_b64>,r=<server_nonce>,p=<proof_b64>`
/// where `cbind_b64` is the widest base64 cbind-input (the `-PLUS` header plus a
/// SHA-512 cert hash).
const fn expected_client_final_msg_size() -> usize {
    2usize // "c="
        .saturating_add(crate::scram::channel_binding::MAX_CBIND_B64_LEN)
        .saturating_add(3) // ",r="
        .saturating_add(crate::scram::types::MAX_SERVER_NONCE_LEN)
        .saturating_add(3) // ",p="
        .saturating_add(SHA256_PROOF_B64_LEN)
}
const _: () = assert!(
    MAX_CLIENT_FINAL_MSG_LEN >= expected_client_final_msg_size(),
    "MAX_CLIENT_FINAL_MSG_LEN below worst-case c=<cbind_b64>,r=<nonce>,p=<proof>",
);

// SASL frames must fit inside the shared outbound buffer.
const _: () = assert!(
    crate::write_buf::MAX_OWNED_SEND_LEN >= sasl_initial_response_frame_size(),
    "MAX_OWNED_SEND_LEN below SASLInitialResponse frame size",
);
const _: () = assert!(
    crate::write_buf::MAX_OWNED_SEND_LEN >= sasl_response_frame_size(),
    "MAX_OWNED_SEND_LEN below SASLResponse frame size",
);

/// Errors from SCRAM wire message construction or parsing.
///
/// `#[non_exhaustive]` (audit MI6) — SCRAM wire-spec
/// extensions (channel-binding, future SASL profiles) may introduce
/// new error classes. Downstream `_ =>` arms absorb additions;
/// exhaustive matches have always been a compatibility hazard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramError {
    /// Server nonce does not start with the client nonce (RFC 5802 section 5.1 MUST).
    NoncePrefixMismatch,
    /// Server iteration count is below the RFC 7677 minimum (4096).
    IterationsTooLow {
        /// The iterations value the server sent.
        iterations: u32,
    },
    /// Server iteration count exceeds the client's sanity cap
    /// ([`MAX_SCRAM_ITERATIONS`]). Closes the client-side DoS surface
    /// where a malicious or mis-configured server would send a
    /// deliberately-large iteration count to stall PBKDF2 for
 /// minutes per connection (audit BS8).
    IterationsTooHigh {
        /// The offending iterations value.
        iterations: u32,
    },
    /// HMAC-SHA-256 key construction rejected. HMAC structurally
    /// accepts any key length (RFC 2104), so this variant is
    /// architecturally unreachable with an intact RustCrypto `hmac`
    /// crate. Emission indicates a supply-chain compromise or
    /// upstream contract break.
    ///
 /// # Why fail-closed, not silent-zero (F54)
    ///
    /// Pre-F54 the HMAC helpers returned `[0u8; 32]` on the dead Err
    /// branch. Even though SCRAM's server-side verification would
    /// REJECT an all-zero `ClientProof` (see the crypto-module docs
    /// for the signature-check math — server computes
    /// `SHA-256(ClientProof XOR server-side ClientSignature) == StoredKey`
    /// which is vanishingly unlikely to hold for an all-zero
    /// ClientProof), the silent-degradation pattern itself is a bad
    /// precedent for crypto code. Explicit `Result` propagation
    /// makes the fail-closed behaviour visible in the type system.
    HmacKeyRejected,
    /// Server-first-message has invalid format.
    MalformedServerFirst,
    /// Server-final-message indicates an error (`e=<text>`).
    ///
    /// F30: carries the server-supplied error text (lossy-ASCII-coerced
    /// via [`crate::ident::BoundedStr::from_bytes_lossy`] so non-UTF-8
    /// server locales survive as `?`-placeholders, not silent-empty).
    /// Previously this variant was opaque — wrapper crates could only
    /// log "server reported authentication error" with zero forensic
    /// detail. The `e=` field content is the primary diagnostic clue
    /// (e.g., "invalid-proof", "server-does-support-channel-binding",
    /// "unknown-user"), so preserving it is load-bearing for ops.
    ServerScramError {
        /// Server-supplied error text from `e=<text>` (RFC 5802 §5.1
        /// server-error-value). RFC-defined tokens are short ASCII
        /// hyphen-separated strings; the longest standard value is
        /// `"server-does-support-channel-binding"` (35 bytes).
        /// Capacity 64 covers all known tokens with ~2× headroom for
        /// future extensions while keeping `ScramError` well under
        /// clippy's `result_large_err` 128-byte threshold.
        message: crate::ident::BoundedStr<64>,
    },
    /// Server-final-message has invalid format.
    MalformedServerFinal,
    /// Server signature verification failed.
    SignatureMismatch,
    /// A base64 decode operation failed.
    Base64DecodeError,
    /// Salt is empty or too long.
    InvalidSalt,
    /// Server nonce too long for our bounded buffer.
    ServerNonceTooLong,
    /// Buffer overflow during message construction.
    BufferOverflow,
    /// Server offered no supported SCRAM mechanism.
    NoSupportedMechanism,
    /// OS-level randomness source unavailable or failing.
    ///
    /// Emitted when `getrandom::getrandom` fails to fill the SCRAM
    /// client-nonce buffer. On Linux this typically means
    /// `/dev/urandom` is inaccessible (capability-restricted
    /// container, seccomp filter) or the kernel entropy pool is
    /// draining (`EAGAIN`). On other platforms: crypto-subsystem
    /// unavailable.
    ///
    /// Pass-#8 F-025: prior to this classification, randomness
    /// failures masqueraded as [`Self::BufferOverflow`] — operator
    /// debugging auth errors saw "message buffer overflow" when the
    /// actual cause was `/dev/urandom` EAGAIN. Now the diagnostic
    /// names the real root cause.
    RandomnessUnavailable,
    /// The consumer required channel binding (`channel_binding=require`) but the
    /// server offered no `SCRAM-SHA-256-PLUS` mechanism, so the exchange is
    /// refused fail-closed rather than falling back to plain SCRAM. Over TLS to a
    /// server that genuinely supports channel binding this cannot happen (it
    /// always advertises `-PLUS`); the refusal fires against a legacy/non-binding
    /// server, or when a downgrade attacker stripped `-PLUS` from the offer.
    ChannelBindingRequired,
}

/// Discriminant-flattened mirror of [`ScramError`] for protocol-error
/// embedding. Carries every variant's identity + small payloads
/// (iteration counts) inline, but **never** the `ServerScramError`
/// text — that text is externalised into
/// `crate::error_arena::ErrorArena` via an `crate::error_arena::ErrorRef`
/// alongside the class.
///
/// # Why a parallel enum rather than mutating `ScramError`
///
/// `ScramError` is the boundary type returned by `parse_*` functions
/// in `scram/wire.rs`; reshaping it would cascade through the wire-
/// parsing surface for no protocol-level benefit. Instead this enum
/// captures the *protocol-error-embedding* shape: same identity
/// classes, no inline 64-B string. Conversion is one-way
/// ([`ScramError::split_into_class_and_text`]); the wire-layer keeps
/// returning fat `ScramError`, the protocol layer stores the slim
/// `(class, detail_ref)` pair.
///
/// # Footprint
///
/// Maximum variant payload is `u32` (`IterationsTooLow`/`TooHigh`);
/// total enum size = 8 B (tag 1 B + 3 B pad + u32 4 B, align 4).
/// Compared to inline `ScramError` (≈ 68 B due to BoundedStr<64>),
/// this saves ≈ 60 B per `ProtocolError::ScramHandshakeFailure`
/// occurrence — which cascades into Action (80 → 32 B) and
/// OutActions (728 → 296 B).
///
/// # Const-asserts colocated in [`crate::error`]
///
/// Size pin and Option-niche pin live next to the
/// `ProtocolError::ScramHandshakeFailure` variant for fail-fast on
/// layout drift at the consumption site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramFailureClass {
    /// Mirror of [`ScramError::NoncePrefixMismatch`].
    NoncePrefixMismatch,
    /// Mirror of [`ScramError::IterationsTooLow`]; carries the
    /// offending iteration count for ops diagnosis.
    IterationsTooLow {
        /// The iterations value the server sent (below RFC 7677 minimum).
        iterations: u32,
    },
    /// Mirror of [`ScramError::IterationsTooHigh`].
    IterationsTooHigh {
        /// The offending iterations value (exceeded client sanity cap).
        iterations: u32,
    },
    /// Mirror of [`ScramError::HmacKeyRejected`].
    HmacKeyRejected,
    /// Mirror of [`ScramError::MalformedServerFirst`].
    MalformedServerFirst,
    /// Mirror of [`ScramError::ServerScramError`] — the inline text
    /// payload has been externalised into the arena alongside this
    /// class; the `ProtocolError::ScramHandshakeFailure.detail`
    /// `ErrorRef` field resolves the text.
    ServerScramError,
    /// Mirror of [`ScramError::MalformedServerFinal`].
    MalformedServerFinal,
    /// Mirror of [`ScramError::SignatureMismatch`].
    SignatureMismatch,
    /// Mirror of [`ScramError::Base64DecodeError`].
    Base64DecodeError,
    /// Mirror of [`ScramError::InvalidSalt`].
    InvalidSalt,
    /// Mirror of [`ScramError::ServerNonceTooLong`].
    ServerNonceTooLong,
    /// Mirror of [`ScramError::BufferOverflow`].
    BufferOverflow,
    /// Mirror of [`ScramError::NoSupportedMechanism`].
    NoSupportedMechanism,
    /// Mirror of [`ScramError::RandomnessUnavailable`].
    RandomnessUnavailable,
    /// Mirror of [`ScramError::ChannelBindingRequired`].
    ChannelBindingRequired,
}

impl ScramError {
    /// One-way conversion: split a wire-layer [`ScramError`] into the
    /// protocol-embedding [`ScramFailureClass`] + optional inline text
    /// (only present for [`ScramError::ServerScramError`]).
    ///
    /// Callers in `dispatch.rs` use this to convert wire-layer SCRAM
    /// errors into the slim `crate::error::ProtocolError::ScramHandshakeFailure`
    /// form: the text (when present) is alloc'd into
    /// `crate::error_arena::ErrorArena` via
    /// `crate::error_arena::ErrorPayload::Scram`, and the resulting
    /// `crate::error_arena::ErrorRef` threads into the
    /// `ProtocolError::ScramHandshakeFailure.detail` field.
    ///
    /// # Total
    ///
    /// Every `ScramError` variant produces a `ScramFailureClass` of
    /// matching identity. Only `ServerScramError` contributes the
    /// optional `Some(text)` half; all other variants return
    /// `(class, None)`.
    #[inline]
    #[must_use]
    pub fn split_into_class_and_text(
        self,
    ) -> (ScramFailureClass, Option<crate::ident::BoundedStr<64>>) {
        match self {
            Self::NoncePrefixMismatch => (ScramFailureClass::NoncePrefixMismatch, None),
            Self::IterationsTooLow { iterations } => {
                (ScramFailureClass::IterationsTooLow { iterations }, None)
            }
            Self::IterationsTooHigh { iterations } => {
                (ScramFailureClass::IterationsTooHigh { iterations }, None)
            }
            Self::HmacKeyRejected => (ScramFailureClass::HmacKeyRejected, None),
            Self::MalformedServerFirst => (ScramFailureClass::MalformedServerFirst, None),
            Self::ServerScramError { message } => {
                (ScramFailureClass::ServerScramError, Some(message))
            }
            Self::MalformedServerFinal => (ScramFailureClass::MalformedServerFinal, None),
            Self::SignatureMismatch => (ScramFailureClass::SignatureMismatch, None),
            Self::Base64DecodeError => (ScramFailureClass::Base64DecodeError, None),
            Self::InvalidSalt => (ScramFailureClass::InvalidSalt, None),
            Self::ServerNonceTooLong => (ScramFailureClass::ServerNonceTooLong, None),
            Self::BufferOverflow => (ScramFailureClass::BufferOverflow, None),
            Self::NoSupportedMechanism => (ScramFailureClass::NoSupportedMechanism, None),
            Self::RandomnessUnavailable => (ScramFailureClass::RandomnessUnavailable, None),
            Self::ChannelBindingRequired => (ScramFailureClass::ChannelBindingRequired, None),
        }
    }
}

impl core::fmt::Display for ScramFailureClass {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The class-only Display mirrors the inline ScramError Display
        // arm-for-arm; the externalised text (when present) is rendered
        // alongside by ProtocolError::Display via the ErrorArena resolver.
        match self {
            Self::NoncePrefixMismatch => {
                f.write_str("SCRAM: server nonce does not start with client nonce")
            }
            Self::IterationsTooLow { iterations } => {
                write!(f, "SCRAM: server iterations {iterations} below RFC 7677 minimum")
            }
            Self::IterationsTooHigh { iterations } => {
                write!(f, "SCRAM: server iterations {iterations} above client sanity cap")
            }
            Self::HmacKeyRejected => f.write_str("SCRAM: HMAC-SHA-256 key construction rejected"),
            Self::MalformedServerFirst => f.write_str("SCRAM: malformed server-first-message"),
            Self::ServerScramError => {
                f.write_str("SCRAM: server reported authentication error")
            }
            Self::MalformedServerFinal => f.write_str("SCRAM: malformed server-final-message"),
            Self::SignatureMismatch => f.write_str("SCRAM: server signature verification failed"),
            Self::Base64DecodeError => f.write_str("SCRAM: base64 decode failed"),
            Self::InvalidSalt => f.write_str("SCRAM: invalid salt"),
            Self::ServerNonceTooLong => f.write_str("SCRAM: server nonce too long"),
            Self::BufferOverflow => f.write_str("SCRAM: message buffer overflow"),
            Self::NoSupportedMechanism => f.write_str("SCRAM: no supported mechanism offered"),
            Self::RandomnessUnavailable => f.write_str("SCRAM: OS randomness source unavailable"),
            Self::ChannelBindingRequired => {
                f.write_str("SCRAM: channel binding required but server offered no -PLUS mechanism")
            }
        }
    }
}

// `core::error::Error` impl on the SCRAM-handshake error — lets
// downstream consumers route this type through any `?`-bubbling
// stack that bounds on `core::error::Error`.
impl core::error::Error for ScramError {}

impl fmt::Display for ScramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoncePrefixMismatch => {
                f.write_str("SCRAM: server nonce does not start with client nonce")
            }
            Self::IterationsTooLow { iterations } => {
                write!(f, "SCRAM: iteration count {iterations} below minimum 4096")
            }
            Self::IterationsTooHigh { iterations } => write!(
                f,
                "SCRAM: iteration count {iterations} above client sanity cap {MAX_SCRAM_ITERATIONS}",
            ),
            Self::HmacKeyRejected => f.write_str(
                "SCRAM: HMAC-SHA-256 key construction failed (fail-closed — architecturally unreachable with intact `hmac` crate)",
            ),
            Self::MalformedServerFirst => f.write_str("SCRAM: malformed server-first-message"),
            Self::ServerScramError { message } => {
                write!(f, "SCRAM: server reported authentication error: {}", message.as_str())
            }
            Self::MalformedServerFinal => f.write_str("SCRAM: malformed server-final-message"),
            Self::SignatureMismatch => f.write_str("SCRAM: server signature mismatch"),
            Self::Base64DecodeError => f.write_str("SCRAM: base64 decode failed"),
            Self::InvalidSalt => f.write_str("SCRAM: invalid salt"),
            Self::ServerNonceTooLong => f.write_str("SCRAM: server nonce too long"),
            Self::BufferOverflow => f.write_str("SCRAM: message buffer overflow"),
            Self::RandomnessUnavailable => f.write_str(
                "SCRAM: OS randomness source unavailable (getrandom failed — /dev/urandom inaccessible, entropy pool draining, or crypto subsystem down)",
            ),
            Self::NoSupportedMechanism => {
                f.write_str("SCRAM: server offered no supported authentication mechanism")
            }
            Self::ChannelBindingRequired => f.write_str(
                "SCRAM: channel binding required (channel_binding=require) but the server offered no SCRAM-SHA-256-PLUS mechanism",
            ),
        }
    }
}

/// Build the client-first-message-bare: `n=<user>,r=<nonce_b64>`.
///
/// Returns the bare message (without the gs2 header) in a bounded buffer.
/// The caller prepends the gs2 channel-binding header for the full
/// client-first. The bare message is IDENTICAL for every channel-binding
/// choice — the gs2 flag lives only in the full message and the client-final
/// `c=` value, never in the bare form that anchors the SCRAM `AuthMessage`.
pub(crate) fn build_client_first_bare(
    user: &[u8],
    client_nonce_b64: &[u8],
) -> Result<arrayvec::ArrayVec<u8, MAX_CLIENT_FIRST_BARE_LEN>, ScramError> {
    let mut buf = arrayvec::ArrayVec::new_const();
    // "n=" + user + ",r=" + nonce
    buf.try_extend_from_slice(b"n=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(user)
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(b",r=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(client_nonce_b64)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(buf)
}

/// Build the full client-first-message: gs2 header + bare.
///
/// `<gs2_header>n=<user>,r=<nonce_b64>` — e.g. `n,,n=user,r=nonce` (no channel
/// binding) or `p=tls-server-end-point,,n=user,r=nonce` (SCRAM-SHA-256-PLUS).
/// The gs2 header is chosen by the caller from the resolved
/// [`SaslChoice`](crate::scram::channel_binding::SaslChoice).
pub(crate) fn build_client_first_message(
    gs2_header: &[u8],
    user: &[u8],
    client_nonce_b64: &[u8],
) -> Result<arrayvec::ArrayVec<u8, MAX_CLIENT_FIRST_MSG_LEN>, ScramError> {
    let mut msg = arrayvec::ArrayVec::new_const();
    msg.try_extend_from_slice(gs2_header)
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.try_extend_from_slice(b"n=")
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.try_extend_from_slice(user)
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.try_extend_from_slice(b",r=")
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.try_extend_from_slice(client_nonce_b64)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(msg)
}

/// Parsed fields from a server-first-message.
///
/// # NOT stored in `ProtoState` — parse-phase local only
///
/// `ServerFirst` is built by [`parse_server_first`] inside a single
/// dispatcher call (`dispatch_auth_in_scram_server_first` in
/// `dispatch.rs`), consumed immediately to compute the client proof
/// and [`crate::scram::types::SecretDigest`], then dropped at function
/// return. It never crosses a `feed_bytes` / `push_command` boundary.
/// The state that DOES persist to the next call
/// (`crate::ProtoState::ConnectingScramAwaitingServerFinal`) carries
/// only the POD `SecretDigest` — the `arrayvec::ArrayVec` fields here
/// never propagate Drop into the state enum.
///
/// Consequence for audits: a "replace `arrayvec::ArrayVec` with `PodBytes`
/// for state-Drop-cleanliness" suggestion does NOT apply here. The
/// fields are fine as `arrayvec::ArrayVec` — the Drop surface is bounded
/// to the parse frame's stack scope.
#[derive(Debug)]
pub(crate) struct ServerFirst {
    /// The full server nonce (`r=<value>`) — must start with client nonce.
    pub(crate) server_nonce: CappedServerNonce,
    /// Base64-decoded salt.
    pub(crate) salt: arrayvec::ArrayVec<u8, MAX_SALT_LEN>,
    /// Iteration count.
    pub(crate) iterations: u32,
}

/// Parse a server-first-message: `r=<nonce>,s=<salt_b64>,i=<iters>`.
///
/// Validates:
/// - Server nonce starts with the client nonce (RFC 5802 section 5.1 MUST).
/// - Iteration count >= 4096 (RFC 7677 section 4.2 MUST).
/// - Salt base64-decodes and fits our bounded buffer.
///
/// # RFC 5802 extensions
///
/// RFC 5802 §5.1 grammar:
/// `server-first-message = [reserved-mext ","] nonce "," salt ","
///                          iteration-count ["," extensions]`
///
/// Splits the message on comma WITHOUT a max-parts cap, takes the 3
/// required fields (r, s, i) in order, and silently ignores any
/// trailing extension parts. The `reserved-mext` prefix is rejected
/// with `MalformedServerFirst` per RFC §5.1 paragraph 4 ("If the
/// server does not support the 'm' extension, it MUST treat the
/// entire message as malformed") — a future `UnexpectedExtension`
/// variant could split the diagnostic if needed, but fail-closed is
/// the required behaviour for an unhandled mandatory extension.
///
/// A naive `splitn(3, ',')` would put trailing extensions into the
/// `i_part` tail (e.g. `i=4096,ext=foo` → parse_u32 fails on
/// `4096,ext=foo` with `InvalidDigit`), misclassifying legitimate
/// RFC-compliant servers (some proxies, non-PG SCRAM servers like
/// MongoDB/Kafka) as "malformed frame".
pub(crate) fn parse_server_first(
    msg: &[u8],
    client_nonce_b64: &[u8],
) -> Result<ServerFirst, ScramError> {
    // Parse the comma-separated fields.
    let msg_str = core::str::from_utf8(msg).map_err(|_| ScramError::MalformedServerFirst)?;

    // Iterate parts without a max-cap so extensions don't stick onto
    // `i_part`. The first three parts must be r, s, i in that order;
    // any additional parts are RFC extensions (skipped silently
    // unless the first part looks like `reserved-mext =` which
    // signals a mandatory extension we don't implement).
    let mut parts = msg_str.split(',');
    let r_part = parts.next().ok_or(ScramError::MalformedServerFirst)?;
    // Reserved-mext detection per RFC 5802 §5.1: if the first part
    // begins with `m=` it's a mandatory extension; without
    // implementation-awareness we must fail closed per RFC §5.1
    // paragraph 4 ("If the server does not support the 'm' extension,
    // it MUST treat the entire message as malformed"). We surface it
    // as MalformedServerFirst for now; a future `UnexpectedExtension`
    // variant can split the diagnostic if needed.
    if r_part.starts_with("m=") {
        return Err(ScramError::MalformedServerFirst);
    }
    let s_part = parts.next().ok_or(ScramError::MalformedServerFirst)?;
    let i_part = parts.next().ok_or(ScramError::MalformedServerFirst)?;
    // Remaining parts (if any) are optional extensions — silently
    // ignored per RFC 5802 §5.1 ("Extensions are used for optional
    // features that don't affect the authentication semantics").

    // r=<server_nonce>
    let server_nonce_str = r_part
        .strip_prefix("r=")
        .ok_or(ScramError::MalformedServerFirst)?;

    // Validate nonce prefix: server nonce must start with client nonce.
    let client_nonce_str =
        core::str::from_utf8(client_nonce_b64).map_err(|_| ScramError::MalformedServerFirst)?;
    if !server_nonce_str.starts_with(client_nonce_str) {
        return Err(ScramError::NoncePrefixMismatch);
    }

    let server_nonce = CappedServerNonce::try_from_bytes(server_nonce_str.as_bytes())
        .map_err(|_| ScramError::ServerNonceTooLong)?;

    // s=<salt_b64>
    let salt_b64 = s_part
        .strip_prefix("s=")
        .ok_or(ScramError::MalformedServerFirst)?;
    let salt = base64_decode_bounded(salt_b64.as_bytes())?;
    if salt.is_empty() {
        return Err(ScramError::InvalidSalt);
    }

    // i=<iterations>
    let iters_str = i_part
        .strip_prefix("i=")
        .ok_or(ScramError::MalformedServerFirst)?;
    let iterations = parse_u32(iters_str.as_bytes())
        .map_err(|_| ScramError::MalformedServerFirst)?;
    if iterations < MIN_SCRAM_ITERATIONS {
        return Err(ScramError::IterationsTooLow { iterations });
    }
    // Bound the upper end. A malicious server sending `iterations = u32::MAX`
    // stalls PBKDF2 for minutes per connection — client-side DoS. Reject
    // anything above `MAX_SCRAM_ITERATIONS`.
    if iterations > MAX_SCRAM_ITERATIONS {
        return Err(ScramError::IterationsTooHigh { iterations });
    }

    Ok(ServerFirst {
        server_nonce,
        salt,
        iterations,
    })
}

/// Build the client-final-message-without-proof: `c=<cbind_b64>,r=<server_nonce>`.
///
/// `cbind_b64` is the base64-encoded cbind-input (`gs2-header || cbind-data`) —
/// `biws` for no channel binding (`n,,`), `eSws` for `y,,`, or the base64 of
/// `p=tls-server-end-point,,` plus the cert hash for SCRAM-SHA-256-PLUS. This is
/// used as part of the SCRAM `AuthMessage` and to construct the full
/// client-final-message (by appending `,p=<proof_b64>`), so the channel binding
/// is cryptographically anchored into the proof.
pub(crate) fn build_client_final_without_proof(
    cbind_b64: &[u8],
    server_nonce: &[u8],
) -> Result<arrayvec::ArrayVec<u8, MAX_CLIENT_FINAL_MSG_LEN>, ScramError> {
    let mut buf = arrayvec::ArrayVec::new_const();
    buf.try_extend_from_slice(b"c=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(cbind_b64)
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(b",r=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(server_nonce)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(buf)
}

/// Build the complete client-final-message: `c=<cbind_b64>,r=<nonce>,p=<proof_b64>`.
pub(crate) fn build_client_final_message(
    cbind_b64: &[u8],
    server_nonce: &[u8],
    proof_b64: &[u8],
) -> Result<arrayvec::ArrayVec<u8, MAX_CLIENT_FINAL_MSG_LEN>, ScramError> {
    let mut buf = build_client_final_without_proof(cbind_b64, server_nonce)?;
    buf.try_extend_from_slice(b",p=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.try_extend_from_slice(proof_b64)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(buf)
}

/// Parse a server-final-message.
///
/// Success: `v=<verifier_b64>` — returns the decoded verifier as SecretDigest.
/// Error: `e=<text>` — returns ScramError::ServerScramError.
pub(crate) fn parse_server_final(msg: &[u8]) -> Result<SecretDigest, ScramError> {
    let msg_str = core::str::from_utf8(msg).map_err(|_| ScramError::MalformedServerFinal)?;

    if let Some(verifier_tail) = msg_str.strip_prefix("v=") {
        // Accept RFC 5802 extensions. Per §5.1:
        // `server-final-message = (server-error / verifier) ["," extensions]`.
        // Split on the first comma to isolate the verifier from any
        // trailing `,ext=...`. A naive `base64_decode_bounded` on the
        // full tail fails on the first comma — legitimate extensions
        // would be misclassified as `MalformedServerFinal`.
        let verifier_b64 = match verifier_tail.split_once(',') {
            Some((head, _ext)) => head,
            None => verifier_tail,
        };
        let decoded = base64_decode_bounded(verifier_b64.as_bytes())?;
        // Use `<[u8; 32]>::try_from` over `&[u8]` — this infallibly
        // succeeds iff the slice has length 32, classifying any size
        // mismatch via `MalformedServerFinal` (tier-2 structural
        // exact-length narrowing). A naive dead-arm double-get with
        // silent fallback `SecretDigest::new([0; 32])` on impossible
        // None branches would rely on downstream `ct_eq` rejecting
        // the all-zero signature — fail-closed by accident (tier-4
        // per CREDO §7 axis 12).
        let sig_bytes: [u8; 32] = <[u8; 32]>::try_from(decoded.as_slice())
            .map_err(|_| ScramError::MalformedServerFinal)?;
        Ok(SecretDigest::new(sig_bytes))
    } else if let Some(err_text) = msg_str.strip_prefix("e=") {
        // F30: preserve the server-supplied diagnostic. `err_text` may
        // contain non-ASCII (theoretically — RFC 5802 §7 restricts
        // server-error-value to `value-safe-char` which is ASCII, but
        // a mis-implemented server could deviate). `from_bytes_lossy`
        // guarantees valid-UTF-8 output either way.
        Err(ScramError::ServerScramError {
            message: crate::ident::BoundedStr::from_bytes_lossy(err_text.as_bytes()),
        })
    } else {
        Err(ScramError::MalformedServerFinal)
    }
}

/// Base64-encode into a stack buffer. Returns the length written.
///
/// Uses `base64ct::Base64` — RustCrypto's constant-time, branchless,
/// `no_std` base64 encoder. This step runs on the SCRAM
/// `ClientProof`, which is derived from the user's password via
/// HMAC, so a side-channel-resistant encoder is required.
/// `base64ct` provides RustCrypto-audited constant-time semantics;
/// the `base64` v0.22 `STANDARD` engine is essentially constant-time
/// in practice (cache-line-sized alphabet table) but does not
/// formalise the property.
///
/// `Base64` is the standard-with-padding alphabet (RFC 4648 §4),
/// matching what PG emits. `default-features = false` keeps the
/// crate `no_std` + `no_alloc`.
pub(crate) fn base64_encode_to_buf(
    input: &[u8],
    out: &mut [u8],
) -> Result<usize, ScramError> {
    use base64ct::{Base64, Encoding};

    let encoded_len = Base64::encoded_len(input);
    if encoded_len > out.len() {
        return Err(ScramError::BufferOverflow);
    }
    // `Base64::encode` returns `Result<&str, InvalidLengthError>`.
    // The &str's bytes ARE `out[..encoded_len]`; the returned ref
    // is a borrow into `out`. We care about the length; discard
    // the &str handle.
    Base64::encode(input, out).map_err(|_| ScramError::BufferOverflow)?;
    Ok(encoded_len)
}

/// Base64-decode into a bounded `arrayvec::ArrayVec`.
///
/// Same constant-time guarantees as the encoder, applied to the
/// salt. (The salt is not secret per se — the server sends it
/// cleartext — but consistency with the encode path keeps the
/// side-channel posture uniform across SCRAM wire parsing.)
fn base64_decode_bounded(
    input: &[u8],
) -> Result<arrayvec::ArrayVec<u8, MAX_SALT_LEN>, ScramError> {
    use base64ct::{Base64, Encoding};

    // Strict RFC 4648 decode.
    //
    // `base64ct::Base64` is branchless + constant-time-audited and
    // enforces RFC 4648 §4 alphabet strictly — any whitespace,
    // newlines, or unicode in the input causes decode failure. Real
    // PostgreSQL servers don't emit whitespace-padded base64 in
    // SCRAM messages, but some third-party SCRAM-compatible proxies
    // (PgBouncer with custom configuration, legacy middleware) may.
    //
    // The CREDO §1 stance: safety > interop. Accepting whitespace-
    // padded base64 would:
    // 1. Relax the SCRAM wire invariant (spec says no whitespace).
    // 2. Require pre-strip into a temporary buffer — more complex
    //    code, more unzeroized intermediate (P1-A class).
    // 3. Mask proxy mis-configuration silently (tier-4).
    //
    // Post-fix keeps strict decode but surfaces the specific failure
    // mode to operators: if the input contains bytes outside the
    // base64 alphabet that are ALSO whitespace (common proxy issue),
    // we still fail — but a future log-level enrichment can detect
    // this pattern pre-call to emit a clearer diagnostic. For now,
    // the single `Base64DecodeError` variant subsumes all failure
    // modes; a follow-up `ScramError::WhitespaceInBase64` variant
    // would require caller-side inspection of `input` which is out
    // of scope for this fix.
    let mut decode_buf = [0u8; MAX_SALT_LEN];
    let decoded: &[u8] = Base64::decode(input, &mut decode_buf)
        .map_err(|_| ScramError::Base64DecodeError)?;
    let mut result = arrayvec::ArrayVec::new_const();
    result
        .try_extend_from_slice(decoded)
        .map_err(|_| ScramError::InvalidSalt)?;
    Ok(result)
}

/// Generate a cryptographically random client nonce (18 bytes),
/// base64-encoded.
///
/// Uses `getrandom::getrandom` directly — no hand-rolled entropy.
///
/// # Test injection
///
/// When `cfg(test)`, the `FIXED_TEST_NONCE` thread-local can be set
/// to inject a deterministic nonce for reproducible test vectors.
/// This injection point is physically absent from non-test builds
/// (tier-1 by build configuration).
#[cfg(not(test))]
pub(crate) fn generate_client_nonce() -> Result<arrayvec::ArrayVec<u8, MAX_CLIENT_NONCE_B64_LEN>, ScramError> {
    let mut raw = zeroize::Zeroizing::new([0u8; 18]);
    // Classify randomness failure separately from buffer overflow.
    // A naive `map_err(|_| BufferOverflow)` would produce
    // misleading operator diagnostics on `/dev/urandom` EAGAIN or
    // container-restricted `getrandom` calls.
    getrandom::getrandom(raw.as_mut()).map_err(|_| ScramError::RandomnessUnavailable)?;
    let mut b64_buf = [0u8; MAX_CLIENT_NONCE_B64_LEN];
    let written = base64_encode_to_buf(&*raw, &mut b64_buf)?;
    let mut result = arrayvec::ArrayVec::new_const();
    let src = b64_buf.get(..written).ok_or(ScramError::BufferOverflow)?;
    result
        .try_extend_from_slice(src)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(result)
}

/// Test-only nonce generator with deterministic injection.
///
/// Also supports the `FORCE_RNG_FAILURE` thread-local flag — when
/// set, the test path short-circuits to `Err(RandomnessUnavailable)`
/// without calling `getrandom`. This lets tests exercise the
/// RNG-failure classification path that is otherwise unreachable
/// (getrandom rarely fails on test hosts).
#[cfg(test)]
pub(crate) fn generate_client_nonce() -> Result<arrayvec::ArrayVec<u8, MAX_CLIENT_NONCE_B64_LEN>, ScramError> {
    // Check forced-failure flag first.
    let forced = FORCE_RNG_FAILURE.with(|cell| *cell.borrow());
    if forced {
        return Err(ScramError::RandomnessUnavailable);
    }
    FIXED_TEST_NONCE.with(|cell| {
        if let Some(fixed) = cell.borrow().as_ref() {
            let mut result = arrayvec::ArrayVec::new_const();
            result
                .try_extend_from_slice(fixed.as_bytes())
                .map_err(|_| ScramError::BufferOverflow)?;
            Ok(result)
        } else {
            // No injection — use real randomness.
            let mut raw = zeroize::Zeroizing::new([0u8; 18]);
            // Mirror the production path's typed classification —
            // `RandomnessUnavailable` on `getrandom` failure.
            getrandom::getrandom(raw.as_mut())
                .map_err(|_| ScramError::RandomnessUnavailable)?;
            let mut b64_buf = [0u8; MAX_CLIENT_NONCE_B64_LEN];
            let written = base64_encode_to_buf(&*raw, &mut b64_buf)?;
            let mut result = arrayvec::ArrayVec::new_const();
            let src = b64_buf.get(..written).ok_or(ScramError::BufferOverflow)?;
            result
                .try_extend_from_slice(src)
                .map_err(|_| ScramError::BufferOverflow)?;
            Ok(result)
        }
    })
}

// Thread-local slot for deterministic nonce injection in tests.
#[cfg(test)]
std::thread_local! {
    static FIXED_TEST_NONCE: std::cell::RefCell<Option<std::string::String>> =
        const { std::cell::RefCell::new(None) };
    /// Thread-local flag to force `generate_client_nonce` to return
    /// `Err(RandomnessUnavailable)` without calling `getrandom`.
    /// Lets tests exercise the RNG-failure classification path.
    static FORCE_RNG_FAILURE: std::cell::RefCell<bool> =
        const { std::cell::RefCell::new(false) };
}

/// RAII guard for `FORCE_RNG_FAILURE`.
///
/// Mirrors [`ScopedTestNonce`] pattern for panic-safe cleanup.
/// Constructing installs the flag; Drop clears it.
#[cfg(test)]
pub(crate) struct ScopedForceRngFailure(core::marker::PhantomData<()>);

#[cfg(test)]
impl ScopedForceRngFailure {
    pub(crate) fn new() -> Self {
        FORCE_RNG_FAILURE.with(|cell| {
            *cell.borrow_mut() = true;
        });
        Self(core::marker::PhantomData)
    }
}

#[cfg(test)]
impl Drop for ScopedForceRngFailure {
    fn drop(&mut self) {
        FORCE_RNG_FAILURE.with(|cell| {
            *cell.borrow_mut() = false;
        });
    }
}

/// Set a fixed nonce for the current test (test-only).
///
/// `#[cfg(test)]` gates it to unit-test builds only — integration
/// tests under `tests/*.rs` compile the crate with `#[cfg(not(test))]`
/// and never see it, which is the correct scope. `pub(crate)` is
/// sufficient for the `#[cfg(test)] mod tests` unit tests that
/// actually call it; the earlier `pub` was a dead-code-lint
/// workaround from before the test-vector scaffolding landed.
///
/// # Prefer [`ScopedTestNonce::new`]
///
/// This function sets the thread-local without an automatic cleanup
/// path. A test that calls `set_test_nonce(..)` and then panics
/// leaves the thread-local populated for the NEXT test on the same
/// thread — leading to non-deterministic failures where a test that
/// expected a real-random nonce gets the stale injected one from a
/// prior panicking run.
///
/// Prefer [`ScopedTestNonce::new`] which is an RAII guard: its Drop
/// impl clears the thread-local even on panic unwinding. Keep the
/// raw `set_test_nonce` available only for existing tests that
/// manually clear in teardown; new tests should use `ScopedTestNonce`.
#[cfg(test)]
pub(crate) fn set_test_nonce(nonce: &str) {
    FIXED_TEST_NONCE.with(|cell| {
        *cell.borrow_mut() = Some(std::string::String::from(nonce));
    });
}

/// RAII guard for [`FIXED_TEST_NONCE`] injection.
///
/// Construct via [`ScopedTestNonce::new(nonce)`]; the constructor
/// installs the nonce in the thread-local, and the Drop impl clears
/// it when the guard goes out of scope. Ensures that a panicking test
/// does NOT leave a stale nonce behind for the next test on the same
/// thread (previously possible with bare `set_test_nonce` because
/// there was no unwind-safe cleanup path).
///
/// Usage:
/// ```ignore
/// #[test]
/// fn scram_with_fixed_nonce() {
///     let _guard = ScopedTestNonce::new("client-test-nonce");
///     // ... test body; panic-safe: guard's Drop clears thread-local
/// }
/// ```
///
/// Caveat: under `panic = "abort"` in release profile, Drop does
/// NOT run. But tests compile with `panic = "unwind"` by default
/// (cargo test), so this guard works as designed in the test
/// harness. The guard is `#[cfg(test)]`-only.
/// RAII guard — no instance state; all side effects live in the
/// thread-local `FIXED_TEST_NONCE`. The zero-sized marker struct
/// gives us a Drop-bearing value to bind to; `PhantomData<()>` marks
/// the unit-style construction honestly (no `_private` underscore-
/// field per user feedback).
#[cfg(test)]
pub(crate) struct ScopedTestNonce(core::marker::PhantomData<()>);

#[cfg(test)]
impl ScopedTestNonce {
    pub(crate) fn new(nonce: &str) -> Self {
        set_test_nonce(nonce);
        Self(core::marker::PhantomData)
    }
}

#[cfg(test)]
impl Drop for ScopedTestNonce {
    fn drop(&mut self) {
        FIXED_TEST_NONCE.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}

/// Why a decimal u32 parse failed.
///
/// F29 (2026-04-21): preserves the specific failure mode for
/// future callers that want structured diagnostic (e.g., distinguishing
/// "iteration count is too large" from "iteration count field is empty"
/// from "iteration count has non-digit bytes"). Current call sites
/// collapse all three to `ScramError::MalformedServerFirst` via
/// `.map_err(|_| ...)` — structurally the right classification since
/// all three are "malformed server-first-message".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParseU32Error {
    /// Input byte slice was empty.
    Empty,
    /// Non-decimal-digit byte encountered (outside `b'0'..=b'9'`).
    InvalidDigit,
    /// Accumulated value overflows `u32::MAX`.
    Overflow,
}

/// Parse a decimal u32 from ASCII bytes with typed error classification.
///
/// Implementation routes through stdlib `u32::from_str_radix` via
/// `core::str::from_utf8` + `ParseIntError::kind()` mapping. The
/// stdlib impl is heavily optimised (LLVM folds pure integer
/// parsing, SIMD fast path on some targets). Kind-mapping preserves
/// the three typed variants (`Empty`, `InvalidDigit`, `Overflow`)
/// that callers may eventually surface; current SCRAM callers
/// collapse all three via `.map_err(|_| MalformedServerFirst)`.
fn parse_u32(bytes: &[u8]) -> Result<u32, ParseU32Error> {
    use core::num::IntErrorKind;
    let s = core::str::from_utf8(bytes).map_err(|_| ParseU32Error::InvalidDigit)?;
    s.parse::<u32>().map_err(|e| match e.kind() {
        IntErrorKind::Empty => ParseU32Error::Empty,
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => ParseU32Error::Overflow,
        // `InvalidDigit` (and any future IntErrorKind variant) collapses here.
        _ => ParseU32Error::InvalidDigit,
    })
}

#[cfg(test)]
mod tests {
    //! Unit tests for the SCRAM wire helpers.
    //!
    //! Test nonce injection (`ScopedTestNonce` / `FIXED_TEST_NONCE`)
    //! is exercised here — also serves as the "caller" that kills
    //! the `dead_code` lint on the injection surface. The RAII-guard
    //! pattern replaces the prior raw `set_test_nonce` + manual
    //! cleanup shape so a panic mid-test cannot leak a stale nonce
    //! into the next test on the same thread.
    use super::{
        ParseU32Error, ScopedForceRngFailure, ScopedTestNonce, ScramError,
        generate_client_nonce, parse_u32,
    };

    #[test]
    fn fixed_test_nonce_injection_round_trips() {
        let injected = "Fyko+d2lbbFgONRv9qkxdawL";
        // RAII guard — Drop clears the thread-local even on panic
        // unwind. A naive `set_test_nonce` + explicit
        // `FIXED_TEST_NONCE.with(..)` cleanup at end-of-test would
        // leak state on any panic between the two calls.
        //
        // Named binding (not `_guard`) per no-underscore-vars user
        // feedback — explicit `drop(guard)` at end is the structural
        // drop signal.
        let guard = ScopedTestNonce::new(injected);

        let generated = generate_client_nonce();
        assert!(generated.is_ok(), "generate_client_nonce must succeed with injected fixed nonce");
        let bytes = match generated {
            Ok(v) => v,
            Err(_) => return,
        };
        assert_eq!(
            bytes.as_slice(),
            injected.as_bytes(),
            "injected nonce must round-trip verbatim via FIXED_TEST_NONCE slot",
        );

        drop(guard);
    }

    #[test]
    fn parse_u32_classifies_failure_modes() {
        assert_eq!(parse_u32(b""), Err(ParseU32Error::Empty));
        assert_eq!(parse_u32(b"12a"), Err(ParseU32Error::InvalidDigit));
        assert_eq!(parse_u32(b"4294967296"), Err(ParseU32Error::Overflow));
        assert_eq!(parse_u32(b"4096"), Ok(4096));
        assert_eq!(parse_u32(b"0"), Ok(0));
        assert_eq!(parse_u32(b"4294967295"), Ok(u32::MAX));
    }

    /// Exercise the `RandomnessUnavailable` error path via forced
    /// injection. The path is architecturally reachable only if
    /// `getrandom` fails (rare on test hosts — containers with
    /// restricted syscalls, /dev/urandom starvation, etc.); the
    /// `ScopedForceRngFailure` RAII guard forces the Err branch
    /// deterministically.
    #[test]
    fn forced_rng_failure_classifies_as_randomness_unavailable() {
        let guard = ScopedForceRngFailure::new();
        let result = generate_client_nonce();
        assert!(
            matches!(result, Err(ScramError::RandomnessUnavailable)),
            "forced RNG failure must classify as RandomnessUnavailable, got {result:?}",
        );
        drop(guard);

        // Post-guard drop: next call should succeed (fall-through to
        // real getrandom or FIXED_TEST_NONCE if set).
        let after = generate_client_nonce();
        assert!(
            after.is_ok(),
            "post-drop: generate_client_nonce must recover, got {after:?}",
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Crypto-free total-function + verifier fuzz for the SCRAM wire parsers.
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod total_function_fuzz {
    //! Universal-coverage, CRYPTO-FREE total-function proof for the two SCRAM
    //! wire parsers ([`parse_server_first`](super::parse_server_first),
    //! [`parse_server_final`](super::parse_server_final)) and the constant-time
    //! signature verifier ([`SecretDigest::ct_eq`](super::SecretDigest::ct_eq)).
    //!
    //! # Why in-crate, and why crypto-free
    //!
    //! The parsers are `pub(crate)`, so a DIRECT fuzz can only live INSIDE the
    //! crate (an integration-test crate cannot name them — which is exactly why
    //! `tests/scram_fuzz_spec.rs` had to route random `server-final` bytes
    //! through the connecting engine, paying one full PBKDF2 key derivation per
    //! sample). Fuzzing the parsers here decouples the panic-safety proof from
    //! SCRAM's crypto entirely: no `salted_password`, no HMAC, no PBKDF2 runs,
    //! so 50k+ adversarial iterations sweep BOTH parsers in well under a second
    //! instead of minutes. The genuinely engine-bound never-Ready invariant
    //! (which does need one derivation apiece) keeps a small, crafted witness
    //! table in `tests/scram_fuzz_spec.rs`.
    //!
    //! # Invariants proven
    //!
    //! 1. **Total function.** On ANY input — malformed, truncated, random, or
    //!    hostile — each parser returns `Ok(_)` or a CLASSIFIED
    //!    [`ScramError`](super::ScramError), and NEVER panics or aborts. Proven
    //!    at the machine level under `catch_unwind`: strictly stronger than the
    //!    source `forbid`-bundle, since it also catches a panic hiding in a
    //!    dependency the parser calls (`base64ct`, `arrayvec`,
    //!    `core::str::from_utf8`).
    //! 2. **Never silent-pass (parser).** An `Ok` from `parse_server_final`
    //!    implies a `v=`-prefixed input; an `Ok` from `parse_server_first`
    //!    implies its documented guarantees (iterations in `[4096, 100000]`, a
    //!    non-empty salt within the bounded buffer, a client-nonce-prefixed
    //!    server nonce).
    //! 3. **Never silent-pass (verifier).** [`SecretDigest::ct_eq`] returns
    //!    `1` (equal) if and ONLY if the two 32-byte digests are byte-equal — a
    //!    randomly-parsed verifier never spuriously matches the expected
    //!    signature. This is the parser-level analog of the engine's "never
    //!    reaches Ready" invariant (in the engine, this ct_eq is the sole gate
    //!    between a parsed `server-final` and advancing the handshake).
    //!
    //! # Teeth
    //!
    //! - A deliberately-planted panic is routed through the SAME `catch_unwind`
    //!   harness and MUST be caught + captured — this proves the net has teeth
    //!   AND (since it would abort, not report, under a `panic="abort"` profile)
    //!   confirms the test profile unwinds. Planted via
    //!   `assert!(core::hint::black_box(false), …)` rather than `panic!`: the
    //!   crate `forbid`-bundle bars `panic!` even in test code, and `assert!` is
    //!   exempt (`black_box` hides the `false` from `assertions_on_constants`).
    //! - A minimum-iteration floor (`>= 50_000`) refuses a vacuous pass.
    //! - A per-parser accept/reject floor (both `> 0`) proves the sweep reached
    //!   the real accept AND reject branches, not only an early length guard.
    //! - A positive + negative verifier control proves `ct_eq` actually
    //!   discriminates (a broken all-equal `ct_eq` would fail the negative
    //!   control's `matched == false` expectation).

    use super::{
        MAX_SALT_LEN, MAX_SCRAM_ITERATIONS, MIN_SCRAM_ITERATIONS, SecretDigest,
        base64_encode_to_buf, parse_server_final, parse_server_first,
    };
    use std::boxed::Box;
    use std::cell::RefCell;
    use std::panic::{self, AssertUnwindSafe};
    use std::string::{String, ToString};
    use std::vec::Vec;
    use std::{eprintln, format, thread_local};

    /// Fixed nonzero seed — every run is byte-identical, so any finding is
    /// reproducible from the printed input. Xorshift64 fixes `0`, hence nonzero.
    const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
    /// Main-sweep iterations; each probes BOTH parsers, so probes ≈ `2 × ITERS`
    /// plus the structured controls. The floor asserts `ITERS` actually ran.
    const ITERS: u32 = 50_000;
    /// The expected-signature constant the verifier fuzz checks `ct_eq` against.
    /// Arbitrary non-trivial pattern; a random parsed verifier equalling it is a
    /// `2^-256` event, so `matched` is effectively always `false` in the sweep —
    /// the positive control below forces the `true` branch deterministically.
    const EXPECTED_SIG: [u8; 32] = [0xA5; 32];
    /// A fixed client nonce for `parse_server_first` (a real base64 SCRAM nonce
    /// shape). The parser validates the server nonce carries this as a prefix.
    const CLIENT_NONCE: &[u8] = b"Fyko+d2lbbFgONRv9qkxdawL";

    // ── deterministic xorshift64 PRNG (no dependency, no clock) ──
    struct Rng(u64);
    impl Rng {
        const fn new(seed: u64) -> Self {
            Self(if seed == 0 { SEED } else { seed })
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn byte(&mut self) -> u8 {
            self.next_u64().to_le_bytes()[0]
        }
        /// A value in `0..bound` (or `0` for an empty bound). `checked_rem`
        /// (not the `%` operator) keeps `clippy::arithmetic_side_effects` quiet;
        /// its `None` arm is dead (the bound is guarded nonzero above).
        fn bounded(&mut self, bound: usize) -> usize {
            if bound == 0 {
                return 0;
            }
            let draw = usize::from(u16::from_le_bytes([self.byte(), self.byte()]));
            draw.checked_rem(bound).unwrap_or(0)
        }
        /// Overwrite `out` with `len` pseudo-random bytes (8 per step).
        fn fill(&mut self, out: &mut Vec<u8>, len: usize) {
            out.clear();
            while out.len() < len {
                let word = self.next_u64().to_le_bytes();
                for &b in word.iter() {
                    if out.len() >= len {
                        break;
                    }
                    out.push(b);
                }
            }
        }
    }

    // ── recording panic hook + RAII restore (mirrors core/decoder_fuzz) ──
    type PanicHook = Box<dyn Fn(&panic::PanicHookInfo<'_>) + Sync + Send + 'static>;
    thread_local! {
        static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
    }
    struct HookGuard {
        prev: Option<PanicHook>,
    }
    impl HookGuard {
        fn install() -> Self {
            let prev = panic::take_hook();
            panic::set_hook(Box::new(|info| {
                let loc = match info.location() {
                    Some(l) => format!("{}:{}:{}", l.file(), l.line(), l.column()),
                    None => String::from("<unknown location>"),
                };
                let payload = info.payload();
                let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    String::from("<non-string panic payload>")
                };
                LAST_PANIC.with(|slot| {
                    *slot.borrow_mut() = Some(format!("{msg} @ {loc}"));
                });
            }));
            Self { prev: Some(prev) }
        }
    }
    impl Drop for HookGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.prev.take() {
                panic::set_hook(prev);
            }
        }
    }

    /// The outcome of one parser probe under `catch_unwind`.
    enum Probe {
        /// The parser returned `Ok` — the input happened to be well-formed. The
        /// structural + verifier cross-checks already ran and held.
        Accepted,
        /// The parser returned a classified `Err` — the honest total path.
        Rejected,
        /// The parser PANICKED — a real finding. Carries the captured message.
        Panicked(String),
    }

    fn take_captured_panic() -> String {
        match LAST_PANIC.with(|slot| slot.borrow_mut().take()) {
            Some(m) => m,
            None => String::from("<no panic message captured>"),
        }
    }

    /// Base64-encode `bytes` to an owned ASCII `Vec` for building `v=`/`s=`
    /// fields. Bounded 128-byte scratch covers every input this fuzz encodes
    /// (`<= 48` bytes → `<= 64` chars); an `Err` yields an empty vec (still a
    /// legal — rejectable — fuzz input, never a panic).
    fn b64(bytes: &[u8]) -> Vec<u8> {
        let mut out = [0u8; 128];
        match base64_encode_to_buf(bytes, &mut out) {
            Ok(n) => out.get(..n).unwrap_or(&[]).to_vec(),
            Err(_) => Vec::new(),
        }
    }

    /// Fuzz [`parse_server_final`] under `catch_unwind`. On `Ok`, cross-check the
    /// `v=` structural guarantee AND the constant-time verifier against
    /// `EXPECTED_SIG` — `ct_eq` must agree with plain byte-equality (proving it
    /// never silently matches a non-equal verifier).
    fn probe_final(input: &[u8]) -> Probe {
        LAST_PANIC.with(|slot| *slot.borrow_mut() = None);
        match panic::catch_unwind(AssertUnwindSafe(|| parse_server_final(input))) {
            Ok(Ok(received)) => {
                assert!(
                    input.starts_with(b"v="),
                    "parse_server_final returned Ok on a non-`v=` input: {input:?}",
                );
                let expected = SecretDigest::new(EXPECTED_SIG);
                let matched = bool::from(expected.ct_eq(&received));
                let byte_equal = received.as_bytes() == &EXPECTED_SIG;
                assert_eq!(
                    matched, byte_equal,
                    "SecretDigest::ct_eq disagreed with byte-equality on a parsed verifier \
                     (silent-match hazard): matched={matched} byte_equal={byte_equal}",
                );
                Probe::Accepted
            }
            Ok(Err(_class)) => Probe::Rejected,
            Err(_) => Probe::Panicked(take_captured_panic()),
        }
    }

    /// Fuzz [`parse_server_first`] under `catch_unwind`. On `Ok`, cross-check the
    /// parser's documented acceptance guarantees.
    fn probe_first(msg: &[u8]) -> Probe {
        LAST_PANIC.with(|slot| *slot.borrow_mut() = None);
        match panic::catch_unwind(AssertUnwindSafe(|| parse_server_first(msg, CLIENT_NONCE))) {
            Ok(Ok(sf)) => {
                assert!(
                    sf.iterations >= MIN_SCRAM_ITERATIONS && sf.iterations <= MAX_SCRAM_ITERATIONS,
                    "parse_server_first accepted out-of-range iterations {}",
                    sf.iterations,
                );
                assert!(
                    !sf.salt.is_empty() && sf.salt.len() <= MAX_SALT_LEN,
                    "parse_server_first accepted an out-of-bounds salt (len {})",
                    sf.salt.len(),
                );
                assert!(
                    sf.server_nonce.as_bytes().starts_with(CLIENT_NONCE),
                    "parse_server_first accepted a server nonce not prefixed by the client nonce",
                );
                Probe::Accepted
            }
            Ok(Err(_class)) => Probe::Rejected,
            Err(_) => Probe::Panicked(take_captured_panic()),
        }
    }

    /// Build an adversarial `server-final` candidate under one of several shaping
    /// modes (fully random, `v=`/`e=`/`x=`-prefixed, random-base64, valid-base64
    /// of K bytes so K==32 occasionally accepts, valid-32 + trailing extension).
    fn shape_final(rng: &mut Rng, buf: &mut Vec<u8>) {
        let mode = rng.bounded(7);
        buf.clear();
        let mut rand = Vec::new();
        match mode {
            0 => {
                let n = rng.bounded(257);
                rng.fill(buf, n);
            }
            1 => {
                buf.extend_from_slice(b"v=");
                let n = rng.bounded(200);
                rng.fill(&mut rand, n);
                buf.extend_from_slice(&rand);
            }
            2 => {
                buf.extend_from_slice(b"e=");
                let n = rng.bounded(96);
                rng.fill(&mut rand, n);
                buf.extend_from_slice(&rand);
            }
            3 => {
                // `v=` + random base64-alphabet chars → reaches the decoder.
                buf.extend_from_slice(b"v=");
                let n = rng.bounded(64);
                for _ in 0..n {
                    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
                    let idx = rng.bounded(alphabet.len());
                    buf.push(*alphabet.get(idx).unwrap_or(&b'A'));
                }
            }
            4 => {
                // `v=` + valid base64 of K random bytes; K==32 → accept path.
                let k = rng.bounded(49);
                rng.fill(&mut rand, k);
                buf.extend_from_slice(b"v=");
                buf.extend_from_slice(&b64(&rand));
            }
            5 => {
                // valid 32-byte verifier + a trailing RFC extension part.
                rng.fill(&mut rand, 32);
                buf.extend_from_slice(b"v=");
                buf.extend_from_slice(&b64(&rand));
                buf.extend_from_slice(b",ext=");
                let mut ext = Vec::new();
                let n = rng.bounded(16);
                rng.fill(&mut ext, n);
                buf.extend_from_slice(&ext);
            }
            _ => {
                // no recognised prefix.
                let n = rng.bounded(64);
                rng.fill(&mut rand, n);
                buf.extend_from_slice(b"x=");
                buf.extend_from_slice(&rand);
            }
        }
    }

    /// Build an adversarial `server-first` candidate under one of several shaping
    /// modes (random, partial, fully valid, wrong-nonce, out-of-range iterations,
    /// reserved-mext, valid + trailing extension, non-utf8 injection).
    fn shape_first(rng: &mut Rng, buf: &mut Vec<u8>) {
        let mode = rng.bounded(8);
        buf.clear();
        let mut salt = Vec::new();
        match mode {
            0 => {
                let n = rng.bounded(257);
                rng.fill(buf, n);
            }
            1 => {
                buf.extend_from_slice(b"r=");
                buf.extend_from_slice(CLIENT_NONCE);
                let mut tail = Vec::new();
                let n = rng.bounded(48);
                rng.fill(&mut tail, n);
                buf.extend_from_slice(&tail);
            }
            2 => {
                // fully valid: r=<client_nonce><srv>,s=<b64 1..16>,i=<4096..100000>.
                buf.extend_from_slice(b"r=");
                buf.extend_from_slice(CLIENT_NONCE);
                buf.extend_from_slice(b"SRVNONCE,s=");
                let salt_len = rng.bounded(15).saturating_add(1);
                rng.fill(&mut salt, salt_len);
                buf.extend_from_slice(&b64(&salt));
                let iters =
                    MIN_SCRAM_ITERATIONS.saturating_add(u32_of(rng.bounded(95905)));
                extend_i(buf, iters);
            }
            3 => {
                // wrong nonce prefix → NoncePrefixMismatch.
                buf.extend_from_slice(b"r=WRONGNONCE,s=");
                rng.fill(&mut salt, 8);
                buf.extend_from_slice(&b64(&salt));
                extend_i(buf, 4096);
            }
            4 => {
                // valid shape, out-of-range iterations → TooLow / TooHigh.
                buf.extend_from_slice(b"r=");
                buf.extend_from_slice(CLIENT_NONCE);
                buf.extend_from_slice(b",s=");
                rng.fill(&mut salt, 8);
                buf.extend_from_slice(&b64(&salt));
                let iters = if rng.bounded(2) == 0 {
                    u32_of(rng.bounded(4096)) // 0..4095 (below MIN)
                } else {
                    MAX_SCRAM_ITERATIONS.saturating_add(u32_of(rng.bounded(1000)).saturating_add(1))
                };
                extend_i(buf, iters);
            }
            5 => {
                // reserved-mext mandatory extension → MalformedServerFirst.
                buf.extend_from_slice(b"m=unsupported,r=");
                buf.extend_from_slice(CLIENT_NONCE);
                buf.extend_from_slice(b",s=Wg==,i=4096");
            }
            6 => {
                // valid + trailing optional extension (silently ignored).
                buf.extend_from_slice(b"r=");
                buf.extend_from_slice(CLIENT_NONCE);
                buf.extend_from_slice(b"SRV,s=");
                rng.fill(&mut salt, 6);
                buf.extend_from_slice(&b64(&salt));
                extend_i(buf, 8192);
                buf.extend_from_slice(b",ext=whatever");
            }
            _ => {
                // non-utf8 injection into an otherwise-shaped message.
                buf.extend_from_slice(b"r=");
                buf.push(0xFF);
                buf.push(0xFE);
                buf.extend_from_slice(CLIENT_NONCE);
                buf.extend_from_slice(b",s=Wg==,i=4096");
            }
        }
    }

    /// Lossless `usize → u32` for a small bounded draw (saturates on the
    /// impossible-large branch; `as` is banned crate-wide).
    fn u32_of(v: usize) -> u32 {
        u32::try_from(v).unwrap_or(u32::MAX)
    }

    /// Append `i=<n>` to a candidate `server-first`.
    fn extend_i(buf: &mut Vec<u8>, iters: u32) {
        buf.extend_from_slice(format!(",i={iters}").as_bytes());
    }

    #[test]
    fn scram_wire_parsers_are_total_functions_and_verifier_never_silent_matches() {
        // Install the recording hook for the whole fuzz (restored on scope exit).
        let guard = HookGuard::install();

        // ── Teeth: a deliberately-planted panic MUST be caught + captured by the
        // exact same harness (proves the net has teeth AND the profile unwinds).
        // `assert!(black_box(false), …)` panics without tripping the crate
        // `forbid(clippy::panic)` (assert! is exempt) or `assertions_on_constants`.
        LAST_PANIC.with(|slot| *slot.borrow_mut() = None);
        let planted = panic::catch_unwind(AssertUnwindSafe(|| {
            assert!(
                core::hint::black_box(false),
                "planted teeth panic — intentional",
            );
        }));
        let teeth_msg = take_captured_panic();
        let teeth_ok = planted.is_err() && teeth_msg.contains("planted teeth panic");

        // ── Deterministic verifier controls (guarantee both ct_eq branches run).
        // Positive: `v=<b64 of EXPECTED_SIG>` accepts AND ct_eq matches.
        let mut ctrl = Vec::new();
        ctrl.extend_from_slice(b"v=");
        ctrl.extend_from_slice(&b64(&EXPECTED_SIG));
        let pos_ok = matches!(probe_final(&ctrl), Probe::Accepted)
            && bool::from(
                SecretDigest::new(EXPECTED_SIG)
                    .ct_eq(&match parse_server_final(&ctrl) {
                        Ok(sig) => sig,
                        Err(_) => SecretDigest::new([0; 32]),
                    }),
            );
        // Negative: `v=<b64 of a different 32-byte value>` accepts but ct_eq must
        // NOT match (a broken all-equal ct_eq would fail here).
        let mut neg = Vec::new();
        neg.extend_from_slice(b"v=");
        neg.extend_from_slice(&b64(&[0x00; 32]));
        let neg_ok = matches!(probe_final(&neg), Probe::Accepted)
            && !bool::from(
                SecretDigest::new(EXPECTED_SIG)
                    .ct_eq(&match parse_server_final(&neg) {
                        Ok(sig) => sig,
                        Err(_) => SecretDigest::new(EXPECTED_SIG),
                    }),
            );

        // ── The main sweep.
        let mut rng = Rng::new(SEED);
        let mut buf: Vec<u8> = Vec::new();
        let mut findings: Vec<String> = Vec::new();
        let mut iterations: u64 = 0;
        let mut probes: u64 = 0;
        let mut final_accept: u64 = 0;
        let mut final_reject: u64 = 0;
        let mut first_accept: u64 = 0;
        let mut first_reject: u64 = 0;

        for _ in 0..ITERS {
            iterations = iterations.saturating_add(1);

            shape_final(&mut rng, &mut buf);
            match probe_final(&buf) {
                Probe::Accepted => final_accept = final_accept.saturating_add(1),
                Probe::Rejected => final_reject = final_reject.saturating_add(1),
                Probe::Panicked(msg) => {
                    findings.push(format!("parse_server_final panicked on {buf:?}: {msg}"))
                }
            }
            probes = probes.saturating_add(1);

            shape_first(&mut rng, &mut buf);
            match probe_first(&buf) {
                Probe::Accepted => first_accept = first_accept.saturating_add(1),
                Probe::Rejected => first_reject = first_reject.saturating_add(1),
                Probe::Panicked(msg) => {
                    findings.push(format!("parse_server_first panicked on {buf:?}: {msg}"))
                }
            }
            probes = probes.saturating_add(1);
        }

        // Restore the normal hook BEFORE the assertions, so any failure prints.
        drop(guard);

        // ── Teeth + control assertions.
        assert!(
            teeth_ok,
            "harness has NO TEETH: a planted panic was not caught + captured \
             (catch_unwind/hook capture broken) — a green run would prove nothing; got {teeth_msg:?}",
        );
        assert!(
            pos_ok,
            "verifier positive control failed: `v=<b64(EXPECTED_SIG)>` must parse AND ct_eq-match",
        );
        assert!(
            neg_ok,
            "verifier negative control failed: a different verifier must parse but NOT ct_eq-match \
             (ct_eq is silently matching non-equal digests)",
        );

        // ── Vacuous-pass guards.
        assert!(
            iterations >= u64::from(ITERS),
            "fuzz ran {iterations} iterations (< {ITERS}) — the sweep is not exercising the surface",
        );
        assert!(
            probes >= 100_000,
            "fuzz recorded only {probes} probes (< 100k)",
        );
        assert!(
            final_accept > 0 && final_reject > 0,
            "parse_server_final sweep did not reach BOTH accept ({final_accept}) and reject ({final_reject})",
        );
        assert!(
            first_accept > 0 && first_reject > 0,
            "parse_server_first sweep did not reach BOTH accept ({first_accept}) and reject ({first_reject})",
        );

        // ── The total-function claim: ZERO parser panicked on ANY input.
        assert!(
            findings.is_empty(),
            "{} SCRAM PARSER PANIC(S) FOUND — a hostile server byte could crash the driver:\n{}",
            findings.len(),
            findings.join("\n"),
        );

        eprintln!(
            "scram total_function_fuzz: {probes} probes ({final_accept}+{final_reject} final, \
             {first_accept}+{first_reject} first), 0 panics (seed {SEED:#018x})",
        );
    }
}
