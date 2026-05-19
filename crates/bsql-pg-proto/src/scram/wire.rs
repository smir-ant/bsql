//! SCRAM-SHA-256 text-protocol message construction and parsing.
//!
//! This module handles the four SCRAM messages exchanged during
//! authentication (RFC 5802 §7):
//!
//! 1. **client-first-message** — `n,,n=<user>,r=<nonce>` (we build it)
//! 2. **server-first-message** — `r=<nonce>,s=<salt>,i=<iters>` (we parse it)
//! 3. **client-final-message** — `c=biws,r=<nonce>,p=<proof>` (we build it)
//! 4. **server-final-message** — `v=<verifier>` or `e=<error>` (we parse it)
//!
//! This is protocol text, not crypto — it is allowed to be hand-written.
//! All cryptographic operations are in [`super::crypto`].
//!
//! # Channel binding
//!
//! Phase 1b does not support channel binding (deferred to 1e as
//! DEF-053). The GS2 header is always `n,,` and the channel binding
//! data is always `biws` (base64 of `n,,`).

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

/// Maximum byte length for the full client-first-message (with GS2 header).
pub const MAX_CLIENT_FIRST_MSG_LEN: usize = 136;

/// Maximum byte length for the client-final-message.
///
/// `c=biws,r=<server_nonce>,p=<proof_b64>` where server nonce can be
/// up to MAX_SERVER_NONCE_LEN and proof_b64 is 44 chars.
pub const MAX_CLIENT_FINAL_MSG_LEN: usize = 384;

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
/// DEF-184 (B28): derived via `usize::div_ceil` (which is method
/// call, not `/` operator, so `clippy::integer_division` does not
/// flag it). Formula: base64 encodes 3 bytes into 4 chars,
/// unpadded length = `ceil(n/3) * 4`. For SHA-256 (32 bytes):
/// `ceil(32/3) * 4 = 11 * 4 = 44` chars — matches the old hard-
/// coded magic number. Drift pin: if `SHA256_DIGEST_LEN` ever
/// changes (it won't — SHA-256 is forever 32 bytes per RFC 6234)
/// or the formula is off, the static_assert below catches it.
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
        .saturating_add(crate::wire::SCRAM_SHA_256_MECHANISM.len())
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
// Drift guards (DEF-057)
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

/// Worst-case full `client-first-message`: GS2 header + bare.
const fn expected_client_first_msg_size() -> usize {
    3usize // GS2 header "n,,"
        .saturating_add(MAX_CLIENT_FIRST_BARE_LEN)
}
const _: () = assert!(
    MAX_CLIENT_FIRST_MSG_LEN >= expected_client_first_msg_size(),
    "MAX_CLIENT_FIRST_MSG_LEN below GS2 header + client-first-bare",
);

/// Worst-case `client-final-message`: `c=biws,r=<server_nonce>,p=<proof_b64>`.
const fn expected_client_final_msg_size() -> usize {
    2usize // "c="
        .saturating_add(4) // "biws" (base64 of GS2 "n,,")
        .saturating_add(3) // ",r="
        .saturating_add(crate::scram::types::MAX_SERVER_NONCE_LEN)
        .saturating_add(3) // ",p="
        .saturating_add(SHA256_PROOF_B64_LEN)
}
const _: () = assert!(
    MAX_CLIENT_FINAL_MSG_LEN >= expected_client_final_msg_size(),
    "MAX_CLIENT_FINAL_MSG_LEN below worst-case c=biws,r=<nonce>,p=<proof>",
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
/// `#[non_exhaustive]` (pass #6 audit MI6) — SCRAM wire-spec
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
    /// minutes per connection (pass #6 audit BS8).
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
    /// # Why fail-closed, not silent-zero (pass #6 F54)
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
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the SCRAM-handshake error.
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
        }
    }
}

/// GS2 header for no channel binding: `n,,`
const GS2_HEADER: &[u8] = b"n,,";

/// Base64 of `n,,` — the channel binding data echoed in client-final.
/// `biws` is the standard encoding of the GS2 header without channel binding.
const CBIND_DATA: &[u8] = b"biws";

/// Build the client-first-message-bare: `n=<user>,r=<nonce_b64>`.
///
/// Returns the bare message (without GS2 header) in a bounded buffer.
/// The caller prepends the GS2 header (`n,,`) for the full client-first.
pub(crate) fn build_client_first_bare(
    user: &[u8],
    client_nonce_b64: &[u8],
) -> Result<heapless::Vec<u8, MAX_CLIENT_FIRST_BARE_LEN>, ScramError> {
    let mut buf = heapless::Vec::new();
    // "n=" + user + ",r=" + nonce
    buf.extend_from_slice(b"n=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(user)
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(b",r=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(client_nonce_b64)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(buf)
}

/// Build the full client-first-message: GS2 header + bare.
///
/// `n,,n=<user>,r=<nonce_b64>`
pub(crate) fn build_client_first_message(
    user: &[u8],
    client_nonce_b64: &[u8],
) -> Result<heapless::Vec<u8, MAX_CLIENT_FIRST_MSG_LEN>, ScramError> {
    let mut msg = heapless::Vec::new();
    msg.extend_from_slice(GS2_HEADER)
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.extend_from_slice(b"n=")
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.extend_from_slice(user)
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.extend_from_slice(b",r=")
        .map_err(|_| ScramError::BufferOverflow)?;
    msg.extend_from_slice(client_nonce_b64)
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
/// ([`crate::ProtoState::ConnectingScramAwaitingServerFinal`]) carries
/// only the POD `SecretDigest` — the `heapless::Vec` fields here
/// never propagate Drop into the state enum.
///
/// Consequence for audits: a "replace `heapless::Vec` with `PodBytes`
/// for state-Drop-cleanliness" suggestion does NOT apply here. The
/// fields are fine as `heapless::Vec` — the Drop surface is bounded
/// to the parse frame's stack scope.
#[derive(Debug)]
pub(crate) struct ServerFirst {
    /// The full server nonce (`r=<value>`) — must start with client nonce.
    pub(crate) server_nonce: CappedServerNonce,
    /// Base64-decoded salt.
    pub(crate) salt: heapless::Vec<u8, MAX_SALT_LEN>,
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
    // Pass #6 BS8: bound the upper end. A malicious server sending
    // `iterations = u32::MAX` stalls PBKDF2 for minutes per connection
    // — client-side DoS. Reject anything above the 10M sanity cap.
    if iterations > MAX_SCRAM_ITERATIONS {
        return Err(ScramError::IterationsTooHigh { iterations });
    }

    Ok(ServerFirst {
        server_nonce,
        salt,
        iterations,
    })
}

/// Build the client-final-message-without-proof: `c=biws,r=<server_nonce>`.
///
/// This is used as part of the AuthMessage and to construct the full
/// client-final-message (by appending `,p=<proof_b64>`).
pub(crate) fn build_client_final_without_proof(
    server_nonce: &[u8],
) -> Result<heapless::Vec<u8, MAX_CLIENT_FINAL_MSG_LEN>, ScramError> {
    let mut buf = heapless::Vec::new();
    buf.extend_from_slice(b"c=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(CBIND_DATA)
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(b",r=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(server_nonce)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(buf)
}

/// Build the complete client-final-message: `c=biws,r=<nonce>,p=<proof_b64>`.
pub(crate) fn build_client_final_message(
    server_nonce: &[u8],
    proof_b64: &[u8],
) -> Result<heapless::Vec<u8, MAX_CLIENT_FINAL_MSG_LEN>, ScramError> {
    let mut buf = build_client_final_without_proof(server_nonce)?;
    buf.extend_from_slice(b",p=")
        .map_err(|_| ScramError::BufferOverflow)?;
    buf.extend_from_slice(proof_b64)
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
        // per CREDO §7 ось 12).
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
/// **DEF-102 (security).** Uses `base64ct::Base64` —
/// RustCrypto's constant-time, branchless, `no_std` base64
/// encoder. The prior `base64` v0.22 `STANDARD` engine is
/// essentially constant-time in practice (cache-line-sized
/// alphabet table) but does not formalise the property; this
/// encoding step runs on the SCRAM `ClientProof`, which is
/// derived from the user's password via HMAC. Switching to
/// `base64ct` elevates this step's side-channel posture from
/// tier-3 (audit "yeah the table is cache-line-sized so it's
/// probably fine") to tier-1 (RustCrypto-audited constant-time).
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

/// Base64-decode into a bounded heapless::Vec.
///
/// DEF-102: same constant-time guarantees as the encoder, applied
/// to the salt. (The salt is not secret per se — the server sends
/// it cleartext — but consistency with the encode path keeps the
/// side-channel posture uniform across SCRAM wire parsing.)
fn base64_decode_bounded(
    input: &[u8],
) -> Result<heapless::Vec<u8, MAX_SALT_LEN>, ScramError> {
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
    let mut result = heapless::Vec::new();
    result
        .extend_from_slice(decoded)
        .map_err(|_| ScramError::InvalidSalt)?;
    Ok(result)
}

/// Generate a cryptographically random client nonce (18 bytes),
/// base64-encoded.
///
/// Uses `getrandom::getrandom` directly per DEF-META-01.
///
/// # Test injection
///
/// When `cfg(test)`, the `FIXED_TEST_NONCE` thread-local can be set
/// to inject a deterministic nonce for reproducible test vectors.
/// This injection point is physically absent from non-test builds
/// (tier-1 by build configuration).
#[cfg(not(test))]
pub(crate) fn generate_client_nonce() -> Result<heapless::Vec<u8, MAX_CLIENT_NONCE_B64_LEN>, ScramError> {
    let mut raw = zeroize::Zeroizing::new([0u8; 18]);
    // Classify randomness failure separately from buffer overflow.
    // A naive `map_err(|_| BufferOverflow)` would produce
    // misleading operator diagnostics on `/dev/urandom` EAGAIN or
    // container-restricted `getrandom` calls.
    getrandom::getrandom(raw.as_mut()).map_err(|_| ScramError::RandomnessUnavailable)?;
    let mut b64_buf = [0u8; MAX_CLIENT_NONCE_B64_LEN];
    let written = base64_encode_to_buf(&*raw, &mut b64_buf)?;
    let mut result = heapless::Vec::new();
    let src = b64_buf.get(..written).ok_or(ScramError::BufferOverflow)?;
    result
        .extend_from_slice(src)
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
pub(crate) fn generate_client_nonce() -> Result<heapless::Vec<u8, MAX_CLIENT_NONCE_B64_LEN>, ScramError> {
    // Check forced-failure flag first.
    let forced = FORCE_RNG_FAILURE.with(|cell| *cell.borrow());
    if forced {
        return Err(ScramError::RandomnessUnavailable);
    }
    FIXED_TEST_NONCE.with(|cell| {
        if let Some(fixed) = cell.borrow().as_ref() {
            let mut result = heapless::Vec::new();
            result
                .extend_from_slice(fixed.as_bytes())
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
            let mut result = heapless::Vec::new();
            let src = b64_buf.get(..written).ok_or(ScramError::BufferOverflow)?;
            result
                .extend_from_slice(src)
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
