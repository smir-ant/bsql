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

/// Maximum base64-decoded salt length.
pub const MAX_SALT_LEN: usize = 64;

/// Base64-encoded length (with RFC 4648 padding) of a SHA-256 digest.
///
/// SHA-256 produces 32 bytes. Base64 encoding with padding is
/// `ceil(N / 3) * 4`; for `N = 32` that is `ceil(32/3) * 4 = 11 * 4 = 44`.
///
/// Named as a constant rather than computed via a `const fn`, because
/// the crate's forbid-bundle bans `clippy::integer_division` even in
/// const context — and `forbid` cannot be downgraded by `#[expect]`.
/// The value is verified by the base64 crate's runtime encoding on the
/// actual proof bytes in the RFC 7677 test vector (see
/// `scram::crypto::tests`), which fails if the padding length drifts.
const SHA256_PROOF_B64_LEN: usize = 44;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramError {
    /// Server nonce does not start with the client nonce (RFC 5802 section 5.1 MUST).
    NoncePrefixMismatch,
    /// Server iteration count is below the RFC 7677 minimum (4096).
    IterationsTooLow {
        /// The iterations value the server sent.
        iterations: u32,
    },
    /// Server-first-message has invalid format.
    MalformedServerFirst,
    /// Server-final-message indicates an error (`e=<text>`).
    ServerScramError,
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
}

impl fmt::Display for ScramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoncePrefixMismatch => {
                f.write_str("SCRAM: server nonce does not start with client nonce")
            }
            Self::IterationsTooLow { iterations } => {
                write!(f, "SCRAM: iteration count {iterations} below minimum 4096")
            }
            Self::MalformedServerFirst => f.write_str("SCRAM: malformed server-first-message"),
            Self::ServerScramError => f.write_str("SCRAM: server reported authentication error"),
            Self::MalformedServerFinal => f.write_str("SCRAM: malformed server-final-message"),
            Self::SignatureMismatch => f.write_str("SCRAM: server signature mismatch"),
            Self::Base64DecodeError => f.write_str("SCRAM: base64 decode failed"),
            Self::InvalidSalt => f.write_str("SCRAM: invalid salt"),
            Self::ServerNonceTooLong => f.write_str("SCRAM: server nonce too long"),
            Self::BufferOverflow => f.write_str("SCRAM: message buffer overflow"),
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
pub fn build_client_first_bare(
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
pub fn build_client_first_message(
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
#[derive(Debug)]
pub struct ServerFirst {
    /// The full server nonce (`r=<value>`) — must start with client nonce.
    pub server_nonce: CappedServerNonce,
    /// Base64-decoded salt.
    pub salt: heapless::Vec<u8, MAX_SALT_LEN>,
    /// Iteration count.
    pub iterations: u32,
}

/// Parse a server-first-message: `r=<nonce>,s=<salt_b64>,i=<iters>`.
///
/// Validates:
/// - Server nonce starts with the client nonce (RFC 5802 section 5.1 MUST).
/// - Iteration count >= 4096 (RFC 7677 section 4.2 MUST).
/// - Salt base64-decodes and fits our bounded buffer.
pub fn parse_server_first(
    msg: &[u8],
    client_nonce_b64: &[u8],
) -> Result<ServerFirst, ScramError> {
    // Parse the three comma-separated fields.
    let msg_str = core::str::from_utf8(msg).map_err(|_| ScramError::MalformedServerFirst)?;

    // Split by commas. We expect exactly three fields: r=..., s=..., i=...
    let mut parts = msg_str.splitn(3, ',');
    let r_part = parts.next().ok_or(ScramError::MalformedServerFirst)?;
    let s_part = parts.next().ok_or(ScramError::MalformedServerFirst)?;
    let i_part = parts.next().ok_or(ScramError::MalformedServerFirst)?;

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
    let iterations = parse_u32(iters_str.as_bytes()).ok_or(ScramError::MalformedServerFirst)?;
    if iterations < MIN_SCRAM_ITERATIONS {
        return Err(ScramError::IterationsTooLow { iterations });
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
pub fn build_client_final_without_proof(
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
pub fn build_client_final_message(
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
pub fn parse_server_final(msg: &[u8]) -> Result<SecretDigest, ScramError> {
    let msg_str = core::str::from_utf8(msg).map_err(|_| ScramError::MalformedServerFinal)?;

    if let Some(verifier_b64) = msg_str.strip_prefix("v=") {
        let decoded = base64_decode_bounded(verifier_b64.as_bytes())?;
        if decoded.len() != 32 {
            return Err(ScramError::MalformedServerFinal);
        }
        let mut arr = [0u8; 32];
        if let Some(dest) = arr.get_mut(..32)
            && let Some(src) = decoded.get(..32)
        {
            dest.copy_from_slice(src);
        }
        Ok(SecretDigest::new(arr))
    } else if msg_str.starts_with("e=") {
        Err(ScramError::ServerScramError)
    } else {
        Err(ScramError::MalformedServerFinal)
    }
}

/// Base64-encode into a stack buffer. Returns the encoded bytes.
///
/// Uses `base64::engine::general_purpose::STANDARD` directly per
/// DEF-META-01 (no facade).
pub fn base64_encode_to_buf(
    input: &[u8],
    out: &mut [u8],
) -> Result<usize, ScramError> {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    let encoded_len = base64::encoded_len(input.len(), true).ok_or(ScramError::BufferOverflow)?;
    if encoded_len > out.len() {
        return Err(ScramError::BufferOverflow);
    }
    let written = STANDARD
        .encode_slice(input, out)
        .map_err(|_| ScramError::BufferOverflow)?;
    Ok(written)
}

/// Base64-decode into a bounded heapless::Vec.
fn base64_decode_bounded(
    input: &[u8],
) -> Result<heapless::Vec<u8, MAX_SALT_LEN>, ScramError> {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    // Decoded length is at most 3/4 of input length.
    let mut decode_buf = [0u8; MAX_SALT_LEN];
    let input_str = core::str::from_utf8(input).map_err(|_| ScramError::Base64DecodeError)?;
    let decoded_len = STANDARD
        .decode_slice(input_str, &mut decode_buf)
        .map_err(|_| ScramError::Base64DecodeError)?;
    let mut result = heapless::Vec::new();
    let src = decode_buf.get(..decoded_len).ok_or(ScramError::Base64DecodeError)?;
    result
        .extend_from_slice(src)
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
pub fn generate_client_nonce() -> Result<heapless::Vec<u8, MAX_CLIENT_NONCE_B64_LEN>, ScramError> {
    let mut raw = zeroize::Zeroizing::new([0u8; 18]);
    getrandom::getrandom(raw.as_mut()).map_err(|_| ScramError::BufferOverflow)?;
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
#[cfg(test)]
pub fn generate_client_nonce() -> Result<heapless::Vec<u8, MAX_CLIENT_NONCE_B64_LEN>, ScramError> {
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
            getrandom::getrandom(raw.as_mut()).map_err(|_| ScramError::BufferOverflow)?;
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
}

/// Set a fixed nonce for the current test (test-only).
#[cfg(test)]
pub fn set_test_nonce(nonce: &str) {
    FIXED_TEST_NONCE.with(|cell| {
        *cell.borrow_mut() = Some(std::string::String::from(nonce));
    });
}

/// Parse a decimal u32 from ASCII bytes.
fn parse_u32(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }
    let mut result: u32 = 0;
    for b in bytes {
        let digit = (*b).checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        result = result.checked_mul(10)?;
        result = result.checked_add(u32::from(digit))?;
    }
    Some(result)
}
