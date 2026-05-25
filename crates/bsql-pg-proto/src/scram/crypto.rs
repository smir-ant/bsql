//! SCRAM-SHA-256 cryptographic operations — composition over RustCrypto.
//!
//! Every operation here is a thin composition of `sha2`, `hmac`, and
//! `pbkdf2` crate calls. Per DEF-META-01 we never hand-roll crypto.
//! No facades, no wrappers — the crate APIs are called directly.
//!
//! # Operations (RFC 5802 §2.2)
//!
//! ```text
//! SaltedPassword := PBKDF2(password, salt, iters)
//! ClientKey      := HMAC(SaltedPassword, "Client Key")
//! StoredKey      := SHA-256(ClientKey)
//! ServerKey      := HMAC(SaltedPassword, "Server Key")
//! ClientSignature:= HMAC(StoredKey, AuthMessage)
//! ClientProof    := ClientKey XOR ClientSignature
//! ServerSignature:= HMAC(ServerKey, AuthMessage)
//! ```
//!
//! # AuthMessage — incremental, zero intermediate buffer
//!
//! `AuthMessage = client-first-bare + "," + server-first + "," +
//! client-final-without-proof`. Rather than assembling this into a
//! temporary `[u8; N]` (which introduces a silent-truncation class if
//! N is too small), the three components are fed directly into
//! `HMAC::update()` calls. No buffer → no overflow → tier-1 by
//! construction.

use crate::scram::types::SecretDigest;
use crate::scram::wire::ScramError;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

/// HMAC-SHA-256 type alias.
type HmacSha256 = Hmac<Sha256>;

/// Derive the SCRAM SaltedPassword via PBKDF2-HMAC-SHA-256.
///
/// Per RFC 7677 §3, the minimum iteration count is 4096 and (/// audit BS8) the maximum client-accepted count is
/// [`crate::scram::wire::MAX_SCRAM_ITERATIONS`] — both bounds enforced
/// at `parse_server_first` before this function is called.
///
/// The result is wrapped in `Zeroizing` for scrub-on-drop.
fn salted_password(password: &[u8], salt: &[u8], iterations: u32) -> Zeroizing<[u8; 32]> {
    let mut out = Zeroizing::new([0u8; 32]);
    pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, out.as_mut());
    out
}

/// HMAC-SHA-256(key, message) → 32 bytes.
///
/// # (audit, — fail-closed explicit Result
///
/// `HmacSha256::new_from_slice` returns `Result<Self, InvalidLength>`
/// in signature, but HMAC-SHA-256 structurally accepts keys of ANY
/// length (RFC 2104: keys shorter than block size are zero-padded,
/// longer than block size are hashed first). The `Err` branch is
/// architecturally dead under the intact RustCrypto `hmac` crate.
///
/// The signature returns `Result<[u8; 32], ScramError>` so the
/// "fail-closed" discipline is visible in the type system, not
/// documentation-only. A naive shape returning `[0u8; 32]` on the
/// dead `Err` branch would shift the fail-closed contract into prose.
///
/// ## SCRAM fail-closed math (critical)
///
/// This is the actual math of what happens if the HMAC `Err` branch
/// ever fires. A surface-level reading might claim "server trivially
/// accepts because ClientProof = [0; 32]" — that is **WRONG**.
/// Triple-check below so future readers don't re-make the mistake.
///
/// SCRAM-SHA-256 signature-check (RFC 5802 §3):
///
/// - Client computes `ClientKey = HMAC(SaltedPassword, "Client Key")`.
///   If HMAC returns zeros, `ClientKey = [0; 32]`.
/// - Client computes `ClientSignature = HMAC(StoredKey, AuthMessage)`
///   where `StoredKey = SHA-256(ClientKey)`. If HMAC returns zeros,
///   `ClientSignature = [0; 32]`.
/// - Client sends `ClientProof = ClientKey XOR ClientSignature = [0; 32]`.
///
/// Server-side verification:
///
/// - Server has real `StoredKey` from the user's stored credentials.
/// - Server computes `ClientSignature_server = HMAC(StoredKey, AuthMessage)`
///   using its real HMAC (not the client's broken one).
/// - Server derives `ClientKey_candidate = ClientProof XOR ClientSignature_server`.
///   With `ClientProof = [0; 32]`, this yields `ClientSignature_server`
///   itself.
/// - Server checks `SHA-256(ClientKey_candidate) == StoredKey`, i.e.,
///   `SHA-256(HMAC(StoredKey, AuthMessage)) == StoredKey`.
///
/// For that equation to hold, `HMAC(StoredKey, AuthMessage)` would
/// need to equal the `ClientKey` from which `StoredKey = SHA-256(ClientKey)`
/// was derived. But those are two HMAC computations with different
/// inputs (`StoredKey, AuthMessage` vs `SaltedPassword, "Client Key"`)
/// — the probability of collision is `2^-256`, cryptographically
/// zero. **Server rejects.** Fail-closed via server rejection.
///
/// ## Why fail-closed explicit Result is still better
///
/// The silent-zero fallback was fail-closed IN PRACTICE but bad
/// pattern:
///
/// 1. **Supply-chain hardening.** If `hmac` crate ever changed its
///    contract (new API, stricter key validation, patch-release bug),
///    silent zeros would be a predictable-ClientProof signal —
///    pattern recognition surface even if not auth bypass.
/// 2. **Refactor safety.** A future change to `compute_client_proof`
///    that short-circuits on zero inputs (optimisation, debugging)
///    would turn the fallback into a real bypass. Explicit Result
///    prevents the change from compiling.
/// 3. **Audit discipline.** Crypto primitives should never silently
///    degrade. The Result makes the `Err` path visible and forces
///    every caller to decide its behaviour.
///
/// Fail-closed via explicit Result preserves the "server rejects"
/// outcome while adding a typed diagnostic that `ScramError::HmacKeyRejected`
/// propagates — the wrapper sees a classified fault, not a timeout
/// on the SASLFinal step.
/// Returns `Zeroizing<[u8; 32]>` rather than bare `[u8; 32]`. Rust
/// move semantics copy the callee's 32-byte return slot into the
/// caller's local — without a Zeroize wrapper, the callee's stack
/// slot persists in memory until a later call frame overwrites it.
/// `Zeroizing` forces Drop-on-exit of the returned wrapper, scrubbing
/// the callee slot when the caller consumes the value (normal path).
/// On `panic = "abort"` Drop is vacuous but the normal-path hygiene
/// is structurally guaranteed.
fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Zeroizing<[u8; 32]>, ScramError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| ScramError::HmacKeyRejected)?;
    mac.update(message);
    Ok(Zeroizing::new(mac.finalize().into_bytes().into()))
}

/// HMAC-SHA256 over the SCRAM AuthMessage, computed incrementally.
///
/// AuthMessage = `client_first_bare` + `","` + `server_first` + `","` +
/// `client_final_without_proof`. Each component is fed directly into
/// `HMAC::update()` — **zero intermediate buffer**. This eliminates the
/// silent-truncation class that a fixed-size staging buffer would
/// introduce: there is no buffer to overflow.
///
/// Tier-1 by construction: overflow is impossible because HMAC accepts
/// arbitrarily many `update()` calls with arbitrarily sized slices.
///
/// Returns `Result` on the architecturally-dead HMAC-key-reject
/// path — see [`hmac_sha256`] for the full fail-closed rationale.
/// Returns `Zeroizing<[u8; 32]>` for the same hygiene reason as
/// [`hmac_sha256`] — the HMAC output over the AuthMessage is
/// password-correlated (used as ClientSignature for the XOR into
/// ClientProof, and as ServerSignature for server verification).
/// Wrapping in `Zeroizing` scrubs both the callee's return slot and the
/// caller's local on scope exit.
fn hmac_auth_message(
    key: &[u8],
    client_first_bare: &[u8],
    server_first: &[u8],
    client_final_without_proof: &[u8],
) -> Result<Zeroizing<[u8; 32]>, ScramError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| ScramError::HmacKeyRejected)?;
    mac.update(client_first_bare);
    mac.update(b",");
    mac.update(server_first);
    mac.update(b",");
    mac.update(client_final_without_proof);
    Ok(Zeroizing::new(mac.finalize().into_bytes().into()))
}

/// Full SCRAM-SHA-256 client proof computation.
///
/// Returns `(client_proof, server_signature)`. Both are 32-byte values.
/// `client_proof` is the XOR of ClientKey and ClientSignature.
/// `server_signature` is returned as `SecretDigest` for constant-time
/// comparison later.
///
/// # AuthMessage — tier-1 no-truncation
///
/// The three AuthMessage components are passed separately and fed
/// incrementally into HMAC. No intermediate buffer is assembled,
/// so the silent-truncation class does not exist. See
/// `hmac_auth_message`.
///
/// # Arguments
///
/// - `password` — the user's password bytes.
/// - `salt` — base64-decoded salt from the server.
/// - `iterations` — iteration count from the server (>= 4096).
/// - `client_first_bare` — the `n=<user>,r=<nonce>` string.
/// - `server_first` — the raw server-first-message bytes from the wire.
/// - `client_final_without_proof` — `c=biws,r=<server_nonce>`.
pub fn compute_client_proof(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    client_first_bare: &[u8],
    server_first: &[u8],
    client_final_without_proof: &[u8],
) -> Result<(Zeroizing<[u8; 32]>, SecretDigest), ScramError> {
    let salted_pw = salted_password(password, salt, iterations);

    // Propagate HMAC errors as typed `ScramError::HmacKeyRejected`
    // rather than silently computing over zeros. All four HMAC calls
    // below are architecturally dead Err (RustCrypto HMAC accepts any
    // key length), but a fail-closed typed error is the correct
    // discipline for crypto primitives.
    //
    // All password-correlated intermediates are wrapped in
    // `Zeroizing`. `stored_key` is `SHA-256(ClientKey)` — on PG
    // server side THIS is the password-equivalent (PG stores StoredKey
    // as the authenticator); a core-dump attacker with `stored_key`
    // can replay against any server indefinitely. `client_signature`
    // leaks StoredKey via `XOR(proof, signature)` algebra. Both
    // zeroed on scope exit.
    let client_key = hmac_sha256(salted_pw.as_ref(), b"Client Key")?;
    let stored_key: Zeroizing<[u8; 32]> =
        Zeroizing::new(Sha256::digest(client_key.as_ref()).into());
    let server_key = hmac_sha256(salted_pw.as_ref(), b"Server Key")?;

    let client_signature = hmac_auth_message(
        stored_key.as_ref(),
        client_first_bare,
        server_first,
        client_final_without_proof,
    )?;
    // SecretDigest owns its 32 bytes with its own ZeroizeOnDrop — wrap
    // the Zeroizing<[u8; 32]> into SecretDigest by consuming the inner
    // value (deref + copy is fine; the Zeroizing wrapper drops at
    // end of this stmt, scrubbing the temporary).
    let server_sig_bytes = hmac_auth_message(
        server_key.as_ref(),
        client_first_bare,
        server_first,
        client_final_without_proof,
    )?;
    let server_signature = SecretDigest::new(*server_sig_bytes);

    // ClientProof = ClientKey XOR ClientSignature (element-wise).
    // Bitwise XOR on u8 cannot overflow — `clippy::arithmetic_side_effects`
    // does not fire on `^` for integer types.
    let mut proof = Zeroizing::new([0u8; 32]);
    for ((p, ck), cs) in proof.iter_mut().zip(client_key.iter()).zip(client_signature.iter()) {
        *p = *ck ^ *cs;
    }

    Ok((proof, server_signature))
}

#[cfg(test)]
mod tests {
    //! RFC 7677 Appendix A test vectors for SCRAM-SHA-256.
    //!
    //! Category (A) spec-conformance: the SCRAM crypto composition
    //! produces bit-exact results against the published reference
    //! exchange in RFC 7677 Appendix A.

    use super::*;

    // RFC 7677 Appendix A exchange:
    //   C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO
    //   S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
    //   C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
    //   S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=

    fn rfc7677_salt() -> [u8; 16] {
        [
            0x5B, 0x6D, 0x99, 0x68, 0x9D, 0x12, 0x35, 0x8E,
            0xEC, 0xA0, 0x4B, 0x14, 0x12, 0x36, 0xFA, 0x81,
        ]
    }

    /// Invariant (spec): SCRAM-SHA-256 with the RFC 7677 Appendix A
    /// parameters produces the correct ClientProof and ServerSignature.
    ///
    /// The three AuthMessage components are passed separately — the
    /// function computes AuthMessage incrementally without a staging
    /// buffer (tier-1 no-truncation).
    #[test]
    fn rfc7677_appendix_a_full_proof() {
        let salt = rfc7677_salt();
        let password = b"pencil";
        let iterations = 4096u32;

        // AuthMessage components from the RFC exchange:
        let client_first_bare = b"n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final_without_proof = b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

        let result = compute_client_proof(
            password,
            &salt,
            iterations,
            client_first_bare,
            server_first,
            client_final_without_proof,
        );
        assert!(result.is_ok(), "RFC 7677 params are well-formed; compute_client_proof must succeed");
        let (proof, server_sig) = match result {
            Ok(v) => v,
            // Dead after the assert above, but the pattern avoids
            // `.unwrap()` / `.expect()` (both forbid-bundle-banned).
            Err(_) => return,
        };

        // `base64ct::Base64::encode` returns
        // `Result<&str, InvalidLengthError>` borrowing into the
        // caller-owned buf — a single `unwrap_or("")` per call.
        // A naive three-layer chain
        // (`base64_encode_to_buf(...).unwrap_or(0)` →
        // `buf.get(..len).unwrap_or(&[])` →
        // `from_utf8(...).unwrap_or("")`) would stack
        // architecturally-dead fallbacks where the typed
        // `&str`-returning shape collapses them into one.
        use base64ct::{Base64, Encoding};

        // Verify via base64 encoding of the proof.
        let mut proof_b64_buf = [0u8; 64];
        let proof_b64 =
            Base64::encode(proof.as_ref(), &mut proof_b64_buf).unwrap_or("");
        assert_eq!(
            proof_b64,
            "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            "ClientProof base64 must match RFC 7677 Appendix A",
        );

        // Verify server signature via base64.
        let mut sig_b64_buf = [0u8; 64];
        let sig_b64 =
            Base64::encode(server_sig.as_bytes(), &mut sig_b64_buf).unwrap_or("");
        assert_eq!(
            sig_b64,
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            "ServerSignature base64 must match RFC 7677 Appendix A",
        );
    }
}
