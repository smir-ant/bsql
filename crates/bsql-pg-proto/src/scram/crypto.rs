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
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

/// HMAC-SHA-256 type alias.
type HmacSha256 = Hmac<Sha256>;

/// Derive the SCRAM SaltedPassword via PBKDF2-HMAC-SHA-256.
///
/// Per RFC 7677 §3, the minimum iteration count is 4096.
/// The caller validates this before invoking.
///
/// The result is wrapped in `Zeroizing` for scrub-on-drop.
fn salted_password(password: &[u8], salt: &[u8], iterations: u32) -> Zeroizing<[u8; 32]> {
    let mut out = Zeroizing::new([0u8; 32]);
    pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, out.as_mut());
    out
}

/// HMAC-SHA-256(key, message) → 32 bytes.
///
/// HMAC-SHA256 accepts any key length; `new_from_slice` cannot fail.
/// The Err branch returns zeros → downstream SCRAM proof will fail
/// server verification openly, not silently.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(m) => m,
        Err(_) => return [0u8; 32],
    };
    mac.update(message);
    mac.finalize().into_bytes().into()
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
fn hmac_auth_message(
    key: &[u8],
    client_first_bare: &[u8],
    server_first: &[u8],
    client_final_without_proof: &[u8],
) -> [u8; 32] {
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(m) => m,
        Err(_) => return [0u8; 32],
    };
    mac.update(client_first_bare);
    mac.update(b",");
    mac.update(server_first);
    mac.update(b",");
    mac.update(client_final_without_proof);
    mac.finalize().into_bytes().into()
}

/// Full SCRAM-SHA-256 client proof computation.
///
/// Returns `(client_proof, server_signature)`. Both are 32-byte values.
/// `client_proof` is the XOR of ClientKey and ClientSignature.
/// `server_signature` is returned as [`SecretDigest`] for constant-time
/// comparison later.
///
/// # AuthMessage — tier-1 no-truncation
///
/// The three AuthMessage components are passed separately and fed
/// incrementally into HMAC. No intermediate buffer is assembled,
/// so the silent-truncation class does not exist. See
/// [`hmac_auth_message`].
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
) -> (Zeroizing<[u8; 32]>, SecretDigest) {
    let salted_pw = salted_password(password, salt, iterations);

    let client_key = Zeroizing::new(hmac_sha256(salted_pw.as_ref(), b"Client Key"));
    let stored_key: [u8; 32] = Sha256::digest(client_key.as_ref()).into();
    let server_key = Zeroizing::new(hmac_sha256(salted_pw.as_ref(), b"Server Key"));

    let client_signature = hmac_auth_message(
        &stored_key,
        client_first_bare,
        server_first,
        client_final_without_proof,
    );
    let server_signature = SecretDigest::new(hmac_auth_message(
        server_key.as_ref(),
        client_first_bare,
        server_first,
        client_final_without_proof,
    ));

    // ClientProof = ClientKey XOR ClientSignature (element-wise).
    // Bitwise XOR on u8 cannot overflow — `clippy::arithmetic_side_effects`
    // does not fire on `^` for integer types.
    let mut proof = Zeroizing::new([0u8; 32]);
    for ((p, ck), cs) in proof.iter_mut().zip(client_key.iter()).zip(client_signature.iter()) {
        *p = *ck ^ *cs;
    }

    (proof, server_signature)
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

        let (proof, server_sig) = compute_client_proof(
            password,
            &salt,
            iterations,
            client_first_bare,
            server_first,
            client_final_without_proof,
        );

        // Verify via base64 encoding of the proof.
        let mut proof_b64_buf = [0u8; 64];
        let proof_b64_len = crate::scram::wire::base64_encode_to_buf(
            proof.as_ref(),
            &mut proof_b64_buf,
        )
        .unwrap_or(0);
        let proof_b64 = core::str::from_utf8(
            proof_b64_buf.get(..proof_b64_len).unwrap_or(&[]),
        )
        .unwrap_or("");
        assert_eq!(
            proof_b64,
            "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            "ClientProof base64 must match RFC 7677 Appendix A",
        );

        // Verify server signature via base64.
        let mut sig_b64_buf = [0u8; 64];
        let sig_b64_len = crate::scram::wire::base64_encode_to_buf(
            server_sig.as_bytes(),
            &mut sig_b64_buf,
        )
        .unwrap_or(0);
        let sig_b64 = core::str::from_utf8(
            sig_b64_buf.get(..sig_b64_len).unwrap_or(&[]),
        )
        .unwrap_or("");
        assert_eq!(
            sig_b64,
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            "ServerSignature base64 must match RFC 7677 Appendix A",
        );
    }
}
