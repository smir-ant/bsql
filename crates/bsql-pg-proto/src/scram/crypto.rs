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

/// HMAC-SHA-256(key, message) -> 32 bytes.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac =
        HmacSha256::new_from_slice(key).unwrap_or_else(|_| HmacSha256::new_from_slice(&[]).unwrap_or_else(|_| {
            // HMAC-SHA256 accepts any key length; this branch is
            // unreachable. We handle it to satisfy the forbid-bundle.
            // Return a zeroed MAC which will fail verification downstream.
            HmacSha256::new(&Default::default())
        }));
    mac.update(message);
    let result = mac.finalize();
    // `into_bytes` returns `GenericArray<u8, U32>` — convert to array.
    let mut out = [0u8; 32];
    let bytes = result.into_bytes();
    if let Some(dest) = out.get_mut(..32)
        && let Some(src) = bytes.get(..32)
    {
        dest.copy_from_slice(src);
    }
    out
}

/// SHA-256 hash of a single input.
fn sha256(input: &[u8]) -> [u8; 32] {
    let hash = Sha256::digest(input);
    let mut out = [0u8; 32];
    if let Some(dest) = out.get_mut(..32)
        && let Some(src) = hash.get(..32)
    {
        dest.copy_from_slice(src);
    }
    out
}

/// Full SCRAM-SHA-256 client proof computation.
///
/// Returns `(client_proof, server_signature)`. Both are 32-byte values.
/// `client_proof` is the XOR of ClientKey and ClientSignature.
/// `server_signature` is returned as [`SecretDigest`] for constant-time
/// comparison later.
///
/// # Arguments
///
/// - `password` — the user's password bytes.
/// - `salt` — base64-decoded salt from the server.
/// - `iterations` — iteration count from the server (>= 4096).
/// - `auth_message` — the concatenation of client-first-bare + "," +
///   server-first + "," + client-final-without-proof.
pub fn compute_client_proof(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> (Zeroizing<[u8; 32]>, SecretDigest) {
    let salted_pw = salted_password(password, salt, iterations);

    let client_key = Zeroizing::new(hmac_sha256(salted_pw.as_ref(), b"Client Key"));
    let stored_key = sha256(client_key.as_ref());
    let server_key = Zeroizing::new(hmac_sha256(salted_pw.as_ref(), b"Server Key"));

    let client_signature = hmac_sha256(&stored_key, auth_message);
    let server_signature = SecretDigest::new(hmac_sha256(server_key.as_ref(), auth_message));

    // ClientProof = ClientKey XOR ClientSignature (element-wise).
    // Bitwise XOR on u8 is a bitwise operation, not arithmetic —
    // it cannot overflow. `clippy::arithmetic_side_effects` does
    // NOT fire on `^` for integer types.
    let mut proof = Zeroizing::new([0u8; 32]);
    // Zip the three arrays element-wise. All are [u8; 32], so the
    // iterator yields exactly 32 triples. No index arithmetic needed.
    let proof_iter = proof.iter_mut();
    let ck_iter = client_key.iter();
    let cs_iter = client_signature.iter();
    for ((p, ck), cs) in proof_iter.zip(ck_iter).zip(cs_iter) {
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
    //
    // Salt base64 "W22ZaJ0SNY7soEsUEjb6gQ==" decodes to 16 bytes:
    //   [0x5B, 0x6D, 0x99, 0x68, 0x9D, 0x12, 0x35, 0x8E,
    //    0xEC, 0xA0, 0x4B, 0x14, 0x12, 0x36, 0xFA, 0x81]

    /// The raw salt bytes from the RFC 7677 exchange.
    fn rfc7677_salt() -> [u8; 16] {
        [
            0x5B, 0x6D, 0x99, 0x68, 0x9D, 0x12, 0x35, 0x8E,
            0xEC, 0xA0, 0x4B, 0x14, 0x12, 0x36, 0xFA, 0x81,
        ]
    }

    /// The AuthMessage per RFC 5802 is:
    /// client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
    fn rfc7677_auth_message() -> &'static [u8] {
        b"n=user,r=rOprNGfwEbeRWgbNEkqO,\
          r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
          s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,\
          c=biws,\
          r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
    }

    /// Invariant (spec): SCRAM-SHA-256 with the RFC 7677 Appendix A
    /// parameters produces the correct ClientProof and ServerSignature.
    ///
    /// We verify the proof and server signature against the base64
    /// values in the published exchange:
    /// - ClientProof: `dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=`
    /// - ServerSignature: `6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=`
    #[test]
    fn rfc7677_appendix_a_full_proof() {
        let salt = rfc7677_salt();
        let password = b"pencil";
        let iterations = 4096u32;
        let auth_message = rfc7677_auth_message();

        let (proof, server_sig) = compute_client_proof(password, &salt, iterations, auth_message);

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
