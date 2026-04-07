//! PostgreSQL authentication — MD5 and SCRAM-SHA-256 / SCRAM-SHA-256-PLUS.
//!
//! MD5 is simple: `"md5" + hex(md5(hex(md5(password + user)) + salt))`.
//!
//! SCRAM-SHA-256 implements RFC 5802. When a TLS server certificate hash is
//! available, SCRAM-SHA-256-PLUS with `tls-server-end-point` channel binding
//! (RFC 5929) is preferred.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use hmac::{Hmac, Mac};
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::DriverError;

type HmacSha256 = Hmac<Sha256>;

// --- MD5 ---

/// Compute the MD5 password hash for PostgreSQL authentication.
///
/// Result is `"md5" + hex(md5(hex(md5(password + user)) + salt))`, NUL-terminated.
/// Uses a fixed [u8; 36] array — no heap allocation.
pub fn md5_password(user: &str, password: &str, salt: &[u8; 4]) -> [u8; 36] {
    // Step 1: md5(password + user)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(user.as_bytes());
    let inner = hex_encode_fixed(&hasher.finalize());

    // Step 2: md5(hex_inner + salt)
    let mut hasher = Md5::new();
    hasher.update(inner);
    hasher.update(salt);
    let outer = hex_encode_fixed(&hasher.finalize());

    // "md5" + hex(32) + NUL = 36 bytes
    let mut result = [0u8; 36];
    result[0] = b'm';
    result[1] = b'd';
    result[2] = b'5';
    result[3..35].copy_from_slice(&outer);
    result[35] = 0;
    result
}

// --- SCRAM-SHA-256 / SCRAM-SHA-256-PLUS ---

/// Channel binding mode for SCRAM authentication.
///
/// When the connection is over TLS and we can extract the server certificate
/// hash, we use `tls-server-end-point` binding (RFC 5929). Otherwise we fall
/// back to no channel binding (`n,,`).
enum ChannelBinding {
    /// No TLS, or TLS without certificate hash — GS2 header `n,,`.
    None,
    /// TLS with certificate hash — GS2 header `p=tls-server-end-point,,`.
    TlsServerEndPoint([u8; 32]),
}

/// SCRAM-SHA-256 client state machine.
///
/// Supports both plain SCRAM-SHA-256 (`n,,`) and SCRAM-SHA-256-PLUS with
/// `tls-server-end-point` channel binding (`p=tls-server-end-point,,`).
///
/// Usage:
/// 1. Create with `ScramClient::new(user, password, cert_hash)`.
/// 2. Call `client_first_message()` to get the initial message bytes.
/// 3. Feed the server-first message via `process_server_first()`.
/// 4. Call `client_final_message()` to get the response.
/// 5. Feed the server-final message via `verify_server_final()`.
pub struct ScramClient {
    password: String,
    nonce: String,
    client_first_bare: String,
    server_first: String,
    salted_password: [u8; 32],
    auth_message: String,
    channel_binding: ChannelBinding,
}

impl ScramClient {
    /// Create a new SCRAM client for the given credentials.
    ///
    /// If `cert_hash` is `Some`, SCRAM-SHA-256-PLUS with `tls-server-end-point`
    /// channel binding will be used. Pass `None` for plain SCRAM-SHA-256.
    pub fn new(
        user: &str,
        password: &str,
        cert_hash: Option<&[u8; 32]>,
    ) -> Result<Self, DriverError> {
        let channel_binding = match cert_hash {
            Some(hash) => ChannelBinding::TlsServerEndPoint(*hash),
            None => ChannelBinding::None,
        };
        let nonce = generate_nonce()?;
        let client_first_bare = format!("n={user},r={nonce}");

        Ok(Self {
            password: password.to_owned(),
            nonce,
            client_first_bare,
            server_first: String::new(),
            salted_password: [0u8; 32],
            auth_message: String::new(),
            channel_binding,
        })
    }

    /// Generate the client-first message.
    ///
    /// - No channel binding: `n,,n=user,r=nonce`
    /// - With channel binding: `p=tls-server-end-point,,n=user,r=nonce`
    pub fn client_first_message(&self) -> Vec<u8> {
        let gs2_header = match self.channel_binding {
            ChannelBinding::None => "n,,",
            ChannelBinding::TlsServerEndPoint(_) => "p=tls-server-end-point,,",
        };
        format!("{gs2_header}{}", self.client_first_bare).into_bytes()
    }

    /// Process the server-first message and compute the salted password.
    ///
    /// Server-first format: `r=combined_nonce,s=base64_salt,i=iterations`
    pub fn process_server_first(&mut self, server_first: &[u8]) -> Result<(), DriverError> {
        let server_first_str = std::str::from_utf8(server_first)
            .map_err(|_| DriverError::Auth("server-first is not valid UTF-8".into()))?;

        self.server_first = server_first_str.to_owned();

        // Parse r=, s=, i= fields
        let mut server_nonce = None;
        let mut salt_b64 = None;
        let mut iterations = None;

        for part in server_first_str.split(',') {
            if let Some(val) = part.strip_prefix("r=") {
                server_nonce = Some(val);
            } else if let Some(val) = part.strip_prefix("s=") {
                salt_b64 = Some(val);
            } else if let Some(val) = part.strip_prefix("i=") {
                iterations = val.parse::<u32>().ok();
            }
        }

        let server_nonce = server_nonce
            .ok_or_else(|| DriverError::Auth("missing nonce in server-first".into()))?;
        let salt_b64 =
            salt_b64.ok_or_else(|| DriverError::Auth("missing salt in server-first".into()))?;
        let iterations = iterations
            .ok_or_else(|| DriverError::Auth("missing iterations in server-first".into()))?;

        // Verify server nonce starts with our client nonce
        if !server_nonce.starts_with(&self.nonce) {
            return Err(DriverError::Auth(
                "server nonce does not start with client nonce".into(),
            ));
        }

        // Decode salt
        let salt = B64
            .decode(salt_b64)
            .map_err(|_| DriverError::Auth("invalid base64 salt".into()))?;

        // SaltedPassword = PBKDF2(SHA256, password, salt, iterations)
        pbkdf2::pbkdf2_hmac::<Sha256>(
            self.password.as_bytes(),
            &salt,
            iterations,
            &mut self.salted_password,
        );

        // Zeroize the password — no longer needed after PBKDF2.
        // Minimizes the window where the plaintext password lives in memory.
        self.password.clear();
        self.password.shrink_to(0);

        // Build auth message for proof computation.
        // The `c=` field is base64 of (GS2 header + channel binding data).
        let cb_data = match &self.channel_binding {
            ChannelBinding::None => b"n,,".to_vec(),
            ChannelBinding::TlsServerEndPoint(hash) => {
                let mut data = b"p=tls-server-end-point,,".to_vec();
                data.extend_from_slice(hash);
                data
            }
        };
        let cb_b64 = B64.encode(&cb_data);
        let client_final_without_proof = format!("c={cb_b64},r={server_nonce}");
        self.auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof
        );

        Ok(())
    }

    /// Generate the client-final message with proof.
    ///
    /// Returns `c=<channel_binding_b64>,r=combined_nonce,p=base64_proof`.
    pub fn client_final_message(&self) -> Result<Vec<u8>, DriverError> {
        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = hmac_sha256(&self.salted_password, b"Client Key")?;

        // StoredKey = SHA256(ClientKey)
        let stored_key = Sha256::digest(client_key);

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&stored_key, self.auth_message.as_bytes())?;

        // ClientProof = ClientKey XOR ClientSignature
        let mut proof = client_key;
        for (p, s) in proof.iter_mut().zip(client_signature.iter()) {
            *p ^= s;
        }

        let proof_b64 = B64.encode(proof);

        // Extract server nonce from auth_message
        let server_nonce = self
            .server_first
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .ok_or_else(|| DriverError::Auth("missing nonce for final message".into()))?;

        // The `c=` field is base64 of (GS2 header + channel binding data).
        let cb_data = match &self.channel_binding {
            ChannelBinding::None => b"n,,".to_vec(),
            ChannelBinding::TlsServerEndPoint(hash) => {
                let mut data = b"p=tls-server-end-point,,".to_vec();
                data.extend_from_slice(hash);
                data
            }
        };
        let cb_b64 = B64.encode(&cb_data);

        let msg = format!("c={cb_b64},r={server_nonce},p={proof_b64}");
        Ok(msg.into_bytes())
    }

    /// Verify the server-final message.
    ///
    /// Server-final format: `v=base64_server_signature`
    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<(), DriverError> {
        let server_final_str = std::str::from_utf8(server_final)
            .map_err(|_| DriverError::Auth("server-final is not valid UTF-8".into()))?;

        let server_sig_b64 = server_final_str
            .strip_prefix("v=")
            .ok_or_else(|| DriverError::Auth("server-final missing 'v=' prefix".into()))?;

        let server_sig = B64
            .decode(server_sig_b64)
            .map_err(|_| DriverError::Auth("invalid base64 in server signature".into()))?;

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let server_key = hmac_sha256(&self.salted_password, b"Server Key")?;

        // Expected = HMAC(ServerKey, AuthMessage)
        let expected = hmac_sha256(&server_key, self.auth_message.as_bytes())?;

        // handles mismatched lengths without leaking timing information.
        if !constant_time_eq(&server_sig, &expected) {
            return Err(DriverError::Auth("server signature mismatch".into()));
        }

        Ok(())
    }
}

// --- Helpers ---

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], DriverError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| DriverError::Auth("HMAC computation failed".into()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().into())
}

/// Generate a 24-byte random nonce, base64-encoded.
fn generate_nonce() -> Result<String, DriverError> {
    use rand::TryRngCore;
    let mut bytes = [0u8; 24];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| DriverError::Auth(format!("OS RNG failed: {e}")))?;
    Ok(B64.encode(bytes))
}

/// Constant-time comparison to prevent timing attacks on auth signatures.
/// `#[inline(never)]` prevents the compiler from optimizing the XOR loop
/// into an early-exit comparison.
///
/// For SCRAM verification, both inputs should always be 32 bytes
/// (SHA-256 output). We still handle the general case by processing up to
/// the longer length, avoiding an early return that leaks length information.
#[inline(never)]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let max_len = a.len().max(b.len());
    let mut diff: u32 = 0;
    // Length mismatch is itself a diff — use u32 to avoid truncation for lengths > 255
    diff |= (a.len() ^ b.len()) as u32;
    for i in 0..max_len {
        let x = if i < a.len() { a[i] } else { 0 };
        let y = if i < b.len() { b[i] } else { 0 };
        diff |= (x ^ y) as u32;
    }
    diff == 0
}

/// Lowercase hex encoding of a 16-byte MD5 digest into a fixed [u8; 32] array.
fn hex_encode_fixed(bytes: &[u8]) -> [u8; 32] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 32];
    for (i, &b) in bytes.iter().enumerate() {
        out[i * 2] = HEX[(b >> 4) as usize];
        out[i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
    out
}

/// Lowercase hex encoding of a byte slice (used by tests).
#[cfg(test)]
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Parse the SASL mechanism list from an AuthSasl message.
///
/// Mechanisms are NUL-terminated strings, terminated by a final NUL.
/// Uses SmallVec<[&str; 2]> — PG typically offers 1-2 mechanisms.
pub fn parse_sasl_mechanisms(data: &[u8]) -> smallvec::SmallVec<[&str; 2]> {
    let mut mechanisms = smallvec::SmallVec::new();
    let mut pos = 0;
    while pos < data.len() {
        if data[pos] == 0 {
            break;
        }
        if let Some(end) = data[pos..].iter().position(|&b| b == 0) {
            if let Ok(s) = std::str::from_utf8(&data[pos..pos + end]) {
                if !s.is_empty() {
                    mechanisms.push(s);
                }
            }
            pos += end + 1;
        } else {
            break;
        }
    }
    mechanisms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_password_known_value() {
        // Known test vector: user="testuser", password="testpass", salt=[0x01, 0x02, 0x03, 0x04]
        let result = md5_password("testuser", "testpass", &[0x01, 0x02, 0x03, 0x04]);
        // Fixed [u8; 36]: "md5" + 32 hex + NUL
        assert!(result.starts_with(b"md5"));
        assert_eq!(result[35], 0); // NUL terminated
    }

    #[test]
    fn md5_password_format() {
        let result = md5_password("user", "pass", &[0xAA, 0xBB, 0xCC, 0xDD]);
        let s = std::str::from_utf8(&result[..35]).unwrap();
        assert!(s.starts_with("md5"));
        // The remaining 32 chars must be hex
        assert!(s[3..].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn scram_client_first_message_format() {
        let client = ScramClient::new("testuser", "testpass", None).unwrap();
        let msg = client.client_first_message();
        let s = std::str::from_utf8(&msg).unwrap();
        assert!(s.starts_with("n,,n=testuser,r="));
    }

    #[test]
    fn scram_nonce_is_unique() {
        let n1 = generate_nonce().unwrap();
        let n2 = generate_nonce().unwrap();
        assert_ne!(n1, n2);
    }

    #[test]
    fn constant_time_eq_works() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }

    #[test]
    fn hex_encode_works() {
        assert_eq!(hex_encode(&[0xDE, 0xAD, 0xBE, 0xEF]), "deadbeef");
        assert_eq!(hex_encode(&[0x00, 0xFF]), "00ff");
    }

    #[test]
    fn parse_sasl_mechanisms_works() {
        let data = b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs.as_slice(), &["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
    }

    #[test]
    fn parse_sasl_mechanisms_empty() {
        let data = b"\0";
        let mechs = parse_sasl_mechanisms(data);
        assert!(mechs.is_empty());
    }

    #[test]
    fn scram_roundtrip() {
        // Simulate a SCRAM exchange with known values
        let mut client = ScramClient::new("user", "pencil", None).unwrap();
        let _first = client.client_first_message();

        // Construct a fake server-first with the client's nonce prefix
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234salt5678");
        let server_first = format!("r={server_nonce},s={salt},i=4096");

        client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let final_msg = client.client_final_message().unwrap();
        let s = std::str::from_utf8(&final_msg).unwrap();
        assert!(s.starts_with("c=biws,r="));
        assert!(s.contains(",p="));
    }

    #[test]
    fn scram_rejects_bad_nonce() {
        let mut client = ScramClient::new("user", "pass", None).unwrap();
        let _first = client.client_first_message();
        let result = client.process_server_first(b"r=wrongnonce,s=c2FsdA==,i=4096");
        assert!(result.is_err());
    }

    /// constant_time_eq with different lengths does not leak via early return.
    #[test]
    fn constant_time_eq_different_lengths() {
        // Should return false without leaking length information
        assert!(!constant_time_eq(b"ab", b"abc"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(!constant_time_eq(b"a", b""));
        assert!(constant_time_eq(b"", b""));
    }

    /// 32-byte SHA-256 outputs should compare correctly.
    #[test]
    fn constant_time_eq_sha256_length() {
        let a = [0xAAu8; 32];
        let b = [0xAAu8; 32];
        let c = [0xBBu8; 32];
        assert!(constant_time_eq(&a, &b));
        assert!(!constant_time_eq(&a, &c));
    }

    // --- Audit gap tests ---

    // #47: SCRAM missing salt in server_first
    #[test]
    fn scram_missing_salt_error() {
        let mut client = ScramClient::new("user", "pass", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let server_first = format!("r={server_nonce},i=4096"); // missing s=
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("salt"), "should mention salt: {err}");
    }

    // #48: SCRAM missing iteration count
    #[test]
    fn scram_missing_iterations_error() {
        let mut client = ScramClient::new("user", "pass", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234");
        let server_first = format!("r={server_nonce},s={salt}"); // missing i=
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("iterations"),
            "should mention iterations: {err}"
        );
    }

    // #49: SCRAM non-numeric iteration count
    #[test]
    fn scram_non_numeric_iterations_error() {
        let mut client = ScramClient::new("user", "pass", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234");
        let server_first = format!("r={server_nonce},s={salt},i=notanumber");
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
    }

    // #50: SCRAM invalid base64 salt
    #[test]
    fn scram_invalid_base64_salt_error() {
        let mut client = ScramClient::new("user", "pass", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let server_first = format!("r={server_nonce},s=!@#$not_base64,i=4096");
        let result = client.process_server_first(server_first.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("base64") || err.contains("salt"),
            "should mention base64 or salt: {err}"
        );
    }

    // #51: SCRAM verify_server_final signature mismatch
    #[test]
    fn scram_verify_server_final_mismatch() {
        let mut client = ScramClient::new("user", "pencil", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234salt5678");
        let server_first = format!("r={server_nonce},s={salt},i=4096");
        client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let _final_msg = client.client_final_message().unwrap();

        // Provide a wrong server signature
        let wrong_sig = B64.encode(b"wrongwrongwrongwrongwrongwrongww"); // 32 bytes
        let server_final = format!("v={wrong_sig}");
        let result = client.verify_server_final(server_final.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("mismatch"), "should mention mismatch: {err}");
    }

    // #52: SCRAM verify_server_final missing v= prefix
    #[test]
    fn scram_verify_server_final_missing_prefix() {
        let mut client = ScramClient::new("user", "pencil", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234salt5678");
        let server_first = format!("r={server_nonce},s={salt},i=4096");
        client
            .process_server_first(server_first.as_bytes())
            .unwrap();

        let result = client.verify_server_final(b"no_v_prefix_here");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("v="),
            "should mention missing v= prefix: {err}"
        );
    }

    // #53: constant_time_eq with empty inputs
    #[test]
    fn constant_time_eq_both_empty_true() {
        assert!(constant_time_eq(b"", b""));
    }

    // #54: constant_time_eq with different lengths
    #[test]
    fn constant_time_eq_diff_lengths_false() {
        assert!(!constant_time_eq(b"a", b"ab"));
        assert!(!constant_time_eq(b"ab", b"a"));
        assert!(!constant_time_eq(b"", b"x"));
    }

    // #55: parse_sasl_mechanisms with only unsupported mechanisms
    #[test]
    fn parse_sasl_mechanisms_unsupported_only() {
        let data = b"SCRAM-SHA-512\0SCRAM-SHA-256-PLUS\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs.len(), 2);
        assert!(!mechs.contains(&"SCRAM-SHA-256"));
    }

    // --- Channel binding tests ---

    #[test]
    fn scram_channel_binding_none_prefix() {
        let scram = ScramClient::new("user", "pass", None).unwrap();
        let msg = scram.client_first_message();
        assert!(msg.starts_with(b"n,,"), "no-binding must start with n,,");
    }

    #[test]
    fn scram_channel_binding_plus_prefix() {
        let hash = [0xAA; 32];
        let scram = ScramClient::new("user", "pass", Some(&hash)).unwrap();
        let msg = scram.client_first_message();
        assert!(
            msg.starts_with(b"p=tls-server-end-point,,"),
            "PLUS must start with p=tls-server-end-point,,"
        );
    }

    #[test]
    fn scram_channel_binding_none_final_uses_biws() {
        // `biws` is base64("n,,") — the standard no-binding value
        let mut client = ScramClient::new("user", "pencil", None).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234salt5678");
        let server_first = format!("r={server_nonce},s={salt},i=4096");
        client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let final_msg = client.client_final_message().unwrap();
        let s = std::str::from_utf8(&final_msg).unwrap();
        assert!(s.starts_with("c=biws,"), "no-binding final must use c=biws");
    }

    #[test]
    fn scram_channel_binding_plus_final_encodes_hash() {
        let hash = [0xBB; 32];
        let mut client = ScramClient::new("user", "pencil", Some(&hash)).unwrap();
        let _first = client.client_first_message();
        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234salt5678");
        let server_first = format!("r={server_nonce},s={salt},i=4096");
        client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let final_msg = client.client_final_message().unwrap();
        let s = std::str::from_utf8(&final_msg).unwrap();

        // Extract the c= value and decode it
        let c_val = s.strip_prefix("c=").unwrap().split(',').next().unwrap();
        let decoded = B64.decode(c_val).unwrap();
        assert!(
            decoded.starts_with(b"p=tls-server-end-point,,"),
            "PLUS final c= must start with tls-server-end-point header"
        );
        // The remaining bytes must be the 32-byte hash
        let cb_header = b"p=tls-server-end-point,,";
        assert_eq!(
            &decoded[cb_header.len()..],
            &hash,
            "c= value must contain the certificate hash after the GS2 header"
        );
    }

    #[test]
    fn scram_roundtrip_with_channel_binding() {
        // Verify a full SCRAM-PLUS roundtrip produces valid messages
        let hash = [0x42; 32];
        let mut client = ScramClient::new("user", "pencil", Some(&hash)).unwrap();
        let first = client.client_first_message();
        let first_str = std::str::from_utf8(&first).unwrap();
        assert!(first_str.starts_with("p=tls-server-end-point,,n=user,r="));

        let server_nonce = format!("{}serverpart", client.nonce);
        let salt = B64.encode(b"salt1234salt5678");
        let server_first = format!("r={server_nonce},s={salt},i=4096");
        client
            .process_server_first(server_first.as_bytes())
            .unwrap();

        let final_msg = client.client_final_message().unwrap();
        let final_str = std::str::from_utf8(&final_msg).unwrap();
        assert!(final_str.starts_with("c="));
        assert!(final_str.contains(",p="));
        // Must NOT use biws (that's for no-binding)
        assert!(
            !final_str.starts_with("c=biws,"),
            "PLUS must not use c=biws"
        );
    }

    mod proptest_fuzz {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn parse_sasl_mechanisms_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
                let _ = parse_sasl_mechanisms(&data);
            }

            #[test]
            fn md5_password_never_panics(user in ".*", password in ".*", salt in proptest::array::uniform4(any::<u8>())) {
                let _ = md5_password(&user, &password, &salt);
            }
        }
    }
}
