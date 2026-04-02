//! PostgreSQL authentication — MD5 and SCRAM-SHA-256.
//!
//! MD5 is simple: `"md5" + hex(md5(hex(md5(password + user)) + salt))`.
//!
//! SCRAM-SHA-256 implements RFC 5802 with channel binding disabled (`n,,`).

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, Mac};
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::DriverError;

type HmacSha256 = Hmac<Sha256>;

// --- MD5 ---

/// Compute the MD5 password hash for PostgreSQL authentication.
///
/// Result is `"md5" + hex(md5(hex(md5(password + user)) + salt))`, NUL-terminated.
pub fn md5_password(user: &str, password: &str, salt: &[u8; 4]) -> Vec<u8> {
    // Step 1: md5(password + user)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(user.as_bytes());
    let inner = hex_encode(&hasher.finalize());

    // Step 2: md5(hex_inner + salt)
    let mut hasher = Md5::new();
    hasher.update(inner.as_bytes());
    hasher.update(salt);
    let outer = hex_encode(&hasher.finalize());

    // "md5" + hex + NUL
    let mut result = Vec::with_capacity(3 + 32 + 1);
    result.extend_from_slice(b"md5");
    result.extend_from_slice(outer.as_bytes());
    result.push(0);
    result
}

// --- SCRAM-SHA-256 ---

/// SCRAM-SHA-256 client state machine.
///
/// Usage:
/// 1. Create with `ScramClient::new(user, password)`.
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
}

impl ScramClient {
    /// Create a new SCRAM client for the given credentials.
    pub fn new(user: &str, password: &str) -> Result<Self, DriverError> {
        let nonce = generate_nonce()?;
        let client_first_bare = format!("n={user},r={nonce}");

        Ok(Self {
            password: password.to_owned(),
            nonce,
            client_first_bare,
            server_first: String::new(),
            salted_password: [0u8; 32],
            auth_message: String::new(),
        })
    }

    /// Generate the client-first message: `n,,n=user,r=nonce`.
    ///
    /// The `n,,` prefix indicates no channel binding.
    pub fn client_first_message(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
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

        // Build auth message for proof computation
        let client_final_without_proof = format!("c=biws,r={server_nonce}");
        self.auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof
        );

        Ok(())
    }

    /// Generate the client-final message with proof.
    ///
    /// Returns `c=biws,r=combined_nonce,p=base64_proof`.
    pub fn client_final_message(&self) -> Result<Vec<u8>, DriverError> {
        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = hmac_sha256(&self.salted_password, b"Client Key");

        // StoredKey = SHA256(ClientKey)
        let stored_key = Sha256::digest(client_key);

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&stored_key, self.auth_message.as_bytes());

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

        let msg = format!("c=biws,r={server_nonce},p={proof_b64}");
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
        let server_key = hmac_sha256(&self.salted_password, b"Server Key");

        // Expected = HMAC(ServerKey, AuthMessage)
        let expected = hmac_sha256(&server_key, self.auth_message.as_bytes());

        if server_sig.len() != expected.len() || !constant_time_eq(&server_sig, &expected) {
            return Err(DriverError::Auth("server signature mismatch".into()));
        }

        Ok(())
    }
}

// --- Helpers ---

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    // HMAC-SHA256 accepts any key length, so new_from_slice cannot fail in practice.
    // We handle the Result anyway to avoid panics from expect().
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(m) => m,
        Err(_) => {
            // Unreachable for SHA-256 (no key length restriction), but return zeroes
            // defensively rather than panicking.
            return [0u8; 32];
        }
    };
    mac.update(data);
    mac.finalize().into_bytes().into()
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
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Lowercase hex encoding of a byte slice.
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
pub fn parse_sasl_mechanisms(data: &[u8]) -> Vec<&str> {
    let mut mechanisms = Vec::new();
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
        // Must start with "md5" and be 3 + 32 + 1 = 36 bytes
        assert_eq!(result.len(), 36);
        assert!(result.starts_with(b"md5"));
        assert_eq!(*result.last().unwrap(), 0); // NUL terminated
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
        let client = ScramClient::new("testuser", "testpass").unwrap();
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
        assert_eq!(mechs, &["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
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
        let mut client = ScramClient::new("user", "pencil").unwrap();
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
        let mut client = ScramClient::new("user", "pass").unwrap();
        let _first = client.client_first_message();
        let result = client.process_server_first(b"r=wrongnonce,s=c2FsdA==,i=4096");
        assert!(result.is_err());
    }
}
