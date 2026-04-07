//! Synchronous TLS upgrade via rustls.
//!
//! Sends SSLRequest to PostgreSQL, reads the single-byte response ('S' = upgrade,
//! 'N' = no TLS), and upgrades the TCP stream to TLS if accepted.
//!
//! This is the sync counterpart to `tls.rs` — uses `rustls::StreamOwned` instead
//! of `tokio_rustls::TlsConnector`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, LazyLock};

use crate::proto;
use crate::DriverError;

/// Cached TLS client config. Built once, reused for all connections.
static TLS_CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    )
});

/// Result of a successful TLS upgrade, carrying both the encrypted stream
/// and the SHA-256 hash of the server's end-entity certificate (for SCRAM
/// channel binding via `tls-server-end-point`).
pub struct TlsUpgradeResult {
    pub stream: rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
    /// SHA-256 hash of the server's end-entity certificate.
    /// `None` if the certificate could not be extracted (should not happen
    /// in practice, but we degrade gracefully to no channel binding).
    pub server_cert_hash: Option<[u8; 32]>,
}

/// Attempt synchronous TLS upgrade on a TCP connection.
///
/// 1. Send SSLRequest (8 bytes).
/// 2. Read server response: 'S' (accept) or 'N' (reject).
/// 3. If 'S', perform TLS handshake with rustls.
///
/// On success, also extracts the server certificate SHA-256 hash for
/// SCRAM-SHA-256-PLUS channel binding (`tls-server-end-point`).
///
/// If `required` is true and server responds 'N', returns an error.
/// If `required` is false and server responds 'N', returns an error that the
/// caller should handle by falling back to plain TCP (reconnecting).
pub fn try_upgrade(
    mut tcp: TcpStream,
    host: &str,
    required: bool,
) -> Result<TlsUpgradeResult, DriverError> {
    // Send SSLRequest
    let mut buf = Vec::with_capacity(8);
    proto::write_ssl_request(&mut buf);
    tcp.write_all(&buf).map_err(DriverError::Io)?;
    tcp.flush().map_err(DriverError::Io)?;

    // Read response byte
    let mut response = [0u8; 1];
    tcp.read_exact(&mut response).map_err(DriverError::Io)?;

    match response[0] {
        b'S' => {
            // Server accepts TLS — perform handshake
            let server_name =
                rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| {
                    DriverError::Protocol(format!("invalid TLS server name '{host}': {e}"))
                })?;

            let tls_conn = rustls::ClientConnection::new(TLS_CONFIG.clone(), server_name)
                .map_err(|e| DriverError::Io(std::io::Error::other(e)))?;

            let stream = rustls::StreamOwned::new(tls_conn, tcp);

            // Extract server certificate hash for SCRAM channel binding.
            // RFC 5929 `tls-server-end-point`: SHA-256 of the DER-encoded
            // end-entity certificate.
            let server_cert_hash = stream
                .conn
                .peer_certificates()
                .and_then(|certs| certs.first())
                .map(|cert| {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(cert.as_ref());
                    let hash: [u8; 32] = hasher.finalize().into();
                    hash
                });

            Ok(TlsUpgradeResult {
                stream,
                server_cert_hash,
            })
        }
        b'N' => {
            if required {
                Err(DriverError::Protocol(
                    "server does not support TLS (sslmode=require)".into(),
                ))
            } else {
                Err(DriverError::Protocol(
                    "server declined TLS (sslmode=prefer, falling back)".into(),
                ))
            }
        }
        other => Err(DriverError::Protocol(format!(
            "unexpected SSL response byte: 0x{other:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_sync_config_cached() {
        // Verify the LazyLock TLS config is accessible and reusable
        let c1 = TLS_CONFIG.clone();
        let c2 = TLS_CONFIG.clone();
        assert!(Arc::ptr_eq(&c1, &c2));
    }
}
