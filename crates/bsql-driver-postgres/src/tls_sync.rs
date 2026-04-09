//! Synchronous TLS upgrade via rustls.
//!
//! Sends SSLRequest to PostgreSQL, reads the single-byte response ('S' = upgrade,
//! 'N' = no TLS), and upgrades the TCP stream to TLS if accepted.
//!
//! This is the sync counterpart to `tls.rs` — uses `rustls::StreamOwned` instead
//! of `tokio_rustls::TlsConnector`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};

use crate::proto;
use crate::types::Config;
use crate::DriverError;

/// Cached TLS client config for the default case (webpki roots, no client auth).
/// Built once, reused for all connections that don't specify custom CA/client certs.
static DEFAULT_TLS_CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();

fn init_default_tls_config() -> Arc<rustls::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    )
}

/// Build a per-connection TLS config when custom CA or client certs are specified.
///
/// - If `ssl_root_cert` is set: reads PEM, parses certs, uses them as the root store
///   instead of the system/webpki defaults.
/// - If `ssl_cert` + `ssl_key` are both set: reads PEMs, configures mTLS client auth.
/// - Otherwise: returns the global default config.
fn build_tls_config(config: &Config) -> Result<Arc<rustls::ClientConfig>, DriverError> {
    let needs_custom =
        config.ssl_root_cert.is_some() || (config.ssl_cert.is_some() && config.ssl_key.is_some());

    if !needs_custom {
        return Ok(DEFAULT_TLS_CONFIG
            .get_or_init(init_default_tls_config)
            .clone());
    }

    // Build root cert store: custom CA or default webpki roots.
    let mut root_store = rustls::RootCertStore::empty();
    if let Some(ref ca_path) = config.ssl_root_cert {
        let pem_data = std::fs::read(ca_path).map_err(|e| {
            DriverError::Protocol(format!("failed to read ssl_root_cert '{ca_path}': {e}"))
        })?;
        let certs = rustls_pemfile::certs(&mut &pem_data[..])
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DriverError::Protocol(format!(
                    "failed to parse PEM certificates from '{ca_path}': {e}"
                ))
            })?;
        if certs.is_empty() {
            return Err(DriverError::Protocol(format!(
                "no certificates found in ssl_root_cert '{ca_path}'"
            )));
        }
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| DriverError::Protocol(format!("failed to add CA certificate: {e}")))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    // Client certificate auth (mTLS) if both cert and key are provided.
    let tls_config =
        if let (Some(ref cert_path), Some(ref key_path)) = (&config.ssl_cert, &config.ssl_key) {
            let cert_pem = std::fs::read(cert_path).map_err(|e| {
                DriverError::Protocol(format!("failed to read ssl_cert '{cert_path}': {e}"))
            })?;
            let key_pem = std::fs::read(key_path).map_err(|e| {
                DriverError::Protocol(format!("failed to read ssl_key '{key_path}': {e}"))
            })?;

            let certs = rustls_pemfile::certs(&mut &cert_pem[..])
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DriverError::Protocol(format!(
                        "failed to parse client certificate from '{cert_path}': {e}"
                    ))
                })?;
            if certs.is_empty() {
                return Err(DriverError::Protocol(format!(
                    "no certificates found in ssl_cert '{cert_path}'"
                )));
            }

            let key = rustls_pemfile::private_key(&mut &key_pem[..])
                .map_err(|e| {
                    DriverError::Protocol(format!(
                        "failed to parse private key from '{key_path}': {e}"
                    ))
                })?
                .ok_or_else(|| {
                    DriverError::Protocol(format!("no private key found in ssl_key '{key_path}'"))
                })?;

            builder.with_client_auth_cert(certs, key).map_err(|e| {
                DriverError::Protocol(format!("failed to configure client certificate auth: {e}"))
            })?
        } else {
            builder.with_no_client_auth()
        };

    Ok(Arc::new(tls_config))
}

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
///
/// When `config` specifies `ssl_root_cert`, `ssl_cert`, or `ssl_key`, a
/// per-connection TLS config is built. Otherwise the global default is reused.
pub fn try_upgrade(
    mut tcp: TcpStream,
    config: &Config,
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
            let server_name = rustls::pki_types::ServerName::try_from(config.host.clone())
                .map_err(|e| {
                    DriverError::Protocol(format!("invalid TLS server name '{}': {e}", config.host))
                })?;

            let tls_cfg = build_tls_config(config)?;

            let tls_conn = rustls::ClientConnection::new(tls_cfg, server_name)
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
    fn tls_sync_default_config_cached() {
        // Verify the OnceLock TLS config is accessible and reusable
        let c1 = DEFAULT_TLS_CONFIG
            .get_or_init(init_default_tls_config)
            .clone();
        let c2 = DEFAULT_TLS_CONFIG
            .get_or_init(init_default_tls_config)
            .clone();
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    /// Helper: build a minimal Config with no custom TLS fields.
    fn default_config() -> Config {
        Config {
            host: "localhost".into(),
            port: 5432,
            user: "test".into(),
            password: "test".into(),
            database: "test".into(),
            ssl: crate::types::SslMode::Require,
            statement_timeout_secs: 30,
            statement_cache_mode: crate::types::StatementCacheMode::Named,
            ssl_root_cert: None,
            ssl_cert: None,
            ssl_key: None,
        }
    }

    /// Encode DER bytes as PEM with the given label.
    fn der_to_pem(label: &str, der: &[u8]) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(der);
        let mut pem = format!("-----BEGIN {label}-----\n");
        for chunk in b64.as_bytes().chunks(76) {
            pem.push_str(std::str::from_utf8(chunk).unwrap());
            pem.push('\n');
        }
        pem.push_str(&format!("-----END {label}-----\n"));
        pem
    }

    #[test]
    fn build_default_config_returns_global() {
        let cfg = default_config();
        let tls1 = build_tls_config(&cfg).unwrap();
        let tls2 = build_tls_config(&cfg).unwrap();
        // Both should return the same global Arc
        assert!(Arc::ptr_eq(&tls1, &tls2));
    }

    #[test]
    fn custom_ca_config_builds() {
        // Generate a self-signed CA certificate
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "bsql-test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = params.self_signed(&ca_key).unwrap();
        let ca_pem = der_to_pem("CERTIFICATE", ca_cert.der());

        let dir = std::env::temp_dir().join("bsql_tls_test_ca");
        std::fs::create_dir_all(&dir).unwrap();
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, &ca_pem).unwrap();

        let mut cfg = default_config();
        cfg.ssl_root_cert = Some(ca_path.to_str().unwrap().to_owned());

        let tls = build_tls_config(&cfg);
        assert!(tls.is_ok(), "custom CA config should build: {tls:?}");

        // Should NOT be the global default (it's a custom config)
        let default = DEFAULT_TLS_CONFIG
            .get_or_init(init_default_tls_config)
            .clone();
        assert!(!Arc::ptr_eq(&tls.unwrap(), &default));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn client_cert_config_builds() {
        // Generate a CA
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "bsql-test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        // Generate a client cert signed by the CA
        let mut client_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        client_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "bsql-test-client");
        let client_key = rcgen::KeyPair::generate().unwrap();
        let client_cert = client_params
            .signed_by(&client_key, &ca_cert, &ca_key)
            .unwrap();

        let client_cert_pem = der_to_pem("CERTIFICATE", client_cert.der());
        let client_key_pem = der_to_pem("PRIVATE KEY", &client_key.serialize_der());

        let dir = std::env::temp_dir().join("bsql_tls_test_client");
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("client.pem");
        let key_path = dir.join("client.key");
        std::fs::write(&cert_path, &client_cert_pem).unwrap();
        std::fs::write(&key_path, &client_key_pem).unwrap();

        let mut cfg = default_config();
        cfg.ssl_cert = Some(cert_path.to_str().unwrap().to_owned());
        cfg.ssl_key = Some(key_path.to_str().unwrap().to_owned());

        let tls = build_tls_config(&cfg);
        assert!(tls.is_ok(), "client cert config should build: {tls:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_ca_file_returns_error() {
        let mut cfg = default_config();
        cfg.ssl_root_cert = Some("/nonexistent/path/ca.pem".to_owned());

        let tls = build_tls_config(&cfg);
        assert!(tls.is_err());
        let err = format!("{}", tls.unwrap_err());
        assert!(
            err.contains("ssl_root_cert"),
            "error should mention ssl_root_cert: {err}"
        );
    }

    #[test]
    fn missing_client_cert_file_returns_error() {
        let mut cfg = default_config();
        cfg.ssl_cert = Some("/nonexistent/path/client.pem".to_owned());
        cfg.ssl_key = Some("/nonexistent/path/client.key".to_owned());

        let tls = build_tls_config(&cfg);
        assert!(tls.is_err());
        let err = format!("{}", tls.unwrap_err());
        assert!(
            err.contains("ssl_cert"),
            "error should mention ssl_cert: {err}"
        );
    }

    #[test]
    fn empty_ca_pem_returns_error() {
        let dir = std::env::temp_dir().join("bsql_tls_test_empty_ca");
        std::fs::create_dir_all(&dir).unwrap();
        let ca_path = dir.join("empty.pem");
        std::fs::write(&ca_path, "").unwrap();

        let mut cfg = default_config();
        cfg.ssl_root_cert = Some(ca_path.to_str().unwrap().to_owned());

        let tls = build_tls_config(&cfg);
        assert!(tls.is_err());
        let err = format!("{}", tls.unwrap_err());
        assert!(
            err.contains("no certificates"),
            "error should mention no certificates: {err}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
