//! The PostgreSQL `SSLRequest` probe and its `'S'` / `'N'` response classifier
//! (TLS-only — compiled out with the `tls` feature off).

use crate::config::{ConnectConfig, SslMode};
use crate::error::DriverError;

/// SSL probe result after sending the SSL request bytes and reading the
/// server's one-byte reply.
///
/// On [`Accepted`](SslProbe::Accepted) the caller wraps the socket in TLS using
/// the provider-explicit config from [`tls::shared_client_config`] and the
/// `server_name` carried here. The probe does NOT build a `rustls::ClientConfig`
/// itself: the workspace pins rustls to the ring provider only, so a bare
/// `ClientConfig::builder()` would install no default provider and fault at the
/// handshake — every driver uses the single `shared_client_config` (ring-explicit)
/// instead.
///
/// [`tls::shared_client_config`]: crate::tls::shared_client_config
pub enum SslProbe {
    /// Server accepted SSL. The caller wraps the socket in TLS to `server_name`.
    Accepted {
        /// The verified server name for the TLS handshake (from `config.host`).
        server_name: rustls::pki_types::ServerName<'static>,
    },
    /// Server refused SSL. Caller continues with plain TCP.
    PlainTcp,
}

/// The static 8-byte PostgreSQL `SSLRequest` packet to write before reading the
/// server's 1-byte reply (classify it with [`classify_ssl_response`]).
pub fn ssl_request_bytes() -> &'static [u8; 8] {
    &bsql_postgres_proto::SSL_REQUEST_WIRE_BYTES
}

/// Classify the server's 1-byte SSL response into an [`SslProbe`], given the
/// `ssl_mode` ALREADY RESOLVED for this endpoint (by
/// [`ConnectConfig::resolve_ssl_mode`](crate::ConnectConfig::resolve_ssl_mode)).
///
/// On acceptance, derives the verified `server_name` from `config.host` (the TLS
/// config is built once per process by [`tls::shared_client_config`], not here).
///
/// On refusal, honours the resolved `ssl_mode`. `Require` is a hard error whose
/// class distinguishes HOW the mode was required:
///
/// - an EXPLICIT `Require` (the consumer set it) → [`DriverError::SslRefused`]:
///   the caller's own TLS contract was violated.
/// - a DEFAULTED-remote `Require` (bsql required TLS by the threat-scoped default
///   for a remote host) → a [`DriverError::Config`] whose message names the fix —
///   set `SslMode::Prefer` or `Disable` explicitly if plaintext to this remote
///   host is intentional. This is the loud replacement for the former silent
///   plaintext fallback to a remote server.
///
/// `Prefer` warns ON STDERR (in debug AND release — an SSL downgrade is a
/// security event a production build must not hide) and falls back to plain TCP.
/// A consumer that must not silently downgrade can additionally assert
/// `Connection::is_encrypted()` after connect. Any other byte (a server
/// `ErrorResponse` start, or an out-of-protocol value) is a hard
/// [`DriverError::Io`] — never a silent fallback.
///
/// [`tls::shared_client_config`]: crate::tls::shared_client_config
pub fn classify_ssl_response(
    response_byte: u8,
    config: &ConnectConfig,
    ssl_mode: SslMode,
) -> Result<SslProbe, DriverError> {
    use bsql_postgres_proto::wire::SslNegotiationOutcome;
    match bsql_postgres_proto::wire::classify_ssl_response_byte(response_byte) {
        SslNegotiationOutcome::Accepted => {
            let server_name: rustls::pki_types::ServerName<'_> = config.host.as_str().try_into()
                .map_err(|_| DriverError::Config("invalid server name for TLS"))?;
            Ok(SslProbe::Accepted {
                server_name: server_name.to_owned(),
            })
        }
        SslNegotiationOutcome::Refused => {
            if ssl_mode == SslMode::Require {
                // Explicit Require → the caller's contract was refused. Defaulted
                // Require (only ever a REMOTE host — a local endpoint defaults to
                // Prefer, never Require) → name the plaintext opt-out in the error,
                // since bsql chose Require on the consumer's behalf.
                return Err(if config.ssl_mode_is_explicit() {
                    DriverError::SslRefused
                } else {
                    DriverError::Config(
                        "the server refused TLS, which bsql required by default for \
                         this remote host; set SslMode::Prefer or Disable explicitly \
                         if a plaintext connection to this remote host is intentional",
                    )
                });
            }
            // Emit in debug AND release: a silent downgrade to plaintext on an
            // untrusted network is exactly the event a production build must
            // surface. stderr keeps it dependency-free (no logging crate). A
            // consumer can also assert `Connection::is_encrypted()` to fail hard.
            eprintln!("[bsql] WARNING: SSL refused by server, falling back to plain TCP. \
                Use SslMode::Require for production over untrusted networks, or assert \
                Connection::is_encrypted().");
            Ok(SslProbe::PlainTcp)
        }
        // `ErrorIncoming` (a server `ErrorResponse` start), `InvalidByte`, or any
        // future-added outcome (`SslNegotiationOutcome` is `#[non_exhaustive]`):
        // fail closed — never a silent fallback to plain TCP.
        _ => Err(DriverError::Io(std::io::Error::other("unexpected SSL response"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The PG `SSLRequest` reply byte for "SSL refused" — the branch under test.
    const REFUSED: u8 = b'N';

    #[test]
    fn refused_explicit_require_is_the_ssl_refused_class() {
        // An EXPLICITLY-required TLS that the server refuses is the caller's own
        // contract being violated → the honest `SslRefused` class (unchanged).
        let config = ConnectConfig::new("db.example.com", "u").ssl_mode(SslMode::Require);
        match classify_ssl_response(REFUSED, &config, SslMode::Require) {
            Err(DriverError::SslRefused) => {}
            Err(other) => panic!("explicit Require refused must be SslRefused, got {other:?}"),
            Ok(_) => panic!("explicit Require refused must NOT fall back to plain TCP"),
        }
    }

    #[test]
    fn refused_defaulted_remote_require_is_a_config_error_naming_the_fix() {
        // WITNESS (c): a remote host with an UNSET mode resolves to `Require`; a
        // refusal is a LOUD classified error whose message names the plaintext
        // opt-out — never a silent plaintext connect to a remote server.
        let config = ConnectConfig::new("db.example.com", "u");
        assert!(!config.ssl_mode_is_explicit(), "the mode must be defaulted here");
        let resolved =
            config.resolve_ssl_mode(&crate::resolve_endpoint(&config.host, config.port));
        assert_eq!(resolved, SslMode::Require, "a remote host defaults to Require");
        match classify_ssl_response(REFUSED, &config, resolved) {
            Err(DriverError::Config(msg)) => {
                assert!(
                    msg.contains("refused TLS")
                        && msg.contains("SslMode::Prefer or Disable")
                        && msg.contains("remote host"),
                    "the defaulted-remote refusal must name the fix, got {msg:?}",
                );
            }
            Err(other) => panic!("defaulted-remote Require refused must be Config, got {other:?}"),
            Ok(_) => panic!("defaulted-remote Require refused must NOT fall back to plain TCP"),
        }
    }

    #[test]
    fn refused_prefer_falls_back_to_plain_tcp() {
        // `Prefer` (explicit or the local default) tolerates a refusal — plain TCP
        // with a stderr warning, never an error.
        let config = ConnectConfig::new("localhost", "u");
        match classify_ssl_response(REFUSED, &config, SslMode::Prefer) {
            Ok(SslProbe::PlainTcp) => {}
            Ok(SslProbe::Accepted { .. }) => {
                panic!("a refusal must never be classified as Accepted")
            }
            Err(e) => panic!("Prefer refused must fall back to PlainTcp, got Err: {e:?}"),
        }
    }
}
