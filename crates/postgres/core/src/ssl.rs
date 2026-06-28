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

/// Prepare the SSL request bytes (8 bytes, static).
pub fn ssl_request_bytes() -> (&'static [u8; 8], bsql_postgres_proto::PgProtocol<bsql_postgres_proto::SslNegotiatingPhase>) {
    let proto = bsql_postgres_proto::PgProtocol::new();
    proto.push_ssl_request()
}

/// Classify the server's 1-byte SSL response into an [`SslProbe`].
///
/// On acceptance, derives the verified `server_name` from `config.host` (the TLS
/// config is built once per process by [`tls::shared_client_config`], not here).
/// On refusal, honours `SslMode`: `Require` is a hard [`DriverError::SslRefused`];
/// `Prefer` warns in debug builds and falls back to plain TCP.
///
/// [`tls::shared_client_config`]: crate::tls::shared_client_config
pub fn classify_ssl_response(
    ssl_proto: bsql_postgres_proto::PgProtocol<bsql_postgres_proto::SslNegotiatingPhase>,
    response_byte: u8,
    config: &ConnectConfig,
) -> Result<SslProbe, DriverError> {
    let classified = ssl_proto.classify_ssl_response(response_byte);
    match classified {
        bsql_postgres_proto::SslClassified::Accepted(_) => {
            let server_name: rustls::pki_types::ServerName<'_> = config.host.as_str().try_into()
                .map_err(|_| DriverError::Config("invalid server name for TLS"))?;
            Ok(SslProbe::Accepted {
                server_name: server_name.to_owned(),
            })
        }
        bsql_postgres_proto::SslClassified::Refused(_) => {
            if config.ssl_mode == SslMode::Require {
                return Err(DriverError::SslRefused);
            }
            #[cfg(debug_assertions)]
            eprintln!("[bsql] WARNING: SSL refused by server, falling back to plain TCP. \
                Use SslMode::Require for production over untrusted networks.");
            Ok(SslProbe::PlainTcp)
        }
        _ => Err(DriverError::Io(std::io::Error::other("unexpected SSL response"))),
    }
}
