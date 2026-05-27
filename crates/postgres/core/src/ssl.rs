use crate::config::{ConnectConfig, SslMode};
use crate::error::DriverError;

/// SSL probe result after sending the SSL request byte and reading response.
pub enum SslProbe {
    /// Server accepted SSL. Caller should wrap TCP in TLS.
    Accepted {
        tls_config: std::sync::Arc<rustls::ClientConfig>,
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

/// Classify the server's 1-byte SSL response and build TLS config if accepted.
pub fn classify_ssl_response(
    ssl_proto: bsql_postgres_proto::PgProtocol<bsql_postgres_proto::SslNegotiatingPhase>,
    response_byte: u8,
    config: &ConnectConfig,
) -> Result<SslProbe, DriverError> {
    let classified = ssl_proto.classify_ssl_response(response_byte);
    match classified {
        bsql_postgres_proto::SslClassified::Accepted(_) => {
            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let server_name: rustls::pki_types::ServerName<'_> = config.host.as_str().try_into()
                .map_err(|_| DriverError::Config("invalid server name for TLS"))?;
            Ok(SslProbe::Accepted {
                tls_config: std::sync::Arc::new(tls_config),
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
