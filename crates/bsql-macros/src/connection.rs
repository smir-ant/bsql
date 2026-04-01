//! Shared compile-time PostgreSQL connection for proc macro validation.
//!
//! Uses `LazyLock` to maintain a single connection across all `query!`
//! invocations within one `cargo build`. The first invocation pays ~5ms
//! for the connection. Subsequent invocations reuse it at ~0 cost.

use std::sync::LazyLock;
use tokio::runtime::Runtime;
use tokio_postgres::Client;

/// The shared connection state, initialized once per compilation.
struct MacroConnection {
    runtime: Runtime,
    client: Client,
    // Keep the connection task alive. If this handle is dropped, the
    // connection closes and all queries fail.
    _conn_handle: tokio::task::JoinHandle<()>,
}

/// The global shared connection. Initialized on first access.
static MACRO_CONN: LazyLock<Result<MacroConnection, String>> = LazyLock::new(|| {
    let database_url = std::env::var("BSQL_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| {
            "bsql: BSQL_DATABASE_URL or DATABASE_URL must be set for compile-time \
             SQL validation. Set one of these environment variables to a PostgreSQL \
             connection URL (e.g. postgres://user:pass@localhost/mydb)."
                .to_string()
        })?;

    let rt = Runtime::new().map_err(|e| format!("bsql: failed to create tokio runtime: {e}"))?;

    let mut pg_config: tokio_postgres::Config = database_url
        .parse()
        .map_err(|e| format!("bsql: invalid DATABASE_URL: {e}"))?;
    pg_config.connect_timeout(std::time::Duration::from_secs(10));

    let (client, connection) = rt
        .block_on(async {
            #[cfg(feature = "tls")]
            {
                let mut roots = rustls::RootCertStore::empty();
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                let tls_config = rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                pg_config.connect(tls).await
            }
            #[cfg(not(feature = "tls"))]
            {
                pg_config.connect(tokio_postgres::NoTls).await
            }
        })
        .map_err(|e| {
            format!(
                "bsql: failed to connect to PostgreSQL at compile time: {e}. \
                 Check that BSQL_DATABASE_URL or DATABASE_URL is set correctly \
                 and the database is running."
            )
        })?;

    let handle = rt.spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("bsql: compile-time connection error: {e}");
        }
    });

    // Set a statement timeout so that a runaway PREPARE or EXPLAIN
    // at compile time cannot hang the build indefinitely.
    rt.block_on(client.simple_query("SET statement_timeout = '30s'"))
        .map_err(|e| format!("bsql: failed to set statement_timeout: {e}"))?;

    Ok(MacroConnection {
        runtime: rt,
        client,
        _conn_handle: handle,
    })
});

/// Run an async operation on the shared compile-time connection.
///
/// Returns a `syn::Error` if the connection is not available.
pub fn with_connection<F, T>(f: F) -> Result<T, syn::Error>
where
    F: FnOnce(&Runtime, &Client) -> Result<T, String>,
{
    let conn = MACRO_CONN
        .as_ref()
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;
    f(&conn.runtime, &conn.client)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))
}
