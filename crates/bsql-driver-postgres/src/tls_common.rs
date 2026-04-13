//! Shared rustls primitives used by both sync and async TLS paths.
//!
//! # Central invariant
//!
//! bsql hard-pins `ring` as the rustls crypto provider and passes it
//! explicitly to every `ClientConfig::builder_with_provider` call. This
//! bypasses rustls 0.23's process-level `CryptoProvider` auto-selection,
//! which panics at runtime when cargo feature unification pulls in BOTH
//! `ring` and `aws-lc-rs` (for example, when a user's project depends on
//! `reqwest` with `rustls-tls-native-roots` alongside bsql's `ring`), or
//! when neither is enabled.
//!
//! The panic this module replaces lives at
//! `rustls-0.23/src/crypto/mod.rs:249` —
//!
//! > "Could not automatically determine the process-level CryptoProvider
//! > from Rustls crate features. Call `CryptoProvider::install_default()`
//! > before this point to select a provider manually, or make sure
//! > exactly one of the `aws-lc-rs` and `ring` features is enabled."
//!
//! That panic is only reachable through the zero-argument
//! `ClientConfig::builder()` constructor. The
//! `builder_with_provider(provider)` constructor takes an explicit
//! provider and never touches the process-level state, so it cannot
//! reach `get_default_or_install_from_crate_features`. `builder()` is
//! forbidden in this crate: every TLS config is built via
//! [`build_client_config`] below.
//!
//! # Cost
//!
//! - **Hot path (per-connection TLS handshake)**: zero allocations for
//!   the crypto provider. `ring_provider()` clones a cached `Arc` — an
//!   atomic refcount bump.
//! - **Cold path (first TLS setup in the process)**: exactly one
//!   `Arc::new(CryptoProvider)` allocation for the process lifetime.
//! - **Default `ClientConfig`**: also cached, one `Arc<ClientConfig>`
//!   allocation total, shared between sync and async paths.
//! - **Custom `ClientConfig`** (`ssl_root_cert`/`ssl_cert`/`ssl_key`):
//!   built once per pool that requests it, reuses the shared provider.

use std::sync::{Arc, LazyLock};

use crate::DriverError;

/// Process-wide `ring` crypto provider, initialized on first access.
///
/// Returns a cloned `Arc` so callers own a refcounted handle without
/// having to worry about static lifetimes. The `LazyLock` guarantees
/// exactly one allocation for the process lifetime; every subsequent
/// call is a pointer read plus an atomic refcount increment.
pub(crate) fn ring_provider() -> Arc<rustls::crypto::CryptoProvider> {
    static PROVIDER: LazyLock<Arc<rustls::crypto::CryptoProvider>> =
        LazyLock::new(|| Arc::new(rustls::crypto::ring::default_provider()));
    PROVIDER.clone()
}

/// Process-wide default [`rustls::ClientConfig`] — webpki roots, no
/// client authentication.
///
/// Shared between the sync and async TLS paths. Built on first access
/// from [`ring_provider`] and `webpki_roots::TLS_SERVER_ROOTS`; every
/// subsequent call returns the cached `Arc`.
///
/// Custom-CA or mTLS configurations bypass this cache and go through
/// [`build_client_config`] directly to get their own `ClientConfig`.
pub(crate) fn default_client_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        // The default config is built from a hardcoded trust anchor
        // and the pinned `ring` provider. It is infallible by
        // construction — any failure here is a bsql/rustls bug and
        // not something a user can fix at runtime, so panic with a
        // clear diagnostic rather than returning a Result up the
        // call chain and polluting every TLS-unrelated code path.
        let config = build_client_config(root_store, None).expect(
            "bsql: default rustls ClientConfig must build with webpki roots \
             and the ring provider — this is a programmer error",
        );
        Arc::new(config)
    });
    CONFIG.clone()
}

/// Build a [`rustls::ClientConfig`] with the given root store and
/// optional mTLS client authentication.
///
/// This is the single place in the crate that invokes
/// `ClientConfig::builder_with_provider`. Both the default and custom
/// code paths go through here so the crypto provider choice is
/// single-sourced — change `ring` to another provider in [`ring_provider`]
/// and every `ClientConfig` in the process picks it up without touching
/// any other file.
///
/// `client_auth` is `Some((certs, key))` for mTLS, `None` for server-only
/// verification.
pub(crate) fn build_client_config(
    root_store: rustls::RootCertStore,
    client_auth: Option<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )>,
) -> Result<rustls::ClientConfig, DriverError> {
    let builder = rustls::ClientConfig::builder_with_provider(ring_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            DriverError::Protocol(format!(
                "rustls: ring provider rejected default protocol versions: {e} \
                 (this is a bsql/rustls bug, please file an issue)"
            ))
        })?
        .with_root_certificates(root_store);

    match client_auth {
        Some((certs, key)) => builder.with_client_auth_cert(certs, key).map_err(|e| {
            DriverError::Protocol(format!("failed to configure client certificate auth: {e}"))
        }),
        None => Ok(builder.with_no_client_auth()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The provider must be cached across calls — `ring_provider()` is
    /// the primary hot-path allocator, and regressing it to a per-call
    /// `Arc::new` would allocate on every TLS handshake.
    #[test]
    fn ring_provider_is_cached_across_calls() {
        let a = ring_provider();
        let b = ring_provider();
        assert!(
            Arc::ptr_eq(&a, &b),
            "ring_provider() must return the same Arc across calls — \
             LazyLock caching is the whole point"
        );
        // At least two references (a and b) plus the one held by LazyLock.
        assert!(Arc::strong_count(&a) >= 3);
    }

    /// The default ClientConfig must also be cached — sync and async
    /// paths share it to avoid building a fresh config per connection.
    #[test]
    fn default_client_config_is_cached_across_calls() {
        let a = default_client_config();
        let b = default_client_config();
        assert!(
            Arc::ptr_eq(&a, &b),
            "default_client_config() must return the same Arc across calls"
        );
    }

    /// The crucial regression test: building a `ClientConfig` through
    /// `build_client_config` must not panic even when cargo feature
    /// unification has enabled BOTH `ring` and `aws-lc-rs` on rustls.
    ///
    /// This test is only meaningful when the dev-dependencies in
    /// `Cargo.toml` enable `rustls` with `features = ["aws-lc-rs"]`,
    /// which forces cargo to unify them with bsql's own `ring` feature.
    /// See the matching `tls_common_panic_test` negative assertion for
    /// proof that the test environment actually reproduces the conflict.
    #[test]
    fn build_client_config_default_does_not_panic() {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let result = build_client_config(root_store, None);
        assert!(
            result.is_ok(),
            "build_client_config must succeed for webpki roots + no-auth: {:?}",
            result.err()
        );
    }

    /// Under cargo feature unification with both `ring` and `aws-lc-rs`
    /// enabled, the legacy `ClientConfig::builder()` constructor is
    /// supposed to panic at rustls-0.23/src/crypto/mod.rs:249 because
    /// it cannot pick a default provider.
    ///
    /// This test catches that panic via `catch_unwind` and fails if the
    /// panic does NOT occur — which would mean the test environment
    /// isn't actually reproducing the conflict and the other tests are
    /// false-green.
    ///
    /// Only runs when the dev-dep pulls in `aws-lc-rs` alongside `ring`.
    /// Guarded behind `feature-unification-repro` (on by default in
    /// this crate's dev builds) so bsql downstream users never compile
    /// the dev-dep.
    #[test]
    #[cfg(feature = "feature-unification-repro")]
    fn legacy_builder_panics_under_feature_unification() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let result = catch_unwind(AssertUnwindSafe(|| {
            // `builder()` goes through `get_default_or_install_from_crate_features`
            // which panics when both ring and aws-lc-rs features are on.
            let _ = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
        }));

        assert!(
            result.is_err(),
            "rustls::ClientConfig::builder() was expected to panic under \
             feature unification (both 'ring' and 'aws-lc-rs' on rustls). \
             If this assertion fails, the dev-dep setup isn't reproducing \
             the conflict and the positive regression tests above are \
             false-green — meaning the fix may not actually protect against \
             the panic in the wild."
        );
    }
}
