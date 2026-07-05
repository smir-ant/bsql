use std::sync::Arc;

/// SSL negotiation mode.
///
/// # Security
///
/// `Prefer` silently falls back to plain TCP if the server refuses SSL.
/// An active network attacker can forge the single-byte 'N' refusal
/// (sent before TLS protects the stream), stripping SSL entirely.
/// Use `Require` for production deployments over untrusted networks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum SslMode {
    /// No SSL. Plain TCP. Use only for localhost/unix socket.
    Disable,
    /// Try SSL; fall back to plain TCP if server refuses.
    /// **WARNING**: vulnerable to active SSL-stripping attacks.
    /// Use `Require` for production over untrusted networks.
    #[default]
    Prefer,
    /// Require SSL. Fail if server refuses. Safe against downgrade.
    Require,
}

// Footprint pin: a 3-variant fieldless enum is a single discriminant byte. A
// variant accidentally carrying data would widen it; the pin catches that.
crate::footprint_pin!(SslMode, size = 1, align = 1);


/// Connection configuration.
///
/// Password is zeroized on drop via `Zeroizing<String>` wrapper.
/// Debug output redacts the password field.
#[derive(Clone)]
#[non_exhaustive]
pub struct ConnectConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: Option<String>,
    password_inner: Option<zeroize::Zeroizing<String>>,
    pub connect_timeout_secs: u64,
    pub ssl_mode: SslMode,
    /// Consumer-supplied PostgreSQL startup parameters (GUCs) sent in the
    /// `StartupMessage` — `search_path`, `application_name`,
    /// `statement_timeout`, or any GUC. Set via [`with_startup_param`],
    /// [`with_search_path`], [`with_application_name`]; ordered by insertion.
    ///
    /// Stored raw and validated at connect (each pair must be NUL-free,
    /// bounded, and not a reserved parameter) — consistent with how `user` /
    /// `database` are validated to their bounded wire types at connect, not at
    /// the builder. Kept private so the builders are the only entry point; the
    /// drivers read it via [`startup_params`].
    ///
    /// [`with_startup_param`]: ConnectConfig::with_startup_param
    /// [`with_search_path`]: ConnectConfig::with_search_path
    /// [`with_application_name`]: ConnectConfig::with_application_name
    /// [`startup_params`]: ConnectConfig::startup_params
    startup_params: Vec<(String, String)>,
    /// Consumer-supplied CA certificate roots (PEM), or `None` to verify the
    /// server certificate against the DEFAULT trust anchors (the baked Mozilla
    /// bundle, under the `webpki-roots` feature). Set via [`with_ca_roots`] or
    /// the `sslrootcert=<path>` DSN key / `PGSSLROOTCERT` env var.
    ///
    /// Stored RAW (the PEM bytes) and parsed into a `rustls` root store at
    /// connect — consistent with `startup_params`, which are also stored raw and
    /// validated at connect. An invalid/empty PEM is a classified
    /// [`DriverError::Config`](crate::DriverError) at connect, never a silent
    /// fallback to the default roots or to plaintext. `Arc<[u8]>` so a
    /// `ConnectConfig` clone (e.g. per pool checkout) is an O(1) refcount bump,
    /// not a PEM deep-copy.
    ///
    /// [`with_ca_roots`]: ConnectConfig::with_ca_roots
    ca_roots_pem: Option<Arc<[u8]>>,
}

// Footprint pin: four owned Strings/Option<String> (host, user, database,
// password) plus a u16 port, a u64 timeout, the SslMode byte, the
// startup-params `Vec<(String, String)>` (one 3-word / 24-byte owned handle;
// its heap buffer is empty until a startup parameter is added), and the
// custom-CA `Option<Arc<[u8]>>` (a 16-byte fat pointer, niche-packed into 16 B;
// `None` until a CA is supplied). Config is built once per connection, so its
// size is not hot — but pinning it keeps a silently-added field on the review
// surface, and the password is a Zeroizing<String> whose 3-word shape must not
// regress. 112 + 24 (the Vec) + 16 (the Arc) = 152.
crate::footprint_pin!(ConnectConfig, size = 152, align = 8);

impl ConnectConfig {
    pub fn password_str(&self) -> Option<&str> {
        self.password_inner.as_deref().map(|s| s.as_str())
    }
}

impl core::fmt::Debug for ConnectConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("password", &self.password_inner.as_ref().map(|_| "[REDACTED]"))
            .field("ssl_mode", &self.ssl_mode)
            // Startup parameters are not secret (search_path, application_name,
            // …), so they are shown in full — consistent with host/user/db.
            .field("startup_params", &self.startup_params)
            .finish()
    }
}

impl ConnectConfig {
    /// Parse a PostgreSQL connection string (DSN).
    ///
    /// Format: `postgres://user:password@host:port/database?sslmode=require`
    ///
    /// All components except user are optional:
    /// - `postgres://user@host/db` — no password, default port
    /// - `postgres://user:pass@host` — no database (defaults to user)
    /// - `postgres://user@host?sslmode=disable`
    pub fn from_dsn(dsn: &str) -> Result<Self, String> {
        let s = dsn.strip_prefix("postgres://")
            .or_else(|| dsn.strip_prefix("postgresql://"))
            .ok_or_else(|| "DSN must start with postgres:// or postgresql://".to_string())?;

        // Split query string
        let (main, query) = match s.split_once('?') {
            Some((m, q)) => (m, Some(q)),
            None => (s, None),
        };

        // Split userinfo@hostpath
        let (userinfo, hostpath) = match main.split_once('@') {
            Some((u, h)) => (u, h),
            None => return Err("missing @ in DSN".to_string()),
        };

        // Parse user:password
        let (user, password) = match userinfo.split_once(':') {
            Some((u, p)) => (u.to_string(), Some(p.to_string())),
            None => (userinfo.to_string(), None),
        };

        if user.is_empty() {
            return Err("empty user in DSN".to_string());
        }

        // Parse host:port/database
        let (hostport, database) = match hostpath.split_once('/') {
            Some((hp, db)) => (hp, if db.is_empty() { None } else { Some(db.to_string()) }),
            None => (hostpath, None),
        };

        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => {
                let port = p.parse::<u16>().map_err(|_| format!("invalid port: {p}"))?;
                (h.to_string(), port)
            }
            None => (hostport.to_string(), 5432),
        };

        // Parse query params
        let mut ssl_mode = SslMode::Prefer;
        let mut timeout = 10u64;
        let mut ca_roots_pem: Option<Arc<[u8]>> = None;
        if let Some(q) = query {
            for param in q.split('&') {
                // A parameter with no `=` carries no value. Silently skipping it
                // would, for `?sslmode`, keep the default `prefer` and downgrade
                // SSL without the caller knowing. Reject it loudly instead.
                let (k, v) = match param.split_once('=') {
                    Some(kv) => kv,
                    None => return Err(format!("malformed DSN parameter (missing '='): {param}")),
                };
                match k {
                    "sslmode" => {
                        ssl_mode = match v {
                            "disable" => SslMode::Disable,
                            "prefer" => SslMode::Prefer,
                            "require" => SslMode::Require,
                            other => return Err(format!("unknown sslmode: {other}")),
                        };
                    }
                    "connect_timeout" => {
                        timeout = v.parse().map_err(|_| format!("invalid timeout: {v}"))?;
                    }
                    "sslrootcert" => {
                        // Read the CA bundle from the given path NOW. libpq reads
                        // `sslrootcert` at connect; here, DSN parsing is the
                        // connect-config assembly step. A missing/unreadable file
                        // is a loud error carrying the path — never a silent
                        // fallback to the default roots. The PEM contents are
                        // parsed into the rustls root store at connect (fail-closed
                        // on an invalid/empty bundle there), consistent with how
                        // startup parameters are stored raw and validated at connect.
                        let bytes = std::fs::read(v)
                            .map_err(|e| format!("cannot read sslrootcert file {v}: {e}"))?;
                        ca_roots_pem = Some(Arc::from(bytes));
                    }
                    // An unrecognised key is a misconfiguration (e.g. a
                    // typo'd `sslmod=require` that would otherwise silently
                    // leave the default `prefer`, downgrading security).
                    // Reject it loudly, consistent with how an unknown
                    // sslmode VALUE is already rejected above.
                    other => return Err(format!("unknown DSN parameter: {other}")),
                }
            }
        }

        Ok(Self {
            host,
            port,
            user,
            database,
            password_inner: password.map(zeroize::Zeroizing::new),
            connect_timeout_secs: timeout,
            ssl_mode,
            startup_params: Vec::new(),
            ca_roots_pem,
        })
    }

    /// Construct from environment variables.
    ///
    /// Reads: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`,
    /// `PGSSLMODE`, `PGSSLROOTCERT`.
    ///
    /// A variable that is **absent** falls back to a documented default
    /// (host=localhost, port=5432, user=`$USER` then `postgres`,
    /// sslmode=prefer). A variable that is **present but malformed** is a real
    /// misconfiguration and is rejected with `Err` — a typo'd `PGPORT=abc` or
    /// `PGSSLMODE=requ1re` never silently degrades to a default that could send
    /// the connection to the wrong port or strip SSL.
    pub fn from_env() -> Result<Self, String> {
        use std::env::VarError;

        let host = match std::env::var("PGHOST") {
            Ok(h) => h,
            Err(VarError::NotPresent) => "localhost".to_string(),
            Err(VarError::NotUnicode(_)) => return Err("PGHOST is not valid UTF-8".to_string()),
        };

        let port = match std::env::var("PGPORT") {
            Ok(p) => p.parse::<u16>().map_err(|_| format!("invalid PGPORT: {p}"))?,
            Err(VarError::NotPresent) => 5432,
            Err(VarError::NotUnicode(_)) => return Err("PGPORT is not valid UTF-8".to_string()),
        };

        let user = match std::env::var("PGUSER") {
            Ok(u) => u,
            Err(VarError::NotPresent) => match std::env::var("USER") {
                Ok(u) => u,
                Err(VarError::NotPresent) => "postgres".to_string(),
                Err(VarError::NotUnicode(_)) => return Err("USER is not valid UTF-8".to_string()),
            },
            Err(VarError::NotUnicode(_)) => return Err("PGUSER is not valid UTF-8".to_string()),
        };

        let database = match std::env::var("PGDATABASE") {
            Ok(d) => Some(d),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => return Err("PGDATABASE is not valid UTF-8".to_string()),
        };

        let password = match std::env::var("PGPASSWORD") {
            Ok(p) => Some(p),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => return Err("PGPASSWORD is not valid UTF-8".to_string()),
        };

        let ssl_mode = match std::env::var("PGSSLMODE") {
            Ok(v) => match v.as_str() {
                "disable" => SslMode::Disable,
                "prefer" => SslMode::Prefer,
                "require" => SslMode::Require,
                // A typo here would otherwise silently fall to `prefer`, which
                // permits an SSL-stripping downgrade. Reject it loudly.
                other => return Err(format!("unknown PGSSLMODE: {other}")),
            },
            Err(VarError::NotPresent) => SslMode::Prefer,
            Err(VarError::NotUnicode(_)) => return Err("PGSSLMODE is not valid UTF-8".to_string()),
        };

        // `PGSSLROOTCERT` names a CA-bundle file; read it now (a present-but-
        // unreadable path is a loud error carrying the path, never a silent
        // fallback to the default roots). Absent → the default trust anchors.
        let ca_roots_pem = match std::env::var("PGSSLROOTCERT") {
            Ok(path) => {
                let bytes = std::fs::read(&path)
                    .map_err(|e| format!("cannot read PGSSLROOTCERT file {path}: {e}"))?;
                Some(Arc::from(bytes))
            }
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => {
                return Err("PGSSLROOTCERT is not valid UTF-8".to_string())
            }
        };

        Ok(Self {
            host,
            port,
            user,
            database,
            password_inner: password.map(zeroize::Zeroizing::new),
            connect_timeout_secs: 10,
            ssl_mode,
            startup_params: Vec::new(),
            ca_roots_pem,
        })
    }

    /// Construct with required fields. Port defaults to 5432.
    pub fn new(host: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 5432,
            user: user.into(),
            database: None,
            password_inner: None,
            connect_timeout_secs: 10,
            ssl_mode: SslMode::default(),
            startup_params: Vec::new(),
            ca_roots_pem: None,
        }
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the database.
    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    /// Set SSL mode.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Set connection timeout in seconds.
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    /// Set the password. Default auth is SCRAM-SHA-256.
    /// Server chooses the actual method — SCRAM works for both
    /// `scram-sha-256` and `md5` pg_hba rules (PG falls back).
    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password_inner = Some(zeroize::Zeroizing::new(pw.into()));
        self
    }

    /// Add a PostgreSQL startup parameter (GUC) sent in the `StartupMessage` —
    /// e.g. `("statement_timeout", "5000")`, `("timezone", "UTC")`.
    ///
    /// The pair is stored raw and validated at connect: a name or value
    /// containing a NUL byte (unrepresentable in the NUL-delimited wire frame),
    /// an over-length name/value, or a reserved name (`user`, `database`,
    /// `client_encoding`, `replication`, `options` — managed by the connection)
    /// is a classified [`DriverError::Config`](crate::DriverError) at connect,
    /// never a corrupt packet. Names are case-insensitive to PostgreSQL's GUC
    /// folding, so the reserved check cannot be bypassed with a different case.
    ///
    /// Chainable and order-preserving; setting the same parameter twice sends
    /// it twice (PostgreSQL applies the last).
    #[must_use]
    pub fn with_startup_param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.startup_params.push((name.into(), value.into()));
        self
    }

    /// Set the session `search_path` on connect (convenience over
    /// [`with_startup_param`](Self::with_startup_param)`("search_path", …)`).
    ///
    /// A connect-time `search_path` is the schema-isolation primitive: it
    /// travels in the `StartupMessage`, so it is in effect before the first
    /// query, and it becomes the session's reset value — surviving a pooled
    /// connection's `RESET ALL` on checkout, so a pooled connection cannot
    /// silently escape its schema.
    #[must_use]
    pub fn with_search_path(self, search_path: impl Into<String>) -> Self {
        self.with_startup_param("search_path", search_path)
    }

    /// Set the session `application_name` on connect (convenience over
    /// [`with_startup_param`](Self::with_startup_param)`("application_name",
    /// …)`). Surfaces in `pg_stat_activity` and server logs.
    #[must_use]
    pub fn with_application_name(self, application_name: impl Into<String>) -> Self {
        self.with_startup_param("application_name", application_name)
    }

    /// Borrow the raw, not-yet-validated startup parameters, in insertion
    /// order. The drivers validate each into a wire `StartupParam` at connect.
    #[must_use]
    pub fn startup_params(&self) -> &[(String, String)] {
        &self.startup_params
    }

    /// Verify the server certificate against a CUSTOM set of CA roots supplied
    /// as PEM, instead of the default (baked Mozilla) trust anchors.
    ///
    /// This is the internal/private-CA path: a fleet whose PostgreSQL servers
    /// present certificates signed by an in-house CA can now use
    /// [`SslMode::Require`] (verified TLS) against them, instead of being forced
    /// to [`SslMode::Disable`] (plaintext) because the baked public roots cannot
    /// validate an internal CA. The supplied roots REPLACE the default anchors
    /// (they do not extend them), matching libpq's `sslrootcert`: the server is
    /// verified against precisely this CA, not this CA plus every public root.
    ///
    /// The PEM is stored raw and parsed into a `rustls` root store at connect. An
    /// invalid or empty PEM is a classified
    /// [`DriverError::Config`](crate::DriverError) at connect — fail-closed,
    /// never a silent fallback to the default roots or to plaintext.
    ///
    /// Chainable; a later call replaces an earlier one (the last CA source wins).
    #[must_use]
    pub fn with_ca_roots(mut self, pem: &[u8]) -> Self {
        self.ca_roots_pem = Some(Arc::from(pem));
        self
    }

    /// Borrow the raw, not-yet-parsed custom CA-roots PEM, or `None` when the
    /// default trust anchors are in force. The drivers parse it into a `rustls`
    /// root store at connect (fail-closed on an invalid/empty bundle).
    #[must_use]
    pub fn ca_roots_pem(&self) -> Option<&[u8]> {
        self.ca_roots_pem.as_deref()
    }
}

/// Validate a config's raw startup parameters into wire
/// [`StartupParam`](bsql_postgres_proto::StartupParam)s.
///
/// Each `(name, value)` is checked for NUL, over-length, and reserved-name
/// violations (via the wire-authority constructor); the first failure surfaces
/// as a classified [`DriverError::Config`](crate::DriverError). Both drivers
/// call this at connect, BEFORE any startup byte is assembled — so a rejected
/// parameter can never reach the wire, and the `StartupMessage` can never carry
/// a NUL or override a reserved parameter.
///
/// The wire `StartupParam` carries the precise cause internally
/// (`StartupParamError`); it is collapsed to a `&'static str` here because
/// [`DriverError::Config`](crate::DriverError) is the crate's established
/// pre-connect-validation shape and does not interpolate a dynamic value.
pub fn validate_startup_params(
    config: &ConnectConfig,
) -> Result<Vec<bsql_postgres_proto::StartupParam>, crate::DriverError> {
    use bsql_postgres_proto::{StartupParam, StartupParamError};
    config
        .startup_params()
        .iter()
        .map(|(name, value)| {
            StartupParam::new(name, value).map_err(|err| {
                crate::DriverError::Config(match err {
                    StartupParamError::Name(_) => "invalid startup parameter name",
                    StartupParamError::Value(_) => "invalid startup parameter value",
                    StartupParamError::Reserved => {
                        "reserved startup parameter name — user, database, \
                         client_encoding, replication, and options are managed \
                         by the connection"
                    }
                    // `StartupParamError` is `#[non_exhaustive]`: a future
                    // rejection class must not silently pass as valid.
                    _ => "invalid startup parameter",
                })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_rejects_unknown_parameter() {
        // A typo'd parameter (e.g. `sslmod` for `sslmode`) must be rejected, not
        // silently ignored — silently ignoring it would leave the default mode.
        let err = ConnectConfig::from_dsn("postgres://u@h?sslmod=require");
        assert!(err.is_err(), "unknown DSN parameter must be rejected");
    }

    #[test]
    fn dsn_accepts_known_parameters() {
        // `expect`/`unwrap` are crate-denied even in tests; match instead.
        let cfg = match ConnectConfig::from_dsn(
            "postgres://u@h:5433/db?sslmode=require&connect_timeout=3",
        ) {
            Ok(c) => c,
            Err(e) => panic!("valid DSN must parse: {e}"),
        };
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.ssl_mode, SslMode::Require);
        assert_eq!(cfg.connect_timeout_secs, 3);
    }

    #[test]
    fn dsn_rejects_unknown_sslmode_value() {
        assert!(ConnectConfig::from_dsn("postgres://u@h?sslmode=verify-full").is_err());
    }

    #[test]
    fn dsn_rejects_valueless_parameter() {
        // `?sslmode` with no value would silently keep the default `prefer`,
        // downgrading SSL. A parameter missing `=` must be rejected loudly.
        assert!(ConnectConfig::from_dsn("postgres://u@h?sslmode").is_err());
    }

    #[test]
    fn startup_param_builders_preserve_order() {
        let cfg = ConnectConfig::new("localhost", "u")
            .with_search_path("myschema")
            .with_startup_param("statement_timeout", "5000")
            .with_application_name("bsql_test");
        assert_eq!(
            cfg.startup_params(),
            &[
                ("search_path".to_string(), "myschema".to_string()),
                ("statement_timeout".to_string(), "5000".to_string()),
                ("application_name".to_string(), "bsql_test".to_string()),
            ],
        );
    }

    #[test]
    fn a_fresh_config_has_no_startup_params() {
        assert!(ConnectConfig::new("localhost", "u").startup_params().is_empty());
    }

    #[test]
    fn a_fresh_config_has_no_ca_roots() {
        assert!(ConnectConfig::new("localhost", "u").ca_roots_pem().is_none());
    }

    #[test]
    fn with_ca_roots_stores_the_raw_pem() {
        const PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let cfg = ConnectConfig::new("localhost", "u").with_ca_roots(PEM);
        assert_eq!(cfg.ca_roots_pem(), Some(PEM), "the raw PEM must round-trip");
    }

    #[test]
    fn dsn_sslrootcert_missing_file_is_a_loud_error() {
        // A present-but-unreadable `sslrootcert` path is a loud error carrying
        // the path — never a silent fallback to the default roots.
        let err =
            ConnectConfig::from_dsn("postgres://u@h?sslrootcert=/no/such/bsql/test/ca.pem");
        match err {
            Err(msg) => assert!(
                msg.contains("sslrootcert"),
                "the error must name the failing key, got {msg:?}",
            ),
            Ok(_) => panic!("an unreadable sslrootcert file must not parse silently"),
        }
    }

    #[test]
    fn dsn_sslrootcert_reads_the_file_into_ca_roots() {
        // The DSN reads the file NOW; the PEM is validated later (at connect).
        const PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let path =
            std::env::temp_dir().join(format!("bsql_w2_ca_{}.pem", std::process::id()));
        if std::fs::write(&path, PEM).is_err() {
            panic!("temp-file write failed; cannot exercise sslrootcert read");
        }
        let dsn = format!("postgres://u@h?sslrootcert={}", path.display());
        let cfg = match ConnectConfig::from_dsn(&dsn) {
            Ok(c) => c,
            Err(e) => panic!("a DSN with a readable sslrootcert must parse: {e}"),
        };
        // Best-effort cleanup before asserting (a leftover temp file is harmless).
        drop(std::fs::remove_file(&path));
        assert_eq!(cfg.ca_roots_pem(), Some(PEM), "the file bytes must be stored");
    }

    #[test]
    fn validate_accepts_ordinary_params() {
        let cfg = ConnectConfig::new("localhost", "u")
            .with_search_path("s")
            .with_startup_param("statement_timeout", "5000");
        let wire = match validate_startup_params(&cfg) {
            Ok(w) => w,
            Err(e) => panic!("ordinary params must validate: {e}"),
        };
        assert_eq!(wire.len(), 2);
    }

    #[test]
    fn validate_rejects_nul_in_value_with_classified_error() {
        // A NUL is unrepresentable in the NUL-delimited StartupMessage; it must
        // be a classified pre-connect error, never a corrupt packet.
        let cfg = ConnectConfig::new("localhost", "u").with_startup_param("search_path", "a\0b");
        match validate_startup_params(&cfg) {
            Err(crate::DriverError::Config(msg)) => {
                assert_eq!(msg, "invalid startup parameter value");
            }
            other => panic!("a NUL value must be a classified Config error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_nul_in_name_with_classified_error() {
        let cfg = ConnectConfig::new("localhost", "u").with_startup_param("a\0b", "x");
        match validate_startup_params(&cfg) {
            Err(crate::DriverError::Config(msg)) => {
                assert_eq!(msg, "invalid startup parameter name");
            }
            other => panic!("a NUL name must be a classified Config error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_reserved_params() {
        // A consumer must not be able to override session identity or the
        // pinned client_encoding via a startup parameter.
        for reserved in ["user", "database", "client_encoding", "replication", "options"] {
            let cfg = ConnectConfig::new("localhost", "u").with_startup_param(reserved, "x");
            match validate_startup_params(&cfg) {
                Err(crate::DriverError::Config(msg)) => assert_eq!(
                    msg,
                    "reserved startup parameter name — user, database, \
                     client_encoding, replication, and options are managed \
                     by the connection",
                ),
                other => panic!("'{reserved}' must be rejected as reserved, got {other:?}"),
            }
        }
    }

    #[test]
    fn validate_reserved_check_is_case_insensitive() {
        // `Client_Encoding=LATIN1` must not slip past and break the UTF-8 pin.
        let cfg =
            ConnectConfig::new("localhost", "u").with_startup_param("Client_Encoding", "LATIN1");
        assert!(matches!(
            validate_startup_params(&cfg),
            Err(crate::DriverError::Config(_)),
        ));
    }
}
