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
}

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
        })
    }

    /// Construct from environment variables.
    ///
    /// Reads: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSSLMODE`.
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

        Ok(Self {
            host,
            port,
            user,
            database,
            password_inner: password.map(zeroize::Zeroizing::new),
            connect_timeout_secs: 10,
            ssl_mode,
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
}
