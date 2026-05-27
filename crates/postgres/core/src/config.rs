/// SSL negotiation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum SslMode {
    /// No SSL. Plain TCP.
    Disable,
    /// Try SSL; fall back to plain TCP if server refuses.
    #[default]
    Prefer,
    /// Require SSL. Fail if server refuses.
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
                if let Some((k, v)) = param.split_once('=') {
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
                        _ => {}
                    }
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
    /// Falls back to defaults: host=localhost, port=5432, user=current OS user.
    pub fn from_env() -> Self {
        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("PGPORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432);
        let user = std::env::var("PGUSER")
            .unwrap_or_else(|_| std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()));
        let database = std::env::var("PGDATABASE").ok();
        let password = std::env::var("PGPASSWORD").ok();
        let ssl_mode = match std::env::var("PGSSLMODE").as_deref() {
            Ok("disable") => SslMode::Disable,
            Ok("require") => SslMode::Require,
            _ => SslMode::Prefer,
        };
        Self {
            host,
            port,
            user,
            database,
            password_inner: password.map(zeroize::Zeroizing::new),
            connect_timeout_secs: 10,
            ssl_mode,
        }
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
