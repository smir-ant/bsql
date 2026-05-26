#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct ConnectConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: Option<String>,
    pub ssl_mode: SslMode,
    pub connect_timeout_secs: u64,
    password_inner: Option<String>,
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
    pub fn new(host: &str, user: &str) -> Self {
        Self {
            host: host.to_string(),
            port: 5432,
            user: user.to_string(),
            password_inner: None,
            database: None,
            ssl_mode: SslMode::Prefer,
            connect_timeout_secs: 10,
        }
    }

    pub fn password_str(&self) -> Option<&str> {
        self.password_inner.as_deref()
    }

    pub fn port(mut self, port: u16) -> Self { self.port = port; self }
    pub fn password(mut self, pw: String) -> Self { self.password_inner = Some(pw); self }
    pub fn database(mut self, db: String) -> Self { self.database = Some(db); self }
    pub fn ssl_mode(mut self, mode: SslMode) -> Self { self.ssl_mode = mode; self }
}
