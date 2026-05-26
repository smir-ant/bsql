#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,
    pub ssl_mode: SslMode,
    pub connect_timeout_secs: u64,
}

impl ConnectConfig {
    pub fn new(host: &str, user: &str) -> Self {
        Self {
            host: host.to_string(),
            port: 5432,
            user: user.to_string(),
            password: None,
            database: None,
            ssl_mode: SslMode::Prefer,
            connect_timeout_secs: 10,
        }
    }

    pub fn port(mut self, port: u16) -> Self { self.port = port; self }
    pub fn password(mut self, pw: String) -> Self { self.password = Some(pw); self }
    pub fn database(mut self, db: String) -> Self { self.database = Some(db); self }
    pub fn ssl_mode(mut self, mode: SslMode) -> Self { self.ssl_mode = mode; self }
}
