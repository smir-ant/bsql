/// Connection configuration.
#[derive(Debug, Clone)]
pub struct ConnectConfig {
    /// PostgreSQL server hostname.
    pub host: String,
    /// PostgreSQL server port (default 5432).
    pub port: u16,
    /// PostgreSQL user name.
    pub user: String,
    /// Database name (defaults to user if absent).
    pub database: Option<String>,
    /// Password for SCRAM/MD5/Cleartext auth. None = Trust.
    pub password: Option<String>,
    /// Connection timeout in seconds. Default: 10.
    pub connect_timeout_secs: u64,
}

impl ConnectConfig {
    /// Construct with required fields. Port defaults to 5432.
    pub fn new(host: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 5432,
            user: user.into(),
            database: None,
            password: None,
            connect_timeout_secs: 10,
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

    /// Set connection timeout in seconds.
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    /// Set the password. Default auth is SCRAM-SHA-256.
    /// Server chooses the actual method — SCRAM works for both
    /// `scram-sha-256` and `md5` pg_hba rules (PG falls back).
    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password = Some(pw.into());
        self
    }
}
