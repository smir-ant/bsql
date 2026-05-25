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
}

impl ConnectConfig {
    /// Construct with required fields. Port defaults to 5432.
    pub fn new(host: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 5432,
            user: user.into(),
            database: None,
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
}
