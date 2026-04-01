//! Connection pool with fail-fast semantics and PgBouncer detection.
//!
//! The pool wraps `deadpool-postgres` with two key behaviors:
//! - **Fail-fast**: `acquire()` returns `PoolExhausted` immediately when no
//!   connections are available. It does not wait. See CREDO principle #17.
//! - **PgBouncer detection**: on pool creation, bsql detects whether the
//!   connection goes through PgBouncer and adjusts prepared statement strategy.

use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use tokio_postgres::types::ToSql;

use crate::error::{ConnectError, BsqlError, BsqlResult};
use crate::stream::QueryStream;
use crate::transaction::Transaction;

/// A PostgreSQL connection pool.
///
/// Wraps `deadpool-postgres` with fail-fast acquire semantics.
/// All connections are returned to the pool when `PoolConnection` is dropped.
pub struct Pool {
    inner: deadpool_postgres::Pool,
    pgbouncer: PgBouncerInfo,
}

/// PgBouncer detection result.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PgBouncerInfo {
    /// True if PgBouncer was detected between the client and PostgreSQL.
    detected: bool,
    /// True if PgBouncer supports server-side prepared statement tracking
    /// (PgBouncer 1.21+ with `prepared_statements=yes`).
    supports_named_stmts: bool,
}

impl PgBouncerInfo {
    const DIRECT: Self = Self {
        detected: false,
        supports_named_stmts: true,
    };
}

/// Builder for configuring a connection pool.
pub struct PoolBuilder {
    host: Option<String>,
    port: Option<u16>,
    dbname: Option<String>,
    user: Option<String>,
    password: Option<String>,
    max_size: usize,
    connect_timeout_secs: u64,
}

impl PoolBuilder {
    pub fn host(mut self, host: &str) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn dbname(mut self, dbname: &str) -> Self {
        self.dbname = Some(dbname.into());
        self
    }

    pub fn user(mut self, user: &str) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// TCP connect timeout in seconds. This is the ONLY timeout in bsql —
    /// it exists because TCP itself will wait forever on a dead network.
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    pub async fn build(self) -> BsqlResult<Pool> {
        let mut cfg = Config::new();
        cfg.host = self.host;
        cfg.port = self.port;
        cfg.dbname = self.dbname;
        cfg.user = self.user;
        cfg.password = self.password;
        cfg.connect_timeout = Some(std::time::Duration::from_secs(self.connect_timeout_secs));
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        // FIX 2: fail-fast — zero wait timeout means acquire() never blocks
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: self.max_size,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(std::time::Duration::ZERO),
                create: None,
                recycle: None,
            },
            ..Default::default()
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| ConnectError::create(e.to_string()))?;

        // FIX 11: detect PgBouncer — propagate connection failure
        let pgbouncer = detect_pgbouncer(&pool).await?;

        Ok(Pool {
            inner: pool,
            pgbouncer,
        })
    }
}

impl Pool {
    /// Connect to PostgreSQL using a connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let config: tokio_postgres::Config = url
            .parse()
            .map_err(|e: tokio_postgres::Error| ConnectError::create(e.to_string()))?;

        let mut cfg = Config::new();
        cfg.host = config.get_hosts().first().map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => s.clone(),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().into_owned(),
        });
        cfg.port = config.get_ports().first().copied();
        cfg.dbname = config.get_dbname().map(String::from);
        cfg.user = config.get_user().map(String::from);
        cfg.password = config
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned());
        cfg.connect_timeout = Some(std::time::Duration::from_secs(5));
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        // FIX 2: fail-fast — zero wait timeout means acquire() never blocks
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: 16,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(std::time::Duration::ZERO),
                create: None,
                recycle: None,
            },
            ..Default::default()
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| ConnectError::create(e.to_string()))?;

        // FIX 11: detect PgBouncer — propagate connection failure
        let pgbouncer = detect_pgbouncer(&pool).await?;

        Ok(Pool {
            inner: pool,
            pgbouncer,
        })
    }

    /// Create a pool builder for fine-grained configuration.
    pub fn builder() -> PoolBuilder {
        PoolBuilder {
            host: None,
            port: None,
            dbname: None,
            user: None,
            password: None,
            max_size: 16,
            connect_timeout_secs: 5,
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available. Does not wait. Does not timeout. See CREDO principle #17.
    pub async fn acquire(&self) -> BsqlResult<PoolConnection> {
        let conn = self.inner.get().await.map_err(BsqlError::from)?;

        Ok(PoolConnection {
            inner: conn,
            pgbouncer: self.pgbouncer,
        })
    }

    /// Whether PgBouncer was detected between the client and PostgreSQL.
    pub fn is_pgbouncer(&self) -> bool {
        self.pgbouncer.detected
    }

    /// Whether named prepared statements can be used.
    ///
    /// False when PgBouncer is detected without `prepared_statements=yes`.
    pub fn supports_named_statements(&self) -> bool {
        self.pgbouncer.supports_named_stmts
    }

    /// Begin a new transaction.
    ///
    /// Acquires a connection from the pool and sends `BEGIN`. The connection
    /// is held for the lifetime of the returned [`Transaction`].
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available. See CREDO principle #17.
    pub async fn begin(&self) -> BsqlResult<Transaction> {
        let conn = self.acquire().await?;
        conn.inner
            .batch_execute("BEGIN")
            .await
            .map_err(BsqlError::from)?;
        Ok(Transaction::new(conn))
    }

    /// Execute a query and return a stream of rows.
    ///
    /// Acquires a connection from the pool and returns a [`QueryStream`]
    /// that holds the connection alive until the stream is consumed or
    /// dropped. Rows arrive one at a time, avoiding buffering the
    /// entire result set in memory.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available. See CREDO principle #17.
    ///
    /// This method is only available on `Pool` (not `PoolConnection` or
    /// `Transaction`) because the stream must own the connection for its
    /// entire lifetime.
    pub async fn query_stream(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> BsqlResult<QueryStream> {
        let conn = self.acquire().await?;
        let stmt = conn
            .inner
            .prepare_cached(sql)
            .await
            .map_err(BsqlError::from)?;

        let row_stream = conn
            .inner
            .query_raw(&stmt, params.iter().copied())
            .await
            .map_err(BsqlError::from)?;

        Ok(QueryStream::new(conn, row_stream))
    }

    /// Current pool status: available and total connections.
    pub fn status(&self) -> PoolStatus {
        let status = self.inner.status();
        PoolStatus {
            available: status.available,
            size: status.size,
            max_size: status.max_size,
        }
    }
}

/// A connection borrowed from the pool.
///
/// Returned to the pool when dropped.
pub struct PoolConnection {
    pub(crate) inner: deadpool_postgres::Object,
    pub(crate) pgbouncer: PgBouncerInfo,
}

impl PoolConnection {
    /// Whether named prepared statements can be used on this connection.
    pub fn supports_named_statements(&self) -> bool {
        self.pgbouncer.supports_named_stmts
    }
}

/// Snapshot of pool utilization.
#[derive(Debug, Clone, Copy)]
pub struct PoolStatus {
    pub available: usize,
    pub size: usize,
    pub max_size: usize,
}

/// Detect PgBouncer on the first connection from the pool.
///
/// Strategy: try `SHOW POOLS` — only PgBouncer responds to this.
/// If PgBouncer is detected, check `SHOW CONFIG` for `prepared_statements`.
///
/// FIX 11: returns `Err` if the initial connection fails, instead of silently
/// returning `DIRECT`. A pool that can't connect on creation is broken.
async fn detect_pgbouncer(pool: &deadpool_postgres::Pool) -> BsqlResult<PgBouncerInfo> {
    let conn = pool.get().await.map_err(|e| {
        ConnectError::with_source(format!("failed to establish initial connection: {e}"), e)
    })?;

    // PgBouncer responds to `SHOW POOLS`; PostgreSQL does not.
    let is_pgbouncer = conn.simple_query("SHOW POOLS").await.is_ok();

    if !is_pgbouncer {
        return Ok(PgBouncerInfo::DIRECT);
    }

    // Check if PgBouncer supports named prepared statements (1.21+)
    let supports_named = match conn.simple_query("SHOW CONFIG").await {
        Ok(messages) => messages.iter().any(|msg| {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                row.get(0) == Some("prepared_statements") && row.get(1) == Some("yes")
            } else {
                false
            }
        }),
        Err(_) => false,
    };

    Ok(PgBouncerInfo {
        detected: true,
        supports_named_stmts: supports_named,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = Pool::builder();
        assert_eq!(b.max_size, 16);
        assert_eq!(b.connect_timeout_secs, 5);
    }

    #[test]
    fn builder_config() {
        let b = Pool::builder()
            .host("localhost")
            .port(5432)
            .dbname("test")
            .user("app")
            .password("secret")
            .max_size(8)
            .connect_timeout(10);

        assert_eq!(b.host.as_deref(), Some("localhost"));
        assert_eq!(b.port, Some(5432));
        assert_eq!(b.dbname.as_deref(), Some("test"));
        assert_eq!(b.user.as_deref(), Some("app"));
        assert_eq!(b.password.as_deref(), Some("secret"));
        assert_eq!(b.max_size, 8);
        assert_eq!(b.connect_timeout_secs, 10);
    }

    #[test]
    fn pgbouncer_direct_defaults() {
        let info = PgBouncerInfo::DIRECT;
        assert!(!info.detected);
        assert!(info.supports_named_stmts);
    }

    #[test]
    fn pool_status_type_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<PoolStatus>();
    }
}
