//! Connection pool with fail-fast semantics, PgBouncer detection,
//! singleflight query coalescing, and read/write splitting.
//!
//! The pool wraps `deadpool-postgres` with key behaviors:
//! - **Fail-fast**: `acquire()` returns `PoolExhausted` immediately when no
//!   connections are available. It does not wait. See CREDO principle #17.
//! - **PgBouncer detection**: on pool creation, bsql detects whether the
//!   connection goes through PgBouncer and adjusts prepared statement strategy.
//! - **Singleflight** (v0.7): identical concurrent SELECT queries are coalesced
//!   into a single PG round-trip. The result is shared via `Arc<[Row]>`.
//! - **Read/write splitting** (v0.7): when replicas are configured, SELECT
//!   queries are routed to replicas. Writes always go to the primary.

use std::sync::Arc;

use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
#[cfg(not(feature = "tls"))]
use tokio_postgres::NoTls;
use tokio_postgres::types::ToSql;

use crate::error::{BsqlError, BsqlResult, ConnectError};
use crate::singleflight::{FlightStatus, Singleflight, sql_key};
use crate::stream::QueryStream;
use crate::transaction::Transaction;

/// A PostgreSQL connection pool.
///
/// Wraps `deadpool-postgres` with fail-fast acquire semantics, singleflight
/// query coalescing, and optional read/write splitting.
pub struct Pool {
    primary: deadpool_postgres::Pool,
    /// Replica pools for read-only queries. Round-robin selection.
    /// Empty when no replicas are configured.
    replicas: Vec<deadpool_postgres::Pool>,
    /// Atomic counter for round-robin replica selection.
    replica_idx: std::sync::atomic::AtomicUsize,
    /// True if PgBouncer was detected between the client and PostgreSQL.
    pgbouncer: bool,
    singleflight: Singleflight,
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
    replica_urls: Vec<String>,
}

impl PoolBuilder {
    /// Configure the pool from a PostgreSQL connection URL.
    ///
    /// Parses `postgres://user:password@host:port/dbname` and fills in
    /// host, port, dbname, user, and password. Other builder settings
    /// (max_size, connect_timeout, replicas) are preserved.
    ///
    /// Returns an error if the URL cannot be parsed.
    pub fn url(mut self, url: &str) -> Result<Self, BsqlError> {
        let parsed = parse_pg_url(url)?;
        self.host = parsed.host;
        self.port = parsed.port;
        self.dbname = parsed.dbname;
        self.user = parsed.user;
        self.password = parsed.password;
        Ok(self)
    }

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

    /// TCP connect timeout in seconds. This is the ONLY timeout in bsql --
    /// it exists because TCP itself will wait forever on a dead network.
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    /// Add a read replica. SELECT queries will be routed to replicas when
    /// the executor uses `query_raw_readonly` (generated for SELECT queries).
    ///
    /// Multiple replicas are selected round-robin. If a replica is unavailable,
    /// the query falls back to the primary.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub fn replica(mut self, url: &str) -> Self {
        self.replica_urls.push(url.into());
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
        // FIX 2: fail-fast -- zero wait timeout means acquire() never blocks
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: self.max_size,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(std::time::Duration::ZERO),
                create: None,
                recycle: None,
            },
            ..Default::default()
        });

        let pool = create_deadpool(cfg)?;

        // FIX 11: detect PgBouncer -- propagate connection failure
        let mut pgbouncer = detect_pgbouncer(&pool).await?;

        // Build replica pools, detecting PgBouncer on each.
        // If ANY pool (primary or replica) is behind PgBouncer, we disable
        // named prepared statements globally. This is conservative but safe:
        // a mixed topology is unusual, and the performance cost of unnamed
        // statements is negligible compared to a hard failure.
        let mut replicas = Vec::with_capacity(self.replica_urls.len());
        for url in &self.replica_urls {
            let replica_pool =
                create_pool_from_url(url, self.max_size, self.connect_timeout_secs).await?;
            pgbouncer |= detect_pgbouncer(&replica_pool).await?;
            replicas.push(replica_pool);
        }

        Ok(Pool {
            primary: pool,
            replicas,
            replica_idx: std::sync::atomic::AtomicUsize::new(0),
            pgbouncer,
            singleflight: Singleflight::new(),
        })
    }
}

impl Pool {
    /// Connect to PostgreSQL using a connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        Pool::builder().url(url)?.build().await
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
            replica_urls: Vec::new(),
        }
    }

    /// Acquire a connection from the primary pool.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available. Does not wait. Does not timeout. See CREDO principle #17.
    pub async fn acquire(&self) -> BsqlResult<PoolConnection> {
        let conn = self.primary.get().await.map_err(BsqlError::from)?;

        Ok(PoolConnection { inner: conn })
    }

    /// Whether PgBouncer was detected between the client and PostgreSQL.
    pub fn is_pgbouncer(&self) -> bool {
        self.pgbouncer
    }

    /// Whether read replicas are configured.
    pub fn has_replicas(&self) -> bool {
        !self.replicas.is_empty()
    }

    /// Begin a new transaction.
    ///
    /// Acquires a connection from the primary pool. `BEGIN` is sent lazily
    /// on the first query inside the transaction (see
    /// [`Transaction::ensure_begun`]). This eliminates one PG round-trip
    /// when the transaction is created.
    ///
    /// If the transaction is committed or dropped without executing any
    /// queries, no `BEGIN`/`COMMIT`/`ROLLBACK` is sent at all and the
    /// connection returns to the pool cleanly.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available. See CREDO principle #17.
    pub async fn begin(&self) -> BsqlResult<Transaction> {
        let conn = self.acquire().await?;
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

    /// Pre-prepare SQL statements on a connection from the pool.
    ///
    /// Acquires one connection and calls `prepare_cached` for each SQL
    /// string. This populates the connection's statement cache so the
    /// first real query on that connection pays zero PREPARE overhead.
    ///
    /// Call this once after creating the pool with the SQL strings your
    /// application uses. The easiest source is your `query!()` calls:
    ///
    /// ```rust,ignore
    /// pool.warmup(&[
    ///     "SELECT id, login FROM users WHERE id = $1",
    ///     "UPDATE tickets SET status = $1 WHERE id = $2",
    /// ]).await?;
    /// ```
    ///
    /// **Best-effort**: if the pool is exhausted or a PREPARE fails
    /// (e.g. table was dropped), the error is returned but partial
    /// warmup of earlier statements still takes effect.
    ///
    /// Calling warmup with an empty slice is a no-op.
    pub async fn warmup(&self, sqls: &[&str]) -> BsqlResult<()> {
        if sqls.is_empty() {
            return Ok(());
        }
        let conn = self.acquire().await?;
        for sql in sqls {
            conn.inner
                .prepare_cached(sql)
                .await
                .map_err(BsqlError::from)?;
        }
        Ok(())
    }

    /// Current pool status: available and total connections.
    pub fn status(&self) -> PoolStatus {
        let status = self.primary.status();
        PoolStatus {
            available: status.available,
            size: status.size,
            max_size: status.max_size,
        }
    }

    // -- Internal singleflight + routing methods --

    /// Execute a query on the primary with singleflight coalescing.
    pub(crate) async fn query_raw_primary(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> BsqlResult<Arc<[tokio_postgres::Row]>> {
        // Singleflight ONLY for parameterless queries.
        // Parameterized queries with different param values have the same SQL
        // text, so keying by SQL alone would return wrong results.
        if params.is_empty() {
            let key = sql_key(sql);
            self.query_with_singleflight(key, sql, params, false).await
        } else {
            self.execute_on_pool(sql, params, false).await
        }
    }

    /// Execute a read-only query. Routes to a replica if available,
    /// falls back to primary. Singleflight only for parameterless queries.
    pub(crate) async fn query_raw_read(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> BsqlResult<Arc<[tokio_postgres::Row]>> {
        if self.replicas.is_empty() {
            return self.query_raw_primary(sql, params).await;
        }

        if params.is_empty() {
            let key = sql_key(sql);
            // Try replica with singleflight
            match self.query_with_singleflight(key, sql, params, true).await {
                Ok(rows) => Ok(rows),
                Err(_) => self.query_with_singleflight(key, sql, params, false).await,
            }
        } else {
            // Parameterized — no singleflight, try replica with fallback
            match self.execute_on_pool(sql, params, true).await {
                Ok(rows) => Ok(rows),
                Err(_) => self.execute_on_pool(sql, params, false).await,
            }
        }
    }

    /// Core singleflight execution. Acquires from primary or replica pool.
    async fn query_with_singleflight(
        &self,
        key: u64,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        use_replica: bool,
    ) -> BsqlResult<Arc<[tokio_postgres::Row]>> {
        match self.singleflight.try_join(key) {
            FlightStatus::Follower(mut rx) => {
                // Wait for the leader to complete
                match rx.recv().await {
                    Ok(rows) => Ok(rows),
                    Err(_) => {
                        // Leader failed or channel closed -- execute ourselves
                        self.execute_on_pool(sql, params, use_replica).await
                    }
                }
            }
            FlightStatus::Leader => match self.execute_on_pool(sql, params, use_replica).await {
                Ok(rows) => {
                    self.singleflight.complete(key, Arc::clone(&rows));
                    Ok(rows)
                }
                Err(e) => {
                    self.singleflight.abandon(key);
                    Err(e)
                }
            },
        }
    }

    /// Execute a query on the appropriate pool (primary or replica).
    async fn execute_on_pool(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        use_replica: bool,
    ) -> BsqlResult<Arc<[tokio_postgres::Row]>> {
        let raw_conn = if use_replica && !self.replicas.is_empty() {
            let idx = self
                .replica_idx
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % self.replicas.len();
            self.replicas[idx].get().await.map_err(BsqlError::from)?
        } else {
            self.primary.get().await.map_err(BsqlError::from)?
        };

        let stmt = raw_conn
            .prepare_cached(sql)
            .await
            .map_err(BsqlError::from)?;

        let rows = raw_conn
            .query(&stmt, params)
            .await
            .map_err(BsqlError::from)?;

        Ok(Arc::from(rows))
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("status", &self.status())
            .field("is_pgbouncer", &self.pgbouncer)
            .field("replicas", &self.replicas.len())
            .finish()
    }
}

/// A connection borrowed from the pool.
///
/// Returned to the pool when dropped.
pub struct PoolConnection {
    pub(crate) inner: deadpool_postgres::Object,
}

/// Snapshot of pool utilization.
#[derive(Debug, Clone, Copy)]
pub struct PoolStatus {
    pub available: usize,
    pub size: usize,
    pub max_size: usize,
}

/// Parsed fields from a PostgreSQL connection URL.
struct ParsedUrl {
    host: Option<String>,
    port: Option<u16>,
    dbname: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

/// Parse a PostgreSQL connection URL into its component fields.
fn parse_pg_url(url: &str) -> BsqlResult<ParsedUrl> {
    let config: tokio_postgres::Config = url
        .parse()
        .map_err(|e: tokio_postgres::Error| ConnectError::create(e.to_string()))?;

    let host = config.get_hosts().first().map(|h| match h {
        tokio_postgres::config::Host::Tcp(s) => s.clone(),
        #[cfg(unix)]
        tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().into_owned(),
    });
    let port = config.get_ports().first().copied();
    let dbname = config.get_dbname().map(String::from);
    let user = config.get_user().map(String::from);
    let password = match config.get_password() {
        Some(p) => Some(
            String::from_utf8(p.to_vec())
                .map_err(|_| ConnectError::create("database password contains invalid UTF-8"))?,
        ),
        None => None,
    };
    Ok(ParsedUrl {
        host,
        port,
        dbname,
        user,
        password,
    })
}

/// Create a deadpool-postgres pool from a connection URL.
///
/// Used internally for both primary and replica pools.
async fn create_pool_from_url(
    url: &str,
    max_size: usize,
    connect_timeout_secs: u64,
) -> BsqlResult<deadpool_postgres::Pool> {
    let parsed = parse_pg_url(url)?;

    let mut cfg = Config::new();
    cfg.host = parsed.host;
    cfg.port = parsed.port;
    cfg.dbname = parsed.dbname;
    cfg.user = parsed.user;
    cfg.password = parsed.password;
    cfg.connect_timeout = Some(std::time::Duration::from_secs(connect_timeout_secs));
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    cfg.pool = Some(deadpool_postgres::PoolConfig {
        max_size,
        timeouts: deadpool_postgres::Timeouts {
            wait: Some(std::time::Duration::ZERO),
            create: None,
            recycle: None,
        },
        ..Default::default()
    });

    let pool = create_deadpool(cfg)?;

    // Verify connectivity
    let _conn = pool
        .get()
        .await
        .map_err(|e| ConnectError::with_source(format!("failed to connect to replica: {e}"), e))?;

    Ok(pool)
}

/// Create a `deadpool_postgres::Pool` from a `Config`.
///
/// When the `tls` feature is enabled, connections use rustls with Mozilla's
/// bundled root certificates. When disabled, connections use `NoTls`.
fn create_deadpool(cfg: Config) -> BsqlResult<deadpool_postgres::Pool> {
    #[cfg(feature = "tls")]
    {
        let tls = make_rustls_connect();
        cfg.create_pool(Some(Runtime::Tokio1), tls)
            .map_err(|e| ConnectError::create(e.to_string()))
    }
    #[cfg(not(feature = "tls"))]
    {
        cfg.create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| ConnectError::create(e.to_string()))
    }
}

/// Build a `MakeRustlsConnect` using Mozilla's bundled root certificates.
///
/// This avoids a runtime dependency on the OS certificate store.
/// `pub(crate)` so `listener.rs` can reuse it.
#[cfg(feature = "tls")]
pub(crate) fn make_rustls_connect() -> tokio_postgres_rustls::MakeRustlsConnect {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_postgres_rustls::MakeRustlsConnect::new(config)
}

/// Detect PgBouncer on the first connection from the pool.
///
/// Strategy: try `SHOW POOLS` -- only PgBouncer responds to this.
///
/// Returns `Err` if the initial connection fails, instead of silently
/// assuming direct. A pool that can't connect on creation is broken.
async fn detect_pgbouncer(pool: &deadpool_postgres::Pool) -> BsqlResult<bool> {
    let conn = pool.get().await.map_err(|e| {
        ConnectError::with_source(format!("failed to establish initial connection: {e}"), e)
    })?;

    // PgBouncer responds to `SHOW POOLS`; PostgreSQL does not.
    Ok(conn.simple_query("SHOW POOLS").await.is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = Pool::builder();
        assert_eq!(b.max_size, 16);
        assert_eq!(b.connect_timeout_secs, 5);
        assert!(b.replica_urls.is_empty());
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
    fn builder_replicas() {
        let b = Pool::builder()
            .replica("postgres://replica1:5432/db")
            .replica("postgres://replica2:5432/db");
        assert_eq!(b.replica_urls.len(), 2);
    }
}
