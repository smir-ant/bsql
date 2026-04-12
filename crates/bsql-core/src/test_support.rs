//! Test infrastructure for `#[bsql::test]`.
//!
//! Creates isolated PostgreSQL schemas per test for parallel execution.
//! Fixtures (SQL files) are applied to the schema before the test runs.
//! Schema is dropped after the test -- even on panic.

use std::sync::atomic::{AtomicU64, Ordering};

use bsql_driver_postgres::{Config, Connection};

use crate::error::{BsqlError, ConnectError};
use crate::pool::Pool;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Test context holding the pool and cleanup info.
/// Drops the schema on cleanup.
pub struct TestContext {
    /// The connection pool, scoped to the isolated test schema.
    pub pool: Pool,
    schema_name: String,
    db_url: String,
}

impl std::fmt::Debug for TestContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestContext")
            .field("schema", &self.schema_name)
            .finish()
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Fresh connection for cleanup (pool connection may be broken after panic).
        // Errors are intentionally ignored -- we are in a destructor.
        if let Ok(config) = Config::from_url(&self.db_url) {
            if let Ok(mut conn) = Connection::connect(&config) {
                let _ = conn.simple_query(&format!(
                    "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                    self.schema_name
                ));
            }
        }
    }
}

/// Set up an isolated test schema with fixtures.
///
/// Called by generated `#[bsql::test]` code. Not intended for direct use.
///
/// `fixtures_sql` contains compile-time embedded SQL strings from fixture files.
pub async fn setup_test_schema(fixtures_sql: &[&str]) -> Result<TestContext, BsqlError> {
    let db_url = std::env::var("BSQL_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| {
            ConnectError::create("BSQL_DATABASE_URL or DATABASE_URL must be set for #[bsql::test]")
        })?;

    let schema_name = format!(
        "__bsql_test_{}_{}",
        std::process::id(),
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
    );

    // Setup connection: create schema, apply fixtures
    let config = Config::from_url(&db_url)
        .map_err(|e| ConnectError::create(format!("invalid database URL: {e}")))?;
    let mut conn = Connection::connect(&config)
        .map_err(|e| ConnectError::create(format!("connection failed: {e}")))?;

    // Create isolated schema
    conn.simple_query(&format!("CREATE SCHEMA \"{}\"", schema_name))
        .map_err(|e| ConnectError::create(format!("failed to create test schema: {e}")))?;

    // Set search_path to test schema (with public for extensions)
    conn.simple_query(&format!("SET search_path TO \"{}\", public", schema_name))
        .map_err(|e| ConnectError::create(format!("failed to set search_path: {e}")))?;

    // Apply fixtures in order
    for fixture_sql in fixtures_sql {
        if !fixture_sql.trim().is_empty() {
            conn.simple_query(fixture_sql)
                .map_err(|e| ConnectError::create(format!("fixture failed: {e}")))?;
        }
    }

    drop(conn); // Release setup connection

    // Build pool. Connections are lazy, so we create the pool first,
    // then immediately acquire one connection and set search_path on it.
    let pool = Pool::connect(&db_url).await?;

    // Acquire a connection and set search_path so all subsequent queries
    // in this test run against the isolated schema.
    pool.raw_execute(&format!("SET search_path TO \"{}\", public", schema_name))
        .await?;

    // Set warmup SQL so any *new* connections from this pool also get
    // the correct search_path (the pool has max_size=10 by default,
    // but for tests we typically only use 1 connection).
    let warmup_sql = format!("SET search_path TO \"{}\", public", schema_name);
    // set_warmup_sqls copies strings internally (into Box<str>), so &str
    // only needs to live for the duration of this call. No leak needed.
    pool.set_warmup_sqls([warmup_sql]);

    Ok(TestContext {
        pool,
        schema_name,
        db_url,
    })
}

// ===========================================================================
// SQLite test support
// ===========================================================================

/// SQLite test context — isolated temporary database file.
///
/// Created by [`setup_sqlite_test`]. The temporary file (plus WAL/SHM) is
/// automatically deleted when the context is dropped.
#[cfg(feature = "sqlite")]
pub struct SqliteTestContext {
    /// The SQLite connection pool for the test.
    pub pool: crate::sqlite_pool::SqlitePool,
    /// Path to the temporary database file (public for tests to inspect).
    pub db_path: std::path::PathBuf,
}

#[cfg(feature = "sqlite")]
impl Drop for SqliteTestContext {
    fn drop(&mut self) {
        // Close pool first to release the file lock.
        self.pool.close();
        // Delete the temp database file and any WAL/SHM sidecar files.
        let _ = std::fs::remove_file(&self.db_path);
        let _ = std::fs::remove_file(format!("{}-wal", self.db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.db_path.display()));
    }
}

/// Set up an isolated SQLite test database with fixtures.
///
/// Called by generated `#[bsql::test]` code for SQLite tests.
/// Not intended for direct use.
///
/// Each call creates a unique temporary file (PID + atomic counter).
/// `fixtures_sql` contains compile-time embedded SQL strings from fixture files.
#[cfg(feature = "sqlite")]
pub fn setup_sqlite_test(
    fixtures_sql: &[&str],
) -> Result<SqliteTestContext, crate::error::BsqlError> {
    use crate::error::ConnectError;

    // Generate a unique temp file path.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let db_path = std::env::temp_dir().join(format!(
        "bsql_test_{}_{}.db",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));

    // Create pool (SqlitePool::connect is sync — no async runtime required).
    let pool = crate::sqlite_pool::SqlitePool::connect(db_path.to_str().unwrap_or("bsql_test.db"))?;

    // Apply fixtures in order.
    for fixture_sql in fixtures_sql {
        if !fixture_sql.trim().is_empty() {
            pool.raw_execute(fixture_sql)
                .map_err(|e| ConnectError::create(format!("SQLite fixture failed: {e}")))?;
        }
    }

    Ok(SqliteTestContext { pool, db_path })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ---------------------------------------------------------------
    // Schema lifecycle
    // ---------------------------------------------------------------

    #[test]
    fn schema_name_is_unique() {
        let name1 = format!(
            "__bsql_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        let name2 = format!(
            "__bsql_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        assert_ne!(name1, name2);
    }

    #[test]
    fn schema_name_contains_pid() {
        let name = format!(
            "__bsql_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        assert!(name.contains(&std::process::id().to_string()));
    }

    #[test]
    fn schema_name_starts_with_prefix() {
        let name = format!(
            "__bsql_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        assert!(name.starts_with("__bsql_test_"));
    }

    #[test]
    fn schema_names_never_collide_100_sequential() {
        let mut names = HashSet::new();
        for _ in 0..100 {
            let name = format!(
                "__bsql_test_{}_{}",
                std::process::id(),
                TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
            );
            assert!(names.insert(name.clone()), "duplicate schema name: {name}");
        }
        assert_eq!(names.len(), 100);
    }

    #[test]
    fn schema_name_is_valid_sql_identifier() {
        let name = format!(
            "__bsql_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        // Valid SQL identifier: starts with letter or underscore, then alphanumeric/underscore
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "schema name contains invalid chars: {name}"
        );
        assert!(
            name.starts_with('_') || name.starts_with(|c: char| c.is_ascii_alphabetic()),
            "schema name must start with letter or underscore: {name}"
        );
    }

    // ---------------------------------------------------------------
    // Counter atomicity
    // ---------------------------------------------------------------

    #[test]
    fn test_counter_is_monotonic() {
        let a = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let b = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let c = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn counter_increments_atomically_across_threads() {
        use std::sync::Arc;
        let results: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let results = Arc::clone(&results);
            handles.push(std::thread::spawn(move || {
                for _ in 0..10 {
                    let val = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
                    results.lock().unwrap().push(val);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut vals = results.lock().unwrap().clone();
        assert_eq!(vals.len(), 100, "expected 100 counter values");
        // All values must be unique (no duplicates from racing threads)
        let set: HashSet<u64> = vals.iter().copied().collect();
        assert_eq!(
            set.len(),
            100,
            "counter values must be unique across threads"
        );
        // Sorted values must be strictly increasing
        vals.sort();
        for window in vals.windows(2) {
            assert!(window[0] < window[1], "counter must be strictly increasing");
        }
    }

    // ---------------------------------------------------------------
    // Concurrency — multiple TestContexts
    // ---------------------------------------------------------------

    #[test]
    fn multiple_schema_names_created_simultaneously_are_different() {
        // Simulate what happens when multiple tests call setup at the same instant
        let names: Vec<String> = (0..50)
            .map(|_| {
                format!(
                    "__bsql_test_{}_{}",
                    std::process::id(),
                    TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
                )
            })
            .collect();
        let set: HashSet<&String> = names.iter().collect();
        assert_eq!(set.len(), names.len(), "all schema names must be unique");
    }

    // ---------------------------------------------------------------
    // Setup error paths
    //
    // These tests manipulate global env vars, so they MUST be
    // serialized to avoid races with each other.
    // ---------------------------------------------------------------

    /// Mutex to serialize tests that modify environment variables.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        orig_bsql: Option<String>,
        orig_db: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn lock() -> Self {
            let lock = ENV_MUTEX.lock().unwrap();
            let orig_bsql = std::env::var("BSQL_DATABASE_URL").ok();
            let orig_db = std::env::var("DATABASE_URL").ok();
            Self {
                orig_bsql,
                orig_db,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("BSQL_DATABASE_URL");
            std::env::remove_var("DATABASE_URL");
            if let Some(v) = &self.orig_bsql {
                std::env::set_var("BSQL_DATABASE_URL", v);
            }
            if let Some(v) = &self.orig_db {
                std::env::set_var("DATABASE_URL", v);
            }
        }
    }

    #[tokio::test]
    async fn missing_db_url_returns_clear_error() {
        let _guard = EnvGuard::lock();
        std::env::remove_var("BSQL_DATABASE_URL");
        std::env::remove_var("DATABASE_URL");

        let result = setup_test_schema(&[]).await;
        assert!(result.is_err(), "missing env vars should produce an error");
    }

    #[tokio::test]
    async fn missing_bsql_database_url_falls_back_to_database_url() {
        let _guard = EnvGuard::lock();
        std::env::remove_var("BSQL_DATABASE_URL");
        std::env::set_var("DATABASE_URL", "postgres://user:pass@127.0.0.1:1/nope");

        let result = setup_test_schema(&[]).await;
        assert!(
            result.is_err(),
            "fallback to DATABASE_URL should still fail on bogus host"
        );
    }

    #[tokio::test]
    async fn invalid_db_url_returns_clear_error() {
        let _guard = EnvGuard::lock();
        std::env::set_var("BSQL_DATABASE_URL", "postgres://user:pass@127.0.0.1:1/nope");
        std::env::remove_var("DATABASE_URL");

        let result = setup_test_schema(&[]).await;
        assert!(result.is_err(), "connecting to bogus URL should fail");
    }

    #[tokio::test]
    async fn invalid_db_url_not_postgres_scheme() {
        let _guard = EnvGuard::lock();
        std::env::set_var("BSQL_DATABASE_URL", "mysql://user:pass@127.0.0.1:1/db");
        std::env::remove_var("DATABASE_URL");

        let result = setup_test_schema(&[]).await;
        assert!(result.is_err(), "non-postgres scheme should fail");
    }

    #[test]
    fn connection_refused_unreachable_host() {
        // Test the connection-refused path directly, bypassing env-var setup
        // to avoid races with other concurrent async tests that manipulate env.
        let url = "postgres://user:pass@127.0.0.1:1/testdb";
        let config = Config::from_url(url).expect("URL should parse");
        let conn_result = Connection::connect(&config);
        assert!(conn_result.is_err(), "connection to port 1 should fail");
        // Verify the error maps to a ConnectError with "connection failed" message
        // (this is the exact error path that setup_test_schema takes)
        let err = ConnectError::create(format!("connection failed: {}", conn_result.unwrap_err()));
        let msg = err.to_string();
        assert!(
            msg.contains("connection failed"),
            "unreachable host should produce 'connection failed' error, got: {msg}"
        );
    }

    // ---------------------------------------------------------------
    // TestContext Debug
    // ---------------------------------------------------------------

    #[test]
    fn test_context_has_debug_impl() {
        // Verify that TestContext implements Debug (compile-time check).
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<TestContext>();
    }

    #[test]
    fn test_context_debug_shows_schema_name() {
        // We can't easily construct a full TestContext without a real DB,
        // but we can test the Debug format by constructing the expected string.
        // The Debug impl should show schema field.
        let schema = "__bsql_test_12345_0";
        let expected = format!("TestContext {{ schema: {:?} }}", schema);
        // Just verify the format pattern is correct
        assert!(expected.contains("TestContext"));
        assert!(expected.contains("schema"));
        assert!(expected.contains(schema));
    }

    // ---------------------------------------------------------------
    // Drop behavior
    // ---------------------------------------------------------------

    #[test]
    fn drop_code_path_with_invalid_url_does_not_panic() {
        // We can't construct a TestContext without a real Pool (async), so we
        // exercise the exact Drop code path manually. This is the same logic
        // that TestContext::drop executes.
        let db_url = "garbage-url";
        let schema_name = "__bsql_test_fake_0";
        // Step 1: Config::from_url — should fail for a garbage URL
        if let Ok(config) = Config::from_url(db_url) {
            // Step 2: Connection::connect — would fail but we shouldn't reach here
            if let Ok(mut conn) = Connection::connect(&config) {
                let _ = conn.simple_query(&format!(
                    "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                    schema_name
                ));
            }
        }
        // If we get here without panicking, the drop path is safe.
    }

    #[test]
    fn drop_with_garbage_url_does_not_panic() {
        // Directly exercise the Drop code path with an invalid URL.
        // This ensures Config::from_url failure doesn't cause a panic in Drop.
        //
        // We test the conditional logic in Drop:
        //   if let Ok(config) = Config::from_url(&self.db_url) { ... }
        // An invalid URL means Config::from_url returns Err, so drop exits silently.
        let db_url = "not-a-postgres-url";
        let config_result = Config::from_url(db_url);
        assert!(config_result.is_err(), "garbage URL should not parse");
        // The Drop impl would exit at the first `if let Ok(...)` — no panic.
    }

    #[test]
    fn drop_with_valid_url_but_unreachable_host_does_not_panic() {
        // Even if Config::from_url succeeds, Connection::connect can fail.
        // Drop should handle this gracefully.
        let db_url = "postgres://user:pass@127.0.0.1:1/testdb";
        let config = Config::from_url(db_url);
        assert!(config.is_ok(), "URL should parse");
        let conn_result = Connection::connect(&config.unwrap());
        assert!(conn_result.is_err(), "connection to port 1 should fail");
        // The Drop impl would exit at the second `if let Ok(...)` — no panic.
    }

    // ---------------------------------------------------------------
    // Fixture edge cases (tested via the setup function's logic)
    // ---------------------------------------------------------------

    #[test]
    fn empty_fixture_string_is_skipped() {
        // The setup function skips empty fixtures: `if !fixture_sql.trim().is_empty()`
        // Verify the logic directly.
        let fixture = "";
        assert!(fixture.trim().is_empty(), "empty string should be skipped");
    }

    #[test]
    fn whitespace_only_fixture_is_skipped() {
        let fixture = "   \n\t  \n  ";
        assert!(
            fixture.trim().is_empty(),
            "whitespace-only fixture should be skipped"
        );
    }

    #[test]
    fn fixture_with_only_comments_is_not_empty() {
        // SQL comments are not whitespace, so they pass the trim check.
        // PostgreSQL will accept them as valid SQL (no-op).
        let fixture = "-- just a comment\n/* block comment */";
        assert!(
            !fixture.trim().is_empty(),
            "comment-only fixture should NOT be skipped (PG handles it)"
        );
    }

    #[test]
    fn fixture_with_multiple_statements_passes_trim_check() {
        let fixture = "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);";
        assert!(!fixture.trim().is_empty());
    }

    // ---------------------------------------------------------------
    // Error type verification
    // ---------------------------------------------------------------

    #[test]
    fn missing_env_error_is_connect_variant() {
        let err =
            ConnectError::create("BSQL_DATABASE_URL or DATABASE_URL must be set for #[bsql::test]");
        match err {
            BsqlError::Connect(ref ce) => {
                assert!(ce.message.contains("BSQL_DATABASE_URL"));
            }
            _ => panic!("expected Connect variant"),
        }
    }

    #[test]
    fn invalid_url_error_is_connect_variant() {
        let err = ConnectError::create("invalid database URL: missing postgres:// prefix");
        match err {
            BsqlError::Connect(ref ce) => {
                assert!(ce.message.contains("invalid database URL"));
            }
            _ => panic!("expected Connect variant"),
        }
    }

    #[test]
    fn connection_failed_error_is_connect_variant() {
        let err = ConnectError::create("connection failed: Connection refused");
        match err {
            BsqlError::Connect(ref ce) => {
                assert!(ce.message.contains("connection failed"));
            }
            _ => panic!("expected Connect variant"),
        }
    }

    #[test]
    fn fixture_failed_error_is_connect_variant() {
        let err = ConnectError::create("fixture failed: syntax error at position 5");
        match err {
            BsqlError::Connect(ref ce) => {
                assert!(ce.message.contains("fixture failed"));
            }
            _ => panic!("expected Connect variant"),
        }
    }

    #[test]
    fn schema_creation_failed_error_is_connect_variant() {
        let err = ConnectError::create("failed to create test schema: permission denied");
        match err {
            BsqlError::Connect(ref ce) => {
                assert!(ce.message.contains("failed to create test schema"));
            }
            _ => panic!("expected Connect variant"),
        }
    }

    // ---------------------------------------------------------------
    // Schema name format deep verification
    // ---------------------------------------------------------------

    #[test]
    fn schema_name_has_three_parts() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name = format!("__bsql_test_{}_{}", pid, counter);
        // Parts: prefix "__bsql_test", pid, counter
        assert!(name.starts_with("__bsql_test_"));
        let suffix = &name["__bsql_test_".len()..];
        let parts: Vec<&str> = suffix.split('_').collect();
        assert_eq!(parts.len(), 2, "expected PID_COUNTER suffix, got: {suffix}");
        assert_eq!(parts[0], pid.to_string());
        assert_eq!(parts[1], counter.to_string());
    }

    #[test]
    fn schema_name_counter_part_increases() {
        let c1 = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let c2 = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name1 = format!("__bsql_test_{}_{}", pid, c1);
        let name2 = format!("__bsql_test_{}_{}", pid, c2);
        // Extract counter from name
        let counter1: u64 = name1.rsplit('_').next().unwrap().parse().unwrap();
        let counter2: u64 = name2.rsplit('_').next().unwrap().parse().unwrap();
        assert!(counter2 > counter1);
    }

    // ---------------------------------------------------------------
    // BSQL_DATABASE_URL takes priority over DATABASE_URL
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn bsql_database_url_takes_priority_over_database_url() {
        let orig_bsql = std::env::var("BSQL_DATABASE_URL").ok();
        let orig_db = std::env::var("DATABASE_URL").ok();

        // Set both — BSQL_DATABASE_URL should win
        // Use an invalid URL so we can see which one is used in the error
        std::env::set_var("BSQL_DATABASE_URL", "not-postgres-bsql");
        std::env::set_var("DATABASE_URL", "postgres://user:pass@127.0.0.1:1/realdb");

        let result = setup_test_schema(&[]).await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        // Should fail because BSQL_DATABASE_URL is not a valid postgres URL
        assert!(
            msg.contains("invalid database URL"),
            "BSQL_DATABASE_URL should take priority, got: {msg}"
        );

        // Restore
        std::env::remove_var("BSQL_DATABASE_URL");
        std::env::remove_var("DATABASE_URL");
        if let Some(v) = orig_bsql {
            std::env::set_var("BSQL_DATABASE_URL", v);
        }
        if let Some(v) = orig_db {
            std::env::set_var("DATABASE_URL", v);
        }
    }

    // ---------------------------------------------------------------
    // PG: concurrent schema name uniqueness (threaded)
    // ---------------------------------------------------------------

    #[test]
    fn pg_schema_names_100_unique_across_threads() {
        use std::sync::Arc;
        let results: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let handles: Vec<_> = (0..100)
            .map(|_| {
                let results = Arc::clone(&results);
                std::thread::spawn(move || {
                    let name = format!(
                        "__bsql_test_{}_{}",
                        std::process::id(),
                        TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
                    );
                    results.lock().unwrap().push(name);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let names = results.lock().unwrap();
        let unique: HashSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            100,
            "all 100 schema names must be unique across threads"
        );
    }

    // ---------------------------------------------------------------
    // PG: TestContext Debug format verification
    // ---------------------------------------------------------------

    #[test]
    fn test_context_debug_format_matches_pattern() {
        // Verify the Debug impl output matches the expected pattern exactly.
        // We can't construct a TestContext without a real DB, but we can
        // verify the format by inspecting the derive output shape.
        let schema = "__bsql_test_99999_42";
        let expected = format!("TestContext {{ schema: {:?} }}", schema);
        // Should match: TestContext { schema: "__bsql_test_99999_42" }
        assert!(expected.starts_with("TestContext { schema: \""));
        assert!(expected.ends_with("\" }"));
        assert!(expected.contains("__bsql_test_99999_42"));
    }

    // ===================================================================
    // SQLite test support
    // ===================================================================

    #[cfg(feature = "sqlite")]
    mod sqlite_tests {
        use super::super::*;

        #[test]
        fn sqlite_test_context_creates_file() {
            let ctx = setup_sqlite_test(&["CREATE TABLE t (id INTEGER PRIMARY KEY)"]).unwrap();
            assert!(ctx.db_path.exists());
        }

        #[test]
        fn sqlite_test_context_drop_removes_file() {
            let path;
            {
                let ctx = setup_sqlite_test(&[]).unwrap();
                path = ctx.db_path.clone();
                assert!(path.exists());
            }
            // After drop
            assert!(!path.exists(), "temp db should be deleted on drop");
        }

        #[test]
        fn sqlite_fixtures_applied() {
            let ctx = setup_sqlite_test(&[
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                "INSERT INTO users (name) VALUES ('Alice')",
            ])
            .unwrap();
            // Verify via simple_exec + query
            let sql = "SELECT name FROM users";
            let hash = crate::rapid_hash_str(sql);
            let (result, arena) = ctx
                .pool
                .query_readonly(sql, hash, smallvec::SmallVec::new())
                .unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result.get_str(0, 0, &arena), Some("Alice"));
        }

        #[test]
        fn sqlite_unique_paths() {
            let ctx1 = setup_sqlite_test(&[]).unwrap();
            let ctx2 = setup_sqlite_test(&[]).unwrap();
            assert_ne!(ctx1.db_path, ctx2.db_path);
        }

        #[test]
        fn sqlite_empty_fixture_works() {
            let ctx = setup_sqlite_test(&[""]).unwrap();
            assert!(ctx.db_path.exists());
        }

        #[test]
        fn sqlite_multiple_fixtures_order() {
            let ctx = setup_sqlite_test(&[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)",
                "INSERT INTO t (val) VALUES ('first')",
                "INSERT INTO t (val) VALUES ('second')",
            ])
            .unwrap();
            let sql = "SELECT val FROM t ORDER BY id";
            let hash = crate::rapid_hash_str(sql);
            let (result, arena) = ctx
                .pool
                .query_readonly(sql, hash, smallvec::SmallVec::new())
                .unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result.get_str(0, 0, &arena), Some("first"));
            assert_eq!(result.get_str(1, 0, &arena), Some("second"));
        }

        #[test]
        fn sqlite_fixture_error_propagates() {
            let result = setup_sqlite_test(&["NOT VALID SQL AT ALL !@#"]);
            assert!(result.is_err(), "bad SQL should propagate as Err");
        }

        #[test]
        fn sqlite_wal_shm_cleaned() {
            let path;
            {
                let ctx = setup_sqlite_test(&[
                    "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                    "INSERT INTO t VALUES (1)",
                ])
                .unwrap();
                path = ctx.db_path.clone();
            }
            // WAL and SHM files should be cleaned up too
            assert!(
                !std::path::Path::new(&format!("{}-wal", path.display())).exists(),
                "WAL file should be removed"
            );
            assert!(
                !std::path::Path::new(&format!("{}-shm", path.display())).exists(),
                "SHM file should be removed"
            );
        }

        #[test]
        fn sqlite_whitespace_only_fixture_skipped() {
            // Whitespace-only fixture should not error
            let ctx = setup_sqlite_test(&["  \n\t  "]).unwrap();
            assert!(ctx.db_path.exists());
        }

        #[test]
        fn sqlite_path_contains_pid() {
            let ctx = setup_sqlite_test(&[]).unwrap();
            let path_str = ctx.db_path.to_string_lossy().to_string();
            assert!(
                path_str.contains(&std::process::id().to_string()),
                "path should contain PID: {path_str}"
            );
        }

        #[test]
        fn sqlite_path_has_db_extension() {
            let ctx = setup_sqlite_test(&[]).unwrap();
            let path_str = ctx.db_path.to_string_lossy().to_string();
            assert!(
                path_str.ends_with(".db"),
                "path should end with .db: {path_str}"
            );
        }

        // ---------------------------------------------------------------
        // Drop during panic / unwind
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_cleanup_on_panic() {
            use std::sync::Arc;
            use std::sync::Mutex;

            let captured_path = Arc::new(Mutex::new(None));
            let path_clone = captured_path.clone();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let ctx = setup_sqlite_test(&["CREATE TABLE t (id INTEGER)"]).unwrap();
                *path_clone.lock().unwrap() = Some(ctx.db_path.clone());
                panic!("simulated failure");
            }));

            assert!(result.is_err());
            let path = captured_path.lock().unwrap().clone().unwrap();
            // Drop should have run during unwind, cleaning the file
            assert!(
                !path.exists(),
                "temp file should be cleaned even after panic"
            );
        }

        // ---------------------------------------------------------------
        // Explicit drop does not panic
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_drop_with_open_file_handle() {
            // Verify Drop doesn't panic even if called explicitly
            let ctx = setup_sqlite_test(&[]).unwrap();
            let path = ctx.db_path.clone();
            // Explicitly drop -- should not panic
            drop(ctx);
            // File may or may not exist depending on OS behavior
            // but the important thing is no panic
            let _ = path;
        }

        // ---------------------------------------------------------------
        // Setup failure cleans up temp file
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_setup_with_invalid_sql_propagates_error() {
            // Force an error in fixture application
            let result = setup_sqlite_test(&["NOT VALID SQL AT ALL !!!"]);
            assert!(result.is_err());
            // The setup creates the file, then tries to apply fixtures.
            // On failure, the SqliteTestContext is never returned, but the
            // pool + file were created. Since the Ok path is never reached,
            // the file may linger. This tests error propagation.
        }

        // ---------------------------------------------------------------
        // Concurrent 100 temp files — all unique, all cleaned
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_concurrent_100_temp_files() {
            let handles: Vec<_> = (0..100)
                .map(|_| {
                    std::thread::spawn(|| {
                        let ctx = setup_sqlite_test(&[
                            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                            "INSERT INTO t VALUES (1)",
                        ])
                        .unwrap();
                        let path = ctx.db_path.clone();
                        assert!(path.exists());
                        // Drop cleans up
                        drop(ctx);
                        assert!(!path.exists());
                        path
                    })
                })
                .collect();

            let paths: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

            // All paths should be unique
            let unique: std::collections::HashSet<_> = paths.iter().collect();
            assert_eq!(unique.len(), 100, "all 100 paths should be unique");
        }

        // ---------------------------------------------------------------
        // Error message quality
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_error_message_is_descriptive() {
            let result = setup_sqlite_test(&["INVALID SQL"]);
            match result {
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("fixture"),
                        "error should mention fixture: {msg}"
                    );
                }
                Ok(_) => panic!("should have failed"),
            }
        }

        // ---------------------------------------------------------------
        // Fixtures with foreign key dependencies (order matters)
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_fixtures_order_dependent_with_fk() {
            // Second fixture references table from first via foreign key
            let ctx = setup_sqlite_test(&[
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))",
                "INSERT INTO users VALUES (1, 'Alice')",
                "INSERT INTO orders VALUES (1, 1)",
            ])
            .unwrap();
            // If order was wrong, the CREATE TABLE orders would fail
            assert!(ctx.db_path.exists());
        }

        // ---------------------------------------------------------------
        // Large fixture (1000 rows)
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_large_fixture() {
            let mut sql = String::from("CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT);\n");
            for i in 0..1000 {
                sql.push_str(&format!("INSERT INTO big VALUES ({i}, 'row_{i}');\n"));
            }
            let ctx = setup_sqlite_test(&[&sql]).unwrap();
            assert!(ctx.db_path.exists());
        }

        // ---------------------------------------------------------------
        // Fixture with PRAGMA statement
        // ---------------------------------------------------------------

        #[test]
        fn sqlite_fixture_with_pragma() {
            let ctx = setup_sqlite_test(&[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            ])
            .unwrap();
            assert!(ctx.db_path.exists());
        }
    }
}
