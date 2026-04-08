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
    // Pool::set_warmup_sqls takes &[&str] but the string must live long enough.
    // Since warmup is best-effort and tests are short-lived, we leak the string
    // to get a 'static lifetime. The leak is bounded (one allocation per test).
    let leaked: &'static str = Box::leak(warmup_sql.into_boxed_str());
    pool.set_warmup_sqls(&[leaked]);

    Ok(TestContext {
        pool,
        schema_name,
        db_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn missing_db_url_returns_clear_error() {
        // Temporarily unset both env vars (if set)
        let orig_bsql = std::env::var("BSQL_DATABASE_URL").ok();
        let orig_db = std::env::var("DATABASE_URL").ok();
        std::env::remove_var("BSQL_DATABASE_URL");
        std::env::remove_var("DATABASE_URL");

        let result = setup_test_schema(&[]).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("BSQL_DATABASE_URL") && msg.contains("DATABASE_URL"),
            "error should mention both env vars, got: {msg}"
        );

        // Restore
        if let Some(v) = orig_bsql {
            std::env::set_var("BSQL_DATABASE_URL", v);
        }
        if let Some(v) = orig_db {
            std::env::set_var("DATABASE_URL", v);
        }
    }

    #[tokio::test]
    async fn invalid_db_url_returns_clear_error() {
        let orig_bsql = std::env::var("BSQL_DATABASE_URL").ok();
        let orig_db = std::env::var("DATABASE_URL").ok();
        std::env::set_var("BSQL_DATABASE_URL", "not-a-valid-url");
        std::env::remove_var("DATABASE_URL");

        let result = setup_test_schema(&[]).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid database URL"),
            "error should mention invalid URL, got: {msg}"
        );

        // Restore
        std::env::remove_var("BSQL_DATABASE_URL");
        if let Some(v) = orig_bsql {
            std::env::set_var("BSQL_DATABASE_URL", v);
        }
        if let Some(v) = orig_db {
            std::env::set_var("DATABASE_URL", v);
        }
    }

    #[test]
    fn test_counter_is_monotonic() {
        let a = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let b = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let c = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn test_context_has_debug_impl() {
        // Verify that TestContext implements Debug (compile-time check).
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<TestContext>();
    }
}
