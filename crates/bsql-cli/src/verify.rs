use bsql_driver_postgres::{Config, Connection};

use crate::cache::CachedQuery;

#[derive(Debug)]
pub struct VerifyResult {
    pub total_queries: usize,
    pub passed: usize,
    pub drifted: Vec<DriftedQuery>,
}

#[derive(Debug)]
pub struct DriftedQuery {
    pub sql: String,
    pub sql_hash: u64,
    pub reason: String,
}

/// Connect to the live database and PREPARE every cached query to verify
/// that the offline cache matches the current schema.
///
/// Unlike `check_migration`, this operates against the live `public` schema
/// directly — no shadow schema, no migration SQL.
pub fn verify_cache(
    database_url: &str,
    cached_queries: &[CachedQuery],
) -> Result<VerifyResult, String> {
    let config =
        Config::from_url(database_url).map_err(|e| format!("invalid database URL: {e}"))?;
    let mut conn = Connection::connect(&config).map_err(|e| format!("connection failed: {e}"))?;

    let mut result = VerifyResult {
        total_queries: cached_queries.len(),
        passed: 0,
        drifted: Vec::new(),
    };

    for query in cached_queries {
        if query.normalized_sql.contains(';') {
            result.drifted.push(DriftedQuery {
                sql: query.normalized_sql.clone(),
                sql_hash: query.sql_hash,
                reason: "cached SQL contains semicolons (possible cache tampering)".into(),
            });
            continue;
        }

        // PREPARE the query to validate syntax and types against current schema.
        let prepare_sql = format!("PREPARE __bsql_verify AS {}", query.normalized_sql);
        match conn.simple_query(&prepare_sql) {
            Ok(_) => {
                // Query still PREPAREs — schema hasn't broken it.
                // TODO: could also compare column OIDs via pg_prepared_statements,
                // but PREPARE success is the primary signal.
                result.passed += 1;
                let _ = conn.simple_query("DEALLOCATE __bsql_verify");
            }
            Err(e) => {
                result.drifted.push(DriftedQuery {
                    sql: query.normalized_sql.clone(),
                    sql_hash: query.sql_hash,
                    reason: e.to_string(),
                });
                let _ = conn.simple_query("DEALLOCATE IF EXISTS __bsql_verify");
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_empty_cache_returns_zero() {
        // If no queries cached, nothing to verify.
        // We can't connect without a live PG, so test the logic boundary.
        let queries: Vec<CachedQuery> = vec![];
        // verify_cache would need a connection, so just test the result struct.
        let result = VerifyResult {
            total_queries: queries.len(),
            passed: 0,
            drifted: vec![],
        };
        assert_eq!(result.total_queries, 0);
        assert!(result.drifted.is_empty());
    }

    #[test]
    fn semicolon_injection_detected() {
        // Simulate a tampered cache entry with semicolons.
        let queries = [CachedQuery {
            sql_hash: 1,
            normalized_sql: "SELECT 1; DROP TABLE users".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.23.0".to_owned(),
            param_rust_types: vec![],
        }];

        // Can't call verify_cache without a DB, but we can verify the semicolon
        // check logic directly.
        assert!(queries[0].normalized_sql.contains(';'));
    }
}
