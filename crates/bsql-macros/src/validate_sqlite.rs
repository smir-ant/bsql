//! Compile-time SQL validation via SQLite `sqlite3_prepare_v2`.
//!
//! Validates SQL syntax, table/column existence, and extracts column metadata
//! (names, declared types, nullability) from the SQLite schema. This is the
//! SQLite counterpart to `validate.rs` (which validates against PostgreSQL).

use crate::parse::ParsedQuery;
use crate::types_sqlite::resolve_sqlite_type;
use crate::validate::{ColumnInfo, ValidationResult};

use bsql_driver_sqlite::conn::SqliteConnection;
use smallvec::SmallVec;

/// Convert PG-style positional parameters (`$1`, `$2`, ...) to SQLite-style (`?1`, `?2`, ...).
pub fn pg_to_sqlite_params(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            // Check if followed by digits (positional parameter)
            if chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                result.push('?');
                // Copy the digits
                while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    result.push(chars.next().unwrap());
                }
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    result
}

/// Validate a parsed query against a live SQLite database at compile time.
///
/// Uses the driver's `compile_validate` method which prepares the statement
/// and extracts column metadata.
pub fn validate_query_sqlite(
    parsed: &ParsedQuery,
    conn: &mut SqliteConnection,
) -> Result<ValidationResult, String> {
    // Convert $N params to ?N for SQLite
    let sqlite_sql = pg_to_sqlite_params(&parsed.positional_sql);

    // Validate via the driver's compile_validate method
    let (driver_columns, param_count) = conn.compile_validate(&sqlite_sql).map_err(|e| {
        format!(
            "SQLite compile-time validation failed: {e}\n  SQL: {}",
            sqlite_sql
        )
    })?;

    // Verify parameter count matches
    if param_count != parsed.params.len() {
        return Err(format!(
            "parameter count mismatch: query declares {} parameters but SQLite \
             expects {}. Check your $name: Type declarations.",
            parsed.params.len(),
            param_count
        ));
    }

    // Map driver column info to ValidationResult columns
    let columns: Vec<ColumnInfo> = driver_columns
        .iter()
        .map(|col| {
            let base_rust_type = resolve_sqlite_type(col.declared_type.as_deref());
            let rust_type = if col.is_nullable {
                format!("Option<{base_rust_type}>")
            } else {
                base_rust_type.to_owned()
            };

            ColumnInfo {
                name: col.name.clone(),
                pg_oid: 0, // SQLite has no OIDs
                pg_type_name: col
                    .declared_type
                    .clone()
                    .unwrap_or_else(|| "(none)".to_owned()),
                is_nullable: col.is_nullable,
                rust_type,
            }
        })
        .collect();

    Ok(ValidationResult {
        columns,
        param_pg_oids: SmallVec::new(), // SQLite doesn't type params
        param_is_pg_enum: SmallVec::new(), // No PG enums in SQLite
        #[cfg(feature = "explain")]
        explain_plan: None,
    })
}

/// Validate all dynamic query variants against SQLite.
///
/// Each variant is prepared independently. The first variant's columns
/// are used as the canonical result type (all variants return the same
/// columns — the SELECT list is identical, only WHERE clauses differ).
///
/// Note: superseded by `validate_clauses_linear_sqlite` which uses O(N+1) PREPAREs.
/// Kept for backward compatibility and tests.
pub fn validate_variants_sqlite(
    variants: &[crate::dynamic::QueryVariant],
    _parsed: &ParsedQuery,
    conn: &mut SqliteConnection,
) -> Result<ValidationResult, String> {
    if variants.is_empty() {
        return Err("internal error: no variants to validate".to_owned());
    }

    let mut canonical: Option<ValidationResult> = None;

    for (idx, variant) in variants.iter().enumerate() {
        let sqlite_sql = pg_to_sqlite_params(&variant.sql);

        let (driver_columns, param_count) = conn.compile_validate(&sqlite_sql).map_err(|e| {
            format!(
                "SQLite compile-time validation failed for variant {idx} (mask={:#06b}): {e}\n  SQL: {}",
                variant.mask, sqlite_sql
            )
        })?;

        if param_count != variant.params.len() {
            return Err(format!(
                "parameter count mismatch in variant {idx}: query declares {} \
                 parameters but SQLite expects {}.",
                variant.params.len(),
                param_count
            ));
        }

        if canonical.is_none() {
            // Use first variant as canonical result
            let columns: Vec<ColumnInfo> = driver_columns
                .iter()
                .map(|col| {
                    let base_rust_type =
                        crate::types_sqlite::resolve_sqlite_type(col.declared_type.as_deref());
                    let rust_type = if col.is_nullable {
                        format!("Option<{base_rust_type}>")
                    } else {
                        base_rust_type.to_owned()
                    };
                    ColumnInfo {
                        name: col.name.clone(),
                        pg_oid: 0,
                        pg_type_name: col
                            .declared_type
                            .clone()
                            .unwrap_or_else(|| "(none)".to_owned()),
                        is_nullable: col.is_nullable,
                        rust_type,
                    }
                })
                .collect();

            canonical = Some(ValidationResult {
                columns,
                param_pg_oids: SmallVec::new(),
                param_is_pg_enum: SmallVec::new(),
                #[cfg(feature = "explain")]
                explain_plan: None,
            });
        }
    }

    canonical.ok_or_else(|| "internal error: no canonical validation result".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- pg_to_sqlite_params ---

    #[test]
    fn convert_simple_params() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM t WHERE id = $1"),
            "SELECT * FROM t WHERE id = ?1"
        );
    }

    #[test]
    fn convert_multiple_params() {
        assert_eq!(
            pg_to_sqlite_params("INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"),
            "INSERT INTO t (a, b, c) VALUES (?1, ?2, ?3)"
        );
    }

    #[test]
    fn convert_no_params() {
        assert_eq!(pg_to_sqlite_params("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn convert_dollar_not_followed_by_digit() {
        assert_eq!(pg_to_sqlite_params("SELECT $abc"), "SELECT $abc");
    }

    #[test]
    fn convert_multi_digit_params() {
        assert_eq!(pg_to_sqlite_params("SELECT $10, $11"), "SELECT ?10, ?11");
    }

    // --- validate_query_sqlite ---

    fn temp_db_path() -> String {
        let id: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let dir = std::env::temp_dir();
        format!("{}/bsql_validate_sqlite_test_{id}.db", dir.display())
    }

    #[test]
    fn validate_simple_select() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE users (id INTEGER NOT NULL, name TEXT, active BOOLEAN NOT NULL)")
            .unwrap();

        let parsed =
            crate::parse::parse_query("SELECT id, name, active FROM users WHERE id = $id: i64")
                .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();

        assert_eq!(result.columns.len(), 3);

        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].rust_type, "i64");
        assert!(!result.columns[0].is_nullable);

        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].rust_type, "Option<String>");
        assert!(result.columns[1].is_nullable);

        assert_eq!(result.columns[2].name, "active");
        assert_eq!(result.columns[2].rust_type, "bool");
        assert!(!result.columns[2].is_nullable);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_invalid_sql() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();

        let parsed = crate::parse::parse_query("SELECT * FROM nonexistent_table").unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("SQLite compile-time validation failed"),
            "error: {err}"
        );

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_param_count_mismatch() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        // Query has 1 param in SQL but 0 declared params
        let mut parsed = crate::parse::parse_query("SELECT id FROM t").unwrap();
        parsed.positional_sql = "SELECT id FROM t WHERE id = $1".to_owned();
        let result = validate_query_sqlite(&parsed, &mut conn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("parameter count mismatch"), "error: {err}");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_expression_columns_are_nullable() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER NOT NULL)").unwrap();

        let parsed =
            crate::parse::parse_query("SELECT COUNT(*) AS cnt, SUM(val) AS total FROM t").unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();

        assert_eq!(result.columns.len(), 2);
        assert!(
            result.columns[0].is_nullable,
            "COUNT(*) should be nullable (safe default)"
        );
        assert!(result.columns[1].is_nullable, "SUM(val) should be nullable");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_various_column_types() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec(
            "CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL, c REAL NOT NULL, d BLOB NOT NULL, e BOOLEAN NOT NULL)",
        )
        .unwrap();

        let parsed = crate::parse::parse_query("SELECT a, b, c, d, e FROM t").unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();

        assert_eq!(result.columns[0].rust_type, "i64");
        assert_eq!(result.columns[1].rust_type, "String");
        assert_eq!(result.columns[2].rust_type, "f64");
        assert_eq!(result.columns[3].rust_type, "Vec<u8>");
        assert_eq!(result.columns[4].rust_type, "bool");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_insert_no_columns() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let parsed =
            crate::parse::parse_query("INSERT INTO t (id, name) VALUES ($id: i64, $name: &str)")
                .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();

        assert!(result.columns.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- validate_variants_sqlite ---

    #[test]
    fn validate_variants_one_optional_clause() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec(
            "CREATE TABLE tickets (id INTEGER NOT NULL, dept_id INTEGER, title TEXT NOT NULL)",
        )
        .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id, title FROM tickets WHERE 1 = 1 \
             [AND dept_id = $dept: Option<i64>]",
        )
        .unwrap();

        let variants = crate::dynamic::expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 2);

        let result = validate_variants_sqlite(&variants, &parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "title");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_variants_two_optional_clauses() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec(
            "CREATE TABLE tickets (id INTEGER NOT NULL, dept_id INTEGER, assignee_id INTEGER, title TEXT NOT NULL)",
        )
        .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id, title FROM tickets WHERE 1 = 1 \
             [AND dept_id = $dept: Option<i64>] \
             [AND assignee_id = $assignee: Option<i64>]",
        )
        .unwrap();

        let variants = crate::dynamic::expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 4);

        let result = validate_variants_sqlite(&variants, &parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_variants_three_optional_clauses() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL, a INTEGER, b INTEGER, c INTEGER)")
            .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id FROM t WHERE 1 = 1 \
             [AND a = $a: Option<i64>] \
             [AND b = $b: Option<i64>] \
             [AND c = $c: Option<i64>]",
        )
        .unwrap();

        let variants = crate::dynamic::expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 8);

        let result = validate_variants_sqlite(&variants, &parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_variants_with_base_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec(
            "CREATE TABLE tickets (id INTEGER NOT NULL, status TEXT NOT NULL, dept_id INTEGER)",
        )
        .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id FROM tickets WHERE status = $status: &str \
             [AND dept_id = $dept: Option<i64>]",
        )
        .unwrap();

        let variants = crate::dynamic::expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 2);

        let result = validate_variants_sqlite(&variants, &parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_variants_invalid_table() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id FROM nonexistent WHERE 1 = 1 \
             [AND a = $a: Option<i64>]",
        )
        .unwrap();

        let variants = crate::dynamic::expand_variants(&parsed).unwrap();
        let result = validate_variants_sqlite(&variants, &parsed, &mut conn);
        assert!(result.is_err());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- pg_to_sqlite_params: additional edge cases ---

    #[test]
    fn convert_dollar_at_end_of_string() {
        // Trailing $ without digits
        assert_eq!(pg_to_sqlite_params("SELECT $"), "SELECT $");
    }

    #[test]
    fn convert_dollar_followed_by_non_alnum() {
        assert_eq!(pg_to_sqlite_params("SELECT $ FROM t"), "SELECT $ FROM t");
    }

    #[test]
    fn convert_consecutive_params() {
        assert_eq!(pg_to_sqlite_params("$1$2$3"), "?1?2?3");
    }

    #[test]
    fn convert_param_in_string_context() {
        // Not a real SQL parser — it converts all $N regardless of context
        assert_eq!(pg_to_sqlite_params("'$1'"), "'?1'");
    }

    // --- validate_query_sqlite: edge cases ---

    #[test]
    fn validate_multiple_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL)")
            .unwrap();

        let parsed =
            crate::parse::parse_query("SELECT a, b FROM t WHERE a = $a: i64 AND b = $b: &str")
                .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_empty_variants_errors() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();

        let parsed = crate::parse::parse_query("SELECT 1").unwrap();
        let result = validate_variants_sqlite(&[], &parsed, &mut conn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("no variants to validate"), "error: {err}");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- pg_to_sqlite_params: edge cases for WHERE/JOIN/DELETE etc ---

    #[test]
    fn convert_where_with_or() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM t WHERE id = $1 OR name = $2"),
            "SELECT * FROM t WHERE id = ?1 OR name = ?2"
        );
    }

    #[test]
    fn convert_where_with_and() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM t WHERE id = $1 AND name = $2"),
            "SELECT * FROM t WHERE id = ?1 AND name = ?2"
        );
    }

    #[test]
    fn convert_where_with_parentheses() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM t WHERE (id = $1)"),
            "SELECT * FROM t WHERE (id = ?1)"
        );
    }

    #[test]
    fn convert_subquery() {
        assert_eq!(
            pg_to_sqlite_params(
                "SELECT * FROM t WHERE id IN (SELECT user_id FROM orders WHERE amount > $1)"
            ),
            "SELECT * FROM t WHERE id IN (SELECT user_id FROM orders WHERE amount > ?1)"
        );
    }

    #[test]
    fn convert_between() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM t WHERE id BETWEEN $1 AND $2"),
            "SELECT * FROM t WHERE id BETWEEN ?1 AND ?2"
        );
    }

    #[test]
    fn convert_join() {
        assert_eq!(
            pg_to_sqlite_params(
                "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = $1"
            ),
            "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = ?1"
        );
    }

    #[test]
    fn convert_delete() {
        assert_eq!(
            pg_to_sqlite_params("DELETE FROM users WHERE id = $1"),
            "DELETE FROM users WHERE id = ?1"
        );
    }

    #[test]
    fn convert_quoted_table() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM \"my_table\" WHERE id = $1"),
            "SELECT * FROM \"my_table\" WHERE id = ?1"
        );
    }

    #[test]
    fn convert_param_used_multiple_times() {
        assert_eq!(
            pg_to_sqlite_params("SELECT * FROM t WHERE id > $1 AND id < $1"),
            "SELECT * FROM t WHERE id > ?1 AND id < ?1"
        );
    }

    #[test]
    fn convert_empty_sql() {
        assert_eq!(pg_to_sqlite_params(""), "");
    }

    #[test]
    fn convert_whitespace_only() {
        assert_eq!(pg_to_sqlite_params("   "), "   ");
    }

    #[test]
    fn convert_insert_with_default_no_params() {
        // INSERT with DEFAULT VALUES — no $N params, no conversion needed
        assert_eq!(
            pg_to_sqlite_params("INSERT INTO t DEFAULT VALUES"),
            "INSERT INTO t DEFAULT VALUES"
        );
    }

    #[test]
    fn convert_update_with_expression() {
        assert_eq!(
            pg_to_sqlite_params("UPDATE t SET score = score + $1 WHERE id = $2"),
            "UPDATE t SET score = score + ?1 WHERE id = ?2"
        );
    }

    // --- validate_query_sqlite: additional edge cases ---

    #[test]
    fn validate_where_or() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL, name TEXT NOT NULL)")
            .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id, name FROM t WHERE id = $id: i64 OR name = $name: &str",
        )
        .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_where_and() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL, name TEXT NOT NULL)")
            .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id, name FROM t WHERE id = $id: i64 AND name = $name: &str",
        )
        .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_subquery_in_where() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)")
            .unwrap();
        conn.exec("CREATE TABLE orders (user_id INTEGER NOT NULL, amount REAL)")
            .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > $min: f64)",
        )
        .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "id");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_between() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL)").unwrap();

        let parsed =
            crate::parse::parse_query("SELECT id FROM t WHERE id BETWEEN $lo: i64 AND $hi: i64")
                .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_join() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)")
            .unwrap();
        conn.exec("CREATE TABLE orders (id INTEGER NOT NULL, user_id INTEGER NOT NULL)")
            .unwrap();

        let parsed = crate::parse::parse_query(
            "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = $id: i64",
        )
        .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_delete() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE users (id INTEGER NOT NULL)")
            .unwrap();

        let parsed = crate::parse::parse_query("DELETE FROM users WHERE id = $id: i64").unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert!(result.columns.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_update_with_expression() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL, score INTEGER NOT NULL)")
            .unwrap();

        let parsed = crate::parse::parse_query(
            "UPDATE t SET score = score + $delta: i64 WHERE id = $id: i64",
        )
        .unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert!(result.columns.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_quoted_table_name() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE \"my_table\" (id INTEGER NOT NULL)")
            .unwrap();

        let parsed =
            crate::parse::parse_query("SELECT id FROM \"my_table\" WHERE id = $id: i64").unwrap();
        let result = validate_query_sqlite(&parsed, &mut conn).unwrap();
        assert_eq!(result.columns.len(), 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
