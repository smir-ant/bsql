//! Compile-time SQL validation via PostgreSQL PREPARE.
//!
//! Connects to the database specified by `BSQL_DATABASE_URL` and validates
//! each query by preparing it. Introspects column types and nullability
//! from `pg_catalog`.

use smallvec::SmallVec;

use bsql_driver_postgres::{ColumnDesc, Connection, DriverError};

use crate::dynamic::QueryVariant;
use crate::parse::ParsedQuery;

/// Metadata about a single result column, resolved from PostgreSQL.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name as returned by PostgreSQL.
    pub name: String,
    /// PostgreSQL type OID.
    pub pg_oid: u32,
    /// PostgreSQL type name (e.g. `"int4"`, `"text"`).
    pub pg_type_name: String,
    /// Whether this column can be NULL.
    pub is_nullable: bool,
    /// The Rust type string for code generation (e.g. `"i32"`, `"Option<String>"`).
    pub rust_type: String,
}

/// Result of validating a query against PostgreSQL.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Output columns (for SELECT or RETURNING queries).
    pub columns: Vec<ColumnInfo>,
    /// PostgreSQL OIDs of the expected parameter types.
    pub param_pg_oids: SmallVec<[u32; 8]>,
    /// Whether each parameter type is a PostgreSQL enum (custom type).
    /// When true, `&str`/`String` params are accepted in addition to
    /// any `#[bsql::pg_enum]`-annotated Rust enum.
    pub param_is_pg_enum: SmallVec<[bool; 8]>,
    /// EXPLAIN plan summary (only populated when `explain` feature is enabled).
    #[cfg(feature = "explain")]
    pub explain_plan: Option<String>,
}

/// Validate a parsed query against a live PostgreSQL instance.
///
/// Uses `conn.prepare_describe()` which:
/// 1. Validates SQL syntax
/// 2. Validates table/column existence
/// 3. Returns column metadata and parameter types
pub fn validate_query(
    parsed: &ParsedQuery,
    conn: &mut Connection,
) -> Result<ValidationResult, String> {
    // Prepare the query — this validates syntax, tables, columns, types.
    let result = conn
        .prepare_describe(&parsed.positional_sql)
        .map_err(|e| format_driver_error(&e, parsed))?;

    // Extract parameter type OIDs
    let param_pg_oids: SmallVec<[u32; 8]> = result.param_oids.iter().copied().collect();

    // Detect PG enums by querying pg_type.typtype for each parameter OID.
    let param_is_pg_enum = detect_pg_enums(conn, &result.param_oids);

    let columns = build_columns(conn, &result.columns)?;

    Ok(ValidationResult {
        columns,
        param_pg_oids,
        param_is_pg_enum,
        #[cfg(feature = "explain")]
        explain_plan: fetch_explain_plan(conn, parsed),
    })
}

/// Resolve column metadata (name, type, nullability) from a prepared statement.
fn build_columns(
    conn: &mut Connection,
    pg_columns: &[ColumnDesc],
) -> Result<Vec<ColumnInfo>, String> {
    let nullable_flags = resolve_nullability_batch(conn, pg_columns);

    // Detect which columns are PG enum types (for the enum error message).
    let enum_flags = detect_column_enums(conn, pg_columns);

    let mut columns = Vec::with_capacity(pg_columns.len());
    for (i, col) in pg_columns.iter().enumerate() {
        let pg_oid = col.type_oid;
        let pg_type_name = bsql_core::types::pg_name_for_oid(pg_oid)
            .unwrap_or("unknown")
            .to_owned();
        let name = col.name.to_string();
        let is_nullable = nullable_flags[i];

        if enum_flags[i] {
            return Err(format!(
                "column \"{name}\" is PostgreSQL enum type `{pg_type_name}`. \
                 Define a Rust enum with #[bsql::pg_enum] or cast to text: {name}::text"
            ));
        }

        let base_rust_type = crate::types::resolve_rust_type(pg_oid)
            .map_err(|msg| format!("column \"{name}\": {msg}"))?;

        let rust_type = if is_nullable {
            format!("Option<{base_rust_type}>")
        } else {
            base_rust_type.to_owned()
        };

        columns.push(ColumnInfo {
            name,
            pg_oid,
            pg_type_name,
            is_nullable,
            rust_type,
        });
    }
    Ok(columns)
}

/// Fetch EXPLAIN output for a query (only when `explain` feature is enabled).
///
/// Returns a human-readable summary of the query plan. Errors are silently
/// ignored -- EXPLAIN is informational and must never block compilation.
#[cfg(feature = "explain")]
fn fetch_explain_plan(conn: &mut Connection, parsed: &ParsedQuery) -> Option<String> {
    // EXPLAIN cannot handle parameterized queries directly. We use
    // EXPLAIN (FORMAT TEXT) with a generic plan (PG 16+ supports
    // EXPLAIN (GENERIC_PLAN) for prepared statements).
    //
    // For older PG versions, we try EXPLAIN on the raw SQL. If it fails
    // (e.g. because of parameters), we skip silently.
    let explain_sql = format!("EXPLAIN (FORMAT TEXT, COSTS) {}", parsed.positional_sql);

    match conn.simple_query_rows(&explain_sql) {
        Ok(rows) => {
            let lines: Vec<String> = rows
                .into_iter()
                .filter_map(|row| row.into_iter().next().flatten())
                .collect();

            if lines.is_empty() {
                None
            } else {
                Some(lines.join("\n"))
            }
        }
        Err(_) => None,
    }
}

/// Determine nullability for all columns in a single PG round-trip.
///
/// For columns backed by a real table, queries `pg_attribute.attnotnull` in
/// batch using string-interpolated OIDs. Computed columns (aggregates,
/// functions) default to nullable (the safe choice).
fn resolve_nullability_batch(conn: &mut Connection, columns: &[ColumnDesc]) -> Vec<bool> {
    let col_count = columns.len();
    // Default: all nullable (safe). We overwrite entries we can resolve.
    let mut result = vec![true; col_count];

    // Collect (table_oid, column_id) pairs for table-backed columns
    let mut table_oids: Vec<u32> = Vec::new();
    let mut col_nums: Vec<i16> = Vec::new();
    let mut col_indices: Vec<usize> = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        if col.table_oid != 0 && col.column_id != 0 {
            table_oids.push(col.table_oid);
            col_nums.push(col.column_id);
            col_indices.push(i);
        }
    }

    if table_oids.is_empty() {
        return result;
    }

    // Build an ARRAY literal for each: '{oid1,oid2,...}' and '{num1,num2,...}'
    let oid_array = format!(
        "ARRAY[{}]::oid[]",
        table_oids
            .iter()
            .map(|o| o.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let num_array = format!(
        "ARRAY[{}]::int2[]",
        col_nums
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    // Single batched query: unnest the OID/attnum arrays and join pg_attribute
    let query = format!(
        "SELECT a.attrelid, a.attnum, NOT a.attnotnull \
         FROM pg_attribute a \
         WHERE (a.attrelid, a.attnum) IN (\
             SELECT unnest({oid_array}), unnest({num_array})\
         )"
    );

    if let Ok(rows) = conn.simple_query_rows(&query) {
        // Build lookup: (table_oid, col_num) -> original column index
        let mut lookup: std::collections::HashMap<(u32, i16), Vec<usize>> =
            std::collections::HashMap::with_capacity(table_oids.len());
        for (idx, (&t, &c)) in table_oids.iter().zip(col_nums.iter()).enumerate() {
            lookup.entry((t, c)).or_default().push(col_indices[idx]);
        }

        for row in &rows {
            // Columns: attrelid (oid as text), attnum (int2 as text), is_nullable (bool as text)
            let oid: u32 = row
                .first()
                .and_then(|v| v.as_deref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let num: i16 = row
                .get(1)
                .and_then(|v| v.as_deref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let is_nullable: bool = row
                .get(2)
                .and_then(|v| v.as_deref())
                .map(|s| s == "t" || s == "true")
                .unwrap_or(true);
            if let Some(indices) = lookup.get(&(oid, num)) {
                for &idx in indices {
                    result[idx] = is_nullable;
                }
            }
        }
    }
    // If the query fails, all columns stay nullable (safe default)

    result
}

/// Detect which parameter OIDs are PostgreSQL enum types.
///
/// Queries `pg_type.typtype` for each OID. Returns `'e'` for enum types.
/// Uses a single batched simple query with string-interpolated OIDs.
fn detect_pg_enums(conn: &mut Connection, oids: &[u32]) -> SmallVec<[bool; 8]> {
    if oids.is_empty() {
        return SmallVec::new();
    }

    let oid_list = oids
        .iter()
        .map(|o| o.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let query = format!("SELECT oid, typtype FROM pg_type WHERE oid IN ({oid_list})");

    let mut enum_map: std::collections::HashMap<u32, bool> =
        std::collections::HashMap::with_capacity(oids.len());

    if let Ok(rows) = conn.simple_query_rows(&query) {
        for row in &rows {
            let oid: u32 = row
                .first()
                .and_then(|v| v.as_deref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let typtype: &str = row.get(1).and_then(|v| v.as_deref()).unwrap_or("b");
            enum_map.insert(oid, typtype == "e");
        }
    }

    oids.iter()
        .map(|oid| enum_map.get(oid).copied().unwrap_or(false))
        .collect()
}

/// Detect which column type OIDs are PostgreSQL enum types.
///
/// Similar to `detect_pg_enums` but for column OIDs. Only queries OIDs
/// that are not in the standard built-in type range (< 10000).
fn detect_column_enums(conn: &mut Connection, columns: &[ColumnDesc]) -> Vec<bool> {
    let mut result = vec![false; columns.len()];

    // Only check non-built-in OIDs (built-in types are never enums)
    let custom_oids: Vec<(usize, u32)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.type_oid >= 10000)
        .map(|(i, c)| (i, c.type_oid))
        .collect();

    if custom_oids.is_empty() {
        return result;
    }

    let oid_list = custom_oids
        .iter()
        .map(|(_, o)| o.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let query = format!("SELECT oid, typtype FROM pg_type WHERE oid IN ({oid_list})");

    if let Ok(rows) = conn.simple_query_rows(&query) {
        let mut enum_set: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for row in &rows {
            let oid: u32 = row
                .first()
                .and_then(|v| v.as_deref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let typtype: &str = row.get(1).and_then(|v| v.as_deref()).unwrap_or("b");
            if typtype == "e" {
                enum_set.insert(oid);
            }
        }
        for &(idx, oid) in &custom_oids {
            if enum_set.contains(&oid) {
                result[idx] = true;
            }
        }
    }

    result
}

/// Check that user-declared parameter types match what PostgreSQL expects.
pub fn check_param_types(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> Result<(), String> {
    check_params_against_pg(
        &parsed.params,
        &validation.param_pg_oids,
        &validation.param_is_pg_enum,
        false,
        "",
    )
}

/// Validate all dynamic query variants against PostgreSQL.
///
/// Each variant is PREPAREd independently. The first variant's columns
/// are used as the canonical result type (all variants must return the
/// same columns — the base SELECT is identical, only WHERE clauses differ).
///
/// Note: superseded by `validate_clauses_linear` which uses O(N+1) PREPAREs.
/// Kept for backward compatibility and tests.
pub fn validate_variants(
    variants: &[QueryVariant],
    parsed: &ParsedQuery,
    conn: &mut Connection,
) -> Result<ValidationResult, String> {
    if variants.len() <= 1 {
        // Single variant or no optional clauses — use normal validation
        return validate_query(parsed, conn);
    }

    // Validate every variant and collect results.
    // All variants must produce the same column set.
    let mut canonical_result: Option<ValidationResult> = None;

    for (i, variant) in variants.iter().enumerate() {
        let result = validate_variant(variant, conn, parsed, i)?;

        // Check parameter type compatibility for this variant
        check_variant_param_types(variant, &result)?;

        if let Some(ref canonical) = canonical_result {
            // Verify column set matches the canonical (variant 0) result.
            // This should always be true for optional WHERE clauses,
            // but we check defensively.
            if result.columns.len() != canonical.columns.len() {
                return Err(format!(
                    "variant {} (mask {:#06b}) returns {} columns, but variant 0 \
                     returns {} columns. Optional clauses must not change the SELECT list.",
                    i,
                    variant.mask,
                    result.columns.len(),
                    canonical.columns.len()
                ));
            }
        } else {
            canonical_result = Some(result);
        }
    }

    canonical_result.ok_or_else(|| "no variants to validate (internal error)".to_owned())
}

/// Validate a dynamic query with O(N+1) PREPAREs instead of 2^N.
///
/// Validates:
/// 1. The base SQL (all optional clauses excluded) — must be valid
/// 2. For each clause i: base SQL + clause i (only that clause included)
///
/// This is sufficient for independent optional WHERE clauses (the common case).
/// Inter-clause interactions (rare) are not checked, but would fail at runtime
/// with a clear PostgreSQL error.
pub fn validate_clauses_linear(
    parsed: &ParsedQuery,
    conn: &mut Connection,
) -> Result<ValidationResult, String> {
    use crate::dynamic;

    let n = parsed.optional_clauses.len();
    if n == 0 {
        return validate_query(parsed, conn);
    }

    // 1. Validate base query (mask=0: no optional clauses)
    let base_variant = dynamic::build_variant(parsed, 0)
        .map_err(|e| format!("internal error building base variant: {e}"))?;
    let base_result = validate_variant(&base_variant, conn, parsed, 0)?;
    check_variant_param_types(&base_variant, &base_result)?;

    // 2. Validate each clause individually (mask = 1<<i)
    for i in 0..n {
        let mask = 1u32 << i;
        let clause_variant = dynamic::build_variant(parsed, mask)
            .map_err(|e| format!("internal error building clause {i} variant: {e}"))?;
        let clause_result = validate_variant(&clause_variant, conn, parsed, i + 1)?;
        check_variant_param_types(&clause_variant, &clause_result)?;

        // Verify column set matches the base
        if clause_result.columns.len() != base_result.columns.len() {
            return Err(format!(
                "clause {} `[{}]` changes the number of result columns ({} vs {}). \
                 Optional clauses must not change the SELECT list.",
                i,
                parsed.optional_clauses[i].sql_fragment,
                clause_result.columns.len(),
                base_result.columns.len()
            ));
        }
    }

    Ok(base_result)
}

fn validate_variant(
    variant: &QueryVariant,
    conn: &mut Connection,
    parsed: &ParsedQuery,
    variant_index: usize,
) -> Result<ValidationResult, String> {
    let result = conn
        .prepare_describe(&variant.sql)
        .map_err(|e| format_variant_driver_error(&e, variant, parsed, variant_index))?;

    let param_pg_oids: SmallVec<[u32; 8]> = result.param_oids.iter().copied().collect();
    let param_is_pg_enum = detect_pg_enums(conn, &result.param_oids);

    let columns = build_columns(conn, &result.columns)?;

    Ok(ValidationResult {
        columns,
        param_pg_oids,
        param_is_pg_enum,
        #[cfg(feature = "explain")]
        explain_plan: None,
    })
}

/// Check parameter types for a specific variant.
pub fn check_variant_param_types(
    variant: &QueryVariant,
    validation: &ValidationResult,
) -> Result<(), String> {
    check_params_against_pg(
        &variant.params,
        &validation.param_pg_oids,
        &validation.param_is_pg_enum,
        true,
        &format!("variant (mask {:#06b})", variant.mask),
    )
}

/// Unified parameter type checking against PostgreSQL OIDs.
///
/// `strip_option_wrapper`: when true, strips `Option<>` before comparison
/// (used for dynamic query variants where optional clause params are `Option<T>`).
///
/// `context`: empty string for static queries, or a description like
/// `"variant (mask 0b0011)"` for error messages.
fn check_params_against_pg(
    params: &[crate::parse::Param],
    pg_oids: &[u32],
    pg_enum_flags: &[bool],
    strip_option_wrapper: bool,
    context: &str,
) -> Result<(), String> {
    if params.len() != pg_oids.len() {
        let ctx = if context.is_empty() {
            String::new()
        } else {
            format!(" in {context}")
        };
        return Err(format!(
            "parameter count mismatch{ctx}: query has {} parameters but PostgreSQL \
             expects {}. Check your $name: Type declarations.",
            params.len(),
            pg_oids.len()
        ));
    }

    for (i, (param, &pg_oid)) in params.iter().zip(pg_oids).enumerate() {
        let is_pg_enum = pg_enum_flags.get(i).copied().unwrap_or(false);

        let check_type = if strip_option_wrapper {
            strip_option(&param.rust_type)
        } else {
            &param.rust_type
        };

        if is_pg_enum {
            if matches!(check_type, "&str" | "String") {
                continue;
            }
            if crate::types::is_known_non_enum_type(check_type) {
                return Err(format!(
                    "type `{}` cannot be used for PostgreSQL enum parameter `${}`. \
                     Use `&str`, `String`, or a `#[bsql::pg_enum]` type.",
                    param.rust_type, param.name
                ));
            }
            // Unknown type (likely a #[pg_enum] type) -- accept, runtime ToSql verifies
            continue;
        }

        if !crate::types::is_param_compatible_extended(check_type, pg_oid) {
            let pg_name = bsql_core::types::pg_name_for_oid(pg_oid).unwrap_or("unknown");
            let extra_hint = match crate::types::resolve_rust_type(pg_oid) {
                Ok(expected) => format!(" (expected `{expected}`)"),
                Err(msg) => format!(" — {msg}"),
            };
            return Err(format!(
                "type mismatch for parameter `${}`: declared `{}` but PostgreSQL \
                 expects `{}` (OID {}){extra_hint}",
                param.name, param.rust_type, pg_name, pg_oid
            ));
        }
    }

    Ok(())
}

/// Strip `Option<...>` wrapper from a type string, returning the inner type.
/// If the type is not `Option<T>`, returns it unchanged.
fn strip_option(ty: &str) -> &str {
    if let Some(inner) = ty.strip_prefix("Option<") {
        if let Some(inner) = inner.strip_suffix('>') {
            return inner;
        }
    }
    ty
}

/// Extract the common parts of a DriverError: message, detail, hint.
fn format_driver_error_base(e: &DriverError) -> String {
    match e {
        DriverError::Server {
            message,
            detail,
            hint,
            position,
            ..
        } => {
            let mut out = format!("PostgreSQL error: {message}");
            if let Some(pos) = position {
                out.push_str(&format!(" (at position {pos})"));
            }
            if let Some(d) = detail {
                out.push_str(&format!("\n  detail: {d}"));
            }
            if let Some(h) = hint {
                out.push_str(&format!("\n  hint: {h}"));
            }
            out
        }
        other => format!("PostgreSQL error: {other}"),
    }
}

/// Format a variant-specific PostgreSQL error with context about which
/// clause combination caused the failure.
fn format_variant_driver_error(
    e: &DriverError,
    variant: &QueryVariant,
    parsed: &ParsedQuery,
    variant_index: usize,
) -> String {
    let n = parsed.optional_clauses.len();
    let included: Vec<usize> = (0..n).filter(|&i| (variant.mask & (1 << i)) != 0).collect();

    let clause_desc = if included.is_empty() {
        "no optional clauses included".to_owned()
    } else {
        let clause_strs: Vec<String> = included
            .iter()
            .map(|&i| {
                format!(
                    "clause {} `[{}]`",
                    i, parsed.optional_clauses[i].sql_fragment
                )
            })
            .collect();
        format!("with {}", clause_strs.join(", "))
    };

    let base_msg = format_driver_error_base(e);
    format!(
        "optional clause variant {} ({clause_desc}) produces invalid SQL:\n  \
         {base_msg}\n  SQL: {}",
        variant_index, variant.sql
    )
}

/// Format a PostgreSQL error into a developer-friendly compile error message.
fn format_driver_error(e: &DriverError, parsed: &ParsedQuery) -> String {
    let mut out = format_driver_error_base(e);

    out.push_str(&format!("\n         SQL: {}", parsed.positional_sql));

    // Show a position indicator if the driver provides one.
    if let DriverError::Server {
        position: Some(pos),
        ..
    } = e
    {
        let col = (*pos as usize).saturating_sub(1); // 1-indexed -> 0-indexed
        let prefix_len = "         SQL: ".len();
        let marker = format!("\n{}{}", " ".repeat(prefix_len + col), "^");
        out.push_str(&marker);
    }

    out
}

/// Validate a query against a live PostgreSQL instance, with "did you mean?"
/// suggestions on failure.
pub fn validate_query_with_suggestions(
    parsed: &ParsedQuery,
    conn: &mut Connection,
) -> Result<ValidationResult, String> {
    match validate_query(parsed, conn) {
        Ok(result) => Ok(result),
        Err(base_error) => {
            // Enhance the error with "did you mean?" suggestions.
            if let Some(suggestion) = crate::suggest::enhance_error(&base_error, conn) {
                Err(format!("{base_error}{suggestion}"))
            } else {
                Err(base_error)
            }
        }
    }
}

// NOTE: `validate_sort_variants` was removed in v0.11. The proc macro cannot
// access sort enum variants (they live in user code), so compile-time validation
// of individual ORDER BY fragments is not possible without a registry. The query
// structure is validated with a dummy ORDER BY, but individual sort SQL fragments
// are verified only at runtime. See sort_enum.rs doc comment for details.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::Param;

    // --- strip_option ---

    #[test]
    fn strip_option_wraps_i32() {
        assert_eq!(strip_option("Option<i32>"), "i32");
    }

    #[test]
    fn strip_option_no_change_plain_type() {
        assert_eq!(strip_option("i32"), "i32");
    }

    #[test]
    fn strip_option_nested() {
        // Option<Option<i32>> -> Option<i32> (only strips outer)
        assert_eq!(strip_option("Option<Option<i32>>"), "Option<i32>");
    }

    #[test]
    fn strip_option_with_str() {
        assert_eq!(strip_option("Option<&str>"), "&str");
    }

    #[test]
    fn strip_option_with_string() {
        assert_eq!(strip_option("Option<String>"), "String");
    }

    #[test]
    fn strip_option_with_whitespace_strips_outer() {
        // strip_option matches "Option<" prefix and ">" suffix regardless of inner content
        assert_eq!(strip_option("Option< i32 >"), " i32 ");
    }

    #[test]
    fn strip_option_empty_string() {
        assert_eq!(strip_option(""), "");
    }

    #[test]
    fn strip_option_prefix_only() {
        // "Option<i32" without closing > should not strip
        assert_eq!(strip_option("Option<i32"), "Option<i32");
    }

    // --- format_driver_error_base ---

    #[test]
    fn format_server_error_basic() {
        let err = DriverError::Server {
            code: "42P01".into(),
            message: "relation \"users\" does not exist".into(),
            detail: None,
            hint: None,
            position: None,
        };
        let msg = format_driver_error_base(&err);
        assert!(msg.contains("relation \"users\" does not exist"));
        assert!(msg.starts_with("PostgreSQL error:"));
    }

    #[test]
    fn format_server_error_with_detail_and_hint() {
        let err = DriverError::Server {
            code: "42P01".into(),
            message: "something went wrong".into(),
            detail: Some("extra detail here".into()),
            hint: Some("try this instead".into()),
            position: None,
        };
        let msg = format_driver_error_base(&err);
        assert!(msg.contains("something went wrong"));
        assert!(msg.contains("detail: extra detail here"));
        assert!(msg.contains("hint: try this instead"));
    }

    #[test]
    fn format_server_error_with_position() {
        let err = DriverError::Server {
            code: "42601".into(),
            message: "syntax error".into(),
            detail: None,
            hint: None,
            position: Some(15),
        };
        let msg = format_driver_error_base(&err);
        assert!(msg.contains("at position 15"));
    }

    #[test]
    fn format_non_server_error() {
        let err = DriverError::Pool("connection lost".into());
        let msg = format_driver_error_base(&err);
        assert!(msg.contains("PostgreSQL error:"));
        assert!(msg.contains("connection lost"));
    }

    // --- format_driver_error (includes SQL) ---

    #[test]
    fn format_driver_error_includes_sql() {
        let err = DriverError::Server {
            code: "42P01".into(),
            message: "relation does not exist".into(),
            detail: None,
            hint: None,
            position: None,
        };
        let parsed = crate::parse::parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        let msg = format_driver_error(&err, &parsed);
        assert!(msg.contains("SQL:"), "should include SQL in error: {msg}");
        assert!(msg.contains("$1"), "should include positional SQL: {msg}");
    }

    #[test]
    fn format_driver_error_includes_position_marker() {
        let err = DriverError::Server {
            code: "42601".into(),
            message: "syntax error".into(),
            detail: None,
            hint: None,
            position: Some(8),
        };
        let parsed = crate::parse::parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        let msg = format_driver_error(&err, &parsed);
        assert!(msg.contains('^'), "should include position marker: {msg}");
    }

    // --- check_params_against_pg ---

    #[test]
    fn check_params_count_mismatch() {
        let params = vec![Param {
            name: "id".into(),
            rust_type: "i32".into(),
            position: 1,
        }];
        // PG expects 2 params but we declared 1
        let pg_oids = [23u32, 25u32]; // int4, text
        let pg_enum = [false, false];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("parameter count mismatch"), "error: {err}");
    }

    #[test]
    fn check_params_count_mismatch_with_context() {
        let params = vec![];
        let pg_oids = [23u32];
        let pg_enum = [false];
        let result =
            check_params_against_pg(&params, &pg_oids, &pg_enum, false, "variant (mask 0b0011)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("variant (mask 0b0011)"),
            "should include context: {err}"
        );
    }

    #[test]
    fn check_params_type_mismatch() {
        let params = vec![Param {
            name: "id".into(),
            rust_type: "&str".into(), // declared &str
            position: 1,
        }];
        let pg_oids = [23u32]; // PG expects int4
        let pg_enum = [false];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("type mismatch"),
            "should mention type mismatch: {err}"
        );
    }

    #[test]
    fn check_params_matching_types_ok() {
        let params = vec![Param {
            name: "id".into(),
            rust_type: "i32".into(),
            position: 1,
        }];
        let pg_oids = [23u32]; // int4
        let pg_enum = [false];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_ok());
    }

    #[test]
    fn check_params_empty_ok() {
        let params: Vec<Param> = vec![];
        let pg_oids: [u32; 0] = [];
        let pg_enum: [bool; 0] = [];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_ok());
    }

    #[test]
    fn check_params_enum_with_str_ok() {
        let params = vec![Param {
            name: "status".into(),
            rust_type: "&str".into(),
            position: 1,
        }];
        let pg_oids = [99999u32]; // some custom enum OID
        let pg_enum = [true];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_ok(), "enum param with &str should be accepted");
    }

    #[test]
    fn check_params_enum_with_string_ok() {
        let params = vec![Param {
            name: "status".into(),
            rust_type: "String".into(),
            position: 1,
        }];
        let pg_oids = [99999u32];
        let pg_enum = [true];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_ok(), "enum param with String should be accepted");
    }

    #[test]
    fn check_params_enum_with_i32_error() {
        let params = vec![Param {
            name: "status".into(),
            rust_type: "i32".into(),
            position: 1,
        }];
        let pg_oids = [99999u32];
        let pg_enum = [true];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("cannot be used for PostgreSQL enum"),
            "should reject i32 for enum: {err}"
        );
    }

    #[test]
    fn check_params_enum_with_custom_type_ok() {
        // Unknown type (likely a #[pg_enum] user type) should be accepted
        let params = vec![Param {
            name: "status".into(),
            rust_type: "MyStatusEnum".into(),
            position: 1,
        }];
        let pg_oids = [99999u32];
        let pg_enum = [true];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_ok(), "custom enum type should be accepted");
    }

    #[test]
    fn check_params_strip_option_in_variant_mode() {
        // In variant mode (strip_option_wrapper=true), Option<i32> -> i32
        let params = vec![Param {
            name: "id".into(),
            rust_type: "Option<i32>".into(),
            position: 1,
        }];
        let pg_oids = [23u32]; // int4
        let pg_enum = [false];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, true, "variant");
        assert!(
            result.is_ok(),
            "Option<i32> stripped to i32 should match int4"
        );
    }

    #[test]
    fn check_params_strip_option_mismatch() {
        let params = vec![Param {
            name: "id".into(),
            rust_type: "Option<&str>".into(),
            position: 1,
        }];
        let pg_oids = [23u32]; // int4
        let pg_enum = [false];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, true, "variant");
        assert!(
            result.is_err(),
            "Option<&str> stripped to &str should not match int4"
        );
    }
}
