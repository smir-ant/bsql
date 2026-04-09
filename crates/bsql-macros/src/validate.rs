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

    let columns = build_columns(conn, &result.columns, &parsed.positional_sql)?;

    Ok(ValidationResult {
        columns,
        param_pg_oids,
        param_is_pg_enum,
        #[cfg(feature = "explain")]
        explain_plan: fetch_explain_plan(conn, parsed),
    })
}

/// Resolve column metadata (name, type, nullability) from a prepared statement.
///
/// `sql` is the normalized SQL string, used to infer NOT NULL for computed
/// columns via `is_known_not_null`.
fn build_columns(
    conn: &mut Connection,
    pg_columns: &[ColumnDesc],
    sql: &str,
) -> Result<Vec<ColumnInfo>, String> {
    let mut nullable_flags = resolve_nullability_batch(conn, pg_columns);

    // Second pass: override known-NOT-NULL computed columns (Fix-6).
    // Parse the SELECT list and check each computed column (table_oid == 0)
    // against known NOT NULL expression patterns.
    let select_exprs = parse_select_expressions(sql);
    for (i, col) in pg_columns.iter().enumerate() {
        if col.table_oid == 0 && nullable_flags[i] {
            // Computed column — check if the expression is known NOT NULL.
            let expr = if i < select_exprs.len() {
                &select_exprs[i]
            } else {
                ""
            };
            if is_known_not_null(&col.name, expr) {
                nullable_flags[i] = false;
            }
        }
    }

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

/// Parse the SELECT clause of a SQL statement and extract individual expressions.
///
/// Handles nested parentheses (e.g., `COALESCE(a, 'x')`, `SUM(CASE ... END)`)
/// by tracking parenthesis depth. Strips trailing `AS alias` from each expression.
///
/// Returns an empty `Vec` if the SQL cannot be parsed (e.g., no SELECT/FROM).
fn parse_select_expressions(sql: &str) -> Vec<String> {
    let lower = sql.to_lowercase();

    // Find "SELECT " (case insensitive)
    let select_start = match lower.find("select ") {
        Some(pos) => pos + 7, // skip "select "
        None => return Vec::new(),
    };

    // Handle SELECT DISTINCT
    let after_select = lower[select_start..].trim_start();
    let offset = if after_select.starts_with("distinct ") {
        select_start + (lower[select_start..].len() - after_select.len()) + 9
    } else {
        select_start
    };

    // Find " FROM " — end of select list.
    // Must find the FROM at depth 0 (not inside subqueries).
    let select_region = &sql[offset..];
    let mut from_pos = None;
    let mut depth: i32 = 0;
    let select_lower = &lower[offset..];
    let bytes = select_lower.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b' ' if depth == 0 && i + 6 <= bytes.len() && &select_lower[i..i + 6] == " from " => {
                from_pos = Some(i);
                break;
            }
            _ => {}
        }
        i += 1;
    }

    let select_list = match from_pos {
        Some(pos) => &select_region[..pos],
        // No FROM clause (e.g., "SELECT 1") — entire remaining string is the select list
        None => select_region.trim_end_matches(';').trim(),
    };

    // Split by commas, respecting parenthesis depth.
    let mut exprs = Vec::new();
    let mut current_start = 0;
    depth = 0;
    let list_bytes = select_list.as_bytes();
    for j in 0..list_bytes.len() {
        match list_bytes[j] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                let raw = select_list[current_start..j].trim();
                exprs.push(strip_alias(raw));
                current_start = j + 1;
            }
            _ => {}
        }
    }
    // Last expression
    let raw = select_list[current_start..].trim();
    if !raw.is_empty() {
        exprs.push(strip_alias(raw));
    }

    exprs
}

/// Strip a trailing `AS alias` from a SELECT expression.
///
/// Handles both `expr AS alias` and `expr alias` (implicit alias).
/// Only strips at depth 0 to avoid stripping `AS` inside subqueries.
fn strip_alias(expr: &str) -> String {
    let lower = expr.to_lowercase();

    // Look for " as " (case insensitive) at depth 0, from right to left.
    if let Some(as_pos) = lower.rfind(" as ") {
        // Verify it's at depth 0
        let depth: i32 = expr[..as_pos]
            .bytes()
            .map(|b| match b {
                b'(' => 1,
                b')' => -1,
                _ => 0,
            })
            .sum();
        if depth == 0 {
            return expr[..as_pos].trim().to_owned();
        }
    }

    expr.trim().to_owned()
}

/// Check if a SQL expression in the SELECT list is known to produce NOT NULL results.
///
/// Analyzes the expression text for patterns that the SQL standard guarantees
/// will never return NULL. Uses both the column name (from `pg_catalog`) and
/// the parsed expression text for maximum coverage.
fn is_known_not_null(col_name: &str, select_expr: &str) -> bool {
    // If the SELECT expression is empty (parsing failed), fall back to the
    // column name reported by PostgreSQL. Bare aggregates like COUNT(*)
    // produce a column name "count".
    let expr_lower = if select_expr.trim().is_empty() {
        col_name.to_lowercase()
    } else {
        select_expr.trim().to_lowercase()
    };

    // COUNT(*) and COUNT(expr) — SQL standard guarantees NOT NULL
    if expr_lower.starts_with("count(") || expr_lower == "count" {
        return true;
    }

    // COALESCE with a literal last argument — guaranteed NOT NULL
    if expr_lower.starts_with("coalesce(") {
        if let Some(last_arg) = expr_lower.rsplit(',').next() {
            let trimmed = last_arg.trim().trim_end_matches(')').trim();
            if is_literal(trimmed) {
                return true;
            }
        }
        return false;
    }

    // EXISTS(...) — always returns boolean, never NULL
    if expr_lower.starts_with("exists(") {
        return true;
    }

    // CASE WHEN ... THEN literal ELSE literal END — not null if both branches are literals
    if expr_lower.starts_with("case ")
        && expr_lower.ends_with(" end")
        && is_case_all_literal_branches(&expr_lower)
    {
        return true;
    }

    // Window functions that always return NOT NULL
    if is_not_null_window_function(&expr_lower) {
        return true;
    }

    // Date/time functions that always return NOT NULL
    if is_not_null_datetime_function(&expr_lower) {
        return true;
    }

    // String/array functions that return NOT NULL (given NOT NULL input assumed)
    if is_not_null_scalar_function(&expr_lower) {
        return true;
    }

    // Literals: numeric, string, boolean
    if is_literal(&expr_lower) {
        return true;
    }

    // CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_USER, etc.
    if expr_lower.starts_with("current_") {
        return true;
    }

    false
}

/// Check if an expression is a literal value (numeric, string, or boolean).
fn is_literal(expr: &str) -> bool {
    let s = expr.trim();
    s.parse::<f64>().is_ok()
        || (s.starts_with('\'') && s.ends_with('\''))
        || s == "true"
        || s == "false"
}

/// Check if a CASE expression has only literal THEN/ELSE branches.
///
/// Matches: `case when ... then 1 else 0 end`, `case when ... then 'a' else 'b' end`
/// Does NOT match if any branch is a column reference or function call.
fn is_case_all_literal_branches(expr: &str) -> bool {
    // Extract all THEN and ELSE values
    let mut rest = expr;
    while let Some(idx) = rest.find(" then ") {
        let after = &rest[idx + 6..];
        // Value runs until next WHEN, ELSE, or END
        let end = after
            .find(" when ")
            .or_else(|| after.find(" else "))
            .or_else(|| after.find(" end"))
            .unwrap_or(after.len());
        let val = after[..end].trim();
        if !is_literal(val) {
            return false;
        }
        rest = &after[end..];
    }
    // Check ELSE
    if let Some(idx) = expr.rfind(" else ") {
        let after = &expr[idx + 6..];
        let end = after.find(" end").unwrap_or(after.len());
        let val = after[..end].trim();
        if !is_literal(val) {
            return false;
        }
    }
    true
}

/// Window functions that are guaranteed to return NOT NULL.
fn is_not_null_window_function(expr: &str) -> bool {
    expr.starts_with("row_number(")
        || expr.starts_with("rank(")
        || expr.starts_with("dense_rank(")
        || expr.starts_with("ntile(")
        || expr.starts_with("cume_dist(")
        || expr.starts_with("percent_rank(")
}

/// Date/time functions guaranteed NOT NULL.
fn is_not_null_datetime_function(expr: &str) -> bool {
    expr.starts_with("now(")
        || expr.starts_with("clock_timestamp(")
        || expr.starts_with("statement_timestamp(")
        || expr.starts_with("transaction_timestamp(")
        || expr == "localtime"
        || expr == "localtimestamp"
        || expr.starts_with("extract(")
        || expr.starts_with("date_part(")
        || expr.starts_with("age(")
        || expr.starts_with("date_trunc(")
}

/// Scalar functions that return NOT NULL given non-NULL arguments.
/// We assume the input is non-NULL here (conservative: only for common patterns).
fn is_not_null_scalar_function(expr: &str) -> bool {
    expr.starts_with("length(")
        || expr.starts_with("char_length(")
        || expr.starts_with("octet_length(")
        || expr.starts_with("lower(")
        || expr.starts_with("upper(")
        || expr.starts_with("trim(")
        || expr.starts_with("ltrim(")
        || expr.starts_with("rtrim(")
        || expr.starts_with("concat(")
        || expr.starts_with("replace(")
        || expr.starts_with("substring(")
        || expr.starts_with("left(")
        || expr.starts_with("right(")
        || expr.starts_with("md5(")
        || expr.starts_with("sha256(")
        || expr.starts_with("encode(")
        || expr.starts_with("decode(")
        || expr.starts_with("abs(")
        || expr.starts_with("ceil(")
        || expr.starts_with("floor(")
        || expr.starts_with("round(")
        || expr.starts_with("trunc(")
        || expr.starts_with("sign(")
        || expr.starts_with("mod(")
        || expr.starts_with("power(")
        || expr.starts_with("sqrt(")
        || expr.starts_with("greatest(")
        || expr.starts_with("least(")
        || expr.starts_with("array_length(")
        || expr.starts_with("cardinality(")
        || expr.starts_with("jsonb_build_object(")
        || expr.starts_with("jsonb_build_array(")
        || expr.starts_with("json_build_object(")
        || expr.starts_with("json_build_array(")
        || expr.starts_with("to_char(")
        || expr.starts_with("to_number(")
        || expr.starts_with("to_date(")
        || expr.starts_with("to_timestamp(")
        || expr.starts_with("gen_random_uuid(")
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
                let plan_text = lines.join("\n");

                // Analyze plan for performance warnings
                let threshold = crate::explain::explain_threshold();
                let warnings = crate::explain::analyze_plan(&plan_text, threshold);
                for warning in &warnings {
                    eprintln!("warning: [bsql] {}", warning.message);
                }

                Some(plan_text)
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

    let columns = build_columns(conn, &result.columns, &variant.sql)?;

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
            code: *b"42P01",
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
            code: *b"42P01",
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
            code: *b"42601",
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
            code: *b"42P01",
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
            code: *b"42601",
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

    // --- is_known_not_null ---

    #[test]
    fn is_known_not_null_count() {
        assert!(is_known_not_null("count", "count(*)"));
        assert!(is_known_not_null("count", "COUNT(id)"));
        assert!(is_known_not_null("total", "count(*)"));
    }

    #[test]
    fn is_known_not_null_coalesce_with_literal() {
        assert!(is_known_not_null("x", "coalesce(name, 'unknown')"));
        assert!(is_known_not_null("x", "COALESCE(a, b, 0)"));
    }

    #[test]
    fn is_known_not_null_coalesce_without_literal() {
        assert!(!is_known_not_null("x", "coalesce(a, b)"));
    }

    #[test]
    fn is_known_not_null_exists() {
        assert!(is_known_not_null("x", "exists(select 1 from t)"));
    }

    #[test]
    fn is_known_not_null_literals() {
        assert!(is_known_not_null("x", "1"));
        assert!(is_known_not_null("x", "'hello'"));
        assert!(is_known_not_null("x", "true"));
        assert!(is_known_not_null("x", "42.5"));
    }

    #[test]
    fn is_known_not_null_current() {
        assert!(is_known_not_null("x", "current_timestamp"));
        assert!(is_known_not_null("x", "current_date"));
    }

    #[test]
    fn is_known_not_null_regular_column() {
        assert!(!is_known_not_null("name", "name"));
        assert!(!is_known_not_null("x", "some_function(a)"));
    }

    // --- parse_select_expressions ---

    #[test]
    fn parse_select_list_simple() {
        let exprs = parse_select_expressions("select id, name from users");
        assert_eq!(exprs, vec!["id", "name"]);
    }

    #[test]
    fn parse_select_list_with_functions() {
        let exprs =
            parse_select_expressions("select count(*), coalesce(name, 'x') as n from users");
        assert_eq!(exprs, vec!["count(*)", "coalesce(name, 'x')"]);
    }

    #[test]
    fn parse_select_list_nested_parens() {
        let exprs =
            parse_select_expressions("select id, sum(case when x > 0 then 1 else 0 end) from t");
        assert_eq!(exprs, vec!["id", "sum(case when x > 0 then 1 else 0 end)"]);
    }

    #[test]
    fn parse_select_list_no_from() {
        // "SELECT 1" has no FROM clause
        let exprs = parse_select_expressions("SELECT 1");
        assert_eq!(exprs, vec!["1"]);
    }

    #[test]
    fn parse_select_list_distinct() {
        let exprs = parse_select_expressions("SELECT DISTINCT id, name FROM t");
        assert_eq!(exprs, vec!["id", "name"]);
    }

    // --- is_known_not_null: aggregate functions that remain nullable ---

    #[test]
    fn sum_remains_nullable() {
        // SUM on an empty group returns NULL
        assert!(!is_known_not_null("total", "sum(col)"));
        assert!(!is_known_not_null("total", "SUM(amount)"));
    }

    #[test]
    fn avg_remains_nullable() {
        assert!(!is_known_not_null("avg", "avg(col)"));
        assert!(!is_known_not_null("average", "AVG(score)"));
    }

    #[test]
    fn max_remains_nullable() {
        assert!(!is_known_not_null("mx", "max(col)"));
        assert!(!is_known_not_null("mx", "MAX(created_at)"));
    }

    #[test]
    fn min_remains_nullable() {
        assert!(!is_known_not_null("mn", "min(col)"));
        assert!(!is_known_not_null("mn", "MIN(id)"));
    }

    #[test]
    fn coalesce_without_literal_remains_nullable() {
        // COALESCE(a, b) where both args are columns — still nullable
        assert!(!is_known_not_null("x", "coalesce(a, b)"));
        assert!(!is_known_not_null("x", "COALESCE(col1, col2)"));
    }

    #[test]
    fn count_distinct_is_not_null() {
        assert!(is_known_not_null("cnt", "count(distinct col)"));
        assert!(is_known_not_null("cnt", "COUNT(DISTINCT id)"));
    }

    #[test]
    fn arithmetic_expression_remains_nullable() {
        // `1 + 1` is an expression, not a single literal — the parser
        // sees "1 + 1" as a whole, which does not match a bare numeric literal
        assert!(!is_known_not_null("x", "1 + 1"));
    }

    #[test]
    fn cast_remains_nullable() {
        assert!(!is_known_not_null("x", "cast(col as integer)"));
        assert!(!is_known_not_null("x", "CAST(name AS TEXT)"));
    }

    #[test]
    fn nested_coalesce_count_is_not_null() {
        // COALESCE(COUNT(*), 0) — COUNT is NOT NULL, plus COALESCE with literal
        // But is_known_not_null checks the outermost expression.
        // It sees "coalesce(count(*), 0)" — COALESCE with literal 0 => NOT NULL
        assert!(is_known_not_null("x", "coalesce(count(*), 0)"));
    }

    #[test]
    fn count_star_not_null() {
        // Redundant but explicit: COUNT(*) is always NOT NULL
        assert!(is_known_not_null("count", "COUNT(*)"));
        assert!(is_known_not_null("x", "count(*)"));
    }

    #[test]
    fn coalesce_with_string_literal_not_null() {
        assert!(is_known_not_null("x", "coalesce(name, 'N/A')"));
    }

    #[test]
    fn coalesce_with_numeric_literal_not_null() {
        assert!(is_known_not_null("x", "coalesce(val, 0)"));
    }

    #[test]
    fn coalesce_with_boolean_literal_not_null() {
        assert!(is_known_not_null("x", "coalesce(flag, false)"));
    }

    // --- parse_select_expressions: more edge cases ---

    #[test]
    fn parse_select_empty_string() {
        let exprs = parse_select_expressions("");
        assert!(exprs.is_empty());
    }

    #[test]
    fn parse_select_star() {
        // SELECT * FROM t — * is the single expression
        let exprs = parse_select_expressions("SELECT * FROM t");
        assert_eq!(exprs, vec!["*"]);
    }

    #[test]
    fn parse_select_subquery_in_from() {
        // SELECT x FROM (SELECT 1 AS x) sub
        // The parser looks for " FROM " at depth 0. The subquery in FROM
        // changes depth, but the outer FROM is at depth 0.
        let exprs = parse_select_expressions("SELECT x FROM (SELECT 1 AS x) sub");
        assert_eq!(exprs, vec!["x"]);
    }

    #[test]
    fn parse_select_case_when() {
        let exprs = parse_select_expressions(
            "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END AS label FROM t",
        );
        assert_eq!(
            exprs,
            vec!["CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END"]
        );
    }

    #[test]
    fn parse_select_mixed_columns_and_aggregates() {
        let exprs =
            parse_select_expressions("SELECT id, COUNT(*), name FROM users GROUP BY id, name");
        assert_eq!(exprs, vec!["id", "COUNT(*)", "name"]);
    }

    #[test]
    fn parse_select_no_select_keyword() {
        // Garbage input — should return empty
        let exprs = parse_select_expressions("INSERT INTO t VALUES (1)");
        assert!(exprs.is_empty());
    }

    // --- is_known_not_null: column name fallback ---

    #[test]
    fn is_known_not_null_column_name_count_fallback() {
        // When select_expr is empty, falls back to col_name
        assert!(is_known_not_null("count", ""));
    }

    #[test]
    fn is_known_not_null_empty_both() {
        // Empty column name and empty expression — not known NOT NULL
        assert!(!is_known_not_null("", ""));
    }

    // --- is_known_not_null: false literal ---

    #[test]
    fn is_known_not_null_false_literal() {
        assert!(is_known_not_null("x", "false"));
    }

    // --- is_known_not_null: COALESCE with negative number ---

    #[test]
    fn is_known_not_null_coalesce_with_negative_number() {
        assert!(is_known_not_null("x", "coalesce(val, -1)"));
    }

    // --- is_known_not_null: COALESCE with floating point literal ---

    #[test]
    fn is_known_not_null_coalesce_with_float_literal() {
        assert!(is_known_not_null("x", "coalesce(val, 0.0)"));
    }

    // --- is_known_not_null: COALESCE with boolean literal ---

    #[test]
    fn is_known_not_null_coalesce_with_true_literal() {
        assert!(is_known_not_null("x", "coalesce(flag, true)"));
    }

    // --- is_known_not_null: EXISTS is always not null ---

    #[test]
    fn is_known_not_null_exists_complex() {
        assert!(is_known_not_null(
            "has_orders",
            "exists(select 1 from orders where user_id = u.id)"
        ));
    }

    // --- is_known_not_null: CURRENT_TIMESTAMP etc ---

    #[test]
    fn is_known_not_null_current_user() {
        assert!(is_known_not_null("x", "current_user"));
    }

    // --- is_known_not_null: string literal ---

    #[test]
    fn is_known_not_null_empty_string_literal() {
        assert!(is_known_not_null("x", "''"));
    }

    // --- is_known_not_null: SUM is nullable ---

    #[test]
    fn sum_of_not_null_column_remains_nullable() {
        // SUM returns NULL for empty groups, even on NOT NULL columns
        assert!(!is_known_not_null("total", "SUM(amount)"));
    }

    // --- parse_select_expressions: trailing semicolon ---

    #[test]
    fn parse_select_with_trailing_semicolon() {
        let exprs = parse_select_expressions("SELECT 1;");
        assert_eq!(exprs, vec!["1"]);
    }

    // --- parse_select_expressions: multiple items no FROM ---

    #[test]
    fn parse_select_multiple_no_from() {
        let exprs = parse_select_expressions("SELECT 1, 'hello', true");
        assert_eq!(exprs, vec!["1", "'hello'", "true"]);
    }

    // --- strip_alias: complex cases ---

    #[test]
    fn strip_alias_simple() {
        assert_eq!(strip_alias("count(*) AS cnt"), "count(*)");
    }

    #[test]
    fn strip_alias_no_alias() {
        assert_eq!(strip_alias("id"), "id");
    }

    #[test]
    fn strip_alias_nested_as_in_parens() {
        // "CASE WHEN status AS thing END AS label" — should strip outer AS
        assert_eq!(
            strip_alias("CASE WHEN x THEN 'a' ELSE 'b' END AS label"),
            "CASE WHEN x THEN 'a' ELSE 'b' END"
        );
    }

    // --- check_params_against_pg: enum param with bool rejected ---

    #[test]
    fn check_params_enum_with_bool_error() {
        let params = vec![Param {
            name: "status".into(),
            rust_type: "bool".into(),
            position: 1,
        }];
        let pg_oids = [99999u32];
        let pg_enum = [true];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("cannot be used for PostgreSQL enum"),
            "should reject bool for enum: {err}"
        );
    }

    // --- check_params_against_pg: multiple params all matching ---

    #[test]
    fn check_params_multiple_matching() {
        let params = vec![
            Param {
                name: "id".into(),
                rust_type: "i32".into(),
                position: 1,
            },
            Param {
                name: "name".into(),
                rust_type: "&str".into(),
                position: 2,
            },
            Param {
                name: "flag".into(),
                rust_type: "bool".into(),
                position: 3,
            },
        ];
        let pg_oids = [23u32, 25, 16]; // int4, text, bool
        let pg_enum = [false, false, false];
        let result = check_params_against_pg(&params, &pg_oids, &pg_enum, false, "");
        assert!(result.is_ok());
    }

    // --- format_driver_error_base: Protocol error ---

    #[test]
    fn format_protocol_error() {
        let err = bsql_driver_postgres::DriverError::Protocol("unexpected msg type 'Z'".into());
        let msg = format_driver_error_base(&err);
        assert!(msg.contains("unexpected msg type"), "error: {msg}");
    }

    // --- is_known_not_null: numeric literal 0 ---

    #[test]
    fn is_known_not_null_zero_literal() {
        assert!(is_known_not_null("x", "0"));
    }

    // --- is_known_not_null: negative number as literal ---

    #[test]
    fn is_known_not_null_negative_number() {
        // "-1" as an expression — parse::<f64>() returns Ok
        assert!(is_known_not_null("x", "-1"));
    }

    // --- is_known_not_null: new patterns ---

    #[test]
    fn case_with_literal_branches_not_null() {
        assert!(is_known_not_null("x", "CASE WHEN a > 0 THEN 1 ELSE 0 END"));
        assert!(is_known_not_null(
            "x",
            "CASE WHEN active THEN 'yes' ELSE 'no' END"
        ));
    }

    #[test]
    fn case_with_column_branch_remains_nullable() {
        assert!(!is_known_not_null(
            "x",
            "CASE WHEN a > 0 THEN name ELSE 'unknown' END"
        ));
    }

    #[test]
    fn row_number_is_not_null() {
        assert!(is_known_not_null("x", "row_number()"));
        assert!(is_known_not_null("x", "rank()"));
        assert!(is_known_not_null("x", "dense_rank()"));
        assert!(is_known_not_null("x", "ntile(4)"));
    }

    #[test]
    fn now_and_datetime_functions_not_null() {
        assert!(is_known_not_null("x", "now()"));
        assert!(is_known_not_null("x", "clock_timestamp()"));
        assert!(is_known_not_null("x", "extract(year from created_at)"));
        assert!(is_known_not_null("x", "date_part('year', created_at)"));
        assert!(is_known_not_null("x", "date_trunc('month', created_at)"));
    }

    #[test]
    fn string_functions_not_null() {
        assert!(is_known_not_null("x", "length(name)"));
        assert!(is_known_not_null("x", "lower(name)"));
        assert!(is_known_not_null("x", "upper(name)"));
        assert!(is_known_not_null("x", "trim(name)"));
        assert!(is_known_not_null("x", "concat(first_name, ' ', last_name)"));
        assert!(is_known_not_null("x", "replace(name, 'old', 'new')"));
    }

    #[test]
    fn math_functions_not_null() {
        assert!(is_known_not_null("x", "abs(amount)"));
        assert!(is_known_not_null("x", "ceil(rating)"));
        assert!(is_known_not_null("x", "floor(rating)"));
        assert!(is_known_not_null("x", "round(price, 2)"));
        assert!(is_known_not_null("x", "greatest(a, b, 0)"));
        assert!(is_known_not_null("x", "least(a, b, 100)"));
    }

    #[test]
    fn array_functions_not_null() {
        assert!(is_known_not_null("x", "array_length(tags, 1)"));
        assert!(is_known_not_null("x", "cardinality(tags)"));
    }

    #[test]
    fn json_build_functions_not_null() {
        assert!(is_known_not_null("x", "jsonb_build_object('key', value)"));
        assert!(is_known_not_null("x", "json_build_array(1, 2, 3)"));
    }

    #[test]
    fn gen_random_uuid_not_null() {
        assert!(is_known_not_null("x", "gen_random_uuid()"));
    }

    #[test]
    fn to_char_and_conversion_functions_not_null() {
        assert!(is_known_not_null("x", "to_char(created_at, 'YYYY-MM-DD')"));
        assert!(is_known_not_null("x", "to_timestamp(epoch_secs)"));
    }

    #[test]
    fn sum_avg_still_nullable() {
        // SUM/AVG return NULL for empty groups — must stay Option
        assert!(!is_known_not_null("x", "sum(amount)"));
        assert!(!is_known_not_null("x", "avg(score)"));
        assert!(!is_known_not_null("x", "max(created_at)"));
        assert!(!is_known_not_null("x", "min(created_at)"));
    }

    #[test]
    fn unknown_function_remains_nullable() {
        assert!(!is_known_not_null("x", "my_custom_func(col)"));
    }
}
