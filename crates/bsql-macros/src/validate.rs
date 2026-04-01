//! Compile-time SQL validation via PostgreSQL PREPARE.
//!
//! Connects to the database specified by `BSQL_DATABASE_URL` and validates
//! each query by preparing it. Introspects column types and nullability
//! from `pg_catalog`.

use smallvec::SmallVec;
use tokio::runtime::Runtime;
use tokio_postgres::Client;

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
/// Uses `client.prepare()` which:
/// 1. Validates SQL syntax
/// 2. Validates table/column existence
/// 3. Returns column metadata and parameter types
pub fn validate_query(
    parsed: &ParsedQuery,
    rt: &Runtime,
    client: &Client,
) -> Result<ValidationResult, String> {
    rt.block_on(validate_async(parsed, client))
}

async fn validate_async(parsed: &ParsedQuery, client: &Client) -> Result<ValidationResult, String> {
    // Prepare the query — this validates syntax, tables, columns, types.
    let stmt = client
        .prepare(&parsed.positional_sql)
        .await
        .map_err(|e| format_pg_error(&e, parsed))?;

    // Extract parameter type OIDs and detect PG enums
    let param_pg_oids: SmallVec<[u32; 8]> = stmt.params().iter().map(|t| t.oid()).collect();
    let param_is_pg_enum: SmallVec<[bool; 8]> = stmt
        .params()
        .iter()
        .map(|t| matches!(t.kind(), postgres_types::Kind::Enum(_)))
        .collect();

    let columns = build_columns(client, stmt.columns()).await?;

    Ok(ValidationResult {
        columns,
        param_pg_oids,
        param_is_pg_enum,
        #[cfg(feature = "explain")]
        explain_plan: fetch_explain_plan(client, parsed).await,
    })
}

/// Resolve column metadata (name, type, nullability) from a prepared statement.
async fn build_columns(
    client: &Client,
    pg_columns: &[tokio_postgres::Column],
) -> Result<Vec<ColumnInfo>, String> {
    let nullable_flags = resolve_nullability_batch(client, pg_columns).await;

    let mut columns = Vec::with_capacity(pg_columns.len());
    for (i, col) in pg_columns.iter().enumerate() {
        let pg_oid = col.type_().oid();
        let pg_type_name = col.type_().name().to_owned();
        let name = col.name().to_owned();
        let is_nullable = nullable_flags[i];

        if matches!(col.type_().kind(), postgres_types::Kind::Enum(_)) {
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
async fn fetch_explain_plan(client: &Client, parsed: &ParsedQuery) -> Option<String> {
    // EXPLAIN cannot handle parameterized queries directly. We use
    // EXPLAIN (FORMAT TEXT) with a generic plan (PG 16+ supports
    // EXPLAIN (GENERIC_PLAN) for prepared statements).
    //
    // For older PG versions, we try EXPLAIN on the raw SQL. If it fails
    // (e.g. because of parameters), we skip silently.
    let explain_sql = format!("EXPLAIN (FORMAT TEXT, COSTS) {}", parsed.positional_sql);

    match client.simple_query(&explain_sql).await {
        Ok(messages) => {
            let lines: Vec<String> = messages
                .iter()
                .filter_map(|msg| {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        row.get(0).map(String::from)
                    } else {
                        None
                    }
                })
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
/// batch using `unnest`. Computed columns (aggregates, functions) default to
/// nullable (the safe choice).
async fn resolve_nullability_batch(
    client: &Client,
    columns: &[tokio_postgres::Column],
) -> Vec<bool> {
    let col_count = columns.len();
    // Default: all nullable (safe). We overwrite entries we can resolve.
    let mut result = vec![true; col_count];

    // Collect (table_oid, column_id) pairs for table-backed columns
    let mut table_oids: Vec<u32> = Vec::new();
    let mut col_nums: Vec<i16> = Vec::new();
    let mut col_indices: Vec<usize> = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        match (col.table_oid(), col.column_id()) {
            (Some(t), Some(c)) if t != 0 && c != 0 => {
                table_oids.push(t);
                col_nums.push(c);
                col_indices.push(i);
            }
            _ => {} // computed → stays true (nullable)
        }
    }

    if table_oids.is_empty() {
        return result;
    }

    // Single batched query: unnest the OID/attnum arrays and join pg_attribute
    let query = "\
        SELECT a.attrelid, a.attnum, NOT a.attnotnull \
        FROM pg_attribute a \
        WHERE (a.attrelid, a.attnum) IN (\
            SELECT unnest($1::oid[]), unnest($2::int2[])\
        )";

    if let Ok(rows) = client.query(query, &[&table_oids, &col_nums]).await {
        // Build lookup: (table_oid, col_num) -> original column index
        let mut lookup: std::collections::HashMap<(u32, i16), Vec<usize>> =
            std::collections::HashMap::with_capacity(table_oids.len());
        for (idx, (&t, &c)) in table_oids.iter().zip(col_nums.iter()).enumerate() {
            lookup.entry((t, c)).or_default().push(col_indices[idx]);
        }

        for row in &rows {
            let oid: u32 = row.get(0);
            let num: i16 = row.get(1);
            let is_nullable: bool = row.get(2);
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
pub fn validate_variants(
    variants: &[QueryVariant],
    parsed: &ParsedQuery,
    rt: &Runtime,
    client: &Client,
) -> Result<ValidationResult, String> {
    if variants.len() <= 1 {
        // Single variant or no optional clauses — use normal validation
        return validate_query(parsed, rt, client);
    }

    // Validate every variant and collect results.
    // All variants must produce the same column set.
    let mut canonical_result: Option<ValidationResult> = None;

    for (i, variant) in variants.iter().enumerate() {
        let result = rt.block_on(validate_variant_async(variant, client, parsed, i))?;

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

async fn validate_variant_async(
    variant: &QueryVariant,
    client: &Client,
    parsed: &ParsedQuery,
    variant_index: usize,
) -> Result<ValidationResult, String> {
    let stmt = client
        .prepare(&variant.sql)
        .await
        .map_err(|e| format_variant_error(&e, variant, parsed, variant_index))?;

    let param_pg_oids: SmallVec<[u32; 8]> = stmt.params().iter().map(|t| t.oid()).collect();
    let param_is_pg_enum: SmallVec<[bool; 8]> = stmt
        .params()
        .iter()
        .map(|t| matches!(t.kind(), postgres_types::Kind::Enum(_)))
        .collect();

    let columns = build_columns(client, stmt.columns()).await?;

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

/// Extract the common parts of a PostgreSQL error: message, detail, hint.
fn format_db_error_base(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        let detail = db_err.detail().unwrap_or("");
        let hint = db_err.hint().unwrap_or("");
        let mut out = format!("PostgreSQL error: {}", db_err.message());
        if !detail.is_empty() {
            out.push_str(&format!("\n  detail: {detail}"));
        }
        if !hint.is_empty() {
            out.push_str(&format!("\n  hint: {hint}"));
        }
        out
    } else {
        format!("PostgreSQL error: {e}")
    }
}

/// Format a variant-specific PostgreSQL error with context about which
/// clause combination caused the failure.
fn format_variant_error(
    e: &tokio_postgres::Error,
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

    let base_msg = format_db_error_base(e);
    format!(
        "optional clause variant {} ({clause_desc}) produces invalid SQL:\n  \
         {base_msg}\n  SQL: {}",
        variant_index, variant.sql
    )
}

/// Format a PostgreSQL error into a developer-friendly compile error message.
fn format_pg_error(e: &tokio_postgres::Error, parsed: &ParsedQuery) -> String {
    let mut out = format_db_error_base(e);

    if let Some(db_err) = e.as_db_error() {
        if let Some(pos) = db_err.position() {
            out.push_str(&format!("\n  position: {pos:?}"));
        }
    }
    out.push_str(&format!("\n  SQL: {}", parsed.positional_sql));

    out
}
