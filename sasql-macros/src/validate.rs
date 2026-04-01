//! Compile-time SQL validation via PostgreSQL PREPARE.
//!
//! Connects to the database specified by `SASQL_DATABASE_URL` and validates
//! each query by preparing it. Introspects column types and nullability
//! from `pg_catalog`.

use tokio::runtime::Runtime;
use tokio_postgres::Client;

use crate::parse::ParsedQuery;

/// Metadata about a single result column, resolved from PostgreSQL.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name as returned by PostgreSQL.
    pub name: String,
    /// PostgreSQL type OID.
    #[allow(dead_code)] // retained for diagnostics and future error messages
    pub pg_oid: u32,
    /// PostgreSQL type name (e.g. `"int4"`, `"text"`).
    #[allow(dead_code)] // retained for diagnostics and future error messages
    pub pg_type_name: String,
    /// Whether this column can be NULL.
    #[allow(dead_code)] // retained for diagnostics; nullability is baked into rust_type
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
    pub param_pg_oids: Vec<u32>,
    /// Whether each parameter type is a PostgreSQL enum (custom type).
    /// When true, `&str`/`String` params are accepted in addition to
    /// any `#[sasql::pg_enum]`-annotated Rust enum.
    pub param_is_pg_enum: Vec<bool>,
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
    let param_pg_oids: Vec<u32> = stmt.params().iter().map(|t| t.oid()).collect();
    let param_is_pg_enum: Vec<bool> = stmt
        .params()
        .iter()
        .map(|t| matches!(t.kind(), postgres_types::Kind::Enum(_)))
        .collect();

    // Resolve nullability for ALL columns in a single batched query
    let nullable_flags = resolve_nullability_batch(client, stmt.columns()).await;

    // Build column metadata
    let mut columns = Vec::with_capacity(stmt.columns().len());
    for (i, col) in stmt.columns().iter().enumerate() {
        let pg_oid = col.type_().oid();
        let pg_type_name = col.type_().name().to_owned();
        let name = col.name().to_owned();
        let is_nullable = nullable_flags[i];

        // Custom PG enums (Kind::Enum) map to EnumString at the type level.
        // EnumString accepts Kind::Enum in its FromSql impl.
        // Users who want typed enums should use #[sasql::pg_enum].
        let is_pg_enum = matches!(col.type_().kind(), postgres_types::Kind::Enum(_));

        let base_rust_type = if is_pg_enum {
            "::sasql_core::types::EnumString"
        } else {
            crate::types::resolve_rust_type(pg_oid)
                .map_err(|msg| format!("column \"{name}\": {msg}"))?
        };

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

    Ok(ValidationResult {
        columns,
        param_pg_oids,
        param_is_pg_enum,
    })
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
        for row in &rows {
            let oid: u32 = row.get(0);
            let num: i16 = row.get(1);
            let is_nullable: bool = row.get(2);
            // Match back to the original column index
            for (idx, (&t, &c)) in table_oids.iter().zip(col_nums.iter()).enumerate() {
                if t == oid && c == num {
                    result[col_indices[idx]] = is_nullable;
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
    if parsed.params.len() != validation.param_pg_oids.len() {
        return Err(format!(
            "parameter count mismatch: query has {} parameters but PostgreSQL \
             expects {}. Check your $name: Type declarations.",
            parsed.params.len(),
            validation.param_pg_oids.len()
        ));
    }

    for (i, (param, &pg_oid)) in parsed
        .params
        .iter()
        .zip(&validation.param_pg_oids)
        .enumerate()
    {
        let is_pg_enum = validation.param_is_pg_enum.get(i).copied().unwrap_or(false);

        // PG enum params accept &str/String (text representation)
        if is_pg_enum {
            if matches!(param.rust_type.as_str(), "&str" | "String") {
                continue;
            }
            // Also accept any type — if the user passes a pg_enum type, ToSql
            // handles it at runtime. We can't verify custom enum types at compile
            // time since their OIDs are dynamic.
            continue;
        }

        if !crate::types::is_param_compatible_extended(&param.rust_type, pg_oid) {
            let pg_name = sasql_core::types::pg_name_for_oid(pg_oid).unwrap_or("unknown");
            // Provide a better error for feature-gated types
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

/// Format a PostgreSQL error into a developer-friendly compile error message.
fn format_pg_error(e: &tokio_postgres::Error, parsed: &ParsedQuery) -> String {
    let msg = e.to_string();

    // Extract the PostgreSQL error code if available
    if let Some(db_err) = e.as_db_error() {
        let detail = db_err.detail().unwrap_or("");
        let hint = db_err.hint().unwrap_or("");
        let position = db_err.position();

        let mut out = format!("PostgreSQL error: {}", db_err.message());

        if !detail.is_empty() {
            out.push_str(&format!("\n  detail: {detail}"));
        }
        if !hint.is_empty() {
            out.push_str(&format!("\n  hint: {hint}"));
        }
        if let Some(pos) = position {
            out.push_str(&format!("\n  position: {pos:?}"));
            out.push_str(&format!("\n  SQL: {}", parsed.positional_sql));
        }

        out
    } else {
        format!("PostgreSQL error: {msg}\n  SQL: {}", parsed.positional_sql)
    }
}
