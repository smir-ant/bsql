#![forbid(unsafe_code)]

//! Proc macros for bsql.
//!
//! This crate is an implementation detail. Use [`bsql`] instead.

extern crate proc_macro;

mod codegen;
#[cfg(feature = "sqlite")]
mod codegen_sqlite;
mod connection;
mod dynamic;
#[cfg(feature = "explain")]
mod explain;
mod offline;
mod parse;
mod pg_enum;
mod sort_enum;
mod sql_norm;
mod stmt_name;
mod suggest;
mod test_macro;
pub(crate) mod types;
#[cfg(feature = "sqlite")]
mod types_sqlite;
mod validate;
#[cfg(feature = "sqlite")]
mod validate_sqlite;

use proc_macro::TokenStream;

/// Validate a SQL query against PostgreSQL at compile time and generate
/// typed Rust code for executing it.
///
/// # Syntax
///
/// ```text
/// bsql::query! {
///     SELECT column1, column2
///     FROM table
///     WHERE column1 = $param_name: RustType
/// }
/// ```
///
/// Parameters are declared inline as `$name: Type`. The macro replaces them
/// with positional `$1`, `$2`, ... and verifies type compatibility against
/// the database schema.
///
/// # Execution methods
///
/// The macro returns an executor with these methods:
/// - `.fetch_one(executor)` — returns exactly one row (errors on 0 or 2+)
/// - `.fetch_all(executor)` — returns all rows as `Vec<T>`
/// - `.fetch_optional(executor)` — returns `Option<T>` (errors on 2+)
/// - `.execute(executor)` — returns affected row count (`u64`)
///
/// # Compile-time guarantees
///
/// - Table and column names are verified against the live database
/// - Parameter types are checked against PostgreSQL's expected types
/// - Nullable columns are automatically mapped to `Option<T>`
/// - Invalid SQL produces a compile error, not a runtime error
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let input2: proc_macro2::TokenStream = input.into();
    match query_impl(input2) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn query_impl(input: proc_macro2::TokenStream) -> Result<proc_macro2::TokenStream, syn::Error> {
    // Extract the SQL string from the input.
    // Accepts either a string literal: query!("SELECT ...")
    // or raw tokens: query! { SELECT ... } converted to string.
    let sql = extract_sql(input)?;

    // 1. Parse: extract params, query kind, normalize SQL, optional clauses, sort placeholder
    let parsed = parse::parse_query(&sql)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

    // Detect backend from database URL (if not offline)
    #[cfg(feature = "sqlite")]
    {
        let backend = connection::detect_backend()
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;
        if backend == Some(connection::Backend::Sqlite) {
            return query_impl_sqlite(parsed);
        }
    }

    // PostgreSQL path (default)
    query_impl_postgres(parsed)
}

/// PostgreSQL query implementation (the original path).
fn query_impl_postgres(parsed: parse::ParsedQuery) -> Result<proc_macro2::TokenStream, syn::Error> {
    // 2. Sort query path — $[sort: EnumType] present
    if parsed.sort_placeholder.is_some() {
        return query_impl_sort(parsed);
    }

    if parsed.optional_clauses.is_empty() {
        // Static query path — no optional clauses
        let validation = if offline::is_offline() {
            // OFFLINE: read cached validation result
            offline::lookup_cached_validation(&parsed)
                .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
        } else {
            // ONLINE: validate against PostgreSQL via PREPARE with suggestions
            let result = connection::with_connection(|conn| {
                validate::validate_query_with_suggestions(&parsed, conn)
            })?;

            // Write to offline cache for future use
            offline::write_cache(&parsed, &result);

            result
        };

        // Check parameter type compatibility
        validate::check_param_types(&parsed, &validation)
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

        // Generate Rust code
        Ok(codegen::generate_query_code(&parsed, &validation))
    } else {
        // Dynamic query path — has optional clauses.
        //
        // Validation: O(N+1) PREPAREs — base query + one per clause.
        // Codegen: O(N) runtime SQL builder (no 2^N match arms).
        let validation = if offline::is_offline() {
            // OFFLINE: read cached validation result for the base query.
            //
            // The cache stores the base query's param_pg_oids (not optional
            // clause params). Param type checking is skipped here because:
            //  1. The online build already validated all clauses' param types.
            //  2. The cached columns are identical (SELECT list never changes).
            //  3. Codegen only needs the column info, not per-clause param OIDs.
            offline::lookup_cached_validation(&parsed)
                .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
        } else {
            // ONLINE: full 2^N validation — every combination checked.
            // "If it compiles, the SQL is correct" — no exceptions.
            let result = connection::with_connection(|conn| {
                let variants = dynamic::expand_variants(&parsed)?;
                validate::validate_variants(&variants, &parsed, conn)
            })?;

            // Write to offline cache for future use
            offline::write_cache(&parsed, &result);

            result
        };

        // Generate dynamic Rust code with runtime SQL dispatcher
        Ok(codegen::generate_dynamic_query_code(&parsed, &validation))
    }
}

/// SQLite query implementation.
///
/// Validates against a live SQLite database at compile time, then generates
/// code that executes via `bsql_core::SqlitePool`.
#[cfg(feature = "sqlite")]
fn query_impl_sqlite(parsed: parse::ParsedQuery) -> Result<proc_macro2::TokenStream, syn::Error> {
    // Sort queries: $[sort: EnumType] present
    if parsed.sort_placeholder.is_some() {
        return query_impl_sqlite_sort(parsed);
    }

    if parsed.optional_clauses.is_empty() {
        // Static query path — no optional clauses
        let validation = if offline::is_offline() {
            offline::lookup_cached_validation(&parsed)
                .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
        } else {
            let result = connection::with_sqlite_connection(|conn| {
                validate_sqlite::validate_query_sqlite(&parsed, conn)
            })?;

            // Write to offline cache for future use
            offline::write_cache(&parsed, &result);

            result
        };

        // SQLite doesn't type parameters at prepare time, so we skip
        // the PG-style param type check. Parameter types are verified
        // at runtime by the SqliteEncode trait.

        Ok(codegen_sqlite::generate_sqlite_query_code(
            &parsed,
            &validation,
        ))
    } else {
        // Dynamic query path — has optional clauses.
        // Validation: O(N+1) — base + each clause individually.
        // Codegen: O(N) runtime SQL builder.
        let validation = if offline::is_offline() {
            offline::lookup_cached_validation(&parsed)
                .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
        } else {
            // Full 2^N validation — every combination checked.
            let result = connection::with_sqlite_connection(|conn| {
                let variants = dynamic::expand_variants(&parsed)?;
                validate_sqlite::validate_variants_sqlite(&variants, &parsed, conn)
            })?;

            offline::write_cache(&parsed, &result);

            result
        };

        Ok(codegen_sqlite::generate_dynamic_sqlite_query_code(
            &parsed,
            &validation,
        ))
    }
}

/// SQLite sort query implementation.
#[cfg(feature = "sqlite")]
fn query_impl_sqlite_sort(
    parsed: parse::ParsedQuery,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let sort_placeholder = parsed.sort_placeholder.as_ref().unwrap();
    let sort_enum_name = &sort_placeholder.enum_name;

    // Replace {SORT} with "1" to validate the query shape
    let dummy_sql = parsed.positional_sql.replace("{SORT}", "1");

    let dummy_parsed = parse::ParsedQuery {
        normalized_sql: parsed.normalized_sql.replace("{sort}", "1"),
        positional_sql: dummy_sql,
        params: parsed.params.clone(),
        kind: parsed.kind,
        statement_name: parsed.statement_name.clone(),
        optional_clauses: parsed.optional_clauses.clone(),
        sort_placeholder: None,
    };

    let validation = if offline::is_offline() {
        offline::lookup_cached_validation(&parsed)
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
    } else {
        let result = connection::with_sqlite_connection(|conn| {
            validate_sqlite::validate_query_sqlite(&dummy_parsed, conn)
        })?;

        offline::write_cache(&parsed, &result);
        result
    };

    Ok(codegen_sqlite::generate_sort_sqlite_query_code(
        &parsed,
        &validation,
        sort_enum_name,
    ))
}

/// Handle sort queries — queries with `$[sort: EnumType]`.
///
/// The sort enum is NOT resolved at macro expansion time (we don't have access
/// to the enum definition from within the proc macro). Instead, we generate code
/// that takes the sort enum as a parameter and uses `match` to select the SQL.
///
/// Validation: we validate each sort variant's expanded SQL at compile time
/// by reading sort variant info. However, since the sort enum is defined via
/// `#[bsql::sort]` in user code, we cannot read its variants from within
/// the `query!` macro. Instead, the generated code uses the enum's `sql()`
/// method at runtime. Validation of individual sort fragments happens when
/// the user compiles — the sort enum's SQL fragments are checked by the user
/// running their tests or by a separate validation step.
///
/// For now: generate code that takes a `sort` parameter with a `sql() -> &str`
/// method, and splices the SQL at runtime via string replacement + pre-hashed
/// dispatch.
fn query_impl_sort(parsed: parse::ParsedQuery) -> Result<proc_macro2::TokenStream, syn::Error> {
    let sort_placeholder = parsed.sort_placeholder.as_ref().unwrap();
    let sort_enum_name = &sort_placeholder.enum_name;

    // Validate the base query shape with a dummy ORDER BY 1.
    let dummy_sql = parsed.positional_sql.replace("{SORT}", "1");

    // Create a temporary ParsedQuery with the dummy SQL for validation
    let dummy_parsed = parse::ParsedQuery {
        normalized_sql: parsed.normalized_sql.replace("{sort}", "1"),
        positional_sql: dummy_sql,
        params: parsed.params.clone(),
        kind: parsed.kind,
        statement_name: parsed.statement_name.clone(),
        optional_clauses: parsed.optional_clauses.clone(),
        sort_placeholder: None,
    };

    let validation = if offline::is_offline() {
        offline::lookup_cached_validation(&parsed)
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
    } else {
        let result = connection::with_connection(|conn| {
            validate::validate_query_with_suggestions(&dummy_parsed, conn)
        })?;

        // Validate each sort fragment by PREPARE'ing the full query with it spliced in.
        // Read fragments from .bsql/sorts/{EnumName}.txt (written by #[bsql::sort]).
        let sorts_dir = std::env::var("CARGO_MANIFEST_DIR")
            .map(|d| std::path::PathBuf::from(d).join(".bsql").join("sorts"))
            .ok();
        if let Some(sorts_dir) = sorts_dir {
            let path = sorts_dir.join(format!("{}.txt", sort_enum_name));
            if let Ok(content) = std::fs::read_to_string(&path) {
                connection::with_connection(|conn| {
                    for fragment in content.lines().filter(|l| !l.is_empty()) {
                        let test_sql = parsed.positional_sql.replace("{SORT}", fragment);
                        let prepare = format!("PREPARE __bsql_sort_check AS {}", test_sql);
                        if let Err(e) = conn.simple_query(&prepare) {
                            return Err(format!("sort fragment '{}' is invalid: {}", fragment, e));
                        }
                        let _ = conn.simple_query("DEALLOCATE __bsql_sort_check");
                    }
                    Ok(())
                })?;
            }
        }

        offline::write_cache(&parsed, &result);
        result
    };

    validate::check_param_types(&parsed, &validation)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

    // Generate sort-aware code
    Ok(codegen::generate_sort_query_code(
        &parsed,
        &validation,
        sort_enum_name,
    ))
}

/// Map query results into a user-defined struct at compile time.
///
/// # Syntax
///
/// ```text
/// bsql::query_as!(MyStruct, "SELECT col1, col2 FROM table WHERE col1 = $param: Type")
/// ```
///
/// The first argument is the target type (a path like `User` or `crate::models::User`).
/// The second argument is the SQL string with inline parameters (same syntax as `query!`).
///
/// Unlike `query!` which generates an anonymous struct, `query_as!` maps results
/// directly into the named struct. Field names must match column names, and rustc
/// verifies field types via struct literal construction — no runtime checks needed.
///
/// # Execution methods
///
/// Same as `query!`: `.fetch_one(executor)`, `.fetch_all(executor)`,
/// `.fetch_optional(executor)`, `.execute(executor)`, `.run(executor)`, `.defer(tx)`.
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let input2: proc_macro2::TokenStream = input.into();
    match query_as_impl(input2) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Arguments for `query_as!`: target type path + SQL string.
struct QueryAsArgs {
    target_type: syn::Path,
    _comma: syn::Token![,],
    sql: syn::LitStr,
}

impl syn::parse::Parse for QueryAsArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(QueryAsArgs {
            target_type: input.parse()?,
            _comma: input.parse()?,
            sql: input.parse()?,
        })
    }
}

fn extract_type_and_sql(
    input: proc_macro2::TokenStream,
) -> Result<(syn::Path, String), syn::Error> {
    let args: QueryAsArgs = syn::parse2(input)?;
    Ok((args.target_type, args.sql.value()))
}

fn query_as_impl(input: proc_macro2::TokenStream) -> Result<proc_macro2::TokenStream, syn::Error> {
    let (target_type, sql) = extract_type_and_sql(input)?;

    let parsed = parse::parse_query(&sql)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

    // Reject sort queries — query_as! does not support $[sort: ...] placeholders
    if parsed.sort_placeholder.is_some() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "query_as! does not support $[sort: ...] placeholders; use query! instead",
        ));
    }

    // Reject dynamic queries with optional clauses (for now)
    if !parsed.optional_clauses.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "query_as! does not support optional clauses; use query! instead",
        ));
    }

    // Detect backend from database URL
    #[cfg(feature = "sqlite")]
    {
        let backend = connection::detect_backend()
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;
        if backend == Some(connection::Backend::Sqlite) {
            return query_as_impl_sqlite(parsed, target_type);
        }
    }

    // PostgreSQL path (default)
    query_as_impl_postgres(parsed, target_type)
}

fn query_as_impl_postgres(
    parsed: parse::ParsedQuery,
    target_type: syn::Path,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let validation = if offline::is_offline() {
        offline::lookup_cached_validation(&parsed)
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
    } else {
        let result = connection::with_connection(|conn| {
            validate::validate_query_with_suggestions(&parsed, conn)
        })?;

        offline::write_cache(&parsed, &result);
        result
    };

    validate::check_param_types(&parsed, &validation)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

    Ok(codegen::generate_query_as_code(
        &parsed,
        &validation,
        &target_type,
    ))
}

#[cfg(feature = "sqlite")]
fn query_as_impl_sqlite(
    parsed: parse::ParsedQuery,
    target_type: syn::Path,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let validation = if offline::is_offline() {
        offline::lookup_cached_validation(&parsed)
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
    } else {
        let result = connection::with_sqlite_connection(|conn| {
            validate_sqlite::validate_query_sqlite(&parsed, conn)
        })?;

        offline::write_cache(&parsed, &result);
        result
    };

    Ok(codegen_sqlite::generate_sqlite_query_as_code(
        &parsed,
        &validation,
        &target_type,
    ))
}

/// Extract the SQL text from the macro input.
///
/// Accepts a string literal: `query!("SELECT ...")`
fn extract_sql(input: proc_macro2::TokenStream) -> Result<String, syn::Error> {
    let lit: syn::LitStr = syn::parse2(input)?;
    Ok(lit.value())
}

/// Derive PostgreSQL enum <-> Rust enum mapping with `FromSql` and `ToSql`.
///
/// # Usage
///
/// ```rust,ignore
/// #[bsql::pg_enum]
/// pub enum TicketStatus {
///     #[sql("new")]
///     New,
///     #[sql("in_progress")]
///     InProgress,
///     #[sql("resolved")]
///     Resolved,
///     #[sql("closed")]
///     Closed,
/// }
/// ```
///
/// Each variant must have a `#[sql("label")]` attribute mapping it to the
/// exact PostgreSQL enum label. The macro generates:
/// - `FromSql` — deserializes from PostgreSQL text representation
/// - `ToSql` — serializes to PostgreSQL text representation
/// - `Display` — formats as the SQL label
/// - Derives: `Debug, Clone, Copy, PartialEq, Eq, Hash`
///
/// If PostgreSQL sends a variant not present in the Rust enum, `FromSql`
/// returns an error describing the schema mismatch.
#[proc_macro_attribute]
pub fn pg_enum(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2: proc_macro2::TokenStream = attr.into();
    let item2: proc_macro2::TokenStream = item.into();
    match pg_enum::expand_pg_enum(attr2, item2) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Define a sort enum for compile-time verified dynamic `ORDER BY` clauses.
///
/// # Usage
///
/// ```rust,ignore
/// #[bsql::sort]
/// pub enum TicketSort {
///     #[sql("t.updated_at DESC, t.id DESC")]
///     UpdatedAt,
///     #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
///     Deadline,
///     #[sql("t.id DESC")]
///     Id,
/// }
/// ```
///
/// Use with the `$[sort: EnumType]` placeholder in `bsql::query!`:
///
/// ```rust,ignore
/// let tickets = bsql::query!(
///     "SELECT id, title FROM tickets ORDER BY $[sort: TicketSort] LIMIT $limit: i64"
/// ).fetch_all(&pool)?;
/// ```
///
/// Each variant must have a `#[sql("...")]` attribute mapping it to the
/// SQL `ORDER BY` fragment. The macro generates:
/// - The enum with `Debug, Clone, Copy, PartialEq, Eq, Hash`
/// - A `sql(&self) -> &'static str` method returning the SQL fragment
/// - `Display` — formats as the SQL fragment
///
/// Unlike `#[bsql::pg_enum]`, sort enums are NOT parameterized values.
/// The SQL fragment is spliced directly into the query string.
#[proc_macro_attribute]
pub fn sort(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2: proc_macro2::TokenStream = attr.into();
    let item2: proc_macro2::TokenStream = item.into();
    match sort_enum::expand_sort_enum(attr2, item2) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Attribute macro for database integration tests with schema isolation.
///
/// Creates an isolated PostgreSQL schema per test, applies SQL fixtures,
/// passes a connected `Pool` to the test function, and drops the schema
/// after the test completes (even on panic).
///
/// # Usage
///
/// ```rust,ignore
/// #[bsql::test]
/// async fn test_basic(pool: bsql::Pool) {
///     pool.raw_execute("SELECT 1").await.unwrap();
/// }
///
/// #[bsql::test(fixtures("schema", "seed"))]
/// async fn test_with_fixtures(pool: bsql::Pool) {
///     let user = bsql::query!("SELECT name FROM users WHERE id = $id: i32")
///         .fetch_one(&pool).await.unwrap();
///     assert_eq!(user.name, "Alice");
/// }
/// ```
///
/// # Fixtures
///
/// Fixture names are resolved to SQL files at compile time from:
/// - `{CARGO_MANIFEST_DIR}/fixtures/{name}.sql`
/// - `{CARGO_MANIFEST_DIR}/tests/fixtures/{name}.sql`
///
/// Fixtures are applied in order after the isolated schema is created.
///
/// # Environment
///
/// Requires `BSQL_DATABASE_URL` or `DATABASE_URL` to be set at runtime.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2: proc_macro2::TokenStream = attr.into();
    let item2: proc_macro2::TokenStream = item.into();
    match test_macro::expand_test(attr2, item2) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_type_and_sql, QueryAsArgs};

    #[test]
    fn parse_query_as_args() {
        let tokens: proc_macro2::TokenStream = "User, \"SELECT id FROM users\"".parse().unwrap();
        let args: QueryAsArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.sql.value(), "SELECT id FROM users");
        // target_type should be "User"
        let last_segment = args.target_type.segments.last().unwrap().ident.to_string();
        assert_eq!(last_segment, "User");
    }

    #[test]
    fn parse_query_as_args_module_path() {
        let tokens: proc_macro2::TokenStream = "crate::models::User, \"SELECT id FROM users\""
            .parse()
            .unwrap();
        let args: QueryAsArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.sql.value(), "SELECT id FROM users");
        let segments: Vec<String> = args
            .target_type
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect();
        assert_eq!(segments, vec!["crate", "models", "User"]);
    }

    #[test]
    fn extract_type_and_sql_basic() {
        let tokens: proc_macro2::TokenStream = "Row, \"SELECT name FROM t WHERE id = $id: i32\""
            .parse()
            .unwrap();
        let (path, sql) = extract_type_and_sql(tokens).unwrap();
        assert_eq!(sql, "SELECT name FROM t WHERE id = $id: i32");
        assert_eq!(path.segments.last().unwrap().ident.to_string(), "Row");
    }

    #[test]
    fn extract_type_and_sql_missing_comma_fails() {
        let tokens: proc_macro2::TokenStream = "User \"SELECT id FROM t\"".parse().unwrap();
        assert!(extract_type_and_sql(tokens).is_err());
    }

    #[test]
    fn extract_type_and_sql_missing_sql_fails() {
        let tokens: proc_macro2::TokenStream = "User,".parse().unwrap();
        assert!(extract_type_and_sql(tokens).is_err());
    }
}
