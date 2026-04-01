#![forbid(unsafe_code)]

//! Proc macros for sasql.
//!
//! This crate is an implementation detail. Use [`sasql`] instead.

extern crate proc_macro;

mod codegen;
mod connection;
mod dynamic;
mod offline;
mod parse;
mod pg_enum;
mod sql_norm;
mod stmt_name;
pub(crate) mod types;
mod validate;

use proc_macro::TokenStream;

/// Validate a SQL query against PostgreSQL at compile time and generate
/// typed Rust code for executing it.
///
/// # Syntax
///
/// ```text
/// sasql::query! {
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

    // 1. Parse: extract params, query kind, normalize SQL, optional clauses
    let parsed = parse::parse_query(&sql)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

    // 2. Expand dynamic query variants (if any optional clauses)
    let variants = dynamic::expand_variants(&parsed)
        .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;

    if parsed.optional_clauses.is_empty() {
        // Static query path — no optional clauses
        let validation = if offline::is_offline() {
            // OFFLINE: read cached validation result
            offline::lookup_cached_validation(&parsed)
                .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
        } else {
            // ONLINE: validate against PostgreSQL via PREPARE
            let result = connection::with_connection(|rt, client| {
                validate::validate_query(&parsed, rt, client)
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
        // Dynamic query path — has optional clauses
        let validation = if offline::is_offline() {
            // OFFLINE: read cached validation result for the base variant
            // Dynamic queries cache the canonical (variant 0) result
            offline::lookup_cached_validation(&parsed)
                .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?
        } else {
            // ONLINE: validate ALL variants against PostgreSQL and check param types
            let result = connection::with_connection(|rt, client| {
                validate::validate_variants(&variants, &parsed, rt, client)
            })?;

            // Write to offline cache for future use
            offline::write_cache(&parsed, &result);

            result
        };

        // Generate dynamic Rust code with match dispatcher
        Ok(codegen::generate_dynamic_query_code(
            &parsed,
            &validation,
            &variants,
        ))
    }
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
/// #[sasql::pg_enum]
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
