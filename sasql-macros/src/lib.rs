#![forbid(unsafe_code)]

//! Proc macros for sasql.
//!
//! This crate is an implementation detail. Use [`sasql`] instead.

extern crate proc_macro;

mod codegen;
mod connection;
mod parse;
mod sql_norm;
mod stmt_name;
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
    let input_str = input.to_string();
    match query_impl(&input_str) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn query_impl(sql: &str) -> Result<proc_macro2::TokenStream, syn::Error> {
    // 1. Parse: extract params, query kind, normalize SQL
    let parsed = parse::parse_query(sql).map_err(|msg| {
        syn::Error::new(proc_macro2::Span::call_site(), msg)
    })?;

    // 2. Check param declarations (duplicates, etc.) — no PG needed
    validate::check_param_declarations(&parsed.params).map_err(|msg| {
        syn::Error::new(proc_macro2::Span::call_site(), msg)
    })?;

    // 3. Validate against PostgreSQL via PREPARE
    let validation = connection::with_connection(|rt, client| {
        validate::validate_query(&parsed, rt, client)
    })?;

    // 4. Check parameter type compatibility
    validate::check_param_types(&parsed, &validation).map_err(|msg| {
        syn::Error::new(proc_macro2::Span::call_site(), msg)
    })?;

    // 5. Generate Rust code
    Ok(codegen::generate_query_code(&parsed, &validation))
}
