//! Code generation for `sasql::query!`.
//!
//! Given a parsed and validated query, generates a Rust expression that:
//! 1. Defines a result struct with typed fields
//! 2. Defines an executor struct that captures parameters
//! 3. Implements `fetch_one`, `fetch_all`, `fetch_optional`, `execute` methods
//! 4. Evaluates to the executor struct (enables the chaining syntax)

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::parse::ParsedQuery;
use crate::validate::ValidationResult;

/// Generate the complete Rust code for a `query!` invocation.
pub fn generate_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> TokenStream {
    let result_struct = gen_result_struct(parsed, validation);
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_executor_impls(parsed, validation);
    let constructor = gen_constructor(parsed);

    quote! {
        {
            #result_struct
            #executor_struct
            #executor_impls
            #constructor
        }
    }
}

/// Generate the result struct (the rows returned by SELECT / RETURNING).
fn gen_result_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new(); // no result struct for execute-only queries
    }

    let struct_name = result_struct_name(parsed);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", sanitize_column_name(&col.name, i));
        let field_type = parse_result_type(&col.rust_type);
        quote! { pub #field_name: #field_type }
    });

    quote! {
        #[derive(Debug)]
        #[allow(non_camel_case_types)]
        pub struct #struct_name {
            #(#fields,)*
        }
    }
}

/// Generate the executor struct (captures query parameters).
fn gen_executor_struct(parsed: &ParsedQuery) -> TokenStream {
    let struct_name = executor_struct_name(parsed);
    let needs_lifetime = has_reference_params(parsed);

    if parsed.params.is_empty() {
        quote! {
            #[allow(non_camel_case_types)]
            struct #struct_name;
        }
    } else {
        let fields = parsed.params.iter().map(|p| {
            let name = format_ident!("{}", p.name);
            let ty = parse_rust_type(&p.rust_type);
            quote! { #name: #ty }
        });

        if needs_lifetime {
            quote! {
                #[allow(non_camel_case_types)]
                struct #struct_name<'_sasql> {
                    #(#fields,)*
                    _marker: ::std::marker::PhantomData<&'_sasql ()>,
                }
            }
        } else {
            quote! {
                #[allow(non_camel_case_types)]
                struct #struct_name {
                    #(#fields,)*
                }
            }
        }
    }
}

/// Generate `fetch_one`, `fetch_all`, `fetch_optional`, `execute` methods.
fn gen_executor_impls(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let sql_lit = &parsed.positional_sql;
    let needs_lifetime = has_reference_params(parsed);

    // Build the params slice: &[&self.id, &self.name, ...]
    let param_refs: Vec<TokenStream> = parsed.params.iter().map(|p| {
        let name = format_ident!("{}", p.name);
        quote! { &self.#name as &(dyn ::sasql_core::pg::ToSql + Sync) }
    }).collect();

    let params_slice = if param_refs.is_empty() {
        quote! { &[] }
    } else {
        quote! { &[#(#param_refs),*] }
    };

    let has_columns = !validation.columns.is_empty();

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let row_decode = gen_row_decode(validation);

        quote! {
            pub async fn fetch_one<E: ::sasql_core::Executor>(
                self,
                executor: &E,
            ) -> ::sasql_core::SasqlResult<#result_name> {
                let rows = executor.query_raw(#sql_lit, #params_slice).await?;
                if rows.len() != 1 {
                    return Err(::sasql_core::error::QueryError::row_count(
                        "exactly 1 row",
                        rows.len() as u64,
                    ));
                }
                let row = &rows[0];
                Ok(#result_name { #row_decode })
            }

            pub async fn fetch_all<E: ::sasql_core::Executor>(
                self,
                executor: &E,
            ) -> ::sasql_core::SasqlResult<Vec<#result_name>> {
                let rows = executor.query_raw(#sql_lit, #params_slice).await?;
                Ok(rows.iter().map(|row| #result_name { #row_decode }).collect())
            }

            pub async fn fetch_optional<E: ::sasql_core::Executor>(
                self,
                executor: &E,
            ) -> ::sasql_core::SasqlResult<Option<#result_name>> {
                let rows = executor.query_raw(#sql_lit, #params_slice).await?;
                match rows.len() {
                    0 => Ok(None),
                    1 => {
                        let row = &rows[0];
                        Ok(Some(#result_name { #row_decode }))
                    }
                    n => Err(::sasql_core::error::QueryError::row_count(
                        "0 or 1 rows",
                        n as u64,
                    )),
                }
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        pub async fn execute<E: ::sasql_core::Executor>(
            self,
            executor: &E,
        ) -> ::sasql_core::SasqlResult<u64> {
            executor.execute_raw(#sql_lit, #params_slice).await
        }
    };

    let impl_block = if needs_lifetime {
        quote! {
            #[allow(non_camel_case_types)]
            impl<'_sasql> #executor_name<'_sasql> {
                #fetch_methods
                #execute_method
            }
        }
    } else {
        quote! {
            #[allow(non_camel_case_types)]
            impl #executor_name {
                #fetch_methods
                #execute_method
            }
        }
    };

    impl_block
}

/// Generate row field decoding: `field_name: row.get(0), ...`
fn gen_row_decode(validation: &ValidationResult) -> TokenStream {
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", sanitize_column_name(&col.name, i));
        let idx = i;
        quote! { #field_name: row.get(#idx) }
    });

    quote! { #(#fields),* }
}

/// Generate the constructor expression that captures variables from scope.
fn gen_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);

    if parsed.params.is_empty() {
        quote! { #executor_name }
    } else {
        let field_inits = parsed.params.iter().map(|p| {
            let name = format_ident!("{}", p.name);
            quote! { #name }
        });

        if has_reference_params(parsed) {
            quote! { #executor_name { #(#field_inits,)* _marker: ::std::marker::PhantomData } }
        } else {
            quote! { #executor_name { #(#field_inits),* } }
        }
    }
}

/// Check if any parameter type contains a reference (`&`).
fn has_reference_params(parsed: &ParsedQuery) -> bool {
    parsed.params.iter().any(|p| p.rust_type.contains('&'))
}

/// Parse a Rust type string into a TokenStream.
///
/// Handles: `i32`, `String`, `Option<i32>`, `Vec<u8>`, `Vec<Vec<u8>>`, `&str`, `&[u8]`, etc.
/// For executor struct fields, reference types get the `'_sasql` lifetime.
fn parse_rust_type(type_str: &str) -> TokenStream {
    // Add lifetime to reference types for the executor struct
    let with_lifetime = type_str
        .replace("&str", "&'_sasql str")
        .replace("&[", "&'_sasql [");

    with_lifetime.parse().unwrap_or_else(|_| {
        // Fallback: try original string
        type_str.parse().unwrap_or_else(|_| {
            let ident = format_ident!("{}", type_str);
            quote! { #ident }
        })
    })
}

/// Parse a Rust type for result struct fields (no lifetime needed — these are owned).
fn parse_result_type(type_str: &str) -> TokenStream {
    type_str.parse().unwrap_or_else(|_| {
        let ident = format_ident!("{}", type_str);
        quote! { #ident }
    })
}

fn result_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("SasqlResult_{}", &parsed.statement_name)
}

fn executor_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("SasqlExecutor_{}", &parsed.statement_name)
}

/// Sanitize a PostgreSQL column name into a valid Rust identifier.
///
/// PG returns `?column?` for unnamed expressions (e.g. `SELECT 1`).
/// This function replaces invalid characters and provides fallback names.
fn sanitize_column_name(name: &str, index: usize) -> String {
    if name == "?column?" || name.is_empty() {
        return format!("col_{index}");
    }

    // Replace non-alphanumeric/underscore chars with underscore
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();

    // Ensure it doesn't start with a digit
    if sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        format!("col_{sanitized}")
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_query;
    use crate::validate::ColumnInfo;

    fn make_validation(columns: Vec<ColumnInfo>) -> ValidationResult {
        ValidationResult {
            columns,
            param_pg_oids: vec![],
        }
    }

    fn col(name: &str, rust_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            pg_oid: 0,
            pg_type_name: String::new(),
            is_nullable: false,
            rust_type: rust_type.into(),
        }
    }

    #[test]
    fn generates_result_struct_with_fields() {
        let parsed = parse_query("SELECT id, name FROM users WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![
            col("id", "i32"),
            col("name", "String"),
        ]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(code_str.contains("pub id : i32"), "missing id field: {code_str}");
        assert!(code_str.contains("pub name : String"), "missing name field: {code_str}");
    }

    #[test]
    fn generates_nullable_field_as_option() {
        let parsed = parse_query("SELECT bio FROM users WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![
            col("bio", "Option<String>"),
        ]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(code_str.contains("Option < String >") || code_str.contains("Option<String>"),
            "missing Option<String>: {code_str}");
    }

    #[test]
    fn generates_fetch_one_method() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(code_str.contains("fetch_one"), "missing fetch_one: {code_str}");
        assert!(code_str.contains("fetch_all"), "missing fetch_all: {code_str}");
        assert!(code_str.contains("fetch_optional"), "missing fetch_optional: {code_str}");
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn no_params_generates_unit_struct() {
        let parsed = parse_query("SELECT 1").unwrap();
        let validation = make_validation(vec![col("col_0", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Unit struct executor (no fields, no braces in constructor)
        assert!(code_str.contains("struct SasqlExecutor_"), "missing executor: {code_str}");
    }

    #[test]
    fn execute_only_query_has_no_result_struct() {
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]); // no columns
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(!code_str.contains("SasqlResult_"), "should not have result struct: {code_str}");
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn param_capture_in_constructor() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // The constructor should reference the variable name `id`
        assert!(code_str.contains("{ id }") || code_str.contains("{id}"),
            "missing param capture: {code_str}");
    }

    #[test]
    fn positional_sql_in_generated_code() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // The generated code should use positional SQL ($1), not named ($id)
        assert!(code_str.contains("$1"), "should contain positional $1: {code_str}");
        assert!(!code_str.contains("$id"), "should not contain named $id: {code_str}");
    }
}
