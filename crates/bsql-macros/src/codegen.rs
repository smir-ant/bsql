//! Code generation for `bsql::query!`.
//!
//! Given a parsed and validated query, generates a Rust expression that:
//! 1. Defines a result struct with typed fields
//! 2. Defines an executor struct that captures parameters
//! 3. Implements `fetch_one`, `fetch_all`, `fetch_optional`, `execute` methods
//! 4. Evaluates to the executor struct (enables the chaining syntax)

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::dynamic::QueryVariant;
use crate::parse::ParsedQuery;
use crate::validate::ValidationResult;

/// Generate the complete Rust code for a `query!` invocation.
pub fn generate_query_code(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    // Static queries (no optional clauses): original codegen path
    if parsed.optional_clauses.is_empty() {
        let result_struct = gen_result_struct(parsed, validation);
        let executor_struct = gen_executor_struct(parsed);
        let executor_impls = gen_executor_impls(parsed, validation);
        let constructor = gen_constructor(parsed);

        return quote! {
            {
                #result_struct
                #executor_struct
                #executor_impls
                #constructor
            }
        };
    }

    // This should not be called for dynamic queries — use generate_dynamic_query_code
    let msg = "internal error: generate_query_code called for dynamic query — use generate_dynamic_query_code";
    quote! { compile_error!(#msg) }
}

/// Generate Rust code for a dynamic query with optional clauses.
///
/// The generated code includes:
/// - A result struct (same for all variants — the SELECT list is identical)
/// - An executor struct capturing all parameters (base + all optional)
/// - A `match` dispatcher that selects the correct SQL variant and params
///   based on which `Option` params are `Some`
pub fn generate_dynamic_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    variants: &[QueryVariant],
) -> TokenStream {
    let result_struct = gen_result_struct(parsed, validation);
    let executor_struct = gen_dynamic_executor_struct(parsed);
    let executor_impls = gen_dynamic_executor_impls(parsed, validation, variants);
    let constructor = gen_dynamic_constructor(parsed);

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
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", deduped_names[i]);
        let field_type = parse_result_type(&col.rust_type);
        quote! { pub #field_name: #field_type }
    });

    // EXPLAIN plan as doc comment (opt-in via `explain` feature)
    #[cfg(feature = "explain")]
    let explain_doc = if let Some(ref plan) = validation.explain_plan {
        let doc_lines: Vec<TokenStream> = std::iter::once(quote! { #[doc = ""] })
            .chain(std::iter::once(quote! { #[doc = "**Query plan:**"] }))
            .chain(std::iter::once(quote! { #[doc = "```text"] }))
            .chain(plan.lines().map(|line| {
                let line_str = format!("{line}");
                quote! { #[doc = #line_str] }
            }))
            .chain(std::iter::once(quote! { #[doc = "```"] }))
            .collect();
        quote! { #(#doc_lines)* }
    } else {
        TokenStream::new()
    };
    #[cfg(not(feature = "explain"))]
    let explain_doc = TokenStream::new();

    quote! {
        #explain_doc
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

    let fields: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            let ty = inject_lifetime(&p.rust_type);
            quote! { #name: #ty }
        })
        .collect();

    quote! {
        #[must_use = "query is not executed until .fetch_one(), .fetch_all(), .fetch_optional(), or .execute() is called"]
        #[allow(non_camel_case_types)]
        struct #struct_name<'_bsql> {
            #(#fields,)*
            _marker: ::std::marker::PhantomData<&'_bsql ()>,
        }
    }
}

/// Generate `fetch_one`, `fetch_all`, `fetch_optional`, `execute` methods.
fn gen_executor_impls(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let sql_lit = &parsed.positional_sql;

    // SELECT -> query_raw_readonly (replica-aware), writes -> query_raw (primary)
    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let query_method = if is_select {
        quote! { query_raw_readonly }
    } else {
        quote! { query_raw }
    };

    // Build the params slice: &[&self.id as &(dyn Encode + Sync), ...]
    let param_refs: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            quote! { &self.#name as &(dyn ::bsql_core::driver::Encode + Sync) }
        })
        .collect();

    let params_slice = if param_refs.is_empty() {
        quote! { &[] }
    } else {
        quote! { &[#(#param_refs),*] }
    };

    // Compute sql_hash at compile time
    let sql_hash_val = bsql_core::rapid_hash_str(&parsed.positional_sql);

    let has_columns = !validation.columns.is_empty();

    // Generate a LIMIT 2 variant for fetch_one/fetch_optional
    let needs_limit = has_columns
        && is_select
        && !parsed.normalized_sql.contains(" limit ")
        && !parsed.normalized_sql.contains(" for ");
    let limited_sql = if needs_limit {
        format!("{} LIMIT 2", parsed.positional_sql)
    } else {
        parsed.positional_sql.clone()
    };
    let limited_sql_lit = &limited_sql;
    let limited_sql_hash_val = bsql_core::rapid_hash_str(&limited_sql);

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let row_decode = gen_row_decode(validation);

        quote! {
            pub async fn fetch_one<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                let owned = executor.#query_method(#limited_sql_lit, #limited_sql_hash_val, #params_slice).await?;
                if owned.len() != 1 {
                    return Err(::bsql_core::error::QueryError::row_count(
                        "exactly 1 row",
                        owned.len() as u64,
                    ));
                }
                let row = owned.row(0);
                Ok(#result_name { #row_decode })
            }

            pub async fn fetch_all<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                let owned = executor.#query_method(#sql_lit, #sql_hash_val, #params_slice).await?;
                Ok(owned.iter().map(|row| #result_name { #row_decode }).collect())
            }

            pub async fn fetch_optional<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                let owned = executor.#query_method(#limited_sql_lit, #limited_sql_hash_val, #params_slice).await?;
                match owned.len() {
                    0 => Ok(None),
                    1 => {
                        let row = owned.row(0);
                        Ok(Some(#result_name { #row_decode }))
                    }
                    n => Err(::bsql_core::error::QueryError::row_count(
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
        pub async fn execute<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            executor.execute_raw(#sql_lit, #sql_hash_val, #params_slice).await
        }
    };

    quote! {
        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #execute_method
        }
    }
}

// ---- Dynamic query codegen ----

/// Generate the executor struct for a dynamic query.
fn gen_dynamic_executor_struct(parsed: &ParsedQuery) -> TokenStream {
    let struct_name = executor_struct_name(parsed);

    let mut fields: Vec<TokenStream> = Vec::new();
    let mut seen_names: Vec<String> = Vec::new();

    for p in &parsed.params {
        let name = param_ident(&p.name);
        let ty = inject_lifetime(&p.rust_type);
        fields.push(quote! { #name: #ty });
        seen_names.push(p.name.clone());
    }

    for clause in &parsed.optional_clauses {
        for p in &clause.params {
            if !seen_names.contains(&p.name) {
                let name = param_ident(&p.name);
                let ty = inject_lifetime(&p.rust_type);
                fields.push(quote! { #name: #ty });
                seen_names.push(p.name.clone());
            }
        }
    }

    quote! {
        #[must_use = "query is not executed until .fetch_one(), .fetch_all(), .fetch_optional(), or .execute() is called"]
        #[allow(non_camel_case_types)]
        struct #struct_name<'_bsql> {
            #(#fields,)*
            _marker: ::std::marker::PhantomData<&'_bsql ()>,
        }
    }
}

/// Generate the impl block for a dynamic query executor.
fn gen_dynamic_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    variants: &[QueryVariant],
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let has_columns = !validation.columns.is_empty();

    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let query_method = if is_select {
        quote! { query_raw_readonly }
    } else {
        quote! { query_raw }
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let row_decode = gen_row_decode(validation);

        let needs_limit = has_columns
            && is_select
            && !parsed.normalized_sql.contains(" limit ")
            && !parsed.normalized_sql.contains(" for ");

        let qm = &query_method;
        let fetch_one_dispatcher =
            gen_variant_dispatcher(parsed, variants, needs_limit, |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.#qm(#sql_lit, #sql_hash, &params_slice[..]).await?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    let row = owned.row(0);
                    Ok(#result_name { #row_decode })
                }
            });

        let fetch_all_dispatcher =
            gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.#qm(#sql_lit, #sql_hash, &params_slice[..]).await?;
                    Ok(owned.iter().map(|row| #result_name { #row_decode }).collect())
                }
            });

        let fetch_optional_dispatcher =
            gen_variant_dispatcher(parsed, variants, needs_limit, |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.#qm(#sql_lit, #sql_hash, &params_slice[..]).await?;
                    match owned.len() {
                        0 => Ok(None),
                        1 => {
                            let row = owned.row(0);
                            Ok(Some(#result_name { #row_decode }))
                        }
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }
            });

        quote! {
            pub async fn fetch_one<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #fetch_one_dispatcher
            }

            pub async fn fetch_all<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                #fetch_all_dispatcher
            }

            pub async fn fetch_optional<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                #fetch_optional_dispatcher
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_dispatcher =
        gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
            quote! {
                executor.execute_raw(#sql_lit, #sql_hash, &params_slice[..]).await
            }
        });

    let execute_method = quote! {
        pub async fn execute<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            #execute_dispatcher
        }
    };

    quote! {
        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #execute_method
        }
    }
}

/// Generate the variant match dispatcher.
fn gen_variant_dispatcher<F>(
    parsed: &ParsedQuery,
    variants: &[QueryVariant],
    inject_limit: bool,
    body_fn: F,
) -> TokenStream
where
    F: Fn(&str, u64) -> TokenStream,
{
    let n = parsed.optional_clauses.len();
    let discriminants: Vec<proc_macro2::Ident> = parsed
        .optional_clauses
        .iter()
        .map(|c| param_ident(&c.params[0].name))
        .collect();

    let match_tuple = quote! { (#(self.#discriminants.is_some()),*) };

    let arms: Vec<TokenStream> = variants
        .iter()
        .map(|variant| {
            let pattern_elements: Vec<TokenStream> = (0..n)
                .map(|i| {
                    if (variant.mask & (1 << i)) != 0 {
                        quote! { true }
                    } else {
                        quote! { false }
                    }
                })
                .collect();
            let pattern = quote! { (#(#pattern_elements),*) };

            let sql_str = if inject_limit {
                format!("{} LIMIT 2", variant.sql)
            } else {
                variant.sql.clone()
            };

            let sql_hash = bsql_core::rapid_hash_str(&sql_str);

            let param_bindings: Vec<TokenStream> = variant
                .params
                .iter()
                .map(|p| {
                    let name = param_ident(&p.name);
                    if p.rust_type.starts_with("Option<") {
                        quote! { self.#name.as_ref().unwrap() as &(dyn ::bsql_core::driver::Encode + Sync) }
                    } else {
                        quote! { &self.#name as &(dyn ::bsql_core::driver::Encode + Sync) }
                    }
                })
                .collect();

            let body = body_fn(&sql_str, sql_hash);

            quote! {
                #pattern => {
                    let params_slice: &[&(dyn ::bsql_core::driver::Encode + Sync)] =
                        &[#(#param_bindings),*];
                    #body
                }
            }
        })
        .collect();

    quote! {
        match #match_tuple {
            #(#arms)*
        }
    }
}

/// Generate the constructor for a dynamic query executor.
fn gen_dynamic_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);

    let mut field_names: Vec<proc_macro2::Ident> = Vec::new();
    let mut seen: Vec<String> = Vec::new();

    for p in &parsed.params {
        field_names.push(param_ident(&p.name));
        seen.push(p.name.clone());
    }

    for clause in &parsed.optional_clauses {
        for p in &clause.params {
            if !seen.contains(&p.name) {
                field_names.push(param_ident(&p.name));
                seen.push(p.name.clone());
            }
        }
    }

    quote! { #executor_name { #(#field_names,)* _marker: ::std::marker::PhantomData } }
}

/// Generate row field decoding using typed getters from bsql_driver::Row.
///
/// For each column, generates the appropriate getter call based on the Rust type:
/// - `i32` -> `row.get_i32(idx).unwrap_or_default()`
/// - `String` -> `row.get_str(idx).map(|s| s.to_owned()).unwrap_or_default()`
/// - `Option<i32>` -> `row.get_i32(idx)`
/// - `Option<String>` -> `row.get_str(idx).map(|s| s.to_owned())`
fn gen_row_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let idx = i;
        let col = &validation.columns[i];
        let decode_expr = gen_column_decode(idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    quote! { #(#fields),* }
}

/// Generate the decode expression for a single column based on its Rust type.
fn gen_column_decode(idx: usize, rust_type: &str) -> TokenStream {
    // Check if it's Option<T>
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        // Nullable column -> return Option<T>
        gen_nullable_decode(idx, inner)
    } else {
        // NOT NULL column -> unwrap_or_default
        gen_not_null_decode(idx, rust_type)
    }
}

/// Generate decode for a NOT NULL column.
fn gen_not_null_decode(idx: usize, rust_type: &str) -> TokenStream {
    match rust_type {
        "bool" => quote! { row.get_bool(#idx).unwrap_or_default() },
        "i16" => quote! { row.get_i16(#idx).unwrap_or_default() },
        "i32" => quote! { row.get_i32(#idx).unwrap_or_default() },
        "i64" => quote! { row.get_i64(#idx).unwrap_or_default() },
        "f32" => quote! { row.get_f32(#idx).unwrap_or_default() },
        "f64" => quote! { row.get_f64(#idx).unwrap_or_default() },
        "String" => quote! { row.get_str(#idx).unwrap_or_default().to_owned() },
        "Vec<u8>" => quote! { row.get_bytes(#idx).unwrap_or_default().to_vec() },
        "u32" => {
            // OID type: decode as i32 then cast
            quote! { row.get_i32(#idx).unwrap_or_default() as u32 }
        }
        "()" => quote! { () },
        _ => gen_feature_gated_decode(idx, rust_type),
    }
}

/// Generate decode for a feature-gated type (uuid, time, chrono, decimal).
/// Uses `row.get_raw(idx)` + the appropriate decode function from bsql_core::driver.
fn gen_feature_gated_decode(idx: usize, rust_type: &str) -> TokenStream {
    match rust_type {
        "::uuid::Uuid" | "uuid::Uuid" => quote! {
            ::bsql_core::driver::decode_uuid_type(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("uuid decode failed")
        },
        "::time::OffsetDateTime" | "time::OffsetDateTime" => quote! {
            ::bsql_core::driver::decode_timestamptz_time(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("timestamptz decode failed")
        },
        "::time::Date" | "time::Date" => quote! {
            ::bsql_core::driver::decode_date_time(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("date decode failed")
        },
        "::time::Time" | "time::Time" => quote! {
            ::bsql_core::driver::decode_time_time(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("time decode failed")
        },
        "::chrono::DateTime<chrono::Utc>" | "chrono::DateTime<chrono::Utc>" => quote! {
            ::bsql_core::driver::decode_timestamptz_chrono(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("timestamptz decode failed")
        },
        "::chrono::NaiveDate" | "chrono::NaiveDate" => quote! {
            ::bsql_core::driver::decode_date_chrono(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("date decode failed")
        },
        "::chrono::NaiveTime" | "chrono::NaiveTime" => quote! {
            ::bsql_core::driver::decode_time_chrono(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("time decode failed")
        },
        "::rust_decimal::Decimal" | "rust_decimal::Decimal" => quote! {
            ::bsql_core::driver::decode_numeric_decimal(
                row.get_raw(#idx).unwrap_or_default()
            ).expect("numeric decode failed")
        },
        // Array types
        "Vec<bool>" => quote! {
            ::bsql_core::driver::decode_array_bool(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<i16>" => quote! {
            ::bsql_core::driver::decode_array_i16(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<i32>" => quote! {
            ::bsql_core::driver::decode_array_i32(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<i64>" => quote! {
            ::bsql_core::driver::decode_array_i64(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<f32>" => quote! {
            ::bsql_core::driver::decode_array_f32(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<f64>" => quote! {
            ::bsql_core::driver::decode_array_f64(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<String>" => quote! {
            ::bsql_core::driver::decode_array_str(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        "Vec<Vec<u8>>" => quote! {
            ::bsql_core::driver::decode_array_bytea(
                row.get_raw(#idx).unwrap_or_default()
            ).unwrap_or_default()
        },
        _ => {
            // Unknown type -- fall back. This should not happen for known PG types.
            quote! { {
                let _raw = row.get_raw(#idx).unwrap_or_default();
                compile_error!(concat!("bsql: unsupported type for decode: ", #rust_type))
            } }
        }
    }
}

/// Generate decode for a nullable column (returns Option<T>).
fn gen_nullable_decode(idx: usize, inner_type: &str) -> TokenStream {
    match inner_type {
        "bool" => quote! { row.get_bool(#idx) },
        "i16" => quote! { row.get_i16(#idx) },
        "i32" => quote! { row.get_i32(#idx) },
        "i64" => quote! { row.get_i64(#idx) },
        "f32" => quote! { row.get_f32(#idx) },
        "f64" => quote! { row.get_f64(#idx) },
        "String" => quote! { row.get_str(#idx).map(|s| s.to_owned()) },
        "Vec<u8>" => quote! { row.get_bytes(#idx).map(|b| b.to_vec()) },
        "u32" => quote! { row.get_i32(#idx).map(|v| v as u32) },
        _ => {
            // Feature-gated types: nullable decode
            let decode = gen_feature_gated_decode(idx, inner_type);
            quote! { {
                if row.is_null(#idx) {
                    None
                } else {
                    Some(#decode)
                }
            } }
        }
    }
}

/// Generate the constructor expression that captures variables from scope.
fn gen_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let field_inits = parsed.params.iter().map(|p| {
        let name = param_ident(&p.name);
        quote! { #name }
    });

    quote! { #executor_name { #(#field_inits,)* _marker: ::std::marker::PhantomData } }
}

/// Parse a Rust type string and inject `'_bsql` lifetime on bare references.
fn inject_lifetime(type_str: &str) -> TokenStream {
    match syn::parse_str::<syn::Type>(type_str) {
        Ok(ty) => {
            let rewritten = add_lifetime_to_refs(ty);
            quote! { #rewritten }
        }
        Err(_) => {
            let msg = format!("internal error: cannot parse type `{type_str}`");
            quote! { compile_error!(#msg) }
        }
    }
}

/// Recursively add `'_bsql` lifetime to bare (elided) references in a type.
fn add_lifetime_to_refs(ty: syn::Type) -> syn::Type {
    match ty {
        syn::Type::Reference(mut r) => {
            if r.lifetime.is_none() {
                r.lifetime = Some(syn::Lifetime::new("'_bsql", proc_macro2::Span::call_site()));
            }
            r.elem = Box::new(add_lifetime_to_refs(*r.elem));
            syn::Type::Reference(r)
        }
        syn::Type::Slice(mut s) => {
            s.elem = Box::new(add_lifetime_to_refs(*s.elem));
            syn::Type::Slice(s)
        }
        syn::Type::Path(mut p) => {
            for seg in &mut p.path.segments {
                if let syn::PathArguments::AngleBracketed(args) = &mut seg.arguments {
                    for arg in &mut args.args {
                        if let syn::GenericArgument::Type(inner) = arg {
                            *inner = add_lifetime_to_refs(inner.clone());
                        }
                    }
                }
            }
            syn::Type::Path(p)
        }
        other => other,
    }
}

/// Parse a Rust type for result struct fields (no lifetime needed — these are owned).
fn parse_result_type(type_str: &str) -> TokenStream {
    match syn::parse_str::<syn::Type>(type_str) {
        Ok(ty) => quote! { #ty },
        Err(_) => {
            let msg = format!("internal error: cannot parse type `{type_str}`");
            quote! { compile_error!(#msg) }
        }
    }
}

/// Deduplicate column names by suffixing duplicates with `_1`, `_2`, etc.
fn deduplicate_column_names(columns: &[crate::validate::ColumnInfo]) -> Vec<String> {
    let names: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| sanitize_column_name(&col.name, i))
        .collect();

    let mut final_names: Vec<String> = Vec::with_capacity(names.len());
    for name in &names {
        let mut candidate = name.clone();
        let mut suffix = 1u32;
        while final_names.contains(&candidate) {
            candidate = format!("{name}_{suffix}");
            suffix += 1;
        }
        final_names.push(candidate);
    }

    final_names
}

fn result_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlResult_{}", &parsed.statement_name)
}

fn executor_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlExecutor_{}", &parsed.statement_name)
}

/// Rust keywords (2021 edition) that cannot be used as bare identifiers.
const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
    "pub", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
    "unsafe", "use", "where", "while", "yield",
];

/// Sanitize a user-declared parameter name into a valid Rust identifier.
fn sanitize_param_name(name: &str) -> String {
    if RUST_KEYWORDS.contains(&name) {
        format!("{name}_")
    } else {
        name.to_owned()
    }
}

/// Create a `format_ident!` for a parameter name, handling Rust keywords.
fn param_ident(name: &str) -> proc_macro2::Ident {
    format_ident!("{}", sanitize_param_name(name))
}

/// Sanitize a PostgreSQL column name into a valid Rust identifier.
fn sanitize_column_name(name: &str, index: usize) -> String {
    if name == "?column?" || name.is_empty() {
        return format!("col_{index}");
    }

    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let sanitized = if sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        format!("col_{sanitized}")
    } else {
        sanitized
    };

    if RUST_KEYWORDS.contains(&sanitized.as_str()) {
        format!("{sanitized}_")
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
            param_pg_oids: smallvec::smallvec![],
            param_is_pg_enum: smallvec::smallvec![],
            #[cfg(feature = "explain")]
            explain_plan: None,
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
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("pub id : i32"),
            "missing id field: {code_str}"
        );
        assert!(
            code_str.contains("pub name : String"),
            "missing name field: {code_str}"
        );
    }

    #[test]
    fn generates_nullable_field_as_option() {
        let parsed = parse_query("SELECT bio FROM users WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("bio", "Option<String>")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("Option < String >") || code_str.contains("Option<String>"),
            "missing Option<String>: {code_str}"
        );
    }

    #[test]
    fn generates_fetch_one_method() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("fetch_one"),
            "missing fetch_one: {code_str}"
        );
        assert!(
            code_str.contains("fetch_all"),
            "missing fetch_all: {code_str}"
        );
        assert!(
            code_str.contains("fetch_optional"),
            "missing fetch_optional: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn no_params_generates_unit_struct() {
        let parsed = parse_query("SELECT 1").unwrap();
        let validation = make_validation(vec![col("col_0", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("struct BsqlExecutor_"),
            "missing executor: {code_str}"
        );
    }

    #[test]
    fn execute_only_query_has_no_result_struct() {
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("BsqlResult_"),
            "should not have result struct: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn positional_sql_in_generated_code() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("$1"),
            "should contain positional $1: {code_str}"
        );
        assert!(
            !code_str.contains("$id"),
            "should not contain named $id: {code_str}"
        );
    }

    #[test]
    fn uses_driver_encode_not_tosql() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("bsql_core :: driver :: Encode"),
            "should use bsql_core::driver::Encode: {code_str}"
        );
        assert!(
            !code_str.contains("ToSql"),
            "should not use ToSql: {code_str}"
        );
    }

    #[test]
    fn uses_typed_getters_not_row_get() {
        let parsed = parse_query("SELECT id, name FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("get_i32"),
            "should use get_i32 for i32 column: {code_str}"
        );
        assert!(
            code_str.contains("get_str"),
            "should use get_str for String column: {code_str}"
        );
    }

    #[test]
    fn select_uses_query_raw_readonly() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("query_raw_readonly"),
            "SELECT should use query_raw_readonly: {code_str}"
        );
    }

    #[test]
    fn insert_uses_query_raw_not_readonly() {
        let parsed = parse_query("INSERT INTO t (a) VALUES ($a: i32) RETURNING id").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("query_raw_readonly"),
            "INSERT should NOT use query_raw_readonly: {code_str}"
        );
        assert!(
            code_str.contains("query_raw"),
            "INSERT RETURNING should use query_raw: {code_str}"
        );
    }

    #[test]
    fn fetch_one_injects_limit_2() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("LIMIT 2"),
            "missing LIMIT 2 in fetch_one: {code_str}"
        );
    }

    #[test]
    fn existing_limit_not_doubled() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32 LIMIT 10").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("LIMIT 2"),
            "should not add LIMIT 2 when LIMIT exists: {code_str}"
        );
    }

    #[test]
    fn for_update_no_limit_injected() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32 FOR UPDATE").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("LIMIT 2"),
            "FOR UPDATE query should NOT get LIMIT 2 injected: {code_str}"
        );
    }

    // --- lifetime injection ---

    #[test]
    fn inject_lifetime_bare_ref_str() {
        let ts = inject_lifetime("&str");
        let s = ts.to_string();
        assert!(s.contains("'_bsql"), "missing lifetime: {s}");
    }

    #[test]
    fn inject_lifetime_bare_ref_slice() {
        let ts = inject_lifetime("&[u8]");
        let s = ts.to_string();
        assert!(s.contains("'_bsql"), "missing lifetime: {s}");
    }

    #[test]
    fn inject_lifetime_no_ref_passes_through() {
        let ts = inject_lifetime("i32");
        let s = ts.to_string();
        assert!(!s.contains("'_bsql"), "i32 should have no lifetime: {s}");
    }

    // --- column dedup ---

    #[test]
    fn duplicate_column_names_deduplicated() {
        let columns = vec![col("id", "i32"), col("id", "i32"), col("name", "String")];
        let names = deduplicate_column_names(&columns);
        assert_eq!(names, vec!["id", "id_1", "name"]);
    }

    #[test]
    fn three_duplicate_columns() {
        let columns = vec![col("id", "i32"), col("id", "i32"), col("id", "i32")];
        let names = deduplicate_column_names(&columns);
        assert_eq!(names, vec!["id", "id_1", "id_2"]);
    }

    // --- sanitize ---

    #[test]
    fn sanitize_unnamed_column() {
        assert_eq!(sanitize_column_name("?column?", 0), "col_0");
    }

    #[test]
    fn sanitize_column_keyword_type() {
        assert_eq!(sanitize_column_name("type", 0), "type_");
    }

    #[test]
    fn sanitize_param_keyword() {
        assert_eq!(sanitize_param_name("type"), "type_");
        assert_eq!(sanitize_param_name("fn"), "fn_");
    }

    #[test]
    fn sanitize_param_non_keyword() {
        assert_eq!(sanitize_param_name("id"), "id");
    }
}
