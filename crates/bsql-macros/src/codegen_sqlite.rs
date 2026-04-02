//! Code generation for SQLite queries.
//!
//! Generates Rust code that executes queries via `bsql_core::SqlitePool`.
//! Structurally parallel to `codegen.rs` (PostgreSQL) but references SQLite
//! driver types for parameter binding and row decoding.
//!
//! Key differences from PostgreSQL codegen:
//! - Parameters use `bsql_driver_sqlite::codec::SqliteEncode` (not PG `Encode`)
//! - Row decoding uses SQLite arena format (LE i64 for integers, LE f64 for reals)
//! - Executor calls reference `bsql_core::SqlitePool` and `bsql_core::SqliteExecutor`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::dynamic::QueryVariant;
use crate::parse::ParsedQuery;
use crate::validate::ValidationResult;
use crate::validate_sqlite::pg_to_sqlite_params;

/// Generate the complete Rust code for a SQLite `query!` invocation (static query).
pub fn generate_sqlite_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> TokenStream {
    let sqlite_sql = pg_to_sqlite_params(&parsed.positional_sql);
    let result_struct = gen_result_struct(parsed, validation);
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_executor_impls(parsed, validation, &sqlite_sql);
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

// --- Result struct (identical structure to PG, different field types possible) ---

fn gen_result_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new();
    }

    let struct_name = result_struct_name(parsed);
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", deduped_names[i]);
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

// --- Executor struct ---

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

// --- Executor impl ---

fn gen_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    sqlite_sql: &str,
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let sql_lit = sqlite_sql;

    let is_select = parsed.kind == crate::parse::QueryKind::Select;

    // Build the params slice for SQLite: convert to ParamValue for channel transport
    let param_conversions: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            gen_param_to_param_value(&name, &p.rust_type)
        })
        .collect();

    let params_build = if param_conversions.is_empty() {
        quote! { let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::SmallVec::new(); }
    } else {
        quote! {
            let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::smallvec![
                #(#param_conversions),*
            ];
        }
    };

    // Compute sql_hash at compile time
    let sql_hash_val = bsql_core::rapid_hash_str(sqlite_sql);

    let has_columns = !validation.columns.is_empty();

    // Generate LIMIT 2 variant for fetch_one/fetch_optional
    let needs_limit = has_columns && is_select && !parsed.normalized_sql.contains(" limit ");

    let limited_sql = if needs_limit {
        format!("{sqlite_sql} LIMIT 2")
    } else {
        sqlite_sql.to_owned()
    };
    let limited_sql_lit = &limited_sql;
    let limited_sql_hash_val = bsql_core::rapid_hash_str(&limited_sql);

    let row_decode = if has_columns {
        gen_sqlite_row_decode(validation)
    } else {
        TokenStream::new()
    };

    // Determine which pool method to call
    let query_method = if is_select {
        quote! { query_readonly }
    } else {
        quote! { query_readwrite }
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let qm = &query_method;

        quote! {
            pub async fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #params_build
                let (result, arena) = pool.#qm(#limited_sql_lit, #limited_sql_hash_val, params).await?;
                if result.len() != 1 {
                    return Err(::bsql_core::error::QueryError::row_count(
                        "exactly 1 row",
                        result.len() as u64,
                    ));
                }
                let _bsql_row_idx: usize = 0;
                Ok(#result_name { #row_decode })
            }

            pub async fn fetch_all(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                #params_build
                let (result, arena) = pool.#qm(#sql_lit, #sql_hash_val, params).await?;
                let mut out = Vec::with_capacity(result.len());
                for _bsql_row_idx in 0..result.len() {
                    out.push(#result_name { #row_decode });
                }
                Ok(out)
            }

            pub async fn fetch_optional(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                #params_build
                let (result, arena) = pool.#qm(#limited_sql_lit, #limited_sql_hash_val, params).await?;
                match result.len() {
                    0 => Ok(None),
                    1 => {
                        let _bsql_row_idx: usize = 0;
                        Ok(Some(#result_name { #row_decode }))
                    }
                    n => Err(::bsql_core::error::QueryError::row_count(
                        "0 or 1 rows",
                        n as u64,
                    )),
                }
            }

            pub async fn fetch_stream(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<::bsql_core::SqliteStreamingQuery> {
                #params_build
                pool.query_streaming(#sql_lit, #sql_hash_val, params, 64).await
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        pub async fn execute(
            self,
            pool: &::bsql_core::SqlitePool,
        ) -> ::bsql_core::BsqlResult<u64> {
            #params_build
            pool.execute_sql(#sql_lit, #sql_hash_val, params).await
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

// --- Param to ParamValue conversion ---

fn gen_param_to_param_value(name: &proc_macro2::Ident, rust_type: &str) -> TokenStream {
    // Handle Option<T> params
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let inner_conv = param_value_conversion(quote! { v }, inner);
        return quote! {
            match &self.#name {
                Some(v) => #inner_conv,
                None => ::bsql_core::driver_sqlite::ParamValue::Null,
            }
        };
    }

    param_value_conversion(quote! { self.#name }, rust_type)
}

fn param_value_conversion(val_expr: TokenStream, rust_type: &str) -> TokenStream {
    match rust_type {
        "bool" => quote! { ::bsql_core::driver_sqlite::ParamValue::Bool(#val_expr) },
        "i8" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(i64::from(#val_expr)) },
        "i16" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(i64::from(#val_expr)) },
        "i32" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(i64::from(#val_expr)) },
        "i64" => quote! { ::bsql_core::driver_sqlite::ParamValue::Int(#val_expr) },
        "f32" => quote! { ::bsql_core::driver_sqlite::ParamValue::Real(f64::from(#val_expr)) },
        "f64" => quote! { ::bsql_core::driver_sqlite::ParamValue::Real(#val_expr) },
        "&str" => quote! { ::bsql_core::driver_sqlite::ParamValue::Text(#val_expr.to_owned()) },
        "String" => quote! { ::bsql_core::driver_sqlite::ParamValue::Text(#val_expr.clone()) },
        "&[u8]" => quote! { ::bsql_core::driver_sqlite::ParamValue::Blob(#val_expr.to_vec()) },
        "Vec<u8>" => quote! { ::bsql_core::driver_sqlite::ParamValue::Blob(#val_expr.clone()) },
        // Feature-gated types: encode as text via ToString/Display
        "::uuid::Uuid"
        | "uuid::Uuid"
        | "::time::PrimitiveDateTime"
        | "time::PrimitiveDateTime"
        | "::time::Date"
        | "time::Date"
        | "::time::Time"
        | "time::Time"
        | "::chrono::NaiveDateTime"
        | "chrono::NaiveDateTime"
        | "::chrono::NaiveDate"
        | "chrono::NaiveDate"
        | "::chrono::NaiveTime"
        | "chrono::NaiveTime"
        | "::rust_decimal::Decimal"
        | "rust_decimal::Decimal" => {
            quote! { ::bsql_core::driver_sqlite::ParamValue::Text(::std::string::ToString::to_string(&#val_expr)) }
        }
        _ => {
            // Unknown type: attempt ToString for text storage
            quote! { ::bsql_core::driver_sqlite::ParamValue::Text(::std::string::ToString::to_string(&#val_expr)) }
        }
    }
}

// --- SQLite row decode ---

/// Generate row field decoding using SQLite QueryResult accessors.
///
/// SQLite arena format:
/// - INTEGER: 8 bytes, little-endian i64
/// - REAL: 8 bytes, little-endian f64
/// - TEXT: raw UTF-8 bytes
/// - BLOB: raw bytes
/// - NULL: nothing (indicated by length == -1)
fn gen_sqlite_row_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let col_idx = i;
        let col = &validation.columns[i];
        let decode_expr = gen_sqlite_column_decode(col_idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    quote! { #(#fields),* }
}

fn gen_sqlite_column_decode(idx: usize, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_sqlite_nullable_decode(idx, inner)
    } else {
        gen_sqlite_not_null_decode(idx, rust_type)
    }
}

fn gen_sqlite_not_null_decode(idx: usize, rust_type: &str) -> TokenStream {
    let col_idx = idx.to_string();
    match rust_type {
        "bool" => {
            let err = gen_decode_error(&col_idx, "bool");
            quote! { result.get_bool(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)? }
        }
        "i64" => {
            let err = gen_decode_error(&col_idx, "i64");
            quote! { result.get_i64(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)? }
        }
        "f64" => {
            let err = gen_decode_error(&col_idx, "f64");
            quote! { result.get_f64(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)? }
        }
        "String" => {
            let err = gen_decode_error(&col_idx, "String");
            quote! { result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?.to_owned() }
        }
        "Vec<u8>" => {
            let err = gen_decode_error(&col_idx, "Vec<u8>");
            quote! { result.get_bytes(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?.to_vec() }
        }
        _ => gen_sqlite_feature_gated_decode(idx, rust_type),
    }
}

fn gen_sqlite_nullable_decode(idx: usize, inner_type: &str) -> TokenStream {
    match inner_type {
        "bool" => quote! { result.get_bool(_bsql_row_idx, #idx, &arena) },
        "i64" => quote! { result.get_i64(_bsql_row_idx, #idx, &arena) },
        "f64" => quote! { result.get_f64(_bsql_row_idx, #idx, &arena) },
        "String" => quote! { result.get_str(_bsql_row_idx, #idx, &arena).map(|s| s.to_owned()) },
        "Vec<u8>" => quote! { result.get_bytes(_bsql_row_idx, #idx, &arena).map(|b| b.to_vec()) },
        // Feature-gated types: if not null, parse from text; if null, return None
        "::uuid::Uuid"
        | "uuid::Uuid"
        | "::time::PrimitiveDateTime"
        | "time::PrimitiveDateTime"
        | "::time::Date"
        | "time::Date"
        | "::time::Time"
        | "time::Time"
        | "::chrono::NaiveDateTime"
        | "chrono::NaiveDateTime"
        | "::chrono::NaiveDate"
        | "chrono::NaiveDate"
        | "::chrono::NaiveTime"
        | "chrono::NaiveTime"
        | "::rust_decimal::Decimal"
        | "rust_decimal::Decimal" => {
            let not_null_decode = gen_sqlite_feature_gated_decode(idx, inner_type);
            quote! {
                if result.is_null(_bsql_row_idx, #idx) {
                    None
                } else {
                    Some(#not_null_decode)
                }
            }
        }
        _ => quote! { result.get_str(_bsql_row_idx, #idx, &arena).map(|s| s.to_owned()) },
    }
}

fn gen_decode_error(col_idx: &str, type_name: &str) -> TokenStream {
    quote! {
        ::bsql_core::error::DecodeError::with_source(
            #col_idx,
            #type_name,
            "NULL or invalid data",
            ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL ", #type_name)),
        )
    }
}

// --- Constructor ---

fn gen_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let field_inits = parsed.params.iter().map(|p| {
        let name = param_ident(&p.name);
        quote! { #name }
    });
    quote! { #executor_name { #(#field_inits,)* _marker: ::std::marker::PhantomData } }
}

// --- Shared helpers (mirrored from codegen.rs) ---

fn result_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlResult_{}", &parsed.statement_name)
}

fn executor_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlExecutor_{}", &parsed.statement_name)
}

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

fn parse_result_type(type_str: &str) -> TokenStream {
    match syn::parse_str::<syn::Type>(type_str) {
        Ok(ty) => quote! { #ty },
        Err(_) => {
            let msg = format!("internal error: cannot parse type `{type_str}`");
            quote! { compile_error!(#msg) }
        }
    }
}

const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
    "pub", "raw", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true",
    "type", "unsafe", "use", "where", "while", "yield",
];

fn sanitize_param_name(name: &str) -> String {
    if RUST_KEYWORDS.contains(&name) {
        format!("{name}_")
    } else {
        name.to_owned()
    }
}

fn param_ident(name: &str) -> proc_macro2::Ident {
    format_ident!("{}", sanitize_param_name(name))
}

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

// ===========================================================================
// Dynamic query codegen (optional clauses)
// ===========================================================================

/// Generate Rust code for a dynamic SQLite query with optional clauses.
///
/// The generated code includes:
/// - A result struct (same for all variants)
/// - An executor struct capturing all parameters (base + all optional)
/// - A `match` dispatcher that selects the correct SQL variant and params
///   based on which `Option` params are `Some`
pub fn generate_dynamic_sqlite_query_code(
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

/// Generate the executor struct for a dynamic query — captures all params.
fn gen_dynamic_executor_struct(parsed: &ParsedQuery) -> TokenStream {
    let struct_name = executor_struct_name(parsed);

    let mut fields: Vec<TokenStream> = Vec::new();
    let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

    for p in &parsed.params {
        let name = param_ident(&p.name);
        let ty = inject_lifetime(&p.rust_type);
        fields.push(quote! { #name: #ty });
        seen_names.insert(p.name.clone());
    }

    for clause in &parsed.optional_clauses {
        for p in &clause.params {
            if seen_names.insert(p.name.clone()) {
                let name = param_ident(&p.name);
                let ty = inject_lifetime(&p.rust_type);
                fields.push(quote! { #name: #ty });
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

/// Generate the impl block for a dynamic SQLite query executor.
fn gen_dynamic_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    variants: &[QueryVariant],
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let has_columns = !validation.columns.is_empty();
    let is_select = parsed.kind == crate::parse::QueryKind::Select;

    let query_method = if is_select {
        quote! { query_readonly }
    } else {
        quote! { query_readwrite }
    };

    let row_decode = if has_columns {
        gen_sqlite_row_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let needs_limit = has_columns && is_select && !parsed.normalized_sql.contains(" limit ");

        let qm = &query_method;
        let fetch_one_dispatcher =
            gen_sqlite_variant_dispatcher(parsed, variants, needs_limit, |sql_lit, sql_hash| {
                quote! {
                    let (result, arena) = pool.#qm(#sql_lit, #sql_hash, params).await?;
                    if result.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            result.len() as u64,
                        ));
                    }
                    let _bsql_row_idx: usize = 0;
                    Ok(#result_name { #row_decode })
                }
            });

        let fetch_all_dispatcher =
            gen_sqlite_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
                quote! {
                    let (result, arena) = pool.#qm(#sql_lit, #sql_hash, params).await?;
                    let mut out = Vec::with_capacity(result.len());
                    for _bsql_row_idx in 0..result.len() {
                        out.push(#result_name { #row_decode });
                    }
                    Ok(out)
                }
            });

        let fetch_optional_dispatcher =
            gen_sqlite_variant_dispatcher(parsed, variants, needs_limit, |sql_lit, sql_hash| {
                quote! {
                    let (result, arena) = pool.#qm(#sql_lit, #sql_hash, params).await?;
                    match result.len() {
                        0 => Ok(None),
                        1 => {
                            let _bsql_row_idx: usize = 0;
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
            pub async fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #fetch_one_dispatcher
            }

            pub async fn fetch_all(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                #fetch_all_dispatcher
            }

            pub async fn fetch_optional(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                #fetch_optional_dispatcher
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_dispatcher =
        gen_sqlite_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
            quote! {
                pool.execute_sql(#sql_lit, #sql_hash, params).await
            }
        });

    let execute_method = quote! {
        pub async fn execute(
            self,
            pool: &::bsql_core::SqlitePool,
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

/// Generate the match dispatcher for SQLite dynamic query variants.
fn gen_sqlite_variant_dispatcher<F>(
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

            // Convert $N -> ?N for SQLite
            let sqlite_sql = pg_to_sqlite_params(&variant.sql);
            let sql_str = if inject_limit {
                format!("{sqlite_sql} LIMIT 2")
            } else {
                sqlite_sql
            };

            let sql_hash = bsql_core::rapid_hash_str(&sql_str);

            // Build param conversions for this variant
            let param_conversions: Vec<TokenStream> = variant
                .params
                .iter()
                .map(|p| {
                    let name = param_ident(&p.name);
                    if p.rust_type.starts_with("Option<") {
                        // Unwrap the Option — we know it's Some because the match says true
                        let inner = p.rust_type
                            .strip_prefix("Option<")
                            .and_then(|s| s.strip_suffix('>'))
                            .unwrap_or(&p.rust_type);
                        let inner_conv = param_value_conversion(quote! { v }, inner);
                        quote! {
                            match &self.#name {
                                Some(v) => #inner_conv,
                                None => ::bsql_core::driver_sqlite::ParamValue::Null,
                            }
                        }
                    } else {
                        gen_param_to_param_value(&name, &p.rust_type)
                    }
                })
                .collect();

            let params_build = if param_conversions.is_empty() {
                quote! { let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::SmallVec::new(); }
            } else {
                quote! {
                    let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::smallvec![
                        #(#param_conversions),*
                    ];
                }
            };

            let body = body_fn(&sql_str, sql_hash);

            quote! {
                #pattern => {
                    #params_build
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

/// Generate the constructor for a dynamic SQLite query executor.
fn gen_dynamic_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);

    let mut field_names: Vec<proc_macro2::Ident> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for p in &parsed.params {
        field_names.push(param_ident(&p.name));
        seen.insert(p.name.clone());
    }

    for clause in &parsed.optional_clauses {
        for p in &clause.params {
            if seen.insert(p.name.clone()) {
                field_names.push(param_ident(&p.name));
            }
        }
    }

    quote! { #executor_name { #(#field_names,)* _marker: ::std::marker::PhantomData } }
}

// ===========================================================================
// Sort query codegen
// ===========================================================================

/// Generate Rust code for a SQLite query with `$[sort: EnumType]`.
///
/// The sort fragment is spliced into SQL at runtime. Each variant gets its
/// own sql_hash. Uses OnceLock cache same as the PG sort codegen.
pub fn generate_sort_sqlite_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    sort_enum_name: &str,
) -> TokenStream {
    let result_struct = gen_result_struct(parsed, validation);
    let sort_enum_ident = format_ident!("{}", sort_enum_name);
    let executor_name = executor_struct_name(parsed);

    // Build executor struct fields: all params + sort
    let param_fields: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            let ty = inject_lifetime(&p.rust_type);
            quote! { #name: #ty }
        })
        .collect();

    let executor_struct = quote! {
        #[must_use = "query is not executed until .fetch_one(), .fetch_all(), .fetch_optional(), or .execute() is called"]
        #[allow(non_camel_case_types)]
        struct #executor_name<'_bsql> {
            #(#param_fields,)*
            sort: #sort_enum_ident,
            _marker: ::std::marker::PhantomData<&'_bsql ()>,
        }
    };

    // Build params
    let param_conversions: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            gen_param_to_param_value(&name, &p.rust_type)
        })
        .collect();

    let params_build = if param_conversions.is_empty() {
        quote! { let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::SmallVec::new(); }
    } else {
        quote! {
            let params: ::bsql_core::driver_sqlite::SmallVec<[::bsql_core::driver_sqlite::ParamValue; 8]> = ::bsql_core::driver_sqlite::smallvec![
                #(#param_conversions),*
            ];
        }
    };

    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let query_method = if is_select {
        quote! { query_readonly }
    } else {
        quote! { query_readwrite }
    };

    // Convert $N -> ?N in the SQL template
    let sqlite_template = pg_to_sqlite_params(&parsed.positional_sql);
    let has_columns = !validation.columns.is_empty();

    let sort_parts: Vec<&str> = sqlite_template.split("{SORT}").collect();
    let sql_prefix = sort_parts[0];
    let sql_suffix = if sort_parts.len() > 1 {
        sort_parts[1]
    } else {
        ""
    };

    let needs_limit = has_columns && is_select && !parsed.normalized_sql.contains(" limit ");

    let limited_suffix = if needs_limit {
        format!("{sql_suffix} LIMIT 2")
    } else {
        sql_suffix.to_owned()
    };
    let limited_suffix_lit = &limited_suffix;

    // Sort SQL cache: builds the final SQL from prefix + sort_fragment + suffix
    let build_sql = quote! {
        static SORT_SQL_CACHE: ::std::sync::OnceLock<::std::sync::Mutex<Vec<(usize, String, u64)>>> = ::std::sync::OnceLock::new();
        let sort_fragment: &'static str = self.sort.sql();
        let cache = SORT_SQL_CACHE.get_or_init(|| ::std::sync::Mutex::new(Vec::new()));
        let key = sort_fragment.as_ptr() as usize;
        let (sql, sql_hash) = {
            let guard = cache.lock().unwrap();
            if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                (entry.1.clone(), entry.2)
            } else {
                drop(guard);
                let built = format!("{}{}{}", #sql_prefix, sort_fragment, #sql_suffix);
                let hash = ::bsql_core::rapid_hash_str(&built);
                let mut guard = cache.lock().unwrap();
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.clone(), entry.2)
                } else {
                    guard.push((key, built.clone(), hash));
                    (built, hash)
                }
            }
        };
    };

    let build_limited_sql = if needs_limit {
        quote! {
            static SORT_LIMITED_SQL_CACHE: ::std::sync::OnceLock<::std::sync::Mutex<Vec<(usize, String, u64)>>> = ::std::sync::OnceLock::new();
            let sort_fragment: &'static str = self.sort.sql();
            let cache = SORT_LIMITED_SQL_CACHE.get_or_init(|| ::std::sync::Mutex::new(Vec::new()));
            let key = sort_fragment.as_ptr() as usize;
            let (sql, sql_hash) = {
                let guard = cache.lock().unwrap();
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.clone(), entry.2)
                } else {
                    drop(guard);
                    let built = format!("{}{}{}", #sql_prefix, sort_fragment, #limited_suffix_lit);
                    let hash = ::bsql_core::rapid_hash_str(&built);
                    let mut guard = cache.lock().unwrap();
                    if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                        (entry.1.clone(), entry.2)
                    } else {
                        guard.push((key, built.clone(), hash));
                        (built, hash)
                    }
                }
            };
        }
    } else {
        build_sql.clone()
    };

    let row_decode = if has_columns {
        gen_sqlite_row_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let qm = &query_method;

        quote! {
            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                pub async fn fetch_one(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    #params_build
                    #build_limited_sql
                    let (result, arena) = pool.#qm(&sql, sql_hash, params).await?;
                    if result.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            result.len() as u64,
                        ));
                    }
                    let _bsql_row_idx: usize = 0;
                    Ok(#result_name { #row_decode })
                }

                pub async fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #params_build
                    #build_sql
                    let (result, arena) = pool.#qm(&sql, sql_hash, params).await?;
                    let mut out = Vec::with_capacity(result.len());
                    for _bsql_row_idx in 0..result.len() {
                        out.push(#result_name { #row_decode });
                    }
                    Ok(out)
                }

                pub async fn fetch_optional(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    #params_build
                    #build_limited_sql
                    let (result, arena) = pool.#qm(&sql, sql_hash, params).await?;
                    match result.len() {
                        0 => Ok(None),
                        1 => {
                            let _bsql_row_idx: usize = 0;
                            Ok(Some(#result_name { #row_decode }))
                        }
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }

                pub async fn execute(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<u64> {
                    #params_build
                    #build_sql
                    pool.execute_sql(&sql, sql_hash, params).await
                }
            }
        }
    } else {
        // Execute-only
        quote! {
            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                pub async fn execute(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<u64> {
                    #params_build
                    #build_sql
                    pool.execute_sql(&sql, sql_hash, params).await
                }
            }
        }
    };

    // Constructor captures params + sort from scope
    let field_inits: Vec<proc_macro2::Ident> =
        parsed.params.iter().map(|p| param_ident(&p.name)).collect();

    let constructor = quote! {
        #executor_name {
            #(#field_inits,)*
            sort,
            _marker: ::std::marker::PhantomData,
        }
    };

    quote! {
        {
            #result_struct
            #executor_struct
            #fetch_methods
            #constructor
        }
    }
}

// ===========================================================================
// Feature-gated type decode for SQLite
// ===========================================================================

/// Generate decode for feature-gated types (time, chrono, uuid, decimal).
/// SQLite stores these as TEXT — we parse from the string representation.
fn gen_sqlite_feature_gated_decode(idx: usize, rust_type: &str) -> TokenStream {
    let col_idx = idx.to_string();
    match rust_type {
        "::uuid::Uuid" | "uuid::Uuid" => {
            let err = gen_decode_error(&col_idx, "uuid::Uuid");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    s.parse::<::uuid::Uuid>().map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #col_idx, "uuid::Uuid", "invalid UUID text", e,
                    ))?
                }
            }
        }
        "::time::PrimitiveDateTime" | "time::PrimitiveDateTime" => {
            let err = gen_decode_error(&col_idx, "time::PrimitiveDateTime");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    ::time::PrimitiveDateTime::parse(s, &::time::format_description::well_known::iso8601::Iso8601::DEFAULT)
                        .or_else(|_| {
                            // Try common SQLite datetime formats
                            ::time::PrimitiveDateTime::parse(s, &::time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
                        })
                        .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                            #col_idx, "time::PrimitiveDateTime", "invalid datetime text", e,
                        ))?
                }
            }
        }
        "::time::Date" | "time::Date" => {
            let err = gen_decode_error(&col_idx, "time::Date");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    ::time::Date::parse(s, &::time::macros::format_description!("[year]-[month]-[day]"))
                        .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                            #col_idx, "time::Date", "invalid date text", e,
                        ))?
                }
            }
        }
        "::time::Time" | "time::Time" => {
            let err = gen_decode_error(&col_idx, "time::Time");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    ::time::Time::parse(s, &::time::macros::format_description!("[hour]:[minute]:[second]"))
                        .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                            #col_idx, "time::Time", "invalid time text", e,
                        ))?
                }
            }
        }
        "::chrono::NaiveDateTime" | "chrono::NaiveDateTime" => {
            let err = gen_decode_error(&col_idx, "chrono::NaiveDateTime");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    s.parse::<::chrono::NaiveDateTime>().map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #col_idx, "chrono::NaiveDateTime", "invalid datetime text", e,
                    ))?
                }
            }
        }
        "::chrono::NaiveDate" | "chrono::NaiveDate" => {
            let err = gen_decode_error(&col_idx, "chrono::NaiveDate");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    s.parse::<::chrono::NaiveDate>().map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #col_idx, "chrono::NaiveDate", "invalid date text", e,
                    ))?
                }
            }
        }
        "::chrono::NaiveTime" | "chrono::NaiveTime" => {
            let err = gen_decode_error(&col_idx, "chrono::NaiveTime");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    s.parse::<::chrono::NaiveTime>().map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #col_idx, "chrono::NaiveTime", "invalid time text", e,
                    ))?
                }
            }
        }
        "::rust_decimal::Decimal" | "rust_decimal::Decimal" => {
            let err = gen_decode_error(&col_idx, "rust_decimal::Decimal");
            quote! {
                {
                    let s = result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?;
                    s.parse::<::rust_decimal::Decimal>().map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #col_idx, "rust_decimal::Decimal", "invalid decimal text", e,
                    ))?
                }
            }
        }
        _ => {
            // Fallback: read as string
            let err = gen_decode_error(&col_idx, rust_type);
            quote! { result.get_str(_bsql_row_idx, #idx, &arena).ok_or_else(|| #err)?.to_owned() }
        }
    }
}
