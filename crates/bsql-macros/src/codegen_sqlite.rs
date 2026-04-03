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
    let arena_result_struct = gen_arena_result_struct(parsed, validation);
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_executor_impls(parsed, validation, &sqlite_sql);
    let constructor = gen_constructor(parsed);

    quote! {
        {
            #result_struct
            #arena_result_struct
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

// --- Arena result struct (for fetch_all with text/blob columns) ---

/// Returns true if any column in the result set is String, Vec<u8>, or
/// an Option wrapping one of those — i.e., types that require heap allocation
/// in the non-arena path and benefit from arena-backed borrowed references.
fn has_arena_columns(validation: &ValidationResult) -> bool {
    validation
        .columns
        .iter()
        .any(|col| is_arena_type(&col.rust_type))
}

/// Returns true if `rust_type` is a type that benefits from arena allocation.
fn is_arena_type(rust_type: &str) -> bool {
    match rust_type {
        "String" | "Vec<u8>" => true,
        _ => {
            if let Some(inner) = rust_type
                .strip_prefix("Option<")
                .and_then(|s| s.strip_suffix('>'))
            {
                matches!(inner, "String" | "Vec<u8>")
            } else {
                false
            }
        }
    }
}

/// Convert a column rust_type to its arena-borrowed equivalent.
/// String -> &'static str, Vec<u8> -> &'static [u8], Option<String> -> Option<&'static str>, etc.
fn arena_result_type(type_str: &str) -> TokenStream {
    match type_str {
        "String" => quote! { &'static str },
        "Vec<u8>" => quote! { &'static [u8] },
        _ => {
            if let Some(inner) = type_str
                .strip_prefix("Option<")
                .and_then(|s| s.strip_suffix('>'))
            {
                match inner {
                    "String" => quote! { Option<&'static str> },
                    "Vec<u8>" => quote! { Option<&'static [u8]> },
                    _ => parse_result_type(type_str),
                }
            } else {
                parse_result_type(type_str)
            }
        }
    }
}

fn arena_result_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlArenaResult_{}", &parsed.statement_name)
}

fn gen_arena_result_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() || !has_arena_columns(validation) {
        return TokenStream::new();
    }

    let struct_name = arena_result_struct_name(parsed);
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", deduped_names[i]);
        let field_type = arena_result_type(&col.rust_type);
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
    let is_write = !is_select;

    // Build the params SmallVec for arena-based methods (streaming only)
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

    // Build direct param refs for all query/execute methods (zero-copy path)
    let direct_param_binds: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            gen_direct_param_ref(&name, &p.rust_type)
        })
        .collect();

    let direct_params_build = if direct_param_binds.is_empty() {
        quote! { let _bsql_params: &[&dyn ::bsql_core::driver_sqlite::SqliteEncode] = &[]; }
    } else {
        quote! {
            let _bsql_params: &[&dyn ::bsql_core::driver_sqlite::SqliteEncode] = &[
                #(#direct_param_binds),*
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

    let direct_decode = if has_columns {
        gen_sqlite_direct_decode(validation)
    } else {
        TokenStream::new()
    };

    let use_arena = has_columns && has_arena_columns(validation);

    let arena_decode = if use_arena {
        gen_sqlite_arena_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);

        let fetch_all_method = if use_arena {
            let arena_name = arena_result_struct_name(parsed);
            quote! {
                /// Fetch all rows. Arena-backed — text/blob columns borrow from a
                /// contiguous bump allocator instead of individual heap allocations.
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<::bsql_core::driver_sqlite::ArenaRows<#arena_name>> {
                    #direct_params_build
                    pool.fetch_all_arena(
                        #sql_lit,
                        #sql_hash_val,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt, _bsql_arena| {
                            Ok(#arena_name { #arena_decode })
                        },
                    )
                }
            }
        } else {
            quote! {
                /// Fetch all rows. Zero-copy direct decode — no arena allocation.
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #direct_params_build
                    pool.fetch_all_direct(
                        #sql_lit,
                        #sql_hash_val,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt| {
                            Ok(#result_name { #direct_decode })
                        },
                    )
                }
            }
        };

        quote! {
            /// Fetch exactly one row. Zero-copy direct decode — no arena allocation.
            pub fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #direct_params_build
                pool.fetch_one_direct(
                    #limited_sql_lit,
                    #limited_sql_hash_val,
                    _bsql_params,
                    #is_write,
                    |_bsql_stmt| {
                        Ok(#result_name { #direct_decode })
                    },
                )
            }

            #fetch_all_method

            /// Fetch zero or one row. Zero-copy direct decode — no arena allocation.
            pub fn fetch_optional(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                #direct_params_build
                pool.fetch_optional_direct(
                    #limited_sql_lit,
                    #limited_sql_hash_val,
                    _bsql_params,
                    #is_write,
                    |_bsql_stmt| {
                        Ok(#result_name { #direct_decode })
                    },
                )
            }

            /// Stream rows in chunks.
            pub fn fetch_stream(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<::bsql_core::SqliteStreamingQuery> {
                #params_build
                pool.query_streaming(#sql_lit, #sql_hash_val, params, 64)
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        /// Execute the statement (INSERT/UPDATE/DELETE), return affected rows.
        pub fn execute(
            self,
            pool: &::bsql_core::SqlitePool,
        ) -> ::bsql_core::BsqlResult<u64> {
            #direct_params_build
            pool.execute_direct(#sql_lit, #sql_hash_val, _bsql_params)
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

// --- Direct param ref for zero-copy path ---

/// Generate a reference to a param for direct bind (no ParamValue allocation).
fn gen_direct_param_ref(name: &proc_macro2::Ident, rust_type: &str) -> TokenStream {
    // For Option<T>, we need special handling to bind NULL or the inner value
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let inner_ref = direct_param_ref_inner(quote! { v }, inner);
        return quote! {
            match &self.#name {
                Some(v) => #inner_ref,
                None => &::bsql_core::driver_sqlite::ParamValue::Null as &dyn ::bsql_core::driver_sqlite::SqliteEncode,
            }
        };
    }
    direct_param_ref_inner(quote! { self.#name }, rust_type)
}

fn direct_param_ref_inner(val_expr: TokenStream, rust_type: &str) -> TokenStream {
    match rust_type {
        "bool" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
            // These types implement SqliteEncode directly (or via ParamValue)
            // Use ParamValue for type coercion (i8->i64, f32->f64, bool->i64)
            let conv = param_value_conversion(val_expr, rust_type);
            quote! { &(#conv) as &dyn ::bsql_core::driver_sqlite::SqliteEncode }
        }
        "&str" => {
            quote! { &::bsql_core::driver_sqlite::ParamValue::Text(#val_expr.to_owned()) as &dyn ::bsql_core::driver_sqlite::SqliteEncode }
        }
        "String" => {
            quote! { &::bsql_core::driver_sqlite::ParamValue::Text(#val_expr.clone()) as &dyn ::bsql_core::driver_sqlite::SqliteEncode }
        }
        "&[u8]" => {
            quote! { &::bsql_core::driver_sqlite::ParamValue::Blob(#val_expr.to_vec()) as &dyn ::bsql_core::driver_sqlite::SqliteEncode }
        }
        "Vec<u8>" => {
            quote! { &::bsql_core::driver_sqlite::ParamValue::Blob(#val_expr.clone()) as &dyn ::bsql_core::driver_sqlite::SqliteEncode }
        }
        _ => {
            // Feature-gated or unknown type: convert via ToString -> ParamValue::Text
            quote! { &::bsql_core::driver_sqlite::ParamValue::Text(::std::string::ToString::to_string(&#val_expr)) as &dyn ::bsql_core::driver_sqlite::SqliteEncode }
        }
    }
}

// --- Direct decode from StmtHandle (zero-copy for fetch_one/fetch_optional) ---

/// Generate field decoding that reads directly from the StmtHandle.
fn gen_sqlite_direct_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let col_idx = i as i32;
        let col = &validation.columns[i];
        let decode_expr = gen_sqlite_direct_column_decode(col_idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    quote! { #(#fields),* }
}

fn gen_sqlite_direct_column_decode(idx: i32, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_sqlite_direct_nullable_decode(idx, inner)
    } else {
        gen_sqlite_direct_not_null_decode(idx, rust_type)
    }
}

fn gen_sqlite_direct_not_null_decode(idx: i32, rust_type: &str) -> TokenStream {
    let col_idx_str = idx.to_string();
    match rust_type {
        "bool" => {
            let err = gen_direct_decode_error(&col_idx_str, "bool");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    _bsql_stmt.column_int64(#idx) != 0
                }
            }
        }
        "i64" => {
            let err = gen_direct_decode_error(&col_idx_str, "i64");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    _bsql_stmt.column_int64(#idx)
                }
            }
        }
        "f64" => {
            let err = gen_direct_decode_error(&col_idx_str, "f64");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    _bsql_stmt.column_double(#idx)
                }
            }
        }
        "String" => {
            let err = gen_direct_decode_error(&col_idx_str, "String");
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) }
                        .to_owned()
                }
            }
        }
        "Vec<u8>" => {
            let err = gen_direct_decode_error(&col_idx_str, "Vec<u8>");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    _bsql_stmt.column_blob(#idx).to_vec()
                }
            }
        }
        _ => gen_sqlite_direct_feature_gated_decode(idx, rust_type),
    }
}

fn gen_sqlite_direct_nullable_decode(idx: i32, inner_type: &str) -> TokenStream {
    match inner_type {
        "bool" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                Some(_bsql_stmt.column_int64(#idx) != 0)
            }
        },
        "i64" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                Some(_bsql_stmt.column_int64(#idx))
            }
        },
        "f64" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                Some(_bsql_stmt.column_double(#idx))
            }
        },
        "String" => quote! {
            _bsql_stmt.column_text(#idx)
                .and_then(|b| Some(unsafe { ::std::str::from_utf8_unchecked(b) }))
                .map(|s| s.to_owned())
        },
        "Vec<u8>" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                Some(_bsql_stmt.column_blob(#idx).to_vec())
            }
        },
        _ => {
            let not_null_decode = gen_sqlite_direct_feature_gated_decode(idx, inner_type);
            quote! {
                if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                    None
                } else {
                    Some(#not_null_decode)
                }
            }
        }
    }
}

fn gen_direct_decode_error(col_idx: &str, type_name: &str) -> TokenStream {
    quote! {
        ::bsql_core::driver_sqlite::SqliteError::Internal(
            format!("NULL or invalid data at column {} (expected {})", #col_idx, #type_name),
        )
    }
}

fn gen_sqlite_direct_feature_gated_decode(idx: i32, rust_type: &str) -> TokenStream {
    let col_idx_str = idx.to_string();
    let err = gen_direct_decode_error(&col_idx_str, rust_type);

    match rust_type {
        "::uuid::Uuid" | "uuid::Uuid" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    s.parse::<::uuid::Uuid>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid UUID in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::time::PrimitiveDateTime" | "time::PrimitiveDateTime" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    ::time::PrimitiveDateTime::parse(s, &::time::format_description::well_known::iso8601::Iso8601::DEFAULT)
                        .or_else(|_| {
                            ::time::PrimitiveDateTime::parse(s, &::time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
                        })
                        .map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                            format!("invalid datetime in column {}: {}", #col_idx_str, e),
                        ))?
                }
            }
        }
        "::time::Date" | "time::Date" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    ::time::Date::parse(s, &::time::macros::format_description!("[year]-[month]-[day]"))
                        .map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                            format!("invalid date in column {}: {}", #col_idx_str, e),
                        ))?
                }
            }
        }
        "::time::Time" | "time::Time" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    ::time::Time::parse(s, &::time::macros::format_description!("[hour]:[minute]:[second]"))
                        .map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                            format!("invalid time in column {}: {}", #col_idx_str, e),
                        ))?
                }
            }
        }
        "::chrono::NaiveDateTime" | "chrono::NaiveDateTime" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    s.parse::<::chrono::NaiveDateTime>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid datetime in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::chrono::NaiveDate" | "chrono::NaiveDate" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    s.parse::<::chrono::NaiveDate>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid date in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::chrono::NaiveTime" | "chrono::NaiveTime" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    s.parse::<::chrono::NaiveTime>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid time in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::rust_decimal::Decimal" | "rust_decimal::Decimal" => {
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let s = unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) };
                    s.parse::<::rust_decimal::Decimal>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid decimal in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        _ => {
            // Fallback: read as text
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    unsafe { ::std::str::from_utf8_unchecked(_bsql_bytes) }
                        .to_owned()
                }
            }
        }
    }
}

// --- Arena-backed decode for fetch_all ---
//
// These functions generate decode code that copies text/blob data into the arena
// and returns &'static str / &'static [u8] via lifetime extension. Non-text
// columns (integers, floats, bools) are decoded identically to the direct path.

fn gen_sqlite_arena_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let col = &validation.columns[i];
        let col_idx = i as i32;
        let decode_expr = gen_sqlite_arena_column_decode(col_idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    quote! { #(#fields),* }
}

fn gen_sqlite_arena_column_decode(idx: i32, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_sqlite_arena_nullable_decode(idx, inner)
    } else {
        gen_sqlite_arena_not_null_decode(idx, rust_type)
    }
}

fn gen_sqlite_arena_not_null_decode(idx: i32, rust_type: &str) -> TokenStream {
    let col_idx_str = idx.to_string();
    match rust_type {
        // Text: copy into arena, return &'static str
        "String" => {
            let err = gen_direct_decode_error(&col_idx_str, "&str");
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let _bsql_off = _bsql_arena.alloc_copy(_bsql_bytes);
                    // SAFETY: SQLite TEXT columns are guaranteed valid UTF-8
                    // (validated on INSERT by sqlite3_bind_text). Skip redundant
                    // re-validation for maximum throughput.
                    let _bsql_s = unsafe { _bsql_arena.get_str_unchecked(_bsql_off, _bsql_bytes.len()) };
                    unsafe { ::bsql_core::driver_sqlite::extend_lifetime_str(_bsql_s) }
                }
            }
        }
        // Blob: copy into arena, return &'static [u8]
        "Vec<u8>" => {
            let err = gen_direct_decode_error(&col_idx_str, "&[u8]");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    let _bsql_raw = _bsql_stmt.column_blob(#idx);
                    let _bsql_off = _bsql_arena.alloc_copy(_bsql_raw);
                    let _bsql_slice = _bsql_arena.get(_bsql_off, _bsql_raw.len());
                    unsafe { ::bsql_core::driver_sqlite::extend_lifetime_bytes(_bsql_slice) }
                }
            }
        }
        // All non-text types: identical to direct decode (no arena needed)
        _ => gen_sqlite_direct_not_null_decode(idx, rust_type),
    }
}

fn gen_sqlite_arena_nullable_decode(idx: i32, inner_type: &str) -> TokenStream {
    match inner_type {
        // Option<String> -> Option<&'static str>
        "String" => quote! {
            match _bsql_stmt.column_text(#idx) {
                None => None,
                Some(_bsql_bytes) => {
                    let _bsql_off = _bsql_arena.alloc_copy(_bsql_bytes);
                    // SAFETY: SQLite TEXT is guaranteed valid UTF-8
                    let _bsql_s = unsafe { _bsql_arena.get_str_unchecked(_bsql_off, _bsql_bytes.len()) };
                    Some(unsafe { ::bsql_core::driver_sqlite::extend_lifetime_str(_bsql_s) })
                }
            }
        },
        // Option<Vec<u8>> -> Option<&'static [u8]>
        "Vec<u8>" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                let _bsql_raw = _bsql_stmt.column_blob(#idx);
                let _bsql_off = _bsql_arena.alloc_copy(_bsql_raw);
                let _bsql_slice = _bsql_arena.get(_bsql_off, _bsql_raw.len());
                Some(unsafe { ::bsql_core::driver_sqlite::extend_lifetime_bytes(_bsql_slice) })
            }
        },
        // Non-text option types: identical to direct decode
        _ => gen_sqlite_direct_nullable_decode(idx, inner_type),
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
    let arena_result_struct = gen_arena_result_struct(parsed, validation);
    let executor_struct = gen_dynamic_executor_struct(parsed);
    let executor_impls = gen_dynamic_executor_impls(parsed, validation, variants);
    let constructor = gen_dynamic_constructor(parsed);

    quote! {
        {
            #result_struct
            #arena_result_struct
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
    let is_write = !is_select;

    let direct_decode = if has_columns {
        gen_sqlite_direct_decode(validation)
    } else {
        TokenStream::new()
    };

    let use_arena = has_columns && has_arena_columns(validation);

    let arena_decode = if use_arena {
        gen_sqlite_arena_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let needs_limit = has_columns && is_select && !parsed.normalized_sql.contains(" limit ");

        let fetch_one_dispatcher = gen_sqlite_direct_variant_dispatcher(
            parsed,
            variants,
            needs_limit,
            |sql_lit, sql_hash| {
                quote! {
                    pool.fetch_one_direct(
                        #sql_lit,
                        #sql_hash,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt| {
                            Ok(#result_name { #direct_decode })
                        },
                    )
                }
            },
        );

        let fetch_all_method = if use_arena {
            let arena_name = arena_result_struct_name(parsed);
            let fetch_all_dispatcher = gen_sqlite_direct_variant_dispatcher(
                parsed,
                variants,
                false,
                |sql_lit, sql_hash| {
                    quote! {
                        pool.fetch_all_arena(
                            #sql_lit,
                            #sql_hash,
                            _bsql_params,
                            #is_write,
                            |_bsql_stmt, _bsql_arena| {
                                Ok(#arena_name { #arena_decode })
                            },
                        )
                    }
                },
            );
            quote! {
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<::bsql_core::driver_sqlite::ArenaRows<#arena_name>> {
                    #fetch_all_dispatcher
                }
            }
        } else {
            let fetch_all_dispatcher = gen_sqlite_direct_variant_dispatcher(
                parsed,
                variants,
                false,
                |sql_lit, sql_hash| {
                    quote! {
                        pool.fetch_all_direct(
                            #sql_lit,
                            #sql_hash,
                            _bsql_params,
                            #is_write,
                            |_bsql_stmt| {
                                Ok(#result_name { #direct_decode })
                            },
                        )
                    }
                },
            );
            quote! {
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #fetch_all_dispatcher
                }
            }
        };

        let fetch_optional_dispatcher = gen_sqlite_direct_variant_dispatcher(
            parsed,
            variants,
            needs_limit,
            |sql_lit, sql_hash| {
                quote! {
                    pool.fetch_optional_direct(
                        #sql_lit,
                        #sql_hash,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt| {
                            Ok(#result_name { #direct_decode })
                        },
                    )
                }
            },
        );

        quote! {
            pub fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #fetch_one_dispatcher
            }

            #fetch_all_method

            pub fn fetch_optional(
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
        gen_sqlite_direct_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
            quote! {
                pool.execute_direct(#sql_lit, #sql_hash, _bsql_params)
            }
        });

    let execute_method = quote! {
        pub fn execute(
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

/// Generate the match dispatcher for SQLite dynamic query variants (direct path — no arena).
fn gen_sqlite_direct_variant_dispatcher<F>(
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

            let sqlite_sql = pg_to_sqlite_params(&variant.sql);
            let sql_str = if inject_limit {
                format!("{sqlite_sql} LIMIT 2")
            } else {
                sqlite_sql
            };

            let sql_hash = bsql_core::rapid_hash_str(&sql_str);

            // Build direct param refs for this variant
            let direct_param_binds: Vec<TokenStream> = variant
                .params
                .iter()
                .map(|p| {
                    let name = param_ident(&p.name);
                    gen_direct_param_ref(&name, &p.rust_type)
                })
                .collect();

            let direct_params_build = if direct_param_binds.is_empty() {
                quote! { let _bsql_params: &[&dyn ::bsql_core::driver_sqlite::SqliteEncode] = &[]; }
            } else {
                quote! {
                    let _bsql_params: &[&dyn ::bsql_core::driver_sqlite::SqliteEncode] = &[
                        #(#direct_param_binds),*
                    ];
                }
            };

            let body = body_fn(&sql_str, sql_hash);

            quote! {
                #pattern => {
                    #direct_params_build
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
    let arena_result_struct = gen_arena_result_struct(parsed, validation);
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

    // Build direct param refs for zero-copy path
    let direct_param_binds: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            gen_direct_param_ref(&name, &p.rust_type)
        })
        .collect();

    let direct_params_build = if direct_param_binds.is_empty() {
        quote! { let _bsql_params: &[&dyn ::bsql_core::driver_sqlite::SqliteEncode] = &[]; }
    } else {
        quote! {
            let _bsql_params: &[&dyn ::bsql_core::driver_sqlite::SqliteEncode] = &[
                #(#direct_param_binds),*
            ];
        }
    };

    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let is_write = !is_select;

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

    let direct_decode = if has_columns {
        gen_sqlite_direct_decode(validation)
    } else {
        TokenStream::new()
    };

    let use_arena = has_columns && has_arena_columns(validation);

    let arena_decode = if use_arena {
        gen_sqlite_arena_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);

        let fetch_all_method = if use_arena {
            let arena_name = arena_result_struct_name(parsed);
            quote! {
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<::bsql_core::driver_sqlite::ArenaRows<#arena_name>> {
                    #direct_params_build
                    #build_sql
                    pool.fetch_all_arena(
                        &sql,
                        sql_hash,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt, _bsql_arena| {
                            Ok(#arena_name { #arena_decode })
                        },
                    )
                }
            }
        } else {
            quote! {
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #direct_params_build
                    #build_sql
                    pool.fetch_all_direct(
                        &sql,
                        sql_hash,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt| {
                            Ok(#result_name { #direct_decode })
                        },
                    )
                }
            }
        };

        quote! {
            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                pub fn fetch_one(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    #direct_params_build
                    #build_limited_sql
                    pool.fetch_one_direct(
                        &sql,
                        sql_hash,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt| {
                            Ok(#result_name { #direct_decode })
                        },
                    )
                }

                #fetch_all_method

                pub fn fetch_optional(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    #direct_params_build
                    #build_limited_sql
                    pool.fetch_optional_direct(
                        &sql,
                        sql_hash,
                        _bsql_params,
                        #is_write,
                        |_bsql_stmt| {
                            Ok(#result_name { #direct_decode })
                        },
                    )
                }

                pub fn execute(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<u64> {
                    #direct_params_build
                    #build_sql
                    pool.execute_direct(&sql, sql_hash, _bsql_params)
                }
            }
        }
    } else {
        // Execute-only
        quote! {
            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                pub fn execute(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<u64> {
                    #direct_params_build
                    #build_sql
                    pool.execute_direct(&sql, sql_hash, _bsql_params)
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
            #arena_result_struct
            #executor_struct
            #fetch_methods
            #constructor
        }
    }
}
