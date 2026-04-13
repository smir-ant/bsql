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
    let for_each_row_struct = gen_for_each_row_struct(parsed, validation);
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_executor_impls(parsed, validation, &sqlite_sql);
    let constructor = gen_constructor(parsed);

    quote! {
        {
            #result_struct
            #arena_result_struct
            #for_each_row_struct
            #executor_struct
            #executor_impls
            #constructor
        }
    }
}

/// Generate the complete Rust code for a SQLite `query_as!` invocation.
///
/// Like `generate_sqlite_query_code` but maps results into `#target_type` instead
/// of generating an anonymous struct.
pub fn generate_sqlite_query_as_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    target_type: &syn::Path,
) -> TokenStream {
    let sqlite_sql = pg_to_sqlite_params(&parsed.positional_sql);
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_query_as_executor_impls(parsed, validation, &sqlite_sql, target_type);
    let constructor = gen_constructor(parsed);

    quote! {
        {
            #executor_struct
            #executor_impls
            #constructor
        }
    }
}

/// Generate executor impls for SQLite `query_as!` — maps results into `#target_type`.
fn gen_query_as_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    sqlite_sql: &str,
    target_type: &syn::Path,
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let sql_lit = sqlite_sql;

    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let is_write = !is_select;

    // Build direct param refs
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

    let sql_hash_val = bsql_core::rapid_hash_str(sqlite_sql);

    let has_columns = !validation.columns.is_empty();

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

    let sqlite_column_check = gen_sqlite_column_count_check(validation);
    let inline_bind = gen_inline_param_bind();

    let fetch_methods = if has_columns {
        let decode_one_inner = wrap_decode_as_bsql_for_path(target_type, &direct_decode);
        let decode_opt_inner = wrap_decode_as_bsql_for_path(target_type, &direct_decode);
        let decode_all = wrap_decode_as_bsql_for_path(target_type, &direct_decode);

        let inline_acquire_one = gen_inline_acquire(is_write);
        let inline_acquire_opt = gen_inline_acquire(is_write);
        let inline_acquire_all = gen_inline_acquire(is_write);

        let fetch_one_method = quote! {
            /// Fetch exactly one row, mapped into the target struct.
            pub fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#target_type> {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_one;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#limited_sql_lit, #limited_sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                #sqlite_column_check
                match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                    ::bsql_core::driver_sqlite::StepResult::Row => {
                        let _bsql_result = #decode_one_inner;
                        if let ::bsql_core::driver_sqlite::StepResult::Row =
                            _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)?
                        {
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            return Err(::bsql_core::BsqlError::from_sqlite(
                                ::bsql_core::driver_sqlite::SqliteError::Internal(
                                    "expected 1 row, got 2+".into(),
                                ),
                            ));
                        }
                        _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                        drop(_bsql_conn);
                        Ok(_bsql_result)
                    }
                    ::bsql_core::driver_sqlite::StepResult::Done => {
                        _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                        drop(_bsql_conn);
                        Err(::bsql_core::BsqlError::from_sqlite(
                            ::bsql_core::driver_sqlite::SqliteError::Internal(
                                "expected 1 row, got 0".into(),
                            ),
                        ))
                    }
                }
            }
        };

        let fetch_optional_method = quote! {
            /// Fetch zero or one row, mapped into the target struct.
            pub fn fetch_optional(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Option<#target_type>> {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_opt;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#limited_sql_lit, #limited_sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                #sqlite_column_check
                match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                    ::bsql_core::driver_sqlite::StepResult::Row => {
                        let _bsql_result = #decode_opt_inner;
                        _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                        drop(_bsql_conn);
                        Ok(Some(_bsql_result))
                    }
                    ::bsql_core::driver_sqlite::StepResult::Done => {
                        _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                        drop(_bsql_conn);
                        Ok(None)
                    }
                }
            }
        };

        let fetch_method = quote! {
            /// Fetch all rows, mapped into the target struct.
            pub fn fetch_all(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<Vec<#target_type>> {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_all;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#sql_lit, #sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                #sqlite_column_check
                let mut _bsql_rows = Vec::new();
                loop {
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            _bsql_rows.push(#decode_all);
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => break,
                    }
                }
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                drop(_bsql_conn);
                Ok(_bsql_rows)
            }
        };

        quote! {
            #fetch_one_method
            #fetch_method
            #fetch_optional_method
        }
    } else {
        TokenStream::new()
    };

    let execute_method = {
        let inline_acquire_exec = gen_inline_acquire(/*is_write=*/ true);
        quote! {
            /// Execute the statement (INSERT/UPDATE/DELETE), return affected rows.
            pub fn execute(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<u64> {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_exec;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#sql_lit, #sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)?;
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                let _bsql_changes = _bsql_conn.__changes();
                drop(_bsql_conn);
                Ok(_bsql_changes)
            }
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

/// Wrap a decode expression for a user-provided type path (query_as!).
/// Similar to `wrap_decode_as_bsql` but uses a `syn::Path` instead of an ident.
fn wrap_decode_as_bsql_for_path(target_type: &syn::Path, decode: &TokenStream) -> TokenStream {
    quote! {
        (|| -> Result<#target_type, ::bsql_core::driver_sqlite::SqliteError> {
            Ok(#target_type { #decode })
        })().map_err(::bsql_core::BsqlError::from_sqlite)?
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

/// Convert a column rust_type to its inner-struct storage type.
///
/// Text columns: stored as `(u32, u32)` byte range into the validated text buffer.
/// Blob columns: stored as `(u32, u32)` (offset, len) into the blob arena.
/// Scalars: stored as-is (Copy types).
/// Option<Text>: stored as `Option<(u32, u32)>`.
fn arena_result_type(type_str: &str) -> TokenStream {
    match type_str {
        "String" => quote! { (u32, u32) },
        "Vec<u8>" => quote! { (u32, u32) },
        _ => {
            if let Some(inner) = type_str
                .strip_prefix("Option<")
                .and_then(|s| s.strip_suffix('>'))
            {
                match inner {
                    "String" | "Vec<u8>" => quote! { Option<(u32, u32)> },
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

// --- ForEach row struct (borrowed from StmtHandle) ---

fn for_each_row_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlForEachRow_{}", &parsed.statement_name)
}

/// Convert a column rust_type to its for_each borrowed equivalent.
/// String -> &'a str, Vec<u8> -> &'a [u8], Option<String> -> Option<&'a str>, etc.
/// Scalar types (i64, f64, bool) remain as-is (they are Copy).
fn for_each_result_type(type_str: &str) -> TokenStream {
    match type_str {
        "String" => quote! { &'a str },
        "Vec<u8>" => quote! { &'a [u8] },
        _ => {
            if let Some(inner) = type_str
                .strip_prefix("Option<")
                .and_then(|s| s.strip_suffix('>'))
            {
                match inner {
                    "String" => quote! { Option<&'a str> },
                    "Vec<u8>" => quote! { Option<&'a [u8]> },
                    _ => parse_result_type(type_str),
                }
            } else {
                parse_result_type(type_str)
            }
        }
    }
}

fn gen_for_each_row_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new();
    }

    let struct_name = for_each_row_struct_name(parsed);
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", deduped_names[i]);
        let field_type = for_each_result_type(&col.rust_type);
        quote! { pub #field_name: #field_type }
    });

    // Check if any column actually uses the 'a lifetime (String->& 'a str, Vec<u8>->& 'a [u8]).
    // If not, we need a PhantomData marker to consume the unused lifetime parameter.
    let needs_lifetime = validation.columns.iter().any(|col| {
        let rt = &col.rust_type;
        matches!(rt.as_str(), "String" | "Vec<u8>")
            || rt.starts_with("Option<String>")
            || rt.starts_with("Option<Vec<u8>>")
    });

    let phantom_field = if needs_lifetime {
        TokenStream::new()
    } else {
        quote! { pub _marker: ::std::marker::PhantomData<&'a ()>, }
    };

    quote! {
        #[derive(Debug)]
        #[allow(non_camel_case_types)]
        pub struct #struct_name<'a> {
            #(#fields,)*
            #phantom_field
        }
    }
}

// --- ForEach decode (zero-copy, borrowed from StmtHandle) ---

fn gen_for_each_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let col_idx = i as i32;
        let col = &validation.columns[i];
        let decode_expr = gen_for_each_column_decode(col_idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    // If no column uses the 'a lifetime, we emit a PhantomData marker field
    let needs_lifetime = validation.columns.iter().any(|col| {
        let rt = &col.rust_type;
        matches!(rt.as_str(), "String" | "Vec<u8>")
            || rt.starts_with("Option<String>")
            || rt.starts_with("Option<Vec<u8>>")
    });

    let phantom_init = if needs_lifetime {
        TokenStream::new()
    } else {
        quote! { , _marker: ::std::marker::PhantomData }
    };

    quote! { #(#fields),* #phantom_init }
}

fn gen_for_each_column_decode(idx: i32, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_for_each_nullable_decode(idx, inner)
    } else {
        gen_for_each_not_null_decode(idx, rust_type)
    }
}

fn gen_for_each_not_null_decode(idx: i32, rust_type: &str) -> TokenStream {
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
        // Zero-copy: borrow &str directly from SQLite's buffer (safe validation)
        "String" => {
            let err = gen_direct_decode_error(&col_idx_str, "&str");
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    ::std::str::from_utf8(_bsql_bytes)
                        .map_err(|_| ::bsql_core::driver_sqlite::SqliteError::Internal(
                            format!("invalid UTF-8 in column {}", #col_idx_str),
                        ))?
                }
            }
        }
        // Zero-copy: borrow &[u8] directly from SQLite's buffer
        "Vec<u8>" => {
            let err = gen_direct_decode_error(&col_idx_str, "&[u8]");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    _bsql_stmt.column_blob(#idx)
                }
            }
        }
        // Scalar types that don't need borrowing
        _ => gen_sqlite_direct_not_null_decode(idx, rust_type),
    }
}

fn gen_for_each_nullable_decode(idx: i32, inner_type: &str) -> TokenStream {
    let col_idx_str = idx.to_string();
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
        // Zero-copy: Option<&str> borrowed from SQLite (safe validation)
        "String" => quote! {
            match _bsql_stmt.column_text(#idx) {
                None => None,
                Some(b) => Some(::std::str::from_utf8(b)
                    .map_err(|_| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid UTF-8 in column {}", #col_idx_str),
                    ))?),
            }
        },
        // Zero-copy: Option<&[u8]> borrowed from SQLite
        "Vec<u8>" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                Some(_bsql_stmt.column_blob(#idx))
            }
        },
        _ => gen_sqlite_direct_nullable_decode(idx, inner_type),
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
        #[must_use = "query is not executed until .fetch_all(), .execute(), or another execution method is called"]
        #[allow(non_camel_case_types)]
        struct #struct_name<'_bsql> {
            #(#fields,)*
            _marker: ::std::marker::PhantomData<&'_bsql ()>,
        }
    }
}

// --- Executor impl ---

/// Generate a runtime column-count bounds check for SQLite decode paths.
///
/// Checks `_bsql_stmt.column_count()` against the expected number of columns.
/// Returns `Err(BsqlError::Decode(...))` on mismatch instead of panicking.
fn gen_sqlite_column_count_check(validation: &ValidationResult) -> TokenStream {
    let expected = validation.columns.len() as i32;
    if expected == 0 {
        return TokenStream::new();
    }
    quote! {
        if _bsql_stmt.column_count() < #expected {
            return Err(::bsql_core::error::DecodeError::column_count(
                #expected as usize,
                _bsql_stmt.column_count() as usize,
            ));
        }
    }
}

/// Wrap a decode expression that may `return Err(SqliteError)` so it converts
/// to `BsqlError`. The decode code was written for closures that return
/// `Result<T, SqliteError>`, but inline code returns `BsqlResult<T>`.
/// This wraps the construction in a closure to bridge the error type.
fn wrap_decode_as_bsql(struct_name: &proc_macro2::Ident, decode: &TokenStream) -> TokenStream {
    quote! {
        (|| -> Result<#struct_name, ::bsql_core::driver_sqlite::SqliteError> {
            Ok(#struct_name { #decode })
        })().map_err(::bsql_core::BsqlError::from_sqlite)?
    }
}

/// Wrap a validated-rows decode expression (arena path with text_buf + blob_arena).
/// Currently unused — fetch_all always uses the direct (owned) path. Kept for
/// potential future use by arena-based batch APIs.
#[allow(dead_code)]
fn wrap_validated_decode(struct_name: &proc_macro2::Ident, decode: &TokenStream) -> TokenStream {
    quote! {
        (|| -> Result<#struct_name, ::bsql_core::driver_sqlite::SqliteError> {
            Ok(#struct_name { #decode })
        })().map_err(::bsql_core::BsqlError::from_sqlite)?
    }
}

/// Same as `wrap_decode_as_bsql` but for for_each row structs with lifetime.
fn wrap_for_each_decode_as_bsql(
    struct_name: &proc_macro2::Ident,
    decode: &TokenStream,
) -> TokenStream {
    quote! {
        (|| -> Result<#struct_name<'_>, ::bsql_core::driver_sqlite::SqliteError> {
            Ok(#struct_name { #decode })
        })().map_err(::bsql_core::BsqlError::from_sqlite)?
    }
}

/// Generate the inline parameter binding code for a given set of params.
/// Binds each param to the stmt using the dyn SqliteEncode slice.
fn gen_inline_param_bind() -> TokenStream {
    quote! {
        _bsql_stmt.clear_bindings();
        for (_bsql_i, _bsql_p) in _bsql_params.iter().enumerate() {
            _bsql_p.bind(_bsql_stmt, (_bsql_i + 1) as i32)
                .map_err(::bsql_core::BsqlError::from_sqlite)?;
        }
    }
}

/// Generate the inline acquire expression -- reader or writer.
fn gen_inline_acquire(is_write: bool) -> TokenStream {
    if is_write {
        quote! { pool.__inner().__acquire_writer().map_err(::bsql_core::BsqlError::from_sqlite)? }
    } else {
        quote! { pool.__inner().__acquire_reader().map_err(::bsql_core::BsqlError::from_sqlite)? }
    }
}

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

    // Column-count bounds check for SQLite — inserted before every decode (Fix-5)
    let sqlite_column_check = gen_sqlite_column_count_check(validation);

    let inline_bind = gen_inline_param_bind();

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);

        // --- Inline fetch_one ---
        let fetch_one_method = {
            let inline_acquire_one = gen_inline_acquire(is_write);
            let decode_one_inner = wrap_decode_as_bsql(&result_name, &direct_decode);
            quote! {
                /// Fetch exactly one row. Inline step loop — no cross-crate call overhead.
                pub fn fetch_one(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    #direct_params_build
                    let mut _bsql_conn = #inline_acquire_one;
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(#limited_sql_lit, #limited_sql_hash_val)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #inline_bind
                    #sqlite_column_check
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_result = #decode_one_inner;
                            // Check for extra rows (LIMIT 2 pattern)
                            if let ::bsql_core::driver_sqlite::StepResult::Row =
                                _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)?
                            {
                                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                                drop(_bsql_conn);
                                return Err(::bsql_core::BsqlError::from_sqlite(
                                    ::bsql_core::driver_sqlite::SqliteError::Internal(
                                        "expected 1 row, got 2+".into(),
                                    ),
                                ));
                            }
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            Ok(_bsql_result)
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => {
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            Err(::bsql_core::BsqlError::from_sqlite(
                                ::bsql_core::driver_sqlite::SqliteError::Internal(
                                    "expected 1 row, got 0".into(),
                                ),
                            ))
                        }
                    }
                }
            }
        };

        // --- Inline fetch_optional ---
        let fetch_optional_method = {
            let inline_acquire_opt = gen_inline_acquire(is_write);
            let decode_opt_inner = wrap_decode_as_bsql(&result_name, &direct_decode);
            quote! {
                /// Fetch zero or one row. Inline step loop — no cross-crate call overhead.
                pub fn fetch_optional(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    #direct_params_build
                    let mut _bsql_conn = #inline_acquire_opt;
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(#limited_sql_lit, #limited_sql_hash_val)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #inline_bind
                    #sqlite_column_check
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_result = #decode_opt_inner;
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            Ok(Some(_bsql_result))
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => {
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            Ok(None)
                        }
                    }
                }
            }
        };

        // --- Inline fetch ---
        // Always use the direct (owned) decode path for fetch_all so that TEXT
        // columns return owned `String` instead of arena `(u32, u32)` offsets.
        // The arena path is reserved for for_each / for_each_map (zero-alloc hot path).
        let fetch_method = {
            let inline_acquire_all = gen_inline_acquire(is_write);
            let decode_all = wrap_decode_as_bsql(&result_name, &direct_decode);
            quote! {
                /// Fetch all rows. Inline step loop — no cross-crate call overhead.
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #direct_params_build
                    let mut _bsql_conn = #inline_acquire_all;
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(#sql_lit, #sql_hash_val)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #inline_bind
                    #sqlite_column_check
                    let mut _bsql_rows = Vec::new();
                    loop {
                        match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                            ::bsql_core::driver_sqlite::StepResult::Row => {
                                _bsql_rows.push(#decode_all);
                            }
                            ::bsql_core::driver_sqlite::StepResult::Done => break,
                        }
                    }
                    _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                    drop(_bsql_conn);
                    Ok(_bsql_rows)
                }
            }
        };

        quote! {
            #fetch_one_method
            #fetch_method
            #fetch_optional_method

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

    let for_each_methods = if has_columns {
        let for_each_row_name = for_each_row_struct_name(parsed);
        let for_each_decode = gen_for_each_decode(validation);

        let inline_acquire_fe = gen_inline_acquire(is_write);
        let inline_acquire_fem = gen_inline_acquire(is_write);
        let decode_fe = wrap_for_each_decode_as_bsql(&for_each_row_name, &for_each_decode);
        let decode_fem = wrap_for_each_decode_as_bsql(&for_each_row_name, &for_each_decode);

        quote! {
            /// Process each row in-place via a closure. Inline step loop —
            /// no cross-crate call overhead. Zero-copy text/blob.
            pub fn for_each<_BsqlForEachF>(
                self,
                pool: &::bsql_core::SqlitePool,
                mut f: _BsqlForEachF,
            ) -> ::bsql_core::BsqlResult<()>
            where
                _BsqlForEachF: FnMut(#for_each_row_name<'_>) -> Result<(), ::bsql_core::BsqlError>,
            {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_fe;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#sql_lit, #sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                #sqlite_column_check
                loop {
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_row = #decode_fe;
                            f(_bsql_row)?;
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => break,
                    }
                }
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                drop(_bsql_conn);
                Ok(())
            }

            /// Process each row in-place, collecting mapped results into a `Vec`.
            /// Inline step loop — no cross-crate call overhead.
            pub fn for_each_map<_BsqlForEachF, _BsqlForEachT>(
                self,
                pool: &::bsql_core::SqlitePool,
                mut f: _BsqlForEachF,
            ) -> ::bsql_core::BsqlResult<Vec<_BsqlForEachT>>
            where
                _BsqlForEachF: FnMut(#for_each_row_name<'_>) -> _BsqlForEachT,
            {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_fem;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#sql_lit, #sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                #sqlite_column_check
                let mut _bsql_rows = Vec::new();
                loop {
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_row = #decode_fem;
                            _bsql_rows.push(f(_bsql_row));
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => break,
                    }
                }
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                drop(_bsql_conn);
                Ok(_bsql_rows)
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_method = {
        let inline_acquire_exec = gen_inline_acquire(/*is_write=*/ true);
        quote! {
            /// Execute the statement (INSERT/UPDATE/DELETE), return affected rows.
            /// Inline — no cross-crate call overhead.
            pub fn execute(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<u64> {
                #direct_params_build
                let mut _bsql_conn = #inline_acquire_exec;
                let _bsql_stmt = _bsql_conn.__get_or_prepare(#sql_lit, #sql_hash_val)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #inline_bind
                _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)?;
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                let _bsql_changes = _bsql_conn.__changes();
                drop(_bsql_conn);
                Ok(_bsql_changes)
            }
        }
    };

    quote! {
        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #for_each_methods
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
                    ::std::str::from_utf8(_bsql_bytes)
                        .map_err(|_| ::bsql_core::driver_sqlite::SqliteError::Internal(
                            format!("invalid UTF-8 in column {}", #col_idx_str),
                        ))?
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
            match _bsql_stmt.column_text(#idx) {
                None => None,
                Some(b) => Some(::std::str::from_utf8(b)
                    .map_err(|_| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid UTF-8 in column {}", #idx),
                    ))?
                    .to_owned()),
            }
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

/// Helper: generate safe column_text -> &str decode (no from_utf8_unchecked).
fn gen_safe_column_text_to_str(idx: i32) -> TokenStream {
    let col_idx_str = idx.to_string();
    let err = gen_direct_decode_error(&col_idx_str, "&str");
    quote! {
        let _bsql_bytes = _bsql_stmt.column_text(#idx)
            .ok_or_else(|| #err)?;
        let s = ::std::str::from_utf8(_bsql_bytes)
            .map_err(|_| ::bsql_core::driver_sqlite::SqliteError::Internal(
                format!("invalid UTF-8 in column {}", #col_idx_str),
            ))?;
    }
}

fn gen_sqlite_direct_feature_gated_decode(idx: i32, rust_type: &str) -> TokenStream {
    let col_idx_str = idx.to_string();
    let text_to_str = gen_safe_column_text_to_str(idx);

    match rust_type {
        "::uuid::Uuid" | "uuid::Uuid" => {
            quote! {
                {
                    #text_to_str
                    s.parse::<::uuid::Uuid>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid UUID in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::time::PrimitiveDateTime" | "time::PrimitiveDateTime" => {
            quote! {
                {
                    #text_to_str
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
                    #text_to_str
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
                    #text_to_str
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
                    #text_to_str
                    s.parse::<::chrono::NaiveDateTime>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid datetime in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::chrono::NaiveDate" | "chrono::NaiveDate" => {
            quote! {
                {
                    #text_to_str
                    s.parse::<::chrono::NaiveDate>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid date in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::chrono::NaiveTime" | "chrono::NaiveTime" => {
            quote! {
                {
                    #text_to_str
                    s.parse::<::chrono::NaiveTime>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid time in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        "::rust_decimal::Decimal" | "rust_decimal::Decimal" => {
            quote! {
                {
                    #text_to_str
                    s.parse::<::rust_decimal::Decimal>().map_err(|e| ::bsql_core::driver_sqlite::SqliteError::Internal(
                        format!("invalid decimal in column {}: {}", #col_idx_str, e),
                    ))?
                }
            }
        }
        _ => {
            // Fallback: read as text (safe)
            quote! {
                {
                    #text_to_str
                    s.to_owned()
                }
            }
        }
    }
}

// --- Arena-backed decode for fetch_all (batch-validated, zero unsafe) ---
//
// Text columns: appended to `_bsql_text_buf` (Vec<u8>), stored as (start, end) u32 range.
// Blob columns: copied into `_bsql_blob_arena`, stored as (offset, len) u32 range.
// Scalar columns: decoded directly, no buffer needed.
//
// After the step loop, `String::from_utf8(_bsql_text_buf)` validates ALL text
// in one SIMD-accelerated pass. No from_utf8_unchecked. No transmute. No unsafe.

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
fn gen_sqlite_arena_not_null_decode(idx: i32, rust_type: &str) -> TokenStream {
    let col_idx_str = idx.to_string();
    match rust_type {
        // Text: append to text_buf, store (start, end) range
        "String" => {
            let err = gen_direct_decode_error(&col_idx_str, "&str");
            quote! {
                {
                    let _bsql_bytes = _bsql_stmt.column_text(#idx)
                        .ok_or_else(|| #err)?;
                    let _bsql_start = _bsql_text_buf.len() as u32;
                    _bsql_text_buf.extend_from_slice(_bsql_bytes);
                    let _bsql_end = _bsql_text_buf.len() as u32;
                    (_bsql_start, _bsql_end)
                }
            }
        }
        // Blob: copy into blob arena, store (offset, len) range
        "Vec<u8>" => {
            let err = gen_direct_decode_error(&col_idx_str, "&[u8]");
            quote! {
                {
                    if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                        return Err(#err);
                    }
                    let _bsql_raw = _bsql_stmt.column_blob(#idx);
                    let _bsql_off = _bsql_blob_arena.alloc_copy(_bsql_raw) as u32;
                    (_bsql_off, _bsql_raw.len() as u32)
                }
            }
        }
        // All non-text types: identical to direct decode (no buffer needed)
        _ => gen_sqlite_direct_not_null_decode(idx, rust_type),
    }
}

#[allow(dead_code)]
fn gen_sqlite_arena_nullable_decode(idx: i32, inner_type: &str) -> TokenStream {
    match inner_type {
        // Option<String> -> Option<(u32, u32)> text range
        "String" => quote! {
            match _bsql_stmt.column_text(#idx) {
                None => None,
                Some(_bsql_bytes) => {
                    let _bsql_start = _bsql_text_buf.len() as u32;
                    _bsql_text_buf.extend_from_slice(_bsql_bytes);
                    let _bsql_end = _bsql_text_buf.len() as u32;
                    Some((_bsql_start, _bsql_end))
                }
            }
        },
        // Option<Vec<u8>> -> Option<(u32, u32)> blob range
        "Vec<u8>" => quote! {
            if _bsql_stmt.column_type(#idx) == ::bsql_core::driver_sqlite::SQLITE_NULL {
                None
            } else {
                let _bsql_raw = _bsql_stmt.column_blob(#idx);
                let _bsql_off = _bsql_blob_arena.alloc_copy(_bsql_raw) as u32;
                Some((_bsql_off, _bsql_raw.len() as u32))
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
    let coercions = gen_ref_coercions(&parsed.params);
    let field_inits = parsed.params.iter().map(|p| {
        let name = param_ident(&p.name);
        quote! { #name }
    });
    quote! {
        #coercions
        #executor_name { #(#field_inits,)* _marker: ::std::marker::PhantomData }
    }
}

/// Generate `let name: &T = &name;` coercions for every reference-typed param.
fn gen_ref_coercions(params: &[crate::parse::Param]) -> TokenStream {
    let stmts: Vec<TokenStream> = params
        .iter()
        .filter(|p| p.rust_type.starts_with('&'))
        .map(|p| {
            let name = param_ident(&p.name);
            let ty: syn::Type = syn::parse_str(&p.rust_type)
                .unwrap_or_else(|_| panic!("cannot parse type `{}`", p.rust_type));
            quote! { let #name: #ty = &#name; }
        })
        .collect();
    quote! { #(#stmts)* }
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
/// - A runtime dispatcher that builds the SQL string by conditionally
///   appending optional clause fragments (O(N) code, not 2^N match arms)
pub fn generate_dynamic_sqlite_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> TokenStream {
    let result_struct = gen_result_struct(parsed, validation);
    let arena_result_struct = gen_arena_result_struct(parsed, validation);
    let for_each_row_struct = gen_for_each_row_struct(parsed, validation);
    let executor_struct = gen_dynamic_executor_struct(parsed);
    let executor_impls = gen_dynamic_executor_impls(parsed, validation);
    let constructor = gen_dynamic_constructor(parsed);

    quote! {
        {
            #result_struct
            #arena_result_struct
            #for_each_row_struct
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
        #[must_use = "query is not executed until .fetch_all(), .execute(), or another execution method is called"]
        #[allow(non_camel_case_types)]
        struct #struct_name<'_bsql> {
            #(#fields,)*
            _marker: ::std::marker::PhantomData<&'_bsql ()>,
        }
    }
}

/// Generate the impl block for a dynamic SQLite query executor.
/// Uses runtime SQL dispatch — O(N) code instead of 2^N match arms.
fn gen_dynamic_executor_impls(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let has_columns = !validation.columns.is_empty();
    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let is_write = !is_select;

    let direct_decode = if has_columns {
        gen_sqlite_direct_decode(validation)
    } else {
        TokenStream::new()
    };

    // Column-count bounds check for SQLite dynamic queries (Fix-5)
    let sqlite_column_check = gen_sqlite_column_count_check(validation);

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let needs_limit = has_columns && is_select && !parsed.normalized_sql.contains(" limit ");

        // --- Inline fetch_one ---
        let fetch_one_dispatcher = {
            let bind = gen_inline_param_bind();
            let decode_one = wrap_decode_as_bsql(&result_name, &direct_decode);
            gen_sqlite_runtime_dispatcher(parsed, needs_limit, is_write, |_| {
                quote! {
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(&_bsql_sql, _bsql_hash)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #bind
                    #sqlite_column_check
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_result = #decode_one;
                            if let ::bsql_core::driver_sqlite::StepResult::Row =
                                _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)?
                            {
                                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                                drop(_bsql_conn);
                                return Err(::bsql_core::BsqlError::from_sqlite(
                                    ::bsql_core::driver_sqlite::SqliteError::Internal(
                                        "expected 1 row, got 2+".into(),
                                    ),
                                ));
                            }
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            return Ok(_bsql_result);
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => {
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            return Err(::bsql_core::BsqlError::from_sqlite(
                                ::bsql_core::driver_sqlite::SqliteError::Internal(
                                    "expected 1 row, got 0".into(),
                                ),
                            ));
                        }
                    }
                }
            })
        };

        // --- Inline fetch_optional ---
        let fetch_optional_dispatcher = {
            let bind = gen_inline_param_bind();
            let decode_opt = wrap_decode_as_bsql(&result_name, &direct_decode);
            gen_sqlite_runtime_dispatcher(parsed, needs_limit, is_write, |_| {
                quote! {
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(&_bsql_sql, _bsql_hash)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #bind
                    #sqlite_column_check
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_result = #decode_opt;
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            return Ok(Some(_bsql_result));
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => {
                            _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                            drop(_bsql_conn);
                            return Ok(None);
                        }
                    }
                }
            })
        };

        // --- Inline fetch ---
        // Always use the direct (owned) decode path for fetch_all so that TEXT
        // columns return owned `String` instead of arena `(u32, u32)` offsets.
        // The arena path is reserved for for_each / for_each_map (zero-alloc hot path).
        let fetch_method = {
            let bind = gen_inline_param_bind();
            let decode_all = wrap_decode_as_bsql(&result_name, &direct_decode);
            let fetch_dispatcher = gen_sqlite_runtime_dispatcher(parsed, false, is_write, |_| {
                quote! {
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(&_bsql_sql, _bsql_hash)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #bind
                    #sqlite_column_check
                    let mut _bsql_rows = Vec::new();
                    loop {
                        match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                            ::bsql_core::driver_sqlite::StepResult::Row => {
                                _bsql_rows.push(#decode_all);
                            }
                            ::bsql_core::driver_sqlite::StepResult::Done => break,
                        }
                    }
                    _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                    drop(_bsql_conn);
                    return Ok(_bsql_rows);
                }
            });
            quote! {
                pub fn fetch_all(
                    self,
                    pool: &::bsql_core::SqlitePool,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #fetch_dispatcher
                }
            }
        };

        quote! {
            pub fn fetch_one(
                self,
                pool: &::bsql_core::SqlitePool,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                #fetch_one_dispatcher
            }

            #fetch_method

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

    let for_each_methods = if has_columns {
        let for_each_row_name = for_each_row_struct_name(parsed);
        let for_each_decode = gen_for_each_decode(validation);

        let decode_fe = wrap_for_each_decode_as_bsql(&for_each_row_name, &for_each_decode);
        let for_each_dispatcher = gen_sqlite_runtime_dispatcher(parsed, false, is_write, |_| {
            let bind = gen_inline_param_bind();
            quote! {
                let _bsql_stmt = _bsql_conn.__get_or_prepare(&_bsql_sql, _bsql_hash)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #bind
                #sqlite_column_check
                loop {
                    match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                        ::bsql_core::driver_sqlite::StepResult::Row => {
                            let _bsql_row = #decode_fe;
                            f(_bsql_row)?;
                        }
                        ::bsql_core::driver_sqlite::StepResult::Done => break,
                    }
                }
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                drop(_bsql_conn);
                return Ok(());
            }
        });

        let decode_fem = wrap_for_each_decode_as_bsql(&for_each_row_name, &for_each_decode);
        let for_each_map_dispatcher =
            gen_sqlite_runtime_dispatcher(parsed, false, is_write, |_| {
                let bind = gen_inline_param_bind();
                quote! {
                    let _bsql_stmt = _bsql_conn.__get_or_prepare(&_bsql_sql, _bsql_hash)
                        .map_err(::bsql_core::BsqlError::from_sqlite)?;
                    #bind
                    #sqlite_column_check
                    let mut _bsql_rows = Vec::new();
                    loop {
                        match _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)? {
                            ::bsql_core::driver_sqlite::StepResult::Row => {
                                let _bsql_row = #decode_fem;
                                _bsql_rows.push(f(_bsql_row));
                            }
                            ::bsql_core::driver_sqlite::StepResult::Done => break,
                        }
                    }
                    _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                    drop(_bsql_conn);
                    return Ok(_bsql_rows);
                }
            });

        quote! {
            pub fn for_each<_BsqlForEachF>(
                self,
                pool: &::bsql_core::SqlitePool,
                mut f: _BsqlForEachF,
            ) -> ::bsql_core::BsqlResult<()>
            where
                _BsqlForEachF: FnMut(#for_each_row_name<'_>) -> Result<(), ::bsql_core::BsqlError>,
            {
                #for_each_dispatcher
            }

            pub fn for_each_map<_BsqlForEachF, _BsqlForEachT>(
                self,
                pool: &::bsql_core::SqlitePool,
                mut f: _BsqlForEachF,
            ) -> ::bsql_core::BsqlResult<Vec<_BsqlForEachT>>
            where
                _BsqlForEachF: FnMut(#for_each_row_name<'_>) -> _BsqlForEachT,
            {
                #for_each_map_dispatcher
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_dispatcher =
        gen_sqlite_runtime_dispatcher(parsed, false, /*is_write=*/ true, |_| {
            let bind = gen_inline_param_bind();
            quote! {
                let _bsql_stmt = _bsql_conn.__get_or_prepare(&_bsql_sql, _bsql_hash)
                    .map_err(::bsql_core::BsqlError::from_sqlite)?;
                #bind
                _bsql_stmt.step().map_err(::bsql_core::BsqlError::from_sqlite)?;
                _bsql_stmt.reset().map_err(::bsql_core::BsqlError::from_sqlite)?;
                let _bsql_changes = _bsql_conn.__changes();
                drop(_bsql_conn);
                return Ok(_bsql_changes);
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
            #for_each_methods
            #execute_method
        }
    }
}

/// Generate a runtime SQL dispatcher for SQLite dynamic queries.
///
/// Same concept as `gen_runtime_dispatcher` in codegen.rs but for SQLite:
/// uses `?N` parameter syntax instead of `$N`, and builds a
/// `Vec<&dyn SqliteEncode>` params slice.
fn gen_sqlite_runtime_dispatcher<F>(
    parsed: &ParsedQuery,
    inject_limit: bool,
    is_write: bool,
    body_fn: F,
) -> TokenStream
where
    F: Fn(()) -> TokenStream,
{
    // Convert the base positional SQL from PG-style $N to SQLite-style ?N
    let pg_sql = &parsed.positional_sql;

    // Pre-split at {OPT_N} markers
    let mut segments: Vec<String> = Vec::new();
    let mut remaining = pg_sql.as_str();
    for i in 0..parsed.optional_clauses.len() {
        let marker = format!("{{OPT_{i}}}");
        if let Some(pos) = remaining.find(&marker) {
            segments.push(pg_to_sqlite_params(&remaining[..pos]));
            remaining = &remaining[pos + marker.len()..];
        } else {
            segments.push(pg_to_sqlite_params(remaining));
            remaining = "";
        }
    }
    segments.push(pg_to_sqlite_params(remaining));

    let base_len: usize = segments.iter().map(|s| s.len()).sum();
    let clause_len: usize = parsed
        .optional_clauses
        .iter()
        .map(|c| c.sql_fragment.len() + 8)
        .sum();
    let capacity = base_len + clause_len + if inject_limit { 10 } else { 0 };

    let first_segment = &segments[0];

    // Base param pushes
    let base_param_pushes: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            let push = gen_direct_param_ref(&name, &p.rust_type);
            quote! {
                _bsql_params.push(#push);
            }
        })
        .collect();

    // Clause appends (same logic as PG but with ?N instead of $N)
    let clause_appends: Vec<TokenStream> = parsed
        .optional_clauses
        .iter()
        .enumerate()
        .map(|(i, clause)| {
            let param_name = param_ident(&clause.params[0].name);
            let trailing = &segments[i + 1];

            // Split fragment at ${P_N} markers
            let frag = &clause.sql_fragment;
            let mut frag_parts: Vec<String> = Vec::new();
            let mut frag_remaining = frag.as_str();
            loop {
                if let Some(pos) = frag_remaining.find("${P_") {
                    frag_parts.push(frag_remaining[..pos].to_owned());
                    let after = &frag_remaining[pos + 4..];
                    if let Some(end) = after.find('}') {
                        frag_remaining = &after[end + 1..];
                    } else {
                        frag_remaining = "";
                        break;
                    }
                } else {
                    frag_parts.push(frag_remaining.to_owned());
                    break;
                }
            }

            let num_refs = frag_parts.len() - 1;

            // Generate fragment append code using ?N (SQLite style)
            let frag_append: Vec<TokenStream> = frag_parts
                .iter()
                .enumerate()
                .map(|(j, part)| {
                    if j == 0 {
                        quote! {
                            _bsql_sql.push(' ');
                            _bsql_sql.push_str(#part);
                        }
                    } else {
                        quote! {
                            _bsql_sql.push('?');
                            {
                                let _n = _bsql_params.len() + 1;
                                if _n < 10 {
                                    _bsql_sql.push((b'0' + _n as u8) as char);
                                } else if _n < 100 {
                                    _bsql_sql.push((b'0' + (_n / 10) as u8) as char);
                                    _bsql_sql.push((b'0' + (_n % 10) as u8) as char);
                                } else {
                                    let _s = _n.to_string();
                                    _bsql_sql.push_str(&_s);
                                }
                            }
                            _bsql_sql.push_str(#part);
                        }
                    }
                })
                .collect();

            // Push unwrapped param value
            let inner_type = clause.params[0]
                .rust_type
                .strip_prefix("Option<")
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or(&clause.params[0].rust_type);
            let inner_ref = direct_param_ref_inner(quote! { _bsql_val }, inner_type);

            let param_push = quote! {
                let _bsql_val = self.#param_name.as_ref().unwrap();
                _bsql_params.push(#inner_ref);
            };

            let trailing_push = if trailing.is_empty() {
                TokenStream::new()
            } else {
                quote! { _bsql_sql.push_str(#trailing); }
            };

            if num_refs <= 1 {
                quote! {
                    if self.#param_name.is_some() {
                        #(#frag_append)*
                        #param_push
                    }
                    #trailing_push
                }
            } else {
                let frag_parts_multi: Vec<TokenStream> = frag_parts
                    .iter()
                    .enumerate()
                    .map(|(j, part)| {
                        if j == 0 {
                            quote! {
                                _bsql_sql.push(' ');
                                _bsql_sql.push_str(#part);
                            }
                        } else {
                            quote! {
                                _bsql_sql.push_str(&_bsql_pn);
                                _bsql_sql.push_str(#part);
                            }
                        }
                    })
                    .collect();

                quote! {
                    if self.#param_name.is_some() {
                        #param_push
                        let _bsql_pn = format!("?{}", _bsql_params.len());
                        #(#frag_parts_multi)*
                    }
                    #trailing_push
                }
            }
        })
        .collect();

    let limit_push = if inject_limit {
        quote! { _bsql_sql.push_str(" LIMIT 2"); }
    } else {
        TokenStream::new()
    };

    let body = body_fn(());
    let n_clauses = parsed.optional_clauses.len();
    let max_params = parsed.params.len() + n_clauses;
    let acquire = gen_inline_acquire(is_write);

    quote! {
        {
            let mut _bsql_sql = ::std::string::String::with_capacity(#capacity);
            _bsql_sql.push_str(#first_segment);
            let mut _bsql_params: ::std::vec::Vec<&dyn ::bsql_core::driver_sqlite::SqliteEncode> =
                ::std::vec::Vec::with_capacity(#max_params);

            #(#base_param_pushes)*
            #(#clause_appends)*
            #limit_push

            let _bsql_hash = ::bsql_core::rapid_hash_str(&_bsql_sql);

            let mut _bsql_conn = #acquire;
            #body
        }
    }
}

/// Generate the constructor for a dynamic SQLite query executor.
fn gen_dynamic_constructor(parsed: &ParsedQuery) -> TokenStream {
    let executor_name = executor_struct_name(parsed);

    let mut all_params: Vec<crate::parse::Param> = Vec::new();
    let mut field_names: Vec<proc_macro2::Ident> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for p in &parsed.params {
        field_names.push(param_ident(&p.name));
        if seen.insert(p.name.clone()) {
            all_params.push(p.clone());
        }
    }

    for clause in &parsed.optional_clauses {
        for p in &clause.params {
            if seen.insert(p.name.clone()) {
                field_names.push(param_ident(&p.name));
                all_params.push(p.clone());
            }
        }
    }

    let coercions = gen_ref_coercions(&all_params);

    quote! {
        #coercions
        #executor_name { #(#field_names,)* _marker: ::std::marker::PhantomData }
    }
}

// ===========================================================================
// Sort query codegen
// ===========================================================================

/// Generate Rust code for a SQLite query with `$[sort: EnumType]`.
///
/// The sort fragment is spliced into SQL at runtime. Each variant gets its
/// own sql_hash. Uses LazyLock cache same as the PG sort codegen.
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
        #[must_use = "query is not executed until .fetch_all(), .execute(), or another execution method is called"]
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
        static SORT_SQL_CACHE: ::std::sync::LazyLock<::std::sync::Mutex<Vec<(usize, String, u64)>>> = ::std::sync::LazyLock::new(|| ::std::sync::Mutex::new(Vec::new()));
        let sort_fragment: &'static str = self.sort.sql();
        let key = sort_fragment.as_ptr() as usize;
        let (sql, sql_hash) = {
            let guard = SORT_SQL_CACHE.lock().unwrap();
            if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                (entry.1.clone(), entry.2)
            } else {
                drop(guard);
                let built = format!("{}{}{}", #sql_prefix, sort_fragment, #sql_suffix);
                let hash = ::bsql_core::rapid_hash_str(&built);
                let mut guard = SORT_SQL_CACHE.lock().unwrap();
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
            static SORT_LIMITED_SQL_CACHE: ::std::sync::LazyLock<::std::sync::Mutex<Vec<(usize, String, u64)>>> = ::std::sync::LazyLock::new(|| ::std::sync::Mutex::new(Vec::new()));
            let sort_fragment: &'static str = self.sort.sql();
            let key = sort_fragment.as_ptr() as usize;
            let (sql, sql_hash) = {
                let guard = SORT_LIMITED_SQL_CACHE.lock().unwrap();
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.clone(), entry.2)
                } else {
                    drop(guard);
                    let built = format!("{}{}{}", #sql_prefix, sort_fragment, #limited_suffix_lit);
                    let hash = ::bsql_core::rapid_hash_str(&built);
                    let mut guard = SORT_LIMITED_SQL_CACHE.lock().unwrap();
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

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);

        // Sort queries always use the direct decode path (safe from_utf8,
        // owned strings) since they go through pool indirection anyway.
        let fetch_method = quote! {
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

                #fetch_method

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
    let coercions = gen_ref_coercions(&parsed.params);
    let field_inits: Vec<proc_macro2::Ident> =
        parsed.params.iter().map(|p| param_ident(&p.name)).collect();

    let constructor = quote! {
        #coercions
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
