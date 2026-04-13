//! Code generation for `bsql::query!`.
//!
//! Given a parsed and validated query, generates a Rust expression that:
//! 1. Defines a result struct with typed fields
//! 2. Defines an executor struct that captures parameters
//! 3. Implements `fetch`, `fetch_one`, `fetch_optional`, `execute` methods
//! 4. Evaluates to the executor struct (enables the chaining syntax)

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

// ---------------------------------------------------------------------------
// Compile-time wire protocol generation
// ---------------------------------------------------------------------------

/// Build the PostgreSQL Parse + Describe message bytes at compile time.
///
/// Produces the exact wire bytes that the driver would build at runtime
/// on a statement cache miss. When these bytes are available, the driver
/// can `write_all(bytes)` directly instead of constructing Parse/Describe
/// from scratch — saving ~200ns on first execution of each query.
///
/// Wire format:
/// ```text
/// 'P' [i32 BE length] [stmt_name\0] [sql\0] [i16 BE param_count] [i32 BE oid]...
/// 'D' [i32 BE length] 'S' [stmt_name\0]
/// ```
fn gen_parse_describe_bytes(sql: &str, sql_hash: u64, param_oids: &[u32]) -> Vec<u8> {
    let stmt_name = make_stmt_name_bytes(sql_hash);
    let mut buf = Vec::new();

    // --- Parse message ---
    buf.push(b'P');
    let parse_payload = stmt_name.len()
        + 1  // NUL after name
        + sql.len()
        + 1  // NUL after SQL
        + 2  // i16 param count
        + param_oids.len() * 4; // i32 per OID
    let parse_len = (parse_payload as i32) + 4;
    buf.extend_from_slice(&parse_len.to_be_bytes());
    buf.extend_from_slice(&stmt_name);
    buf.push(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
    for &oid in param_oids {
        buf.extend_from_slice(&(oid as i32).to_be_bytes());
    }

    // --- Describe message (Statement) ---
    buf.push(b'D');
    let describe_payload = 1 + stmt_name.len() + 1; // 'S' + name + NUL
    let describe_len = (describe_payload as i32) + 4;
    buf.extend_from_slice(&describe_len.to_be_bytes());
    buf.push(b'S');
    buf.extend_from_slice(&stmt_name);
    buf.push(0);

    buf
}

/// Generate the statement name bytes from a sql_hash.
/// Format: `s_{hash:016x}` — exactly 18 bytes.
/// Must match `bsql-driver-postgres::stmt_cache::make_stmt_name`.
fn make_stmt_name_bytes(hash: u64) -> [u8; 18] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 18];
    buf[0] = b's';
    buf[1] = b'_';
    let bytes = hash.to_be_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[2 + i * 2] = HEX[(b >> 4) as usize];
        buf[2 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
    buf
}

use crate::parse::ParsedQuery;
use crate::validate::ValidationResult;

/// Return the effective SQL for code generation.
///
/// If the two-phase PREPARE mechanism rewrote the SQL (e.g. added `::jsonb`
/// casts), use the rewritten version. Otherwise use `parsed.positional_sql`.
fn effective_sql<'a>(parsed: &'a ParsedQuery, validation: &'a ValidationResult) -> &'a str {
    validation
        .rewritten_sql
        .as_deref()
        .unwrap_or(&parsed.positional_sql)
}

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
/// - A runtime dispatcher that builds the SQL string by conditionally
///   appending optional clause fragments (O(N) code, not 2^N match arms)
pub fn generate_dynamic_query_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> TokenStream {
    let result_struct = gen_result_struct(parsed, validation);
    let for_each_row_struct = gen_pg_for_each_row_struct(parsed, validation);
    let executor_struct = gen_dynamic_executor_struct(parsed);
    let executor_impls = gen_dynamic_executor_impls(parsed, validation);
    let constructor = gen_dynamic_constructor(parsed);

    quote! {
        {
            #result_struct
            #for_each_row_struct
            #executor_struct
            #executor_impls
            #constructor
        }
    }
}

/// Generate Rust code for a query with a `$[sort: EnumType]` placeholder.
///
/// The generated code:
/// - Defines a result struct (same for all sort variants)
/// - Defines an executor struct capturing parameters + sort enum
/// - At runtime, calls `sort.sql()` to get the fragment, constructs the final
///   SQL by replacing `{SORT}`, and dispatches via the sort enum's `sql()` method
///
/// Since sort fragments are spliced into SQL at runtime (each variant is a
/// different SQL string), each variant gets its own sql_hash. The generated
/// code builds the SQL string at runtime using `str::replace`.
pub fn generate_sort_query_code(
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

    // Build params slice
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

    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let readonly_val = is_select;

    let eff_sql_sort = effective_sql(parsed, validation);
    let sql_template = eff_sql_sort;
    let has_columns = !validation.columns.is_empty();

    // Split the SQL template at {SORT} to enable zero-allocation concatenation.
    // The sort fragment is spliced between prefix and suffix, and the result is
    // cached in a static map keyed by the &'static str fragment pointer.
    let sort_parts: Vec<&str> = sql_template.split("{SORT}").collect();
    let sql_prefix = sort_parts[0];
    let sql_suffix = if sort_parts.len() > 1 {
        sort_parts[1]
    } else {
        ""
    };

    // Pre-compute the prefix/suffix for the LIMIT 2 variant
    let needs_limit = has_columns
        && is_select
        && !parsed.normalized_sql.contains(" limit ")
        && !parsed.normalized_sql.contains(" for ");

    let limited_suffix = if needs_limit {
        format!("{sql_suffix} LIMIT 2")
    } else {
        sql_suffix.to_owned()
    };
    let limited_suffix_lit = &limited_suffix;

    // Generate the sort SQL lookup helper that caches (Arc<str>, u64) per sort fragment.
    // Uses a static Mutex<Vec> cache: since sort enums have a small finite number of
    // variants and sort.sql() returns &'static str, we cache using the pointer value
    // as key. First call per variant allocates once into an Arc<str>; all subsequent
    // calls clone the Arc (cheap ref-count bump) and borrow from the local clone.
    //
    // No Box::leak — the Arc<str> is owned by the cache Vec (which lives in a static
    // LazyLock) and by the local clone (which lives for the duration of the function).
    // Total memory is bounded: one Arc<str> per unique sort variant (typically 3-10).
    let build_sql = quote! {
        // Cache: maps sort fragment &'static str pointer -> (Arc<str>, hash)
        static SORT_SQL_CACHE: ::std::sync::LazyLock<::std::sync::Mutex<Vec<(usize, ::std::sync::Arc<str>, u64)>>> = ::std::sync::LazyLock::new(|| ::std::sync::Mutex::new(Vec::new()));
        let sort_fragment: &'static str = self.sort.sql();
        let key = sort_fragment.as_ptr() as usize;
        let (sql_arc, sql_hash) = {
            let guard = SORT_SQL_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                (entry.1.clone(), entry.2)
            } else {
                drop(guard);
                let built = format!("{}{}{}", #sql_prefix, sort_fragment, #sql_suffix);
                let hash = ::bsql_core::driver::hash_sql(&built);
                let arc: ::std::sync::Arc<str> = ::std::sync::Arc::from(built);
                let mut guard = SORT_SQL_CACHE.lock().unwrap_or_else(|e| e.into_inner());
                // Double-check after re-acquiring lock
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.clone(), entry.2)
                } else {
                    guard.push((key, arc.clone(), hash));
                    (arc, hash)
                }
            }
        };
        let sql: &str = &sql_arc;
    };

    let build_limited_sql = if needs_limit {
        quote! {
            // Cache: maps sort fragment &'static str pointer -> (Arc<str>, hash)
            static SORT_LIMITED_SQL_CACHE: ::std::sync::LazyLock<::std::sync::Mutex<Vec<(usize, ::std::sync::Arc<str>, u64)>>> = ::std::sync::LazyLock::new(|| ::std::sync::Mutex::new(Vec::new()));
            let sort_fragment: &'static str = self.sort.sql();
            let key = sort_fragment.as_ptr() as usize;
            let (sql_arc, sql_hash) = {
                let guard = SORT_LIMITED_SQL_CACHE.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.clone(), entry.2)
                } else {
                    drop(guard);
                    let built = format!("{}{}{}", #sql_prefix, sort_fragment, #limited_suffix_lit);
                    let hash = ::bsql_core::driver::hash_sql(&built);
                    let arc: ::std::sync::Arc<str> = ::std::sync::Arc::from(built);
                    let mut guard = SORT_LIMITED_SQL_CACHE.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                        (entry.1.clone(), entry.2)
                    } else {
                        guard.push((key, arc.clone(), hash));
                        (arc, hash)
                    }
                }
            };
            let sql: &str = &sql_arc;
        }
    } else {
        build_sql.clone()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);
        let row_decode = gen_row_decode(validation);
        let column_check = gen_column_count_check(validation);

        quote! {
            #[allow(non_camel_case_types)]
            pub struct #stream_name {
                inner: ::bsql_core::QueryStream,
            }

            #[allow(non_camel_case_types)]
            impl #stream_name {
                ::bsql_core::__bsql_fn! {
                    pub fn next(&mut self) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                        if let Some(row) = self.inner.next_row() {
                            #column_check
                            return Ok(Some(#result_name { #row_decode }));
                        }
                        if !::bsql_core::__bsql_call!(self.inner.fetch_next_chunk())? {
                            return Ok(None);
                        }
                        match self.inner.next_row() {
                            Some(row) => {
                                #column_check
                                Ok(Some(#result_name { #row_decode }))
                            },
                            None => Ok(None),
                        }
                    }
                }

                pub fn remaining(&self) -> usize {
                    self.inner.remaining()
                }
            }

            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                ::bsql_core::__bsql_fn! {
                    pub fn fetch_one(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<#result_name> {
                        #build_limited_sql
                        let mut target = executor.into(); let owned = ::bsql_core::__bsql_call!(target.query(::bsql_core::Sql::precomputed(sql, sql_hash, #readonly_val), #params_slice))?;
                        if owned.len() != 1 {
                            return Err(::bsql_core::error::QueryError::row_count(
                                "exactly 1 row",
                                owned.len() as u64,
                            ));
                        }
                        let row = owned.row(0);
                        #column_check
                        Ok(#result_name { #row_decode })
                    }
                }

                ::bsql_core::__bsql_fn! {
                    pub fn fetch_optional(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                        #build_limited_sql
                        let mut target = executor.into(); let owned = ::bsql_core::__bsql_call!(target.query(::bsql_core::Sql::precomputed(sql, sql_hash, #readonly_val), #params_slice))?;
                        match owned.len() {
                            0 => Ok(None),
                            1 => {
                                let row = owned.row(0);
                                #column_check
                                Ok(Some(#result_name { #row_decode }))
                            }
                            n => Err(::bsql_core::error::QueryError::row_count(
                                "0 or 1 rows",
                                n as u64,
                            )),
                        }
                    }
                }

                ::bsql_core::__bsql_fn! {
                    pub fn fetch_all(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                        #build_sql
                        let mut target = executor.into(); let owned = ::bsql_core::__bsql_call!(target.query(::bsql_core::Sql::precomputed(sql, sql_hash, #readonly_val), #params_slice))?;
                        owned.iter().map(|row| {
                            #column_check
                            Ok(#result_name { #row_decode })
                        }).collect::<::bsql_core::BsqlResult<Vec<_>>>()
                    }
                }

                ::bsql_core::__bsql_fn! {
                    pub fn fetch_stream(
                        self,
                        pool: &::bsql_core::Pool,
                    ) -> ::bsql_core::BsqlResult<#stream_name> {
                        #build_sql
                        let inner = ::bsql_core::__bsql_call!(pool.query_stream(sql, sql_hash, #params_slice))?;
                        Ok(#stream_name { inner })
                    }
                }

                ::bsql_core::__bsql_fn! {
                    pub fn execute(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<u64> {
                        #build_sql
                        let mut target = executor.into(); ::bsql_core::__bsql_call!(target.execute(::bsql_core::Sql::precomputed(sql, sql_hash, false), #params_slice))
                    }
                }

                /// Buffer this operation in a transaction for pipeline flush on commit.
                ::bsql_core::__bsql_fn! {
                    pub fn defer(self, tx: &mut ::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                        #build_sql
                        ::bsql_core::__bsql_call!(tx.defer_execute(sql, sql_hash, #params_slice))
                    }
                }

            }
        }
    } else {
        // Execute-only (no result columns)
        quote! {
            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                ::bsql_core::__bsql_fn! {
                    pub fn execute(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<u64> {
                        #build_sql
                        let mut target = executor.into(); ::bsql_core::__bsql_call!(target.execute(::bsql_core::Sql::precomputed(sql, sql_hash, false), #params_slice))
                    }
                }

                /// Buffer this operation in a transaction for pipeline flush on commit.
                ::bsql_core::__bsql_fn! {
                    pub fn defer(self, tx: &mut ::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                        #build_sql
                        ::bsql_core::__bsql_call!(tx.defer_execute(sql, sql_hash, #params_slice))
                    }
                }
            }
        }
    };

    let for_each_row_struct = gen_pg_for_each_row_struct(parsed, validation);

    // Constructor: captures params + sort from scope
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
            #for_each_row_struct
            #executor_struct
            #fetch_methods
            #constructor
        }
    }
}

/// Generate the complete Rust code for a `query_as!` invocation.
///
/// Like `generate_query_code` but maps results into `#target_type` instead of
/// generating an anonymous struct. No result struct, rows struct, or borrowed
/// wrappers are generated — rustc verifies field names and types via the struct
/// literal `#target_type { field: decode, ... }`.
pub fn generate_query_as_code(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    target_type: &syn::Path,
) -> TokenStream {
    let executor_struct = gen_executor_struct(parsed);
    let executor_impls = gen_query_as_executor_impls(parsed, validation, target_type);
    let constructor = gen_constructor(parsed);

    quote! {
        {
            #executor_struct
            #executor_impls
            #constructor
        }
    }
}

/// Generate executor impls for `query_as!` — maps results into `#target_type`.
///
/// Uses the same Sql-based codegen pattern as query!. The only difference:
/// `type Row = #target_type` (user-defined struct) instead of a generated anonymous struct.
fn gen_query_as_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    target_type: &syn::Path,
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let eff_sql = effective_sql(parsed, validation);
    let sql_lit = eff_sql;

    let is_select = parsed.kind == crate::parse::QueryKind::Select;

    let param_refs: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            quote! { &self.#name as &(dyn ::bsql_core::driver::Encode + Sync) }
        })
        .collect();

    let sql_hash_val = bsql_core::rapid_hash_str(eff_sql);

    let has_columns = !validation.columns.is_empty();

    let needs_limit = has_columns
        && is_select
        && !parsed.normalized_sql.contains(" limit ")
        && !parsed.normalized_sql.contains(" for ");
    let limited_sql = if needs_limit {
        format!("{} LIMIT 2", eff_sql)
    } else {
        eff_sql.to_owned()
    };
    let limited_sql_lit = &limited_sql;
    let limited_sql_hash_val = bsql_core::rapid_hash_str(&limited_sql);

    let (decode_bindings, field_names) = if has_columns {
        gen_query_as_decode_bindings(validation)
    } else {
        (TokenStream::new(), TokenStream::new())
    };

    let column_check = gen_column_count_check(validation);

    let params_slice = if param_refs.is_empty() {
        quote! { &[] }
    } else {
        quote! { &[#(#param_refs),*] }
    };

    // Sql::precomputed values (zero-cost: compile-time consts)
    let readonly_val = is_select;
    let sql_ctor = quote! { ::bsql_core::Sql::precomputed(#sql_lit, #sql_hash_val, #readonly_val) };
    let limited_sql_ctor = quote! { ::bsql_core::Sql::precomputed(#limited_sql_lit, #limited_sql_hash_val, #readonly_val) };

    let fetch_methods = if has_columns {
        quote! {
            ::bsql_core::__bsql_fn! {
                pub fn fetch_one(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<#target_type> {
                    let mut target = executor.into();
                    let owned = ::bsql_core::__bsql_call!(target.query(#limited_sql_ctor, #params_slice))?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    let row = owned.row(0);
                    #column_check
                    #decode_bindings
                    Ok(#target_type { #field_names })
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn fetch_all(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<Vec<#target_type>> {
                    let mut target = executor.into();
                    let owned = ::bsql_core::__bsql_call!(target.query(#sql_ctor, #params_slice))?;
                    owned.iter().map(|row| {
                        #column_check
                        #decode_bindings
                        Ok(#target_type { #field_names })
                    }).collect::<::bsql_core::BsqlResult<Vec<_>>>()
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn fetch_optional(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<Option<#target_type>> {
                    let mut target = executor.into();
                    let owned = ::bsql_core::__bsql_call!(target.query(#limited_sql_ctor, #params_slice))?;
                    match owned.len() {
                        0 => Ok(None),
                        1 => {
                            let row = owned.row(0);
                            #column_check
                            #decode_bindings
                            Ok(Some(#target_type { #field_names }))
                        }
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }
            }
        }
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        ::bsql_core::__bsql_fn! {
            pub fn execute(
                self,
                executor: impl Into<::bsql_core::QueryTarget<'_>>,
            ) -> ::bsql_core::BsqlResult<u64> {
                let mut target = executor.into();
                ::bsql_core::__bsql_call!(target.execute(#sql_ctor, #params_slice))
            }
        }
    };

    let defer_method = quote! {
        /// Buffer this operation in a transaction for pipeline flush on commit.
        ::bsql_core::__bsql_fn! {
            pub fn defer(self, tx: &mut ::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                ::bsql_core::__bsql_call!(tx.defer_execute(#sql_lit, #sql_hash_val, #params_slice))
            }
        }
    };

    quote! {
        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #execute_method
            #defer_method
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
                let line_str = line.to_string();
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
        #[must_use = "query is not executed until .fetch_all(), .execute(), or another execution method is called"]
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
    let eff_sql = effective_sql(parsed, validation);
    let sql_lit = eff_sql;

    let is_select = parsed.kind == crate::parse::QueryKind::Select;

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
    let sql_hash_val = bsql_core::rapid_hash_str(eff_sql);

    let has_columns = !validation.columns.is_empty();

    // Generate a LIMIT 2 variant for fetch_one/fetch_optional
    let needs_limit = has_columns
        && is_select
        && !parsed.normalized_sql.contains(" limit ")
        && !parsed.normalized_sql.contains(" for ");
    let limited_sql = if needs_limit {
        format!("{} LIMIT 2", eff_sql)
    } else {
        eff_sql.to_owned()
    };
    let limited_sql_lit = &limited_sql;
    let limited_sql_hash_val = bsql_core::rapid_hash_str(&limited_sql);

    // Cache row decode once, reuse for all methods (F-27)
    let row_decode = if has_columns {
        gen_row_decode(validation)
    } else {
        TokenStream::new()
    };

    // Column-count bounds check — inserted before every row decode (Fix-5)
    let column_check = gen_column_count_check(validation);

    // Compile-time Parse+Describe wire bytes for zero-cost first-execution
    let param_oids: Vec<u32> = validation.param_pg_oids.iter().copied().collect();
    let parse_bytes = gen_parse_describe_bytes(eff_sql, sql_hash_val, &param_oids);
    let parse_bytes_tokens: Vec<TokenStream> = parse_bytes.iter().map(|b| quote! { #b }).collect();
    let parse_bytes_len = parse_bytes.len();

    // Limited SQL variant (fetch_one/fetch_optional with LIMIT 2) — also gets its own Parse bytes
    let limited_parse_bytes =
        gen_parse_describe_bytes(&limited_sql, limited_sql_hash_val, &param_oids);
    let limited_parse_tokens: Vec<TokenStream> =
        limited_parse_bytes.iter().map(|b| quote! { #b }).collect();
    let limited_parse_len = limited_parse_bytes.len();

    let readonly_val = is_select;
    let sql_ctor = quote! {
        {
            static _BSQL_PARSE: [u8; #parse_bytes_len] = [#(#parse_bytes_tokens),*];
            ::bsql_core::Sql::with_parse(#sql_lit, #sql_hash_val, #readonly_val, &_BSQL_PARSE)
        }
    };
    let limited_sql_ctor = quote! {
        {
            static _BSQL_PARSE_LIM: [u8; #limited_parse_len] = [#(#limited_parse_tokens),*];
            ::bsql_core::Sql::with_parse(#limited_sql_lit, #limited_sql_hash_val, #readonly_val, &_BSQL_PARSE_LIM)
        }
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);

        quote! {
            ::bsql_core::__bsql_fn! {
                pub fn fetch_one(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    let mut target = executor.into();
                    let owned = ::bsql_core::__bsql_call!(target.query(#limited_sql_ctor, #params_slice))?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    let row = owned.row(0);
                    #column_check
                    Ok(#result_name { #row_decode })
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn fetch_optional(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    let mut target = executor.into();
                    let owned = ::bsql_core::__bsql_call!(target.query(#limited_sql_ctor, #params_slice))?;
                    match owned.len() {
                        0 => Ok(None),
                        1 => {
                            let row = owned.row(0);
                            #column_check
                            Ok(Some(#result_name { #row_decode }))
                        }
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn fetch_all(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    let mut target = executor.into();
                    let owned = ::bsql_core::__bsql_call!(target.query(#sql_ctor, #params_slice))?;
                    owned.iter().map(|row| {
                        #column_check
                        Ok(#result_name { #row_decode })
                    }).collect::<::bsql_core::BsqlResult<Vec<_>>>()
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn fetch_stream(
                    self,
                    pool: &::bsql_core::Pool,
                ) -> ::bsql_core::BsqlResult<#stream_name> {
                    let inner = ::bsql_core::__bsql_call!(pool.query_stream(#sql_lit, #sql_hash_val, #params_slice))?;
                    Ok(#stream_name { inner })
                }
            }
        }
    } else {
        TokenStream::new()
    };

    // Use extracted gen_stream_struct (F-26)
    let stream_struct = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);
        gen_stream_struct(&result_name, &stream_name, &row_decode, &column_check)
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        ::bsql_core::__bsql_fn! {
            pub fn execute(
                self,
                executor: impl Into<::bsql_core::QueryTarget<'_>>,
            ) -> ::bsql_core::BsqlResult<u64> {
                let mut target = executor.into();
                ::bsql_core::__bsql_call!(target.execute(#sql_ctor, #params_slice))
            }
        }
    };

    let defer_method = quote! {
        /// Buffer this operation in a transaction for pipeline flush on commit.
        ::bsql_core::__bsql_fn! {
            pub fn defer(self, tx: &mut ::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                ::bsql_core::__bsql_call!(tx.defer_execute(#sql_lit, #sql_hash_val, #params_slice))
            }
        }
    };

    // --- PG for_each (streaming from raw wire bytes — zero-alloc) ---
    let for_each_row_struct = if has_columns {
        gen_pg_for_each_row_struct(parsed, validation)
    } else {
        TokenStream::new()
    };

    let for_each_methods = if has_columns && is_select {
        let fe_row_name = pg_for_each_row_struct_name(parsed);

        // Use inline raw-bytes decode (no PgDataRow, no SmallVec) for all queries.
        // For feature-gated types that need PgDataRow, the raw-bytes decoder
        // constructs a minimal single-column wrapper only for those columns.
        let (fe_raw_stmts, fe_raw_inits) = gen_pg_for_each_raw_decode(validation);
        let (fe_raw_stmts2, fe_raw_inits2) = gen_pg_for_each_raw_decode(validation);
        let raw_column_check = gen_raw_column_count_check(validation);

        quote! {
            /// Process each row directly from the wire buffer via a closure.
            ///
            /// Zero arena allocation, zero SmallVec — the generated code decodes
            /// columns sequentially inline from the raw DataRow message bytes.
            ::bsql_core::__bsql_fn! {
                pub fn for_each<_BsqlForEachF>(
                    self,
                    pool: &::bsql_core::Pool,
                    mut f: _BsqlForEachF,
                ) -> ::bsql_core::BsqlResult<()>
                where
                    _BsqlForEachF: FnMut(#fe_row_name<'_>) -> Result<(), ::bsql_core::BsqlError>,
                {
                    ::bsql_core::__bsql_call!(pool.__for_each_raw_bytes(
                        #sql_lit,
                        #sql_hash_val,
                        #params_slice,
                        true,
                        |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                            #raw_column_check
                            #fe_raw_stmts
                            let _bsql_typed = #fe_row_name { #fe_raw_inits };
                            f(_bsql_typed)
                        },
                    ))
                }
            }

            /// Process each row, collecting mapped results into a `Vec`.
            ::bsql_core::__bsql_fn! {
                pub fn for_each_map<_BsqlForEachF, _BsqlForEachT>(
                    self,
                    pool: &::bsql_core::Pool,
                    mut f: _BsqlForEachF,
                ) -> ::bsql_core::BsqlResult<Vec<_BsqlForEachT>>
                where
                    _BsqlForEachF: FnMut(#fe_row_name<'_>) -> _BsqlForEachT,
                {
                    let mut _bsql_results: Vec<_BsqlForEachT> = Vec::new();
                    ::bsql_core::__bsql_call!(pool.__for_each_raw_bytes(
                        #sql_lit,
                        #sql_hash_val,
                        #params_slice,
                        true,
                        |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                            #raw_column_check
                            #fe_raw_stmts2
                            let _bsql_typed = #fe_row_name { #fe_raw_inits2 };
                            _bsql_results.push(f(_bsql_typed));
                            Ok(())
                        },
                    ))?;
                    Ok(_bsql_results)
                }
            }
        }
    } else {
        TokenStream::new()
    };

    quote! {
        #stream_struct
        #for_each_row_struct

        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #for_each_methods
            #execute_method
            #defer_method
        }
    }
}

// ---- Dynamic query codegen ----

/// Generate the executor struct for a dynamic query.
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

/// Generate the impl block for a dynamic query executor.
fn gen_dynamic_executor_impls(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let has_columns = !validation.columns.is_empty();

    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let readonly_val = is_select;

    // Cache row decode once, reuse for fetch methods + stream struct (F-27)
    let row_decode = if has_columns {
        gen_row_decode(validation)
    } else {
        TokenStream::new()
    };

    // Column-count bounds check — inserted before every row decode (Fix-5)
    let column_check = gen_column_count_check(validation);

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);

        let needs_limit = has_columns
            && is_select
            && !parsed.normalized_sql.contains(" limit ")
            && !parsed.normalized_sql.contains(" for ");

        let owned_fetch_one_optional = {
            let fetch_one_dispatcher = gen_runtime_dispatcher(parsed, needs_limit, |_| {
                quote! {
                    let mut target = executor.into(); let owned = ::bsql_core::__bsql_call!(target.query(::bsql_core::Sql::precomputed(&_bsql_sql, _bsql_hash, #readonly_val), &_bsql_params[..]))?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    let row = owned.row(0);
                    #column_check
                    Ok(#result_name { #row_decode })
                }
            });

            let fetch_optional_dispatcher = gen_runtime_dispatcher(parsed, needs_limit, |_| {
                quote! {
                    let mut target = executor.into(); let owned = ::bsql_core::__bsql_call!(target.query(::bsql_core::Sql::precomputed(&_bsql_sql, _bsql_hash, #readonly_val), &_bsql_params[..]))?;
                    match owned.len() {
                        0 => Ok(None),
                        1 => {
                            let row = owned.row(0);
                            #column_check
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
                ::bsql_core::__bsql_fn! {
                    pub fn fetch_one(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<#result_name> {
                        #fetch_one_dispatcher
                    }
                }

                ::bsql_core::__bsql_fn! {
                    pub fn fetch_optional(
                        self,
                        executor: impl Into<::bsql_core::QueryTarget<'_>>,
                    ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                        #fetch_optional_dispatcher
                    }
                }
            }
        };

        let fetch_dispatcher = gen_runtime_dispatcher(parsed, false, |_| {
            quote! {
                let mut target = executor.into(); let owned = ::bsql_core::__bsql_call!(target.query(::bsql_core::Sql::precomputed(&_bsql_sql, _bsql_hash, #readonly_val), &_bsql_params[..]))?;
                owned.iter().map(|row| {
                    #column_check
                    Ok(#result_name { #row_decode })
                }).collect::<::bsql_core::BsqlResult<Vec<_>>>()
            }
        });

        let fetch_stream_dispatcher = gen_runtime_dispatcher(parsed, false, |_| {
            quote! {
                let inner = ::bsql_core::__bsql_call!(pool.query_stream(&_bsql_sql, _bsql_hash, &_bsql_params[..]))?;
                Ok(#stream_name { inner })
            }
        });

        quote! {
            #owned_fetch_one_optional

            ::bsql_core::__bsql_fn! {
                pub fn fetch_all(
                    self,
                    executor: impl Into<::bsql_core::QueryTarget<'_>>,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #fetch_dispatcher
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn fetch_stream(
                    self,
                    pool: &::bsql_core::Pool,
                ) -> ::bsql_core::BsqlResult<#stream_name> {
                    #fetch_stream_dispatcher
                }
            }
        }
    } else {
        TokenStream::new()
    };

    // Use extracted gen_stream_struct (F-26)
    let stream_struct = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);
        gen_stream_struct(&result_name, &stream_name, &row_decode, &column_check)
    } else {
        TokenStream::new()
    };

    let execute_dispatcher = gen_runtime_dispatcher(parsed, false, |_| {
        quote! {
            let mut target = executor.into(); ::bsql_core::__bsql_call!(target.execute(::bsql_core::Sql::precomputed(&_bsql_sql, _bsql_hash, false), &_bsql_params[..]))
        }
    });

    let execute_method = quote! {
        ::bsql_core::__bsql_fn! {
            pub fn execute(
                self,
                executor: impl Into<::bsql_core::QueryTarget<'_>>,
            ) -> ::bsql_core::BsqlResult<u64> {
                #execute_dispatcher
            }
        }
    };

    let defer_dispatcher = gen_runtime_dispatcher(parsed, false, |_| {
        quote! {
            ::bsql_core::__bsql_call!(tx.defer_execute(&_bsql_sql, _bsql_hash, &_bsql_params[..]))
        }
    });

    let defer_method = quote! {
        /// Buffer this operation in a transaction for pipeline flush on commit.
        ::bsql_core::__bsql_fn! {
            pub fn defer(self, tx: &mut ::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                #defer_dispatcher
            }
        }
    };

    // --- PG for_each for dynamic queries ---
    let for_each_methods = if has_columns && is_select {
        let fe_row_name = pg_for_each_row_struct_name(parsed);
        let (fe_raw_stmts, fe_raw_inits) = gen_pg_for_each_raw_decode(validation);
        let (fe_raw_stmts2, fe_raw_inits2) = gen_pg_for_each_raw_decode(validation);
        let raw_column_check = gen_raw_column_count_check(validation);

        let for_each_dispatcher = gen_runtime_dispatcher(parsed, false, |_| {
            quote! {
                ::bsql_core::__bsql_call!(pool.__for_each_raw_bytes(
                    &_bsql_sql,
                    _bsql_hash,
                    &_bsql_params[..],
                    true,
                    |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                        #raw_column_check
                        #fe_raw_stmts
                        let _bsql_typed = #fe_row_name { #fe_raw_inits };
                        f(_bsql_typed)
                    },
                ))
            }
        });

        let for_each_map_dispatcher = gen_runtime_dispatcher(parsed, false, |_| {
            quote! {
                ::bsql_core::__bsql_call!(pool.__for_each_raw_bytes(
                    &_bsql_sql,
                    _bsql_hash,
                    &_bsql_params[..],
                    true,
                    |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                        #raw_column_check
                        #fe_raw_stmts2
                        let _bsql_typed = #fe_row_name { #fe_raw_inits2 };
                        _bsql_results.push(f(_bsql_typed));
                        Ok(())
                    },
                ))
            }
        });

        quote! {
            ::bsql_core::__bsql_fn! {
                pub fn for_each<_BsqlForEachF>(
                    self,
                    pool: &::bsql_core::Pool,
                    mut f: _BsqlForEachF,
                ) -> ::bsql_core::BsqlResult<()>
                where
                    _BsqlForEachF: FnMut(#fe_row_name<'_>) -> Result<(), ::bsql_core::BsqlError>,
                {
                    #for_each_dispatcher
                }
            }

            ::bsql_core::__bsql_fn! {
                pub fn for_each_map<_BsqlForEachF, _BsqlForEachT>(
                    self,
                    pool: &::bsql_core::Pool,
                    mut f: _BsqlForEachF,
                ) -> ::bsql_core::BsqlResult<Vec<_BsqlForEachT>>
                where
                    _BsqlForEachF: FnMut(#fe_row_name<'_>) -> _BsqlForEachT,
                {
                    let mut _bsql_results: Vec<_BsqlForEachT> = Vec::new();
                    #for_each_map_dispatcher?;
                    Ok(_bsql_results)
                }
            }
        }
    } else {
        TokenStream::new()
    };

    quote! {
        #stream_struct

        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #for_each_methods
            #execute_method
            #defer_method
        }
    }
}

/// Generate a runtime SQL dispatcher for dynamic queries with optional clauses.
///
/// Instead of generating 2^N match arms (one per optional-clause combination),
/// generates O(N) code that builds the SQL string at runtime by conditionally
/// appending each clause fragment when its `Option` parameter is `Some`.
///
/// The generated code:
/// 1. Splits the base SQL at `{OPT_N}` placeholders (pre-computed at compile time)
/// 2. For each clause: if `self.param.is_some()`, appends the fragment with
///    a dynamically-numbered `$N` parameter placeholder
/// 3. Computes the sql_hash via `::bsql_core::rapid_hash_str`
/// 4. Calls `body_fn` with the constructed SQL + hash + params
///
/// The closure receives a unit argument (for compatibility with the call sites)
/// and should reference the runtime variables `_bsql_sql`, `_bsql_hash`, and
/// `_bsql_params` in its generated code.
fn gen_runtime_dispatcher<F>(parsed: &ParsedQuery, inject_limit: bool, body_fn: F) -> TokenStream
where
    F: Fn(()) -> TokenStream,
{
    // Pre-split the positional SQL at {OPT_N} markers into N+1 segments.
    // E.g. "SELECT id FROM t WHERE a IS NULL{OPT_0}{OPT_1} ORDER BY id"
    // becomes ["SELECT id FROM t WHERE a IS NULL", "", " ORDER BY id"]
    let mut segments: Vec<String> = Vec::new();
    let mut remaining = parsed.positional_sql.as_str();
    for i in 0..parsed.optional_clauses.len() {
        let marker = format!("{{OPT_{i}}}");
        if let Some(pos) = remaining.find(&marker) {
            segments.push(remaining[..pos].to_owned());
            remaining = &remaining[pos + marker.len()..];
        } else {
            // Should not happen if parse.rs is correct, but be defensive
            segments.push(remaining.to_owned());
            remaining = "";
        }
    }
    segments.push(remaining.to_owned());

    // Estimate total SQL length for String::with_capacity
    let base_len: usize = segments.iter().map(|s| s.len()).sum();
    let clause_len: usize = parsed
        .optional_clauses
        .iter()
        .map(|c| c.sql_fragment.len() + 8) // +8 for " " padding + "$NN"
        .sum();
    let capacity = base_len + clause_len + if inject_limit { 10 } else { 0 };

    // The first segment is always present
    let first_segment = &segments[0];

    // Generate code to push base params
    let base_param_pushes: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            quote! {
                _bsql_params.push(&self.#name as &(dyn ::bsql_core::driver::Encode + Sync));
            }
        })
        .collect();

    // For each optional clause, generate the conditional append code
    let clause_appends: Vec<TokenStream> = parsed
        .optional_clauses
        .iter()
        .enumerate()
        .map(|(i, clause)| {
            let param_name = param_ident(&clause.params[0].name);
            // The trailing segment after this {OPT_N} marker
            let trailing = &segments[i + 1];

            // Pre-split the clause sql_fragment at ${P_N} markers to get
            // text segments that we can emit as string literals.
            // E.g. "AND dept_id = ${P_1}" -> ["AND dept_id = ", ""]
            // At runtime we splice in "$<param_number>" between them.
            let frag = &clause.sql_fragment;
            let mut frag_parts: Vec<String> = Vec::new();
            let mut frag_remaining = frag.as_str();

            // Each clause has exactly one unique param (enforced by parser),
            // but the param can appear multiple times. Split at all ${P_N}.
            loop {
                if let Some(pos) = frag_remaining.find("${P_") {
                    frag_parts.push(frag_remaining[..pos].to_owned());
                    // Skip past ${P_N}
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

            // Number of ${P_N} placeholders = frag_parts.len() - 1
            let num_refs = frag_parts.len() - 1;

            // Generate the fragment append code
            let frag_append: Vec<TokenStream> = frag_parts
                .iter()
                .enumerate()
                .map(|(j, part)| {
                    if j == 0 {
                        // First segment: push " " + text (leading space before clause)
                        quote! {
                            _bsql_sql.push(' ');
                            _bsql_sql.push_str(#part);
                        }
                    } else {
                        // After each ${P_N}: push "$<n>" then remaining text
                        quote! {
                            _bsql_sql.push('$');
                            {
                                // Inline itoa: avoid format! allocation for param numbers
                                let _n = _bsql_params.len() + 1;
                                if _n < 10 {
                                    _bsql_sql.push((b'0' + _n as u8) as char);
                                } else if _n < 100 {
                                    _bsql_sql.push((b'0' + (_n / 10) as u8) as char);
                                    _bsql_sql.push((b'0' + (_n % 10) as u8) as char);
                                } else {
                                    // Fallback for 100+ params (rare)
                                    let _s = _n.to_string();
                                    _bsql_sql.push_str(&_s);
                                }
                            }
                            _bsql_sql.push_str(#part);
                        }
                    }
                })
                .collect();

            // Push the param reference (once, after all fragment parts that reference it)
            let param_push = quote! {
                _bsql_params.push(self.#param_name.as_ref().unwrap() as &(dyn ::bsql_core::driver::Encode + Sync));
            };

            // Also append any trailing segment after {OPT_N} (usually empty or " ORDER BY ...")
            let trailing_push = if trailing.is_empty() {
                TokenStream::new()
            } else {
                quote! { _bsql_sql.push_str(#trailing); }
            };

            // For clauses where param appears multiple times in the fragment,
            // we only push the param once but reference it with incrementing $N.
            // However, each ${P_N} occurrence refers to the same param.
            // We need to push the param N times so each $N gets its own slot.
            // Actually no -- each appearance of the same param in one clause
            // uses the SAME ${P_N} value, so it should be the same $N at runtime.
            // Let me reconsider: the clause has one unique param but it can appear
            // multiple times. In PG, each $N reference can reuse the same number.
            // So we push the param once and emit the same $N for each occurrence.
            //
            // But wait: our fragment splitting above splits at EVERY ${P_N}.
            // The issue is that if the same param appears twice, we get two splits
            // but should emit the SAME $N for both. And push only once.
            //
            // Revised approach: push param first, then use that fixed $N for all refs.
            if num_refs <= 1 {
                // Common case: param appears exactly once in fragment
                quote! {
                    if self.#param_name.is_some() {
                        #(#frag_append)*
                        #param_push
                    }
                    #trailing_push
                }
            } else {
                // Rare case: same param appears multiple times in fragment.
                // Push param first, capture the $N, then emit all refs with same $N.
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
                            // All refs use the same _bsql_pn
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
                        let _bsql_pn = format!("${}", _bsql_params.len());
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

    quote! {
        {
            let mut _bsql_sql = ::std::string::String::with_capacity(#capacity);
            _bsql_sql.push_str(#first_segment);
            let mut _bsql_params: ::std::vec::Vec<&(dyn ::bsql_core::driver::Encode + Sync)> =
                ::std::vec::Vec::with_capacity(#max_params);

            // Base params (always present)
            #(#base_param_pushes)*

            // Optional clause fragments (conditionally appended)
            #(#clause_appends)*

            #limit_push

            let _bsql_hash = ::bsql_core::rapid_hash_str(&_bsql_sql);

            #body
        }
    }
}

/// Generate the constructor for a dynamic query executor.
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

/// Generate the stream struct and its `next()` / `remaining()` methods.
/// Shared by static, dynamic, and sort codegen paths.
fn gen_stream_struct(
    result_name: &proc_macro2::Ident,
    stream_name: &proc_macro2::Ident,
    row_decode: &TokenStream,
    column_check: &TokenStream,
) -> TokenStream {
    quote! {
        #[allow(non_camel_case_types)]
        pub struct #stream_name {
            inner: ::bsql_core::QueryStream,
        }

        #[allow(non_camel_case_types)]
        impl #stream_name {
            /// Get the next typed row, or `None` when all rows have been consumed.
            ///
            /// Fetches the next chunk from PG when the current chunk is exhausted
            /// (true streaming via `Execute(max_rows=64)`).
            ::bsql_core::__bsql_fn! {
                pub fn next(&mut self) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    if let Some(row) = self.inner.next_row() {
                        #column_check
                        return Ok(Some(#result_name { #row_decode }));
                    }
                    if !::bsql_core::__bsql_call!(self.inner.fetch_next_chunk())? {
                        return Ok(None);
                    }
                    match self.inner.next_row() {
                        Some(row) => {
                            #column_check
                            Ok(Some(#result_name { #row_decode }))
                        },
                        None => Ok(None),
                    }
                }
            }

            /// Number of remaining rows in the current chunk.
            pub fn remaining(&self) -> usize {
                self.inner.remaining()
            }
        }
    }
}

// ---- PG for_each codegen ----

/// Name for the PG for_each row struct (borrowed lifetime version).
fn pg_for_each_row_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlForEachRow_{}", &parsed.statement_name)
}

/// Convert a column rust_type to its PG for_each borrowed equivalent.
///
/// `String` -> `&'a str` (zero-copy from wire buffer),
/// `Vec<u8>` -> `&'a [u8]`,
/// `Option<String>` -> `Option<&'a str>`, etc.
/// Scalar types (i32, i64, f64, bool) are Copy and remain as-is.
fn pg_for_each_result_type(type_str: &str) -> TokenStream {
    match type_str {
        "String" => quote! { &'a str },
        "Vec<u8>" => quote! { &'a [u8] },
        "Vec<String>" => quote! { Vec<&'a str> },
        "Vec<Vec<u8>>" => quote! { Vec<&'a [u8]> },
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

/// Generate the PG for_each row struct with borrowed lifetime.
fn gen_pg_for_each_row_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new();
    }

    let struct_name = pg_for_each_row_struct_name(parsed);
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = validation.columns.iter().enumerate().map(|(i, col)| {
        let field_name = format_ident!("{}", deduped_names[i]);
        let field_type = pg_for_each_result_type(&col.rust_type);
        quote! { pub #field_name: #field_type }
    });

    // Check if any column actually uses the 'a lifetime.
    let needs_lifetime = validation.columns.iter().any(|col| {
        let rt = &col.rust_type;
        matches!(rt.as_str(), "String" | "Vec<u8>" | "Vec<String>" | "Vec<Vec<u8>>")
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

// ---- PG for_each RAW BYTES inline decode (no PgDataRow, no SmallVec) ----

/// PostgreSQL binary wire size for fixed-size types.
///
/// Returns `Some(n)` when the type has a known, constant wire representation
/// size in PostgreSQL binary format. Returns `None` for variable-length types.
fn pg_wire_size(rust_type: &str) -> Option<usize> {
    match rust_type {
        "bool" => Some(1),
        "i16" => Some(2),
        "i32" | "f32" | "u32" => Some(4),
        "i64" | "f64" => Some(8),
        _ => None,
    }
}

/// Generate inline sequential decode for PG for_each raw-bytes path.
///
/// Instead of constructing a `PgDataRow` and calling `.get_i32(idx)` etc.,
/// this generates code that advances `_bsql_pos` through `_bsql_data` sequentially,
/// reading each column's 4-byte length prefix followed by the column bytes.
///
/// **Compile-time column offsets**: when leading columns are fixed-size NOT NULL
/// (bool, i16, i32, i64, f32, f64, u32), their byte offsets within the DataRow
/// payload are known at compile time. The generated code reads them via literal
/// array indices — no length prefix read, no NULL branch, no position tracking.
/// A single bounds check at the start enables the compiler to elide all per-column
/// bounds checks for the fixed prefix.
///
/// For basic types (bool, i16, i32, i64, f32, f64, str, bytes): direct inline decode.
/// For feature-gated types (uuid, time, chrono, decimal, arrays): extracts the raw
/// column slice and calls the same `::bsql_core::driver::decode_*` functions.
fn gen_pg_for_each_raw_decode(validation: &ValidationResult) -> (TokenStream, TokenStream) {
    let deduped_names = deduplicate_column_names(&validation.columns);

    // Determine the fixed-size NOT NULL prefix: the longest leading run of columns
    // whose wire size is known at compile time and that are NOT NULL.
    let fixed_prefix_len = validation
        .columns
        .iter()
        .take_while(|col| {
            !col.rust_type.starts_with("Option<") && pg_wire_size(&col.rust_type).is_some()
        })
        .count();

    // Compute byte offsets for the fixed prefix.
    // DataRow payload: [2 bytes col_count] ([4 bytes len] [data bytes])*
    let mut fixed_offsets: Vec<(usize, usize)> = Vec::new(); // (data_offset, wire_size)
    let mut cursor: usize = 2; // skip i16 col_count
    for col in &validation.columns[..fixed_prefix_len] {
        let ws = pg_wire_size(&col.rust_type).unwrap();
        cursor += 4; // skip length prefix (present in wire but we don't read it)
        fixed_offsets.push((cursor, ws));
        cursor += ws;
    }
    let fixed_end = cursor; // _bsql_pos starts here for remaining columns

    // Generate decode statements.
    let mut decode_stmts: Vec<TokenStream> = Vec::new();

    // --- Fixed-prefix columns: compile-time offsets, direct access ---
    if fixed_prefix_len > 0 {
        let min_size = fixed_end;
        decode_stmts.push(quote! {
            if _bsql_data.len() < #min_size {
                return Err(::bsql_core::error::DecodeError::with_source(
                    "row", "fixed-layout", "truncated DataRow payload",
                    ::std::io::Error::new(
                        ::std::io::ErrorKind::UnexpectedEof,
                        "row too short for fixed-offset decode",
                    ),
                ));
            }
        });

        for (i, (data_offset, _ws)) in fixed_offsets.iter().enumerate() {
            let field_name = format_ident!("{}", deduped_names[i]);
            let col = &validation.columns[i];
            decode_stmts.push(gen_pg_raw_fixed_decode(&field_name, &col.rust_type, *data_offset));
        }
    }

    // --- Remaining columns: sequential decode with _bsql_pos ---
    let remaining_start = fixed_prefix_len;
    if remaining_start < validation.columns.len() {
        let init_pos = fixed_end;
        decode_stmts.push(quote! {
            let mut _bsql_pos: usize = #init_pos;
        });

        for (name, col) in deduped_names[remaining_start..].iter().zip(&validation.columns[remaining_start..]) {
            let field_name = format_ident!("{}", name);
            decode_stmts.push(gen_pg_raw_column_decode(&field_name, &col.rust_type));
        }
    }

    let field_inits: Vec<TokenStream> = deduped_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let field_name = format_ident!("{}", name);
            let _ = i;
            quote! { #field_name }
        })
        .collect();

    let needs_lifetime = validation.columns.iter().any(|col| {
        let rt = &col.rust_type;
        matches!(rt.as_str(), "String" | "Vec<u8>" | "Vec<String>" | "Vec<Vec<u8>>")
            || rt.starts_with("Option<String>")
            || rt.starts_with("Option<Vec<u8>>")
    });

    let phantom_init = if needs_lifetime {
        TokenStream::new()
    } else {
        quote! { , _marker: ::std::marker::PhantomData }
    };

    let stmts = quote! {
        #(#decode_stmts)*
    };
    let inits = quote! {
        #(#field_inits),* #phantom_init
    };
    (stmts, inits)
}

/// Generate direct-access decode for a fixed-size NOT NULL column at a
/// compile-time-known byte offset within the DataRow payload.
///
/// Emits code that reads directly via literal indices — no length prefix
/// read, no NULL check, no `_bsql_pos` tracking.
fn gen_pg_raw_fixed_decode(
    field_name: &proc_macro2::Ident,
    rust_type: &str,
    data_offset: usize,
) -> TokenStream {
    match rust_type {
        "bool" => {
            let o = data_offset;
            quote! { let #field_name = _bsql_data[#o] != 0; }
        }
        "i16" => {
            let o0 = data_offset;
            let o1 = data_offset + 1;
            quote! { let #field_name = i16::from_be_bytes([_bsql_data[#o0], _bsql_data[#o1]]); }
        }
        "i32" => {
            let o0 = data_offset;
            let o1 = data_offset + 1;
            let o2 = data_offset + 2;
            let o3 = data_offset + 3;
            quote! { let #field_name = i32::from_be_bytes([_bsql_data[#o0], _bsql_data[#o1], _bsql_data[#o2], _bsql_data[#o3]]); }
        }
        "i64" => {
            let o0 = data_offset;
            let o1 = data_offset + 1;
            let o2 = data_offset + 2;
            let o3 = data_offset + 3;
            let o4 = data_offset + 4;
            let o5 = data_offset + 5;
            let o6 = data_offset + 6;
            let o7 = data_offset + 7;
            quote! { let #field_name = i64::from_be_bytes([_bsql_data[#o0], _bsql_data[#o1], _bsql_data[#o2], _bsql_data[#o3], _bsql_data[#o4], _bsql_data[#o5], _bsql_data[#o6], _bsql_data[#o7]]); }
        }
        "f32" => {
            let o0 = data_offset;
            let o1 = data_offset + 1;
            let o2 = data_offset + 2;
            let o3 = data_offset + 3;
            quote! { let #field_name = f32::from_be_bytes([_bsql_data[#o0], _bsql_data[#o1], _bsql_data[#o2], _bsql_data[#o3]]); }
        }
        "f64" => {
            let o0 = data_offset;
            let o1 = data_offset + 1;
            let o2 = data_offset + 2;
            let o3 = data_offset + 3;
            let o4 = data_offset + 4;
            let o5 = data_offset + 5;
            let o6 = data_offset + 6;
            let o7 = data_offset + 7;
            quote! { let #field_name = f64::from_be_bytes([_bsql_data[#o0], _bsql_data[#o1], _bsql_data[#o2], _bsql_data[#o3], _bsql_data[#o4], _bsql_data[#o5], _bsql_data[#o6], _bsql_data[#o7]]); }
        }
        "u32" => {
            let o0 = data_offset;
            let o1 = data_offset + 1;
            let o2 = data_offset + 2;
            let o3 = data_offset + 3;
            quote! { let #field_name = i32::from_be_bytes([_bsql_data[#o0], _bsql_data[#o1], _bsql_data[#o2], _bsql_data[#o3]]) as u32; }
        }
        _ => unreachable!("gen_pg_raw_fixed_decode called with non-fixed type: {rust_type}"),
    }
}

/// Generate the inline decode for a single column in the raw-bytes path.
///
/// Emits: read 4-byte length, advance _bsql_pos, decode value, advance _bsql_pos.
fn gen_pg_raw_column_decode(field_name: &proc_macro2::Ident, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_pg_raw_nullable_decode(field_name, inner)
    } else {
        gen_pg_raw_not_null_decode(field_name, rust_type)
    }
}

/// NOT NULL decode for raw-bytes path.
fn gen_pg_raw_not_null_decode(field_name: &proc_macro2::Ident, rust_type: &str) -> TokenStream {
    let field_str = field_name.to_string();
    match rust_type {
        "bool" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "bool", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL bool")),
                ));
            } else {
                let _v = _bsql_data[_bsql_pos] != 0;
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        "i16" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "i16", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL i16")),
                ));
            } else {
                let _v = i16::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                ]);
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        "i32" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "i32", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL i32")),
                ));
            } else {
                let _v = i32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]);
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        "i64" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "i64", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL i64")),
                ));
            } else {
                let _v = i64::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                    _bsql_data[_bsql_pos + 4], _bsql_data[_bsql_pos + 5],
                    _bsql_data[_bsql_pos + 6], _bsql_data[_bsql_pos + 7],
                ]);
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        "f32" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "f32", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL f32")),
                ));
            } else {
                let _v = f32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]);
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        "f64" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "f64", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL f64")),
                ));
            } else {
                let _v = f64::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                    _bsql_data[_bsql_pos + 4], _bsql_data[_bsql_pos + 5],
                    _bsql_data[_bsql_pos + 6], _bsql_data[_bsql_pos + 7],
                ]);
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        // Zero-copy: borrow &str from raw bytes
        "String" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "&str", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL &str")),
                ));
            } else {
                let _end = _bsql_pos + _bsql_len as usize;
                let _v = ::bsql_core::driver::decode_str(&_bsql_data[_bsql_pos.._end])
                    .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #field_str, "&str", "invalid UTF-8", e,
                    ))?;
                _bsql_pos = _end;
                _v
            };
        },
        // Zero-copy: borrow &[u8] from raw bytes
        "Vec<u8>" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "&[u8]", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL &[u8]")),
                ));
            } else {
                let _end = _bsql_pos + _bsql_len as usize;
                let _v = &_bsql_data[_bsql_pos.._end];
                _bsql_pos = _end;
                _v
            };
        },
        "u32" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "u32", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL u32")),
                ));
            } else {
                let _v = i32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]) as u32;
                _bsql_pos += _bsql_len as usize;
                _v
            };
        },
        "()" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            if _bsql_len > 0 { _bsql_pos += _bsql_len as usize; }
            let #field_name = ();
        },
        // Zero-copy array decode: borrow &str / &[u8] from wire buffer
        "Vec<String>" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "text[]", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL text[]")),
                ));
            } else {
                let _end = _bsql_pos + _bsql_len as usize;
                let _v = ::bsql_core::driver::decode_array_str_borrowed(&_bsql_data[_bsql_pos.._end])
                    .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #field_str, "text[]", "invalid array data", e,
                    ))?;
                _bsql_pos = _end;
                _v
            };
        },
        "Vec<Vec<u8>>" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name = if _bsql_len < 0 {
                return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "bytea[]", "NULL or invalid data",
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL bytea[]")),
                ));
            } else {
                let _end = _bsql_pos + _bsql_len as usize;
                let _v = ::bsql_core::driver::decode_array_bytea_borrowed(&_bsql_data[_bsql_pos.._end])
                    .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #field_str, "bytea[]", "invalid array data", e,
                    ))?;
                _bsql_pos = _end;
                _v
            };
        },
        // Feature-gated types: extract raw column slice and delegate to codec functions
        _ => gen_pg_raw_feature_decode(field_name, rust_type),
    }
}

/// Nullable decode for raw-bytes path.
fn gen_pg_raw_nullable_decode(field_name: &proc_macro2::Ident, inner_type: &str) -> TokenStream {
    let field_str = field_name.to_string();
    match inner_type {
        "bool" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<bool> = if _bsql_len < 0 { None } else {
                let _v = _bsql_data[_bsql_pos] != 0;
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        "i16" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<i16> = if _bsql_len < 0 { None } else {
                let _v = i16::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                ]);
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        "i32" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<i32> = if _bsql_len < 0 { None } else {
                let _v = i32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]);
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        "i64" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<i64> = if _bsql_len < 0 { None } else {
                let _v = i64::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                    _bsql_data[_bsql_pos + 4], _bsql_data[_bsql_pos + 5],
                    _bsql_data[_bsql_pos + 6], _bsql_data[_bsql_pos + 7],
                ]);
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        "f32" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<f32> = if _bsql_len < 0 { None } else {
                let _v = f32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]);
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        "f64" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<f64> = if _bsql_len < 0 { None } else {
                let _v = f64::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                    _bsql_data[_bsql_pos + 4], _bsql_data[_bsql_pos + 5],
                    _bsql_data[_bsql_pos + 6], _bsql_data[_bsql_pos + 7],
                ]);
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        // Zero-copy: Option<&str>
        "String" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<&str> = if _bsql_len < 0 { None } else {
                let _end = _bsql_pos + _bsql_len as usize;
                let _v = ::bsql_core::driver::decode_str(&_bsql_data[_bsql_pos.._end])
                    .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                        #field_str, "&str", "invalid UTF-8", e,
                    ))?;
                _bsql_pos = _end;
                Some(_v)
            };
        },
        // Zero-copy: Option<&[u8]>
        "Vec<u8>" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<&[u8]> = if _bsql_len < 0 { None } else {
                let _end = _bsql_pos + _bsql_len as usize;
                let _v = &_bsql_data[_bsql_pos.._end];
                _bsql_pos = _end;
                Some(_v)
            };
        },
        "u32" => quote! {
            let _bsql_len = i32::from_be_bytes([
                _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
            ]);
            _bsql_pos += 4;
            let #field_name: Option<u32> = if _bsql_len < 0 { None } else {
                let _v = i32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]) as u32;
                _bsql_pos += _bsql_len as usize;
                Some(_v)
            };
        },
        // Feature-gated nullable types
        _ => gen_pg_raw_nullable_feature_decode(field_name, inner_type),
    }
}

/// Feature-gated NOT NULL decode for raw-bytes path.
///
/// Extracts the raw column bytes inline and calls the same decode functions.
fn gen_pg_raw_feature_decode(field_name: &proc_macro2::Ident, rust_type: &str) -> TokenStream {
    let field_str = field_name.to_string();
    // Read length and extract raw slice
    let read_raw = quote! {
        let _bsql_len = i32::from_be_bytes([
            _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
            _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
        ]);
        _bsql_pos += 4;
        let _bsql_raw: &[u8] = if _bsql_len < 0 {
            &[]
        } else {
            let _end = _bsql_pos + _bsql_len as usize;
            let _v = &_bsql_data[_bsql_pos.._end];
            _bsql_pos = _end;
            _v
        };
    };

    let decode_expr = match rust_type {
        "::uuid::Uuid" | "uuid::Uuid" => quote! {
            let #field_name = match ::bsql_core::driver::decode_uuid_type(_bsql_raw) {
                Ok(v) => v,
                Err(e) => return Err(::bsql_core::error::DecodeError::with_source(
                    #field_str, "uuid", "invalid data", e,
                )),
            };
        },
        _ => {
            // For all other feature-gated types, construct a temporary PgDataRow
            // from the raw column slice. This is a fallback that still benefits
            // from skipping the SmallVec pre-scan of ALL columns.
            // We re-use the existing for_each decode via PgDataRow with a single column.
            let col_idx_lit = 0usize;
            let decode = gen_not_null_decode(col_idx_lit, rust_type);
            quote! {
                let #field_name = {
                    // Build a single-column DataRow for the decode function
                    let mut _bsql_tmp = Vec::with_capacity(6 + _bsql_raw.len());
                    _bsql_tmp.extend_from_slice(&1i16.to_be_bytes());
                    _bsql_tmp.extend_from_slice(&(_bsql_raw.len() as i32).to_be_bytes());
                    _bsql_tmp.extend_from_slice(_bsql_raw);
                    let _bsql_row = ::bsql_core::driver::PgDataRow::new(&_bsql_tmp)
                        .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                            #field_str, "decode", "invalid data", e,
                        ))?;
                    let row = &_bsql_row;
                    #decode
                };
            }
        }
    };

    quote! {
        #read_raw
        #decode_expr
    }
}

/// Feature-gated nullable decode for raw-bytes path.
fn gen_pg_raw_nullable_feature_decode(
    field_name: &proc_macro2::Ident,
    inner_type: &str,
) -> TokenStream {
    let field_str = field_name.to_string();

    match inner_type {
        "::uuid::Uuid" | "uuid::Uuid" => quote! {
            let #field_name = {
                let _bsql_len = i32::from_be_bytes([
                    _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                    _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                ]);
                _bsql_pos += 4;
                if _bsql_len < 0 {
                    None
                } else {
                    let _end = _bsql_pos + _bsql_len as usize;
                    let _raw = &_bsql_data[_bsql_pos.._end];
                    _bsql_pos = _end;
                    Some(match ::bsql_core::driver::decode_uuid_type(_raw) {
                        Ok(v) => v,
                        Err(e) => return Err(::bsql_core::error::DecodeError::with_source(
                            #field_str, "uuid", "invalid data", e,
                        )),
                    })
                }
            };
        },
        _ => {
            // Fallback for all other feature-gated nullable types
            let col_idx_lit = 0usize;
            let decode = gen_nullable_decode(col_idx_lit, inner_type);
            quote! {
                let #field_name = {
                    let _bsql_len = i32::from_be_bytes([
                        _bsql_data[_bsql_pos], _bsql_data[_bsql_pos + 1],
                        _bsql_data[_bsql_pos + 2], _bsql_data[_bsql_pos + 3],
                    ]);
                    _bsql_pos += 4;
                    if _bsql_len < 0 {
                        None
                    } else {
                        let _end = _bsql_pos + _bsql_len as usize;
                        let _raw = &_bsql_data[_bsql_pos.._end];
                        _bsql_pos = _end;
                        // Build a single-column DataRow for the decode function
                        let mut _bsql_tmp = Vec::with_capacity(6 + _raw.len());
                        _bsql_tmp.extend_from_slice(&1i16.to_be_bytes());
                        _bsql_tmp.extend_from_slice(&(_raw.len() as i32).to_be_bytes());
                        _bsql_tmp.extend_from_slice(_raw);
                        let _bsql_row = ::bsql_core::driver::PgDataRow::new(&_bsql_tmp)
                            .map_err(|e| ::bsql_core::error::DecodeError::with_source(
                                #field_str, "decode", "invalid data", e,
                            ))?;
                        let row = &_bsql_row;
                        #decode
                    }
                };
            }
        }
    }
}

/// Generate a runtime column-count bounds check for `Row`-based decode paths.
///
/// If the row has fewer columns than expected (e.g., schema drift between
/// compile-time and runtime), returns `Err(DecodeError::column_count(...))`
/// instead of panicking with an index-out-of-bounds.
///
/// The variable `row` must be in scope when the generated code runs.
fn gen_column_count_check(validation: &ValidationResult) -> TokenStream {
    let expected = validation.columns.len();
    if expected == 0 {
        return TokenStream::new();
    }
    quote! {
        if row.column_count() < #expected {
            return Err(::bsql_core::error::DecodeError::column_count(
                #expected,
                row.column_count(),
            ));
        }
    }
}

/// Generate a runtime column-count bounds check for the raw-bytes (`_bsql_data`) path.
///
/// Reads the i16 num_cols from the DataRow header and checks it against the
/// expected column count. Returns `Err` on mismatch instead of panicking.
fn gen_raw_column_count_check(validation: &ValidationResult) -> TokenStream {
    let expected = validation.columns.len() as i16;
    if expected == 0 {
        return TokenStream::new();
    }
    quote! {
        {
            let _bsql_num_cols = i16::from_be_bytes([_bsql_data[0], _bsql_data[1]]);
            if _bsql_num_cols < #expected {
                return Err(::bsql_core::error::DecodeError::column_count(
                    #expected as usize,
                    _bsql_num_cols as usize,
                ));
            }
        }
    }
}

/// Generate row field decoding using typed getters from bsql_driver_postgres::Row.
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

/// Like `gen_row_decode` but for query_as! — decodes into named bindings first,
/// then returns field names for struct construction. This produces clearer rustc
/// error messages when nullable columns don't match struct field types.
///
/// Generated code:
/// ```ignore
/// let title: Option<String> = ...; // bsql: nullable column
/// let id: i32 = ...;
/// Ok(MyStruct { id, title })       // rustc error here is clearer
/// ```
fn gen_query_as_decode_bindings(validation: &ValidationResult) -> (TokenStream, TokenStream) {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let mut bindings = Vec::new();
    let mut field_names = Vec::new();

    for (i, name) in deduped_names.iter().enumerate() {
        let field_name = format_ident!("{}", name);
        let col = &validation.columns[i];
        let decode_expr = gen_column_decode(i, &col.rust_type);
        let rust_type = parse_result_type(&col.rust_type);

        bindings.push(quote! {
            let #field_name: #rust_type = #decode_expr;
        });
        field_names.push(quote! { #field_name });
    }

    let bindings_ts = quote! { #(#bindings)* };
    let fields_ts = quote! { #(#field_names),* };
    (bindings_ts, fields_ts)
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

/// Generate a decode error for a NOT NULL column that received NULL/invalid data.
///
/// Uses `DecodeError::with_source` since `DecodeError`'s `source` field is
/// `pub(crate)` and cannot be set from user code via struct literal.
fn gen_not_null_decode_error(col_idx: &str, type_name: &str) -> TokenStream {
    quote! {
        ::bsql_core::error::DecodeError::with_source(
            #col_idx,
            #type_name,
            "NULL or invalid data",
            ::std::io::Error::new(::std::io::ErrorKind::InvalidData, concat!("expected NOT NULL ", #type_name)),
        )
    }
}

/// Generate decode for a NOT NULL column.
///
/// Uses `.ok_or_else(|| ...)` instead of `.unwrap_or_default()` so that
/// corrupt/invalid data is propagated as an error rather than silently
/// returning zero/false/"".
fn gen_not_null_decode(idx: usize, rust_type: &str) -> TokenStream {
    let col_idx = idx.to_string();
    match rust_type {
        "bool" => {
            let err = gen_not_null_decode_error(&col_idx, "bool");
            quote! { row.get_bool(#idx).ok_or_else(|| #err)? }
        }
        "i16" => {
            let err = gen_not_null_decode_error(&col_idx, "i16");
            quote! { row.get_i16(#idx).ok_or_else(|| #err)? }
        }
        "i32" => {
            let err = gen_not_null_decode_error(&col_idx, "i32");
            quote! { row.get_i32(#idx).ok_or_else(|| #err)? }
        }
        "i64" => {
            let err = gen_not_null_decode_error(&col_idx, "i64");
            quote! { row.get_i64(#idx).ok_or_else(|| #err)? }
        }
        "f32" => {
            let err = gen_not_null_decode_error(&col_idx, "f32");
            quote! { row.get_f32(#idx).ok_or_else(|| #err)? }
        }
        "f64" => {
            let err = gen_not_null_decode_error(&col_idx, "f64");
            quote! { row.get_f64(#idx).ok_or_else(|| #err)? }
        }
        "String" => {
            let err = gen_not_null_decode_error(&col_idx, "String");
            quote! { row.get_str(#idx).ok_or_else(|| #err)?.to_owned() }
        }
        "Vec<u8>" => {
            let err = gen_not_null_decode_error(&col_idx, "Vec<u8>");
            quote! { row.get_bytes(#idx).ok_or_else(|| #err)?.to_vec() }
        }
        "u32" => {
            // OID type: decode as i32 then cast
            let err = gen_not_null_decode_error(&col_idx, "u32");
            quote! { row.get_i32(#idx).ok_or_else(|| #err)? as u32 }
        }
        "()" => quote! { () },
        _ => gen_feature_gated_decode(idx, rust_type),
    }
}

/// Wrap a fallible decode expression in a match that converts `Err(DriverError)`
/// to `Err(BsqlError::Decode)` instead of panicking.
///
/// Uses `DecodeError::with_source` to construct the error, since `DecodeError`'s
/// `source` field is `pub(crate)` and cannot be set from user code via struct literal.
fn gen_decode_match(idx: usize, type_name: &str, decode_expr: TokenStream) -> TokenStream {
    let col_idx = idx.to_string();
    quote! {
        match #decode_expr {
            Ok(v) => v,
            Err(e) => return Err(::bsql_core::error::DecodeError::with_source(
                #col_idx,
                #type_name,
                "invalid data",
                e,
            )),
        }
    }
}

/// Generate decode for a feature-gated type (uuid, time, chrono, decimal).
/// Uses `row.get_raw(idx)` + the appropriate decode function from bsql_core::driver.
///
/// Returns a `BsqlError::Decode` on failure instead of panicking, so the
/// generated code propagates errors via `?` in the enclosing `BsqlResult`.
fn gen_feature_gated_decode(idx: usize, rust_type: &str) -> TokenStream {
    match rust_type {
        "::uuid::Uuid" | "uuid::Uuid" => gen_decode_match(
            idx,
            "uuid",
            quote! {
                ::bsql_core::driver::decode_uuid_type(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        "::time::OffsetDateTime" | "time::OffsetDateTime" => gen_decode_match(
            idx,
            "timestamptz",
            quote! {
                ::bsql_core::driver::decode_timestamptz_time(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        // TIMESTAMP (without tz) -> PrimitiveDateTime: same binary format as timestamptz,
        // strip the UTC offset to get date + time without timezone
        "::time::PrimitiveDateTime" | "time::PrimitiveDateTime" => gen_decode_match(
            idx,
            "timestamp",
            quote! {
                ::bsql_core::driver::decode_timestamptz_time(
                    row.get_raw(#idx).unwrap_or_default()
                ).map(|odt| ::time::PrimitiveDateTime::new(odt.date(), odt.time()))
            },
        ),
        "::time::Date" | "time::Date" => gen_decode_match(
            idx,
            "date",
            quote! {
                ::bsql_core::driver::decode_date_time(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        "::time::Time" | "time::Time" => gen_decode_match(
            idx,
            "time",
            quote! {
                ::bsql_core::driver::decode_time_time(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        "::chrono::DateTime<::chrono::Utc>"
        | "::chrono::DateTime<chrono::Utc>"
        | "chrono::DateTime<chrono::Utc>"
        | "chrono::DateTime<Utc>" => gen_decode_match(
            idx,
            "timestamptz",
            quote! {
                ::bsql_core::driver::decode_timestamptz_chrono(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        // TIMESTAMP (without tz) -> NaiveDateTime: same binary format as timestamptz,
        // strip the UTC offset via .naive_utc()
        "::chrono::NaiveDateTime" | "chrono::NaiveDateTime" => gen_decode_match(
            idx,
            "timestamp",
            quote! {
                ::bsql_core::driver::decode_timestamptz_chrono(
                    row.get_raw(#idx).unwrap_or_default()
                ).map(|dt| dt.naive_utc())
            },
        ),
        "::chrono::NaiveDate" | "chrono::NaiveDate" => gen_decode_match(
            idx,
            "date",
            quote! {
                ::bsql_core::driver::decode_date_chrono(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        "::chrono::NaiveTime" | "chrono::NaiveTime" => gen_decode_match(
            idx,
            "time",
            quote! {
                ::bsql_core::driver::decode_time_chrono(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
        "::rust_decimal::Decimal" | "rust_decimal::Decimal" => gen_decode_match(
            idx,
            "numeric",
            quote! {
                ::bsql_core::driver::decode_numeric_decimal(
                    row.get_raw(#idx).unwrap_or_default()
                )
            },
        ),
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
        // Feature-gated array types: decode each element using the scalar decode fn.
        // For timestamp/date/time arrays, we reuse the existing scalar decode functions
        // by converting each i64 element back to an 8-byte big-endian buffer.
        "Vec<::time::OffsetDateTime>" | "Vec<time::OffsetDateTime>" => gen_decode_match(
            idx,
            "timestamptz[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_i64(raw).and_then(|micros_vec| {
                    let mut out = Vec::with_capacity(micros_vec.len());
                    for micros in micros_vec {
                        let buf = micros.to_be_bytes();
                        out.push(::bsql_core::driver::decode_timestamptz_time(&buf)?);
                    }
                    Ok(out)
                })
            } },
        ),
        "Vec<::time::PrimitiveDateTime>" | "Vec<time::PrimitiveDateTime>" => gen_decode_match(
            idx,
            "timestamp[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_i64(raw).and_then(|micros_vec| {
                    let mut out = Vec::with_capacity(micros_vec.len());
                    for micros in micros_vec {
                        let buf = micros.to_be_bytes();
                        let odt = ::bsql_core::driver::decode_timestamptz_time(&buf)?;
                        out.push(::time::PrimitiveDateTime::new(odt.date(), odt.time()));
                    }
                    Ok(out)
                })
            } },
        ),
        "Vec<::time::Date>" | "Vec<time::Date>" => gen_decode_match(
            idx,
            "date[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_i32(raw).and_then(|days_vec| {
                    let mut out = Vec::with_capacity(days_vec.len());
                    for days in days_vec {
                        let buf = days.to_be_bytes();
                        out.push(::bsql_core::driver::decode_date_time(&buf)?);
                    }
                    Ok(out)
                })
            } },
        ),
        "Vec<::time::Time>" | "Vec<time::Time>" => gen_decode_match(
            idx,
            "time[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_i64(raw).and_then(|micros_vec| {
                    let mut out = Vec::with_capacity(micros_vec.len());
                    for micros in micros_vec {
                        let buf = micros.to_be_bytes();
                        out.push(::bsql_core::driver::decode_time_time(&buf)?);
                    }
                    Ok(out)
                })
            } },
        ),
        "Vec<::uuid::Uuid>" | "Vec<uuid::Uuid>" => gen_decode_match(
            idx,
            "uuid[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_bytea(raw).and_then(|elements| {
                    let mut out = Vec::with_capacity(elements.len());
                    for bytes in &elements {
                        out.push(::bsql_core::driver::decode_uuid_type(bytes)?);
                    }
                    Ok(out)
                })
            } },
        ),
        "Vec<::rust_decimal::Decimal>" | "Vec<rust_decimal::Decimal>" => gen_decode_match(
            idx,
            "numeric[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_bytea(raw).and_then(|elements| {
                    let mut out = Vec::with_capacity(elements.len());
                    for bytes in &elements {
                        out.push(::bsql_core::driver::decode_numeric_decimal(bytes)?);
                    }
                    Ok(out)
                })
            } },
        ),
        "Vec<::chrono::DateTime<::chrono::Utc>>" | "Vec<chrono::DateTime<chrono::Utc>>" => {
            gen_decode_match(
                idx,
                "timestamptz[]",
                quote! { {
                    let raw = row.get_raw(#idx).unwrap_or_default();
                    ::bsql_core::driver::decode_array_i64(raw).and_then(|micros_vec| {
                        let mut out = Vec::with_capacity(micros_vec.len());
                        for micros in micros_vec {
                            let buf = micros.to_be_bytes();
                            out.push(::bsql_core::driver::decode_timestamptz_chrono(&buf)?);
                        }
                        Ok(out)
                    })
                } },
            )
        }
        "Vec<::chrono::NaiveDateTime>" | "Vec<chrono::NaiveDateTime>" => gen_decode_match(
            idx,
            "timestamp[]",
            quote! { {
                let raw = row.get_raw(#idx).unwrap_or_default();
                ::bsql_core::driver::decode_array_i64(raw).and_then(|micros_vec| {
                    let mut out = Vec::with_capacity(micros_vec.len());
                    for micros in micros_vec {
                        let buf = micros.to_be_bytes();
                        let dt = ::bsql_core::driver::decode_timestamptz_chrono(&buf)?;
                        out.push(dt.naive_utc());
                    }
                    Ok(out)
                })
            } },
        ),
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
///
/// For reference-typed parameters (`&str`, `&[i32]`, etc.) we emit a
/// `let name: &T = &name;` rebinding so that owned types like `String`
/// and `Vec<T>` auto-deref into the expected reference.
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
///
/// This allows callers to pass `String` where `&str` is expected, or
/// `Vec<i32>` where `&[i32]` is expected — the rebinding triggers Rust's
/// auto-deref coercion.
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

fn stream_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlStream_{}", &parsed.statement_name)
}

/// Rust keywords (2024 edition) that cannot be used as bare identifiers.
const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
    "pub", "raw", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true",
    "type", "unsafe", "use", "where", "while", "yield",
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
            rewritten_sql: None,
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
            code_str.contains("fn fetch_all"),
            "missing fetch_all method: {code_str}"
        );
        assert!(
            code_str.contains("fn fetch_all"),
            "missing fetch_all (renamed to fetch): {code_str}"
        );
        assert!(
            code_str.contains("fetch_optional"),
            "missing fetch_optional: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
        assert!(
            !code_str.contains("fn run"),
            "run alias should be removed: {code_str}"
        );
        // get and stream aliases should NOT be generated on the executor
        // (fn get on BsqlRows/BsqlSingleRef is fine — check for executor method signature)
        assert!(
            !code_str.contains("fn get (self , executor"),
            "get alias should be removed: {code_str}"
        );
        assert!(
            !code_str.contains("fn stream"),
            "stream alias should be removed: {code_str}"
        );
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
    fn select_uses_sql_precomputed_readonly() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("Sql :: with_parse"),
            "SELECT should use Sql::with_parse: {code_str}"
        );
        assert!(
            code_str.contains(". query"),
            "SELECT should use .query(): {code_str}"
        );
    }

    #[test]
    fn insert_uses_execute_not_readonly() {
        let parsed = parse_query("INSERT INTO t (a) VALUES ($a: i32) RETURNING id").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("Sql :: with_parse"),
            "INSERT RETURNING should use Sql::with_parse: {code_str}"
        );
        assert!(
            !code_str.contains("query_raw"),
            "INSERT RETURNING should NOT use query_raw: {code_str}"
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

    // --- Option<T> lifetime injection ---

    #[test]
    fn inject_lifetime_option_str() {
        let ts = inject_lifetime("Option<&str>");
        let s = ts.to_string();
        assert!(
            s.contains("'_bsql"),
            "Option<&str> inner ref needs lifetime: {s}"
        );
        assert!(s.contains("Option"), "should still be Option: {s}");
    }

    #[test]
    fn inject_lifetime_option_i32_no_lifetime() {
        let ts = inject_lifetime("Option<i32>");
        let s = ts.to_string();
        assert!(
            !s.contains("'_bsql"),
            "Option<i32> should have no lifetime: {s}"
        );
        assert!(s.contains("Option"), "should still be Option: {s}");
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

    #[test]
    fn sanitize_raw_keyword() {
        assert_eq!(sanitize_param_name("raw"), "raw_");
        assert_eq!(sanitize_column_name("raw", 0), "raw_");
    }

    #[test]
    fn not_null_decode_uses_ok_or_else() {
        let parsed = parse_query("SELECT id FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Should NOT use unwrap_or_default — should use ok_or_else
        assert!(
            !code_str.contains("unwrap_or_default"),
            "should not use unwrap_or_default for NOT NULL decode: {code_str}"
        );
        assert!(
            code_str.contains("ok_or_else"),
            "should use ok_or_else for NOT NULL decode: {code_str}"
        );
    }

    #[test]
    fn timestamp_decode_has_primitive_date_time() {
        let parsed = parse_query("SELECT ts FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("ts", "::time::PrimitiveDateTime")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("decode_timestamptz_time"),
            "PrimitiveDateTime should use timestamptz decode: {code_str}"
        );
        assert!(
            code_str.contains("PrimitiveDateTime"),
            "should reference PrimitiveDateTime: {code_str}"
        );
    }

    #[test]
    fn timestamp_decode_has_naive_date_time() {
        let parsed = parse_query("SELECT ts FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("ts", "::chrono::NaiveDateTime")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("decode_timestamptz_chrono"),
            "NaiveDateTime should use timestamptz decode: {code_str}"
        );
        assert!(
            code_str.contains("naive_utc"),
            "should convert to naive_utc: {code_str}"
        );
    }

    // --- fetch_ref tests ---

    #[test]
    fn select_returns_owned_structs_no_zero_copy() {
        let parsed = parse_query("SELECT id, name FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Zero-copy wrappers are removed — no BsqlRows, no BsqlSingleRef
        assert!(
            !code_str.contains("BsqlRows_"),
            "BsqlRows should be removed: {code_str}"
        );
        assert!(
            !code_str.contains("BsqlSingleRef_"),
            "BsqlSingleRef should be removed: {code_str}"
        );
        assert!(
            !code_str.contains("fetch_ref"),
            "fetch_ref should not exist: {code_str}"
        );
        // All methods return owned types
        assert!(
            code_str.contains("fn fetch_all"),
            "missing fetch_all method: {code_str}"
        );
        assert!(
            code_str.contains("fn fetch_one"),
            "missing fetch_one method: {code_str}"
        );
        assert!(
            code_str.contains("fn fetch_optional"),
            "missing fetch_optional method: {code_str}"
        );
        // fetch returns Vec, not BsqlRows
        assert!(
            code_str.contains("Vec <"),
            "fetch should return Vec: {code_str}"
        );
    }

    #[test]
    fn fetch_ref_not_generated_for_insert() {
        let parsed = parse_query("INSERT INTO t (a) VALUES ($a: i32) RETURNING id").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // fetch_ref is only for SELECT (read-only) queries
        assert!(
            !code_str.contains("fetch_ref"),
            "INSERT should NOT have fetch_ref: {code_str}"
        );
    }

    #[test]
    fn fetch_ref_not_generated_for_execute_only() {
        let parsed = parse_query("DELETE FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("fetch_ref"),
            "execute-only should NOT have fetch_ref: {code_str}"
        );
        assert!(
            !code_str.contains("BsqlRows_"),
            "execute-only should NOT have BsqlRows: {code_str}"
        );
    }

    // --- Fix-5: column count bounds check ---

    #[test]
    fn generated_code_includes_column_count_check() {
        let parsed = parse_query("SELECT id, name FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // The generated code must contain column_count checks before decode
        assert!(
            code_str.contains("column_count"),
            "generated code should include column_count bounds check: {code_str}"
        );
        // Must reference DecodeError::column_count
        assert!(
            code_str.contains("DecodeError"),
            "generated code should reference DecodeError for column mismatch: {code_str}"
        );
    }

    #[test]
    fn column_count_check_uses_correct_count() {
        let parsed = parse_query("SELECT a, b, c FROM t WHERE 1 = $x: i32").unwrap();
        let validation = make_validation(vec![col("a", "i32"), col("b", "i32"), col("c", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // 3 columns — the check must reference 3usize
        assert!(
            code_str.contains("3usize") || code_str.contains("3 usize"),
            "column_count check should use expected=3: {code_str}"
        );
    }

    #[test]
    fn no_column_count_check_for_execute_only() {
        // An UPDATE with no RETURNING has no columns — no check needed
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("column_count"),
            "execute-only query should NOT have column_count check: {code_str}"
        );
    }

    // --- gen_column_count_check unit tests ---

    #[test]
    fn gen_column_count_check_zero_columns_returns_empty() {
        let validation = make_validation(vec![]);
        let check = gen_column_count_check(&validation);
        assert!(
            check.is_empty(),
            "0 columns should produce empty check: {}",
            check
        );
    }

    #[test]
    fn gen_column_count_check_one_column() {
        let validation = make_validation(vec![col("id", "i32")]);
        let check = gen_column_count_check(&validation);
        let code = check.to_string();
        assert!(
            code.contains("column_count"),
            "1 column should produce a check: {code}"
        );
        assert!(
            code.contains("1usize") || code.contains("1 usize"),
            "should check for 1 column: {code}"
        );
    }

    #[test]
    fn gen_column_count_check_ten_columns() {
        let cols: Vec<ColumnInfo> = (0..10).map(|i| col(&format!("c{i}"), "i32")).collect();
        let validation = make_validation(cols);
        let check = gen_column_count_check(&validation);
        let code = check.to_string();
        assert!(
            code.contains("10usize") || code.contains("10 usize"),
            "should check for 10 columns: {code}"
        );
    }

    #[test]
    fn gen_column_count_check_references_decode_error() {
        let validation = make_validation(vec![col("a", "i32"), col("b", "String")]);
        let check = gen_column_count_check(&validation);
        let code = check.to_string();
        assert!(
            code.contains("DecodeError :: column_count"),
            "should reference DecodeError::column_count: {code}"
        );
    }

    // --- query_as! codegen tests ---

    #[test]
    fn query_as_uses_target_type_not_anonymous_struct() {
        let parsed = parse_query("SELECT id, name FROM users WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let target_type: syn::Path = syn::parse_str("User").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        // Should reference User { ... } for struct construction
        assert!(
            code_str.contains("User"),
            "should reference target type User: {code_str}"
        );
        // Should NOT generate BsqlResult_ anonymous struct
        assert!(
            !code_str.contains("BsqlResult_"),
            "should not generate anonymous result struct: {code_str}"
        );
        // Should NOT generate BsqlRows_ or BsqlSingleRef_ wrappers
        assert!(
            !code_str.contains("BsqlRows_"),
            "should not generate rows struct: {code_str}"
        );
        assert!(
            !code_str.contains("BsqlSingleRef_"),
            "should not generate single ref struct: {code_str}"
        );
    }

    #[test]
    fn query_as_generates_fetch_methods() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let target_type: syn::Path = syn::parse_str("MyRow").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        assert!(
            code_str.contains("fetch_one"),
            "missing fetch_one: {code_str}"
        );
        assert!(
            code_str.contains("fn fetch_all"),
            "missing fetch_all: {code_str}"
        );
        assert!(
            code_str.contains("fetch_optional"),
            "missing fetch_optional: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
        assert!(
            !code_str.contains("fn run"),
            "run alias should be removed: {code_str}"
        );
        assert!(code_str.contains("fn defer"), "missing defer: {code_str}");
    }

    #[test]
    fn query_as_with_module_path() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let target_type: syn::Path = syn::parse_str("crate::models::User").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        assert!(
            code_str.contains("crate :: models :: User"),
            "should use fully qualified path: {code_str}"
        );
    }

    #[test]
    fn query_as_no_columns_has_no_fetch() {
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let target_type: syn::Path = syn::parse_str("User").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("fetch_one"),
            "execute-only should not have fetch_one: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn query_as_has_column_count_check() {
        let parsed = parse_query("SELECT id, name FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let target_type: syn::Path = syn::parse_str("User").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        assert!(
            code_str.contains("column_count"),
            "should have column count check: {code_str}"
        );
    }

    #[test]
    fn query_as_nullable_column() {
        let parsed = parse_query("SELECT email FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("email", "Option<String>")]);
        let target_type: syn::Path = syn::parse_str("UserEmail").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        // Should reference the target type, not anonymous struct
        assert!(
            code_str.contains("UserEmail"),
            "should use target type: {code_str}"
        );
        // Should have nullable decode (get_str that returns Option)
        assert!(
            code_str.contains("get_str"),
            "should decode String column: {code_str}"
        );
    }

    #[test]
    fn query_as_injects_limit_2() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let target_type: syn::Path = syn::parse_str("Row").unwrap();
        let code = generate_query_as_code(&parsed, &validation, &target_type);
        let code_str = code.to_string();

        assert!(
            code_str.contains("LIMIT 2"),
            "missing LIMIT 2 in query_as fetch_one: {code_str}"
        );
    }

    // --- auto-deref coercion tests ---

    #[test]
    fn constructor_coerces_ref_str_param() {
        let parsed = parse_query("SELECT id FROM t WHERE name = $name: &str").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Should contain `let name : & str = & name ;`
        assert!(
            code_str.contains("let name : & str = & name"),
            "missing auto-deref coercion for &str: {code_str}"
        );
    }

    #[test]
    fn constructor_coerces_ref_slice_param() {
        let parsed = parse_query("SELECT id FROM t WHERE id = ANY($ids: &[i32])").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Should contain `let ids : & [i32] = & ids ;`
        assert!(
            code_str.contains("let ids : & [i32] = & ids"),
            "missing auto-deref coercion for &[i32]: {code_str}"
        );
    }

    #[test]
    fn constructor_no_coercion_for_owned_types() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Should NOT contain a let coercion for i32
        assert!(
            !code_str.contains("let id : i32 = & id"),
            "should not coerce owned type i32: {code_str}"
        );
    }

    #[test]
    fn gen_ref_coercions_empty_for_no_refs() {
        let params = vec![crate::parse::Param {
            name: "id".into(),
            rust_type: "i32".into(),
            position: 1,
        }];
        let coercions = gen_ref_coercions(&params);
        assert!(
            coercions.is_empty(),
            "no coercions expected for i32: {}",
            coercions
        );
    }

    #[test]
    fn gen_ref_coercions_str_and_slice() {
        let params = vec![
            crate::parse::Param {
                name: "name".into(),
                rust_type: "&str".into(),
                position: 1,
            },
            crate::parse::Param {
                name: "ids".into(),
                rust_type: "&[i32]".into(),
                position: 2,
            },
            crate::parse::Param {
                name: "age".into(),
                rust_type: "i32".into(),
                position: 3,
            },
        ];
        let coercions = gen_ref_coercions(&params);
        let code = coercions.to_string();
        assert!(
            code.contains("let name : & str = & name"),
            "missing &str coercion: {code}"
        );
        assert!(
            code.contains("let ids : & [i32] = & ids"),
            "missing &[i32] coercion: {code}"
        );
        assert!(!code.contains("let age"), "should not coerce i32: {code}");
    }

    // --- Compile-time Parse+Describe bytes ---

    #[test]
    fn make_stmt_name_bytes_format() {
        let name = make_stmt_name_bytes(0xDEADBEEF12345678);
        assert_eq!(&name[0..2], b"s_");
        assert_eq!(&name[2..], b"deadbeef12345678");
        assert_eq!(name.len(), 18);
    }

    #[test]
    fn make_stmt_name_bytes_zero() {
        let name = make_stmt_name_bytes(0);
        assert_eq!(&name, b"s_0000000000000000");
    }

    #[test]
    fn make_stmt_name_bytes_max() {
        let name = make_stmt_name_bytes(u64::MAX);
        assert_eq!(&name, b"s_ffffffffffffffff");
    }

    #[test]
    fn gen_parse_describe_bytes_structure() {
        let sql = "SELECT id FROM users WHERE id = $1";
        let hash = bsql_core::rapid_hash_str(sql);
        let param_oids = vec![23u32]; // int4

        let bytes = gen_parse_describe_bytes(sql, hash, &param_oids);

        // Parse message starts with 'P'
        assert_eq!(bytes[0], b'P', "first byte must be Parse type 'P'");

        // Find NUL after statement name
        let name_end = bytes[5..].iter().position(|&b| b == 0).unwrap() + 5;
        let stmt_name = &bytes[5..name_end];
        assert_eq!(stmt_name.len(), 18, "statement name must be 18 bytes");
        assert_eq!(&stmt_name[0..2], b"s_", "statement name must start with s_");

        // SQL text follows after NUL
        let sql_start = name_end + 1;
        let sql_end = bytes[sql_start..].iter().position(|&b| b == 0).unwrap() + sql_start;
        let embedded_sql = std::str::from_utf8(&bytes[sql_start..sql_end]).unwrap();
        assert_eq!(embedded_sql, sql, "embedded SQL must match input");

        // Param count and OID
        let param_count_pos = sql_end + 1;
        let param_count = i16::from_be_bytes([bytes[param_count_pos], bytes[param_count_pos + 1]]);
        assert_eq!(param_count, 1, "must have 1 parameter");

        let oid_pos = param_count_pos + 2;
        let oid = i32::from_be_bytes([
            bytes[oid_pos],
            bytes[oid_pos + 1],
            bytes[oid_pos + 2],
            bytes[oid_pos + 3],
        ]);
        assert_eq!(oid, 23, "OID must be 23 (int4)");

        // Describe message follows
        let describe_start = oid_pos + 4;
        assert_eq!(bytes[describe_start], b'D', "Describe must follow Parse");

        // Describe contains 'S' (statement) + same name
        let describe_body_start = describe_start + 5; // type + length
        assert_eq!(
            bytes[describe_body_start], b'S',
            "Describe must be for Statement"
        );
    }

    #[test]
    fn gen_parse_describe_bytes_no_params() {
        let sql = "SELECT 1";
        let hash = bsql_core::rapid_hash_str(sql);
        let bytes = gen_parse_describe_bytes(sql, hash, &[]);

        assert_eq!(bytes[0], b'P');
        // Find param count (after SQL NUL)
        let sql_end = bytes[5..].iter().position(|&b| b == 0).unwrap() + 5;
        let sql_text_end = bytes[sql_end + 1..].iter().position(|&b| b == 0).unwrap() + sql_end + 1;
        let param_count_pos = sql_text_end + 1;
        let param_count = i16::from_be_bytes([bytes[param_count_pos], bytes[param_count_pos + 1]]);
        assert_eq!(param_count, 0, "no params");
    }

    #[test]
    fn gen_parse_describe_bytes_multiple_params() {
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)";
        let hash = bsql_core::rapid_hash_str(sql);
        let oids = vec![23u32, 25u32, 16u32]; // int4, text, bool
        let bytes = gen_parse_describe_bytes(sql, hash, &oids);

        assert_eq!(bytes[0], b'P');
        // Verify it contains all 3 OIDs somewhere in the bytes
        // OID 23 = 0x00000017, OID 25 = 0x00000019, OID 16 = 0x00000010
        let bytes_str: Vec<String> = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let hex = bytes_str.join("");
        assert!(hex.contains("00000017"), "must contain OID 23 (int4)");
        assert!(hex.contains("00000019"), "must contain OID 25 (text)");
        assert!(hex.contains("00000010"), "must contain OID 16 (bool)");
    }
}
