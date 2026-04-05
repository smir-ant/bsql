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
    let for_each_row_struct = gen_pg_for_each_row_struct(parsed, validation);
    let executor_struct = gen_dynamic_executor_struct(parsed);
    let executor_impls = gen_dynamic_executor_impls(parsed, validation, variants);
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
        #[must_use = "query is not executed until .fetch(), .run(), or another execution method is called"]
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
    let query_method = if is_select {
        quote! { query_raw_readonly }
    } else {
        quote! { query_raw }
    };

    let sql_template = &parsed.positional_sql;
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

    // Generate the sort SQL lookup helper that caches (String, u64) per sort fragment.
    // Uses a static mutex-free DashMap-like approach: since sort enums have a small
    // finite number of variants and sort.sql() returns &'static str, we cache using
    // the pointer value as key. First call per variant allocates once; all subsequent
    // calls return (&str, u64) with zero allocation.
    //
    // NOTE (M-2): The generated code contains `unsafe { &*sql }` to convert a raw
    // pointer back to a reference. This is safe because the pointed-to String is
    // stored in a static Vec that only appends and is never deallocated. The pointer
    // remains valid for 'static. This cannot be refactored away without either
    // leaking memory (Box::leak) or adding a dependency like `once_cell::Lazy`.
    let build_sql = quote! {
        // Cache: maps sort fragment &'static str pointer -> (full SQL, hash)
        static SORT_SQL_CACHE: ::std::sync::OnceLock<::std::sync::Mutex<Vec<(usize, String, u64)>>> = ::std::sync::OnceLock::new();
        let sort_fragment: &'static str = self.sort.sql();
        let cache = SORT_SQL_CACHE.get_or_init(|| ::std::sync::Mutex::new(Vec::new()));
        let key = sort_fragment.as_ptr() as usize;
        let (sql, sql_hash) = {
            let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                (entry.1.as_str() as *const str, entry.2)
            } else {
                drop(guard);
                let built = format!("{}{}{}", #sql_prefix, sort_fragment, #sql_suffix);
                let hash = ::bsql_core::driver::hash_sql(&built);
                let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
                // Double-check after re-acquiring lock
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.as_str() as *const str, entry.2)
                } else {
                    guard.push((key, built, hash));
                    let entry = guard.last().unwrap();
                    (entry.1.as_str() as *const str, entry.2)
                }
            }
        };
        // SAFETY: the str is stored in the static Vec and never removed/moved because
        // Vec only appends and lives for 'static. The pointer remains valid.
        let sql: &str = unsafe { &*sql };
    };

    let build_limited_sql = if needs_limit {
        quote! {
            static SORT_LIMITED_SQL_CACHE: ::std::sync::OnceLock<::std::sync::Mutex<Vec<(usize, String, u64)>>> = ::std::sync::OnceLock::new();
            let sort_fragment: &'static str = self.sort.sql();
            let cache = SORT_LIMITED_SQL_CACHE.get_or_init(|| ::std::sync::Mutex::new(Vec::new()));
            let key = sort_fragment.as_ptr() as usize;
            let (sql, sql_hash) = {
                let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                    (entry.1.as_str() as *const str, entry.2)
                } else {
                    drop(guard);
                    let built = format!("{}{}{}", #sql_prefix, sort_fragment, #limited_suffix_lit);
                    let hash = ::bsql_core::driver::hash_sql(&built);
                    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = guard.iter().find(|e| e.0 == key) {
                        (entry.1.as_str() as *const str, entry.2)
                    } else {
                        guard.push((key, built, hash));
                        let entry = guard.last().unwrap();
                        (entry.1.as_str() as *const str, entry.2)
                    }
                }
            };
            let sql: &str = unsafe { &*sql };
        }
    } else {
        build_sql.clone()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);
        let row_decode = gen_row_decode(validation);
        let r_rows_name = rows_struct_name(parsed);
        let r_single_name = single_ref_struct_name(parsed);

        let qm = &query_method;

        // For SELECT: fetch/fetch_one/fetch_optional return zero-copy borrowed wrappers.
        // For non-SELECT: fetch_one/fetch_optional return owned types.
        let zero_copy_methods = if is_select {
            quote! {
                /// Fetch all rows, returning a wrapper with zero-allocation borrowed access.
                pub fn fetch<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<#r_rows_name> {
                    #build_sql
                    let owned = executor.query_raw_readonly(sql, sql_hash, #params_slice)?;
                    Ok(#r_rows_name { owned })
                }

                /// Fetch exactly one row as a borrowed wrapper.
                pub fn fetch_one<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<#r_single_name> {
                    #build_limited_sql
                    let owned = executor.query_raw_readonly(sql, sql_hash, #params_slice)?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    Ok(#r_single_name { owned })
                }

                /// Fetch zero or one row as a borrowed wrapper.
                pub fn fetch_optional<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<Option<#r_single_name>> {
                    #build_limited_sql
                    let owned = executor.query_raw_readonly(sql, sql_hash, #params_slice)?;
                    match owned.len() {
                        0 => Ok(None),
                        1 => Ok(Some(#r_single_name { owned })),
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }
            }
        } else {
            quote! {
                pub fn fetch_one<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    #build_limited_sql
                    let owned = executor.#qm(sql, sql_hash, #params_slice)?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    let row = owned.row(0);
                    Ok(#result_name { #row_decode })
                }

                pub fn fetch_optional<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    #build_limited_sql
                    let owned = executor.#qm(sql, sql_hash, #params_slice)?;
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
        };

        quote! {
            #[allow(non_camel_case_types)]
            pub struct #stream_name {
                inner: ::bsql_core::QueryStream,
            }

            #[allow(non_camel_case_types)]
            impl #stream_name {
                pub fn next(&mut self) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    if let Some(row) = self.inner.next_row() {
                        return Ok(Some(#result_name { #row_decode }));
                    }
                    if !self.inner.fetch_next_chunk()? {
                        return Ok(None);
                    }
                    match self.inner.next_row() {
                        Some(row) => Ok(Some(#result_name { #row_decode })),
                        None => Ok(None),
                    }
                }

                pub fn remaining(&self) -> usize {
                    self.inner.remaining()
                }
            }

            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                #zero_copy_methods

                pub fn fetch_all<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                    #build_sql
                    let owned = executor.#qm(sql, sql_hash, #params_slice)?;
                    owned.iter().map(|row| Ok(#result_name { #row_decode })).collect::<::bsql_core::BsqlResult<Vec<_>>>()
                }

                pub fn fetch_stream(
                    self,
                    pool: &::bsql_core::Pool,
                ) -> ::bsql_core::BsqlResult<#stream_name> {
                    #build_sql
                    let inner = pool.query_stream(sql, sql_hash, #params_slice)?;
                    Ok(#stream_name { inner })
                }

                pub fn execute<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<u64> {
                    #build_sql
                    executor.execute_raw(sql, sql_hash, #params_slice)
                }

                /// Buffer this operation in a transaction for pipeline flush on commit.
                pub fn defer(self, tx: &::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                    #build_sql
                    tx.defer_execute(sql, sql_hash, #params_slice)
                }

                /// Execute (INSERT/UPDATE/DELETE). Returns affected row count.
                pub fn run<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<u64> {
                    self.execute(executor)
                }
            }
        }
    } else {
        // Execute-only (no result columns)
        quote! {
            #[allow(non_camel_case_types)]
            impl<'_bsql> #executor_name<'_bsql> {
                pub fn execute<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<u64> {
                    #build_sql
                    executor.execute_raw(sql, sql_hash, #params_slice)
                }

                /// Buffer this operation in a transaction for pipeline flush on commit.
                pub fn defer(self, tx: &::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
                    #build_sql
                    tx.defer_execute(sql, sql_hash, #params_slice)
                }

                /// Execute (INSERT/UPDATE/DELETE). Returns affected row count.
                pub fn run<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<u64> {
                    self.execute(executor)
                }
            }
        }
    };

    // fetch_ref / fetch_one_ref / fetch_optional_ref removed for sort queries —
    // fetch/fetch_one/fetch_optional now return zero-copy borrowed wrappers directly.
    let fetch_ref_block = TokenStream::new();

    let for_each_row_struct = gen_pg_for_each_row_struct(parsed, validation);
    let rows_struct = gen_rows_struct(parsed, validation);

    // Constructor: captures params + sort from scope
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
            #for_each_row_struct
            #rows_struct
            #executor_struct
            #fetch_methods
            #fetch_ref_block
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
        #[must_use = "query is not executed until .fetch(), .run(), or another execution method is called"]
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

    // Cache row decode once, reuse for all methods (F-27)
    let row_decode = if has_columns {
        gen_row_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);

        // For SELECT queries, fetch_one/fetch_optional are generated in simple_api_fetch
        // returning zero-copy borrowed wrappers. For non-SELECT (e.g. INSERT RETURNING),
        // they return owned types here.
        let owned_fetch_one_optional = if is_select {
            TokenStream::new()
        } else {
            quote! {
                pub fn fetch_one<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    let owned = executor.#query_method(#limited_sql_lit, #limited_sql_hash_val, #params_slice)?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    let row = owned.row(0);
                    Ok(#result_name { #row_decode })
                }

                pub fn fetch_optional<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    let owned = executor.#query_method(#limited_sql_lit, #limited_sql_hash_val, #params_slice)?;
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
        };

        quote! {
            #owned_fetch_one_optional

            pub fn fetch_all<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                let owned = executor.#query_method(#sql_lit, #sql_hash_val, #params_slice)?;
                owned.iter().map(|row| Ok(#result_name { #row_decode })).collect::<::bsql_core::BsqlResult<Vec<_>>>()
            }

            pub fn fetch_stream(
                self,
                pool: &::bsql_core::Pool,
            ) -> ::bsql_core::BsqlResult<#stream_name> {
                let inner = pool.query_stream(#sql_lit, #sql_hash_val, #params_slice)?;
                Ok(#stream_name { inner })
            }
        }
    } else {
        TokenStream::new()
    };

    // fetch_ref / fetch_one_ref / fetch_optional_ref removed — fetch/fetch_one/fetch_optional
    // now return the zero-copy borrowed wrappers directly.
    let fetch_ref_methods = TokenStream::new();

    // Use extracted gen_stream_struct (F-26)
    let stream_struct = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);
        gen_stream_struct(&result_name, &stream_name, &row_decode)
    } else {
        TokenStream::new()
    };

    // Rows struct for fetch_ref
    let rows_struct = if has_columns {
        gen_rows_struct(parsed, validation)
    } else {
        TokenStream::new()
    };

    let execute_method = quote! {
        pub fn execute<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            executor.execute_raw(#sql_lit, #sql_hash_val, #params_slice)
        }
    };

    let defer_method = quote! {
        /// Buffer this operation in a transaction for pipeline flush on commit.
        pub fn defer(self, tx: &::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
            tx.defer_execute(#sql_lit, #sql_hash_val, #params_slice)
        }
    };

    // --- PG for_each ---
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

        quote! {
            /// Process each row directly from the wire buffer via a closure.
            ///
            /// Zero arena allocation, zero SmallVec — the generated code decodes
            /// columns sequentially inline from the raw DataRow message bytes.
            pub fn for_each<_BsqlForEachF>(
                self,
                pool: &::bsql_core::Pool,
                mut f: _BsqlForEachF,
            ) -> ::bsql_core::BsqlResult<()>
            where
                _BsqlForEachF: FnMut(#fe_row_name<'_>) -> Result<(), ::bsql_core::BsqlError>,
            {
                pool.__for_each_raw_bytes(
                    #sql_lit,
                    #sql_hash_val,
                    #params_slice,
                    true,
                    |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                        #fe_raw_stmts
                        let _bsql_typed = #fe_row_name { #fe_raw_inits };
                        f(_bsql_typed)
                    },
                )
            }

            /// Process each row, collecting mapped results into a `Vec`.
            pub fn for_each_map<_BsqlForEachF, _BsqlForEachT>(
                self,
                pool: &::bsql_core::Pool,
                mut f: _BsqlForEachF,
            ) -> ::bsql_core::BsqlResult<Vec<_BsqlForEachT>>
            where
                _BsqlForEachF: FnMut(#fe_row_name<'_>) -> _BsqlForEachT,
            {
                let mut _bsql_results: Vec<_BsqlForEachT> = Vec::new();
                pool.__for_each_raw_bytes(
                    #sql_lit,
                    #sql_hash_val,
                    #params_slice,
                    true,
                    |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                        #fe_raw_stmts2
                        let _bsql_typed = #fe_row_name { #fe_raw_inits2 };
                        _bsql_results.push(f(_bsql_typed));
                        Ok(())
                    },
                )?;
                Ok(_bsql_results)
            }
        }
    } else {
        TokenStream::new()
    };

    // --- Simple API (fetch/fetch_one/fetch_optional/run) ---
    // fetch(), fetch_one(), fetch_optional() now return zero-copy borrowed wrappers
    // (the old fetch_ref / fetch_one_ref / fetch_optional_ref).
    // fetch_all() remains as the owned-String variant for users who need it.
    let simple_api_fetch = if has_columns && is_select {
        let r_rows_name = rows_struct_name(parsed);
        let r_single_name = single_ref_struct_name(parsed);

        quote! {
            /// Fetch all rows, returning a wrapper with zero-allocation borrowed access.
            ///
            /// Unlike `fetch_all` which copies every `String`/`Vec<u8>` into owned
            /// structs, `fetch` keeps the data in the arena and hands out `&str` /
            /// `&[u8]` references via the `get(idx)` / `iter()` methods.
            pub fn fetch<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#r_rows_name> {
                let owned = executor.query_raw_readonly(#sql_lit, #sql_hash_val, #params_slice)?;
                Ok(#r_rows_name { owned })
            }

            /// Fetch exactly one row as a borrowed wrapper.
            pub fn fetch_one<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#r_single_name> {
                let owned = executor.query_raw_readonly(#limited_sql_lit, #limited_sql_hash_val, #params_slice)?;
                if owned.len() != 1 {
                    return Err(::bsql_core::error::QueryError::row_count(
                        "exactly 1 row",
                        owned.len() as u64,
                    ));
                }
                Ok(#r_single_name { owned })
            }

            /// Fetch zero or one row as a borrowed wrapper.
            pub fn fetch_optional<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Option<#r_single_name>> {
                let owned = executor.query_raw_readonly(#limited_sql_lit, #limited_sql_hash_val, #params_slice)?;
                match owned.len() {
                    0 => Ok(None),
                    1 => Ok(Some(#r_single_name { owned })),
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

    let simple_api_run = quote! {
        /// Execute (INSERT/UPDATE/DELETE). Returns affected row count.
        pub fn run<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            self.execute(executor)
        }
    };

    quote! {
        #stream_struct
        #for_each_row_struct
        #rows_struct

        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #fetch_ref_methods
            #for_each_methods
            #execute_method
            #defer_method
            #simple_api_fetch
            #simple_api_run
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
        #[must_use = "query is not executed until .fetch(), .run(), or another execution method is called"]
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

    // Cache row decode once, reuse for fetch methods + stream struct (F-27)
    let row_decode = if has_columns {
        gen_row_decode(validation)
    } else {
        TokenStream::new()
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);

        let needs_limit = has_columns
            && is_select
            && !parsed.normalized_sql.contains(" limit ")
            && !parsed.normalized_sql.contains(" for ");

        let qm = &query_method;

        // For non-SELECT (e.g. INSERT RETURNING), generate owned fetch_one/fetch_optional.
        // For SELECT, these are generated in simple_api_fetch as zero-copy wrappers.
        let owned_fetch_one_optional = if is_select {
            TokenStream::new()
        } else {
            let fetch_one_dispatcher =
                gen_variant_dispatcher(parsed, variants, needs_limit, |sql_lit, sql_hash| {
                    quote! {
                        let owned = executor.#qm(#sql_lit, #sql_hash, &params_slice[..])?;
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

            let fetch_optional_dispatcher =
                gen_variant_dispatcher(parsed, variants, needs_limit, |sql_lit, sql_hash| {
                    quote! {
                        let owned = executor.#qm(#sql_lit, #sql_hash, &params_slice[..])?;
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
                pub fn fetch_one<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<#result_name> {
                    #fetch_one_dispatcher
                }

                pub fn fetch_optional<E: ::bsql_core::Executor>(
                    self,
                    executor: &E,
                ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                    #fetch_optional_dispatcher
                }
            }
        };

        let fetch_all_dispatcher = gen_variant_dispatcher(
            parsed,
            variants,
            false,
            |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.#qm(#sql_lit, #sql_hash, &params_slice[..])?;
                    owned.iter().map(|row| Ok(#result_name { #row_decode })).collect::<::bsql_core::BsqlResult<Vec<_>>>()
                }
            },
        );

        let fetch_stream_dispatcher =
            gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
                quote! {
                    let inner = pool.query_stream(#sql_lit, #sql_hash, &params_slice[..])?;
                    Ok(#stream_name { inner })
                }
            });

        quote! {
            #owned_fetch_one_optional

            pub fn fetch_all<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                #fetch_all_dispatcher
            }

            pub fn fetch_stream(
                self,
                pool: &::bsql_core::Pool,
            ) -> ::bsql_core::BsqlResult<#stream_name> {
                #fetch_stream_dispatcher
            }
        }
    } else {
        TokenStream::new()
    };

    // fetch_ref / fetch_one_ref / fetch_optional_ref removed for dynamic queries —
    // fetch/fetch_one/fetch_optional now return zero-copy borrowed wrappers directly.
    let fetch_ref_methods = TokenStream::new();

    // Use extracted gen_stream_struct (F-26)
    let stream_struct = if has_columns {
        let result_name = result_struct_name(parsed);
        let stream_name = stream_struct_name(parsed);
        gen_stream_struct(&result_name, &stream_name, &row_decode)
    } else {
        TokenStream::new()
    };

    // Rows struct for fetch_ref (dynamic queries)
    let rows_struct = if has_columns {
        gen_rows_struct(parsed, validation)
    } else {
        TokenStream::new()
    };

    let execute_dispatcher =
        gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
            quote! {
                executor.execute_raw(#sql_lit, #sql_hash, &params_slice[..])
            }
        });

    let execute_method = quote! {
        pub fn execute<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            #execute_dispatcher
        }
    };

    let defer_dispatcher = gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
        quote! {
            tx.defer_execute(#sql_lit, #sql_hash, &params_slice[..])
        }
    });

    let defer_method = quote! {
        /// Buffer this operation in a transaction for pipeline flush on commit.
        pub fn defer(self, tx: &::bsql_core::Transaction) -> ::bsql_core::BsqlResult<()> {
            #defer_dispatcher
        }
    };

    // --- PG for_each for dynamic queries ---
    let for_each_methods = if has_columns && is_select {
        let fe_row_name = pg_for_each_row_struct_name(parsed);
        let (fe_raw_stmts, fe_raw_inits) = gen_pg_for_each_raw_decode(validation);
        let (fe_raw_stmts2, fe_raw_inits2) = gen_pg_for_each_raw_decode(validation);

        let for_each_dispatcher =
            gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
                quote! {
                    pool.__for_each_raw_bytes(
                        #sql_lit,
                        #sql_hash,
                        &params_slice[..],
                        true,
                        |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                            #fe_raw_stmts
                            let _bsql_typed = #fe_row_name { #fe_raw_inits };
                            f(_bsql_typed)
                        },
                    )
                }
            });

        let for_each_map_dispatcher =
            gen_variant_dispatcher(parsed, variants, false, |sql_lit, sql_hash| {
                quote! {
                    pool.__for_each_raw_bytes(
                        #sql_lit,
                        #sql_hash,
                        &params_slice[..],
                        true,
                        |_bsql_data: &[u8]| -> ::bsql_core::BsqlResult<()> {
                            #fe_raw_stmts2
                            let _bsql_typed = #fe_row_name { #fe_raw_inits2 };
                            _bsql_results.push(f(_bsql_typed));
                            Ok(())
                        },
                    )
                }
            });

        quote! {
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
    } else {
        TokenStream::new()
    };

    // --- Simple API (fetch/fetch_one/fetch_optional/run) ---
    // For SELECT queries, fetch/fetch_one/fetch_optional return zero-copy borrowed wrappers.
    // For non-SELECT (e.g. dynamic INSERT RETURNING), they delegate to the owned variants.
    let simple_api_fetch = if has_columns && is_select {
        let r_rows_name = rows_struct_name(parsed);
        let r_single_name = single_ref_struct_name(parsed);

        let needs_limit = has_columns
            && is_select
            && !parsed.normalized_sql.contains(" limit ")
            && !parsed.normalized_sql.contains(" for ");

        let fetch_dispatcher = gen_variant_dispatcher(
            parsed,
            variants,
            false,
            |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.query_raw_readonly(#sql_lit, #sql_hash, &params_slice[..])?;
                    Ok(#r_rows_name { owned })
                }
            },
        );

        let fetch_one_dispatcher = gen_variant_dispatcher(
            parsed,
            variants,
            needs_limit,
            |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.query_raw_readonly(#sql_lit, #sql_hash, &params_slice[..])?;
                    if owned.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            owned.len() as u64,
                        ));
                    }
                    Ok(#r_single_name { owned })
                }
            },
        );

        let fetch_optional_dispatcher = gen_variant_dispatcher(
            parsed,
            variants,
            needs_limit,
            |sql_lit, sql_hash| {
                quote! {
                    let owned = executor.query_raw_readonly(#sql_lit, #sql_hash, &params_slice[..])?;
                    match owned.len() {
                        0 => Ok(None),
                        1 => Ok(Some(#r_single_name { owned })),
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }
            },
        );

        quote! {
            /// Fetch all rows, returning a wrapper with zero-allocation borrowed access.
            pub fn fetch<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#r_rows_name> {
                #fetch_dispatcher
            }

            /// Fetch exactly one row as a borrowed wrapper.
            pub fn fetch_one<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#r_single_name> {
                #fetch_one_dispatcher
            }

            /// Fetch zero or one row as a borrowed wrapper.
            pub fn fetch_optional<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Option<#r_single_name>> {
                #fetch_optional_dispatcher
            }
        }
    } else {
        TokenStream::new()
    };

    let simple_api_run = quote! {
        /// Execute (INSERT/UPDATE/DELETE). Returns affected row count.
        pub fn run<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            self.execute(executor)
        }
    };

    quote! {
        #stream_struct
        #rows_struct

        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #fetch_ref_methods
            #for_each_methods
            #execute_method
            #defer_method
            #simple_api_fetch
            #simple_api_run
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

/// Generate the stream struct and its `next()` / `remaining()` methods.
/// Shared by static, dynamic, and sort codegen paths.
fn gen_stream_struct(
    result_name: &proc_macro2::Ident,
    stream_name: &proc_macro2::Ident,
    row_decode: &TokenStream,
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
            pub fn next(&mut self) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                if let Some(row) = self.inner.next_row() {
                    return Ok(Some(#result_name { #row_decode }));
                }
                if !self.inner.fetch_next_chunk()? {
                    return Ok(None);
                }
                match self.inner.next_row() {
                    Some(row) => Ok(Some(#result_name { #row_decode })),
                    None => Ok(None),
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

// ---- PG for_each RAW BYTES inline decode (no PgDataRow, no SmallVec) ----

/// Generate inline sequential decode for PG for_each raw-bytes path.
///
/// Instead of constructing a `PgDataRow` and calling `.get_i32(idx)` etc.,
/// this generates code that advances `_bsql_pos` through `_bsql_data` sequentially,
/// reading each column's 4-byte length prefix followed by the column bytes.
///
/// For basic types (bool, i16, i32, i64, f32, f64, str, bytes): direct inline decode.
/// For feature-gated types (uuid, time, chrono, decimal, arrays): extracts the raw
/// column slice and calls the same `::bsql_core::driver::decode_*` functions.
fn gen_pg_for_each_raw_decode(validation: &ValidationResult) -> (TokenStream, TokenStream) {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let decode_stmts: Vec<TokenStream> = deduped_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let field_name = format_ident!("{}", name);
            let col = &validation.columns[i];
            gen_pg_raw_column_decode(&field_name, &col.rust_type)
        })
        .collect();

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
        matches!(rt.as_str(), "String" | "Vec<u8>")
            || rt.starts_with("Option<String>")
            || rt.starts_with("Option<Vec<u8>>")
    });

    let phantom_init = if needs_lifetime {
        TokenStream::new()
    } else {
        quote! { , _marker: ::std::marker::PhantomData }
    };

    let stmts = quote! {
        let mut _bsql_pos: usize = 2; // skip i16 num_cols
        #(#decode_stmts)*
    };
    let inits = quote! {
        #(#field_inits),* #phantom_init
    };
    (stmts, inits)
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

// ---- Borrowed row decode for fetch_ref (zero-allocation) ----

/// Generate the borrowed row decode: same as `gen_row_decode` but `String` stays
/// `&str` and `Vec<u8>` stays `&[u8]` — no `.to_owned()` / `.to_vec()`.
fn gen_borrowed_row_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let idx = i;
        let col = &validation.columns[i];
        let decode_expr = gen_borrowed_column_decode(idx, &col.rust_type);
        quote! { #field_name: #decode_expr }
    });

    quote! { #(#fields),* }
}

/// Generate the borrowed decode expression for a single column.
fn gen_borrowed_column_decode(idx: usize, rust_type: &str) -> TokenStream {
    if let Some(inner) = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        gen_borrowed_nullable_decode(idx, inner)
    } else {
        gen_borrowed_not_null_decode(idx, rust_type)
    }
}

/// Borrowed NOT NULL decode: `String` -> `row.get_str(idx)?` (no `.to_owned()`),
/// `Vec<u8>` -> `row.get_bytes(idx)?` (no `.to_vec()`), scalars unchanged.
fn gen_borrowed_not_null_decode(idx: usize, rust_type: &str) -> TokenStream {
    let col_idx = idx.to_string();
    match rust_type {
        "String" => {
            let err = gen_not_null_decode_error(&col_idx, "String");
            quote! { row.get_str(#idx).ok_or_else(|| #err)? }
        }
        "Vec<u8>" => {
            let err = gen_not_null_decode_error(&col_idx, "Vec<u8>");
            quote! { row.get_bytes(#idx).ok_or_else(|| #err)? }
        }
        // All other types: identical to the owned decode
        _ => gen_not_null_decode(idx, rust_type),
    }
}

/// Borrowed nullable decode: `Option<String>` -> `row.get_str(idx)` (no `.map(to_owned)`),
/// `Option<Vec<u8>>` -> `row.get_bytes(idx)` (no `.map(to_vec)`), scalars unchanged.
fn gen_borrowed_nullable_decode(idx: usize, inner_type: &str) -> TokenStream {
    match inner_type {
        "String" => quote! { row.get_str(#idx) },
        "Vec<u8>" => quote! { row.get_bytes(#idx) },
        // All other types: identical to the owned decode
        _ => gen_nullable_decode(idx, inner_type),
    }
}

/// Generate the `BsqlRows_xxx` wrapper struct, the `BsqlSingleRef_xxx` wrapper,
/// and their impls, using the borrowed for_each row struct.
fn gen_rows_struct(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new();
    }

    let rows_name = rows_struct_name(parsed);
    let single_ref_name = single_ref_struct_name(parsed);
    let fe_row_name = pg_for_each_row_struct_name(parsed);
    let borrowed_decode = gen_borrowed_row_decode(validation);

    // Check if the for_each struct needs a phantom marker
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

    quote! {
        /// Wrapper owning `OwnedResult` with zero-allocation borrowed row access.
        #[derive(Debug)]
        #[allow(non_camel_case_types)]
        pub struct #rows_name {
            owned: ::bsql_core::OwnedResult,
        }

        #[allow(non_camel_case_types)]
        impl #rows_name {
            /// Number of rows.
            pub fn len(&self) -> usize { self.owned.len() }

            /// Whether the result set is empty.
            pub fn is_empty(&self) -> bool { self.owned.is_empty() }

            /// Get a borrowed row by index.
            pub fn get(&self, idx: usize) -> ::bsql_core::BsqlResult<#fe_row_name<'_>> {
                let row = self.owned.row(idx);
                Ok(#fe_row_name { #borrowed_decode #phantom_init })
            }

            /// Iterate over borrowed rows.
            pub fn iter(&self) -> impl Iterator<Item = ::bsql_core::BsqlResult<#fe_row_name<'_>>> + '_ {
                (0..self.owned.len()).map(move |i| self.get(i))
            }
        }

        /// Wrapper owning `OwnedResult` for a single borrowed row (fetch_one / fetch_optional).
        #[derive(Debug)]
        #[allow(non_camel_case_types)]
        pub struct #single_ref_name {
            owned: ::bsql_core::OwnedResult,
        }

        #[allow(non_camel_case_types)]
        impl #single_ref_name {
            /// Get the borrowed row.
            pub fn get(&self) -> ::bsql_core::BsqlResult<#fe_row_name<'_>> {
                let row = self.owned.row(0);
                Ok(#fe_row_name { #borrowed_decode #phantom_init })
            }
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

fn stream_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlStream_{}", &parsed.statement_name)
}

fn rows_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlRows_{}", &parsed.statement_name)
}

fn single_ref_struct_name(parsed: &ParsedQuery) -> proc_macro2::Ident {
    format_ident!("BsqlSingleRef_{}", &parsed.statement_name)
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
        // Simple API
        assert!(
            // The `fetch` alias: look for `fn fetch <` to avoid matching fetch_one/etc
            code_str.contains("fn fetch <"),
            "missing fetch method: {code_str}"
        );
        assert!(
            code_str.contains("fn run"),
            "missing run method: {code_str}"
        );
        // get and stream aliases should NOT be generated on the executor
        // (fn get on BsqlRows/BsqlSingleRef is fine — check for executor method signature)
        assert!(
            !code_str.contains("fn get < E"),
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
    fn generates_zero_copy_fetch_methods_for_select() {
        let parsed = parse_query("SELECT id, name FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("name", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // fetch_ref / fetch_one_ref / fetch_optional_ref are removed;
        // fetch / fetch_one / fetch_optional now return the zero-copy wrappers directly.
        assert!(
            !code_str.contains("fetch_ref"),
            "fetch_ref should be removed: {code_str}"
        );
        assert!(
            !code_str.contains("fetch_one_ref"),
            "fetch_one_ref should be removed: {code_str}"
        );
        assert!(
            !code_str.contains("fetch_optional_ref"),
            "fetch_optional_ref should be removed: {code_str}"
        );
        assert!(
            code_str.contains("BsqlRows_"),
            "missing BsqlRows struct: {code_str}"
        );
        assert!(
            code_str.contains("BsqlSingleRef_"),
            "missing BsqlSingleRef struct: {code_str}"
        );
        // fetch_one should return BsqlSingleRef, not BsqlResult
        assert!(
            code_str.contains("fn fetch_one"),
            "missing fetch_one method: {code_str}"
        );
        assert!(
            code_str.contains("fn fetch_optional"),
            "missing fetch_optional method: {code_str}"
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

    #[test]
    fn fetch_ref_borrowed_decode_no_to_owned() {
        let parsed = parse_query("SELECT name, data FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![col("name", "String"), col("data", "Vec<u8>")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // The BsqlRows get() method should NOT contain to_owned/to_vec
        // Find the BsqlRows impl section
        let rows_impl_start = code_str.find("BsqlRows_").unwrap();
        let rows_section = &code_str[rows_impl_start..];
        // Find the next "impl" boundary after the BsqlRows impl to isolate the section
        let section_end = rows_section
            .find("BsqlSingleRef_")
            .unwrap_or(rows_section.len());
        let rows_section = &rows_section[..section_end];

        assert!(
            !rows_section.contains("to_owned"),
            "BsqlRows decode should NOT use to_owned: {rows_section}"
        );
        assert!(
            !rows_section.contains("to_vec"),
            "BsqlRows decode should NOT use to_vec: {rows_section}"
        );
    }

    #[test]
    fn fetch_ref_borrowed_nullable_no_to_owned() {
        let parsed = parse_query("SELECT bio, avatar FROM t WHERE 1 = $a: i32").unwrap();
        let validation = make_validation(vec![
            col("bio", "Option<String>"),
            col("avatar", "Option<Vec<u8>>"),
        ]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Isolate BsqlRows section
        let rows_impl_start = code_str.find("BsqlRows_").unwrap();
        let rows_section = &code_str[rows_impl_start..];
        let section_end = rows_section
            .find("BsqlSingleRef_")
            .unwrap_or(rows_section.len());
        let rows_section = &rows_section[..section_end];

        assert!(
            !rows_section.contains("to_owned"),
            "nullable BsqlRows decode should NOT use to_owned: {rows_section}"
        );
        assert!(
            !rows_section.contains("to_vec"),
            "nullable BsqlRows decode should NOT use to_vec: {rows_section}"
        );
    }
}
