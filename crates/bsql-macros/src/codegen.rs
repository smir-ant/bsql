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
    // But as a safety fallback, generate a compile error.
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

    // EXPLAIN plan as doc comment (v0.7, opt-in via `explain` feature)
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
///
/// Always emits `<'_bsql>` lifetime and `PhantomData` — no branching.
/// When no fields use the lifetime, PhantomData ties it to the struct.
/// This is zero-cost (PhantomData is ZST).
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
///
/// FIX 10: for `fetch_one` and `fetch_optional`, if the SQL has no LIMIT clause,
/// inject `LIMIT 2` so PG stops early instead of fetching an entire table.
///
/// v0.7: SELECT queries call `query_raw_readonly` (routes to replicas when
/// available). Writes use `query_raw` (always primary).
fn gen_executor_impls(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let sql_lit = &parsed.positional_sql;

    // v0.7: SELECT -> query_raw_readonly (replica-aware), writes -> query_raw (primary)
    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let query_method = if is_select {
        quote! { query_raw_readonly }
    } else {
        quote! { query_raw }
    };

    // Build the params slice: &[&self.id, &self.name, ...]
    let param_refs: Vec<TokenStream> = parsed
        .params
        .iter()
        .map(|p| {
            let name = param_ident(&p.name);
            quote! { &self.#name as &(dyn ::bsql_core::pg::ToSql + Sync) }
        })
        .collect();

    let params_slice = if param_refs.is_empty() {
        quote! { &[] }
    } else {
        quote! { &[#(#param_refs),*] }
    };

    let has_columns = !validation.columns.is_empty();

    // FIX 10: generate a LIMIT 2 variant for fetch_one/fetch_optional.
    // Only for SELECT queries -- LIMIT cannot be appended to INSERT/UPDATE/DELETE RETURNING.
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

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let row_decode = gen_row_decode(validation);

        quote! {
            pub async fn fetch_one<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<#result_name> {
                let rows = executor.#query_method(#limited_sql_lit, #params_slice).await?;
                if rows.len() != 1 {
                    return Err(::bsql_core::error::QueryError::row_count(
                        "exactly 1 row",
                        rows.len() as u64,
                    ));
                }
                let row = &rows[0];
                Ok(#result_name { #row_decode })
            }

            pub async fn fetch_all<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Vec<#result_name>> {
                let rows = executor.#query_method(#sql_lit, #params_slice).await?;
                Ok(rows.iter().map(|row| #result_name { #row_decode }).collect())
            }

            pub async fn fetch_optional<E: ::bsql_core::Executor>(
                self,
                executor: &E,
            ) -> ::bsql_core::BsqlResult<Option<#result_name>> {
                let rows = executor.#query_method(#limited_sql_lit, #params_slice).await?;
                match rows.len() {
                    0 => Ok(None),
                    1 => {
                        let row = &rows[0];
                        Ok(Some(#result_name { #row_decode }))
                    }
                    n => Err(::bsql_core::error::QueryError::row_count(
                        "0 or 1 rows",
                        n as u64,
                    )),
                }
            }

            /// Stream rows one at a time, decoding each into the typed result struct.
            ///
            /// Only available on `&Pool` — the returned stream holds a connection
            /// from the pool for its entire lifetime.
            ///
            /// The returned stream is `!Unpin`. Use `tokio::pin!` or `Box::pin`
            /// before calling `StreamExt::next`.
            pub async fn fetch_stream(
                self,
                pool: &::bsql_core::Pool,
            ) -> ::bsql_core::BsqlResult<
                impl ::bsql_core::Stream<Item = ::bsql_core::BsqlResult<#result_name>> + '_
            > {
                use ::bsql_core::Stream as _;
                let raw = pool.query_stream(#sql_lit, #params_slice).await?;
                Ok(StreamMap { inner: raw, _phantom: ::std::marker::PhantomData::<#result_name> })
            }
        }
    } else {
        TokenStream::new()
    };

    let stream_map_def = gen_stream_map_adapter(parsed, validation);

    let execute_method = quote! {
        pub async fn execute<E: ::bsql_core::Executor>(
            self,
            executor: &E,
        ) -> ::bsql_core::BsqlResult<u64> {
            executor.execute_raw(#sql_lit, #params_slice).await
        }
    };

    quote! {
        #stream_map_def

        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #execute_method
        }
    }
}

// ---- Dynamic query codegen ----

/// Generate the executor struct for a dynamic query.
///
/// Captures all base params + all optional params (as their declared
/// `Option<T>` types). Always has a lifetime because optional params
/// may contain references.
fn gen_dynamic_executor_struct(parsed: &ParsedQuery) -> TokenStream {
    let struct_name = executor_struct_name(parsed);

    // Collect all fields: base params + optional clause params
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
///
/// Contains `fetch_one`, `fetch_all`, `fetch_optional`, `execute` methods.
/// Each method dispatches to the correct SQL variant via a `match` on
/// which `Option` params are `Some`.
fn gen_dynamic_executor_impls(
    parsed: &ParsedQuery,
    validation: &ValidationResult,
    variants: &[QueryVariant],
) -> TokenStream {
    let executor_name = executor_struct_name(parsed);
    let has_columns = !validation.columns.is_empty();

    // Build the match dispatcher that all methods share

    // v0.7: SELECT -> query_raw_readonly (replica-aware), writes -> query_raw
    let is_select = parsed.kind == crate::parse::QueryKind::Select;
    let query_method = if is_select {
        quote! { query_raw_readonly }
    } else {
        quote! { query_raw }
    };

    let fetch_methods = if has_columns {
        let result_name = result_struct_name(parsed);
        let row_decode = gen_row_decode(validation);

        // For fetch_one/fetch_optional: check if we can inject LIMIT 2
        let needs_limit = has_columns
            && is_select
            && !parsed.normalized_sql.contains(" limit ")
            && !parsed.normalized_sql.contains(" for ");

        let qm = &query_method;
        let fetch_one_dispatcher =
            gen_variant_dispatcher(parsed, variants, needs_limit, |sql_lit| {
                quote! {
                    let rows = executor.#qm(#sql_lit, &params_slice[..]).await?;
                    if rows.len() != 1 {
                        return Err(::bsql_core::error::QueryError::row_count(
                            "exactly 1 row",
                            rows.len() as u64,
                        ));
                    }
                    let row = &rows[0];
                    Ok(#result_name { #row_decode })
                }
            });

        let fetch_all_dispatcher = gen_variant_dispatcher(parsed, variants, false, |sql_lit| {
            quote! {
                let rows = executor.#qm(#sql_lit, &params_slice[..]).await?;
                Ok(rows.iter().map(|row| #result_name { #row_decode }).collect())
            }
        });

        let fetch_optional_dispatcher =
            gen_variant_dispatcher(parsed, variants, needs_limit, |sql_lit| {
                quote! {
                    let rows = executor.#qm(#sql_lit, &params_slice[..]).await?;
                    match rows.len() {
                        0 => Ok(None),
                        1 => {
                            let row = &rows[0];
                            Ok(Some(#result_name { #row_decode }))
                        }
                        n => Err(::bsql_core::error::QueryError::row_count(
                            "0 or 1 rows",
                            n as u64,
                        )),
                    }
                }
            });

        let fetch_stream_dispatcher = gen_variant_dispatcher(parsed, variants, false, |sql_lit| {
            quote! {
                let raw = pool.query_stream(#sql_lit, &params_slice[..]).await?;
                Ok(StreamMap { inner: raw, _phantom: ::std::marker::PhantomData::<#result_name> })
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

            /// Stream rows one at a time, decoding each into the typed result struct.
            ///
            /// Only available on `&Pool` — the returned stream holds a connection
            /// from the pool for its entire lifetime.
            ///
            /// The returned stream is `!Unpin`. Use `tokio::pin!` or `Box::pin`
            /// before calling `StreamExt::next`.
            pub async fn fetch_stream(
                self,
                pool: &::bsql_core::Pool,
            ) -> ::bsql_core::BsqlResult<
                impl ::bsql_core::Stream<Item = ::bsql_core::BsqlResult<#result_name>> + '_
            > {
                use ::bsql_core::Stream as _;
                #fetch_stream_dispatcher
            }
        }
    } else {
        TokenStream::new()
    };

    let stream_map_def = gen_stream_map_adapter(parsed, validation);

    let execute_dispatcher = gen_variant_dispatcher(parsed, variants, false, |sql_lit| {
        quote! {
            executor.execute_raw(#sql_lit, &params_slice[..]).await
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
        #stream_map_def

        #[allow(non_camel_case_types)]
        impl<'_bsql> #executor_name<'_bsql> {
            #fetch_methods
            #execute_method
        }
    }
}

/// Generate the variant match dispatcher.
///
/// Creates a `match (p0.is_some(), p1.is_some(), ...) { ... }` block
/// where each arm builds the correct SQL string and params slice, then
/// calls the provided `body_fn` closure.
fn gen_variant_dispatcher<F>(
    parsed: &ParsedQuery,
    variants: &[QueryVariant],
    inject_limit: bool,
    body_fn: F,
) -> TokenStream
where
    F: Fn(&str) -> TokenStream,
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
            // Build the match pattern: (true/false, true/false, ...)
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

            // Build the SQL string
            let sql_str = if inject_limit {
                format!("{} LIMIT 2", variant.sql)
            } else {
                variant.sql.clone()
            };

            // Build the params slice for this variant
            let param_bindings: Vec<TokenStream> = variant
                .params
                .iter()
                .map(|p| {
                    let name = param_ident(&p.name);
                    if p.rust_type.starts_with("Option<") {
                        // Optional param — unwrap (we know it's Some in this arm)
                        quote! { self.#name.as_ref().unwrap() as &(dyn ::bsql_core::pg::ToSql + Sync) }
                    } else {
                        quote! { &self.#name as &(dyn ::bsql_core::pg::ToSql + Sync) }
                    }
                })
                .collect();

            let body = body_fn(&sql_str);

            quote! {
                #pattern => {
                    let params_slice: &[&(dyn ::bsql_core::pg::ToSql + Sync)] =
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

    // Collect all field names: base + optional
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

/// Generate the StreamMap adapter struct and Stream impl.
///
/// Shared between static and dynamic codegen paths.
/// Returns empty tokens when the query has no columns.
fn gen_stream_map_adapter(parsed: &ParsedQuery, validation: &ValidationResult) -> TokenStream {
    if validation.columns.is_empty() {
        return TokenStream::new();
    }

    let result_name = result_struct_name(parsed);
    let row_decode = gen_row_decode(validation);

    quote! {
        /// Maps `QueryStream` (raw rows) to typed result structs.
        struct StreamMap<T> {
            inner: ::bsql_core::QueryStream,
            _phantom: ::std::marker::PhantomData<T>,
        }

        impl ::bsql_core::Stream for StreamMap<#result_name> {
            type Item = ::bsql_core::BsqlResult<#result_name>;

            fn poll_next(
                mut self: ::std::pin::Pin<&mut Self>,
                cx: &mut ::std::task::Context<'_>,
            ) -> ::std::task::Poll<Option<Self::Item>> {
                ::std::pin::Pin::new(&mut self.inner)
                    .poll_next(cx)
                    .map(|opt| opt.map(|res| {
                        let row = res?;
                        Ok(#result_name { #row_decode })
                    }))
            }
        }
    }
}

/// Generate row field decoding: `field_name: row.get(0), ...`
fn gen_row_decode(validation: &ValidationResult) -> TokenStream {
    let deduped_names = deduplicate_column_names(&validation.columns);
    let fields = deduped_names.iter().enumerate().map(|(i, name)| {
        let field_name = format_ident!("{}", name);
        let idx = i;
        quote! { #field_name: row.get(#idx) }
    });

    quote! { #(#fields),* }
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
///
/// Uses `syn::parse_str` to build a proper type AST, then walks it to add
/// lifetimes. This handles nested types correctly: `Option<&str>`, `&[&str]`,
/// `Vec<&[u8]>`, etc.
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
///
/// For `SELECT u.id, t.id FROM ...` this produces `["id", "id_1"]`.
fn deduplicate_column_names(columns: &[crate::validate::ColumnInfo]) -> Vec<String> {
    let names: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| sanitize_column_name(&col.name, i))
        .collect();

    // Deduplicate: suffix with _1, _2, etc. until unique
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
///
/// Suffixes Rust keywords with `_` (e.g. `type` -> `type_`).
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
///
/// PG returns `?column?` for unnamed expressions (e.g. `SELECT 1`).
/// This function replaces invalid characters, provides fallback names,
/// and suffixes Rust keywords with `_` (e.g. `type` -> `type_`).
fn sanitize_column_name(name: &str, index: usize) -> String {
    if name == "?column?" || name.is_empty() {
        return format!("col_{index}");
    }

    // Replace non-alphanumeric/underscore chars with underscore
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

    // Ensure it doesn't start with a digit
    let sanitized = if sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        format!("col_{sanitized}")
    } else {
        sanitized
    };

    // Suffix Rust keywords to avoid conflicts
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
        assert!(
            code_str.contains("fetch_stream"),
            "missing fetch_stream: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn fetch_stream_uses_pool_not_executor() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // fetch_stream takes &Pool, not generic E: Executor
        assert!(
            code_str.contains("pool : & :: bsql_core :: Pool")
                || code_str.contains("pool: &::bsql_core::Pool"),
            "fetch_stream should accept &Pool: {code_str}"
        );
    }

    #[test]
    fn fetch_stream_generates_stream_map() {
        let parsed = parse_query("SELECT id, login FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("login", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("StreamMap"),
            "missing StreamMap adapter: {code_str}"
        );
        assert!(
            code_str.contains("poll_next"),
            "StreamMap should implement poll_next: {code_str}"
        );
    }

    #[test]
    fn execute_only_query_has_no_fetch_stream() {
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("fetch_stream"),
            "execute-only query should not have fetch_stream: {code_str}"
        );
        assert!(
            !code_str.contains("StreamMap"),
            "execute-only query should not have StreamMap: {code_str}"
        );
    }

    #[test]
    fn no_params_generates_unit_struct() {
        let parsed = parse_query("SELECT 1").unwrap();
        let validation = make_validation(vec![col("col_0", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Unit struct executor (no fields, no braces in constructor)
        assert!(
            code_str.contains("struct BsqlExecutor_"),
            "missing executor: {code_str}"
        );
    }

    #[test]
    fn execute_only_query_has_no_result_struct() {
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]); // no columns
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("BsqlResult_"),
            "should not have result struct: {code_str}"
        );
        assert!(code_str.contains("execute"), "missing execute: {code_str}");
    }

    #[test]
    fn param_capture_in_constructor() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // The constructor should reference the variable name `id` and include PhantomData
        assert!(
            code_str.contains("id ,") || code_str.contains("id,"),
            "missing param capture: {code_str}"
        );
        assert!(
            code_str.contains("PhantomData"),
            "missing PhantomData: {code_str}"
        );
    }

    #[test]
    fn positional_sql_in_generated_code() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // The generated code should use positional SQL ($1), not named ($id)
        assert!(
            code_str.contains("$1"),
            "should contain positional $1: {code_str}"
        );
        assert!(
            !code_str.contains("$id"),
            "should not contain named $id: {code_str}"
        );
    }

    // --- FIX 5: lifetime injection via syn ---

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
    fn inject_lifetime_option_ref() {
        let ts = inject_lifetime("Option<&str>");
        let s = ts.to_string();
        assert!(
            s.contains("'_bsql"),
            "missing lifetime in Option<&str>: {s}"
        );
    }

    #[test]
    fn inject_lifetime_no_ref_passes_through() {
        let ts = inject_lifetime("i32");
        let s = ts.to_string();
        assert!(!s.contains("'_bsql"), "i32 should have no lifetime: {s}");
    }

    #[test]
    fn inject_lifetime_ref_slice_of_refs() {
        let ts = inject_lifetime("&[&str]");
        let s = ts.to_string();
        // Both references should get lifetimes
        assert_eq!(
            s.matches("'_bsql").count(),
            2,
            "expected 2 lifetimes in &[&str]: {s}"
        );
    }

    // --- FIX 6: duplicate column names ---

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

    #[test]
    fn generates_result_struct_with_deduplicated_fields() {
        let parsed = parse_query("SELECT 1").unwrap();
        let validation = make_validation(vec![col("id", "i32"), col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(code_str.contains("id"), "missing id field: {code_str}");
        assert!(code_str.contains("id_1"), "missing id_1 field: {code_str}");
    }

    // --- FIX 10: LIMIT injection ---

    #[test]
    fn fetch_one_injects_limit_2() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // fetch_one should use "... LIMIT 2", fetch_all should use original SQL
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

        // Should NOT inject an additional LIMIT
        assert!(
            !code_str.contains("LIMIT 2"),
            "should not add LIMIT 2 when LIMIT exists: {code_str}"
        );
    }

    // --- FOR UPDATE should NOT get LIMIT 2 ---

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

    #[test]
    fn for_update_skip_locked_no_limit() {
        let parsed =
            parse_query("SELECT id FROM t WHERE id = $id: i32 FOR UPDATE SKIP LOCKED").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("LIMIT 2"),
            "FOR UPDATE SKIP LOCKED should NOT get LIMIT 2: {code_str}"
        );
    }

    // --- column dedup collision: ["id_1", "id", "id"] ---

    #[test]
    fn column_dedup_collision_with_existing_suffix() {
        // If columns are ["id_1", "id", "id"], the second "id" must NOT
        // become "id_1" (collision with first column). It should be "id_2".
        let columns = vec![col("id_1", "i32"), col("id", "i32"), col("id", "i32")];
        let names = deduplicate_column_names(&columns);
        assert_eq!(names[0], "id_1");
        assert_eq!(names[1], "id");
        assert_eq!(
            names[2], "id_2",
            "should skip id_1 which is already taken: {names:?}"
        );
    }

    #[test]
    fn column_dedup_complex_collision() {
        // ["a", "a", "a_1", "a"] should produce ["a", "a_1", "a_1_1", "a_2"]
        // Wait — let me think through the algorithm:
        // 1. "a" -> no collision -> ["a"]
        // 2. "a" -> collision -> try "a_1" -> collision -> try "a_2" -> ok -> ["a", "a_2"]
        // 3. "a_1" -> no collision -> ["a", "a_2", "a_1"]
        // 4. "a" -> collision -> try "a_1" -> collision -> try "a_2" -> collision -> try "a_3" -> ok
        let columns = vec![
            col("a", "i32"),
            col("a", "i32"),
            col("a_1", "i32"),
            col("a", "i32"),
        ];
        let names = deduplicate_column_names(&columns);
        // All names must be unique
        let unique: std::collections::HashSet<&str> = names.iter().map(|s| s.as_str()).collect();
        assert_eq!(unique.len(), 4, "all names must be unique: {names:?}");
    }

    // --- bad-path coverage: sanitize_column_name ---

    #[test]
    fn sanitize_unnamed_column() {
        assert_eq!(sanitize_column_name("?column?", 0), "col_0");
        assert_eq!(sanitize_column_name("?column?", 3), "col_3");
    }

    #[test]
    fn sanitize_empty_column_name() {
        assert_eq!(sanitize_column_name("", 0), "col_0");
    }

    #[test]
    fn sanitize_column_starting_with_digit() {
        assert_eq!(sanitize_column_name("1abc", 0), "col_1abc");
    }

    #[test]
    fn sanitize_column_with_special_chars() {
        assert_eq!(sanitize_column_name("my-col.name", 0), "my_col_name");
    }

    #[test]
    fn sanitize_normal_column_passthrough() {
        assert_eq!(sanitize_column_name("id", 0), "id");
        assert_eq!(sanitize_column_name("user_name", 0), "user_name");
    }

    // --- Rust keyword sanitization ---

    #[test]
    fn sanitize_column_keyword_type() {
        assert_eq!(sanitize_column_name("type", 0), "type_");
    }

    #[test]
    fn sanitize_column_keyword_fn() {
        assert_eq!(sanitize_column_name("fn", 0), "fn_");
    }

    #[test]
    fn sanitize_column_keyword_match() {
        assert_eq!(sanitize_column_name("match", 0), "match_");
    }

    #[test]
    fn sanitize_column_non_keyword_passthrough() {
        assert_eq!(sanitize_column_name("status", 0), "status");
    }

    #[test]
    fn sanitize_param_keyword() {
        assert_eq!(sanitize_param_name("type"), "type_");
        assert_eq!(sanitize_param_name("fn"), "fn_");
        assert_eq!(sanitize_param_name("match"), "match_");
    }

    #[test]
    fn sanitize_param_non_keyword() {
        assert_eq!(sanitize_param_name("id"), "id");
        assert_eq!(sanitize_param_name("name"), "name");
    }

    // --- codegen: INSERT/UPDATE/DELETE without RETURNING has no result struct ---

    #[test]
    fn insert_no_returning_has_execute_only() {
        let parsed = parse_query("INSERT INTO t (a) VALUES ($a: i32)").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("fn fetch_one"),
            "should not have fn fetch_one: {code_str}"
        );
        assert!(
            !code_str.contains("fn fetch_all"),
            "should not have fn fetch_all: {code_str}"
        );
        assert!(
            code_str.contains("fn execute"),
            "missing fn execute: {code_str}"
        );
    }

    #[test]
    fn delete_with_returning_has_fetch_methods() {
        let parsed = parse_query("DELETE FROM t WHERE id = $id: i32 RETURNING id").unwrap();
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
    }

    // --- LIMIT injection edge cases ---

    #[test]
    fn insert_returning_no_limit_injected() {
        // LIMIT cannot be appended to INSERT...RETURNING
        let parsed = parse_query("INSERT INTO t (a) VALUES ($a: i32) RETURNING id").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("LIMIT 2"),
            "INSERT RETURNING should NOT get LIMIT: {code_str}"
        );
    }

    #[test]
    fn update_returning_no_limit_injected() {
        let parsed =
            parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32 RETURNING id").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("LIMIT 2"),
            "UPDATE RETURNING should NOT get LIMIT: {code_str}"
        );
    }

    #[test]
    fn for_share_no_limit_injected() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32 FOR SHARE").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("LIMIT 2"),
            "FOR SHARE should NOT get LIMIT: {code_str}"
        );
    }

    // --- lifetime injection edge cases ---

    #[test]
    fn inject_lifetime_vec_no_ref() {
        let ts = inject_lifetime("Vec<i32>");
        let s = ts.to_string();
        assert!(
            !s.contains("'_bsql"),
            "Vec<i32> should have no lifetime: {s}"
        );
    }

    #[test]
    fn inject_lifetime_option_i32_no_ref() {
        let ts = inject_lifetime("Option<i32>");
        let s = ts.to_string();
        assert!(
            !s.contains("'_bsql"),
            "Option<i32> should have no lifetime: {s}"
        );
    }

    #[test]
    fn inject_lifetime_path_type() {
        let ts = inject_lifetime("time::OffsetDateTime");
        let s = ts.to_string();
        assert!(
            !s.contains("'_bsql"),
            "time::OffsetDateTime needs no lifetime: {s}"
        );
    }

    // --- multiple params with refs and non-refs ---

    #[test]
    fn mixed_ref_and_owned_params() {
        let parsed =
            parse_query("SELECT id FROM t WHERE a = $name: &str AND b = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        // Both params should appear
        assert!(code_str.contains("name"), "missing name param: {code_str}");
        assert!(code_str.contains("id"), "missing id param: {code_str}");
        // The ref param should get a lifetime
        assert!(
            code_str.contains("'_bsql"),
            "ref param should have lifetime: {code_str}"
        );
    }

    // --- single column result struct ---

    #[test]
    fn single_column_result_struct() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("pub id : i32"),
            "single column struct: {code_str}"
        );
    }

    // --- keyword column in generated code ---

    #[test]
    fn keyword_column_name_in_result_struct() {
        let parsed = parse_query("SELECT 1").unwrap();
        let validation = make_validation(vec![col("type", "String")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("type_"),
            "keyword column should be suffixed: {code_str}"
        );
        // Should NOT contain bare `type` as a field name (which would be invalid Rust)
        // The suffixed version `type_` is a valid identifier
    }

    // --- v0.7: read/write routing ---

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

        // INSERT should NOT use query_raw_readonly
        assert!(
            !code_str.contains("query_raw_readonly"),
            "INSERT should NOT use query_raw_readonly: {code_str}"
        );
        // It should use query_raw (for the RETURNING fetch methods)
        assert!(
            code_str.contains("query_raw"),
            "INSERT RETURNING should use query_raw: {code_str}"
        );
    }

    #[test]
    fn update_uses_execute_raw() {
        let parsed = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("query_raw_readonly"),
            "UPDATE should not use query_raw_readonly: {code_str}"
        );
        assert!(
            code_str.contains("execute_raw"),
            "UPDATE should use execute_raw: {code_str}"
        );
    }

    #[test]
    fn delete_uses_execute_raw() {
        let parsed = parse_query("DELETE FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("query_raw_readonly"),
            "DELETE should not use query_raw_readonly: {code_str}"
        );
        assert!(
            code_str.contains("execute_raw"),
            "DELETE should use execute_raw: {code_str}"
        );
    }

    // --- v0.7: EXPLAIN doc comment ---

    #[cfg(feature = "explain")]
    #[test]
    fn explain_plan_embedded_as_doc_comment() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = ValidationResult {
            columns: vec![col("id", "i32")],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            explain_plan: Some("Seq Scan on t  (cost=0.00..1.00 rows=1)".into()),
        };
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            code_str.contains("Query plan"),
            "should contain EXPLAIN header: {code_str}"
        );
        assert!(
            code_str.contains("Seq Scan"),
            "should embed plan text: {code_str}"
        );
    }

    #[test]
    fn no_explain_plan_no_doc_comment() {
        let parsed = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        let validation = make_validation(vec![col("id", "i32")]);
        let code = generate_query_code(&parsed, &validation);
        let code_str = code.to_string();

        assert!(
            !code_str.contains("Query plan"),
            "should NOT contain EXPLAIN header when explain_plan is None: {code_str}"
        );
    }
}
