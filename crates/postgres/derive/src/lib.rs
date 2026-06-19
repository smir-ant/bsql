//! Procedural-macro pair-crate for `bsql-pg-proto`.
//!
//! Hosts internal-discipline derives. Mirror of the
//! `serde` / `serde-derive` convention — runtime types live in
//! `bsql-pg-proto`, derive macros live here. This split is required by
//! Rust's language rule: `proc-macro = true` crates can only export
//! `proc_macro` / `proc_macro_attribute` / `proc_macro_derive`
//! functions — they cannot host runtime types or values.
//!
//! # Currently provided
//!
//! - [`Pristine`] — derives `is_pristine()` invariant check. Required
//!   for [`crate::pristine::Pristine`][bsql_postgres_proto_pristine_link]
//!   trait impl on types like `SessionParams`. Lifts broad-scope
//!   tier-3 to tier-1 by-construction: the compiler emits the
//!   field-by-field check; missing a new field is impossible.
//!
//! [bsql_postgres_proto_pristine_link]: https://docs.rs/bsql-pg-proto
//!
//! # Path resolution invariant
//!
//! Generated code references `::bsql_postgres_proto::pristine::Pristine`
//! through an absolute path. The `bsql-pg-proto` crate adds
//! `extern crate self as bsql_postgres_proto;` at its lib root so this
//! path resolves both:
//!
//! - **Inside `bsql-pg-proto` itself** — when `#[derive(Pristine)]`
//!   is applied to types defined in `bsql-pg-proto` (e.g.
//!   `SessionParams`); the `self` aliasing makes
//!   `::bsql_postgres_proto::pristine::Pristine` resolve to the local trait.
//! - **In downstream user crates** (Phase 2+) — standard external
//!   crate path resolution.
//!
//! # No `unsafe` in our code
//!
//! Per CREDO §1, `bsql-pg-proto-derive` is `#![forbid(unsafe_code)]`.
//! Transitive `unsafe` lives in `syn`/`quote`/`proc-macro2` (token
//! parsing/manipulation) — bounded and well-audited per CREDO §11.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
#![deny(unused_must_use, unused_lifetimes, missing_docs)]
// Opt out of the workspace tier-4 `disallowed_methods` ledger. Like the
// proto crate, this proc-macro crate enforces a stricter, whole-crate
// forbid-bundle (above: unwrap/expect/panic/as/arithmetic class) and uses
// the `try_from(..).unwrap_or(SATURATION)` dead-arm shape as the
// sanctioned forbid-bundle-compliant form (e.g. clamping a parsed param
// count into a wire u16), not as a silent data fallback — so the
// per-site ledger that fits the driver crates would be noise here.
#![allow(
    clippy::disallowed_methods,
    reason = "stricter whole-crate forbid-bundle; .unwrap_or* is the sanctioned dead-arm shape for parse-time wire-field clamping, not a silent data fallback"
)]

// `alloc` crate is implicitly available in proc-macro context
// (proc-macro crates compile with full `std`), but the explicit
// `extern crate alloc;` keeps the lexer/extractor modules'
// `use alloc::vec::Vec` path resolution stable across rustc
// versions and makes the `no_std`-aware shape obvious to auditors.
extern crate alloc;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Field, Fields, Type};

// `prepared!` proc-macro pipeline:
//
// - `sql_lexer`: narrow-scope SQL tokenizer.
// - `extract`: placeholder + cast extraction with validation.
// - `typemap`: PG type-name → Rust type token mapping.
//
// The macro entry (`prepared(...)`) lives at the bottom of this
// file, alongside the existing `#[proc_macro_derive(Pristine)]`.
mod sql_lexer;
mod extract;
mod typemap;

/// `#[derive(Pristine)]` — derives the
/// `bsql_postgres_proto::pristine::Pristine` trait impl plus an inherent
/// `__pristine_const(&self) -> bool` const fn.
///
/// # Compile-fail spec (negative coverage)
///
/// All examples below intentionally fail to compile. They pin the
/// derive's tier-1 rejection contract — without these doctests, a
/// future macro relaxation that silently accepts an unsupported
/// shape would not be caught.
///
/// **Generic struct** — rejected at derive entry (would need
/// generic impl shape, out of scope for v1.0):
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// struct WithGeneric<T> { x: T }
/// ```
///
/// **Enum** — no canonical pristine semantic for sum types:
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// enum E { A, B }
/// ```
///
/// **Union** — same rejection class as enum:
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// union U { a: u32, b: u32 }
/// ```
///
/// **Tuple struct** — lacks named fields needed for per-field
/// dispatch:
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// struct Tup(u32, bool);
/// ```
///
/// **Unsupported field type — float** (forbid-bundle bans `float_cmp`
/// at the derive's expansion site, plus floats have no clean
/// "pristine" definition w/r/t NaN):
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// struct WithFloat { x: f32 }
/// ```
///
/// **Unsupported field type — reference** (cannot inspect borrowed
/// content for pristine):
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// struct WithRef<'a> { x: &'a u32 }
/// ```
///
/// **Unsupported field type — tuple field** (no per-field
/// inspection within tuples):
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// #[derive(Pristine)]
/// struct WithTuple { pair: (u32, bool) }
/// ```
///
/// **Unsupported field type — `NonZeroU64`** (semantically never
/// zero — pristine == 0 doesn't apply; caller must hand-roll):
///
/// ```compile_fail
/// use bsql_postgres_proto::Pristine;
/// use core::num::NonZeroU64;
/// #[derive(Pristine)]
/// struct WithNonZero { x: NonZeroU64 }
/// ```
///
/// # Generated code shape
///
/// For a struct with named fields, generates:
///
/// ```text
/// impl ::bsql_postgres_proto::pristine::Pristine for #StructName {
///     fn is_pristine(&self) -> bool {
///         <per-field check 1> && <per-field check 2> && ...
///     }
/// }
///
/// impl #StructName {
///     #[inline]
///     #[must_use]
///     pub const fn __pristine_const(&self) -> bool {
///         <per-field check 1> && <per-field check 2> && ...
///     }
/// }
/// ```
///
/// The inherent `__pristine_const` exists alongside the trait impl
/// because trait methods cannot be `const` on stable Rust (as of
/// MSRV 1.95). Compile-time pristine assertions
/// (`const _: () = assert!(EMPTY.__pristine_const())`) use the
/// inherent path; runtime polymorphic dispatch uses the trait method.
///
/// # Per-field check synthesis
///
/// Each field's check is emitted based on type inspection:
///
/// | Field type | Generated check | Const-fn safe? |
/// |---|---|---|
/// | `Option<T>` (any T) | `self.field.is_none()` | yes — `Option::is_none` is `const fn` since 1.48 |
/// | `bool` | `!self.field` | yes — `!` on `bool` is const-evaluable |
/// | Integer types (u8/u16/u32/u64/u128/usize, i8/i16/i32/i64/i128/isize) | `self.field == 0` | yes — primitive `==` is const-evaluable |
///
/// **Unsupported field types fail compile** with a clear error
/// pointing at the offending field. Currently rejected:
/// - Struct types (would require `ConstDefault` trait, not stable).
/// - Floats (forbid-bundle bans `float_cmp`).
/// - Enums (would require per-variant pristine semantics — caller
///   should hand-roll).
/// - References / pointers (no meaningful "pristine" semantic).
///
/// # Error reporting
///
/// All errors emit `compile_error!` with the field name and the
/// failing constraint. Errors are **scoped** — the generated impl is
/// produced only when ALL fields pass; otherwise pure
/// `compile_error!` output, no half-impl that confuses the user.
#[proc_macro_derive(Pristine)]
pub fn derive_pristine(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_pristine_impl(&input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Inner implementation returning `syn::Result<TokenStream2>` so
/// errors propagate via `?` and convert to `compile_error!` at the
/// public entry point.
fn derive_pristine_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = &input.ident;

    // Reject non-struct, non-named-fields, generics with type/lifetime
    // params (would require generic impl shape — out of scope for v1.0
    // since SessionParams has no generics).
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Pristine derive does not support generic types in v1.0 — \
             SessionParams (the only current consumer) has no generics. \
             Open an issue if a use case lands.",
        ));
    }

    let fields = match &input.data {
        Data::Struct(DataStruct { fields: Fields::Named(named), .. }) => &named.named,
        Data::Struct(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "Pristine derive requires a struct with named fields \
                 (tuple structs and unit structs lack the field-name \
                 introspection needed to emit per-field checks).",
            ));
        }
        Data::Enum(_) | Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "Pristine derive supports only structs (enums and \
                 unions have no canonical \"pristine\" definition; \
                 hand-roll the trait impl for those shapes).",
            ));
        }
    };

    // Synthesise per-field checks. Errors are accumulated so the user
    // sees ALL unsupported fields at once, not just the first.
    let mut checks: Vec<TokenStream2> = Vec::with_capacity(fields.len());
    let mut errors: Vec<syn::Error> = Vec::new();

    for field in fields {
        match synthesise_check(field) {
            Ok(check) => checks.push(check),
            Err(e) => errors.push(e),
        }
    }

    if !errors.is_empty() {
        // Combine all errors into one — `syn::Error` supports
        // `combine` to chain them.
        let combined = errors
            .into_iter()
            .reduce(|mut acc, e| {
                acc.combine(e);
                acc
            });
        return match combined {
            Some(e) => Err(e),
            None => Ok(TokenStream2::new()),
        };
    }

    // Combine checks with &&. Empty-fields struct → trivially pristine
    // (true). This is the correct neutral element for conjunction.
    let body = if checks.is_empty() {
        quote! { true }
    } else {
        quote! { #( #checks )&&* }
    };

    let expanded = quote! {
        impl ::bsql_postgres_proto::pristine::Pristine for #struct_name {
            #[inline]
            fn is_pristine(&self) -> bool {
                #body
            }
        }

        impl #struct_name {
            /// Compile-time pristine check, generated by
            /// `#[derive(Pristine)]`. Equivalent to the
            /// [`::bsql_postgres_proto::pristine::Pristine::is_pristine`]
            /// trait method but `const fn` for use in `const _: () =
            /// assert!(...)` pin contexts.
            ///
            /// Auto-generated. **Do not modify by hand** — rebuild the
            /// derive instead.
            #[inline]
            #[must_use]
            #[doc(hidden)]
            pub const fn __pristine_const(&self) -> bool {
                #body
            }
        }
    };

    Ok(expanded)
}

/// Inspect a field's type and emit the appropriate pristine check.
///
/// Returns `Err` if the field type is not in the supported set.
fn synthesise_check(field: &Field) -> syn::Result<TokenStream2> {
    let name = field.ident.as_ref().ok_or_else(|| {
        syn::Error::new_spanned(
            field,
            "Pristine derive: unnamed field encountered (tuple-struct \
             support is rejected at the struct level — this is a \
             defensive guard in case syn parses an exotic shape).",
        )
    })?;

    if is_option(&field.ty) {
        return Ok(quote! { self.#name.is_none() });
    }

    if is_bool(&field.ty) {
        return Ok(quote! { !self.#name });
    }

    if is_integer(&field.ty) {
        return Ok(quote! { self.#name == 0 });
    }

    // PhantomData<T> is a ZST — no runtime data, always trivially
    // pristine. Future-proofs the derive for state-machine types
    // with phantom markers (e.g. `ReplyId<K>`'s
    // `PhantomData<fn() -> K>` pattern). PhantomData is exempt from
    // `dead_code` lint regardless of access, so the check body is
    // simply the constant `true` — no `let _ = &self.#name` placeholder
    // needed (and the let-underscore form is banned crate-wide).
    if is_phantom_data(&field.ty) {
        return Ok(quote! { true });
    }

    Err(syn::Error::new_spanned(
        &field.ty,
        format!(
            "Pristine derive: field `{}` has unsupported type. \
             Supported: Option<T>, bool, integer (u8/u16/u32/u64/u128/usize, \
             i8/i16/i32/i64/i128/isize), PhantomData<T>. Hand-roll \
             the trait impl for other shapes.",
            name,
        ),
    ))
}

/// True iff the type is `Option<T>` (any T) at the syntactic level.
///
/// Accepts both `Option<T>` and `core::option::Option<T>` /
/// `std::option::Option<T>` (the canonical fully-qualified paths).
/// This is a syntactic check, not semantic — a `type MyOption<T> =
/// Option<T>` alias would not be matched. The `SessionParams`
/// canonical use site uses bare `Option<T>` so this is sufficient.
fn is_option(ty: &Type) -> bool {
    if let Type::Path(p) = ty
        && let Some(seg) = p.path.segments.last()
    {
        return seg.ident == "Option";
    }
    false
}

/// True iff the type is `bool`.
fn is_bool(ty: &Type) -> bool {
    if let Type::Path(p) = ty
        && let Some(seg) = p.path.segments.last()
    {
        return seg.ident == "bool";
    }
    false
}

/// True iff the type is one of Rust's primitive integer types.
fn is_integer(ty: &Type) -> bool {
    if let Type::Path(p) = ty
        && let Some(seg) = p.path.segments.last()
    {
        let s = seg.ident.to_string();
        return matches!(
            s.as_str(),
            "u8" | "u16" | "u32" | "u64" | "u128" | "usize"
            | "i8" | "i16" | "i32" | "i64" | "i128" | "isize"
        );
    }
    false
}

/// True iff the type is `PhantomData<T>` (any T) at the syntactic
/// level. Same syntactic-only caveat as [`is_option`] — accepts
/// `PhantomData<T>` / `core::marker::PhantomData<T>` /
/// `std::marker::PhantomData<T>` regardless of namespace path,
/// matches by last-segment ident only.
fn is_phantom_data(ty: &Type) -> bool {
    if let Type::Path(p) = ty
        && let Some(seg) = p.path.segments.last()
    {
        return seg.ident == "PhantomData";
    }
    false
}

// ═════════════════════════════════════════════════════════════════════
// `prepared!` proc-macro (function-like).
//
// Closes the SQL-injection class as a category that does not compile:
// the only path to construct a `PreparedQuery<P, R>` is through this
// macro; the only path to execute one is
// `ReadyGuard::execute_prepared`; `PreparedQuery`'s fields are
// `pub(crate)` so direct struct initialisation from external crates
// is impossible (E0451).
//
// Hostile-bypass probes P1-P12 are pinned via `trybuild` files under
// `tests/prepared_macro_spec/`.
// ═════════════════════════════════════════════════════════════════════

/// Compile-time prepared PostgreSQL query.
///
/// Accepts ONE argument: a string literal containing SQL with
/// explicit cast annotations. Expands to a `const`-eligible struct
/// literal of `bsql_postgres_proto::PreparedQuery` with parameter and
/// row types pinned at the type level.
///
/// # Tier-1 SQL-injection closure
///
/// The macro accepts only `syn::LitStr` input. A runtime string
/// (`prepared!(some_var)`) is a different token-stream shape and is
/// rejected with `compile_error!` at expansion. The `compile_error!`
/// points at the offending token.
///
/// **Note on `concat!`**: proc-macros see their arguments as raw
/// token-streams BEFORE other macros expand, so
/// `prepared!(concat!("a", "b"))` is rejected. Use a single string
/// literal: `prepared!("ab")`. Future versions may support
/// `concat!`-style composition via custom evaluation.
///
/// # Required SQL shape
///
/// Every `$N` placeholder and every SELECT/RETURNING column MUST
/// carry an explicit cast annotation — either postfix `expr::TYPE`
/// or prefix `CAST(expr AS TYPE)`. Un-annotated placeholders / columns
/// emit `compile_error!` pointing at the offending byte.
///
/// # Supported PG type names (v1.0)
///
/// Lowercase ASCII only. Six OIDs with full text-format decode +
/// encode coverage in the runtime crate:
///
/// | PG name | Rust type      | OID | Notes                       |
/// |---------|----------------|-----|-----------------------------|
/// | `int2`  | `i16`          | 21  | smallint                    |
/// | `int4`  | `i32`          | 23  | integer                     |
/// | `int8`  | `i64`          | 20  | bigint                      |
/// | `oid`   | `u32`          | 26  | object identifier           |
/// | `bool`  | `bool`         | 16  | boolean                     |
/// | `text`  | `&'static str` | 25  | UTF-8 text                  |
///
/// Wider type coverage (`bytea`, `varchar`, `float4`, `float8`,
/// `timestamp`, `timestamptz`, `date`, `time`, `numeric`, `uuid`,
/// `jsonb`, `interval`, ...) is gated on each type's
/// `DecodeFormat<TextFmt>` + `EncodeBinary` impl landing in the
/// runtime crate first. The restriction is honest engineering:
/// macro-expand-time rejection is the load-bearing tier-1 line —
/// the type table grows in lockstep with runtime-side impls.
///
/// # Statement shapes
///
/// Accepts SELECT / INSERT / UPDATE / DELETE / WITH. DDL (CREATE /
/// DROP / ALTER) is rejected.
///
/// # Examples
///
/// ```ignore
/// use bsql_postgres_proto::{prepared, PreparedQuery};
///
/// const Q_USER_BY_ID: PreparedQuery<(i32,), (i32, &'static str)> = prepared!(
///     "SELECT id::int4, name::text FROM users WHERE id = $1::int4"
/// );
///
/// const Q_INSERT_RET: PreparedQuery<(&'static str,), (i32,)> = prepared!(
///     "INSERT INTO users (name) VALUES ($1::text) RETURNING id::int4"
/// );
/// ```
///
/// # Span discipline
///
/// All diagnostics emitted by this macro carry spans pointing at the
/// originating bytes in the SQL string (when possible — for
/// classification-level errors the span falls back to the
/// macro-call site). `trybuild` golden files pin diagnostics
/// word-for-word.
#[proc_macro]
pub fn prepared(input: TokenStream) -> TokenStream {
    prepared_impl(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Inner implementation returning `syn::Result<TokenStream2>`. Each
/// failure path emits `compile_error!` via the wrapper above.
fn prepared_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    // Only accept a single string-literal token. Any other shape —
    // including a const ident, a bare identifier, a function call —
    // is rejected with a clear diagnostic. `concat!("...")` works
    // because `concat!` expands to a `LitStr` at the macro-call site.
    let lit: syn::LitStr = syn::parse2(input).map_err(|_| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "prepared!: SQL must be a single string literal (not an \
             identifier, expression, or macro invocation). Use \
             `prepared!(\"SELECT ...\")`. Note: `concat!(\"a\", \"b\")` is \
             NOT accepted at the proc-macro level — proc-macros see their \
             arguments as raw token-streams before other macros expand. \
             Use one literal.",
        )
    })?;
    let sql_string = lit.value();
    let sql_span = lit.span();

    // Phase 1 — tokenise.
    let tokens = sql_lexer::tokenise(&sql_string).map_err(|err| {
        // We have a byte offset; encode it into the message so the
        // user can locate the issue. Span granularity inside a
        // `LitStr` is rustc-version-dependent (post-2024 `proc_macro::Span::byte_range`
        // would allow exact pointing); for stable 1.95 use the
        // whole-literal span with the offset in the message.
        let msg = alloc::format!(
            "{} (at byte offset {} in SQL literal)",
            err.message, err.byte_offset
        );
        syn::Error::new(sql_span, msg)
    })?;

    // Phase 2 — extract placeholders + columns + statement shape.
    let (params, columns, _shape) = extract::extract(&sql_string, &tokens, sql_span)?;

    // P11 closure — content-addressed stmt_name via SHA-256-96. The
    // 24-hex-char truncation gives a 2⁻⁹⁶ collision space, effectively
    // tier-1 for any realistic codebase. Memo §7 P11.
    let stmt_name = sha256_96_stmt_name(&sql_string);

    // Phase 3 typemap already ran inside extract() (every placeholder
    // and column carries its `rust_type` + `oid_path` token stream).

    // Phase 6 — assemble wire templates + struct literal.
    let parse_template_bytes = build_parse_template_bytes(&stmt_name, &sql_string, &params);
    let bind_execute_prefix_bytes = build_bind_execute_prefix_bytes(&stmt_name, &params);

    // Build the tuple types `(P1, P2, ...)` / `(R1, R2, ...)`.
    let params_tuple = build_tuple_type(params.iter().map(|p| &p.rust_type));
    let rows_tuple = build_tuple_type(columns.iter().map(|c| &c.rust_type));

    // OID arrays (compile-time `[u32; N]`).
    let param_oids_iter = params.iter().map(|p| &p.oid_path);
    let row_oids_iter = columns.iter().map(|c| &c.oid_path);

    // Static byte arrays — emitted as `&'static [u8]` references to
    // private const items so the macro keeps its visibility surface
    // tight. `#[doc(hidden)] const` items live in user binary's
    // `.rodata`.
    let parse_template_lit = byte_array_literal(&parse_template_bytes);
    let bind_execute_prefix_lit = byte_array_literal(&bind_execute_prefix_bytes);

    // The macro emits a *block expression* that evaluates to a
    // `PreparedQuery<P, R>`. The user binds it to a `const`:
    //
    //   const Q: PreparedQuery<(i32,), (i32,)> = prepared!(...);
    //
    // All const items inside the block are inlined into the consumer
    // crate's `.rodata`; the struct literal references them via
    // `&Self::PARSE_TEMPLATE` patterns. To keep the names from
    // colliding with user identifiers we hide them under doubly-
    // underscored names (hygiene convention for macro internals).
    let expanded = quote! {
        {
            #[doc(hidden)]
            const __BSQL_PREPARED_PARAM_OIDS: &[u32] = &[ #( #param_oids_iter ),* ];
            #[doc(hidden)]
            const __BSQL_PREPARED_ROW_OIDS: &[u32] = &[ #( #row_oids_iter ),* ];
            #[doc(hidden)]
            const __BSQL_PREPARED_PARSE_TEMPLATE: &[u8] = #parse_template_lit;
            #[doc(hidden)]
            const __BSQL_PREPARED_BIND_EXECUTE_PREFIX: &[u8] = #bind_execute_prefix_lit;
            ::bsql_postgres_proto::prepared::new_prepared_query::<
                #params_tuple,
                #rows_tuple,
            >(
                #lit,
                #stmt_name,
                __BSQL_PREPARED_PARAM_OIDS,
                __BSQL_PREPARED_ROW_OIDS,
                __BSQL_PREPARED_PARSE_TEMPLATE,
                __BSQL_PREPARED_BIND_EXECUTE_PREFIX,
            )
        }
    };
    Ok(expanded)
}

/// Build a `quote!`-able tuple type token stream from an iterator
/// of Rust type tokens. Arity 0 → `()`; arity 1 → `(T0,)`;
/// arity ≥ 2 → `(T0, T1, ...)`. Tier-1 ensures the trailing comma
/// on singletons (Rust requires `(T,)` not `(T)`).
fn build_tuple_type<'a, I>(types: I) -> TokenStream2
where
    I: IntoIterator<Item = &'a TokenStream2>,
{
    let items: alloc::vec::Vec<&TokenStream2> = types.into_iter().collect();
    match items.as_slice() {
        [] => quote! { () },
        [single] => quote! { ( #single , ) },
        many => quote! { ( #( #many ),* ) },
    }
}

/// SHA-256 of the SQL bytes truncated to 96 bits → 24 hex chars →
/// statement name `bsql_p_<24hex>`. 96-bit collision space supports
/// content-addressed stmt-cache reuse (P11 hostile-bypass closure).
fn sha256_96_stmt_name(sql: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(sql.as_bytes());
    // Take the first 12 bytes (96 bits). hex-encode into 24 chars.
    let mut name = String::with_capacity(32);
    name.push_str("bsql_p_");
    for byte in digest.iter().take(12) {
        // Manual hex emit keeps the macro alloc-light. `format!("{:02x}", b)`
        // would also work but pulls in fmt machinery; this is build-time
        // code so the saving is on macro-expansion latency only.
        let hi = byte >> 4;
        let lo = byte & 0x0F;
        name.push(hex_char(hi));
        name.push(hex_char(lo));
    }
    name
}

#[inline]
fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0'.saturating_add(nibble)),
        10..=15 => char::from(b'a'.saturating_add(nibble.saturating_sub(10))),
        _ => '0',
    }
}

/// Build the byte array of the Parse frame template per PG §55.2.2:
///
/// ```text
/// b'P' | len_i32_be | stmt_name NUL | sql NUL | n_param_types_i16_be |
///   oid[0] i32_be | oid[1] i32_be | ...
/// ```
///
/// Length includes itself (PG convention: length is the i32-BE field
/// and the body up to but excluding the tag byte).
fn build_parse_template_bytes(
    stmt_name: &str,
    sql: &str,
    params: &[extract::ParamSpec],
) -> alloc::vec::Vec<u8> {
    let stmt_name_bytes = stmt_name.as_bytes();
    let sql_bytes = sql.as_bytes();
    let n_params_u16 = u16::try_from(params.len()).unwrap_or(0);
    // length = 4 (self) + stmt_name + 1 NUL + sql + 1 NUL + 2 (n_param_types)
    //         + 4 × n_param_types
    let length_usize = 4_usize
        .saturating_add(stmt_name_bytes.len())
        .saturating_add(1)
        .saturating_add(sql_bytes.len())
        .saturating_add(1)
        .saturating_add(2)
        .saturating_add(4_usize.saturating_mul(params.len()));
    let length_u32 = u32::try_from(length_usize).unwrap_or(u32::MAX);
    let mut out: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(length_usize.saturating_add(1));
    out.push(b'P');
    out.extend_from_slice(&length_u32.to_be_bytes());
    out.extend_from_slice(stmt_name_bytes);
    out.push(0);
    out.extend_from_slice(sql_bytes);
    out.push(0);
    out.extend_from_slice(&n_params_u16.to_be_bytes());
    // Phase 3 typemap doesn't expose the numeric OID — only a token
    // path. The macro's `oid_path` is a `quote!`-emitted const path
    // resolved at the consumer crate's expansion. For the Parse
    // template we need the actual byte values at MACRO time.
    //
    // Decision: emit per-OID bytes by RECOMPUTING the OID from the
    // type name in the typemap. Practical approach: extract.rs
    // records the OID-path token, but we ALSO need to know the numeric
    // OID at macro-expansion to bake the Parse template. Solution:
    // pre-resolve OIDs to numeric values inside extract.rs and pass
    // them through ParamSpec.
    //
    // Implementation: ParamSpec carries `oid_value: u32` alongside
    // the token form. Same for ColumnSpec.
    for spec in params {
        out.extend_from_slice(&spec.oid_value.to_be_bytes());
    }
    out
}

/// Build the byte array of the Bind frame's BODY prefix (everything
/// after the `'B'` tag + length, before the format-code block), per
/// PG §55.2.2:
///
/// ```text
/// portal_NUL | stmt_name_NUL
/// ```
///
/// - `portal_NUL` is the empty-portal sentinel (`0x00`).
/// - `stmt_name_NUL` is the macro's content-addressed stmt_name +
///   trailing NUL.
///
/// The param format-code block, `n_params`, the per-param values, and
/// the result-format trailer are ALL emitted at frame-build time from
/// the `ParamsWriter` impl of the argument tuple (`FORMATS`, `COUNT`,
/// `write_params`). The macro deliberately bakes NO format/count bytes:
/// the declared param formats and the actual value encoding must come
/// from ONE source so they cannot drift. Baking the format here would
/// re-state the encoding choice in a second place; the only place that
/// knows how a value is encoded is the encoder, so the encoder's trait
/// is also the sole declarer of the format on the wire.
fn build_bind_execute_prefix_bytes(
    stmt_name: &str,
    _params: &[extract::ParamSpec],
) -> alloc::vec::Vec<u8> {
    let stmt_name_bytes = stmt_name.as_bytes();
    let mut out: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(32);
    out.push(0); // empty portal NUL
    out.extend_from_slice(stmt_name_bytes);
    out.push(0); // stmt_name NUL
    out
}

/// Emit a `quote!`-able byte-array literal `&[u8]` from raw bytes.
fn byte_array_literal(bytes: &[u8]) -> TokenStream2 {
    // Each byte as a `u8` literal — `quote!` interpolates these
    // directly into the consumer crate's source code as a `const`
    // `[u8; N]` slice. LLVM hoists this into `.rodata` automatically.
    let byte_lits: alloc::vec::Vec<proc_macro2::Literal> = bytes
        .iter()
        .map(|b| proc_macro2::Literal::u8_unsuffixed(*b))
        .collect();
    quote! { &[ #( #byte_lits ),* ] }
}
