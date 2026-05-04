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
//!   for [`crate::pristine::Pristine`][bsql_pg_proto_pristine_link]
//!   trait impl on types like `SessionParams`. See `DEF-211 INNO-01`
//!   in `deferred.md` for the design rationale: lifts BS-11
//!   broad-scope tier-3 to tier-1 by-construction (compiler emits the
//!   field-by-field check; missing a new field is impossible).
//!
//! [bsql_pg_proto_pristine_link]: https://docs.rs/bsql-pg-proto
//!
//! # Path resolution invariant
//!
//! Generated code references `::bsql_pg_proto::pristine::Pristine`
//! through an absolute path. The `bsql-pg-proto` crate adds
//! `extern crate self as bsql_pg_proto;` at its lib root so this
//! path resolves both:
//!
//! - **Inside `bsql-pg-proto` itself** — when `#[derive(Pristine)]`
//!   is applied to types defined in `bsql-pg-proto` (e.g.
//!   `SessionParams`); the `self` aliasing makes
//!   `::bsql_pg_proto::pristine::Pristine` resolve to the local trait.
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

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Field, Fields, Type};

/// `#[derive(Pristine)]` — derives the
/// `bsql_pg_proto::pristine::Pristine` trait impl plus an inherent
/// `__pristine_const(&self) -> bool` const fn.
///
/// # Generated code shape
///
/// For a struct with named fields, generates:
///
/// ```text
/// impl ::bsql_pg_proto::pristine::Pristine for #StructName {
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
        impl ::bsql_pg_proto::pristine::Pristine for #struct_name {
            #[inline]
            fn is_pristine(&self) -> bool {
                #body
            }
        }

        impl #struct_name {
            /// Compile-time pristine check, generated by
            /// `#[derive(Pristine)]`. Equivalent to the
            /// [`::bsql_pg_proto::pristine::Pristine::is_pristine`]
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

    Err(syn::Error::new_spanned(
        &field.ty,
        format!(
            "Pristine derive: field `{}` has unsupported type. \
             Supported: Option<T>, bool, integer (u8/u16/u32/u64/u128/usize, \
             i8/i16/i32/i64/i128/isize). Hand-roll the trait impl for \
             other shapes.",
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
