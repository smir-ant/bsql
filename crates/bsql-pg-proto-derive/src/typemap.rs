//! PG type-name → Rust type token mapping.
//!
//! `bsql-pg-proto`'s `DecodeFormat + EncodeBinary` matrix supports
//! six types end-to-end (`i16`, `i32`, `i64`, `u32`, `bool`, `&str`).
//! The macro maps:
//!
//! | PG name in SQL cast | Rust type token   | OID const path                         |
//! |---------------------|-------------------|----------------------------------------|
//! | `int2`              | `i16`             | `::bsql_pg_proto::oids::INT2`          |
//! | `int4`              | `i32`             | `::bsql_pg_proto::oids::INT4`          |
//! | `int8`              | `i64`             | `::bsql_pg_proto::oids::INT8`          |
//! | `oid`               | `u32`             | `::bsql_pg_proto::oids::OID`           |
//! | `bool`              | `bool`            | `::bsql_pg_proto::oids::BOOL`          |
//! | `text`              | `&'static str`    | `::bsql_pg_proto::oids::TEXT`          |
//!
//! # Tier discipline
//!
//! Honest mapping: only types with full decode+encode coverage in
//! the runtime crate are accepted. Pretending to support `bytea` /
//! `varchar` / `timestamp` / etc. when they have no `DecodeFormat`
//! impl would compile the macro but fail at runtime when the
//! generated `R::decode` body referenced a missing trait impl. The
//! macro instead emits `compile_error!` at expansion with a
//! type-list pointing the user at supported names.
//!
//! Wider type coverage lands as `DecodeFormat<TextFmt>` impls for
//! `&[u8]` / `f32` / `f64` / etc. ship in the runtime crate. This
//! table grows in lockstep — single source of truth for the
//! type↔OID↔Rust mapping.
//!
//! # Why static `&'static str` (and not `String` or owned)
//!
//! The macro generates `const Q: PreparedQuery<...>` items. Every
//! field of `PreparedQuery` MUST be `const`-eligible — owned `String`
//! is not. `&'static str` lives in `.rodata` of the consumer binary
//! and satisfies the `const` constraint.

extern crate alloc;
use alloc::string::String;
use proc_macro2::{Span, TokenStream};
use quote::quote;

/// Result of looking up a PG type name in the macro's table.
pub(crate) struct TypeMapEntry {
    /// Rust type token (e.g., `quote!(i32)`).
    pub(crate) rust_type: TokenStream,
    /// PG OID const path token (e.g., `quote!(::bsql_pg_proto::oids::INT4)`).
    /// Emitted into the consumer crate so a drift between `oids::*`
    /// and the literal `oid_value` would surface via the runtime
    /// crate's const-asserts against `pg_type.dat`.
    pub(crate) oid_path: TokenStream,
    /// Numeric OID value resolved at macro-expansion time. Used to
    /// bake the Parse-frame `n_param_types` body — Parse OIDs MUST
    /// be in the static `.rodata` template, so we need
    /// the integer at macro-expand time, not just the token-path.
    /// Drift-pinned: each branch's `oid_value` matches the path's
    /// PG-catalog constant (pinned in `bsql-pg-proto::decode::oids`
    /// via `assert!(BOOL == 16)` etc.).
    pub(crate) oid_value: u32,
}

/// Look up a PG type name (lowercase ASCII, as appears after a `::`
/// cast or `AS TYPE` keyword) and return the matched Rust type
/// token + OID const path.
///
/// Returns `Err(syn::Error)` with a span-attached diagnostic on
/// unsupported names. The diagnostic enumerates the currently
/// supported set so the user can fix the SQL immediately.
///
/// # Case sensitivity
///
/// v1 accepts **lowercase only**: `int4`, not `INT4`. The caller
/// (extractor)
/// is responsible for converting the source bytes to lowercase if
/// it wants case-insensitive matching; this function does NOT fold
/// — that ambiguity belongs in the extractor, not the type table.
pub(crate) fn lookup_pg_type(name_ascii_lower: &str, span: Span) -> syn::Result<TypeMapEntry> {
    // Drift-pin: `oid_value` literals MUST equal the
    // `bsql_pg_proto::decode::oids::*` constants. Those are
    // build-time asserted against `pg_type.dat` in the runtime
    // crate; a drift here would surface at macro-expansion time
    // ONLY if the runtime crate's `oid_value` differs (e.g. INT4
    // changed from 23 to 32) — but the runtime `const _: () = assert!(INT4 == 23)`
    // (decode.rs:2883) makes that a runtime-crate build failure
    // FIRST. Belt-and-braces: catastrophic desync = build failure.
    let (rust_type, oid_path, oid_value) = match name_ascii_lower {
        "int2" => (
            quote! { i16 },
            quote! { ::bsql_pg_proto::oids::INT2 },
            21_u32,
        ),
        "int4" => (
            quote! { i32 },
            quote! { ::bsql_pg_proto::oids::INT4 },
            23_u32,
        ),
        "int8" => (
            quote! { i64 },
            quote! { ::bsql_pg_proto::oids::INT8 },
            20_u32,
        ),
        "oid" => (
            quote! { u32 },
            quote! { ::bsql_pg_proto::oids::OID },
            26_u32,
        ),
        "bool" => (
            quote! { bool },
            quote! { ::bsql_pg_proto::oids::BOOL },
            16_u32,
        ),
        "text" => (
            quote! { &'static str },
            quote! { ::bsql_pg_proto::oids::TEXT },
            25_u32,
        ),
        // Decoder/encoder support for wider types is pending in the
        // runtime crate. Once `DecodeFormat<TextFmt>` impls land for
        // additional types in `bsql-pg-proto`, expand this table in
        // lockstep.
        //
        // Diagnostic shape rationale:
        //   - Lead with the rejected type name in backticks so it
        //     stands out in IDE squiggles.
        //   - Enumerate the v1.0 supported set with their Rust
        //     mapping in parentheses (the user wants to know "what
        //     CAN I use here?", not just "what failed").
        //   - Group future-coverage types into a single bracketed
        //     line so the user sees the scope at a glance.
        other => {
            let mut msg = String::with_capacity(384);
            msg.push_str("prepared!: unsupported PG type `");
            msg.push_str(other);
            msg.push_str(
                "`.\n\n\
                 Supported in v1.0 (6 OIDs with full text-format \
                 decode + encode coverage):\n\
                 \x20\x20- `int2` → `i16` (OID 21)\n\
                 \x20\x20- `int4` → `i32` (OID 23)\n\
                 \x20\x20- `int8` → `i64` (OID 20)\n\
                 \x20\x20- `oid`  → `u32` (OID 26)\n\
                 \x20\x20- `bool` → `bool` (OID 16)\n\
                 \x20\x20- `text` → `&'static str` (OID 25)\n\n\
                 Why the restriction: the runtime crate's \
                 `DecodeFormat<TextFmt>` + `EncodeBinary` matrix \
                 currently implements only these six types. Adding \
                 a type here without the runtime decoder/encoder \
                 would compile the macro but fail at runtime when \
                 the generated `R::decode` body references a missing \
                 trait impl — that's why we reject at macro-expand.\n\n\
                 Wider type coverage [bytea / varchar / bpchar / \
                 name / float4 / float8 / numeric / timestamp / \
                 timestamptz / date / time / uuid / jsonb / interval] \
                 lands as the missing `DecodeFormat<TextFmt>` impls \
                 ship in the runtime crate. This table grows in \
                 lockstep — single source of truth for the type ↔ \
                 OID ↔ Rust-type mapping.",
            );
            return Err(syn::Error::new(span, msg));
        }
    };
    Ok(TypeMapEntry { rust_type, oid_path, oid_value })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every supported name resolves; case must be lowercase.
    #[test]
    fn lookup_supported_names() {
        let span = Span::call_site();
        for name in ["int2", "int4", "int8", "oid", "bool", "text"] {
            assert!(
                lookup_pg_type(name, span).is_ok(),
                "expected {name} to resolve",
            );
        }
    }

    /// Uppercase is NOT accepted (v1 lowercase-only).
    #[test]
    fn lookup_uppercase_rejected() {
        let span = Span::call_site();
        assert!(lookup_pg_type("INT4", span).is_err());
        assert!(lookup_pg_type("Int4", span).is_err());
    }

    /// Future-coverage types are rejected at expansion until their
    /// runtime decoder/encoder support lands.
    #[test]
    fn lookup_pending_types_rejected() {
        let span = Span::call_site();
        for name in ["bytea", "varchar", "bpchar", "name", "float4", "float8",
                     "timestamp", "timestamptz", "uuid", "jsonb", "numeric", "char"] {
            assert!(
                lookup_pg_type(name, span).is_err(),
                "expected {name} to be rejected (runtime support pending)",
            );
        }
    }

    /// Empty / garbage names are rejected.
    #[test]
    fn lookup_garbage_rejected() {
        let span = Span::call_site();
        assert!(lookup_pg_type("", span).is_err());
        assert!(lookup_pg_type("not_a_pg_type", span).is_err());
    }
}
