//! Proc-macro foundation for bsql's compile-checked query API.
//!
//! This crate hosts [`query!`](query) — the compile-checked query macro.
//! It reads the schema catalog that `bsql-build` generates at build time
//! and types each SQL query against the schema replayed from the
//! consumer's migration DDL. An unknown table or column — or any query
//! that does not type-check — is a `compile_error!`.
//!
//! # The build.rs -> macro channel
//!
//! The consumer's `build.rs` calls `bsql_build::emit_catalog(..)`, which
//! writes the catalog to `OUT_DIR` and sets
//! `cargo:rustc-env=BSQL_SCHEMA_CATALOG=<abs path>`. Cargo injects
//! rustc-env vars into the rustc invocation that compiles the consumer;
//! this proc-macro runs inside that rustc, so it reads the path via
//! `std::env::var("BSQL_SCHEMA_CATALOG")` at expansion and opens the
//! file.
//!
//! # Fail-closed (no stale-cache blind spot)
//!
//! If the env var is absent (the consumer forgot the `build.rs`), or the
//! catalog file is unreadable, the macro emits a `compile_error!` — it
//! NEVER passes a query against a missing or stale catalog. A silent
//! pass there would BE the stale-schema blind spot this whole design
//! exists to eliminate. Freshness is guaranteed upstream by the
//! `cargo:rerun-if-changed` on the migrations directory (membership) and
//! each file (content), so the catalog this macro reads is always
//! regenerated from the current migrations.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget
)]
#![deny(unused_must_use, unused_lifetimes, missing_docs)]
// This crate keeps the workspace tier-4 `disallowed_methods` ledger
// (`unwrap_or*` / `map_or*`) fully ENABLED: it uses none of those
// silent-fallback shapes. A missing or unreadable catalog is a hard
// `compile_error!`, and the only `Option`/`Result` handling here goes
// through explicit `match` / `?`, never a default-substituting combinator.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::SystemTime;

use proc_macro::TokenStream;
use proc_macro2::{Literal, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};

/// The rustc-env variable that `bsql-build` sets to the catalog's path.
const CATALOG_ENV_VAR: &str = "BSQL_SCHEMA_CATALOG";

/// The rustc-env variable `bsql_build::CatalogBuilder::emit` sets to the
/// external-type bridge file's path. Absent when the consumer uses the plain
/// `emit` / `emit_catalog` free functions (no bridges — the native types).
const BRIDGES_ENV_VAR: &str = "BSQL_TYPE_BRIDGES";

// ════════════════════════════════════════════════════════════════════
// query! — typed-record twins + DataRow-payload decode
// ════════════════════════════════════════════════════════════════════
//
// `query!(Name, "<SQL>")` types one SQL query against the schema replayed
// from the consumer's migrations and emits two typed records plus their
// decoders, the const wire artifact, and the `TypedQuery` execution bridge.
// Expansion calls `bsql_build::infer_query` (the build-time SQL inference
// engine) to obtain the output row shape and the `$N` parameter types. The
// EMITTED code references ONLY `::bsql::__rt::` paths — the umbrella crate's
// hidden re-export of the shipped runtime primitives (`DataRowRef`,
// `Cell<BinaryFmt>`, `DecodeError`, `QueryFingerprint`, `PreparedQuery`,
// `TypedQuery`, the OID / query-budget constants, and the `wire_pin!`
// footprint guard). A consumer therefore depends on `bsql` (with
// `features = ["macros"]`) and nothing else to reach the flagship, and its
// runtime closure carries no build-time query toolchain: `bsql-query-macros`
// is a host-only proc-macro and never emits a reference to itself or to
// `bsql-build`.

/// Compile-checked, schema-typed query record generator.
///
/// `query!(Name, "<SQL>")` parses the SQL string literal at expansion,
/// infers its output row shape and parameter types against the schema
/// replayed from the consumer's migration DDL (via the build-generated
/// catalog), and emits two typed-record types plus their decoders:
///
/// * `Name` — the borrowed, zero-copy record: one field per projected
///   output column, where a `text` column borrows the input bytes as
///   `&str` (so the borrowed decode allocates nothing). It carries a
///   `<'q>` lifetime only when it has a borrowing (`text`) field.
/// * `NameOwned` — the owned twin: the same fields, but `text` columns
///   are `String`.
///
/// A `NOT NULL` column maps to `T`; a nullable column maps to
/// `Option<T>`. Each type has a `decode` associated fn that turns a raw
/// `DataRow` payload (the wire bytes after the field-count header) into
/// the record, reusing the shipped binary decoders re-exported by the
/// umbrella crate. A `NULL` arriving in a `NOT NULL`-typed column
/// is a classified `DecodeError::NullInNonNullColumn`, never a silent
/// default or a panic.
///
/// Any SQL that does not type-check — an unknown table/column, a
/// duplicate output column name, or an expression/parameter whose type
/// cannot be inferred without an explicit cast — is a `compile_error!`
/// carrying the inference engine's message.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    match query_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Generate a Rust type for every user-defined PostgreSQL type declared in the
/// consumer's migrations — with ZERO derives, ZERO OID annotations, and no
/// hand-maintained `type_name`.
///
/// `bsql::user_types!()` reads the SAME build catalog `query!` types against and
/// emits, for each `CREATE TYPE name AS ENUM ('a', 'b', ...)` migration, a
/// public Rust `enum` (`enum Name { A, B }`, variants in the declared order —
/// which is PostgreSQL's enum sort order, so the derived `Ord` matches the
/// server's) plus its wire decode/encode. A `query!` selecting an enum column
/// decodes into that generated type; a variant renamed or deleted in a later
/// migration regenerates the type, and any code that named the old variant
/// stops compiling — drift is a BUILD error, not a runtime surprise.
///
/// Invoke it ONCE, in a module whose names are in scope at your `query!` call
/// sites (a `query!` names the generated type by its bare PascalCase name):
///
/// ```rust,ignore
/// bsql::user_types!();   // generates `enum Mood { Happy, Sad }`, etc.
///
/// bsql::query!(GetMood, "SELECT m FROM feelings WHERE id = $1");
/// // GetMood's `m` field is `Mood`; bind a `Mood` param with `mood.as_label()`.
/// ```
///
/// Takes no arguments. A migration whose type or a label cannot form a valid
/// Rust identifier, or two labels that PascalCase to the same variant, is a
/// loud `compile_error!` — never a silent mangle or collision.
#[proc_macro]
pub fn user_types(input: TokenStream) -> TokenStream {
    match user_types_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// `query!(Name, "SQL")` input: a record base name and a SQL string.
struct QueryInput {
    name: Ident,
    sql: syn::LitStr,
}

impl Parse for QueryInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let _comma: Token![,] = input.parse()?;
        let sql: syn::LitStr = input.parse()?;
        if !input.is_empty() {
            return Err(input.error(
                "query!: expected exactly `Name, \"SQL\"` — a record name, a \
                 comma, and one SQL string literal",
            ));
        }
        Ok(QueryInput { name, sql })
    }
}

fn query_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    let QueryInput { name, sql } = syn::parse2(input)?;
    let sql_span = sql.span();

    // Strip the recognized `/* bsql:allow-scan: <reason> */` directive (if
    // present) from the SQL before anything else, so it never reaches the
    // inference engine or the baked wire bytes (the content address stays
    // identical whether or not a query is acknowledged). Its presence is the
    // per-query acknowledgment of a known full-scan-on-toggle and is consumed
    // only by the SQLite conformance cross-check below.
    let (sql_text, scan_acknowledged) = strip_scan_ack(&sql.value());

    // Rebuild the catalog the consumer's build.rs wrote (fail-closed on a
    // missing/unreadable/corrupt catalog).
    let catalog = load_build_catalog(name.span())?;

    // The external-type bridges (possibly empty): each remaps a column of a
    // given native pivot type into a consumer-chosen target type via an
    // infallible converter free function. Fail-closed on a corrupt channel.
    let bridges = load_bridges(name.span())?;

    // Lower the dynamic sugar (`OPTIONAL(...)`, `= ANY($N)`, runtime
    // `ORDER BY { ... }`) and type the result against the schema. Any
    // failure — a malformed sugar marker, an unknown table/column, a
    // duplicate output column, an uncast expression or parameter — is
    // surfaced verbatim as a compile error pointed at the SQL literal.
    // There is no "assume a type" path. A query with NO dynamic sugar
    // lowers to itself byte-for-byte, so its wire artifact is identical to
    // the non-dynamic path.
    let shape = bsql_build::infer_dynamic_query(&catalog, &sql_text).map_err(|err| {
        // An unknown column / table names the missing symbol; if a known name
        // is within one typo (a restricted Damerau-Levenshtein match against
        // the queried table's columns / the catalog's table names), append it
        // so the fix is one glance away. No candidate within threshold leaves
        // the message exactly as-is — never a misleading guess.
        let message = match catalog.did_you_mean(&err) {
            Some(name) => format!("query!: {err} — did you mean `{name}`?"),
            None => format!("query!: {err}"),
        };
        syn::Error::new(sql_span, message)
    })?;

    // Resolve every user-defined enum the query references into its generated
    // Rust type identifier, ONCE (a migration enum whose name cannot form a
    // valid Rust identifier is a loud error here, not at each use site). The
    // generated enum TYPES themselves are emitted by the separate
    // `bsql::user_types!()` macro; `query!` only NAMES them (they must be in
    // scope at the call site, exactly as a hand-written record field type would
    // be).
    let enums = resolve_enum_types(&catalog, &shape, name.span())?;

    // The typed-record twins (borrowed + owned) and their decoders.
    let records = emit_records(&name, &shape.columns, &bridges, &enums)?;
    // The const wire artifact(s): the uninhabited fingerprint carrier(s),
    // their `QueryFingerprint` impl(s) (baked Parse / Bind-prefix templates
    // + OID lists, all derived from the lowered shape), the validated
    // `PreparedQuery` const(s) minted through the proto-owned `run`
    // boundary, the dynamic-form budget assertions, and — for a runtime
    // `ORDER BY` allow-set — the closed selector enum.
    let wire = emit_dynamic_wire(&name, &shape, &bridges, &enums)?;

    // SQLite conformance cross-check (only when this crate's `sqlite` feature
    // is on AND the consumer's build.rs emitted a template). It opens the
    // build-time SQLite template under a deny-all-but-readonly authorizer and
    // asserts real SQLite's `prepare`+decltype/table_info agree with the
    // lattice's per-column (type, nullable), and that no OPTIONAL toggle
    // forces an unacknowledged full-table scan. A disagreement is a loud
    // `compile_error!`.
    #[cfg(feature = "sqlite")]
    verify_sqlite(sql_span, &shape, scan_acknowledged)?;
    // Without the `sqlite` feature there is no SQLite target, so the
    // acknowledgment flag is consumed here (the directive was already
    // stripped from the SQL above). This is a plain bool, not a dropped
    // fallible result.
    #[cfg(not(feature = "sqlite"))]
    let _ = scan_acknowledged;

    // The SQLite typed-runtime bridge (`impl SqliteTypedQuery for {Name}Query`),
    // emitted ONLY when the umbrella's `sqlite` runtime driver is present (the
    // `sqlite-runtime` macro feature) AND the query is SQLite-decodable — every
    // projected column a SQLite storage class, unbridged, and no PostgreSQL-only
    // dynamic sugar. A non-decodable query emits nothing here, so calling it on
    // the SQLite driver is a located compile error, never a silent mis-decode.
    // Empty (`quote!()`) with the feature off, so the PostgreSQL-only expansion
    // is byte-identical to before.
    #[cfg(feature = "sqlite-runtime")]
    let sqlite_typed = emit_sqlite_typed(&name, &shape, &bridges, &enums)?;
    #[cfg(not(feature = "sqlite-runtime"))]
    let sqlite_typed = TokenStream2::new();

    Ok(quote! {
        #records
        #wire
        #sqlite_typed
    })
}

/// Generate the Rust types for every user-defined type in the build catalog.
/// Empty input; a stray token is a loud error. Currently emits one `enum` per
/// `CREATE TYPE ... AS ENUM` migration (composites/domains are additive).
fn user_types_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    if !input.is_empty() {
        return Err(syn::Error::new_spanned(
            input,
            "bsql::user_types!() takes no arguments",
        ));
    }
    let span = Span::call_site();
    let catalog = load_build_catalog(span)?;

    let mut items = Vec::new();
    for (type_name, ty) in &catalog.user_types {
        match ty {
            bsql_build::UserType::Enum { labels } => {
                items.push(emit_user_enum(type_name, labels, span)?);
            }
            // A domain is TRANSPARENT — it decodes/encodes as its base type, so
            // there is no generated Rust type: a `query!` column typed as the
            // domain resolves directly to the base's Rust type. Nothing to emit.
            bsql_build::UserType::Domain { .. } => {}
            // A composite generates a Rust `struct` with a row-type binary frame
            // decoder (`PgComposite`). It resolves its FIELD types against the
            // whole catalog, so it takes the catalog.
            bsql_build::UserType::Composite { fields } => {
                items.push(emit_user_composite(&catalog, type_name, fields, span)?);
            }
        }
    }
    Ok(quote! { #(#items)* })
}

/// Emit one generated Rust `enum` for a `CREATE TYPE name AS ENUM (...)`: the
/// type, its inherent `label` / `as_label` methods, and its
/// [`::bsql::__rt::PgEnum`] impl (the wire label⟷variant mapping, defined ONCE
/// here so `query!`'s decode and a bound parameter cannot disagree). The
/// variant order is the declared label order — PostgreSQL's enum sort order —
/// so the derived `Ord`/`PartialOrd` matches the server's ordering.
fn emit_user_enum(enum_name: &str, labels: &[String], span: Span) -> syn::Result<TokenStream2> {
    let type_ident = pascal_ident(enum_name, "enum type", span)?;

    // One validated variant identifier per label, with a loud collision guard:
    // two labels that PascalCase to the SAME variant (e.g. `a_b` and `a-b`) are
    // an ambiguous mapping, never a silent last-wins.
    let mut variant_idents: Vec<Ident> = Vec::with_capacity(labels.len());
    let mut seen: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for label in labels {
        let variant = pascal_ident(label, "enum variant", span)?;
        let key = variant.to_string();
        if let Some(first) = seen.get(&key) {
            return Err(syn::Error::new(
                span,
                format!(
                    "bsql::user_types!(): enum `{enum_name}` labels `{first}` and \
                     `{label}` both map to the Rust variant `{key}`. Rename one \
                     label in the migration so each maps to a distinct variant."
                ),
            ));
        }
        seen.insert(key, label.clone());
        variant_idents.push(variant);
    }

    // Parallel (variant, label) slices for the two match bodies. The labels are
    // interpolated as string literals (exact bytes, case-sensitive).
    let variants_for_encode = &variant_idents;
    let labels_for_encode = labels;
    let variants_for_decode = &variant_idents;
    let labels_for_decode = labels;

    let type_doc = format!(
        "The `{enum_name}` PostgreSQL enum, generated from the migration \
         `CREATE TYPE {enum_name} AS ENUM (...)`. Variants are in the declared \
         (PostgreSQL sort) order. Decode an `{enum_name}` column with `query!`; \
         bind one as a parameter with [`Self::as_label`]."
    );
    let dead_reason =
        "a generated user type may be referenced by any, all, or none of the crate's queries";

    Ok(quote! {
        #[doc = #type_doc]
        #[derive(
            ::core::fmt::Debug, ::core::clone::Clone, ::core::marker::Copy,
            ::core::cmp::PartialEq, ::core::cmp::Eq,
            ::core::cmp::PartialOrd, ::core::cmp::Ord, ::core::hash::Hash,
        )]
        #[allow(dead_code, reason = #dead_reason)]
        pub enum #type_ident {
            #( #variant_idents ),*
        }

        impl #type_ident {
            /// This value's PostgreSQL enum label (the exact declared text).
            #[allow(dead_code, reason = #dead_reason)]
            #[must_use]
            pub fn label(self) -> &'static str {
                <Self as ::bsql::__rt::PgEnum>::wire_label(self)
            }

            /// Bind this value as a `query!` parameter (an enum-typed label the
            /// server coerces from context — a PG enum has no `text` cast).
            #[allow(dead_code, reason = #dead_reason)]
            #[must_use]
            pub fn as_label(self) -> ::bsql::__rt::EnumLabel<Self> {
                ::bsql::__rt::EnumLabel::new(self)
            }
        }

        impl ::bsql::__rt::PgEnum for #type_ident {
            fn wire_label(self) -> &'static str {
                match self {
                    #( #type_ident::#variants_for_encode => #labels_for_encode, )*
                }
            }

            fn from_wire_label(
                __label: &str,
            ) -> ::core::result::Result<Self, ::bsql::__rt::DecodeError> {
                match __label {
                    #( #labels_for_decode =>
                        ::core::result::Result::Ok(#type_ident::#variants_for_decode), )*
                    _ => ::core::result::Result::Err(
                        ::bsql::__rt::DecodeError::UnknownEnumLabel,
                    ),
                }
            }
        }
    })
}

/// Emit one generated Rust `struct` for a `CREATE TYPE name AS (fields)`
/// composite: the struct (one `Option<T>` field per attribute — a composite
/// attribute is always nullable on the wire) and its
/// [`::bsql::__rt::PgComposite`] impl (the row-type binary frame decoder,
/// defined ONCE here so `query!`'s decode of a composite column cannot disagree
/// with it). Field order is the declared order (the wire frame's field order).
///
/// The decoder walks the frame with [`::bsql::__rt::CompositeReader`] and decodes
/// each field into its OWNED value by RECURSING into that field's own existing
/// decoder — a native `Cell<BinaryFmt>` scalar/array (via `ColCellAt`), a nested
/// composite (`PgComposite::decode_row`), or an enum label
/// (`PgEnum::from_wire_label`) — never a second copy of the scalar decoders. The
/// struct is OWNED and `'static` (its `text`/`bytea` fields copy), so it is a
/// valid record field in both the borrowed and owned `query!` record twins.
///
/// A field whose type the catalog cannot resolve (neither native nor a modeled
/// user type) is a loud error; a field name that cannot form a Rust identifier is
/// a loud error. The struct derives only `Debug`, `Clone`, `PartialEq` — a
/// composite may carry a float field, so it is deliberately not `Eq`/`Ord`/`Hash`.
fn emit_user_composite(
    catalog: &bsql_build::Catalog,
    composite_name: &str,
    fields: &[bsql_build::CompositeField],
    span: Span,
) -> syn::Result<TokenStream2> {
    let struct_ident = pascal_ident(composite_name, "composite type", span)?;

    // Resolve every field's canonical PG type to a `RustType`, up front, so a
    // field of an unsupported type is a loud error naming the field.
    let mut field_types = Vec::with_capacity(fields.len());
    for field in fields {
        let ty = catalog.resolve_field_type(&field.pg_type).ok_or_else(|| {
            syn::Error::new(
                span,
                format!(
                    "bsql::user_types!(): composite `{composite_name}` field \
                     `{}` has type `{}`, which is not a supported column type \
                     (neither a native type nor a modeled enum / domain / \
                     composite). A composite with an unsupported field cannot be \
                     generated.",
                    field.name, field.pg_type
                ),
            )
        })?;
        field_types.push(ty);
    }

    // Resolve the enum / composite idents the FIELDS reference (a nested
    // composite field, or an enum field), against the same catalog — so a bad
    // nested-type name fails once, here.
    let field_idents_resolver = resolve_user_type_idents(catalog, field_types.iter().copied(), span)?;
    // Composite fields decode into the NATIVE bsql types — external-type bridges
    // fire on top-level columns, not composite fields (a documented boundary).
    let no_bridges = Bridges { entries: Vec::new() };

    let mut field_idents = Vec::with_capacity(fields.len());
    let mut field_type_tokens = Vec::with_capacity(fields.len());
    let mut field_docs = Vec::with_capacity(fields.len());
    let mut decode_exprs = Vec::with_capacity(fields.len());
    for (field, &ty) in fields.iter().zip(field_types.iter()) {
        field_idents.push(make_field_ident(&field.name, span)?);
        // Every composite field is nullable on the wire, so the Rust field is
        // `Option<T>` (owned twin — the struct is `'static`).
        field_type_tokens.push(field_type_bridged(
            ty,
            true,
            true,
            &no_bridges,
            &field_idents_resolver,
        )?);
        decode_exprs.push(decode_value_expr_bridged(
            ty,
            true,
            &no_bridges,
            &field_idents_resolver,
        )?);
        field_docs.push(format!(
            "The `{}` attribute of the `{composite_name}` composite (always \
             `Option` — a composite attribute is nullable on the wire).",
            field.name
        ));
    }

    let nfields = u32::try_from(fields.len()).map_err(|_| {
        syn::Error::new(
            span,
            format!("bsql::user_types!(): composite `{composite_name}` has too many fields"),
        )
    })?;

    let type_doc = format!(
        "The `{composite_name}` PostgreSQL composite type, generated from the \
         migration `CREATE TYPE {composite_name} AS (...)`. One `Option<T>` field \
         per attribute (a composite attribute is always nullable on the wire). \
         Decode a `{composite_name}` column with `query!`."
    );
    let dead_reason =
        "a generated user type may be referenced by any, all, or none of the crate's queries";

    // The generated `decode_row` locals whose lifetime SPANS the per-field
    // `let #field_ident = …` bindings (the frame param and the reader) are given
    // MIXED-SITE hygiene, so a composite attribute literally named `__frame` /
    // `__reader` — a `let __reader = …` from `#field_idents` (call-site hygiene) —
    // cannot shadow them: the compiler treats a mixed-site local and a same-text
    // call-site local as DISTINCT bindings, so `next_field(&mut #reader)` and
    // `finish(#reader)` always resolve to the reader, never to a field local of
    // the same name. (The `__bytes` match binding does NOT need this: it lives in
    // the inner match-arm scope, fully consumed before the outer field `let`, so a
    // field named `__bytes` is already benign — and it must share the plain
    // hygiene of the `decode_value_expr_bridged`-emitted `__bytes` it feeds.) This
    // is the composite peer of the enum's keyword-label raw-ident handling.
    let frame = Ident::new("__frame", Span::mixed_site());
    let reader = Ident::new("__reader", Span::mixed_site());

    Ok(quote! {
        #[doc = #type_doc]
        #[derive(::core::fmt::Debug, ::core::clone::Clone, ::core::cmp::PartialEq)]
        #[allow(dead_code, reason = #dead_reason)]
        pub struct #struct_ident {
            #( #[doc = #field_docs] pub #field_idents: #field_type_tokens, )*
        }

        impl ::bsql::__rt::PgComposite for #struct_ident {
            fn decode_row(
                #frame: &[u8],
            ) -> ::core::result::Result<Self, ::bsql::__rt::DecodeError> {
                let mut #reader = ::bsql::__rt::CompositeReader::new(#frame, #nfields)?;
                #(
                    let #field_idents = match ::bsql::__rt::CompositeReader::next_field(
                        &mut #reader,
                    )? {
                        ::core::option::Option::Some(__bytes) =>
                            ::core::option::Option::Some(#decode_exprs),
                        ::core::option::Option::None => ::core::option::Option::None,
                    };
                )*
                ::bsql::__rt::CompositeReader::finish(#reader)?;
                ::core::result::Result::Ok(#struct_ident { #( #field_idents ),* })
            }
        }
    })
}

// ════════════════════════════════════════════════════════════════════
// copy! — compile-checked binary COPY-in carrier
// ════════════════════════════════════════════════════════════════════
//
// `copy!(Name, "table", (col, ...))` validates the target table + columns +
// their types against the same build catalog `query!` reads, and emits an
// uninhabited `Name` carrier implementing `TypedCopyIn`: a GAT `Row<'q>` tuple
// pinning the column encode types (a NOT NULL column is `T`, a nullable column
// `Option<T>`; a `text` / `bytea` column borrows as `&'q str` / `&'q [u8]`), and
// a const `SQL` = `COPY <table> (<cols>) FROM STDIN WITH (FORMAT binary)` baked
// from the catalog identifiers. A driver's `copy_in_typed::<Name>(rows)` streams
// each row through the SHARED `ParamsWriter` binary leaves — the same encoders
// the `query!` param path uses — so binary-COPY is faster (no text
// parse/format) AND injection-safe by construction (no text to mis-escape, and
// the identifiers are a compile-time constant, never a runtime splice). A
// wrong-typed or wrong-arity row is a compile error at the `copy_in_typed` call.

/// Compile-checked binary COPY-in carrier generator.
///
/// `copy!(Name, "table", (col1, col2, …))` validates the target `table` and its
/// `col`s against the schema replayed from the consumer's migration DDL (the
/// build-generated catalog `query!` reads), and emits an uninhabited `Name`
/// carrier implementing `bsql::TypedCopyIn`. A driver's
/// `copy_in_typed::<Name>(rows)` bulk-loads `rows` — each a typed tuple matching
/// the columns' Rust types — via the fastest, injection-safe-by-construction
/// PGCOPY *binary* path.
///
/// A `NOT NULL` column maps to `T`; a nullable column to `Option<T>` (pass
/// `None` for a SQL NULL). A `text` / `bytea` column borrows the caller's data
/// (`&'q str` / `&'q [u8]`), so a streamed bulk load copies each field once with
/// no owned-`String` per field.
///
/// At most [`MAX_COPY_COLUMNS`] (32) columns per carrier — the row tuple is a
/// `ParamsWriter`, whose tuple impls cover arity `0..=32`. A wider table is a
/// tailored `compile_error!` (split the load, or use the raw `copy_in`), never
/// an untailored trait-bound error.
///
/// An unknown table / column, a duplicate column, an empty or over-32 column
/// list, or a column whose type binary COPY does not yet support (an array /
/// unsupported type — use the raw `copy_in` for those) is a `compile_error!`,
/// never a silent pass.
#[proc_macro]
pub fn copy(input: TokenStream) -> TokenStream {
    match copy_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// The maximum number of columns a `copy!` carrier may name — the arity ceiling
/// of the `ParamsWriter` tuple impls (`0..=32`) the row tuple is built from.
/// Kept in lockstep with `bsql_postgres_proto::MAX_PARAMS_ARITY`; a wider column
/// list is a tailored `copy!` compile error rather than a raw trait-bound
/// failure on the `Row<'q>: ParamsWriter` bound.
const MAX_COPY_COLUMNS: usize = 32;

/// `copy!(Name, "table", (cols))` input: a carrier name, a table string literal,
/// and a parenthesized column-identifier list.
struct CopyInput {
    name: Ident,
    table: syn::LitStr,
    columns: Vec<Ident>,
}

impl Parse for CopyInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let _comma1: Token![,] = input.parse()?;
        let table: syn::LitStr = input.parse()?;
        let _comma2: Token![,] = input.parse()?;
        let content;
        syn::parenthesized!(content in input);
        let cols = content.parse_terminated(Ident::parse, Token![,])?;
        if !input.is_empty() {
            return Err(input.error(
                "copy!: expected exactly `Name, \"table\", (col, …)` — a carrier \
                 name, a comma, a table string, a comma, and a parenthesized \
                 column list",
            ));
        }
        let columns: Vec<Ident> = cols.into_iter().collect();
        if columns.is_empty() {
            return Err(syn::Error::new(
                table.span(),
                "copy!: the column list must name at least one column",
            ));
        }
        Ok(CopyInput { name, table, columns })
    }
}

/// Validate a caller-supplied identifier for splicing into the COPY command:
/// a plain unquoted PostgreSQL identifier (a leading ASCII letter or `_`, then
/// letters / digits / `_` / `$`, at most 63 bytes). Rejects a
/// schema-qualified / dotted name (copy! targets a catalog table by its bare
/// name) and anything else injection-shaped — rejection is injection-proof by
/// construction, exactly as `SafeIdent` / `SafeTable` do on the raw path.
/// Returns the case-folded (lowercase) form PostgreSQL resolves an unquoted
/// identifier to — the key the catalog stores and the text spliced into the SQL.
fn validate_copy_table(raw: &str, span: Span) -> syn::Result<String> {
    if raw.contains('.') {
        return Err(syn::Error::new(
            span,
            format!(
                "copy!: the table `{raw}` must be a bare unquoted identifier — a \
                 schema-qualified name is not supported; name the catalog table \
                 directly."
            ),
        ));
    }
    let ok = match raw.as_bytes().split_first() {
        Some((&first, rest)) => {
            raw.len() <= 63
                && (first.is_ascii_alphabetic() || first == b'_')
                && rest
                    .iter()
                    .all(|&b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
        }
        None => false,
    };
    if ok {
        Ok(raw.to_ascii_lowercase())
    } else {
        Err(syn::Error::new(
            span,
            format!(
                "copy!: the table `{raw}` is not a plain unquoted SQL identifier \
                 (a letter or '_' then letters / digits / '_' / '$', at most 63 \
                 bytes)"
            ),
        ))
    }
}

/// Case-fold a column identifier the way PostgreSQL folds an unquoted name (to
/// lowercase), stripping a Rust raw-identifier prefix (`r#type` → `type`) so a
/// column that collides with a Rust keyword can still be named.
fn fold_column_ident(col: &Ident) -> String {
    let raw = col.to_string();
    let bare = match raw.strip_prefix("r#") {
        Some(stripped) => stripped,
        None => raw.as_str(),
    };
    bare.to_ascii_lowercase()
}

fn copy_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    let CopyInput {
        name,
        table,
        columns,
    } = syn::parse2(input)?;
    let table_span = table.span();

    // The catalog-folded table name (lowercase, the catalog key + the spliced
    // SQL text). Injection-safe by validation, exactly like the raw path's
    // `SafeTable`.
    let table_folded = validate_copy_table(&table.value(), table_span)?;

    // Rebuild the catalog the consumer's build.rs wrote (fail-closed on a
    // missing / unreadable / corrupt catalog), then resolve the target table.
    let catalog = load_build_catalog(name.span())?;
    let table_cols = catalog.tables.get(&table_folded).ok_or_else(|| {
        syn::Error::new(
            table_span,
            format!(
                "copy!: unknown table `{table_folded}` — no such table in the \
                 schema replayed from the migrations. copy! validates the target \
                 against the SAME build catalog as query!."
            ),
        )
    })?;

    // Arity ceiling: the row tuple is a `ParamsWriter`, whose tuple impls cover
    // 0..=32. A wider column list would otherwise hit an untailored E0277 on the
    // `Row<'q>: ParamsWriter` bound — bulk-load targets commonly exceed 16
    // columns, so this loud, tailored rejection names the cap and the escape
    // hatch instead of a raw trait-bound error.
    if columns.len() > MAX_COPY_COLUMNS {
        return Err(syn::Error::new(
            table_span,
            format!(
                "copy!: {} columns — typed binary COPY supports at most \
                 {MAX_COPY_COLUMNS} columns per carrier. Split the load across \
                 narrower `copy!` carriers, or use the raw `copy_in` for a wider \
                 table.",
                columns.len()
            ),
        ));
    }

    // Per column: fold + look up + map the catalog `pg_type` to its Rust encode
    // type (scalar types only — an array / unsupported column is a loud
    // rejection, never a silent skip). Reject a duplicate column (PostgreSQL
    // itself rejects `COPY t (a, a)`), so the failure is at build time.
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut field_markers: Vec<TokenStream2> = Vec::with_capacity(columns.len());
    let mut col_names: Vec<String> = Vec::with_capacity(columns.len());
    for col in &columns {
        let folded = fold_column_ident(col);
        if !seen.insert(folded.clone()) {
            return Err(syn::Error::new(
                col.span(),
                format!("copy!: column `{folded}` is listed more than once"),
            ));
        }
        let info = table_cols.get(&folded).ok_or_else(|| {
            syn::Error::new(
                col.span(),
                format!(
                    "copy!: unknown column `{folded}` in table `{table_folded}`"
                ),
            )
        })?;
        let ty = bsql_build::scalar_rust_type_for_pg(&info.pg_type).ok_or_else(|| {
            syn::Error::new(
                col.span(),
                format!(
                    "copy!: column `{folded}` has type `{}`, which typed binary \
                     COPY does not support — scalar columns only (an array or an \
                     otherwise-unsupported type cannot be bulk-loaded via copy! \
                     yet; use the raw `copy_in` for pre-formatted COPY data of \
                     those columns).",
                    info.pg_type
                ),
            )
        })?;
        // Borrowed row-field type (`&'q str` for text) — one copy per field, no
        // owned String; nullable columns wrap in `Option<..>`. Projected from
        // the SAME `col_spec` source query! uses.
        field_markers.push(field_type(ty, !info.not_null, false));
        col_names.push(folded);
    }

    let row_tuple = tuple_type(&field_markers);
    let sql = format!(
        "COPY {table_folded} ({}) FROM STDIN WITH (FORMAT binary)",
        col_names.join(", ")
    );

    let carrier_doc = format!(
        "Compile-checked binary COPY-in carrier for `{table_folded}`. Its \
         [`TypedCopyIn`](::bsql::TypedCopyIn) impl pins the target columns' Rust \
         types ([`Row`](::bsql::TypedCopyIn::Row)) and the catalog-baked COPY \
         command ([`SQL`](::bsql::TypedCopyIn::SQL)); a driver's \
         `copy_in_typed::<{name}>(rows)` bulk-loads typed rows through the fastest \
         PGCOPY binary path."
    );
    let allow_reason = "the generated COPY carrier is part of the public surface; a consumer may use any subset of it";

    Ok(quote! {
        #[doc = #carrier_doc]
        #[allow(dead_code, reason = #allow_reason)]
        pub enum #name {}

        impl ::bsql::__rt::TypedCopyIn for #name {
            type Row<'q> = #row_tuple;
            const SQL: &'static str = #sql;
        }
    })
}

/// Strip the recognized `/* bsql:allow-scan[: reason] */` acknowledgment
/// directive from a query string. Returns the cleaned SQL and whether the
/// directive was present. Only the FIRST block comment that contains the
/// marker is removed; ordinary block comments are left untouched. An
/// unterminated block comment is left as-is for the downstream parser to
/// report.
fn strip_scan_ack(sql: &str) -> (String, bool) {
    const MARKER: &str = "bsql:allow-scan";
    let mut search_from = 0usize;
    while let Some(rel) = sql.get(search_from..).and_then(|s| s.find("/*")) {
        let open = search_from + rel;
        let after_open = open + 2;
        let Some(crel) = sql.get(after_open..).and_then(|s| s.find("*/")) else {
            // Unterminated comment: nothing safe to strip.
            break;
        };
        let close = after_open + crel;
        let is_marker = sql
            .get(after_open..close)
            .is_some_and(|inner| inner.to_ascii_lowercase().contains(MARKER));
        if is_marker {
            let mut cleaned = String::with_capacity(sql.len());
            if let Some(head) = sql.get(..open) {
                cleaned.push_str(head);
            }
            if let Some(tail) = sql.get(close + 2..) {
                cleaned.push_str(tail);
            }
            return (cleaned, true);
        }
        search_from = close + 2;
    }
    (sql.to_string(), false)
}

/// Run the build-time SQLite conformance cross-check for one query, when
/// the consumer emitted a SQLite template database (the
/// `BSQL_SQLITE_TEMPLATE` rustc-env channel is present).
///
/// When no template channel is present, the outcome depends on whether the
/// consumer's build DECLARED a SQLite target (the `BSQL_SQLITE_TARGET`
/// marker the build helper sets from the same call that emits the template):
///
/// * marker absent — a deliberately PostgreSQL-only build. There is no
///   SQLite target to conform against, so this returns `Ok(())` (it is not a
///   skipped schema check, it is the absence of a SQLite target).
/// * marker present — a SQLite target was declared but its template never
///   reached expansion. That is a loud `compile_error!`, never a silent
///   disengage of the conformance oracle.
#[cfg(feature = "sqlite")]
fn verify_sqlite(
    span: Span,
    shape: &bsql_build::DynamicShape,
    scan_acknowledged: bool,
) -> syn::Result<()> {
    let path = match std::env::var(bsql_build::SQLITE_TEMPLATE_ENV_VAR) {
        Ok(path) => path,
        // No template channel. Distinguish a deliberate PostgreSQL-only build
        // (no SQLite target declared → nothing to conform against) from a
        // consumer who DECLARED a SQLite target but whose template never
        // reached expansion. The build helper sets `BSQL_SQLITE_TARGET` from
        // the SAME call that emits the template, so its presence WITHOUT a
        // template is a loud misconfiguration (a hand-written `build.rs` that
        // declared the target but did not emit, or a corrupted OUT_DIR), never
        // a silent disengage of the SQLite conformance oracle.
        Err(_) => {
            return match std::env::var(bsql_build::SQLITE_TARGET_ENV_VAR) {
                Ok(_) => Err(syn::Error::new(
                    span,
                    "query!: this build declared a SQLite conformance target but \
                     no SQLite template reached expansion. The consumer's \
                     `build.rs` must call `bsql_build::emit(migrations)` (or \
                     `bsql_build::emit_sqlite_template(migrations)`) with \
                     `bsql-build`'s `sqlite` feature enabled in \
                     `[build-dependencies]`. Refusing to type a query without \
                     the SQLite conformance oracle it was told to run.",
                )),
                Err(_) => Ok(()),
            };
        }
    };
    bsql_build::verify_sqlite_conformance(
        std::path::Path::new(&path),
        shape,
        scan_acknowledged,
    )
    .map_err(|err| {
        syn::Error::new(
            span,
            format!(
                "query!: SQLite conformance failure: {err} — the query types \
                 against the schema but does not conform to real SQLite \
                 (validated at build time against the migration-replayed \
                 template database)."
            ),
        )
    })
}

/// The parsed schema catalog, cached per proc-macro process.
///
/// The catalog is a rustc-env path a consumer's `build.rs` emits ONCE before
/// the crate is compiled, so within a single compilation it is immutable;
/// every `query!` in the crate reads the SAME bytes. This caches the parsed
/// [`bsql_build::Catalog`] keyed on `(path, mtime, len)` so an N-query crate
/// parses it once, not N times. The `(mtime, len)` guard makes the cache
/// self-invalidating: a changed path, a moved mtime, or a changed byte length
/// is a miss and the fresh bytes are parsed. `cargo` always compiles in a
/// FRESH process (so a rebuilt catalog is a new process with an empty cache),
/// so the guard matters only for a PERSISTENT proc-macro host (e.g. an IDE's
/// `proc-macro-srv`) that outlives a rebuild — there the `mtime` catches every
/// normal rewrite, and `len` additionally catches a length-changing rewrite
/// within a coarse-mtime filesystem tick (HFS+ 1 s, FAT 2 s). The only
/// remaining theoretical stale window is an IDE-host, same-tick, SAME-length
/// rewrite — self-heals on the next tick and never yields a wrong compiled
/// ARTIFACT (cargo, which produces artifacts, is always a fresh process). The
/// value is shared as an [`Rc`] so a hit costs one pointer clone, never an
/// `O(catalog_bytes)` copy.
///
/// Only the catalog is cached — it is plain span-free data. The external-type
/// bridge set is deliberately NOT cached: a bridge entry carries `syn` tokens
/// whose spans are minted per invocation, and reusing them across a later
/// expansion is unsound (see [`load_bridges`]).
struct CachedFile<T> {
    /// The rustc-env path the value was parsed from.
    path: String,
    /// The file's `(modification time, byte length)` when it was parsed, both
    /// read from ONE `metadata()` stat. `None` if the stat failed — in which
    /// case the cache is never consulted (a fresh read re-derives the
    /// fail-closed error), so a missing stat can never serve a possibly-changed
    /// file.
    stamp: Option<(SystemTime, u64)>,
    /// The parsed value, shared by pointer.
    value: Rc<T>,
}

thread_local! {
    /// Per-process cache of the parsed schema catalog (see [`CachedFile`]).
    static CATALOG_CACHE: RefCell<Option<CachedFile<bsql_build::Catalog>>> =
        const { RefCell::new(None) };
}

/// The file's `(modification time, byte length)` from ONE `metadata()` stat,
/// or `None` if it cannot be stat'd (or the mtime is unavailable). `len` is a
/// free extra key component — the same stat already carries it — that catches a
/// same-mtime-tick length-changing rewrite on a coarse-mtime filesystem.
fn file_stamp(path: &str) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime = meta.modified().ok()?;
    Some((mtime, meta.len()))
}

/// A shared clone of the cached value IFF the slot holds one for the SAME path
/// and a DEFINITE, matching `(mtime, len)` stamp. A missing stat (`stamp ==
/// None`) is always a miss, so the cache never serves a file it cannot prove is
/// unchanged.
fn cache_hit<T>(
    slot: &Option<CachedFile<T>>,
    path: &str,
    stamp: Option<(SystemTime, u64)>,
) -> Option<Rc<T>> {
    let cached = slot.as_ref()?;
    let s = stamp?;
    if cached.path == path && cached.stamp == Some(s) {
        Some(Rc::clone(&cached.value))
    } else {
        None
    }
}

/// Read the catalog text via the rustc-env channel and rebuild the
/// `bsql_build::Catalog`, memoized per proc-macro process (see [`CachedFile`]).
/// Fail-closed: an absent env var, an unreadable file, or a corrupt catalog is
/// a `compile_error!` — never a silent pass against a missing schema (which
/// would be the stale-schema blind spot this design exists to remove). Only a
/// SUCCESSFUL parse is cached, so every error path still runs fresh with the
/// calling query's own span.
fn load_build_catalog(span: Span) -> syn::Result<Rc<bsql_build::Catalog>> {
    let path = std::env::var(CATALOG_ENV_VAR).map_err(|_| {
        syn::Error::new(
            span,
            format!(
                "query!: the schema catalog environment variable \
                 `{CATALOG_ENV_VAR}` is not set. The consumer crate must have \
                 a `build.rs` calling `bsql_build::emit_catalog(..)` (and \
                 `bsql-build` in its `[build-dependencies]`). Refusing to type \
                 a query against a missing schema."
            ),
        )
    })?;
    let stamp = file_stamp(&path);
    if let Some(hit) = CATALOG_CACHE.with_borrow(|slot| cache_hit(slot, &path, stamp)) {
        return Ok(hit);
    }
    let text = std::fs::read_to_string(&path).map_err(|err| {
        syn::Error::new(
            span,
            format!(
                "query!: cannot read the schema catalog at `{path}`: {err}. The \
                 catalog is generated by `bsql-build` into OUT_DIR at build \
                 time; an unreadable catalog fails closed rather than typing a \
                 query against an unknown schema."
            ),
        )
    })?;
    let mut catalog = bsql_build::parse_catalog(&text).map_err(|err| {
        syn::Error::new(
            span,
            format!("query!: the schema catalog at `{path}` is malformed: {err}"),
        )
    })?;
    // Attach the user-defined types (`CREATE TYPE ... AS ENUM`) from their own
    // channel. Both files are written together by the same build.rs run and the
    // catalog file is rewritten every build, so caching the merged value under
    // the catalog file's stamp is sound (they change atomically). The data is
    // span-free (plain strings), so — unlike the bridge tokens — it is safe to
    // memoize. An absent channel means an older/mismatched build-dep with no
    // user types: treated as none (a user-typed column then stays a loud
    // `UnsupportedPgType`, never a silent miss).
    catalog.user_types = load_user_types(span)?;
    let value = Rc::new(catalog);
    CATALOG_CACHE.with_borrow_mut(|slot| {
        *slot = Some(CachedFile {
            path,
            stamp,
            value: Rc::clone(&value),
        });
    });
    Ok(value)
}

/// Read the user-defined types (`CREATE TYPE ... AS ENUM`) from the
/// `BSQL_USER_TYPES` rustc-env channel and parse them into the catalog's map.
/// An absent channel (an older build-dep, or a build predating this feature) is
/// an EMPTY map — a user-typed column then stays a loud `UnsupportedPgType`,
/// never a silent miss. An unreadable or malformed present file fails closed
/// (the file is machine-generated; a bad line means the build-script channel is
/// corrupt), never a silent decode of an enum column back to an unknown type.
fn load_user_types(
    span: Span,
) -> syn::Result<std::collections::BTreeMap<String, bsql_build::UserType>> {
    let path = match std::env::var(bsql_build::USER_TYPES_ENV_VAR) {
        Ok(path) => path,
        // No channel: no user types (native-only build).
        Err(_) => return Ok(std::collections::BTreeMap::new()),
    };
    let text = std::fs::read_to_string(&path).map_err(|err| {
        syn::Error::new(
            span,
            format!(
                "query!: cannot read the user-types file at `{path}`: {err}. It is \
                 generated by `bsql-build` into OUT_DIR; an unreadable file fails \
                 closed rather than silently dropping a user-defined type."
            ),
        )
    })?;
    bsql_build::parse_user_types(&text).map_err(|err| {
        syn::Error::new(
            span,
            format!("query!: the user-types file at `{path}` is malformed: {err}"),
        )
    })
}

/// One resolved external-type bridge: the native pivot type it fires on, the
/// consumer's target type, and the infallible converter free-fn path. The
/// bridge reshapes only the record FIELD VALUE; the wire OID / const validator
/// ride the native pivot, so an OID drift is still a build error.
struct BridgeEntry {
    /// The native pivot `RustType` this bridge fires on (a scalar column of
    /// this type, or a 1-D array whose element is this type).
    rust_type: bsql_build::RustType,
    /// The consumer's target field type (parsed from the registered string).
    target: syn::Type,
    /// The consumer's converter path: `fn(NativeOwned) -> Target`.
    converter: syn::Path,
}

/// The resolved set of external-type bridges for this build (possibly empty).
struct Bridges {
    entries: Vec<BridgeEntry>,
}

impl Bridges {
    /// The bridge firing on a SCALAR column of native pivot `ty`, if any.
    fn scalar(&self, ty: bsql_build::RustType) -> Option<&BridgeEntry> {
        self.entries.iter().find(|entry| entry.rust_type == ty)
    }

    /// The bridge firing on the ELEMENT of a 1-D array column, if any.
    fn element(&self, elem: bsql_build::ElemType) -> Option<&BridgeEntry> {
        self.scalar(elem.as_scalar())
    }

    /// The bridge firing on a column of type `ty` (scalar directly, or array
    /// per element), plus whether `ty` is an array (so the target is wrapped
    /// `Vec<Option<Target>>` and the converter is applied per element).
    fn for_column(&self, ty: bsql_build::RustType) -> Option<(&BridgeEntry, bool)> {
        match ty {
            bsql_build::RustType::Array(elem) => self.element(elem).map(|entry| (entry, true)),
            scalar => self.scalar(scalar).map(|entry| (entry, false)),
        }
    }
}

/// Read the external-type bridges via the rustc-env channel. An absent channel
/// (the consumer used the plain `emit` / `emit_catalog` free functions) yields
/// an EMPTY set — identical to the prior native-only behavior. Fail-closed on
/// an unreadable / corrupt file, an unknown pg-type, a conflict, or an
/// unparsable target/converter path — never a silently-dropped bridge (which
/// would decode a column the consumer chose to bridge back into the native
/// type).
fn load_bridges(span: Span) -> syn::Result<Bridges> {
    let path = match std::env::var(BRIDGES_ENV_VAR) {
        Ok(path) => path,
        // No channel: the consumer registered no bridges. Native types.
        Err(_) => return Ok(Bridges { entries: Vec::new() }),
    };
    // The bridge set is NOT memoized: a `BridgeEntry` holds `syn::Type` /
    // `syn::Path` tokens whose SPANS are minted in the invocation that parsed
    // them, and reusing span-bearing tokens across a later `query!` expansion
    // is unsound (the cached spans dangle relative to the new invocation). The
    // catalog IS memoized because it is plain span-free data. The bridge file
    // is tiny (a handful of lines) and most consumers register none, so the
    // per-call reparse is negligible — the meaningful win is the catalog.
    let text = std::fs::read_to_string(&path).map_err(|err| {
        syn::Error::new(
            span,
            format!(
                "query!: cannot read the external-type bridge file at `{path}`: \
                 {err}. It is generated by `bsql_build::CatalogBuilder::emit` into \
                 OUT_DIR; an unreadable bridge file fails closed rather than \
                 silently decoding a bridged column back into its native type."
            ),
        )
    })?;
    let specs = bsql_build::parse_bridges(&text).map_err(|err| {
        syn::Error::new(
            span,
            format!("query!: the external-type bridge file at `{path}` is malformed: {err}"),
        )
    })?;

    let mut entries: Vec<BridgeEntry> = Vec::with_capacity(specs.len());
    for spec in specs {
        let rust_type =
            bsql_build::scalar_rust_type_for_pg(&spec.pg_type).ok_or_else(|| {
                syn::Error::new(
                    span,
                    format!(
                        "query!: external-type bridge for `{}` has no native pivot \
                         (it is not a natively-supported canonical PostgreSQL type)",
                        spec.pg_type
                    ),
                )
            })?;
        if entries.iter().any(|entry| entry.rust_type == rust_type) {
            return Err(syn::Error::new(
                span,
                format!(
                    "query!: two external-type bridges resolve to the same native \
                     pivot type (one is `{}`); bsql decodes them identically, so \
                     they cannot bridge to different targets",
                    spec.pg_type
                ),
            ));
        }
        let target: syn::Type = syn::parse_str(&spec.target_type_path).map_err(|err| {
            syn::Error::new(
                span,
                format!(
                    "query!: external-type bridge target `{}` is not a valid Rust \
                     type: {err}",
                    spec.target_type_path
                ),
            )
        })?;
        let converter: syn::Path = syn::parse_str(&spec.converter_fn_path).map_err(|err| {
            syn::Error::new(
                span,
                format!(
                    "query!: external-type bridge converter `{}` is not a valid Rust \
                     path: {err}",
                    spec.converter_fn_path
                ),
            )
        })?;
        entries.push(BridgeEntry {
            rust_type,
            target,
            converter,
        });
    }
    Ok(Bridges { entries })
}

/// Whether a column borrows the input bytes in the borrowed record — a `text`
/// / `bytea` column that is NOT bridged. A bridged column decodes into an
/// owned target (the converter takes the owned native value), so it never
/// borrows, exactly like the self-owning value types.
fn column_borrows(ty: bsql_build::RustType, bridges: &Bridges) -> bool {
    borrows_input(ty) && bridges.for_column(ty).is_none()
}

/// How a column type's borrowed-record field aliases the decode input.
///
/// The SOLE axis that distinguishes the value types from the two borrowing
/// families: it drives the borrowed field spelling (`&'q str` / `&'q [u8]`),
/// whether the record carries `<'q>`, and the owned-twin copy-out
/// (`String::from` / `<[u8]>::to_vec`). A new borrow shape is a compile error
/// at every `match BorrowKind` below (no `_` arm), never a silent default.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BorrowKind {
    /// A self-owning value type (integers, floats, `bool`, the bsql-native
    /// value types, and a 1-D array): both record twins carry the SAME field
    /// type; the decode owns its value.
    ByValue,
    /// `text`: the borrowed twin aliases the input as `&'q str`; the owned
    /// twin copies to `String`.
    Str,
    /// `bytea`: the borrowed twin aliases the input as `&'q [u8]`; the owned
    /// twin copies to `Vec<u8>`.
    Bytes,
}

/// The SINGLE per-column type descriptor — the sole source of truth for every
/// per-type facet the `query!` expansion needs.
///
/// # Why one descriptor closes the silent mis-decode class
///
/// The record decode, the row-tuple marker, and the wire OID all derive from
/// ONE `ColSpec` row, so they cannot drift:
///
/// * [`ColSpec::marker`] is the row-tuple element (whose
///   `ColCellAt::OID` is the single source of the row's `ROW_OIDS` — the
///   runtime sources the OID slice FROM this marker's type) AND
///   the type the record decode routes through
///   (`<marker as ColCellAt>::decode_at`). Decoder and wire OID are therefore
///   the SAME source: a same-width divergence (e.g. `int4` vs `oid`, both
///   4 bytes) is structurally impossible in the OID slice (it IS the marker's
///   OID) and a loud `error[E0308]` at the record's struct literal (the field
///   type disagrees with the decoded value) — never
///   a wire `-1` silently decoded as `4294967295`.
/// * [`ColSpec::oid_value`] is the scalar column OID's numeric value baked
///   into the `Parse` template (the raw wire representation); the runtime
///   const-checks those baked bytes against the tuple's own
///   `ParamsWriter::OIDS`, so the wire cannot lie about the declared types.
///
/// Adding a supported PostgreSQL type is adding ONE row to [`col_spec`].
struct ColSpec {
    /// The row-tuple + decode marker in `'static` form (`__rt` paths). Feeds
    /// BOTH the `Row` tuple element and the `ColCellAt::decode_at` decode.
    marker: TokenStream2,
    /// The owned record-field base type (public `::bsql::` paths). For
    /// [`BorrowKind::ByValue`] it is the field type of both twins; for the
    /// borrowing families it is the OWNED spelling (`String` / `Vec<u8>`),
    /// with the borrowed spelling derived from [`ColSpec::borrow`].
    field_owned: TokenStream2,
    /// How the borrowed twin aliases the input (drives `<'q>` + copy-out).
    borrow: BorrowKind,
    /// The scalar column OID's numeric value (baked into the Parse template).
    oid_value: u32,
    /// The 1-D array (`T[]`) OID's numeric value.
    array_oid_value: u32,
    /// The fixed binary width `(bytes, i32-length-prefix)` for the const-offset
    /// fast path, or `None` for a variable-width type.
    fixed_width: Option<(usize, i32)>,
    /// Whether the type implements `Eq` (false only for the IEEE-754 floats:
    /// `NaN` is not reflexively equal).
    impls_eq: bool,
}

/// The single source of truth: every per-type facet for one column type.
///
/// This is the ONLY exhaustive `match` over the supported type set in the
/// macro; every other per-type function is a thin projection of one field
/// here. A new [`bsql_build::RustType`] variant is a non-exhaustive-match
/// compile error AT THIS TABLE — the author must state its marker, OIDs,
/// width, borrow shape, and `Eq`-ness in one place — never a distant
/// misattributed error or a silent fall-through.
fn col_spec(ty: bsql_build::RustType) -> ColSpec {
    use bsql_build::RustType;
    // Compact constructor for a self-owning scalar whose row-tuple marker and
    // owned field spelling coincide (the primitives) or differ only by re-export
    // path (the bsql-native value types: `marker` on the `__rt` path, `field`
    // on the public path — the same type either way).
    let by_value = |marker: TokenStream2,
                    field: TokenStream2,
                    oid_value: u32,
                    array_oid_value: u32,
                    fixed_width: Option<(usize, i32)>,
                    impls_eq: bool| ColSpec {
        marker,
        field_owned: field,
        borrow: BorrowKind::ByValue,
        oid_value,
        array_oid_value,
        fixed_width,
        impls_eq,
    };
    match ty {
        RustType::I16 => by_value(
            quote!(i16), quote!(i16),
            21,
            1005,
            Some((2, 2)), true,
        ),
        RustType::I32 => by_value(
            quote!(i32), quote!(i32),
            23,
            1007,
            Some((4, 4)), true,
        ),
        RustType::I64 => by_value(
            quote!(i64), quote!(i64),
            20,
            1016,
            Some((8, 8)), true,
        ),
        RustType::U32 => by_value(
            quote!(u32), quote!(u32),
            26,
            1028,
            Some((4, 4)), true,
        ),
        RustType::Bool => by_value(
            quote!(bool), quote!(bool),
            16,
            1000,
            Some((1, 1)), true,
        ),
        // The IEEE-754 floats are fixed-width value types, but `NaN != NaN`, so
        // they are `PartialEq` only — a record with a float column cannot derive
        // `Eq`.
        RustType::F32 => by_value(
            quote!(f32), quote!(f32),
            700,
            1021,
            Some((4, 4)), false,
        ),
        RustType::F64 => by_value(
            quote!(f64), quote!(f64),
            701,
            1022,
            Some((8, 8)), false,
        ),
        // `uuid` is a fixed 16-byte value; `timestamptz` / `timestamp` a fixed
        // 8-byte `i64`; `date` a 4-byte `i32`; `time` an 8-byte `i64`;
        // `interval` a fixed 16-byte three-field record — all self-owning value
        // types that join the const-offset fast path.
        RustType::Uuid => by_value(
            quote!(::bsql::__rt::Uuid), quote!(::bsql::Uuid),
            2950,
            2951,
            Some((16, 16)), true,
        ),
        RustType::Timestamptz => by_value(
            quote!(::bsql::__rt::Timestamptz), quote!(::bsql::Timestamptz),
            1184,
            1185,
            Some((8, 8)), true,
        ),
        RustType::Timestamp => by_value(
            quote!(::bsql::__rt::Timestamp), quote!(::bsql::Timestamp),
            1114,
            1115,
            Some((8, 8)), true,
        ),
        RustType::Date => by_value(
            quote!(::bsql::__rt::Date), quote!(::bsql::Date),
            1082,
            1182,
            Some((4, 4)), true,
        ),
        RustType::Time => by_value(
            quote!(::bsql::__rt::Time), quote!(::bsql::Time),
            1083,
            1183,
            Some((8, 8)), true,
        ),
        RustType::Interval => by_value(
            quote!(::bsql::__rt::Interval), quote!(::bsql::Interval),
            1186,
            1187,
            Some((16, 16)), true,
        ),
        // `json` / `jsonb` / `numeric` are variable-width, self-owning value
        // types (String / Box<[u16]>-backed) — decoded on the per-cell path.
        RustType::Json => by_value(
            quote!(::bsql::__rt::Json), quote!(::bsql::Json),
            114,
            199,
            None, true,
        ),
        RustType::Jsonb => by_value(
            quote!(::bsql::__rt::Jsonb), quote!(::bsql::Jsonb),
            3802,
            3807,
            None, true,
        ),
        RustType::Numeric => by_value(
            quote!(::bsql::__rt::Numeric), quote!(::bsql::Numeric),
            1700,
            1231,
            None, true,
        ),
        // `text` borrows the input as `&'q str` (owned twin: `String`);
        // variable-width, decoded on the per-cell path.
        RustType::Text => ColSpec {
            marker: quote!(&'static str),
            field_owned: quote!(::std::string::String),
            borrow: BorrowKind::Str,
            oid_value: 25,
            array_oid_value: 1009,
            fixed_width: None,
            impls_eq: true,
        },
        // `bytea` mirrors `text` over bytes: borrows `&'q [u8]` (owned twin:
        // `Vec<u8>`).
        RustType::Bytea => ColSpec {
            marker: quote!(&'static [u8]),
            field_owned: quote!(::std::vec::Vec<u8>),
            borrow: BorrowKind::Bytes,
            oid_value: 17,
            array_oid_value: 1001,
            fixed_width: None,
            impls_eq: true,
        },
        // A 1-D array (`T[]`) is a self-owning `Vec<Option<T>>` whose OID is the
        // element's ARRAY OID; it derives entirely from the element's own row.
        // The element `Option<T>` is intrinsic (a PG array may hold NULLs); the
        // element is the OWNED form (a `text[]` element is `String`), so the
        // array marker (`__rt` element path) and array field (public element
        // path) are the same type, differing only by re-export path — exactly
        // like the value scalars.
        RustType::Array(elem) => {
            let e = col_spec(elem.as_scalar());
            // The owned element in both spellings (marker vs field path).
            let (elem_marker, elem_field) = match e.borrow {
                BorrowKind::ByValue => (e.marker, e.field_owned),
                BorrowKind::Str => {
                    (quote!(::std::string::String), quote!(::std::string::String))
                }
                BorrowKind::Bytes => {
                    (quote!(::std::vec::Vec<u8>), quote!(::std::vec::Vec<u8>))
                }
            };
            ColSpec {
                marker: quote!(::std::vec::Vec<::core::option::Option<#elem_marker>>),
                field_owned: quote!(::std::vec::Vec<::core::option::Option<#elem_field>>),
                borrow: BorrowKind::ByValue,
                oid_value: e.array_oid_value,
                // An array-of-array has no single element-array OID. Structurally
                // dead: inference rejects a multi-dimensional array, so this
                // never bakes into a slice. Fail-closed to a non-OID `0` that
                // would trip the wire OID const check rather than a
                // plausible-wrong OID.
                array_oid_value: 0,
                fixed_width: None,
                impls_eq: e.impls_eq,
            }
        }
        // A user-defined enum decodes on the WIRE exactly like `text` — a PG
        // enum value is its label bytes — so its row-tuple marker + OID ride the
        // TEXT pivot (`&'static str`, OID 25). The row OID is SOURCED from this
        // marker's `ColCellAt::OID`, so it cannot disagree with the decode. The
        // RECORD FIELD is the generated Rust enum, and the decode reshapes the
        // `&str` label into it via `PgEnum::from_wire_label`; those are applied
        // at the per-column codegen layer (which has the catalog to resolve the
        // enum's name), so `field_owned` here is a never-read placeholder.
        // `borrow: ByValue` because the field is an OWNED enum (no `<'q>`);
        // `fixed_width: None` so an enum column takes the per-cell decode path
        // (where the reshape lives), never the const-offset fast path.
        // `impls_eq: true` — a generated enum derives `Eq`.
        RustType::UserEnum(_) => ColSpec {
            marker: quote!(&'static str),
            // Placeholder — `field_type_bridged`/decode intercept `UserEnum`
            // before reading this, substituting the generated enum path.
            field_owned: quote!(&'static str),
            borrow: BorrowKind::ByValue,
            oid_value: 25,
            // Enum arrays are not modeled (a `mood[]` column stays a loud
            // `UnsupportedPgType` at inference), so the array OID is never baked.
            array_oid_value: 1009,
            fixed_width: None,
            impls_eq: true,
        },
        // A user-defined COMPOSITE decodes on the wire as its row-type BINARY
        // frame (walked by `CompositeReader` into the generated struct), NOT as
        // any native marker. Like the enum, `field_type_bridged`/decode intercept
        // `UserComposite` before reading the placeholders here, substituting the
        // generated struct path + `PgComposite::decode_row`. The row-tuple marker
        // is a never-decoded `&'static str` placeholder (the row tuple feeds only
        // the row OID source — the runtime decode routes through the record's own
        // `decode`, never `Q::Row`), and the OID rides the same TEXT placeholder
        // (a composite's real OID is server-dynamic, so it is not pinned —
        // exactly the enum's boundary).
        // `borrow: ByValue` because the struct is OWNED and `'static` (no `<'q>`);
        // `fixed_width: None` so a composite column takes the per-cell decode path
        // (where the reshape lives). `impls_eq: false` — a composite may carry a
        // float field, so the generated struct derives only `PartialEq`, and a
        // record with a composite column is `PartialEq` but not `Eq`.
        RustType::UserComposite(_) => ColSpec {
            marker: quote!(&'static str),
            field_owned: quote!(&'static str),
            borrow: BorrowKind::ByValue,
            oid_value: 25,
            // Composite arrays are not modeled (an `addr[]` column stays a loud
            // `UnsupportedPgType` at inference), so the array OID is never baked.
            array_oid_value: 1009,
            fixed_width: None,
            impls_eq: false,
        },
    }
}

/// The fixed binary width `(bytes, i32-length)` of a column type for the
/// const-offset fast path, or `None` for a variable-width type — projected
/// from the single [`col_spec`] source.
fn fixed_width(ty: bsql_build::RustType) -> Option<(usize, i32)> {
    col_spec(ty).fixed_width
}

/// Whether a column type borrows the input bytes in the borrowed record (so
/// the record must carry `<'q>`): the string type `text` and the byte-string
/// type `bytea`. Projected from [`col_spec`]'s borrow shape — exhaustive over
/// [`BorrowKind`], so a new borrow family forces a decision here.
fn borrows_input(ty: bsql_build::RustType) -> bool {
    match col_spec(ty).borrow {
        BorrowKind::ByValue => false,
        BorrowKind::Str | BorrowKind::Bytes => true,
    }
}

/// Whether a column type implements `Eq` (so the generated record can derive
/// it) — projected from the single [`col_spec`] source. False only for the
/// IEEE-754 floats (and any array of them): `NaN` is not reflexively equal.
fn type_impls_eq(ty: bsql_build::RustType) -> bool {
    col_spec(ty).impls_eq
}

/// A record field's Rust type — projected from the single [`col_spec`]
/// source. A borrowing column is `&'q _` in the borrowed record and owned
/// (`String` / `Vec<u8>`) in the owned twin; a value type / 1-D array carries
/// the SAME self-owning field type in both twins; a nullable column is wrapped
/// in `Option<..>`. A drift between this field type and the decoded value (the
/// `col_spec` marker's `ColCellAt::At`) is a loud `error[E0308]` at the
/// record's struct literal — never silent.
fn field_type(ty: bsql_build::RustType, nullable: bool, is_owned: bool) -> TokenStream2 {
    let spec = col_spec(ty);
    let base = match spec.borrow {
        // Value types + 1-D arrays: both twins carry the owned self-owning type.
        BorrowKind::ByValue => spec.field_owned,
        // `text` / `bytea`: the owned twin copies (`String` / `Vec<u8>`), the
        // borrowed twin aliases the input.
        BorrowKind::Str => {
            if is_owned {
                spec.field_owned
            } else {
                quote!(&'q str)
            }
        }
        BorrowKind::Bytes => {
            if is_owned {
                spec.field_owned
            } else {
                quote!(&'q [u8])
            }
        }
    };
    if nullable {
        quote!(::core::option::Option<#base>)
    } else {
        base
    }
}

/// A record field's Rust type, honoring an external-type bridge. A bridged
/// scalar column becomes the BARE target type (both record twins — the target
/// is self-owning); a bridged array column becomes `Vec<Option<Target>>` (the
/// element `Option` is intrinsic to a PG array). An unbridged column falls
/// through to [`field_type`] (the native type, borrowed / owned per twin). A
/// nullable column wraps the base in `Option<..>`.
fn field_type_bridged(
    ty: bsql_build::RustType,
    nullable: bool,
    is_owned: bool,
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    let base = match ty {
        // A user-defined enum decodes into its generated Rust enum — the SAME
        // owned type in both twins (a fieldless enum is self-owning, so there is
        // no borrowed spelling and no `<'q>` contribution). This takes priority
        // over the bridge/native paths: an enum is never a bridge target and its
        // native `col_spec.field_owned` is a placeholder.
        bsql_build::RustType::UserEnum(id) => {
            let enum_ident = enums.ident(id)?;
            quote!(#enum_ident)
        }
        // A user-defined composite decodes into its generated Rust struct — the
        // SAME owned, `'static` type in both record twins (the struct owns its
        // fields, so there is no borrowed spelling and no `<'q>` contribution). A
        // composite is never a bridge target and its native `col_spec.field_owned`
        // is a placeholder, so this takes priority over the bridge/native paths.
        bsql_build::RustType::UserComposite(id) => {
            let struct_ident = enums.composite_ident(id)?;
            quote!(#struct_ident)
        }
        _ => match bridges.for_column(ty) {
            // Bridged: the target field type is the SAME for both twins (the
            // target owns its value; the converter takes the owned native value).
            Some((entry, is_array)) => {
                let target = &entry.target;
                if is_array {
                    quote!(::std::vec::Vec<::core::option::Option<#target>>)
                } else {
                    quote!(#target)
                }
            }
            // Unbridged: the native base type (non-null; the Option wrap is below).
            None => field_type(ty, false, is_owned),
        },
    };
    Ok(if nullable {
        quote!(::core::option::Option<#base>)
    } else {
        base
    })
}

/// Build a field identifier from an output column name, using a raw
/// identifier for a Rust keyword (`type` -> `r#type`). A name that is not
/// a valid identifier at all is a loud error (alias it in the SQL).
fn make_field_ident(name: &str, span: Span) -> syn::Result<Ident> {
    if let Ok(id) = syn::parse_str::<Ident>(name) {
        return Ok(id);
    }
    if let Ok(id) = syn::parse_str::<Ident>(&format!("r#{name}")) {
        return Ok(id);
    }
    Err(syn::Error::new(
        span,
        format!(
            "query!: output column name `{name}` is not a valid Rust \
             identifier; alias it in the SQL with `AS <valid_name>`"
        ),
    ))
}

/// Convert a PostgreSQL identifier or enum label to a candidate PascalCase Rust
/// identifier body: split on any run of non-alphanumeric characters (`_`, `-`,
/// spaces, punctuation), uppercase each segment's first character, and
/// concatenate (digits kept in place). The result may still be an invalid Rust
/// identifier (empty, or leading digit) — the caller validates via
/// [`syn::parse_str`] and reports a loud error, never a silent mangle.
fn pascal_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut at_boundary = true;
    for ch in s.chars() {
        if ch.is_alphanumeric() {
            if at_boundary {
                out.extend(ch.to_uppercase());
                at_boundary = false;
            } else {
                out.push(ch);
            }
        } else {
            at_boundary = true;
        }
    }
    out
}

/// PascalCase `raw` into a validated Rust identifier (a raw identifier for a
/// keyword where legal), or a loud error naming the offending source. `kind`
/// names the role (`enum type` / `enum variant`) for the message; `span`
/// locates it. Never a silent mangle — a name that cannot form a valid
/// identifier is a build error the migration author fixes.
fn pascal_ident(raw: &str, kind: &str, span: Span) -> syn::Result<Ident> {
    let candidate = pascal_case(raw);
    if let Ok(id) = syn::parse_str::<Ident>(&candidate) {
        return Ok(id);
    }
    if let Ok(id) = syn::parse_str::<Ident>(&format!("r#{candidate}")) {
        return Ok(id);
    }
    Err(syn::Error::new(
        span,
        format!(
            "query!: {kind} `{raw}` cannot be mapped to a valid Rust identifier \
             (PascalCased to `{candidate}`). Rename it in the migration to a name \
             that forms a valid Rust identifier."
        ),
    ))
}

/// Resolves a user-defined-type id to its generated Rust TYPE identifier — a
/// `RustType::UserEnum(id)` to the `user_types!()`-emitted `enum`, and a
/// `RustType::UserComposite(id)` to the emitted `struct` — that a column or
/// composite field of that type decodes into.
///
/// (Named `EnumTypes` historically; it now resolves BOTH user enums and
/// composites — a private macro-internal helper with no external surface.)
///
/// Built once per `query!` over exactly the user types the query references
/// (and, for a composite's own decoder, over its field types), so a migration
/// type whose name cannot form a valid Rust identifier fails ONCE, loudly, and
/// the per-column / per-param / per-field codegen looks the ident up against a
/// pre-validated map (the lookup carries the query span for the — structurally
/// unreachable — "id not pre-resolved" internal guard, keeping the codegen
/// panic-free without an `unwrap`).
struct EnumTypes {
    /// `UserEnumId.0` -> the generated enum's PascalCase type identifier.
    by_id: std::collections::BTreeMap<u32, Ident>,
    /// `UserCompositeId.0` -> the generated struct's PascalCase type identifier.
    composites_by_id: std::collections::BTreeMap<u32, Ident>,
    /// The query name's span, for the internal-guard diagnostic.
    span: Span,
}

impl EnumTypes {
    /// The generated type identifier for a user-enum id. Every enum the query's
    /// columns / params carry was resolved at construction, so this is total for
    /// them; a miss is an internal logic error surfaced loudly (never a panic).
    fn ident(&self, id: bsql_build::UserEnumId) -> syn::Result<&Ident> {
        self.by_id.get(&id.0).ok_or_else(|| {
            syn::Error::new(self.span, "query!: internal error — unresolved user-enum id")
        })
    }

    /// The generated struct identifier for a user-composite id. Total for every
    /// composite the query's columns / a composite's fields carry (resolved at
    /// construction); a miss is an internal logic error surfaced loudly.
    fn composite_ident(&self, id: bsql_build::UserCompositeId) -> syn::Result<&Ident> {
        self.composites_by_id.get(&id.0).ok_or_else(|| {
            syn::Error::new(
                self.span,
                "query!: internal error — unresolved user-composite id",
            )
        })
    }
}

/// Resolve every user enum / composite the query's columns and parameters
/// reference into an [`EnumTypes`] map, against the SAME catalog the query was
/// typed with. A bad type name is a loud error here, once, rather than at each
/// use site.
fn resolve_enum_types(
    catalog: &bsql_build::Catalog,
    shape: &bsql_build::DynamicShape,
    span: Span,
) -> syn::Result<EnumTypes> {
    let types = shape
        .columns
        .iter()
        .map(|col| col.ty)
        .chain(shape.params.iter().map(|p| p.element()));
    resolve_user_type_idents(catalog, types, span)
}

/// Resolve the user enums / composites among an iterator of [`RustType`]s into
/// an [`EnumTypes`] map (against `catalog`), validating each type's name into a
/// Rust identifier ONCE. Shared by the query-shape resolver above and the
/// composite emitter (which resolves its own FIELD types). Native types are
/// skipped; a duplicate id is resolved once.
fn resolve_user_type_idents(
    catalog: &bsql_build::Catalog,
    types: impl Iterator<Item = bsql_build::RustType>,
    span: Span,
) -> syn::Result<EnumTypes> {
    let mut by_id = std::collections::BTreeMap::new();
    let mut composites_by_id = std::collections::BTreeMap::new();
    for ty in types {
        match ty {
            bsql_build::RustType::UserEnum(id) => {
                if by_id.contains_key(&id.0) {
                    continue;
                }
                let (enum_name, _labels) = catalog.user_enum(id).ok_or_else(|| {
                    syn::Error::new(span, "query!: internal error — unresolved user-enum id")
                })?;
                by_id.insert(id.0, pascal_ident(enum_name, "enum type", span)?);
            }
            bsql_build::RustType::UserComposite(id) => {
                if composites_by_id.contains_key(&id.0) {
                    continue;
                }
                let (comp_name, _fields) = catalog.user_composite(id).ok_or_else(|| {
                    syn::Error::new(span, "query!: internal error — unresolved user-composite id")
                })?;
                composites_by_id.insert(id.0, pascal_ident(comp_name, "composite type", span)?);
            }
            _ => {}
        }
    }
    Ok(EnumTypes {
        by_id,
        composites_by_id,
        span,
    })
}

/// Emit the borrowed + owned record twins and their `decode` fns.
fn emit_records(
    name: &Ident,
    columns: &[bsql_build::InferredColumn],
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    let owned_name = format_ident!("{}Owned", name);

    // One field identifier per output column. A duplicate output column
    // name is already a loud `InferError::DuplicateOutputColumn` from the
    // inference engine; the Rust "two fields, one name" rule (E0124) is
    // the structural backstop.
    let mut field_idents = Vec::with_capacity(columns.len());
    // Per-column 0-based index, as a `u16` for the `TruncatedColumnLen`
    // diagnostic on a short row — the width of `DecodeError::column_idx` (the wire
    // column count is a non-negative `i16`, so `u16` holds every index). Bounded
    // loudly so the cast is never lossy.
    let mut col_idx_u16 = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        field_idents.push(make_field_ident(&col.name, name.span())?);
        match u16::try_from(idx) {
            Ok(value) => col_idx_u16.push(value),
            Err(_) => {
                return Err(syn::Error::new(
                    name.span(),
                    "query!: too many output columns (a column index exceeds the u16 index space)",
                ))
            }
        }
    }

    // The `DataRow` column-count header value (an `i16`). Bounded loudly
    // by the same > 256 guard as the column indices above.
    let n_i16: i16 = match i16::try_from(columns.len()) {
        Ok(value) => value,
        Err(_) => {
            return Err(syn::Error::new(
                name.span(),
                "query!: more than 256 output columns are not supported",
            ))
        }
    };

    let has_borrowed = columns.iter().any(|c| column_borrows(c.ty, bridges));
    // The vectorized fast path applies only when every column is a
    // fixed-width binary type AND none is nullable: a NULL or a
    // variable-width column would shift every later column's offset, so
    // const offsets only hold under both conditions. A bridge does NOT
    // change the wire shape (the native pivot is decoded, then reshaped), so
    // eligibility keys on the NATIVE fixed width exactly as before.
    let all_fixed_not_null = columns
        .iter()
        .all(|c| !c.nullable && fixed_width(c.ty).is_some());

    let borrowed_fields = field_idents
        .iter()
        .zip(columns)
        .map(|(id, col)| {
            let ty = field_type_bridged(col.ty, col.nullable, false, bridges, enums)?;
            // `pub`: a record is DATA (the query's output row), not an invariant —
            // no encapsulation is load-bearing. Public fields let a generic /
            // cross-module data layer (e.g. `SyncBackend`) READ a `Vec<Q::Owned>`
            // returned across the module boundary; module-private fields made an
            // owned record opaque outside its defining module.
            Ok(quote! { pub #id: #ty })
        })
        .collect::<syn::Result<Vec<_>>>()?;

    // The borrowed record carries `<'q>` ONLY when it has a borrowing
    // (text / bytea) field — otherwise the lifetime would be unused, which
    // the workspace `unused_lifetimes` floor forbids.
    let borrowed_generics = if has_borrowed { quote!(<'q>) } else { quote!() };
    let borrowed_input = if has_borrowed {
        quote!(body: &'q [u8])
    } else {
        quote!(body: &[u8])
    };

    let cx = Codegen { bridges, enums };
    let borrowed_body = decode_body(
        &field_idents,
        columns,
        &col_idx_u16,
        n_i16,
        all_fixed_not_null,
        false,
        &cx,
    )?;

    let allow_reason = "generated typed-record fields are the query's output row shape; a consumer may read any subset of the columns";

    // `Eq` is derived only when EVERY column type implements it; a record with
    // a float column derives `PartialEq` but not `Eq` (`f32`/`f64` are not
    // `Eq`). A BRIDGED column's target `Eq`-ness is unknown to the macro (e.g.
    // `serde_json::Value` is `PartialEq` but not `Eq`), so a bridged column is
    // conservatively treated as non-`Eq`. Both twins share the same column set,
    // so one decision covers both.
    let derives = if columns
        .iter()
        .all(|c| bridges.for_column(c.ty).is_none() && type_impls_eq(c.ty))
    {
        quote!(Debug, Clone, PartialEq, Eq)
    } else {
        quote!(Debug, Clone, PartialEq)
    };

    // The owned twin. When the query projects a borrowing (`text` / `bytea`)
    // column, the owned record genuinely DIFFERS from the borrowed one
    // (`String` / `Vec<u8>` vs `&'q str` / `&'q [u8]`), so it is a distinct
    // struct with its own `decode`. When NO column borrows (the common
    // all-scalar case — integers/float/bool/uuid/temporal/numeric/arrays, and
    // BRIDGED columns, all self-owning), the owned fields, the owned decode
    // body, AND the borrowed record's spelling (lifetime-free in this branch)
    // are byte-identical, so the owned twin is a plain type ALIAS instead of a
    // duplicate struct + four derives + a ~45-line `decode` monomorphization.
    // `#name` is `'static + Send` here, satisfying `TypedQuery::Owned: Send +
    // 'static`, and both `#owned_name::decode` and `type Owned = #owned_name`
    // (in `emit_dynamic_wire`) resolve straight through the alias.
    let owned_items = if has_borrowed {
        let owned_fields = field_idents
            .iter()
            .zip(columns)
            .map(|(id, col)| {
                let ty = field_type_bridged(col.ty, col.nullable, true, bridges, enums)?;
                // `pub` — see the borrowed-twin note above (a record is data, not
                // an invariant; a returned `Vec<Q::Owned>` must be readable).
                Ok(quote! { pub #id: #ty })
            })
            .collect::<syn::Result<Vec<_>>>()?;
        let owned_body = decode_body(
            &field_idents,
            columns,
            &col_idx_u16,
            n_i16,
            all_fixed_not_null,
            true,
            &cx,
        )?;
        quote! {
            #[derive(#derives)]
            #[allow(dead_code, reason = #allow_reason)]
            pub struct #owned_name {
                #(#owned_fields),*
            }

            impl #owned_name {
                /// Decode one raw `DataRow` payload (the wire bytes after the
                /// field-count header) into the owned record.
                pub fn decode(body: &[u8])
                    -> ::core::result::Result<Self, ::bsql::__rt::DecodeError>
                {
                    #owned_body
                }
            }
        }
    } else {
        quote! {
            pub type #owned_name = #name;
        }
    };

    Ok(quote! {
        #[derive(#derives)]
        #[allow(dead_code, reason = #allow_reason)]
        pub struct #name #borrowed_generics {
            #(#borrowed_fields),*
        }

        impl #borrowed_generics #name #borrowed_generics {
            /// Decode one raw `DataRow` payload (the wire bytes after the
            /// field-count header) into the borrowed record. Borrowed
            /// `text` columns alias the input bytes — zero allocation.
            pub fn decode(#borrowed_input)
                -> ::core::result::Result<Self, ::bsql::__rt::DecodeError>
            {
                #borrowed_body
            }
        }

        #owned_items
    })
}

/// The body of one `decode` fn: the optional vectorized fast path (only
/// for an all-fixed-width, all-NOT-NULL row) followed by the general
/// per-cell path (which classifies NULL / variable-width / oversized
/// rows). When the fast path is not eligible, only the per-cell path is
/// emitted.
/// The per-column codegen resolution context: the external-type bridges and the
/// resolved user enums, bundled so the record-decode coordinator threads one
/// reference instead of two (and stays within the argument-count wall).
struct Codegen<'a> {
    /// The external-type bridge overrides (possibly empty).
    bridges: &'a Bridges,
    /// The resolved user-enum type identifiers for this query.
    enums: &'a EnumTypes,
}

fn decode_body(
    field_idents: &[Ident],
    columns: &[bsql_build::InferredColumn],
    col_idx_u16: &[u16],
    n_i16: i16,
    all_fixed_not_null: bool,
    is_owned: bool,
    cx: &Codegen<'_>,
) -> syn::Result<TokenStream2> {
    let per_cell = per_cell_path(field_idents, columns, col_idx_u16, is_owned, cx.bridges, cx.enums)?;
    // The fast path only fires when every column is fixed-width; a user-enum
    // column is variable-width (`fixed_width` is `None`), so a query with an
    // enum column never takes the fast path — `fast_path` never sees an enum and
    // stays enum-agnostic.
    if all_fixed_not_null {
        let fast = fast_path(field_idents, columns, n_i16, cx.bridges);
        Ok(quote! {
            #fast
            #per_cell
        })
    } else {
        Ok(per_cell)
    }
}

/// The vectorized all-fixed-width path: the whole `DataRow` body is the
/// 2-byte count header plus, per column, a 4-byte length prefix and a
/// fixed-width payload, with no NULL and no variable column — so the body
/// is exactly `2 + TOTAL` bytes and every column sits at a constant
/// offset. The body length is validated once; each column's length prefix
/// is checked against its fixed width and its payload read at a const
/// offset. ANY deviation (a NULL, a wrong width, an oversized or
/// truncated row) `break`s to the general per-cell path, which classifies
/// it precisely — so this path can never mask an error, only accelerate
/// the well-formed case.
fn fast_path(
    field_idents: &[Ident],
    columns: &[bsql_build::InferredColumn],
    n_i16: i16,
    bridges: &Bridges,
) -> TokenStream2 {
    // Count header value (i16) and the exact post-header byte total.
    let n = columns.len();
    let n_i16 = Literal::i16_suffixed(n_i16);
    let mut total: usize = 0;
    for col in columns {
        if let Some((width, _)) = fixed_width(col.ty) {
            total = total.saturating_add(4).saturating_add(width);
        }
    }
    let total_lit = Literal::usize_unsuffixed(total);

    let col_stmts = field_idents.iter().zip(columns).enumerate().map(|(i, (id, col))| {
        let is_last = i + 1 == n;
        // The trailing remainder after the LAST column's payload is
        // unused (we return right after), so bind it to `_`; earlier
        // remainders feed the next column.
        let trailing = if is_last { quote!(_) } else { quote!(__after) };
        match fixed_width(col.ty) {
            Some((width, width_i32)) => {
                let width_lit = Literal::usize_unsuffixed(width);
                let width_i32_lit = Literal::i32_suffixed(width_i32);
                // The SAME marker the const validator pins the wire OID to —
                // decoded via `ColCellAt::decode_at`, so the fast path reads the
                // OID-validated type exactly like the per-cell path.
                let marker = col_spec(col.ty).marker;
                // The native decoded value. A bridged fixed-width column (a
                // value-type / primitive — `text` / `bytea` / arrays are not
                // fixed-width, so they never reach this path) reshapes it with
                // the converter; the native decode itself is unchanged.
                let bound = match bridges.scalar(col.ty) {
                    Some(entry) => {
                        let conv = &entry.converter;
                        quote! { #conv(__native) }
                    }
                    None => quote! { __native },
                };
                quote! {
                    let ::core::option::Option::Some((__len, __after)) =
                        __after.split_first_chunk::<4>() else { break 'fast };
                    if i32::from_be_bytes(*__len) != #width_i32_lit { break 'fast; }
                    let ::core::option::Option::Some((__data, #trailing)) =
                        __after.split_first_chunk::<#width_lit>() else { break 'fast };
                    let __native = match <#marker as ::bsql::__rt::ColCellAt<'_>>::decode_at(__data) {
                        ::core::result::Result::Ok(__value) => __value,
                        ::core::result::Result::Err(__err) =>
                            return ::core::result::Result::Err(__err),
                    };
                    let #id = #bound;
                }
            }
            // Unreachable: `fast_path` is only emitted when every column
            // is fixed-width. If a non-fixed column ever reached here, the
            // correct behaviour is to defer to the per-cell path — never a
            // silent misread — so break out.
            None => quote! { break 'fast; },
        }
    });

    quote! {
        'fast: {
            let ::core::option::Option::Some((__count, __after)) =
                body.split_first_chunk::<2>() else { break 'fast };
            if i16::from_be_bytes(*__count) != #n_i16 { break 'fast; }
            if __after.len() != #total_lit { break 'fast; }
            #(#col_stmts)*
            return ::core::result::Result::Ok(Self { #(#field_idents),* });
        }
    }
}

/// The general per-cell path: walk the row column by column via the
/// shipped `DataRowRef` / `ColumnsIter`, decoding each cell with
/// `Cell<BinaryFmt>`. This covers NULL (a NULL in a NOT-NULL column is a
/// classified `NullInNonNullColumn`; a nullable column becomes
/// `Option<T>`), variable-width `text`, and an oversized / truncated row
/// (each malformed shape surfaces a classified `DecodeError`). No silent
/// default, no panic.
fn per_cell_path(
    field_idents: &[Ident],
    columns: &[bsql_build::InferredColumn],
    col_idx_u16: &[u16],
    is_owned: bool,
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    if columns.is_empty() {
        // No projected columns: validate the row body and build the empty
        // record. `parse` still fails closed on a malformed count header.
        return Ok(quote! {
            ::bsql::__rt::DataRowRef::parse(body)?;
            ::core::result::Result::Ok(Self {})
        });
    }

    let stmts = field_idents
        .iter()
        .zip(columns)
        .zip(col_idx_u16)
        .map(|((id, col), idx)| {
            let idx_lit = Literal::u16_suffixed(*idx);
            let value = per_cell_value_expr(col.ty, col.nullable, is_owned, bridges, enums)?;
            Ok(quote! {
                let __cell = match ::core::iter::Iterator::next(&mut __cols) {
                    ::core::option::Option::Some(__result) => __result?,
                    // The row declared fewer columns than the query
                    // projects: a classified short-row error, never a
                    // silently-defaulted field.
                    ::core::option::Option::None => return ::core::result::Result::Err(
                        ::bsql::__rt::DecodeError::TruncatedColumnLen {
                            column_idx: #idx_lit,
                        },
                    ),
                };
                let #id = #value;
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;

    Ok(quote! {
        let __row = ::bsql::__rt::DataRowRef::parse(body)?;
        let mut __cols = ::bsql::__rt::DataRowRef::columns(&__row);
        #(#stmts)*
        ::core::result::Result::Ok(Self { #(#field_idents),* })
    })
}

/// One per-cell column value expression. A NOT-NULL column returns the
/// decoded value or a classified `NullInNonNullColumn` on NULL; a
/// nullable column returns `Some(value)` / `None`.
fn per_cell_value_expr(
    ty: bsql_build::RustType,
    nullable: bool,
    is_owned: bool,
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    let decode_expr = decode_value_expr_bridged(ty, is_owned, bridges, enums)?;
    Ok(if nullable {
        quote! {
            match __cell {
                ::core::option::Option::Some(__bytes) =>
                    ::core::option::Option::Some(#decode_expr),
                ::core::option::Option::None => ::core::option::Option::None,
            }
        }
    } else {
        quote! {
            match __cell {
                ::core::option::Option::Some(__bytes) => #decode_expr,
                // A NULL in a NOT-NULL-typed column: a classified
                // decode error, never a silent default or panic.
                ::core::option::Option::None => return ::core::result::Result::Err(
                    ::bsql::__rt::DecodeError::NullInNonNullColumn,
                ),
            }
        }
    })
}

/// The decode call for one non-NULL cell body (`__bytes`).
///
/// Decodes through `<marker as ColCellAt>::decode_at` on the SAME marker the
/// runtime sources the row OID from (`<marker as ColCellAt>::OID` IS the row's
/// OID), so the decoded value is definitionally the OID-declared type — decoder
/// and row OID are one source and cannot drift. Owned `text` copies
/// the borrowed `&str` into a `String`, owned `bytea` the `&[u8]` into a
/// `Vec<u8>`; every value type / array decodes by value. Exhaustive over
/// [`BorrowKind`] — a new borrow family forces its owned copy-out here.
fn decode_call_expr(ty: bsql_build::RustType, is_owned: bool) -> TokenStream2 {
    let spec = col_spec(ty);
    let marker = spec.marker;
    let raw = quote! {
        <#marker as ::bsql::__rt::ColCellAt<'_>>::decode_at(__bytes)?
    };
    match spec.borrow {
        BorrowKind::ByValue => raw,
        BorrowKind::Str => {
            if is_owned {
                quote! { ::std::string::String::from(#raw) }
            } else {
                raw
            }
        }
        BorrowKind::Bytes => {
            if is_owned {
                quote! { <[u8]>::to_vec(#raw) }
            } else {
                raw
            }
        }
    }
}

/// One non-NULL cell body (`__bytes`) decoded into the record field value,
/// honoring an external-type bridge. An unbridged column is the native
/// [`decode_call_expr`]. A bridged SCALAR column applies the converter to the
/// OWNED native value: `conv(<owned native decode>)` (the converter takes the
/// owned native type, so a bridged `text` column materializes its `String`
/// first — it is no longer zero-copy, exactly like the owned twin). A bridged
/// ARRAY column decodes the native `Vec<Option<NativeElem>>` and applies the
/// converter per element, yielding `Vec<Option<Target>>` — the whole-column
/// `Option` (for a nullable array) is added by the caller. The reshape is a
/// free-fn call the optimizer inlines away; the wire decode is unchanged.
fn decode_value_expr_bridged(
    ty: bsql_build::RustType,
    is_owned: bool,
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    // A user-defined enum decodes its LABEL TEXT (the `&str` wire pivot, via the
    // SAME `ColCellAt` decode the const validator pins the OID to) and reshapes
    // it into the generated Rust enum through `PgEnum::from_wire_label`. That
    // reshape is FALLIBLE — a label the migration did not declare is a
    // classified `DecodeError::UnknownEnumLabel` (never a panic or a
    // plausible-but-wrong variant). The `&str` borrows `__bytes` only for the
    // duration of `from_wire_label`, which returns an OWNED enum, so an enum
    // field never carries `<'q>` (its `col_spec.borrow` is `ByValue`).
    if let bsql_build::RustType::UserEnum(id) = ty {
        let enum_ident = enums.ident(id)?;
        let label = decode_call_expr(ty, is_owned);
        return Ok(quote! {
            <#enum_ident as ::bsql::__rt::PgEnum>::from_wire_label(#label)?
        });
    }
    // A user-defined composite decodes its ROW-TYPE BINARY FRAME (`__bytes`) into
    // the generated struct by walking the frame — `PgComposite::decode_row`
    // recurses into each field's own decoder. FALLIBLE: a malformed / arity-
    // drifted frame is a classified `DecodeError` (never a panic or a partial
    // record). The struct is OWNED and `'static`, so a composite field never
    // carries `<'q>` (its `col_spec.borrow` is `ByValue`).
    if let bsql_build::RustType::UserComposite(id) = ty {
        let struct_ident = enums.composite_ident(id)?;
        return Ok(quote! {
            <#struct_ident as ::bsql::__rt::PgComposite>::decode_row(__bytes)?
        });
    }
    Ok(match bridges.for_column(ty) {
        None => decode_call_expr(ty, is_owned),
        Some((entry, false)) => {
            // Scalar bridge: reshape the OWNED native value (same for both
            // record twins, since the target owns its value).
            let conv = &entry.converter;
            let native = decode_call_expr(ty, true);
            quote! { #conv(#native) }
        }
        Some((entry, true)) => {
            // Array-element bridge: decode the native `Vec<Option<NativeElem>>`,
            // then map the converter over each present element.
            let conv = &entry.converter;
            let target = &entry.target;
            let marker = col_spec(ty).marker;
            quote! {
                ::core::iter::Iterator::collect::<
                    ::std::vec::Vec<::core::option::Option<#target>>,
                >(::core::iter::Iterator::map(
                    ::core::iter::IntoIterator::into_iter(
                        <#marker as ::bsql::__rt::ColCellAt<'_>>::decode_at(__bytes)?,
                    ),
                    |__elem| ::core::option::Option::map(__elem, #conv),
                ))
            }
        }
    })
}

// ════════════════════════════════════════════════════════════════════
// query! — const wire artifact + fingerprint seal
// ════════════════════════════════════════════════════════════════════
//
// In addition to the typed-record twins above, `query!` emits the
// compile-time wire artifact: the pre-baked `Parse` / `Bind`-prefix byte
// templates, the parameter / row OID lists, and the content-addressed
// statement name — all derived from the inferred `QueryShape`. The
// artifact is delivered as an uninhabited carrier type + a
// `QueryFingerprint` impl, and the `PreparedQuery` const is minted
// through the proto-owned `run::<Carrier>()` boundary, which forces the
// VALIDATING constructor in the runtime crate. A drift between the baked
// wire bytes and the declared parameter / row types is a const-eval
// failure (`error[E0080]`); a fabricated artifact cannot compile.

/// The scalar column OID numeric value for a supported Rust type —
/// projected from the single [`col_spec`] source. Baked into the `Parse`
/// template's trailing OID section (the numeric wire representation); the
/// runtime `new_prepared_query` then const-checks those baked bytes
/// against the parameter tuple's own `ParamsWriter::OIDS`, so the wire
/// cannot lie about the declared parameter types.
fn rust_type_oid(ty: bsql_build::RustType) -> u32 {
    col_spec(ty).oid_value
}

/// The `'static`-lifetime tuple-element marker for the `Params` / `Row`
/// type-level tuples — projected from the single [`col_spec`] source. This
/// is the SAME marker the record decode routes through
/// (`<marker as ColCellAt>::decode_at`), so the decoder and this OID-pinned
/// row-tuple element cannot disagree. `text` is `&'static str` (the
/// static-placeholder lifetime idiom the runtime decoders project to
/// `&'a str`).
fn tuple_marker(ty: bsql_build::RustType) -> TokenStream2 {
    col_spec(ty).marker
}

/// Build a tuple-type token stream from element markers. Arity 0 ->
/// `()`; arity 1 -> `(T0,)` (the trailing comma is load-bearing);
/// arity >= 2 -> `(T0, T1, ...)`.
fn tuple_type(markers: &[TokenStream2]) -> TokenStream2 {
    match markers {
        [] => quote!(()),
        [single] => quote!((#single,)),
        many => quote!((#(#many),*)),
    }
}

/// The array (`T[]`) OID numeric value for a SCALAR element type — for a
/// single array parameter feeding a `col = ANY($N)` in-list. Projected from
/// the single [`col_spec`] source (its `array_oid_value`), baked into the
/// `Parse` template's OID section. An array element (structurally dead:
/// inference rejects a nested array) yields the fail-closed non-OID `0`.
fn array_oid(ty: bsql_build::RustType) -> u32 {
    col_spec(ty).array_oid_value
}

/// The `'static`-lifetime type-level marker for one parameter tuple
/// element. A toggled optional filter is `Option<T>` (passing `None`
/// disables it); a `= ANY($N)` in-list is the array slice `&'static [T]`;
/// a plain scalar is `T`.
fn param_tuple_marker(
    shape: bsql_build::ParamShape,
    enums: &EnumTypes,
    lt: &TokenStream2,
) -> syn::Result<TokenStream2> {
    use bsql_build::{ParamShape, RustType};
    // The scalar element marker for a parameter, at the lifetime `lt`. A `text` /
    // `bytea` parameter BORROWS the caller's bytes (`&'lt str` / `&'lt [u8]`),
    // so `lt` threads the verb's parameter lifetime through — letting a RUNTIME
    // `&str` bind, not only a `&'static` literal. A user-enum parameter is NOT
    // the `&str` wire pivot (which the OUTPUT row-tuple uses): it is
    // `EnumLabel<TheEnum>`, an `unspecified`-typed (OID 0) label the server
    // infers to the enum from context — a `text` (25) parameter has no cast to
    // an enum and is rejected. The phantom enum type keeps it enum-specific, so
    // a query expecting one enum rejects another's label at compile time. A
    // by-value scalar (`i64`, `Numeric`, …) ignores `lt` — it owns its data.
    let scalar = |ty: RustType| -> syn::Result<TokenStream2> {
        match ty {
            RustType::UserEnum(id) => {
                let enum_ident = enums.ident(id)?;
                Ok(quote!(::bsql::__rt::EnumLabel<#enum_ident>))
            }
            // A composite PARAMETER (binding a whole composite value as `$N`, the
            // row-type binary ENCODE) is a follow-up — decode is the high-value
            // half and lands first. The precise reason it is staged as a WHOLE
            // (rather than shipping the subset that IS encodable): an ALL-NATIVE
            // composite's field type OIDs are STABLE, so its `record` frame could
            // be encoded — BUT a composite with an enum / domain / nested-composite
            // field needs SERVER-DYNAMIC OIDs both for the composite's own type
            // (the `$N` param OID, to select the binary recv function) and for that
            // field inside the frame (`record_recv` validates each field OID
            // concretely), and bsql does NO connect-time OID resolution (the same
            // boundary the enum decode rides). Shipping only the all-native subset
            // would be a NON-UNIVERSAL partial, so the whole feature stages behind
            // a loud, located rejection — never a silently-wrong or half-correct
            // encode.
            RustType::UserComposite(_) => Err(syn::Error::new(
                enums.span,
                "query!: binding a user-defined COMPOSITE value as a `$N` parameter \
                 is not yet supported (decode of a composite column works); pass the \
                 composite's fields as separate scalar parameters, or construct the \
                 composite in SQL with `ROW($1, $2, ...)::your_type`.",
            )),
            _ => {
                let spec = col_spec(ty);
                Ok(match spec.borrow {
                    BorrowKind::Str => quote!(& #lt str),
                    BorrowKind::Bytes => quote!(& #lt [u8]),
                    BorrowKind::ByValue => spec.marker,
                })
            }
        }
    };
    match shape {
        ParamShape::Scalar(ty) => scalar(ty),
        ParamShape::Optional(ty) => {
            let inner = scalar(ty)?;
            Ok(quote!(::core::option::Option<#inner>))
        }
        // A `col = ANY($N)` in-list over a user enum would bind an ARRAY of the
        // enum, whose element OID is server-dynamic and whose array wire framing
        // is out of scope for v1 — a loud rejection, never a silently-wrong
        // encoding. Native-element arrays are unchanged.
        ParamShape::Array(RustType::UserEnum(_)) => Err(syn::Error::new(
            enums.span,
            "query!: a `= ANY($N)` in-list over a user-defined enum column is not \
             yet supported; compare the enum column with a scalar `$N` instead \
             (`WHERE col = $1`).",
        )),
        ParamShape::Array(ty) => {
            let elem = tuple_marker(ty);
            Ok(quote!(& #lt [#elem]))
        }
    }
}

/// The numeric OID baked into the Parse template for one parameter. A
/// toggled `Option<T>` keeps the scalar OID (a SQL NULL is typed by its
/// column); a `= ANY($N)` array uses the element type's array OID; a
/// user-enum parameter is UNSPECIFIED (0) — the server infers the enum type
/// from context (a PG enum has no implicit `text` cast).
fn param_oid_value(shape: bsql_build::ParamShape) -> u32 {
    use bsql_build::{ParamShape, RustType};
    match shape {
        ParamShape::Scalar(RustType::UserEnum(_)) | ParamShape::Optional(RustType::UserEnum(_)) => {
            0
        }
        ParamShape::Scalar(ty) | ParamShape::Optional(ty) => rust_type_oid(ty),
        ParamShape::Array(ty) => array_oid(ty),
    }
}

/// The type-level `Params` / `Row` tuple markers, shared across every wire
/// variant of one query (only the runtime `ORDER BY` SQL differs between
/// variants). Bundled so the per-carrier emit takes one argument for both.
struct TypeTuples {
    /// The `Params` tuple type at `'static` (from the lowered `$N` shapes) — the
    /// OID/const-validator marker (`QueryFingerprint::Params`, `PREPARED`).
    params: TokenStream2,
    /// The SAME `Params` tuple at the GAT lifetime `'p` (`text`/`bytea`/array
    /// params borrow `&'p …`) — the verb-argument marker (`TypedQuery::Params<'p>`),
    /// so a RUNTIME `&str` binds. Scalar/param-free queries make it identical to
    /// [`params`](Self::params).
    params_p: TokenStream2,
    /// The `Row` tuple type (from the projected columns' native markers).
    row: TokenStream2,
}

/// Emit every const wire artifact for one (possibly dynamic) query: the
/// dynamic-form budget assertions, then either ONE carrier (no runtime
/// `ORDER BY` allow-set) or one carrier per allowed ordering plus the
/// closed selector enum the caller picks from at runtime.
fn emit_dynamic_wire(
    name: &Ident,
    shape: &bsql_build::DynamicShape,
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    // The runtime `ParamsWriter` impls cover tuple arity 0..=32 (raised from 16
    // so a wide parameterised query is not capped); the `RowDecode` result
    // decoders remain 0..=16 (a SEPARATE wire mechanism). A query outside either
    // envelope is a loud rejection, not a silent truncation or an opaque
    // trait-bound error.
    if shape.params.len() > 32 {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "query!: {} parameters — the prepared-query wire path \
                 supports at most 32 `$N` parameters",
                shape.params.len()
            ),
        ));
    }
    if shape.columns.len() > 16 {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "query!: {} result columns — the prepared-query wire path \
                 supports at most 16 projected columns",
                shape.columns.len()
            ),
        ));
    }

    // Type-level Params / Row tuples (markers), shared across every wire
    // variant — only the ORDER BY differs between variants. Params come
    // from the lowered `$N` shapes (scalar / Option / array); Row from the
    // projected columns (base marker — nullability is the records' concern,
    // not the wire OID's).
    let param_markers: Vec<TokenStream2> = shape
        .params
        .iter()
        .map(|p| param_tuple_marker(*p, enums, &quote!('static)))
        .collect::<syn::Result<Vec<_>>>()?;
    // The SAME markers at the verb-argument GAT lifetime `'p` (borrowing params
    // become `&'p …`), for `TypedQuery::Params<'p>`.
    let param_markers_p: Vec<TokenStream2> = shape
        .params
        .iter()
        .map(|p| param_tuple_marker(*p, enums, &quote!('p)))
        .collect::<syn::Result<Vec<_>>>()?;
    let row_markers: Vec<TokenStream2> =
        shape.columns.iter().map(|c| tuple_marker(c.ty)).collect();
    let tuples = TypeTuples {
        params: tuple_type(&param_markers),
        params_p: tuple_type(&param_markers_p),
        row: tuple_type(&row_markers),
    };

    // Dynamic-form budgets, enforced at const-evaluation: an over-budget
    // query is `error[E0080]` at the `query!` site (never a silent
    // truncation of filters / orderings). Within budget the assert is a
    // no-op the optimizer drops.
    let n_optional = shape
        .params
        .iter()
        .filter(|p| matches!(p, bsql_build::ParamShape::Optional(_)))
        .count();
    let n_optional_lit = Literal::usize_unsuffixed(n_optional);
    let n_variants_lit = Literal::usize_unsuffixed(shape.variants.len());
    let budget = quote! {
        const _: () = ::core::assert!(
            #n_optional_lit <= ::bsql::__rt::query_budget::MAX_OPTIONAL_FILTERS,
            "query!: too many OPTIONAL(...) toggled filters in one query",
        );
        const _: () = ::core::assert!(
            #n_variants_lit <= ::bsql::__rt::query_budget::MAX_ORDER_BY_VARIANTS,
            "query!: too many runtime ORDER BY orderings in one query",
        );
    };

    match &shape.order_by {
        // No runtime ORDER BY allow-set: one carrier named `{Name}Query`
        // (identical to the non-dynamic path).
        None => {
            let variant = shape.variants.first().ok_or_else(|| {
                syn::Error::new(name.span(), "query!: internal error — no wire variant")
            })?;
            let carrier = format_ident!("{}Query", name);
            let one = emit_carrier(
                &carrier,
                name,
                &variant.wire_sql,
                &shape.params,
                &shape.columns,
                &tuples,
                bridges,
            )?;
            Ok(quote! {
                #budget
                #one
            })
        }
        // A runtime ORDER BY allow-set: one carrier per ordering plus a
        // closed selector enum. The caller picks a variant at runtime; an
        // ordering outside the set cannot be named (the enum has only the
        // declared variants), and no SQL string is built.
        Some(orderings) => {
            let mut carriers: Vec<TokenStream2> = Vec::with_capacity(orderings.len());
            let mut variant_idents: Vec<Ident> = Vec::with_capacity(orderings.len());
            let mut carrier_idents: Vec<Ident> = Vec::with_capacity(orderings.len());
            for (ordering, variant) in orderings.iter().zip(shape.variants.iter()) {
                let variant_ident = format_ident!("{}", ordering.variant_ident);
                let carrier = format_ident!("{}{}Query", name, ordering.variant_ident);
                let emitted = emit_carrier(
                    &carrier,
                    name,
                    &variant.wire_sql,
                    &shape.params,
                    &shape.columns,
                    &tuples,
                    bridges,
                )?;
                carriers.push(emitted);
                variant_idents.push(variant_ident);
                carrier_idents.push(carrier);
            }
            let enum_ident = format_ident!("{}OrderBy", name);
            let enum_doc = format!(
                "Runtime `ORDER BY` selector for the `{name}` query — a CLOSED \
                 allow-set of orderings. Pick one variant at runtime; \
                 [`{enum_ident}::prepared`](Self::prepared) returns its baked, \
                 content-addressed prepared query. An ordering outside the set \
                 cannot be named (no SQL string is built, so there is no \
                 injection surface)."
            );
            let prepared_doc =
                "The baked prepared query for the selected ordering.".to_string();
            let allow_reason = "the generated ORDER BY selector is part of the query's public surface; a consumer may use any subset of its variants";
            let params_tuple = &tuples.params;
            let row_tuple = &tuples.row;
            let selector = quote! {
                #[doc = #enum_doc]
                #[derive(::core::fmt::Debug, ::core::clone::Clone, ::core::marker::Copy, ::core::cmp::PartialEq, ::core::cmp::Eq)]
                #[allow(dead_code, reason = #allow_reason)]
                pub enum #enum_ident {
                    #(#variant_idents),*
                }

                impl #enum_ident {
                    #[doc = #prepared_doc]
                    #[allow(dead_code, reason = #allow_reason)]
                    #[must_use]
                    pub const fn prepared(
                        self,
                    ) -> ::bsql::__rt::PreparedQuery<#params_tuple, #row_tuple> {
                        match self {
                            #( Self::#variant_idents => #carrier_idents::PREPARED, )*
                        }
                    }
                }
            };
            Ok(quote! {
                #budget
                #(#carriers)*
                #selector
            })
        }
    }
}

/// Emit ONE const wire artifact: the uninhabited carrier type, its
/// `QueryFingerprint` impl (baked Parse / Bind-prefix templates + OID
/// lists, derived from the lowered shape), the validated `PreparedQuery`
/// const minted through the proto-owned `run` boundary, and the
/// `wire_pin!` footprint guard.
fn emit_carrier(
    carrier: &Ident,
    name: &Ident,
    wire_sql: &str,
    params: &[bsql_build::ParamShape],
    columns: &[bsql_build::InferredColumn],
    tuples: &TypeTuples,
    bridges: &Bridges,
) -> syn::Result<TokenStream2> {
    let params_tuple = &tuples.params;
    let params_tuple_p = &tuples.params_p;
    let row_tuple = &tuples.row;
    // Content-addressed statement name: SHA-256 of the (lowered) SQL text,
    // truncated to 96 bits, hex-encoded, prefixed. Two distinct queries
    // cannot share a name without colliding their content addresses.
    let stmt_name = sha256_96_stmt_name(wire_sql);

    // Numeric param OIDs for baking the Parse template's OID section (an
    // array param bakes its array OID; a toggled Option keeps its scalar).
    let param_oid_values: Vec<u32> = params.iter().map(|p| param_oid_value(*p)).collect();

    // Pre-baked wire byte templates.
    let parse_template_bytes = build_parse_template_bytes(&stmt_name, wire_sql, &param_oid_values)?;
    let bind_prefix_bytes = build_bind_execute_prefix_bytes(&stmt_name);
    let parse_template_lit = byte_array_literal(&parse_template_bytes);
    let bind_prefix_lit = byte_array_literal(&bind_prefix_bytes);

    let carrier_doc = format!(
        "Uninhabited fingerprint carrier for the `{name}` query. Its \
         [`QueryFingerprint`](::bsql::QueryFingerprint) \
         impl holds the const wire artifact; \
         [`{carrier}::PREPARED`](Self::PREPARED) is the validated \
         prepared query minted through the proto-owned `run` boundary."
    );
    let prepared_doc = format!(
        "The validated, content-addressed prepared query for `{name}`, \
         minted at compile time through the proto-owned `run` boundary. \
         Its wire bytes are const-checked against the declared parameter \
         and row types; a drift is a build error."
    );
    let allow_reason =
        "the generated prepared-query artifact is part of the query's public surface; a consumer may use any subset of it";

    // The borrowed record's GAT projection. A query that projects a
    // borrowing (`text` / `bytea`) column makes the borrowed record carry
    // `<'q>` (it borrows the input bytes); a query with no borrowing column
    // makes the record lifetime-free, so the `TypedQuery::Record<'q>` GAT
    // leaves `'q` unused (verified to compile). A bridged `text` / `bytea`
    // column decodes into an owned target, so it does NOT borrow — the same
    // rule `emit_records` applies, so both agree on whether `<'q>` appears.
    let owned_name = format_ident!("{}Owned", name);
    let has_borrowed = columns.iter().any(|c| column_borrows(c.ty, bridges));
    let record_ty = if has_borrowed {
        quote!(#name<'q>)
    } else {
        quote!(#name)
    };

    Ok(quote! {
        #[doc = #carrier_doc]
        #[allow(dead_code, reason = #allow_reason)]
        pub enum #carrier {}

        impl ::bsql::__rt::QueryFingerprint for #carrier {
            type Params = #params_tuple;
            type Row = #row_tuple;
            const SQL: &'static str = #wire_sql;
            const STMT_NAME: &'static str = #stmt_name;
            const PARSE_TEMPLATE: &'static [u8] = #parse_template_lit;
            const BIND_EXECUTE_PREFIX: &'static [u8] = #bind_prefix_lit;
        }

        impl #carrier {
            #[doc = #prepared_doc]
            #[allow(dead_code, reason = #allow_reason)]
            pub const PREPARED: ::bsql::__rt::PreparedQuery<#params_tuple, #row_tuple> =
                ::bsql::__rt::prepared::run::<#carrier>();
        }

        // The execution bridge: ties this carrier to its prepared query and
        // the typed-record decoders, so a driver's `query::<#carrier>()` runs
        // the query and yields the macro's typed records. `PREPARED` is minted
        // through the proto-owned `run` boundary (re-running the const
        // validator, free + .rodata-deduped) rather than reaching the inherent
        // const above — referencing `#carrier::PREPARED` here would be
        // ambiguous between the inherent and this trait const.
        impl ::bsql::__rt::TypedQuery for #carrier {
            type Params<'p> = #params_tuple_p;
            type Row = #row_tuple;
            type Record<'q> = #record_ty;
            type Owned = #owned_name;
            const PREPARED: ::bsql::__rt::PreparedQuery<#params_tuple, #row_tuple> =
                ::bsql::__rt::prepared::run::<#carrier>();
            fn decode_borrowed(body: &[u8])
                -> ::core::result::Result<Self::Record<'_>, ::bsql::__rt::DecodeError>
            {
                #name::decode(body)
            }
            fn decode_owned(body: &[u8])
                -> ::core::result::Result<Self::Owned, ::bsql::__rt::DecodeError>
            {
                #owned_name::decode(body)
            }
        }

        // Footprint guard on the artifact: the carrier is a zero-size,
        // value-less marker; the wire data lives in `.rodata`.
        ::bsql::__rt::wire_pin!(#carrier, size = 0, align = 1);
    })
}

/// SHA-256 of the SQL bytes, truncated to 96 bits, hex-encoded, and
/// prefixed -> the content-addressed statement name `bsql_q_<24hex>`.
fn sha256_96_stmt_name(sql: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(sql.as_bytes());
    let mut name = String::with_capacity(31);
    name.push_str("bsql_q_");
    for byte in digest.iter().take(12) {
        name.push(hex_char(byte >> 4));
        name.push(hex_char(byte & 0x0F));
    }
    name
}

/// One lowercase hex digit for a 0..=15 nibble.
fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0'.saturating_add(nibble)),
        10..=15 => char::from(b'a'.saturating_add(nibble.saturating_sub(10))),
        // Unreachable for a masked nibble; fail to a stable digit
        // rather than panic (the input is always `>> 4` or `& 0x0F`).
        _ => '0',
    }
}

/// Build the `Parse`-frame template bytes (PG §55.2.2):
///
/// ```text
/// b'P' | len_i32_be | stmt_name | NUL | sql | NUL | n_param_types_i16_be |
///   oid_i32_be × n
/// ```
///
/// The length field is self-inclusive (counts itself and the body, but
/// not the leading tag byte). Sizes are computed with saturating /
/// checked arithmetic and a loud overflow rejection — never a wrapped
/// or truncated length.
fn build_parse_template_bytes(
    stmt_name: &str,
    sql: &str,
    param_oids: &[u32],
) -> syn::Result<Vec<u8>> {
    let stmt_bytes = stmt_name.as_bytes();
    let sql_bytes = sql.as_bytes();
    // length = 4 (self) + stmt_name + NUL + sql + NUL + 2 (n_param_types)
    //          + 4 × n
    let length_usize = 4usize
        .saturating_add(stmt_bytes.len())
        .saturating_add(1)
        .saturating_add(sql_bytes.len())
        .saturating_add(1)
        .saturating_add(2)
        .saturating_add(4usize.saturating_mul(param_oids.len()));
    // The PG length field is an i32; a query whose Parse frame would
    // exceed that is a loud rejection, not a wrapped length on the wire.
    let length_u32 = u32::try_from(length_usize).map_err(|_| {
        syn::Error::new(
            Span::call_site(),
            "query!: SQL too large to encode in a single Parse frame",
        )
    })?;
    let n_params_u16 = u16::try_from(param_oids.len()).map_err(|_| {
        syn::Error::new(
            Span::call_site(),
            "query!: more than 65535 parameters cannot be encoded",
        )
    })?;
    let mut out: Vec<u8> = Vec::with_capacity(length_usize.saturating_add(1));
    out.push(b'P');
    out.extend_from_slice(&length_u32.to_be_bytes());
    out.extend_from_slice(stmt_bytes);
    out.push(0);
    out.extend_from_slice(sql_bytes);
    out.push(0);
    out.extend_from_slice(&n_params_u16.to_be_bytes());
    for oid in param_oids {
        out.extend_from_slice(&oid.to_be_bytes());
    }
    Ok(out)
}

/// Build the `Bind`-frame prefix bytes: the empty-portal NUL followed by
/// the content-addressed statement name and its NUL. The param
/// format-code block, value block, and result-format trailer are NOT
/// baked here — they are emitted at frame-build time from the runtime
/// `ParamsWriter`, the sole binary-format authority, so the declared
/// format and the encoded value cannot drift.
fn build_bind_execute_prefix_bytes(stmt_name: &str) -> Vec<u8> {
    let stmt_bytes = stmt_name.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(stmt_bytes.len().saturating_add(2));
    out.push(0); // empty portal NUL
    out.extend_from_slice(stmt_bytes);
    out.push(0); // stmt_name NUL
    out
}

/// Emit a `&[u8]` byte-array literal token stream. LLVM hoists the
/// resulting `const` slice into the consumer crate's `.rodata`.
fn byte_array_literal(bytes: &[u8]) -> TokenStream2 {
    let lits = bytes.iter().map(|b| Literal::u8_unsuffixed(*b));
    quote! { &[ #( #lits ),* ] }
}

// ════════════════════════════════════════════════════════════════════
// query! — SQLite typed-runtime bridge (feature `sqlite-runtime`)
// ════════════════════════════════════════════════════════════════════
//
// The PostgreSQL `TypedQuery` above decodes a `DataRow` byte payload at const
// offsets validated against wire OIDs — a model SQLite does not share (rusqlite
// hands back native storage-class values). So the carrier ALSO implements
// `SqliteTypedQuery` over the SAME record twins: `const SQL` (the portable,
// `$N`-positional form SQLite binds) plus per-field decoders that read each
// column through the driver's `FromColumn`, VERIFYING the actual storage class
// against the declared field type at runtime (SQLite is dynamically typed). This
// half is emitted ONLY under the `sqlite-runtime` feature (the umbrella's
// `sqlite` driver) AND only for a SQLite-decodable query; otherwise nothing is
// emitted here and the PostgreSQL expansion is byte-identical.

/// The Rust field target for a SQLite typed decode of column type `ty`, or
/// `None` if the type is NOT a SQLite storage class (a PostgreSQL-only
/// `uuid`/`timestamptz`/`numeric`/array/… — decoded only on the PostgreSQL
/// path). Owned targets copy text/blob (`String` / `Vec<u8>`); borrowed targets
/// alias the source (`&'q str` / `&'q [u8]`), matching the record twin the
/// `emit_records` path built. Exhaustive over [`bsql_build::RustType`], so a new
/// column type forces a storable/not decision here.
#[cfg(feature = "sqlite-runtime")]
fn sqlite_target(ty: bsql_build::RustType, is_owned: bool) -> Option<TokenStream2> {
    use bsql_build::RustType;
    Some(match ty {
        // SQLite stores every integer as `i64`; a narrower Rust target range-checks
        // (a classified `IntegerOutOfRange`, never a wrapped read).
        RustType::I16 => quote!(i16),
        RustType::I32 => quote!(i32),
        RustType::I64 => quote!(i64),
        RustType::U32 => quote!(u32),
        RustType::Bool => quote!(bool),
        // SQLite REAL is `f64`.
        RustType::F64 => quote!(f64),
        RustType::Text => {
            if is_owned {
                quote!(::std::string::String)
            } else {
                quote!(&'q str)
            }
        }
        RustType::Bytea => {
            if is_owned {
                quote!(::std::vec::Vec<u8>)
            } else {
                quote!(&'q [u8])
            }
        }
        // NOT a SQLite storage class: `f32` (SQLite has only `f64` REAL) and the
        // PostgreSQL-only value types / arrays. A query projecting one is decoded
        // on the PostgreSQL path only; the SQLite bridge is skipped for the whole
        // query, so `sqlite_conn.query::<That>()` is a located compile error.
        RustType::F32
        | RustType::Uuid
        | RustType::Timestamptz
        | RustType::Timestamp
        | RustType::Date
        | RustType::Time
        | RustType::Interval
        | RustType::Json
        | RustType::Jsonb
        | RustType::Numeric
        // A user-defined PostgreSQL enum is not a SQLite concept (`CREATE TYPE
        // ... AS ENUM` is PG-only; the SQLite conformance template cannot even
        // replay it), so a query projecting one is PostgreSQL-only — the SQLite
        // bridge is skipped and `sqlite_conn.query::<That>()` is a located
        // compile error. A user-defined COMPOSITE is likewise PG-only (its
        // row-type binary frame has no SQLite storage class).
        | RustType::UserEnum(_)
        | RustType::UserComposite(_)
        | RustType::Array(_) => return None,
    })
}

/// One SQLite record-field decode expression: read column `idx` through the
/// driver's `FromColumn`, verifying the storage class. A nullable column routes
/// through `read_optional` (`NULL` → `None`); a NOT-NULL column through
/// `read_required` (`NULL` → the classified `UnexpectedNull`). `None` if the
/// column type is not a SQLite storage class.
#[cfg(feature = "sqlite-runtime")]
fn sqlite_field_decode(
    ty: bsql_build::RustType,
    nullable: bool,
    is_owned: bool,
    idx: usize,
) -> Option<TokenStream2> {
    let target = sqlite_target(ty, is_owned)?;
    let idx_lit = Literal::usize_unsuffixed(idx);
    let helper = if nullable {
        quote!(read_optional)
    } else {
        quote!(read_required)
    };
    Some(quote! {
        ::bsql::__rt_sqlite::#helper::<#target, __S>(__src, #idx_lit)?
    })
}

/// Emit the SQLite typed-runtime bridge `impl SqliteTypedQuery for {Name}Query`,
/// or NOTHING (`quote!()`) when the query is not SQLite-decodable — a
/// PostgreSQL-only dynamic form (`OPTIONAL(...)`, `= ANY(...)`, a runtime
/// `ORDER BY` allow-set), a bridged column, or a column type SQLite cannot
/// store. Emitting nothing (rather than a fallible runtime impl) keeps the
/// not-decodable case a LOCATED compile error at the `sqlite_conn.query::<Q>()`
/// call site, never a silent mis-decode.
#[cfg(feature = "sqlite-runtime")]
fn emit_sqlite_typed(
    name: &Ident,
    shape: &bsql_build::DynamicShape,
    bridges: &Bridges,
    enums: &EnumTypes,
) -> syn::Result<TokenStream2> {
    // A runtime `ORDER BY` allow-set or an `OPTIONAL(...)` / `= ANY(...)` param
    // is PostgreSQL-runtime sugar with no SQLite lowering — skip the bridge.
    if shape.order_by.is_some()
        || shape
            .params
            .iter()
            .any(|p| !matches!(p, bsql_build::ParamShape::Scalar(_)))
    {
        return Ok(TokenStream2::new());
    }

    // The typed `$N` parameter tuple — the SAME markers the PostgreSQL
    // `TypedQuery::Params` uses (built through the one `param_tuple_marker` /
    // `tuple_type` authority), so a `query!` binds the SAME typed parameters on
    // both backends. `SqliteTypedQuery::Params` is UNBOUNDED, so a scalar type
    // SQLite cannot bind (a `u64`, a `Uuid`, an `EnumLabel`) still emits a valid
    // impl; the `SqliteBindParams` requirement on the driver's `query::<Q>` verb
    // makes running such a carrier a LOCATED call-site error, never a mis-decode.
    // At the verb-argument GAT lifetime `'p` (`text`/`bytea` params borrow
    // `&'p …`), matching `TypedQuery::Params<'p>` — so a RUNTIME `&str` binds the
    // SAME typed parameter on both backends.
    let param_markers_p: Vec<TokenStream2> = shape
        .params
        .iter()
        .map(|p| param_tuple_marker(*p, enums, &quote!('p)))
        .collect::<syn::Result<Vec<_>>>()?;
    let params_tuple_p = tuple_type(&param_markers_p);

    // Per-column decode expressions (borrowed + owned twins). Bail to "no
    // bridge" the moment a column is bridged or not SQLite-storable.
    let mut borrowed_inits = Vec::with_capacity(shape.columns.len());
    let mut owned_inits = Vec::with_capacity(shape.columns.len());
    for (idx, col) in shape.columns.iter().enumerate() {
        if bridges.for_column(col.ty).is_some() {
            return Ok(TokenStream2::new());
        }
        let (Some(borrowed), Some(owned)) = (
            sqlite_field_decode(col.ty, col.nullable, false, idx),
            sqlite_field_decode(col.ty, col.nullable, true, idx),
        ) else {
            return Ok(TokenStream2::new());
        };
        let id = make_field_ident(&col.name, name.span())?;
        borrowed_inits.push(quote! { #id: #borrowed });
        owned_inits.push(quote! { #id: #owned });
    }

    // The SQLite-preparable SQL: the portable `infer_sql` form the build-time
    // conformance oracle prepares against real SQLite, with `$N` rewritten to
    // SQLite's `?N` numbered form by the SAME `sqlite_placeholder_form`
    // authority the oracle uses — so the baked runtime string is byte-identical
    // to the one build-time validation proved SQLite prepares (no drift, not a
    // "happens to accept `$N`" assumption).
    let infer_sql = &shape
        .variants
        .first()
        .ok_or_else(|| syn::Error::new(name.span(), "query!: internal error — no wire variant"))?
        .infer_sql;
    let sqlite_sql = bsql_build::sqlite_placeholder_form(infer_sql);

    let carrier = format_ident!("{}Query", name);
    let owned_name = format_ident!("{}Owned", name);
    // Mirror `emit_records`: the borrowed record carries `<'q>` iff a column
    // borrows the input (`text` / `bytea`, unbridged), so the GAT projection
    // agrees with the emitted struct.
    let has_borrowed = shape.columns.iter().any(|c| column_borrows(c.ty, bridges));
    let record_ty = if has_borrowed {
        quote!(#name<'q>)
    } else {
        quote!(#name)
    };

    Ok(quote! {
        impl ::bsql::__rt_sqlite::SqliteTypedQuery for #carrier {
            type Params<'p> = #params_tuple_p;
            type Record<'q> = #record_ty;
            type Owned = #owned_name;
            const SQL: &'static str = #sqlite_sql;

            fn decode_row<'q, __S: ::bsql::__rt_sqlite::ColumnSource<'q>>(
                __src: &__S,
            ) -> ::core::result::Result<Self::Record<'q>, ::bsql::__rt_sqlite::SqliteError> {
                ::core::result::Result::Ok(#name { #(#borrowed_inits),* })
            }

            fn decode_row_owned<'__a, __S: ::bsql::__rt_sqlite::ColumnSource<'__a>>(
                __src: &__S,
            ) -> ::core::result::Result<Self::Owned, ::bsql::__rt_sqlite::SqliteError> {
                ::core::result::Result::Ok(#owned_name { #(#owned_inits),* })
            }
        }
    })
}

// ════════════════════════════════════════════════════════════════════
// #[bsql::test] — schema-per-test isolation attribute
// ════════════════════════════════════════════════════════════════════
//
// A different flavor from `query!` above: `query!` is a build-time SQL typer;
// `#[bsql::test]` is a pure syntactic transform that wraps one `async fn` in a
// `#[test]` running it against an isolated PostgreSQL schema. It reads NOTHING
// at expansion (no catalog, no environment) and emits code naming only the
// hidden `::bsql::__test_rt::` runtime, so — like `query!` — a consumer reaches
// it through the single `bsql` crate. Both macros are host-only token
// transformers sharing the syn/quote toolchain, which is why they share a crate.

/// Run an integration test in its own isolated PostgreSQL schema — over the
/// async OR the sync driver.
///
/// Applied to an `async fn` taking a single `conn: &mut bsql::pg::Connection`,
/// it runs the body over the async driver; applied to a plain `fn` taking a
/// single `conn: &mut bsql::pg_sync::Connection`, it runs the body over the
/// blocking driver (no runtime). Either way it emits a `#[test]` that connects
/// to the server named by the `BSQL_TEST_DSN` environment variable, creates a
/// unique schema, runs the body against a connection pinned to that schema (via
/// its connect-time `search_path`), and drops the schema on exit — even if the
/// test panics. Two `#[bsql::test]` tests run in parallel without interfering,
/// because each sees only its own schema.
///
/// ```rust,ignore
/// #[bsql::test]
/// async fn creates_a_user(conn: &mut bsql::pg::Connection) {
///     conn.execute_sql("CREATE TABLE users (id int)").await.unwrap();
/// }   // schema auto-dropped, even on panic
///
/// #[bsql::test]
/// fn creates_a_user_sync(conn: &mut bsql::pg_sync::Connection) {
///     conn.execute_sql("CREATE TABLE users (id int)").unwrap();
/// }   // same isolation + teardown, over the blocking driver
/// ```
///
/// The attribute takes no arguments. Other attributes on the function
/// (`#[ignore]`, `#[should_panic]`, …) are forwarded to the generated `#[test]`.
/// The function must be non-generic, return `()`, and take exactly one
/// `conn: &mut Connection` argument — anything else is a `compile_error!`. The
/// `async`-ness selects the driver; the connection argument type must match it
/// (an `async fn` with a sync connection, or a plain `fn` with an async
/// connection, is a type-mismatch compile error against the harness — never a
/// silent mis-expansion).
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    match expand_test(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Whether a type is the unit type `()`.
fn is_unit_type(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Tuple(t) if t.elems.is_empty())
}

/// Build the `#[bsql::test]` wrapper, or a classified `syn::Error` that becomes
/// a `compile_error!` at the offending span.
fn expand_test(attr: TokenStream2, item: TokenStream2) -> Result<TokenStream2, syn::Error> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            &attr,
            "bsql::test: this attribute takes no arguments",
        ));
    }

    let func: syn::ItemFn = syn::parse2(item)?;

    // The `async`-ness selects the driver (async fn → async harness, plain fn →
    // sync harness). Both are otherwise validated identically below; only the
    // emitted runtime entry point and the closure flavor differ. A connection
    // argument type that does not match the chosen driver is caught by the
    // harness's own bound (a type-mismatch compile error), not here — so there
    // is no mis-expansion.
    let is_async = func.sig.asyncness.is_some();

    if !func.sig.generics.params.is_empty() || func.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            "bsql::test: the test function must not be generic",
        ));
    }
    match &func.sig.output {
        syn::ReturnType::Default => {}
        syn::ReturnType::Type(_, ty) if is_unit_type(ty.as_ref()) => {}
        syn::ReturnType::Type(arrow, _) => {
            return Err(syn::Error::new_spanned(
                arrow,
                "bsql::test: the test function must return `()`",
            ));
        }
    }

    // Exactly one argument, `conn: &mut Connection`. The pattern and type are
    // echoed verbatim into the generated async closure, so the caller's own
    // `use` for the connection type keeps working.
    let mut inputs = func.sig.inputs.iter();
    let (pat, ty) = match (inputs.next(), inputs.next()) {
        (Some(syn::FnArg::Typed(arg)), None) => (&arg.pat, &arg.ty),
        (Some(syn::FnArg::Receiver(recv)), _) => {
            return Err(syn::Error::new_spanned(
                recv,
                "bsql::test: the test function must take `conn: &mut Connection`, not `self`",
            ));
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "bsql::test: the test function must take exactly one argument, \
                 `conn: &mut Connection`",
            ));
        }
    };

    let attrs = &func.attrs;
    let vis = &func.vis;
    let name = &func.sig.ident;
    let name_str = name.to_string();
    let body = &func.block;

    // The only async-vs-sync divergence: the runtime entry point and whether the
    // body closure is an async closure (returning a future the harness `.await`s)
    // or a plain closure the harness calls directly. The `#pat: #ty` annotation
    // rides both, so a connection type that does not match the chosen harness's
    // bound is a clear compile error at the closure.
    let call = if is_async {
        quote! {
            ::bsql::__test_rt::run_schema_isolated_test(
                #name_str,
                async move |#pat: #ty| #body,
            );
        }
    } else {
        quote! {
            ::bsql::__test_rt::run_schema_isolated_test_sync(
                #name_str,
                move |#pat: #ty| #body,
            );
        }
    };

    Ok(quote! {
        #(#attrs)*
        #[::core::prelude::v1::test]
        #vis fn #name() {
            #call
        }
    })
}
