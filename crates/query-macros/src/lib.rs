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
    let shape = bsql_build::infer_dynamic_query(&catalog, &sql_text)
        .map_err(|err| syn::Error::new(sql_span, format!("query!: {err}")))?;

    // The typed-record twins (borrowed + owned) and their decoders.
    let records = emit_records(&name, &shape.columns, &bridges)?;
    // The const wire artifact(s): the uninhabited fingerprint carrier(s),
    // their `QueryFingerprint` impl(s) (baked Parse / Bind-prefix templates
    // + OID lists, all derived from the lowered shape), the validated
    // `PreparedQuery` const(s) minted through the proto-owned `run`
    // boundary, the dynamic-form budget assertions, and — for a runtime
    // `ORDER BY` allow-set — the closed selector enum.
    let wire = emit_dynamic_wire(&name, &shape, &bridges)?;

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

    Ok(quote! {
        #records
        #wire
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

/// Read the catalog text via the rustc-env channel and rebuild the
/// `bsql_build::Catalog`. Fail-closed: an absent env var, an unreadable
/// file, or a corrupt catalog is a `compile_error!` — never a silent pass
/// against a missing schema (which would be the stale-schema blind spot
/// this design exists to remove).
fn load_build_catalog(span: Span) -> syn::Result<bsql_build::Catalog> {
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
    bsql_build::parse_catalog(&text).map_err(|err| {
        syn::Error::new(
            span,
            format!("query!: the schema catalog at `{path}` is malformed: {err}"),
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
///   `ColCellAt::OID` the runtime const validator pins into `ROW_OIDS`) AND
///   the type the record decode routes through
///   (`<marker as ColCellAt>::decode_at`). Decoder and wire OID are therefore
///   the SAME source: a same-width divergence (e.g. `int4` vs `oid`, both
///   4 bytes) is either an `error[E0080]` at the const validator (the OID
///   disagrees with the marker) or a loud `error[E0308]` at the record's
///   struct literal (the field type disagrees with the decoded value) — never
///   a wire `-1` silently decoded as `4294967295`.
/// * [`ColSpec::oid_path`] / [`ColSpec::oid_value`] are the scalar column OID
///   in its two spellings (the `oids::*` const path baked into the OID slice,
///   and the numeric value baked into the Parse template), kept together so
///   they cannot disagree.
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
    /// The scalar column OID's `oids::*` const path (into the OID slices).
    oid_path: TokenStream2,
    /// The scalar column OID's numeric value (baked into the Parse template).
    oid_value: u32,
    /// The 1-D array (`T[]`) OID's `oids::*_ARRAY` const path.
    array_oid_path: TokenStream2,
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
                    oid_path: TokenStream2,
                    oid_value: u32,
                    array_oid_path: TokenStream2,
                    array_oid_value: u32,
                    fixed_width: Option<(usize, i32)>,
                    impls_eq: bool| ColSpec {
        marker,
        field_owned: field,
        borrow: BorrowKind::ByValue,
        oid_path,
        oid_value,
        array_oid_path,
        array_oid_value,
        fixed_width,
        impls_eq,
    };
    match ty {
        RustType::I16 => by_value(
            quote!(i16), quote!(i16),
            quote!(::bsql::__rt::oids::INT2), 21,
            quote!(::bsql::__rt::oids::INT2_ARRAY), 1005,
            Some((2, 2)), true,
        ),
        RustType::I32 => by_value(
            quote!(i32), quote!(i32),
            quote!(::bsql::__rt::oids::INT4), 23,
            quote!(::bsql::__rt::oids::INT4_ARRAY), 1007,
            Some((4, 4)), true,
        ),
        RustType::I64 => by_value(
            quote!(i64), quote!(i64),
            quote!(::bsql::__rt::oids::INT8), 20,
            quote!(::bsql::__rt::oids::INT8_ARRAY), 1016,
            Some((8, 8)), true,
        ),
        RustType::U32 => by_value(
            quote!(u32), quote!(u32),
            quote!(::bsql::__rt::oids::OID), 26,
            quote!(::bsql::__rt::oids::OID_ARRAY), 1028,
            Some((4, 4)), true,
        ),
        RustType::Bool => by_value(
            quote!(bool), quote!(bool),
            quote!(::bsql::__rt::oids::BOOL), 16,
            quote!(::bsql::__rt::oids::BOOL_ARRAY), 1000,
            Some((1, 1)), true,
        ),
        // The IEEE-754 floats are fixed-width value types, but `NaN != NaN`, so
        // they are `PartialEq` only — a record with a float column cannot derive
        // `Eq`.
        RustType::F32 => by_value(
            quote!(f32), quote!(f32),
            quote!(::bsql::__rt::oids::FLOAT4), 700,
            quote!(::bsql::__rt::oids::FLOAT4_ARRAY), 1021,
            Some((4, 4)), false,
        ),
        RustType::F64 => by_value(
            quote!(f64), quote!(f64),
            quote!(::bsql::__rt::oids::FLOAT8), 701,
            quote!(::bsql::__rt::oids::FLOAT8_ARRAY), 1022,
            Some((8, 8)), false,
        ),
        // `uuid` is a fixed 16-byte value; `timestamptz` / `timestamp` a fixed
        // 8-byte `i64`; `date` a 4-byte `i32`; `time` an 8-byte `i64`;
        // `interval` a fixed 16-byte three-field record — all self-owning value
        // types that join the const-offset fast path.
        RustType::Uuid => by_value(
            quote!(::bsql::__rt::Uuid), quote!(::bsql::Uuid),
            quote!(::bsql::__rt::oids::UUID), 2950,
            quote!(::bsql::__rt::oids::UUID_ARRAY), 2951,
            Some((16, 16)), true,
        ),
        RustType::Timestamptz => by_value(
            quote!(::bsql::__rt::Timestamptz), quote!(::bsql::Timestamptz),
            quote!(::bsql::__rt::oids::TIMESTAMPTZ), 1184,
            quote!(::bsql::__rt::oids::TIMESTAMPTZ_ARRAY), 1185,
            Some((8, 8)), true,
        ),
        RustType::Timestamp => by_value(
            quote!(::bsql::__rt::Timestamp), quote!(::bsql::Timestamp),
            quote!(::bsql::__rt::oids::TIMESTAMP), 1114,
            quote!(::bsql::__rt::oids::TIMESTAMP_ARRAY), 1115,
            Some((8, 8)), true,
        ),
        RustType::Date => by_value(
            quote!(::bsql::__rt::Date), quote!(::bsql::Date),
            quote!(::bsql::__rt::oids::DATE), 1082,
            quote!(::bsql::__rt::oids::DATE_ARRAY), 1182,
            Some((4, 4)), true,
        ),
        RustType::Time => by_value(
            quote!(::bsql::__rt::Time), quote!(::bsql::Time),
            quote!(::bsql::__rt::oids::TIME), 1083,
            quote!(::bsql::__rt::oids::TIME_ARRAY), 1183,
            Some((8, 8)), true,
        ),
        RustType::Interval => by_value(
            quote!(::bsql::__rt::Interval), quote!(::bsql::Interval),
            quote!(::bsql::__rt::oids::INTERVAL), 1186,
            quote!(::bsql::__rt::oids::INTERVAL_ARRAY), 1187,
            Some((16, 16)), true,
        ),
        // `json` / `jsonb` / `numeric` are variable-width, self-owning value
        // types (String / Box<[u16]>-backed) — decoded on the per-cell path.
        RustType::Json => by_value(
            quote!(::bsql::__rt::Json), quote!(::bsql::Json),
            quote!(::bsql::__rt::oids::JSON), 114,
            quote!(::bsql::__rt::oids::JSON_ARRAY), 199,
            None, true,
        ),
        RustType::Jsonb => by_value(
            quote!(::bsql::__rt::Jsonb), quote!(::bsql::Jsonb),
            quote!(::bsql::__rt::oids::JSONB), 3802,
            quote!(::bsql::__rt::oids::JSONB_ARRAY), 3807,
            None, true,
        ),
        RustType::Numeric => by_value(
            quote!(::bsql::__rt::Numeric), quote!(::bsql::Numeric),
            quote!(::bsql::__rt::oids::NUMERIC), 1700,
            quote!(::bsql::__rt::oids::NUMERIC_ARRAY), 1231,
            None, true,
        ),
        // `text` borrows the input as `&'q str` (owned twin: `String`);
        // variable-width, decoded on the per-cell path.
        RustType::Text => ColSpec {
            marker: quote!(&'static str),
            field_owned: quote!(::std::string::String),
            borrow: BorrowKind::Str,
            oid_path: quote!(::bsql::__rt::oids::TEXT),
            oid_value: 25,
            array_oid_path: quote!(::bsql::__rt::oids::TEXT_ARRAY),
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
            oid_path: quote!(::bsql::__rt::oids::BYTEA),
            oid_value: 17,
            array_oid_path: quote!(::bsql::__rt::oids::BYTEA_ARRAY),
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
                oid_path: e.array_oid_path,
                oid_value: e.array_oid_value,
                // An array-of-array has no single element-array OID. Structurally
                // dead: inference rejects a multi-dimensional array, so this
                // never bakes into a slice. Fail-closed to a non-OID `0` that
                // would trip the const validator rather than a plausible-wrong
                // OID.
                array_oid_path: quote!(0u32),
                array_oid_value: 0,
                fixed_width: None,
                impls_eq: e.impls_eq,
            }
        }
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
) -> TokenStream2 {
    let base = match bridges.for_column(ty) {
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
    };
    if nullable {
        quote!(::core::option::Option<#base>)
    } else {
        base
    }
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

/// Emit the borrowed + owned record twins and their `decode` fns.
fn emit_records(
    name: &Ident,
    columns: &[bsql_build::InferredColumn],
    bridges: &Bridges,
) -> syn::Result<TokenStream2> {
    let owned_name = format_ident!("{}Owned", name);

    // One field identifier per output column. A duplicate output column
    // name is already a loud `InferError::DuplicateOutputColumn` from the
    // inference engine; the Rust "two fields, one name" rule (E0124) is
    // the structural backstop.
    let mut field_idents = Vec::with_capacity(columns.len());
    // Per-column 0-based index, as a `u8` for the `TruncatedColumnLen`
    // diagnostic on a short row. Bounded loudly so the cast is never lossy.
    let mut col_idx_u8 = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        field_idents.push(make_field_ident(&col.name, name.span())?);
        match u8::try_from(idx) {
            Ok(value) => col_idx_u8.push(value),
            Err(_) => {
                return Err(syn::Error::new(
                    name.span(),
                    "query!: more than 256 output columns are not supported",
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

    let borrowed_fields = field_idents.iter().zip(columns).map(|(id, col)| {
        let ty = field_type_bridged(col.ty, col.nullable, false, bridges);
        quote! { #id: #ty }
    });
    let owned_fields = field_idents.iter().zip(columns).map(|(id, col)| {
        let ty = field_type_bridged(col.ty, col.nullable, true, bridges);
        quote! { #id: #ty }
    });

    // The borrowed record carries `<'q>` ONLY when it has a borrowing
    // (text / bytea) field — otherwise the lifetime would be unused, which
    // the workspace `unused_lifetimes` floor forbids.
    let borrowed_generics = if has_borrowed { quote!(<'q>) } else { quote!() };
    let borrowed_input = if has_borrowed {
        quote!(body: &'q [u8])
    } else {
        quote!(body: &[u8])
    };

    let borrowed_body = decode_body(
        &field_idents,
        columns,
        &col_idx_u8,
        n_i16,
        all_fixed_not_null,
        false,
        bridges,
    );
    let owned_body = decode_body(
        &field_idents,
        columns,
        &col_idx_u8,
        n_i16,
        all_fixed_not_null,
        true,
        bridges,
    );

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

    Ok(quote! {
        #[derive(#derives)]
        #[allow(dead_code, reason = #allow_reason)]
        pub struct #name #borrowed_generics {
            #(#borrowed_fields),*
        }

        #[derive(#derives)]
        #[allow(dead_code, reason = #allow_reason)]
        pub struct #owned_name {
            #(#owned_fields),*
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

        impl #owned_name {
            /// Decode one raw `DataRow` payload (the wire bytes after the
            /// field-count header) into the owned record.
            pub fn decode(body: &[u8])
                -> ::core::result::Result<Self, ::bsql::__rt::DecodeError>
            {
                #owned_body
            }
        }
    })
}

/// The body of one `decode` fn: the optional vectorized fast path (only
/// for an all-fixed-width, all-NOT-NULL row) followed by the general
/// per-cell path (which classifies NULL / variable-width / oversized
/// rows). When the fast path is not eligible, only the per-cell path is
/// emitted.
fn decode_body(
    field_idents: &[Ident],
    columns: &[bsql_build::InferredColumn],
    col_idx_u8: &[u8],
    n_i16: i16,
    all_fixed_not_null: bool,
    is_owned: bool,
    bridges: &Bridges,
) -> TokenStream2 {
    let per_cell = per_cell_path(field_idents, columns, col_idx_u8, is_owned, bridges);
    if all_fixed_not_null {
        let fast = fast_path(field_idents, columns, n_i16, bridges);
        quote! {
            #fast
            #per_cell
        }
    } else {
        per_cell
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
    col_idx_u8: &[u8],
    is_owned: bool,
    bridges: &Bridges,
) -> TokenStream2 {
    if columns.is_empty() {
        // No projected columns: validate the row body and build the empty
        // record. `parse` still fails closed on a malformed count header.
        return quote! {
            ::bsql::__rt::DataRowRef::parse(body)?;
            ::core::result::Result::Ok(Self {})
        };
    }

    let stmts = field_idents
        .iter()
        .zip(columns)
        .zip(col_idx_u8)
        .map(|((id, col), idx)| {
            let idx_lit = Literal::u8_suffixed(*idx);
            let value = per_cell_value_expr(col.ty, col.nullable, is_owned, bridges);
            quote! {
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
            }
        });

    quote! {
        let __row = ::bsql::__rt::DataRowRef::parse(body)?;
        let mut __cols = ::bsql::__rt::DataRowRef::columns(&__row);
        #(#stmts)*
        ::core::result::Result::Ok(Self { #(#field_idents),* })
    }
}

/// One per-cell column value expression. A NOT-NULL column returns the
/// decoded value or a classified `NullInNonNullColumn` on NULL; a
/// nullable column returns `Some(value)` / `None`.
fn per_cell_value_expr(
    ty: bsql_build::RustType,
    nullable: bool,
    is_owned: bool,
    bridges: &Bridges,
) -> TokenStream2 {
    let decode_expr = decode_value_expr_bridged(ty, is_owned, bridges);
    if nullable {
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
    }
}

/// The decode call for one non-NULL cell body (`__bytes`).
///
/// Decodes through `<marker as ColCellAt>::decode_at` on the SAME marker the
/// const validator pins the wire OID to (`<marker as ColCellAt>::OID` becomes
/// `ROW_OIDS`), so the decoded value is definitionally the OID-validated type
/// — decoder and wire OID are one source and cannot drift. Owned `text` copies
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
) -> TokenStream2 {
    match bridges.for_column(ty) {
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
    }
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

/// The PostgreSQL type OID for a supported Rust type. This is the
/// The scalar column OID numeric value — projected from the single
/// [`col_spec`] source. Needed to bake the OID bytes into the `Parse`
/// template; the matching `oids::*` const path ([`oid_path`]) is what the
/// emitted OID slices reference, and the runtime crate's validating
/// constructor cross-checks the two so they cannot drift.
fn rust_type_oid(ty: bsql_build::RustType) -> u32 {
    col_spec(ty).oid_value
}

/// The `oids::*` const path token for a Rust type — emitted into the
/// `PARAM_OIDS` / `ROW_OIDS` slices so they resolve through the runtime
/// crate's pinned OID constants. Projected from the single [`col_spec`]
/// source, so the row-tuple marker (whose `ColCellAt::OID` the const
/// validator pins to this slice) and this path come from one row.
fn oid_path(ty: bsql_build::RustType) -> TokenStream2 {
    col_spec(ty).oid_path
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
/// the single [`col_spec`] source (its `array_oid_value`); cross-checked
/// against the runtime crate's `oids::*_ARRAY` constants by the const
/// validator. An array element (structurally dead: inference rejects a nested
/// array) yields the fail-closed non-OID `0`.
fn array_oid(ty: bsql_build::RustType) -> u32 {
    col_spec(ty).array_oid_value
}

/// The `oids::*_ARRAY` const path token for an element type's array OID —
/// projected from the single [`col_spec`] source (its `array_oid_path`).
fn array_oid_path(ty: bsql_build::RustType) -> TokenStream2 {
    col_spec(ty).array_oid_path
}

/// The `'static`-lifetime type-level marker for one parameter tuple
/// element. A toggled optional filter is `Option<T>` (passing `None`
/// disables it); a `= ANY($N)` in-list is the array slice `&'static [T]`;
/// a plain scalar is `T`.
fn param_tuple_marker(shape: bsql_build::ParamShape) -> TokenStream2 {
    use bsql_build::ParamShape;
    match shape {
        ParamShape::Scalar(ty) => tuple_marker(ty),
        ParamShape::Optional(ty) => {
            let inner = tuple_marker(ty);
            quote!(::core::option::Option<#inner>)
        }
        ParamShape::Array(ty) => {
            let elem = tuple_marker(ty);
            quote!(&'static [#elem])
        }
    }
}

/// The numeric OID baked into the Parse template for one parameter. A
/// toggled `Option<T>` keeps the scalar OID (a SQL NULL is typed by its
/// column); a `= ANY($N)` array uses the element type's array OID.
fn param_oid_value(shape: bsql_build::ParamShape) -> u32 {
    use bsql_build::ParamShape;
    match shape {
        ParamShape::Scalar(ty) | ParamShape::Optional(ty) => rust_type_oid(ty),
        ParamShape::Array(ty) => array_oid(ty),
    }
}

/// The `oids::*` const path token emitted into `PARAM_OIDS` for one
/// parameter — the runtime const validator cross-checks it against the
/// `ParamsWriter` tuple's declared OIDs.
fn param_oid_path(shape: bsql_build::ParamShape) -> TokenStream2 {
    use bsql_build::ParamShape;
    match shape {
        ParamShape::Scalar(ty) | ParamShape::Optional(ty) => oid_path(ty),
        ParamShape::Array(ty) => array_oid_path(ty),
    }
}

/// The type-level `Params` / `Row` tuple markers, shared across every wire
/// variant of one query (only the runtime `ORDER BY` SQL differs between
/// variants). Bundled so the per-carrier emit takes one argument for both.
struct TypeTuples {
    /// The `Params` tuple type (from the lowered `$N` shapes).
    params: TokenStream2,
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
) -> syn::Result<TokenStream2> {
    // The runtime `ParamsWriter` / `RowDecode` impls (and the whole
    // prepared-statement wire path) cover tuple arity 0..=16. A query
    // outside that envelope is a loud rejection, not a silent truncation
    // or an opaque trait-bound error.
    if shape.params.len() > 16 {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "query!: {} parameters — the prepared-query wire path \
                 supports at most 16 `$N` parameters",
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
    let param_markers: Vec<TokenStream2> =
        shape.params.iter().map(|p| param_tuple_marker(*p)).collect();
    let row_markers: Vec<TokenStream2> =
        shape.columns.iter().map(|c| tuple_marker(c.ty)).collect();
    let tuples = TypeTuples {
        params: tuple_type(&param_markers),
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
    let row_tuple = &tuples.row;
    // Content-addressed statement name: SHA-256 of the (lowered) SQL text,
    // truncated to 96 bits, hex-encoded, prefixed. Two distinct queries
    // cannot share a name without colliding their content addresses.
    let stmt_name = sha256_96_stmt_name(wire_sql);

    let param_oid_paths = params.iter().map(|p| param_oid_path(*p));
    let row_oid_paths = columns.iter().map(|c| oid_path(c.ty));

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
            const PARAM_OIDS: &'static [u32] = &[ #( #param_oid_paths ),* ];
            const ROW_OIDS: &'static [u32] = &[ #( #row_oid_paths ),* ];
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
            type Params = #params_tuple;
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
