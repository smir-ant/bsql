//! Proc-macro foundation for bsql's compile-checked query API.
//!
//! This crate hosts [`schema_check`] — the seed of the future `query!`
//! macro. It reads the schema catalog that `bsql-build` generates at
//! build time and validates that a referenced `table.column` pair exists
//! in the real schema replayed from the consumer's migration DDL. An
//! unknown table or column is a `compile_error!`.
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

/// Validate that a `table.column` reference exists in the schema replayed
/// from the consumer's migrations.
///
/// ```ignore
/// // Compiles iff `users.id` exists in the migration-replayed schema:
/// bsql_query_macros::schema_check!(users.id);
/// ```
///
/// The macro expands to a unit expression `()` on success. On failure it
/// expands to a `compile_error!` naming the unknown table or column, or
/// the missing/unreadable catalog (fail-closed — never a silent pass).
///
/// This is the foundation seed: it proves the migrations -> catalog ->
/// proc-macro chain end-to-end. Full SQL parsing, type inference, and
/// code generation are layered on top in later work.
#[proc_macro]
pub fn schema_check(input: TokenStream) -> TokenStream {
    match schema_check_impl(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// A `table.column` reference: two identifiers separated by a dot.
struct ColumnRef {
    table: Ident,
    column: Ident,
}

impl Parse for ColumnRef {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let table: Ident = input.parse()?;
        let _dot: Token![.] = input.parse()?;
        let column: Ident = input.parse()?;
        if !input.is_empty() {
            return Err(input.error(
                "schema_check!: expected a single `table.column` reference",
            ));
        }
        Ok(ColumnRef { table, column })
    }
}

fn schema_check_impl(
    input: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let ColumnRef { table, column } = syn::parse2(input)?;
    let table_name = table.to_string();
    let column_name = column.to_string();

    let catalog = load_catalog(table.span())?;

    if !catalog.has_table(&table_name) {
        return Err(syn::Error::new(
            table.span(),
            format!(
                "schema_check!: unknown table `{table_name}` — it is not \
                 defined by any migration. Known tables: [{}]",
                catalog.table_list()
            ),
        ));
    }
    if !catalog.has_column(&table_name, &column_name) {
        return Err(syn::Error::new(
            column.span(),
            format!(
                "schema_check!: table `{table_name}` has no column \
                 `{column_name}`. Known columns: [{}]",
                catalog.column_list(&table_name)
            ),
        ));
    }

    Ok(quote! { () })
}

/// Read and parse the catalog via the rustc-env channel. Fail-closed:
/// an absent env var or an unreadable file is a `compile_error!`.
fn load_catalog(span: Span) -> syn::Result<Catalog> {
    let path = std::env::var(CATALOG_ENV_VAR).map_err(|_| {
        syn::Error::new(
            span,
            format!(
                "schema_check!: the schema catalog environment variable \
                 `{CATALOG_ENV_VAR}` is not set. The consumer crate must \
                 have a `build.rs` calling `bsql_build::emit_catalog(..)` \
                 (and `bsql-build` in its `[build-dependencies]`). \
                 Refusing to validate against a missing schema — a silent \
                 pass here would be exactly the stale-schema blind spot \
                 this check exists to prevent."
            ),
        )
    })?;

    let text = std::fs::read_to_string(&path).map_err(|err| {
        syn::Error::new(
            span,
            format!(
                "schema_check!: cannot read the schema catalog at `{path}`: \
                 {err}. The catalog is generated by `bsql-build` into \
                 OUT_DIR at build time; an unreadable catalog fails closed \
                 rather than passing a query against an unknown schema."
            ),
        )
    })?;

    Ok(Catalog::parse(&text))
}

/// In-memory view of the line-oriented catalog text. Each line is
/// `table\tcolumn\tpg_type\t<0|1>`. We only need table/column existence
/// for the foundation check; `pg_type` / `not_null` are carried for the
/// later typing slices but ignored here.
struct Catalog {
    entries: Vec<CatalogRow>,
}

struct CatalogRow {
    table: String,
    column: String,
}

impl Catalog {
    fn parse(text: &str) -> Self {
        let mut entries = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split('\t');
            // A well-formed line always has these two leading fields; a
            // malformed line missing them carries no usable table/column
            // and is skipped (the catalog is machine-generated by
            // bsql-build, so this is a defensive guard, not a parse of
            // untrusted input).
            let table = match fields.next() {
                Some(value) if !value.is_empty() => value.to_string(),
                _ => continue,
            };
            let column = match fields.next() {
                Some(value) if !value.is_empty() => value.to_string(),
                _ => continue,
            };
            entries.push(CatalogRow { table, column });
        }
        Catalog { entries }
    }

    fn has_table(&self, table: &str) -> bool {
        self.entries.iter().any(|row| row.table == table)
    }

    fn has_column(&self, table: &str, column: &str) -> bool {
        self.entries
            .iter()
            .any(|row| row.table == table && row.column == column)
    }

    /// Comma-separated sorted list of distinct known tables (for the
    /// "unknown table" diagnostic).
    fn table_list(&self) -> String {
        let mut tables: Vec<&str> = self.entries.iter().map(|row| row.table.as_str()).collect();
        tables.sort_unstable();
        tables.dedup();
        tables.join(", ")
    }

    /// Comma-separated sorted list of a table's known columns (for the
    /// "unknown column" diagnostic).
    fn column_list(&self, table: &str) -> String {
        let mut columns: Vec<&str> = self
            .entries
            .iter()
            .filter(|row| row.table == table)
            .map(|row| row.column.as_str())
            .collect();
        columns.sort_unstable();
        columns.dedup();
        columns.join(", ")
    }
}

// ════════════════════════════════════════════════════════════════════
// query! — typed-record twins + DataRow-payload decode
// ════════════════════════════════════════════════════════════════════
//
// `query!(Name, "<SQL>")` types one SQL query against the schema replayed
// from the consumer's migrations and emits two typed records plus their
// decoders. Expansion calls `bsql_build::infer_query` (the build-time SQL
// inference engine) to obtain the output row shape and the `$N` parameter
// types. The EMITTED code references ONLY the shipped runtime decode
// primitives in `bsql-postgres-proto` (`DataRowRef`, `ColumnsIter`,
// `Cell<BinaryFmt>`, `DecodeError`) — never this crate or `bsql-build`,
// so a consumer's runtime closure carries no build-time query toolchain.

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
/// the record, reusing the shipped binary decoders in
/// `bsql-postgres-proto`. A `NULL` arriving in a `NOT NULL`-typed column
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
    let sql_text = sql.value();
    let sql_span = sql.span();

    // Rebuild the catalog the consumer's build.rs wrote (fail-closed on a
    // missing/unreadable/corrupt catalog).
    let catalog = load_build_catalog(name.span())?;

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
    let records = emit_records(&name, &shape.columns)?;
    // The const wire artifact(s): the uninhabited fingerprint carrier(s),
    // their `QueryFingerprint` impl(s) (baked Parse / Bind-prefix templates
    // + OID lists, all derived from the lowered shape), the validated
    // `PreparedQuery` const(s) minted through the proto-owned `run`
    // boundary, the dynamic-form budget assertions, and — for a runtime
    // `ORDER BY` allow-set — the closed selector enum.
    let wire = emit_dynamic_wire(&name, &shape)?;

    Ok(quote! {
        #records
        #wire
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

/// The fixed binary width (in bytes) of a column type, as `(usize, i32)`
/// for the const-generic chunk length and the length-prefix comparison.
/// `text` is variable-width and has no fixed size.
fn fixed_width(ty: bsql_build::RustType) -> Option<(usize, i32)> {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => Some((2, 2)),
        RustType::I32 => Some((4, 4)),
        RustType::I64 => Some((8, 8)),
        RustType::U32 => Some((4, 4)),
        RustType::Bool => Some((1, 1)),
        RustType::Text => None,
    }
}

/// The Rust type used as the `Cell<BinaryFmt>` `Self` in a decode call
/// (`text` decodes through `&str`, borrowing the input bytes).
fn cell_marker(ty: bsql_build::RustType) -> TokenStream2 {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => quote!(i16),
        RustType::I32 => quote!(i32),
        RustType::I64 => quote!(i64),
        RustType::U32 => quote!(u32),
        RustType::Bool => quote!(bool),
        RustType::Text => quote!(&str),
    }
}

/// A record field's Rust type. Borrowed text is `&'q str`; owned text is
/// `String`; a nullable column is wrapped in `Option<..>`.
fn field_type(ty: bsql_build::RustType, nullable: bool, is_owned: bool) -> TokenStream2 {
    use bsql_build::RustType;
    let base = match ty {
        RustType::I16 => quote!(i16),
        RustType::I32 => quote!(i32),
        RustType::I64 => quote!(i64),
        RustType::U32 => quote!(u32),
        RustType::Bool => quote!(bool),
        RustType::Text => {
            if is_owned {
                quote!(::std::string::String)
            } else {
                quote!(&'q str)
            }
        }
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
) -> syn::Result<TokenStream2> {
    use bsql_build::RustType;

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

    let has_text = columns.iter().any(|c| matches!(c.ty, RustType::Text));
    // The vectorized fast path applies only when every column is a
    // fixed-width binary type AND none is nullable: a NULL or a
    // variable-width column would shift every later column's offset, so
    // const offsets only hold under both conditions.
    let all_fixed_not_null = columns
        .iter()
        .all(|c| !c.nullable && fixed_width(c.ty).is_some());

    let borrowed_fields = field_idents.iter().zip(columns).map(|(id, col)| {
        let ty = field_type(col.ty, col.nullable, false);
        quote! { #id: #ty }
    });
    let owned_fields = field_idents.iter().zip(columns).map(|(id, col)| {
        let ty = field_type(col.ty, col.nullable, true);
        quote! { #id: #ty }
    });

    // The borrowed record carries `<'q>` ONLY when it has a borrowing
    // (text) field — otherwise the lifetime would be unused, which the
    // workspace `unused_lifetimes` floor forbids.
    let borrowed_generics = if has_text { quote!(<'q>) } else { quote!() };
    let borrowed_input = if has_text {
        quote!(body: &'q [u8])
    } else {
        quote!(body: &[u8])
    };

    let borrowed_body =
        decode_body(&field_idents, columns, &col_idx_u8, n_i16, all_fixed_not_null, false);
    let owned_body =
        decode_body(&field_idents, columns, &col_idx_u8, n_i16, all_fixed_not_null, true);

    let allow_reason = "generated typed-record fields are the query's output row shape; a consumer may read any subset of the columns";

    Ok(quote! {
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[allow(dead_code, reason = #allow_reason)]
        pub struct #name #borrowed_generics {
            #(#borrowed_fields),*
        }

        #[derive(Debug, Clone, PartialEq, Eq)]
        #[allow(dead_code, reason = #allow_reason)]
        pub struct #owned_name {
            #(#owned_fields),*
        }

        impl #borrowed_generics #name #borrowed_generics {
            /// Decode one raw `DataRow` payload (the wire bytes after the
            /// field-count header) into the borrowed record. Borrowed
            /// `text` columns alias the input bytes — zero allocation.
            pub fn decode(#borrowed_input)
                -> ::core::result::Result<Self, ::bsql_postgres_proto::DecodeError>
            {
                #borrowed_body
            }
        }

        impl #owned_name {
            /// Decode one raw `DataRow` payload (the wire bytes after the
            /// field-count header) into the owned record.
            pub fn decode(body: &[u8])
                -> ::core::result::Result<Self, ::bsql_postgres_proto::DecodeError>
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
) -> TokenStream2 {
    let per_cell = per_cell_path(field_idents, columns, col_idx_u8, is_owned);
    if all_fixed_not_null {
        let fast = fast_path(field_idents, columns, n_i16);
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
                let marker = cell_marker(col.ty);
                quote! {
                    let ::core::option::Option::Some((__len, __after)) =
                        __after.split_first_chunk::<4>() else { break 'fast };
                    if i32::from_be_bytes(*__len) != #width_i32_lit { break 'fast; }
                    let ::core::option::Option::Some((__data, #trailing)) =
                        __after.split_first_chunk::<#width_lit>() else { break 'fast };
                    let #id = match <#marker as ::bsql_postgres_proto::Cell<
                        ::bsql_postgres_proto::BinaryFmt,
                    >>::decode(__data) {
                        ::core::result::Result::Ok(__value) => __value,
                        ::core::result::Result::Err(__err) =>
                            return ::core::result::Result::Err(__err),
                    };
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
) -> TokenStream2 {
    if columns.is_empty() {
        // No projected columns: validate the row body and build the empty
        // record. `parse` still fails closed on a malformed count header.
        return quote! {
            ::bsql_postgres_proto::DataRowRef::parse(body)?;
            ::core::result::Result::Ok(Self {})
        };
    }

    let stmts = field_idents
        .iter()
        .zip(columns)
        .zip(col_idx_u8)
        .map(|((id, col), idx)| {
            let idx_lit = Literal::u8_suffixed(*idx);
            let value = per_cell_value_expr(col.ty, col.nullable, is_owned);
            quote! {
                let __cell = match ::core::iter::Iterator::next(&mut __cols) {
                    ::core::option::Option::Some(__result) => __result?,
                    // The row declared fewer columns than the query
                    // projects: a classified short-row error, never a
                    // silently-defaulted field.
                    ::core::option::Option::None => return ::core::result::Result::Err(
                        ::bsql_postgres_proto::DecodeError::TruncatedColumnLen {
                            column_idx: #idx_lit,
                        },
                    ),
                };
                let #id = #value;
            }
        });

    quote! {
        let __row = ::bsql_postgres_proto::DataRowRef::parse(body)?;
        let mut __cols = ::bsql_postgres_proto::DataRowRef::columns(&__row);
        #(#stmts)*
        ::core::result::Result::Ok(Self { #(#field_idents),* })
    }
}

/// One per-cell column value expression. A NOT-NULL column returns the
/// decoded value or a classified `NullInNonNullColumn` on NULL; a
/// nullable column returns `Some(value)` / `None`.
fn per_cell_value_expr(ty: bsql_build::RustType, nullable: bool, is_owned: bool) -> TokenStream2 {
    let decode_expr = decode_call_expr(ty, is_owned);
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
                    ::bsql_postgres_proto::DecodeError::NullInNonNullColumn,
                ),
            }
        }
    }
}

/// The decode call for one non-NULL cell body (`__bytes`). Owned `text`
/// copies the borrowed `&str` into a `String`; every other type decodes
/// directly through `Cell<BinaryFmt>`.
fn decode_call_expr(ty: bsql_build::RustType, is_owned: bool) -> TokenStream2 {
    use bsql_build::RustType;
    let marker = cell_marker(ty);
    let raw = quote! {
        <#marker as ::bsql_postgres_proto::Cell<::bsql_postgres_proto::BinaryFmt>>::decode(__bytes)?
    };
    match (ty, is_owned) {
        (RustType::Text, true) => quote! { ::std::string::String::from(#raw) },
        _ => raw,
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
/// `RustType -> OID` map, verified against the runtime crate's
/// `oids::*` constants (`int2`/INT2 = 21, `int4`/INT4 = 23,
/// `int8`/INT8 = 20, `oid`/OID = 26, `bool`/BOOL = 16, `text`/TEXT =
/// 25). The numeric value is needed to bake the OID bytes into the
/// `Parse` template; the matching `oids::*` const path (below) is what
/// the emitted OID slices reference, and the runtime crate's validating
/// constructor cross-checks the two so they cannot drift.
fn rust_type_oid(ty: bsql_build::RustType) -> u32 {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => 21,
        RustType::I32 => 23,
        RustType::I64 => 20,
        RustType::U32 => 26,
        RustType::Bool => 16,
        RustType::Text => 25,
    }
}

/// The `oids::*` const path token for a Rust type — emitted into the
/// `PARAM_OIDS` / `ROW_OIDS` slices so they resolve through the runtime
/// crate's pinned OID constants (rather than restating raw integers).
fn oid_path(ty: bsql_build::RustType) -> TokenStream2 {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => quote!(::bsql_postgres_proto::oids::INT2),
        RustType::I32 => quote!(::bsql_postgres_proto::oids::INT4),
        RustType::I64 => quote!(::bsql_postgres_proto::oids::INT8),
        RustType::U32 => quote!(::bsql_postgres_proto::oids::OID),
        RustType::Bool => quote!(::bsql_postgres_proto::oids::BOOL),
        RustType::Text => quote!(::bsql_postgres_proto::oids::TEXT),
    }
}

/// The `'static`-lifetime tuple-element marker for the `Params` / `Row`
/// type-level tuples. `text` is `&'static str` (the static-placeholder
/// lifetime idiom the runtime decoders project to `&'a str`). Used only
/// for OID / arity pinning, so a nullable column maps to its base
/// marker (NULL handling lives in the typed records, not here).
fn tuple_marker(ty: bsql_build::RustType) -> TokenStream2 {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => quote!(i16),
        RustType::I32 => quote!(i32),
        RustType::I64 => quote!(i64),
        RustType::U32 => quote!(u32),
        RustType::Bool => quote!(bool),
        RustType::Text => quote!(&'static str),
    }
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

/// The array (`T[]`) OID for an element type — for a single array
/// parameter feeding a `col = ANY($N)` in-list. Verified against PG
/// `pg_type.typarray`; cross-checked against the runtime crate's
/// `oids::*_ARRAY` constants by the const validator.
fn array_oid(ty: bsql_build::RustType) -> u32 {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => 1005,
        RustType::I32 => 1007,
        RustType::I64 => 1016,
        RustType::U32 => 1028,
        RustType::Bool => 1000,
        RustType::Text => 1009,
    }
}

/// The `oids::*_ARRAY` const path token for an element type's array OID.
fn array_oid_path(ty: bsql_build::RustType) -> TokenStream2 {
    use bsql_build::RustType;
    match ty {
        RustType::I16 => quote!(::bsql_postgres_proto::oids::INT2_ARRAY),
        RustType::I32 => quote!(::bsql_postgres_proto::oids::INT4_ARRAY),
        RustType::I64 => quote!(::bsql_postgres_proto::oids::INT8_ARRAY),
        RustType::U32 => quote!(::bsql_postgres_proto::oids::OID_ARRAY),
        RustType::Bool => quote!(::bsql_postgres_proto::oids::BOOL_ARRAY),
        RustType::Text => quote!(::bsql_postgres_proto::oids::TEXT_ARRAY),
    }
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

/// Emit every const wire artifact for one (possibly dynamic) query: the
/// dynamic-form budget assertions, then either ONE carrier (no runtime
/// `ORDER BY` allow-set) or one carrier per allowed ordering plus the
/// closed selector enum the caller picks from at runtime.
fn emit_dynamic_wire(
    name: &Ident,
    shape: &bsql_build::DynamicShape,
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
    let params_tuple = tuple_type(&param_markers);
    let row_tuple = tuple_type(&row_markers);

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
            #n_optional_lit <= ::bsql_postgres_proto::query_budget::MAX_OPTIONAL_FILTERS,
            "query!: too many OPTIONAL(...) toggled filters in one query",
        );
        const _: () = ::core::assert!(
            #n_variants_lit <= ::bsql_postgres_proto::query_budget::MAX_ORDER_BY_VARIANTS,
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
                &params_tuple,
                &row_tuple,
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
                    &params_tuple,
                    &row_tuple,
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
                    ) -> ::bsql_postgres_proto::PreparedQuery<#params_tuple, #row_tuple> {
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
    params_tuple: &TokenStream2,
    row_tuple: &TokenStream2,
) -> syn::Result<TokenStream2> {
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
         [`QueryFingerprint`](::bsql_postgres_proto::QueryFingerprint) \
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

    Ok(quote! {
        #[doc = #carrier_doc]
        #[allow(dead_code, reason = #allow_reason)]
        pub enum #carrier {}

        impl ::bsql_postgres_proto::QueryFingerprint for #carrier {
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
            pub const PREPARED: ::bsql_postgres_proto::PreparedQuery<#params_tuple, #row_tuple> =
                ::bsql_postgres_proto::prepared::run::<#carrier>();
        }

        // Footprint guard on the artifact: the carrier is a zero-size,
        // value-less marker; the wire data lives in `.rodata`.
        ::bsql_postgres_proto::wire_pin!(#carrier, size = 0, align = 1);
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
