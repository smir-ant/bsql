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
use proc_macro2::Span;
use quote::quote;
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
