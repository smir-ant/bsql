//! Fixture build script — the exact shape a real consumer with external-type
//! bridges uses.
//!
//! It replays `migrations/` into the catalog AND registers external-type
//! bridges via the `CatalogBuilder`, then emits both proc-macro channels (the
//! schema catalog + the bridge overrides). Three bridges are registered:
//!
//!   * `timestamptz` -> a dep-free FIXTURE-LOCAL stand-in type (`MyTs`), proving
//!     the free-fn bridge mechanism needs no external crate and is orphan-proof
//!     (the stand-in is foreign to bsql, yet a free-fn converter bridges it);
//!   * `uuid` -> the REAL external `uuid::Uuid`, proving real-world use with
//!     bsql depending on and forcing NOTHING (both the target type and the
//!     converter travel as strings);
//!   * `numeric` -> a dep-free FIXTURE-LOCAL `MyDecimal` stand-in, proving the
//!     variable-width, arbitrary-precision `bsql::Numeric` pivot bridges into a
//!     consumer's chosen decimal type with no forced dependency;
//!   * `date` -> a dep-free FIXTURE-LOCAL `MyDate` stand-in built from the
//!     native `bsql::Date`'s civil conversion, proving the temporal pivot
//!     bridges into a consumer's calendar-date type (a `chrono::NaiveDate`
//!     stand-in) with no forced dependency.
//!
//! Both target/converter paths name this crate by its own package name so they
//! resolve from the integration-test crates where `query!` is invoked. Any
//! error is propagated (fail the build) — never swallowed.
//!
//! This is deliberately PostgreSQL-only (`.emit_catalog()`, no SQLite template):
//! the bridged `uuid` / `timestamptz` columns have no portable SQLite form, and
//! the literal `::cast` syntax the tests use is not SQLite SQL. Using the
//! PG-only terminal keeps the SQLite conformance oracle disengaged even when the
//! surrounding workspace build activates `bsql-build`'s `sqlite` feature via
//! feature unification.

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::Catalog::from_migrations("migrations")?
        .bridge(
            "timestamptz",
            "bsql_query_bridge_fixture::bridge::MyTs",
            "bsql_query_bridge_fixture::bridge::to_myts",
        )
        .bridge(
            "uuid",
            "uuid::Uuid",
            "bsql_query_bridge_fixture::bridge::to_uuid",
        )
        .bridge(
            "numeric",
            "bsql_query_bridge_fixture::bridge::MyDecimal",
            "bsql_query_bridge_fixture::bridge::to_decimal",
        )
        .bridge(
            "date",
            "bsql_query_bridge_fixture::bridge::MyDate",
            "bsql_query_bridge_fixture::bridge::to_mydate",
        )
        .emit_catalog()
}
