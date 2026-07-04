//! Fixture consumer for the external-type bridge.
//!
//! This crate's `build.rs` registers three bridges (see it), keyed on canonical
//! PostgreSQL types, whose targets and converters this module provides. A
//! consumer wires the bridge ONCE (in `build.rs` + these free functions) and
//! then EVERY `query!` across the crate decodes the bridged columns into the
//! chosen types with no per-query code.
//!
//! `query!` is invoked in the integration tests (`tests/`), which name this
//! crate by its package name so the bridge target/converter paths resolve.

/// The consumer-owned bridge module: one target type / converter per bridged
/// PostgreSQL type. Each converter is INFALLIBLE — the consumer owns the
/// total / saturating semantics.
pub mod bridge {
    /// A dep-free stand-in for an external timestamp type (e.g. what
    /// `chrono::DateTime` / `time::OffsetDateTime` would be in a real
    /// consumer). It is foreign to bsql, so it could not be reached by an
    /// `impl bsql::Cell for MyTs` (E0117) — the free-fn converter below is the
    /// orphan-proof seam. Holds the raw PostgreSQL-epoch microseconds.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MyTs(pub i64);

    /// The `timestamptz` bridge converter: reshape the native `Timestamptz`
    /// into the fixture-local `MyTs`. Infallible — the raw PG-epoch micros are
    /// a total function of the value (the consumer owns this choice).
    #[must_use]
    pub fn to_myts(v: bsql::Timestamptz) -> MyTs {
        MyTs(v.as_micros())
    }

    /// The `uuid` bridge converter: reshape the native `bsql::Uuid` into the
    /// REAL `uuid::Uuid`, by copying the 16 raw bytes. Infallible.
    #[must_use]
    pub fn to_uuid(v: bsql::Uuid) -> uuid::Uuid {
        uuid::Uuid::from_bytes(*v.as_bytes())
    }

    /// A dep-free stand-in for an external decimal type (e.g. what
    /// `rust_decimal::Decimal` / `bigdecimal::BigDecimal` would be in a real
    /// consumer). It is foreign to bsql, so it could not be reached by an
    /// `impl bsql::Cell for MyDecimal` (E0117) — the free-fn converter below is
    /// the orphan-proof seam. It holds the EXACT decimal text, so no precision
    /// is lost bridging an arbitrary-precision `numeric` (a decimal crate with
    /// bounded precision would be the consumer's own tradeoff, not bsql's).
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct MyDecimal(pub String);

    /// The `numeric` bridge converter: reshape the native, arbitrary-precision
    /// `bsql::Numeric` into the fixture-local `MyDecimal` by taking its exact
    /// decimal text. Infallible — the exact string is a total function of the
    /// value (the consumer owns the choice of how much precision to keep).
    #[must_use]
    pub fn to_decimal(v: bsql::Numeric) -> MyDecimal {
        MyDecimal(v.to_string())
    }

    /// A dep-free stand-in for an external calendar-date type (e.g. what
    /// `chrono::NaiveDate` / `time::Date` would be in a real consumer). Foreign
    /// to bsql, so it could only be reached by the orphan-proof free-fn
    /// converter below (an `impl bsql::Cell for MyDate` would be E0117). Holds
    /// the proleptic-Gregorian `(year, month, day)`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MyDate {
        /// Astronomical year (`0` = 1 BC).
        pub year: i32,
        /// Month `1..=12` (`0` for the `±infinity` sentinels).
        pub month: u8,
        /// Day `1..=31` (`0` for the `±infinity` sentinels).
        pub day: u8,
    }

    /// The `date` bridge converter: reshape the native `bsql::Date` into the
    /// fixture-local `MyDate` via the dependency-free civil conversion — exactly
    /// what a real consumer would do to build a `chrono::NaiveDate`. Infallible:
    /// the `±infinity` sentinels have no civil date, so the consumer maps them
    /// to a zeroed `MyDate` (its own total-function choice).
    #[must_use]
    pub fn to_mydate(v: bsql::Date) -> MyDate {
        match v.to_civil() {
            Some((year, month, day)) => MyDate { year, month, day },
            None => MyDate { year: 0, month: 0, day: 0 },
        }
    }
}
