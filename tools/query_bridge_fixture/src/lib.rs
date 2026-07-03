//! Fixture consumer for the external-type bridge.
//!
//! This crate's `build.rs` registers two bridges (see it), keyed on canonical
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
}
