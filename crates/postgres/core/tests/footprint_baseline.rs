//! Footprint baseline — the single committed record of the measured footprint
//! of every stable public type, plus the per-connection resident estimate and
//! the minimum-consumer binary size at the time of measurement.
//!
//! The per-type pins live co-located with each type as `footprint_pin!`
//! (`const _: ()`, an `E0080` build failure on drift). This test is the
//! *human-readable ledger*: it re-asserts the same numbers from one place so a
//! reviewer can see the whole footprint surface at a glance, and it records the
//! two numbers that are NOT a single type's `size_of` — the per-connection
//! resident estimate and the consumer binary size — so future drift is
//! comparable.
//!
//! All numbers measured @ aarch64-apple-darwin, rustc 1.96.0.
//!
//! If a stable public type's size legitimately changes, BOTH its co-located
//! `footprint_pin!` and the matching line here move in the same commit — that
//! is the point: the byte cost lands on the review surface, never drifting
//! silently.

use core::mem::{align_of, size_of};

use bsql_postgres_core::{
    ArenaSealError, ConnectConfig, DbError, DriverError, Notification, QueryResult, Row, SslMode,
};

/// `(size, align)` baseline for every stable public type of `bsql-postgres-core`.
/// This mirrors the co-located `footprint_pin!` anchors; a divergence between
/// the two means one was edited without the other.
#[test]
fn core_stable_public_types_match_baseline() {
    // (measured size, measured align, expected size, expected align, name)
    let rows: &[(usize, usize, usize, usize, &str)] = &[
        (size_of::<Row>(), align_of::<Row>(), 16, 8, "Row"),
        (size_of::<ArenaSealError>(), align_of::<ArenaSealError>(), 1, 1, "ArenaSealError"),
        (size_of::<DriverError>(), align_of::<DriverError>(), 32, 8, "DriverError"),
        (size_of::<DbError>(), align_of::<DbError>(), 120, 8, "DbError"),
        (size_of::<ConnectConfig>(), align_of::<ConnectConfig>(), 136, 8, "ConnectConfig"),
        (size_of::<SslMode>(), align_of::<SslMode>(), 1, 1, "SslMode"),
        (size_of::<Notification>(), align_of::<Notification>(), 56, 8, "Notification"),
        (size_of::<QueryResult>(), align_of::<QueryResult>(), 72, 8, "QueryResult"),
    ];
    for &(sz, al, exp_sz, exp_al, name) in rows {
        assert_eq!(sz, exp_sz, "footprint baseline drift (size) for {name}");
        assert_eq!(al, exp_al, "footprint baseline drift (align) for {name}");
    }
}

/// Per-connection resident footprint anchor.
///
/// The dominant heap component a single open PostgreSQL connection holds,
/// excluding the OS-kernel socket buffers (which the driver does not allocate),
/// is the engine's read buffer — a fixed `READ_BUF_CAP`-byte allocation made
/// once per connection. The constant is pinned in `bsql-postgres-proto`; this
/// re-asserts it from the consumer side so a tuning change lands on the review
/// surface here too.
#[test]
fn per_connection_resident_estimate() {
    // The read buffer is a fixed 4 KiB allocation made once per connection.
    assert_eq!(
        bsql_postgres_proto::READ_BUF_CAP, 4096,
        "per-connection read buffer size changed; update the resident estimate"
    );
}

/// Minimum-consumer binary size — recorded for comparison.
///
/// Measured by building a minimal SQLite-only consumer (`open_in_memory` +
/// `execute` + `query`) in release and stripping it:
///
/// ```text
///   release, stripped:  ≈ 2.03 MB  (aarch64-apple-darwin)
/// ```
///
/// This figure is dominated by the **bundled SQLite C engine** that `rusqlite`
/// statically links, NOT by bsql's own Rust code — bsql's contribution is a
/// small fraction of it. A PostgreSQL-only consumer is the other anchor but
/// pulls in the async runtime and TLS stack; the SQLite figure is the smaller,
/// more reproducible floor. It is recorded as text (a binary-size measurement
/// cannot be a const assertion); regenerate by building a minimal consumer in
/// release and stripping it.
///
/// This test holds no runtime assertion — it exists to anchor the documented
/// number in a committed, greppable location alongside the type pins.
#[test]
fn min_consumer_binary_size_is_recorded() {
    // Documentation anchor only; see the doc comment for the measured figure.
}
