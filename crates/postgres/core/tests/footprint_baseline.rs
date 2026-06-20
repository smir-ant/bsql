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
    ConnectConfig, DbError, DriverError, Notification, OwnedRow, OwnedRowTooLarge,
    PreparedStatement, QueryResult, Row, RowTooLarge, SslMode,
};

/// `(size, align)` baseline for every stable public type of `bsql-postgres-core`.
/// This mirrors the co-located `footprint_pin!` anchors; a divergence between
/// the two means one was edited without the other.
#[test]
fn core_stable_public_types_match_baseline() {
    // (measured size, measured align, expected size, expected align, name)
    let rows: &[(usize, usize, usize, usize, &str)] = &[
        (size_of::<Row>(), align_of::<Row>(), 16, 8, "Row"),
        (size_of::<OwnedRow>(), align_of::<OwnedRow>(), 16, 8, "OwnedRow"),
        (size_of::<OwnedRowTooLarge>(), align_of::<OwnedRowTooLarge>(), 0, 1, "OwnedRowTooLarge"),
        (size_of::<RowTooLarge>(), align_of::<RowTooLarge>(), 0, 1, "RowTooLarge"),
        (size_of::<DriverError>(), align_of::<DriverError>(), 120, 8, "DriverError"),
        (size_of::<DbError>(), align_of::<DbError>(), 120, 8, "DbError"),
        (size_of::<ConnectConfig>(), align_of::<ConnectConfig>(), 112, 8, "ConnectConfig"),
        (size_of::<SslMode>(), align_of::<SslMode>(), 1, 1, "SslMode"),
        (size_of::<PreparedStatement>(), align_of::<PreparedStatement>(), 104, 8, "PreparedStatement"),
        (size_of::<Notification>(), align_of::<Notification>(), 56, 8, "Notification"),
        (size_of::<QueryResult>(), align_of::<QueryResult>(), 72, 8, "QueryResult"),
    ];
    for &(sz, al, exp_sz, exp_al, name) in rows {
        assert_eq!(sz, exp_sz, "footprint baseline drift (size) for {name}");
        assert_eq!(al, exp_al, "footprint baseline drift (align) for {name}");
    }
}

/// Per-connection resident footprint estimate.
///
/// The live memory a single open PostgreSQL connection holds, excluding the
/// OS-kernel socket buffers (which the driver does not allocate):
///
/// ```text
///   component                       bytes   where
///   read buffer (Vec<u8>)            4096   heap, fixed at connect (READ_BUF_CAP)
///   Session (holds the protocol      2528   inline in Connection
///     state machine + pump scratch)
///   ----------------------------------------
///   ≈ 6.6 KB resident per idle connection
/// ```
///
/// `Session` is a transitional type (the protocol state machine plus pump
/// scratch); it is slated to shrink when a unified engine replaces the per-driver
/// pump. The 4096-byte read buffer is a tuning constant (`READ_BUF_CAP`), pinned
/// in `bsql-postgres-proto`, not a type footprint. This estimate is recorded as
/// a comparison anchor; the live `Session` size is asserted here so a regression
/// in the dominant resident component is visible.
#[test]
fn per_connection_resident_estimate() {
    // The read buffer is a fixed 4 KiB allocation made once per connection.
    assert_eq!(
        bsql_postgres_proto::READ_BUF_CAP, 4096,
        "per-connection read buffer size changed; update the resident estimate"
    );
    // The dominant inline component. This is a transitional (pre-engine) type;
    // the pin tracks the current cost so a regression before the rewrite is
    // visible, and is regenerated when the engine type lands.
    assert_eq!(
        size_of::<bsql_postgres_core::Session>(),
        2528,
        "Session inline size changed; update the per-connection resident estimate"
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
