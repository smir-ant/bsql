#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// A future undocumented `pub` item is a build error, not silent doc rot.
#![deny(missing_docs)]
// Mechanical-cast wall (tier-1) completing the workspace floor's
// `cast_sign_loss` + `integer_division` forbid: an `as` conversion, a truncating
// or sign-flipping `as` cast, and `unreachable!` are all rejected at compile
// time — a future `len as u32` on an untrusted-value decode path is a build
// error, not a hand scan. `deny` (not `forbid`) preserves a greppable, reasoned
// `#[expect(..., reason = "...")]` escape for a provably-lossless widening (the
// workspace keystone `allow_attributes_without_reason` forces the reason).
#![deny(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unreachable
)]
// Panic-class mechanical wall (tier-1): an unbounded `arr[i]` and an overflowing
// `+`/`-`/`*` on a cursor are now rejected by rustc, not review. `deny` (not
// `forbid`) keeps a reasoned `#[expect]` escape. Indexing in test code is
// exempted by clippy.toml `allow-indexing-slicing-in-tests`;
// `arithmetic_side_effects` has NO such key, so the `cfg_attr(test, allow)`
// below scopes it to production.
#![deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
#![cfg_attr(
    test,
    allow(
        clippy::arithmetic_side_effects,
        reason = "test assertions/fixtures use bare arithmetic; the production deny above is the tier-1 wall"
    )
)]

//! Embedded SQLite driver.
//!
//! Footprint is a measured, build-gated dimension here as in the PostgreSQL
//! crates. The [`footprint_pin!`] macro pins the `size_of` and `align_of` of
//! each stable public type with a `const _: ()` item — a layout drift becomes
//! an `E0080` const-eval failure at `cargo check`, including for a type
//! constructed nowhere. This crate does not depend on the PostgreSQL proto
//! crate that carries the analogous `wire_pin!`, so it defines its own
//! self-contained copy of the same mechanism. Runtime cost is zero.
//!
//! Baseline footprint (measured @ aarch64-apple-darwin, rustc 1.96.0):
//!
//! ```text
//!   TYPE           size  align
//!   Row              16      8   Arc<arena> + u32 row index (lazy handle)
//!   RowSet           16      8   Option<Arc<arena>> + u32 row count (lazy)
//!   BorrowedRow       8      8   one &rusqlite::Row (streaming, zero-copy)
//!   SqliteValue      32      8   widest variant Text(String)/Blob(Vec<u8>)
//!   ValueRef         24      8   widest variant Text/Blob (&[u8] fat pointer)
//!   Type              1      1   five field-less storage-class variants
//!   QueryResult      32      8   RowSet + Arc<[String]> (one shared arena)
//!   SqliteError      32      8   String-carrying variants (niche-packed tag)
//! ```

/// Pin the `size_of` AND `align_of` of a nameable type at build time — a
/// layout drift becomes an `E0080` const-eval failure. The emitted
/// `const _: ()` item is evaluated at `cargo check`, including for a type
/// constructed nowhere, and is fully erased by codegen (zero runtime cost).
///
/// Self-contained twin of the PostgreSQL crates' analogous footprint pin; this
/// crate has no dependency on those crates, so it carries its own copy.
macro_rules! footprint_pin {
    ($t:ty, size = $n:expr, align = $a:expr $(,)?) => {
        const _: () = {
            assert!(
                core::mem::size_of::<$t>() == $n,
                concat!("FOOTPRINT DRIFT (size) for ", stringify!($t))
            );
            assert!(
                core::mem::align_of::<$t>() == $a,
                concat!("FOOTPRINT DRIFT (align) for ", stringify!($t))
            );
        };
    };
}
pub(crate) use footprint_pin;

mod bind;
mod cancel;
mod connection;
mod error;
// The per-connection N+1 query detector — a diagnostics-only, zero-cost-off
// tracker (a self-contained twin of the PostgreSQL detector). Compiled only under
// the `n1-detect` feature; a default build has no tracker type and no field.
#[cfg(feature = "n1-detect")]
mod n1;
// The migration RUNNER — the cross-backend twin of the PostgreSQL runner over
// the SAME `MigrationSource` / ledger / checksum / drift contract. A
// self-contained copy of the pure logic (the embedded crate depends on no
// `bsql-postgres-core`), pinned to the SAME known-answer vector.
mod migrate;
mod typed;
mod value;

pub use bind::{SqliteBindParams, SqliteBindValue};
pub use cancel::SqliteCancelToken;
pub use connection::{
    BorrowedRow, Connection, QueryResult, Row, RowSet, SqliteStatement, SqliteTypedStatement,
    Transaction, TypedRows,
};
pub use error::SqliteError;
pub use migrate::{
    AppliedMigration, DriftKind, MigrationError, MigrationReport, MigrationSource,
    MigrationSourceError, MigrationStatus, LEDGER_TABLE,
};
#[cfg(feature = "n1-detect")]
pub use n1::{N1Report, N1Tracker};
pub use typed::{ColumnSource, SqliteTypedQuery};
pub use value::{FromColumn, SqliteValue, Type, ValueRef};

// The typed-decode helper functions the `query!` expansion names. Reachable
// (not `pub(crate)`) ONLY so the macro's emitted `SqliteTypedQuery` impl can
// call them through the umbrella's hidden `__rt_sqlite` re-export; a consumer
// never names them directly.
#[doc(hidden)]
pub use typed::{read_optional, read_required};
