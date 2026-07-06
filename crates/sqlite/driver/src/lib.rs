#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

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

mod connection;
mod error;
mod value;

pub use connection::{BorrowedRow, Connection, QueryResult, Row, RowSet, Transaction};
pub use error::SqliteError;
pub use value::{FromColumn, SqliteValue, Type, ValueRef};
