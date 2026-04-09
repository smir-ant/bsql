#![forbid(unsafe_code)]

//! Runtime support for bsql.
//!
//! This crate provides the types that `bsql::query!` generated code depends on:
//! error types, connection pool, and the executor trait.
//!
//! You should not depend on this crate directly — use [`bsql`] instead.

pub mod error;
pub mod executor;
pub mod listener;
pub mod pool;
pub mod singleflight;
#[cfg(feature = "sqlite")]
pub mod sqlite_pool;
pub mod stream;
pub mod test_support;
pub mod transaction;
pub mod types;
pub mod util;

/// Re-export bsql_driver_postgres types used by generated code.
/// Users do not need to depend on bsql-driver directly.
pub mod driver {
    pub use bsql_driver_postgres::arena::{acquire_arena, release_arena};
    pub use bsql_driver_postgres::hash_sql;
    pub use bsql_driver_postgres::{Arena, Config, Encode, PgDataRow, QueryResult, Row};

    // Scalar decode functions for generated inline raw-bytes code
    pub use bsql_driver_postgres::codec::decode_str;

    // Array decode functions for generated code
    pub use bsql_driver_postgres::codec::{
        decode_array_bool, decode_array_bytea, decode_array_f32, decode_array_f64,
        decode_array_i16, decode_array_i32, decode_array_i64, decode_array_str,
    };

    // Feature-gated decode functions for generated code
    #[cfg(feature = "decimal")]
    pub use bsql_driver_postgres::codec::decode_numeric_decimal;
    #[cfg(feature = "uuid")]
    pub use bsql_driver_postgres::codec::decode_uuid_type;
    #[cfg(feature = "chrono")]
    pub use bsql_driver_postgres::codec::{
        decode_date_chrono, decode_time_chrono, decode_timestamptz_chrono,
    };
    #[cfg(feature = "time")]
    pub use bsql_driver_postgres::codec::{
        decode_date_time, decode_time_time, decode_timestamptz_time,
    };
}

/// Re-export bsql_driver_sqlite types used by generated SQLite code.
#[cfg(feature = "sqlite")]
pub mod driver_sqlite {
    pub use bsql_driver_sqlite::codec::SqliteEncode;
    pub use bsql_driver_sqlite::conn::SqliteConnection;
    pub use bsql_driver_sqlite::ffi::{StepResult, StmtHandle};
    pub use bsql_driver_sqlite::pool::ParamValue;
    pub use bsql_driver_sqlite::SqliteError;
    pub use smallvec::{smallvec, SmallVec};

    // Arena types for arena-backed fetch
    pub use bsql_arena::{acquire_arena, Arena, ArenaRows, ValidatedRows};

    /// SQLite NULL type indicator (matches `SQLITE_NULL` = 5).
    /// Re-exported here so generated code does not need a direct libsqlite3-sys dep.
    pub const SQLITE_NULL: i32 = 5;
}

// --- Helper macros for async/sync conditional code generation ---
//
// These macros are used by `bsql::query!` generated code to conditionally
// add/remove `async` and `.await` based on whether the `async` feature is
// enabled. The cfg check happens here in bsql-core (where the feature is
// defined), not in the user's crate.

/// Conditionally adds `.await` when the `async` feature is enabled.
/// In sync mode, passes the expression through unchanged.
#[cfg(feature = "async")]
#[macro_export]
#[doc(hidden)]
macro_rules! __bsql_call {
    ($expr:expr) => {
        $expr.await
    };
}

/// Conditionally adds `.await` when the `async` feature is enabled.
/// In sync mode, passes the expression through unchanged.
#[cfg(not(feature = "async"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __bsql_call {
    ($expr:expr) => {
        $expr
    };
}

/// Conditionally adds `async` keyword to function definitions.
/// In sync mode, emits a regular `fn`.
#[cfg(feature = "async")]
#[macro_export]
#[doc(hidden)]
macro_rules! __bsql_fn {
    ($(#[$meta:meta])* pub fn $($rest:tt)*) => {
        $(#[$meta])* pub async fn $($rest)*
    };
}

/// Conditionally adds `async` keyword to function definitions.
/// In sync mode, emits a regular `fn`.
#[cfg(not(feature = "async"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __bsql_fn {
    ($(#[$meta:meta])* pub fn $($rest:tt)*) => {
        $(#[$meta])* pub fn $($rest)*
    };
}

pub use error::{BsqlError, BsqlResult};
pub use executor::{OwnedResult, QueryTarget};
pub use listener::{Listener, Notification};
pub use pool::{PgPool, Pool, PoolBuilder, PoolConnection, PoolStatus};
#[cfg(feature = "sqlite")]
pub use sqlite_pool::{SqlitePool, SqliteStreamingQuery, SqliteTransaction};
pub use stream::QueryStream;
pub use transaction::{IsolationLevel, Transaction};

/// Hash a string using rapidhash. Used by singleflight, statement naming,
/// and offline cache keys. Not part of the public API.
#[doc(hidden)]
pub fn rapid_hash_str(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = rapidhash::quality::RapidHasher::default();
    s.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rapid_hash_str_deterministic() {
        let h1 = rapid_hash_str("SELECT 1");
        let h2 = rapid_hash_str("SELECT 1");
        assert_eq!(h1, h2);
    }

    #[test]
    fn rapid_hash_str_different_inputs_differ() {
        let h1 = rapid_hash_str("SELECT 1");
        let h2 = rapid_hash_str("SELECT 2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn rapid_hash_str_empty_string() {
        let h = rapid_hash_str("");
        // Should not panic, and the hash should be consistent
        assert_eq!(h, rapid_hash_str(""));
    }
}
