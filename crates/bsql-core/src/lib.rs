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
pub mod transaction;
pub mod types;
pub mod util;

/// Re-export bsql_driver_postgres types used by generated code.
/// Users do not need to depend on bsql-driver directly.
pub mod driver {
    pub use bsql_driver_postgres::arena::{acquire_arena, release_arena};
    pub use bsql_driver_postgres::hash_sql;
    pub use bsql_driver_postgres::{Arena, Encode, PgDataRow, QueryResult, Row};

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
    pub use bsql_driver_sqlite::SqliteError;
    pub use bsql_driver_sqlite::codec::SqliteEncode;
    pub use bsql_driver_sqlite::conn::SqliteConnection;
    pub use bsql_driver_sqlite::ffi::{StepResult, StmtHandle};
    pub use bsql_driver_sqlite::pool::ParamValue;
    pub use smallvec::{SmallVec, smallvec};

    // Arena types for arena-backed fetch_all
    pub use bsql_arena::{Arena, ArenaRows, ValidatedRows, acquire_arena};

    /// SQLite NULL type indicator (matches `SQLITE_NULL` = 5).
    /// Re-exported here so generated code does not need a direct libsqlite3-sys dep.
    pub const SQLITE_NULL: i32 = 5;
}

pub use error::{BsqlError, BsqlResult};
pub use executor::Executor;
pub use listener::{Listener, Notification};
pub use pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
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
