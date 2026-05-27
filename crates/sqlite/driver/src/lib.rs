#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod connection;
mod error;

pub use connection::{Connection, FromText, QueryResult, Row, SqliteValue};
pub use error::SqliteError;
