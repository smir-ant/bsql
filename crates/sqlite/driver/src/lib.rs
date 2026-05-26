#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod connection;
mod error;

pub use connection::{Connection, QueryResult, Row};
pub use error::SqliteError;
