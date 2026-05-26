#![forbid(unsafe_code)]

mod connection;
mod error;

pub use connection::{Connection, QueryResult, Row};
pub use error::SqliteError;
