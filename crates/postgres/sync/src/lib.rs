#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod config;
mod connection;
mod error;

pub use config::{ConnectConfig, SslMode};
pub use connection::{Connection, PreparedStatement, QueryResult, Row};
pub use error::{DbError, DriverError};
