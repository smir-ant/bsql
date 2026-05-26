#![forbid(unsafe_code)]

mod config;
mod connection;
mod error;

pub use config::{ConnectConfig, SslMode};
pub use connection::{Connection, PreparedStatement, QueryResult, Row};
pub use error::{DbError, DriverError};
