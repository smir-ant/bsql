#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

pub mod config;
pub mod error;
pub mod session;
pub mod ssl;
pub mod types;

pub use config::{ConnectConfig, SslMode};
pub use error::{DbError, DriverError};
pub use session::{Handshake, HandshakeAction, PumpAction, Session};
pub use types::{FromText, Notification, PreparedStatement, QueryResult, Row};
