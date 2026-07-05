#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Shared core for bsql PostgreSQL drivers (async + sync).
//!
//! Contains: `Row` (Arc-shared arena), `ResultCollector` (materialiser over the
//! sans-IO engine surface), `ConnectConfig`, `DriverError`, `tls`/`ssl` modules.
//! Both `bsql-postgres-async` and `bsql-postgres-sync` depend on this.

pub mod test_scenarios;

pub mod config;
// The transport-generic driver engine (`Core<S>`): every non-I/O verb written
// ONCE, shared by the async and sync drivers, monomorphised per transport.
pub mod driver;
pub mod error;
pub mod footprint;
pub mod materialize;
// The per-connection N+1 query detector — a diagnostics-only, zero-cost-off
// tracker. Compiled only under the `n1-detect` feature; a default build has no
// tracker type, no field, and no query-path branch.
#[cfg(feature = "n1-detect")]
pub mod n1;
// The per-connection notification ledger (a bounded, counted no-drop buffer)
// and the sink adapter that captures every surfaced notification into it.
pub mod notify;
pub mod sql_ident;
// The PostgreSQL `SSLRequest` probe + response classifier. TLS-only: with the
// `tls` feature OFF the probe is never sent (the driver connects plaintext
// directly) and this module — which names `rustls` server-name types — is not
// compiled.
#[cfg(feature = "tls")]
pub mod ssl;
// The in-memory fake PostgreSQL backend behind the engine's transport seam.
// Feature-gated OFF by default so the real transport path — and the shipped
// runtime closure — is untouched unless a consumer opts into the testkit.
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod tls;
// The bounded, typed result of a compile-checked `query!` — `Rows<Q>` plus the
// `RowsBuilder` prebuffer collector both drivers feed.
pub mod typed_rows;
pub mod types;

pub use config::{resolve_endpoint, validate_startup_params, ConnectConfig, Endpoint, SslMode};
pub use driver::{Core, PreparedStatement};
pub use error::{ColumnError, DbError, DriverError};
pub use materialize::{CollectedResult, DbErrorSink, ResultCollector};
#[cfg(feature = "n1-detect")]
pub use n1::{N1Report, N1Tracker};
pub use notify::{capture_notify, NotificationLedger, TypedNotification};
pub use sql_ident::{SafeIdent, SafeTable};
pub use typed_rows::{Rows, RowsBuilder};
pub use types::{ArenaBuilder, ArenaSealError, Notification, QueryResult, Row};
