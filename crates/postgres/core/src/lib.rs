#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// Mechanical-cast wall (tier-1) completing the workspace floor's
// `cast_sign_loss` + `integer_division` forbid: an `as` conversion, a truncating
// or sign-flipping `as` cast, and `unreachable!` are all rejected at compile
// time — a future `len as u32` on the untrusted-byte materialize path is a build
// error, not a hand scan. `deny` (not `forbid`) preserves a greppable, reasoned
// `#[expect(..., reason = "...")]` escape for a provably-lossless widening (the
// workspace keystone `allow_attributes_without_reason` forces the reason).
#![deny(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unreachable
)]
// Panic-class mechanical wall (tier-1): the last two mechanical classes — an
// unbounded `arr[i]` and an overflowing `+`/`-`/`*` on a cursor — are now
// rejected by rustc, not review, so a hostile server byte cannot drive a
// bounds-panic or a wrapping overflow on the decode / TLS path. Every existing
// production site was first converted to `.get(..).ok_or(<classified>)?` /
// `checked_*`; `deny` (not `forbid`) keeps a reasoned `#[expect]` escape.
// Indexing in test code is exempted by the clippy.toml
// `allow-indexing-slicing-in-tests` key; `arithmetic_side_effects` has NO such
// key, so the `cfg_attr(test, allow)` below scopes it to production (a test
// `assert_eq!(x, a + b)` on small constants is legitimate).
#![deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
#![cfg_attr(
    test,
    allow(
        clippy::arithmetic_side_effects,
        reason = "test assertions and fixtures use bare arithmetic on small constants; the production deny above is the tier-1 wall, mirroring clippy.toml's allow-*-in-tests carve-outs for the panic class"
    )
)]

//! Shared core for bsql PostgreSQL drivers (async + sync).
//!
//! Contains: `Row` (Arc-shared arena), `ResultCollector` (materialiser over the
//! sans-IO engine surface), `ConnectConfig`, `DriverError`, `tls`/`ssl` modules.
//! Both `bsql-postgres-async` and `bsql-postgres-sync` depend on this.

// The shared live-PostgreSQL SQL-mechanism scenario library
// (`define_sql_scenario_tests!`), run by BOTH drivers' `--ignored` live
// suites. A test-only concern, so it lives behind the OFF-BY-DEFAULT
// `test-scenarios` feature: a production `cargo build -p bsql-postgres-core`
// compiles no `test_scenarios` module and the `#[macro_export]` macro never
// enters core's public API. The drivers turn it on through a
// `[dev-dependencies]` edge, so it is present only when their test targets
// are built, never for a downstream consumer.
#[cfg(feature = "test-scenarios")]
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
pub use types::{ArenaBuilder, ArenaSealError, Notification, QueryResult, Row, RowSet};

/// The sealed parameter-encoding contract every `*_params` / typed verb takes
/// (`&(a, b)` tuples up to arity 16). Re-exported so a consumer can write a
/// generic helper — `fn run<P: ParamsWriter>(…)` — without a direct proto dep.
pub use bsql_postgres_proto::ParamsWriter;
