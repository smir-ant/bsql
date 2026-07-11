#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// Every consumer-facing public item carries a doc comment — a future
// undocumented `pub` on the error / config / notify surface is a build error,
// not silent doc rot.
#![deny(missing_docs)]
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

// bsql's `footprint_pin!` guards assert exact `size_of` / `align_of` values
// computed for 64-bit pointers; on a non-64-bit target they fail as a wall of
// confusing `E0080` "FOOTPRINT DRIFT" panics. This one honest line replaces that
// wall. 64-bit is the only supported width (i686 / wasm32 / 32-bit ARM are
// unrequested and unsupported); 64-bit builds are unaffected.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("bsql requires a 64-bit target; the footprint pins assume 64-bit pointers");

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

// The transport-agnostic building blocks of driver-level query cancellation:
// the unforgeable `CancelKey` authenticator and the credential-free `Redial`
// endpoint snapshot. The I/O half (dial + write the CancelRequest) lives in each
// driver's `CancelToken`.
pub mod cancel;
pub mod config;
// The transport-generic driver engine (`Core<S>`): every non-I/O verb written
// ONCE, shared by the async and sync drivers, monomorphised per transport.
pub mod driver;
pub mod error;
pub mod footprint;
pub mod materialize;
// The migration RUNNER: applies a consumer's migration set to a live database,
// exactly once, in deterministic order, atomically per migration, with a ledger
// + checksum-drift detection + a concurrency advisory lock. Defined once over
// `Core<S>`, so both PostgreSQL drivers share it.
pub mod migrate;
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

pub use cancel::{CancelKey, Redial};
pub use config::{
    resolve_endpoint, validate_startup_params, ChannelBindingMode, ConnectConfig, Endpoint,
    SslMode, UNIX_SOCKET_UNSUPPORTED,
};
#[cfg(feature = "scram")]
pub use config::resolve_channel_binding;
pub use driver::{Core, PreparedStatement};
pub use error::{ColumnError, DbError, DriverError};
pub use materialize::{DbErrorSink, ResultCollector};
pub use migrate::{
    AppliedMigration, DriftKind, MigrationError, MigrationReport, MigrationSource,
    MigrationSourceError, MigrationStatus, LEDGER_TABLE,
};
#[cfg(feature = "n1-detect")]
pub use n1::{N1Report, N1Tracker};
pub use notify::{capture_notify, NotificationLedger, TypedNotification};
pub use sql_ident::{SafeIdent, SafeTable};
pub use typed_rows::{Rows, RowsBuilder};
pub use types::{
    ArenaBuilder, ArenaSealError, BorrowedRow, Notification, QueryResult, Row, RowRef, RowSet,
};

/// The typed `CommandComplete` tag ([`QueryResult::command_tag`]'s type — a
/// `Copy` enum a consumer matches on, e.g. `CommandTag::Insert { rows }`, rather
/// than substring-parsing a string). Re-exported so a consumer can name the tag
/// type without a direct proto dep.
pub use bsql_postgres_proto::command_tag::CommandTag;

/// The sealed parameter-encoding contract every `*_params` / typed verb takes
/// (`&(a, b)` tuples up to arity 16). Re-exported so a consumer can write a
/// generic helper — `fn run<P: ParamsWriter>(…)` — without a direct proto dep.
pub use bsql_postgres_proto::ParamsWriter;

/// The compile-checked binary-COPY carrier trait a `copy!` invocation implements
/// (its target `Q::SQL` + typed `Q::Row<'q>`). Re-exported so a driver's
/// `copy_in_typed::<Q>` verb — and a consumer naming the bound — reach it without
/// a direct proto dep.
pub use bsql_postgres_proto::TypedCopyIn;
