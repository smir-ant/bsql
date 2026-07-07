#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// A future undocumented `pub` item is a build error, not silent doc rot.
#![deny(missing_docs)]
// Mechanical-cast wall (tier-1) completing the workspace floor's
// `cast_sign_loss` + `integer_division` forbid: an `as` conversion, a truncating
// or sign-flipping `as` cast, and `unreachable!` are all rejected at compile
// time — a future `len as u32` on an untrusted-byte path is a build error, not a
// hand scan. `deny` (not `forbid`) preserves a greppable, reasoned
// `#[expect(..., reason = "...")]` escape for a provably-lossless widening (the
// workspace keystone `allow_attributes_without_reason` forces the reason).
#![deny(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unreachable
)]
// Panic-class mechanical wall (tier-1): an unbounded `arr[i]` and an overflowing
// `+`/`-`/`*` on a cursor are now rejected by rustc, not review. `deny` (not
// `forbid`) keeps a reasoned `#[expect]` escape. Indexing in test code is
// exempted by clippy.toml `allow-indexing-slicing-in-tests`;
// `arithmetic_side_effects` has NO such key, so the `cfg_attr(test, allow)`
// below scopes it to production.
#![deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
#![cfg_attr(
    test,
    allow(
        clippy::arithmetic_side_effects,
        reason = "test assertions/fixtures use bare arithmetic; the production deny above is the tier-1 wall"
    )
)]

//! Sync PostgreSQL driver built on the `bsql-postgres-proto` sans-IO engine.
//!
//! [`Connection`] owns an `Engine` over a `Wire<SyncSocket>` and drives each
//! verb with the engine's single-poll executor over the blocking socket. The
//! linear `Live` token the engine threads is held as the connection's health
//! bit (`Some` = reusable, `None` = dead). The engine's tier-1 error model
//! returns the token inside `Ok(Outcome { live, status })` whenever the
//! connection is alive — including on a recoverable server error (reported as
//! `CommandStatus::ServerErrored`, the connection already drained to a clean
//! idle) — so a query-level error never kills the connection and there is no
//! separate token-reclaim step.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports (`Row`, `DriverError`,
//! `ConnectConfig`, `Notification`, …) carry their `size_of`/`align_of` pins in
//! `bsql-postgres-core`, where they are defined; re-exporting does not change a
//! type's footprint, so they are not re-pinned here. The engine surface types
//! the driver composes (`Engine`, `Live`, `Surface`, …) carry their pins in
//! `bsql-postgres-proto`. The sync driver has no futures of its own — its
//! operations are blocking method calls whose working set lives on the caller's
//! stack — so there is no `future_pin!` surface here; the `Connection` shell is
//! a thin handle (engine + token + control socket + cached params) and is not
//! separately pinned.

mod cancel;
mod connection;
mod pool;
mod transport;

pub use bsql_postgres_core::{
    ColumnError, ConnectConfig, DbError, DriverError, Notification, ParamsWriter, QueryResult,
    Row, RowRef, Rows, SafeIdent, SafeTable, SslMode, TypedNotification,
};

// Re-export the compile-checked-query bound so a consumer can NAME the `query::<Q>`
// verb's `Q: TypedQuery` constraint (e.g. in a generic-over-backend data layer)
// through the driver alone, without reaching for the umbrella's `macros`
// re-export. Symmetric with the SQLite driver, which already exposes
// `SqliteTypedQuery`.
pub use bsql_postgres_proto::TypedQuery;

pub use cancel::CancelToken;
pub use connection::{Connection, CopyInWriter, PreparedStatement, Transaction};
pub use pool::{Pool, PooledConnection};

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_static<T: 'static>() {}
    fn _assertions() {
        _assert_send::<Connection>();
        _assert_send::<Row>();
        _assert_sync::<Row>();
        _assert_static::<Row>();
        _assert_send::<Pool>();
        _assert_sync::<Pool>();
        // PooledConnection owns the connection + an Arc to the pool, so it is
        // Send + 'static (movable across threads), not a borrow-based guard.
        _assert_send::<PooledConnection>();
        _assert_static::<PooledConnection>();
        // The CancelToken is a DETACHED capability: Send + Sync + 'static so it
        // can move to another thread and be shared while the owning connection's
        // blocking query is in flight (the whole out-of-band design).
        _assert_send::<CancelToken>();
        _assert_sync::<CancelToken>();
        _assert_static::<CancelToken>();
    }
};

// Footprint pin: the cancel key (8) + the redial snapshot (48) = 56 bytes,
// matching the async driver's token (both compose the same core building blocks).
const _: () = assert!(core::mem::size_of::<CancelToken>() == 56);
