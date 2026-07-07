//! Write-once cross-backend data access — the `SyncBackend` trait unifying the
//! two BLOCKING drivers (`bsql::pg_sync` + `bsql::sqlite`) behind one generic
//! surface, so a data layer written ONCE runs on PostgreSQL or embedded SQLite.
//!
//! # Why only the blocking pair
//!
//! `pg_sync` and `sqlite` are BOTH blocking and expose the SAME verb set over
//! the SAME `query!` record twins (each carrier implements both `TypedQuery` for
//! the PostgreSQL wire and `SqliteTypedQuery` for the SQLite value model). The
//! async/sync split that would force a maybe-async "universal" trait does NOT
//! apply here — so this unification is a COMPILER guarantee (fully monomorphised,
//! no `dyn`, no boxing), not a lowest-common-denominator runtime abstraction. The
//! async driver is unified separately (a future `AsyncBackend` over async-fn-in-
//! trait), never forced into one maybe-async shape.
//!
//! # The shape
//!
//! - [`SyncQueries`] is the data-verb surface shared by a connection AND a
//!   transaction guard: `execute_sql` plus the compile-checked typed
//!   `fetch_all` / `fetch_one` / `fetch_opt` (each taking the query's typed
//!   `Q::Params` — the SAME tuple on both backends).
//! - [`SyncBackend`] adds the transaction combinator over a `type Tx<'t>` guard
//!   GAT (itself a [`SyncQueries`]), so a transaction body is ALSO generic over
//!   the backend.
//! - [`RunsOn`] bridges a `query!` carrier to a concrete backend, keyed on the
//!   backend so the carrier's `Params` / `Owned` are UNAMBIGUOUS even though it
//!   implements two typed-query traits with same-named associated types.
//!
//! # The consumer signature
//!
//! ```ignore
//! fn load_user<B>(conn: &mut B, id: i64) -> Result<UserByIdOwned, B::Error>
//! where
//!     B: SyncBackend,
//!     UserByIdQuery: RunsOn<B, Params = (i64,), Owned = UserByIdOwned>,
//! {
//!     conn.fetch_one::<UserByIdQuery>((id,))
//! }
//! ```
//!
//! `B: SyncBackend` names the backend; ONE `RunsOn<B, …>` bound per distinct
//! `query!` the function runs names the carrier's params + owned record. No
//! `dyn`, no HRTB soup, no unnameable lifetimes — the container GAT is hidden by
//! routing owned records through `Vec<Q::Owned>`.

// The cross-backend error classification lives at the crate root (`crate::BackendError`
// — sqlstate + the constraint-class predicates + `is_no_rows`), where it is available
// to the ASYNC driver too. `SyncQueries::Error` bounds on it so a generic data layer
// classifies a failure (e.g. no-rows) identically across backends.
use crate::BackendError;

/// The data-verb surface shared by a connection and a transaction guard.
///
/// Both a `Connection` and its `Transaction` guard implement this, so the same
/// generic code runs at the connection level or inside a `transaction` body.
pub trait SyncQueries {
    /// The backend's classified error (`DriverError` on PostgreSQL,
    /// `SqliteError` on SQLite) — a [`BackendError`], so a generic consumer can
    /// classify a failure (e.g. no-rows) identically across backends.
    type Error: BackendError;

    /// Run a raw-SQL statement for its affected-row count. The dynamic escape
    /// hatch — the compile-checked path is `fetch_*`.
    ///
    /// # Portable contract
    ///
    /// The PORTABLE contract is a SINGLE statement. Multi-statement `sql` (several
    /// statements separated by `;`) is BACKEND-DEFINED and NOT portable:
    /// PostgreSQL's simple-query protocol executes every statement and returns the
    /// last one's count, while SQLite executes only the first and classifies the
    /// trailing text as an error. A generic data layer must therefore pass one
    /// statement per call; a caller needing a multi-statement batch should use the
    /// concrete driver's batch verb, not this cross-backend surface.
    ///
    /// # Errors
    ///
    /// The backend's classified [`Error`](Self::Error) on a SQL / server failure.
    fn execute_sql(&mut self, sql: &str) -> Result<u64, Self::Error>;

    /// Run a compile-checked `query!` and collect its rows as owned records.
    ///
    /// `params` is the query's typed `Q::Params` tuple (`()` for a param-free
    /// query) — the SAME tuple on both backends.
    ///
    /// # Errors
    ///
    /// The backend's classified [`Error`](Self::Error) on a SQL / server /
    /// decode failure.
    // `track_caller` under `n1-detect` so the detector's `Location::caller()`
    // (captured at the leaf driver verb) propagates THROUGH this generic frame to
    // the consumer's call site, not this trait method. Zero cost when off.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    fn fetch_all<'p, Q>(&mut self, params: Q::Params<'p>) -> Result<Vec<Q::Owned>, Self::Error>
    where
        Self: Sized,
        Q: RunsOn<Self>,
    {
        Q::fetch_all(self, params)
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row.
    ///
    /// # Errors
    ///
    /// The backend's classified [`Error`](Self::Error), including a no-rows /
    /// too-many-rows error, matching the concrete driver's `query_one`.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    fn fetch_one<'p, Q>(&mut self, params: Q::Params<'p>) -> Result<Q::Owned, Self::Error>
    where
        Self: Sized,
        Q: RunsOn<Self>,
    {
        Q::fetch_one(self, params)
    }

    /// Run a compile-checked `query!` expecting AT MOST one row.
    ///
    /// # Errors
    ///
    /// The backend's classified [`Error`](Self::Error), including a
    /// too-many-rows error, matching the concrete driver's `query_opt`.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    fn fetch_opt<'p, Q>(&mut self, params: Q::Params<'p>) -> Result<Option<Q::Owned>, Self::Error>
    where
        Self: Sized,
        Q: RunsOn<Self>,
    {
        Q::fetch_opt(self, params)
    }
}

/// A blocking SQL backend: a connection that also runs transactions.
///
/// Implemented by `pg_sync::Connection` and `sqlite::Connection`. The receiver
/// is `&mut self` (the stronger of the two drivers' requirements; SQLite's
/// interior-mutable `&self` satisfies it by reborrow).
pub trait SyncBackend: SyncQueries + Sized {
    /// The transaction guard handed to a [`transaction`](Self::transaction)
    /// body — itself a [`SyncQueries`] over the SAME error, so the body is
    /// generic over the backend too.
    type Tx<'t>: SyncQueries<Error = Self::Error>
    where
        Self: 't;

    /// Run `f` inside a transaction: `Ok` commits, `Err` rolls back. The guard
    /// exposes only the data verbs (no manual `commit`), so a desync is a
    /// compile error — the same closure-scoped safety the concrete drivers give.
    ///
    /// # Ergonomics of a GENERIC transaction body
    ///
    /// A generic body running the RAW-SQL verb ([`execute_sql`](SyncQueries::execute_sql))
    /// is CLEAN — no extra bound. A generic body running a TYPED
    /// [`fetch_all`](SyncQueries::fetch_all) / `fetch_one` / `fetch_opt` on the
    /// guard is the SCOPE LIMIT: it needs a higher-ranked bound over the guard's
    /// lifetime — `for<'t> Q: RunsOn<Self::Tx<'t>, ..>` — because [`Tx`](Self::Tx)
    /// is a distinct type per lifetime and [`RunsOn`] keys on the concrete
    /// receiver. That bound is provable when ASSUMED (a generic function
    /// definition type-checks), but the trait solver cannot INFER the backend
    /// through it at a call site (`E0277`, a known solver limitation — a turbofish
    /// works around it, but the HRTB bound is off the flagship's clean shape). So
    /// the recommended pattern keeps TYPED reads at connection level and uses the
    /// transaction for raw-SQL grouping (or a concrete backend for typed work
    /// inside a transaction).
    ///
    /// # Errors
    ///
    /// The backend's classified [`Error`](SyncQueries::Error) from `f` (rolled
    /// back) or from the BEGIN / COMMIT itself.
    fn transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error>;
}

/// Bridges a `query!` carrier to a concrete backend `B`: "carrier `Self` runs on
/// `B`, taking `Params`, decoding to `Owned`."
///
/// Keyed on `B` so `Params` / `Owned` are UNAMBIGUOUS — a carrier implements two
/// typed-query traits (`TypedQuery`, `SqliteTypedQuery`) that each declare
/// same-named associated types; routing through `RunsOn<B>` lets each backend's
/// blanket impl pick its own (the macro makes both the SAME concrete types). The
/// blanket impls (one per backend + its transaction guard) live in this crate,
/// so the orphan rule is satisfied by construction (the trait is local).
pub trait RunsOn<B: SyncQueries + ?Sized> {
    /// The carrier's typed parameter tuple on `B`, at the verb-argument
    /// lifetime `'p` (a `text`/`bytea` param borrows `&'p …`).
    type Params<'p>;
    /// The carrier's owned decoded record on `B`.
    type Owned;

    /// Collect owned records on `B`. Provided by the per-backend blanket impls.
    ///
    /// # Errors
    ///
    /// `B`'s classified error on a SQL / server / decode failure.
    // `track_caller` (under `n1-detect`) on the declaration AND every impl below,
    // so the N+1 detector's `Location::caller()` reaches the consumer through this
    // forwarder rather than stopping here.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    fn fetch_all<'p>(conn: &mut B, params: Self::Params<'p>) -> Result<Vec<Self::Owned>, B::Error>;

    /// Exactly one owned record on `B`.
    ///
    /// # Errors
    ///
    /// `B`'s classified error (incl. no-rows / too-many-rows).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    fn fetch_one<'p>(conn: &mut B, params: Self::Params<'p>) -> Result<Self::Owned, B::Error>;

    /// At most one owned record on `B`.
    ///
    /// # Errors
    ///
    /// `B`'s classified error (incl. too-many-rows).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    fn fetch_opt<'p>(conn: &mut B, params: Self::Params<'p>) -> Result<Option<Self::Owned>, B::Error>;
}

// ── PostgreSQL (blocking) impls ─────────────────────────────────────────────

#[cfg(feature = "postgres-sync")]
mod pg_impls {
    use super::{RunsOn, SyncBackend, SyncQueries};
    use bsql_postgres_sync::{Connection, DriverError, Transaction, TypedQuery};

    impl SyncQueries for Connection {
        type Error = DriverError;
        fn execute_sql(&mut self, sql: &str) -> Result<u64, Self::Error> {
            Connection::execute_sql(self, sql)
        }
    }

    impl SyncBackend for Connection {
        type Tx<'t> = Transaction<'t>;
        fn transaction<R>(
            &mut self,
            f: impl FnOnce(&mut Transaction<'_>) -> Result<R, DriverError>,
        ) -> Result<R, DriverError> {
            Connection::transaction(self, f)
        }
    }

    impl SyncQueries for Transaction<'_> {
        type Error = DriverError;
        fn execute_sql(&mut self, sql: &str) -> Result<u64, Self::Error> {
            Transaction::execute_sql(self, sql)
        }
    }

    /// One blanket body reused for the connection and the transaction guard —
    /// both expose the identical typed verbs, so the `$conn` receiver differs but
    /// the code does not.
    macro_rules! pg_runs_on {
        ($target:ty) => {
            impl<Q: TypedQuery> RunsOn<$target> for Q {
                type Params<'p> = <Q as TypedQuery>::Params<'p>;
                type Owned = <Q as TypedQuery>::Owned;
                #[cfg_attr(feature = "n1-detect", track_caller)]
                fn fetch_all<'p>(
                    conn: &mut $target,
                    params: Self::Params<'p>,
                ) -> Result<Vec<Self::Owned>, DriverError> {
                    Ok(conn.query::<Q>(params)?.into_owned()?)
                }
                #[cfg_attr(feature = "n1-detect", track_caller)]
                fn fetch_one<'p>(
                    conn: &mut $target,
                    params: Self::Params<'p>,
                ) -> Result<Self::Owned, DriverError> {
                    conn.query_one::<Q>(params)
                }
                #[cfg_attr(feature = "n1-detect", track_caller)]
                fn fetch_opt<'p>(
                    conn: &mut $target,
                    params: Self::Params<'p>,
                ) -> Result<Option<Self::Owned>, DriverError> {
                    conn.query_opt::<Q>(params)
                }
            }
        };
    }
    pg_runs_on!(Connection);
    pg_runs_on!(Transaction<'_>);
}

// ── SQLite (embedded, blocking) impls ───────────────────────────────────────

#[cfg(feature = "sqlite")]
mod sqlite_impls {
    use super::{RunsOn, SyncBackend, SyncQueries};
    use bsql_sqlite::{Connection, SqliteBindParams, SqliteError, SqliteTypedQuery, Transaction};

    /// The SQLite transaction-guard adapter. SQLite's `transaction` lends a
    /// SHARED `&Transaction` (interior mutability), but [`SyncBackend::transaction`](super::SyncBackend::transaction)
    /// hands the body `&mut Self::Tx` (the stronger, PostgreSQL-shaped receiver).
    /// This newtype bridges the two: a local the body borrows mutably, forwarding
    /// each verb to the shared guard — so ONE generic transaction body serves
    /// both backends despite the receiver difference.
    #[derive(Debug)]
    pub struct SqliteTx<'t>(pub(crate) &'t Transaction<'t>);

    impl SyncQueries for Connection {
        type Error = SqliteError;
        fn execute_sql(&mut self, sql: &str) -> Result<u64, Self::Error> {
            // SQLite's raw-SQL affected-count verb is `execute`; it takes `&self`,
            // satisfied by the `&mut self` reborrow.
            Connection::execute_sql(self, sql)
        }
    }

    impl SyncBackend for Connection {
        type Tx<'t> = SqliteTx<'t>;
        fn transaction<R>(
            &mut self,
            f: impl FnOnce(&mut SqliteTx<'_>) -> Result<R, SqliteError>,
        ) -> Result<R, SqliteError> {
            Connection::transaction(self, |tx| {
                let mut guard = SqliteTx(tx);
                f(&mut guard)
            })
        }
    }

    impl SyncQueries for SqliteTx<'_> {
        type Error = SqliteError;
        fn execute_sql(&mut self, sql: &str) -> Result<u64, Self::Error> {
            self.0.execute_sql(sql)
        }
    }

    /// One blanket body reused for the connection and the adapter — `$recv`
    /// resolves the receiver (`conn` vs the adapter's inner `conn.0`).
    macro_rules! sqlite_runs_on {
        ($target:ty, |$conn:ident| $recv:expr) => {
            impl<Q> RunsOn<$target> for Q
            where
                Q: SqliteTypedQuery,
                for<'p> <Q as SqliteTypedQuery>::Params<'p>: SqliteBindParams,
            {
                type Params<'p> = <Q as SqliteTypedQuery>::Params<'p>;
                type Owned = <Q as SqliteTypedQuery>::Owned;
                #[cfg_attr(feature = "n1-detect", track_caller)]
                fn fetch_all<'p>(
                    $conn: &mut $target,
                    params: Self::Params<'p>,
                ) -> Result<Vec<Self::Owned>, SqliteError> {
                    $recv.query::<Q>(params)?.into_owned()
                }
                #[cfg_attr(feature = "n1-detect", track_caller)]
                fn fetch_one<'p>(
                    $conn: &mut $target,
                    params: Self::Params<'p>,
                ) -> Result<Self::Owned, SqliteError> {
                    $recv.query_one::<Q>(params)
                }
                #[cfg_attr(feature = "n1-detect", track_caller)]
                fn fetch_opt<'p>(
                    $conn: &mut $target,
                    params: Self::Params<'p>,
                ) -> Result<Option<Self::Owned>, SqliteError> {
                    $recv.query_opt::<Q>(params)
                }
            }
        };
    }
    sqlite_runs_on!(Connection, |conn| conn);
    sqlite_runs_on!(SqliteTx<'_>, |conn| conn.0);
}

#[cfg(feature = "sqlite")]
pub use sqlite_impls::SqliteTx;
