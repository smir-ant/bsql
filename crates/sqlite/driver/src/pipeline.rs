//! Heterogeneous atomic pipelining — the SQLite SEQUENTIAL twin of the PostgreSQL
//! `pipeline`.
//!
//! `conn.pipeline((UserById::bind((7,)), OrderById::bind((9,)), …))` runs N
//! compile-checked `query!` commands SEQUENTIALLY inside ONE transaction, yielding
//! the typed tuple `(TypedRows<Q0>, TypedRows<Q1>, …)` and the all-or-nothing
//! contract: a mid-batch failure rolls back the WHOLE transaction and returns ZERO
//! results. SQLite is IN-PROCESS, so there is no round-trip win — the value is ONE
//! mental model + transaction atomicity across the batch.
//!
//! # SQLite typed pipelines are READ-ONLY under a conformance build
//!
//! The atomicity here is READ-consistency across the batch's SELECTs, NOT a
//! write-batch guarantee. Under the blessed dual-target build (`sqlite` +
//! `macros-sqlite`), the SQLite conformance oracle validates every typed `query!`
//! under a READONLY authorizer, so a typed WRITE (`INSERT`/`UPDATE`/`DELETE`)
//! carrier is REJECTED at its `query!` definition site — and SQLite exposes no
//! typed `execute::<Q>` verb — so a SQLite pipeline element is always a SELECT.
//! (The PostgreSQL `pipeline` DOES type writes, so its batch is genuinely a write
//! batch; that is the one place the two backends' pipelines differ. A write-bearing
//! example like `InsertLog::bind(..)` therefore belongs on the PostgreSQL side,
//! not here.)
//!
//! # Why a parallel trait, not the PostgreSQL one
//!
//! The embedded SQLite crate keeps its zero-`bsql-postgres-core` boundary, so it
//! cannot name the PostgreSQL `TypedQuery` the
//! core `Pipeline` is built on — a SQLite carrier implements
//! [`SqliteTypedQuery`](crate::SqliteTypedQuery). So the batch types are a
//! STRUCTURAL twin here (`Bound` / [`SqlitePipeline`] over `SqliteTypedQuery`), with
//! the SAME `conn.pipeline((Q::bind(..), …))` surface. This mirrors the existing
//! per-backend divergence (`bsql_postgres_core::Rows` vs
//! [`TypedRows`](crate::TypedRows) for a single query).
//!
//! # Airtight all-or-nothing (structural)
//!
//! The batch runs inside [`Connection::transaction`](crate::Connection::transaction):
//! each command is a `query::<Qi>` in sequence, and the FIRST error short-circuits
//! the closure, which drives the guard's ROLLBACK — the whole transaction is undone,
//! so a command before the failure is rolled back and the `Ok` tuple is built ONLY
//! when every command succeeded and the transaction COMMITTED.

use std::marker::PhantomData;

use crate::bind::SqliteBindParams;
use crate::connection::{Transaction, TypedRows};
use crate::error::SqliteError;
use crate::typed::SqliteTypedQuery;

/// A compile-checked `query!` carrier BOUND with its parameters — one element of a
/// SQLite [`SqlitePipeline`] batch (the SQLite twin of
/// the PostgreSQL `bsql_postgres_core::Bound`). Holds `Q::Params<'p>` plus a phantom of the
/// carrier `Q`.
pub struct Bound<'p, Q: SqliteTypedQuery> {
    params: Q::Params<'p>,
    _q: PhantomData<fn() -> Q>,
}

impl<'p, Q: SqliteTypedQuery> Bound<'p, Q> {
    /// Bind a `query!` carrier `Q` with its parameter tuple.
    #[inline]
    #[must_use]
    pub fn new(params: Q::Params<'p>) -> Self {
        Self {
            params,
            _q: PhantomData,
        }
    }

    /// Consume the bound, yielding its parameters (moved into the `query` verb).
    #[inline]
    fn into_params(self) -> Q::Params<'p> {
        self.params
    }
}

impl<Q: SqliteTypedQuery> core::fmt::Debug for Bound<'_, Q> {
    /// Never prints the bound parameter VALUES.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Bound").finish_non_exhaustive()
    }
}

/// Ergonomic constructor for a SQLite [`Bound`] command: `UserById::bind((7,))`.
///
/// A blanket ext trait over every [`SqliteTypedQuery`] carrier.
pub trait BindExt: SqliteTypedQuery + Sized {
    /// Bind this carrier with its parameter tuple for a SQLite [`SqlitePipeline`].
    #[inline]
    fn bind<'p>(params: Self::Params<'p>) -> Bound<'p, Self> {
        Bound::new(params)
    }
}

impl<Q: SqliteTypedQuery> BindExt for Q {}

mod sealed {
    /// Module-private seal: only the crate-internal tuple impls satisfy
    /// [`SqlitePipeline`](super::SqlitePipeline).
    pub trait Sealed {}
}

/// A heterogeneous atomic batch of bound SQLite `query!` commands — a tuple
/// `(Bound<Q0>, Bound<Q1>, …)` of arity `1..=16`, mapping to
/// [`Output`](Self::Output) `= (TypedRows<Q0>, TypedRows<Q1>, …)`. Run it via
/// [`Connection::pipeline`](crate::Connection::pipeline). Sealed.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a SQLite `pipeline` batch",
    label = "expected a tuple of `1..=16` bound `query!` commands, e.g. `(UserById::bind((7,)), OrderById::bind((9,)))`",
    note = "each element must be a `Bound<Q>` — build one with `Q::bind(params)` (the `BindExt` ext trait over every SQLite `query!` carrier); a SINGLE command needs a trailing comma: `(cmd,)`, not `(cmd)`",
    note = "`SqlitePipeline` is sealed — only the crate-internal tuple impls (arity 1..=16) of `Bound`s qualify; a downstream `impl SqlitePipeline for ...` is forbidden by construction"
)]
pub trait SqlitePipeline<'p>: sealed::Sealed {
    /// The result tuple — one [`TypedRows<Qi>`](crate::TypedRows) per command.
    type Output;

    /// Run every command sequentially through the transaction guard, collecting the
    /// typed results. The FIRST error short-circuits (`?`), so the guard rolls the
    /// WHOLE transaction back — the all-or-nothing contract.
    #[doc(hidden)]
    fn run(self, tx: &Transaction<'_>) -> Result<Self::Output, SqliteError>;
}

/// Generate the sealed [`SqlitePipeline`] impl for one arity.
macro_rules! sqlite_pipeline_impl {
    ($($q:ident : $b:ident : $idx:tt),+ $(,)?) => {
        impl<'p, $($q,)+> sealed::Sealed for ($(Bound<'p, $q>,)+)
        where
            $($q: SqliteTypedQuery,)+
        {}

        impl<'p, $($q,)+> SqlitePipeline<'p> for ($(Bound<'p, $q>,)+)
        where
            $($q: SqliteTypedQuery,)+
            // Each command binds through the typed `query` verb, which requires the
            // carrier's param tuple be SQLite-bindable — a PG-only param type (a
            // `u64` / `Uuid` / temporal) is a LOCATED compile error at the batch call.
            $($q::Params<'p>: SqliteBindParams,)+
        {
            type Output = ($(TypedRows<$q>,)+);

            #[inline]
            fn run(self, tx: &Transaction<'_>) -> Result<Self::Output, SqliteError> {
                let ($($b,)+) = self;
                // Left-to-right: each `?` short-circuits into the guard's ROLLBACK.
                Ok(($(tx.query::<$q>($b.into_params())?,)+))
            }
        }
    };
}

sqlite_pipeline_impl!(Q0:b0:0);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9, Q10:b10:10);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9, Q10:b10:10, Q11:b11:11);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9, Q10:b10:10, Q11:b11:11, Q12:b12:12);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9, Q10:b10:10, Q11:b11:11, Q12:b12:12, Q13:b13:13);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9, Q10:b10:10, Q11:b11:11, Q12:b12:12, Q13:b13:13, Q14:b14:14);
sqlite_pipeline_impl!(Q0:b0:0, Q1:b1:1, Q2:b2:2, Q3:b3:3, Q4:b4:4, Q5:b5:5, Q6:b6:6, Q7:b7:7, Q8:b8:8, Q9:b9:9, Q10:b10:10, Q11:b11:11, Q12:b12:12, Q13:b13:13, Q14:b14:14, Q15:b15:15);
