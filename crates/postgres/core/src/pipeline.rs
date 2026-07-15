//! Heterogeneous atomic pipelining — [`Bound<Q>`] + the sealed [`Pipeline`] trait.
//!
//! `conn.pipeline((UserById::bind((7,)), InsertLog::bind((msg,)), …))` sends N
//! compile-checked `query!` commands with ONE trailing `Sync`, forming a SINGLE
//! implicit transaction. This is bsql's identity — PURE `query!` carriers, NO
//! runtime SQL builder: each tuple element is a carrier bound with its params,
//! not a fragment.
//!
//! # The airtight all-or-nothing contract (why the tuple, why no partial)
//!
//! N extended-query commands under one `Sync` are ONE implicit transaction (PG
//! §55.2.3 / §55.5): on a mid-batch error the commands BEFORE the failure are
//! ROLLED BACK, the failing one errors, and the ones AFTER are SKIPPED. So the
//! ONLY airtight result is all-or-nothing — the whole batch commits and returns
//! every result, or it errors and returns ZERO. Returning "the results before the
//! failure" would be WRONG (those writes were rolled back), so it is FORBIDDEN by
//! construction: [`Core::pipeline`](crate::Core::pipeline) builds the
//! [`Output`](Pipeline::Output) tuple ONLY after the pump reaches the batch's
//! clean trailing `ReadyForQuery`, which the server emits only if the whole
//! implicit transaction COMMITTED. A rolled-back / failing / skipped command can
//! never be materialised into an `Ok`.
//!
//! # Typed per element — no erasure
//!
//! Each element's rows decode against ITS carrier's compile-time OIDs into
//! [`Rows<Qi>`](crate::Rows) — no type erasure, no downcast. The result of an
//! arity-`k` batch is a `k`-tuple `(Rows<Q0>, Rows<Q1>, …)`, one typed container
//! per command, so the typed-row guarantee is preserved per element.
//!
//! # Arity cap
//!
//! [`Pipeline`] is SEALED with hand-written tuple impls for arity `1..=16` (the
//! same macro-of-impls shape [`ParamsWriter`](bsql_postgres_proto::ParamsWriter)
//! uses for its `0..=32` impls). Sixteen matches the result-column decode arity and
//! is ample for a heterogeneous batch; it is trivially expandable — add rows to the
//! `pipeline_impl!` invocation list below.

use std::io;
use std::marker::PhantomData;

use bsql_postgres_proto::engine::Transport;
use bsql_postgres_proto::TypedQuery;

use crate::driver::Core;
use crate::{DriverError, Rows, RowsBuilder};

/// A compile-checked `query!` carrier BOUND with its parameters — one element of a
/// [`Pipeline`] batch. NOT a runtime SQL fragment: it holds `Q::Params<'p>` plus a
/// phantom of the carrier `Q`, so a batch is a tuple of typed, pre-bound commands.
///
/// Build one with [`Bound::new`] or the ergonomic [`BindExt::bind`]
/// (`UserById::bind((7,))`). The `'p` is the shortest common borrow of the
/// parameters — a `text` / `bytea` param borrows `&'p str` / `&'p [u8]`, so a
/// RUNTIME `String` / buffer binds (the same GAT story as
/// [`TypedQuery::Params`]).
pub struct Bound<'p, Q: TypedQuery> {
    params: Q::Params<'p>,
    _q: PhantomData<fn() -> Q>,
}

impl<'p, Q: TypedQuery> Bound<'p, Q> {
    /// Bind a `query!` carrier `Q` with its parameter tuple.
    #[inline]
    #[must_use]
    pub fn new(params: Q::Params<'p>) -> Self {
        Self {
            params,
            _q: PhantomData,
        }
    }

    /// Borrow the bound parameters (the staging path serialises them onto the wire).
    #[inline]
    pub(crate) fn params(&self) -> &Q::Params<'p> {
        &self.params
    }
}

impl<Q: TypedQuery> core::fmt::Debug for Bound<'_, Q> {
    /// Never prints the bound parameter VALUES (no PII, and `Q::Params` need not be
    /// `Debug`) — only that this is a bound command.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Bound").finish_non_exhaustive()
    }
}

/// Ergonomic constructor for a [`Bound`] command: `UserById::bind((7,))`.
///
/// A blanket ext trait over every [`TypedQuery`] carrier — the macro emits the
/// carrier, this adds the `bind` associated fn without a macro change.
pub trait BindExt: TypedQuery + Sized {
    /// Bind this carrier with its parameter tuple for a [`Pipeline`] batch.
    #[inline]
    fn bind<'p>(params: Self::Params<'p>) -> Bound<'p, Self> {
        Bound::new(params)
    }
}

impl<Q: TypedQuery> BindExt for Q {}

mod sealed {
    /// Module-private seal: only the crate-internal tuple impls below satisfy
    /// [`Pipeline`](super::Pipeline). A downstream `impl Pipeline for MyType` is
    /// impossible — the batch shape is a closed set (arity `1..=16` of `Bound`s).
    pub trait Sealed {}
}

/// A heterogeneous atomic batch of bound `query!` commands — a tuple
/// `(Bound<Q0>, Bound<Q1>, …)` of arity `1..=16`, mapping to
/// [`Output`](Self::Output) `= (Rows<Q0>, Rows<Q1>, …)`.
///
/// Sealed (see [`Core::pipeline`](crate::Core::pipeline) for the airtight
/// all-or-nothing contract). The two `#[doc(hidden)]` methods are the driver-facing
/// staging + finishing seam a consumer never names.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `pipeline` batch",
    label = "expected a tuple of `1..=16` bound `query!` commands, e.g. `(UserById::bind((7,)), InsertLog::bind((msg,)))`",
    note = "each element must be a `Bound<Q>` — build one with `Q::bind(params)` (the `BindExt` ext trait over every `query!` carrier); a SINGLE command needs a trailing comma: `(cmd,)`, not `(cmd)`",
    note = "`Pipeline` is sealed — only the crate-internal tuple impls (arity 1..=16) of `Bound`s qualify; a downstream `impl Pipeline for ...` is forbidden by construction"
)]
pub trait Pipeline<'p>: sealed::Sealed {
    /// The result tuple — one [`Rows<Qi>`](crate::Rows) per command, in order.
    type Output;

    /// Number of commands in the batch (`1..=16`).
    const ARITY: usize;

    /// Stage the `i`-th command's request frames onto the engine — the PER-COMMAND
    /// staging cursor the windowed drive calls to interleave staging with window
    /// drains (command `0` with `first = true` resets the buffer + seats pipeline
    /// mode), pushing the command's content-addressed statement name onto `plan`
    /// for the cache settle. The driver invokes it for `i` in `0..ARITY`; an
    /// out-of-range `i` is a fail-closed classified [`DriverError`], never reached.
    ///
    /// A monolithic "stage all" would preclude the windowed batcher (constant send
    /// memory, deadlock-free), so staging is a cursor: [`Core::pipeline`](crate::Core::pipeline)
    /// stages one command, checks the send-buffer high-water, and flushes+drains a
    /// window before staging the next — exactly as `execute_batch` streams its
    /// parameter sets.
    #[doc(hidden)]
    fn stage_nth<S: Transport<Error = io::Error>>(
        &self,
        core: &mut Core<S>,
        i: usize,
        plan: &mut Vec<&'static str>,
    ) -> Result<(), DriverError>;

    /// Consume the per-command row prebuffers (one [`RowsBuilder`] per command, in
    /// order) into the typed [`Output`](Self::Output) tuple, stamping each `Qi`.
    #[doc(hidden)]
    fn finish(builders: Vec<RowsBuilder>) -> Result<Self::Output, DriverError>;
}

/// Generate the sealed [`Pipeline`] impl for one arity: the `Bound`-tuple → `Rows`-
/// tuple mapping, the per-element staging (element `0` is `first = true`), and the
/// in-order finish. Mirrors `params::params_writer_impl!`.
macro_rules! pipeline_impl {
    ($count:literal; $($q:ident : $idx:tt : $first:literal),+ $(,)?) => {
        impl<'p, $($q,)+> sealed::Sealed for ($(Bound<'p, $q>,)+)
        where
            $($q: TypedQuery,)+
        {}

        impl<'p, $($q,)+> Pipeline<'p> for ($(Bound<'p, $q>,)+)
        where
            $($q: TypedQuery,)+
        {
            type Output = ($(Rows<$q>,)+);
            const ARITY: usize = $count;

            #[inline]
            fn stage_nth<S: Transport<Error = io::Error>>(
                &self,
                core: &mut Core<S>,
                i: usize,
                plan: &mut Vec<&'static str>,
            ) -> Result<(), DriverError> {
                match i {
                    $(
                        $idx => core.stage_pipeline_cmd::<$q>(&self.$idx, $first, plan),
                    )+
                    // The driver stages `i` in `0..ARITY`, so this arm is
                    // unreachable; classified fail-closed (never an index panic),
                    // the sanctioned dead-arm shape the `finish` `ok_or` uses.
                    _ => Err(DriverError::UnclassifiedFailure),
                }
            }

            #[inline]
            fn finish(builders: Vec<RowsBuilder>) -> Result<Self::Output, DriverError> {
                // The collector produces exactly `ARITY` builders on the success
                // path (one per command, in order); the `ok_or` arm is fail-closed
                // (classified, never an index panic) against an impossible short Vec.
                let mut it = builders.into_iter();
                Ok((
                    $(
                        it.next().ok_or(DriverError::UnclassifiedFailure)?.finish::<$q>(),
                    )+
                ))
            }
        }
    };
}

pipeline_impl!(1; Q0:0:true);
pipeline_impl!(2; Q0:0:true, Q1:1:false);
pipeline_impl!(3; Q0:0:true, Q1:1:false, Q2:2:false);
pipeline_impl!(4; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false);
pipeline_impl!(5; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false);
pipeline_impl!(6; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false);
pipeline_impl!(7; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false);
pipeline_impl!(8; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false);
pipeline_impl!(9; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false);
pipeline_impl!(10; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false);
pipeline_impl!(11; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false, Q10:10:false);
pipeline_impl!(12; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false, Q10:10:false, Q11:11:false);
pipeline_impl!(13; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false, Q10:10:false, Q11:11:false, Q12:12:false);
pipeline_impl!(14; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false, Q10:10:false, Q11:11:false, Q12:12:false, Q13:13:false);
pipeline_impl!(15; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false, Q10:10:false, Q11:11:false, Q12:12:false, Q13:13:false, Q14:14:false);
pipeline_impl!(16; Q0:0:true, Q1:1:false, Q2:2:false, Q3:3:false, Q4:4:false, Q5:5:false, Q6:6:false, Q7:7:false, Q8:8:false, Q9:9:false, Q10:10:false, Q11:11:false, Q12:12:false, Q13:13:false, Q14:14:false, Q15:15:false);
