//! [`TypedQuery`] — the bridge from a compile-checked `query!` artifact to
//! its execution over the engine and back to its TYPED records.
//!
//! A `query!(Foo, "<SQL>")` invocation emits, in the consumer crate, an
//! uninhabited carrier `FooQuery` carrying the const wire artifact (its
//! [`QueryFingerprint`](crate::QueryFingerprint) impl + the validated
//! [`PreparedQuery`] minted through the proto-owned `run` boundary), plus two
//! typed records — a borrowed `Foo<'q>` (text columns are `&'q str`, so the
//! borrowed decode allocates nothing) and an owned twin `FooOwned` (text is
//! `String`). [`TypedQuery`] ties those four artifacts together into one trait
//! a driver can monomorphise over: given `Q::PREPARED` it runs the query, and
//! given [`Q::decode_borrowed`](TypedQuery::decode_borrowed) /
//! [`Q::decode_owned`](TypedQuery::decode_owned) it turns each raw `DataRow`
//! payload into the record.
//!
//! # The canonical typed-row story
//!
//! The borrowed record [`Q::Record<'q>`](TypedQuery::Record) is the canonical
//! result row: it is served from an owned prebuffer the driver collects (so the
//! borrow is into a buffer the caller owns, not the transient engine ingest
//! buffer), and a per-row decode failure is a `Result` *item*, never a
//! connection-killing fault. The owned [`Q::Owned`](TypedQuery::Owned) is the
//! explicit `'static + Send` escape for a row that must outlive the prebuffer.
//! There is no type-erased row on this path — the column types are pinned at
//! compile time by the macro.
//!
//! # Why this trait is NOT sealed
//!
//! Mirrors [`QueryFingerprint`](crate::QueryFingerprint): the carrier and its impl are emitted in the
//! consumer crate, so a seal would be unsatisfiable from there (and a
//! re-exported seal token would be hand-reachable — deflection, not
//! enforcement). The load-bearing guarantee is not the openness of this trait
//! but the const validator behind [`PreparedQuery`]: the only way to obtain a
//! `PreparedQuery` for [`PREPARED`](TypedQuery::PREPARED) is through the
//! validating `run` boundary, so even a hand-written `TypedQuery` impl cannot
//! mint a query whose wire bytes lie about their declared shape.

use crate::decode::DecodeError;
use crate::params::ParamsWriter;
use crate::prepared::{PreparedQuery, RowDecode};

/// Ties a compile-checked `query!` carrier to its prepared query and its typed
/// record decoders.
///
/// Implemented by the macro for each `query!` carrier (`FooQuery`). A driver's
/// typed `query` method is generic over `Q: TypedQuery`: it runs
/// [`Q::PREPARED`](Self::PREPARED) over the engine, collects each `DataRow`
/// payload into an owned prebuffer, and later decodes rows lazily through
/// [`decode_borrowed`](Self::decode_borrowed) (zero-copy borrowed record) or
/// [`decode_owned`](Self::decode_owned) (owned twin).
///
/// # Associated items
///
/// - [`Params`](Self::Params) / [`Row`](Self::Row) — the parameter and row
///   tuple marker types the macro pins, carrying the wire OIDs / formats. They
///   are the exact `P` / `R` of [`PREPARED`](Self::PREPARED).
/// - [`Record<'q>`](Self::Record) — the borrowed record GAT. For a query with a
///   text column it is `Foo<'q>` (the `'q` borrows the prebuffer); for a query
///   with no text column it is `Foo` (the `'q` is harmlessly unused).
/// - [`Owned`](Self::Owned) — the owned twin `FooOwned`, `Send + 'static` so a
///   row can outlive the prebuffer.
///
/// # Misuse diagnostic
///
/// The single most common `query!` mistake is passing the generated RECORD type
/// (`Foo`) where a runnable CARRIER (`FooQuery`) is required — `conn.query::<Foo>()`
/// instead of `conn.query::<FooQuery>()`. The `#[diagnostic::on_unimplemented]`
/// below names that fix in the query author's own vocabulary (use the `…Query`
/// carrier; the bare record holds a decoded row and is not runnable) rather than a
/// raw "`Foo: TypedQuery` is not satisfied" wall — the PostgreSQL peer of the
/// SQLite driver's `SqliteTypedQuery` on-unimplemented message.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a runnable `query!` carrier",
    label = "not a `query!` carrier",
    note = "run a compile-checked query through the CARRIER the `query!` macro emits: for `query!(Foo, \"…\")` that is `FooQuery` — pass it to `query` / `query_one` / `query_opt`. The bare `Foo` is the decoded-row RECORD type (it holds a row's values), not a runnable query."
)]
pub trait TypedQuery {
    /// The parameter tuple marker — the `$N` Rust types, supplying the wire
    /// param OIDs / formats.
    ///
    /// Bounded on [`ParamsWriter`] only — NOT `Copy`. A param can be a non-`Copy`
    /// owned value (a `Numeric`, whose arbitrary-precision digit payload is
    /// heap-backed; a `Json` / `Jsonb` string), and the typed execution path
    /// moves the whole tuple into the engine by value and serialises it once
    /// through [`ParamsWriter::write_params`] — it is never copied or re-bound,
    /// so `Copy` was unnecessary and would exclude these owned params. The
    /// runtime-SQL `query_prepared` / `query_params` / `execute_params` escape
    /// hatch mirrors this: it borrows the param tuple all the way to the engine
    /// (`&P`), so it too binds a non-`Copy` owned param — the two paths are
    /// symmetric, with no `Copy`-only asymmetry between them.
    ///
    /// # Why this is a lifetime GAT
    ///
    /// A `text` / `bytea` parameter is a BORROW (`&'p str` / `&'p [u8]`), so the
    /// parameter tuple is lifetime-POLYMORPHIC: a typed verb accepts `Params<'p>`
    /// for the caller's `'p`, letting a RUNTIME `&str` bind (not only a `&'static`
    /// literal). The lifetime never reaches the wire — the OIDs / formats are
    /// lifetime-invariant, and the const validator uses the `'static`
    /// instantiation ([`PREPARED`](Self::PREPARED) is `PreparedQuery<Self::Params<'static>,
    /// _>`), so the compile-time OID pin is unchanged. A scalar / param-free query
    /// makes `Params<'p>` `'p`-invariant (`(i64,)` / `()`).
    type Params<'p>: ParamsWriter;
    /// The row tuple marker — the projected column Rust types, supplying the
    /// wire row OIDs.
    type Row: RowDecode;
    /// The borrowed record at lifetime `'q` (the macro's `Foo<'q>`; or `Foo`
    /// with `'q` unused for a query that projects no text column). Text columns
    /// borrow the prebuffer as `&'q str`, so the borrowed decode allocates
    /// nothing.
    type Record<'q>;
    /// The owned record twin (the macro's `FooOwned`). `Send + 'static` so a
    /// row decoded from it outlives the prebuffer and crosses a task boundary.
    type Owned: Send + 'static;

    /// The validated, content-addressed prepared query — exactly
    /// `FooQuery::PREPARED`, minted at compile time through the proto-owned
    /// `run` boundary. Its wire bytes are const-checked against
    /// [`Params`](Self::Params) / [`Row`](Self::Row); a drift is a build error.
    ///
    /// Uses the `'static` instantiation of the parameter GAT — the OIDs the
    /// validator pins are lifetime-invariant, so this is byte-identical to the
    /// pre-GAT `PreparedQuery<Self::Params, Self::Row>`.
    const PREPARED: PreparedQuery<Self::Params<'static>, Self::Row>;

    /// Decode one raw `DataRow` payload (the wire bytes beginning with the
    /// 2-byte column-count header) into the borrowed record. Text columns alias
    /// `body` — zero allocation.
    ///
    /// # Errors
    ///
    /// A [`DecodeError`] when the row body does not match the query's
    /// compile-time column shape (a NULL in a NOT-NULL column, a wrong binary
    /// width, a truncated / oversized body) — never a silent default or panic.
    fn decode_borrowed(body: &[u8]) -> Result<Self::Record<'_>, DecodeError>;

    /// Decode one raw `DataRow` payload into the owned record twin (text
    /// columns copied into `String`).
    ///
    /// # Errors
    ///
    /// As [`decode_borrowed`](Self::decode_borrowed).
    fn decode_owned(body: &[u8]) -> Result<Self::Owned, DecodeError>;
}
