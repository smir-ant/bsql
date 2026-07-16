//! [`TypedQuery`] — the bridge from a compile-checked `query!` artifact to
//! its execution over the engine and back to its TYPED records.
//!
//! A `query!(Foo, "<SQL>")` invocation emits, in the consumer crate, the OWNED
//! record `Foo` (text columns are `String`) — which is ITSELF the carrier: it
//! carries the const wire artifact (its
//! [`QueryFingerprint`](crate::QueryFingerprint) impl + the validated
//! [`PreparedQuery`] minted through the proto-owned `run` boundary) AND
//! implements [`TypedQuery`]. A borrowing query additionally emits the zero-copy
//! borrowed VIEW `FooRef<'q>` (text columns are `&'q str`, so the borrowed decode
//! allocates nothing); an all-scalar query has no borrowed twin (`Foo` self-owns
//! and serves both roles). So there is ONE user-facing name `Foo`:
//! `conn.query::<Foo>(params)` runs it (only a lifetime-free type can be a
//! `TypedQuery` carrier, and only the owned record is lifetime-free — a borrowed
//! `FooRef<'q>` carries a lifetime and cannot). [`TypedQuery`] ties the artifacts
//! together into one trait a driver can monomorphise over: given `Q::PREPARED` it
//! runs the query, and given
//! [`Q::decode_borrowed`](TypedQuery::decode_borrowed) /
//! [`Q::decode_owned`](TypedQuery::decode_owned) it turns each raw `DataRow`
//! payload into the record.
//!
//! # The canonical typed-row story
//!
//! The owned [`Q::Owned`](TypedQuery::Owned) (= the carrier `Foo` itself) is the
//! canonical row a consumer names in a signature — `'static + Send`, so a row
//! decoded from it outlives the prebuffer and crosses a task boundary;
//! `query_one` / `into_owned` return it. The borrowed
//! [`Q::Record<'q>`](TypedQuery::Record) (`FooRef<'q>`) is the ZERO-COPY view
//! served from an owned prebuffer the driver collects (so the borrow is into a
//! buffer the caller owns, not the transient engine ingest buffer) by
//! `Rows::iter` / `query_each`; a per-row decode failure is a `Result` *item*,
//! never a connection-killing fault. There is no type-erased row on this path —
//! the column types are pinned at compile time by the macro.
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
/// Implemented by the macro for each `query!` carrier — the record `Foo` itself
/// for a plain query (so `query::<Foo>()` runs it), or a separate `Foo…Query`
/// marker per ordering for a runtime `ORDER BY` query. A driver's
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
///   text column it is `FooRef<'q>` (the `'q` borrows the prebuffer); for a query
///   with no text column it is `Foo` (the `'q` is harmlessly unused).
/// - [`Owned`](Self::Owned) — the owned record `Foo` itself (`= Self` for a plain
///   query), `Send + 'static` so a row can outlive the prebuffer.
///
/// # Misuse diagnostic
///
/// For a PLAIN `query!(Foo, "…")` the record `Foo` IS the carrier, so
/// `conn.query::<Foo>()` is CORRECT — the former "record vs `FooQuery` carrier"
/// footgun is now unrepresentable. The remaining unsatisfied cases are: a type
/// that is not a `query!` record at all; the borrowed VIEW `FooRef` (a decoded
/// row's borrow, not runnable); and a runtime `ORDER BY { … }` query's record
/// (each ordering is a separate `Foo…Query` carrier picked via the `FooOrderBy`
/// selector — one `Foo` cannot carry N orderings' distinct prepared plans). The
/// `#[diagnostic::on_unimplemented]` below names those fixes in the query author's
/// own vocabulary rather than a raw "`Foo: TypedQuery` is not satisfied" wall —
/// the PostgreSQL peer of the SQLite driver's `SqliteTypedQuery` message.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a runnable `query!` carrier",
    label = "not a runnable `query!` query",
    note = "run a compile-checked query by turbofishing the type `query!` names: `query!(Foo, \"…\")` makes `Foo` runnable via `query` / `query_one` / `query_opt`. A `FooRef` is the borrowed row VIEW (it holds a decoded row's borrow), not a runnable query. A runtime `ORDER BY {{ … }}` query is run per-ordering through its `Foo…Query` carriers — pick one via the `FooOrderBy` selector."
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
    /// The borrowed record at lifetime `'q` (the macro's `FooRef<'q>`; or `Foo`
    /// with `'q` unused for a query that projects no text column). Text columns
    /// borrow the prebuffer as `&'q str`, so the borrowed decode allocates
    /// nothing.
    type Record<'q>;
    /// The owned record (the macro's `Foo` — the carrier itself for a plain
    /// query). `Send + 'static` so a row decoded from it outlives the prebuffer
    /// and crosses a task boundary.
    type Owned: Send + 'static;

    /// The validated, content-addressed prepared query — exactly
    /// `Foo::PREPARED` (or `Foo…Query::PREPARED` for a runtime-`ORDER BY`
    /// ordering), minted at compile time through the proto-owned
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
