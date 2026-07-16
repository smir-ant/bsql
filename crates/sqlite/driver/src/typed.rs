//! Typed runtime for the compile-checked `query!` flagship over SQLite.
//!
//! This is the SQLite half of the flagship's execution surface. The build-time
//! half (schema catalog + real-SQLite conformance cross-check) already ships; a
//! `query!(Foo, "<SQL>")` emits the OWNED record `Foo` — which is ITSELF the
//! carrier — plus the borrowed VIEW `FooRef<'q>` for a borrowing query, and —
//! when the SQLite runtime is enabled AND the query is SQLite-decodable — a
//! [`SqliteTypedQuery`] impl on `Foo`. A driver's typed verbs
//! (`Connection::query::<Foo>` and friends) run it and decode into the typed
//! records — the SAME one-name surface as the PostgreSQL bridge.
//!
//! # SQLite reality: verify, never coerce
//!
//! SQLite is dynamically typed — the storage class rides the *value*, not the
//! column — so the typed decode VERIFIES the value's actual storage class
//! against the record's declared field type ([`crate::FromColumn`]): a `TEXT`
//! arriving where the catalog declared `INTEGER` is a classified
//! [`SqliteError::TypeMismatch`](crate::SqliteError::TypeMismatch), never a
//! silent coercion; a `NULL` in a non-`Option` field is
//! [`SqliteError::UnexpectedNull`](crate::SqliteError::UnexpectedNull). This is
//! the runtime peer of the PostgreSQL path's compile-time OID pinning: there the
//! column type is fixed on the wire, here it is checked per value.

use crate::error::SqliteError;
use crate::value::{typed_get, typed_get_opt, FromColumn, ValueRef};

/// A per-row column source the typed decode reads through — a lightweight VIEW,
/// never a stored value.
///
/// Implemented by BOTH the eager arena row (borrowing the shared result arena)
/// and the streaming [`BorrowedRow`](crate::BorrowedRow) (borrowing SQLite's own
/// column buffer), so the ONE macro-emitted [`SqliteTypedQuery::decode_row`]
/// serves the eager and the streaming typed paths identically.
///
/// The lifetime `'a` is the source's underlying storage borrow: [`cell`](Self::cell)
/// returns a [`ValueRef<'a>`] whose `Text`/`Blob` bytes alias that storage
/// zero-copy, so a borrowed typed record (`&'a str` fields) borrows the
/// CONTAINER (the result arena / the row buffer), not a transient per-row
/// handle. That is what lets a typed borrowed record outlive the view the decode
/// read it through.
pub trait ColumnSource<'a> {
    /// The borrowed value of column `col`, or
    /// [`SqliteError::ColumnIndexOutOfBounds`](crate::SqliteError::ColumnIndexOutOfBounds)
    /// if `col` is past the row.
    fn cell(&self, col: usize) -> Result<ValueRef<'a>, SqliteError>;
    /// The number of columns the row carries.
    fn column_count(&self) -> usize;
}

/// The compile-checked `query!` carrier's SQLite execution bridge.
///
/// The `query!` macro emits an impl of this on the query's carrier — the record
/// `Foo` itself — WHENEVER the SQLite runtime is enabled (the umbrella `sqlite`
/// feature) AND the query is SQLite-decodable — every projected column is a
/// SQLite storage class (INTEGER / REAL / TEXT / BLOB), unbridged, and the query
/// uses no PostgreSQL-only dynamic sugar (`OPTIONAL(...)`, `= ANY(...)`, a
/// runtime `ORDER BY` allow-set). A carrier that does NOT implement it makes a
/// `sqlite_conn.query::<That>()` a LOCATED compile error at the call site —
/// never a silent runtime mis-decode.
///
/// # Why not the PostgreSQL `TypedQuery`
///
/// The PostgreSQL `TypedQuery` decodes a raw `DataRow` byte payload at const
/// offsets validated against wire OIDs — a wire model SQLite does not share
/// (rusqlite hands back native storage-class values, not a `DataRow`). So the
/// carrier implements BOTH traits over the SAME record twins: `TypedQuery` for
/// the PostgreSQL wire, `SqliteTypedQuery` for the SQLite value model.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a SQLite-runnable `query!` carrier",
    note = "the SQLite typed runtime decodes only a query whose every projected column is a SQLite storage class (INTEGER/REAL/TEXT/BLOB), is unbridged, and which uses no PostgreSQL-only dynamic sugar (OPTIONAL, = ANY, runtime ORDER BY allow-sets); such a query is PostgreSQL-only"
)]
pub trait SqliteTypedQuery {
    /// The `$N` parameter tuple — the SAME tuple type the PostgreSQL
    /// [`TypedQuery::Params`](https://docs.rs/bsql) uses for this carrier (the
    /// macro emits both from one source), so a `query!` runs on either backend
    /// with the SAME typed parameters.
    ///
    /// Deliberately UNBOUNDED here: a carrier whose parameters are not all
    /// SQLite-bindable (a `u64`, or a PostgreSQL-only type) still implements this
    /// trait — the [`SqliteBindParams`](crate::SqliteBindParams) requirement lives
    /// on the driver's `query::<Q>` verb, so such a carrier is a LOCATED compile
    /// error at the call site, never an impl-site break that would fail a
    /// PostgreSQL-only build.
    ///
    /// A lifetime GAT (matching `TypedQuery::Params<'p>`): a `text` / `bytea`
    /// parameter is a borrow (`&'p str` / `&'p [u8]`), so a typed verb accepts
    /// `Params<'p>` for the caller's `'p` — a RUNTIME `&str` binds, not only a
    /// `&'static` literal. A scalar / param-free query is `'p`-invariant.
    type Params<'p>;
    /// The borrowed record at lifetime `'q` — the macro's `FooRef<'q>` (text
    /// columns are `&'q str`, aliasing the source) or `Foo` for an all-scalar
    /// row (the `'q` is then unused).
    type Record<'q>;
    /// The owned record (the macro's `Foo` — the carrier itself; text is
    /// `String`), `'static` so a decoded row outlives the result arena.
    type Owned: 'static;
    /// The SQLite-preparable SQL text — the portable form with `$N` positional
    /// parameters (which SQLite binds by index), baked at expansion.
    const SQL: &'static str;
    /// Decode one row (from any [`ColumnSource`]) into the borrowed record,
    /// classifying any storage-class mismatch or unexpected NULL.
    ///
    /// # Errors
    ///
    /// A [`SqliteError`] when a value's storage class does not match the
    /// declared field type, or a `NULL` lands in a non-`Option` field.
    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError>;
    /// Decode one row into the owned twin (text/blob copied out).
    ///
    /// # Errors
    ///
    /// As [`decode_row`](Self::decode_row).
    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError>;
}

/// Read a NOT-NULL typed column, classifying any failure. The macro-emitted
/// [`SqliteTypedQuery::decode_row`] calls this per non-nullable field.
///
/// A real `NULL` is the classified
/// [`SqliteError::UnexpectedNull`](crate::SqliteError::UnexpectedNull); a
/// storage-class mismatch is
/// [`SqliteError::TypeMismatch`](crate::SqliteError::TypeMismatch).
///
/// # Errors
///
/// Propagates the classified read error described above.
pub fn read_required<'a, T: FromColumn<'a>, S: ColumnSource<'a>>(
    src: &S,
    col: usize,
) -> Result<T, SqliteError> {
    typed_get(col, src.cell(col)?)
}

/// Read a nullable typed column, classifying any failure. The macro-emitted
/// [`SqliteTypedQuery::decode_row`] calls this per nullable field.
///
/// A real `NULL` is `Ok(None)` (distinct from a storage-class mismatch, which is
/// `Err`); a present value of the right type is `Ok(Some(_))`.
///
/// # Errors
///
/// A [`SqliteError::TypeMismatch`](crate::SqliteError::TypeMismatch) on a
/// storage-class mismatch (a real `NULL` is `Ok(None)`, never an error).
pub fn read_optional<'a, T: FromColumn<'a>, S: ColumnSource<'a>>(
    src: &S,
    col: usize,
) -> Result<Option<T>, SqliteError> {
    typed_get_opt(col, src.cell(col)?)
}
