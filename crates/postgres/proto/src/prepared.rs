//! `PreparedQuery<P, R>` runtime type + `RowDecode` trait.
//!
//! This module hosts the runtime artefacts the compile-checked `query!`
//! macro produces struct literals of:
//!
//! - [`PreparedQuery<Params, Row>`] — content-addressed prepared SQL
//!   query with type-level parameter and row-shape pinning.
//! - [`RowDecode`] — sealed trait for tuple-arity row decoding.
//! - `new_prepared_query<P, R>(...)` — the only constructor for
//!   `PreparedQuery`, and a VALIDATING one: it cross-checks every wire
//!   argument against the declared `P` / `R` type-level shape and fails
//!   const-evaluation (`error[E0080]`) on any drift. There is no
//!   unchecked twin. Combined with the struct's `pub(crate)` fields
//!   (direct literal construction from outside fails `error[E0451]`),
//!   a fabricated artifact that lies about its shape cannot compile.
//! - [`QueryFingerprint`] + [`run`] — the uninhabited-carrier path. A
//!   query macro emits a zero-size carrier type plus its wire artifact
//!   as a `QueryFingerprint` impl; `run::<Q>()` is the only way to mint
//!   the `PreparedQuery`, forcing the validating constructor.
//!
//! # Tier-1 SQL injection closure
//!
//! The hostile-probe matrix for SQL injection closes tier-1 inside
//! the language for the in-source probes: private fields, sealed
//! trait, content-addressed stmt_name. The remaining out-of-language
//! probes are OS-level boundaries where `forbid(unsafe_code)` in
//! this crate ends and the user's `unsafe` contract or `.rodata`
//! memory protection begins.
//!
//! # Wire format — binary-uniform
//!
//! Params AND results are uniformly PostgreSQL binary on this path.
//! The macro's baked `bind_execute_prefix` carries ONLY the portal NUL
//! and stmt-name NUL — it bakes NO format/count bytes. The param
//! format-code block, `n_params`, the param values, and the
//! `n_result_formats = 1, [Binary]` result trailer are emitted at
//! frame-build time. The declared param formats are written straight
//! from [`ParamsWriter::FORMATS`](crate::params::ParamsWriter::FORMATS)
//! — the same const the encoder pins to `write_params` — so the
//! declared format and the encoded value share one source and cannot
//! drift. Decoding mirrors the choice: every column goes through
//! [`Cell<BinaryFmt>`](crate::decode::Cell).

use core::marker::PhantomData;

use crate::decode::{BinaryFmt, Cell, DecodeError, FormatCode};
use crate::params::ParamsWriter;

mod sealed {
    /// Module-private seal for [`super::RowDecode`]. Only the
    /// crate-internal tuple impls below may satisfy this trait.
    /// Adding a custom `impl RowDecode for MyRow` from a downstream
    /// crate is impossible — closes the row-decoder hostile probe.
    pub trait RowDecodeSealed {}
}

/// Per-row decoder for prepared queries — uses GAT lifetime.
///
/// Implemented for tuples `(T1,)` through `(T16,)`. The associated
/// `Row<'a>` GAT (stable since Rust 1.65) carries the per-decode-call
/// lifetime — so a row containing `&'a str` borrows from the input
/// bytes for exactly the duration of the decode call. Sealed.
///
/// # GAT rationale
///
/// Pre-GAT, the trait would need a lifetime parameter on the type
/// or HRTB on every bound, both of which break the
/// `const Q: PreparedQuery<(i32,), (i32, &'??? str)>` ergonomics
/// (the user can't type a lifetime in a `const` context cleanly).
/// With a GAT the user types **the marker type** — e.g.,
/// `Row1Int_StrText` or a more idiomatic shape — and the GAT projects
/// to the actual decoded tuple.
///
/// For v1 the marker IS the row tuple but with `&'static str` (the
/// "static placeholder lifetime" idiom), and `Row<'a>` projects to
/// `(T1, ..., &'a str, ...)` by substitution. The macro infers the
/// marker from the cast annotations and the GAT does the lifetime
/// substitution at decode time.
///
/// # `ARITY` and `OIDS`
///
/// Per-impl: tuple arity and per-element [`Cell::OID`](crate::decode::Cell)
/// are drift-pinned. A future schema change that desynced the OID list
/// fails the build.
///
/// # NULL handling
///
/// `bytes_per_col[i]` is `Some(&'a [u8])` for non-NULL columns and
/// `None` for SQL NULL. v1 requires every column non-NULL — NULL
/// returns [`DecodeError::NullInNonNullColumn`]. Nullable columns
/// (`Option<T>` elements) are not yet supported.
//
// Structural diagnostic for sealed-trait E0277. Without the
// attribute, a hostile user trying `impl RowDecode for Foo {}` gets
// the raw «trait bound `Foo: RowDecodeSealed` is not satisfied»
// message — the sealed supertrait is module-private, so they cannot
// fix it from outside. The attribute carries an instructive note
// explaining that only crate-internal tuple impls (arity 0..=16) are
// valid, lifting the diagnostic-UX surface from «contributor must
// remember to look up RowDecodeSealed» to «compiler itself instructs
// the user».
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid prepared-query row type",
    label = "valid row types are tuples `()` through `(T1, T2, ..., T16)` where each Ti implements `ColCellAt`",
    note = "`RowDecode` is sealed — only the crate-internal tuple impls (arity 0..=16) over primitive cell types (`i16`, `i32`, `i64`, `u32`, `bool`, `&'static str`) can satisfy it; downstream `impl RowDecode for ...` is forbidden by construction"
)]
pub trait RowDecode: sealed::RowDecodeSealed + Sized {
    /// Number of columns this row carries.
    const ARITY: u16;
    /// Per-column PG type OIDs.
    const OIDS: &'static [u32];

    /// GAT projection: the decoded tuple at lifetime `'a`. For row
    /// shape `(i32, &'static str)` this projects to `(i32, &'a str)`.
    type Row<'a>;

    /// Decode `bytes_per_col` (one entry per column) into `Row<'a>`.
    ///
    /// # Preconditions (caller-side)
    ///
    /// - `bytes_per_col.len() == Self::ARITY` — caller verified.
    /// - `formats.len() >= Self::ARITY` — caller verified.
    /// - `bytes_per_col[i]` corresponds to column `i` of the row.
    ///
    /// # Errors
    ///
    /// [`DecodeError`] when any per-column byte body fails its
    /// type's `Cell::decode`. The first failing column
    /// short-circuits the row decode.
    fn decode<'a>(
        bytes_per_col: &[Option<&'a [u8]>],
        formats: &[FormatCode],
    ) -> Result<Self::Row<'a>, DecodeError>;
}

// ─────────────────── Tuple impls via macro ───────────────────
//
// Mirror of `params::params_writer_impl!`. Arity 0..=16.
//
// For each arity N we generate two trait impls:
//   1. Static marker `(T1, ..., TN)` (e.g., `(i32, &'static str)`).
//   2. The Row<'a> projection substitutes the static lifetime for `'a`
//      using a helper trait `ColCellAt<'a>` that maps the marker
//      type to its at-`'a` borrowing shape.
//
// `ColCellAt<'a>` is sealed crate-internal; only the primitive
// `Cell<'a, BinaryFmt>`-impl types have it.

/// Crate-internal projection: marker type → at-`'a` decoded type.
/// `i32 → i32` (no lifetime), `&'static str → &'a str`, etc.
//
// Structural diagnostic for the cell-type rejection path. Without the
// attribute, a `query!` whose inferred row carries an unsupported cell
// type (e.g. `u64`) emits the bare «trait bound `u64:
// col_cell_at_sealed::Sealed` is not satisfied» message — the sealed
// module is private, so the contributor cannot inspect the candidates.
// The attribute below routes them to the supported list directly.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a supported prepared-query row cell type",
    label = "supported cell types are `i16`, `i32`, `i64`, `u32`, `bool`, and `&'static str` (rendered as `&'a str` at decode time)",
    note = "`ColCellAt` is sealed — extend the supported set by adding a `col_cell_at_primitive!` invocation in `prepared.rs`; downstream `impl ColCellAt for ...` is forbidden by construction"
)]
pub trait ColCellAt<'a>: col_cell_at_sealed::Sealed {
    /// The type decoded from binary-format column bytes at lifetime `'a`.
    type At;
    /// PG OID for this column type (drift-pinned against
    /// `Cell::OID` per impl).
    const OID: u32;
    /// Decode a single column body.
    fn decode_at(bytes: &'a [u8]) -> Result<Self::At, DecodeError>;
}

mod col_cell_at_sealed {
    /// Module-private seal — only this crate's primitive impls.
    pub trait Sealed {}
}

// Primitive markers: i16/i32/i64/u32/bool — At = Self, lifetime
// transparent.
macro_rules! col_cell_at_primitive {
    ($($t:ty),+ $(,)?) => {
        $(
            impl col_cell_at_sealed::Sealed for $t {}
            impl<'a> ColCellAt<'a> for $t {
                type At = $t;
                const OID: u32 = <$t as Cell<'a, BinaryFmt>>::OID;
                #[inline]
                fn decode_at(bytes: &'a [u8]) -> Result<Self::At, DecodeError> {
                    <$t as Cell<'a, BinaryFmt>>::decode(bytes)
                }
            }
        )+
    };
}

col_cell_at_primitive!(i16, i32, i64, u32, bool);

// `&'static str` marker: At = &'a str, lifetime substituted at the
// decode site.
impl col_cell_at_sealed::Sealed for &'static str {}
impl<'a> ColCellAt<'a> for &'static str {
    type At = &'a str;
    const OID: u32 = <&'static str as Cell<'static, BinaryFmt>>::OID;
    #[inline]
    fn decode_at(bytes: &'a [u8]) -> Result<Self::At, DecodeError> {
        <&'a str as Cell<'a, BinaryFmt>>::decode(bytes)
    }
}

macro_rules! row_decode_impl {
    // Zero-arity special case: the unit tuple.
    () => {
        impl sealed::RowDecodeSealed for () {}
        impl RowDecode for () {
            const ARITY: u16 = 0;
            const OIDS: &'static [u32] = &[];
            type Row<'a> = ();
            #[inline]
            fn decode<'a>(
                _bytes_per_col: &[Option<&'a [u8]>],
                _formats: &[FormatCode],
            ) -> Result<Self::Row<'a>, DecodeError> {
                Ok(())
            }
        }
    };

    // N-arity case — generate impl for (T1, ..., TN).
    ($count:literal, [$($t:ident : $idx:tt),+ $(,)?]) => {
        impl<$($t),+> sealed::RowDecodeSealed for ($($t,)+)
        where
            $($t: for<'a> ColCellAt<'a>,)+
        {}

        impl<$($t),+> RowDecode for ($($t,)+)
        where
            $($t: for<'a> ColCellAt<'a>,)+
        {
            const ARITY: u16 = $count;
            const OIDS: &'static [u32] = &[
                $(<$t as ColCellAt<'static>>::OID,)+
            ];
            type Row<'a> = ($(<$t as ColCellAt<'a>>::At,)+);
            #[inline]
            fn decode<'a>(
                bytes_per_col: &[Option<&'a [u8]>],
                _formats: &[FormatCode],
            ) -> Result<Self::Row<'a>, DecodeError> {
                Ok((
                    $({
                        let slot = bytes_per_col.get($idx).copied().flatten()
                            .ok_or(DecodeError::NullInNonNullColumn)?;
                        <$t as ColCellAt<'a>>::decode_at(slot)?
                    },)+
                ))
            }
        }
    };
}

row_decode_impl!();
row_decode_impl!(1, [A: 0]);
row_decode_impl!(2, [A: 0, B: 1]);
row_decode_impl!(3, [A: 0, B: 1, C: 2]);
row_decode_impl!(4, [A: 0, B: 1, C: 2, D: 3]);
row_decode_impl!(5, [A: 0, B: 1, C: 2, D: 3, E: 4]);
row_decode_impl!(6, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5]);
row_decode_impl!(7, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6]);
row_decode_impl!(8, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7]);
row_decode_impl!(9, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8]);
row_decode_impl!(10, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9]);
row_decode_impl!(11, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10]);
row_decode_impl!(12, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11]);
row_decode_impl!(13, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12]);
row_decode_impl!(14, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12, N: 13]);
row_decode_impl!(15, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12, N: 13, O: 14]);
row_decode_impl!(16, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12, N: 13, O: 14, P: 15]);

// ═════════════════════════════════════════════════════════════════════
// PreparedQuery<P, R>
// ═════════════════════════════════════════════════════════════════════

/// Compile-time prepared PostgreSQL query with type-level parameter
/// and row-shape binding.
///
/// **Construct only via the compile-checked `query!` macro.** All fields
/// are `pub(crate)` and there is no public constructor. The macro
/// emits a struct literal through the crate-internal
/// [`new_prepared_query`] which has `#[doc(hidden)]` visibility and
/// is unstable; the only sanctioned consumer is the macro itself.
///
/// # Tier-1 SQL-injection closure
///
/// External crates cannot construct or mutate this struct because:
/// - all fields are `pub(crate)`; struct-literal construction from
///   outside is `error[E0451]`.
/// - no inherent `new()` method exists.
/// - field access from outside the crate is `error[E0616]`.
/// - the macro accepts only `syn::LitStr` input.
/// - tuple types are nominally distinct; args / queries with
///   mismatched parameter tuples fail to type-check at the
///   `execute_prepared` boundary.
/// - `ParamsWriter` and `RowDecode` are sealed.
/// - `stmt_name` is content-addressed via SHA-256-96.
///
/// The remaining hostile-probe surface lives at OS-level
/// boundaries: `forbid(unsafe_code)` in this crate, user-side
/// `unsafe` as the user's contract, and `.rodata` writes that
/// segfault rather than corrupt the prepared-query template.
///
/// # Type parameters
///
/// - `Params` — tuple of parameter Rust types, must impl
///   [`ParamsWriter`]. The tuple's per-element type is determined
///   from the SQL cast annotation (`$1::int4` → `i32` in the macro's
///   type map).
/// - `Row` — tuple of column Rust types, must impl [`RowDecode`].
///   Same mapping applies to the SELECT/RETURNING column casts.
///
/// # Sizes
///
/// `size_of::<PreparedQuery<(i32,), (i32, &'static str)>>()`:
/// - `sql: &'static str` — 16 B fat pointer
/// - `stmt_name: &'static str` — 16 B
/// - `param_oids: &'static [u32]` — 16 B
/// - `row_oids: &'static [u32]` — 16 B
/// - `parse_template: &'static [u8]` — 16 B
/// - `bind_execute_prefix: &'static [u8]` — 16 B
/// - `_phantom: PhantomData<fn(P) -> R>` — 0 B
/// - **Total: 96 B**, in caller's `.rodata`.
///
/// Pinned ≤ 128 B in `lib.rs` const-assert block.
///
/// # Variance
///
/// `_phantom: PhantomData<fn(Params) -> Row>` makes the type
/// **invariant** in both `Params` and `Row`. A
/// `PreparedQuery<(i32,), _>` cannot be re-used as
/// `PreparedQuery<(u32,), _>` even though the OIDs would match.
/// Tier-1 by-construction at the type level.
#[derive(Debug)]
pub struct PreparedQuery<Params, Row>
where
    Params: ParamsWriter,
    Row: RowDecode,
{
    /// SQL string, lives in `.rodata` of the consumer crate.
    pub(crate) sql: &'static str,
    /// Content-addressed statement name (SHA-256-96 truncation +
    /// `bsql_p_` prefix). Closes the "predictable stmt-name
    /// collision across consumers" probe — the digest depends on
    /// the full SQL text, so two distinct queries cannot share a
    /// stmt-name without colliding their content addresses.
    pub(crate) stmt_name: &'static str,
    /// Parameter OID list — exactly `Params::COUNT` entries.
    /// Drift-pinned via const-assert (per macro expansion) against
    /// `<Params as ParamsWriter>::OIDS`.
    pub(crate) param_oids: &'static [u32],
    /// Row column OID list — exactly `<Row as RowDecode>::ARITY`
    /// entries. The runtime builds a synthetic `RowDesc` from this
    /// list for the SELECT/RETURNING path.
    pub(crate) row_oids: &'static [u32],
    /// Pre-built Parse-frame bytes. PG §55.2.2 layout, fully
    /// computable at macro-expansion (sql + stmt_name + n_params +
    /// per-param OID list are all static). Emitted as
    /// `&'static [u8]` into the consumer crate's `.rodata`.
    pub(crate) parse_template: &'static [u8],
    /// Pre-built Bind-frame prefix bytes — empty portal NUL + stmt_name
    /// NUL ONLY. The runtime appends the param format-code block,
    /// `n_params`, the per-param values, and the result-format trailer
    /// at execute time (all from the argument tuple's `ParamsWriter`),
    /// then patches the length. The prefix bakes NO format/count bytes:
    /// the format declaration must come from the same source that
    /// encodes the values.
    pub(crate) bind_execute_prefix: &'static [u8],
    /// `PhantomData<fn(Params) -> Row>` — invariant in both type
    /// parameters; zero size.
    pub(crate) _phantom: PhantomData<fn(Params) -> Row>,
}

// Manual Clone + Copy impls — the auto-derives would require
// `Params: Clone + Copy` bounds which don't hold for generic
// tuples. Since every field is a `&'static` reference or ZST,
// the struct is trivially `Copy`.
impl<P, R> Clone for PreparedQuery<P, R>
where
    P: ParamsWriter,
    R: RowDecode,
{
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<P, R> Copy for PreparedQuery<P, R>
where
    P: ParamsWriter,
    R: RowDecode,
{
}

impl<P, R> PreparedQuery<P, R>
where
    P: ParamsWriter,
    R: RowDecode,
{
    /// Borrow the SQL string. Used by tests and `Debug` flows that
    /// want to inspect the query without re-routing through the
    /// macro. NOT a SQL-injection bypass: the returned `&str` is
    /// `'static` and read-only; the caller cannot route it to
    /// `Parse` / `SimpleQuery` to mint a hostile prepared statement
    /// because the macro is the only path to `PreparedQuery`.
    ///
    /// Direct field reads from outside the crate are `error[E0616]`
    /// against the `pub(crate)` field; this typed accessor exposes
    /// the data through a documented method with documented intent.
    #[inline]
    #[must_use]
    pub fn sql(&self) -> &'static str {
        self.sql
    }

    /// Borrow the content-addressed statement name (24-hex-char
    /// SHA-256-96 truncation prefixed with `bsql_p_`).
    #[inline]
    #[must_use]
    pub fn stmt_name(&self) -> &'static str {
        self.stmt_name
    }

    /// Parameter OID list — exactly [`ParamsWriter::COUNT`] entries.
    #[inline]
    #[must_use]
    pub fn param_oids(&self) -> &'static [u32] {
        self.param_oids
    }

    /// Row column OID list — exactly [`RowDecode::ARITY`] entries.
    #[inline]
    #[must_use]
    pub fn row_oids(&self) -> &'static [u32] {
        self.row_oids
    }

    /// `#[doc(hidden)]` accessor used by integration tests to assert
    /// the pre-baked Parse template byte layout (P tag, length,
    /// stmt_name, sql, n_param_types, per-param OIDs).
    ///
    /// **Not a SQL-injection bypass**: the returned `&'static [u8]`
    /// is read-only `.rodata`. Production callers never call this
    /// directly — the `BindPrepared::execute` path stages the bytes
    /// via `SendBytesBorrowed` internally. The `_for_test` suffix
    /// + `#[doc(hidden)]` signal the test-only intent.
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn parse_template_for_test(&self) -> &'static [u8] {
        self.parse_template
    }

    /// `#[doc(hidden)]` accessor for tests. Mirror of
    /// [`Self::parse_template_for_test`].
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn bind_execute_prefix_for_test(&self) -> &'static [u8] {
        self.bind_execute_prefix
    }
}

/// Validating constructor — the ONLY path to mint a
/// [`PreparedQuery`]. Both query-generating macros route their
/// `const` expansion through here; there is no unchecked twin.
/// The `#[doc(hidden)]` attribute keeps it out of public docs; `pub`
/// visibility is required because the macro's expansion lands in the
/// consumer crate, not this one.
///
/// # Validating: malformed construction is a build error
///
/// Every argument is cross-checked against the type-level shape the
/// caller declared (`P` / `R`). A drift is a const-evaluation failure
/// (`error[E0080]`) when the result binds to a `const` — never a
/// silently-wrong artifact:
///
/// - the param OID list must equal the parameter tuple's declared
///   OIDs (`<P as ParamsWriter>::OIDS`) — element for element;
/// - the row OID list must equal the row tuple's declared OIDs
///   (`<R as RowDecode>::OIDS`);
/// - the parameter formats are uniformly binary
///   (`<P as ParamsWriter>::FORMATS`) — the binary-uniform wire
///   contract, where `ParamsWriter` is the sole format authority;
/// - the pre-baked `Parse`-frame template's trailing parameter-OID
///   section (its `n_param_types` count and per-param OID words) must
///   match the param OID list — so the wire bytes cannot lie about
///   the parameter types they declare to the server.
///
/// Because the OID-list lengths are themselves pinned to the tuple
/// arity inside [`ParamsWriter`] / [`RowDecode`], an arity mismatch is
/// caught transitively by the element-wise OID comparison.
///
/// # What this closes, and the honest boundary
///
/// Direct construction of a *lying* artifact — one whose wire bytes
/// disagree with its declared `P` / `R` — is now a compile error. The
/// remaining surface is a caller who hand-builds a *self-consistent*
/// artifact with their own literal SQL: that is identical to writing
/// the SQL literal in the macro, i.e. the caller authoring their own
/// query, not untrusted runtime data crossing the boundary. The
/// injection class (a runtime string becoming SQL) stays closed
/// because every entry takes only `&'static str`.
#[doc(hidden)]
#[inline]
#[must_use]
pub const fn new_prepared_query<P, R>(
    sql: &'static str,
    stmt_name: &'static str,
    param_oids: &'static [u32],
    row_oids: &'static [u32],
    parse_template: &'static [u8],
    bind_execute_prefix: &'static [u8],
) -> PreparedQuery<P, R>
where
    P: ParamsWriter,
    R: RowDecode,
{
    assert!(
        oids_equal(param_oids, <P as ParamsWriter>::OIDS),
        "PreparedQuery param OID list does not match the parameter \
         tuple's declared OIDs",
    );
    assert!(
        oids_equal(row_oids, <R as RowDecode>::OIDS),
        "PreparedQuery row OID list does not match the row tuple's \
         declared OIDs",
    );
    assert!(
        all_formats_binary(<P as ParamsWriter>::FORMATS),
        "PreparedQuery parameter formats must be uniformly binary",
    );
    assert!(
        parse_template_oid_section_matches(parse_template, param_oids, stmt_name, sql),
        "PreparedQuery Parse-frame template OID section does not match \
         the declared param OIDs",
    );
    PreparedQuery {
        sql,
        stmt_name,
        param_oids,
        row_oids,
        parse_template,
        bind_execute_prefix,
        _phantom: PhantomData,
    }
}

/// Const element-wise equality of two `u32` (OID) slices — no
/// indexing (the crate forbids `clippy::indexing_slicing`), walked in
/// lockstep via `split_first`.
const fn oids_equal(left: &[u32], right: &[u32]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let (mut a, mut b) = (left, right);
    loop {
        match (a.split_first(), b.split_first()) {
            (Some((x, a_rest)), Some((y, b_rest))) => {
                if *x != *y {
                    return false;
                }
                a = a_rest;
                b = b_rest;
            }
            (None, None) => return true,
            // Lengths were equality-checked above; this arm is
            // structurally unreachable but returns the fail-closed
            // answer rather than asserting.
            _ => return false,
        }
    }
}

/// Const check that every parameter format code is
/// [`FormatCode::Binary`] — the binary-uniform wire contract.
const fn all_formats_binary(formats: &[FormatCode]) -> bool {
    let mut rest = formats;
    loop {
        match rest.split_first() {
            Some((code, tail)) => {
                if !matches!(code, FormatCode::Binary) {
                    return false;
                }
                rest = tail;
            }
            None => return true,
        }
    }
}

/// Const validation that the pre-baked `Parse`-frame template carries
/// exactly the declared parameter OIDs in its trailing
/// `n_param_types` + per-param OID section.
///
/// `Parse` wire layout (PG §55.2.2):
///
/// ```text
/// b'P' | len_i32_be(4) | stmt_name | NUL | sql | NUL |
///   n_param_types_i16_be(2) | oid_i32_be(4) × n
/// ```
///
/// The full byte length is recomputed from the known parts; a
/// mismatch (or a drifted OID / count in the tail) fails closed.
/// No indexing and no panicking arithmetic — bounds come from
/// `split_at` / `split_first_chunk`, sizes from saturating ops.
const fn parse_template_oid_section_matches(
    template: &[u8],
    param_oids: &[u32],
    stmt_name: &'static str,
    sql: &'static str,
) -> bool {
    let n = param_oids.len();
    // tag(1) + len(4) + stmt_name + NUL(1) + sql + NUL(1)
    //   + n_param_types(2) + oid(4)·n
    let expected_len = 1usize
        .saturating_add(4)
        .saturating_add(stmt_name.len())
        .saturating_add(1)
        .saturating_add(sql.len())
        .saturating_add(1)
        .saturating_add(2)
        .saturating_add(4usize.saturating_mul(n));
    if template.len() != expected_len {
        return false;
    }
    // The trailing section is the last n_param_types(2) + oid(4)·n bytes.
    let tail_len = 2usize.saturating_add(4usize.saturating_mul(n));
    let (_, tail) = template.split_at(template.len().saturating_sub(tail_len));
    let (declared_count, mut oid_bytes) = match tail.split_first_chunk::<2>() {
        Some((count_bytes, rest)) => (u16::from_be_bytes(*count_bytes), rest),
        None => return false,
    };
    // Walk the per-param OID words against `param_oids` in lockstep,
    // counting consumed words in u16 space (so the declared count is
    // compared without a usize→u16 cast, which the crate's `as`-ban
    // and non-const `try_from` both rule out).
    let mut rest = param_oids;
    let mut consumed: u16 = 0;
    loop {
        match (oid_bytes.split_first_chunk::<4>(), rest.split_first()) {
            (Some((oid_word, ob_rest)), Some((want, r_rest))) => {
                if u32::from_be_bytes(*oid_word) != *want {
                    return false;
                }
                oid_bytes = ob_rest;
                rest = r_rest;
                consumed = consumed.saturating_add(1);
            }
            (None, None) => return declared_count == consumed,
            _ => return false,
        }
    }
}

// ═════════════════════════════════════════════════════════════════════
// Fingerprint carrier + run boundary
// ═════════════════════════════════════════════════════════════════════

/// Per-query fingerprint carrier.
///
/// A query-generating macro emits ONE uninhabited carrier type per
/// query (a zero-size, value-less marker) together with one
/// `impl QueryFingerprint` for it. The impl carries the whole `const`
/// wire artifact — the SQL text, its content-addressed statement name,
/// the parameter / row OID lists, and the pre-baked `Parse` /
/// `Bind`-prefix byte templates — plus the parameter and row tuple
/// types at the type level.
///
/// [`run`] is the only way to turn a carrier into a
/// [`PreparedQuery`], and it forces the validating constructor
/// ([`new_prepared_query`]). A carrier whose wire bytes drift from its
/// type-level shape therefore fails const-evaluation (`error[E0080]`)
/// at the `run::<Q>()` site.
///
/// The trait is intentionally NOT sealed: the carrier and its impl are
/// emitted in the consumer crate, so a seal would be unsatisfiable
/// from there (and a re-exported seal token would be hand-reachable —
/// deflection, not enforcement). The load-bearing guarantee is the
/// const validator behind [`run`], not the openness of this trait.
pub trait QueryFingerprint {
    /// Parameter tuple type — pins the `$N` Rust types at the type
    /// level and supplies the declared parameter OIDs / formats.
    type Params: ParamsWriter;
    /// Row tuple type — pins the projected column Rust types and the
    /// declared row OIDs.
    type Row: RowDecode;
    /// SQL text (lives in the consumer crate's `.rodata`).
    const SQL: &'static str;
    /// Content-addressed statement name (a hash of [`Self::SQL`]).
    const STMT_NAME: &'static str;
    /// Parameter OID list, one entry per `$N`.
    const PARAM_OIDS: &'static [u32];
    /// Projected-column OID list.
    const ROW_OIDS: &'static [u32];
    /// Pre-baked `Parse`-frame template bytes.
    const PARSE_TEMPLATE: &'static [u8];
    /// Pre-baked `Bind`-frame prefix bytes (portal NUL + stmt-name
    /// NUL); the param format block / values / result-format trailer
    /// are emitted at frame-build time from [`Self::Params`].
    const BIND_EXECUTE_PREFIX: &'static [u8];
}

/// Mint the [`PreparedQuery`] for a fingerprint carrier `Q`.
///
/// The ONLY path from a [`QueryFingerprint`] carrier to a usable
/// [`PreparedQuery`]. It forces [`new_prepared_query`], so the
/// carrier's wire bytes are validated against its declared `Params` /
/// `Row` at const-evaluation — a fabricated or drifted carrier is a
/// build error (`error[E0080]`), not a silently-wrong query.
#[doc(hidden)]
#[inline]
#[must_use]
pub const fn run<Q>() -> PreparedQuery<Q::Params, Q::Row>
where
    Q: QueryFingerprint,
{
    new_prepared_query::<Q::Params, Q::Row>(
        <Q as QueryFingerprint>::SQL,
        <Q as QueryFingerprint>::STMT_NAME,
        <Q as QueryFingerprint>::PARAM_OIDS,
        <Q as QueryFingerprint>::ROW_OIDS,
        <Q as QueryFingerprint>::PARSE_TEMPLATE,
        <Q as QueryFingerprint>::BIND_EXECUTE_PREFIX,
    )
}

// ═════════════════════════════════════════════════════════════════════
// Static trailer bytes for the Bind frame.
//
// PG §55.2.2: after the per-param payload comes
// `n_result_formats: u16_be` followed by that many format codes. The
// macro path elects binary results for EVERY column via the compact
// form (one code applied to all): `n_result_formats = 1, [Binary]`.
// The synthetic `RowDesc` (all-binary) and `RowDecode`'s
// `Cell<BinaryFmt>` decode are pinned to the same choice.
// ═════════════════════════════════════════════════════════════════════

/// `[0x00, 0x01, 0x00, 0x01]` — the static 4-byte
/// `n_result_formats = 1, formats = [Binary]` trailer for the Bind
/// frame. Stable bytes; LLVM places this in `.rodata`.
pub(crate) const BIND_RESULT_FORMATS_ALL_BINARY: [u8; 4] = [0, 1, 0, 1];

/// `'E', 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00` —
/// the static 10-byte Execute frame for the empty-portal +
/// `max_rows = 0` (fetch-all) case. PG §55.2.5 layout:
///
/// ```text
/// 'E' | len_i32_be(9 = 4 self + 1 NUL + 4 max_rows) | empty_portal NUL | max_rows_i32_be(0)
/// ```
///
/// All-static for the macro: every prepared query uses the empty
/// portal and `fetch-all` semantics in v1.0. PortalSuspended /
/// max_rows ≠ 0 are not supported.
pub(crate) const EXECUTE_EMPTY_PORTAL_NO_LIMIT: [u8; 10] = [
    b'E', // tag
    0, 0, 0, 9, // length = 9 (self-inclusive)
    0, // empty portal NUL
    0, 0, 0, 0, // max_rows = 0
];

// Compile-time pins for the static byte arrays.
const _: () = assert!(BIND_RESULT_FORMATS_ALL_BINARY.len() == 4);
const _: () = assert!(EXECUTE_EMPTY_PORTAL_NO_LIMIT.len() == 10);
const _: () = assert!(EXECUTE_EMPTY_PORTAL_NO_LIMIT[0] == b'E');
const _: () = assert!(EXECUTE_EMPTY_PORTAL_NO_LIMIT[4] == 9); // length field

#[cfg(test)]
mod tests {
    //! Spec tests pinning the static byte literals + tuple impl
    //! shape.
    use super::*;
    use crate::decode::oids;

    /// `()` tuple has ARITY 0 and empty OIDS slice.
    #[test]
    fn row_decode_unit_tuple() {
        assert_eq!(<() as RowDecode>::ARITY, 0);
        assert_eq!(<() as RowDecode>::OIDS.len(), 0);
    }

    /// Single-column tuple has ARITY 1 and one OID.
    #[test]
    fn row_decode_singleton_tuple() {
        assert_eq!(<(i32,) as RowDecode>::ARITY, 1);
        assert_eq!(<(i32,) as RowDecode>::OIDS, &[oids::INT4]);
    }

    /// Two-column tuple matches per-element OIDs.
    #[test]
    fn row_decode_pair_tuple() {
        assert_eq!(<(i32, &'static str) as RowDecode>::ARITY, 2);
        assert_eq!(
            <(i32, &'static str) as RowDecode>::OIDS,
            &[oids::INT4, oids::TEXT],
        );
    }

    /// 16-tuple resolves without macro errors.
    #[test]
    fn row_decode_max_arity_16() {
        type Sixteen = (i32, i32, i32, i32, i32, i32, i32, i32,
                        i32, i32, i32, i32, i32, i32, i32, i32);
        assert_eq!(<Sixteen as RowDecode>::ARITY, 16);
        assert_eq!(<Sixteen as RowDecode>::OIDS.len(), 16);
    }

    /// Decode forwards to per-element `Cell<BinaryFmt>`. The
    /// GAT projection substitutes the input lifetime for `&'static str`.
    #[test]
    fn row_decode_binary_smoke() {
        // PG binary format for i32: 4-byte big-endian.
        let col0: &[u8] = &42_i32.to_be_bytes();
        let col1: &[u8] = b"hello";
        let bytes: [Option<&[u8]>; 2] = [Some(col0), Some(col1)];
        let formats: [FormatCode; 2] = [FormatCode::Binary; 2];
        let result = <(i32, &'static str) as RowDecode>::decode(&bytes, &formats);
        assert!(matches!(result, Ok((42_i32, "hello"))));
    }

    /// NULL in a non-Option column returns DecodeError::NullInNonNullColumn.
    #[test]
    fn row_decode_null_in_required_column_errors() {
        let bytes: [Option<&[u8]>; 1] = [None];
        let formats: [FormatCode; 1] = [FormatCode::Binary];
        let result = <(i32,) as RowDecode>::decode(&bytes, &formats);
        assert!(matches!(result, Err(DecodeError::NullInNonNullColumn)));
    }

    /// Static byte trailers match expected layout.
    #[test]
    fn static_trailer_bytes_layout() {
        assert_eq!(&BIND_RESULT_FORMATS_ALL_BINARY, &[0, 1, 0, 1]);
        assert_eq!(
            &EXECUTE_EMPTY_PORTAL_NO_LIMIT,
            &[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]
        );
    }
}
