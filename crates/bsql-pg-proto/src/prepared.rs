//! DEF-244 — `PreparedQuery<P, R>` runtime type + `RowDecode` trait.
//!
//! See `/tmp/def244-design-memo.md` for the full design rationale.
//! This module hosts the runtime artefacts that the `prepared!`
//! proc-macro produces struct literals of:
//!
//! - [`PreparedQuery<Params, Row>`] — content-addressed prepared SQL
//!   query with type-level parameter and row-shape pinning.
//! - [`RowDecode`] — sealed trait for tuple-arity row decoding.
//! - [`BindPrepared`](crate::push_command::BindPrepared) — `PushCommand`
//!   impl that pairs a prepared query with its argument tuple at the
//!   call site.
//! - `new_prepared_query<P, R>(...)` — the only constructor for
//!   `PreparedQuery`. Crate-internal visibility on the function +
//!   `pub(crate)` fields on the struct close the P1/P2/P3 hostile
//!   probes (memo §7).
//!
//! # Tier-1 SQL injection closure (memo §7)
//!
//! Eight of twelve hostile probes close tier-1 inside the language:
//! private fields, sealed trait, content-addressed stmt_name. The
//! remaining four (P4/P5/P10/P12) are OS-level boundaries where
//! `forbid(unsafe_code)` in this crate ends and the user's `unsafe`
//! contract or `.rodata` memory protection begins. This boundary
//! framing matches DEF-248 Sub-A's `panic = "abort"` precedent.
//!
//! # Format choice — text in v1.0
//!
//! The macro emits `bind_execute_prefix` with the compact
//! format-code block (`n_format_codes = 1, formats = [Text]` for
//! N ≥ 1; `n_format_codes = 0` for N = 0). All-text per memo §5.4.
//! When DEF-228 lands binary-format decoders for more types, a
//! macro flag will allow elective binary; the runtime path remains
//! unchanged (text is the safer default for ad-hoc primitive types
//! and matches today's [`DecodeFormat<TextFmt>`] matrix).

use core::marker::PhantomData;

use crate::decode::{DecodeError, DecodeFormat, FormatCode, TextFmt};
use crate::params::ParamsWriter;

mod sealed {
    /// Module-private seal for [`super::RowDecode`]. Only the
    /// crate-internal tuple impls below may satisfy this trait.
    /// Adding a custom `impl RowDecode for MyRow` from a downstream
    /// crate is impossible — closes the P8-equivalent hostile probe
    /// for the row-decoder side.
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
/// Per-impl: tuple arity and per-element [`DecodeFormat::OID`] are
/// drift-pinned. A future schema change that desynced the OID list
/// fails the build.
///
/// # NULL handling
///
/// `bytes_per_col[i]` is `Some(&'a [u8])` for non-NULL columns and
/// `None` for SQL NULL. v1 requires every column non-NULL — NULL
/// returns [`DecodeError::NullInNonNullColumn`]. Nullable columns
/// (`Option<T>` elements) track DEF-228.
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
    /// type's `DecodeFormat::decode`. The first failing column
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
//      using a helper trait `ColTextAt<'a>` that maps the marker
//      type to its at-`'a` borrowing shape.
//
// `ColTextAt<'a>` is sealed crate-internal; only the primitive
// `DecodeFormat<'a, TextFmt>`-impl types have it.

/// Crate-internal projection: marker type → at-`'a` decoded type.
/// `i32 → i32` (no lifetime), `&'static str → &'a str`, etc.
pub trait ColTextAt<'a>: col_text_at_sealed::Sealed {
    /// The type decoded from text-format bytes at lifetime `'a`.
    type At;
    /// PG OID for this column type (drift-pinned against
    /// `DecodeFormat::OID` per impl).
    const OID: u32;
    /// Decode a single column body.
    fn decode_at(bytes: &'a [u8]) -> Result<Self::At, DecodeError>;
}

mod col_text_at_sealed {
    /// Module-private seal — only this crate's primitive impls.
    pub trait Sealed {}
}

// Primitive markers: i16/i32/i64/u32/bool — At = Self, lifetime
// transparent.
macro_rules! col_text_at_primitive {
    ($($t:ty),+ $(,)?) => {
        $(
            impl col_text_at_sealed::Sealed for $t {}
            impl<'a> ColTextAt<'a> for $t {
                type At = $t;
                const OID: u32 = <$t as DecodeFormat<'a, TextFmt>>::OID;
                #[inline]
                fn decode_at(bytes: &'a [u8]) -> Result<Self::At, DecodeError> {
                    <$t as DecodeFormat<'a, TextFmt>>::decode(bytes)
                }
            }
        )+
    };
}

col_text_at_primitive!(i16, i32, i64, u32, bool);

// `&'static str` marker: At = &'a str, lifetime substituted at the
// decode site.
impl col_text_at_sealed::Sealed for &'static str {}
impl<'a> ColTextAt<'a> for &'static str {
    type At = &'a str;
    const OID: u32 = <&'static str as DecodeFormat<'static, TextFmt>>::OID;
    #[inline]
    fn decode_at(bytes: &'a [u8]) -> Result<Self::At, DecodeError> {
        <&'a str as DecodeFormat<'a, TextFmt>>::decode(bytes)
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
            $($t: for<'a> ColTextAt<'a>,)+
        {}

        impl<$($t),+> RowDecode for ($($t,)+)
        where
            $($t: for<'a> ColTextAt<'a>,)+
        {
            const ARITY: u16 = $count;
            const OIDS: &'static [u32] = &[
                $(<$t as ColTextAt<'static>>::OID,)+
            ];
            type Row<'a> = ($(<$t as ColTextAt<'a>>::At,)+);
            #[inline]
            fn decode<'a>(
                bytes_per_col: &[Option<&'a [u8]>],
                _formats: &[FormatCode],
            ) -> Result<Self::Row<'a>, DecodeError> {
                Ok((
                    $({
                        let slot = bytes_per_col.get($idx).copied().flatten()
                            .ok_or(DecodeError::NullInNonNullColumn)?;
                        <$t as ColTextAt<'a>>::decode_at(slot)?
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
/// **Construct only via the [`prepared`](macro@crate::prepared) macro.** All fields
/// are `pub(crate)` and there is no public constructor. The macro
/// emits a struct literal through the crate-internal
/// [`new_prepared_query`] which has `#[doc(hidden)]` visibility and
/// is unstable; the only sanctioned consumer is the macro itself.
///
/// # Tier-1 SQL-injection closure (memo §7)
///
/// External crates cannot construct or mutate this struct because:
/// - **P1/P9** — all fields are `pub(crate)`; struct-literal
///   construction from outside is `error[E0451]`.
/// - **P2** — no inherent `new()` method exists.
/// - **P3** — fields are `pub(crate)`; field access is `error[E0616]`.
/// - **P6** — the macro accepts only `syn::LitStr` input.
/// - **P7** — tuple types are nominally distinct; arg/queries with
///   mismatched parameter tuples fail to type-check at the
///   `execute_prepared` boundary.
/// - **P8** — `ParamsWriter` and `RowDecode` are sealed.
/// - **P11** — stmt_name is content-addressed via SHA-256-96.
///
/// P4/P5/P10/P12 are OS-level boundaries (`forbid(unsafe_code)` in
/// this crate; user-side `unsafe` is the user's contract;
/// `.rodata` writes segfault). Memo §12 framing.
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
    /// `bsql_p_` prefix). Memo §7 P11 closure.
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
    /// Pre-built Bind-frame prefix bytes. Covers `'B'`, length
    /// placeholder, empty portal, stmt_name, compact format-code
    /// block, and `n_params`. The runtime appends per-param values
    /// (via `args.write_params(...)`) and the n_result_formats
    /// trailer at execute time, then patches the length.
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
    /// `Parse`/`SimpleQuery` to mint a hostile prepared statement
    /// because the macro is the only path to `PreparedQuery`.
    ///
    /// Memo §7 P3 framing — pub field reads are E0616 from outside
    /// the crate; this accessor exposes the data through a typed
    /// method with documented intent.
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

/// Crate-internal constructor — the ONLY sanctioned path to mint
/// a [`PreparedQuery`]. Called exclusively by the macro's
/// expansion. The `#[doc(hidden)]` attribute keeps it out of public
/// docs; `pub` visibility is required for the macro's expansion
/// site (which is in the consumer crate, not this one).
///
/// # Tier-1 SQL-injection closure
///
/// `pub fn` is the macro's escape hatch. **Audit-trust class**:
/// this function is "macro plumbing" — the only documented caller
/// is the `prepared!` macro. A hostile caller invoking
/// `new_prepared_query` directly is bypassing the macro's lex +
/// validation pipeline — but they get exactly what they invoke:
/// the static bytes the macro would emit, with whatever SQL they
/// chose to construct. CREDO §0 tier-3 documented-discipline
/// boundary: the macro is the contracted path; direct calls are
/// out-of-scope adversarial usage that doesn't affect the
/// SQL-injection class for users who follow the contract.
///
/// To close this fully (tier-2 by-construction) at a future major:
/// could use a sealed-via-private-token pattern (token type lives
/// in a `pub(crate)` module the proc-macro pair-crate doesn't
/// reach — but that breaks the macro's expansion path since
/// `bsql-pg-proto-derive` can't construct `pub(crate)` tokens).
/// Open for DEF-244 follow-up; documented as tier-3-by-discipline
/// here per memo §7 P3 closing remarks.
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

// ═════════════════════════════════════════════════════════════════════
// Static trailer bytes for the Bind frame.
//
// PG §55.2.2: after the per-param payload comes
// `n_result_formats: u16_be`. v1 uses `0` (all-text default per
// memo §5.4); when DEF-228 lands binary-result, this would conditionally
// expand to `1, [Binary]` for prepared queries electing binary.
// ═════════════════════════════════════════════════════════════════════

/// `[0x00, 0x00]` — the static 2-byte `n_result_formats = 0`
/// trailer for the Bind frame. Stable bytes; LLVM places this in
/// `.rodata` and the action layer emits via `SendBytesStatic`.
pub(crate) const BIND_N_RESULT_FORMATS_ZERO: [u8; 2] = [0, 0];

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
/// max_rows ≠ 0 are out of scope (track DEF-035 stmt-cache + 1c-6
/// portal-Close).
pub(crate) const EXECUTE_EMPTY_PORTAL_NO_LIMIT: [u8; 10] = [
    b'E', // tag
    0, 0, 0, 9, // length = 9 (self-inclusive)
    0, // empty portal NUL
    0, 0, 0, 0, // max_rows = 0
];

// Compile-time pins for the static byte arrays.
const _: () = assert!(BIND_N_RESULT_FORMATS_ZERO.len() == 2);
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

    /// Decode forwards to per-element `DecodeFormat<TextFmt>`. The
    /// GAT projection substitutes the input lifetime for `&'static str`.
    #[test]
    fn row_decode_text_smoke() {
        // PG text format for i32: ASCII decimal.
        let col0: &[u8] = b"42";
        let col1: &[u8] = b"hello";
        let bytes: [Option<&[u8]>; 2] = [Some(col0), Some(col1)];
        let formats: [FormatCode; 2] = [FormatCode::Text; 2];
        let result = <(i32, &'static str) as RowDecode>::decode(&bytes, &formats);
        assert!(matches!(result, Ok((42_i32, "hello"))));
    }

    /// NULL in a non-Option column returns DecodeError::NullInNonNullColumn.
    #[test]
    fn row_decode_null_in_required_column_errors() {
        let bytes: [Option<&[u8]>; 1] = [None];
        let formats: [FormatCode; 1] = [FormatCode::Text];
        let result = <(i32,) as RowDecode>::decode(&bytes, &formats);
        assert!(matches!(result, Err(DecodeError::NullInNonNullColumn)));
    }

    /// Static byte trailers match expected layout.
    #[test]
    fn static_trailer_bytes_layout() {
        assert_eq!(&BIND_N_RESULT_FORMATS_ZERO, &[0, 0]);
        assert_eq!(
            &EXECUTE_EMPTY_PORTAL_NO_LIMIT,
            &[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]
        );
    }
}
