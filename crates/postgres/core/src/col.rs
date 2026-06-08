//! Sealed `Col` identifier vocabulary — the foundation of the typed
//! dynamic-SQL Fragment builder.
//!
//! # What this module is
//!
//! A *column identifier* in bsql is never a runtime `&str`. It is one of
//! a closed, compiler-known set of zero-sized marker types minted by the
//! [`columns!`](crate::columns) macro. Each such marker implements
//! [`Col`], whose [`Col::as_sql`] returns a `&'static str` that points at
//! a `.rodata` literal — the table/column name baked into the binary.
//!
//! The single absolute rule of the whole builder lives here: **there is
//! no `Col::from_str`, no `From<&str>`, no raw-`&str` -> identifier path
//! anywhere.** The producible identifier universe is provably exactly
//! `{the columns!-declared names}`. A runtime / HTTP string reaches a
//! column only through [`DynCol::parse`] / `TryFrom<&str>`, whose match
//! arms *are* the allowlist; an unknown name is a typed
//! [`UnknownColumn`] error, never an injected identifier.
//!
//! # Two sealed relations
//!
//! 1. [`ColType`] — the closed set of column *value* types. Exactly six
//!    Rust types are admissible: `i16`, `i32`, `i64`, `u32`, `bool`, and
//!    the lifetime-free [`Text`] marker (for `text` columns). Sealed via
//!    the module-private `type_seal::Sealed`, so a seventh column type is
//!    impossible: `impl ColType for f64` is `E0117` (orphan, primitive),
//!    `impl ColType for SomeLocal` is `E0277` (private supertrait).
//!
//! 2. [`Col`] — the closed set of column *identifiers*. Sealed via the
//!    `#[doc(hidden)] pub mod col_seal` supertrait (it must be reachable
//!    by the macro expansion in caller hygiene, hence `pub`, but is
//!    doc-hidden so it is not part of the documented surface). Each
//!    `Col` carries an associated [`Col::Ty`] marker, which lets a future
//!    `eq` combinator reject a wrong-typed value bind at compile time
//!    (`E0308` in both directions — the "better than sqlx" payoff).
//!
//! A blanket `impl<C: Col> AsIdent for C` plus an `impl AsIdent for
//! DynCol` per table give identity-only combinator holes a single
//! [`AsIdent`] bound that accepts both the static ZST path and the
//! runtime [`DynCol`] bridge with no overload duplication.
//!
//! # OID source
//!
//! The six PostgreSQL type OIDs are sourced from
//! [`bsql_postgres_proto::decode::oids`] — the single drift-pinned source
//! of truth for wire OIDs. If an OID constant moves, this module fails to
//! build, not silently desync.
//!
//! # Tier statement (honest)
//!
//! - **Tier-1 (compile)** — the closed *identifier* universe: no foreign
//!   type can be a `Col` (`E0117`), no un-sealed local type can be a
//!   `Col` (`E0277`), no raw `&str` is usable where a `Col`/`AsIdent` is
//!   required (`E0277`). The closed *type* set is likewise tier-1
//!   (`E0117`/`E0277`).
//! - **Tier-1 (compile)** — a wrong-typed value bind against a column's
//!   [`Col::Ty`] is `E0308` (the typed equality guard, realised by the
//!   later Fragment slice).
//! - **Tier-1 (build, degradation)** — a regression that gives a column
//!   marker a non-zero size is `E0080` via the macro-emitted
//!   `const _: () = assert!(size_of == 0)`.
//! - **Tier-3 (by discipline — the honest floor)** — a hostile downstream
//!   crate *can* reach the `#[doc(hidden)] pub col_seal::Sealed`
//!   supertrait and hand-write `impl Col for Rogue` whose `as_sql`
//!   returns a hand-typed `'static` literal, or `String::leak()` (the
//!   `forbid` bundle bans `mem::forget`, not `Box::leak`/`String::leak`).
//!   Both compile. **The injection guarantee therefore does not rest on
//!   seal unforgeability** — it rests on the *return type*: `as_sql`
//!   returns `&'static str`, so returning a *runtime* `String`'s slice is
//!   `E0515`. This is the identical tier-3 floor as the documented raw
//!   `SimpleQuery::new` / `Parse::new` seam (commit `c05072a`); no
//!   witness-token machinery can beat the cross-crate proc-macro-hygiene
//!   floor on stable, so none is added.

use bsql_postgres_proto::decode::oids;

/// Zero-sized type marker for a `text` column.
///
/// Used as a [`Col::Ty`] in place of `&str` because an associated type
/// cannot name an elided lifetime in associated-type position
/// (`type Ty = &str` is `E0637`), and `macro_rules!` cannot rewrite a
/// captured `:ty` fragment `&str` into `&'static str`. `Text` keeps the
/// `'static` requirement out of the public column grammar; the borrowed
/// view is exposed through [`ColType::Value`] as `&'a str`.
///
/// Mirrors the proto crate's `impl col_text_at_sealed::Sealed for
/// &'static str` pattern for the same lifetime-erasure reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Text;

/// Module-private seal for [`ColType`]. The six in-crate impls below are
/// the *only* possible impls: a downstream `impl ColType for Local` fails
/// with `E0277` (this supertrait is not nameable), and `impl ColType for
/// f64` fails with `E0117` (orphan rule, `f64` is foreign). Together they
/// close the column-type set to exactly six.
mod type_seal {
    /// Supertrait seal for [`super::ColType`]. Module-private.
    pub trait Sealed {}

    impl Sealed for i16 {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
    impl Sealed for u32 {}
    impl Sealed for bool {}
    impl Sealed for super::Text {}
}

/// The closed set of column *value* types. Exactly six Rust types satisfy
/// this trait: `i16`, `i32`, `i64`, `u32`, `bool`, and [`Text`]. Sealed
/// via [`type_seal::Sealed`] — see the module-level docs for why a
/// seventh type is impossible.
///
/// Each impl pins the PostgreSQL type [`ColType::OID`] (from
/// [`bsql_postgres_proto::decode::oids`]) and names the borrowed value
/// view [`ColType::Value`] used by the eventual decode/bind side.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a supported bsql column type",
    note = "column types must be one of: i16, i32, i64, u32, bool, Text"
)]
pub trait ColType: type_seal::Sealed {
    /// The PostgreSQL type OID for this column type.
    const OID: u32;

    /// The borrowed Rust value view for one cell of this column type.
    /// Scalars are `Self`; [`Text`]'s view is `&'a str`.
    type Value<'a>;
}

impl ColType for i16 {
    const OID: u32 = oids::INT2;
    type Value<'a> = i16;
}
impl ColType for i32 {
    const OID: u32 = oids::INT4;
    type Value<'a> = i32;
}
impl ColType for i64 {
    const OID: u32 = oids::INT8;
    type Value<'a> = i64;
}
impl ColType for u32 {
    const OID: u32 = oids::OID;
    type Value<'a> = u32;
}
impl ColType for bool {
    const OID: u32 = oids::BOOL;
    type Value<'a> = bool;
}
impl ColType for Text {
    const OID: u32 = oids::TEXT;
    type Value<'a> = &'a str;
}

/// Module-private-ish seal for [`Col`].
///
/// This module is `pub` but `#[doc(hidden)]` because the
/// [`columns!`](crate::columns) macro expands in *caller* hygiene and
/// must be able to write `impl col_seal::Sealed for $col {}` — a
/// genuinely module-private seal would be `E0603` from the caller crate.
/// It is doc-hidden so it is not part of the documented surface.
///
/// # Honest tier note
///
/// Because this seal is reachable, a hostile downstream crate *can*
/// `impl col_seal::Sealed` + `impl Col` for its own type. That does not
/// breach the injection guarantee: see the module-level tier statement —
/// the guarantee rests on [`Col::as_sql`]'s `&'static str` return type
/// (`E0515` on a runtime string), not on this seal being unforgeable.
#[doc(hidden)]
pub mod col_seal {
    /// Supertrait seal for [`super::Col`]. Reachable by macro expansion;
    /// see the module docs for the deliberate `pub` + `#[doc(hidden)]`.
    pub trait Sealed {}
}

/// A SQL column *identifier*. The only implementors are the zero-sized
/// marker types minted by the [`columns!`](crate::columns) macro.
///
/// There is no `from_str`, no `From<&str>`, no raw-`&str` -> identifier
/// path. [`Col::as_sql`] returns a `&'static str` pointing at a `.rodata`
/// literal — the column's name baked into the binary at compile time.
///
/// # Sealed
///
/// Via [`col_seal::Sealed`]. See that module's docs for the honest tier
/// statement (the seal is reachable; the injection guarantee rests on the
/// `&'static str` return type, not on seal unforgeability).
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a SQL column identifier",
    note = "`Col` is sealed — only `columns!`-declared column markers are identifiers; there is no raw-`&str` -> identifier path"
)]
pub trait Col: col_seal::Sealed + Copy + 'static {
    /// The column's value type — one of the six [`ColType`] members.
    /// Lets a typed `eq` combinator reject a wrong-typed value bind at
    /// compile time.
    type Ty: ColType;

    /// The column's SQL identifier as a `.rodata` literal.
    ///
    /// # Invariant
    ///
    /// The return type is `&'static str` *by design*: it is the leg that
    /// makes a runtime string in identifier position an `E0515` compile
    /// error. Do not weaken it.
    fn as_sql(&self) -> &'static str;

    /// The column's PostgreSQL type OID, folded from [`Col::Ty`].
    const PG_OID: u32 = <Self::Ty as ColType>::OID;
}

/// Identity-only bound accepting both a static [`Col`] marker (via the
/// blanket impl below) and a runtime [`DynCol`] bridge (via the per-table
/// impl the [`columns!`](crate::columns) macro emits). Used by combinator
/// holes — like `order_by` — that need an identifier but not its type.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a SQL column identifier usable here",
    note = "an identifier hole (e.g. `order_by`) accepts a `columns!`-minted `Col` marker or a `DynCol` — there is no raw-`&str` -> identifier path"
)]
pub trait AsIdent {
    /// The identifier's SQL text as a `.rodata` literal.
    fn ident(&self) -> &'static str;
}

impl<C: Col> AsIdent for C {
    #[inline]
    fn ident(&self) -> &'static str {
        self.as_sql()
    }
}

/// A runtime string did not match any [`columns!`](crate::columns)-declared
/// column name. Returned by [`DynCol::parse`] / the per-table
/// `TryFrom<&str>` — the only bridge from a runtime `&str` to a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownColumn;

impl core::fmt::Display for UnknownColumn {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("unknown column identifier")
    }
}

impl std::error::Error for UnknownColumn {}

/// Mint a closed column vocabulary for one table.
///
/// Given a table name and a `name: type` list (where each `type` is one
/// of the six [`ColType`](crate::col::ColType) members — `i16`, `i32`,
/// `i64`, `u32`, `bool`, [`Text`](crate::col::Text)), this expands to a
/// module named after the table containing:
///
/// - one zero-sized [`Col`](crate::col::Col) marker per column (the
///   static / typed path), each carrying its `type Ty` for the typed
///   equality guard, with a build-time `const` assertion that the marker
///   is genuinely zero-sized;
/// - one *exhaustive* `DynCol` enum (the runtime-string bridge), with
///   `as_sql`, `parse`, `TryFrom<&str>`, and an `AsIdent` impl.
///
/// `DynCol` is deliberately **not** `#[non_exhaustive]`: a downstream
/// `match` that forgets a column is `E0004` (exhaustiveness), proving
/// column-completeness at the match site. The tradeoff — adding a column
/// is a breaking change — is the correct one for a closed vocabulary.
///
/// # Example
///
/// ```
/// use bsql_postgres_core::col::{Col, AsIdent, Text};
/// bsql_postgres_core::columns! {
///     users => [ id: i32, name: Text, age: i16, active: bool ]
/// }
///
/// // Static / typed path — zero-sized, compile-known identifier:
/// assert_eq!(users::id.as_sql(), "id");
/// assert_eq!(<users::id as Col>::PG_OID, 23); // INT4
///
/// // Runtime / HTTP bridge — the ONLY path from a runtime &str:
/// assert_eq!(users::DynCol::parse("name"), Ok(users::DynCol::name));
/// assert!(users::DynCol::parse("name; DROP TABLE users; --").is_err());
/// ```
#[macro_export]
macro_rules! columns {
    ($table:ident => [ $($col:ident : $ty:ty),+ $(,)? ]) => {
        #[allow(non_snake_case, non_camel_case_types, dead_code)]
        pub mod $table {
            use $crate::col::{Col, ColType, Text, UnknownColumn, col_seal};

            $(
                /// `columns!`-minted zero-sized column identifier marker.
                #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
                pub struct $col;

                impl col_seal::Sealed for $col {}

                impl Col for $col {
                    type Ty = $ty;
                    #[inline]
                    fn as_sql(&self) -> &'static str { stringify!($col) }
                    const PG_OID: u32 = <$ty as ColType>::OID;
                }

                const _: () = assert!(
                    ::core::mem::size_of::<$col>() == 0,
                    concat!(
                        "column identifier `", stringify!($col),
                        "` must be a zero-sized type"
                    )
                );
            )+

            /// Exhaustive runtime-string bridge for this table's columns.
            ///
            /// Deliberately not `#[non_exhaustive]`: a downstream `match`
            /// that forgets a variant is `E0004`.
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum DynCol { $($col,)+ }

            impl DynCol {
                /// This column's SQL identifier as a `.rodata` literal.
                #[inline]
                pub fn as_sql(self) -> &'static str {
                    match self { $(DynCol::$col => stringify!($col),)+ }
                }

                /// Map a runtime `&str` to a column. The match arms ARE
                /// the allowlist; the only catch-all is
                /// `Err(UnknownColumn)`. No arm returns the input as
                /// identifier text.
                #[inline]
                pub fn parse(s: &str) -> ::core::result::Result<Self, UnknownColumn> {
                    match s {
                        $(_ if s == stringify!($col) => ::core::result::Result::Ok(DynCol::$col),)+
                        _ => ::core::result::Result::Err(UnknownColumn),
                    }
                }
            }

            impl<'a> ::core::convert::TryFrom<&'a str> for DynCol {
                type Error = UnknownColumn;
                #[inline]
                fn try_from(s: &'a str) -> ::core::result::Result<Self, UnknownColumn> {
                    DynCol::parse(s)
                }
            }

            impl $crate::col::AsIdent for DynCol {
                #[inline]
                fn ident(&self) -> &'static str { self.as_sql() }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::{AsIdent, Col, ColType, Text, UnknownColumn};

    // Declare a vocabulary covering all six column types for the tests.
    crate::columns! {
        users => [
            id: i32,
            name: Text,
            age: i16,
            active: bool,
            big: i64,
            ref_oid: u32,
        ]
    }

    #[test]
    fn as_sql_round_trips_the_rodata_literal() {
        // Static markers return the declared name byte-for-byte.
        assert_eq!(users::id.as_sql(), "id");
        assert_eq!(users::name.as_sql(), "name");
        assert_eq!(users::age.as_sql(), "age");
        assert_eq!(users::active.as_sql(), "active");
        assert_eq!(users::big.as_sql(), "big");
        assert_eq!(users::ref_oid.as_sql(), "ref_oid");
    }

    #[test]
    fn as_ident_blanket_matches_as_sql() {
        // The blanket `impl<C: Col> AsIdent` routes to `as_sql`.
        assert_eq!(AsIdent::ident(&users::id), "id");
        assert_eq!(AsIdent::ident(&users::name), "name");
    }

    #[test]
    fn pg_oid_folds_to_the_right_oid() {
        // PG_OID is folded from each column's `type Ty: ColType`.
        assert_eq!(<users::id as Col>::PG_OID, <i32 as ColType>::OID);
        assert_eq!(<users::name as Col>::PG_OID, <Text as ColType>::OID);
        assert_eq!(<users::age as Col>::PG_OID, <i16 as ColType>::OID);
        assert_eq!(<users::active as Col>::PG_OID, <bool as ColType>::OID);
        assert_eq!(<users::big as Col>::PG_OID, <i64 as ColType>::OID);
        assert_eq!(<users::ref_oid as Col>::PG_OID, <u32 as ColType>::OID);

        // And to the concrete wire OIDs (drift pin against proto).
        assert_eq!(<users::id as Col>::PG_OID, 23); // int4
        assert_eq!(<users::name as Col>::PG_OID, 25); // text
        assert_eq!(<users::age as Col>::PG_OID, 21); // int2
        assert_eq!(<users::active as Col>::PG_OID, 16); // bool
        assert_eq!(<users::big as Col>::PG_OID, 20); // int8
        assert_eq!(<users::ref_oid as Col>::PG_OID, 26); // oid
    }

    #[test]
    fn dyncol_as_sql_matches_static_markers() {
        assert_eq!(users::DynCol::id.as_sql(), "id");
        assert_eq!(users::DynCol::name.as_sql(), users::name.as_sql());
        assert_eq!(users::DynCol::ref_oid.as_sql(), "ref_oid");
    }

    #[test]
    fn dyncol_as_ident_matches_as_sql() {
        assert_eq!(AsIdent::ident(&users::DynCol::active), "active");
    }

    #[test]
    fn runtime_bridge_accepts_declared_names() {
        assert_eq!(users::DynCol::parse("id"), Ok(users::DynCol::id));
        assert_eq!(users::DynCol::parse("name"), Ok(users::DynCol::name));
        assert_eq!(users::DynCol::parse("ref_oid"), Ok(users::DynCol::ref_oid));
        // The mapped identifier is the rodata literal, not the input ptr.
        let mapped = users::DynCol::parse("name").map(|c| c.as_sql());
        assert_eq!(mapped, Ok("name"));
    }

    #[test]
    fn runtime_bridge_rejects_injection_and_unknowns() {
        // The producible identifier universe is exactly the declared set;
        // every hostile / unknown payload is a typed Err, never SQL text.
        let payloads = [
            "name; DROP TABLE users; --",
            "id; DELETE FROM users WHERE 1=1",
            "id) OR 1=1 --",
            "\"name\"",
            "name\0",
            "1=1",
            "",
            " id ",  // whitespace-padded — not an exact match
            "ID",    // case-different — not an exact match
            "DROP",
            "unknown_column",
        ];
        for p in payloads {
            assert_eq!(
                users::DynCol::parse(p),
                Err(UnknownColumn),
                "payload {p:?} must be rejected, never become an identifier"
            );
        }
    }

    #[test]
    fn try_from_str_is_the_same_bridge() {
        use core::convert::TryFrom;
        assert_eq!(users::DynCol::try_from("active"), Ok(users::DynCol::active));
        assert_eq!(users::DynCol::try_from("nope"), Err(UnknownColumn));
    }

    #[test]
    fn multi_column_projection_collects_or_fails_closed() {
        // The make-or-break ?fields= pattern: a CSV maps to a Vec of
        // columns, failing closed on any unknown member.
        let ok: Result<Vec<_>, _> =
            "id,name,age".split(',').map(users::DynCol::parse).collect();
        assert_eq!(
            ok,
            Ok(vec![users::DynCol::id, users::DynCol::name, users::DynCol::age])
        );

        let bad: Result<Vec<_>, _> =
            "id,evil; DROP,age".split(',').map(users::DynCol::parse).collect();
        assert_eq!(bad, Err(UnknownColumn));
    }

    #[test]
    fn footprint_markers_are_zst_dyncol_is_one_byte() {
        use core::mem::{align_of, size_of};
        // Every static identifier marker is zero-sized.
        assert_eq!(size_of::<users::id>(), 0);
        assert_eq!(size_of::<users::name>(), 0);
        assert_eq!(size_of::<users::age>(), 0);
        assert_eq!(size_of::<users::active>(), 0);
        assert_eq!(size_of::<users::big>(), 0);
        assert_eq!(size_of::<users::ref_oid>(), 0);
        assert_eq!(size_of::<Text>(), 0);
        assert_eq!(size_of::<UnknownColumn>(), 0);
        // The runtime carrier is a single discriminant byte.
        assert_eq!(size_of::<users::DynCol>(), 1);
        assert_eq!(align_of::<users::DynCol>(), 1);
    }

    // Build-time footprint anchors (tier-1 degradation pins): a non-ZST
    // regression fails the build via the macro-emitted const-assert; these
    // const items pin the public markers at the module level too.
    const _: () = assert!(core::mem::size_of::<Text>() == 0);
    const _: () = assert!(core::mem::size_of::<UnknownColumn>() == 0);
    const _: () = assert!(core::mem::size_of::<users::DynCol>() == 1);
}
