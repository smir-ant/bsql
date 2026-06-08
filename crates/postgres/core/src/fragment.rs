//! The sealed `Fragment` value type — the runtime SQL skeleton that backs
//! the typed dynamic-SQL builder (slice 2: the algebra core; slice 3: the
//! typed combinator surface).
//!
//! # What a `Fragment` is
//!
//! A [`Fragment`] is a *runtime value* assembled from a static **spine**
//! (the `SELECT … FROM …` skeleton produced by [`fragment!`](crate::fragment))
//! plus typed clause slots layered on at runtime joints:
//!
//! - a `where_clause: Option<`[`Predicate`]`>` — the composed `WHERE`
//!   filter, built from typed [`ColPredicate`] comparisons;
//! - an `order` list of [`OrderTerm`]s — the `ORDER BY` keys;
//! - a `limit` / `offset` — bound `$N` values.
//!
//! Each slot is a *typed field*, not text, so the SQL clause keywords
//! (`WHERE`, `AND`, `ORDER BY`, `LIMIT`, …) are emitted exactly once, in a
//! fixed order, by [`Fragment::build`] — never by a per-call token that
//! could dangle (no `AND` is ever emitted that is not between two operands).
//!
//! The spine itself is an ordered sequence of [`Chunk`]s, where each chunk
//! is either
//!
//! - a `.rodata` `&'static str` literal ([`Chunk::Rodata`]) — a keyword,
//!   a punctuation fragment, or a [`Col::as_sql`](crate::col::Col::as_sql)
//!   identifier (a future slice), all compile-time text baked into the
//!   binary; or
//! - a positional value hole ([`Chunk::Hole`]) carrying exactly one
//!   binary-encoded [`BoundValue`].
//!
//! The producible SQL-text universe is provably
//! `{Chunk::Rodata literals}` ∪ `{$N placeholders}` — and *nothing else*.
//! A bound value is **always** a `$N` placeholder carrying a binary wire
//! block, **never** interpolated as text.
//!
//! # The absolute rule
//!
//! There is no raw-`&str` → SQL path. There is no `Fragment::from_str`,
//! no `From<&str>`, no [`BoundValue`] variant that becomes spine text.
//! A value entering a hole is encoded to PostgreSQL binary wire bytes via
//! [`bsql_postgres_proto::decode::EncodeBinary`] and emitted as a `$N`
//! block — it can never become part of the SQL skeleton.
//!
//! # Constructing fragments
//!
//! The primary constructor is the [`fragment!`](crate::fragment) macro: a
//! *literal* SQL skeleton with typed `{}` holes, e.g.
//!
//! ```ignore
//! let f = fragment!(
//!     "SELECT id FROM users WHERE id = {} AND active = {}",
//!     user_id, is_active,
//! );
//! ```
//!
//! The literal text between holes is kept as [`Chunk::Rodata`]; each `{}`
//! becomes the next `$N` and pushes its bind at the same visit. The
//! literal skeleton is author-written compile-time text — the sanctioned
//! static spine, the one greppable bridge (tier-3-by-discipline, exactly
//! like the documented `SimpleQuery::new` / `Parse::new` seam at commit
//! `c05072a`).
//!
//! # Renumbering (the subtle core)
//!
//! `$N` is **never stored** in a hole. It is *derived* at
//! [`Fragment::build`] by a single left-to-right counter walking, in fixed
//! clause order, the spine then the `WHERE`, `ORDER BY`, `LIMIT`, and
//! `OFFSET` holes. Therefore composition (spine [`Fragment::append`] *and*
//! the slice-3 [`Fragment::and_where`] fold) is pure structural
//! accumulation that never touches assembled text, and renumbering is
//! automatically contiguous on the next `build()`. This is the keystone: a
//! standalone predicate whose hole is `$1`, after composing onto a spine
//! that already has two holes, has that hole renumbered to `$3` in the
//! combined output with zero text re-parse.
//!
//! # The combinator surface (slice 3 — "better than sqlx")
//!
//! The make-or-break payoff is **compile-time column↔value type checking**.
//! The [`ColPredicate`] blanket trait gives every [`Col`](crate::col::Col)
//! marker the comparators `eq`/`ne`/`lt`/`le`/`gt`/`ge`, each taking a value
//! of *exactly* the column's value type
//! (`<C::Ty as ColType>::Value<'_>`). A wrong-typed value is `E0308`
//! *before* any runtime check: `users::age.gt("oops")` on an `i16` column
//! does not compile. There is **no silent widening** — `users::age.gt(1i32)`
//! on an `i16` column is also `E0308`.
//!
//! ```
//! use bsql_postgres_core::columns;
//! use bsql_postgres_core::fragment::{ColPredicate, Fragment, Chunk};
//! columns! { users => [ id: i32, name: Text, age: i16 ] }
//!
//! let f = Fragment::__from_chunks(vec![Chunk::Rodata("SELECT id FROM users")])
//!     .and_where(users::age.gt(18i16))
//!     .and_where(users::name.eq("al"))
//!     .order_by(users::name);
//! let a = f.build().expect("no overflow");
//! assert_eq!(a.sql, "SELECT id FROM users WHERE (age > $1 AND name = $2) ORDER BY name");
//! assert_eq!(a.oids, vec![21, 25]); // INT2, TEXT
//! ```
//!
//! ## Where-tree parenthesisation (no precedence footgun)
//!
//! The top-level filter chain is a pure associative **AND** list:
//! [`Fragment::and_where`] is the *sole* filter combinator. There is
//! deliberately **no** `or_where` on the builder — a flat
//! `.and_where(B).or_where(C)` would emit `WHERE A AND B OR C`, which SQL
//! parses as `(A AND B) OR C` (AND binds tighter than OR), *not* the
//! call-order grouping `A AND (B OR C)` a reader of the fluent chain
//! expects. That is a silent, truth-table-divergent wrong `WHERE`. `OR` is
//! expressed only by [`Predicate::or`], which self-parenthesises, so the
//! grouping is always explicit and visible at the call site:
//! `age.gt(1).or(age.lt(0))` → `(age > $1 OR age < $2)`.
//!
//! Leaf predicates (`col OP $N`, `col IS [NOT] NULL`) are emitted bare;
//! every *combination* node ([`Predicate::and`] / [`Predicate::or`])
//! self-wraps in parentheses, so operator precedence can never leak across
//! a composition boundary.
//!
//! ## Honest tier boundary (hand-written `WHERE` in the spine)
//!
//! [`Fragment::build`] emits ` WHERE ` exactly once, iff `where_clause` is
//! `Some`. If an author hand-writes a `WHERE` *into the static spine*
//! (`fragment!("… WHERE deleted = false")`) **and then** calls
//! [`and_where`](Self::and_where), the output double-`WHERE`s
//! (`… WHERE deleted = false WHERE (…)`). This is a tier-4 discipline
//! boundary common to *every* possible builder shape: it cannot be lifted
//! by the type system here (the spine is opaque `Chunk::Rodata` text by
//! the time it reaches the builder). The sanctioned path is to express the
//! filter through [`and_where`], never by hand-writing `WHERE` into the
//! spine; a future `fragment!`-macro lint can detect a trailing `WHERE` in
//! the literal and is the place to lift this (out of slice-3 scope).
//!
//! # Owned, not borrowed
//!
//! [`Fragment`] has **no lifetime parameter**. A text bind is stored as
//! [`BoundValue::Text(String)`](BoundValue::Text) (one allocation, moved
//! not cloned). The alternative — a borrowed `Fragment<'a>` saving exactly
//! one allocation + the text bytes per *text* bind (scalars are
//! allocation-free either way) — would poison every downstream signature
//! (struct fields, returns, every slice-3 combinator) with a lifetime
//! parameter. Priority "fewer moving parts / clean composition" outranks
//! the priority-4 per-text-bind allocation saving, so owned wins. The
//! representation is identical either way — only [`BoundValue::Text`]'s
//! payload would change (`String` → `&'a str`) — so a borrowed variant is
//! a deferrable pure optimization if a measured hot path ever demands it.
//!
//! # Tier statement (honest)
//!
//! - **Tier-1 (compile)** — the injection wall is [`Chunk::Rodata`]'s
//!   `&'static str` field: a *runtime* `String` cannot enter a `Rodata`
//!   chunk (`E0597`, in-crate *and* cross-crate). This is the identical
//!   floor as [`Col::as_sql`](crate::col::Col::as_sql)'s `&'static str`
//!   return type and the `c05072a` `SimpleQuery::new` seam.
//! - **Tier-1 (compile)** — a non-bindable hole type (`f64`, a foreign
//!   struct, …) is `E0277`: it does not implement [`IntoBound`]. The hole
//!   accepts exactly `i16`, `i32`, `i64`, `u32`, `bool`, `&str`, `String`.
//! - **Tier-1 (compile)** — [`BoundValue`] is a closed `enum` (no
//!   downstream crate can add a variant); there is no `Raw` variant
//!   (`E0599`), so a value can *only* become a `$N` binary block.
//! - **Tier-3 (by discipline — the honest floor)** — `String::leak()`
//!   yields a `&'static str` that *can* enter a `Rodata` chunk and so
//!   reach the spine. This is the deliberate, greppable floor — identical
//!   to slice-1 `col.rs` and `c05072a` (the `forbid` bundle bans
//!   `mem::forget`, not `String::leak`). The normal path is closed; the
//!   leak is never accidental. The floor *compiles* — proving the design
//!   introduces no new floor beyond slice-1's:
//!
//!   ```
//!   use bsql_postgres_core::fragment::{Chunk, Fragment};
//!   // `String::leak()` is the deliberate, greppable tier-3 floor — it
//!   // yields a `&'static str`, the one thing a Rodata chunk accepts.
//!   let leaked: &'static str = String::from("DROP TABLE users").leak();
//!   let f = Fragment::__from_chunks(vec![Chunk::Rodata(leaked)]);
//!   let assembled = f.build().expect("no holes, cannot overflow");
//!   assert_eq!(assembled.sql, "DROP TABLE users");
//!   ```
//!
//! # On sealing the value entry gate
//!
//! [`IntoBound`] is deliberately **not** sealed. A rogue downstream
//! `impl IntoBound for Evil` compiles, but is *benign*: it can only return
//! a closed [`BoundValue`], which can only become a `$N` binary block
//! (never spine text), and it maps to an existing variant carrying that
//! variant's *correct* OID — it cannot mint a wrong OID. The injection
//! wall rests on [`Chunk::Rodata`]'s `&'static str` field (`E0597`), not on
//! the value-entry trait being unforgeable. Sealing it would be one extra
//! module for zero invariant, so it is omitted.

use crate::col::{AsIdent, Col, ColType};
use bsql_postgres_proto::decode::EncodeBinary;
use bsql_postgres_proto::{WriteBuf, WriteBufFull};

/// The closed runtime bind carrier — exactly one variant per supported
/// PostgreSQL value type. An `enum` is closed by construction (no
/// downstream crate can add a variant), so no supertrait seal is needed:
/// there is no `Raw`/text-passthrough variant, so a bound value can *only*
/// ever become a `$N` binary wire block, never SQL skeleton text.
///
/// A text bind is stored owned ([`BoundValue::Text(String)`](Self::Text))
/// to keep [`Fragment`] lifetime-free; see the module docs for the
/// owned-vs-borrowed tradeoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundValue {
    /// A `smallint` (PG OID 21) bind.
    I16(i16),
    /// An `integer` (PG OID 23) bind.
    I32(i32),
    /// A `bigint` (PG OID 20) bind.
    I64(i64),
    /// An `oid` (PG OID 26) bind.
    U32(u32),
    /// A `boolean` (PG OID 16) bind.
    Bool(bool),
    /// A `text` (PG OID 25) bind, stored owned.
    Text(String),
}

impl BoundValue {
    /// This bind's PostgreSQL type OID, folded from the single source of
    /// truth ([`EncodeBinary::OID`] in the proto crate) so a wire-OID
    /// change there fails *this* build rather than silently desyncing.
    #[inline]
    #[must_use]
    pub fn oid(&self) -> u32 {
        match self {
            BoundValue::I16(_) => <i16 as EncodeBinary>::OID,
            BoundValue::I32(_) => <i32 as EncodeBinary>::OID,
            BoundValue::I64(_) => <i64 as EncodeBinary>::OID,
            BoundValue::U32(_) => <u32 as EncodeBinary>::OID,
            BoundValue::Bool(_) => <bool as EncodeBinary>::OID,
            BoundValue::Text(_) => <&str as EncodeBinary>::OID,
        }
    }

    /// Write this bind's payload (no length prefix) into `dst`.
    ///
    /// Scalars delegate straight to their [`EncodeBinary`] impl. Text
    /// encodes through `&str` — proto seals `EncodeBinary` for `&str`, not
    /// `String`, so the owned payload is borrowed as `self.as_str()` first.
    /// This is load-bearing: there is no `impl EncodeBinary for String`.
    #[inline]
    fn encode_body(&self, dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
        match self {
            BoundValue::I16(v) => v.encode_to(dst),
            BoundValue::I32(v) => v.encode_to(dst),
            BoundValue::I64(v) => v.encode_to(dst),
            BoundValue::U32(v) => v.encode_to(dst),
            BoundValue::Bool(v) => v.encode_to(dst),
            BoundValue::Text(s) => s.as_str().encode_to(dst),
        }
    }

    /// Encode this bind as one PostgreSQL Bind-message parameter block:
    /// a big-endian `i32` length prefix followed by the payload bytes.
    ///
    /// # Errors
    ///
    /// [`WriteBufFull`] if the payload exceeds the bounded
    /// [`WriteBuf`] capacity (e.g. an oversized text bind). Surfaced as a
    /// classified error — never a panic — honoring `deny(unwrap_used)`.
    fn encode_block(&self) -> Result<Vec<u8>, WriteBufFull> {
        let mut buf = WriteBuf::new();
        buf.with_i32_length_prefixed_body(|w| self.encode_body(w))?;
        Ok(buf.as_bytes().to_vec())
    }
}

/// The compile-time wall that makes a non-bindable hole type an `E0277`.
///
/// Implemented for exactly the seven admissible Rust hole types: `i16`,
/// `i32`, `i64`, `u32`, `bool`, `&str`, and `String`. A `{}` hole accepts
/// exactly these — there is no raw-text interpolation path.
///
/// # Not sealed (deliberate)
///
/// A rogue downstream `impl IntoBound for Evil` compiles but is benign: it
/// can only return a closed [`BoundValue`] (a `$N` binary block, never
/// spine text), mapping to an existing variant's correct OID. The
/// injection wall is [`Chunk::Rodata`]'s `&'static str` field, not this
/// trait being unforgeable. See the module docs.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be bound into a fragment hole",
    note = "a `{{}}` hole accepts exactly one of: i16, i32, i64, u32, bool, &str, String — there is no raw-text interpolation path"
)]
pub trait IntoBound {
    /// Convert `self` into the closed runtime bind carrier.
    fn into_bound(self) -> BoundValue;
}

impl IntoBound for i16 {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::I16(self)
    }
}
impl IntoBound for i32 {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::I32(self)
    }
}
impl IntoBound for i64 {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::I64(self)
    }
}
impl IntoBound for u32 {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::U32(self)
    }
}
impl IntoBound for bool {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::Bool(self)
    }
}
impl IntoBound for &str {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::Text(self.to_owned())
    }
}
impl IntoBound for String {
    #[inline]
    fn into_bound(self) -> BoundValue {
        BoundValue::Text(self)
    }
}

/// One element of a [`Fragment`]'s skeleton: either a `.rodata` literal or
/// a positional value hole.
///
/// `#[doc(hidden)]` because it is named only by the
/// [`fragment!`](crate::fragment) macro expansion (caller hygiene) — the
/// same pattern as slice-1's `col_seal`. It is *not* a backdoor:
/// [`Chunk::Rodata`] holds a `&'static str`, so a runtime `String` cannot
/// enter it (`E0597`), in-crate *and* cross-crate.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chunk {
    /// A compile-time `.rodata` SQL literal (keyword, punctuation, or a
    /// `Col` identifier). The `&'static str` field is the injection wall.
    Rodata(&'static str),
    /// A positional value hole carrying its bind inline. `$N` is *not*
    /// stored here — it is derived at [`Fragment::build`].
    Hole(BoundValue),
}

/// A sort direction for an [`ORDER BY`](Fragment::order_by) key.
///
/// A closed `enum`, so it emits exactly ` ASC` / ` DESC` `.rodata`
/// literals — never runtime text. The default (no `.asc()`/`.desc()`) emits
/// no direction token, deferring to PostgreSQL's default (`ASC`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// Ascending order (` ASC`).
    Asc,
    /// Descending order (` DESC`).
    Desc,
}

impl Dir {
    /// The `.rodata` SQL token for this direction (leading space included).
    #[inline]
    const fn as_sql(self) -> &'static str {
        match self {
            Dir::Asc => " ASC",
            Dir::Desc => " DESC",
        }
    }
}

/// One `ORDER BY` key: an identifier (always a `.rodata` `&'static str`
/// sourced from [`Col::as_sql`](crate::col::Col::as_sql) /
/// [`AsIdent::ident`](crate::col::AsIdent::ident)) plus an optional
/// direction. Internal — multi-key comma-gluing is free over a `Vec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderTerm {
    ident: &'static str,
    dir: Option<Dir>,
}

/// A composable `WHERE`-clause expression: an ordered sequence of
/// [`Chunk`]s carrying its own bound holes.
///
/// A `Predicate` is *not* a [`Fragment`]: it cannot be
/// [`build`](Fragment::build)-assembled on its own (a boolean expression is
/// not a statement), and it cannot be `AND`-ed onto a spine. It is the
/// algebra element handed to [`Fragment::and_where`].
///
/// # Parenthesisation
///
/// A *leaf* predicate (`col OP $N`, `col IS [NOT] NULL`) carries no
/// internal boolean operator and is emitted bare. A *combination*
/// ([`Predicate::and`] / [`Predicate::or`]) self-wraps in parentheses, so
/// operator precedence can never leak across a composition boundary — see
/// the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "a Predicate does nothing until handed to `Fragment::and_where`"]
pub struct Predicate {
    chunks: Vec<Chunk>,
}

impl Predicate {
    /// Construct a predicate from a pre-built chunk vector.
    ///
    /// `#[doc(hidden)]` — used only by [`ColPredicate`]'s comparators
    /// (caller hygiene). Not a raw-SQL backdoor: [`Chunk::Rodata`] carries a
    /// `&'static str`, so spine text is compile-time-literal only (the moat:
    /// `E0597` on a runtime `String`).
    #[doc(hidden)]
    #[inline]
    pub fn __raw(chunks: Vec<Chunk>) -> Self {
        Self { chunks }
    }

    /// Combine two predicates with `AND`, self-wrapping in parentheses:
    /// `a.and(b)` → `(a AND b)`. See the module docs on parenthesisation.
    #[inline]
    pub fn and(self, other: Predicate) -> Predicate {
        self.combine(" AND ", other)
    }

    /// Combine two predicates with `OR`, self-wrapping in parentheses:
    /// `a.or(b)` → `(a OR b)`. This is the *only* way to express `OR` —
    /// there is no `or_where` on the builder (see the module docs).
    #[inline]
    pub fn or(self, other: Predicate) -> Predicate {
        self.combine(" OR ", other)
    }

    /// `( self <glue> other )` — the self-wrapping combination node.
    fn combine(self, glue: &'static str, other: Predicate) -> Predicate {
        let mut chunks = Vec::with_capacity(self.chunks.len() + other.chunks.len() + 3);
        chunks.push(Chunk::Rodata("("));
        chunks.extend(self.chunks);
        chunks.push(Chunk::Rodata(glue));
        chunks.extend(other.chunks);
        chunks.push(Chunk::Rodata(")"));
        Predicate { chunks }
    }
}

/// Typed comparison combinators on a [`Col`](crate::col::Col) marker — the
/// "better than sqlx" compile-time column↔value type guard.
///
/// A blanket `impl<C: Col> ColPredicate for C {}` gives every
/// `columns!`-minted column marker `eq`/`ne`/`lt`/`le`/`gt`/`ge`. Each takes
/// a value of *exactly* the column's value type
/// (`<Self::Ty as ColType>::Value<'_>`), so a wrong-typed value is `E0308`
/// at compile time — with **no silent widening** (`age.gt(1i32)` on an
/// `i16` column is `E0308`). See the module docs and the
/// `fragment_compile_fail` trybuild harness.
///
/// The produced [`Predicate`] places the identifier as
/// [`Chunk::Rodata`]`(self.as_sql())` and the value as a
/// [`Chunk::Hole`] — preserving the injection wall (identifier from
/// `.rodata`, value as a `$N` bind, never spine text).
pub trait ColPredicate: Col {
    /// `col = $N`.
    #[inline]
    fn eq<'v>(self, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        self.cmp(" = ", v)
    }
    /// `col <> $N`.
    #[inline]
    fn ne<'v>(self, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        self.cmp(" <> ", v)
    }
    /// `col < $N`.
    #[inline]
    fn lt<'v>(self, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        self.cmp(" < ", v)
    }
    /// `col <= $N`.
    #[inline]
    fn le<'v>(self, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        self.cmp(" <= ", v)
    }
    /// `col > $N`.
    #[inline]
    fn gt<'v>(self, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        self.cmp(" > ", v)
    }
    /// `col >= $N`.
    #[inline]
    fn ge<'v>(self, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        self.cmp(" >= ", v)
    }

    /// `col <op> $N` — the shared leaf builder. The identifier is
    /// [`Chunk::Rodata`] (`.rodata`), the operator is a `&'static str`
    /// token, and the value is a [`Chunk::Hole`] (`$N` bind). `op` carries
    /// its surrounding spaces (e.g. `" = "`).
    #[inline]
    fn cmp<'v>(self, op: &'static str, v: <Self::Ty as ColType>::Value<'v>) -> Predicate
    where
        <Self::Ty as ColType>::Value<'v>: IntoBound,
    {
        Predicate::__raw(vec![
            Chunk::Rodata(self.as_sql()),
            Chunk::Rodata(op),
            Chunk::Hole(v.into_bound()),
        ])
    }

    /// `col IS NULL` — no value, no `$N` hole.
    #[inline]
    fn is_null(self) -> Predicate {
        Predicate::__raw(vec![Chunk::Rodata(self.as_sql()), Chunk::Rodata(" IS NULL")])
    }

    /// `col IS NOT NULL` — no value, no `$N` hole.
    #[inline]
    fn is_not_null(self) -> Predicate {
        Predicate::__raw(vec![
            Chunk::Rodata(self.as_sql()),
            Chunk::Rodata(" IS NOT NULL"),
        ])
    }
}

impl<C: Col> ColPredicate for C {}

/// A runtime SQL statement: a static **spine** plus typed clause slots
/// (`WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`). Owned (no lifetime parameter) —
/// see the module docs for why.
///
/// Build the spine with the [`fragment!`](crate::fragment) macro, then layer
/// clauses with [`and_where`](Self::and_where) / [`order_by`](Self::order_by)
/// / [`limit`](Self::limit) / [`offset`](Self::offset); compose spine pieces
/// with [`append`](Self::append); assemble the terminal SQL + bind blocks
/// with [`build`](Self::build).
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "a Fragment does nothing until `build()` assembles it"]
pub struct Fragment {
    spine: Vec<Chunk>,
    where_clause: Option<Predicate>,
    order: Vec<OrderTerm>,
    limit: Option<BoundValue>,
    offset: Option<BoundValue>,
}

impl Fragment {
    /// Construct a fragment from a pre-built spine chunk vector. Clause
    /// slots (`WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`) start empty.
    ///
    /// `#[doc(hidden)]` — this is the [`fragment!`](crate::fragment) macro's
    /// sole constructor (caller hygiene; a macro expands in caller scope and
    /// must name a reachable path). It is *not* a public raw-SQL backdoor:
    /// [`Chunk::Rodata`] carries a `&'static str`, so the only way to put
    /// text on the spine is a compile-time literal (the moat: `E0597` on a
    /// runtime `String`).
    #[doc(hidden)]
    #[inline]
    pub fn __from_chunks(chunks: Vec<Chunk>) -> Self {
        Self {
            spine: chunks,
            where_clause: None,
            order: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Compose spine pieces by concatenating their spine chunk vectors, and
    /// fold the clause slots totally (never silently dropping):
    ///
    /// - spines are concatenated (renumbering stays contiguous at
    ///   [`build`](Self::build), `$N` derived not stored);
    /// - `WHERE` clauses are `AND`-folded (`self AND other`, parenthesised);
    /// - `ORDER BY` keys are concatenated (`self`'s first);
    /// - `LIMIT` / `OFFSET` keep `self`'s if present, else take `other`'s.
    ///
    /// The intended use is composing static spine pieces *before* adding
    /// clauses; the total fold means appending two clause-bearing fragments
    /// is well-defined rather than a silent drop.
    pub fn append(mut self, mut other: Fragment) -> Fragment {
        self.spine.append(&mut other.spine);
        self.where_clause = match (self.where_clause.take(), other.where_clause.take()) {
            (None, w) | (w, None) => w,
            (Some(a), Some(b)) => Some(a.and(b)),
        };
        self.order.append(&mut other.order);
        if self.limit.is_none() {
            self.limit = other.limit.take();
        }
        if self.offset.is_none() {
            self.offset = other.offset.take();
        }
        self
    }

    /// Add a typed [`Predicate`] to the `WHERE` clause, `AND`-folded onto
    /// any existing filter (`existing AND p`, parenthesised). This is the
    /// **sole** filter combinator: there is no `or_where` (it would be a
    /// silent precedence footgun — see the module docs). `OR` is expressed
    /// via [`Predicate::or`].
    pub fn and_where(mut self, p: Predicate) -> Fragment {
        self.where_clause = Some(match self.where_clause.take() {
            None => p,
            Some(existing) => existing.and(p),
        });
        self
    }

    /// Append an `ORDER BY` key. Accepts any [`AsIdent`] — a static
    /// [`Col`](crate::col::Col) marker *or* a runtime `DynCol` (the `?sort=`
    /// bridge) — so there is no raw-`&str` ordering path (`&str: AsIdent` is
    /// `E0277`). Direction defaults to PostgreSQL's `ASC`; set it with
    /// [`asc`](Self::asc) / [`desc`](Self::desc) / [`dir`](Self::dir).
    pub fn order_by<I: AsIdent>(mut self, key: I) -> Fragment {
        self.order.push(OrderTerm {
            ident: key.ident(),
            dir: None,
        });
        self
    }

    /// Set the direction of the most recently added [`order_by`](Self::order_by)
    /// key. A no-op if no key has been added yet.
    pub fn dir(mut self, dir: Dir) -> Fragment {
        if let Some(term) = self.order.last_mut() {
            term.dir = Some(dir);
        }
        self
    }

    /// Mark the most recent `ORDER BY` key ascending (` ASC`). Shorthand for
    /// [`dir(Dir::Asc)`](Self::dir).
    #[inline]
    pub fn asc(self) -> Fragment {
        self.dir(Dir::Asc)
    }

    /// Mark the most recent `ORDER BY` key descending (` DESC`). Shorthand
    /// for [`dir(Dir::Desc)`](Self::dir).
    #[inline]
    pub fn desc(self) -> Fragment {
        self.dir(Dir::Desc)
    }

    /// Set the `LIMIT` to a bound `$N` value (`bigint`, OID 20). A `LIMIT`
    /// is *data*, so it travels as a `$N` bind, never spine text.
    pub fn limit(mut self, n: i64) -> Fragment {
        self.limit = Some(BoundValue::I64(n));
        self
    }

    /// Set the `OFFSET` to a bound `$N` value (`bigint`, OID 20). An
    /// `OFFSET` is *data*, so it travels as a `$N` bind, never spine text.
    pub fn offset(mut self, n: i64) -> Fragment {
        self.offset = Some(BoundValue::I64(n));
        self
    }

    /// Assemble the terminal SQL text (with contiguous `$1..$N` holes) and
    /// the ordered list of binary-encoded bind blocks.
    ///
    /// A single left-to-right counter walks the chunks in **fixed clause
    /// order** — spine, then ` WHERE …` (once, iff a filter is present),
    /// then ` ORDER BY …`, then ` LIMIT $N`, then ` OFFSET $N`. `Rodata`
    /// chunks append their literal verbatim; `Hole` chunks emit the next
    /// `$N`, push the bind's OID, and encode the bind block — all at the
    /// same visit, so SQL placeholders and bind blocks can never fall out of
    /// step. Call order is decoupled from SQL clause order (a robustness
    /// bonus: `.limit(10).and_where(…)` emits the `WHERE` before the
    /// `LIMIT`).
    ///
    /// # Errors
    ///
    /// [`WriteBufFull`] if any bind exceeds the bounded [`WriteBuf`]
    /// capacity (e.g. an oversized text bind). Surfaced as a classified
    /// error — never a panic.
    pub fn build(&self) -> Result<Assembled, WriteBufFull> {
        let mut sql = String::new();
        let mut binds: Vec<Vec<u8>> = Vec::new();
        let mut oids: Vec<u32> = Vec::new();
        let mut n: u32 = 0;

        // 1. Static spine.
        for chunk in &self.spine {
            emit_chunk(chunk, &mut sql, &mut binds, &mut oids, &mut n)?;
        }

        // 2. WHERE — emitted exactly once iff a filter is present, so a
        //    dangling AND/WHERE is structurally unreachable.
        if let Some(p) = &self.where_clause {
            sql.push_str(" WHERE ");
            for chunk in &p.chunks {
                emit_chunk(chunk, &mut sql, &mut binds, &mut oids, &mut n)?;
            }
        }

        // 3. ORDER BY — comma-glued keys, each with its optional direction.
        if let Some((first, rest)) = self.order.split_first() {
            sql.push_str(" ORDER BY ");
            sql.push_str(first.ident);
            if let Some(d) = first.dir {
                sql.push_str(d.as_sql());
            }
            for term in rest {
                sql.push_str(", ");
                sql.push_str(term.ident);
                if let Some(d) = term.dir {
                    sql.push_str(d.as_sql());
                }
            }
        }

        // 4. LIMIT / OFFSET — bound $N values.
        if let Some(v) = &self.limit {
            sql.push_str(" LIMIT ");
            emit_chunk(&Chunk::Hole(v.clone()), &mut sql, &mut binds, &mut oids, &mut n)?;
        }
        if let Some(v) = &self.offset {
            sql.push_str(" OFFSET ");
            emit_chunk(&Chunk::Hole(v.clone()), &mut sql, &mut binds, &mut oids, &mut n)?;
        }

        Ok(Assembled { sql, binds, oids })
    }
}

/// Emit one chunk into the running assembly: `Rodata` appends its literal;
/// `Hole` emits the next `$N`, its OID, and its encoded bind block — all in
/// one pass so placeholders and binds can never desync.
#[inline]
fn emit_chunk(
    chunk: &Chunk,
    sql: &mut String,
    binds: &mut Vec<Vec<u8>>,
    oids: &mut Vec<u32>,
    n: &mut u32,
) -> Result<(), WriteBufFull> {
    match chunk {
        Chunk::Rodata(s) => sql.push_str(s),
        Chunk::Hole(v) => {
            *n += 1;
            sql.push('$');
            sql.push_str(&n.to_string());
            oids.push(v.oid());
            binds.push(v.encode_block()?);
        }
    }
    Ok(())
}

/// The testable terminal output of [`Fragment::build`].
///
/// - [`sql`](Self::sql) — the assembled SQL text with contiguous `$1..$N`.
/// - [`binds`](Self::binds) — the ordered binary bind blocks, one per hole,
///   each a `{i32 length prefix, payload bytes}` block (the PostgreSQL Bind
///   parameter layout produced via
///   [`EncodeBinary`](bsql_postgres_proto::decode::EncodeBinary)).
/// - [`oids`](Self::oids) — the ordered parameter type OIDs, one per hole.
///
/// The actual wire send (a runtime-count param writer + Bind/Execute
/// integration) is a later slice; this terminal makes slice 2 fully
/// testable without a live server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assembled {
    /// The assembled SQL with contiguous `$1..$N` placeholders.
    pub sql: String,
    /// The ordered binary bind blocks (`{i32 len, bytes}` each).
    pub binds: Vec<Vec<u8>>,
    /// The ordered parameter type OIDs.
    pub oids: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::{Assembled, BoundValue, Chunk, ColPredicate, Fragment, IntoBound};
    use crate::fragment;
    use bsql_postgres_proto::WriteBuf;
    use bsql_postgres_proto::decode::EncodeBinary;

    // A vocabulary covering every column type for the slice-3 combinator
    // tests (the typed predicate guard, where-tree, order/limit/offset).
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

    // `expect`/`unwrap` are crate-denied even in tests (see owned_row.rs);
    // assemble via match.
    fn built(f: Fragment) -> Assembled {
        match f.build() {
            Ok(a) => a,
            Err(e) => panic!("build failed: {e}"),
        }
    }

    /// Reference: encode one value through the *real* proto `EncodeBinary`
    /// using the same length-prefix helper `BoundValue::encode_block` uses.
    /// This is the differential oracle — `Fragment::build` must produce
    /// byte-identical bind blocks.
    fn reference_block<T: EncodeBinary>(v: &T) -> Vec<u8> {
        let mut buf = WriteBuf::new();
        match buf.with_i32_length_prefixed_body(|w| v.encode_to(w)) {
            Ok(()) => {}
            Err(e) => panic!("reference block did not fit: {e}"),
        }
        buf.as_bytes().to_vec()
    }

    #[test]
    fn fragment_linear_holes_number_in_source_order() {
        // PROBE1: contiguous holes + ordered binds.
        let f = fragment!(
            "SELECT id FROM users WHERE id = {} AND active = {}",
            42i32,
            true
        );
        let a = built(f);
        assert_eq!(a.sql, "SELECT id FROM users WHERE id = $1 AND active = $2");
        assert_eq!(a.oids, vec![23, 16]); // INT4, BOOL
        assert_eq!(a.binds.len(), 2);
        // Bind 0 is the i32, bind 1 is the bool — source order preserved.
        assert_eq!(a.binds[0], reference_block(&42i32));
        assert_eq!(a.binds[1], reference_block(&true));
        // Literal byte pins for the i32 and bool blocks.
        assert_eq!(a.binds[0], vec![0, 0, 0, 4, 0, 0, 0, 42]);
        assert_eq!(a.binds[1], vec![0, 0, 0, 1, 1]);
    }

    #[test]
    fn encoded_bind_bytes_match_proto_for_all_six_types() {
        // Each of the six bindable types, encoded via Fragment::build,
        // must equal the proto EncodeBinary output (differential).
        let cases: Vec<(BoundValue, Vec<u8>, u32)> = vec![
            (BoundValue::I16(-2), reference_block(&-2i16), <i16 as EncodeBinary>::OID),
            (BoundValue::I32(42), reference_block(&42i32), <i32 as EncodeBinary>::OID),
            (BoundValue::I64(1), reference_block(&1i64), <i64 as EncodeBinary>::OID),
            (BoundValue::U32(26), reference_block(&26u32), <u32 as EncodeBinary>::OID),
            (BoundValue::Bool(true), reference_block(&true), <bool as EncodeBinary>::OID),
            (
                BoundValue::Text(String::from("dynamic")),
                reference_block(&"dynamic"),
                <&str as EncodeBinary>::OID,
            ),
        ];
        for (val, expected_block, expected_oid) in cases {
            let f = Fragment::__from_chunks(vec![
                Chunk::Rodata("x = "),
                Chunk::Hole(val.clone()),
            ]);
            let a = built(f);
            assert_eq!(a.sql, "x = $1");
            assert_eq!(a.oids, vec![expected_oid], "oid mismatch for {val:?}");
            assert_eq!(a.binds, vec![expected_block], "block mismatch for {val:?}");
        }
    }

    #[test]
    fn encode_block_literal_byte_pins() {
        // Hand-computed byte pins (independent of the proto oracle) for
        // each type, locking the exact wire layout.
        let pins: Vec<(BoundValue, Vec<u8>)> = vec![
            (BoundValue::I16(-2), vec![0, 0, 0, 2, 0xff, 0xfe]),
            (BoundValue::I32(42), vec![0, 0, 0, 4, 0, 0, 0, 42]),
            (BoundValue::I64(1), vec![0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1]),
            (BoundValue::U32(26), vec![0, 0, 0, 4, 0, 0, 0, 0x1a]),
            (BoundValue::Bool(false), vec![0, 0, 0, 1, 0]),
            (BoundValue::Bool(true), vec![0, 0, 0, 1, 1]),
            (
                BoundValue::Text(String::from("hi")),
                vec![0, 0, 0, 2, 0x68, 0x69],
            ),
            (
                BoundValue::Text(String::from("dynamic")),
                vec![0, 0, 0, 7, 0x64, 0x79, 0x6e, 0x61, 0x6d, 0x69, 0x63],
            ),
        ];
        for (val, expected) in pins {
            let f = Fragment::__from_chunks(vec![Chunk::Hole(val.clone())]);
            let a = built(f);
            assert_eq!(a.binds, vec![expected], "byte pin mismatch for {val:?}");
        }
    }

    #[test]
    fn oid_folds_from_proto_single_source() {
        // BoundValue::oid() must equal the proto EncodeBinary OID — the
        // single drift-pinned source of truth.
        assert_eq!(BoundValue::I16(0).oid(), <i16 as EncodeBinary>::OID);
        assert_eq!(BoundValue::I32(0).oid(), <i32 as EncodeBinary>::OID);
        assert_eq!(BoundValue::I64(0).oid(), <i64 as EncodeBinary>::OID);
        assert_eq!(BoundValue::U32(0).oid(), <u32 as EncodeBinary>::OID);
        assert_eq!(BoundValue::Bool(false).oid(), <bool as EncodeBinary>::OID);
        assert_eq!(BoundValue::Text(String::new()).oid(), <&str as EncodeBinary>::OID);
        // Concrete wire OIDs (pin against proto's catalog).
        assert_eq!(BoundValue::I16(0).oid(), 21);
        assert_eq!(BoundValue::I32(0).oid(), 23);
        assert_eq!(BoundValue::I64(0).oid(), 20);
        assert_eq!(BoundValue::U32(0).oid(), 26);
        assert_eq!(BoundValue::Bool(false).oid(), 16);
        assert_eq!(BoundValue::Text(String::new()).oid(), 25);
    }

    #[test]
    fn into_bound_maps_each_type_to_its_variant() {
        assert_eq!(IntoBound::into_bound(7i16), BoundValue::I16(7));
        assert_eq!(IntoBound::into_bound(7i32), BoundValue::I32(7));
        assert_eq!(IntoBound::into_bound(7i64), BoundValue::I64(7));
        assert_eq!(IntoBound::into_bound(7u32), BoundValue::U32(7));
        assert_eq!(IntoBound::into_bound(true), BoundValue::Bool(true));
        assert_eq!(
            IntoBound::into_bound("hi"),
            BoundValue::Text(String::from("hi"))
        );
        assert_eq!(
            IntoBound::into_bound(String::from("yo")),
            BoundValue::Text(String::from("yo"))
        );
    }

    #[test]
    fn two_fragment_compose_renumbers_contiguously() {
        // PROBE2: the second fragment's hole ($1 standalone) renumbers to
        // $2 in the combined output. append never touches assembled text.
        let left = fragment!("a = {}", 1i32);
        let right = fragment!(" AND b = {}", 2i32);
        // Each fragment numbers from $1 on its own.
        assert_eq!(built(left.clone()).sql, "a = $1");
        assert_eq!(built(right.clone()).sql, " AND b = $1");
        let combined = built(left.append(right));
        assert_eq!(combined.sql, "a = $1 AND b = $2");
        assert_eq!(combined.oids, vec![23, 23]);
        assert_eq!(combined.binds[0], reference_block(&1i32));
        assert_eq!(combined.binds[1], reference_block(&2i32));
    }

    #[test]
    fn three_way_compose_is_associative() {
        // PROBE3: left- and right-nested append give byte-identical
        // Assembled. $N is derived at build(), so structure is all that
        // matters.
        let mk = || {
            (
                fragment!("a = {}", 1i32),
                fragment!(" AND b = {}", 2i32),
                fragment!(" AND c = {}", 3i32),
            )
        };
        let (f1, f2, f3) = mk();
        let left_nested = built(f1.append(f2).append(f3));
        let (g1, g2, g3) = mk();
        let right_nested = built(g1.append(g2.append(g3)));
        assert_eq!(left_nested, right_nested);
        assert_eq!(left_nested.sql, "a = $1 AND b = $2 AND c = $3");
        assert_eq!(left_nested.oids, vec![23, 23, 23]);
    }

    #[test]
    fn bound_value_never_becomes_a_placeholder_or_spine_text() {
        // PROBE4: a hostile value string travels in the bind BODY, never
        // the spine. The spine has exactly one $1 and no DROP.
        let payload = "$1; DROP TABLE users; --";
        let f = fragment!("SELECT * FROM t WHERE note = {}", payload);
        let a = built(f);
        assert_eq!(a.sql, "SELECT * FROM t WHERE note = $1");
        assert!(!a.sql.contains("DROP"));
        // The payload bytes are in the single bind block (after the 4-byte
        // length prefix), proving it is a binary $1 value not text.
        let block = &a.binds[0];
        assert_eq!(a.binds.len(), 1);
        assert_eq!(block, &reference_block(&payload));
        // First two payload bytes are '$' '1' — the literal value content.
        assert_eq!(&block[4..6], b"$1");
    }

    #[test]
    fn author_dollar_quote_in_skeleton_is_verbatim_rodata() {
        // PROBE5: a `$tag$` dollar-quote in the SKELETON stays verbatim;
        // it is Rodata, structurally distinct from a Hole, so it can never
        // be mis-renumbered.
        let f = fragment!("SELECT $tag$lit$tag$ , {}", 7i32);
        let a = built(f);
        assert_eq!(a.sql, "SELECT $tag$lit$tag$ , $1");
        assert_eq!(a.oids, vec![23]);
    }

    #[test]
    fn zero_hole_fragment_assembles_to_pure_spine() {
        let f = fragment!("SELECT 1");
        let a = built(f);
        assert_eq!(a.sql, "SELECT 1");
        assert!(a.binds.is_empty());
        assert!(a.oids.is_empty());
    }

    #[test]
    fn escaped_braces_are_literal_not_holes() {
        // `{{` / `}}` are literal braces, not holes. One real hole.
        let f = fragment!("data = {{not a hole}} AND id = {}", 9i32);
        let a = built(f);
        assert_eq!(a.sql, "data = {not a hole} AND id = $1");
        assert_eq!(a.oids, vec![23]);
    }

    #[test]
    fn oversized_text_bind_surfaces_write_buf_full_not_panic() {
        // A text bind larger than the bounded WriteBuf returns
        // Err(WriteBufFull), honoring deny(unwrap_used).
        let huge = "x".repeat(1_000_000);
        let f = Fragment::__from_chunks(vec![Chunk::Hole(BoundValue::Text(huge))]);
        let result = f.build();
        assert!(result.is_err(), "oversized text bind must error, not panic");
    }

    // ----------------------------------------------------------------
    // SLICE 3 — the typed combinator surface.
    // ----------------------------------------------------------------

    /// A static `SELECT … FROM …` spine, as the `fragment!` macro would
    /// produce one (pure `Rodata`, no holes).
    fn spine(s: &'static str) -> Fragment {
        Fragment::__from_chunks(vec![Chunk::Rodata(s)])
    }

    #[test]
    fn single_predicate_assembles_with_one_bound_hole() {
        // SLICE3-1: one typed predicate -> `col = $1`, value is a $1 bind.
        let a = built(spine("SELECT id FROM users").and_where(users::id.eq(7i32)));
        assert_eq!(a.sql, "SELECT id FROM users WHERE id = $1");
        assert_eq!(a.oids, vec![23]); // INT4
        assert_eq!(a.binds.len(), 1);
        assert_eq!(a.binds[0], reference_block(&7i32));
        // The identifier is spine text; the value rides the bind body.
        assert!(!a.sql.contains('7'));
    }

    #[test]
    fn the_brief_chain_assembles_exactly() {
        // SLICE3-2: the make-or-break headline chain.
        let a = built(
            spine("SELECT id FROM users")
                .and_where(users::age.gt(18i16))
                .and_where(users::name.eq("al"))
                .order_by(users::name),
        );
        assert_eq!(
            a.sql,
            "SELECT id FROM users WHERE (age > $1 AND name = $2) ORDER BY name"
        );
        assert_eq!(a.oids, vec![21, 25]); // INT2, TEXT
        assert_eq!(a.binds.len(), 2);
        assert_eq!(a.binds[0], reference_block(&18i16));
        assert_eq!(a.binds[1], reference_block(&"al"));
        // Literal byte pins.
        assert_eq!(a.binds[0], vec![0, 0, 0, 2, 0, 18]); // i16 18 BE
        assert_eq!(a.binds[1], vec![0, 0, 0, 2, 97, 108]); // "al"
    }

    #[test]
    fn each_comparator_emits_the_right_operator() {
        let cases: [(Fragment, &str); 6] = [
            (spine("S").and_where(users::id.eq(1i32)), "S WHERE id = $1"),
            (spine("S").and_where(users::id.ne(1i32)), "S WHERE id <> $1"),
            (spine("S").and_where(users::id.lt(1i32)), "S WHERE id < $1"),
            (spine("S").and_where(users::id.le(1i32)), "S WHERE id <= $1"),
            (spine("S").and_where(users::id.gt(1i32)), "S WHERE id > $1"),
            (spine("S").and_where(users::id.ge(1i32)), "S WHERE id >= $1"),
        ];
        for (f, expected) in cases {
            assert_eq!(built(f).sql, expected);
        }
    }

    #[test]
    fn two_and_where_fold_into_one_parenthesised_node() {
        // SLICE3-3: two top-level `.and_where` AND-fold to `(a AND b)`.
        let a = built(
            spine("S")
                .and_where(users::id.eq(1i32))
                .and_where(users::age.lt(5i16)),
        );
        assert_eq!(a.sql, "S WHERE (id = $1 AND age < $2)");
        assert_eq!(a.oids, vec![23, 21]);
    }

    #[test]
    fn predicate_or_self_wraps_and_nests_correctly() {
        // SLICE3-4: OR is expressed only via Predicate::or, which wraps.
        // `id = $1 AND (age > $2 OR age < $3)` — the call-order grouping is
        // preserved and parenthesised, never mis-precedenced.
        let inner = users::age.gt(10i16).or(users::age.lt(0i16));
        let a = built(spine("S").and_where(users::id.eq(1i32).and(inner)));
        assert_eq!(a.sql, "S WHERE (id = $1 AND (age > $2 OR age < $3))");
        assert_eq!(a.oids, vec![23, 21, 21]);
    }

    #[test]
    fn deeply_nested_predicate_tree_parenthesises_unambiguously() {
        // SLICE3-5: ((id=$1 OR id=$2) AND (age>$3 OR age<$4)).
        let left = users::id.eq(1i32).or(users::id.eq(2i32));
        let right = users::age.gt(3i16).or(users::age.lt(4i16));
        let a = built(spine("SELECT 1").and_where(left.and(right)));
        assert_eq!(
            a.sql,
            "SELECT 1 WHERE ((id = $1 OR id = $2) AND (age > $3 OR age < $4))"
        );
        assert_eq!(a.oids, vec![23, 23, 21, 21]);
    }

    #[test]
    fn is_null_and_is_not_null_emit_no_hole() {
        // SLICE3-6: IS [NOT] NULL has no value and consumes no $N.
        let a = built(
            spine("S")
                .and_where(users::name.is_null())
                .and_where(users::id.gt(5i32)),
        );
        assert_eq!(a.sql, "S WHERE (name IS NULL AND id > $1)");
        assert_eq!(a.oids, vec![23]);
        assert_eq!(a.binds.len(), 1); // no hole/shift for IS NULL

        let b = built(spine("S").and_where(users::name.is_not_null()));
        assert_eq!(b.sql, "S WHERE name IS NOT NULL");
        assert!(b.binds.is_empty());
    }

    #[test]
    fn order_by_accepts_a_static_col() {
        let a = built(spine("S").order_by(users::id).desc());
        assert_eq!(a.sql, "S ORDER BY id DESC");

        let b = built(spine("S").order_by(users::name).asc());
        assert_eq!(b.sql, "S ORDER BY name ASC");

        // No direction -> no token (PG default ASC).
        let c = built(spine("S").order_by(users::age));
        assert_eq!(c.sql, "S ORDER BY age");
    }

    #[test]
    fn order_by_accepts_a_runtime_dyncol_the_sort_bridge() {
        // SLICE3-7: the ?sort= bridge — a runtime string maps to a DynCol
        // (the allowlist), then orders with NO raw-&str path.
        let key = match users::DynCol::parse("age") {
            Ok(k) => k,
            Err(e) => panic!("parse failed: {e}"),
        };
        let a = built(spine("S").order_by(key).asc());
        assert_eq!(a.sql, "S ORDER BY age ASC");
    }

    #[test]
    fn multi_key_order_by_comma_glues() {
        let a = built(spine("S").order_by(users::id).desc().order_by(users::name));
        assert_eq!(a.sql, "S ORDER BY id DESC, name");
    }

    #[test]
    fn limit_and_offset_are_bound_dollar_n_values() {
        // SLICE3-8: LIMIT/OFFSET are data -> $N binds (bigint, OID 20),
        // numbered after WHERE holes.
        let a = built(
            spine("S")
                .and_where(users::id.eq(1i32))
                .limit(10)
                .offset(20),
        );
        assert_eq!(a.sql, "S WHERE id = $1 LIMIT $2 OFFSET $3");
        assert_eq!(a.oids, vec![23, 20, 20]);
        assert_eq!(a.binds.len(), 3);
        assert_eq!(a.binds[1], reference_block(&10i64));
        assert_eq!(a.binds[2], reference_block(&20i64));
        // LIMIT/OFFSET literals are NOT in the spine text.
        assert!(!a.sql.contains("10"));
        assert!(!a.sql.contains("20"));
    }

    #[test]
    fn six_hole_mix_numbers_contiguously_across_all_clauses() {
        // SLICE3-9: 4 where holes + limit + offset = $1..$6 contiguous.
        let a = built(
            spine("SELECT id FROM users")
                .and_where(
                    users::id
                        .eq(1i32)
                        .and(users::name.eq("x"))
                        .and(users::age.gt(2i16).or(users::active.eq(true))),
                )
                .order_by(users::name)
                .desc()
                .limit(5)
                .offset(15),
        );
        assert_eq!(
            a.sql,
            "SELECT id FROM users WHERE ((id = $1 AND name = $2) AND (age > $3 OR active = $4)) \
             ORDER BY name DESC LIMIT $5 OFFSET $6"
        );
        assert_eq!(a.oids, vec![23, 25, 21, 16, 20, 20]);
        assert_eq!(a.binds.len(), 6);
        // Exactly six placeholders, each $1..$6 appearing once.
        assert_eq!(a.sql.matches('$').count(), 6);
        for k in 1..=6u32 {
            let tok = format!("${k}");
            assert_eq!(a.sql.matches(&tok).count(), 1, "{tok} must appear once");
        }
    }

    #[test]
    fn call_order_decoupled_from_sql_clause_order() {
        // SLICE3-10: clauses emit in fixed SQL order regardless of the
        // builder call order (a robustness bonus of the slot design).
        let a = built(
            spine("S")
                .limit(10)
                .order_by(users::id)
                .and_where(users::active.eq(true)),
        );
        assert_eq!(a.sql, "S WHERE active = $1 ORDER BY id LIMIT $2");
        assert_eq!(a.oids, vec![16, 20]);
    }

    #[test]
    fn conditional_filter_loop_keeps_one_fragment_type() {
        // SLICE3-11: the bread-and-butter dynamic-filter pattern — a loop
        // re-binds `q` with `.and_where`. This compiles ONLY because the
        // accumulate-then-emit `Fragment` is ONE type (a typestate
        // `Fragment<NoWhere|HasWhere>` would make the loop var E0308).
        let mut q = spine("SELECT id FROM users");
        let want_active: Option<bool> = Some(true);
        let max_age: Option<i16> = Some(40);
        if let Some(b) = want_active {
            q = q.and_where(users::active.eq(b));
        }
        if let Some(m) = max_age {
            q = q.and_where(users::age.lt(m));
        }
        let a = built(q);
        assert_eq!(
            a.sql,
            "SELECT id FROM users WHERE (active = $1 AND age < $2)"
        );
        assert_eq!(a.oids, vec![16, 21]);
    }

    #[test]
    fn predicate_value_is_a_bind_never_spine_text() {
        // SLICE3-12: a hostile value rides the bind body; the spine has
        // exactly one $1 and no injected SQL.
        let payload = "$2 OR 1=1; DROP TABLE users; --";
        let a = built(spine("SELECT * FROM users").and_where(users::name.eq(payload)));
        assert_eq!(a.sql, "SELECT * FROM users WHERE name = $1");
        assert_eq!(a.sql.matches('$').count(), 1);
        assert!(!a.sql.contains("DROP"));
        assert_eq!(a.binds.len(), 1);
        assert_eq!(a.binds[0], reference_block(&payload));
        // The payload bytes follow the 4-byte length prefix.
        assert_eq!(&a.binds[0][4..6], b"$2");
    }

    #[test]
    fn borrowed_text_value_from_a_dropped_temp_is_copied_into_the_hole() {
        // SLICE3-13: a &str argument that outlives only a dropped String is
        // fine — into_bound copies into an owned BoundValue::Text, so no
        // lifetime leaks into the owned Predicate/Fragment.
        let a = {
            let owned = String::from("dynamic_name");
            spine("S").and_where(users::name.eq(owned.as_str()))
            // `owned` dropped here; the Fragment owns a copy.
        };
        let a = built(a);
        assert_eq!(a.sql, "S WHERE name = $1");
        assert_eq!(&a.binds[0][4..], b"dynamic_name");
    }

    #[test]
    fn all_six_column_types_bind_with_their_oids() {
        let a = built(
            spine("S").and_where(
                users::id
                    .eq(1i32)
                    .and(users::name.eq("n"))
                    .and(users::age.eq(2i16))
                    .and(users::active.eq(true))
                    .and(users::big.eq(3i64))
                    .and(users::ref_oid.eq(4u32)),
            ),
        );
        assert_eq!(a.oids, vec![23, 25, 21, 16, 20, 26]);
    }

    #[test]
    fn dir_is_a_noop_with_no_order_key() {
        // `.desc()` before any order_by silently no-ops (no panic, no token).
        let a = built(spine("S").desc());
        assert_eq!(a.sql, "S");
    }

    #[test]
    fn append_and_folds_two_where_bearing_fragments() {
        // append is total: two clause-bearing fragments compose, never drop.
        let left = spine("a").and_where(users::id.eq(1i32));
        let right = spine(" b").and_where(users::age.gt(2i16)).limit(9);
        let a = built(left.append(right));
        assert_eq!(a.sql, "a b WHERE (id = $1 AND age > $2) LIMIT $3");
        assert_eq!(a.oids, vec![23, 21, 20]);
    }
}
