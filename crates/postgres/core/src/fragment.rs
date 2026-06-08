//! The sealed `Fragment` value type — the runtime SQL skeleton that backs
//! the typed dynamic-SQL builder (slice 2: the algebra core).
//!
//! # What a `Fragment` is
//!
//! A [`Fragment`] is a *runtime value*: an ordered sequence of
//! [`Chunk`]s, where each chunk is either
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
//! [`Fragment::build`] by a single left-to-right counter walking the chunk
//! vector. Therefore composition ([`Fragment::append`]) is pure structural
//! concatenation of the chunk vectors that never touches assembled text,
//! and renumbering is automatically contiguous and *associative* over the
//! combined vector on the next `build()`. This is the slice-3 keystone: a
//! standalone fragment whose hole is `$1`, after `append`, has that hole
//! renumbered to `$2` in the combined output with zero text re-parse.
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

/// A runtime SQL skeleton: an ordered sequence of [`Chunk`]s. Owned (no
/// lifetime parameter) — see the module docs for why.
///
/// Build one with the [`fragment!`](crate::fragment) macro; compose with
/// [`Fragment::append`]; assemble the terminal SQL + bind blocks with
/// [`Fragment::build`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    chunks: Vec<Chunk>,
}

impl Fragment {
    /// Construct a fragment from a pre-built chunk vector.
    ///
    /// `#[doc(hidden)]` — this is the [`fragment!`](crate::fragment) macro's
    /// sole constructor (caller hygiene; a macro expands in caller scope and
    /// must name a reachable path). It is *not* a public raw-SQL backdoor:
    /// [`Chunk::Rodata`] carries a `&'static str`, so the only way to put
    /// text on the spine is a compile-time literal (the moat: `E0597` on a
    /// runtime `String`).
    #[doc(hidden)]
    #[inline]
    #[must_use]
    pub fn __from_chunks(chunks: Vec<Chunk>) -> Self {
        Self { chunks }
    }

    /// Compose two fragments by concatenating their chunk vectors.
    ///
    /// This is the slice-3 seam. Because `$N` is derived at
    /// [`build`](Self::build) and never stored, this is pure structural
    /// concatenation — the second fragment's holes are renumbered to follow
    /// the first's automatically and contiguously on the next `build()`,
    /// with zero text re-parse. Associative: `a.append(b).append(c)` and
    /// `a.append(b.append(c))` produce byte-identical assemblies.
    #[inline]
    #[must_use]
    pub fn append(mut self, mut other: Fragment) -> Fragment {
        self.chunks.append(&mut other.chunks);
        self
    }

    /// Assemble the terminal SQL text (with contiguous `$1..$N` holes) and
    /// the ordered list of binary-encoded bind blocks.
    ///
    /// A single left-to-right counter walks the chunk vector once:
    /// `Rodata` chunks append their literal verbatim; `Hole` chunks emit
    /// the next `$N`, push the bind's OID, and encode the bind block — all
    /// at the same visit, so SQL placeholders and bind blocks can never
    /// fall out of step (they are produced by one pass over one vector).
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
        for chunk in &self.chunks {
            match chunk {
                Chunk::Rodata(s) => sql.push_str(s),
                Chunk::Hole(v) => {
                    n += 1;
                    sql.push('$');
                    sql.push_str(&n.to_string());
                    oids.push(v.oid());
                    binds.push(v.encode_block()?);
                }
            }
        }
        Ok(Assembled { sql, binds, oids })
    }
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
    use super::{Assembled, BoundValue, Chunk, Fragment, IntoBound};
    use crate::fragment;
    use bsql_postgres_proto::WriteBuf;
    use bsql_postgres_proto::decode::EncodeBinary;

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
}
