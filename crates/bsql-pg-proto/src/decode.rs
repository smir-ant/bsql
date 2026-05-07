//! Row-schema + row-body decoding primitives. Phase 1c-2a.
//!
//! `bsql-pg-proto` owns the raw wire encoding of a result-set: the
//! `RowDescription` frame tells us column count, type OIDs, and per-column
//! format codes; each `DataRow` frame carries the column values. This
//! module parses `RowDescription` into [`RowDesc`] (shared between
//! [`crate::Action::StreamRow`] and [`crate::Reply::QueryComplete`]) and
//! will host the `DataRow` body parser + typed decoders in 1c-2b/c.
//!
//! # Why POD + bounded capacity
//!
//! The crate is `no_alloc`. `RowDesc` is a flat inline struct holding
//! a `[ColumnDesc; MAX_ROW_COLUMNS]` array alongside a `u16` populated
//! count — `Copy`, no `Drop`. Result-sets with more than
//! [`MAX_ROW_COLUMNS`] columns land in
//! [`crate::ProtocolError::TooManyColumns`] at parse time (tier-2
//! structural — the bound is enforced at construction, no silent
//! truncation).
//!
//! # Tier notes
//!
//! Schema ingest is **tier-2 structural**. The parser produces `RowDesc`
//! only on well-formed payloads (`MalformedRowDescription` on framing
//! errors, `UnexpectedFormatCode` on values outside `{0, 1}` — round-4
//! finding #5). A malformed response tears the connection down via the
//! usual `Errored` outcome.
//!
//! Schema access is **tier-1 compile** on pairing:
//! `Action::StreamRow` carries `&'r RowDesc` — the `'r` lifetime
//! prevents the user from using a stale schema after the protocol
//! advances to a new query.

use core::fmt;

/// Maximum columns per result-set supported by 1c-2. Queries returning
/// more columns classify as [`crate::ProtocolError::TooManyColumns`] —
/// the connection stays alive (recoverable), the user retries with a
/// narrower projection.
///
/// 32 covers typical application queries with headroom. Widening this
/// bound grows [`RowDesc`] linearly and propagates up through
/// [`crate::Reply::QueryComplete`].
pub const MAX_ROW_COLUMNS: usize = 32;

/// PostgreSQL wire format for one column's bytes.
///
/// - [`FormatCode::Text`] (wire code `0`) — ASCII-ish representation
///   (e.g., `"42"` for int4, `"t"`/`"f"` for bool). Simple Query always
///   uses text.
/// - [`FormatCode::Binary`] (wire code `1`) — PG's typed binary layout
///   (BE integers, fixed-width / length-prefixed strings). Selected
///   per-column in Extended Query via the Bind frame.
///
/// Any other wire value classifies as
/// [`crate::ProtocolError::UnexpectedFormatCode`] (round-4 finding #5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum FormatCode {
    /// Text format — `0` on the wire.
    #[default]
    Text = 0,
    /// Binary format — `1` on the wire.
    Binary = 1,
}

impl FormatCode {
    /// Classify a wire i16 format-code byte into the typed variant.
    ///
    /// PG §55.2.2 defines exactly two legal values: `0` (text) and
    /// `1` (binary). Any other value is a server-side wire violation
    /// and returns the offending code in `Err` for the caller to wrap
    /// into [`ProtocolError::UnexpectedFormatCode`].
    ///
    /// # F32 (2026-04-21)
    ///
    /// Centralises the `{0, 1}` classification so future extended-query
    /// sub-phases (1c-3b Describe / 1c-3c BindExecute) that also parse
    /// format codes don't each rewrite the same match. A new illegal
    /// value surfaces with identical diagnostic across every callsite.
    #[inline]
    pub const fn try_from_wire_i16(code: i16) -> Result<Self, i16> {
        match code {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            other => Err(other),
        }
    }

    /// DEF-154 (V) P2-5 helper: the wire i16 representation.
    /// Centralises the `self as i16` coercion in a match — matches
    /// the `try_from_wire_i16` literals exactly. A body-swap drift
    /// is caught by the round-trip const-assert below.
    #[inline]
    #[must_use]
    pub const fn as_wire_i16(self) -> i16 {
        match self {
            Self::Text => 0,
            Self::Binary => 1,
        }
    }
}

// DEF-154 (V) P2-5: round-trip compile pin for FormatCode.
const _: () = {
    assert!(
        matches!(FormatCode::try_from_wire_i16(FormatCode::Text.as_wire_i16()), Ok(FormatCode::Text)),
        "FormatCode round-trip broken: Text",
    );
    assert!(
        matches!(FormatCode::try_from_wire_i16(FormatCode::Binary.as_wire_i16()), Ok(FormatCode::Binary)),
        "FormatCode round-trip broken: Binary",
    );
};

/// Bit-packed set of [`FormatCode`] values for the columns of a
/// result-set. Bit `i` is `1` if column `i` is [`FormatCode::Binary`],
/// `0` otherwise (default = [`FormatCode::Text`]).
///
/// # DEF-194 (2026-04-27): tier-1 size win
///
/// Replaces the pre-DEF-194 storage `[FormatCode; MAX_ROW_COLUMNS]`
/// (32 bytes for the 32-column inline cap). Storage is one `u32`,
/// exactly the bit-width of [`MAX_ROW_COLUMNS`] (= 32). Saves 28 bytes
/// per [`RowDesc`] and removes the `[FormatCode; 32]` niche from the
/// outer struct (the new u32 storage is non-niche, so
/// `Option<RowDesc>` may grow by one discriminant byte +
/// alignment — net per-Option saving is 24-28 B depending on layout).
///
/// # Tier
///
/// Construction is **tier-2 structural**: [`Self::set`] returns
/// [`OutOfRange`] for `idx >= MAX_ROW_COLUMNS`, never panics.
/// Round-trip — `set(i, code)` followed by `get(i)` returns the same
/// `code` — is verified by tier-1 unit tests below.
///
/// # `repr(transparent)`
///
/// Pinned via `#[repr(transparent)]` so the struct layout is exactly
/// the inner `u32`. A future field addition would change the
/// `size_of::<FormatCodeSet>() == 4` const-assert and the build
/// would fail — adding a field is a decision point, not a silent
/// regression.
// DEF-194 follow-up 2026-04-27 — `Default` derive REMOVED to eliminate
// the tier-3 `Default::default() == empty()` audit gap. `Default::default`
// is not const-fn-callable (RU-01: const traits unstable), so the
// identity could only be verified via runtime test — a tier-3 surface
// where a custom-impl Default could silently diverge from `empty()`.
// Callers use `FormatCodeSet::empty()` explicitly; production search
// confirms zero `::default()` consumers in the crate. **Tier-1 by
// removal** — no surface = no possibility of drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct FormatCodeSet {
    /// Bit `i` (0-indexed) is `1` iff column `i` is [`FormatCode::Binary`].
    /// Bits at positions `>= MAX_ROW_COLUMNS` MUST always be `0` — every
    /// constructor and mutator on this type preserves that invariant.
    bits: u32,
}

/// Error returned by [`FormatCodeSet::set`] when the column index
/// exceeds [`MAX_ROW_COLUMNS`].
///
/// Kept as a typed sentinel (rather than a `bool` / `Option`) so a
/// caller mapping the failure into a higher-level classification
/// (e.g. [`crate::ProtocolError::MalformedRowDescription`]) does
/// so explicitly. Tier-3 classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfRange {
    /// Caller-supplied index that exceeded the bound.
    pub idx: usize,
    /// Maximum index permitted (one past = [`MAX_ROW_COLUMNS`]).
    pub max: usize,
}

impl fmt::Display for OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FormatCodeSet index {} exceeds MAX_ROW_COLUMNS = {}",
            self.idx, self.max,
        )
    }
}

// Tier-1 storage-width pin. `FormatCodeSet` uses `u32` with the
// invariant "bit i ↔ column i", which only works while the bit-width
// of u32 (= 32) is at least `MAX_ROW_COLUMNS`. If `MAX_ROW_COLUMNS`
// bumps from 32, the storage type must widen (u64 / u128 / `[u32; N]`)
// AND the `get` / `set` shift logic must follow.
const _: () = assert!(
    MAX_ROW_COLUMNS == 32,
    "FormatCodeSet uses u32 (32-bit) storage tied to MAX_ROW_COLUMNS == 32. \
     If MAX_ROW_COLUMNS changes, switch storage type to a wider integer or \
     a [u32; N] array AND update the shift logic in get/set/empty.",
);

const _: () = assert!(
    core::mem::size_of::<FormatCodeSet>() == 4,
    "FormatCodeSet must remain 4 bytes (single u32). repr(transparent) \
     pins the layout; adding any field would break this invariant and \
     erode the DEF-194 size win.",
);

impl FormatCodeSet {
    /// Empty set — every column position resolves to [`FormatCode::Text`].
    /// Used by [`RowDesc::EMPTY`] and as the zero-init seed inside
    /// [`parse_row_description`].
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// Compute the bit-mask for column index `idx`. Returns `None`
    /// for `idx >= MAX_ROW_COLUMNS`.
    ///
    /// Implementation: repeated `wrapping_mul(2)` instead of a single
    /// `checked_shl`. Both forms are `const fn` stable, but the
    /// shift-by-usize form requires a usize → u32 conversion in const
    /// context which is **not yet const-stable** (RU-01 in
    /// `deferred.md` §C — `TryFrom<usize> for u32` const-trait gated
    /// on rust-lang/rust#143874). Repeated multiplication sidesteps
    /// the conversion entirely; the const-eval cost is `O(idx)` at
    /// **compile time only** (runtime accessors don't call this — see
    /// the inline note below).
    ///
    /// # Tier-1 round-trip enabler
    ///
    /// Existence as a `const fn` is what enables the round-trip
    /// const-assert block below to verify
    /// `set(i, code) → get(i) == code` **at compile time** for every
    /// `(i ∈ 0..32, code ∈ {Text, Binary})` pair (64 distinct
    /// assertions). Without `mask_for_const`, the round-trip
    /// guarantee would live as runtime tests (tier-3 verified) instead
    /// of compile-time pin (tier-1 by-construction).
    #[inline]
    const fn mask_for_const(idx: usize) -> Option<u32> {
        if idx >= MAX_ROW_COLUMNS {
            return None;
        }
        // O(idx) const loop. const-stable since Rust 1.46. At runtime
        // the function is `#[inline]` and called from `get` / `set`
        // which then turns the loop into LLVM-optimised code; modern
        // compilers reduce this to a single shift on the happy path.
        let mut mask = 1u32;
        let mut i = 0usize;
        while i < idx {
            mask = mask.wrapping_mul(2);
            i = i.wrapping_add(1);
        }
        Some(mask)
    }

    /// Format code at column `idx`.
    ///
    /// Returns `None` for `idx >= MAX_ROW_COLUMNS` — defensive
    /// secondary guard. Production call sites
    /// ([`RowDesc::format_code`]) gate `idx < n_columns` first; this
    /// accessor's bound shields against constructor / refactor drift
    /// independently.
    ///
    /// # `const fn` (DEF-194 follow-up 2026-04-27)
    ///
    /// Promoted to `const fn` so the round-trip pin block below can
    /// verify `set(i, code) → get(i) == code` at **compile time** —
    /// elevating round-trip from tier-3 runtime-test to tier-1
    /// const-assert.
    #[inline]
    #[must_use]
    pub const fn get(&self, idx: usize) -> Option<FormatCode> {
        let mask = match Self::mask_for_const(idx) {
            Some(m) => m,
            None => return None,
        };
        if self.bits & mask != 0 {
            Some(FormatCode::Binary)
        } else {
            Some(FormatCode::Text)
        }
    }

    /// Set the format code at column `idx`.
    ///
    /// Returns `Err(OutOfRange)` for `idx >= MAX_ROW_COLUMNS`. The
    /// caller MUST classify the failure (the only call site today is
    /// [`parse_row_description`], which maps it to
    /// [`crate::ProtocolError::MalformedRowDescription`] alongside the
    /// existing dead-arm classification of out-of-range slot writes).
    ///
    /// # `const fn` with `&mut self` (Rust 1.83+ const_mut_refs)
    ///
    /// Const-mut-refs is stable since Rust 1.83.0 (workspace MSRV =
    /// 1.95). Promoting `set` to `const fn` is what lets the
    /// round-trip const-assert block below mutate a `FormatCodeSet`
    /// at compile time — completing the tier-1 elevation.
    pub const fn set(&mut self, idx: usize, code: FormatCode) -> Result<(), OutOfRange> {
        let mask = match Self::mask_for_const(idx) {
            Some(m) => m,
            None => return Err(OutOfRange { idx, max: MAX_ROW_COLUMNS }),
        };
        match code {
            FormatCode::Text => self.bits &= !mask,
            FormatCode::Binary => self.bits |= mask,
        }
        Ok(())
    }

    /// Raw bit pattern — diagnostic / test access only.
    ///
    /// Bit `i` (0-indexed) is `1` iff column `i` is
    /// [`FormatCode::Binary`]. Bits beyond [`MAX_ROW_COLUMNS`] are
    /// always zero by construction.
    #[inline]
    #[must_use]
    pub const fn raw_bits(self) -> u32 {
        self.bits
    }

    /// Construct from a raw bit pattern — diagnostic / test access
    /// only. Caller is responsible for ensuring bits at positions
    /// `>= MAX_ROW_COLUMNS` are zero. Production call paths use
    /// [`Self::set`] in a loop ([`parse_row_description`]).
    #[inline]
    #[must_use]
    pub const fn from_raw_bits(bits: u32) -> Self {
        Self { bits }
    }
}

// ─────────────────────────────────────────────────────────────────
// DEF-194 follow-up 2026-04-27 — tier-1 round-trip compile pin.
// ─────────────────────────────────────────────────────────────────
//
// Verifies at COMPILE TIME that for every (idx, code) pair where
// idx ∈ 0..MAX_ROW_COLUMNS and code ∈ {Text, Binary}:
//
//     1. empty().get(idx) == Some(Text)        — zero-init semantic
//     2. set(idx, code) succeeds               — bound respected
//     3. (after set) get(idx) == Some(code)    — round-trip
//     4. set(idx, Text) clears                 — explicit Text-write
//     5. (after Text-set) get(idx) == Text     — clear semantic
//
// 64 distinct properties × 32 column positions = 320 assertions
// total, all verified at compile time. A body-swap of `&` / `|` /
// `!` in `set`, an inverse-swap of arms in `get`, or a misclassified
// idx → mask mapping in `mask_for_const` would fail this block at
// const-eval time.
//
// Pre-(this block) the round-trip property lived in runtime unit
// tests (tier-3 verified) — a deletion of the test would be silent.
// Post-(this block) the round-trip is a build-failure if violated.
//
// Loop body is `assert!(matches!(...))` — both const-stable.
const _: () = {
    let mut idx = 0usize;
    while idx < MAX_ROW_COLUMNS {
        // (1) Empty set: every position is Text.
        let s_empty = FormatCodeSet::empty();
        assert!(
            matches!(s_empty.get(idx), Some(FormatCode::Text)),
            "FormatCodeSet round-trip pin: empty().get(idx) must be Text",
        );

        // (2 + 3): set(idx, Binary) succeeds, get(idx) returns Binary.
        let mut s = FormatCodeSet::empty();
        let r = s.set(idx, FormatCode::Binary);
        assert!(
            matches!(r, Ok(())),
            "FormatCodeSet round-trip pin: set(idx, Binary) must succeed for idx in 0..MAX_ROW_COLUMNS",
        );
        assert!(
            matches!(s.get(idx), Some(FormatCode::Binary)),
            "FormatCodeSet round-trip pin: set(idx, Binary).get(idx) must be Binary",
        );

        // (4 + 5): set(idx, Text) clears the bit; get(idx) returns Text.
        let r = s.set(idx, FormatCode::Text);
        assert!(
            matches!(r, Ok(())),
            "FormatCodeSet round-trip pin: set(idx, Text) must succeed",
        );
        assert!(
            matches!(s.get(idx), Some(FormatCode::Text)),
            "FormatCodeSet round-trip pin: set(idx, Text).get(idx) must be Text",
        );

        idx = idx.wrapping_add(1);
    }
};

// Out-of-range pin: idx == MAX_ROW_COLUMNS yields None on get and
// Err(OutOfRange) on set. Pins the boundary-classification arm.
// `Option::is_none` / `Result::is_err` are const-stable since
// Rust 1.48 — preferred over `matches!` here for clippy
// (`redundant_pattern_matching`).
const _: () = {
    let s = FormatCodeSet::empty();
    assert!(
        s.get(MAX_ROW_COLUMNS).is_none(),
        "FormatCodeSet boundary pin: get(MAX_ROW_COLUMNS) must be None",
    );
    let mut s_mut = FormatCodeSet::empty();
    assert!(
        s_mut.set(MAX_ROW_COLUMNS, FormatCode::Binary).is_err(),
        "FormatCodeSet boundary pin: set(MAX_ROW_COLUMNS, _) must be Err",
    );
};

// Independence pin: setting bit i does NOT affect bit j (j != i).
// Catches a hypothetical mask-broadcast bug (`self.bits = mask`
// instead of `self.bits |= mask`) at compile time.
const _: () = {
    let mut s = FormatCodeSet::empty();
    assert!(matches!(s.set(0, FormatCode::Binary), Ok(())));
    assert!(matches!(s.set(31, FormatCode::Binary), Ok(())));
    assert!(matches!(s.get(0), Some(FormatCode::Binary)));
    assert!(matches!(s.get(31), Some(FormatCode::Binary)));
    // Bits 1..31 must remain Text.
    let mut j = 1usize;
    while j < 31 {
        assert!(
            matches!(s.get(j), Some(FormatCode::Text)),
            "FormatCodeSet independence pin: set(0,Binary)+set(31,Binary) must not affect bits 1..31",
        );
        j = j.wrapping_add(1);
    }
};

// DEF-194 follow-up 2026-04-27 — tier-1 elevation of OutOfRange field
// preservation.
//
// Pre-(this pin): `set(idx, _).err().idx == idx` was verified by the
// runtime test `set_out_of_range_returns_err_with_idx_field_preserved`
// (tier-3). A field-swap regression where `set` returned
// `OutOfRange { idx: 0, max }` (or `idx: max` etc.) instead of
// `idx: caller_idx` would compile, propagate to operator diagnostics,
// and pass static analysis — only the runtime test would notice.
//
// Post-(this pin): const-eval verifies the `.idx` and `.max` field
// surface is preserved exactly through `set`'s OOR path. Tier-3 →
// tier-1.
//
// Pattern: `assert!(result.is_err(), …)` first (guarantees the Ok
// arm is unreachable in const-eval), then `match` with an empty Ok
// arm (architecturally dead under the prior assertion). This avoids
// `assert!(false, …)` which clippy::assertions_on_constants rejects.
const _: () = {
    // Three offending indices spanning the typical range: just past
    // the boundary (32), well-beyond (99), and pathological (usize::MAX).
    // Each is documented-dead from a `set` mutation perspective (no bits
    // touched on Err path), so we also verify state-preservation.
    let mut s_a = FormatCodeSet::from_raw_bits(0xdead_beef);
    let r_a = s_a.set(MAX_ROW_COLUMNS, FormatCode::Binary);
    assert!(r_a.is_err(), "set(MAX_ROW_COLUMNS, _) must fail");
    if let Err(OutOfRange { idx, max }) = r_a {
        assert!(
            idx == MAX_ROW_COLUMNS,
            "OutOfRange.idx must equal caller's offending index (boundary case)",
        );
        assert!(
            max == MAX_ROW_COLUMNS,
            "OutOfRange.max must equal MAX_ROW_COLUMNS",
        );
    }
    // State must NOT have mutated on Err.
    assert!(
        s_a.raw_bits() == 0xdead_beef,
        "Failed set must leave raw_bits untouched (OOR boundary case)",
    );

    let mut s_b = FormatCodeSet::from_raw_bits(0xdead_beef);
    let r_b = s_b.set(99, FormatCode::Text);
    assert!(r_b.is_err(), "set(99, _) must fail");
    if let Err(OutOfRange { idx, max }) = r_b {
        assert!(
            idx == 99,
            "OutOfRange.idx must equal caller's offending index (well-beyond)",
        );
        assert!(max == MAX_ROW_COLUMNS);
    }
    assert!(s_b.raw_bits() == 0xdead_beef, "Failed set leaves state");

    let mut s_c = FormatCodeSet::from_raw_bits(0xdead_beef);
    let r_c = s_c.set(usize::MAX, FormatCode::Binary);
    assert!(r_c.is_err(), "set(usize::MAX, _) must fail");
    if let Err(OutOfRange { idx, max }) = r_c {
        assert!(
            idx == usize::MAX,
            "OutOfRange.idx must equal caller's offending index (pathological)",
        );
        assert!(max == MAX_ROW_COLUMNS);
    }
    assert!(s_c.raw_bits() == 0xdead_beef, "Failed set leaves state");
};

// DEF-194 follow-up 2026-04-27 — tier-1 elevation of raw_bits round-trip.
//
// Pre-(this pin): `from_raw_bits(x).raw_bits() == x` was verified by the
// runtime test `raw_bits_round_trip` (tier-3). A hidden transformation
// in either accessor (e.g. xor with constant, byte-swap) would compile
// silently and only show on inspect-and-rebuild flows.
//
// Post-(this pin): const-eval verifies the round-trip on multiple
// representative bit patterns (zero, all-ones, alternating, single
// bits at low and high positions). Tier-3 → tier-1.
const _: () = {
    // Pattern 1: zero (covers empty()-equivalent path).
    assert!(FormatCodeSet::from_raw_bits(0).raw_bits() == 0);
    // Pattern 2: all-ones (covers max-pattern path; every bit Binary).
    assert!(FormatCodeSet::from_raw_bits(u32::MAX).raw_bits() == u32::MAX);
    // Pattern 3: alternating 0xa..a (covers interleaved bits).
    assert!(FormatCodeSet::from_raw_bits(0xaaaa_aaaa).raw_bits() == 0xaaaa_aaaa);
    // Pattern 4: alternating 0x5..5 (complement of 3 — both halves).
    assert!(FormatCodeSet::from_raw_bits(0x5555_5555).raw_bits() == 0x5555_5555);
    // Pattern 5: single low bit (catches a mask-by-low-byte bug).
    assert!(FormatCodeSet::from_raw_bits(1).raw_bits() == 1);
    // Pattern 6: single high bit (catches a sign-flag / shift-direction bug).
    assert!(FormatCodeSet::from_raw_bits(0x8000_0000).raw_bits() == 0x8000_0000);
    // Pattern 7: arbitrary "magic" pattern.
    assert!(FormatCodeSet::from_raw_bits(0xdead_beef).raw_bits() == 0xdead_beef);
};

/// Per-column metadata from a `RowDescription` frame.
///
/// Carries the load-bearing fields for row decoding: the PG type OID
/// (which tells the caller what Rust type to decode into) and the
/// format code (which tells the decoder whether to parse text or
/// binary representation).
///
/// **Fields dropped vs PG spec**: `table_oid`, `attr_num`, `type_size`,
/// `type_mod`, column name. Names can be restored in 1c-6 if
/// runtime-reflection tooling requires them; the macro layer (Phase 2)
/// resolves names at compile time and does not need the runtime copy.
///
/// # DEF-189 — derived projection, not stored representation
///
/// Pre-DEF-189, [`RowDesc`] stored an inline `[ColumnDesc; 32]` array
/// (8 B per slot, 256 B total + n_columns + padding = 264 B). Post-DEF-189,
/// [`RowDesc`] uses a struct-of-arrays layout (`[u32; 32]` for OIDs,
/// `[FormatCode; 32]` for format codes) — 162 B total, ~38% smaller.
/// `ColumnDesc` is now produced on demand by [`RowDesc::get`] /
/// [`RowDesc::columns`]; it is no longer the storage shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColumnDesc {
    /// PostgreSQL type OID (e.g. `23` = `int4`, `25` = `text`). Match
    /// via the constants in [`crate::oids`].
    pub type_oid: u32,
    /// Text or binary.
    pub format_code: FormatCode,
}

/// Schema of a result-set's rows.
///
/// # DEF-194 (2026-04-27): bit-packed format codes
///
/// Format codes are now stored in a [`FormatCodeSet`] (one `u32`,
/// 1 bit per column). Replaces the `[FormatCode; 32]` storage,
/// saving 28 bytes per descriptor (164 → 136 B). The per-column
/// accessor `format_code(idx)` reads `(packed >> idx) & 1` —
/// branchless, single-instruction on every relevant ISA.
///
/// # DEF-189 — struct-of-arrays (SoA) layout
///
/// POD layout: a parallel array of OIDs (one `u32` slot per column
/// up to [`MAX_ROW_COLUMNS`]) + a bit-packed [`FormatCodeSet`] +
/// a `u16` populated count. `Copy`, no `Drop`. Equality compares
/// the full storage — trailing slots are zero-filled by every
/// constructor and never mutated thereafter.
///
/// ```text
/// n_columns:    u16              [2 B]
/// (padding to align u32)         [2 B]
/// type_oids:    [u32; 32]        [128 B]
/// format_codes: FormatCodeSet    [4 B]   (DEF-194: was [FormatCode; 32] = 32 B)
/// total:                         [136 B] (DEF-194: was 164 B)
/// ```
///
/// Pre-DEF-189 was an array of `(u32 + FormatCode)` rows = 8 B per slot
/// = 264 B. Pre-DEF-194 SoA was 164 B. Post-DEF-194 SoA is 136 B.
/// Combined saving vs the original AoS form: **128 B per descriptor**.
///
/// # Per-row hot-path access
///
/// The streaming fast-path reads `desc.type_oid(i)` (single u32 array
/// lookup) and `desc.format_code(i)` (bit-pack mask read on a u32 —
/// fits one cache line shared with adjacent metadata). Both O(1),
/// branchless on the happy path, no `&ColumnDesc` reconstruction
/// needed for the hot path.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(4))]
pub struct RowDesc {
    /// DEF-195: column count is `BoundedU8<MAX_ROW_COLUMNS>` (1 byte,
    /// `NonZeroU8`-backed niche). Tier-2 by-construct: the parser
    /// rejects any wire `n_columns > MAX_ROW_COLUMNS` before
    /// constructing this field, so an out-of-bounds row descriptor
    /// cannot exist in safe code. The niche absorbs the discriminant
    /// of `Option<RowDesc>`, shrinking it from 140 → 136 B.
    n_columns: crate::bounded::BoundedU8<MAX_ROW_COLUMNS>,
    /// Padding so `type_oids` is 4-byte aligned. Always zero
    /// (initialised by constructors).
    _pad: [u8; 3],
    type_oids: [u32; MAX_ROW_COLUMNS],
    /// DEF-194: bit-packed (1 bit per column). Replaces the
    /// pre-DEF-194 `[FormatCode; MAX_ROW_COLUMNS]` (32 B → 4 B).
    format_codes: FormatCodeSet,
}

// DEF-203 unified: BoundedU8 now takes `const MAX: usize` directly,
// so `BoundedU8<MAX_ROW_COLUMNS>` works without a `_U8` helper const.

impl RowDesc {
    /// Empty descriptor (0 columns). Used as a test fixture and as
    /// the schema-less sentinel for empty-query / NoData paths.
    ///
    /// DEF-194: `format_codes: FormatCodeSet::empty()` zero-initialises
    /// every column position to [`FormatCode::Text`] (bit 0 = Text).
    /// Semantically identical to the pre-DEF-194 array literal
    /// `[FormatCode::Text; 32]`.
    pub const EMPTY: Self = Self {
        n_columns: crate::bounded::BoundedU8::ZERO,
        _pad: [0; 3],
        type_oids: [0; MAX_ROW_COLUMNS],
        format_codes: FormatCodeSet::empty(),
    };

    /// Number of populated columns.
    ///
    /// Non-const: `From<u8> for usize` is not yet a const trait
    /// on stable (RU-01 watch).
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.n_columns.get())
    }

    /// Number of populated columns as a `u16` — matches the wire
    /// representation. Returns `u16` for backward-compat with the
    /// pre-DEF-195 public API; internally `BoundedU8<32>` enforces
    /// the range invariant.
    ///
    /// Non-const for the same reason as [`Self::len`] (RU-01).
    #[inline]
    #[must_use]
    pub fn n_columns(&self) -> u16 {
        u16::from(self.n_columns.get())
    }

    /// Whether the descriptor carries any columns.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.n_columns.get() == 0
    }

    /// PG type OID for column `idx`, or `None` if out of range.
    ///
    /// DEF-189: O(1) indexed lookup into `type_oids` SoA array. Hot-path
    /// callers (per-column decode) get a single bounds-checked u32 read.
    #[inline]
    #[must_use]
    pub fn type_oid(&self, idx: usize) -> Option<u32> {
        if idx >= self.len() {
            return None;
        }
        self.type_oids.get(idx).copied()
    }

    /// Format code for column `idx`, or `None` if out of range.
    ///
    /// DEF-189: O(1) indexed lookup; DEF-194 reads from the bit-packed
    /// [`FormatCodeSet`] (single `u32` shift + mask). The
    /// `idx >= self.len()` gate is the populated-prefix bound; the
    /// inner `FormatCodeSet::get` carries an independent
    /// `idx >= MAX_ROW_COLUMNS` defensive guard (returns `None`).
    #[inline]
    #[must_use]
    pub fn format_code(&self, idx: usize) -> Option<FormatCode> {
        if idx >= self.len() {
            return None;
        }
        self.format_codes.get(idx)
    }

    /// Construct a `ColumnDesc` for column `idx`, or `None` if out
    /// of range. Reconstructs the AoS shape on demand from the SoA
    /// storage.
    ///
    /// DEF-189: returns `Option<ColumnDesc>` (by value) instead of the
    /// pre-DEF-189 `Option<&ColumnDesc>`. `ColumnDesc` is 8 B; on
    /// 64-bit targets returning by value is identical in ABI cost to
    /// returning by ref, with no pointer-stability surprise.
    #[inline]
    #[must_use]
    pub fn get(&self, idx: usize) -> Option<ColumnDesc> {
        Some(ColumnDesc {
            type_oid: self.type_oid(idx)?,
            format_code: self.format_code(idx)?,
        })
    }

    /// Iterate over populated columns in declaration order.
    ///
    /// Each yielded item is a `ColumnDesc` reconstructed from the SoA
    /// storage. For decode hot paths prefer the per-array accessors
    /// ([`Self::type_oid`], [`Self::format_code`]) — they avoid the
    /// per-step struct construction.
    #[inline]
    #[must_use]
    pub fn columns_iter(&self) -> RowDescColumnsIter<'_> {
        RowDescColumnsIter {
            desc: self,
            idx: 0,
            len: self.len(),
        }
    }
}

/// Iterator yielded by [`RowDesc::columns_iter`].
#[derive(Debug, Clone)]
pub struct RowDescColumnsIter<'a> {
    desc: &'a RowDesc,
    idx: usize,
    len: usize,
}

impl Iterator for RowDescColumnsIter<'_> {
    type Item = ColumnDesc;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.len {
            return None;
        }
        let cd = self.desc.get(self.idx)?;
        self.idx = self.idx.saturating_add(1);
        Some(cd)
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.len.saturating_sub(self.idx);
        (n, Some(n))
    }
}

impl ExactSizeIterator for RowDescColumnsIter<'_> {}
impl core::iter::FusedIterator for RowDescColumnsIter<'_> {}

// RowDesc uses full-array Eq (trailing slots are zero-filled by
// constructors and never mutated thereafter, so byte-equality of the
// SoA arrays implies logical equality of populated-prefix semantics).
impl PartialEq for RowDesc {
    fn eq(&self, other: &Self) -> bool {
        self.n_columns == other.n_columns
            && self.type_oids == other.type_oids
            && self.format_codes == other.format_codes
    }
}
impl Eq for RowDesc {}

// DEF-194 size pin: 136 B post-bit-pack format_codes (vs 164 B pre-194).
// `MAX_ROW_COLUMNS = 32`: 4 (n_columns + pad) + 128 (type_oids) +
// 4 (format_codes FormatCodeSet u32) = 136 B.
const _: () = assert!(
    core::mem::size_of::<RowDesc>() == 136,
    "RowDesc size pin: 136 B post-DEF-194 bit-pack format_codes. \
     Pre-194 was 164 B (32 bytes [FormatCode; 32]); pre-DEF-189 was \
     264 B (AoS [ColumnDesc; 32]). If MAX_ROW_COLUMNS bumps from 32, \
     update both this pin AND `FormatCodeSet`'s storage type \
     (u32 → wider integer / array) AND the const-assert tying the \
     two together. New size = 4 + 4 * MAX_ROW_COLUMNS + \
     bytes_for(FormatCodeSet) rounded for alignment.",
);
const _: () = assert!(
    core::mem::align_of::<RowDesc>() == 4,
    "RowDesc alignment must remain 4 (u32 type_oids force this). \
     repr(C, align(4)) keeps the layout drift-pinned.",
);

// DEF-194 follow-up 2026-04-27 — Option<RowDesc> exact pin.
//
// Glass-arch audit closure: pre-(this pin) `Option<RowDesc>` size was
// claimed-but-not-verified (`row_desc_slot ~140` in lib.rs comment, no
// const-assert). A future change that replaced FormatCodeSet with a
// non-niche-friendly type, or added a non-Copy field to RowDesc, would
// silently regress `Option<RowDesc>` size by 4-N bytes — invisible to
// tests, observable only via PgProtocol-level size budget drift.
//
// Exact pin at 140 B (aarch64-apple-darwin observed) makes the size
// a build-time decision point. The Option niches NOT through a
// FormatCode value (FormatCodeSet is non-niche u32 storage) but instead
// uses a discriminant byte + 3 bytes alignment padding to align(4).
//
// Pre-DEF-194: Option<RowDesc> was ~168 B (164 B RowDesc + 1 disc + 3
// padding); FormatCode niche was either unused or inadequate against
// the [u32; 32] aligned layout. Post-DEF-194: 140 B exactly; saving
// **28 B per Option<RowDesc>** (cascading into PgProtocol::row_desc_slot).
const _: () = assert!(
    core::mem::size_of::<Option<RowDesc>>() == 136,
    "Option<RowDesc> exact pin: 136 B post-DEF-195 (= 136 RowDesc, niche \
     absorbed via `BoundedU8<32>::NonZeroU8` in `n_columns` first field). \
     Pre-195 was 140 B (= 136 + 1 discriminant + 3 align padding). Saving \
     **4 B per Option<RowDesc>**, cascading into PgProtocol::row_desc_slot.",
);

/// DEF-189: lifetime-bound borrow of a [`RowDesc`] living inside
/// [`crate::PgProtocol::row_desc_slot`].
///
/// `RowDescBorrow<'r>` is the public read-only handle the user receives
/// for SELECT-bearing replies and per-row events. It is `Copy` (8 B —
/// just a `&RowDesc` reference) and ties its validity to the
/// `&'r mut PgProtocol` borrow chain that produced it.
///
/// # Lifetime contract
///
/// `'r` is the same lifetime as the `OutActions<'_, 'r>` /
/// `StreamItem<'r>` that delivered the borrow. While the borrow is
/// alive, the borrow checker blocks any `&mut PgProtocol` re-entry —
/// in particular, the next `iter_rows` / `feed_bytes` / `push_command`
/// call cannot fire until this borrow drops. This means `row_desc_slot`
/// cannot be cleared, and the underlying `RowDesc` stays valid.
///
/// # Why a separate type vs `&'r RowDesc`
///
/// Three reasons:
///
/// 1. **API stability**: the internal storage layout (currently a
///    direct `Option<RowDesc>` slot, possibly future per-column SoA
///    arrays addressed by an external buffer) is hidden behind this
///    borrow. Users access via `n_columns()` / `type_oid(i)` /
///    `format_code(i)` and don't depend on field projections.
/// 2. **Implementation flexibility**: future versions may back the
///    borrow with byte-range descriptors into a dedicated buffer
///    (per the architect's DEF-189 lazy-borrow design alternative),
///    or with rkyv-style zero-copy archives — without breaking user
///    code.
/// 3. **Discoverability**: `RowDescBorrow::n_columns(&self)` chains
///    fluently in user code; `(&'r RowDesc).len()` is less natural.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct RowDescBorrow<'r> {
    inner: &'r RowDesc,
}

impl<'r> RowDescBorrow<'r> {
    /// Construct from an immutable reference. Crate-internal — the
    /// public path is via [`crate::PgProtocol::current_row_desc`].
    #[inline]
    #[must_use]
    pub(crate) const fn from_ref(inner: &'r RowDesc) -> Self {
        Self { inner }
    }

    /// Number of populated columns.
    ///
    /// Non-const after DEF-195: `u16::from(u8)` is not const-trait
    /// stable yet (RU-01).
    #[inline]
    #[must_use]
    pub fn n_columns(&self) -> u16 {
        self.inner.n_columns()
    }

    /// Number of populated columns as `usize`.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the descriptor carries any columns.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// PG type OID for column `idx`, or `None` if out of range.
    ///
    /// Single bounds-checked u32 read into the underlying SoA storage.
    #[inline]
    #[must_use]
    pub fn type_oid(&self, idx: usize) -> Option<u32> {
        self.inner.type_oid(idx)
    }

    /// Format code for column `idx`, or `None` if out of range.
    #[inline]
    #[must_use]
    pub fn format_code(&self, idx: usize) -> Option<FormatCode> {
        self.inner.format_code(idx)
    }

    /// Construct a [`ColumnDesc`] for column `idx`, or `None` if out
    /// of range. Mirrors [`RowDesc::get`].
    #[inline]
    #[must_use]
    pub fn get(&self, idx: usize) -> Option<ColumnDesc> {
        self.inner.get(idx)
    }

    /// Iterate over populated columns as [`ColumnDesc`] tuples.
    #[inline]
    #[must_use]
    pub fn columns_iter(&self) -> RowDescColumnsIter<'r> {
        self.inner.columns_iter()
    }
}

// RowDescBorrow size pin: 8 B (single reference on 64-bit, 4 B on
// 32-bit targets). Ensures the borrow doesn't grow into a payload
// — a future regression that inlined the descriptor would land here.
const _: () = assert!(
    core::mem::size_of::<RowDescBorrow<'_>>() == core::mem::size_of::<&RowDesc>(),
    "RowDescBorrow must be the same size as &RowDesc — adding a payload \
     defeats the lazy-projection design.",
);

impl fmt::Display for FormatCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => f.write_str("text"),
            Self::Binary => f.write_str("binary"),
        }
    }
}

/// Parse a `RowDescription` payload (body of the `'T'` frame, after
/// the 5-byte header) into a [`RowDesc`].
///
/// Wire layout (PG §55.7):
/// ```text
///   int16  column_count
///   for each column:
///     cstring  name           (NUL-terminated; not stored — 1c-2 MVP)
///     int32    table_oid      (dropped)
///     int16    attr_num       (dropped)
///     int32    type_oid       ← captured
///     int16    type_size      (dropped)
///     int32    type_mod       (dropped)
///     int16    format_code    ← captured (0 = Text, 1 = Binary)
/// ```
///
/// # Error classifications
///
/// - [`crate::ProtocolError::MalformedRowDescription`] — payload too
///   short, negative column count, missing name NUL, truncated
///   per-column metadata.
/// - [`crate::ProtocolError::TooManyColumns`] — column count exceeds
///   [`MAX_ROW_COLUMNS`] (result-set too wide for this crate's bounded
///   storage).
/// - [`crate::ProtocolError::UnexpectedFormatCode`] — wire value not in
///   `{0, 1}` (round-4 finding #5).
// DEF-184 (A1+A13): Err is ProtocolError ~72 B, below 128 B
// result_large_err threshold post-ErrorArena externalisation.
#[cold]
pub(crate) fn parse_row_description(
    payload: &[u8],
) -> Result<RowDesc, crate::error::ProtocolError> {
    use crate::error::ProtocolError;
    let malformed = || ProtocolError::MalformedRowDescription {
        payload_len: payload.len(),
    };

    // column_count: i16 BE at offset 0.
    let (count_bytes, mut rest) = payload.split_first_chunk::<2>().ok_or_else(malformed)?;
    let n_columns_i16 = i16::from_be_bytes(*count_bytes);
    if n_columns_i16 < 0 {
        return Err(malformed());
    }
    // `n_columns_i16 >= 0`, so `u16::try_from` is infallible (just a
    // bit-width narrowing from a non-negative i16). Keep the Result
    // chain for the crate's no-panic discipline.
    let n_columns = u16::try_from(n_columns_i16).map_err(|_| malformed())?;
    let n_columns_usize = usize::from(n_columns);

    // Tier-2 structural: reject results too wide for inline storage.
    if n_columns_usize > MAX_ROW_COLUMNS {
        return Err(ProtocolError::TooManyColumns {
            count: n_columns_usize,
            max: MAX_ROW_COLUMNS,
        });
    }

    // DEF-189: SoA per-column parse. Populated slots overwrite the
    // zero-initialised array / bit-pack; trailing slots remain default.
    // DEF-194: format_codes is now a bit-packed FormatCodeSet (u32);
    // population is via `FormatCodeSet::set(idx, code)?` instead of
    // an array slot write.
    let mut type_oids = [0u32; MAX_ROW_COLUMNS];
    let mut format_codes = FormatCodeSet::empty();
    for idx in 0..n_columns_usize {
        // Name: cstring (NUL-terminated). We skip the bytes; round-4
        // finding #2 typed-newtypes already covers identifier discipline
        // elsewhere.
        let nul_pos = rest.iter().position(|&b| b == 0).ok_or_else(malformed)?;
        let name_end = nul_pos.saturating_add(1);
        let after_name = rest.get(name_end..).ok_or_else(malformed)?;

        // 18 bytes of metadata after name: table_oid(4) + attr_num(2) +
        // type_oid(4) + type_size(2) + type_mod(4) + format_code(2).
        let (meta, next_cursor) = after_name
            .split_first_chunk::<18>()
            .ok_or_else(malformed)?;

        // Destructure into the two fields we keep. Slice-pattern makes
        // the offsets readable inline (no magic-index arithmetic).
        let &[
            _tbl0, _tbl1, _tbl2, _tbl3,        // table_oid
            _att0, _att1,                      // attr_num
            toid0, toid1, toid2, toid3,        // type_oid
            _ts0, _ts1,                        // type_size
            _tm0, _tm1, _tm2, _tm3,            // type_mod
            fc0, fc1,                          // format_code
        ] = meta;
        let type_oid = u32::from_be_bytes([toid0, toid1, toid2, toid3]);
        let format_code_i16 = i16::from_be_bytes([fc0, fc1]);
        let format_code = FormatCode::try_from_wire_i16(format_code_i16)
            .map_err(|code| ProtocolError::UnexpectedFormatCode { code })?;

        // Bounds: idx < n_columns_usize ≤ MAX_ROW_COLUMNS. Both writes
        // are architecturally-infallible under the upstream
        // n_columns_usize gate. The dead Err arms classify as
        // MalformedRowDescription rather than silently dropping the
        // column (forbid-bundle no-panic discipline).
        let oid_slot = type_oids.get_mut(idx).ok_or_else(malformed)?;
        *oid_slot = type_oid;
        // DEF-194: FormatCodeSet::set returns OutOfRange for
        // idx >= MAX_ROW_COLUMNS; under the upstream gate this Err is
        // dead. Map to malformed for surface uniformity with the
        // type_oids slot write above.
        format_codes.set(idx, format_code).map_err(|_| malformed())?;
        rest = next_cursor;
    }

    // Trailing bytes after the declared column count are a framing
    // bug; `rest` must be empty at this point.
    if !rest.is_empty() {
        return Err(malformed());
    }

    // DEF-195/DEF-203: convert validated `n_columns` (u16, range-checked
    // above against MAX_ROW_COLUMNS) to the `BoundedU8<MAX_ROW_COLUMNS>`
    // niche-bearing type. Both narrowings have architecturally-dead
    // Err paths (the upstream guard rejects any value > 32).
    let n_columns_bounded = <crate::bounded::BoundedU8<MAX_ROW_COLUMNS>
        as crate::bounded::BoundedLen<MAX_ROW_COLUMNS>>::try_new_usize(n_columns_usize)
        .ok_or_else(malformed)?;
    Ok(RowDesc {
        n_columns: n_columns_bounded,
        _pad: [0; 3],
        type_oids,
        format_codes,
    })
}

/// Parse a `ParameterDescription` payload (body of the `'t'` frame,
/// after the 5-byte header) into a [`crate::action::ParamOids`].
/// 1c-3c.
///
/// Wire layout (PG §55.2.2):
/// ```text
///   int16  parameter_count
///   for each parameter:
///     int32  type_oid
/// ```
///
/// # Error classifications
///
/// - [`crate::error::ProtocolError::MalformedParameterDescription`] —
///   payload shorter than the 2-byte count header, negative count,
///   or body length does not match `count × 4`.
/// - [`crate::error::ProtocolError::TooManyParameters`] — count
///   exceeds [`crate::params::MAX_PARAMS_ARITY`] (16). A statement
///   with more placeholders can be Parsed by the server but cannot
///   be Bound against by this crate, so the describe result is
///   useless downstream — fail loudly at parse time.
///
/// Cold path — called once per statement-level Describe reply.
#[cold]
// DEF-184 (A1+A13): Err is ProtocolError ~72 B, below 128 B
// result_large_err threshold post-ErrorArena externalisation.
pub(crate) fn parse_parameter_description(
    payload: &[u8],
) -> Result<crate::action::ParamOids, crate::error::ProtocolError> {
    use crate::error::ProtocolError;
    let malformed = || ProtocolError::MalformedParameterDescription {
        payload_len: payload.len(),
    };

    // parameter_count: i16 BE at offset 0.
    let (count_bytes, rest) = payload.split_first_chunk::<2>().ok_or_else(malformed)?;
    let n_params_i16 = i16::from_be_bytes(*count_bytes);
    if n_params_i16 < 0 {
        return Err(malformed());
    }
    // `n_params_i16 >= 0`, so `u16::try_from` is infallible (widening
    // from non-negative i16). Keep Result chain for panic-ban
    // discipline.
    let n_params = u16::try_from(n_params_i16).map_err(|_| malformed())?;
    let n_params_usize = usize::from(n_params);

    // Tier-2 structural: reject counts too high for inline storage.
    // MAX_PARAMS_ARITY matches the Bind-side cap — receiving more
    // OIDs than we can ever Bind against means the describe result
    // is useless downstream.
    if n_params_usize > crate::params::MAX_PARAMS_ARITY {
        return Err(ProtocolError::TooManyParameters {
            count: n_params_usize,
            max: crate::params::MAX_PARAMS_ARITY,
        });
    }

    // Body length must exactly equal `count × 4` (one i32 per OID).
    // Trailing bytes imply wire corruption; short body implies the
    // declared count lies. Both classify as framing error.
    let expected_body_len = n_params_usize.checked_mul(4).ok_or_else(malformed)?;
    if rest.len() != expected_body_len {
        return Err(malformed());
    }

    // F7 (pass-#7 audit): `split_first_chunk::<4>()` returns typed
    // `Option<(&[u8; 4], &[u8])>` — the typed fixed-array ref
    // replaces the `chunks_exact(4)` + `[a,b,c,d]` slice-pattern
    // approach. No dead `_ =>` fallback arm needed; the Option::None
    // path is architecturally dead (body_len check above proves
    // remaining bytes suffice) yet surfaces as `Err(malformed())`
    // rather than `unreachable!()` (forbid-bundle).
    let mut oids = [0u32; crate::params::MAX_PARAMS_ARITY];
    let mut cursor = rest;
    for slot in oids.iter_mut().take(n_params_usize) {
        let (chunk, tail) = cursor.split_first_chunk::<4>().ok_or_else(malformed)?;
        *slot = u32::from_be_bytes(*chunk);
        cursor = tail;
    }

    Ok(crate::action::ParamOids::from_parts(n_params, oids))
}

// ════════════════════════════════════════════════════════════════════
// 1c-2b — DataRow parser + ColumnsIter
// ════════════════════════════════════════════════════════════════════

/// Decode-time errors — classify malformed row bodies independently
/// of wire-level [`crate::ProtocolError`].
///
/// A [`DecodeError`] means the caller tried to parse an individual row
/// or column and the bytes don't match the PG DataRow shape. These
/// are per-row diagnostic errors: the protocol state machine already
/// accepted the frame as well-formed at the framing layer (the D
/// tag + length were intact); the body's internal structure is the
/// issue.
///
/// **Why separate from `ProtocolError`**: `ProtocolError` tears down
/// the connection. `DecodeError` surfaces to the row consumer who
/// can choose to skip the row, fail the application query, or
/// classify as a driver bug (the server sent a malformed row body)
/// — the connection itself is still healthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// DataRow body too short to contain the 2-byte column count
    /// header. Malformed frame.
    TruncatedRow,
    /// DataRow's 2-byte column count header parses as a negative
    /// signed value (PG §55.7 requires a non-negative i16). Wire
    /// protocol violation — servers never send this under spec
    /// compliance; arrival implies a bug / corruption / adversarial
    /// frame. Pass-#8 F-041.
    ///
    /// Split from [`Self::TruncatedRow`]: the latter means "body too
    /// short"; this means "column count is signed-invalid." Different
    /// classes, different operator diagnostics.
    InvalidColumnCount {
        /// The offending i16 count value (always negative; positive
        /// values are well-formed and don't reach this arm).
        count: i16,
    },
    /// A column's 4-byte length prefix is missing (fewer bytes
    /// remain than expected). `column_idx` is 0-based, bounded by
    /// [`MAX_ROW_COLUMNS`] = 32 — fits `u8` with headroom.
    TruncatedColumnLen {
        /// Zero-based column index where the truncation was detected.
        column_idx: u8,
    },
    /// A column's declared length prefix is negative and is not the
    /// sentinel `-1` (which encodes SQL `NULL`). Other negative
    /// values are wire-level invalid.
    NegativeColumnLength {
        /// Zero-based column index.
        column_idx: u8,
        /// The offending length value.
        length: i32,
    },
    /// A column's data region is shorter than the declared length
    /// prefix (partial row).
    TruncatedColumnData {
        /// Zero-based column index.
        column_idx: u8,
        /// Length declared by the prefix.
        declared_len: usize,
        /// Bytes actually remaining in the row body.
        remaining: usize,
    },
    /// Column bytes are not valid UTF-8. Applies to text-format
    /// columns (including `&str` and all integer decoders, which
    /// read ASCII digits). 1c-2c.
    NonUtf8,
    /// Failed to parse a numeric text-format column into the target
    /// Rust integer type — bad digit, sign out of range, or
    /// overflow. 1c-2c.
    IntParse,
    /// Failed to parse a boolean — PG text format emits `"t"` / `"f"`;
    /// anything else classifies here. 1c-2c.
    BoolParse,
    /// A binary-format fixed-size column's byte length doesn't match
    /// the decoder's expectation (e.g. an `i32` decoder receiving 3
    /// bytes, or 5). 1c-3b binary-path classification — separate from
    /// [`Self::TruncatedColumnData`] which reports row-scoped
    /// truncation with a column index. Binary decoders run per-column
    /// through [`FromPgBinary`] and don't know the column index at
    /// their call site; this variant is honest about that.
    BinaryLengthMismatch {
        /// Bytes the decoder expected (fixed-size for ints / bool).
        expected_len: u8,
        /// Bytes actually received.
        actual_len: u16,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedRow => f.write_str("DataRow body too short for column count header"),
            Self::InvalidColumnCount { count } => write!(
                f,
                "DataRow column count header is negative ({count}); PG §55.7 requires a non-negative i16",
            ),
            Self::TruncatedColumnLen { column_idx } => {
                write!(f, "column {column_idx}: length prefix truncated")
            }
            Self::NegativeColumnLength { column_idx, length } => write!(
                f,
                "column {column_idx}: invalid negative length {length} (only -1 = SQL NULL is valid)",
            ),
            Self::TruncatedColumnData {
                column_idx,
                declared_len,
                remaining,
            } => write!(
                f,
                "column {column_idx}: data truncated — declared {declared_len} bytes, only {remaining} remain",
            ),
            Self::NonUtf8 => f.write_str("column bytes are not valid UTF-8"),
            Self::IntParse => f.write_str("column text is not a valid integer for the target type"),
            Self::BoolParse => f.write_str("column text is not a PG boolean (expected \"t\" or \"f\")"),
            Self::BinaryLengthMismatch { expected_len, actual_len } => write!(
                f,
                "binary column byte length mismatch: expected {expected_len}, got {actual_len}",
            ),
        }
    }
}

/// Zero-copy reference to a `DataRow` frame body.
///
/// Wraps the body bytes (everything after the 5-byte frame header)
/// and parses the 2-byte column count header eagerly. Per-column
/// data is lazily iterated via [`DataRowRef::columns`].
///
/// # Lifetimes
///
/// `'a` borrows the body bytes. Typically obtained from
/// [`crate::Action::StreamRow::row_bytes`], in which case `'a` is
/// the `'r` lifetime of the owning [`crate::OutActions`]. The
/// iterator yields column slices that share this borrow — no
/// copying, no allocation.
#[derive(Debug, Clone, Copy)]
pub struct DataRowRef<'a> {
    /// Body bytes AFTER the 2-byte column-count header.
    ///
    /// DEF-154 (U) P2/P3: store the post-header slice directly
    /// (stripped at `parse` time via `split_first_chunk::<2>()`).
    /// Pre-(U) the full body was stored and `columns()` re-stripped
    /// the header via `self.body.get(2..).unwrap_or(&[])` — silent
    /// fallback pattern user banned. Post-(U) the column iterator
    /// starts from the stored slice directly — tier-1 infallible,
    /// no Option, no fallback.
    body_after_count: &'a [u8],
    /// Parsed column count.
    n_columns: u16,
}

impl<'a> DataRowRef<'a> {
    /// Parse a `DataRow` frame body. Returns the declared column count
    /// without walking the column payloads — that happens in
    /// [`Self::columns`].
    ///
    /// # Errors
    ///
    /// - [`DecodeError::TruncatedRow`] — body is shorter than 2 bytes,
    ///   or the count header decodes to a negative `i16` (invalid).
    #[inline]
    pub fn parse(body: &'a [u8]) -> Result<Self, DecodeError> {
        let (count_bytes, body_after_count) =
            body.split_first_chunk::<2>().ok_or(DecodeError::TruncatedRow)?;
        let n_columns_i16 = i16::from_be_bytes(*count_bytes);
        if n_columns_i16 < 0 {
            // Pass-#8 F-041: distinguish "body too short" (TruncatedRow)
            // from "count header signed-invalid" (InvalidColumnCount).
            // Different classes; different operator diagnostics.
            return Err(DecodeError::InvalidColumnCount { count: n_columns_i16 });
        }
        // `n_columns_i16 >= 0` (proved above) ⟹ `try_from` infallible.
        // The Err arm is architecturally dead, but classified as
        // `TruncatedRow` rather than silently fabricating a 0-column
        // row — if a future refactor of the negative-check above
        // introduces a seam, the dead arm becomes honest diagnostic
        // output instead of "empty row with no error". Tier-3 audit
        // → tier-2 structural: misfire classifies, does not mask.
        let n_columns = u16::try_from(n_columns_i16).map_err(|_| DecodeError::TruncatedRow)?;
        Ok(Self { body_after_count, n_columns })
    }

    /// Declared column count.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.n_columns)
    }

    /// Whether the row carries zero columns (unusual — typically DML
    /// responses have no DataRow; a 0-column DataRow is exotic).
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.n_columns == 0
    }

    /// Iterator over columns in declaration order.
    ///
    /// Each item is `Result<Option<&'a [u8]>, DecodeError>`:
    /// - `Ok(Some(bytes))` — non-NULL column; `bytes` is the raw
    ///   payload (length-prefix stripped).
    /// - `Ok(None)` — SQL `NULL` (wire-level length prefix = `-1`).
    /// - `Err(DecodeError)` — malformed row body; iteration should
    ///   stop.
    ///
    /// Body bytes are advanced by `4 + data_len` per column; the
    /// iterator stops after `n_columns` items or on the first error.
    #[inline]
    #[must_use]
    pub fn columns(&self) -> ColumnsIter<'a> {
        // DEF-154 (U): tier-1 — `body_after_count` is the
        // post-header slice stored at parse time. No runtime
        // `.get(2..).unwrap_or(&[])` fallback.
        ColumnsIter {
            remaining: self.body_after_count,
            columns_left: self.n_columns,
            column_idx: 0u8,
        }
    }
}

/// Lazy iterator over a [`DataRowRef`]'s columns.
///
/// Produced by [`DataRowRef::columns`]. Each call to [`Iterator::next`]
/// reads one `(length, data)` pair from the remaining body bytes.
///
/// # Iterator semantics
///
/// - Yields exactly `n_columns` items on a well-formed row (then
///   returns `None`).
/// - On the first [`DecodeError`], that error is yielded; subsequent
///   `.next()` calls yield `None` (fused after error via the
///   `columns_left` counter saturating-decrement — further iteration
///   stops cleanly).
#[derive(Debug, Clone)]
pub struct ColumnsIter<'a> {
    remaining: &'a [u8],
    columns_left: u16,
    /// Zero-based column index, bounded by [`MAX_ROW_COLUMNS`] = 32 —
    /// `u8` with headroom. Propagated into `DecodeError::TruncatedColumn*`.
    column_idx: u8,
}

impl<'a> ColumnsIter<'a> {
    /// F-042 (pass-#8): centralised fuse-and-error helper.
    ///
    /// Before F-042 the pattern `self.remaining = &[]; self.columns_left = 0;
    /// return Some(Err(...))` appeared at 4 sites in `next`. A future
    /// refactor adding a 5th error arm and forgetting the fuse would
    /// let iteration continue past the error — drift-prone. This
    /// helper makes the fuse+error path a single expression and
    /// makes every new error arm structurally-fused by default.
    #[inline]
    fn fuse_and_error(&mut self, e: DecodeError) -> Option<Result<Option<&'a [u8]>, DecodeError>> {
        self.remaining = &[];
        self.columns_left = 0;
        Some(Err(e))
    }
}

impl<'a> Iterator for ColumnsIter<'a> {
    type Item = Result<Option<&'a [u8]>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.columns_left == 0 {
            return None;
        }
        let idx = self.column_idx;
        self.column_idx = idx.saturating_add(1);
        self.columns_left = self.columns_left.saturating_sub(1);

        // 4-byte length prefix.
        let (len_bytes, after_len) = match self.remaining.split_first_chunk::<4>() {
            Some(pair) => pair,
            None => return self.fuse_and_error(DecodeError::TruncatedColumnLen { column_idx: idx }),
        };
        let len = i32::from_be_bytes(*len_bytes);

        // DEF-184 (A5/B10): collapsed sign-path cascade.
        //
        // Pre-(184) had 3 sequential sign checks:
        //   if len == -1 { NULL }
        //   if len < 0 { NegativeColumnLength }
        //   usize::try_from(len) { ... Err → NegativeColumnLength }
        // Three comparisons per column × 32 max cols × 1M rows =
        // ~96M redundant compares on row-heavy workloads.
        //
        // Post-(184): single NULL shortcut + fold the `< -1` case
        // into `usize::try_from` Err branch (which also catches
        // hypothetical i32→usize overflow on 16-bit targets, even
        // though MSRV implicitly disallows those). Two compares:
        // `len == -1` (null) and `usize::try_from` (non-negative).
        // LLVM fuses the try_from sign check with the comparison.
        if len == -1 {
            // SQL NULL — no data bytes to consume.
            self.remaining = after_len;
            return Some(Ok(None));
        }
        let Ok(len_usize) = usize::try_from(len) else {
            // `len < -1` (wire violation) OR i32-that-doesn't-fit-
            // usize (architecturally impossible on 32+-bit MSRV
            // targets since i32 range ⊂ usize range). The audit's
            // proposed `wrapping_add(1) as u32` trick is blocked
            // by crate-wide `as_conversions` forbid — try_from is
            // the accepted substitute with LLVM fusing the
            // non-negative fast path.
            return self.fuse_and_error(DecodeError::NegativeColumnLength {
                column_idx: idx,
                length: len,
            });
        };

        match after_len.split_at_checked(len_usize) {
            Some((data, next)) => {
                self.remaining = next;
                Some(Ok(Some(data)))
            }
            None => {
                let remaining = after_len.len();
                self.fuse_and_error(DecodeError::TruncatedColumnData {
                    column_idx: idx,
                    declared_len: len_usize,
                    remaining,
                })
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = usize::from(self.columns_left);
        (n, Some(n))
    }
}

impl ExactSizeIterator for ColumnsIter<'_> {}
impl core::iter::FusedIterator for ColumnsIter<'_> {}

// ════════════════════════════════════════════════════════════════════
// 1c-2c — Text-format decoders
// ════════════════════════════════════════════════════════════════════

/// PostgreSQL **text-format** column decoder for a Rust type.
///
/// PG's text format — the default for Simple Query — encodes all
/// values as ASCII-ish strings (`"42"`, `"t"`, `"hello"`). This
/// trait's implementations wrap `core::str::from_utf8` and
/// `FromStr`-style parses with type-specific error classification.
///
/// # Lifetime
///
/// `'a` ties the decoder's output to the input byte slice. For
/// `&str` the output borrows the input directly (zero-copy). For
/// owned types like `i32` / `bool`, `'a` is phantom.
///
/// # Usage
///
/// DEF-140 (pass-#8 doc polish): the example models the crate's own
/// discipline — no `unwrap()` / `panic!()` in the happy path.
/// `cols.next()` returns `Option<Result<Option<&[u8]>, DecodeError>>`
/// and is matched structurally via `let Some(...) else`. Real user
/// code can adapt to its own error strategy (`?` into custom errors,
/// slogged through a macro in Phase 2's `query!`, etc.).
///
/// DEF-154 (R) P1-3: the doc-test below is now COMPILE-CHECKED
/// (pre-(R) was `rust,ignore` — pure prose that rotted silently on
/// any signature change). DEF-154 (Y): migrated from
/// `Action::StreamRow` to `StreamItem::Row` (row-bearing path is
/// now exclusively `iter_rows`). If a future refactor alters
/// `DataRowRef::parse`, `RowColumns::next`, `StreamItem::Row`,
/// `FromPgText` trait shape, or `DecodeError` variants, this example
/// fails to compile in CI.
///
/// ```rust
/// use bsql_pg_proto::{DataRowRef, DecodeError, FromPgText, StreamItem};
///
/// fn example(item: StreamItem<'_>) -> Result<(), DecodeError> {
///     let StreamItem::Row { row_bytes, .. } = item else { return Ok(()) };
///     let row = DataRowRef::parse(row_bytes)?;
///     let mut cols = row.columns();
///
///     // `Option::None` from `next()` = fewer columns than expected.
///     // `Option::None` from the inner `Ok(None)` = SQL NULL.
///     // Both surfaces via structural match, no `unwrap()`.
///     let Some(id_result) = cols.next() else { return Ok(()) };
///     let _id: Option<i32> = id_result?.map(i32::from_pg_text).transpose()?;
///
///     let Some(name_result) = cols.next() else { return Ok(()) };
///     let _name: Option<&str> = name_result?.map(<&str>::from_pg_text).transpose()?;
///
///     Ok(())
/// }
/// ```
///
/// # Error
///
/// [`DecodeError::NonUtf8`] for non-UTF-8 bytes on decoders that
/// genuinely require UTF-8 validation (`&str`, `Vec<u8>`).
/// Type-specific parse errors:
/// - integer types → [`DecodeError::IntParse`] (DEF-184 A6/B13:
///   single-pass ASCII-digit parser treats non-digit bytes
///   uniformly; non-ASCII/non-UTF-8 input classifies as IntParse,
///   NOT NonUtf8, because UTF-8 validation is skipped as redundant
///   for strict-ASCII integer grammar).
/// - `bool` → [`DecodeError::BoolParse`]
///
/// # Binary format
///
/// For PG binary-format columns (selected via Bind in Extended
/// Query, 1c-3), a parallel `FromPgBinary` trait lands alongside
/// the binary codec. Text vs binary dispatch at the caller level
/// via `ColumnDesc::format_code`.
pub trait FromPgText<'a>: Sized {
    /// PG type OID this text decoder targets.
    ///
    /// F-038 (pass-#8): parallel to [`FromPgBinary::OID`] and
    /// [`EncodeBinary::OID`]. Enables the future `query!` macro
    /// (Phase 2) to validate at compile time that a Rust type
    /// chosen by the user matches the PG catalog OID the server
    /// declared in `RowDescription` — independent of which
    /// format (text/binary) the column uses. Symmetry-complete
    /// three-trait family.
    const OID: u32;

    /// Decode the column's text-format bytes.
    fn from_pg_text(bytes: &'a [u8]) -> Result<Self, DecodeError>;
}

// DEF-184 (A6/B13): dedicated ASCII-digit integer parser.
//
// Pre-(184) used stdlib `core::str::from_utf8(bytes)?.parse::<T>()`
// — two sequential walks over the bytes:
// 1. `from_utf8` SSE2-scans for non-UTF8.
// 2. `str::parse` re-scans, validates digits, accumulates.
//
// PG text-format integers are strictly `[-+]?[0-9]+` per PG §55.7 —
// always ASCII. UTF-8 validation is redundant (a non-digit byte is
// already an IntParse error; a non-ASCII byte is non-digit). Skip
// it: one walk, one classification path. ~2× on int-heavy text
// SELECT workloads (analytics default).
//
// Accumulates into correct-sign arm avoiding i*::MIN overflow (if
// we accumulated as positive then negated, `-32768` on i16 would
// trip). Each step uses `checked_mul` / `checked_add` / `checked_sub`
// per `clippy::arithmetic_side_effects` forbid.
//
// DEF-207 (2026-05-07): for i16/i32 the digit loop now uses a
// **wider accumulator** (`parse_pg_int_signed_widened!`) so the
// per-digit `checked_mul + checked_add/sub` chain (2 overflow
// branches per iteration) collapses to `wrapping_mul(10) +
// wrapping_add(d)` (no per-digit overflow check). The pre-loop
// length bound + a single end-of-loop `try_from` validate the
// entire range. i64 stays on the original checked-arithmetic
// macro because the next-wider native type (i128) compiles to
// multi-instruction sequences on 64-bit targets, losing the win.

/// Parse a signed ASCII-digit integer with overflow checked at
/// every digit. Original DEF-184 form. Used by `i64` (where the
/// next-wider type would be i128 — non-native on 64-bit, slower
/// than the checked path).
macro_rules! parse_pg_int_signed {
    ($bytes:expr, $t:ty) => {{
        let (is_neg, digits) = match $bytes.split_first() {
            Some((&b'-', rest)) => (true, rest),
            Some((&b'+', rest)) => (false, rest),
            Some(_) => (false, $bytes),
            None => return Err(DecodeError::IntParse),
        };
        if digits.is_empty() {
            return Err(DecodeError::IntParse);
        }
        let mut acc: $t = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(DecodeError::IntParse);
            }
            // `b - b'0'` is 0..=9, always fits u8 → $t via From.
            let d = <$t>::from(b.saturating_sub(b'0'));
            acc = acc.checked_mul(10).ok_or(DecodeError::IntParse)?;
            if is_neg {
                acc = acc.checked_sub(d).ok_or(DecodeError::IntParse)?;
            } else {
                acc = acc.checked_add(d).ok_or(DecodeError::IntParse)?;
            }
        }
        Ok(acc)
    }};
}

/// Parse a signed ASCII-digit integer using a **wider** accumulator
/// type than the result. DEF-207 (2026-05-07) — branch-budget
/// reduction for the i16/i32 hot loop on text-format integer
/// columns (the dominant cost on int-heavy SELECT analytics).
///
/// # How it removes branches
///
/// The classic `checked_mul + checked_add/sub` form has 2
/// overflow-detection branches per digit. With a wider
/// accumulator and a digit-count pre-check, **the wrapping
/// arithmetic cannot actually wrap during the loop** — the
/// pre-check bounds the maximum reachable value safely below
/// `$acc::MAX`. One end-of-loop `<$result>::try_from(signed_acc)`
/// validates against the result-type's range.
///
/// Per-digit branches: **1** (digit validation) — was 3 (digit +
/// 2× overflow).
///
/// # Constraints
///
/// - `$acc` MUST be wider than `$result` (e.g. `i32` for `i16`,
///   `i64` for `i32`). Signed.
/// - `$max_digits` MUST satisfy `9 * 10^$max_digits + 9 < $acc::MAX`
///   so `wrapping_mul(10).wrapping_add(9)` cannot wrap during
///   the loop. For:
///   - i16 result + i32 acc + 5 digits: max acc reach = 99_999;
///     i32::MAX = 2_147_483_647. ✓
///   - i32 result + i64 acc + 10 digits: max acc reach =
///     9_999_999_999; i64::MAX ≈ 9.22 × 10^18. ✓
///
/// # Sign handling
///
/// Accumulate as positive, apply `wrapping_neg` at end if
/// `is_neg`. `wrapping_neg` on the in-range values we care about
/// (≤ 10^10 for i32) is just regular negation; the wider
/// accumulator gives headroom that avoids the original
/// "accumulate-into-correct-sign" complication of the checked
/// form (where `-i16::MIN = 32768` would overflow the result type
/// before negation). Final `try_from` validates `signed_acc ∈
/// $result::MIN..=$result::MAX`.
macro_rules! parse_pg_int_signed_widened {
    ($bytes:expr, $result:ty, $acc:ty, $max_digits:expr) => {{
        // Sign strip — identical to the checked-arithmetic form.
        let (is_neg, digits) = match $bytes.split_first() {
            Some((&b'-', rest)) => (true, rest),
            Some((&b'+', rest)) => (false, rest),
            Some(_) => (false, $bytes),
            None => return Err(DecodeError::IntParse),
        };
        // Length pre-check — bounds the max accumulator reach so
        // `wrapping_mul(10).wrapping_add(9)` cannot actually wrap
        // during the loop. Empty digit run is also caught here.
        if digits.is_empty() || digits.len() > $max_digits {
            return Err(DecodeError::IntParse);
        }
        // Hot loop — single per-digit branch (digit valid?), no
        // overflow checks. Wrapping ops are always-defined; the
        // length bound above ensures the value stays below
        // `$acc::MAX` so wrapping never actually wraps for valid
        // input.
        let mut acc: $acc = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(DecodeError::IntParse);
            }
            // `b.saturating_sub(b'0')` ∈ 0..=9 on the valid path
            // (validated in the line above); identical semantics
            // to `b - b'0'` here, but lint-safe under
            // `clippy::arithmetic_side_effects` forbid.
            let d = <$acc>::from(b.saturating_sub(b'0'));
            acc = acc.wrapping_mul(10).wrapping_add(d);
        }
        // Sign at end. `wrapping_neg` is correct for all
        // in-range values; the impossible `acc == $acc::MIN`
        // edge would cycle back to itself but is unreachable
        // given the length pre-check.
        let signed: $acc = if is_neg { acc.wrapping_neg() } else { acc };
        // Final range check — validates `signed` fits in
        // `$result::MIN..=$result::MAX`. This is the SOLE overflow
        // check on the entire path.
        <$result>::try_from(signed).map_err(|_| DecodeError::IntParse)
    }};
}

/// Parse an unsigned ASCII-digit integer. Used for u32 (PG OID).
/// Rejects leading `-`; `+` prefix accepted as a no-op.
macro_rules! parse_pg_int_unsigned {
    ($bytes:expr, $t:ty) => {{
        let digits = match $bytes.split_first() {
            Some((&b'-', _)) => return Err(DecodeError::IntParse),
            Some((&b'+', rest)) => rest,
            Some(_) => $bytes,
            None => return Err(DecodeError::IntParse),
        };
        if digits.is_empty() {
            return Err(DecodeError::IntParse);
        }
        let mut acc: $t = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(DecodeError::IntParse);
            }
            let d = <$t>::from(b.saturating_sub(b'0'));
            acc = acc.checked_mul(10).ok_or(DecodeError::IntParse)?;
            acc = acc.checked_add(d).ok_or(DecodeError::IntParse)?;
        }
        Ok(acc)
    }};
}

impl FromPgText<'_> for i16 {
    const OID: u32 = oids::INT2;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        // DEF-207 (2026-05-07): widened-accumulator path. i32
        // accumulator + 5-digit cap (i16::MAX = 32767 = 5 digits).
        // Max acc reach with 5 digits = 99_999 << i32::MAX ≈ 2.15B,
        // so wrapping_mul(10).wrapping_add(9) cannot wrap during
        // the loop. Single end-cast `i16::try_from` validates
        // i16::MIN..=i16::MAX.
        parse_pg_int_signed_widened!(bytes, i16, i32, 5)
    }
}

impl FromPgText<'_> for i32 {
    const OID: u32 = oids::INT4;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        // DEF-207 (2026-05-07): widened-accumulator path. i64
        // accumulator + 10-digit cap (i32::MAX = 2_147_483_647 =
        // 10 digits). Max acc reach with 10 digits = 9_999_999_999
        // << i64::MAX ≈ 9.22 × 10^18, so wrapping_mul(10) +
        // wrapping_add(9) cannot wrap during the loop. Single
        // end-cast `i32::try_from` validates i32::MIN..=i32::MAX.
        parse_pg_int_signed_widened!(bytes, i32, i64, 10)
    }
}

impl FromPgText<'_> for i64 {
    const OID: u32 = oids::INT8;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        // DEF-207 (2026-05-07): i64 stays on the original
        // checked-arithmetic macro. The wider native accumulator
        // (i128) compiles to multi-instruction sequences on
        // 64-bit targets — losing the speed gain that motivates
        // the widened-acc form for i16/i32. Capping at 18 digits
        // (skipping i64::MAX) would be incorrect — `9_223_372_
        // 036_854_775_807` is a valid 19-digit i64.
        parse_pg_int_signed!(bytes, i64)
    }
}

impl FromPgText<'_> for u32 {
    const OID: u32 = oids::OID;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        parse_pg_int_unsigned!(bytes, u32)
    }
}

/// PG boolean text format: `"t"` = true, `"f"` = false. Anything
/// else (including `"true"`, `"TRUE"`, `"1"`, `"0"`) classifies as
/// [`DecodeError::BoolParse`] — PG is strict about its own format.
impl FromPgText<'_> for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes {
            b"t" => Ok(true),
            b"f" => Ok(false),
            _ => Err(DecodeError::BoolParse),
        }
    }
}

/// Text column as `&str` — zero-copy, validates UTF-8 only.
impl<'a> FromPgText<'a> for &'a str {
    const OID: u32 = oids::TEXT;
    /// DEF-202 — SIMD-accelerated UTF-8 validation via `simdutf8`.
    ///
    /// `core::str::from_utf8` is scalar bytewise (with an ASCII
    /// fast-path that aborts on the first non-ASCII byte; cheap on
    /// short ASCII, expensive on multi-byte UTF-8).
    /// `simdutf8::basic::from_utf8` uses lane-wise vector shuffles +
    /// masks via NEON on aarch64.
    ///
    /// Bench evidence (aarch64-apple-darwin, criterion `pre-simdutf8`
    /// vs `def202-simdutf8` baselines, 5-column rows):
    /// * **Long ASCII** (~200 B, descriptive text): −49.9% (~2× faster).
    ///   Realistic Postgres workload: log lines, descriptions, JSON.
    /// * **Multi-byte UTF-8** (~78 B Cyrillic): −74.0% (~3.9× faster).
    ///   Internationalised content: non-Latin names, free-form text.
    /// * **Short ASCII** (17 B `alice@example.com`): +9.9%.
    ///   Acceptable cost: 0.7 ns/col absolute regression on the cheapest
    ///   case (where total time is already 8 ns/col). A length-threshold
    ///   hybrid was tested and rejected — the dispatch branch costs
    ///   ~1.5 ns/col, exceeding the savings on the short-ASCII path.
    ///
    /// Behaviour is byte-identical to `core::str::from_utf8`: both
    /// accept the same byte sequences, reject the same non-UTF-8
    /// inputs, and produce the same `&str` for valid input.
    /// `simdutf8::basic::Utf8Error` is discriminator-only; collapsed
    /// to `DecodeError::NonUtf8` here, matching the pre-DEF-202 contract.
    #[inline]
    fn from_pg_text(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        simdutf8::basic::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// ═════════════════════════════════════════════════════════════════
// FromPgBinary — parallel to FromPgText for PG binary-format
// columns (1c-3b: Bind-selected binary format per-parameter).
//
// Binary format byte layout matches PG §55.7 — fixed-size ints are
// big-endian two's complement, `bool` is a single byte 0/1, `text`
// is raw UTF-8 bytes. Every impl's `OID` const is drift-pinned
// against `oids::*` to catch type-mapping bugs at build time.
// ═════════════════════════════════════════════════════════════════

/// Decode a column's binary-format bytes into a typed Rust value.
///
/// Parallel to [`FromPgText`]; the caller dispatches between text
/// and binary decoders based on [`ColumnDesc::format_code`]. Extended
/// Query (1c-3b) selects binary via the Bind frame's per-param /
/// per-result format-code arrays; Simple Query always uses text.
///
/// # OID drift-pin
///
/// Every impl exposes a `const OID: u32` matching the PG type it
/// decodes. The crate's [`oids`] module is drift-pinned against the
/// canonical PG catalog (`pg_type.dat`); a const-assert per impl
/// verifies `<T as FromPgBinary>::OID == oids::X` at build time.
/// A future refactor that breaks the type↔OID mapping fails the
/// build, not at runtime.
///
/// # Sealed
///
/// The [`sealed::FromPgBinarySealed`] supertrait is module-private
/// (DEF-115-class seal). Downstream crates cannot impl the trait
/// for their own Rust types — the binary-codec surface is a fixed
/// set of primitives in 1c-3b; wider types land with their
/// dedicated sub-phases (arrays 1c-6, uuid / timestamp Phase 2+).
pub trait FromPgBinary<'a>: Sized + sealed::FromPgBinarySealed {
    /// PG type OID this decoder handles. Drift-pinned against
    /// [`oids`] via const-assert.
    const OID: u32;

    /// Decode the column's binary-format bytes.
    ///
    /// # Errors
    ///
    /// - [`DecodeError::TruncatedColumnData`] — input length doesn't
    ///   match the type's fixed size (for fixed-size types).
    /// - [`DecodeError::BoolParse`] — byte outside `{0, 1}` for `bool`.
    /// - [`DecodeError::NonUtf8`] — non-UTF-8 bytes for `&str` / text.
    fn from_pg_binary(bytes: &'a [u8]) -> Result<Self, DecodeError>;
}

mod sealed {
    pub trait FromPgBinarySealed {}
    pub trait EncodeBinarySealed {}
}

// Fixed-size signed integer decoders: N bytes big-endian.
macro_rules! impl_from_pg_binary_int {
    ($($t:ty, $oid:expr, $n:literal),+ $(,)?) => {
        $(
            impl sealed::FromPgBinarySealed for $t {}
            impl FromPgBinary<'_> for $t {
                const OID: u32 = $oid;
                #[inline]
                fn from_pg_binary(bytes: &[u8]) -> Result<Self, DecodeError> {
                    // Binary fixed-size ints: exactly N bytes. Any
                    // other length is classified via
                    // `BinaryLengthMismatch` — a per-type honest error
                    // that doesn't lie about a column index the decoder
                    // can't know.
                    let arr: &[u8; $n] = bytes
                        .first_chunk::<$n>()
                        .filter(|_| bytes.len() == $n)
                        .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                            expected_len: $n,
                            actual_len: u16::try_from(bytes.len()).unwrap_or(u16::MAX),
                        })?;
                    Ok(<$t>::from_be_bytes(*arr))
                }
            }
        )+
    };
}

impl_from_pg_binary_int!(
    i16, oids::INT2, 2,
    i32, oids::INT4, 4,
    i64, oids::INT8, 8,
    u32, oids::OID, 4,
);

/// PG binary `bool`: one byte — `0` = false, `1` = true.
/// Wrong byte length classifies as [`DecodeError::BinaryLengthMismatch`];
/// length-1 with an out-of-range byte classifies as
/// [`DecodeError::BoolParse`].
impl sealed::FromPgBinarySealed for bool {}
impl FromPgBinary<'_> for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn from_pg_binary(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            [_] => Err(DecodeError::BoolParse),
            _ => Err(DecodeError::BinaryLengthMismatch {
                expected_len: 1,
                actual_len: u16::try_from(bytes.len()).unwrap_or(u16::MAX),
            }),
        }
    }
}

/// PG binary `text`: raw UTF-8 bytes. Zero-copy borrow.
///
/// # UTF-8 validation cost
///
/// Every column read walks the column bytes to verify UTF-8 well-formedness
/// — `core::str::from_utf8` is O(N) with a well-tuned SSE2 fast path (~1 ns
/// per byte on modern x86). A 32-byte text column costs ~32 ns; a typical
/// 1000-row SELECT with 5 text columns pays ~160 μs of total validation.
///
/// Under `#![forbid(unsafe_code)]` this validation cannot be skipped —
/// `core::str::from_utf8_unchecked` is unsafe and inaccessible in the crate.
/// Callers who need to bypass should hold the bytes as `&[u8]` (via a
/// separate `FromPgBinary<Target = &[u8]>` impl — not implemented today)
/// and validate externally if / when they need a `&str`.
///
/// PG binary `text` is NOMINALLY UTF-8 per `client_encoding`; a buggy
/// server / misconfigured encoding setting could produce invalid bytes.
/// The Err path classifies as [`DecodeError::NonUtf8`] without
/// panicking — consistent with the column-level safety contract.
impl sealed::FromPgBinarySealed for &str {}
impl<'a> FromPgBinary<'a> for &'a str {
    const OID: u32 = oids::TEXT;
    #[inline]
    fn from_pg_binary(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        core::str::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// Compile-time symmetry pins: text and binary decoders for the
// same Rust type MUST target the same PG type OID. A refactor that
// breaks this breaks the build.
//
// F-038 (pass-#8): `FromPgText` now also carries `OID`; the three
// traits (text / binary / encode) form a closed symmetry family.
// Adding a new Rust type that impls any ONE of these forces matching
// impls + identical OIDs across all three, verified here.
const _: () = {
    assert!(<i16 as FromPgBinary>::OID == oids::INT2);
    assert!(<i32 as FromPgBinary>::OID == oids::INT4);
    assert!(<i64 as FromPgBinary>::OID == oids::INT8);
    assert!(<u32 as FromPgBinary>::OID == oids::OID);
    assert!(<bool as FromPgBinary>::OID == oids::BOOL);
    assert!(<&str as FromPgBinary>::OID == oids::TEXT);
    // Text↔binary OID symmetry: the same Rust type MUST target the
    // same PG type OID across text and binary decoders. A refactor
    // that skewed one against the other would mean the same Rust
    // type decoded differently depending on `ColumnDesc::format_code`
    // — a classification bug. Pinned below.
    assert!(<i16 as FromPgText>::OID == <i16 as FromPgBinary>::OID);
    assert!(<i32 as FromPgText>::OID == <i32 as FromPgBinary>::OID);
    assert!(<i64 as FromPgText>::OID == <i64 as FromPgBinary>::OID);
    assert!(<u32 as FromPgText>::OID == <u32 as FromPgBinary>::OID);
    assert!(<bool as FromPgText>::OID == <bool as FromPgBinary>::OID);
    assert!(<&str as FromPgText>::OID == <&str as FromPgBinary>::OID);
};

// ═════════════════════════════════════════════════════════════════
// EncodeBinary — PG binary format write path (mirror of FromPgBinary).
// Used by ParamsWriter (1c-3b) to serialise parameter values into
// the Bind frame's per-param length+bytes layout.
// ═════════════════════════════════════════════════════════════════

/// Encode a Rust value into PG binary format bytes, directly into
/// a [`crate::write_buf::WriteBuf`].
///
/// Parallel to [`FromPgBinary`] — the `OID` constants pair up
/// across the two traits so the Phase 2 `query!` macro can check
/// param-type OIDs against the `Parse`-time schema fingerprint at
/// compile time.
///
/// Zero-alloc: writes directly into the caller's `WriteBuf`. No
/// intermediate heap buffer, no stack fixture — the caller owns
/// the output storage.
///
/// # Sealed
///
/// Same seal discipline as [`FromPgBinary`] — downstream crates
/// cannot add impls for their own types.
pub trait EncodeBinary: sealed::EncodeBinarySealed {
    /// PG type OID this encoder produces. Drift-pinned against
    /// [`oids`] and cross-asserted against the matching
    /// [`FromPgBinary`] impl.
    const OID: u32;

    /// Write the encoded bytes into `dst`. The caller is responsible
    /// for the surrounding per-param length prefix (PG Bind frame
    /// layout); `encode_to` writes only the payload bytes.
    ///
    /// # Errors
    ///
    /// [`crate::write_buf::WriteBufFull`] if the buffer can't fit
    /// the encoded output — architecturally-bounded at the call
    /// site via the Bind-message size const-assert, but surfaced
    /// as a classified error rather than a panic.
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>;
}

macro_rules! impl_encode_binary_int {
    ($($t:ty, $oid:expr, $push:ident),+ $(,)?) => {
        $(
            impl sealed::EncodeBinarySealed for $t {}
            impl EncodeBinary for $t {
                const OID: u32 = $oid;
                #[inline]
                fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
                    -> Result<(), crate::write_buf::WriteBufFull>
                {
                    dst.$push(*self)
                }
            }
        )+
    };
}

impl_encode_binary_int!(
    i16, oids::INT2, push_i16_be,
    i32, oids::INT4, push_i32_be,
    u32, oids::OID, push_u32_be,
);

impl sealed::EncodeBinarySealed for i64 {}
impl EncodeBinary for i64 {
    const OID: u32 = oids::INT8;
    #[inline]
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i64_be(*self)
    }
}

/// `bool` encoder: `0x00` for `false`, `0x01` for `true`.
impl sealed::EncodeBinarySealed for bool {}
impl EncodeBinary for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_u8(u8::from(*self))
    }
}

/// `&str` encoder — raw UTF-8 bytes (Rust invariant guarantees
/// UTF-8 validity, nothing to check).
impl sealed::EncodeBinarySealed for &str {}
impl EncodeBinary for &str {
    const OID: u32 = oids::TEXT;
    #[inline]
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self.as_bytes())
    }
}

// Drift-pins: every EncodeBinary impl's OID matches the
// corresponding FromPgBinary impl AND the canonical `oids::*`
// constant. One const-block pins the whole set.
const _: () = {
    assert!(<i16 as EncodeBinary>::OID == oids::INT2);
    assert!(<i32 as EncodeBinary>::OID == oids::INT4);
    assert!(<i64 as EncodeBinary>::OID == oids::INT8);
    assert!(<u32 as EncodeBinary>::OID == oids::OID);
    assert!(<bool as EncodeBinary>::OID == oids::BOOL);
    assert!(<&str as EncodeBinary>::OID == oids::TEXT);
    // Cross-trait symmetry (text-format OID ≡ binary-format OID ≡ catalog OID).
    assert!(<i16 as EncodeBinary>::OID == <i16 as FromPgBinary>::OID);
    assert!(<i32 as EncodeBinary>::OID == <i32 as FromPgBinary>::OID);
    assert!(<i64 as EncodeBinary>::OID == <i64 as FromPgBinary>::OID);
    assert!(<u32 as EncodeBinary>::OID == <u32 as FromPgBinary>::OID);
    assert!(<bool as EncodeBinary>::OID == <bool as FromPgBinary>::OID);
    assert!(<&str as EncodeBinary>::OID == <&str as FromPgBinary>::OID);
};

/// PostgreSQL built-in type OID constants for the subset 1c-2
/// decoders cover. Full list at
/// `https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat`.
///
/// Callers match these against [`ColumnDesc::type_oid`] to
/// dispatch the right [`FromPgText`] impl. The macro layer
/// (Phase 2) consumes this mapping at compile time via
/// `query!`-generated decoders.
///
/// # Tier-1 compile drift-pin
///
/// The `const _: () = { assert!(...) }` block below asserts every
/// constant against its canonical PG catalog value. A typo
/// (`INT4 = 32` instead of `23`) fails the build. No runtime test
/// required — the drift guard is the type system itself.
pub mod oids {
    /// `bool` (1-byte typtype `b`).
    pub const BOOL: u32 = 16;
    /// `bytea`.
    pub const BYTEA: u32 = 17;
    /// `"char"` — internal 1-byte char, not standard `char(n)`.
    pub const CHAR: u32 = 18;
    /// `name` — fixed 64-byte identifier (NAMEDATALEN).
    pub const NAME: u32 = 19;
    /// `int8` / `bigint`.
    pub const INT8: u32 = 20;
    /// `int2` / `smallint`.
    pub const INT2: u32 = 21;
    /// `int4` / `integer`.
    pub const INT4: u32 = 23;
    /// `text`.
    pub const TEXT: u32 = 25;
    /// `oid` — object identifier (u32).
    pub const OID: u32 = 26;
    /// `float4` / `real`.
    pub const FLOAT4: u32 = 700;
    /// `float8` / `double precision`.
    pub const FLOAT8: u32 = 701;
    /// `bpchar` — `char(n)`, blank-padded.
    pub const BPCHAR: u32 = 1042;
    /// `varchar` — `varchar(n)`.
    pub const VARCHAR: u32 = 1043;
    /// `timestamp` — timestamp without time zone.
    pub const TIMESTAMP: u32 = 1114;
    /// `timestamptz` — timestamp with time zone.
    pub const TIMESTAMPTZ: u32 = 1184;
    /// `uuid`.
    pub const UUID: u32 = 2950;
    /// `jsonb`.
    pub const JSONB: u32 = 3802;

    // Tier-1 compile drift-pin against the canonical PG catalog
    // (src/include/catalog/pg_type.dat). A typo in any constant
    // above breaks the build here — no runtime test needed.
    const _: () = {
        assert!(BOOL == 16, "oids::BOOL drift from pg_type.dat");
        assert!(BYTEA == 17, "oids::BYTEA drift from pg_type.dat");
        assert!(CHAR == 18, "oids::CHAR drift from pg_type.dat");
        assert!(NAME == 19, "oids::NAME drift from pg_type.dat");
        assert!(INT8 == 20, "oids::INT8 drift from pg_type.dat");
        assert!(INT2 == 21, "oids::INT2 drift from pg_type.dat");
        assert!(INT4 == 23, "oids::INT4 drift from pg_type.dat");
        assert!(TEXT == 25, "oids::TEXT drift from pg_type.dat");
        assert!(OID == 26, "oids::OID drift from pg_type.dat");
        assert!(FLOAT4 == 700, "oids::FLOAT4 drift from pg_type.dat");
        assert!(FLOAT8 == 701, "oids::FLOAT8 drift from pg_type.dat");
        assert!(BPCHAR == 1042, "oids::BPCHAR drift from pg_type.dat");
        assert!(VARCHAR == 1043, "oids::VARCHAR drift from pg_type.dat");
        assert!(TIMESTAMP == 1114, "oids::TIMESTAMP drift from pg_type.dat");
        assert!(TIMESTAMPTZ == 1184, "oids::TIMESTAMPTZ drift from pg_type.dat");
        assert!(UUID == 2950, "oids::UUID drift from pg_type.dat");
        assert!(JSONB == 3802, "oids::JSONB drift from pg_type.dat");
    };
}

#[cfg(test)]
mod parse_tests {
    //! `parse_row_description` conformance per PG §55.7 + bad-path
    //! classification. Category (1)/(B) per reforge.md §4.11 —
    //! spec-conformance table + tier-3 framing-error shield.
    //!
    //! Assertion style: every test uses `assert!(matches!(...))` +
    //! optional `assert_eq!` on destructured fields. The crate-root
    //! forbid bundle bans `panic!`, `.expect()`, `.unwrap()`, and
    //! `unreachable!()` even in unit tests, so the usual
    //! `expect_err("...")` idiom is replaced by `matches!(Err(...))`.
    //! Diagnostic messages on mismatch go into the `assert!` format
    //! string (evaluated only on failure).
    extern crate alloc;
    use super::*;
    use crate::error::ProtocolError;

    /// Build one RowDescription column block: name + NUL + 18 bytes of
    /// metadata (table_oid, attr_num, type_oid, type_size, type_mod,
    /// format_code).
    fn column_block(name: &[u8], type_oid: u32, format_code: i16) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        out.extend_from_slice(name);
        out.push(0);
        out.extend_from_slice(&0i32.to_be_bytes()); // table_oid
        out.extend_from_slice(&0i16.to_be_bytes()); // attr_num
        out.extend_from_slice(&type_oid.to_be_bytes());
        out.extend_from_slice(&(-1i16).to_be_bytes()); // type_size = variable
        out.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod = none
        out.extend_from_slice(&format_code.to_be_bytes());
        out
    }

    /// Build a full RowDescription body. `columns.len() ≤ i16::MAX`
    /// is guaranteed by `MAX_ROW_COLUMNS = 32 ≪ i16::MAX`; the
    /// `unwrap_or(0)` branch below is architecturally dead but
    /// honours the forbid-bundle ban on `unwrap()`.
    fn build(columns: &[(&[u8], u32, i16)]) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        let count = i16::try_from(columns.len()).unwrap_or(0);
        out.extend_from_slice(&count.to_be_bytes());
        for (name, oid, fc) in columns {
            out.extend_from_slice(&column_block(name, *oid, *fc));
        }
        out
    }

    /// Invariant (spec): a well-formed 2-column payload parses to the
    /// declared count, per-column OIDs, and text format codes.
    #[test]
    fn two_column_text_format_roundtrip() {
        let body = build(&[(b"id", 23, 0), (b"name", 25, 0)]);
        let result = parse_row_description(&body);
        let expected: [ColumnDesc; 2] = [
            ColumnDesc {
                type_oid: 23,
                format_code: FormatCode::Text,
            },
            ColumnDesc {
                type_oid: 25,
                format_code: FormatCode::Text,
            },
        ];
        // DEF-189: SoA storage; reconstruct AoS view via columns_iter().
        let actual: alloc::vec::Vec<ColumnDesc> = match &result {
            Ok(desc) => desc.columns_iter().collect(),
            Err(_) => alloc::vec::Vec::new(),
        };
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "expected 2-column text parse, got {result:?}",
        );
    }

    /// Invariant (spec): format code 1 parses as Binary.
    #[test]
    fn binary_format_parsed() {
        let body = build(&[(b"x", 23, 1)]);
        let result = parse_row_description(&body);
        assert!(
            matches!(
                &result,
                Ok(desc) if matches!(
                    desc.get(0),
                    Some(ColumnDesc { format_code: FormatCode::Binary, .. }),
                ),
            ),
            "expected Binary format first column, got {result:?}",
        );
    }

    /// Invariant (spec + round-4 #5): format code outside `{0, 1}`
    /// classifies as `UnexpectedFormatCode`, not a silent fallback.
    #[test]
    fn format_code_out_of_range_is_classified() {
        let body = build(&[(b"x", 23, 7)]);
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::UnexpectedFormatCode { code: 7 })),
            "expected UnexpectedFormatCode {{ code: 7 }}, got {result:?}",
        );
    }

    /// Invariant: negative column count classifies as malformed (not
    /// a usize wrap-around).
    #[test]
    fn negative_column_count_is_malformed() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&(-1i16).to_be_bytes());
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for negative count, got {result:?}",
        );
    }

    /// Invariant: a column count exceeding `MAX_ROW_COLUMNS` classifies
    /// as `TooManyColumns` with the actual counts — the caller can
    /// message the user clearly.
    #[test]
    fn column_count_exceeding_max_is_classified() {
        // Declare count = MAX + 1 (still fits i16); parser rejects
        // before per-column parsing.
        let over = MAX_ROW_COLUMNS.saturating_add(1);
        let count = i16::try_from(over).unwrap_or(0);
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&count.to_be_bytes());
        let result = parse_row_description(&body);
        assert!(
            matches!(
                result,
                Err(ProtocolError::TooManyColumns { count: c, max }) if c == over && max == MAX_ROW_COLUMNS,
            ),
            "expected TooManyColumns {{ count: {over}, max: {MAX_ROW_COLUMNS} }}, got {result:?}",
        );
    }

    /// Invariant: payload too short for the column count header is
    /// malformed.
    #[test]
    fn payload_too_short_for_count_is_malformed() {
        for (label, buf) in [("empty", &[][..]), ("1-byte", &[0][..])] {
            let result = parse_row_description(buf);
            assert!(
                matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
                "{label} payload: expected MalformedRowDescription, got {result:?}",
            );
        }
    }

    /// Invariant: a column body missing the 18-byte metadata tail is
    /// malformed (spec framing desync).
    #[test]
    fn column_metadata_truncated_is_malformed() {
        // Declare 1 column but give only name + partial metadata.
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(b"x\0");
        body.extend_from_slice(&[0u8; 10]); // only 10 of 18 bytes
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for truncated metadata, got {result:?}",
        );
    }

    /// Invariant: a column name without NUL terminator is malformed.
    #[test]
    fn column_name_unterminated_is_malformed() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(b"no_nul_here_ever");
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for unterminated name, got {result:?}",
        );
    }

    /// Invariant: trailing bytes after the declared column count are a
    /// framing bug (shouldn't happen on a well-formed server), classified
    /// as malformed.
    #[test]
    fn trailing_bytes_after_columns_is_malformed() {
        let mut body = build(&[(b"x", 23, 0)]);
        body.push(0xAA); // stray trailing byte
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for trailing bytes, got {result:?}",
        );
    }

    /// Invariant: exactly `MAX_ROW_COLUMNS` columns parses cleanly
    /// (boundary value).
    #[test]
    fn exactly_max_columns_parses() {
        let cols: alloc::vec::Vec<(&[u8], u32, i16)> = (0..MAX_ROW_COLUMNS)
            .map(|_i| (&b"c"[..], 23u32, 0i16))
            .collect();
        let body = build(&cols);
        let result = parse_row_description(&body);
        assert!(
            matches!(&result, Ok(desc) if desc.len() == MAX_ROW_COLUMNS),
            "expected MAX_ROW_COLUMNS parse, got {result:?}",
        );
    }
}

#[cfg(test)]
mod data_row_tests {
    //! `DataRowRef` + `ColumnsIter` spec-conformance per PG §55.7
    //! `DataRow` shape + bad-path classification.
    //!
    //! Body layout: i16 column-count + per-column `(i32 length,
    //! data-bytes)`. `length = -1` encodes SQL NULL.

    extern crate alloc;
    use super::*;

    /// Build a DataRow body: 2-byte count + per-column payloads.
    /// `None` = NULL, `Some(bytes)` = data.
    fn build(columns: &[Option<&[u8]>]) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        let count = i16::try_from(columns.len()).unwrap_or(0);
        out.extend_from_slice(&count.to_be_bytes());
        for col in columns {
            match col {
                Some(data) => {
                    let len = i32::try_from(data.len()).unwrap_or(0);
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(data);
                }
                None => {
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        out
    }

    /// Parse a body and return the row — with `assert` fail path
    /// that avoids the forbid-bundle's bans on `panic!`, `.unwrap()`,
    /// `.expect()`, `unreachable!()`, and `assert!(false)`.
    ///
    /// The `assert!(matches!(...))` ensures Ok on well-formed input;
    /// if it fires, the test fails before reaching the `else` branch,
    /// so the `return` is defensive dead code satisfying
    /// borrow-checker exhaustiveness on the post-assert decomposition.
    fn must_parse(body: &[u8]) -> DataRowRef<'_> {
        let result = DataRowRef::parse(body);
        assert!(
            result.is_ok(),
            "fixture parse should succeed, got {result:?}",
        );
        result.unwrap_or(DataRowRef {
            body_after_count: &[],
            n_columns: 0,
        })
    }

    /// Invariant (spec): a well-formed 2-column row yields both
    /// values in order; length + data round-trip verbatim.
    #[test]
    fn two_column_row_roundtrip() {
        let body = build(&[Some(b"hello"), Some(b"world")]);
        let row = must_parse(&body);
        assert_eq!(row.len(), 2);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert_eq!(items.len(), 2);
        assert!(matches!(items.first(), Some(Ok(Some(b"hello")))));
        assert!(matches!(items.get(1), Some(Ok(Some(b"world")))));
    }

    /// Invariant (spec): `length = -1` encodes SQL NULL, surfaced as
    /// `Ok(None)` — distinct from empty bytes `Ok(Some(b""))`.
    #[test]
    fn null_column_is_none() {
        let body = build(&[Some(b"x"), None, Some(b"y")]);
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert_eq!(items.len(), 3);
        assert!(matches!(items.first(), Some(Ok(Some(b"x")))));
        assert!(matches!(items.get(1), Some(Ok(None))));
        assert!(matches!(items.get(2), Some(Ok(Some(b"y")))));
    }

    /// Invariant: empty column (`length = 0`) surfaces as
    /// `Ok(Some(&[]))` — distinct from NULL.
    #[test]
    fn empty_column_is_not_null() {
        let body = build(&[Some(b""), Some(b"nonempty")]);
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert!(
            matches!(items.first(), Some(Ok(Some(s))) if s.is_empty()),
            "expected Ok(Some(empty)), got {:?}", items.first(),
        );
        assert!(matches!(items.get(1), Some(Ok(Some(b"nonempty")))));
    }

    /// Invariant: 0-column row parses — valid edge case.
    #[test]
    fn zero_column_row_parses() {
        let body = build(&[]);
        let row = must_parse(&body);
        assert!(row.is_empty());
        assert_eq!(row.columns().count(), 0);
    }

    /// Invariant: body shorter than the 2-byte count header is
    /// classified as `TruncatedRow`.
    #[test]
    fn truncated_count_header() {
        for buf in [&[][..], &[0][..]] {
            let result = DataRowRef::parse(buf);
            assert!(
                matches!(result, Err(DecodeError::TruncatedRow)),
                "expected TruncatedRow, got {result:?}",
            );
        }
    }

    /// Invariant: negative column count (i.e. count header decodes to
    /// a negative `i16`) is classified as `InvalidColumnCount { count }`
    /// with the offending i16 preserved for diagnostics. Pass-#8 F-041
    /// split this class out from the `TruncatedRow` "body too short"
    /// bucket to give operators distinct root causes.
    #[test]
    fn negative_column_count() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&(-3i16).to_be_bytes());
        let result = DataRowRef::parse(&body);
        assert!(
            matches!(result, Err(DecodeError::InvalidColumnCount { count: -3 })),
            "negative count: expected InvalidColumnCount {{ count: -3 }}, got {result:?}",
        );
    }

    /// Invariant: missing column length prefix surfaces as
    /// `TruncatedColumnLen`.
    #[test]
    fn missing_column_length_prefix() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&2i16.to_be_bytes()); // claim 2 columns
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(b"a"); // first column fine
        body.extend_from_slice(&[0, 0]); // partial length prefix for second
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert_eq!(items.len(), 2);
        assert!(matches!(items.first(), Some(Ok(Some(b"a")))));
        assert!(
            matches!(
                items.get(1),
                Some(Err(DecodeError::TruncatedColumnLen { column_idx: 1 })),
            ),
            "expected TruncatedColumnLen, got {:?}", items.get(1),
        );
    }

    /// Invariant: negative length that isn't `-1` classifies as
    /// `NegativeColumnLength`.
    #[test]
    fn negative_column_length_not_null_sentinel() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&(-7i32).to_be_bytes());
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert!(matches!(
            items.first(),
            Some(Err(DecodeError::NegativeColumnLength {
                column_idx: 0,
                length: -7,
            })),
        ));
    }

    /// Invariant: data region shorter than declared length classifies
    /// as `TruncatedColumnData` and identifies the shortage.
    #[test]
    fn truncated_column_data() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&10i32.to_be_bytes()); // claim 10 bytes
        body.extend_from_slice(b"short"); // only 5 provided
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert!(
            matches!(
                items.first(),
                Some(Err(DecodeError::TruncatedColumnData {
                    column_idx: 0,
                    declared_len: 10,
                    remaining: 5,
                })),
            ),
            "expected TruncatedColumnData, got {:?}", items.first(),
        );
    }

    /// Invariant: iterator is fused after an error — subsequent
    /// `.next()` calls return `None`, not re-yielding the error or
    /// advancing past broken bytes. Protects against infinite-loop
    /// consumers and double-processing.
    #[test]
    fn iterator_fuses_after_error() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&3i16.to_be_bytes()); // 3 columns claimed
        body.extend_from_slice(&(-99i32).to_be_bytes()); // invalid first col
        let row = must_parse(&body);
        let mut iter = row.columns();
        // First next: the error.
        assert!(matches!(iter.next(), Some(Err(_))));
        // Second next: fused None (not another error, not a stale value).
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    /// Invariant: `ExactSizeIterator::len()` reflects the declared
    /// column count pre-iteration and decrements with each `.next()`.
    #[test]
    fn exact_size_hint() {
        let body = build(&[Some(b"a"), Some(b"b"), Some(b"c")]);
        let row = must_parse(&body);
        let mut iter = row.columns();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        // Consume three items. Iterator yields Result; drop the
        // yielded Result via explicit match — no `let _ = next()`
        // per crate convention.
        match iter.next() {
            Some(_) | None => {}
        }
        assert_eq!(iter.size_hint(), (2, Some(2)));
        match iter.next() {
            Some(_) | None => {}
        }
        match iter.next() {
            Some(_) | None => {}
        }
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }
}

#[cfg(test)]
mod from_pg_text_tests {
    //! `FromPgText` impls — per-type text-format decoding plus the
    //! bad-path classification matrix (non-UTF-8, unparsable digits,
    //! overflow, non-canonical bool).

    use super::*;

    /// **One invariant, one test**: `i32::from_pg_text` correctly
    /// maps PG text representation into the Result<i32, DecodeError>
    /// contract — happy paths, overflow, malformed digits, non-ASCII.
    /// An arm-body swap in my impl (e.g., returning `NonUtf8` for
    /// overflow) fails this table.
    ///
    /// DEF-184 (A6/B13): non-ASCII/non-UTF-8 bytes now classify as
    /// `IntParse` (not `NonUtf8`). Pre-(184) the decoder did a
    /// redundant `from_utf8` walk before `str::parse`; post-(184)
    /// the single-pass ASCII-digit parser treats ANY non-digit byte
    /// uniformly as IntParse. The `NonUtf8` variant is preserved
    /// for `&str` / `Vec<u8>` decoders that genuinely require
    /// UTF-8 validation (arbitrary user text columns).
    #[test]
    fn i32_decoder_matrix() {
        // Happy paths.
        assert!(matches!(i32::from_pg_text(b"0"), Ok(0)));
        assert!(matches!(i32::from_pg_text(b"42"), Ok(42)));
        assert!(matches!(i32::from_pg_text(b"-17"), Ok(-17)));
        assert!(matches!(i32::from_pg_text(b"+17"), Ok(17)));
        assert!(matches!(i32::from_pg_text(b"2147483647"), Ok(i32::MAX)));
        assert!(matches!(i32::from_pg_text(b"-2147483648"), Ok(i32::MIN)));

        // Overflow → IntParse.
        assert!(matches!(i32::from_pg_text(b"2147483648"), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b"-2147483649"), Err(DecodeError::IntParse)));

        // Garbage → IntParse (empty, non-digit, trailing, whitespace).
        assert!(matches!(i32::from_pg_text(b""), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b"abc"), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b"12a"), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b" 12"), Err(DecodeError::IntParse)));

        // DEF-184 (A6/B13): non-ASCII bytes → IntParse (single-pass
        // ASCII-digit validator treats any non-digit byte uniformly).
        // Pre-(184) this was NonUtf8 via upstream from_utf8 walk.
        assert!(matches!(i32::from_pg_text(&[0xFF]), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(&[0xC3, 0x28]), Err(DecodeError::IntParse)));
    }

    /// **One invariant, one test**: parallel `i16` / `i64` / `u32`
    /// impls delegate to stdlib `FromStr` with per-type ranges and
    /// map failures to `IntParse`. Catches macro-expansion errors
    /// where a type's impl would mis-wire to another's range.
    #[test]
    fn other_integer_decoders_matrix() {
        // i16 boundaries.
        assert!(matches!(i16::from_pg_text(b"32767"), Ok(i16::MAX)));
        assert!(matches!(i16::from_pg_text(b"-32768"), Ok(i16::MIN)));
        assert!(matches!(i16::from_pg_text(b"32768"), Err(DecodeError::IntParse)));

        // i64 boundaries.
        assert!(matches!(i64::from_pg_text(b"9223372036854775807"), Ok(i64::MAX)));
        assert!(matches!(i64::from_pg_text(b"9223372036854775808"), Err(DecodeError::IntParse)));

        // u32 boundaries + negative rejection.
        assert!(matches!(u32::from_pg_text(b"0"), Ok(0)));
        assert!(matches!(u32::from_pg_text(b"4294967295"), Ok(u32::MAX)));
        assert!(matches!(u32::from_pg_text(b"4294967296"), Err(DecodeError::IntParse)));
        assert!(matches!(u32::from_pg_text(b"-1"), Err(DecodeError::IntParse)));
    }

    /// **One invariant, one test**: `bool::from_pg_text` accepts
    /// **exactly** PG's canonical `"t"` / `"f"` wire form — nothing
    /// else. PG server is strict on wire format; lax parsers that
    /// accept `"true"` / `"1"` / etc. would mask protocol desync if
    /// the server ever switched to a non-standard encoding.
    #[test]
    fn bool_decoder_matrix() {
        // Canonical accepts.
        assert!(matches!(bool::from_pg_text(b"t"), Ok(true)));
        assert!(matches!(bool::from_pg_text(b"f"), Ok(false)));

        // Every non-canonical form (including common false-friends
        // from SQL literal contexts) must classify as BoolParse, NOT
        // be coerced.
        for bad in [
            &b"true"[..], &b"false"[..], &b"TRUE"[..], &b"T"[..], &b"F"[..],
            &b"1"[..], &b"0"[..], &b"yes"[..], &b"no"[..], &b""[..],
        ] {
            assert!(
                matches!(bool::from_pg_text(bad), Err(DecodeError::BoolParse)),
                "expected BoolParse for {bad:?}",
            );
        }
    }

    /// **One invariant, one test**: `&str::from_pg_text` is a
    /// zero-copy UTF-8 validator. Output pointer must equal input
    /// pointer (no internal copy); non-UTF-8 input classifies as
    /// `NonUtf8`; empty input is valid.
    #[test]
    fn str_decoder_matrix() {
        let bytes: &[u8] = b"hello world";
        let result = <&str>::from_pg_text(bytes);
        assert!(matches!(result, Ok("hello world")));
        if let Ok(s) = result {
            // Zero-copy invariant — the returned &str borrows the
            // same memory region as the input &[u8].
            assert_eq!(s.as_ptr(), bytes.as_ptr());
        }

        // Empty is valid.
        assert!(matches!(<&str>::from_pg_text(b""), Ok("")));

        // Non-UTF-8 (lone continuation byte).
        assert!(matches!(<&str>::from_pg_text(&[0x80]), Err(DecodeError::NonUtf8)));
    }

    // OID drift-pin is tier-1 compile — see `decode::oids::const _`
    // block. Runtime test removed (was redundant with the
    // compile-time assertion).
}

#[cfg(test)]
mod format_code_set_tests {
    //! DEF-194: bit-packed [`FormatCodeSet`] semantic + invariant tests.
    //!
    //! Every public-API surface is exercised. The 12 §7 axes (CREDO):
    //! - **Cardinality**: empty (0 cols), single, max (32), overflow (33+).
    //! - **Presence**: all-default, partial, all-set, alternating pattern.
    //! - **Temporal**: set→get round-trip, set→clear→get, multi-write.
    //! - **Size**: idx 0..32 valid; idx ≥ 32 → None / Err uniformly.
    //! - **State lifecycle**: empty seed, mid-populate, fully populated.
    //! - **Failure composition**: OutOfRange classifies, never silent.
    //! - **Memory-leak**: POD Copy, no Drop (covered by lib.rs needs_drop pin).
    //! - **Fallback**: every out-of-range path returns explicit None / Err.
    //!
    //! Concurrency / trust / platform / resource axes — not applicable
    //! (POD Copy, no I/O, branchless u32 ops portable across all
    //! supported targets).
    //!
    //! **Skepticism shield**: every test name pins a single inverse-swap
    //! the compiler would not catch. Removing any test = a compilable
    //! drift surface; `cargo test` is the only catcher.
    extern crate alloc;
    use super::*;
    use alloc::format;

    // ─────────────────────────────────────────────────────────
    // DEF-194 follow-up 2026-04-27 — five tests REMOVED per CREDO §4.11.
    //
    // The const-assert blocks above (round-trip pin + boundary pin +
    // independence pin) verify these properties at COMPILE TIME for
    // every (idx ∈ 0..32, code ∈ {Text, Binary}) pair. Runtime
    // duplicates are redundant by §4.11.1 algorithm:
    //
    //   - empty_resolves_every_index_to_text
    //     → covered by round-trip pin step (1)
    //   - set_then_get_round_trip_all_positions
    //     → covered by round-trip pin steps (2)+(3)
    //   - set_text_after_binary_clears_bit
    //     → covered by round-trip pin steps (4)+(5)
    //   - independent_columns_dont_alias
    //     → covered by independence pin
    //   - get_out_of_range_returns_none
    //     → covered by boundary pin
    //
    // Tests retained below are tier-1-orthogonal — they cover surfaces
    // const-asserts can't pin: OutOfRange `.idx` field surface,
    // raw_bits round-trip API, Display impl, parser integration.
    // ─────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────
    // DEF-194 follow-up 2026-04-27 — two MORE tests REMOVED
    // (tier-3 → tier-1 elevation):
    //
    //   - set_out_of_range_returns_err_with_idx_field_preserved
    //     → covered by OutOfRange field preservation pin (3 cases:
    //        boundary MAX_ROW_COLUMNS, well-beyond 99, pathological
    //        usize::MAX) + state-preservation assertion on each
    //   - raw_bits_round_trip
    //     → covered by raw_bits round-trip pin (7 patterns: zero,
    //        all-ones, two alternating, low/high single bit, magic)
    //
    // Both elevations live as `const _: () = { ... }` blocks above
    // the test module — verified at compile time, no runtime cycles.
    // ─────────────────────────────────────────────────────────

    // DEF-194 follow-up 2026-04-27 — `default_matches_empty` test
    // removed alongside the `Default` derive (tier-3 → tier-1 by
    // removal of the `default()` surface entirely; see the
    // FormatCodeSet struct decl above for the rationale).

    /// `OutOfRange::Display` carries the offending idx + max — used
    /// by future operator diagnostics. Pin the format so a body swap
    /// (idx vs max) is caught.
    #[test]
    fn out_of_range_display_carries_idx_and_max() {
        let err = OutOfRange { idx: 99, max: 32 };
        let rendered = format!("{err}");
        assert!(rendered.contains("99"), "Display must contain idx, got: {rendered}");
        assert!(rendered.contains("32"), "Display must contain max, got: {rendered}");
    }

    /// Size pin (runtime witness for the const-assert above). Catches
    /// a future field addition that bypasses repr(transparent) — the
    /// const-assert would also fire, but a runtime test gives a
    /// second witness in the test report and surfaces in diff review.
    #[test]
    fn size_is_4_bytes() {
        assert_eq!(core::mem::size_of::<FormatCodeSet>(), 4);
    }

    /// DEF-194 follow-up 2026-04-27 — glass-arch wide-RowDesc test.
    /// Pre-DEF-194 the storage was `[FormatCode; 32]` array; bit-pack
    /// post-194 stores all 32 codes in a single u32. The narrow
    /// 2-column test below covers ordinary parser integration; THIS
    /// test pins the wide edge: 32 columns with alternating formats,
    /// closing the §4.11.1 "tier-1 on paper, broken on max inputs" seam.
    ///
    /// Specifically pins:
    /// - **Bit ordering**: column N writes bit N (not bit 31-N or some
    ///   other inversion). Pre-194 array layout had no ordering
    ///   ambiguity; bit-pack post-194 introduces a bit-position
    ///   semantic that must match column index linearly.
    /// - **All 32 bits independently settable**: max-cap row produces
    ///   a FormatCodeSet with the full alternating pattern preserved
    ///   end-to-end through parser → RowDesc → format_code(idx).
    /// - **Bit 31 (high bit) round-trip**: covers the boundary that
    ///   `mask_for_const(31) = 0x80000000` against future changes
    ///   that might accidentally use sign-flagged shift.
    #[test]
    fn def194_wide_row_description_32_alternating_formats() {
        use alloc::vec::Vec;
        let mut frame: Vec<u8> = Vec::new();
        // MAX_ROW_COLUMNS = 32 fits i16 trivially; const-asserts in
        // this module pin the value. The Err arm is architecturally
        // dead but explicit (forbid-bundle bans `.expect()`).
        let n_cols: i16 = match i16::try_from(MAX_ROW_COLUMNS) {
            Ok(v) => v,
            Err(_) => return,
        };
        frame.extend_from_slice(&n_cols.to_be_bytes());
        for idx in 0..MAX_ROW_COLUMNS {
            let name = format!("c{idx}");
            frame.extend_from_slice(name.as_bytes());
            frame.push(0);
            frame.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            frame.extend_from_slice(&0i16.to_be_bytes()); // attr_num
            frame.extend_from_slice(&25u32.to_be_bytes()); // type_oid (TEXT)
            frame.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
            frame.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
            // Even idx = Text (0), odd idx = Binary (1).
            let fmt: i16 = if idx % 2 == 0 { 0 } else { 1 };
            frame.extend_from_slice(&fmt.to_be_bytes());
        }

        let result = parse_row_description(&frame);
        assert!(result.is_ok(), "32-col parse must succeed, got {result:?}");
        if let Ok(desc) = result {
            assert_eq!(usize::from(desc.n_columns()), MAX_ROW_COLUMNS);
            for idx in 0..MAX_ROW_COLUMNS {
                let expected = if idx % 2 == 0 {
                    FormatCode::Text
                } else {
                    FormatCode::Binary
                };
                assert_eq!(
                    desc.format_code(idx),
                    Some(expected),
                    "column {idx}: expected {expected:?} (idx % 2 == {})",
                    idx % 2,
                );
            }
            // Boundary: format_code(MAX_ROW_COLUMNS) is None.
            assert_eq!(desc.format_code(MAX_ROW_COLUMNS), None);
        }
    }

    /// `RowDesc` end-to-end: setting columns through the parser path
    /// produces a descriptor whose `format_code(idx)` reflects the
    /// stored bit-pack. Validates the integration of `RowDesc` ←
    /// `FormatCodeSet` (pre-DEF-194 was direct array slot write;
    /// post-194 is `FormatCodeSet::set`). Catches a parser-side
    /// regression that mis-wires the `format_codes.set(...)` call.
    #[test]
    fn row_desc_format_code_via_parser() {
        // Build a RowDescription frame body with two columns:
        // col 0: name="x", text format (code=0)
        // col 1: name="y", binary format (code=1)
        // PG layout: int16 count + per-column (cstring name + 18 bytes meta).
        let mut frame = alloc::vec::Vec::new();
        frame.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
        for (name, fmt) in [(b"x".as_ref(), 0i16), (b"y".as_ref(), 1i16)] {
            frame.extend_from_slice(name);
            frame.push(0); // NUL
            // table_oid(4) + attr_num(2) + type_oid(4) + type_size(2)
            // + type_mod(4) + format_code(2) = 18 bytes.
            frame.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            frame.extend_from_slice(&0i16.to_be_bytes()); // attr_num
            frame.extend_from_slice(&25u32.to_be_bytes()); // type_oid (TEXT)
            frame.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
            frame.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
            frame.extend_from_slice(&fmt.to_be_bytes()); // format_code
        }
        let result = parse_row_description(&frame);
        assert!(result.is_ok(), "parse must succeed, got {result:?}");
        if let Ok(desc) = result {
            assert_eq!(desc.n_columns(), 2);
            assert_eq!(desc.format_code(0), Some(FormatCode::Text));
            assert_eq!(desc.format_code(1), Some(FormatCode::Binary));
            // Trailing slots default to Text via FormatCodeSet::empty.
            for idx in 2..MAX_ROW_COLUMNS {
                assert_eq!(desc.format_code(idx), None, "idx {idx} >= n_columns");
            }
        }
    }
}
