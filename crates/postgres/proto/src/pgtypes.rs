//! Dep-free bsql-native semantic types for PostgreSQL columns the
//! compile-checked `query!` path can decode without an external crate.
//!
//! The primitive decode matrix (`i16`/`i32`/`i64`/`u32`/`bool`/`&str`/
//! `&[u8]`/`f32`/`f64`) covers the machine types, but a real schema is full
//! of `uuid` primary keys, `timestamptz` audit columns, and `json`/`jsonb`
//! documents. This module gives each a small, honest, **dependency-free**
//! Rust type so `query!` can type a column of that type instead of rejecting
//! it as unsupported.
//!
//! Each type is a thin newtype over the value PostgreSQL puts on the binary
//! wire — no calendar math, no JSON parsing, no allocation beyond what the
//! payload requires. To decode a column straight into a wider-ecosystem type
//! (`uuid::Uuid`, `time::OffsetDateTime`, `chrono::DateTime`,
//! `serde_json::Value`, …), a consumer registers a build-time **external-type
//! bridge**: a converter free function `fn(bsql::Timestamptz) -> Target`, and
//! `query!` then decodes that column directly into `Target`. The bridge is not
//! feature-flagged and forces no dependency — the target type and converter
//! travel as strings through the build catalog, so bsql itself depends on no
//! external crate. The types here are the always-available dep-free core these
//! bridges convert **from**.
//!
//! # Where the trait impls live
//!
//! The types are defined here; their wire impls sit beside their peers so
//! the drift-pins stay in one place per trait:
//!
//! - [`crate::decode::Cell<BinaryFmt>`] + [`crate::decode::EncodeBinary`]
//!   (decode / encode) — in `decode.rs`.
//! - [`crate::prepared::ColCellAt`] (the `query!` row-tuple marker) — in
//!   `prepared.rs`.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{self, Write as _};
use core::str::FromStr;

/// PostgreSQL `uuid` (catalog OID 2950) as a dependency-free 16-byte value.
///
/// The binary wire form of a `uuid` is exactly its 16 raw bytes, so this is
/// a newtype over `[u8; 16]` — the canonical, allocation-free representation.
/// [`Display`](fmt::Display) renders the standard lowercase hyphenated
/// `8-4-4-4-12` form and [`FromStr`] parses it (and the compact 32-hex form),
/// so a value round-trips through its text form.
///
/// # When to use / when not
///
/// Use this to decode a `uuid` column dependency-free. To decode straight into
/// `uuid::Uuid` fields instead, register a build-time external-type bridge
/// (a `fn(bsql::Uuid) -> uuid::Uuid` converter, e.g.
/// `uuid::Uuid::from_bytes(*v.as_bytes())`) — bsql forces no dependency. This
/// type is the always-available core, not a replacement for a full UUID library
/// (it deliberately has no v4/v7 generation, no versioning inspection).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// Wrap 16 raw bytes (the exact `uuid` binary wire payload) as a [`Uuid`].
    #[inline]
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Borrow the 16 raw bytes.
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Copy out the 16 raw bytes.
    #[inline]
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for Uuid {
    /// Render the canonical lowercase hyphenated form
    /// (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, byte) in self.0.iter().enumerate() {
            // Hyphens precede the byte at indices 4, 6, 8, 10 — the
            // `8-4-4-4-12` grouping (2 hex chars per byte).
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_char('-')?;
            }
            f.write_char(char::from(hex_nibble(byte.wrapping_shr(4))))?;
            f.write_char(char::from(hex_nibble(byte & 0x0F)))?;
        }
        Ok(())
    }
}

/// Classified failure of [`Uuid::from_str`]. Never a silent default — a
/// malformed UUID string is one of these, naming the offending position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum UuidParseError {
    /// The input is neither the 36-char hyphenated form nor the 32-char
    /// compact hex form. `len` is the actual character length.
    WrongLength {
        /// The rejected input's byte length.
        len: usize,
    },
    /// A hyphen was expected at this 0-based position (the `8-4-4-4-12`
    /// grouping of the 36-char form) but a different byte was found.
    InvalidHyphen {
        /// 0-based position where a `-` was required.
        pos: usize,
    },
    /// A non-hex byte appeared where a hex digit was required.
    InvalidChar {
        /// 0-based position of the offending byte.
        pos: usize,
    },
}

impl fmt::Display for UuidParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { len } => write!(
                f,
                "uuid string length {len} is neither 36 (hyphenated) nor 32 (compact hex)",
            ),
            Self::InvalidHyphen { pos } => {
                write!(f, "uuid string: expected '-' at position {pos}")
            }
            Self::InvalidChar { pos } => {
                write!(f, "uuid string: non-hex byte at position {pos}")
            }
        }
    }
}

impl core::error::Error for UuidParseError {}

impl FromStr for Uuid {
    type Err = UuidParseError;

    /// Parse the standard hyphenated `8-4-4-4-12` form OR the compact
    /// 32-hex-digit form. Case-insensitive on the hex digits. Any other
    /// shape (braces, URN prefix, wrong length) is a classified
    /// [`UuidParseError`], never a lenient guess.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hyphenated = match s.len() {
            32 => false,
            36 => true,
            other => return Err(UuidParseError::WrongLength { len: other }),
        };
        let mut out = [0u8; 16];
        let mut out_idx = 0usize;
        let mut high: Option<u8> = None;
        for (i, byte) in s.bytes().enumerate() {
            if hyphenated && matches!(i, 8 | 13 | 18 | 23) {
                if byte != b'-' {
                    return Err(UuidParseError::InvalidHyphen { pos: i });
                }
                continue;
            }
            let nibble = hex_value(byte).ok_or(UuidParseError::InvalidChar { pos: i })?;
            match high {
                None => high = Some(nibble),
                Some(hi) => {
                    let value = hi.saturating_mul(16).saturating_add(nibble);
                    match out.get_mut(out_idx) {
                        Some(slot) => *slot = value,
                        // Unreachable: the length gate above admits exactly
                        // 16 output bytes. Fail closed rather than panic.
                        None => return Err(UuidParseError::WrongLength { len: s.len() }),
                    }
                    out_idx = out_idx.saturating_add(1);
                    high = None;
                }
            }
        }
        // A well-formed input consumes exactly 16 bytes with no dangling
        // half-byte. The length gate guarantees this, but verify rather
        // than assume.
        if out_idx != 16 || high.is_some() {
            return Err(UuidParseError::WrongLength { len: s.len() });
        }
        Ok(Self(out))
    }
}

/// PostgreSQL `timestamptz` (catalog OID 1184): microseconds since the
/// PostgreSQL epoch **2000-01-01 00:00:00 UTC** (NOT the Unix epoch).
///
/// The binary wire form is an `i64` microsecond count relative to that
/// epoch. A `timestamptz` is always UTC on the wire (PostgreSQL stores UTC
/// and only applies the session `TimeZone` for *text* rendering, which the
/// binary path never uses), so [`Self::to_unix_micros`] is an exact
/// conversion.
///
/// This is a dependency-free value carrier, not a calendar library: it does
/// not format a human date (for that, register an external-type bridge to a
/// `chrono::DateTime` / `time::OffsetDateTime` via a converter free function).
/// It exposes the raw microsecond count and an honest Unix-epoch conversion
/// so the epoch offset is not a footgun.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamptz(i64);

/// Microseconds between the Unix epoch (1970-01-01) and the PostgreSQL epoch
/// (2000-01-01): `946_684_800` seconds × 1_000_000. Written as a literal so
/// there is no runtime multiply (and no `clippy::arithmetic_side_effects`
/// exception); pinned below against the factored form.
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

// Drift-pin: the literal equals `946_684_800 s × 1_000_000 µs/s`. A typo in
// the literal fails the build here rather than silently shifting every
// timestamp by a constant.
const _: () = assert!(PG_EPOCH_UNIX_MICROS == 946_684_800 * 1_000_000);

impl Timestamptz {
    /// Wrap a raw PostgreSQL-epoch microsecond count (2000-01-01 UTC based).
    ///
    /// Accepts **any** `i64` by design, with no range check: this is a
    /// faithful wrapper of the value on the wire, and PostgreSQL's
    /// `timestamptz` legitimately uses the full `i64` range — including the
    /// `infinity` / `-infinity` sentinels it transmits as `i64::MAX` /
    /// `i64::MIN`. The overflow guard lives on [`Self::to_unix_micros`]
    /// instead, the only operation that adds to the raw value (the fixed
    /// 30-year epoch shift, which is what can actually overflow). Constructor
    /// and converter are asymmetric on purpose — not a missing check.
    #[inline]
    #[must_use]
    pub const fn from_micros(pg_epoch_micros: i64) -> Self {
        Self(pg_epoch_micros)
    }

    /// The raw microsecond count relative to the PostgreSQL epoch
    /// (2000-01-01 00:00:00 UTC).
    #[inline]
    #[must_use]
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Convert to microseconds since the **Unix** epoch (1970-01-01 UTC).
    ///
    /// Returns `None` on the (astronomically distant) `i64` overflow rather
    /// than wrapping — the conversion adds a fixed 30-year offset, so only a
    /// value within 30 years of `i64::MAX` microseconds can overflow.
    #[inline]
    #[must_use]
    pub const fn to_unix_micros(self) -> Option<i64> {
        self.0.checked_add(PG_EPOCH_UNIX_MICROS)
    }

    /// Build from microseconds since the **Unix** epoch (1970-01-01 UTC).
    ///
    /// Returns `None` on `i64` overflow of the epoch shift rather than
    /// wrapping (the inverse of [`Self::to_unix_micros`]).
    #[inline]
    #[must_use]
    pub const fn from_unix_micros(unix_micros: i64) -> Option<Self> {
        match unix_micros.checked_sub(PG_EPOCH_UNIX_MICROS) {
            Some(pg) => Some(Self(pg)),
            None => None,
        }
    }
}

/// PostgreSQL `timestamp` (catalog OID 1114): a **naive** wall-clock
/// timestamp with NO time zone, as microseconds since the PostgreSQL epoch
/// 2000-01-01 00:00:00.
///
/// The wire form matches [`Timestamptz`] (an `i64` microsecond count), but
/// the value carries **no zone**: it is a wall-clock reading, not an instant.
/// There is deliberately no `to_unix_micros` here — converting a
/// zone-less wall clock to a Unix instant would require assuming a zone,
/// which would be a silent lie. Use [`Self::as_micros`] and apply your own
/// zone knowledge, or register an external-type bridge to a naive-datetime type
/// (e.g. `chrono::NaiveDateTime`) via a converter free function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Wrap a raw PostgreSQL-epoch microsecond count (2000-01-01 based,
    /// zone-less).
    #[inline]
    #[must_use]
    pub const fn from_micros(pg_epoch_micros: i64) -> Self {
        Self(pg_epoch_micros)
    }

    /// The raw microsecond count relative to the PostgreSQL epoch
    /// (2000-01-01 00:00:00, wall-clock, no zone).
    #[inline]
    #[must_use]
    pub const fn as_micros(self) -> i64 {
        self.0
    }
}

/// PostgreSQL `json` (catalog OID 114): a JSON document as its raw UTF-8
/// text, verbatim.
///
/// bsql is deliberately **dependency-free** here: it does NOT parse,
/// validate, or canonicalise the JSON structure — the exact bytes PostgreSQL
/// sends (which the server already validated as well-formed JSON on the way
/// in) are surfaced as a UTF-8 string via [`Self::as_str`]. To decode straight
/// into a parsed `serde_json::Value` (or `sonic_rs::Value`), register an
/// external-type bridge via a converter free function; this type is the
/// always-available dep-free core.
///
/// The `json` wire form is the raw text (no framing), unlike [`Jsonb`] which
/// carries a leading version byte.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Json(String);

impl Json {
    /// Wrap a JSON document's text.
    #[inline]
    #[must_use]
    pub const fn new(text: String) -> Self {
        Self(text)
    }

    /// Borrow the JSON document's text.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the owned text.
    #[inline]
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Json {
    /// Write the raw JSON text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// PostgreSQL `jsonb` (catalog OID 3802): a JSON document as its decoded
/// UTF-8 text.
///
/// The `jsonb` **binary wire** form is a single version byte (currently
/// always `1`) followed by the UTF-8 JSON text; this type stores the text
/// after the version byte is validated and stripped. Like [`Json`] it is
/// dependency-free — the JSON structure is not parsed. A version byte other
/// than `1`, or an empty body, is a classified
/// [`crate::DecodeError`](crate::decode::DecodeError), never silently
/// accepted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Jsonb(String);

impl Jsonb {
    /// Wrap a JSON document's text (the version byte is a wire concern,
    /// handled by the decoder / encoder — this holds only the text).
    #[inline]
    #[must_use]
    pub const fn new(text: String) -> Self {
        Self(text)
    }

    /// Borrow the JSON document's text.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the owned text.
    #[inline]
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Jsonb {
    /// Write the raw JSON text (without the wire version byte).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The largest display scale (fractional decimal digits) PostgreSQL's binary
/// `numeric` wire encodes without loss: `NUMERIC_DSCALE_MASK` = `0x3FFF`.
///
/// `numeric_recv` masks the received `dscale` with this value, so a value with
/// more than 16383 fractional digits would be silently corrupted on the wire.
/// [`Numeric::from_str`] rejects such a value as a classified error rather than
/// constructing one that cannot round-trip. Written as a literal, pinned below.
const NUMERIC_MAX_DSCALE: u16 = 0x3FFF;

const _: () = assert!(NUMERIC_MAX_DSCALE == 16383);

/// One base-10000 digit group's decimal width — PostgreSQL's `DEC_DIGITS`.
/// Each wire digit group holds four decimal digits (a value `0..=9999`).
const NBASE_DIGITS: usize = 4;

/// PostgreSQL `numeric` / `decimal` (catalog OID 1700): an exact,
/// arbitrary-precision decimal value, dependency-free.
///
/// A `numeric` on the binary wire is `ndigits · weight · sign · dscale` followed
/// by `ndigits` base-10000 digit groups (each `0..=9999`, most-significant
/// first). The value of a finite number is `± Σ digits[i] · 10000^(weight − i)`;
/// `dscale` is the *display* scale (fractional decimal digits) and governs
/// rendering, not the stored magnitude. This type stores exactly those fields —
/// the digit groups in a [`Box<[u16]>`](alloc::boxed::Box) — so it is **lossless**
/// and **arbitrary-precision**: a value with hundreds of digits (well past the
/// `i128` range) is held exactly. That variable-length payload is one heap
/// allocation per value, inherent to a numeric of unbounded size; every scalar
/// bsql-native type but this one is fixed-width.
///
/// `NaN`, `+Infinity`, and `-Infinity` are representable (PostgreSQL emits the
/// infinities from version 14). [`Display`](fmt::Display) renders the exact
/// PostgreSQL text form (`"NaN"` / `"Infinity"` / `"-Infinity"` for the specials,
/// the exact decimal string with `dscale` fractional digits for a number), and
/// [`FromStr`] parses it, so a value round-trips through its text form.
///
/// # Canonical form
///
/// PostgreSQL always sends a canonical encoding: leading and trailing all-zero
/// digit *groups* are stripped (interior zero groups are kept), and `dscale`
/// carries the fractional width independently of the stored groups. [`FromStr`]
/// canonicalises identically, so two `Numeric`s are [`Eq`] iff they render the
/// same string — `1.5` and `1.50` differ (their `dscale` differs), matching
/// PostgreSQL's distinct text renderings, while `NaN == NaN` (numeric `NaN` is
/// reflexively equal, unlike a float `NaN`). The decode path stores exactly what
/// the server sends, never re-normalising, so it stays faithful.
///
/// # When to use / when not
///
/// Use this to decode a `numeric` column dependency-free and to bind a numeric
/// parameter. It is a faithful VALUE carrier, not a decimal-arithmetic library:
/// it does no addition, rounding, or comparison of magnitudes. To compute with
/// the value, register a build-time external-type bridge to a decimal crate
/// (`rust_decimal::Decimal`, `bigdecimal::BigDecimal`, …) via a converter free
/// function — the exact decimal string ([`Display`](fmt::Display)) or the raw
/// components ([`weight`](Self::weight) / [`scale`](Self::scale) /
/// [`base_10000_digits`](Self::base_10000_digits)) reconstruct the value with no
/// loss.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Numeric(NumericRepr);

/// The internal representation of a [`Numeric`]: a finite number's raw wire
/// fields, or one of the three non-finite specials. Private so the digit-group
/// invariant (`0..NBASE`, canonical) cannot be violated by construction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NumericRepr {
    /// A finite number `± Σ digits[i] · 10000^(weight − i)`, displayed with
    /// `dscale` fractional decimal digits. `digits` are base-10000 groups
    /// (each `0..NBASE`), most-significant first, in canonical form (no leading
    /// or trailing all-zero group). Zero is `digits: []`, `weight: 0`.
    Finite {
        /// The value's sign (`true` = negative). Never `true` for zero.
        negative: bool,
        /// The base-10000 exponent of the first (most-significant) digit group.
        weight: i16,
        /// The display scale — fractional decimal digits (`0..=16383`).
        dscale: u16,
        /// The base-10000 digit groups, most-significant first.
        digits: Box<[u16]>,
    },
    /// Not-a-number (`'NaN'::numeric`).
    NaN,
    /// Positive infinity (`'Infinity'::numeric`; PostgreSQL 14+).
    PosInfinity,
    /// Negative infinity (`'-Infinity'::numeric`; PostgreSQL 14+).
    NegInfinity,
}

impl Numeric {
    /// The `NaN` value.
    #[inline]
    #[must_use]
    pub const fn nan() -> Self {
        Self(NumericRepr::NaN)
    }

    /// The `+Infinity` value (PostgreSQL 14+).
    #[inline]
    #[must_use]
    pub const fn infinity() -> Self {
        Self(NumericRepr::PosInfinity)
    }

    /// The `-Infinity` value (PostgreSQL 14+).
    #[inline]
    #[must_use]
    pub const fn neg_infinity() -> Self {
        Self(NumericRepr::NegInfinity)
    }

    /// Construct a finite number from its canonical raw wire fields. Crate-only:
    /// the caller (the binary decoder and [`FromStr`]) guarantees the digit
    /// groups are canonical (`0..NBASE`, no leading / trailing zero group), so
    /// the type-level invariant holds by construction.
    #[inline]
    pub(crate) fn finite(negative: bool, weight: i16, dscale: u16, digits: Box<[u16]>) -> Self {
        Self(NumericRepr::Finite {
            negative,
            weight,
            dscale,
            digits,
        })
    }

    /// `true` iff this is `NaN`.
    #[inline]
    #[must_use]
    pub fn is_nan(&self) -> bool {
        matches!(self.0, NumericRepr::NaN)
    }

    /// `true` iff this is `+Infinity` or `-Infinity`.
    #[inline]
    #[must_use]
    pub fn is_infinite(&self) -> bool {
        matches!(self.0, NumericRepr::PosInfinity | NumericRepr::NegInfinity)
    }

    /// `true` iff this is a finite number (not `NaN` / `±Infinity`).
    #[inline]
    #[must_use]
    pub fn is_finite(&self) -> bool {
        matches!(self.0, NumericRepr::Finite { .. })
    }

    /// `true` iff the value carries a negative sign — a negative finite number,
    /// or `-Infinity`. (`NaN` has no sign; zero is never negative.)
    #[inline]
    #[must_use]
    pub fn is_negative(&self) -> bool {
        match &self.0 {
            NumericRepr::Finite { negative, .. } => *negative,
            NumericRepr::NegInfinity => true,
            NumericRepr::NaN | NumericRepr::PosInfinity => false,
        }
    }

    /// The base-10000 digit groups (each `0..NBASE`), most-significant first, in
    /// canonical form. Empty for zero and for the non-finite specials. Paired
    /// with [`weight`](Self::weight) and [`scale`](Self::scale) this reconstructs
    /// the exact value for a bridge converter, with no allocation.
    #[inline]
    #[must_use]
    pub fn base_10000_digits(&self) -> &[u16] {
        match &self.0 {
            NumericRepr::Finite { digits, .. } => digits,
            NumericRepr::NaN | NumericRepr::PosInfinity | NumericRepr::NegInfinity => &[],
        }
    }

    /// The base-10000 exponent of the first digit group. `0` for zero and for
    /// the non-finite specials (which carry no digits).
    #[inline]
    #[must_use]
    pub fn weight(&self) -> i16 {
        match &self.0 {
            NumericRepr::Finite { weight, .. } => *weight,
            NumericRepr::NaN | NumericRepr::PosInfinity | NumericRepr::NegInfinity => 0,
        }
    }

    /// The display scale — the number of fractional decimal digits
    /// [`Display`](fmt::Display) renders. `0` for the non-finite specials.
    #[inline]
    #[must_use]
    pub fn scale(&self) -> u16 {
        match &self.0 {
            NumericRepr::Finite { dscale, .. } => *dscale,
            NumericRepr::NaN | NumericRepr::PosInfinity | NumericRepr::NegInfinity => 0,
        }
    }
}

impl fmt::Display for Numeric {
    /// Render the exact PostgreSQL text form. A finite number prints its
    /// integer groups (the most-significant group without leading zeros,
    /// interior groups zero-padded to four digits), then — when `dscale > 0` —
    /// a `.` and exactly `dscale` fractional digits (drawn from the fractional
    /// groups, zero-filled past the stored digits). The specials print `NaN` /
    /// `Infinity` / `-Infinity`, matching PostgreSQL's own rendering.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (negative, weight, dscale, digits) = match &self.0 {
            NumericRepr::NaN => return f.write_str("NaN"),
            NumericRepr::PosInfinity => return f.write_str("Infinity"),
            NumericRepr::NegInfinity => return f.write_str("-Infinity"),
            NumericRepr::Finite {
                negative,
                weight,
                dscale,
                digits,
            } => (*negative, *weight, *dscale, digits),
        };
        if negative {
            f.write_char('-')?;
        }
        // Integer part. `weight < 0` means the value is below 1, so the integer
        // part is a single `0`; otherwise every group from index 0 to `weight`
        // is emitted (a zero-filled group past the stored digits).
        if weight < 0 {
            f.write_char('0')?;
        } else {
            let mut group_idx: usize = 0;
            // `weight >= 0` here, so `weight + 1` groups form the integer part
            // (index 0 through `weight`, a zero-filled group past the stored
            // digits). `try_from` cannot fail for a non-negative `i16`.
            let int_groups = usize::try_from(weight).unwrap_or(0).saturating_add(1);
            while group_idx < int_groups {
                let dig = digits.get(group_idx).copied().unwrap_or(0);
                if group_idx == 0 {
                    write!(f, "{dig}")?;
                } else {
                    write!(f, "{dig:04}")?;
                }
                group_idx = group_idx.saturating_add(1);
            }
        }
        if dscale == 0 {
            return Ok(());
        }
        f.write_char('.')?;
        // Fractional groups begin at digit index `weight + 1` (which may be
        // negative — a leading run of implicit zero groups — or past the stored
        // digits — a trailing run of implicit zeros). `printed` counts emitted
        // fractional decimal digits; each group contributes up to four.
        let mut frac_group = i32::from(weight).saturating_add(1);
        let mut printed: u16 = 0;
        while printed < dscale {
            let dig = if frac_group >= 0 {
                usize::try_from(frac_group)
                    .ok()
                    .and_then(|i| digits.get(i).copied())
                    .unwrap_or(0)
            } else {
                0
            };
            // Emit up to four decimal digits of this group, capped at the
            // remaining `dscale`. `group_digits` holds the four ASCII chars.
            let mut group_digits = [b'0'; NBASE_DIGITS];
            format_group_ascii(dig, &mut group_digits);
            let remaining = dscale.saturating_sub(printed);
            let take = core::cmp::min(remaining, NBASE_DIGITS_U16);
            for &ch in group_digits.iter().take(usize::from(take)) {
                f.write_char(char::from(ch))?;
            }
            printed = printed.saturating_add(take);
            frac_group = frac_group.saturating_add(1);
        }
        Ok(())
    }
}

/// [`NBASE_DIGITS`] as a `u16` (four), for the fractional-digit budget.
const NBASE_DIGITS_U16: u16 = 4;

const _: () = assert!(NBASE_DIGITS == 4 && NBASE_DIGITS_U16 == 4);

/// Write the four decimal digits of a base-10000 group value (`0..NBASE`) into
/// `out` as ASCII, most-significant first (`5000` -> `b"5000"`, `7` ->
/// `b"0007"`). Pure `u16` arithmetic via `checked` remainder / division method
/// calls (the crate bans the `/` and `%` operators), never a panic.
fn format_group_ascii(mut value: u16, out: &mut [u8; NBASE_DIGITS]) {
    // Fill from the least-significant digit backwards.
    for slot in out.iter_mut().rev() {
        let digit = value.checked_rem(10).unwrap_or(0);
        *slot = b'0'.wrapping_add(u8_from_u16_digit(digit));
        value = value.checked_div(10).unwrap_or(0);
    }
}

/// A single decimal digit `0..=9` (a `u16`) narrowed to `u8`. The input is a
/// remainder mod 10 so it always fits; the `unwrap_or` is a dead fail-closed
/// landing pad, never reached.
#[inline]
fn u8_from_u16_digit(digit: u16) -> u8 {
    u8::try_from(digit).unwrap_or(0)
}

/// Classified failure of [`Numeric::from_str`]. Never a silent default — a
/// malformed decimal string is one of these, naming the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NumericParseError {
    /// The input was empty, or held a sign / decimal point with no digits.
    NoDigits,
    /// A byte that is neither a digit, a leading sign, nor a single decimal
    /// point appeared. `pos` is its 0-based position. Exponent (`e` / `E`)
    /// notation lands here — it is rejected rather than silently parsed.
    InvalidChar {
        /// 0-based position of the offending byte.
        pos: usize,
    },
    /// More than one decimal point appeared.
    MultiplePoints,
    /// The value's integer part is too large for the binary `numeric` wire
    /// format (its base-10000 exponent `weight` overflows the `i16` field —
    /// roughly 131072 integer digits, PostgreSQL's own limit).
    IntegerOverflow,
    /// The value has more fractional digits than the binary `numeric` wire
    /// format's display scale can carry without loss (`dscale > 16383`).
    FractionOverflow,
}

impl fmt::Display for NumericParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDigits => f.write_str("numeric string has no digits"),
            Self::InvalidChar { pos } => {
                write!(f, "numeric string: invalid byte at position {pos}")
            }
            Self::MultiplePoints => f.write_str("numeric string has more than one decimal point"),
            Self::IntegerOverflow => f.write_str(
                "numeric integer part is too large for the binary wire format (weight overflows i16)",
            ),
            Self::FractionOverflow => {
                f.write_str("numeric has more than 16383 fractional digits (exceeds wire dscale)")
            }
        }
    }
}

impl core::error::Error for NumericParseError {}

impl FromStr for Numeric {
    type Err = NumericParseError;

    /// Parse the exact PostgreSQL text form: an optional `+` / `-` sign then a
    /// decimal number (`123`, `1.5`, `.5`, `100.00`), OR one of the specials
    /// `NaN` / `Infinity` / `-Infinity` / `inf` (case-insensitive, optional
    /// sign). Exponent notation (`1e5`) is a classified
    /// [`NumericParseError::InvalidChar`], never a lenient parse. The result is
    /// canonical (leading / trailing zero groups stripped), so
    /// [`Display`](fmt::Display) round-trips it.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(special) = parse_numeric_special(s) {
            return Ok(special);
        }
        parse_numeric_finite(s)
    }
}

/// Match the non-finite specials case-insensitively (`NaN`, `Infinity`, `inf`,
/// with an optional leading sign for the infinities). Returns `None` if `s` is
/// not a special, so the finite parser runs.
fn parse_numeric_special(s: &str) -> Option<Numeric> {
    let (negative, body) = match s.as_bytes().split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s.as_bytes()),
    };
    if s.eq_ignore_ascii_case("nan") {
        return Some(Numeric::nan());
    }
    // `Infinity` / `inf` — the sign was split above, so match the bare word.
    if body.eq_ignore_ascii_case(b"infinity") || body.eq_ignore_ascii_case(b"inf") {
        return Some(if negative {
            Numeric::neg_infinity()
        } else {
            Numeric::infinity()
        });
    }
    None
}

/// Parse a finite decimal string into a canonical [`Numeric`]. Splits the sign,
/// integer digits, and fractional digits (rejecting any other byte, a second
/// decimal point, or an empty digit set), groups them into base-10000 digits,
/// computes the canonical `weight` / `dscale`, and strips leading / trailing
/// zero groups.
fn parse_numeric_finite(s: &str) -> Result<Numeric, NumericParseError> {
    let bytes = s.as_bytes();
    let (negative, rest_off) = match bytes.split_first() {
        Some((b'-', _)) => (true, 1usize),
        Some((b'+', _)) => (false, 1usize),
        _ => (false, 0usize),
    };
    let mut int_digits: Vec<u8> = Vec::new();
    let mut frac_digits: Vec<u8> = Vec::new();
    let mut seen_point = false;
    let mut any_digit = false;
    for (offset, &b) in bytes.iter().enumerate() {
        if offset < rest_off {
            continue;
        }
        match b {
            b'.' => {
                if seen_point {
                    return Err(NumericParseError::MultiplePoints);
                }
                seen_point = true;
            }
            b'0'..=b'9' => {
                any_digit = true;
                let digit = b.wrapping_sub(b'0');
                if seen_point {
                    frac_digits.push(digit);
                } else {
                    int_digits.push(digit);
                }
            }
            _ => return Err(NumericParseError::InvalidChar { pos: offset }),
        }
    }
    if !any_digit {
        return Err(NumericParseError::NoDigits);
    }
    let dscale = u16::try_from(frac_digits.len())
        .ok()
        .filter(|d| *d <= NUMERIC_MAX_DSCALE)
        .ok_or(NumericParseError::FractionOverflow)?;

    // Integer groups: chunk the integer decimal digits into fours from the
    // RIGHT (least-significant), then reverse to most-significant-first.
    let mut groups: Vec<u16> = Vec::new();
    let mut int_groups: Vec<u16> = Vec::new();
    for chunk in int_digits.rchunks(NBASE_DIGITS) {
        int_groups.push(group_from_digits(chunk, false));
    }
    int_groups.reverse();
    let int_group_count = int_groups.len();
    groups.append(&mut int_groups);
    // Fractional groups: chunk from the LEFT (most-significant); the final
    // short chunk is the HIGH digits of its group, so it is left-aligned
    // (right-padded with zeros).
    for chunk in frac_digits.chunks(NBASE_DIGITS) {
        groups.push(group_from_digits(chunk, true));
    }

    // The most-significant integer group sits at base-10000 exponent
    // `int_group_count - 1`; with no integer groups the first fractional group
    // is at exponent -1.
    let mut weight: i16 = if int_group_count == 0 {
        -1
    } else {
        i16::try_from(int_group_count.saturating_sub(1))
            .map_err(|_| NumericParseError::IntegerOverflow)?
    };

    // Strip leading zero groups (each drops the weight by one).
    let mut start = 0usize;
    while groups.get(start).copied() == Some(0) {
        start = start.saturating_add(1);
        weight = weight.saturating_sub(1);
    }
    // Strip trailing zero groups (dscale is unaffected).
    let mut end = groups.len();
    while end > start && groups.get(end.saturating_sub(1)).copied() == Some(0) {
        end = end.saturating_sub(1);
    }
    let digits: Box<[u16]> = groups.get(start..end).unwrap_or(&[]).into();
    if digits.is_empty() {
        // Zero — a canonical zero carries no digits and weight 0, but keeps its
        // display scale (`0.000` has dscale 3). Sign is dropped (no negative
        // zero).
        return Ok(Numeric::finite(false, 0, dscale, Box::from([].as_slice())));
    }
    Ok(Numeric::finite(negative, weight, dscale, digits))
}

/// Parse a 1..=4-byte slice of decimal digit VALUES (`0..=9`, already
/// sign-checked by the caller) into a base-10000 group. When `fractional`, the
/// slice holds the HIGH digits of the group, so the value is left-aligned
/// (`[5]` -> `5000`); otherwise it is the low digits (`[5]` -> `5`).
fn group_from_digits(digits: &[u8], fractional: bool) -> u16 {
    let mut acc: u16 = 0;
    for &d in digits {
        acc = acc.saturating_mul(10).saturating_add(u16::from(d));
    }
    if fractional {
        // Left-align: scale by 10 for each missing low digit.
        let missing = NBASE_DIGITS.saturating_sub(digits.len());
        for _ in 0..missing {
            acc = acc.saturating_mul(10);
        }
    }
    acc
}

/// ASCII hex digit (lowercase) for a 0..=15 nibble. The `_` arm is
/// unreachable for a masked nibble; it returns `b'?'` (a fail-closed
/// non-hex byte) rather than panicking.
const fn hex_nibble(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0'.saturating_add(nibble),
        10..=15 => b'a'.saturating_add(nibble.saturating_sub(10)),
        _ => b'?',
    }
}

/// Value 0..=15 of an ASCII hex digit byte (either case), or `None` for a
/// non-hex byte.
const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte.saturating_sub(b'0')),
        b'a'..=b'f' => Some(byte.saturating_sub(b'a').saturating_add(10)),
        b'A'..=b'F' => Some(byte.saturating_sub(b'A').saturating_add(10)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    //! Unit spec for the dep-free types: UUID text round-trip + classified
    //! parse failures, and the timestamp epoch arithmetic.
    use super::*;
    use alloc::string::ToString as _;

    #[test]
    fn uuid_display_round_trips_hyphenated() {
        let bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let u = Uuid::from_bytes(bytes);
        let text = u.to_string();
        assert_eq!(text, "550e8400-e29b-41d4-a716-446655440000");
        // Display -> FromStr is the identity.
        assert_eq!(Uuid::from_str(&text), Ok(u));
        assert_eq!(u.as_bytes(), &bytes);
        assert_eq!(u.to_bytes(), bytes);
    }

    #[test]
    fn uuid_from_str_accepts_compact_and_uppercase() {
        let hyphenated = "550e8400-e29b-41d4-a716-446655440000";
        let compact = "550E8400E29B41D4A716446655440000"; // 32, uppercase
        assert_eq!(
            Uuid::from_str(compact).expect("compact parses"),
            Uuid::from_str(hyphenated).expect("hyphenated parses"),
        );
    }

    #[test]
    fn uuid_from_str_classifies_bad_input() {
        // Wrong length.
        assert_eq!(
            Uuid::from_str("abcd"),
            Err(UuidParseError::WrongLength { len: 4 }),
        );
        // A non-hyphen where the 36-char form requires one (position 8).
        assert_eq!(
            Uuid::from_str("550e8400xe29b-41d4-a716-446655440000"),
            Err(UuidParseError::InvalidHyphen { pos: 8 }),
        );
        // A non-hex byte in the compact form (position 0).
        assert_eq!(
            Uuid::from_str("z50e8400e29b41d4a716446655440000"),
            Err(UuidParseError::InvalidChar { pos: 0 }),
        );
    }

    #[test]
    fn timestamptz_epoch_conversion_is_exact_and_reversible() {
        // The PG epoch itself is Unix 946_684_800 s = 946_684_800_000_000 µs.
        let epoch = Timestamptz::from_micros(0);
        assert_eq!(epoch.to_unix_micros(), Some(946_684_800_000_000));

        // A known instant: 2000-01-01 00:00:01 UTC = 1 s after the PG epoch.
        let one_sec = Timestamptz::from_micros(1_000_000);
        assert_eq!(one_sec.as_micros(), 1_000_000);
        assert_eq!(one_sec.to_unix_micros(), Some(946_684_801_000_000));

        // Round-trip through the Unix representation.
        let unix = 1_700_000_000_000_000_i64; // some 2023 instant, µs
        let ts = Timestamptz::from_unix_micros(unix).expect("in range");
        assert_eq!(ts.to_unix_micros(), Some(unix));
    }

    #[test]
    fn timestamptz_epoch_conversion_saturates_at_overflow() {
        // Near i64::MAX, adding the 30-year offset overflows -> honest None,
        // never a wrapped instant.
        let far = Timestamptz::from_micros(i64::MAX);
        assert_eq!(far.to_unix_micros(), None);
        assert_eq!(Timestamptz::from_unix_micros(i64::MIN), None);
    }

    #[test]
    fn from_micros_wraps_any_i64_including_infinity_sentinels() {
        // `from_micros` is unchecked BY DESIGN: PG's `timestamptz` uses the
        // full i64 range, transmitting `infinity` / `-infinity` as i64::MAX /
        // i64::MIN. The constructor must wrap them faithfully (the overflow
        // guard lives on `to_unix_micros`, not here).
        assert_eq!(Timestamptz::from_micros(i64::MAX).as_micros(), i64::MAX);
        assert_eq!(Timestamptz::from_micros(i64::MIN).as_micros(), i64::MIN);
    }

    #[test]
    fn timestamp_is_raw_micros_only() {
        let ts = Timestamp::from_micros(42);
        assert_eq!(ts.as_micros(), 42);
    }

    /// The canonical decimal battery: every value's `FromStr` -> `Display`
    /// reproduces the input string EXACTLY (canonical forms round-trip), and
    /// the parse is lossless (arbitrary precision past `i128`). This is the
    /// dep-free proof of the Display / FromStr algorithm that the live
    /// round-trip battery then confirms against PostgreSQL's own rendering.
    #[test]
    fn numeric_from_str_display_round_trips_canonical() {
        for s in [
            "0",
            "1",
            "-1",
            "0.1",
            "0.0001",
            "3.14159265358979323846",
            "1.500",
            "0.000",
            "100",
            "10000",
            "12340000",
            "100000001", // interior zero group
            "-123456789012345678901234567890", // > i128 magnitude
            "9999999999999999999999999999999999999999.0001", // arbitrary precision
            "0.00001234",
            "1234567890.987654321",
        ] {
            let n = Numeric::from_str(s).expect("canonical decimal parses");
            assert_eq!(n.to_string(), s, "round-trip mismatch for {s}");
        }
    }

    /// `.5` and `0.5` are the SAME canonical value (identical `weight` /
    /// `digits` / `dscale`), and both render `0.5` — the leading-`0` form.
    #[test]
    fn numeric_leading_dot_normalises() {
        let a = Numeric::from_str(".5").expect(". form parses");
        let b = Numeric::from_str("0.5").expect("0. form parses");
        assert_eq!(a, b);
        assert_eq!(a.to_string(), "0.5");
        assert_eq!(b.to_string(), "0.5");
    }

    /// `1.5` and `1.50` are DISTINCT (their `dscale` differs), matching
    /// PostgreSQL's distinct text renderings — the value carrier is
    /// scale-faithful, not scale-collapsing.
    #[test]
    fn numeric_scale_is_significant() {
        let a = Numeric::from_str("1.5").expect("parses");
        let b = Numeric::from_str("1.50").expect("parses");
        assert_ne!(a, b);
        assert_eq!(a.to_string(), "1.5");
        assert_eq!(b.to_string(), "1.50");
    }

    /// The three non-finite specials round-trip and classify correctly, with
    /// `NaN == NaN` (numeric `NaN` is reflexively equal, unlike a float `NaN`).
    #[test]
    fn numeric_specials_round_trip() {
        let nan = Numeric::from_str("NaN").expect("NaN parses");
        assert!(nan.is_nan() && !nan.is_finite() && !nan.is_infinite());
        assert_eq!(nan.to_string(), "NaN");
        assert_eq!(nan, Numeric::nan());

        let pinf = Numeric::from_str("Infinity").expect("Infinity parses");
        assert!(pinf.is_infinite() && !pinf.is_negative());
        assert_eq!(pinf.to_string(), "Infinity");
        assert_eq!(pinf, Numeric::infinity());

        let ninf = Numeric::from_str("-Infinity").expect("-Infinity parses");
        assert!(ninf.is_infinite() && ninf.is_negative());
        assert_eq!(ninf.to_string(), "-Infinity");
        assert_eq!(ninf, Numeric::neg_infinity());

        // Case-insensitive aliases.
        assert_eq!(Numeric::from_str("nan").expect("nan"), Numeric::nan());
        assert_eq!(Numeric::from_str("inf").expect("inf"), Numeric::infinity());
        assert_eq!(
            Numeric::from_str("-inf").expect("-inf"),
            Numeric::neg_infinity(),
        );
    }

    /// Raw component accessors reconstruct the value for a zero-alloc bridge.
    #[test]
    fn numeric_component_accessors() {
        let n = Numeric::from_str("1.5").expect("parses");
        assert_eq!(n.weight(), 0);
        assert_eq!(n.scale(), 1);
        assert_eq!(n.base_10000_digits(), &[1, 5000]);
        assert!(!n.is_negative());

        let z = Numeric::from_str("0.000").expect("parses");
        assert_eq!(z.weight(), 0);
        assert_eq!(z.scale(), 3);
        let empty: &[u16] = &[];
        assert_eq!(z.base_10000_digits(), empty);
    }

    /// Every malformed shape is a CLASSIFIED [`NumericParseError`], never a
    /// panic or a lenient guess. Exponent notation is deliberately rejected.
    #[test]
    fn numeric_from_str_classifies_bad_input() {
        assert_eq!(Numeric::from_str(""), Err(NumericParseError::NoDigits));
        assert_eq!(Numeric::from_str("-"), Err(NumericParseError::NoDigits));
        assert_eq!(Numeric::from_str("."), Err(NumericParseError::NoDigits));
        assert_eq!(
            Numeric::from_str("1.2.3"),
            Err(NumericParseError::MultiplePoints),
        );
        // Exponent notation is not a lenient parse.
        assert_eq!(
            Numeric::from_str("1e5"),
            Err(NumericParseError::InvalidChar { pos: 1 }),
        );
        assert_eq!(
            Numeric::from_str("12x3"),
            Err(NumericParseError::InvalidChar { pos: 2 }),
        );
    }

    #[test]
    fn json_and_jsonb_carry_text_verbatim() {
        let doc = String::from(r#"{"a":1,"b":[true,null]}"#);
        let j = Json::new(doc.clone());
        assert_eq!(j.as_str(), r#"{"a":1,"b":[true,null]}"#);
        assert_eq!(j.to_string(), r#"{"a":1,"b":[true,null]}"#);
        assert_eq!(j.clone().into_string(), doc);

        let jb = Jsonb::new(doc.clone());
        assert_eq!(jb.as_str(), r#"{"a":1,"b":[true,null]}"#);
        assert_eq!(jb.to_string(), r#"{"a":1,"b":[true,null]}"#);
        assert_eq!(jb.into_string(), doc);
    }
}
