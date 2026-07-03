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

use alloc::string::String;
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
