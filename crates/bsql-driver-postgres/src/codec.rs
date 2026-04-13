//! Binary encode/decode for PostgreSQL types.
//!
//! All decoding operates on raw byte slices (from the arena or wire buffer).
//! Encoding appends big-endian bytes to a `Vec<u8>`.
//!
//! PostgreSQL binary format is big-endian for all numeric types.

use crate::DriverError;
#[cfg(feature = "chrono")]
use chrono::{Datelike, Timelike};

// --- PG epoch constants ---
//
// PostgreSQL stores timestamps/dates relative to its own epoch: 2000-01-01 00:00:00 UTC.
// These constants allow const arithmetic instead of constructing epoch objects at runtime.

/// Seconds from Unix epoch (1970-01-01) to PG epoch (2000-01-01).
#[cfg(any(feature = "time", feature = "chrono"))]
const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;

/// Microseconds from Unix epoch to PG epoch.
#[cfg(any(feature = "time", feature = "chrono"))]
const PG_EPOCH_UNIX_MICROS: i64 = PG_EPOCH_UNIX_SECS * 1_000_000;

/// Julian day number of 2000-01-01 (the PG epoch).
#[cfg(feature = "time")]
const PG_EPOCH_JULIAN_DAY: i32 = 2_451_545;

// --- Encode trait ---

/// Encode a Rust value into PostgreSQL binary format.
///
/// Implementations append the binary representation to `buf`. The length prefix
/// is handled by the caller (wire protocol layer), not the encoder.
///
/// # Example
///
/// ```
/// use bsql_driver_postgres::Encode;
///
/// let mut buf = Vec::new();
/// 42i32.encode_binary(&mut buf);
/// assert_eq!(buf, &[0, 0, 0, 42]);
/// ```
pub trait Encode {
    /// Append the binary-encoded value to `buf`.
    fn encode_binary(&self, buf: &mut Vec<u8>);

    /// The PostgreSQL OID for this type.
    fn type_oid(&self) -> u32;

    /// Static version of [`type_oid`] — available without an instance.
    ///
    /// Used by `Option<T>` to report the correct OID when the value is `None`.
    /// The `Self: Sized` bound keeps `Encode` dyn-compatible.
    fn pg_type_oid() -> u32
    where
        Self: Sized,
    {
        0
    }

    /// Whether this value represents SQL NULL.
    ///
    /// When true, the wire protocol sends length -1 with no data bytes.
    /// Default is false. Implementations for `Option<T>` override this.
    fn is_null(&self) -> bool {
        false
    }

    /// Encode the binary value directly into `dst` at position 0.
    ///
    /// Returns `true` if the encoded length matches `dst.len()` (i.e., same size
    /// as the template slot). Returns `false` if the size differs, signaling the
    /// caller to fall back to a full rebuild.
    ///
    /// The default implementation uses a small inline buffer and copies. Fixed-size
    /// types (i32, i64, etc.) override this to write directly — eliminating the
    /// scratch buffer double-copy on the bind-template hot path.
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        // Fallback: encode into a temporary buffer, check size, copy.
        // All built-in types override this with direct writes — this
        // path only runs for user-defined Encode implementations.
        let mut tmp = Vec::with_capacity(dst.len());
        self.encode_binary(&mut tmp);
        if tmp.len() == dst.len() {
            dst.copy_from_slice(&tmp);
            true
        } else {
            false
        }
    }
}

// --- Encode implementations ---

/// Delegate to the single-source OID mapping.
#[inline]
fn oid(ty: &str) -> u32 {
    crate::oid_map::default_pg_oid_for_rust_type(ty)
}

impl Encode for bool {
    fn pg_type_oid() -> u32 {
        oid("bool")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.push(if *self { 1 } else { 0 });
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 1 {
            return false;
        }
        dst[0] = if *self { 1 } else { 0 };
        true
    }
}

impl Encode for i16 {
    fn pg_type_oid() -> u32 {
        oid("i16")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 2 {
            return false;
        }
        dst.copy_from_slice(&self.to_be_bytes());
        true
    }
}

impl Encode for i32 {
    fn pg_type_oid() -> u32 {
        oid("i32")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 4 {
            return false;
        }
        dst.copy_from_slice(&self.to_be_bytes());
        true
    }
}

impl Encode for i64 {
    fn pg_type_oid() -> u32 {
        oid("i64")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.to_be_bytes());
        true
    }
}

impl Encode for f32 {
    fn pg_type_oid() -> u32 {
        oid("f32")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 4 {
            return false;
        }
        dst.copy_from_slice(&self.to_be_bytes());
        true
    }
}

impl Encode for f64 {
    fn pg_type_oid() -> u32 {
        oid("f64")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.to_be_bytes());
        true
    }
}

impl Encode for &str {
    fn pg_type_oid() -> u32 {
        oid("&str")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        let bytes = self.as_bytes();
        if bytes.len() != dst.len() {
            return false;
        }
        dst.copy_from_slice(bytes);
        true
    }
}

impl Encode for String {
    fn pg_type_oid() -> u32 {
        oid("String")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        self.as_str().encode_at(dst)
    }
}

impl Encode for &[u8] {
    fn pg_type_oid() -> u32 {
        oid("&[u8]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if self.len() != dst.len() {
            return false;
        }
        dst.copy_from_slice(self);
        true
    }
}

impl Encode for Vec<u8> {
    fn pg_type_oid() -> u32 {
        oid("Vec<u8>")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if self.len() != dst.len() {
            return false;
        }
        dst.copy_from_slice(self);
        true
    }
}

impl Encode for u32 {
    fn pg_type_oid() -> u32 {
        oid("u32")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 4 {
            return false;
        }
        dst.copy_from_slice(&self.to_be_bytes());
        true
    }
}

// --- Option<T> Encode — NULL parameter support ---

impl<T: Encode> Encode for Option<T> {
    fn pg_type_oid() -> u32 {
        T::pg_type_oid()
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        if let Some(val) = self {
            val.encode_binary(buf);
        }
        // If None, encode_binary is a no-op — is_null() returns true,
        // and the wire layer sends length -1 with no data bytes.
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        match self {
            Some(val) => val.type_oid(),
            None => T::pg_type_oid(),
        }
    }

    #[inline]
    fn is_null(&self) -> bool {
        self.is_none()
    }
}

// --- Feature-gated Encode implementations ---

#[cfg(feature = "uuid")]
impl Encode for uuid::Uuid {
    fn pg_type_oid() -> u32 {
        oid("uuid::Uuid")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 16 {
            return false;
        }
        dst.copy_from_slice(self.as_bytes());
        true
    }
}

#[cfg(feature = "time")]
impl Encode for time::OffsetDateTime {
    fn pg_type_oid() -> u32 {
        oid("time::OffsetDateTime")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_micros().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_micros().to_be_bytes());
        true
    }
}

#[cfg(feature = "time")]
trait OffsetDateTimeExt {
    fn encode_pg_micros(&self) -> i64;
}

#[cfg(feature = "time")]
impl OffsetDateTimeExt for time::OffsetDateTime {
    #[inline]
    fn encode_pg_micros(&self) -> i64 {
        // PG stores timestamptz as i64 microseconds since PG epoch (2000-01-01).
        // Use const arithmetic instead of constructing an epoch OffsetDateTime.
        let unix_nanos = self.unix_timestamp_nanos();
        let unix_micros = (unix_nanos / 1000) as i64;
        unix_micros.saturating_sub(PG_EPOCH_UNIX_MICROS)
    }
}

#[cfg(feature = "time")]
impl Encode for time::Date {
    fn pg_type_oid() -> u32 {
        oid("time::Date")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_days().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 4 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_days().to_be_bytes());
        true
    }
}

#[cfg(feature = "time")]
trait DateExt {
    fn encode_pg_days(&self) -> i32;
}

#[cfg(feature = "time")]
impl DateExt for time::Date {
    #[inline]
    fn encode_pg_days(&self) -> i32 {
        // PG stores date as i32 days since 2000-01-01.
        // Use Julian day arithmetic instead of constructing an epoch Date.
        self.to_julian_day() - PG_EPOCH_JULIAN_DAY
    }
}

#[cfg(feature = "time")]
impl Encode for time::Time {
    fn pg_type_oid() -> u32 {
        oid("time::Time")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_micros().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_micros().to_be_bytes());
        true
    }
}

#[cfg(feature = "time")]
trait TimeExt {
    fn encode_pg_micros(&self) -> i64;
}

#[cfg(feature = "time")]
impl TimeExt for time::Time {
    #[inline]
    fn encode_pg_micros(&self) -> i64 {
        // PG stores time as i64 microseconds since midnight
        let midnight = time::Time::MIDNIGHT;
        let diff = *self - midnight;
        diff.whole_microseconds() as i64
    }
}

#[cfg(feature = "time")]
impl Encode for time::PrimitiveDateTime {
    fn pg_type_oid() -> u32 {
        oid("time::PrimitiveDateTime")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_micros().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_micros().to_be_bytes());
        true
    }
}

#[cfg(feature = "time")]
trait PrimitiveDateTimeExt {
    fn encode_pg_micros(&self) -> i64;
}

#[cfg(feature = "time")]
impl PrimitiveDateTimeExt for time::PrimitiveDateTime {
    #[inline]
    fn encode_pg_micros(&self) -> i64 {
        // TIMESTAMP (without tz) has the same binary format as TIMESTAMPTZ:
        // i64 microseconds since PG epoch (2000-01-01 00:00:00).
        // Use const arithmetic instead of constructing an epoch OffsetDateTime.
        let unix_nanos = self.assume_utc().unix_timestamp_nanos();
        let unix_micros = (unix_nanos / 1000) as i64;
        unix_micros.saturating_sub(PG_EPOCH_UNIX_MICROS)
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDateTime {
    fn pg_type_oid() -> u32 {
        oid("chrono::NaiveDateTime")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_micros().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_micros().to_be_bytes());
        true
    }
}

#[cfg(feature = "chrono")]
trait NaiveDateTimeExt {
    fn encode_pg_micros(&self) -> i64;
}

#[cfg(feature = "chrono")]
impl NaiveDateTimeExt for chrono::NaiveDateTime {
    #[inline]
    fn encode_pg_micros(&self) -> i64 {
        // TIMESTAMP has same binary format: i64 microseconds since PG epoch
        let unix_micros = self.and_utc().timestamp_micros();
        unix_micros.saturating_sub(PG_EPOCH_UNIX_MICROS)
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::DateTime<chrono::Utc> {
    fn pg_type_oid() -> u32 {
        oid("chrono::DateTime<chrono::Utc>")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_micros().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_micros().to_be_bytes());
        true
    }
}

#[cfg(feature = "chrono")]
trait ChronoDateTimeUtcExt {
    fn encode_pg_micros(&self) -> i64;
}

#[cfg(feature = "chrono")]
impl ChronoDateTimeUtcExt for chrono::DateTime<chrono::Utc> {
    #[inline]
    fn encode_pg_micros(&self) -> i64 {
        // PG epoch: 2000-01-01 00:00:00 UTC = Unix timestamp 946684800
        let unix_micros = self.timestamp_micros();
        unix_micros.saturating_sub(PG_EPOCH_UNIX_MICROS)
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDate {
    fn pg_type_oid() -> u32 {
        oid("chrono::NaiveDate")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_days().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 4 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_days().to_be_bytes());
        true
    }
}

#[cfg(feature = "chrono")]
trait ChronoNaiveDateExt {
    fn encode_pg_days(&self) -> i32;
}

#[cfg(feature = "chrono")]
impl ChronoNaiveDateExt for chrono::NaiveDate {
    #[inline]
    fn encode_pg_days(&self) -> i32 {
        // Use const days offset instead of constructing an epoch NaiveDate.
        // chrono NaiveDate doesn't have to_julian_day, but num_days_from_ce() works:
        // PG epoch (2000-01-01) num_days_from_ce = 730120
        const PG_EPOCH_CE_DAYS: i32 = 730_120;
        let days_i64 = (self.num_days_from_ce() - PG_EPOCH_CE_DAYS) as i64;
        i32::try_from(days_i64).unwrap_or(if days_i64 < 0 { i32::MIN } else { i32::MAX })
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveTime {
    fn pg_type_oid() -> u32 {
        oid("chrono::NaiveTime")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.encode_pg_micros().to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }

    #[inline]
    fn encode_at(&self, dst: &mut [u8]) -> bool {
        if dst.len() != 8 {
            return false;
        }
        dst.copy_from_slice(&self.encode_pg_micros().to_be_bytes());
        true
    }
}

#[cfg(feature = "chrono")]
trait ChronoNaiveTimeExt {
    fn encode_pg_micros(&self) -> i64;
}

#[cfg(feature = "chrono")]
impl ChronoNaiveTimeExt for chrono::NaiveTime {
    #[inline]
    fn encode_pg_micros(&self) -> i64 {
        // Use num_seconds_from_midnight() to avoid constructing a midnight object.
        let secs = self.num_seconds_from_midnight() as i64;
        let nanos = self.nanosecond() % 1_000_000_000; // strip leap-second flag
        let micros_in_sec = (nanos / 1000) as i64;
        secs * 1_000_000 + micros_in_sec
    }
}

#[cfg(feature = "decimal")]
impl Encode for rust_decimal::Decimal {
    fn pg_type_oid() -> u32 {
        oid("rust_decimal::Decimal")
    }

    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // PG NUMERIC binary format:
        //   i16 ndigits  — number of base-10000 digit groups
        //   i16 weight   — exponent of first digit (units of 10^4)
        //   i16 sign     — 0x0000 = positive, 0x4000 = negative
        //   i16 dscale   — number of digits after decimal point
        //   [i16; ndigits] — base-10000 digit values
        //
        // Special case: zero is encoded as ndigits=0, weight=0, sign=0, dscale=0.

        if self.is_zero() {
            // ndigits=0, weight=0, sign=+, dscale=scale
            let dscale = i16::try_from(self.scale()).unwrap_or(i16::MAX);
            buf.extend_from_slice(&0i16.to_be_bytes()); // ndigits
            buf.extend_from_slice(&0i16.to_be_bytes()); // weight
            buf.extend_from_slice(&0x0000i16.to_be_bytes()); // sign
            buf.extend_from_slice(&dscale.to_be_bytes()); // dscale
            return;
        }

        let sign: i16 = if self.is_sign_negative() {
            0x4000
        } else {
            0x0000
        };
        let scale = self.scale();

        // Get the absolute value as a u128 of unscaled digits
        let abs = self.abs();
        let mut mantissa = abs.mantissa().unsigned_abs();

        // Collect decimal digits of mantissa (max ~39 for u128)
        let mut decimal_digits: smallvec::SmallVec<[i16; 32]> = smallvec::SmallVec::new();
        while mantissa > 0 {
            decimal_digits.push((mantissa % 10) as i16);
            mantissa /= 10;
        }
        decimal_digits.reverse();

        // The mantissa digits represent the unscaled number. The decimal
        // point sits `scale` positions from the right. For 0.001 (mantissa=1,
        // scale=3), decimal_digits=[1] and the value is 1 × 10^-3.
        let total_digits = decimal_digits.len();
        let scale_usize = scale as usize;
        let int_len = total_digits.saturating_sub(scale_usize);
        let sig_frac_len = total_digits - int_len; // significant fractional digits

        // Build padded digit sequence aligned to base-10000 boundaries:
        //   [int_pad zeros][integer digits][implicit frac zeros][significant frac digits][frac_pad zeros]
        let mut padded: smallvec::SmallVec<[i16; 32]> = smallvec::SmallVec::new();

        // Integer part: pad left to multiple of 4
        let int_pad = if int_len > 0 {
            (4 - (int_len % 4)) % 4
        } else {
            0
        };
        padded.extend(std::iter::repeat(0i16).take(int_pad));
        padded.extend_from_slice(&decimal_digits[..int_len]);

        // Fractional part: implicit leading zeros (scale - sig_frac_len)
        // then significant digits, then pad right to multiple of 4.
        // E.g., 0.001: scale=3, sig_frac_len=1 → 2 implicit zeros → [0,0,1]
        let implicit_zeros = scale_usize.saturating_sub(sig_frac_len);
        padded.extend(std::iter::repeat(0i16).take(implicit_zeros));
        padded.extend_from_slice(&decimal_digits[int_len..]);
        let frac_total = implicit_zeros + sig_frac_len; // = scale_usize
        let frac_pad = (4 - (frac_total % 4)) % 4;
        padded.extend(std::iter::repeat(0i16).take(frac_pad));

        // Group into base-10000 digits
        let mut pg_digits: smallvec::SmallVec<[i16; 12]> = smallvec::SmallVec::new();
        for chunk in padded.chunks(4) {
            let d = chunk[0] * 1000 + chunk[1] * 100 + chunk[2] * 10 + chunk[3];
            pg_digits.push(d);
        }

        // Integer group count (for weight and stripping)
        let int_groups = if int_len > 0 {
            (int_len + int_pad) / 4
        } else {
            0
        };

        // Strip leading zero groups from fractional part (adjust weight)
        let mut leading_frac_zeros = 0usize;
        for i in int_groups..pg_digits.len() {
            if pg_digits[i] == 0 {
                leading_frac_zeros += 1;
            } else {
                break;
            }
        }

        // Strip trailing zero groups from fractional part
        while pg_digits.len() > int_groups + leading_frac_zeros
            && pg_digits.last().copied() == Some(0)
        {
            pg_digits.pop();
        }

        // Remove leading fractional zeros (they affect weight, not digit values)
        if leading_frac_zeros > 0 {
            pg_digits.drain(int_groups..int_groups + leading_frac_zeros);
        }

        let ndigits = pg_digits.len() as i16;

        // Weight = exponent of the first base-10000 digit group.
        let weight: i16 = if int_groups > 0 {
            let w = (int_groups - 1) as i32;
            w.clamp(i16::MIN as i32, i16::MAX as i32) as i16
        } else {
            // Pure fractional: negative weight based on leading zero groups skipped
            let w = -(leading_frac_zeros as i32 + 1);
            w.clamp(i16::MIN as i32, i16::MAX as i32) as i16
        };

        let dscale = i16::try_from(scale).unwrap_or(i16::MAX);
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for d in &pg_digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

// --- Decode functions ---

/// Decode a boolean from binary format (1 byte: 0x00 = false, 0x01 = true).
///
/// # Errors
///
/// Returns `DriverError::Protocol` if the data is not exactly 1 byte.
#[inline]
pub fn decode_bool(data: &[u8]) -> Result<bool, DriverError> {
    if data.len() != 1 {
        return Err(DriverError::Protocol(format!(
            "bool: expected 1 byte, got {}",
            data.len()
        )));
    }
    Ok(data[0] != 0)
}

/// Decode a 16-bit integer from binary format (2 bytes, big-endian).
#[inline]
pub fn decode_i16(data: &[u8]) -> Result<i16, DriverError> {
    if data.len() != 2 {
        return Err(DriverError::Protocol(format!(
            "i16: expected 2 bytes, got {}",
            data.len()
        )));
    }
    Ok(i16::from_be_bytes([data[0], data[1]]))
}

/// Decode a 32-bit integer from binary format (4 bytes, big-endian).
#[inline]
pub fn decode_i32(data: &[u8]) -> Result<i32, DriverError> {
    if data.len() != 4 {
        return Err(DriverError::Protocol(format!(
            "i32: expected 4 bytes, got {}",
            data.len()
        )));
    }
    Ok(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Decode a 64-bit integer from binary format (8 bytes, big-endian).
#[inline]
pub fn decode_i64(data: &[u8]) -> Result<i64, DriverError> {
    if data.len() != 8 {
        return Err(DriverError::Protocol(format!(
            "i64: expected 8 bytes, got {}",
            data.len()
        )));
    }
    Ok(i64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

/// Decode a 32-bit float from binary format (4 bytes, big-endian IEEE 754).
#[inline]
pub fn decode_f32(data: &[u8]) -> Result<f32, DriverError> {
    if data.len() != 4 {
        return Err(DriverError::Protocol(format!(
            "f32: expected 4 bytes, got {}",
            data.len()
        )));
    }
    Ok(f32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Decode a 64-bit float from binary format (8 bytes, big-endian IEEE 754).
#[inline]
pub fn decode_f64(data: &[u8]) -> Result<f64, DriverError> {
    if data.len() != 8 {
        return Err(DriverError::Protocol(format!(
            "f64: expected 8 bytes, got {}",
            data.len()
        )));
    }
    Ok(f64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

/// Decode a UTF-8 string from binary format (variable length).
///
/// Returns the string slice directly — zero-copy when data lives in the arena.
/// Uses SIMD-accelerated validation (SSE4.2/AVX2 on x86_64, NEON on aarch64)
/// via `simdutf8`, falling back to scalar on unsupported targets.
#[inline]
pub fn decode_str(data: &[u8]) -> Result<&str, DriverError> {
    simdutf8::basic::from_utf8(data)
        .map_err(|e| DriverError::Protocol(format!("invalid UTF-8 in text column: {e}")))
}

/// Decode raw bytes (bytea) — identity function, zero-copy.
#[inline]
pub fn decode_bytes(data: &[u8]) -> &[u8] {
    data
}

/// Decode a UUID from binary format (exactly 16 bytes).
#[inline]
pub fn decode_uuid(data: &[u8]) -> Result<[u8; 16], DriverError> {
    if data.len() != 16 {
        return Err(DriverError::Protocol(format!(
            "uuid: expected 16 bytes, got {}",
            data.len()
        )));
    }
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(data);
    Ok(uuid)
}

/// Encode a parameter value into the wire buffer with its 4-byte length prefix.
///
/// NULL values get a length of -1 with no data.
pub fn encode_param(buf: &mut Vec<u8>, param: &dyn Encode) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // placeholder for length
    param.encode_binary(buf);
    let data_len = (buf.len() - start - 4) as i32;
    buf[start..start + 4].copy_from_slice(&data_len.to_be_bytes());
}

// --- Array encode helper ---

/// Write the PG binary array header for a 1-dimensional array.
///
/// For empty arrays (n_elements == 0), writes ndim=0 per PG convention.
/// For non-empty arrays, writes a full 1-D header with lower_bound=1.
fn encode_array_header(buf: &mut Vec<u8>, n_elements: usize, elem_oid: u32) {
    if n_elements == 0 {
        buf.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0 (PG empty array convention)
        buf.extend_from_slice(&0i32.to_be_bytes()); // has_null = 0
        buf.extend_from_slice(&(elem_oid as i32).to_be_bytes()); // element OID
        return;
    }
    buf.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
    buf.extend_from_slice(&0i32.to_be_bytes()); // has_null = 0 (we don't support NULL elements in encode)
    buf.extend_from_slice(&(elem_oid as i32).to_be_bytes()); // element OID
    buf.extend_from_slice(&(n_elements as i32).to_be_bytes()); // length
    buf.extend_from_slice(&1i32.to_be_bytes()); // lower_bound = 1
}

// --- Array Encode implementations ---

impl Encode for [bool] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("bool"));
        // Pre-allocate: 4 (len prefix) + 1 (data) = 5 bytes per element
        buf.reserve(self.len() * 5);
        for val in self {
            let tmp = [0u8, 0, 0, 1, if *val { 1 } else { 0 }];
            buf.extend_from_slice(&tmp);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<bool>")
    }
}

impl Encode for &[bool] {
    fn pg_type_oid() -> u32 {
        oid("&[bool]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<bool> {
    fn pg_type_oid() -> u32 {
        oid("Vec<bool>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [i16] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("i16"));
        // Pre-allocate: 4 (len prefix) + 2 (data) = 6 bytes per element
        buf.reserve(self.len() * 6);
        for val in self {
            let mut tmp = [0u8; 6];
            tmp[0..4].copy_from_slice(&2i32.to_be_bytes());
            tmp[4..6].copy_from_slice(&val.to_be_bytes());
            buf.extend_from_slice(&tmp);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<i16>")
    }
}

impl Encode for &[i16] {
    fn pg_type_oid() -> u32 {
        oid("&[i16]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<i16> {
    fn pg_type_oid() -> u32 {
        oid("Vec<i16>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [i32] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("i32"));
        // Pre-allocate: 4 (len prefix) + 4 (data) = 8 bytes per element
        buf.reserve(self.len() * 8);
        for val in self {
            let mut tmp = [0u8; 8];
            tmp[0..4].copy_from_slice(&4i32.to_be_bytes());
            tmp[4..8].copy_from_slice(&val.to_be_bytes());
            buf.extend_from_slice(&tmp);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<i32>")
    }
}

impl Encode for &[i32] {
    fn pg_type_oid() -> u32 {
        oid("&[i32]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<i32> {
    fn pg_type_oid() -> u32 {
        oid("Vec<i32>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [i64] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("i64"));
        // Pre-allocate: 4 (len prefix) + 8 (data) = 12 bytes per element
        buf.reserve(self.len() * 12);
        for val in self {
            let mut tmp = [0u8; 12];
            tmp[0..4].copy_from_slice(&8i32.to_be_bytes());
            tmp[4..12].copy_from_slice(&val.to_be_bytes());
            buf.extend_from_slice(&tmp);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<i64>")
    }
}

impl Encode for &[i64] {
    fn pg_type_oid() -> u32 {
        oid("&[i64]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<i64> {
    fn pg_type_oid() -> u32 {
        oid("Vec<i64>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [f32] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("f32"));
        // Pre-allocate: 4 (len prefix) + 4 (data) = 8 bytes per element
        buf.reserve(self.len() * 8);
        for val in self {
            let mut tmp = [0u8; 8];
            tmp[0..4].copy_from_slice(&4i32.to_be_bytes());
            tmp[4..8].copy_from_slice(&val.to_be_bytes());
            buf.extend_from_slice(&tmp);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<f32>")
    }
}

impl Encode for &[f32] {
    fn pg_type_oid() -> u32 {
        oid("&[f32]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<f32> {
    fn pg_type_oid() -> u32 {
        oid("Vec<f32>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [f64] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("f64"));
        // Pre-allocate: 4 (len prefix) + 8 (data) = 12 bytes per element
        buf.reserve(self.len() * 12);
        for val in self {
            let mut tmp = [0u8; 12];
            tmp[0..4].copy_from_slice(&8i32.to_be_bytes());
            tmp[4..12].copy_from_slice(&val.to_be_bytes());
            buf.extend_from_slice(&tmp);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<f64>")
    }
}

impl Encode for &[f64] {
    fn pg_type_oid() -> u32 {
        oid("&[f64]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<f64> {
    fn pg_type_oid() -> u32 {
        oid("Vec<f64>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [&str] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("String"));
        for val in self {
            let bytes = val.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<String>")
    }
}

impl Encode for &[&str] {
    fn pg_type_oid() -> u32 {
        oid("&[&str]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<String> {
    fn pg_type_oid() -> u32 {
        oid("Vec<String>")
    }
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("String"));
        for val in self {
            let bytes = val.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [String] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("String"));
        for val in self {
            let bytes = val.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<String>")
    }
}

impl Encode for &[String] {
    fn pg_type_oid() -> u32 {
        oid("&[String]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [&[u8]] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("Vec<u8>"));
        for val in self {
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<Vec<u8>>")
    }
}

impl Encode for &[&[u8]] {
    fn pg_type_oid() -> u32 {
        oid("&[&[u8]]")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for [Vec<u8>] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), oid("Vec<u8>"));
        for val in self {
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        oid("Vec<Vec<u8>>")
    }
}

impl Encode for &[Vec<u8>] {
    fn pg_type_oid() -> u32 {
        oid("Vec<Vec<u8>>")
    }

    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

impl Encode for Vec<Vec<u8>> {
    fn pg_type_oid() -> u32 {
        oid("Vec<Vec<u8>>")
    }
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        Self::pg_type_oid()
    }
}

// --- Array decode functions ---

/// Decode a PG binary array, returning the raw element byte slices.
///
/// PG binary array format:
/// - i32: ndim (number of dimensions, we only support 1)
/// - i32: has_null flag (0 = no NULLs, 1 = may have NULLs)
/// - i32: element type OID
/// - For each dimension: i32 length, i32 lower_bound
/// - For each element: i32 data_length (-1 = NULL), then data bytes
fn decode_array_elements(data: &[u8]) -> Result<Vec<&[u8]>, DriverError> {
    if data.len() < 12 {
        return Err(DriverError::Protocol(format!(
            "array: expected >= 12 bytes header, got {}",
            data.len()
        )));
    }
    let ndim = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if ndim == 0 {
        return Ok(Vec::new());
    }
    if ndim != 1 {
        return Err(DriverError::Protocol(format!(
            "array: only 1-dimensional arrays supported, got {ndim}"
        )));
    }
    // _has_null at [4..8], _elem_oid at [8..12]
    if data.len() < 20 {
        return Err(DriverError::Protocol(
            "array: truncated dimension header".into(),
        ));
    }
    let n_elements_raw = i32::from_be_bytes([data[12], data[13], data[14], data[15]]);
    if n_elements_raw < 0 {
        return Err(DriverError::Protocol(
            "array: negative element count".into(),
        ));
    }
    let n_elements = n_elements_raw as usize;
    // Guard against malicious or corrupt messages that claim millions of elements.
    // 10M elements is well beyond any reasonable PostgreSQL array; a larger count
    // almost certainly indicates a corrupt message and would cause OOM on allocation.
    const MAX_ARRAY_ELEMENTS: usize = 10_000_000;
    if n_elements > MAX_ARRAY_ELEMENTS {
        return Err(DriverError::Protocol(format!(
            "array element count {n_elements} exceeds limit of {MAX_ARRAY_ELEMENTS}"
        )));
    }
    // lower_bound at [16..20]
    let mut pos = 20;
    let mut elements = Vec::with_capacity(n_elements);
    for _ in 0..n_elements {
        if pos + 4 > data.len() {
            return Err(DriverError::Protocol("array: truncated element".into()));
        }
        let elem_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        if elem_len < 0 {
            // NULL element -- skip (arrays of non-nullable types shouldn't have this)
            continue;
        }
        let elem_len = elem_len as usize;
        if pos + elem_len > data.len() {
            return Err(DriverError::Protocol(
                "array: truncated element data".into(),
            ));
        }
        elements.push(&data[pos..pos + elem_len]);
        pos += elem_len;
    }
    Ok(elements)
}

/// Decode a PG binary array of i32.
pub fn decode_array_i32(data: &[u8]) -> Result<Vec<i32>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(decode_i32)
        .collect()
}

/// Decode a PG binary array of i16.
pub fn decode_array_i16(data: &[u8]) -> Result<Vec<i16>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(decode_i16)
        .collect()
}

/// Decode a PG binary array of i64.
pub fn decode_array_i64(data: &[u8]) -> Result<Vec<i64>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(decode_i64)
        .collect()
}

/// Decode a PG binary array of f32.
pub fn decode_array_f32(data: &[u8]) -> Result<Vec<f32>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(decode_f32)
        .collect()
}

/// Decode a PG binary array of f64.
pub fn decode_array_f64(data: &[u8]) -> Result<Vec<f64>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(decode_f64)
        .collect()
}

/// Decode a PG binary array of booleans.
pub fn decode_array_bool(data: &[u8]) -> Result<Vec<bool>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(decode_bool)
        .collect()
}

/// Decode a PG binary array of text/varchar strings.
pub fn decode_array_str(data: &[u8]) -> Result<Vec<String>, DriverError> {
    decode_array_elements(data)?
        .into_iter()
        .map(|d| decode_str(d).map(|s| s.to_owned()))
        .collect()
}

/// Decode a PG binary array of bytea values.
pub fn decode_array_bytea(data: &[u8]) -> Result<Vec<Vec<u8>>, DriverError> {
    Ok(decode_array_elements(data)?
        .into_iter()
        .map(|d| d.to_vec())
        .collect())
}

// --- Feature-gated decode functions ---

/// Decode a UUID from 16 raw bytes into `uuid::Uuid`.
#[cfg(feature = "uuid")]
#[inline]
pub fn decode_uuid_type(data: &[u8]) -> Result<uuid::Uuid, DriverError> {
    let bytes = decode_uuid(data)?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

/// Decode PG timestamptz (i64 microseconds since 2000-01-01) to `time::OffsetDateTime`.
#[cfg(feature = "time")]
#[inline]
pub fn decode_timestamptz_time(data: &[u8]) -> Result<time::OffsetDateTime, DriverError> {
    let micros = decode_i64(data)?;
    // PG epoch = Unix 946684800
    let unix_micros = micros + 946_684_800i64 * 1_000_000;
    let secs = unix_micros.div_euclid(1_000_000);
    let nanos = (unix_micros.rem_euclid(1_000_000) * 1000) as i128;
    time::OffsetDateTime::from_unix_timestamp_nanos(secs as i128 * 1_000_000_000 + nanos)
        .map_err(|e| DriverError::Protocol(format!("timestamptz decode: {e}")))
}

/// Decode PG date (i32 days since 2000-01-01) to `time::Date`.
#[cfg(feature = "time")]
#[inline]
pub fn decode_date_time(data: &[u8]) -> Result<time::Date, DriverError> {
    let days = decode_i32(data)?;
    // Use Julian day arithmetic instead of constructing an epoch Date.
    let julian_day = PG_EPOCH_JULIAN_DAY as i64 + days as i64;
    if julian_day < i32::MIN as i64 || julian_day > i32::MAX as i64 {
        return Err(DriverError::Protocol(format!(
            "date out of range: {days} days"
        )));
    }
    time::Date::from_julian_day(julian_day as i32)
        .map_err(|_| DriverError::Protocol(format!("date out of range: {days} days")))
}

/// Decode PG time (i64 microseconds since midnight) to `time::Time`.
#[cfg(feature = "time")]
#[inline]
pub fn decode_time_time(data: &[u8]) -> Result<time::Time, DriverError> {
    let micros = decode_i64(data)?;

    // would cause `as u8` to wrap to wrong values.
    if !(0..86_400_000_000).contains(&micros) {
        return Err(DriverError::Protocol(format!(
            "time out of range: {micros}us (must be 0..86_400_000_000)"
        )));
    }
    let total_secs = micros / 1_000_000;
    let h = (total_secs / 3600) as u8;
    let m = ((total_secs % 3600) / 60) as u8;
    let s = (total_secs % 60) as u8;
    let micro = (micros % 1_000_000) as u32;
    time::Time::from_hms_micro(h, m, s, micro)
        .map_err(|e| DriverError::Protocol(format!("time decode: {e}")))
}

/// Decode PG timestamptz to `chrono::DateTime<chrono::Utc>`.
#[cfg(feature = "chrono")]
#[inline]
pub fn decode_timestamptz_chrono(
    data: &[u8],
) -> Result<chrono::DateTime<chrono::Utc>, DriverError> {
    let micros = decode_i64(data)?;
    let pg_epoch_unix_micros: i64 = 946_684_800 * 1_000_000;
    let unix_micros = micros + pg_epoch_unix_micros;
    let secs = unix_micros.div_euclid(1_000_000);
    let nsecs = (unix_micros.rem_euclid(1_000_000) * 1000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs)
        .ok_or_else(|| DriverError::Protocol(format!("timestamptz out of range: {micros}us")))
}

/// Decode PG date to `chrono::NaiveDate`.
#[cfg(feature = "chrono")]
#[inline]
pub fn decode_date_chrono(data: &[u8]) -> Result<chrono::NaiveDate, DriverError> {
    let days = decode_i32(data)?;
    // Use const CE-day arithmetic instead of constructing an epoch NaiveDate.
    const PG_EPOCH_CE_DAYS: i32 = 730_120;
    let ce_days = PG_EPOCH_CE_DAYS as i64 + days as i64;
    if ce_days < i32::MIN as i64 || ce_days > i32::MAX as i64 {
        return Err(DriverError::Protocol(format!(
            "date out of range: {days} days"
        )));
    }
    chrono::NaiveDate::from_num_days_from_ce_opt(ce_days as i32)
        .ok_or_else(|| DriverError::Protocol(format!("date out of range: {days} days")))
}

/// Decode PG time to `chrono::NaiveTime`.
#[cfg(feature = "chrono")]
#[inline]
pub fn decode_time_chrono(data: &[u8]) -> Result<chrono::NaiveTime, DriverError> {
    let micros = decode_i64(data)?;

    // when cast to u32, producing wrong time values.
    if !(0..86_400_000_000).contains(&micros) {
        return Err(DriverError::Protocol(format!(
            "time out of range: {micros}us (must be 0..86_400_000_000)"
        )));
    }
    let total_secs = (micros / 1_000_000) as u32;
    let micro = (micros % 1_000_000) as u32;
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(total_secs, micro * 1000)
        .ok_or_else(|| DriverError::Protocol(format!("time out of range: {micros}us")))
}

/// Decode PG numeric binary to `rust_decimal::Decimal`.
///
/// PG NUMERIC binary: i16 ndigits, i16 weight, i16 sign, i16 dscale,
/// followed by ndigits base-10000 digit values (i16 each).
///
/// The value is: sum(digit[i] * 10^(4 * (weight - i))) for i in 0..ndigits.
#[cfg(feature = "decimal")]
pub fn decode_numeric_decimal(data: &[u8]) -> Result<rust_decimal::Decimal, DriverError> {
    if data.len() < 8 {
        return Err(DriverError::Protocol(format!(
            "numeric: expected >= 8 bytes header, got {}",
            data.len()
        )));
    }
    let ndigits = i16::from_be_bytes([data[0], data[1]]) as usize;
    let weight = i16::from_be_bytes([data[2], data[3]]) as i32;
    let sign = i16::from_be_bytes([data[4], data[5]]);
    let _dscale = i16::from_be_bytes([data[6], data[7]]) as u32;

    if data.len() != 8 + ndigits * 2 {
        return Err(DriverError::Protocol(format!(
            "numeric: expected {} bytes, got {}",
            8 + ndigits * 2,
            data.len()
        )));
    }

    if ndigits == 0 {
        return Ok(rust_decimal::Decimal::ZERO);
    }

    // Read digit values
    let mut digits: smallvec::SmallVec<[i64; 16]> = smallvec::SmallVec::with_capacity(ndigits);
    for i in 0..ndigits {
        let off = 8 + i * 2;
        digits.push(i16::from_be_bytes([data[off], data[off + 1]]) as i64);
    }

    // Compute the value arithmetically: sum(digit[i] * 10^(4*(weight-i)))
    // Build a u128 mantissa and track the scale (fractional digits).
    let mut mantissa: u128 = 0;
    for &d in &digits {
        mantissa = mantissa
            .checked_mul(10_000)
            .and_then(|m| m.checked_add(d as u128))
            .ok_or_else(|| DriverError::Protocol("numeric value too large for Decimal".into()))?;
    }

    // The value with all digits is: mantissa * 10^(4 * (weight - ndigits + 1))
    // If weight >= ndigits-1, we need to multiply by 10^(4*(weight - ndigits + 1))
    // If weight < ndigits-1, we have fractional digits
    let exponent = 4 * (weight - ndigits as i32 + 1);
    let result = if exponent >= 0 {
        // All integer: multiply mantissa by 10^exponent
        let factor = 10u128
            .checked_pow(exponent as u32)
            .ok_or_else(|| DriverError::Protocol("numeric exponent too large".into()))?;
        let m = mantissa
            .checked_mul(factor)
            .ok_or_else(|| DriverError::Protocol("numeric value too large for Decimal".into()))?;
        if m > u128::from(u64::MAX) {
            // Decimal max mantissa is 96 bits, fall back to string for huge values
            let s = m.to_string();
            s.parse::<rust_decimal::Decimal>()
                .map_err(|e| DriverError::Protocol(format!("numeric parse error: {e}")))?
        } else {
            rust_decimal::Decimal::from_i128_with_scale(m as i128, 0)
        }
    } else {
        // Has fractional part: scale = -exponent
        let scale = (-exponent) as u32;
        // rust_decimal stores mantissa as 96-bit integer with scale
        if mantissa <= u128::from(u64::MAX) {
            rust_decimal::Decimal::from_i128_with_scale(mantissa as i128, scale)
        } else {
            // Large mantissa — use string fallback
            let mut s = mantissa.to_string();
            if scale as usize >= s.len() {
                let zeros = scale as usize - s.len() + 1;
                s = format!("0.{}{s}", "0".repeat(zeros));
            } else {
                let dot_pos = s.len() - scale as usize;
                s.insert(dot_pos, '.');
            }
            s.parse::<rust_decimal::Decimal>()
                .map_err(|e| DriverError::Protocol(format!("numeric parse error: {e}")))?
        }
    };

    if sign == 0x4000 {
        Ok(-result)
    } else {
        Ok(result)
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    // --- Encode round-trips ---

    #[test]
    fn bool_roundtrip() {
        let mut buf = Vec::new();
        true.encode_binary(&mut buf);
        assert!(decode_bool(&buf).unwrap());

        buf.clear();
        false.encode_binary(&mut buf);
        assert!(!decode_bool(&buf).unwrap());
    }

    #[test]
    fn i16_roundtrip() {
        let mut buf = Vec::new();
        12345i16.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), 12345);

        buf.clear();
        (-1i16).encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), -1);

        buf.clear();
        i16::MIN.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MIN);

        buf.clear();
        i16::MAX.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MAX);
    }

    #[test]
    fn i32_roundtrip() {
        let mut buf = Vec::new();
        42i32.encode_binary(&mut buf);
        assert_eq!(buf, &[0, 0, 0, 42]);
        assert_eq!(decode_i32(&buf).unwrap(), 42);

        buf.clear();
        i32::MAX.encode_binary(&mut buf);
        assert_eq!(decode_i32(&buf).unwrap(), i32::MAX);

        buf.clear();
        i32::MIN.encode_binary(&mut buf);
        assert_eq!(decode_i32(&buf).unwrap(), i32::MIN);
    }

    #[test]
    fn i64_roundtrip() {
        let mut buf = Vec::new();
        1234567890123i64.encode_binary(&mut buf);
        assert_eq!(decode_i64(&buf).unwrap(), 1234567890123);
    }

    #[test]
    fn f32_roundtrip() {
        let mut buf = Vec::new();
        3.14f32.encode_binary(&mut buf);
        let decoded = decode_f32(&buf).unwrap();
        assert!((decoded - 3.14).abs() < f32::EPSILON);
    }

    #[test]
    fn f64_roundtrip() {
        let mut buf = Vec::new();
        std::f64::consts::PI.encode_binary(&mut buf);
        let decoded = decode_f64(&buf).unwrap();
        assert!((decoded - std::f64::consts::PI).abs() < f64::EPSILON);
    }

    #[test]
    fn str_roundtrip() {
        let mut buf = Vec::new();
        "hello world".encode_binary(&mut buf);
        assert_eq!(decode_str(&buf).unwrap(), "hello world");
    }

    #[test]
    fn string_roundtrip() {
        let mut buf = Vec::new();
        let s = String::from("test string");
        s.encode_binary(&mut buf);
        assert_eq!(decode_str(&buf).unwrap(), "test string");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut buf = Vec::new();
        let data: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        data.encode_binary(&mut buf);
        assert_eq!(decode_bytes(&buf), data);
    }

    #[test]
    fn vec_u8_roundtrip() {
        let mut buf = Vec::new();
        let data = vec![1u8, 2, 3, 4, 5];
        data.encode_binary(&mut buf);
        assert_eq!(decode_bytes(&buf), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn u32_encode() {
        let mut buf = Vec::new();
        42u32.encode_binary(&mut buf);
        assert_eq!(buf, &[0, 0, 0, 42]);
    }

    #[test]
    fn uuid_roundtrip() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let decoded = decode_uuid(&uuid_bytes).unwrap();
        assert_eq!(decoded, uuid_bytes);
    }

    // --- Error cases ---

    #[test]
    fn decode_bool_wrong_length() {
        assert!(decode_bool(&[]).is_err());
        assert!(decode_bool(&[0, 0]).is_err());
    }

    #[test]
    fn decode_i32_wrong_length() {
        assert!(decode_i32(&[0, 0, 0]).is_err());
        assert!(decode_i32(&[0, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn decode_i64_wrong_length() {
        assert!(decode_i64(&[0; 7]).is_err());
        assert!(decode_i64(&[0; 9]).is_err());
    }

    #[test]
    fn decode_f32_wrong_length() {
        assert!(decode_f32(&[0; 3]).is_err());
    }

    #[test]
    fn decode_f64_wrong_length() {
        assert!(decode_f64(&[0; 7]).is_err());
    }

    #[test]
    fn decode_str_invalid_utf8() {
        assert!(decode_str(&[0xFF, 0xFE]).is_err());
    }

    #[test]
    fn decode_uuid_wrong_length() {
        assert!(decode_uuid(&[0; 15]).is_err());
        assert!(decode_uuid(&[0; 17]).is_err());
    }

    #[test]
    fn empty_str_decode() {
        assert_eq!(decode_str(&[]).unwrap(), "");
    }

    #[test]
    fn empty_bytes_decode() {
        assert_eq!(decode_bytes(&[]).len(), 0);
    }

    // --- Type OIDs ---

    #[test]
    fn type_oids_correct() {
        assert_eq!(true.type_oid(), 16);
        assert_eq!(0i16.type_oid(), 21);
        assert_eq!(0i32.type_oid(), 23);
        assert_eq!(0i64.type_oid(), 20);
        assert_eq!(0f32.type_oid(), 700);
        assert_eq!(0f64.type_oid(), 701);
        assert_eq!("".type_oid(), 25);
        assert_eq!(String::new().type_oid(), 25);
        let b: &[u8] = &[];
        assert_eq!(b.type_oid(), 17);
        assert_eq!(Vec::<u8>::new().type_oid(), 17);
        assert_eq!(0u32.type_oid(), 26);
    }

    // --- Encode param with length prefix ---

    #[test]
    fn encode_param_i32() {
        let mut buf = Vec::new();
        encode_param(&mut buf, &42i32);
        // 4 bytes length (=4) + 4 bytes data
        assert_eq!(buf.len(), 8);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 4);
        let val = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(val, 42);
    }

    #[test]
    fn encode_param_str() {
        let mut buf = Vec::new();
        encode_param(&mut buf, &"hello");
        // 4 bytes length (=5) + 5 bytes data
        assert_eq!(buf.len(), 9);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 5);
        assert_eq!(&buf[4..], b"hello");
    }

    #[test]
    fn option_none_is_null() {
        let val: Option<i32> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 23); // reports i32 OID even for None
    }

    #[test]
    fn option_some_encodes() {
        let val: Option<i32> = Some(42);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 23);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(buf, &[0, 0, 0, 42]);
    }

    #[test]
    fn option_none_encode_is_noop() {
        let val: Option<i32> = None;
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty(), "None encode should produce no bytes");
    }

    // --- Audit gap tests ---

    // #1: decode_i16 wrong length
    #[test]
    fn decode_i16_wrong_length() {
        assert!(decode_i16(&[]).is_err());
        assert!(decode_i16(&[0]).is_err());
        assert!(decode_i16(&[0, 0, 0]).is_err());
    }

    // #2: f32 NaN roundtrip
    #[test]
    fn f32_nan_roundtrip() {
        let mut buf = Vec::new();
        f32::NAN.encode_binary(&mut buf);
        let decoded = decode_f32(&buf).unwrap();
        assert!(decoded.is_nan(), "NaN should survive roundtrip");
    }

    // #2: f64 NaN roundtrip
    #[test]
    fn f64_nan_roundtrip() {
        let mut buf = Vec::new();
        f64::NAN.encode_binary(&mut buf);
        let decoded = decode_f64(&buf).unwrap();
        assert!(decoded.is_nan(), "NaN should survive roundtrip");
    }

    // #3: f32 +Infinity/-Infinity roundtrip
    #[test]
    fn f32_infinity_roundtrip() {
        let mut buf = Vec::new();
        f32::INFINITY.encode_binary(&mut buf);
        assert_eq!(decode_f32(&buf).unwrap(), f32::INFINITY);

        buf.clear();
        f32::NEG_INFINITY.encode_binary(&mut buf);
        assert_eq!(decode_f32(&buf).unwrap(), f32::NEG_INFINITY);
    }

    // #3: f64 +Infinity/-Infinity roundtrip
    #[test]
    fn f64_infinity_roundtrip() {
        let mut buf = Vec::new();
        f64::INFINITY.encode_binary(&mut buf);
        assert_eq!(decode_f64(&buf).unwrap(), f64::INFINITY);

        buf.clear();
        f64::NEG_INFINITY.encode_binary(&mut buf);
        assert_eq!(decode_f64(&buf).unwrap(), f64::NEG_INFINITY);
    }

    // #4: f32 +0.0 vs -0.0 bit-pattern preservation
    #[test]
    fn f32_signed_zero_roundtrip() {
        let mut buf = Vec::new();
        0.0f32.encode_binary(&mut buf);
        let decoded = decode_f32(&buf).unwrap();
        assert_eq!(decoded.to_bits(), 0.0f32.to_bits(), "+0.0 bits must match");

        buf.clear();
        (-0.0f32).encode_binary(&mut buf);
        let decoded = decode_f32(&buf).unwrap();
        assert_eq!(
            decoded.to_bits(),
            (-0.0f32).to_bits(),
            "-0.0 bits must match"
        );
    }

    // #4: f64 +0.0 vs -0.0 bit-pattern preservation
    #[test]
    fn f64_signed_zero_roundtrip() {
        let mut buf = Vec::new();
        0.0f64.encode_binary(&mut buf);
        let decoded = decode_f64(&buf).unwrap();
        assert_eq!(decoded.to_bits(), 0.0f64.to_bits(), "+0.0 bits must match");

        buf.clear();
        (-0.0f64).encode_binary(&mut buf);
        let decoded = decode_f64(&buf).unwrap();
        assert_eq!(
            decoded.to_bits(),
            (-0.0f64).to_bits(),
            "-0.0 bits must match"
        );
    }

    // #5: i64 boundary values
    #[test]
    fn i64_boundary_roundtrip() {
        let mut buf = Vec::new();
        i64::MIN.encode_binary(&mut buf);
        assert_eq!(decode_i64(&buf).unwrap(), i64::MIN);

        buf.clear();
        i64::MAX.encode_binary(&mut buf);
        assert_eq!(decode_i64(&buf).unwrap(), i64::MAX);
    }

    // #6: i16 boundary values (already partially tested, ensuring completeness)
    #[test]
    fn i16_boundary_standalone() {
        let mut buf = Vec::new();
        i16::MIN.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MIN);

        buf.clear();
        i16::MAX.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MAX);
    }

    // #7: decode_date_chrono negative days (dates before 2000-01-01)
    #[cfg(feature = "chrono")]
    #[test]
    fn decode_date_chrono_negative_days() {
        // -365 days from PG epoch = 1999-01-01
        let data = (-365i32).to_be_bytes();
        let date = decode_date_chrono(&data).unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(1999, 1, 1).unwrap());
    }

    // #8: decode_date_chrono day=0 (exactly 2000-01-01)
    #[cfg(feature = "chrono")]
    #[test]
    fn decode_date_chrono_day_zero() {
        let data = 0i32.to_be_bytes();
        let date = decode_date_chrono(&data).unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
    }

    // #9: decode_date_time negative days
    #[cfg(feature = "time")]
    #[test]
    fn decode_date_time_negative_days() {
        let data = (-1i32).to_be_bytes();
        let date = decode_date_time(&data).unwrap();
        let expected = time::Date::from_calendar_date(1999, time::Month::December, 31).unwrap();
        assert_eq!(date, expected);
    }

    // #10: decode_time_time midnight (0 microseconds)
    #[cfg(feature = "time")]
    #[test]
    fn decode_time_time_midnight() {
        let data = 0i64.to_be_bytes();
        let t = decode_time_time(&data).unwrap();
        assert_eq!(t, time::Time::MIDNIGHT);
    }

    // #11: decode_time_time max (23:59:59.999999)
    #[cfg(feature = "time")]
    #[test]
    fn decode_time_time_max_value() {
        let micros: i64 = 86_400_000_000 - 1; // 23:59:59.999999
        let data = micros.to_be_bytes();
        let t = decode_time_time(&data).unwrap();
        assert_eq!(t.hour(), 23);
        assert_eq!(t.minute(), 59);
        assert_eq!(t.second(), 59);
        assert_eq!(t.microsecond(), 999999);
    }

    // #12: decode_time_time negative microseconds
    #[cfg(feature = "time")]
    #[test]
    fn decode_time_time_negative_micros_error() {
        let data = (-1i64).to_be_bytes();
        let result = decode_time_time(&data);
        assert!(result.is_err(), "negative microseconds should error");
    }

    // #13: decode_time_time >= 86400000000
    #[cfg(feature = "time")]
    #[test]
    fn decode_time_time_overflow_error() {
        let data = 86_400_000_000i64.to_be_bytes();
        let result = decode_time_time(&data);
        assert!(result.is_err(), ">= 24h microseconds should error");
    }

    // #14: decode_timestamptz_time PG epoch
    #[cfg(feature = "time")]
    #[test]
    fn decode_timestamptz_time_pg_epoch() {
        let data = 0i64.to_be_bytes();
        let dt = decode_timestamptz_time(&data).unwrap();
        // PG epoch is 2000-01-01 00:00:00 UTC
        assert_eq!(dt.year(), 2000);
        assert_eq!(dt.month(), time::Month::January);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
        assert_eq!(dt.second(), 0);
    }

    // #15: decode_numeric_decimal zero
    #[cfg(feature = "decimal")]
    #[test]
    fn decode_numeric_decimal_zero() {
        // PG numeric zero: ndigits=0, weight=0, sign=0, dscale=0
        let mut data = Vec::new();
        data.extend_from_slice(&0i16.to_be_bytes()); // ndigits
        data.extend_from_slice(&0i16.to_be_bytes()); // weight
        data.extend_from_slice(&0i16.to_be_bytes()); // sign
        data.extend_from_slice(&0i16.to_be_bytes()); // dscale
        let dec = decode_numeric_decimal(&data).unwrap();
        assert!(dec.is_zero());
    }

    // #16: decode_numeric_decimal negative
    #[cfg(feature = "decimal")]
    #[test]
    fn decode_numeric_decimal_negative() {
        // -42: ndigits=1, weight=0, sign=0x4000, dscale=0, digit=42
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes()); // ndigits=1
        data.extend_from_slice(&0i16.to_be_bytes()); // weight=0
        data.extend_from_slice(&0x4000i16.to_be_bytes()); // sign=negative
        data.extend_from_slice(&0i16.to_be_bytes()); // dscale=0
        data.extend_from_slice(&42i16.to_be_bytes()); // digit=42
        let dec = decode_numeric_decimal(&data).unwrap();
        assert_eq!(dec, rust_decimal::Decimal::new(-42, 0));
    }

    // #17: decode_numeric_decimal pure fractional (0.001)
    #[cfg(feature = "decimal")]
    #[test]
    fn decode_numeric_decimal_pure_fractional() {
        // 0.001: ndigits=1, weight=-1, sign=0, dscale=3, digit=1000
        // The digit 1000 at weight=-1 means 1000 * 10^(-4) = 0.1
        // Actually: 0.001 = 1 * 10^(-3). In PG format: weight=-1, digit=10
        // PG base-10000: weight=-1, digit=10 -> 10 * 10^(-4) = 0.001
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes()); // ndigits=1
        data.extend_from_slice(&(-1i16).to_be_bytes()); // weight=-1
        data.extend_from_slice(&0i16.to_be_bytes()); // sign=positive
        data.extend_from_slice(&3i16.to_be_bytes()); // dscale=3
        data.extend_from_slice(&10i16.to_be_bytes()); // digit=10 -> 10/10000 = 0.001
        let dec = decode_numeric_decimal(&data).unwrap();
        // rust_decimal preserves trailing zeros from dscale, normalize to compare value
        let dec_normalized = dec.normalize();
        assert_eq!(dec_normalized.to_string(), "0.001");
    }

    // --- Decimal encode round-trip tests ---
    // These verify that encode → decode produces the original value.
    // Critical for catching regressions in the PG NUMERIC encoder.

    #[cfg(feature = "decimal")]
    fn decimal_encode_roundtrip(s: &str) {
        use rust_decimal::Decimal;
        use std::str::FromStr;
        let original = Decimal::from_str(s).unwrap();
        let mut buf = Vec::new();
        original.encode_binary(&mut buf);
        let decoded = decode_numeric_decimal(&buf).unwrap();
        assert_eq!(
            decoded.normalize().to_string(),
            original.normalize().to_string(),
            "round-trip failed for {s}: encoded {} bytes",
            buf.len()
        );
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_zero() {
        decimal_encode_roundtrip("0");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_one() {
        decimal_encode_roundtrip("1");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_negative() {
        decimal_encode_roundtrip("-42.5");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_large_integer() {
        decimal_encode_roundtrip("123456789");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_pure_fractional_0001() {
        decimal_encode_roundtrip("0.001");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_pure_fractional_00001() {
        decimal_encode_roundtrip("0.0001");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_pure_fractional_000001() {
        decimal_encode_roundtrip("0.00001");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_mixed() {
        decimal_encode_roundtrip("12345.6789");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_trailing_zeros() {
        decimal_encode_roundtrip("100.00");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_small_negative_fraction() {
        decimal_encode_roundtrip("-0.007");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_high_scale() {
        // rust_decimal max scale is 28
        decimal_encode_roundtrip("0.0000000000000000000000000001");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_roundtrip_large_with_fraction() {
        decimal_encode_roundtrip("999999999999999999.999999999999");
    }

    // #18: decode_array_elements empty (ndim=0)
    #[test]
    fn decode_array_empty() {
        // ndim=0 means empty array
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes()); // ndim=0
        data.extend_from_slice(&0i32.to_be_bytes()); // has_null
        data.extend_from_slice(&23i32.to_be_bytes()); // element OID (int4)
        let elems = decode_array_i32(&data).unwrap();
        assert!(elems.is_empty());
    }

    // #19: decode_array_elements multi-dimensional error
    #[test]
    fn decode_array_multidim_error() {
        let mut data = Vec::new();
        data.extend_from_slice(&2i32.to_be_bytes()); // ndim=2
        data.extend_from_slice(&0i32.to_be_bytes()); // has_null
        data.extend_from_slice(&23i32.to_be_bytes()); // element OID
                                                      // Add enough fake dimension data
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes());
        let result = decode_array_i32(&data);
        assert!(result.is_err(), "multi-dimensional should error");
    }

    // #20: decode_array_elements truncated data
    #[test]
    fn decode_array_truncated_error() {
        // Header says 1 element but data is cut short
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // ndim=1
        data.extend_from_slice(&0i32.to_be_bytes()); // has_null
        data.extend_from_slice(&23i32.to_be_bytes()); // elem OID
        data.extend_from_slice(&1i32.to_be_bytes()); // n_elements=1
        data.extend_from_slice(&1i32.to_be_bytes()); // lower_bound
                                                     // Missing element data
        let result = decode_array_i32(&data);
        assert!(result.is_err(), "truncated array should error");
    }

    // #21: Option<T> encode: Some(42i32) -> non-null data
    #[test]
    fn option_some_i32_produces_data() {
        let val: Option<i32> = Some(42);
        assert!(!val.is_null());
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(decode_i32(&buf).unwrap(), 42);
    }

    // #22: Option<T> encode: None::<i32> -> is_null()
    #[test]
    fn option_none_i32_is_null() {
        let val: Option<i32> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 23);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    // #23: Empty string encode/decode
    #[test]
    fn empty_string_encode_decode() {
        let mut buf = Vec::new();
        "".encode_binary(&mut buf);
        assert!(buf.is_empty());
        assert_eq!(decode_str(&buf).unwrap(), "");

        buf.clear();
        String::new().encode_binary(&mut buf);
        assert!(buf.is_empty());
        assert_eq!(decode_str(&buf).unwrap(), "");
    }

    // #24: Empty bytes encode/decode
    #[test]
    fn empty_bytes_encode_decode() {
        let mut buf = Vec::new();
        let empty: &[u8] = &[];
        empty.encode_binary(&mut buf);
        assert!(buf.is_empty());
        assert_eq!(decode_bytes(&buf).len(), 0);

        buf.clear();
        Vec::<u8>::new().encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    // #25: Large string (1MB) encode/decode
    #[test]
    fn large_string_encode_decode() {
        let big = "x".repeat(1_000_000);
        let mut buf = Vec::new();
        big.as_str().encode_binary(&mut buf);
        assert_eq!(buf.len(), 1_000_000);
        assert_eq!(decode_str(&buf).unwrap(), big);
    }

    // #26: UUID nil (all zeros)
    #[test]
    fn uuid_nil() {
        let nil = [0u8; 16];
        let decoded = decode_uuid(&nil).unwrap();
        assert_eq!(decoded, [0u8; 16]);
    }

    // #27: UUID max (all 0xFF)
    #[test]
    fn uuid_max() {
        let max = [0xFF; 16];
        let decoded = decode_uuid(&max).unwrap();
        assert_eq!(decoded, [0xFF; 16]);
    }

    // #26/#27 with uuid feature: uuid::Uuid nil and max
    #[cfg(feature = "uuid")]
    #[test]
    fn uuid_type_nil_and_max() {
        let nil = [0u8; 16];
        let uuid = decode_uuid_type(&nil).unwrap();
        assert_eq!(uuid, uuid::Uuid::nil());

        let max = [0xFF; 16];
        let uuid = decode_uuid_type(&max).unwrap();
        assert_eq!(uuid, uuid::Uuid::max());
    }

    // --- Array encode tests ---

    // Array encode: bool
    #[test]
    fn encode_array_bool_empty() {
        let arr: &[bool] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_bool(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_bool_single() {
        let arr: &[bool] = &[true];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_bool(&buf).unwrap();
        assert_eq!(decoded, vec![true]);
    }

    #[test]
    fn encode_array_bool_multi() {
        let arr: &[bool] = &[true, false, true, false];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_bool(&buf).unwrap();
        assert_eq!(decoded, vec![true, false, true, false]);
    }

    #[test]
    fn encode_array_bool_vec_delegate() {
        let v = vec![false, true];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_bool(&buf).unwrap();
        assert_eq!(decoded, vec![false, true]);
        assert_eq!(v.type_oid(), 1000);
    }

    // Array encode: i16
    #[test]
    fn encode_array_i16_empty() {
        let arr: &[i16] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i16(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_i16_single() {
        let arr: &[i16] = &[42];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i16(&buf).unwrap();
        assert_eq!(decoded, vec![42i16]);
    }

    #[test]
    fn encode_array_i16_multi_boundary() {
        let arr: &[i16] = &[i16::MIN, -1, 0, 1, i16::MAX];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i16(&buf).unwrap();
        assert_eq!(decoded, vec![i16::MIN, -1, 0, 1, i16::MAX]);
    }

    #[test]
    fn encode_array_i16_vec_delegate() {
        let v = vec![100i16, 200];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_i16(&buf).unwrap();
        assert_eq!(decoded, vec![100i16, 200]);
        assert_eq!(v.type_oid(), 1005);
    }

    // Array encode: i32
    #[test]
    fn encode_array_i32_empty() {
        let arr: &[i32] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i32(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_i32_single() {
        let arr: &[i32] = &[42];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i32(&buf).unwrap();
        assert_eq!(decoded, vec![42]);
    }

    #[test]
    fn encode_array_i32_multi_boundary() {
        let arr: &[i32] = &[i32::MIN, -1, 0, 1, i32::MAX];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i32(&buf).unwrap();
        assert_eq!(decoded, vec![i32::MIN, -1, 0, 1, i32::MAX]);
    }

    #[test]
    fn encode_array_i32_vec_delegate() {
        let v = vec![10, 20, 30];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_i32(&buf).unwrap();
        assert_eq!(decoded, vec![10, 20, 30]);
        assert_eq!(v.type_oid(), 1007);
    }

    // Array encode: i64
    #[test]
    fn encode_array_i64_empty() {
        let arr: &[i64] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i64(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_i64_single() {
        let arr: &[i64] = &[9999999999i64];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i64(&buf).unwrap();
        assert_eq!(decoded, vec![9999999999i64]);
    }

    #[test]
    fn encode_array_i64_multi_boundary() {
        let arr: &[i64] = &[i64::MIN, -1, 0, 1, i64::MAX];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_i64(&buf).unwrap();
        assert_eq!(decoded, vec![i64::MIN, -1, 0, 1, i64::MAX]);
    }

    #[test]
    fn encode_array_i64_vec_delegate() {
        let v = vec![1i64, 2, 3];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_i64(&buf).unwrap();
        assert_eq!(decoded, vec![1i64, 2, 3]);
        assert_eq!(v.type_oid(), 1016);
    }

    // Array encode: f32
    #[test]
    fn encode_array_f32_empty() {
        let arr: &[f32] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_f32(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_f32_single() {
        let arr: &[f32] = &[3.14];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_f32(&buf).unwrap();
        assert!((decoded[0] - 3.14).abs() < f32::EPSILON);
    }

    #[test]
    fn encode_array_f32_multi_boundary() {
        let arr: &[f32] = &[
            f32::MIN,
            -0.0,
            0.0,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_f32(&buf).unwrap();
        assert_eq!(decoded[0], f32::MIN);
        assert_eq!(decoded[1].to_bits(), (-0.0f32).to_bits());
        assert_eq!(decoded[2].to_bits(), 0.0f32.to_bits());
        assert_eq!(decoded[3], f32::MAX);
        assert_eq!(decoded[4], f32::INFINITY);
        assert_eq!(decoded[5], f32::NEG_INFINITY);
    }

    #[test]
    fn encode_array_f32_vec_delegate() {
        let v = vec![1.0f32, 2.0];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_f32(&buf).unwrap();
        assert_eq!(decoded, vec![1.0f32, 2.0]);
        assert_eq!(v.type_oid(), 1021);
    }

    // Array encode: f64
    #[test]
    fn encode_array_f64_empty() {
        let arr: &[f64] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_f64(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_f64_single() {
        let arr: &[f64] = &[std::f64::consts::PI];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_f64(&buf).unwrap();
        assert!((decoded[0] - std::f64::consts::PI).abs() < f64::EPSILON);
    }

    #[test]
    fn encode_array_f64_multi_boundary() {
        let arr: &[f64] = &[
            f64::MIN,
            -0.0,
            0.0,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_f64(&buf).unwrap();
        assert_eq!(decoded[0], f64::MIN);
        assert_eq!(decoded[1].to_bits(), (-0.0f64).to_bits());
        assert_eq!(decoded[2].to_bits(), 0.0f64.to_bits());
        assert_eq!(decoded[3], f64::MAX);
        assert_eq!(decoded[4], f64::INFINITY);
        assert_eq!(decoded[5], f64::NEG_INFINITY);
    }

    #[test]
    fn encode_array_f64_vec_delegate() {
        let v = vec![1.0f64, 2.0];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_f64(&buf).unwrap();
        assert_eq!(decoded, vec![1.0f64, 2.0]);
        assert_eq!(v.type_oid(), 1022);
    }

    // Array encode: text (&[&str] and Vec<String>)
    #[test]
    fn encode_array_str_empty() {
        let arr: &[&str] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_str_single() {
        let arr: &[&str] = &["hello"];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert_eq!(decoded, vec!["hello".to_string()]);
    }

    #[test]
    fn encode_array_str_multi() {
        let arr: &[&str] = &["hello", "", "world"];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert_eq!(
            decoded,
            vec!["hello".to_string(), "".to_string(), "world".to_string()]
        );
    }

    #[test]
    fn encode_array_str_boundary_unicode() {
        let arr: &[&str] = &["\u{1F600}", "\u{00E9}"];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert_eq!(
            decoded,
            vec!["\u{1F600}".to_string(), "\u{00E9}".to_string()]
        );
    }

    #[test]
    fn encode_array_vec_string() {
        let v = vec!["foo".to_string(), "bar".to_string()];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert_eq!(decoded, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(v.type_oid(), 1009);
    }

    #[test]
    fn encode_array_slice_string() {
        let v = vec!["foo".to_string(), "bar".to_string()];
        let slice: &[String] = &v;
        let mut buf = Vec::new();
        slice.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert_eq!(decoded, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(slice.type_oid(), 1009);
    }

    #[test]
    fn encode_array_slice_string_empty() {
        let v: Vec<String> = vec![];
        let slice: &[String] = &v;
        let mut buf = Vec::new();
        slice.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_ref_slice_string() {
        let v = ["alpha".to_string(), "beta".to_string()];
        let r: &&[String] = &&v[..];
        let mut buf = Vec::new();
        r.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert_eq!(decoded, vec!["alpha".to_string(), "beta".to_string()]);
        assert_eq!(r.type_oid(), 1009);
    }

    #[test]
    fn encode_array_vec_string_empty() {
        let v: Vec<String> = vec![];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_str(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    // Array encode: bytea (&[&[u8]] and Vec<Vec<u8>>)
    #[test]
    fn encode_array_bytea_empty() {
        let arr: &[&[u8]] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_bytea(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_array_bytea_single() {
        let data: &[u8] = &[0xDE, 0xAD];
        let arr: &[&[u8]] = &[data];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_bytea(&buf).unwrap();
        assert_eq!(decoded, vec![vec![0xDE, 0xAD]]);
    }

    #[test]
    fn encode_array_bytea_multi() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[];
        let c: &[u8] = &[0xFF];
        let arr: &[&[u8]] = &[a, b, c];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        let decoded = decode_array_bytea(&buf).unwrap();
        assert_eq!(decoded, vec![vec![1, 2, 3], vec![], vec![0xFF]]);
    }

    #[test]
    fn encode_array_vec_vec_u8() {
        let v = vec![vec![10u8, 20], vec![30]];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_bytea(&buf).unwrap();
        assert_eq!(decoded, vec![vec![10u8, 20], vec![30]]);
        assert_eq!(v.type_oid(), 1001);
    }

    #[test]
    fn encode_array_vec_vec_u8_empty() {
        let v: Vec<Vec<u8>> = vec![];
        let mut buf = Vec::new();
        v.encode_binary(&mut buf);
        let decoded = decode_array_bytea(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    // Array type OIDs
    #[test]
    fn array_type_oids_correct() {
        let b: &[bool] = &[];
        assert_eq!(b.type_oid(), 1000);
        let i2: &[i16] = &[];
        assert_eq!(i2.type_oid(), 1005);
        let i4: &[i32] = &[];
        assert_eq!(i4.type_oid(), 1007);
        let i8: &[i64] = &[];
        assert_eq!(i8.type_oid(), 1016);
        let f4: &[f32] = &[];
        assert_eq!(f4.type_oid(), 1021);
        let f8: &[f64] = &[];
        assert_eq!(f8.type_oid(), 1022);
        let t: &[&str] = &[];
        assert_eq!(t.type_oid(), 1009);
        let by: &[&[u8]] = &[];
        assert_eq!(by.type_oid(), 1001);

        assert_eq!(Vec::<bool>::new().type_oid(), 1000);
        assert_eq!(Vec::<i16>::new().type_oid(), 1005);
        assert_eq!(Vec::<i32>::new().type_oid(), 1007);
        assert_eq!(Vec::<i64>::new().type_oid(), 1016);
        assert_eq!(Vec::<f32>::new().type_oid(), 1021);
        assert_eq!(Vec::<f64>::new().type_oid(), 1022);
        assert_eq!(Vec::<String>::new().type_oid(), 1009);
        assert_eq!(Vec::<Vec<u8>>::new().type_oid(), 1001);
    }

    // Empty array header format: ndim=0
    #[test]
    fn encode_array_empty_ndim_zero() {
        let arr: &[i32] = &[];
        let mut buf = Vec::new();
        arr.encode_binary(&mut buf);
        // Empty array: ndim=0 (4 bytes), has_null=0 (4 bytes), elem_oid (4 bytes) = 12 bytes
        assert_eq!(buf.len(), 12);
        let ndim = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(ndim, 0, "empty array must have ndim=0");
        let elem_oid = i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        assert_eq!(
            elem_oid, 23,
            "element OID must be preserved for empty arrays"
        );
    }

    // --- encode_at tests ---

    #[test]
    fn encode_at_bool() {
        let mut dst = [0u8; 1];
        assert!(true.encode_at(&mut dst));
        assert_eq!(dst[0], 1);
        assert!(false.encode_at(&mut dst));
        assert_eq!(dst[0], 0);
        // Wrong size returns false.
        assert!(!true.encode_at(&mut [0u8; 2]));
    }

    #[test]
    fn encode_at_i16() {
        let mut dst = [0u8; 2];
        assert!(0x1234i16.encode_at(&mut dst));
        assert_eq!(dst, [0x12, 0x34]);
        assert!(!42i16.encode_at(&mut [0u8; 4]));
    }

    #[test]
    fn encode_at_i32() {
        let mut dst = [0u8; 4];
        assert!(42i32.encode_at(&mut dst));
        assert_eq!(dst, [0, 0, 0, 42]);
        assert!(!42i32.encode_at(&mut [0u8; 8]));
    }

    #[test]
    fn encode_at_i64() {
        let mut dst = [0u8; 8];
        assert!(1234567890123i64.encode_at(&mut dst));
        assert_eq!(dst, 1234567890123i64.to_be_bytes());
        assert!(!42i64.encode_at(&mut [0u8; 4]));
    }

    #[test]
    fn encode_at_f32() {
        let mut dst = [0u8; 4];
        assert!(3.14f32.encode_at(&mut dst));
        assert_eq!(dst, 3.14f32.to_be_bytes());
        assert!(!3.14f32.encode_at(&mut [0u8; 8]));
    }

    #[test]
    fn encode_at_f64() {
        let mut dst = [0u8; 8];
        assert!(3.14f64.encode_at(&mut dst));
        assert_eq!(dst, 3.14f64.to_be_bytes());
        assert!(!3.14f64.encode_at(&mut [0u8; 4]));
    }

    #[test]
    fn encode_at_u32() {
        let mut dst = [0u8; 4];
        assert!(42u32.encode_at(&mut dst));
        assert_eq!(dst, 42u32.to_be_bytes());
    }

    #[test]
    fn encode_at_str_default_fallback() {
        // Variable-length types use the default encode_at fallback.
        let s: &str = "hello";
        let mut dst = [0u8; 5];
        assert!(s.encode_at(&mut dst));
        assert_eq!(&dst, b"hello");
        // Wrong size returns false.
        assert!(!s.encode_at(&mut [0u8; 3]));
    }

    #[test]
    fn encode_at_matches_encode_binary() {
        // Verify encode_at produces identical bytes to encode_binary for all
        // fixed-size types.
        fn check<T: Encode>(val: T, expected_len: usize) {
            let mut buf = Vec::new();
            val.encode_binary(&mut buf);
            assert_eq!(buf.len(), expected_len);
            let mut dst = vec![0u8; expected_len];
            assert!(val.encode_at(&mut dst));
            assert_eq!(
                buf, dst,
                "encode_at must produce same bytes as encode_binary"
            );
        }
        check(true, 1);
        check(false, 1);
        check(42i16, 2);
        check(i16::MAX, 2);
        check(42i32, 4);
        check(i32::MIN, 4);
        check(42i64, 8);
        check(3.14f32, 4);
        check(3.14f64, 8);
        check(42u32, 4);
    }

    // --- 10KB string encode/decode roundtrip ---

    #[test]
    fn str_10kb_roundtrip() {
        let big = "A".repeat(10 * 1024);
        let mut buf = Vec::new();
        big.as_str().encode_binary(&mut buf);
        assert_eq!(buf.len(), 10 * 1024);
        assert_eq!(decode_str(&buf).unwrap(), big);
    }

    // --- Empty Vec<u8> encode roundtrip ---

    #[test]
    fn empty_vec_u8_encode_roundtrip() {
        let mut buf = Vec::new();
        Vec::<u8>::new().encode_binary(&mut buf);
        assert!(buf.is_empty(), "empty Vec<u8> should produce no bytes");
        assert_eq!(decode_bytes(&buf).len(), 0);
    }

    // --- f32 MIN/MAX roundtrip ---

    #[test]
    fn f32_min_max_roundtrip() {
        let mut buf = Vec::new();
        f32::MIN.encode_binary(&mut buf);
        assert_eq!(decode_f32(&buf).unwrap(), f32::MIN);

        buf.clear();
        f32::MAX.encode_binary(&mut buf);
        assert_eq!(decode_f32(&buf).unwrap(), f32::MAX);
    }

    // --- f64 MIN/MAX roundtrip ---

    #[test]
    fn f64_min_max_roundtrip() {
        let mut buf = Vec::new();
        f64::MIN.encode_binary(&mut buf);
        assert_eq!(decode_f64(&buf).unwrap(), f64::MIN);

        buf.clear();
        f64::MAX.encode_binary(&mut buf);
        assert_eq!(decode_f64(&buf).unwrap(), f64::MAX);
    }

    // --- i32 zero roundtrip ---

    #[test]
    fn i32_zero_roundtrip() {
        let mut buf = Vec::new();
        0i32.encode_binary(&mut buf);
        assert_eq!(decode_i32(&buf).unwrap(), 0);
    }

    // --- i64 zero roundtrip ---

    #[test]
    fn i64_zero_roundtrip() {
        let mut buf = Vec::new();
        0i64.encode_binary(&mut buf);
        assert_eq!(decode_i64(&buf).unwrap(), 0);
    }

    // --- i16 zero roundtrip ---

    #[test]
    fn i16_zero_roundtrip() {
        let mut buf = Vec::new();
        0i16.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), 0);
    }

    // --- f32 subnormal roundtrip ---

    #[test]
    fn f32_subnormal_roundtrip() {
        let mut buf = Vec::new();
        f32::MIN_POSITIVE.encode_binary(&mut buf);
        assert_eq!(decode_f32(&buf).unwrap(), f32::MIN_POSITIVE);
    }

    // --- f64 subnormal roundtrip ---

    #[test]
    fn f64_subnormal_roundtrip() {
        let mut buf = Vec::new();
        f64::MIN_POSITIVE.encode_binary(&mut buf);
        assert_eq!(decode_f64(&buf).unwrap(), f64::MIN_POSITIVE);
    }

    // --- f32 NaN bit-pattern preservation ---

    #[test]
    fn f32_nan_bit_preservation() {
        let mut buf = Vec::new();
        f32::NAN.encode_binary(&mut buf);
        let decoded = decode_f32(&buf).unwrap();
        assert!(decoded.is_nan());
        // Bit-pattern should be preserved
        assert_eq!(decoded.to_bits(), f32::NAN.to_bits());
    }

    // --- f64 NaN bit-pattern preservation ---

    #[test]
    fn f64_nan_bit_preservation() {
        let mut buf = Vec::new();
        f64::NAN.encode_binary(&mut buf);
        let decoded = decode_f64(&buf).unwrap();
        assert!(decoded.is_nan());
        assert_eq!(decoded.to_bits(), f64::NAN.to_bits());
    }

    // --- Option<T> roundtrip for all scalar types ---

    #[test]
    fn option_bool_some_roundtrip() {
        let val: Option<bool> = Some(true);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 16);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(decode_bool(&buf).unwrap());
    }

    #[test]
    fn option_bool_none_is_null() {
        let val: Option<bool> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 16); // reports bool OID even for None
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn option_i16_some_roundtrip() {
        let val: Option<i16> = Some(i16::MIN);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 21);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MIN);
    }

    #[test]
    fn option_i16_none_is_null() {
        let val: Option<i16> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 21);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn option_i64_some_roundtrip() {
        let val: Option<i64> = Some(i64::MAX);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 20);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(decode_i64(&buf).unwrap(), i64::MAX);
    }

    #[test]
    fn option_i64_none_is_null() {
        let val: Option<i64> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 20);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn option_f32_some_roundtrip() {
        let val: Option<f32> = Some(f32::INFINITY);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 700);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(decode_f32(&buf).unwrap(), f32::INFINITY);
    }

    #[test]
    fn option_f32_none_is_null() {
        let val: Option<f32> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 700);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn option_f64_some_nan_roundtrip() {
        let val: Option<f64> = Some(f64::NAN);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 701);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(decode_f64(&buf).unwrap().is_nan());
    }

    #[test]
    fn option_f64_none_is_null() {
        let val: Option<f64> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 701);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn option_string_some_roundtrip() {
        let val: Option<String> = Some("hello".to_owned());
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 25);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(decode_str(&buf).unwrap(), "hello");
    }

    #[test]
    fn option_string_none_is_null() {
        let val: Option<String> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 25);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn option_vec_u8_some_roundtrip() {
        let val: Option<Vec<u8>> = Some(vec![0xDE, 0xAD]);
        assert!(!val.is_null());
        assert_eq!(val.type_oid(), 17);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert_eq!(decode_bytes(&buf), &[0xDE, 0xAD]);
    }

    #[test]
    fn option_vec_u8_none_is_null() {
        let val: Option<Vec<u8>> = None;
        assert!(val.is_null());
        assert_eq!(val.type_oid(), 17);
        let mut buf = Vec::new();
        val.encode_binary(&mut buf);
        assert!(buf.is_empty());
    }

    // --- encode_at for variable-length types ---

    #[test]
    fn encode_at_vec_u8_same_size() {
        let v = vec![1u8, 2, 3];
        let mut dst = [0u8; 3];
        assert!(v.encode_at(&mut dst));
        assert_eq!(dst, [1, 2, 3]);
    }

    #[test]
    fn encode_at_vec_u8_wrong_size() {
        let v = vec![1u8, 2, 3];
        let mut dst = [0u8; 5];
        assert!(!v.encode_at(&mut dst));
    }

    #[test]
    fn encode_at_byte_slice_same_size() {
        let data: &[u8] = &[0xAA, 0xBB];
        let mut dst = [0u8; 2];
        assert!(data.encode_at(&mut dst));
        assert_eq!(dst, [0xAA, 0xBB]);
    }

    #[test]
    fn encode_at_byte_slice_wrong_size() {
        let data: &[u8] = &[0xAA, 0xBB];
        let mut dst = [0u8; 4];
        assert!(!data.encode_at(&mut dst));
    }

    #[test]
    fn encode_at_string_same_size() {
        let s = String::from("hi");
        let mut dst = [0u8; 2];
        assert!(s.encode_at(&mut dst));
        assert_eq!(&dst, b"hi");
    }

    #[test]
    fn encode_at_string_wrong_size() {
        let s = String::from("hi");
        let mut dst = [0u8; 5];
        assert!(!s.encode_at(&mut dst));
    }

    // --- encode_param with NULL ---

    #[test]
    fn encode_param_null_option() {
        // When param is_null(), wire protocol should NOT call encode_binary.
        // encode_param writes length + data, but for NULL, the caller handles
        // it differently (writes -1 length). encode_param just writes 0-length.
        let val: Option<i32> = None;
        let mut buf = Vec::new();
        encode_param(&mut buf, &val);
        // encode_param writes 4-byte length prefix + 0 bytes of data (since
        // encode_binary is a no-op for None). Length = 0.
        assert_eq!(buf.len(), 4);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 0);
    }

    // --- decode_array with negative element count ---

    #[test]
    fn decode_array_negative_element_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // ndim=1
        data.extend_from_slice(&0i32.to_be_bytes()); // has_null
        data.extend_from_slice(&23i32.to_be_bytes()); // elem OID
        data.extend_from_slice(&(-1i32).to_be_bytes()); // n_elements=-1 (negative!)
        data.extend_from_slice(&1i32.to_be_bytes()); // lower_bound
        let result = decode_array_i32(&data);
        assert!(result.is_err(), "negative element count should error");
        assert!(result.unwrap_err().to_string().contains("negative"));
    }

    // --- decode_array with excessive element count ---

    #[test]
    fn decode_array_excessive_element_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // ndim=1
        data.extend_from_slice(&0i32.to_be_bytes()); // has_null
        data.extend_from_slice(&23i32.to_be_bytes()); // elem OID
        data.extend_from_slice(&20_000_000i32.to_be_bytes()); // way over 10M limit
        data.extend_from_slice(&1i32.to_be_bytes()); // lower_bound
        let result = decode_array_i32(&data);
        assert!(result.is_err(), "excessive element count should error");
        assert!(result.unwrap_err().to_string().contains("exceeds limit"));
    }

    // --- decode_array header too short ---

    #[test]
    fn decode_array_header_too_short() {
        let data = [0u8; 8]; // less than 12 bytes minimum
        let result = decode_array_i32(&data);
        assert!(result.is_err(), "header too short should error");
    }

    // --- decode_array truncated dimension header ---

    #[test]
    fn decode_array_truncated_dimension_header() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // ndim=1
        data.extend_from_slice(&0i32.to_be_bytes()); // has_null
        data.extend_from_slice(&23i32.to_be_bytes()); // elem OID
                                                      // Missing n_elements and lower_bound (only 12 bytes, need 20)
        let result = decode_array_i32(&data);
        assert!(result.is_err(), "truncated dimension header should error");
    }

    mod proptest_fuzz {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn i32_roundtrip(val: i32) {
                let mut buf = Vec::new();
                val.encode_binary(&mut buf);
                let decoded = decode_i32(&buf).unwrap();
                prop_assert_eq!(decoded, val);
            }

            #[test]
            fn i64_roundtrip(val: i64) {
                let mut buf = Vec::new();
                val.encode_binary(&mut buf);
                let decoded = decode_i64(&buf).unwrap();
                prop_assert_eq!(decoded, val);
            }

            #[test]
            fn i16_roundtrip(val: i16) {
                let mut buf = Vec::new();
                val.encode_binary(&mut buf);
                let decoded = decode_i16(&buf).unwrap();
                prop_assert_eq!(decoded, val);
            }

            #[test]
            fn f32_roundtrip(val: f32) {
                let mut buf = Vec::new();
                val.encode_binary(&mut buf);
                let decoded = decode_f32(&buf).unwrap();
                if val.is_nan() {
                    prop_assert!(decoded.is_nan());
                } else {
                    prop_assert_eq!(decoded, val);
                }
            }

            #[test]
            fn f64_roundtrip(val: f64) {
                let mut buf = Vec::new();
                val.encode_binary(&mut buf);
                let decoded = decode_f64(&buf).unwrap();
                if val.is_nan() {
                    prop_assert!(decoded.is_nan());
                } else {
                    prop_assert_eq!(decoded, val);
                }
            }

            #[test]
            fn bool_roundtrip(val: bool) {
                let mut buf = Vec::new();
                val.encode_binary(&mut buf);
                let decoded = decode_bool(&buf).unwrap();
                prop_assert_eq!(decoded, val);
            }

            #[test]
            fn str_roundtrip(val in "\\PC*") {
                let mut buf = Vec::new();
                val.as_str().encode_binary(&mut buf);
                let decoded = decode_str(&buf).unwrap();
                prop_assert_eq!(decoded, val.as_str());
            }

            #[test]
            fn decode_i32_arbitrary_never_panics(data in proptest::collection::vec(any::<u8>(), 0..16)) {
                let _ = decode_i32(&data);
            }

            #[test]
            fn decode_str_arbitrary_never_panics(data in proptest::collection::vec(any::<u8>(), 0..1024)) {
                let _ = decode_str(&data);
            }
        }
    }

    // --- pg_type_oid: Option<T> reports correct OID for None ---

    // --- pg_type_oid: Option<T> reports correct OID for None — every type ---

    #[test]
    fn option_none_type_oid_scalars() {
        // Every scalar type: None must report the same OID as Some
        assert_eq!(None::<bool>.type_oid(), 16);
        assert_eq!(None::<i16>.type_oid(), 21);
        assert_eq!(None::<i32>.type_oid(), 23);
        assert_eq!(None::<i64>.type_oid(), 20);
        assert_eq!(None::<f32>.type_oid(), 700);
        assert_eq!(None::<f64>.type_oid(), 701);
        assert_eq!(None::<String>.type_oid(), 25);
        assert_eq!(None::<Vec<u8>>.type_oid(), 17);
        assert_eq!(None::<u32>.type_oid(), 26);

        // Some must also match
        assert_eq!(Some(true).type_oid(), 16);
        assert_eq!(Some(0i16).type_oid(), 21);
        assert_eq!(Some(0i32).type_oid(), 23);
        assert_eq!(Some(0i64).type_oid(), 20);
        assert_eq!(Some(0f32).type_oid(), 700);
        assert_eq!(Some(0f64).type_oid(), 701);
        assert_eq!(Some(String::new()).type_oid(), 25);
        assert_eq!(Some(Vec::<u8>::new()).type_oid(), 17);
        assert_eq!(Some(0u32).type_oid(), 26);
    }

    #[test]
    fn option_none_type_oid_arrays() {
        assert_eq!(None::<Vec<bool>>.type_oid(), 1000);
        assert_eq!(None::<Vec<i16>>.type_oid(), 1005);
        assert_eq!(None::<Vec<i32>>.type_oid(), 1007);
        assert_eq!(None::<Vec<i64>>.type_oid(), 1016);
        assert_eq!(None::<Vec<f32>>.type_oid(), 1021);
        assert_eq!(None::<Vec<f64>>.type_oid(), 1022);
        assert_eq!(None::<Vec<String>>.type_oid(), 1009);
        assert_eq!(None::<Vec<Vec<u8>>>.type_oid(), 1001);
    }

    #[test]
    fn option_none_type_oid_nested_option() {
        // Option<Option<T>> — the inner Option's pg_type_oid forwards to T
        assert_eq!(None::<Option<i32>>.type_oid(), 23);
        assert_eq!(Some(None::<i32>).type_oid(), 23);
        assert_eq!(Some(Some(42i32)).type_oid(), 23);
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn option_none_type_oid_uuid() {
        assert_eq!(None::<uuid::Uuid>.type_oid(), 2950);
    }

    #[cfg(feature = "time")]
    #[test]
    fn option_none_type_oid_time() {
        assert_eq!(None::<time::OffsetDateTime>.type_oid(), 1184);
        assert_eq!(None::<time::Date>.type_oid(), 1082);
        assert_eq!(None::<time::Time>.type_oid(), 1083);
        assert_eq!(None::<time::PrimitiveDateTime>.type_oid(), 1114);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn option_none_type_oid_chrono() {
        assert_eq!(None::<chrono::NaiveDateTime>.type_oid(), 1114);
        assert_eq!(None::<chrono::DateTime<chrono::Utc>>.type_oid(), 1184);
        assert_eq!(None::<chrono::NaiveDate>.type_oid(), 1082);
        assert_eq!(None::<chrono::NaiveTime>.type_oid(), 1083);
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn option_none_type_oid_decimal() {
        assert_eq!(None::<rust_decimal::Decimal>.type_oid(), 1700);
    }

    #[test]
    fn pg_type_oid_static_all_types() {
        // Scalars
        assert_eq!(bool::pg_type_oid(), 16);
        assert_eq!(i16::pg_type_oid(), 21);
        assert_eq!(i32::pg_type_oid(), 23);
        assert_eq!(i64::pg_type_oid(), 20);
        assert_eq!(f32::pg_type_oid(), 700);
        assert_eq!(f64::pg_type_oid(), 701);
        assert_eq!(<&str>::pg_type_oid(), 25);
        assert_eq!(String::pg_type_oid(), 25);
        assert_eq!(<&[u8]>::pg_type_oid(), 17);
        assert_eq!(Vec::<u8>::pg_type_oid(), 17);
        assert_eq!(u32::pg_type_oid(), 26);

        // Option forwards to inner
        assert_eq!(<Option<i32>>::pg_type_oid(), 23);
        assert_eq!(<Option<String>>::pg_type_oid(), 25);
        assert_eq!(<Option<bool>>::pg_type_oid(), 16);
        assert_eq!(<Option<i64>>::pg_type_oid(), 20);
        assert_eq!(<Option<f64>>::pg_type_oid(), 701);
        assert_eq!(<Option<Vec<u8>>>::pg_type_oid(), 17);

        // Arrays
        assert_eq!(<&[bool]>::pg_type_oid(), 1000);
        assert_eq!(Vec::<bool>::pg_type_oid(), 1000);
        assert_eq!(<&[i16]>::pg_type_oid(), 1005);
        assert_eq!(Vec::<i16>::pg_type_oid(), 1005);
        assert_eq!(<&[i32]>::pg_type_oid(), 1007);
        assert_eq!(Vec::<i32>::pg_type_oid(), 1007);
        assert_eq!(<&[i64]>::pg_type_oid(), 1016);
        assert_eq!(Vec::<i64>::pg_type_oid(), 1016);
        assert_eq!(<&[f32]>::pg_type_oid(), 1021);
        assert_eq!(Vec::<f32>::pg_type_oid(), 1021);
        assert_eq!(<&[f64]>::pg_type_oid(), 1022);
        assert_eq!(Vec::<f64>::pg_type_oid(), 1022);
        assert_eq!(<&[&str]>::pg_type_oid(), 1009);
        assert_eq!(Vec::<String>::pg_type_oid(), 1009);
        assert_eq!(<&[String]>::pg_type_oid(), 1009);
        assert_eq!(<&[&[u8]]>::pg_type_oid(), 1001);
        assert_eq!(<&[Vec<u8>]>::pg_type_oid(), 1001);
        assert_eq!(Vec::<Vec<u8>>::pg_type_oid(), 1001);
    }
}
