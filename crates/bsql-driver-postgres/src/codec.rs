//! Binary encode/decode for PostgreSQL types.
//!
//! All decoding operates on raw byte slices (from the arena or wire buffer).
//! Encoding appends big-endian bytes to a `Vec<u8>`.
//!
//! PostgreSQL binary format is big-endian for all numeric types.

use crate::DriverError;

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

    /// Whether this value represents SQL NULL.
    ///
    /// When true, the wire protocol sends length -1 with no data bytes.
    /// Default is false. Implementations for `Option<T>` override this.
    fn is_null(&self) -> bool {
        false
    }
}

// --- Encode implementations ---

impl Encode for bool {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.push(if *self { 1 } else { 0 });
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        16 // bool
    }
}

impl Encode for i16 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        21 // int2
    }
}

impl Encode for i32 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        23 // int4
    }
}

impl Encode for i64 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        20 // int8
    }
}

impl Encode for f32 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        700 // float4
    }
}

impl Encode for f64 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        701 // float8
    }
}

impl Encode for &str {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        25 // text
    }
}

impl Encode for String {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        25 // text
    }
}

impl Encode for &[u8] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        17 // bytea
    }
}

impl Encode for Vec<u8> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        17 // bytea
    }
}

impl Encode for u32 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        26 // oid
    }
}

// --- Option<T> Encode — NULL parameter support ---

impl<T: Encode> Encode for Option<T> {
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
        // For NULL, we return 0 (unspecified). This is safe because the Parse
        // message's param_oids array always gets explicit, concrete type OIDs
        // from codegen — the Encode::type_oid for Option is only used in the
        // fallback path and PG infers the type from context when it sees 0.
        match self {
            Some(val) => val.type_oid(),
            None => 0,
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
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        2950 // uuid
    }
}

#[cfg(feature = "time")]
impl Encode for time::OffsetDateTime {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // PG epoch: 2000-01-01 00:00:00 UTC
        // PG stores timestamptz as i64 microseconds since PG epoch
        let pg_epoch =
            time::OffsetDateTime::from_unix_timestamp(946_684_800).expect("PG epoch is valid");
        let diff = *self - pg_epoch;

        // fits in i64, but extreme values from time::OffsetDateTime could overflow.
        let micros_128 = diff.whole_microseconds();
        let micros: i64 = if micros_128 >= i64::MIN as i128 && micros_128 <= i64::MAX as i128 {
            micros_128 as i64
        } else {
            // Clamp to PG's valid range boundaries
            if micros_128 < 0 { i64::MIN } else { i64::MAX }
        };
        buf.extend_from_slice(&micros.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1184 // timestamptz
    }
}

#[cfg(feature = "time")]
impl Encode for time::Date {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // PG stores date as i32 days since 2000-01-01
        let pg_epoch = time::Date::from_calendar_date(2000, time::Month::January, 1)
            .expect("PG epoch date is valid");
        let days_i64 = (*self - pg_epoch).whole_days();
        let days =
            i32::try_from(days_i64).unwrap_or(if days_i64 < 0 { i32::MIN } else { i32::MAX });
        buf.extend_from_slice(&days.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1082 // date
    }
}

#[cfg(feature = "time")]
impl Encode for time::Time {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // PG stores time as i64 microseconds since midnight
        let midnight = time::Time::MIDNIGHT;
        let diff = *self - midnight;
        let micros = diff.whole_microseconds() as i64;
        buf.extend_from_slice(&micros.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1083 // time
    }
}

#[cfg(feature = "time")]
impl Encode for time::PrimitiveDateTime {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // TIMESTAMP (without tz) has the same binary format as TIMESTAMPTZ:
        // i64 microseconds since PG epoch (2000-01-01 00:00:00)
        let pg_epoch =
            time::OffsetDateTime::from_unix_timestamp(946_684_800).expect("PG epoch is valid");
        let as_utc = self.assume_utc();
        let diff = as_utc - pg_epoch;
        let micros_128 = diff.whole_microseconds();
        let micros: i64 = if micros_128 >= i64::MIN as i128 && micros_128 <= i64::MAX as i128 {
            micros_128 as i64
        } else {
            if micros_128 < 0 { i64::MIN } else { i64::MAX }
        };
        buf.extend_from_slice(&micros.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1114 // timestamp (without timezone)
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDateTime {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // TIMESTAMP has same binary format: i64 microseconds since PG epoch
        let pg_epoch_unix_micros: i64 = 946_684_800 * 1_000_000;
        let unix_micros = self.and_utc().timestamp_micros();
        let pg_micros = unix_micros.saturating_sub(pg_epoch_unix_micros);
        buf.extend_from_slice(&pg_micros.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1114 // timestamp (without timezone)
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::DateTime<chrono::Utc> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // PG epoch: 2000-01-01 00:00:00 UTC = Unix timestamp 946684800
        let pg_epoch_unix_micros: i64 = 946_684_800 * 1_000_000;
        let unix_micros = self.timestamp_micros();
        let pg_micros = unix_micros.saturating_sub(pg_epoch_unix_micros);
        buf.extend_from_slice(&pg_micros.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1184 // timestamptz
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDate {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("PG epoch date valid");
        let days_i64 = (*self - pg_epoch).num_days();
        let days =
            i32::try_from(days_i64).unwrap_or(if days_i64 < 0 { i32::MIN } else { i32::MAX });
        buf.extend_from_slice(&days.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1082 // date
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveTime {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("midnight valid");
        let diff = *self - midnight;

        // Panic on None instead of silently encoding midnight (0).
        let micros = diff
            .num_microseconds()
            .expect("time-of-day difference always fits i64");
        buf.extend_from_slice(&micros.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1083 // time
    }
}

#[cfg(feature = "decimal")]
impl Encode for rust_decimal::Decimal {
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

        // Collect decimal digits (max ~39 for u128, SmallVec caps at 32 inline)
        let mut decimal_digits: smallvec::SmallVec<[i16; 32]> = smallvec::SmallVec::new();
        while mantissa > 0 {
            decimal_digits.push((mantissa % 10) as i16);
            mantissa /= 10;
        }
        decimal_digits.reverse();

        // decimal_digits now has the full unscaled number.
        // The decimal point is `scale` digits from the right.
        // Integer part length:
        let total_digits = decimal_digits.len();
        let int_len = total_digits.saturating_sub(scale as usize);

        // Pad integer part on the left so its length is a multiple of 4
        let int_pad = if int_len > 0 {
            (4 - (int_len % 4)) % 4
        } else {
            0
        };
        // Pad fractional part on the right so total is a multiple of 4
        let frac_len = total_digits - int_len;
        let frac_pad = (4 - (frac_len % 4)) % 4;

        let mut padded: smallvec::SmallVec<[i16; 32]> = smallvec::SmallVec::new();
        padded.extend(std::iter::repeat_n(0i16, int_pad));
        padded.extend_from_slice(&decimal_digits);
        padded.extend(std::iter::repeat_n(0i16, frac_pad));

        // Group into base-10000 digits
        let mut pg_digits: smallvec::SmallVec<[i16; 12]> = smallvec::SmallVec::new();
        for chunk in padded.chunks(4) {
            let d = chunk[0] * 1000 + chunk[1] * 100 + chunk[2] * 10 + chunk[3];
            pg_digits.push(d);
        }

        // Strip trailing zero groups from the fractional part
        let int_groups = (int_len + int_pad) / 4;
        while pg_digits.len() > int_groups && *pg_digits.last().unwrap() == 0 {
            pg_digits.pop();
        }

        let ndigits = pg_digits.len() as i16;

        // for large scales, cast to i16 only at the end with saturation.
        let weight: i16 = if int_len > 0 {
            let w = (int_len + int_pad) / 4 - 1;
            w as i16
        } else {
            // Pure fractional: weight is negative
            // E.g., 0.0001 has weight -1 (first group is 10^-4)
            let w = -((scale as usize - frac_len + frac_pad) as i32 / 4 + 1);
            // Clamp to i16 range (PG weight is i16 on the wire)
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
        1700 // numeric
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
        encode_array_header(buf, self.len(), 16);
        for val in self {
            buf.extend_from_slice(&1i32.to_be_bytes()); // elem_len = 1
            buf.push(if *val { 1 } else { 0 });
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1000 // bool[]
    }
}

impl Encode for &[bool] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1000
    }
}

impl Encode for Vec<bool> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1000
    }
}

impl Encode for [i16] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 21);
        for val in self {
            buf.extend_from_slice(&2i32.to_be_bytes()); // elem_len = 2
            buf.extend_from_slice(&val.to_be_bytes());
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1005 // int2[]
    }
}

impl Encode for &[i16] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1005
    }
}

impl Encode for Vec<i16> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1005
    }
}

impl Encode for [i32] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 23);
        for val in self {
            buf.extend_from_slice(&4i32.to_be_bytes()); // elem_len = 4
            buf.extend_from_slice(&val.to_be_bytes());
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1007 // int4[]
    }
}

impl Encode for &[i32] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1007
    }
}

impl Encode for Vec<i32> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1007
    }
}

impl Encode for [i64] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 20);
        for val in self {
            buf.extend_from_slice(&8i32.to_be_bytes()); // elem_len = 8
            buf.extend_from_slice(&val.to_be_bytes());
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1016 // int8[]
    }
}

impl Encode for &[i64] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1016
    }
}

impl Encode for Vec<i64> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1016
    }
}

impl Encode for [f32] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 700);
        for val in self {
            buf.extend_from_slice(&4i32.to_be_bytes()); // elem_len = 4
            buf.extend_from_slice(&val.to_be_bytes());
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1021 // float4[]
    }
}

impl Encode for &[f32] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1021
    }
}

impl Encode for Vec<f32> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1021
    }
}

impl Encode for [f64] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 701);
        for val in self {
            buf.extend_from_slice(&8i32.to_be_bytes()); // elem_len = 8
            buf.extend_from_slice(&val.to_be_bytes());
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1022 // float8[]
    }
}

impl Encode for &[f64] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1022
    }
}

impl Encode for Vec<f64> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1022
    }
}

impl Encode for [&str] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 25);
        for val in self {
            let bytes = val.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1009 // text[]
    }
}

impl Encode for &[&str] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1009
    }
}

impl Encode for Vec<String> {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 25);
        for val in self {
            let bytes = val.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1009 // text[]
    }
}

impl Encode for [&[u8]] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 17);
        for val in self {
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1001 // bytea[]
    }
}

impl Encode for &[&[u8]] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1001
    }
}

impl Encode for [Vec<u8>] {
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        encode_array_header(buf, self.len(), 17);
        for val in self {
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val);
        }
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1001 // bytea[]
    }
}

impl Encode for &[Vec<u8>] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        (**self).encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1001
    }
}

impl Encode for Vec<Vec<u8>> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        self.as_slice().encode_binary(buf);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        1001 // bytea[]
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
    let pg_epoch = time::Date::from_calendar_date(2000, time::Month::January, 1)
        .expect("PG epoch date is valid");
    pg_epoch
        .checked_add(time::Duration::days(days as i64))
        .ok_or_else(|| DriverError::Protocol(format!("date out of range: {days} days")))
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
    let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("PG epoch valid");

    // silently mapped ALL dates before 2000-01-01 to epoch. Check sign first.
    let result = if days >= 0 {
        pg_epoch.checked_add_days(chrono::Days::new(days as u64))
    } else {
        pg_epoch.checked_sub_days(chrono::Days::new(days.unsigned_abs() as u64))
    };
    result.ok_or_else(|| DriverError::Protocol(format!("date out of range: {days} days")))
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
mod tests {
    use super::*;

    // --- Encode round-trips ---

    #[test]
    fn bool_roundtrip() {
        let mut buf = Vec::new();
        true.encode_binary(&mut buf);
        assert_eq!(decode_bool(&buf).unwrap(), true);

        buf.clear();
        false.encode_binary(&mut buf);
        assert_eq!(decode_bool(&buf).unwrap(), false);
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
        assert_eq!(val.type_oid(), 0);
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
}
