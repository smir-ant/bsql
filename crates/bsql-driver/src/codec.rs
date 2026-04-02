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
/// use bsql_driver::Encode;
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
        let micros = diff.whole_microseconds() as i64;
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
        let days = (*self - pg_epoch).whole_days() as i32;
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

#[cfg(feature = "chrono")]
impl Encode for chrono::DateTime<chrono::Utc> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        // PG epoch: 2000-01-01 00:00:00 UTC = Unix timestamp 946684800
        let pg_epoch_unix_micros: i64 = 946_684_800 * 1_000_000;
        let unix_micros = self.timestamp_micros();
        let pg_micros = unix_micros - pg_epoch_unix_micros;
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
        let days = (*self - pg_epoch).num_days() as i32;
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
        let micros = diff.num_microseconds().unwrap_or(0);
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
            buf.extend_from_slice(&0i16.to_be_bytes()); // ndigits
            buf.extend_from_slice(&0i16.to_be_bytes()); // weight
            buf.extend_from_slice(&0x0000i16.to_be_bytes()); // sign
            buf.extend_from_slice(&(self.scale() as i16).to_be_bytes()); // dscale
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

        // Collect decimal digits
        let mut decimal_digits = Vec::new();
        while mantissa > 0 {
            decimal_digits.push((mantissa % 10) as i16);
            mantissa /= 10;
        }
        decimal_digits.reverse();

        // decimal_digits now has the full unscaled number.
        // The decimal point is `scale` digits from the right.
        // Integer part length:
        let total_digits = decimal_digits.len();
        let int_len = if total_digits > scale as usize {
            total_digits - scale as usize
        } else {
            0
        };

        // Pad integer part on the left so its length is a multiple of 4
        let int_pad = if int_len > 0 {
            (4 - (int_len % 4)) % 4
        } else {
            0
        };
        // Pad fractional part on the right so total is a multiple of 4
        let frac_len = total_digits - int_len;
        let frac_pad = (4 - (frac_len % 4)) % 4;

        let mut padded = vec![0i16; int_pad];
        padded.extend_from_slice(&decimal_digits);
        padded.extend(std::iter::repeat(0i16).take(frac_pad));

        // Group into base-10000 digits
        let mut pg_digits = Vec::new();
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
        let weight = if int_len > 0 {
            ((int_len + int_pad) / 4 - 1) as i16
        } else {
            // Pure fractional: weight is negative
            // E.g., 0.0001 has weight -1 (first group is 10^-4)
            -((scale as usize - frac_len + frac_pad) as i16 / 4 + 1)
        };

        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&(scale as i16).to_be_bytes());
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
#[inline]
pub fn decode_str(data: &[u8]) -> Result<&str, DriverError> {
    std::str::from_utf8(data)
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
    let n_elements = i32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
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
    pg_epoch
        .checked_add_days(chrono::Days::new(days.max(0) as u64))
        .or_else(|| pg_epoch.checked_sub_days(chrono::Days::new(days.unsigned_abs() as u64)))
        .ok_or_else(|| DriverError::Protocol(format!("date out of range: {days} days")))
}

/// Decode PG time to `chrono::NaiveTime`.
#[cfg(feature = "chrono")]
#[inline]
pub fn decode_time_chrono(data: &[u8]) -> Result<chrono::NaiveTime, DriverError> {
    let micros = decode_i64(data)?;
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
    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let off = 8 + i * 2;
        digits.push(i16::from_be_bytes([data[off], data[off + 1]]) as i64);
    }

    // Build the string representation: integer part . fractional part
    // The first digit has weight `weight` (units of 10^4).
    // Digits with index <= weight contribute to the integer part.
    // Digits with index > weight contribute to the fractional part.
    let mut int_part = String::new();
    let mut frac_part = String::new();

    for (i, &d) in digits.iter().enumerate() {
        let idx = i as i32;
        if idx <= weight {
            // Integer part digit
            if int_part.is_empty() {
                // First group: no leading zeros
                int_part.push_str(&d.to_string());
            } else {
                // Subsequent groups: always 4 digits (pad with leading zeros)
                int_part.push_str(&format!("{d:04}"));
            }
        } else {
            // Fractional part digit: always 4 digits
            frac_part.push_str(&format!("{d:04}"));
        }
    }

    // If weight >= ndigits, the integer part needs trailing zeros
    // (e.g., weight=2 with 1 digit means digit * 10^8)
    for _ in ndigits as i32..=weight {
        int_part.push_str("0000");
    }

    if int_part.is_empty() {
        int_part.push('0');
        // If weight < 0, we need leading fractional zeros
        // weight=-1 means first digit is at 10^-4 position (no extra zeros needed)
        // weight=-2 means first digit is at 10^-8 (4 leading fractional zeros)
        for _ in 0..(-1 - weight) {
            frac_part.insert_str(0, "0000");
        }
    }

    // Trim trailing fractional zeros
    let frac_trimmed = frac_part.trim_end_matches('0');

    let s = if frac_trimmed.is_empty() {
        int_part
    } else {
        format!("{int_part}.{frac_trimmed}")
    };

    let mut result: rust_decimal::Decimal = s
        .parse()
        .map_err(|e| DriverError::Protocol(format!("numeric parse error: {e} (from \"{s}\")")))?;

    if sign == 0x4000 {
        result.set_sign_negative(true);
    }

    Ok(result)
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
}
