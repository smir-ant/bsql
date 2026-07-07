//! SQLite value model: the borrowed [`ValueRef`] view, the owned
//! [`SqliteValue`], the storage-class [`Type`] tag, and the classified
//! typed-read trait [`FromColumn`].
//!
//! SQLite has exactly five storage classes (NULL, INTEGER, REAL, TEXT, BLOB);
//! [`Type`] and both value enums enumerate all five with no residual. A typed
//! read that does not match the actual storage class is a classified
//! [`SqliteError`] — never a silent `None`.

use crate::error::SqliteError;

/// A SQLite storage class — the dynamic type of a stored value.
///
/// SQLite is dynamically typed: the storage class lives on the *value*, not
/// the column. These are the only five classes the engine can hand back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// `NULL`.
    Null,
    /// 64-bit signed integer.
    Integer,
    /// 64-bit IEEE-754 floating point.
    Real,
    /// UTF-8 (or declared-encoding) text.
    Text,
    /// Arbitrary byte blob.
    Blob,
}

// Footprint pin: a five-variant field-less enum is a single discriminant byte.
crate::footprint_pin!(Type, size = 1, align = 1);

impl core::fmt::Display for Type {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Null => "NULL",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
        })
    }
}

/// A zero-copy borrowed view of a single value — the ONE value vocabulary the
/// driver uses on BOTH sides of the wire.
///
/// * **Reading** a column: `Text`/`Blob` borrow the underlying byte buffer
///   directly (the arena's cell bytes) — no allocation, no copy. The borrow's
///   lifetime `'a` is bounded by the row it came from, so a `ValueRef` cannot
///   outlive the row step that produced it.
/// * **Binding** a parameter: the same enum is the parameter model for every
///   `*_params` verb. Each variant binds in its TRUE SQLite storage class —
///   `Null` binds SQL `NULL`, `Integer`/`Real` bind numerically (no affinity
///   coercion), `Text`/`Blob` bind the borrowed bytes zero-copy. This is why
///   `NULL` and `BLOB` parameters are expressible at all (a text-only param
///   model can bind neither), and why an integer bound against an
///   affinity-less comparison is compared as an integer, not silently as text.
///   The `From` impls below keep common binds terse (`42_i64.into()`,
///   `"name".into()`, `Some(x).into()` → the value or `Null`).
///
/// `Text` is raw bytes, not `&str`: SQLite does not guarantee a `TEXT` value is
/// valid UTF-8. Validation happens only when a `&str`/`String` is requested,
/// and a failure is the classified [`SqliteError::InvalidUtf8`], never a lossy
/// replacement.
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
    /// `NULL`.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit IEEE-754 floating point.
    Real(f64),
    /// Text bytes (declared encoding; not guaranteed UTF-8).
    Text(&'a [u8]),
    /// Blob bytes.
    Blob(&'a [u8]),
}

// Footprint pin: the widest variant is a fat slice pointer (ptr + len = 16),
// plus the discriminant. Lifetime parameters do not affect layout.
crate::footprint_pin!(ValueRef<'_>, size = 24, align = 8);

impl ValueRef<'_> {
    /// The storage class of this value.
    #[must_use]
    pub fn data_type(&self) -> Type {
        match self {
            Self::Null => Type::Null,
            Self::Integer(_) => Type::Integer,
            Self::Real(_) => Type::Real,
            Self::Text(_) => Type::Text,
            Self::Blob(_) => Type::Blob,
        }
    }
}

impl<'a> From<rusqlite::types::ValueRef<'a>> for ValueRef<'a> {
    fn from(v: rusqlite::types::ValueRef<'a>) -> Self {
        match v {
            rusqlite::types::ValueRef::Null => Self::Null,
            rusqlite::types::ValueRef::Integer(n) => Self::Integer(n),
            rusqlite::types::ValueRef::Real(f) => Self::Real(f),
            rusqlite::types::ValueRef::Text(b) => Self::Text(b),
            rusqlite::types::ValueRef::Blob(b) => Self::Blob(b),
        }
    }
}

// ─── parameter binding ──────────────────────────────────────────────────────
//
// `ValueRef` is the parameter model for every `*_params` verb. Binding runs
// through rusqlite's `ToSql` seam, but the driver's public surface stays
// `ValueRef` (rusqlite's own `ToSql` never leaks into a signature), and each
// bind is BORROWED — the `ToSqlOutput::Borrowed` path copies nothing on the
// Rust side, so a `&[ValueRef]` param list allocates nothing per parameter.

impl<'a> From<ValueRef<'a>> for rusqlite::types::ValueRef<'a> {
    fn from(v: ValueRef<'a>) -> Self {
        match v {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(n) => Self::Integer(n),
            ValueRef::Real(f) => Self::Real(f),
            ValueRef::Text(b) => Self::Text(b),
            ValueRef::Blob(b) => Self::Blob(b),
        }
    }
}

impl rusqlite::types::ToSql for ValueRef<'_> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        // BORROWED: the parameter's storage class is carried through unchanged
        // (no coercion) and its bytes (for Text/Blob) are lent, not copied. The
        // returned view's lifetime is `&self`'s, which the value's own `'a`
        // outlives (covariance narrows it), so this is a zero-copy bind.
        Ok(rusqlite::types::ToSqlOutput::Borrowed((*self).into()))
    }
}

// Terse constructors for the common bind cases. Non-borrowing scalars leave the
// target lifetime free; `&str`/`&[u8]` borrow it; `Option<T>` collapses to the
// inner value or `Null` — the ergonomic NULL bind.

impl From<i64> for ValueRef<'_> {
    fn from(n: i64) -> Self {
        Self::Integer(n)
    }
}

impl From<i32> for ValueRef<'_> {
    fn from(n: i32) -> Self {
        Self::Integer(i64::from(n))
    }
}

impl From<f64> for ValueRef<'_> {
    fn from(f: f64) -> Self {
        Self::Real(f)
    }
}

impl From<bool> for ValueRef<'_> {
    fn from(b: bool) -> Self {
        // SQLite has no boolean storage class; a boolean binds as the integers
        // 0/1, mirroring the `get::<bool>` read side (which accepts 0/1 only).
        Self::Integer(i64::from(b))
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &'a str) -> Self {
        Self::Text(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(b: &'a [u8]) -> Self {
        Self::Blob(b)
    }
}

impl<'a, T: Into<ValueRef<'a>>> From<Option<T>> for ValueRef<'a> {
    fn from(o: Option<T>) -> Self {
        // `Some(v)` binds `v`'s storage class; `None` binds SQL `NULL`. This is
        // the ergonomic nullable bind: `Some("x").into()` / `None::<&str>.into()`.
        match o {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

/// An OWNED SQLite value — the `'static` counterpart of the borrowed
/// [`ValueRef`].
///
/// Two roles: (1) an owned snapshot a caller can build and stash beyond a row's
/// borrow, and (2) an owned parameter source — [`as_ref`](Self::as_ref) yields a
/// [`ValueRef`] for binding, so a value that cannot be borrowed at the call site
/// (a computed `String`/`Vec<u8>`) still binds in its true storage class.
///
/// `Text` is a `String` (always valid UTF-8 by construction). A row's raw,
/// possibly-non-UTF-8 `TEXT` bytes are read borrowed via [`ValueRef`] /
/// `get::<&[u8]>`, never forced through this owned type.
#[derive(Debug, Clone)]
pub enum SqliteValue {
    /// `NULL`.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit IEEE-754 floating point.
    Real(f64),
    /// Validated UTF-8 text.
    Text(String),
    /// Owned blob bytes.
    Blob(Vec<u8>),
}

// Footprint pin: the widest variants (`Text(String)` / `Blob(Vec<u8>)`) are
// each three words, plus the discriminant.
crate::footprint_pin!(SqliteValue, size = 32, align = 8);

impl SqliteValue {
    /// A zero-copy borrowed view of this owned value.
    #[must_use]
    pub fn as_ref(&self) -> ValueRef<'_> {
        match self {
            Self::Null => ValueRef::Null,
            Self::Integer(n) => ValueRef::Integer(*n),
            Self::Real(f) => ValueRef::Real(*f),
            Self::Text(s) => ValueRef::Text(s.as_bytes()),
            Self::Blob(b) => ValueRef::Blob(b.as_slice()),
        }
    }

    /// The storage class of this value.
    #[must_use]
    pub fn data_type(&self) -> Type {
        self.as_ref().data_type()
    }
}

/// A Rust type that can be decoded from a single SQLite column value.
///
/// Every built-in impl is *classified*: a value whose storage class does not
/// match returns a [`SqliteError`] (never a silent `None`). The largest
/// difference from a text-parsing model is that decoding is driven by SQLite's
/// native storage class, not by re-parsing a stringified form — `get::<i64>`
/// on an `INTEGER` reads the integer directly, and `get::<i64>` on a `TEXT`
/// value is a [`SqliteError::TypeMismatch`], not a hopeful `str::parse`.
///
/// # Numeric coercion: lossless-or-loud (by design)
///
/// The one place a value is coerced ACROSS storage classes is
/// `INTEGER → f64`, and it is deliberately ASYMMETRIC: `get::<f64>` accepts an
/// `INTEGER` in `[-(2^53), 2^53]` (every such integer is exactly representable
/// as `f64`, so the widening is LOSSLESS), and an integer OUTSIDE that range is
/// the classified [`SqliteError::InexactFloat`] ("read it as `i64`") — never a
/// silently-rounded approximation. The reverse — `get::<i64>` on a `REAL` — is
/// NOT coerced at all: it is a [`SqliteError::TypeMismatch`], because a real is a
/// LOSSY source for an integer (the fractional part, or a magnitude past
/// `i64`). The rule is uniform: a cross-class read succeeds only when it is
/// provably lossless, and is a loud classified error otherwise. The narrowing
/// integer reads (`i16`/`i32`/`u32`/`u64` from an out-of-range `i64`) follow the
/// same rule via [`SqliteError::IntegerOutOfRange`].
///
/// The lifetime `'a` is the borrow of the source value: an owned target
/// (`String`, `Vec<u8>`, `i64`, …) ignores it; a borrowed target (`&'a str`,
/// `&'a [u8]`) borrows the column buffer zero-copy.
pub trait FromColumn<'a>: Sized {
    /// Decode column `column`'s `value`, classifying any mismatch. `column` is
    /// carried only to tag a returned error with its location.
    fn from_column(column: usize, value: ValueRef<'a>) -> Result<Self, SqliteError>;
}

/// Shared classified read: a real `NULL` on a non-nullable read is
/// [`SqliteError::UnexpectedNull`] (distinct from a type mismatch); otherwise
/// the value is decoded via [`FromColumn`].
pub(crate) fn typed_get<'a, T: FromColumn<'a>>(
    column: usize,
    value: ValueRef<'a>,
) -> Result<T, SqliteError> {
    if matches!(value, ValueRef::Null) {
        return Err(SqliteError::UnexpectedNull { column });
    }
    T::from_column(column, value)
}

/// Shared classified nullable read: a real `NULL` is `Ok(None)` (distinct from
/// a type mismatch, which is `Err`); otherwise `Ok(Some(decoded))`.
pub(crate) fn typed_get_opt<'a, T: FromColumn<'a>>(
    column: usize,
    value: ValueRef<'a>,
) -> Result<Option<T>, SqliteError> {
    if matches!(value, ValueRef::Null) {
        return Ok(None);
    }
    T::from_column(column, value).map(Some)
}

impl FromColumn<'_> for i64 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            ValueRef::Integer(n) => Ok(n),
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Integer,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for i16 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            // SQLite integers are `i64`; a narrower `i16` (a `smallint` column)
            // range-checks. Out of range is the classified `IntegerOutOfRange`,
            // never a truncated/wrapped read — mirroring `i32`/`u32`.
            ValueRef::Integer(n) => {
                Self::try_from(n).map_err(|_| SqliteError::IntegerOutOfRange { column, value: n })
            }
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Integer,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for i32 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            ValueRef::Integer(n) => {
                Self::try_from(n).map_err(|_| SqliteError::IntegerOutOfRange { column, value: n })
            }
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Integer,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for u32 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            // SQLite integers are signed `i64`; a `u32` read range-checks (a
            // rowid, count, or bitfield). A negative or out-of-range value is the
            // classified `IntegerOutOfRange`, never a truncated/wrapped read.
            ValueRef::Integer(n) => {
                Self::try_from(n).map_err(|_| SqliteError::IntegerOutOfRange { column, value: n })
            }
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Integer,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for u64 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            // A `u64` read of an `i64` fails only on a NEGATIVE value (every
            // non-negative `i64` fits `u64`) — again classified, never wrapped.
            ValueRef::Integer(n) => {
                Self::try_from(n).map_err(|_| SqliteError::IntegerOutOfRange { column, value: n })
            }
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Integer,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for f64 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            ValueRef::Real(f) => Ok(f),
            // A lossless INTEGER -> f64 coercion, bounded to the range where
            // every integer round-trips through f64's 53-bit mantissa exactly.
            // Outside it, returning a rounded value would be a silent loss, so
            // the honest signal is the classified `InexactFloat` ("read it as
            // i64") rather than an approximation.
            ValueRef::Integer(n) if (-(1i64 << 53)..=(1i64 << 53)).contains(&n) => {
                Ok(exact_i64_as_f64(n))
            }
            ValueRef::Integer(n) => Err(SqliteError::InexactFloat { column, value: n }),
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Real,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for f32 {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        // The narrowing float read — the checked peer of the `i16`/`i32`
        // narrowings and of `f64`'s integer coercion. SQLite has no `f32`
        // storage class (a REAL is always an 8-byte `f64`), so a read succeeds
        // only when the conversion is provably lossless, and is a loud classified
        // error otherwise — never a silently rounded/overflowed approximation.
        match value {
            // A REAL narrows to `f32` only when it round-trips exactly
            // (`f64 -> f32 -> f64` is the identity). A magnitude past `f32::MAX`
            // (which would narrow to `±inf`) or a value needing more than `f32`'s
            // 24-bit mantissa fails the round-trip and is the classified
            // `InexactFloatNarrowing`. NaN is a valid `f32` value (representable),
            // so it is accepted directly — but a `!=`-based round-trip check would
            // reject it (`NaN != NaN`), so it is special-cased first.
            ValueRef::Real(v) if v.is_nan() => Ok(Self::NAN),
            ValueRef::Real(v) => {
                let narrowed = narrow_f64_to_f32(v);
                if f64::from(narrowed) == v {
                    Ok(narrowed)
                } else {
                    Err(SqliteError::InexactFloatNarrowing { column, value: v })
                }
            }
            // An INTEGER read as `f32` is lossless only within `f32`'s exact
            // integer range `[-(2^24), 2^24]` (its 24-bit mantissa) — the tighter
            // peer of `f64`'s `[-(2^53), 2^53]`. Outside it, the classified
            // `InexactFloat` ("read it as i64"), never a rounded value.
            ValueRef::Integer(n) if (-(1i64 << 24)..=(1i64 << 24)).contains(&n) => {
                Ok(exact_i64_as_f32(n))
            }
            ValueRef::Integer(n) => Err(SqliteError::InexactFloat { column, value: n }),
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Real,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for bool {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        match value {
            ValueRef::Integer(0) => Ok(false),
            ValueRef::Integer(1) => Ok(true),
            ValueRef::Integer(n) => Err(SqliteError::NotABoolean { column, value: n }),
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Integer,
                found: other.data_type(),
            }),
        }
    }
}

impl<'a> FromColumn<'a> for &'a str {
    fn from_column(column: usize, value: ValueRef<'a>) -> Result<Self, SqliteError> {
        match value {
            ValueRef::Text(bytes) => {
                core::str::from_utf8(bytes).map_err(|_| SqliteError::InvalidUtf8 { column })
            }
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Text,
                found: other.data_type(),
            }),
        }
    }
}

impl<'a> FromColumn<'a> for &'a [u8] {
    fn from_column(column: usize, value: ValueRef<'a>) -> Result<Self, SqliteError> {
        match value {
            ValueRef::Blob(bytes) => Ok(bytes),
            other => Err(SqliteError::TypeMismatch {
                column,
                expected: Type::Blob,
                found: other.data_type(),
            }),
        }
    }
}

impl FromColumn<'_> for String {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        <&str>::from_column(column, value).map(str::to_owned)
    }
}

impl FromColumn<'_> for Vec<u8> {
    fn from_column(column: usize, value: ValueRef<'_>) -> Result<Self, SqliteError> {
        <&[u8]>::from_column(column, value).map(<[u8]>::to_vec)
    }
}

/// Convert an integer already proven to lie within `[-(2^53), 2^53]` to `f64`.
/// Every such integer is exactly representable, so this is lossless (not a
/// silent truncation). Isolated behind a checked-then-widen boundary so the
/// one lossless widening cast in the crate is auditable in a single place.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "caller proves n is within [-(2^53), 2^53]; every such integer is exactly representable as f64, so the widening is lossless"
)]
fn exact_i64_as_f64(n: i64) -> f64 {
    n as f64
}

/// Convert an integer already proven to lie within `[-(2^24), 2^24]` to `f32`.
/// Every such integer is exactly representable in `f32`'s 24-bit mantissa, so
/// this is lossless (not a silent truncation) — the `f32` peer of
/// [`exact_i64_as_f64`], isolated behind the same checked-then-widen boundary.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "caller proves n is within [-(2^24), 2^24]; every such integer is exactly representable as f32, so the widening is lossless"
)]
fn exact_i64_as_f32(n: i64) -> f32 {
    n as f32
}

/// Narrow a `f64` to `f32` (round-to-nearest). LOSSY in general, so the sole
/// caller ([`f32::from_column`](FromColumn::from_column)) checks the round-trip
/// (`f64::from(narrowed) == source`) and classifies a non-round-tripping value
/// rather than returning it — this helper only isolates the one narrowing cast in
/// the crate so it is auditable in a single place.
#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "the sole caller checks the round-trip and rejects a lossy narrow as InexactFloatNarrowing; this isolates the one f64->f32 cast for audit"
)]
fn narrow_f64_to_f32(v: f64) -> f32 {
    v as f32
}
