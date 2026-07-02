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

/// A zero-copy borrowed view of a single column's value.
///
/// `Text`/`Blob` borrow the underlying byte buffer directly (SQLite's own
/// column memory on the streaming path, or the owned cell's buffer on the
/// eager path) — no allocation, no copy. The borrow's lifetime `'a` is bounded
/// by the row it came from, so a `ValueRef` cannot outlive the row step that
/// produced it.
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

/// An owned SQLite value — the materialized cell of an eager [`Row`].
///
/// `Text` is stored as a validated `String`: the eager materialization
/// enforces UTF-8 once, up front, and a non-UTF-8 `TEXT` cell fails the whole
/// query with a classified [`SqliteError::InvalidUtf8`] rather than being
/// silently lossy. A caller that must handle non-UTF-8 text bytes uses the
/// streaming [`Connection::query_each`] path with `value_ref` /
/// `get::<&[u8]>` instead.
///
/// [`Row`]: crate::Row
/// [`Connection::query_each`]: crate::Connection::query_each
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
