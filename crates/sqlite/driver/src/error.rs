use crate::value::Type;

/// Errors surfaced by the SQLite driver.
///
/// Every read-path failure is a *classified* variant — a typed read of a
/// column whose SQLite storage class does not match the requested Rust type
/// is a [`SqliteError::TypeMismatch`], a real SQL `NULL` read as non-nullable
/// is a distinct [`SqliteError::UnexpectedNull`], and so on. None is ever
/// collapsed into a silent `None`: a wrong-type read and a genuine `NULL` are
/// never indistinguishable.
#[derive(Debug)]
#[non_exhaustive]
pub enum SqliteError {
    /// Opening / configuring the database failed.
    Open(String),
    /// A query or statement failed for a driver-level reason without a
    /// preserved SQLite error code.
    Query(String),
    /// A SQLite engine error with its preserved extended error code for
    /// programmatic matching (constraint violation, busy, etc.).
    Sqlite {
        /// The SQLite extended error code, if the engine supplied one.
        code: Option<i32>,
        /// The engine's textual error message.
        message: String,
    },
    /// A transaction closure failed and the subsequent `ROLLBACK` also failed,
    /// leaving the connection in an indeterminate transactional state. Both the
    /// original cause and the rollback failure are preserved — neither is
    /// silently dropped.
    TransactionRollbackFailed {
        /// The error the user's closure returned (the primary cause).
        original: Box<SqliteError>,
        /// The error the cleanup `ROLLBACK` returned.
        rollback: Box<SqliteError>,
    },
    /// A typed read requested a Rust type incompatible with the column's actual
    /// SQLite storage class (e.g. reading an `Integer` column as `&str`). This
    /// is a *classified* failure, never a silent `None`.
    TypeMismatch {
        /// The zero-based column index that was read.
        column: usize,
        /// The storage class the requested Rust type maps to.
        expected: Type,
        /// The storage class actually present in the column.
        found: Type,
    },
    /// A non-nullable typed read (`get`) hit a real SQL `NULL`. Distinct from
    /// [`SqliteError::TypeMismatch`]: the column *is* the right shape, it is
    /// simply absent. Use `get_opt` to read a nullable column as `Option<T>`.
    UnexpectedNull {
        /// The zero-based column index that held `NULL`.
        column: usize,
    },
    /// A text column's bytes were not valid UTF-8 when read as `&str`/`String`.
    /// The raw bytes are always recoverable via `value_ref` /
    /// `get::<&[u8]>` — validation is only enforced at the point a UTF-8 view
    /// is requested, never silently lossy.
    InvalidUtf8 {
        /// The zero-based column index whose text bytes failed validation.
        column: usize,
    },
    /// An `Integer` value did not fit the requested narrower integer type
    /// (e.g. an `i64` outside `i32`'s range read as `i32`). Never truncated
    /// silently — the honest signal is "read it as `i64`".
    IntegerOutOfRange {
        /// The zero-based column index that was read.
        column: usize,
        /// The value that did not fit.
        value: i64,
    },
    /// An `Integer` value was read as `f64` but lies outside the `[-(2^53),
    /// 2^53]` range in which every integer round-trips through `f64`'s 53-bit
    /// mantissa exactly. Returning a rounded approximation would be a silent
    /// loss; this classified error says "read it as `i64`".
    InexactFloat {
        /// The zero-based column index that was read.
        column: usize,
        /// The integer value that is not exactly representable as `f64`.
        value: i64,
    },
    /// An `Integer` value other than `0` or `1` was read as `bool`. SQLite has
    /// no boolean storage class; a boolean is stored as the integers `0`/`1`,
    /// so any other integer is ambiguous rather than silently truthy.
    NotABoolean {
        /// The zero-based column index that was read.
        column: usize,
        /// The integer value that is not a canonical boolean.
        value: i64,
    },
    /// A column index was out of bounds for the row's column count.
    ColumnIndexOutOfBounds {
        /// The requested zero-based index.
        index: usize,
        /// The number of columns actually present.
        count: usize,
    },
    /// A by-name column lookup named a column absent from the result.
    UnknownColumn {
        /// The column name that did not resolve.
        name: String,
    },
}

// Footprint pin: sized by the widest variant. The `String`-carrying variants
// (`Sqlite { code: Option<i32>, message: String }`, `UnknownColumn { name }`)
// dominate the small `usize`/`i64`/`Type` field variants and the two-`Box`
// `TransactionRollbackFailed`; the discriminant packs into the `String`'s
// non-null-pointer niche, so the whole error stays three words. A new variant
// carrying a wider payload would widen it and trip this pin.
crate::footprint_pin!(SqliteError, size = 32, align = 8);

impl SqliteError {
    /// Check if this is a constraint violation (SQLITE_CONSTRAINT = 19).
    #[must_use]
    pub fn is_constraint_violation(&self) -> bool {
        matches!(self, Self::Sqlite { code: Some(c), .. } if *c == 19)
    }

    /// Check if this is a busy/locked error (SQLITE_BUSY = 5).
    #[must_use]
    pub fn is_busy(&self) -> bool {
        matches!(self, Self::Sqlite { code: Some(c), .. } if *c == 5)
    }

    /// SQLite error code, if available.
    #[must_use]
    pub fn code(&self) -> Option<i32> {
        match self {
            Self::Sqlite { code, .. } => *code,
            _ => None,
        }
    }
}

impl core::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Open(msg) => write!(f, "sqlite open: {msg}"),
            Self::Query(msg) => write!(f, "sqlite: {msg}"),
            Self::Sqlite { code: Some(c), message } => write!(f, "sqlite error {c}: {message}"),
            Self::Sqlite { code: None, message } => write!(f, "sqlite: {message}"),
            Self::TransactionRollbackFailed { original, rollback } => write!(
                f,
                "transaction failed ({original}) and ROLLBACK also failed ({rollback})",
            ),
            Self::TypeMismatch { column, expected, found } => write!(
                f,
                "column {column}: type mismatch — requested {expected}, but the value is {found}",
            ),
            Self::UnexpectedNull { column } => write!(
                f,
                "column {column}: unexpected NULL (use get_opt for a nullable column)",
            ),
            Self::InvalidUtf8 { column } => {
                write!(f, "column {column}: text is not valid UTF-8")
            }
            Self::IntegerOutOfRange { column, value } => {
                write!(f, "column {column}: integer {value} out of range for the requested type")
            }
            Self::InexactFloat { column, value } => write!(
                f,
                "column {column}: integer {value} is not exactly representable as f64",
            ),
            Self::NotABoolean { column, value } => {
                write!(f, "column {column}: integer {value} is not a boolean (0 or 1)")
            }
            Self::ColumnIndexOutOfBounds { index, count } => {
                write!(f, "column index {index} out of bounds ({count} columns)")
            }
            Self::UnknownColumn { name } => write!(f, "unknown column {name:?}"),
        }
    }
}

impl std::error::Error for SqliteError {}

impl From<rusqlite::Error> for SqliteError {
    fn from(e: rusqlite::Error) -> Self {
        let code = match &e {
            rusqlite::Error::SqliteFailure(err, _) => Some(err.extended_code),
            _ => None,
        };
        Self::Sqlite {
            code,
            message: e.to_string(),
        }
    }
}
