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
    /// An eager [`query_sql`](crate::Connection::query_sql) result's text/blob
    /// bytes (or its column count) exceeded the 32-bit bounds of the shared
    /// arena's slot fields — a `> 4 GiB` eager materialization. Rejected loudly
    /// rather than returned with mis-addressed cells; stream the result with
    /// [`query_each_sql`](crate::Connection::query_each_sql) (constant memory, no
    /// cap) instead.
    ResultTooLarge,
    /// A TYPED at-most-one verb ([`query_one`](crate::Connection::query_one) /
    /// [`query_opt`](crate::Connection::query_opt)) received more than one row.
    /// The typed flagship's `query_one` / `query_opt` are exactly-one /
    /// at-most-one — the SAME contract the PostgreSQL typed verbs enforce, so a
    /// query ported PostgreSQL→SQLite keeps its multi-row semantics. (The dynamic
    /// [`query_one_sql`](crate::Connection::query_one_sql) /
    /// [`query_opt_sql`](crate::Connection::query_opt_sql) stay first-row.)
    TooManyRows,
    /// An in-flight query was INTERRUPTED by a
    /// [`SqliteCancelToken`](crate::SqliteCancelToken) (`sqlite3_interrupt`) from
    /// another thread — the SQLite cross-backend twin of the PostgreSQL cancel.
    /// A classified variant (not a bare `Sqlite { code }`), so a caller can match
    /// the cancel it requested; the connection is REUSABLE afterward (the
    /// interrupt aborts the statement, not the connection).
    Interrupted,
}

// Footprint pin: sized by the widest variant. The `String`-carrying variants
// (`Sqlite { code: Option<i32>, message: String }`, `UnknownColumn { name }`)
// dominate the small `usize`/`i64`/`Type` field variants and the two-`Box`
// `TransactionRollbackFailed`; the discriminant packs into the `String`'s
// non-null-pointer niche, so the whole error stays three words. A new variant
// carrying a wider payload would widen it and trip this pin.
crate::footprint_pin!(SqliteError, size = 32, align = 8);

/// Low 8 bits of a SQLite extended result code — its PRIMARY code.
///
/// SQLite reports errors as EXTENDED codes of the form `primary | (sub << 8)`
/// (e.g. `SQLITE_CONSTRAINT_UNIQUE = 2067` extends `SQLITE_CONSTRAINT = 19`,
/// `SQLITE_BUSY_SNAPSHOT = 517` extends `SQLITE_BUSY = 5`). The primary code
/// is the low byte; masking recovers the class from any subtype.
const PRIMARY_CODE_MASK: i32 = 0xFF;

/// Primary `SQLITE_BUSY`.
const PRIMARY_BUSY: i32 = 5;

/// Primary `SQLITE_CONSTRAINT`.
const PRIMARY_CONSTRAINT: i32 = 19;

impl SqliteError {
    /// The PRIMARY SQLite result code carried by a [`SqliteError::Sqlite`], if
    /// any. [`From<rusqlite::Error>`] stores the full EXTENDED code; this masks
    /// it to the low-byte primary so a class predicate matches every subtype.
    fn primary_code(&self) -> Option<i32> {
        match self {
            Self::Sqlite { code: Some(c), .. } => Some(*c & PRIMARY_CODE_MASK),
            _ => None,
        }
    }

    /// Check if this is a constraint violation (primary `SQLITE_CONSTRAINT`).
    ///
    /// Matches on the PRIMARY code, so every specific constraint the engine
    /// reports as an extended code — `UNIQUE` (2067), `NOT NULL` (1299),
    /// `FOREIGN KEY` (787), `PRIMARY KEY` (1555), `CHECK` (275), … — is
    /// recognised, not just a bare `19`.
    #[must_use]
    pub fn is_constraint_violation(&self) -> bool {
        self.primary_code() == Some(PRIMARY_CONSTRAINT)
    }

    /// Check if this is a busy/locked error (primary `SQLITE_BUSY`).
    ///
    /// Matches on the PRIMARY code, so `SQLITE_BUSY_SNAPSHOT` (517) — the code
    /// the driver's default WAL journaling yields on a write conflict — counts
    /// as busy, not just a bare `5`.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.primary_code() == Some(PRIMARY_BUSY)
    }

    /// SQLite EXTENDED error code, if available (the full `primary | sub << 8`;
    /// the specific subtype is preserved here — only the boolean class
    /// predicates mask to the primary code).
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
            Self::ResultTooLarge => write!(
                f,
                "eager result exceeds the 4 GiB arena bound — stream it with query_each_sql instead",
            ),
            Self::TooManyRows => write!(
                f,
                "typed query_one/query_opt expected at most one row, but the query returned more \
                 than one",
            ),
            Self::Interrupted => write!(f, "sqlite: query interrupted (canceled)"),
        }
    }
}

impl std::error::Error for SqliteError {}

impl From<rusqlite::Error> for SqliteError {
    fn from(e: rusqlite::Error) -> Self {
        // A `sqlite3_interrupt`-aborted statement is classified into its OWN
        // variant so a caller can match the cancel it requested — never folded
        // into an opaque `Sqlite { code: 9 }`. Every other engine failure keeps
        // its preserved extended code.
        if let rusqlite::Error::SqliteFailure(err, _) = &e
            && err.code == rusqlite::ErrorCode::OperationInterrupted
        {
            return Self::Interrupted;
        }
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
