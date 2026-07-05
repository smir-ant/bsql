use core::ops::ControlFlow;
use std::path::Path;

use crate::error::SqliteError;
use crate::value::{typed_get, typed_get_opt, FromColumn, SqliteValue, Type, ValueRef};

/// The materialized result of an eager query: every row owned, decoded from
/// SQLite's native storage classes.
#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    /// The result rows, in server order.
    pub rows: Vec<Row>,
    /// The number of columns each row carries.
    pub column_count: usize,
    /// The column names, in column order.
    pub column_names: Vec<String>,
}

// Footprint pin: two Vecs (3 words each) + a usize column count.
crate::footprint_pin!(QueryResult, size = 56, align = 8);

/// An eagerly-materialized row: one heap allocation for the cell vector, plus
/// one per `TEXT`/`BLOB` cell that owns its bytes.
///
/// Reads are classified: [`Row::get`] returns `Err` on a type mismatch or an
/// unexpected `NULL`, never a silent `None`. For a nullable column use
/// [`Row::get_opt`], which distinguishes a real `NULL` (`Ok(None)`) from a
/// type mismatch (`Err`). Text/blob reads borrow the owned cell's buffer
/// zero-copy (`get::<&str>` / `get::<&[u8]>`); `get::<String>` /
/// `get::<Vec<u8>>` copy.
#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    columns: Vec<SqliteValue>,
}

// Footprint pin: a single `Vec<SqliteValue>` (3 words). A row is one heap
// allocation of owned values; the pin keeps the handle a bare Vec.
crate::footprint_pin!(Row, size = 24, align = 8);

impl Row {
    /// A zero-copy borrowed view of column `col`, or
    /// [`SqliteError::ColumnIndexOutOfBounds`] if `col` is past the row.
    pub fn value_ref(&self, col: usize) -> Result<ValueRef<'_>, SqliteError> {
        match self.columns.get(col) {
            Some(v) => Ok(v.as_ref()),
            None => Err(SqliteError::ColumnIndexOutOfBounds {
                index: col,
                count: self.columns.len(),
            }),
        }
    }

    /// Read column `col` as `T`, classifying any failure. A real `NULL` is
    /// [`SqliteError::UnexpectedNull`] (distinct from a type mismatch); a
    /// wrong storage class is [`SqliteError::TypeMismatch`]. For a nullable
    /// column use [`Row::get_opt`].
    pub fn get<'a, T: FromColumn<'a>>(&'a self, col: usize) -> Result<T, SqliteError> {
        typed_get(col, self.value_ref(col)?)
    }

    /// Read a nullable column `col` as `Option<T>`: a real `NULL` is
    /// `Ok(None)`, a present value of the right type is `Ok(Some(_))`, and a
    /// wrong storage class is `Err` — `NULL` and mismatch are never conflated.
    pub fn get_opt<'a, T: FromColumn<'a>>(
        &'a self,
        col: usize,
    ) -> Result<Option<T>, SqliteError> {
        typed_get_opt(col, self.value_ref(col)?)
    }

    /// Whether column `col` is SQL `NULL`.
    pub fn is_null(&self, col: usize) -> Result<bool, SqliteError> {
        Ok(matches!(self.value_ref(col)?, ValueRef::Null))
    }

    /// The storage class of column `col`.
    pub fn data_type(&self, col: usize) -> Result<Type, SqliteError> {
        Ok(self.value_ref(col)?.data_type())
    }

    /// The number of columns in this row.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Whether the row has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Read the column named `name` as `T`, resolving the name against
    /// `column_names` (the [`QueryResult::column_names`] of the same query). A
    /// name absent from the result is [`SqliteError::UnknownColumn`], never a
    /// silent `None`.
    pub fn get_by_name<'a, T: FromColumn<'a>>(
        &'a self,
        name: &str,
        column_names: &[String],
    ) -> Result<T, SqliteError> {
        match column_names.iter().position(|n| n == name) {
            Some(idx) => self.get(idx),
            None => Err(SqliteError::UnknownColumn { name: name.to_owned() }),
        }
    }
}

/// A borrowed view of a single row on the streaming path, valid only for the
/// duration of the [`Connection::query_each`] callback that receives it.
///
/// `Text`/`Blob` reads (`get::<&str>` / `get::<&[u8]>` / [`BorrowedRow::value_ref`])
/// borrow SQLite's own column buffer with zero copy. The view's lifetime `'r`
/// is bounded by the row step, so nothing it lends can escape the callback —
/// enforced by the `for<'r>` bound on `query_each`.
pub struct BorrowedRow<'r> {
    row: &'r rusqlite::Row<'r>,
}

// Footprint pin: a single shared reference (one word).
crate::footprint_pin!(BorrowedRow<'_>, size = 8, align = 8);

impl core::fmt::Debug for BorrowedRow<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BorrowedRow")
            .field("column_count", &self.column_count())
            .finish()
    }
}

impl<'r> BorrowedRow<'r> {
    /// A zero-copy borrowed view of column `col`, valid for the row step
    /// (`'r`), or [`SqliteError::ColumnIndexOutOfBounds`] if `col` is past the
    /// row.
    pub fn value_ref(&self, col: usize) -> Result<ValueRef<'r>, SqliteError> {
        let count = self.column_count();
        if col >= count {
            return Err(SqliteError::ColumnIndexOutOfBounds { index: col, count });
        }
        Ok(self.row.get_ref(col)?.into())
    }

    /// Read column `col` as `T`, classifying any failure. Borrowed targets
    /// (`&str` / `&[u8]`) borrow SQLite's column buffer zero-copy for `'r`.
    pub fn get<T: FromColumn<'r>>(&self, col: usize) -> Result<T, SqliteError> {
        typed_get(col, self.value_ref(col)?)
    }

    /// Read a nullable column `col` as `Option<T>`: a real `NULL` is
    /// `Ok(None)`, distinct from a type mismatch (`Err`).
    pub fn get_opt<T: FromColumn<'r>>(&self, col: usize) -> Result<Option<T>, SqliteError> {
        typed_get_opt(col, self.value_ref(col)?)
    }

    /// Whether column `col` is SQL `NULL`.
    pub fn is_null(&self, col: usize) -> Result<bool, SqliteError> {
        Ok(matches!(self.value_ref(col)?, ValueRef::Null))
    }

    /// The storage class of column `col`.
    pub fn data_type(&self, col: usize) -> Result<Type, SqliteError> {
        Ok(self.value_ref(col)?.data_type())
    }

    /// The number of columns in this row.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.row.as_ref().column_count()
    }
}

/// An embedded SQLite connection.
pub struct Connection {
    inner: rusqlite::Connection,
}

impl core::fmt::Debug for Connection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    /// Open (or create) a database at `path`, enabling WAL journaling and
    /// foreign-key enforcement.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let inner = rusqlite::Connection::open(path).map_err(|e| SqliteError::Open(e.to_string()))?;
        inner
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Open a private in-memory database with foreign-key enforcement.
    pub fn open_in_memory() -> Result<Self, SqliteError> {
        let inner =
            rusqlite::Connection::open_in_memory().map_err(|e| SqliteError::Open(e.to_string()))?;
        inner
            .execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Execute a statement, returning the number of rows changed.
    pub fn execute(&self, sql: &str) -> Result<usize, SqliteError> {
        Ok(self.inner.execute(sql, [])?)
    }

    /// Execute a parameterized statement, returning the number of rows changed.
    pub fn execute_params(&self, sql: &str, params: &[&str]) -> Result<usize, SqliteError> {
        Ok(self.inner.execute(sql, rusqlite::params_from_iter(params))?)
    }

    /// Run `sql` and eagerly materialize every row.
    pub fn query(&self, sql: &str) -> Result<QueryResult, SqliteError> {
        self.query_collect(sql, [])
    }

    /// Run a parameterized `sql` and eagerly materialize every row.
    pub fn query_params(&self, sql: &str, params: &[&str]) -> Result<QueryResult, SqliteError> {
        self.query_collect(sql, rusqlite::params_from_iter(params))
    }

    /// Shared eager-collect core for [`Self::query`] / [`Self::query_params`].
    fn query_collect(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<QueryResult, SqliteError> {
        let mut stmt = self.inner.prepare(sql)?;
        let col_count = stmt.column_count();
        let column_names: Vec<String> =
            stmt.column_names().iter().map(|s| (*s).to_owned()).collect();
        let mut rows_out = Vec::new();

        let mut rows = stmt.query(params)?;
        while let Some(row) = rows.next()? {
            rows_out.push(read_row(row, col_count)?);
        }

        Ok(QueryResult { rows: rows_out, column_count: col_count, column_names })
    }

    /// Stream a query's rows one at a time through `on_row`, decoding each row
    /// borrowed (zero-copy) with nothing accumulated — constant memory
    /// independent of the row count.
    ///
    /// The callback returns [`ControlFlow`]: `Continue(())` to keep streaming,
    /// `Break(e)` to stop early. The result is `Ok(None)` when every row was
    /// streamed, `Ok(Some(e))` when the callback broke early with `e`, or
    /// `Err` on a SQL / step failure.
    ///
    /// The `for<'r>` bound makes each [`BorrowedRow`] valid only inside the
    /// call: a `&str`/`&[u8]` borrowed from a row cannot be stashed in anything
    /// that outlives the callback (a compile error), so a streamed borrow can
    /// never dangle.
    pub fn query_each<F, E>(&self, sql: &str, on_row: F) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.query_each_collect(sql, [], on_row)
    }

    /// Parameterized peer of [`Self::query_each`].
    pub fn query_each_params<F, E>(
        &self,
        sql: &str,
        params: &[&str],
        on_row: F,
    ) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.query_each_collect(sql, rusqlite::params_from_iter(params), on_row)
    }

    /// Shared streaming core for [`Self::query_each`] / [`Self::query_each_params`].
    fn query_each_collect<F, E>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        mut on_row: F,
    ) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        let mut stmt = self.inner.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        while let Some(row) = rows.next()? {
            if let ControlFlow::Break(e) = on_row(BorrowedRow { row }) {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    /// Run a parameterized query and return exactly its first row, or
    /// [`SqliteError::Query`] if it produced none.
    pub fn query_params_one(&self, sql: &str, params: &[&str]) -> Result<Row, SqliteError> {
        self.query_params(sql, params)?
            .rows
            .into_iter()
            .next()
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_owned()))
    }

    /// Run a parameterized query and return its first row, if any.
    pub fn query_params_opt(&self, sql: &str, params: &[&str]) -> Result<Option<Row>, SqliteError> {
        Ok(self.query_params(sql, params)?.rows.into_iter().next())
    }

    /// Run a query and return exactly its first row, or [`SqliteError::Query`]
    /// if it produced none.
    pub fn query_one(&self, sql: &str) -> Result<Row, SqliteError> {
        self.query(sql)?
            .rows
            .into_iter()
            .next()
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_owned()))
    }

    /// Run a query and return its first row, if any.
    pub fn query_opt(&self, sql: &str) -> Result<Option<Row>, SqliteError> {
        Ok(self.query(sql)?.rows.into_iter().next())
    }

    /// Begin a transaction.
    pub fn begin(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("BEGIN")?;
        Ok(())
    }

    /// Commit the current transaction.
    pub fn commit(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("COMMIT")?;
        Ok(())
    }

    /// Roll back the current transaction.
    pub fn rollback(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("ROLLBACK")?;
        Ok(())
    }

    /// Execute a closure within a transaction. COMMIT on Ok, ROLLBACK on Err.
    /// Tier-1 safety: transaction boundary = closure scope.
    pub fn transaction<R>(
        &self,
        f: impl FnOnce(&Self) -> Result<R, SqliteError>,
    ) -> Result<R, SqliteError> {
        self.inner.execute_batch("BEGIN")?;
        match f(self) {
            Ok(val) => {
                self.inner.execute_batch("COMMIT")?;
                Ok(val)
            }
            Err(e) => match self.inner.execute_batch("ROLLBACK") {
                // ROLLBACK undid the transaction: return the closure's error.
                Ok(()) => Err(e),
                // ROLLBACK also failed: the connection is in an indeterminate
                // transactional state. Preserve both causes rather than
                // silently dropping the rollback failure.
                Err(rb) => Err(SqliteError::TransactionRollbackFailed {
                    original: Box::new(e),
                    rollback: Box::new(SqliteError::from(rb)),
                }),
            },
        }
    }

    /// Execute multiple SQL statements separated by semicolons.
    pub fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        self.inner.execute_batch(sql).map_err(SqliteError::from)
    }

    /// Close the connection, surfacing any final flush error.
    pub fn close(self) -> Result<(), SqliteError> {
        self.inner
            .close()
            .map_err(|(_conn, e)| SqliteError::Query(e.to_string()))
    }
}

/// Materialize one row into owned cells, decoding from SQLite's native storage
/// classes. A `TEXT` cell whose bytes are not valid UTF-8 fails the row with a
/// classified [`SqliteError::InvalidUtf8`] rather than a lossy replacement.
fn read_row(row: &rusqlite::Row<'_>, col_count: usize) -> Result<Row, SqliteError> {
    let mut columns = Vec::with_capacity(col_count);
    for col in 0..col_count {
        let owned = match ValueRef::from(row.get_ref(col)?) {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(n) => SqliteValue::Integer(n),
            ValueRef::Real(f) => SqliteValue::Real(f),
            ValueRef::Text(bytes) => {
                let s = core::str::from_utf8(bytes)
                    .map_err(|_| SqliteError::InvalidUtf8 { column: col })?;
                SqliteValue::Text(s.to_owned())
            }
            ValueRef::Blob(bytes) => SqliteValue::Blob(bytes.to_vec()),
        };
        columns.push(owned);
    }
    Ok(Row { columns })
}
