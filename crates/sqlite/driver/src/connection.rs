use std::path::Path;

use rusqlite::types::Value;

use crate::error::SqliteError;

/// Trait for converting text-format values to Rust types.
pub trait FromText: Sized {
    fn from_text(s: &str) -> Option<Self>;
}

impl FromText for i32 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for i64 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for f64 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for bool {
    fn from_text(s: &str) -> Option<Self> {
        match s { "1" | "true" | "TRUE" => Some(true), "0" | "false" | "FALSE" => Some(false), _ => None }
    }
}
impl FromText for String { fn from_text(s: &str) -> Option<Self> { Some(s.to_string()) } }

#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub column_count: usize,
    pub column_names: Vec<String>,
}

// Footprint pin: two Vecs (3 words each) + a usize column count.
crate::footprint_pin!(QueryResult, size = 56, align = 8);

/// Native SQLite value — no double-conversion.
#[derive(Debug, Clone)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

// Footprint pin: sized by the widest variant — Text(String) / Blob(Vec<u8>),
// each 3 words — plus the discriminant. A new variant carrying a wider payload
// would widen every cell; the pin catches it.
crate::footprint_pin!(SqliteValue, size = 32, align = 8);

#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    columns: Vec<SqliteValue>,
}

// Footprint pin: a single Vec<SqliteValue> (3 words). A row is one heap
// allocation of native values; the pin keeps the handle a bare Vec.
crate::footprint_pin!(Row, size = 24, align = 8);

impl Row {
    pub fn get_str(&self, idx: usize) -> Option<&str> {
        match self.columns.get(idx)? {
            SqliteValue::Text(s) => Some(s.as_str()),
            SqliteValue::Null => None,
            _ => None,
        }
    }

    /// Get text representation of any value type.
    pub fn get_text(&self, idx: usize) -> Option<String> {
        match self.columns.get(idx)? {
            SqliteValue::Text(s) => Some(s.clone()),
            SqliteValue::Integer(n) => Some(n.to_string()),
            SqliteValue::Real(f) => Some(f.to_string()),
            SqliteValue::Blob(_) => None,
            SqliteValue::Null => None,
        }
    }

    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        match self.columns.get(idx)? {
            SqliteValue::Integer(n) => i32::try_from(*n).ok(),
            SqliteValue::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        match self.columns.get(idx)? {
            SqliteValue::Integer(n) => Some(*n),
            SqliteValue::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        match self.columns.get(idx)? {
            SqliteValue::Real(f) => Some(*f),
            // Convert an integer to f64 only when it is exactly representable.
            // f64 has a 53-bit mantissa, so every integer in the closed range
            // [-(2^53), 2^53] round-trips through f64 with no loss. The bound
            // check below proves the value lies in that range; the `as f64`
            // cast is therefore exact and lossless on that domain (not a
            // silent truncation). A larger integer is not returned as a
            // rounded approximation — `None` is the honest "not cleanly
            // convertible, read it as an integer instead" signal.
            SqliteValue::Integer(n) if (-(1i64 << 53)..=(1i64 << 53)).contains(n) => {
                // `*n` is proven within [-(2^53), 2^53]; `as f64` is exact here.
                Some(*n as f64)
            }
            SqliteValue::Integer(_) => None,
            SqliteValue::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self.columns.get(idx)? {
            SqliteValue::Integer(0) => Some(false),
            SqliteValue::Integer(1) => Some(true),
            SqliteValue::Text(s) => match s.as_str() {
                "1" | "true" | "TRUE" => Some(true),
                "0" | "false" | "FALSE" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn get_raw(&self, idx: usize) -> Option<&[u8]> {
        match self.columns.get(idx)? {
            SqliteValue::Text(s) => Some(s.as_bytes()),
            SqliteValue::Blob(b) => Some(b.as_slice()),
            SqliteValue::Integer(_) => None,
            _ => None,
        }
    }

    pub fn is_null(&self, idx: usize) -> bool {
        matches!(self.columns.get(idx), Some(SqliteValue::Null) | None)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn get<T: FromText>(&self, idx: usize) -> Option<T> {
        let text = self.get_text(idx)?;
        T::from_text(&text)
    }

    pub fn get_by_name<'a>(&'a self, name: &str, column_names: &[String]) -> Option<&'a [u8]> {
        let idx = column_names.iter().position(|n| n == name)?;
        self.get_raw(idx)
    }
}

pub struct Connection {
    inner: rusqlite::Connection,
}

impl Connection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let inner = rusqlite::Connection::open(path)
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        inner.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn open_in_memory() -> Result<Self, SqliteError> {
        let inner = rusqlite::Connection::open_in_memory()
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        inner.execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn execute(&self, sql: &str) -> Result<usize, SqliteError> {
        Ok(self.inner.execute(sql, [])?)
    }

    pub fn execute_params(&self, sql: &str, params: &[&str]) -> Result<usize, SqliteError> {
        let boxed: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::types::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::types::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
        Ok(self.inner.execute(sql, refs.as_slice())?)
    }

    pub fn query(&self, sql: &str) -> Result<QueryResult, SqliteError> {
        let mut stmt = self.inner.prepare(sql)?;
        let col_count = stmt.column_count();
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows_out = Vec::new();

        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            rows_out.push(read_row(row, col_count)?);
        }

        Ok(QueryResult { rows: rows_out, column_count: col_count, column_names })
    }

    pub fn query_params(&self, sql: &str, params: &[&str]) -> Result<QueryResult, SqliteError> {
        let boxed: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::types::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::types::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();

        let mut stmt = self.inner.prepare(sql)?;
        let col_count = stmt.column_count();
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows_out = Vec::new();

        let mut rows = stmt.query(refs.as_slice())?;
        while let Some(row) = rows.next()? {
            rows_out.push(read_row(row, col_count)?);
        }

        Ok(QueryResult { rows: rows_out, column_count: col_count, column_names })
    }

    pub fn query_params_one(&self, sql: &str, params: &[&str]) -> Result<Row, SqliteError> {
        let result = self.query_params(sql, params)?;
        result.rows.into_iter().next()
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_string()))
    }

    pub fn query_params_opt(&self, sql: &str, params: &[&str]) -> Result<Option<Row>, SqliteError> {
        let result = self.query_params(sql, params)?;
        Ok(result.rows.into_iter().next())
    }

    pub fn query_one(&self, sql: &str) -> Result<Row, SqliteError> {
        let result = self.query(sql)?;
        result.rows.into_iter().next()
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_string()))
    }

    pub fn query_opt(&self, sql: &str) -> Result<Option<Row>, SqliteError> {
        let result = self.query(sql)?;
        Ok(result.rows.into_iter().next())
    }

    pub fn begin(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("BEGIN")?;
        Ok(())
    }

    pub fn commit(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("COMMIT")?;
        Ok(())
    }

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
            Ok(val) => { self.inner.execute_batch("COMMIT")?; Ok(val) }
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

    pub fn close(self) -> Result<(), SqliteError> {
        self.inner.close().map_err(|(_conn, e)| SqliteError::Query(e.to_string()))
    }
}

fn read_row(row: &rusqlite::Row<'_>, col_count: usize) -> Result<Row, SqliteError> {
    let mut columns = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let val: Value = row.get(i)?;
        columns.push(match val {
            Value::Null => SqliteValue::Null,
            Value::Integer(n) => SqliteValue::Integer(n),
            Value::Real(f) => SqliteValue::Real(f),
            Value::Text(s) => SqliteValue::Text(s),
            Value::Blob(b) => SqliteValue::Blob(b),
        });
    }
    Ok(Row { columns })
}

#[cfg(test)]
mod tests {
    use super::{Row, SqliteValue};

    fn int_row(n: i64) -> Row {
        Row { columns: vec![SqliteValue::Integer(n)] }
    }

    #[test]
    fn get_f64_accepts_two_pow_53_exactly() {
        // 2^53 is the largest magnitude that round-trips through f64's 53-bit
        // mantissa exactly; it must convert (not be rejected as out of range).
        let two_pow_53: i64 = 9_007_199_254_740_992;
        let row = int_row(two_pow_53);
        match row.get_f64(0) {
            Some(v) => {
                assert_eq!(v, two_pow_53 as f64);
                // Round-trips back to the exact integer with no loss.
                assert_eq!(v as i64, two_pow_53);
            }
            None => panic!("2^53 must convert exactly to f64"),
        }
    }

    #[test]
    fn get_f64_accepts_large_in_range_integers() {
        // These are all within [-(2^53), 2^53] and exactly representable, yet
        // the old i32-only guard wrongly rejected them.
        for n in [4_000_000_000_i64, 1_000_000_000_000, -1_000_000_000_000] {
            let row = int_row(n);
            match row.get_f64(0) {
                Some(v) => assert_eq!(v, n as f64),
                None => panic!("in-range integer {n} must convert to f64"),
            }
        }
    }

    #[test]
    fn get_f64_rejects_above_two_pow_53() {
        // 2^53 + 1 is the first integer that f64 cannot represent exactly;
        // returning a rounded value would be a silent loss, so expect None.
        let above: i64 = 9_007_199_254_740_993;
        assert!(int_row(above).get_f64(0).is_none(), "2^53+1 is not exact in f64");
    }
}
