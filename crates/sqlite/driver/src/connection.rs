use std::path::Path;

use rusqlite::types::Value;

use crate::error::SqliteError;

#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub column_count: usize,
    pub column_names: Vec<String>,
}

#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    columns: Vec<Option<Vec<u8>>>,
}

impl Row {
    pub fn get_str(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx)?.as_deref().and_then(|b| core::str::from_utf8(b).ok())
    }

    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_str(idx)?.parse().ok()
    }

    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_str(idx)?.parse().ok()
    }

    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_str(idx)?.parse().ok()
    }

    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self.get_str(idx)? {
            "1" | "true" | "TRUE" => Some(true),
            "0" | "false" | "FALSE" => Some(false),
            _ => None,
        }
    }

    pub fn get_raw(&self, idx: usize) -> Option<&[u8]> {
        self.columns.get(idx)?.as_deref()
    }

    pub fn is_null(&self, idx: usize) -> bool {
        matches!(self.columns.get(idx), Some(None))
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
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

    pub fn close(self) -> Result<(), SqliteError> {
        self.inner.close().map_err(|(_conn, e)| SqliteError::Query(e.to_string()))
    }
}

fn read_row(row: &rusqlite::Row<'_>, col_count: usize) -> Result<Row, SqliteError> {
    let mut columns = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let val: Value = row.get(i)?;
        columns.push(value_to_bytes(val));
    }
    Ok(Row { columns })
}

fn value_to_bytes(val: Value) -> Option<Vec<u8>> {
    match val {
        Value::Null => None,
        Value::Integer(n) => Some(n.to_string().into_bytes()),
        Value::Real(f) => Some(f.to_string().into_bytes()),
        Value::Text(s) => Some(s.into_bytes()),
        Value::Blob(b) => Some(b),
    }
}
