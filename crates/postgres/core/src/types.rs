use std::sync::Arc;

/// Result of a query — rows + command tag + column count.
#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub command_tag: String,
    pub column_count: usize,
    pub column_names: Arc<[String]>,
}

/// A single result row. Column values are raw bytes decoded on access.
#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    columns: Vec<Option<Vec<u8>>>,
}

impl Row {
    pub fn from_columns(columns: Vec<Option<Vec<u8>>>) -> Self {
        Self { columns }
    }

    pub fn get_str(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx)?.as_deref().and_then(|b| core::str::from_utf8(b).ok())
    }

    pub fn get_i32(&self, idx: usize) -> Option<i32> { self.get_str(idx)?.parse().ok() }
    pub fn get_i64(&self, idx: usize) -> Option<i64> { self.get_str(idx)?.parse().ok() }
    pub fn get_f64(&self, idx: usize) -> Option<f64> { self.get_str(idx)?.parse().ok() }

    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self.get_str(idx)? { "t" => Some(true), "f" => Some(false), _ => None }
    }

    pub fn get_raw(&self, idx: usize) -> Option<&[u8]> {
        self.columns.get(idx)?.as_deref()
    }

    pub fn is_null(&self, idx: usize) -> bool {
        matches!(self.columns.get(idx), Some(None))
    }

    pub fn len(&self) -> usize { self.columns.len() }
    pub fn is_empty(&self) -> bool { self.columns.is_empty() }

    pub fn get_by_name<'a>(&'a self, name: &str, column_names: &[String]) -> Option<&'a [u8]> {
        let idx = column_names.iter().position(|n| n == name)?;
        self.get_raw(idx)
    }

    pub fn get<T: FromText>(&self, idx: usize) -> Option<T> {
        T::from_text(self.get_str(idx)?)
    }
}

/// Trait for converting PG text-format values to Rust types.
pub trait FromText: Sized {
    fn from_text(s: &str) -> Option<Self>;
}

impl FromText for i16 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for i32 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for i64 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for f32 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for f64 { fn from_text(s: &str) -> Option<Self> { s.parse().ok() } }
impl FromText for bool {
    fn from_text(s: &str) -> Option<Self> {
        match s { "t" => Some(true), "f" => Some(false), _ => None }
    }
}
impl FromText for String {
    fn from_text(s: &str) -> Option<Self> { Some(s.to_string()) }
}

/// Handle to a server-side prepared statement.
#[derive(Debug)]
pub struct PreparedStatement {
    pub(crate) stmt_name: bsql_postgres_proto::StmtName,
    pub(crate) row_desc: Option<bsql_postgres_proto::decode::RowDesc>,
    pub(crate) column_names: Arc<[String]>,
}

impl PreparedStatement {
    pub fn returns_rows(&self) -> bool { self.row_desc.is_some() }
    pub fn column_names(&self) -> &[String] { &self.column_names }
}

/// An async notification received via LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
    pub pid: i32,
}
