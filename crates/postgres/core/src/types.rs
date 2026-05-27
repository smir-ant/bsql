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

/// Column range within a Row's data buffer. NULL = offset u32::MAX.
#[derive(Debug, Clone, Copy)]
struct ColRange {
    offset: u32,
    len: u32,
}

impl ColRange {
    const NULL: Self = Self { offset: u32::MAX, len: 0 };

    fn is_null(self) -> bool { self.offset == u32::MAX }
}

/// A single result row. Column values stored contiguously in one buffer.
/// 2 allocations per row (data + offsets) instead of N+1.
#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    data: Vec<u8>,
    cols: Vec<ColRange>,
}

impl Row {
    /// Build a Row from the old Vec<Option<Vec<u8>>> format (migration helper).
    pub fn from_columns(columns: Vec<Option<Vec<u8>>>) -> Self {
        let mut data = Vec::new();
        let mut cols = Vec::with_capacity(columns.len());
        for col in &columns {
            match col {
                Some(bytes) => {
                    let offset = data.len() as u32;
                    let len = bytes.len() as u32;
                    data.extend_from_slice(bytes);
                    cols.push(ColRange { offset, len });
                }
                None => cols.push(ColRange::NULL),
            }
        }
        Self { data, cols }
    }

    /// Build a Row incrementally during streaming.
    pub fn builder(n_cols: usize) -> RowBuilder {
        RowBuilder {
            data: Vec::new(),
            cols: Vec::with_capacity(n_cols),
        }
    }

    pub fn get_str(&self, idx: usize) -> Option<&str> {
        let raw = self.get_raw(idx)?;
        core::str::from_utf8(raw).ok()
    }

    pub fn get_i32(&self, idx: usize) -> Option<i32> { self.get_str(idx)?.parse().ok() }
    pub fn get_i64(&self, idx: usize) -> Option<i64> { self.get_str(idx)?.parse().ok() }
    pub fn get_f64(&self, idx: usize) -> Option<f64> { self.get_str(idx)?.parse().ok() }

    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self.get_str(idx)? { "t" => Some(true), "f" => Some(false), _ => None }
    }

    pub fn get_raw(&self, idx: usize) -> Option<&[u8]> {
        let cr = self.cols.get(idx)?;
        if cr.is_null() { return None; }
        self.data.get(cr.offset as usize..(cr.offset + cr.len) as usize)
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.cols.get(idx).map_or(true, |cr| cr.is_null())
    }

    pub fn len(&self) -> usize { self.cols.len() }
    pub fn is_empty(&self) -> bool { self.cols.is_empty() }

    pub fn get_by_name<'a>(&'a self, name: &str, column_names: &[String]) -> Option<&'a [u8]> {
        let idx = column_names.iter().position(|n| n == name)?;
        self.get_raw(idx)
    }

    pub fn get<T: FromText>(&self, idx: usize) -> Option<T> {
        T::from_text(self.get_str(idx)?)
    }
}

/// Incremental row builder for streaming construction.
pub struct RowBuilder {
    data: Vec<u8>,
    cols: Vec<ColRange>,
}

impl RowBuilder {
    pub fn push_value(&mut self, bytes: &[u8]) {
        let offset = self.data.len() as u32;
        let len = bytes.len() as u32;
        self.data.extend_from_slice(bytes);
        self.cols.push(ColRange { offset, len });
    }

    pub fn push_null(&mut self) {
        self.cols.push(ColRange::NULL);
    }

    pub fn extend_last(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
        if let Some(last) = self.cols.last_mut() {
            last.len += bytes.len() as u32;
        }
    }

    pub fn finish(self) -> Row {
        Row { data: self.data, cols: self.cols }
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.cols.clear();
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
    pub stmt_name: bsql_postgres_proto::StmtName,
    pub row_desc: Option<bsql_postgres_proto::decode::RowDesc>,
    pub column_names: Arc<[String]>,
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
