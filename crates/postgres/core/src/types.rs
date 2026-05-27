use std::num::NonZeroU32;
use std::sync::Arc;

// ─── Column slot ────────────────────────────────────────────

/// Per-column metadata. 8 bytes, niche-packed.
/// NULL = `len_plus_one: None` (compiler-enforced handling).
#[derive(Debug, Clone, Copy)]
struct ColSlot {
    offset: u32,
    /// `None` = SQL NULL. `Some(n)` where `n.get() - 1` = byte length.
    /// Empty string '' = Some(1). This encoding fits NULL into the
    /// Option niche — zero extra bytes vs the old u32::MAX sentinel.
    len_plus_one: Option<NonZeroU32>,
}

impl ColSlot {
    fn null() -> Self {
        Self { offset: 0, len_plus_one: None }
    }

    fn value(offset: u32, len: u32) -> Self {
        Self {
            offset,
            len_plus_one: NonZeroU32::new(len.saturating_add(1)),
        }
    }

    fn byte_len(&self) -> Option<u32> {
        Some(self.len_plus_one?.get().saturating_sub(1))
    }
}

// Compile-time size pin: ColSlot must be exactly 8 bytes (niche-packed).
const _: () = assert!(core::mem::size_of::<ColSlot>() == 8);

// ─── Arena ──────────────────────────────────────────────────

/// Shared backing store for all rows in a query result.
/// 1 Arc + 3 Vecs = 4 heap allocations total, regardless of row count.
#[derive(Debug)]
struct ArenaInner {
    data: Vec<u8>,
    slots: Vec<ColSlot>,
    n_cols: u16,
    _n_rows: u32,
}

// ─── Row ────────────────────────────────────────────────────

/// A single result row. 16 bytes: Arc pointer + row index.
/// `'static + Clone + Send + Sync`. Clone = Arc refcount bump.
#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    arena: Arc<ArenaInner>,
    row_idx: u32,
}

impl Row {
    pub fn get_raw(&self, col: usize) -> Option<&[u8]> {
        let inner = &*self.arena;
        let n = usize::from(inner.n_cols);
        if col >= n { return None; }
        let base = (self.row_idx as usize).checked_mul(n)?;
        let slot = inner.slots.get(base.checked_add(col)?)?;
        let len = slot.byte_len()? as usize;
        let start = slot.offset as usize;
        inner.data.get(start..start.checked_add(len)?)
    }

    pub fn get_str(&self, col: usize) -> Option<&str> {
        core::str::from_utf8(self.get_raw(col)?).ok()
    }

    pub fn get_i32(&self, col: usize) -> Option<i32> { self.get_str(col)?.parse().ok() }
    pub fn get_i64(&self, col: usize) -> Option<i64> { self.get_str(col)?.parse().ok() }
    pub fn get_f64(&self, col: usize) -> Option<f64> { self.get_str(col)?.parse().ok() }

    pub fn get_bool(&self, col: usize) -> Option<bool> {
        match self.get_str(col)? { "t" => Some(true), "f" => Some(false), _ => None }
    }

    pub fn is_null(&self, col: usize) -> bool {
        let inner = &*self.arena;
        let n = usize::from(inner.n_cols);
        if col >= n { return true; }
        let base = (self.row_idx as usize) * n;
        inner.slots.get(base + col)
            .is_none_or(|s| s.len_plus_one.is_none())
    }

    pub fn len(&self) -> usize { usize::from(self.arena.n_cols) }
    pub fn is_empty(&self) -> bool { self.arena.n_cols == 0 }

    pub fn get_by_name<'a>(&'a self, name: &str, column_names: &[String]) -> Option<&'a [u8]> {
        let idx = column_names.iter().position(|n| n == name)?;
        self.get_raw(idx)
    }

    pub fn get<T: FromText>(&self, col: usize) -> Option<T> {
        T::from_text(self.get_str(col)?)
    }
}

// ─── ArenaBuilder ───────────────────────────────────────────

/// Builds the shared arena during streaming. One builder per query.
/// finish() seals and produces the Arc-shared arena + Row handles.
pub struct ArenaBuilder {
    data: Vec<u8>,
    slots: Vec<ColSlot>,
    n_cols: u16,
    rows_finished: u32,
}

impl ArenaBuilder {
    pub fn new(n_cols: usize) -> Self {
        Self {
            data: Vec::new(),
            slots: Vec::new(),
            n_cols: n_cols as u16,
            rows_finished: 0,
        }
    }

    pub fn push_value(&mut self, bytes: &[u8]) {
        let offset = self.data.len() as u32;
        let len = bytes.len() as u32;
        self.data.extend_from_slice(bytes);
        self.slots.push(ColSlot::value(offset, len));
    }

    pub fn push_null(&mut self) {
        self.slots.push(ColSlot::null());
    }

    /// Extend the last pushed column's data (for chunked columns).
    pub fn extend_last(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
        if let Some(slot) = self.slots.last_mut()
            && let Some(old_len) = slot.byte_len()
        {
            *slot = ColSlot::value(slot.offset, old_len + bytes.len() as u32);
        }
    }

    pub fn end_row(&mut self) {
        self.rows_finished += 1;
    }

    /// Seal the arena and produce Row handles.
    pub fn finish(self) -> Vec<Row> {
        let n_rows = self.rows_finished;
        let arena = Arc::new(ArenaInner {
            data: self.data,
            slots: self.slots,
            n_cols: self.n_cols,
            _n_rows: n_rows,
        });
        (0..n_rows)
            .map(|i| Row { arena: arena.clone(), row_idx: i })
            .collect()
    }
}

// ─── QueryResult ────────────────────────────────────────────

/// Result of a query — rows + command tag + column count.
#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub command_tag: String,
    pub column_count: usize,
    pub column_names: Arc<[String]>,
}

// ─── FromText ───────────────────────────────────────────────

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

// ─── PreparedStatement ──────────────────────────────────────

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

// ─── Notification ───────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
    pub pid: i32,
}
