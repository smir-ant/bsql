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

    /// Build a value slot from an offset and a pre-encoded `len + 1`.
    /// `len_plus_one` is constructed by [`ArenaBuilder`] after a checked
    /// `len + 1` that already rejected `len == u32::MAX`, so this constructor
    /// never sees a saturated or wrapped value — the bad state is unrepresentable
    /// here by the time it is reached.
    fn value(offset: u32, len_plus_one: NonZeroU32) -> Self {
        Self { offset, len_plus_one: Some(len_plus_one) }
    }

    fn byte_len(&self) -> Option<u32> {
        // `len_plus_one` is always a checked `len + 1 >= 1`, so `- 1` cannot
        // underflow. The `?` propagates SQL NULL (no value), not an error.
        self.len_plus_one?.get().checked_sub(1)
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

// Footprint pin: Row is one Arc pointer + a u32 row index, niche-free.
// Keeping it pointer-sized + a word is what makes Clone an Arc refcount bump
// and lets a row cross threads as a 16-byte handle.
crate::footprint_pin!(Row, size = 16, align = 8);

impl Row {
    pub fn get_raw(&self, col: usize) -> Option<&[u8]> {
        let inner = &*self.arena;
        let n = usize::from(inner.n_cols);
        if col >= n { return None; }
        let base = usize::try_from(self.row_idx).ok()?.checked_mul(n)?;
        let slot = inner.slots.get(base.checked_add(col)?)?;
        let len = usize::try_from(slot.byte_len()?).ok()?;
        let start = usize::try_from(slot.offset).ok()?;
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
        let Ok(row_idx) = usize::try_from(self.row_idx) else { return true; };
        let base = row_idx * n;
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

// ─── RowTooLarge ────────────────────────────────────────────

/// A result row (or the whole arena) could not be represented within the
/// 32-bit on-arena fields: more columns than `u16`, or a cell offset/length
/// that overflows `u32`. Construction fails loudly instead of saturating to a
/// sentinel that would silently mis-address subsequent cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowTooLarge;

impl core::fmt::Display for RowTooLarge {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("result row too large to encode (exceeds 32-bit arena bounds)")
    }
}

impl std::error::Error for RowTooLarge {}

// Footprint pin: a zero-size error marker — it carries no data, only its type
// identity. A field accidentally added here would make every fallible row-build
// `Result` wider; the ZST pin catches that.
crate::footprint_pin!(RowTooLarge, size = 0, align = 1);

// ─── ArenaBuilder ───────────────────────────────────────────

/// Builds the shared arena during streaming. One builder per query.
/// `finish()` seals and produces the Arc-shared arena + Row handles.
///
/// Any cell offset/length or column count that would overflow the 32-bit
/// arena fields sets a sticky overflow flag rather than saturating; `finish()`
/// converts that flag into [`RowTooLarge`] so a too-large result fails loudly
/// instead of returning silently corrupted rows.
pub struct ArenaBuilder {
    data: Vec<u8>,
    slots: Vec<ColSlot>,
    n_cols: u16,
    rows_finished: u32,
    /// Set when a bound was exceeded; sealed into a `RowTooLarge` by `finish()`.
    overflow: bool,
}

impl ArenaBuilder {
    /// Create a builder for a row shape with `n_cols` columns. A column count
    /// that does not fit `u16` marks the builder overflowed; `finish()` then
    /// fails loudly instead of mis-indexing rows against a truncated count.
    pub fn new(n_cols: usize) -> Self {
        let (n, overflow) = match u16::try_from(n_cols) {
            Ok(n) => (n, false),
            Err(_) => (0, true),
        };
        Self {
            data: Vec::new(),
            slots: Vec::new(),
            n_cols: n,
            rows_finished: 0,
            overflow,
        }
    }

    pub fn push_value(&mut self, bytes: &[u8]) {
        // Offset and `len + 1` must both fit u32 (a NULL is encoded by the
        // niche, so a real value needs `len + 1 >= 1`). On overflow, record a
        // NULL placeholder and mark the builder so `finish()` fails loudly —
        // never a saturated offset/length that would mis-address bytes.
        let Ok(offset) = u32::try_from(self.data.len()) else {
            self.overflow = true;
            self.slots.push(ColSlot::null());
            return;
        };
        let len_plus_one = u32::try_from(bytes.len())
            .ok()
            .and_then(|len| len.checked_add(1))
            .and_then(NonZeroU32::new);
        match len_plus_one {
            Some(lp1) => {
                self.data.extend_from_slice(bytes);
                self.slots.push(ColSlot::value(offset, lp1));
            }
            None => {
                self.overflow = true;
                self.slots.push(ColSlot::null());
            }
        }
    }

    pub fn push_null(&mut self) {
        self.slots.push(ColSlot::null());
    }

    /// Extend the last pushed column's data (for chunked columns).
    pub fn extend_last(&mut self, bytes: &[u8]) {
        if let Some(slot) = self.slots.last_mut()
            && let Some(old_len) = slot.byte_len()
        {
            // New total `old_len + extra`, then `+ 1` for the niche encoding,
            // all checked. On overflow mark the builder; do not saturate.
            let new_lp1 = u32::try_from(bytes.len())
                .ok()
                .and_then(|extra| old_len.checked_add(extra))
                .and_then(|total| total.checked_add(1))
                .and_then(NonZeroU32::new);
            match new_lp1 {
                Some(lp1) => {
                    self.data.extend_from_slice(bytes);
                    let offset = slot.offset;
                    *slot = ColSlot::value(offset, lp1);
                }
                None => self.overflow = true,
            }
        }
    }

    pub fn end_row(&mut self) {
        self.rows_finished += 1;
    }

    /// Seal the arena and produce Row handles. Fails with [`RowTooLarge`] if
    /// any column count, offset, or length overflowed the 32-bit fields.
    pub fn finish(self) -> Result<Vec<Row>, RowTooLarge> {
        if self.overflow {
            return Err(RowTooLarge);
        }
        let n_rows = self.rows_finished;
        let arena = Arc::new(ArenaInner {
            data: self.data,
            slots: self.slots,
            n_cols: self.n_cols,
            _n_rows: n_rows,
        });
        Ok((0..n_rows)
            .map(|i| Row { arena: arena.clone(), row_idx: i })
            .collect())
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

// Footprint pin: a Vec (3 words) + a String (3 words) + a usize + an Arc<[_]>
// (2 words, fat pointer). A new field, or swapping a field to a wider owned
// type, shows up here.
crate::footprint_pin!(QueryResult, size = 72, align = 8);

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

// Footprint pin: dominated by the inline StmtName (a fixed 63-byte bounded
// string + length) plus Option<RowDesc> and an Arc<[String]>. The inline name
// is what avoids a heap allocation per prepared statement; if that bounded
// capacity changed, this pin would move.
crate::footprint_pin!(PreparedStatement, size = 104, align = 8);

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

// Footprint pin: two owned Strings (3 words each) + an i32 backend PID. The
// strings are owned so a Notification outlives the read buffer it was decoded
// from; the pin documents that owned shape.
crate::footprint_pin!(Notification, size = 56, align = 8);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_builder_round_trips_values_and_nulls() {
        let mut ab = ArenaBuilder::new(2);
        ab.push_value(b"hi");
        ab.push_null();
        ab.end_row();
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected overflow: {e}"),
        };
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get_raw(0), Some(&b"hi"[..]));
        assert!(row.is_null(1));
    }

    #[test]
    fn arena_builder_extend_last_concatenates() {
        let mut ab = ArenaBuilder::new(1);
        ab.push_value(b"foo");
        ab.extend_last(b"bar");
        ab.end_row();
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected overflow: {e}"),
        };
        assert_eq!(rows[0].get_raw(0), Some(&b"foobar"[..]));
    }

    #[test]
    fn arena_builder_rejects_too_many_columns() {
        // A column count beyond u16 cannot be addressed by the slot index; the
        // builder must fail loud at finish(), never saturate and mis-index.
        let ab = ArenaBuilder::new(usize::from(u16::MAX) + 1);
        assert_eq!(ab.finish().map(|_| ()), Err(RowTooLarge));
    }
}
