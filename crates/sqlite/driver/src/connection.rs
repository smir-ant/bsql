use core::marker::PhantomData;
use core::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::error::SqliteError;
use crate::typed::{ColumnSource, SqliteTypedQuery};
use crate::value::{typed_get, typed_get_opt, FromColumn, Type, ValueRef};

/// Default busy timeout applied by [`Connection::open`] / [`open_in_memory`]: a
/// locked-database operation waits up to this long for the lock before returning
/// a CLASSIFIED busy error (`is_busy()`), never a hang. Override per connection
/// with [`Connection::set_busy_timeout`] (`Duration::ZERO` = fail immediately).
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

// ─── Arena ───────────────────────────────────────────────────────────────────

/// One cell in the shared arena.
///
/// Integer/real values inline (no arena bytes); text/blob reference a byte range
/// in the arena's `data`. NULL is the `Null` variant — a real absence, encoded
/// by the discriminant with no sentinel. The `u32` offset/len keep the slot at
/// 16 bytes; a whole result whose text/blob bytes exceed `u32::MAX` (a `> 4 GiB`
/// eager materialization) is rejected loudly at seal rather than mis-addressed
/// (stream it via [`Connection::query_each_sql`] instead — that path has no cap).
#[derive(Debug, Clone, Copy)]
enum CellSlot {
    Null,
    Integer(i64),
    Real(f64),
    Text { offset: u32, len: u32 },
    Blob { offset: u32, len: u32 },
}

// The slot is exactly two words: an 8-byte payload (an `i64`/`f64`, or a
// `(u32, u32)` offset/len pair) plus the discriminant. A wider payload here
// would bloat every result's slot table, so it is pinned.
const _: () = assert!(core::mem::size_of::<CellSlot>() == 16);

/// Shared backing store for every row in one eager result: the concatenated
/// text/blob bytes, the per-cell slot table addressed by the fixed `n_cols`
/// stride, and the column names (shared so a minted [`Row`] resolves a name
/// without the caller threading a slice). One `Arc<ArenaInner>` backs every
/// [`Row`] handle; the row COUNT lives in the owning [`RowSet`].
#[derive(Debug)]
struct ArenaInner {
    data: Vec<u8>,
    slots: Vec<CellSlot>,
    n_cols: u16,
    column_names: Arc<[String]>,
}

/// Builds the shared arena during the row-step loop. One builder per query;
/// [`finish`](Self::finish) seals it into the `Arc`-shared [`RowSet`].
///
/// A cell offset/length or column count that overflows the 32-bit slot fields
/// sets a sticky `overflow` flag rather than saturating; `finish` converts it
/// into [`SqliteError::ResultTooLarge`] so a too-large eager result fails loudly
/// instead of returning mis-addressed cells.
struct ArenaBuilder {
    data: Vec<u8>,
    slots: Vec<CellSlot>,
    n_cols: u16,
    rows: u32,
    overflow: bool,
}

impl ArenaBuilder {
    /// A builder for a `n_cols`-wide row shape. A count past `u16` marks the
    /// builder overflowed (real SQLite caps columns well under `u16::MAX`, so
    /// this only fires on a fabricated width); `finish` then fails loudly.
    fn new(n_cols: usize) -> Self {
        let (n, overflow) = match u16::try_from(n_cols) {
            Ok(n) => (n, false),
            Err(_) => (0, true),
        };
        Self { data: Vec::new(), slots: Vec::new(), n_cols: n, rows: 0, overflow }
    }

    /// Append one cell, decoded from SQLite's native storage class. Text/blob
    /// bytes are copied into the arena ONCE (verbatim, UTF-8 unvalidated — a
    /// `TEXT` cell keeps its raw bytes; validation is deferred to `get::<&str>`);
    /// integer/real values inline into the slot.
    fn push_ref(&mut self, v: rusqlite::types::ValueRef<'_>) {
        match v {
            rusqlite::types::ValueRef::Null => self.slots.push(CellSlot::Null),
            rusqlite::types::ValueRef::Integer(n) => self.slots.push(CellSlot::Integer(n)),
            rusqlite::types::ValueRef::Real(f) => self.slots.push(CellSlot::Real(f)),
            rusqlite::types::ValueRef::Text(b) => self.push_bytes(b, true),
            rusqlite::types::ValueRef::Blob(b) => self.push_bytes(b, false),
        }
    }

    /// Copy `bytes` into the arena, recording an offset/len slot. On a 32-bit
    /// overflow (offset, length, or their sum), mark the builder and push a NULL
    /// placeholder — `finish` fails loudly, never a saturated range that would
    /// mis-address bytes.
    fn push_bytes(&mut self, bytes: &[u8], is_text: bool) {
        let offset = u32::try_from(self.data.len()).ok();
        let len = u32::try_from(bytes.len()).ok();
        let Some((offset, len)) = offset.zip(len).filter(|(o, l)| o.checked_add(*l).is_some())
        else {
            self.overflow = true;
            self.slots.push(CellSlot::Null);
            return;
        };
        self.data.extend_from_slice(bytes);
        self.slots
            .push(if is_text { CellSlot::Text { offset, len } } else { CellSlot::Blob { offset, len } });
    }

    /// Close the row currently being built (its `n_cols` cells were pushed).
    ///
    /// The row COUNTER is guarded exactly like the byte dimension: a result that
    /// overflows the `u32` count (past `u32::MAX` rows) sets the sticky overflow
    /// flag so `finish` fails loud with `ResultTooLarge`, never a silent
    /// saturation that would leave `len()` under-reporting and the tail rows
    /// unaddressable. A pure-integer result writes ZERO arena bytes, so the byte
    /// guard alone would miss this — the counter needs its own loud guard.
    fn end_row(&mut self) {
        match self.rows.checked_add(1) {
            Some(n) => self.rows = n,
            None => self.overflow = true,
        }
    }

    /// Seal the arena into a [`RowSet`] sharing `column_names`.
    ///
    /// # Errors
    ///
    /// [`SqliteError::ResultTooLarge`] if any column count, offset, or length
    /// overflowed the 32-bit slot fields (a `> 4 GiB` eager result).
    fn finish(self, column_names: Arc<[String]>) -> Result<RowSet, SqliteError> {
        if self.overflow {
            return Err(SqliteError::ResultTooLarge);
        }
        // A rowless seal allocates NO arena; the invariant `arena.is_some() ==
        // (n_rows > 0)` keeps handle-minting from addressing an absent store.
        if self.rows == 0 {
            return Ok(RowSet { arena: None, n_rows: 0 });
        }
        let arena = Arc::new(ArenaInner {
            data: self.data,
            slots: self.slots,
            n_cols: self.n_cols,
            column_names,
        });
        Ok(RowSet { arena: Some(arena), n_rows: self.rows })
    }
}

// ─── RowSet ──────────────────────────────────────────────────────────────────

/// The sealed rows of one eager result: ONE shared arena (or none, for a
/// command that returned no rows) plus the row count. Every [`Row`] handle is
/// minted ON DEMAND — a 16-byte `Arc`-clone — by [`get`](Self::get) /
/// [`iter`](Self::iter), so the whole set costs the arena's allocations
/// regardless of row count and NO eager `Vec<Row>` is ever built. This is the
/// SQLite parallel of the PostgreSQL dynamic `RowSet`: one owned backing store,
/// lazy per-row access, zero per-row allocation. A caller normally reaches it
/// through the [`QueryResult`] facade rather than naming it directly.
#[derive(Debug, Clone, Default)]
pub struct RowSet {
    /// `None` = a command that produced no result set: no arena is allocated.
    /// `Some` = the shared arena backing every row. Invariant (held by
    /// [`ArenaBuilder::finish`]): `Some` iff `n_rows > 0`.
    arena: Option<Arc<ArenaInner>>,
    /// The number of rows the arena addresses (its 32-bit row-index bound).
    n_rows: u32,
}

// Footprint pin: `Option<Arc<_>>` niche-packs to one 8-byte pointer, plus a
// `u32` row count = 12 B padded to 16.
crate::footprint_pin!(RowSet, size = 16, align = 8);

impl RowSet {
    /// The number of result rows.
    #[must_use]
    #[expect(
        clippy::manual_unwrap_or_default,
        reason = "`unwrap_or_default()` is banned by the tier-4 silent-fallback ledger; this \
                  explicit match is the sanctioned dead arm for an infallible narrow (`usize` is \
                  at least 32 bits on every supported target, so `n_rows: u32` always fits)"
    )]
    pub fn len(&self) -> usize {
        match usize::try_from(self.n_rows) {
            Ok(n) => n,
            Err(_) => 0,
        }
    }

    /// Whether the result produced no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    /// The row at zero-based index `i`, or `None` if `i >= len()`. A 16-byte
    /// [`Row`] handle built on demand — O(1), exactly one `Arc` refcount bump,
    /// never a per-row allocation.
    #[must_use]
    pub fn get(&self, i: usize) -> Option<Row> {
        // A `usize` index past `u32::MAX` cannot address a `u32`-bounded arena.
        let idx = u32::try_from(i).ok()?;
        self.row_at(idx)
    }

    /// Mint the handle for `idx` if it is in range and an arena backs it.
    fn row_at(&self, idx: u32) -> Option<Row> {
        if idx >= self.n_rows {
            return None;
        }
        self.arena
            .as_ref()
            .map(|arena| Row { arena: Arc::clone(arena), row_idx: idx })
    }

    /// A lazy iterator over the rows. Each yielded [`Row`] is one `Arc`-clone
    /// handle — no per-row allocation, no pre-materialised `Vec`.
    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        (0..self.n_rows).filter_map(move |idx| self.row_at(idx))
    }

    /// Materialise every row into an owned `Vec<Row>` — the escape hatch for a
    /// caller that truly needs a random-access owned collection (pays the N
    /// `Arc`-clones + the `16·N`-byte `Vec`). Prefer [`iter`](Self::iter) /
    /// [`get`](Self::get) unless an owned `Vec` is genuinely required.
    #[must_use]
    pub fn into_vec(self) -> Vec<Row> {
        self.iter().collect()
    }
}

// ─── QueryResult ─────────────────────────────────────────────────────────────

/// The result of an eager query — rows + column names.
///
/// The rows are held LAZILY: `QueryResult` owns one [`RowSet`] (a single shared
/// `Arc` over the row bytes), and its accessors mint [`Row`] handles on demand
/// rather than eagerly building a `Vec<Row>`. A whole R-row × C-column result
/// costs a constant number of allocations (the arena's `data`/`slots` plus the
/// shared name/arena `Arc`s), NOT the eager model's `1 + R + T` (an outer
/// `Vec<Row>`, a per-row `Vec`, and a per-`TEXT`/`BLOB`-cell owned buffer).
/// Read the rows with [`iter`](Self::iter), [`get`](Self::get), or
/// [`len`](Self::len). This mirrors the PostgreSQL dynamic result model — one
/// owned backing store, zero per-row allocation.
#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    /// The result rows, held as one shared arena (see [`RowSet`]). Private so
    /// the eager-`Vec<Row>` shape cannot be reintroduced by a caller.
    rows: RowSet,
    /// The column names, in column order. Shared (by `Arc`) with the arena, so
    /// a minted [`Row`] resolves a by-name lookup without a threaded slice.
    pub column_names: Arc<[String]>,
}

// Footprint pin: a `RowSet` (16 B) + an `Arc<[String]>` (16 B, fat pointer) =
// 32 B, down from the 56 B the old eager `Vec<Row>` + `usize` + `Vec<String>`
// cost. The redundant `column_count` field is gone (it equals
// `column_names.len()`, exposed as [`QueryResult::column_count`]).
crate::footprint_pin!(QueryResult, size = 32, align = 8);

impl QueryResult {
    /// The number of result rows (no allocation — read from the arena's count).
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the result produced no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The number of columns each row carries (the length of
    /// [`column_names`](Self::column_names) — one authority, no divergence).
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }

    /// The row at zero-based index `i`, or `None` if `i >= len()`. A 16-byte
    /// `Arc`-clone [`Row`] handle built on demand — O(1), one refcount bump.
    #[must_use]
    pub fn get(&self, i: usize) -> Option<Row> {
        self.rows.get(i)
    }

    /// A lazy iterator over the rows; each yielded [`Row`] is one `Arc`-clone
    /// handle, minted on demand — no pre-materialised `Vec`.
    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        self.rows.iter()
    }

    /// Materialise every row into an owned `Vec<Row>` — the escape hatch for a
    /// caller that truly needs a random-access owned collection (pays the N
    /// `Arc`-clones + the `16·N`-byte `Vec`). Prefer [`iter`](Self::iter) /
    /// [`get`](Self::get) unless an owned `Vec` is genuinely required.
    #[must_use]
    pub fn into_vec(self) -> Vec<Row> {
        self.rows.into_vec()
    }
}

// ─── Row ─────────────────────────────────────────────────────────────────────

/// A single result row — a 16-byte handle (`Arc` pointer + row index) into the
/// shared arena. `'static + Clone + Send + Sync`; `Clone` is an `Arc` refcount
/// bump. The row carries its own column names (via the shared arena), so a
/// by-name read needs no threaded slice.
///
/// Reads are classified: [`Row::get`] returns `Err` on a type mismatch or an
/// unexpected `NULL`, never a silent `None`. For a nullable column use
/// [`Row::get_opt`], which distinguishes a real `NULL` (`Ok(None)`) from a
/// type mismatch (`Err`). Text/blob reads borrow the arena's cell bytes
/// zero-copy (`get::<&str>` / `get::<&[u8]>`); `get::<String>` /
/// `get::<Vec<u8>>` copy. UTF-8 is validated lazily at `get::<&str>`, never
/// eagerly — a non-UTF-8 `TEXT` cell fails only when read as text, its raw
/// bytes always recoverable via [`value_ref`](Self::value_ref).
#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    arena: Arc<ArenaInner>,
    row_idx: u32,
}

// Footprint pin: one `Arc` pointer + a `u32` row index. Keeping it a 16-byte
// handle is what makes `Clone` a refcount bump and lets a row cross threads.
crate::footprint_pin!(Row, size = 16, align = 8);

// Tier-1 static assertion (matching the PostgreSQL driver's discipline): `Row`
// is `Send + Sync + 'static`, as its doc claims — a 16-byte handle can cross
// threads and outlive any borrow. `footprint_pin!` covers only size/align, so a
// future non-`Send`/non-`Sync` field in `ArenaInner` would silently falsify the
// doc; this pins it. Type-checked, never run.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_static<T: 'static>() {}
    fn _assertions() {
        _assert_send::<Row>();
        _assert_sync::<Row>();
        _assert_static::<Row>();
    }
};

impl Row {
    /// A zero-copy borrowed view of column `col`, or
    /// [`SqliteError::ColumnIndexOutOfBounds`] if `col` is past the row.
    ///
    /// The text/blob byte slices borrow the shared arena for `&self`'s lifetime,
    /// so the view is honestly zero-copy.
    pub fn value_ref(&self, col: usize) -> Result<ValueRef<'_>, SqliteError> {
        resolve_cell(&self.arena, self.row_idx, col)
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
        usize::from(self.arena.n_cols)
    }

    /// Whether the row has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.arena.n_cols == 0
    }

    /// Read the column named `name` as `T`. The row carries its own column
    /// names (via the shared arena), so no slice is threaded. A name absent from
    /// the result is [`SqliteError::UnknownColumn`], never a silent `None`.
    pub fn get_by_name<'a, T: FromColumn<'a>>(
        &'a self,
        name: &str,
    ) -> Result<T, SqliteError> {
        match self.arena.column_names.iter().position(|n| n == name) {
            Some(idx) => self.get(idx),
            None => Err(SqliteError::UnknownColumn { name: name.to_owned() }),
        }
    }
}

/// Resolve column `col` of row `row_idx` in `inner` to a borrowed [`ValueRef`],
/// or a classified error. The single arena cell-resolution routine, called by
/// BOTH the owned [`Row`] handle ([`Row::value_ref`]) and the borrowed
/// [`ArenaRowRef`] view — so the two cannot drift, and the borrowed view's
/// zero-copy cell reuses the exact slot/byte-range logic the handle proved.
///
/// The returned [`ValueRef`] borrows `inner` (the `&ArenaInner` argument), so
/// its lifetime is the caller's arena borrow — NOT the receiver: an
/// [`ArenaRowRef<'a>`] holding `&'a ArenaInner` yields a `ValueRef<'a>` (the
/// container's lifetime), which is what makes a typed borrowed record outlive
/// the per-row view.
///
/// The arena is built by `ArenaBuilder` with `slots.len() == n_rows * n_cols`
/// and every text/blob range in-bounds, and a row index is used only when
/// `< n_rows`, so for an in-range `col` the slot and byte lookups are total BY
/// CONSTRUCTION. The `?` / `.get()` fail-closed arms are the architecturally
/// unreachable dead path — never a panic, never an out-of-bounds index, never a
/// fabricated value.
fn resolve_cell(inner: &ArenaInner, row_idx: u32, col: usize) -> Result<ValueRef<'_>, SqliteError> {
    let n_cols = usize::from(inner.n_cols);
    if col >= n_cols {
        return Err(SqliteError::ColumnIndexOutOfBounds { index: col, count: n_cols });
    }
    let corrupt =
        || SqliteError::Query("arena slot resolution failed (invariant violated)".to_owned());
    let row_base = usize::try_from(row_idx).map_err(|_| corrupt())?;
    let slot_idx = row_base
        .checked_mul(n_cols)
        .and_then(|b| b.checked_add(col))
        .ok_or_else(corrupt)?;
    match *inner.slots.get(slot_idx).ok_or_else(corrupt)? {
        CellSlot::Null => Ok(ValueRef::Null),
        CellSlot::Integer(n) => Ok(ValueRef::Integer(n)),
        CellSlot::Real(f) => Ok(ValueRef::Real(f)),
        CellSlot::Text { offset, len } => Ok(ValueRef::Text(slice_arena(&inner.data, offset, len)?)),
        CellSlot::Blob { offset, len } => Ok(ValueRef::Blob(slice_arena(&inner.data, offset, len)?)),
    }
}

/// Borrow `data[offset .. offset+len]`, fail-closed to a classified error if the
/// range is out of bounds. The range is in-bounds BY CONSTRUCTION (the builder
/// only records a slot after copying its bytes and checking the 32-bit sum), so
/// the `None` arms are the architecturally unreachable dead path.
fn slice_arena(data: &[u8], offset: u32, len: u32) -> Result<&[u8], SqliteError> {
    let corrupt = || SqliteError::Query("arena byte range out of bounds (invariant violated)".to_owned());
    let start = usize::try_from(offset).map_err(|_| corrupt())?;
    let end = start.checked_add(usize::try_from(len).map_err(|_| corrupt())?).ok_or_else(corrupt)?;
    data.get(start..end).ok_or_else(corrupt)
}

/// A borrowed view of a single row on the streaming path, valid only for the
/// duration of the [`Connection::query_each_sql`] callback that receives it.
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

// A streaming row is a typed-decode column source: `cell` lends SQLite's own
// column buffer for the row step (`'r`), so a borrowed typed record decoded
// through it borrows that buffer zero-copy.
impl<'r> ColumnSource<'r> for BorrowedRow<'r> {
    fn cell(&self, col: usize) -> Result<ValueRef<'r>, SqliteError> {
        self.value_ref(col)
    }

    fn column_count(&self) -> usize {
        BorrowedRow::column_count(self)
    }
}

/// An embedded SQLite connection.
pub struct Connection {
    inner: rusqlite::Connection,
    /// The diagnostics-only N+1 query detector. Present ONLY under the
    /// `n1-detect` feature — a default build has no such field, so the typed
    /// verbs stay byte-identical blocking `fn`s and the footprint is unchanged.
    /// `RefCell` because the typed verbs record through `&self` (SQLite's engine
    /// is already `!Sync` via rusqlite's interior mutability, so this adds no new
    /// thread-safety constraint).
    #[cfg(feature = "n1-detect")]
    n1: core::cell::RefCell<crate::n1::N1Tracker>,
}

impl core::fmt::Debug for Connection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    /// Wrap an already-configured rusqlite connection into a driver `Connection`,
    /// initialising the (feature-gated) N+1 tracker. The single construction seam
    /// both `open` constructors funnel through, so the `n1-detect` field is
    /// initialised in exactly one place.
    fn wrap(inner: rusqlite::Connection) -> Self {
        Self {
            inner,
            #[cfg(feature = "n1-detect")]
            n1: core::cell::RefCell::new(crate::n1::N1Tracker::new()),
        }
    }

    /// Open (or create) a database at `path`, enabling WAL journaling and
    /// foreign-key enforcement, and a [`DEFAULT_BUSY_TIMEOUT`] so a briefly-locked
    /// database waits (bounded) rather than failing instantly under WAL contention.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let inner = rusqlite::Connection::open(path).map_err(|e| SqliteError::Open(e.to_string()))?;
        inner
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        inner
            .busy_timeout(DEFAULT_BUSY_TIMEOUT)
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        Ok(Self::wrap(inner))
    }

    /// Open a private in-memory database with foreign-key enforcement and the
    /// [`DEFAULT_BUSY_TIMEOUT`].
    pub fn open_in_memory() -> Result<Self, SqliteError> {
        let inner =
            rusqlite::Connection::open_in_memory().map_err(|e| SqliteError::Open(e.to_string()))?;
        inner
            .execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        inner
            .busy_timeout(DEFAULT_BUSY_TIMEOUT)
            .map_err(|e| SqliteError::Open(e.to_string()))?;
        Ok(Self::wrap(inner))
    }

    /// Set how long a locked-database operation waits for the lock before
    /// returning a CLASSIFIED busy error (`SqliteError::is_busy()`), never a
    /// hang. Overrides the [`DEFAULT_BUSY_TIMEOUT`] the `open` constructors set;
    /// `Duration::ZERO` disables the wait entirely (a contended operation fails
    /// IMMEDIATELY, the honest fail-loud with no hidden blocking).
    pub fn set_busy_timeout(&self, timeout: Duration) -> Result<(), SqliteError> {
        self.inner.busy_timeout(timeout).map_err(SqliteError::from)
    }

    /// Mint a detached [`SqliteCancelToken`](crate::SqliteCancelToken) for this
    /// connection's in-flight (or next) query — the cross-backend twin of the
    /// PostgreSQL `conn.cancel_token()`.
    ///
    /// The token is `Send + Sync + 'static` and borrows nothing from the
    /// connection, so it can be obtained BEFORE a long / compute-bound query and
    /// handed to another thread that calls
    /// [`cancel`](crate::SqliteCancelToken::cancel) mid-query. The interrupted
    /// step surfaces as [`SqliteError::Interrupted`](crate::SqliteError::Interrupted)
    /// and the connection stays reusable.
    #[must_use]
    pub fn cancel_token(&self) -> crate::SqliteCancelToken {
        crate::SqliteCancelToken::new(self.inner.get_interrupt_handle())
    }

    /// The detected N+1 anti-patterns on this connection so far — one entry per
    /// `(query, source line)` site that ran the SAME `query!` past the detector's
    /// threshold (25) within a single logical operation. Present ONLY under the
    /// `n1-detect` feature; purely diagnostic — enabling detection cannot change
    /// what any query returns. The SQLite twin of the PostgreSQL driver's
    /// `n1_report()`, returning the SAME [`N1Report`](crate::N1Report) shape.
    ///
    /// Returns an owned snapshot (the tracker lives behind interior mutability),
    /// so a caller iterating it holds no borrow of the connection.
    #[cfg(feature = "n1-detect")]
    #[must_use]
    pub fn n1_report(&self) -> Vec<crate::N1Report> {
        self.n1.borrow().report().to_vec()
    }

    /// Record one execution of a typed `query!` from source location `caller`.
    /// Diagnostics-only (see [`N1Tracker`](crate::N1Tracker)); the typed verbs
    /// funnel their `#[track_caller]` site here.
    #[cfg(feature = "n1-detect")]
    fn n1_record(&self, sql: &'static str, caller: &'static core::panic::Location<'static>) {
        self.n1.borrow_mut().record(sql, caller);
    }

    /// Forget the N+1 recency window at a logical-operation boundary (a
    /// transaction commit/rollback), so repetition of a query ACROSS separate
    /// operations is forgiven while a per-row loop WITHIN one is caught.
    #[cfg(feature = "n1-detect")]
    fn n1_reset(&self) {
        self.n1.borrow_mut().reset();
    }

    /// Execute a statement, returning the number of rows changed.
    pub fn execute(&self, sql: &str) -> Result<u64, SqliteError> {
        Ok(changes_to_u64(self.inner.execute(sql, [])?))
    }

    /// Execute a parameterized statement, returning the number of rows changed.
    ///
    /// Each parameter binds in its TRUE SQLite storage class (see [`ValueRef`]):
    /// `Null` binds SQL `NULL`, `Integer`/`Real` bind numerically with no
    /// affinity coercion, `Text`/`Blob` bind their bytes zero-copy — so a `NULL`
    /// or `BLOB` parameter is expressible, and an integer is compared as an
    /// integer.
    pub fn execute_params(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<u64, SqliteError> {
        Ok(changes_to_u64(self.inner.execute(sql, rusqlite::params_from_iter(params))?))
    }

    /// Run `sql` and eagerly materialize every row.
    ///
    /// The `_sql` suffix marks the DYNAMIC (raw-SQL-text) verb, distinct from the
    /// compile-checked typed flagship [`query`](Self::query)`::<Q>` that runs a
    /// `query!` carrier — the same naming split the PostgreSQL driver uses
    /// (`query_sql` dynamic, `query` typed).
    pub fn query_sql(&self, sql: &str) -> Result<QueryResult, SqliteError> {
        self.query_collect(sql, [])
    }

    /// Run a parameterized `sql` and eagerly materialize every row. Each
    /// parameter binds in its true storage class — see [`Self::execute_params`].
    pub fn query_params(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<QueryResult, SqliteError> {
        self.query_collect(sql, rusqlite::params_from_iter(params))
    }

    /// Shared eager-collect core for [`Self::query_sql`] / [`Self::query_params`].
    ///
    /// Materializes every row into ONE shared arena (a single `data`/`slots`
    /// pair) and a lazy [`RowSet`] over it — no per-row `Vec`, no per-cell owned
    /// buffer. The column names are built once and shared (by `Arc`) between the
    /// result and the arena, so a by-name read on a minted [`Row`] threads no
    /// slice.
    fn query_collect(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<QueryResult, SqliteError> {
        let mut stmt = self.inner.prepare(sql)?;
        let col_count = stmt.column_count();
        let column_names: Arc<[String]> =
            stmt.column_names().iter().map(|s| (*s).to_owned()).collect();

        let mut builder = ArenaBuilder::new(col_count);
        let mut rows = stmt.query(params)?;
        while let Some(row) = rows.next()? {
            for col in 0..col_count {
                builder.push_ref(row.get_ref(col)?);
            }
            builder.end_row();
        }

        let rows = builder.finish(Arc::clone(&column_names))?;
        Ok(QueryResult { rows, column_names })
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
    pub fn query_each_sql<F, E>(&self, sql: &str, on_row: F) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.query_each_collect(sql, [], on_row)
    }

    /// Parameterized peer of [`Self::query_each_sql`].
    pub fn query_each_params<F, E>(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
        on_row: F,
    ) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.query_each_collect(sql, rusqlite::params_from_iter(params), on_row)
    }

    /// Shared streaming core for [`Self::query_each_sql`] / [`Self::query_each_params`].
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
    pub fn query_params_one(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<Row, SqliteError> {
        self.query_params(sql, params)?
            .get(0)
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_owned()))
    }

    /// Run a parameterized query and return its first row, if any.
    pub fn query_params_opt(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<Option<Row>, SqliteError> {
        Ok(self.query_params(sql, params)?.get(0))
    }

    /// Run a query and return exactly its first row, or [`SqliteError::Query`]
    /// if it produced none.
    pub fn query_one_sql(&self, sql: &str) -> Result<Row, SqliteError> {
        self.query_sql(sql)?
            .get(0)
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_owned()))
    }

    /// Run a query and return its first row, if any.
    pub fn query_opt_sql(&self, sql: &str) -> Result<Option<Row>, SqliteError> {
        Ok(self.query_sql(sql)?.get(0))
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// query over SQLite.
    ///
    /// `Q` is a `query!` carrier (`FooQuery`); the projected column types and
    /// nullability were fixed at build time against the migration-replayed
    /// schema, so this returns typed records, not a dynamic row. Each `$N`
    /// parameter binds through `params` in its TRUE SQLite storage class (see
    /// [`ValueRef`]) — a `NULL` / `BLOB` parameter is expressible, an integer is
    /// bound as an integer.
    ///
    /// Because SQLite is dynamically typed, decoding VERIFIES each value's actual
    /// storage class against the record's declared field type: a mismatch (the
    /// catalog declared `INTEGER`, a `TEXT` arrives) is a classified
    /// [`SqliteError::TypeMismatch`], and a `NULL` in a non-`Option` field is
    /// [`SqliteError::UnexpectedNull`] — surfaced lazily at
    /// [`TypedRows::iter`] / [`TypedRows::into_owned`], never a silent coercion.
    ///
    /// # Errors
    ///
    /// A prepare / step failure is a classified [`SqliteError`]; per-row decode
    /// failures surface from the returned [`TypedRows`].
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<TypedRows<Q>, SqliteError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::SQL, core::panic::Location::caller());
        let result = self.query_collect(Q::SQL, rusqlite::params_from_iter(params))?;
        Ok(TypedRows { result, _q: PhantomData })
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned typed record. Zero rows is [`SqliteError::Query`]; more than one is
    /// [`SqliteError::TooManyRows`] — the same exactly-one contract the
    /// PostgreSQL typed `query_one` enforces (the typed flagship reads the same
    /// on both backends).
    ///
    /// Streams and decodes ONLY the first row (no whole-result arena), then steps
    /// once more to reject a second — so a by-key lookup pays for one row plus one
    /// step, never a materialization. The dynamic
    /// [`query_one_sql`](Self::query_one_sql) is the first-row variant.
    ///
    /// # Errors
    ///
    /// [`SqliteError::Query`] on zero rows, [`SqliteError::TooManyRows`] on two or
    /// more, or a classified [`SqliteError`] on a prepare / step / decode failure.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<Q::Owned, SqliteError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::SQL, core::panic::Location::caller());
        self.query_first_owned::<Q>(params)?
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_owned()))
    }

    /// Run a compile-checked `query!` expecting AT MOST one row, returning the
    /// owned typed record if present or `None` if absent — the by-key
    /// maybe-absent shape. More than one row is [`SqliteError::TooManyRows`] (the
    /// same at-most-one contract as the PostgreSQL typed `query_opt`).
    ///
    /// Streams and decodes ONLY the first row (no whole-result arena), then steps
    /// once more to reject a second. The dynamic
    /// [`query_opt_sql`](Self::query_opt_sql) is the first-row variant.
    ///
    /// # Errors
    ///
    /// [`SqliteError::TooManyRows`] on two or more rows, or a classified
    /// [`SqliteError`] on a prepare / step / decode failure (zero rows is
    /// `Ok(None)`).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_opt<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<Option<Q::Owned>, SqliteError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::SQL, core::panic::Location::caller());
        self.query_first_owned::<Q>(params)
    }

    /// Shared at-most-one decode-direct body behind
    /// [`query_one`](Self::query_one) / [`query_opt`](Self::query_opt): prepare,
    /// step to the first row (if any), decode it into the owned twin without
    /// materialising the whole-result arena, then step ONCE more to enforce the
    /// at-most-one contract.
    ///
    /// A second row is the classified [`SqliteError::TooManyRows`] — matching the
    /// PostgreSQL TYPED `query_one` / `query_opt` (exactly-one / at-most-one), so
    /// the typed flagship reads the same on both backends and a query ported
    /// PostgreSQL→SQLite keeps its multi-row semantics. The extra step is one
    /// `sqlite3_step`, never a materialization (the same cost model as the
    /// PostgreSQL break-on-second-row path). The DYNAMIC `query_one_sql` /
    /// `query_opt_sql` deliberately stay first-row.
    fn query_first_owned<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<Option<Q::Owned>, SqliteError> {
        let mut stmt = self.inner.prepare(Q::SQL)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;
        // Decode the first row's OWNED twin (copies text/blob out, so it no
        // longer borrows the row step) — or return `None` for zero rows. The
        // borrowed view is dropped at the end of this `match`, releasing `rows`
        // for the second step below.
        let first = match rows.next()? {
            Some(row) => Q::decode_row_owned(&BorrowedRow { row })?,
            None => return Ok(None),
        };
        // A second row means the caller asked for at most one but got more.
        if rows.next()?.is_some() {
            return Err(SqliteError::TooManyRows);
        }
        Ok(Some(first))
    }

    /// Stream a compile-checked `query!`'s TYPED rows one at a time through
    /// `on_row` in CONSTANT memory — the streaming peer of [`query`](Self::query).
    ///
    /// Each borrowed record is decoded directly off SQLite's row buffer (text/blob
    /// columns alias it, zero-copy) and handed to `on_row`; the `for<'q>` bound
    /// makes the record valid only inside the call, so nothing it lends can
    /// escape (a compile error). `on_row` returns [`ControlFlow`]: `Continue(())`
    /// keeps streaming, `Break(e)` stops early. The result is `Ok(None)` when
    /// every row streamed, `Ok(Some(e))` on an early break, or `Err` on a
    /// prepare / step / decode failure — a decode error is LOUD (it stops the
    /// stream), never skipped.
    ///
    /// # Errors
    ///
    /// A classified [`SqliteError`] on a prepare / step / decode failure.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<Q, F, E>(
        &self,
        params: &[ValueRef<'_>],
        mut on_row: F,
    ) -> Result<Option<E>, SqliteError>
    where
        Q: SqliteTypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::SQL, core::panic::Location::caller());
        let mut stmt = self.inner.prepare(Q::SQL)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;
        while let Some(row) = rows.next()? {
            let view = BorrowedRow { row };
            let record = Q::decode_row(&view)?;
            if let ControlFlow::Break(e) = on_row(record) {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    /// Begin a transaction.
    pub fn begin(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("BEGIN")?;
        Ok(())
    }

    /// Commit the current transaction (a logical-operation boundary: the N+1
    /// recency window is forgotten under `n1-detect`, matching the PostgreSQL
    /// `commit()` so the manual `begin`/`commit` path resets identically to the
    /// closure `transaction` path and to both PostgreSQL drivers).
    pub fn commit(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("COMMIT")?;
        #[cfg(feature = "n1-detect")]
        self.n1_reset();
        Ok(())
    }

    /// Roll back the current transaction (a logical-operation boundary: the N+1
    /// recency window is forgotten under `n1-detect`, matching PostgreSQL's
    /// `rollback()`).
    pub fn rollback(&self) -> Result<(), SqliteError> {
        self.inner.execute_batch("ROLLBACK")?;
        #[cfg(feature = "n1-detect")]
        self.n1_reset();
        Ok(())
    }

    /// Execute a closure within a transaction. COMMIT on `Ok`, ROLLBACK on `Err`.
    ///
    /// The closure receives a [`Transaction`] guard, NOT the `Connection`: the
    /// guard exposes only the data verbs, so a nested `tx.transaction(..)` or a
    /// manual `tx.commit()` / `tx.begin()` / `tx.close()` inside the body is a
    /// COMPILE error (E0599 — no such method), never the runtime "cannot start a
    /// transaction within a transaction" the old `&Connection` argument allowed.
    /// The transaction boundary IS the closure scope — tier-1, enforced by the
    /// type the closure is handed.
    ///
    /// (The guard is a `&Connection` borrow; a body that DELIBERATELY captures
    /// the outer `Connection` and re-enters `transaction` on it is out of scope —
    /// this closes the ergonomic nesting misuse, not a hand-rolled bypass, and
    /// matches the PostgreSQL driver's transaction guard exactly.)
    pub fn transaction<R>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> Result<R, SqliteError>,
    ) -> Result<R, SqliteError> {
        self.inner.execute_batch("BEGIN")?;
        let tx = Transaction { conn: self };
        let result = match f(&tx) {
            Ok(val) => self
                .inner
                .execute_batch("COMMIT")
                .map(|()| val)
                .map_err(SqliteError::from),
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
        };
        // Either terminator closes a logical operation: forget the N+1 recency
        // window so repetition ACROSS transactions is forgiven (a no-op with the
        // feature off).
        #[cfg(feature = "n1-detect")]
        self.n1_reset();
        result
    }

    /// Execute multiple SQL statements separated by semicolons.
    pub fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        self.inner.execute_batch(sql).map_err(SqliteError::from)
    }

    /// Close the connection, surfacing any final flush error.
    ///
    /// The error is routed through the same `From<rusqlite::Error>` every other
    /// path uses, so a busy/locked-on-close error PRESERVES its extended code and
    /// stays matchable by `is_busy()` / `code()` — the old string-flattening
    /// declassified it.
    pub fn close(self) -> Result<(), SqliteError> {
        self.inner.close().map_err(|(_conn, e)| SqliteError::from(e))
    }
}

/// Widen a rusqlite change count (`usize`) to the cross-backend `u64` (matching
/// the PostgreSQL drivers' affected-row type). Infallible on every supported
/// target — `usize` is at most 64 bits, so it always fits `u64`.
#[expect(
    clippy::manual_unwrap_or_default,
    reason = "`unwrap_or_default()` is banned by the tier-4 silent-fallback ledger; this explicit \
              match is the sanctioned dead arm for the structurally-infallible `usize -> u64` \
              widening (`usize` is at most 64 bits on every supported target)"
)]
fn changes_to_u64(n: usize) -> u64 {
    match u64::try_from(n) {
        Ok(v) => v,
        Err(_) => 0,
    }
}

// ─── Transaction guard ───────────────────────────────────────────────────────

/// The handle a [`Connection::transaction`] closure receives — a borrowing guard
/// over the connection that exposes ONLY the data verbs.
///
/// It has no `begin` / `commit` / `rollback` / `transaction` / `close`, so the
/// transaction-lifecycle misuses that the old `&Connection` argument allowed —
/// a nested `tx.transaction(..)` (runtime "cannot start a transaction within a
/// transaction"), a manual `tx.commit()` that desyncs the driver's BEGIN/COMMIT
/// bracketing, a `tx.close()` mid-transaction — are COMPILE errors (E0599), not
/// runtime surprises. The commit-on-`Ok` / rollback-on-`Err` boundary is driven
/// solely by the closure's result; the guard cannot touch it.
///
/// Every data verb delegates to the borrowed connection, so a well-formed body
/// compiles with zero call-site change beyond the argument type.
///
/// Residual (inherent, shared with the PostgreSQL guard): the guard closes the
/// METHOD-level misuse, but raw SQL text cannot be typed away on any
/// `execute(&str)` surface — a body that runs `tx.execute("COMMIT")` (or
/// `"BEGIN"` / `"SAVEPOINT s"`) as a string still reaches the engine. That is a
/// property of accepting arbitrary SQL text, not a gap in this guard; the
/// compile-checked boundary is the set of METHODS the closure is handed.
#[derive(Debug)]
pub struct Transaction<'c> {
    conn: &'c Connection,
}

impl Transaction<'_> {
    /// Execute a statement, returning the number of rows changed.
    pub fn execute(&self, sql: &str) -> Result<u64, SqliteError> {
        self.conn.execute(sql)
    }

    /// Execute a parameterized statement, returning the number of rows changed.
    pub fn execute_params(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<u64, SqliteError> {
        self.conn.execute_params(sql, params)
    }

    /// Execute multiple SQL statements separated by semicolons.
    pub fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        self.conn.execute_batch(sql)
    }

    /// Run `sql` and eagerly materialize every row.
    pub fn query_sql(&self, sql: &str) -> Result<QueryResult, SqliteError> {
        self.conn.query_sql(sql)
    }

    /// Run a parameterized `sql` and eagerly materialize every row.
    pub fn query_params(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<QueryResult, SqliteError> {
        self.conn.query_params(sql, params)
    }

    /// Run a query and return exactly its first row, or
    /// [`SqliteError::Query`] if it produced none.
    pub fn query_one_sql(&self, sql: &str) -> Result<Row, SqliteError> {
        self.conn.query_one_sql(sql)
    }

    /// Run a query and return its first row, if any.
    pub fn query_opt_sql(&self, sql: &str) -> Result<Option<Row>, SqliteError> {
        self.conn.query_opt_sql(sql)
    }

    /// Run a parameterized query and return exactly its first row, or
    /// [`SqliteError::Query`] if it produced none.
    pub fn query_params_one(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<Row, SqliteError> {
        self.conn.query_params_one(sql, params)
    }

    /// Run a parameterized query and return its first row, if any.
    pub fn query_params_opt(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
    ) -> Result<Option<Row>, SqliteError> {
        self.conn.query_params_opt(sql, params)
    }

    /// Stream a query's rows one at a time through `on_row`, zero-copy.
    pub fn query_each_sql<F, E>(&self, sql: &str, on_row: F) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.conn.query_each_sql(sql, on_row)
    }

    /// Parameterized peer of [`Self::query_each_sql`].
    pub fn query_each_params<F, E>(
        &self,
        sql: &str,
        params: &[ValueRef<'_>],
        on_row: F,
    ) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.conn.query_each_params(sql, params, on_row)
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — inside the
    /// transaction. See [`Connection::query`].
    ///
    /// # Errors
    ///
    /// A classified [`SqliteError`] on a prepare / step failure.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<TypedRows<Q>, SqliteError> {
        self.conn.query::<Q>(params)
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row — inside the
    /// transaction. See [`Connection::query_one`].
    ///
    /// # Errors
    ///
    /// [`SqliteError::Query`] on zero rows, [`SqliteError::TooManyRows`] on two or
    /// more, or a classified [`SqliteError`] on a prepare / step / decode failure.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<Q::Owned, SqliteError> {
        self.conn.query_one::<Q>(params)
    }

    /// Run a compile-checked `query!` expecting AT MOST one row — inside the
    /// transaction. See [`Connection::query_opt`].
    ///
    /// # Errors
    ///
    /// [`SqliteError::TooManyRows`] on two or more rows, or a classified
    /// [`SqliteError`] on a prepare / step / decode failure (zero rows is
    /// `Ok(None)`).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_opt<Q: SqliteTypedQuery>(
        &self,
        params: &[ValueRef<'_>],
    ) -> Result<Option<Q::Owned>, SqliteError> {
        self.conn.query_opt::<Q>(params)
    }

    /// Stream a compile-checked `query!`'s TYPED rows through `on_row` — inside
    /// the transaction. See [`Connection::query_each`].
    ///
    /// # Errors
    ///
    /// A classified [`SqliteError`] on a prepare / step / decode failure.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<Q, F, E>(
        &self,
        params: &[ValueRef<'_>],
        on_row: F,
    ) -> Result<Option<E>, SqliteError>
    where
        Q: SqliteTypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        self.conn.query_each::<Q, F, E>(params, on_row)
    }
}

// ─── Typed flagship (compile-checked query!) ─────────────────────────────────

/// A borrowed VIEW of one eager-result row that reads columns straight from the
/// shared arena — the eager-path [`ColumnSource`] the typed decode consumes.
///
/// Unlike the owned [`Row`] handle (an `Arc`-clone whose `value_ref` lends the
/// arena for the RECEIVER'S borrow), this holds a `&'a ArenaInner` and lends
/// each cell for `'a` — the CONTAINER'S lifetime. That is what lets a typed
/// borrowed record ([`SqliteTypedQuery::Record`]) decoded through it outlive the
/// per-row view and borrow the [`TypedRows`] buffer directly (a `&'a str` field
/// aliases the arena, zero-copy), exactly like the PostgreSQL typed path's
/// records aliasing their prebuffer.
struct ArenaRowRef<'a> {
    arena: &'a ArenaInner,
    row_idx: u32,
}

impl<'a> ColumnSource<'a> for ArenaRowRef<'a> {
    fn cell(&self, col: usize) -> Result<ValueRef<'a>, SqliteError> {
        // `self.arena` is `&'a ArenaInner` (a `Copy` reference read through
        // `&self`), so `resolve_cell` returns `ValueRef<'a>` — the arena's
        // lifetime, not the receiver's.
        resolve_cell(self.arena, self.row_idx, col)
    }

    fn column_count(&self) -> usize {
        usize::from(self.arena.n_cols)
    }
}

impl RowSet {
    /// A lazy iterator over the rows as arena VIEWS (each borrows the shared
    /// arena for `&self`), for the typed decode. No per-row allocation, no
    /// pre-materialised `Vec`.
    fn arena_rows(&self) -> impl Iterator<Item = ArenaRowRef<'_>> + '_ {
        // `n_rows > 0` implies `arena.is_some()` (the `ArenaBuilder::finish`
        // invariant), so this yields exactly `n_rows` views; a rowless set (no
        // arena) yields none.
        self.arena
            .as_ref()
            .into_iter()
            .flat_map(|arena| (0..self.n_rows).map(move |row_idx| ArenaRowRef { arena, row_idx }))
    }
}

/// The bounded, typed result of a compile-checked `query!` over SQLite.
///
/// Holds one eager [`QueryResult`] (a single shared arena — a `data` byte pool +
/// a `CellSlot` table, `Arc`-shared) and decodes it lazily into the query's
/// typed records: [`iter`](Self::iter) yields the borrowed record
/// `Q::Record<'_>` (text/blob columns alias the arena — zero-copy), and
/// [`into_owned`](Self::into_owned) yields the `'static` owned twin. This mirrors
/// the PostgreSQL typed `Rows<Q>`: a constant number of allocations per result,
/// ZERO per row (a decoded borrowed record is built by value from arena cells).
///
/// # Borrow discipline (compiler-enforced escape wall)
///
/// A borrowed record from [`iter`](Self::iter) borrows `self`, so it cannot
/// outlive the `TypedRows`: holding one past a drop is an `E0505` borrow error. A
/// row that must outlive the buffer goes through [`into_owned`](Self::into_owned).
#[must_use = "a TypedRows holds the query's result; read it via iter() or into_owned()"]
pub struct TypedRows<Q: SqliteTypedQuery> {
    /// The eager result whose shared arena backs every decoded record. Private
    /// so the lazy arena shape cannot be bypassed.
    result: QueryResult,
    /// Pins the row type without owning a `Q`. `fn() -> Q` is covariant in `Q`
    /// and imposes no auto-trait bound on the uninhabited carrier.
    _q: PhantomData<fn() -> Q>,
}

impl<Q: SqliteTypedQuery> TypedRows<Q> {
    /// The number of result rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.result.len()
    }

    /// Whether the result produced no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.result.is_empty()
    }

    /// Decode the rows lazily into borrowed records.
    ///
    /// A plain iterator — records can coexist, be `collect`ed, or random-
    /// accessed (each `next` re-decodes from the arena). Each item is the
    /// borrowed record or a classified [`SqliteError`] for a row whose value
    /// storage classes do not match the record's declared field types (a
    /// mismatch or an unexpected `NULL`) — never a silent coercion.
    pub fn iter(&self) -> impl Iterator<Item = Result<Q::Record<'_>, SqliteError>> + '_ {
        self.result.rows.arena_rows().map(|src| Q::decode_row(&src))
    }

    /// Decode every row into the owned twin, allocating one owned buffer per
    /// text/blob cell. The owned records outlive the arena.
    ///
    /// # Errors
    ///
    /// The first row whose value storage classes do not match the record's
    /// declared field types is a classified [`SqliteError`] — the whole call
    /// fails rather than returning a partial vector.
    pub fn into_owned(self) -> Result<Vec<Q::Owned>, SqliteError> {
        let mut out = Vec::with_capacity(self.result.len());
        for src in self.result.rows.arena_rows() {
            out.push(Q::decode_row_owned(&src)?);
        }
        Ok(out)
    }
}

impl<Q: SqliteTypedQuery> core::fmt::Debug for TypedRows<Q> {
    /// Hand-written (not derived): the derive would demand `Q: Debug`, but the
    /// carrier `Q` is an uninhabited marker. `PhantomData<fn() -> Q>` needs no
    /// `Q: Debug`, so the impl is bound-free.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TypedRows").field("rows", &self.result.len()).finish()
    }
}

#[cfg(test)]
mod arena_tests {
    //! Unit tests for the shared-arena seal — private types (`ArenaBuilder`,
    //! `RowSet`) driven directly, no live SQLite. Real SQLite caps a result's
    //! column count far below `u16::MAX`, so the overflow guard can only be
    //! reached with a fabricated width here; that this arm is exercised is what
    //! keeps [`SqliteError::ResultTooLarge`] a real, wired guard rather than a
    //! manufactured variant.

    use super::{ArenaBuilder, SqliteError};
    use std::sync::Arc;

    #[test]
    fn oversize_column_count_seals_as_result_too_large() {
        // A width past `u16::MAX` sets the sticky overflow flag; `finish` must
        // reject it loudly, never mis-address against a truncated stride.
        let mut b = ArenaBuilder::new(usize::from(u16::MAX) + 1);
        b.push_ref(rusqlite::types::ValueRef::Integer(1));
        b.end_row();
        let names: Arc<[String]> = Arc::from(Vec::<String>::new());
        assert!(matches!(b.finish(names), Err(SqliteError::ResultTooLarge)));
    }

    #[test]
    fn oversize_row_count_seals_as_result_too_large() {
        // A pure-integer result writes ZERO arena bytes, so the byte overflow
        // guard never fires — the ROW COUNTER needs its own loud guard. Preset
        // the counter at the `u32` ceiling (a real 4-billion-row eager result
        // would exhaust the slot table first, so the counter is driven via the
        // private field here); one more `end_row` must overflow it, and the seal
        // must fail loud rather than silently saturate `len()`.
        let mut b = ArenaBuilder::new(1);
        b.rows = u32::MAX;
        b.push_ref(rusqlite::types::ValueRef::Integer(1));
        b.end_row();
        let names: Arc<[String]> = Arc::from(vec!["x".to_string()]);
        assert!(matches!(b.finish(names), Err(SqliteError::ResultTooLarge)));
    }

    #[test]
    fn rowless_seal_allocates_no_arena() {
        // The `arena.is_some() == (n_rows > 0)` invariant: a seal with no rows
        // produces an arena-less `RowSet` that mints no handle.
        let b = ArenaBuilder::new(2);
        let names: Arc<[String]> = Arc::from(vec!["a".to_string(), "b".to_string()]);
        let rs = b.finish(names).expect("empty seal");
        assert!(rs.is_empty());
        assert_eq!(rs.len(), 0);
        assert!(rs.get(0).is_none());
    }
}
