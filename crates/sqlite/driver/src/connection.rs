use core::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::error::SqliteError;
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
/// (stream it via [`Connection::query_each`] instead — that path has no cap).
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
        let inner = &*self.arena;
        let n_cols = usize::from(inner.n_cols);
        if col >= n_cols {
            return Err(SqliteError::ColumnIndexOutOfBounds { index: col, count: n_cols });
        }
        // The arena is built by `ArenaBuilder` with `slots.len() == n_rows *
        // n_cols` and every text/blob range in-bounds, and a `Row` is minted
        // only for `row_idx < n_rows`, so for an in-range `col` the slot and
        // byte lookups below are total BY CONSTRUCTION. The `?` / `.get()`
        // fail-closed arms are the architecturally unreachable dead path — never
        // a panic, never an out-of-bounds index, never a fabricated value —
        // mirroring the PostgreSQL arena's fail-closed shape.
        let corrupt = || SqliteError::Query("arena slot resolution failed (invariant violated)".to_owned());
        let row_base = usize::try_from(self.row_idx).map_err(|_| corrupt())?;
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
        Ok(Self { inner })
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
        Ok(Self { inner })
    }

    /// Set how long a locked-database operation waits for the lock before
    /// returning a CLASSIFIED busy error (`SqliteError::is_busy()`), never a
    /// hang. Overrides the [`DEFAULT_BUSY_TIMEOUT`] the `open` constructors set;
    /// `Duration::ZERO` disables the wait entirely (a contended operation fails
    /// IMMEDIATELY, the honest fail-loud with no hidden blocking).
    pub fn set_busy_timeout(&self, timeout: Duration) -> Result<(), SqliteError> {
        self.inner.busy_timeout(timeout).map_err(SqliteError::from)
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
    pub fn query(&self, sql: &str) -> Result<QueryResult, SqliteError> {
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

    /// Shared eager-collect core for [`Self::query`] / [`Self::query_params`].
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
        params: &[ValueRef<'_>],
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
    pub fn query_one(&self, sql: &str) -> Result<Row, SqliteError> {
        self.query(sql)?
            .get(0)
            .ok_or_else(|| SqliteError::Query("query returned no rows".to_owned()))
    }

    /// Run a query and return its first row, if any.
    pub fn query_opt(&self, sql: &str) -> Result<Option<Row>, SqliteError> {
        Ok(self.query(sql)?.get(0))
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
        match f(&tx) {
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
    pub fn query(&self, sql: &str) -> Result<QueryResult, SqliteError> {
        self.conn.query(sql)
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
    pub fn query_one(&self, sql: &str) -> Result<Row, SqliteError> {
        self.conn.query_one(sql)
    }

    /// Run a query and return its first row, if any.
    pub fn query_opt(&self, sql: &str) -> Result<Option<Row>, SqliteError> {
        self.conn.query_opt(sql)
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
    pub fn query_each<F, E>(&self, sql: &str, on_row: F) -> Result<Option<E>, SqliteError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.conn.query_each(sql, on_row)
    }

    /// Parameterized peer of [`Self::query_each`].
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
