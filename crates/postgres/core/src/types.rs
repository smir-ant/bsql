use std::num::NonZeroU32;
use std::sync::Arc;

use bsql_postgres_proto::{Cell, DecodeError, TextFmt};

use crate::error::ColumnError;

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

/// Shared backing store for all rows in a query result: the concatenated cell
/// bytes plus the per-cell slot table, addressed by the fixed `n_cols` stride.
/// One `Arc<ArenaInner>` backs every [`Row`] handle of a result. The row COUNT
/// is not stored here — it lives in the [`RowSet`] that owns the `Arc`, because
/// the arena only needs the stride (`n_cols`) to resolve a `(row_idx, col)`
/// cell; the count bounds the handle minting, which [`RowSet`] does.
#[derive(Debug)]
struct ArenaInner {
    data: Vec<u8>,
    slots: Vec<ColSlot>,
    n_cols: u16,
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
    /// Raw bytes of column `col`.
    ///
    /// - `Ok(Some(bytes))` — a non-NULL column's raw payload (borrowed from the
    ///   arena, zero-copy).
    /// - `Ok(None)` — the column is SQL `NULL`.
    /// - `Err(ColumnError::OutOfRange { .. })` — `col` is `>=` the row's column
    ///   count.
    ///
    /// SQL NULL and out-of-range are DISTINCT outcomes — never both collapsed
    /// into a single `None`, which is exactly the ambiguity the previous
    /// `Option<&[u8]>` return carried.
    pub fn get_raw(&self, col: usize) -> Result<Option<&[u8]>, ColumnError> {
        let inner = &*self.arena;
        let n_cols = usize::from(inner.n_cols);
        if col >= n_cols {
            // `n_cols` is the arena's `u16` stride (infallible `u16 -> u32`);
            // `col` is a caller-supplied `usize` that CAN exceed `u32` on a
            // 64-bit target, but such an index is trivially out of range, so
            // the pure diagnostic is capped rather than a forbidden unwrap.
            #[expect(
                clippy::manual_unwrap_or,
                reason = "unwrap_or is banned by the silent-fallback ledger; this explicit \
                          match is the sanctioned dead arm for the structurally-out-of-range cap"
            )]
            let col = match u32::try_from(col) {
                Ok(c) => c,
                Err(_) => u32::MAX,
            };
            return Err(ColumnError::OutOfRange { col, n_cols: u32::from(inner.n_cols) });
        }
        // Uniform-width arena: slot `(row_idx, col)` lives at
        // `row_idx * n_cols + col`, which `ArenaBuilder::finish` guarantees is
        // `< slots.len()` and whose value slot always resolves to in-bounds
        // data. The checked arithmetic and `.get()` below therefore cannot fail
        // for an in-range column on a well-formed arena; a `None` here would mean
        // the arena's construction invariant was violated (architecturally
        // unreachable). It is fail-closed to a classified decode error — never an
        // out-of-bounds index, and never a fabricated NULL that would mask the
        // inconsistency — mirroring the typed row-body path's `TruncatedRow`
        // fail-closed shape.
        let corrupt = || ColumnError::Decode(DecodeError::TruncatedRow);
        let row_base = usize::try_from(self.row_idx).map_err(|_| corrupt())?;
        let slot_idx = row_base
            .checked_mul(n_cols)
            .and_then(|b| b.checked_add(col))
            .ok_or_else(corrupt)?;
        let slot = inner.slots.get(slot_idx).ok_or_else(corrupt)?;
        let byte_len = match slot.byte_len() {
            // NULL is encoded by the slot's niche — a real absence, distinct from
            // any error path above.
            None => return Ok(None),
            Some(l) => l,
        };
        let len = usize::try_from(byte_len).map_err(|_| corrupt())?;
        let start = usize::try_from(slot.offset).map_err(|_| corrupt())?;
        let end = start.checked_add(len).ok_or_else(corrupt)?;
        let bytes = inner.data.get(start..end).ok_or_else(corrupt)?;
        Ok(Some(bytes))
    }

    /// Decode column `col` into any type proto's classified text-decode matrix
    /// covers (`i16`, `i32`, `i64`, `u32`, `bool`, `&str`).
    ///
    /// This is the single routing point for the typed accessors: it fetches the
    /// raw bytes (propagating SQL NULL as `Ok(None)` and out-of-range as
    /// `Err(ColumnError::OutOfRange)`) and decodes them through
    /// [`Cell<TextFmt>`](bsql_postgres_proto::Cell) — the same classified decoder
    /// the compile-checked `query!` path uses. A byte sequence that does not
    /// parse as `T` is a classified `Err(ColumnError::Decode(..))`, never a
    /// silently-swallowed `None`.
    ///
    /// - `Ok(Some(v))` — decoded value.
    /// - `Ok(None)` — SQL `NULL` (a dynamic read is nullable-by-default).
    /// - `Err(ColumnError::OutOfRange { .. })` — index past the row's width.
    /// - `Err(ColumnError::Decode(..))` — the bytes did not decode as `T`.
    ///
    /// Floating-point columns are read through the sibling methods
    /// [`get_f32`](Self::get_f32) / [`get_f64`](Self::get_f64), not this generic:
    /// PG's binary-uniform typed path never decodes a float from text, so proto's
    /// `Cell<TextFmt>` matrix has no float member — the two `get_fNN` methods are
    /// the classified `float4` / `float8` text path, with the same zero-swallow
    /// discipline.
    pub fn get<'a, T>(&'a self, col: usize) -> Result<Option<T>, ColumnError>
    where
        T: Cell<'a, TextFmt>,
    {
        match self.get_raw(col)? {
            None => Ok(None),
            Some(bytes) => T::decode(bytes).map(Some).map_err(ColumnError::Decode),
        }
    }

    /// Decode column `col` as UTF-8 text (`&str`), zero-copy.
    ///
    /// Non-UTF-8 bytes are a classified `Err(ColumnError::Decode(NonUtf8))` —
    /// the previous `.ok()` on `from_utf8` silently dropped that failure.
    pub fn get_str(&self, col: usize) -> Result<Option<&str>, ColumnError> {
        self.get::<&str>(col)
    }

    /// Decode column `col` as `i32` (PG `int4` text format).
    pub fn get_i32(&self, col: usize) -> Result<Option<i32>, ColumnError> {
        self.get::<i32>(col)
    }

    /// Decode column `col` as `i64` (PG `int8` text format).
    pub fn get_i64(&self, col: usize) -> Result<Option<i64>, ColumnError> {
        self.get::<i64>(col)
    }

    /// Decode column `col` as `bool` (PG boolean text format: `"t"` / `"f"`).
    pub fn get_bool(&self, col: usize) -> Result<Option<bool>, ColumnError> {
        self.get::<bool>(col)
    }

    /// Decode column `col` as `f32` (PG `float4` text format).
    ///
    /// The `float4` sibling of [`get_f64`](Self::get_f64), with the exact same
    /// classified hand-path: PostgreSQL's binary-uniform typed path never decodes
    /// a float from text, so proto's `Cell` matrix has no text-float decoder and
    /// this driver-layer decoder is the sole classified `float4` text path. UTF-8
    /// is validated (`Err(ColumnError::Decode(NonUtf8))`) and a non-float parse is
    /// a classified `Err(ColumnError::FloatParse)` — never a swallowed `None`.
    /// Accepts PG's `NaN` / `Infinity` / `-Infinity` spellings (Rust's `f32`
    /// parser is case-insensitive on those).
    pub fn get_f32(&self, col: usize) -> Result<Option<f32>, ColumnError> {
        match self.get_raw(col)? {
            None => Ok(None),
            Some(bytes) => {
                let text = core::str::from_utf8(bytes)
                    .map_err(|_| ColumnError::Decode(DecodeError::NonUtf8))?;
                let value: f32 = text.parse().map_err(|_| ColumnError::FloatParse)?;
                Ok(Some(value))
            }
        }
    }

    /// Decode column `col` as `f64` (PG `float8` text format).
    ///
    /// PostgreSQL's binary-uniform typed path never decodes a float from text, so
    /// proto's `Cell` matrix has no text-float decoder; this driver-layer decoder
    /// is the sole classified `float8` text path (its `float4` peer is
    /// [`get_f32`](Self::get_f32) — the float story is symmetric). UTF-8 is
    /// validated (`Err(ColumnError::Decode(NonUtf8))`) and a non-float parse is a
    /// classified `Err(ColumnError::FloatParse)` — never a swallowed `None`.
    /// Accepts PG's `NaN` / `Infinity` / `-Infinity` spellings (Rust's `f64`
    /// parser is case-insensitive on those).
    pub fn get_f64(&self, col: usize) -> Result<Option<f64>, ColumnError> {
        match self.get_raw(col)? {
            None => Ok(None),
            Some(bytes) => {
                let text = core::str::from_utf8(bytes)
                    .map_err(|_| ColumnError::Decode(DecodeError::NonUtf8))?;
                let value: f64 = text.parse().map_err(|_| ColumnError::FloatParse)?;
                Ok(Some(value))
            }
        }
    }

    /// Whether column `col` exists AND is SQL `NULL`.
    ///
    /// Returns `false` for a present value and for an out-of-range index (an
    /// absent column is not a NULL value). Use [`get_raw`](Self::get_raw) to tell
    /// out-of-range (`Err`) from NULL (`Ok(None)`) — this helper deliberately
    /// answers only the narrow "is this in-range column NULL?" question.
    pub fn is_null(&self, col: usize) -> bool {
        matches!(self.get_raw(col), Ok(None))
    }

    pub fn len(&self) -> usize { usize::from(self.arena.n_cols) }
    pub fn is_empty(&self) -> bool { self.arena.n_cols == 0 }

    /// Decode the column named `name` (resolved against `column_names`) into any
    /// type proto's classified text-decode matrix covers.
    ///
    /// - `Err(ColumnError::UnknownColumn)` — no column with that name.
    /// - otherwise identical to [`get`](Self::get) for the resolved index.
    pub fn get_by_name<'a, T>(
        &'a self,
        name: &str,
        column_names: &[String],
    ) -> Result<Option<T>, ColumnError>
    where
        T: Cell<'a, TextFmt>,
    {
        match column_names.iter().position(|n| n == name) {
            Some(idx) => self.get::<T>(idx),
            None => Err(ColumnError::UnknownColumn),
        }
    }
}

// ─── ArenaSealError ─────────────────────────────────────────

/// Why sealing an [`ArenaBuilder`] into [`Row`] handles failed.
///
/// The arena addresses every cell by a single per-row stride (`n_cols`), so it
/// can only faithfully represent rows that all share that shape and fit its
/// 32-bit fields. Both failure modes are rejected loudly at
/// [`ArenaBuilder::finish`] rather than sealed into an arena that would
/// mis-address or truncate cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaSealError {
    /// A result row (or the whole arena) could not be represented within the
    /// 32-bit on-arena fields: more columns than `u16`, or a cell offset/length
    /// that overflows `u32`. Rejected instead of saturating to a sentinel that
    /// would silently mis-address subsequent cells.
    TooLarge,
    /// The rows fed to one arena did not all have the same column count. A
    /// single arena's fixed stride addresses row `r` column `c` at slot
    /// `n_cols * r + c`; a row whose width differs from the first would
    /// mis-address every following cell. A heterogeneous batch — a
    /// multi-statement `simple_query` whose statements return different column
    /// counts — is therefore rejected rather than returned with cells read from
    /// the wrong offsets. The uniform-width invariant `finish` relies on is thus
    /// held by construction: the builder never seals a non-uniform arena.
    MixedRowWidth,
}

impl core::fmt::Display for ArenaSealError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TooLarge => {
                f.write_str("result row too large to encode (exceeds 32-bit arena bounds)")
            }
            Self::MixedRowWidth => f.write_str(
                "result rows had different column counts \
                 (a mixed-width multi-statement batch cannot be one result set)",
            ),
        }
    }
}

impl std::error::Error for ArenaSealError {}

// Footprint pin: a fieldless two-variant enum — one discriminant byte, no
// payload. A future variant carrying data would widen every fallible row-build
// `Result`; the pin catches that.
crate::footprint_pin!(ArenaSealError, size = 1, align = 1);

// ─── ArenaBuilder ───────────────────────────────────────────

/// Builds the shared arena during streaming. One builder per query.
/// `finish()` seals and produces the Arc-shared arena + Row handles.
///
/// Any cell offset/length or column count that would overflow the 32-bit
/// arena fields sets a sticky overflow flag rather than saturating; `finish()`
/// converts that flag into [`ArenaSealError::TooLarge`] so a too-large result
/// fails loudly instead of returning silently corrupted rows.
///
/// # Uniform-width invariant
///
/// Every sealed arena addresses cells by the fixed stride `n_cols` (row `r`,
/// column `c` lives at slot `n_cols * r + c`). The builder therefore records
/// the column count of each row and refuses to seal an arena whose rows are not
/// all `n_cols` wide: a differing row sets a sticky mismatch flag that
/// `finish()` converts into [`ArenaSealError::MixedRowWidth`]. This makes the
/// stride correct **by construction** — a heterogeneous batch cannot produce
/// mis-addressed cells, only a loud error.
pub struct ArenaBuilder {
    data: Vec<u8>,
    slots: Vec<ColSlot>,
    n_cols: u16,
    rows_finished: u32,
    /// Columns pushed for the row currently being built (since the last
    /// `end_row`). Checked against `n_cols` at `end_row` to enforce the
    /// uniform-width invariant.
    cols_in_row: u32,
    /// Set when a bound was exceeded; sealed into [`ArenaSealError::TooLarge`]
    /// by `finish()`.
    overflow: bool,
    /// Set when a completed row's column count differed from `n_cols`; sealed
    /// into [`ArenaSealError::MixedRowWidth`] by `finish()`.
    width_mismatch: bool,
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
            cols_in_row: 0,
            overflow,
            width_mismatch: false,
        }
    }

    pub fn push_value(&mut self, bytes: &[u8]) {
        // Count this column toward the current row's width (checked at end_row).
        self.cols_in_row = self.cols_in_row.saturating_add(1);
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
        // Count this column toward the current row's width (checked at end_row).
        self.cols_in_row = self.cols_in_row.saturating_add(1);
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
        // Enforce the uniform-width invariant: a completed row must contribute
        // exactly `n_cols` slots, or the fixed stride would mis-address it.
        if self.cols_in_row != u32::from(self.n_cols) {
            self.width_mismatch = true;
        }
        self.cols_in_row = 0;
        self.rows_finished = self.rows_finished.saturating_add(1);
    }

    /// Seal the arena into a [`RowSet`] — ONE shared `Arc` over the row bytes,
    /// with per-row [`Row`] handles minted lazily on access. This does NOT
    /// eagerly build a `Vec<Row>` of N handles: the whole result costs the
    /// arena's allocations regardless of row count, and a single-row read
    /// ([`RowSet::get`]) clones the `Arc` exactly once, not N times.
    ///
    /// # Errors
    ///
    /// [`ArenaSealError::TooLarge`] if any column count, offset, or length
    /// overflowed the 32-bit fields; [`ArenaSealError::MixedRowWidth`] if the
    /// fed rows did not all have the same column count (which would mis-address
    /// cells against the arena's single stride).
    pub fn finish(self) -> Result<RowSet, ArenaSealError> {
        if self.overflow {
            return Err(ArenaSealError::TooLarge);
        }
        if self.width_mismatch {
            return Err(ArenaSealError::MixedRowWidth);
        }
        let n_rows = self.rows_finished;
        // A rowless seal allocates NO arena — the invariant `arena.is_some() ==
        // (n_rows > 0)` keeps `get` / `iter` from minting a handle over an
        // absent backing store.
        if n_rows == 0 {
            return Ok(RowSet { arena: None, n_rows: 0 });
        }
        let arena = Arc::new(ArenaInner {
            data: self.data,
            slots: self.slots,
            n_cols: self.n_cols,
        });
        Ok(RowSet { arena: Some(arena), n_rows })
    }
}

// ─── RowSet ──────────────────────────────────────────────────

/// The sealed rows of one dynamic result: ONE shared arena (or none, for a
/// command that returned no rows) plus the row count. Every [`Row`] handle is
/// minted ON DEMAND — a 16-byte `Arc`-clone — by [`get`](Self::get) /
/// [`iter`](Self::iter), so the whole set costs the arena's allocations
/// regardless of row count and NO eager `Vec<Row>` is ever built. A single-row
/// read is one `Arc` refcount bump, independent of the row count.
///
/// This is the dynamic-path parallel of the typed [`Rows<Q>`](crate::Rows): one
/// owned backing store, lazy per-row access, zero per-row allocation. It backs
/// [`QueryResult`], whose accessors delegate here; a caller normally reaches it
/// through the `QueryResult` facade rather than naming it directly.
#[derive(Debug, Clone, Default)]
pub struct RowSet {
    /// `None` = a command that produced no result set (e.g. `INSERT`/`UPDATE`):
    /// no arena is allocated. `Some` = the shared arena backing every row.
    /// Invariant (held by [`ArenaBuilder::finish`]): `Some` iff `n_rows > 0`.
    arena: Option<Arc<ArenaInner>>,
    /// The number of rows the arena addresses. A `u32`, matching the arena's
    /// 32-bit row-index field; the handle minters bound `row_idx` by it.
    n_rows: u32,
}

// Footprint pin: `Option<Arc<_>>` niche-packs to one 8-byte pointer (a null
// pointer encodes `None`), plus a `u32` row count = 12 B padded to 16. Widening
// `n_rows`, or adding a field, shows up here.
crate::footprint_pin!(RowSet, size = 16, align = 8);

impl RowSet {
    /// The number of result rows.
    #[must_use]
    #[expect(
        clippy::manual_unwrap_or_default,
        reason = "`unwrap_or_default()` is banned by the tier-4 silent-fallback ledger; \
                  this explicit match is the sanctioned dead arm for an infallible narrow \
                  (`usize >= 32` bits on every supported target, so `n_rows: u32` always fits)"
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

    /// The row at zero-based index `i`, or `None` if `i >= len()`.
    ///
    /// A 16-byte [`Row`] handle built on demand — O(1), exactly one `Arc`
    /// refcount bump, never a per-row allocation and never dependent on the row
    /// count. This is the single-row read the driver's `query_one_sql` /
    /// `query_opt_sql` route through, so a one-row fetch clones the `Arc` ONCE.
    #[must_use]
    pub fn get(&self, i: usize) -> Option<Row> {
        // A `usize` index past `u32::MAX` cannot address a `u32`-bounded arena,
        // so it is out of range — `None`, never a wrapped index.
        let idx = u32::try_from(i).ok()?;
        self.row_at(idx)
    }

    /// Mint the handle for `idx` if it is in range and an arena backs it. The
    /// shared helper behind [`get`](Self::get) and [`iter`](Self::iter).
    fn row_at(&self, idx: u32) -> Option<Row> {
        if idx >= self.n_rows {
            return None;
        }
        // `arena` is `Some` whenever `n_rows > 0` (the `finish` invariant), and
        // `idx < n_rows` here, so this maps rather than fabricating a handle
        // over an absent arena.
        self.arena
            .as_ref()
            .map(|arena| Row { arena: Arc::clone(arena), row_idx: idx })
    }

    /// A lazy iterator over the rows. Each yielded [`Row`] is one `Arc`-clone
    /// handle — no per-row allocation, no pre-materialised `Vec`. Mirrors
    /// [`Rows::iter`](crate::Rows::iter)'s lazy shape on the dynamic path.
    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        (0..self.n_rows).filter_map(move |idx| self.row_at(idx))
    }

    /// Materialise every row into an owned `Vec<Row>` — the escape hatch for a
    /// caller that truly needs a random-access owned collection.
    ///
    /// This pays the N `Arc`-clones plus the `16·N`-byte `Vec` the lazy
    /// container otherwise avoids; prefer [`iter`](Self::iter) / [`get`](Self::get)
    /// unless an owned `Vec` is genuinely required.
    #[must_use]
    pub fn into_vec(self) -> Vec<Row> {
        self.iter().collect()
    }
}

// ─── QueryResult ────────────────────────────────────────────

/// Result of a query — rows + command tag + column names.
///
/// The rows are held LAZILY: `QueryResult` owns one [`RowSet`] (a single shared
/// `Arc` over the row bytes), and its accessors mint [`Row`] handles on demand
/// rather than eagerly building a `Vec<Row>`. Read the rows with
/// [`iter`](Self::iter) (a lazy iterator), [`get`](Self::get) (O(1) random
/// access), or [`len`](Self::len) — or [`into_vec`](Self::into_vec) for the rare
/// owned-`Vec` case. This mirrors the typed [`Rows<Q>`](crate::Rows) container:
/// one owned backing store, zero per-row allocation, decode/handle-mint on read.
#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    /// The result rows, held as one shared arena (see [`RowSet`]). Private so
    /// the eager-`Vec<Row>` shape cannot be reintroduced by a caller; the
    /// row accessors below are the sole entry.
    rows: RowSet,
    /// The command tag (`"SELECT 5"`, `"INSERT 0 1"`, …); empty when none.
    pub command_tag: String,
    /// The result-column names, in column order.
    pub column_names: Arc<[String]>,
}

// Footprint pin: a `RowSet` (16 B: a niche-packed `Option<Arc>` + a `u32`) + a
// String (3 words) + an Arc<[_]> (2 words, fat pointer) = 56 B, down from the
// 64 B the old eager `Vec<Row>` field cost (a `RowSet` is 16 B vs a `Vec`'s 24).
// A new field, or swapping a field to a wider owned type, shows up here.
crate::footprint_pin!(QueryResult, size = 56, align = 8);

impl QueryResult {
    /// Assemble a result from its sealed rows and metadata. The row-container
    /// field is private, so this is the sole constructor — a caller cannot
    /// splice in an eager `Vec<Row>`.
    pub fn new(rows: RowSet, command_tag: String, column_names: Arc<[String]>) -> Self {
        Self { rows, command_tag, column_names }
    }

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
        let row = rows.get(0).expect("row 0");
        assert_eq!(row.get_raw(0), Ok(Some(&b"hi"[..])));
        assert!(row.is_null(1));
        // Out-of-range index is None, never a fabricated handle.
        assert!(rows.get(1).is_none());
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
        assert_eq!(rows.get(0).expect("row 0").get_raw(0), Ok(Some(&b"foobar"[..])));
    }

    #[test]
    fn arena_builder_rejects_too_many_columns() {
        // A column count beyond u16 cannot be addressed by the slot index; the
        // builder must fail loud at finish(), never saturate and mis-index.
        let ab = ArenaBuilder::new(usize::from(u16::MAX) + 1);
        assert_eq!(ab.finish().map(|_| ()), Err(ArenaSealError::TooLarge));
    }

    #[test]
    fn arena_builder_rejects_mixed_row_width() {
        // The first row (1 col) establishes the stride; a following 2-col row
        // would make `get_raw` read cells from the wrong offsets. The builder
        // must reject the heterogeneous arena at finish() rather than seal it.
        let mut ab = ArenaBuilder::new(1);
        ab.push_value(b"a");
        ab.end_row();
        ab.push_value(b"x");
        ab.push_value(b"y");
        ab.end_row();
        assert_eq!(ab.finish().map(|_| ()), Err(ArenaSealError::MixedRowWidth));
    }

    #[test]
    fn arena_builder_accepts_uniform_multi_row() {
        // Uniform-width rows across several `end_row`s seal cleanly — the width
        // guard must not reject a well-formed multi-row result.
        let mut ab = ArenaBuilder::new(2);
        ab.push_value(b"a");
        ab.push_null();
        ab.end_row();
        ab.push_value(b"b");
        ab.push_value(b"c");
        ab.end_row();
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected seal error: {e}"),
        };
        assert_eq!(rows.len(), 2);
        let r0 = rows.get(0).expect("row 0");
        let r1 = rows.get(1).expect("row 1");
        assert_eq!(r0.get_raw(0), Ok(Some(&b"a"[..])));
        assert!(r0.is_null(1));
        assert_eq!(r1.get_raw(0), Ok(Some(&b"b"[..])));
        assert_eq!(r1.get_raw(1), Ok(Some(&b"c"[..])));
        // The lazy `iter` walk yields the SAME values as random-access `get`.
        // (A raw slice borrows the per-item handle, so copy it out to compare.)
        let via_iter: Vec<Vec<u8>> = rows
            .iter()
            .map(|r| r.get_raw(0).expect("in range").expect("non-null").to_vec())
            .collect();
        assert_eq!(via_iter, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    /// Build a one-row arena whose single column holds `bytes` (non-NULL).
    fn one_col_row(bytes: &[u8]) -> RowSet {
        let mut ab = ArenaBuilder::new(1);
        ab.push_value(bytes);
        ab.end_row();
        match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected seal error: {e}"),
        }
    }

    #[test]
    fn typed_read_distinguishes_null_value_and_out_of_range() {
        // One row, two columns: col 0 = "42" (value), col 1 = SQL NULL. Every
        // outcome of a dynamic read must be a DISTINCT value — the old
        // `Option<T>` return collapsed all three of these into a bare `None`.
        let mut ab = ArenaBuilder::new(2);
        ab.push_value(b"42");
        ab.push_null();
        ab.end_row();
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected seal error: {e}"),
        };
        let row = rows.get(0).expect("row 0");
        // Present value → Ok(Some).
        assert_eq!(row.get_i32(0), Ok(Some(42)));
        // SQL NULL → Ok(None) — a value that happens to be absent, NOT an error.
        assert_eq!(row.get_i32(1), Ok(None));
        // Index past the row's width → a classified error, NOT a fake NULL.
        assert_eq!(row.get_i32(2), Err(ColumnError::OutOfRange { col: 2, n_cols: 2 }));
    }

    #[test]
    fn garbage_int_read_is_classified_not_swallowed() {
        // RED (old behaviour): `get_i32` was `get_str(col)?.parse().ok()`, so a
        // column holding non-integer text ("hello") parsed to `None` — silently
        // swallowed and INDISTINGUISHABLE from a SQL NULL.
        //
        // GREEN (now): the same read is a classified
        // `Err(ColumnError::Decode(DecodeError::IntParse))`. The value-was-NULL
        // outcome (`Ok(None)`) and the bytes-were-garbage outcome (`Err(..)`) are
        // now different values a caller can branch on.
        let rows = one_col_row(b"hello");
        assert_eq!(
            rows.get(0).expect("row 0").get_i32(0),
            Err(ColumnError::Decode(DecodeError::IntParse)),
        );
    }

    #[test]
    fn wrong_rust_type_for_column_surfaces_a_classified_error() {
        // Asking for the WRONG Rust type on the text wire surfaces as a classified
        // decode error rather than a silent misparse: an integer column decoded as
        // `bool` is `BoolParse`; a boolean column decoded as `i32` is `IntParse`.
        // (On the text wire every column is ASCII, so reading an int column as
        // `&str` is a legitimate text read — verified below — which is why a
        // separate OID pre-check adds little on this path.)
        let int_row = one_col_row(b"42");
        let int0 = int_row.get(0).expect("row 0");
        assert_eq!(
            int0.get_bool(0),
            Err(ColumnError::Decode(DecodeError::BoolParse)),
        );
        // The same int column read as text is a valid read, not an error.
        assert_eq!(int0.get_str(0), Ok(Some("42")));

        let bool_row = one_col_row(b"t");
        let bool0 = bool_row.get(0).expect("row 0");
        assert_eq!(
            bool0.get_i32(0),
            Err(ColumnError::Decode(DecodeError::IntParse)),
        );
        assert_eq!(bool0.get_bool(0), Ok(Some(true)));
    }

    #[test]
    fn non_utf8_text_read_is_classified() {
        // A lone 0xFF is not valid UTF-8; reading it as `&str` is a classified
        // `NonUtf8`, never a swallowed `None` (the old `get_str` did `.ok()` on
        // `from_utf8`).
        let rows = one_col_row(&[0xff]);
        assert_eq!(
            rows.get(0).expect("row 0").get_str(0),
            Err(ColumnError::Decode(DecodeError::NonUtf8)),
        );
    }

    #[test]
    fn float_read_classifies_null_value_and_parse_failure() {
        // Float text decode: value → Ok(Some), NULL → Ok(None), non-float text →
        // a classified FloatParse (never a swallowed None).
        let mut ab = ArenaBuilder::new(3);
        ab.push_value(b"2.5");
        ab.push_null();
        ab.push_value(b"not-a-float");
        ab.end_row();
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected seal error: {e}"),
        };
        let row = rows.get(0).expect("row 0");
        assert_eq!(row.get_f64(0), Ok(Some(2.5)));
        assert_eq!(row.get_f64(1), Ok(None));
        assert_eq!(row.get_f64(2), Err(ColumnError::FloatParse));
        // PG float specials parse through the same path.
        let specials = one_col_row(b"NaN");
        assert!(matches!(specials.get(0).expect("row 0").get_f64(0), Ok(Some(v)) if v.is_nan()));
    }

    #[test]
    fn f32_read_mirrors_f64_classification() {
        // `get_f32` is the `float4` sibling of `get_f64` with the identical
        // classified hand-path: value → Ok(Some), NULL → Ok(None), non-float text
        // → FloatParse, non-UTF-8 → Decode(NonUtf8). No swallowed None anywhere.
        let mut ab = ArenaBuilder::new(4);
        ab.push_value(b"2.5");
        ab.push_null();
        ab.push_value(b"not-a-float");
        ab.push_value(&[0xff]);
        ab.end_row();
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected seal error: {e}"),
        };
        let row = rows.get(0).expect("row 0");
        assert_eq!(row.get_f32(0), Ok(Some(2.5)));
        assert_eq!(row.get_f32(1), Ok(None));
        assert_eq!(row.get_f32(2), Err(ColumnError::FloatParse));
        assert_eq!(row.get_f32(3), Err(ColumnError::Decode(DecodeError::NonUtf8)));
        // Out-of-range stays a classified error, distinct from NULL.
        assert_eq!(row.get_f32(4), Err(ColumnError::OutOfRange { col: 4, n_cols: 4 }));
        // PG float specials parse through the same path.
        let specials = one_col_row(b"-Infinity");
        assert!(matches!(specials.get(0).expect("row 0").get_f32(0), Ok(Some(v)) if v.is_infinite() && v < 0.0));
    }

    #[test]
    fn get_by_name_resolves_or_classifies_unknown() {
        let rows = one_col_row(b"alice");
        let row = rows.get(0).expect("row 0");
        let names = vec!["name".to_string()];
        assert_eq!(row.get_by_name::<&str>("name", &names), Ok(Some("alice")));
        assert_eq!(
            row.get_by_name::<&str>("missing", &names),
            Err(ColumnError::UnknownColumn),
        );
    }

    /// N→1 clone witness: the single-row read (`query_one`'s path) mints EXACTLY
    /// one `Row` handle — one `Arc` refcount bump — regardless of the row count,
    /// where the retired eager `Vec<Row>` cloned the `Arc` once per row. Proven
    /// by `Arc::strong_count`: `get` on a 1000-row set lifts the count by one,
    /// not by a thousand.
    #[test]
    fn single_row_read_clones_the_arc_once_not_per_row() {
        const N: u32 = 1000;
        let mut ab = ArenaBuilder::new(1);
        for i in 0..N {
            ab.push_value(format!("row{i}").as_bytes());
            ab.end_row();
        }
        let rows = match ab.finish() {
            Ok(r) => r,
            Err(e) => panic!("unexpected seal error: {e}"),
        };
        assert_eq!(rows.len(), 1000);
        let arena = rows.arena.as_ref().expect("populated set has an arena");
        // Sealed: only the `RowSet` holds the arena.
        assert_eq!(Arc::strong_count(arena), 1);

        // The single-row read clones the `Arc` EXACTLY once — independent of the
        // 1000 rows behind it (the whole point of the lazy handle).
        let row0 = rows.get(0).expect("row 0");
        assert_eq!(Arc::strong_count(arena), 2, "get(0) is one clone, not N");
        assert_eq!(row0.get_str(0), Ok(Some("row0")));
        drop(row0);
        assert_eq!(Arc::strong_count(arena), 1, "the handle released its clone");

        // Value identity across the three read paths on the same backing store.
        assert_eq!(rows.get(999).expect("last").get_str(0), Ok(Some("row999")));
        let via_iter: Vec<Option<String>> = rows
            .iter()
            .map(|r| r.get_str(0).expect("decode").map(str::to_owned))
            .collect();
        assert_eq!(via_iter.len(), 1000);
        assert_eq!(via_iter.first(), Some(&Some("row0".to_string())));
        assert_eq!(via_iter.last(), Some(&Some("row999".to_string())));
        // The eager escape hatch materialises the SAME values (opt-in N clones).
        let owned = rows.into_vec();
        assert_eq!(owned.len(), 1000);
        assert_eq!(owned.first().expect("row 0").get_str(0), Ok(Some("row0")));
    }
}
