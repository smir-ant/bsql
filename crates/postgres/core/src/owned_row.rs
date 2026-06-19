//! `OwnedRow` — the single-allocation owned row representation.
//!
//! This is the keystone "owned escape" of the converged row model: when a
//! borrowed row (lent from the read buffer) must outlive the buffer, it is
//! copied once into an `OwnedRow`. The whole row — column metadata *and*
//! cell bytes — lives in **one** `Box<[u8]>`, so an owned row is exactly one
//! heap allocation (the earlier design assumed two; that was unnecessary).
//!
//! `OwnedRow` is `Send + Sync + 'static + Clone` (clone = one buffer copy),
//! contains no `unsafe`, and never panics on access.
//!
//! ## Layout (one `Box<[u8]>`, native-endian)
//!
//! ```text
//! [ count: u32 ] [ (off: u32, len_plus_one: u32) × count ] [ cell bytes … ]
//! ```
//!
//! - `count` — number of columns.
//! - per column: `off` is the byte offset of the cell's data **within the
//!   data region** (which starts right after the header); `len_plus_one`
//!   encodes the cell length with a niche: **`0` means SQL NULL**, and a
//!   non-zero `n` means a value of length `n - 1`. So an *empty* value
//!   (empty string / empty bytea) is `len_plus_one == 1` and is provably
//!   distinct from NULL (`0`) — there is **no `u32::MAX` sentinel**, matching
//!   the niche discipline of the rest of the crate (NULL = absence of a
//!   value, never a magic length).
//!
//! Out-of-range column access and SQL NULL are also kept distinct: see
//! [`OwnedRow::cell`].

/// The row was too large to represent: more columns than `u32::MAX`, or a
/// total/offset/length that overflows the 32-bit fields. Never silently
/// truncated — construction fails loudly instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedRowTooLarge;

impl core::fmt::Display for OwnedRowTooLarge {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("row too large to encode as an OwnedRow (exceeds u32 bounds)")
    }
}

impl std::error::Error for OwnedRowTooLarge {}

/// A single-allocation owned row. See the module docs for the layout.
#[derive(Debug, Clone)]
#[must_use]
pub struct OwnedRow {
    // Private + sole constructor `from_cells` ⇒ the header is always
    // well-formed, so the "impossible" branches in the accessors below are
    // genuinely unreachable, not silent fallbacks.
    data: Box<[u8]>,
}

const HEADER_PREFIX: usize = 4; // the leading `count: u32`
const SLOT_BYTES: usize = 8; // (off: u32, len_plus_one: u32)

impl OwnedRow {
    /// Build an owned row from borrowed cells. `None` = SQL NULL,
    /// `Some(bytes)` = a value (possibly empty). One allocation.
    ///
    /// Fails with [`OwnedRowTooLarge`] if the row cannot fit the 32-bit
    /// fields — never truncates.
    pub fn from_cells(cells: &[Option<&[u8]>]) -> Result<Self, OwnedRowTooLarge> {
        let count = cells.len();
        // count must fit u32.
        u32::try_from(count).map_err(|_| OwnedRowTooLarge)?;

        // header = prefix + count slots
        let slots_bytes = count.checked_mul(SLOT_BYTES).ok_or(OwnedRowTooLarge)?;
        let header = HEADER_PREFIX.checked_add(slots_bytes).ok_or(OwnedRowTooLarge)?;

        // data region length = Σ cell lengths; each len+1 must fit u32, and
        // each running offset must fit u32.
        let mut data_len: usize = 0;
        for bytes in cells.iter().flatten() {
            let len = bytes.len();
            // len_plus_one must fit u32 ⇒ len <= u32::MAX - 1.
            let len_u32 = u32::try_from(len).map_err(|_| OwnedRowTooLarge)?;
            len_u32.checked_add(1).ok_or(OwnedRowTooLarge)?;
            // running offset must fit u32.
            u32::try_from(data_len).map_err(|_| OwnedRowTooLarge)?;
            data_len = data_len.checked_add(len).ok_or(OwnedRowTooLarge)?;
        }
        // final offset bound check.
        u32::try_from(data_len).map_err(|_| OwnedRowTooLarge)?;
        let total = header.checked_add(data_len).ok_or(OwnedRowTooLarge)?;

        let mut buf: Vec<u8> = vec![0u8; total];

        // count
        write_u32(&mut buf, 0, u32::try_from(count).map_err(|_| OwnedRowTooLarge)?)
            .ok_or(OwnedRowTooLarge)?;

        // slots + data
        let mut off: u32 = 0;
        for (i, cell) in cells.iter().enumerate() {
            let slot_at = HEADER_PREFIX
                .checked_add(i.checked_mul(SLOT_BYTES).ok_or(OwnedRowTooLarge)?)
                .ok_or(OwnedRowTooLarge)?;
            match cell {
                None => {
                    // NULL: len_plus_one = 0; off value irrelevant.
                    write_u32(&mut buf, slot_at, off).ok_or(OwnedRowTooLarge)?;
                    let lenp1_at = slot_at.checked_add(4).ok_or(OwnedRowTooLarge)?;
                    write_u32(&mut buf, lenp1_at, 0).ok_or(OwnedRowTooLarge)?;
                }
                Some(bytes) => {
                    let len = bytes.len();
                    let len_u32 = u32::try_from(len).map_err(|_| OwnedRowTooLarge)?;
                    let lenp1 = len_u32.checked_add(1).ok_or(OwnedRowTooLarge)?;
                    write_u32(&mut buf, slot_at, off).ok_or(OwnedRowTooLarge)?;
                    let lenp1_at = slot_at.checked_add(4).ok_or(OwnedRowTooLarge)?;
                    write_u32(&mut buf, lenp1_at, lenp1).ok_or(OwnedRowTooLarge)?;
                    // copy bytes into the data region.
                    let off_usize = usize::try_from(off).map_err(|_| OwnedRowTooLarge)?;
                    let dstart = header.checked_add(off_usize).ok_or(OwnedRowTooLarge)?;
                    let dend = dstart.checked_add(len).ok_or(OwnedRowTooLarge)?;
                    buf.get_mut(dstart..dend)
                        .ok_or(OwnedRowTooLarge)?
                        .copy_from_slice(bytes);
                    off = off.checked_add(len_u32).ok_or(OwnedRowTooLarge)?;
                }
            }
        }

        Ok(Self { data: buf.into_boxed_slice() })
    }

    /// Number of columns.
    #[must_use]
    pub fn col_count(&self) -> usize {
        // Dead-arm fallback, not a silent data substitution: `data` is only
        // ever produced by this type's sole constructor, which always writes
        // a valid 4-byte column-count header first, so `read_u32(.., 0)` is
        // always `Some` and the `usize::try_from` widening never fails on a
        // >=32-bit target. The `0` is the forbid-bundle-compliant landing for
        // a branch unreachable under the type's own invariant.
        #[allow(clippy::disallowed_methods, reason = "dead arm under the sole-constructor invariant (header always present, widening infallible on >=32-bit); 0 is the unreachable landing, not a data default")]
        let count = read_u32(&self.data, 0)
            .and_then(|c| usize::try_from(c).ok())
            .unwrap_or(0);
        count
    }

    /// Access one cell.
    ///
    /// - `None` — `col` is out of range.
    /// - `Some(None)` — the cell is SQL NULL.
    /// - `Some(Some(bytes))` — a value (`bytes` may be empty, which is
    ///   distinct from NULL).
    #[must_use]
    pub fn cell(&self, col: usize) -> Option<Option<&[u8]>> {
        if col >= self.col_count() {
            return None;
        }
        let slot_at = HEADER_PREFIX.checked_add(col.checked_mul(SLOT_BYTES)?)?;
        let off = read_u32(&self.data, slot_at)?;
        let lenp1 = read_u32(&self.data, slot_at.checked_add(4)?)?;
        if lenp1 == 0 {
            return Some(None); // SQL NULL
        }
        let len = usize::try_from(lenp1.checked_sub(1)?).ok()?;
        let count = self.col_count();
        let header = HEADER_PREFIX.checked_add(count.checked_mul(SLOT_BYTES)?)?;
        let dstart = header.checked_add(usize::try_from(off).ok()?)?;
        let dend = dstart.checked_add(len)?;
        Some(Some(self.data.get(dstart..dend)?))
    }

    /// `true` if the cell is SQL NULL. `None` if `col` is out of range.
    #[must_use]
    pub fn is_null(&self, col: usize) -> Option<bool> {
        self.cell(col).map(|v| v.is_none())
    }
}

fn read_u32(data: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let arr: [u8; 4] = data.get(at..end)?.try_into().ok()?;
    Some(u32::from_ne_bytes(arr))
}

fn write_u32(buf: &mut [u8], at: usize, v: u32) -> Option<()> {
    let end = at.checked_add(4)?;
    buf.get_mut(at..end)?.copy_from_slice(&v.to_ne_bytes());
    Some(())
}

// One allocation: an `OwnedRow` is exactly a `Box<[u8]>` (pointer + len).
const _: () = assert!(core::mem::size_of::<OwnedRow>() == core::mem::size_of::<Box<[u8]>>());

// The owned escape must be freely shareable across threads and outlive any
// borrow — this is the entire reason it exists.
const _: () = {
    const fn assert_send_sync_static<T: Send + Sync + 'static>() {}
    assert_send_sync_static::<OwnedRow>();
};

#[cfg(test)]
mod tests {
    use super::*;

    // `expect`/`unwrap` are crate-denied even in tests; build via match.
    fn build(cells: &[Option<&[u8]>]) -> OwnedRow {
        match OwnedRow::from_cells(cells) {
            Ok(r) => r,
            Err(e) => panic!("from_cells failed: {e}"),
        }
    }

    #[test]
    fn round_trip_distinguishes_empty_null_and_oob() {
        // col0 = "hi", col1 = NULL, col2 = "" (empty, NOT null).
        let row = build(&[Some(b"hi"), None, Some(b"")]);

        assert_eq!(row.col_count(), 3);
        assert_eq!(row.cell(0), Some(Some(&b"hi"[..]))); // value
        assert_eq!(row.cell(1), Some(None)); // SQL NULL
        assert_eq!(row.cell(2), Some(Some(&b""[..]))); // empty value, != NULL
        assert_eq!(row.cell(3), None); // out of range, != NULL

        assert_eq!(row.is_null(0), Some(false));
        assert_eq!(row.is_null(1), Some(true)); // NULL
        assert_eq!(row.is_null(2), Some(false)); // empty is not null
        assert_eq!(row.is_null(3), None); // OOB
    }

    #[test]
    fn empty_row_and_clone() {
        let row = build(&[]);
        assert_eq!(row.col_count(), 0);
        assert_eq!(row.cell(0), None);
        let cloned = row.clone();
        assert_eq!(cloned.col_count(), 0);
    }

    #[test]
    fn many_cells_offsets_are_correct() {
        let cells: Vec<Option<&[u8]>> =
            vec![Some(&b"alpha"[..]), None, Some(&b"b"[..]), Some(&b""[..]), Some(&b"gamma!"[..])];
        let row = build(&cells);
        assert_eq!(row.col_count(), 5);
        assert_eq!(row.cell(0), Some(Some(&b"alpha"[..])));
        assert_eq!(row.cell(1), Some(None));
        assert_eq!(row.cell(2), Some(Some(&b"b"[..])));
        assert_eq!(row.cell(3), Some(Some(&b""[..])));
        assert_eq!(row.cell(4), Some(Some(&b"gamma!"[..])));
        assert_eq!(row.cell(5), None);
    }
}
