//! [`Rows<Q>`] — the bounded, typed result of a compile-checked `query!`.
//!
//! The macro path does NOT route rows through the Arc-arena [`Row`](crate::Row)
//! materialiser. Instead a driver collects every `DataRow` payload verbatim into
//! one owned prebuffer ([`RowsBuilder`]) with an INFALLIBLE sink, settles the
//! connection to a clean idle, and only THEN hands the caller a [`Rows<Q>`] that
//! decodes rows lazily into the macro's typed records.
//!
//! # Why the sink is infallible + decode is post-verb (tier-1 no-swallow)
//!
//! The engine's verb sink can only `Continue` (its break payload is
//! uninhabited), so [`RowsBuilder::feed`] cannot return a `Result` — it copies
//! the row bytes into the prebuffer, an operation that cannot fail in a way the
//! sink could report. Row DECODING happens afterwards, from
//! [`Rows::iter`] / [`Rows::into_owned`], AFTER the verb has already settled the
//! connection to a clean idle and the linear token is repooled. So a per-row
//! decode failure is a `Result` *item* (or an `Err` return) handed to the
//! caller — it CANNOT harm the connection, because by the time it is computed
//! the connection is already idle and reusable. The no-swallow guarantee is
//! structural: the copy sink never fails, and the decode that can fail runs
//! where a failure is just a value.
//!
//! # Allocation
//!
//! A [`Rows<Q>`] is two allocations per result — the `wire` byte vector and the
//! `slots` span vector — and zero per row. [`Rows::iter`] borrows the prebuffer
//! (text columns are `&str` aliases — zero-copy). [`Rows::into_owned`] pays one
//! `Vec` plus one `String` per text cell, and only when it is called.
//!
//! # Oversize rows (bounded reassembly, small rows untouched)
//!
//! A row that fits one engine buffer fill arrives whole as [`Surface::Row`] and
//! is appended to `wire` with a single `extend` — zero extra work, zero extra
//! allocation. A row WIDER than the engine's inline read buffer
//! (`READ_BUF_CAP` = 4096) cannot reside whole in that buffer, so the engine
//! streams it as [`Surface::RowChunk`] pieces terminated by
//! [`Surface::RowChunkEnd`]. [`RowsBuilder::feed`] REASSEMBLES those chunks by
//! appending each one contiguously into the SAME `wire` buffer (exactly where a
//! whole `Surface::Row` body would land) and recording one span at the
//! terminator, so a reassembled oversize row becomes an ordinary `(offset, len)`
//! span in `wire` indistinguishable from an inline row — it decodes through the
//! identical [`TypedQuery::decode_borrowed`] path. Only an oversize row (rare +
//! large) pays the per-chunk copy; the common small-row path is byte-identical
//! to before. This mirrors the dynamic [`Row`](crate::Row) materialiser, which
//! reassembles the same chunk stream into one contiguous `DataRow` body.
//!
//! [`Row`]: crate::Row

use std::marker::PhantomData;

use bsql_postgres_proto::engine::Surface;
use bsql_postgres_proto::{DecodeError, TypedQuery};

use crate::error::DbError;
use crate::materialize::{parse_error_response, DbErrorSink};

/// The per-ROW record in the prebuffer's span vector: one row's `DataRow`
/// payload LENGTH (from its 2-byte column-count header onward). Not per-column —
/// [`TypedQuery::decode_borrowed`] parses the columns out of one contiguous
/// `DataRow` payload itself.
///
/// The row's byte OFFSET into `wire` is NOT stored — it is the running
/// prefix-sum of every prior row's length, reconstructed by a `usize`
/// accumulator as [`Rows::iter`] / [`Rows::into_owned`] walk the spans in order.
/// Storing only the length is 4 B/row instead of the 16 B a
/// `{ offset: usize, len: usize }` slot cost — a 4× cut on a million-row result
/// (4 MB vs 16 MB) — and there is no random-access row getter (the only readers
/// are the two sequential walks), so the offset never needs to materialise. A
/// per-row payload length fits a `u32`: an inline row is bounded by the engine's
/// inline frame buffer (`READ_BUF_CAP` = 4096), and a REASSEMBLED oversize row
/// (the chunk-streamed wide-row case) is bounded by the PostgreSQL `DataRow`
/// message length field — a 4-byte signed integer, so under 2 GiB — both well
/// within a `u32`, while the cumulative offset stays a full `usize`.
type RowLen = u32;

// The per-row span footprint: one `u32` length (4 B), down from the 16 B a
// `{ offset: usize, len: usize }` slot cost. Widening `RowLen` back to a `usize`
// or a two-field struct fails this E0080 drift pin.
const _: () = assert!(
    core::mem::size_of::<RowLen>() == 4,
    "per-row span must be a single 4-byte length; the byte offset is a derived \
     prefix-sum of prior lengths, never stored",
);

/// Type-erased prebuffer collector fed by a driver's typed-query sink.
///
/// Q-agnostic by construction: it accumulates raw `DataRow` bytes plus their
/// per-row spans and the command metadata, with NO knowledge of the row type —
/// the `Q` is stamped on only at [`finish`](Self::finish), because decoding is
/// deferred. [`feed`](Self::feed) is infallible (the engine sink cannot report a
/// `Result`); any anomaly is parked and surfaced after the verb settles.
///
/// `#[doc(hidden)]`: an INTERNAL decode/collect seam, not a consumer API — the
/// drivers build it inside `Core`'s typed verbs and hand back a finished
/// [`Rows<Q>`], so a consumer never names it on the happy path. It stays `pub`
/// (reachable, not `pub(crate)`) ONLY so the query fixture's offline decode +
/// allocation tests can feed it synthetic `DataRow` bytes through the single
/// `bsql` dependency; it is kept OUT of the rendered public docs so it does not
/// masquerade as part of the consumer surface.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RowsBuilder {
    wire: Vec<u8>,
    slots: Vec<RowLen>,
    affected: u64,
    db_error: Option<DbError>,
    /// Bytes appended to `wire` so far for the IN-PROGRESS oversize (chunk-
    /// streamed) row, reset to 0 at each [`Surface::RowChunkEnd`]. It is 0
    /// whenever no oversize row is mid-reassembly — the common all-small-rows
    /// result never leaves it non-zero and never touches it. This is the ONLY
    /// new footprint: one `usize` in the TRANSIENT builder, NOT in the returned
    /// [`Rows<Q>`] (which still moves out only `wire` + `slots` + `affected`),
    /// so the documented "2 allocations per result, 0 per row" of a `Rows<Q>`
    /// is preserved exactly.
    oversize_len: usize,
}

impl RowsBuilder {
    /// A fresh, empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consume one surface event. Infallible — it copies row bytes and parks
    /// metadata, never failing in a way the engine sink could report.
    pub fn feed(&mut self, surface: Surface<'_>) {
        match surface {
            Surface::Row(body) => {
                self.wire.extend_from_slice(body);
                // Record the payload LENGTH only; the byte offset is the running
                // prefix-sum of prior lengths, reconstructed on the walk. An
                // inline row is bounded by the inline frame buffer
                // (`READ_BUF_CAP` = 4096; an oversize row instead arrives as the
                // `RowChunk` / `RowChunkEnd` pair below), so the narrow to `u32`
                // is structurally infallible here. The sink cannot return a
                // `Result`, so the dead arm saturates rather than storing a wrong
                // length: an (impossible) over-long body then over-runs `wire` at
                // decode → a classified `TruncatedRow`, never a silent corruption
                // of this or a later row's offset.
                let payload_len = match RowLen::try_from(body.len()) {
                    Ok(len) => len,
                    Err(_) => RowLen::MAX,
                };
                self.slots.push(payload_len);
            }
            // An oversize row (wider than `READ_BUF_CAP` = 4096) is streamed by
            // the engine as `RowChunk` pieces terminated by `RowChunkEnd`.
            // REASSEMBLE it into `wire` exactly where a whole `Surface::Row` body
            // would land: append each chunk contiguously and accumulate the
            // running length, so at the terminator the row is ONE contiguous
            // span in `wire` indistinguishable from an inline row — it decodes
            // through the identical `Q::decode_borrowed(row_body(..))` path. The
            // small-row path above is untouched (this arm never runs for a row
            // that fit one buffer fill).
            Surface::RowChunk(bytes) => {
                self.wire.extend_from_slice(bytes);
                // Cumulative across the row's chunks, so a full `usize`;
                // `saturating_add` never saturates in practice (the sum is the
                // row's body length, under 2 GiB) and a dead saturation only
                // over-records, failing CLOSED to a `TruncatedRow` at decode.
                self.oversize_len = self.oversize_len.saturating_add(bytes.len());
            }
            Surface::RowChunkEnd => {
                // The reassembled row is complete: record its total length as ONE
                // span, then reset the accumulator so the NEXT row (small or
                // oversize) starts clean. The `u32` narrow saturates only on a
                // PostgreSQL-impossible > 4 GiB row to `RowLen::MAX`, which
                // fails CLOSED to a classified `TruncatedRow` at decode, never a
                // silent wrong length.
                let payload_len = match RowLen::try_from(self.oversize_len) {
                    Ok(len) => len,
                    Err(_) => RowLen::MAX,
                };
                self.slots.push(payload_len);
                self.oversize_len = 0;
            }
            Surface::Deliver { tag, .. } => {
                // The affected-row count rides the command tag (`SELECT 3` →
                // 3); a tagless extended-protocol ack carries none.
                self.affected = match tag {
                    Some(t) => t.rows_or_zero(),
                    None => 0,
                };
            }
            Surface::Fail(body) => self.db_error = Some(parse_error_response(body)),
            // Asynchronous / COPY frames are not part of a typed row result.
            Surface::Notice(_)
            | Surface::Notify(_)
            | Surface::ParamStatus(_)
            | Surface::CopyData(_)
            | Surface::CopyDone => {}
        }
    }

    /// Stamp the row type and produce the finished [`Rows<Q>`]. Moves the
    /// prebuffer in — no copy. (The returned [`Rows`] is itself `#[must_use]`.)
    pub fn finish<Q: TypedQuery>(self) -> Rows<Q> {
        Rows {
            wire: self.wire,
            slots: self.slots,
            affected: self.affected,
            _q: PhantomData,
        }
    }
}

impl DbErrorSink for RowsBuilder {
    fn take_db_error(&mut self) -> Option<DbError> {
        self.db_error.take()
    }
}

/// The bounded, typed result of a compile-checked `query!`.
///
/// Holds the result's `DataRow` payloads in one owned prebuffer plus their
/// per-row spans, and decodes them lazily into the query's typed records:
/// [`iter`](Self::iter) yields the borrowed record `Q::Record<'_>` (text columns
/// alias the prebuffer — zero-copy), and [`into_owned`](Self::into_owned) yields
/// the `'static + Send` owned twin.
///
/// # Borrow discipline (compiler-enforced escape wall)
///
/// A borrowed record from [`iter`](Self::iter) borrows `self`, so it cannot
/// outlive the `Rows`: dropping the `Rows` while a record is still held is an
/// `E0505` borrow error. A row that must outlive the buffer goes through
/// [`into_owned`](Self::into_owned).
#[must_use = "a Rows holds the query's result; read it via iter() or into_owned()"]
pub struct Rows<Q: TypedQuery> {
    /// Every result `DataRow` payload, concatenated (each begins with its
    /// 2-byte column-count header).
    wire: Vec<u8>,
    /// One payload LENGTH per row into `wire`; the byte offset is the running
    /// prefix-sum of prior lengths, reconstructed on the sequential walk.
    slots: Vec<RowLen>,
    /// Affected-row count from the command tag.
    affected: u64,
    /// Pins the row type without owning a `Q`. `fn() -> Q` is covariant in `Q`
    /// and imposes no auto-trait bound on the uninhabited carrier.
    _q: PhantomData<fn() -> Q>,
}

impl<Q: TypedQuery> Rows<Q> {
    /// Number of result rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the result has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// The affected-row count reported by the command tag.
    #[must_use]
    pub fn affected(&self) -> u64 {
        self.affected
    }

    /// The contiguous `DataRow` payload at `offset` for `len` bytes.
    ///
    /// The `(offset, len)` pair is reconstructed by the caller's prefix-sum walk
    /// from the very bytes [`RowsBuilder::feed`] wrote into `wire`, so the slice
    /// always resolves; the `None` arm is fail-closed (classified, never an
    /// out-of-bounds index) against a future seam.
    fn row_body(wire: &[u8], offset: usize, len: usize) -> Result<&[u8], DecodeError> {
        wire.get(offset..)
            .and_then(|tail| tail.get(..len))
            .ok_or(DecodeError::TruncatedRow)
    }

    /// Decode the rows lazily into borrowed records.
    ///
    /// A plain iterator — records can coexist, be `collect`ed, or random-
    /// accessed (each `next` re-decodes from the prebuffer). Each item is the
    /// borrowed record or a classified [`DecodeError`] for a row whose bytes do
    /// not match the query's compile-time shape; a decode failure is an `Err`
    /// ITEM, never a connection fault (the connection settled before any decode
    /// runs).
    pub fn iter(&self) -> impl Iterator<Item = Result<Q::Record<'_>, DecodeError>> + '_ {
        let wire = self.wire.as_slice();
        // The byte offset is not stored per row: it is the running prefix-sum of
        // the prior rows' lengths, threaded through this `usize` accumulator as
        // the walk advances (the spans are visited strictly in order).
        // Cumulative, so it stays a full `usize` even when a per-row length is a
        // `u32`. `saturating_add` (the arithmetic wall forbids a bare `+`) never
        // saturates in practice — the sum equals `wire.len()` — and a dead
        // saturation would fail-closed to a classified `TruncatedRow`.
        let mut offset: usize = 0;
        self.slots.iter().map(move |&len| {
            // `usize >= u32` on every supported target, so the widen is
            // infallible; a failure is classified (not swallowed) as a
            // `TruncatedRow` rather than fed on as a wrong length.
            let len = usize::try_from(len).map_err(|_| DecodeError::TruncatedRow)?;
            let start = offset;
            offset = offset.saturating_add(len);
            Q::decode_borrowed(Self::row_body(wire, start, len)?)
        })
    }

    /// Decode every row into the owned twin, allocating one `Vec` plus one
    /// `String` per text cell. The owned records outlive the prebuffer.
    ///
    /// # Errors
    ///
    /// The first row whose bytes do not match the query's compile-time shape is
    /// a classified [`DecodeError`] — the whole call fails rather than returning
    /// a partial, silently-truncated vector.
    pub fn into_owned(self) -> Result<Vec<Q::Owned>, DecodeError> {
        let mut out = Vec::with_capacity(self.slots.len());
        // Same derived-offset walk as `iter`: the running prefix-sum reconstructs
        // each row's byte offset without a stored field.
        let mut offset: usize = 0;
        for &len in &self.slots {
            // Infallible widen (`usize >= u32`); a failure is a classified
            // `TruncatedRow`, never a swallowed default.
            let len = usize::try_from(len).map_err(|_| DecodeError::TruncatedRow)?;
            let start = offset;
            offset = offset.saturating_add(len);
            out.push(Q::decode_owned(Self::row_body(&self.wire, start, len)?)?);
        }
        Ok(out)
    }
}

impl<Q: TypedQuery> std::fmt::Debug for Rows<Q> {
    /// Hand-written (not derived): the derive would demand `Q: Debug`, but the
    /// carrier `Q` is an uninhabited marker that implements nothing. The
    /// `PhantomData<fn() -> Q>` needs no `Q: Debug`, so the impl is bound-free.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rows")
            .field("rows", &self.slots.len())
            .field("affected", &self.affected)
            .field("wire_bytes", &self.wire.len())
            .finish()
    }
}

#[cfg(test)]
mod reassembly_tests {
    //! Deterministic OFFLINE proof of the oversize-row REASSEMBLY bookkeeping in
    //! [`RowsBuilder::feed`]: that `RowChunk` pieces append into `wire`
    //! contiguously, that `RowChunkEnd` records ONE span equal to the chunk
    //! SUM, and that the running accumulator RESETS so a following row (small or
    //! oversize) is not corrupted. A dropped reset or a double span-push would
    //! otherwise pass the whole offline suite silently — the live PG witness
    //! needs a running server, and the testkit frames only whole rows (it cannot
    //! emit `RowChunk`). These feed synthetic [`Surface`] sequences straight into
    //! the builder and assert the raw prebuffer bytes + spans (no decode): the
    //! bytes are arbitrary since `feed` copies + measures, never parses.
    //!
    //! This module is a descendant of the one defining `RowsBuilder`, so it reads
    //! the private `wire` / `slots` / `oversize_len` fields directly — the exact
    //! state the reassembly maintains.

    use bsql_postgres_proto::engine::Surface;

    use super::RowsBuilder;

    #[test]
    fn oversize_row_between_small_rows_reassembles_and_resets() {
        // Row(3) → [RowChunk(2), RowChunk(2), RowChunkEnd] → Row(4). The middle
        // row is oversize (two chunks); its span must be the SUM (4), the wire
        // the exact concatenation, and the accumulator 0 after the terminator so
        // the trailing small row is clean.
        let mut b = RowsBuilder::new();
        b.feed(Surface::Row(b"AAA"));
        b.feed(Surface::RowChunk(b"BB"));
        b.feed(Surface::RowChunk(b"CC"));
        b.feed(Surface::RowChunkEnd);
        b.feed(Surface::Row(b"DDDD"));

        assert_eq!(b.slots, vec![3u32, 4u32, 4u32], "spans: small, chunk-sum, small");
        assert_eq!(b.wire, b"AAABBCCDDDD".to_vec(), "wire is the exact byte concatenation");
        assert_eq!(b.oversize_len, 0, "accumulator reset after RowChunkEnd");
    }

    #[test]
    fn two_chunked_rows_back_to_back_each_reset() {
        // Two oversize rows back-to-back: [RowChunk(2), RowChunk(1), End] then
        // [RowChunk(3), End]. The accumulator must reset between them, so the
        // second span is 3 (NOT 3+3), proving no cross-row leakage.
        let mut b = RowsBuilder::new();
        b.feed(Surface::RowChunk(b"AA"));
        b.feed(Surface::RowChunk(b"B"));
        b.feed(Surface::RowChunkEnd);
        b.feed(Surface::RowChunk(b"CCC"));
        b.feed(Surface::RowChunkEnd);

        assert_eq!(b.slots, vec![3u32, 3u32], "each chunked row's span is its own chunk sum");
        assert_eq!(b.wire, b"AABCCC".to_vec(), "wire is the two rows concatenated");
        assert_eq!(b.oversize_len, 0, "accumulator reset after the last RowChunkEnd");
    }

    #[test]
    fn chunked_row_as_the_only_row() {
        // A single oversize row (two chunks) with nothing before or after.
        let mut b = RowsBuilder::new();
        b.feed(Surface::RowChunk(b"XXXX"));
        b.feed(Surface::RowChunk(b"YY"));
        b.feed(Surface::RowChunkEnd);

        assert_eq!(b.slots, vec![6u32], "one span equal to the chunk sum");
        assert_eq!(b.wire, b"XXXXYY".to_vec(), "wire is the reassembled row");
        assert_eq!(b.oversize_len, 0, "accumulator reset");
    }
}
