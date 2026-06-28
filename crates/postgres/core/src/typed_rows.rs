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
//! [`Row`]: crate::Row

use std::marker::PhantomData;

use bsql_postgres_proto::engine::Surface;
use bsql_postgres_proto::{DecodeError, TypedQuery};

use crate::error::DbError;
use crate::materialize::{parse_error_response, DbErrorSink};

/// A per-ROW span into the prebuffer's `wire` byte vector. Not per-column —
/// [`TypedQuery::decode_borrowed`] parses the columns out of one contiguous
/// `DataRow` payload itself.
#[derive(Debug, Clone, Copy)]
struct RowSlot {
    /// Byte offset of the row's `DataRow` payload (its 2-byte column-count
    /// header onward) within `wire`.
    offset: usize,
    /// Byte length of that payload.
    len: usize,
}

/// Type-erased prebuffer collector fed by a driver's typed-query sink.
///
/// Q-agnostic by construction: it accumulates raw `DataRow` bytes plus their
/// per-row spans and the command metadata, with NO knowledge of the row type —
/// the `Q` is stamped on only at [`finish`](Self::finish), because decoding is
/// deferred. [`feed`](Self::feed) is infallible (the engine sink cannot report a
/// `Result`); any anomaly is parked and surfaced after the verb settles.
#[derive(Debug, Default)]
pub struct RowsBuilder {
    wire: Vec<u8>,
    slots: Vec<RowSlot>,
    affected: u64,
    db_error: Option<DbError>,
    saw_oversize: bool,
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
                let offset = self.wire.len();
                self.wire.extend_from_slice(body);
                self.slots.push(RowSlot {
                    offset,
                    len: body.len(),
                });
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
            // An oversize row streams as chunks. The bounded typed decoder needs
            // one contiguous payload per row, so flag it for a classified
            // `OversizeRow` after settle rather than reassembling — and never a
            // silent truncation.
            Surface::RowChunk(_) | Surface::RowChunkEnd => self.saw_oversize = true,
            // Asynchronous / COPY frames are not part of a typed row result.
            Surface::Notice(_)
            | Surface::Notify(_)
            | Surface::ParamStatus(_)
            | Surface::CopyData(_)
            | Surface::CopyDone => {}
        }
    }

    /// Whether an oversize (chunk-streamed) row was observed — the driver maps
    /// this to a classified [`DriverError::OversizeRow`](crate::DriverError::OversizeRow).
    #[must_use]
    pub fn saw_oversize(&self) -> bool {
        self.saw_oversize
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
    /// One `(offset, len)` span per row into `wire`.
    slots: Vec<RowSlot>,
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

    /// The contiguous `DataRow` payload for one row span.
    ///
    /// The spans are produced by [`RowsBuilder::feed`] from the very bytes in
    /// `wire`, so the slice always resolves; the `None` arm is fail-closed
    /// (classified, never an out-of-bounds index) against a future seam.
    fn row_body<'a>(wire: &'a [u8], slot: &RowSlot) -> Result<&'a [u8], DecodeError> {
        wire.get(slot.offset..)
            .and_then(|tail| tail.get(..slot.len))
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
        self.slots
            .iter()
            .map(move |slot| Q::decode_borrowed(Self::row_body(wire, slot)?))
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
        for slot in &self.slots {
            out.push(Q::decode_owned(Self::row_body(&self.wire, slot)?)?);
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
