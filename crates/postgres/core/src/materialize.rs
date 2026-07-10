//! `Surface` → `Row` materialisation shared by the blocking and async drivers.
//!
//! The sans-I/O engine surfaces query results as RAW wire bytes through a
//! [`Surface`] sink — it is `no_std` and cannot name a typed [`Row`](crate::Row).
//! This module turns that borrowed-bytes stream into the owned, Arc-arena
//! [`RowSet`] the driver API returns: one [`ArenaBuilder`] per result set, so the
//! whole result costs the arena's allocations regardless of row count, never a
//! per-row allocation — and the [`RowSet`] mints each [`Row`](crate::Row) handle
//! lazily on access, so no eager `Vec<Row>` is built either.
//!
//! A [`ResultCollector`] is fed every [`Surface`] event a verb's pump produces.
//! The pump's sink can only ever `Continue` (its break payload is uninhabited),
//! so it cannot return a `Result`; the collector therefore *parks* any decode
//! failure and surfaces it from [`finish`](ResultCollector::finish), so a
//! malformed row fails the whole result loudly rather than truncating it
//! silently. The command tag, affected-row count, result OIDs, and column names
//! are captured from [`Surface::Deliver`]; a server error from
//! [`Surface::Fail`] is parsed into a [`DbError`].
//!
//! Row text/binary format is irrelevant to this layer: the `DataRow` *framing*
//! (`[i16 n_cols] [per col: i32 len (-1 = NULL)] [bytes]`) is identical in both
//! formats, and the cell bytes are copied verbatim into the arena — the typed
//! `Row` accessors above interpret them.

use bsql_postgres_proto::command_tag::CommandTag;
use bsql_postgres_proto::engine::Surface;

use crate::error::{DbError, DriverError};
use crate::types::{ArenaBuilder, Notification, RowSet};

/// A collector that parks a server `ErrorResponse` for a driver's settle step
/// to classify.
///
/// Both result-collecting shapes implement it: the Arc-arena
/// [`ResultCollector`] (the dynamic simple-query path) and the typed-row
/// [`RowsBuilder`](crate::RowsBuilder) (the compile-checked `query!` path). A
/// driver's `settle` is generic over this seam, so one token-management path —
/// restore the linear token on an alive `Ok`, map a `ServerErrored` status to
/// the parked [`DbError`], leave the connection dead on a fatal `Err` — serves
/// both result shapes with no duplicated, drift-prone copy.
pub trait DbErrorSink {
    /// Take the parsed server error parked from a [`Surface::Fail`], if one was
    /// observed during the pump. Consumes it (a second call returns `None`).
    fn take_db_error(&mut self) -> Option<DbError>;

    /// Take the too-wide classification parked from a [`Surface::Overcap`] — the
    /// `(count, max)` of a result whose column count exceeded the driver's cap —
    /// if one was observed during the pump. Consumes it (a second call returns
    /// `None`). The client-side peer of [`take_db_error`](Self::take_db_error):
    /// checked FIRST by the driver's settle step so a too-wide result surfaces its
    /// specific classification, not the generic recovered-failure fallback.
    ///
    /// Defaults to `None`: only the dynamic [`ResultCollector`] can observe an
    /// over-cap (the typed-row [`RowsBuilder`](crate::RowsBuilder) decodes a
    /// compile-capped column count that cannot exceed the cap), so it alone
    /// overrides this.
    fn take_overcap(&mut self) -> Option<(usize, usize)> {
        None
    }
}

impl DbErrorSink for ResultCollector {
    fn take_db_error(&mut self) -> Option<DbError> {
        self.db_error.take()
    }

    fn take_overcap(&mut self) -> Option<(usize, usize)> {
        self.overcap.take()
    }
}

/// Accumulates a [`Surface`] stream into owned rows plus result metadata.
///
/// Construct with [`new`](Self::new), feed every surface event with
/// [`feed`](Self::feed), then read the metadata accessors and call
/// [`finish`](Self::finish) (which consumes the collector) for the rows. A
/// server error parked from [`Surface::Fail`] is read through the
/// [`DbErrorSink`] seam (the same seam the driver's settle step uses for both
/// this collector and the typed-row builder).
#[derive(Default)]
pub struct ResultCollector {
    builder: Option<ArenaBuilder>,
    /// Reassembly buffer for an oversize row delivered as `RowChunk` pieces.
    chunk: Vec<u8>,
    /// The typed command tag captured at the delivery. Stored as the `Copy`
    /// [`CommandTag`] the engine parsed — NOT a heap `String`, so a delivery
    /// costs no tag allocation; the affected-row count is a typed projection of
    /// it ([`affected`](Self::affected)), so no separate `affected` field.
    command_tag: CommandTag,
    oids: Vec<u32>,
    column_names: Vec<String>,
    db_error: Option<DbError>,
    decode_error: Option<DriverError>,
    /// The `(count, max)` of a too-wide `RowDescription` parked from a
    /// [`Surface::Overcap`]. Read through [`DbErrorSink::take_overcap`] by the
    /// driver's settle step, which maps it to the recoverable
    /// `DriverError::TooManyColumns` — the client-side peer of `db_error`.
    overcap: Option<(usize, usize)>,
}

impl ResultCollector {
    /// A fresh, empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consume one surface event from a verb's pump.
    ///
    /// `RowChunk` pieces of an oversize row are reassembled into one whole
    /// `DataRow` body, which is parsed at the terminating `RowChunkEnd` exactly
    /// as a whole [`Surface::Row`] is — so an oversize row decodes identically to
    /// an inline one.
    pub fn feed(&mut self, surface: Surface<'_>) {
        match surface {
            Surface::Row(body) => self.push_row_body(body),
            Surface::RowChunk(bytes) => self.chunk.extend_from_slice(bytes),
            Surface::RowChunkEnd => {
                // Detach the reassembled body to parse it (`push_row_body` needs
                // `&mut self`, so it cannot also borrow `self.chunk`), then RETAIN
                // the buffer's capacity for the next oversize row rather than
                // dropping it — matching the typed `query_each` path's `clear()`
                // reuse. `mem::take` ALONE left `Vec::new()` behind, freeing +
                // reallocating per oversize row; putting the cleared buffer back
                // keeps streaming a run of oversize rows at constant allocation.
                let mut body = core::mem::take(&mut self.chunk);
                self.push_row_body(&body);
                body.clear();
                self.chunk = body;
            }
            Surface::Deliver { tag, oids, names } => {
                // Store the `Copy` tag verbatim — no `to_string()` allocation.
                // The affected-row count is a typed projection of it, read on
                // demand via `affected()`.
                self.command_tag = match tag {
                    Some(t) => *t,
                    // A tagless boundary (the extended-protocol acks / a
                    // `Describe` completion): the empty tag, no row count.
                    None => CommandTag::EMPTY,
                };
                // Reuse the `oids` Vec SPINE across a multi-statement batch's
                // successive `Deliver`s: `clear` + `extend_from_slice` keeps the
                // backing allocation instead of dropping it and allocating a
                // fresh one per statement, as `= to_vec()` did. This matters
                // because a delivered statement's OIDs stay cached and ride EVERY
                // subsequent completion in the batch (e.g. the pooled
                // `reset_session` round-trip surfaces the `pg_advisory_unlock_all`
                // OIDs on three `Deliver`s), so the old fresh-Vec-per-`Deliver`
                // allocated once per repeat where one reused buffer suffices.
                //
                // `column_names` deliberately stays `= to_vec()`: it flows into
                // `build_query_result`'s `Arc::from(_.into_boxed_slice())` on the
                // HOT `query_sql` path, and `into_boxed_slice()` is free only when
                // cap == len — which `to_vec` guarantees but `extend`'s amortized
                // growth (cap 4 for 2 names) does not, so reusing that spine would
                // trade this cold-path win for a shrink-realloc on the hot path.
                self.oids.clear();
                self.oids.extend_from_slice(oids);
                self.column_names = names.to_vec();
            }
            Surface::Fail(body) => self.db_error = Some(parse_error_response(body)),
            // A too-wide result classified as recoverable `TooManyColumns`: park
            // the counts for the settle step, exactly as `Fail` parks a server
            // error. The pump then reaches its `Failed` boundary and drains.
            Surface::Overcap { count, max } => self.overcap = Some((count, max)),
            // Asynchronous / COPY frames are not part of a row result set; the
            // copy affected-count rides the trailing `Deliver` like any command.
            Surface::Notice(_)
            | Surface::Notify(_)
            | Surface::ParamStatus(_)
            | Surface::CopyData(_)
            | Surface::CopyDone => {}
        }
    }

    /// Render the captured command tag into an owned `String` (its wire text,
    /// `"SELECT 3"` / `"INSERT 0 1"` / …; empty when none).
    ///
    /// The drivers' `simple_query` returns this and drops the rest. The tag is
    /// stored as a `Copy` [`CommandTag`], so this allocates the string EXACTLY
    /// ONCE — at the point a caller actually wants text — rather than on every
    /// delivery; a row-returning verb that never asks for the text (it goes
    /// through `finish` into a `QueryResult` that stores the `Copy` tag) pays no
    /// tag allocation at all.
    #[must_use]
    pub fn into_command_tag(self) -> String {
        self.command_tag.to_string()
    }

    /// The affected-row count captured at the delivery — a typed projection of
    /// the command tag (`0` for a countless command).
    #[must_use]
    pub fn affected(&self) -> u64 {
        self.command_tag.rows_or_zero()
    }

    /// The result-column type OIDs recovered at the delivery (empty when none).
    #[must_use]
    pub fn oids(&self) -> &[u32] {
        &self.oids
    }

    /// The result-column names recovered at the delivery (empty when none).
    #[must_use]
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Seal the arena and yield the row-collecting verb's result pieces: the
    /// sealed [`RowSet`], the typed [`CommandTag`], and the result-column names —
    /// exactly what [`QueryResult`](crate::QueryResult) is assembled from.
    /// Consumes the collector; the affected-row count, when a caller needs it
    /// instead of the rows, is read from [`affected`](Self::affected) BEFORE this
    /// call.
    ///
    /// # Errors
    ///
    /// [`DriverError::RowDecodeFailed`] if any fed row was wire-malformed;
    /// [`DriverError::RowTooLarge`] if the arena exceeded its 32-bit bounds; or
    /// [`DriverError::MixedResultWidth`] if a multi-statement batch delivered
    /// rows of differing column counts (which the single arena's fixed stride
    /// cannot represent without mis-addressing cells).
    pub fn finish(self) -> Result<(RowSet, CommandTag, Vec<String>), DriverError> {
        if let Some(e) = self.decode_error {
            return Err(e);
        }
        let rows = match self.builder {
            // A verb that produced no `DataRow` never created a builder; its
            // result is the empty (arena-less) `RowSet`.
            Some(b) => b.finish()?,
            None => RowSet::default(),
        };
        Ok((rows, self.command_tag, self.column_names))
    }

    /// Parse one whole `DataRow` body (`[i16 n_cols] [per col: i32 len] [bytes]`)
    /// into the arena. A length-prefix that runs past the body, or a negative
    /// column count / length other than the `-1` NULL sentinel, parks a decode
    /// error rather than mis-addressing cells.
    fn push_row_body(&mut self, body: &[u8]) {
        let mut cursor = 0usize;
        let n_cols = match read_be_i16(body, &mut cursor) {
            Some(n) if n >= 0 => n,
            _ => return self.record_decode_error(),
        };
        let n_cols = match usize::try_from(n_cols) {
            Ok(n) => n,
            Err(_) => return self.record_decode_error(),
        };
        if self.builder.is_none() {
            self.builder = Some(ArenaBuilder::new(n_cols));
        }
        for _ in 0..n_cols {
            let len = match read_be_i32(body, &mut cursor) {
                Some(l) => l,
                None => return self.record_decode_error(),
            };
            if len == -1 {
                if let Some(b) = self.builder.as_mut() {
                    b.push_null();
                }
                continue;
            }
            let len = match usize::try_from(len) {
                Ok(l) => l,
                Err(_) => return self.record_decode_error(),
            };
            let end = match cursor.checked_add(len) {
                Some(e) => e,
                None => return self.record_decode_error(),
            };
            let cell = match body.get(cursor..end) {
                Some(c) => c,
                None => return self.record_decode_error(),
            };
            cursor = end;
            if let Some(b) = self.builder.as_mut() {
                b.push_value(cell);
            }
        }
        if let Some(b) = self.builder.as_mut() {
            b.end_row();
        }
    }

    /// Park the first decode failure; later ones are subsumed by it.
    fn record_decode_error(&mut self) {
        if self.decode_error.is_none() {
            self.decode_error = Some(DriverError::RowDecodeFailed);
        }
    }
}

/// Read a big-endian `i16` at `*cursor`, advancing it on success.
fn read_be_i16(buf: &[u8], cursor: &mut usize) -> Option<i16> {
    let end = cursor.checked_add(2)?;
    let bytes: [u8; 2] = buf.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(i16::from_be_bytes(bytes))
}

/// Read a big-endian `i32` at `*cursor`, advancing it on success.
fn read_be_i32(buf: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = cursor.checked_add(4)?;
    let bytes: [u8; 4] = buf.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(i32::from_be_bytes(bytes))
}

/// Parse a server `ErrorResponse` / `NoticeResponse` body into a [`DbError`].
///
/// The body is a sequence of `[type:u8] [value:CString]` fields ended by a `0`
/// type byte (PG §55.7). Only the structured fields the driver surfaces are
/// extracted: `C` (SQLSTATE), `S`/`V` (severity), `M` (message), `D` (detail),
/// `H` (hint). The SQLSTATE is always ASCII; the human-readable fields are
/// decoded with replacement of any non-UTF-8 bytes (display text, mirroring the
/// engine's lossy bounded-string handling), so a malformed field never aborts
/// the whole error.
pub fn parse_error_response(body: &[u8]) -> DbError {
    // The SQLSTATE (`C`) is the ONLY field with a fixed shape (5 ASCII chars), so
    // it lands in an inline `[u8; 5]` with no allocation; the human-readable
    // fields stay owned `String`s. `[b' '; 5]` (space-padded) is the empty/absent
    // code until a `C` field is seen.
    let mut code = [b' '; 5];
    let mut severity: Option<String> = None;
    let mut message = String::new();
    let mut detail: Option<String> = None;
    let mut hint: Option<String> = None;

    let mut rest = body;
    while let Some((type_byte, after_type)) = rest.split_first() {
        let type_byte = *type_byte;
        if type_byte == 0 {
            break;
        }
        let nul = match after_type.iter().position(|&b| b == 0) {
            Some(p) => p,
            None => break,
        };
        let value_bytes = match after_type.get(..nul) {
            Some(v) => v,
            None => break,
        };
        match type_byte {
            // The SQLSTATE narrows to 5 ASCII bytes with no allocation; a
            // malformed (non-5-char / non-ASCII) wire code is padded/truncated,
            // never a panic (the `decoder_fuzz` gate proves totality).
            b'C' => code = crate::error::sqlstate_bytes(value_bytes),
            // PG sends `S` (localized) then `V` (non-localized, 9.6+); the later
            // `V` wins, keeping the stable non-localized severity.
            b'S' | b'V' => severity = Some(String::from_utf8_lossy(value_bytes).into_owned()),
            b'M' => message = String::from_utf8_lossy(value_bytes).into_owned(),
            b'D' => detail = Some(String::from_utf8_lossy(value_bytes).into_owned()),
            b'H' => hint = Some(String::from_utf8_lossy(value_bytes).into_owned()),
            _ => {}
        }
        let next = match nul.checked_add(1) {
            Some(n) => n,
            None => break,
        };
        rest = match after_type.get(next..) {
            Some(r) => r,
            None => break,
        };
    }

    DbError {
        code,
        severity,
        message,
        detail,
        hint,
    }
}

/// Parse a `NotificationResponse` body (`[i32 pid] [channel CString] [payload
/// CString]`) into an owned [`Notification`].
///
/// # Errors
///
/// [`DriverError::NonUtf8Payload`] if the channel or payload is not valid UTF-8
/// (surfaced rather than silently substituting replacement characters — the
/// payload is application data, not display text), or
/// [`DriverError::NotificationUnavailable`] if the frame body is structurally
/// malformed.
pub fn parse_notification(body: &[u8]) -> Result<Notification, DriverError> {
    let pid_bytes: [u8; 4] = match body.get(..4).and_then(|s| s.try_into().ok()) {
        Some(b) => b,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let pid = i32::from_be_bytes(pid_bytes);
    let rest = match body.get(4..) {
        Some(r) => r,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let chan_nul = match rest.iter().position(|&b| b == 0) {
        Some(p) => p,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let channel_bytes = match rest.get(..chan_nul) {
        Some(c) => c,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let after_chan = match chan_nul.checked_add(1).and_then(|n| rest.get(n..)) {
        Some(a) => a,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let pay_nul = match after_chan.iter().position(|&b| b == 0) {
        Some(p) => p,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let payload_bytes = match after_chan.get(..pay_nul) {
        Some(p) => p,
        None => return Err(DriverError::NotificationUnavailable),
    };
    let channel = match core::str::from_utf8(channel_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => return Err(DriverError::NonUtf8Payload),
    };
    let payload = match core::str::from_utf8(payload_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => return Err(DriverError::NonUtf8Payload),
    };
    Ok(Notification {
        channel,
        payload,
        pid,
    })
}

#[cfg(test)]
mod result_collector_width_tests {
    //! A single `simple_query` batch surfaces one [`Surface::Deliver`] per
    //! statement and each statement's rows as [`Surface::Row`]. When two
    //! statements return DIFFERENT column counts, flattening their rows into
    //! one fixed-stride arena would mis-address cells — so the collector must
    //! reject the batch with [`DriverError::MixedResultWidth`], never return
    //! silently wrong data. These tests drive the collector with hand-built
    //! `DataRow` bodies (no live PG).

    use bsql_postgres_proto::engine::Surface;

    use super::ResultCollector;
    use crate::error::DriverError;

    /// Assemble a `DataRow` body: `[i16 n_cols][per col: i32 len][bytes]`, one
    /// column per element (no NULLs — length-prefixed cell bytes).
    fn row_body(cells: &[&[u8]]) -> Vec<u8> {
        let n_cols = i16::try_from(cells.len()).expect("test row within i16");
        let mut body = n_cols.to_be_bytes().to_vec();
        for cell in cells {
            let len = i32::try_from(cell.len()).expect("test cell within i32");
            body.extend_from_slice(&len.to_be_bytes());
            body.extend_from_slice(cell);
        }
        body
    }

    #[test]
    fn mixed_width_multi_statement_is_rejected() {
        // `SELECT 1; SELECT 'a','b'; SELECT 'z'` shape: widths 1, 2, 1. The
        // first row locks the stride at 1; the 2-col row would make row 2 read
        // from the wrong offset. Must be a loud error, not wrong data.
        let mut c = ResultCollector::new();
        c.feed(Surface::Row(&row_body(&[b"1"])));
        c.feed(Surface::Row(&row_body(&[b"a", b"b"])));
        c.feed(Surface::Row(&row_body(&[b"z"])));
        assert!(
            matches!(c.finish(), Err(DriverError::MixedResultWidth)),
            "a mixed-width batch must be rejected as MixedResultWidth",
        );
    }

    #[test]
    fn uniform_width_multi_statement_reads_correctly() {
        // Same width across statements: rows flatten into one arena whose fixed
        // stride addresses every cell correctly. This is the case the width
        // guard must NOT reject.
        let mut c = ResultCollector::new();
        c.feed(Surface::Row(&row_body(&[b"1", b"one"])));
        c.feed(Surface::Row(&row_body(&[b"2", b"two"])));
        let (rows, _tag, _names) = c.finish().expect("uniform-width rows seal cleanly");
        assert_eq!(rows.len(), 2);
        let r0 = rows.get(0).expect("row 0");
        let r1 = rows.get(1).expect("row 1");
        assert_eq!(r0.get_raw(0), Ok(Some(&b"1"[..])));
        assert_eq!(r0.get_raw(1), Ok(Some(&b"one"[..])));
        assert_eq!(r1.get_raw(0), Ok(Some(&b"2"[..])));
        assert_eq!(r1.get_raw(1), Ok(Some(&b"two"[..])));
    }
}

#[cfg(test)]
mod parse_notification_tests {
    //! `parse_notification` runs on UNTRUSTED server bytes, so its error branches
    //! (the structurally-malformed `NotificationUnavailable` and the
    //! non-UTF-8 `NonUtf8Payload`, which is rejected rather than lossily
    //! substituted because a payload is application data, not display text) are
    //! exercised here with byte literals — no live PG.

    use super::parse_notification;
    use crate::error::DriverError;

    /// Assemble a `NotificationResponse` body: `[i32 pid][channel\0][payload\0]`.
    fn body(pid: i32, channel: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut b = pid.to_be_bytes().to_vec();
        b.extend_from_slice(channel);
        b.push(0);
        b.extend_from_slice(payload);
        b.push(0);
        b
    }

    #[test]
    fn parses_a_well_formed_notification() {
        let n = parse_notification(&body(4242, b"chan", b"payload")).expect("parses");
        assert_eq!(n.pid, 4242);
        assert_eq!(n.channel, "chan");
        assert_eq!(n.payload, "payload");
    }

    #[test]
    fn empty_channel_and_payload_parse() {
        let n = parse_notification(&body(1, b"", b"")).expect("parses");
        assert_eq!(n.channel, "");
        assert_eq!(n.payload, "");
        assert_eq!(n.pid, 1);
    }

    #[test]
    fn non_utf8_payload_is_classified_not_substituted() {
        // A lone 0xFF is invalid UTF-8; the payload is rejected, not substituted.
        let b = body(1, b"chan", &[0xff]);
        assert!(matches!(
            parse_notification(&b),
            Err(DriverError::NonUtf8Payload)
        ));
    }

    #[test]
    fn non_utf8_channel_is_classified() {
        let b = body(1, &[0xff], b"payload");
        assert!(matches!(
            parse_notification(&b),
            Err(DriverError::NonUtf8Payload)
        ));
    }

    #[test]
    fn truncated_body_is_unavailable() {
        // Fewer than 4 bytes — no room for the pid.
        assert!(matches!(
            parse_notification(&[0, 0]),
            Err(DriverError::NotificationUnavailable)
        ));
        // pid present but the channel has no NUL terminator.
        assert!(matches!(
            parse_notification(&[0, 0, 0, 1, b'c', b'h']),
            Err(DriverError::NotificationUnavailable)
        ));
        // channel terminated, but the payload has no NUL terminator.
        assert!(matches!(
            parse_notification(&[0, 0, 0, 1, b'c', 0, b'p']),
            Err(DriverError::NotificationUnavailable)
        ));
    }
}
