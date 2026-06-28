//! `Surface` → `Row` materialisation shared by the blocking and async drivers.
//!
//! The sans-I/O engine surfaces query results as RAW wire bytes through a
//! [`Surface`] sink — it is `no_std` and cannot name a typed [`Row`]. This
//! module turns that borrowed-bytes stream into the owned, Arc-arena [`Row`] the
//! driver API returns: one [`ArenaBuilder`] per result set, so the whole result
//! costs the arena's four allocations regardless of row count, never a per-row
//! allocation.
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

use bsql_postgres_proto::engine::Surface;

use crate::error::{DbError, DriverError};
use crate::types::{ArenaBuilder, Notification, Row};

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
}

impl DbErrorSink for ResultCollector {
    fn take_db_error(&mut self) -> Option<DbError> {
        self.db_error.take()
    }
}

/// The fully materialised result of a row-collecting verb.
#[derive(Debug)]
pub struct CollectedResult {
    /// The result rows (empty for a command that returns none).
    pub rows: Vec<Row>,
    /// The command tag (`"SELECT 5"`, `"INSERT 0 1"`, …); empty when none.
    pub command_tag: String,
    /// The affected-row count from the command tag (0 for a countless command).
    pub affected: u64,
    /// The result-column names recovered from the `RowDescription`.
    pub column_names: Vec<String>,
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
    command_tag: String,
    affected: u64,
    oids: Vec<u32>,
    column_names: Vec<String>,
    db_error: Option<DbError>,
    decode_error: Option<DriverError>,
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
                let body = core::mem::take(&mut self.chunk);
                self.push_row_body(&body);
            }
            Surface::Deliver { tag, oids, names } => {
                match tag {
                    Some(t) => {
                        self.command_tag = t.to_string();
                        self.affected = t.rows_or_zero();
                    }
                    // A tagless boundary (the extended-protocol acks / a
                    // `Describe` completion): no command tag, no row count.
                    None => {
                        self.command_tag = String::new();
                        self.affected = 0;
                    }
                }
                self.oids = oids.to_vec();
                self.column_names = names.to_vec();
            }
            Surface::Fail(body) => self.db_error = Some(parse_error_response(body)),
            // Asynchronous / COPY frames are not part of a row result set; the
            // copy affected-count rides the trailing `Deliver` like any command.
            Surface::Notice(_)
            | Surface::Notify(_)
            | Surface::ParamStatus(_)
            | Surface::CopyData(_)
            | Surface::CopyDone => {}
        }
    }

    /// The command tag captured at the delivery (empty when none).
    #[must_use]
    pub fn command_tag(&self) -> &str {
        &self.command_tag
    }

    /// The affected-row count captured at the delivery.
    #[must_use]
    pub fn affected(&self) -> u64 {
        self.affected
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

    /// Seal the arena and produce the [`CollectedResult`].
    ///
    /// # Errors
    ///
    /// [`DriverError::RowDecodeFailed`] if any fed row was wire-malformed, or
    /// [`DriverError::RowTooLarge`] if the arena exceeded its 32-bit bounds.
    pub fn finish(self) -> Result<CollectedResult, DriverError> {
        if let Some(e) = self.decode_error {
            return Err(e);
        }
        let rows = match self.builder {
            Some(b) => b.finish()?,
            None => Vec::new(),
        };
        Ok(CollectedResult {
            rows,
            command_tag: self.command_tag,
            affected: self.affected,
            column_names: self.column_names,
        })
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
pub(crate) fn parse_error_response(body: &[u8]) -> DbError {
    let mut code = String::new();
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
        let value = String::from_utf8_lossy(value_bytes).into_owned();
        match type_byte {
            b'C' => code = value,
            // PG sends `S` (localized) then `V` (non-localized, 9.6+); the later
            // `V` wins, keeping the stable non-localized severity.
            b'S' | b'V' => severity = Some(value),
            b'M' => message = value,
            b'D' => detail = Some(value),
            b'H' => hint = Some(value),
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
