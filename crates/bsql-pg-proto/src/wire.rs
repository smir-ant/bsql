//! PostgreSQL wire-protocol byte constants.
//!
//! These are compile-time `const` literals from the published PG wire
//! specification (PostgreSQL 17 §55.7, "Message Formats"). Any
//! modification is a protocol break and must be reviewed against the
//! upstream spec.
//!
//! The full PG protocol uses dozens of message tags; **Phase 1a only
//! ships the tags the Ping flow actually traverses**: `Sync` outbound,
//! `ReadyForQuery` and `ErrorResponse` inbound. Other tags land with
//! their drivers per reforge.md §3.5 (no manufactured constants).

/// Frontend `Sync` message tag (`'S'`).
///
/// Sent by the client to flush a pipelined batch. In Phase 1a we use it
/// as the Ping primitive: the only legal server response to a `Sync` in
/// `Idle` is a `ReadyForQuery`. PG protocol spec §55.2.4 (Extended Query).
pub const TAG_SYNC: u8 = b'S';

/// Backend `ReadyForQuery` message tag (`'Z'`).
///
/// Carries one byte of payload — the transaction status indicator
/// (`'I'` idle, `'T'` in-transaction, `'E'` failed transaction). In
/// Phase 1a we accept any of the three (we are layer-below the
/// transaction state machine; it lands in 1c).
pub const TAG_READY_FOR_QUERY: u8 = b'Z';

/// Backend `ErrorResponse` message tag (`'E'`).
///
/// A server-side error. Variable-length payload of typed fields. Phase
/// 1a does not parse the field set — it classifies the entire message
/// as a single [`ProtocolError::ServerError`] and drops it.
///
/// [`ProtocolError::ServerError`]: crate::error::ProtocolError::ServerError
pub const TAG_ERROR_RESPONSE: u8 = b'E';

/// The complete `Sync` frame on the wire.
///
/// PG `Sync` has a 5-byte body: tag (`'S'`) + 4-byte length-field
/// (value `4`, big-endian — the length includes itself but excludes
/// the tag).
///
/// This is a `&'static [u8]` because the message is parameter-free; we
/// ship it via [`crate::action::SendBuf::Static`] with zero alloc and
/// zero copy.
pub const SYNC_WIRE_BYTES: [u8; 5] = [TAG_SYNC, 0, 0, 0, 4];

/// Compile-time check: `Sync` body length matches the spec.
///
/// Tier-1 against typo-induced wire breaks. If this assert fires, the
/// crate does not build.
const _: () = assert!(SYNC_WIRE_BYTES.len() == 5);
const _: () = assert!(SYNC_WIRE_BYTES[0] == b'S');
const _: () = assert!(
    SYNC_WIRE_BYTES[1] == 0
        && SYNC_WIRE_BYTES[2] == 0
        && SYNC_WIRE_BYTES[3] == 0
        && SYNC_WIRE_BYTES[4] == 4,
    "Sync length-field must be 4 (length includes itself, no payload)",
);
