//! Transaction-status indicator carried in every `ReadyForQuery` frame.
//!
//! [`TxStatus`] is the one typed value the connecting/active engine reads out
//! of a `ReadyForQuery` (`'Z'`) frame and threads to the driver. It is the
//! sole survivor of the former action module — the side-effect/`Action`
//! machinery of the old push-path engine is gone.

/// PostgreSQL transaction-status indicator carried in every
/// `ReadyForQuery` frame (PG §55.7).
///
/// PG defines exactly three legal values on the wire:
/// `'I'` (idle), `'T'` (in-transaction), `'E'` (failed transaction
/// — needs `ROLLBACK`). Any other byte is a server-side wire
/// violation and classifies as
/// `crate::ProtocolError::MalformedReadyForQuery` at dispatch
/// time — users never receive an invalid `TxStatus`.
///
/// # Tier-1 compile guarantees for consumers
///
/// Exhaustive `match` on `TxStatus` catches every legal state at
/// build time. A refactor that adds a new PG tx-status (future
/// spec change) forces every consumer to handle it. A naive
/// `tx_status: u8` form has no compiler help for the byte-match
/// — forgetting the `'E'` arm would be a tier-3
/// review-discipline seam.
///
/// # NOT `#[non_exhaustive]`
///
/// PG §55.7 defines `{'I', 'T', 'E'}` and this set is closed by
/// the wire protocol — a fourth status would require a major
/// protocol revision. Sealing via `non_exhaustive` would force
/// downstream catch-all arms for a case that **cannot exist on a
/// well-formed wire**; the dispatcher rejects non-{I,T,E} bytes
/// at framing-time as `MalformedReadyForQuery`. Closed-by-spec →
/// exhaustive `match` is the load-bearing tier-1 invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TxStatus {
    /// `'I'` — idle, no transaction in progress.
    Idle = b'I',
    /// `'T'` — inside an explicit or implicit transaction block.
    InTransaction = b'T',
    /// `'E'` — transaction failed; commands are ignored until
    /// `ROLLBACK` or `ROLLBACK TO SAVEPOINT`.
    Failed = b'E',
}

impl TxStatus {
    /// Parse a PG wire byte into the typed status.
    ///
    /// Returns `Err(b)` carrying the offending byte when `b` is
    /// outside `{'I', 'T', 'E'}` — lets callers forward the actual
    /// rejected value to diagnostics if they choose. Mirrors the
    /// `FormatCode::try_from_wire_i16` shape.
    #[inline]
    pub const fn try_from_byte(b: u8) -> Result<Self, u8> {
        match b {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::Failed),
            other => Err(other),
        }
    }

    /// The underlying PG wire byte. Used by builders + diagnostics.
    ///
    /// Explicit match (not `self as u8`) — the crate forbids
    /// `clippy::as_conversions`. With `#[repr(u8)]` and explicit
    /// discriminants above, each arm is a direct literal lookup;
    /// LLVM folds the match to a constant per monomorphic call.
    #[inline]
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

// Round-trip compile pin for TxStatus.
// `try_from_byte(byte(v)) == Ok(v)` must hold for every variant —
// catches a body-swap drift (e.g. `Self::Idle => b'T'`) at build
// time rather than in an integration test. Tier-1 compile.
const _: () = {
    assert!(
        matches!(TxStatus::try_from_byte(TxStatus::Idle.byte()), Ok(TxStatus::Idle)),
        "TxStatus round-trip broken: Idle",
    );
    assert!(
        matches!(TxStatus::try_from_byte(TxStatus::InTransaction.byte()), Ok(TxStatus::InTransaction)),
        "TxStatus round-trip broken: InTransaction",
    );
    assert!(
        matches!(TxStatus::try_from_byte(TxStatus::Failed.byte()), Ok(TxStatus::Failed)),
        "TxStatus round-trip broken: Failed",
    );
};
