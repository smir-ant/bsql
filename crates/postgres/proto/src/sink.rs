//! `Sink` — the borrowing visitor output surface (converged §2.2).
//!
//! The protocol's eventual single output surface: instead of the state
//! machine *accumulating* a batch of owned `Action`s plus generation-tagged
//! arenas (the `StagedAction`/`materialise`/`OutActions` apparatus), a
//! re-enterable driver hands the host **one event at a time, by borrow**,
//! via the methods below. Each payload borrows in place from the read /
//! write buffer; the borrow ends when the method returns, so a host that
//! tries to retain it across the next event does not borrow-check (the
//! whole reason the staging/arena machinery exists — coexisting batched
//! payloads — ceases to be representable).
//!
//! This is introduced ALONGSIDE the existing `feed_bytes`/`OutActions`
//! surface; the driver(s) and the `Action` apparatus migrate onto it
//! incrementally, and the old surface is deleted once nothing uses it. So
//! the ~600 sans-IO tests that pattern-match `OutActions`/`Action` keep
//! passing throughout the migration rather than breaking all at once.
//!
//! ## Closed event set
//!
//! The method set IS the protocol's full output vocabulary — a host cannot
//! introduce a new event kind, only choose which of these it observes
//! (every method has a default `Flow::Continue` body, so a host overrides
//! only what it cares about and the rest compile-fold away when the host's
//! concrete type monomorphizes). The trait is intentionally NOT sealed:
//! the host (a driver in another crate) must be able to implement it.

use core::num::NonZeroU64;

/// Host-driven flow control, returned by every [`Sink`] callback.
///
/// One unified type for both the push surface here and the pull row cursor
/// (`col_next`'s `NeedMore`): `Stop` is zero-cost backpressure / early
/// termination — the driver stops pumping events and returns control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "the driver must honor the requested Flow (Continue vs Stop)"]
pub enum Flow {
    /// Keep delivering events.
    Continue,
    /// Stop delivering events and return control to the caller.
    Stop,
}

/// Outcome of one [`PgProtocol::drive`](crate::PgProtocol) pump: why the
/// driver loop returned control to the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "the host must act on the drive outcome (read more / stream rows / close)"]
pub enum DriveStatus {
    /// All buffered frames for the in-flight command were consumed and the
    /// protocol is back to Ready/Idle.
    Idle,
    /// The read buffer is exhausted; the host must read more bytes from the
    /// socket, feed them, and call `drive` again.
    NeedMore,
    /// The protocol entered row streaming; the host pulls rows (`col_next`)
    /// before re-driving. (Row events move into `drive` itself once the
    /// unified engine/cursor lands.)
    Streaming,
    /// Terminal: a `Fail` or `Close` was delivered; the host should close
    /// the socket.
    Closed,
    /// A [`Sink`] callback returned [`Flow::Stop`]; the host asked to stop.
    Stopped,
}

/// Receives protocol output events, one at a time, each borrowed in place.
///
/// Every method defaults to [`Flow::Continue`] (ignore-and-continue), so a
/// host implements only the events it needs. Returning [`Flow::Stop`] asks
/// the driver to stop and yield control.
pub trait Sink {
    /// Outbound bytes to write to the server (a fully framed message),
    /// borrowed from the write buffer.
    fn on_send(&mut self, _bytes: &[u8]) -> Flow {
        Flow::Continue
    }

    /// A command completed successfully; route `reply` to the correlator
    /// `id`. `reply` carries no borrowed data (payloads are externalized).
    fn on_deliver(&mut self, _id: NonZeroU64, _reply: &crate::action::Reply) -> Flow {
        Flow::Continue
    }

    /// A command failed; route the failure to the correlator `id`. The
    /// classified cause is read separately via `PgProtocol::fail_cause`
    /// (same contract as today's `Action::FailReply`).
    fn on_fail(&mut self, _id: NonZeroU64) -> Flow {
        Flow::Continue
    }

    /// The protocol asks the host to close the socket (terminal).
    fn on_close(&mut self) -> Flow {
        Flow::Continue
    }

    /// An asynchronous `NotificationResponse` (LISTEN/NOTIFY): `pid` is the
    /// notifying backend's process id, `payload` (channel + message) is
    /// borrowed from the read buffer.
    fn on_notify(
        &mut self,
        _pid: i32,
        _payload: &crate::notifications_arena::NotificationPayload,
    ) -> Flow {
        Flow::Continue
    }

    /// A `NoticeResponse` (non-fatal server message), borrowed from the
    /// read buffer.
    fn on_notice(&mut self, _payload: &crate::notices_arena::NoticePayload) -> Flow {
        Flow::Continue
    }

    /// A chunk of `COPY ... TO STDOUT` data, borrowed from the read buffer.
    fn on_copy_chunk(&mut self, _bytes: &[u8]) -> Flow {
        Flow::Continue
    }

    /// One column value within the current row, borrowed from the read
    /// buffer. `None` is SQL NULL (distinct from an empty value, which is
    /// `Some(&[])`). Cells arrive in column order between [`Sink::on_row`]
    /// boundaries.
    fn on_cell(&mut self, _value: Option<&[u8]>) -> Flow {
        Flow::Continue
    }

    /// The current row is complete (all its cells have been delivered).
    fn on_row(&mut self) -> Flow {
        Flow::Continue
    }

    /// The current result set is complete (no more rows).
    fn on_query_end(&mut self) -> Flow {
        Flow::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A host that counts rows + cells and stops after a row budget —
    /// exercises the default-method fold + the `Flow::Stop` backpressure
    /// contract without any protocol machinery.
    #[derive(Default)]
    struct CountingSink {
        cells: usize,
        rows: usize,
        budget: usize,
    }

    impl Sink for CountingSink {
        fn on_cell(&mut self, _value: Option<&[u8]>) -> Flow {
            self.cells = self.cells.saturating_add(1);
            Flow::Continue
        }

        fn on_row(&mut self) -> Flow {
            self.rows = self.rows.saturating_add(1);
            if self.rows >= self.budget { Flow::Stop } else { Flow::Continue }
        }
    }

    #[test]
    fn defaults_continue_and_stop_propagates() {
        let mut s = CountingSink { budget: 2, ..Default::default() };
        // unobserved events use the default Continue body.
        assert_eq!(s.on_send(b"x"), Flow::Continue);
        assert_eq!(s.on_close(), Flow::Continue);
        // cells + rows.
        assert_eq!(s.on_cell(Some(b"a")), Flow::Continue);
        assert_eq!(s.on_cell(None), Flow::Continue); // NULL
        assert_eq!(s.on_cell(Some(b"")), Flow::Continue); // empty != NULL
        assert_eq!(s.on_row(), Flow::Continue); // row 1 < budget 2
        assert_eq!(s.on_row(), Flow::Stop); // row 2 == budget → Stop
        assert_eq!(s.cells, 3);
        assert_eq!(s.rows, 2);
    }
}
