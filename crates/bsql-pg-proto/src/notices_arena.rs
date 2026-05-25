//! Multi-slot arena for `NoticeResponse` (`'N'`) payloads — PG §55.7.
//!
//! Structural mirror of `crate::notifications_arena::NotificationsArena`
//! (LISTEN/NOTIFY). PG notices carry operator-informational text
//! (VACUUM progress, PL/pgSQL `RAISE NOTICE`, implicit cast warnings)
//! that the caller may want to log, display, or route.
//!
//! # Why an arena
//!
//! NoticeResponse has the same wire format as ErrorResponse: up to
//! ~288 B of bounded strings (message + detail + hint). Inlining into
//! `Action` would balloon the variant past the 24 B pin. The arena
//! holds payloads on the heap; `Action::Notice` carries a 4 B
//! `NoticeRef` handle.
//!
//! # Lifecycle
//!
//! - **Lazy allocation**: `Option<Box<NoticesArena>>` on `ActiveInner`.
//! - **Per-cycle clear**: cleared at `feed_bytes` entry alongside
//!   other arenas. Refs from cycle N resolve `Stale` in cycle N+1.
//! - **Slot cap**: `MAX_NOTICES_PER_CALL = 9`, matching OutActions cap.

use crate::error_arena::ArenaError;
use crate::ident::BoundedStr;

/// Maximum notices per `feed_bytes` call.
pub(crate) const MAX_NOTICES_PER_CALL: usize = 9;

/// Per-notice payload — operator-informational text from the server.
///
/// Uses `BoundedStr` (NOT `SecretBoundedStr`) — notices carry
/// diagnostic text (VACUUM progress, PL/pgSQL RAISE NOTICE), not
/// SQL fragments or credentials. Dropping zeroize avoids ~200
/// cycles per payload drop for zero security benefit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticePayload {
    /// PG severity level (WARNING, NOTICE, DEBUG, INFO, LOG).
    pub severity: BoundedStr<32>,
    /// SQLSTATE code (5 ASCII chars, e.g. "00000").
    pub code: crate::error::SqlStateCode,
    /// Human-readable message (M field). Truncated at 128 B.
    pub message: BoundedStr<128>,
    /// Optional detail (D field). Often empty.
    pub detail: BoundedStr<96>,
    /// Optional hint (H field). Often empty.
    pub hint: BoundedStr<64>,
}

/// Gen-tagged handle into `NoticesArena`.
///
/// 4 B (`Copy`): slot 1 B + generation 2 B + 1 B padding.
/// Carried by `Action::Notice` so Action stays `Copy` at 24 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoticeRef {
    slot: crate::bounded::BoundedU8<{ MAX_NOTICES_PER_CALL.saturating_sub(1) }>,
    generation: u16,
}

/// Multi-slot notices arena.
#[derive(Debug)]
pub(crate) struct NoticesArena {
    slots: heapless::Vec<NoticePayload, MAX_NOTICES_PER_CALL>,
    generation: u16,
}

impl NoticesArena {
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: heapless::Vec::new(),
            generation: 0,
        }
    }

    #[inline]
    pub(crate) fn alloc(&mut self, payload: NoticePayload) -> Option<NoticeRef> {
        let slot_idx_usize = self.slots.len();
        let Ok(slot_idx_u8) = u8::try_from(slot_idx_usize) else {
            core::hint::cold_path();
            return None;
        };
        self.slots.push(payload).ok()?;
        let slot = crate::bounded::BoundedU8::try_new(slot_idx_u8)?;
        Some(NoticeRef {
            slot,
            generation: self.generation,
        })
    }

    #[inline]
    pub(crate) fn get(&self, r: NoticeRef) -> Result<&NoticePayload, ArenaError> {
        if r.generation != self.generation {
            return Err(ArenaError::Stale);
        }
        let idx = usize::from(r.slot.get());
        match self.slots.get(idx) {
            Some(payload) => Ok(payload),
            None => Err(ArenaError::Empty),
        }
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.slots.clear();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

// Drift pins.
const _: () = assert!(
    core::mem::size_of::<NoticeRef>() == 4,
    "NoticeRef must be 4 B (slot BoundedU8 + gen u16 + pad).",
);
const _: () = assert!(
    core::mem::size_of::<Option<NoticeRef>>() == 4,
    "Option<NoticeRef> must niche-pack to 4 B via BoundedU8 niche.",
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_and_get_round_trip() {
        let mut arena = NoticesArena::new();
        let payload = NoticePayload {
            severity: BoundedStr::from_bytes_lossy(b"WARNING"),
            code: crate::error::SqlStateCode::from_bytes(b"01000"),
            message: BoundedStr::from_bytes_lossy(b"test notice"),
            detail: BoundedStr::default(),
            hint: BoundedStr::default(),
        };
        let r = arena.alloc(payload);
        assert!(r.is_some());
        let r = r.filter(|_| true);
        if let Some(notice_ref) = r {
            assert!(arena.get(notice_ref).is_ok());
        }
    }

    #[test]
    fn stale_after_clear() {
        let mut arena = NoticesArena::new();
        let payload = NoticePayload {
            severity: BoundedStr::from_bytes_lossy(b"NOTICE"),
            code: crate::error::SqlStateCode::from_bytes(b"00000"),
            message: BoundedStr::from_bytes_lossy(b"hi"),
            detail: BoundedStr::default(),
            hint: BoundedStr::default(),
        };
        let r = arena.alloc(payload);
        arena.clear();
        if let Some(notice_ref) = r {
            assert!(matches!(arena.get(notice_ref), Err(ArenaError::Stale)));
        }
    }

    #[test]
    fn multi_alloc_within_cap() {
        let mut arena = NoticesArena::new();
        for i in 0..MAX_NOTICES_PER_CALL {
            let payload = NoticePayload {
                severity: BoundedStr::from_bytes_lossy(b"NOTICE"),
                code: crate::error::SqlStateCode::from_bytes(b"00000"),
                message: BoundedStr::from_bytes_lossy(
                    &[b'A'.wrapping_add(u8::try_from(i).unwrap_or(0))],
                ),
                detail: BoundedStr::default(),
                hint: BoundedStr::default(),
            };
            assert!(arena.alloc(payload).is_some());
        }
        assert_eq!(arena.len(), MAX_NOTICES_PER_CALL);
    }
}
