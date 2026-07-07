//! N+1 query detection for the SQLite backend — a **diagnostics-only** tracker
//! (feature `n1-detect`).
//!
//! This is the SQLite twin of the PostgreSQL driver's N+1 detector: the SAME
//! `N1Report` shape, the SAME threshold, and the SAME window-reset-at-transaction
//! semantics, so `conn.n1_report()` reads identically across both backends. It is
//! a SELF-CONTAINED copy (not a re-export of the PostgreSQL core's tracker):
//! depending on `bsql-postgres-core` would drag the entire PostgreSQL + rustls
//! runtime tree into the embedded SQLite crate. The tracker has no external
//! dependency, so the copy adds none.
//!
//! # Zero behavioural effect
//!
//! [`N1Tracker::record`] returns nothing and reads nothing that steers control
//! flow. A miscount (a false positive under an unlucky window eviction) is at
//! most a spurious [`N1Report`], never a change in what a query returns. The
//! whole tracker is compiled out unless the `n1-detect` feature is on, so a
//! default build has no field, no branch, and no `#[track_caller]` cost on the
//! query path.
//!
//! # Cost regime
//!
//! The recency window is a fixed inline `[WindowSlot; 16]` array (384 B; a vacant
//! slot is a `count == 0` sentinel, not an `Option`) — no per-query allocation,
//! no growth. The only heap use is the [`N1Report`] vector, which stays empty
//! (`Vec::new` does not allocate) until an actual N+1 is detected.
//!
//! [`N1Tracker::record`]: N1Tracker::record

use core::panic::Location;

/// A detected N+1 anti-pattern: one query executed `count` times from a single
/// source location.
///
/// Returned (as a slice) by [`Connection::n1_report`](crate::Connection::n1_report).
/// Purely diagnostic — the driver builds it as a side effect of running queries
/// and never acts on it. Field-for-field identical to the PostgreSQL driver's
/// `N1Report`, so a consumer's N+1 handling is backend-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct N1Report {
    /// The query's SQL text (the `query!` carrier's `&'static str`).
    pub sql: &'static str,
    /// The source file of the repeated call site (from `#[track_caller]`).
    pub file: &'static str,
    /// The 1-based source line of the repeated call site.
    pub line: u32,
    /// How many times the query has run from this site since the last window
    /// reset — kept current as the count climbs past the threshold.
    pub count: u32,
}

/// How many slots the recency window holds. Small and fixed: the window only has
/// to span the distinct queries active within one logical operation (the scope an
/// N+1 loop lives in), and a bounded array keeps recording allocation-free. A
/// distinct key beyond the 16th evicts the least-recently-used slot.
const WINDOW: usize = 16;

/// Default repeat count at which a `(query, call-site)` pair is reported — the
/// same threshold as the PostgreSQL detector.
const DEFAULT_THRESHOLD: u32 = 25;

/// One recency-window entry: a `(sql, call-site)` key plus its running count and a
/// recency stamp for LRU eviction.
///
/// The key is two pointer identities — `sql_token` from the `&'static str`'s data
/// pointer (stable per `query!` site) and `loc_token` from the `&'static
/// Location`'s pointer (stable per call site). The composite distinguishes call
/// sites even when one generic helper dispatches several queries from one line.
///
/// Emptiness rides a `count == 0` SENTINEL rather than an `Option` wrapper: an
/// occupied slot always has `count >= 1`, so `count == 0` can never be a valid
/// entry — keeping each slot at 24 bytes (two `usize` + two `u32`).
#[derive(Debug, Clone, Copy)]
struct WindowSlot {
    /// `sql.as_ptr().addr()` — the query text's data-pointer identity.
    sql_token: usize,
    /// `ptr::from_ref(caller).addr()` — the call site's `Location` identity.
    loc_token: usize,
    /// Times this key has been recorded since it entered the window. `0` is the
    /// EMPTY-slot sentinel.
    count: u32,
    /// The `tick` at the most recent record — the LRU recency stamp.
    last_tick: u32,
}

impl WindowSlot {
    /// The empty-slot sentinel (`count == 0` marks an unoccupied slot).
    const EMPTY: Self = Self {
        sql_token: 0,
        loc_token: 0,
        count: 0,
        last_tick: 0,
    };

    /// Whether this slot is unoccupied (the `count == 0` sentinel).
    const fn is_vacant(&self) -> bool {
        self.count == 0
    }
}

// Footprint pin (compiled ONLY under `n1-detect`): the recency window stays 384 B
// — 16 slots of 24 B each — matching the PostgreSQL tracker. A drift (a re-added
// `Option`, a widened field) is an `E0080` const-eval failure at `cargo check`.
const _: () = {
    assert!(
        core::mem::size_of::<WindowSlot>() == 24,
        "N1 WindowSlot must stay 24 B (no Option, u32 tick)"
    );
    assert!(
        core::mem::size_of::<[WindowSlot; WINDOW]>() == 384,
        "the N1 recency window must stay 384 B"
    );
};

/// The per-connection N+1 detector: a bounded recency window plus the reports it
/// has produced.
///
/// See the [module docs](self) for the diagnostics-only, zero-cost-off contract.
#[derive(Debug)]
pub struct N1Tracker {
    /// The bounded recency window of active `(sql, call-site)` keys.
    window: [WindowSlot; WINDOW],
    /// One report per site that has reached the threshold. Empty (and unallocated)
    /// until the first detection.
    reports: Vec<N1Report>,
    /// The repeat count that trips a report.
    threshold: u32,
    /// A monotonically increasing logical clock stamped on each record, used to
    /// pick the least-recently-used slot to evict.
    tick: u32,
}

impl Default for N1Tracker {
    fn default() -> Self {
        Self::new()
    }
}

impl N1Tracker {
    /// A fresh tracker with the default threshold and an empty window.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            window: [WindowSlot::EMPTY; WINDOW],
            reports: Vec::new(),
            threshold: DEFAULT_THRESHOLD,
            tick: 0,
        }
    }

    /// Record one execution of `sql` from call site `caller`.
    ///
    /// Diagnostics-only: returns nothing and never influences the caller. Bumps
    /// the `(sql, call-site)` key's count in the recency window and, once the count
    /// reaches the threshold, records (or refreshes) the matching [`N1Report`].
    pub fn record(&mut self, sql: &'static str, caller: &'static Location<'static>) {
        let sql_token = sql.as_ptr().addr();
        let loc_token = core::ptr::from_ref(caller).addr();
        let threshold = self.threshold;
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;

        let count = self.bump_window(sql_token, loc_token, tick);
        if count >= threshold {
            self.upsert_report(sql, caller, count);
        }
    }

    /// Bump the count for `(sql_token, loc_token)` in the window, inserting the
    /// key (evicting the LRU slot if full) if it is new. Returns the new count.
    fn bump_window(&mut self, sql_token: usize, loc_token: usize, tick: u32) -> u32 {
        for slot in self.window.iter_mut() {
            if !slot.is_vacant() && slot.sql_token == sql_token && slot.loc_token == loc_token {
                slot.count = slot.count.saturating_add(1);
                slot.last_tick = tick;
                return slot.count;
            }
        }

        let mut vacant: Option<usize> = None;
        let mut lru_idx: usize = 0;
        let mut lru_tick: u32 = u32::MAX;
        for (i, slot) in self.window.iter().enumerate() {
            if slot.is_vacant() {
                vacant = Some(i);
                break;
            }
            if slot.last_tick < lru_tick {
                lru_tick = slot.last_tick;
                lru_idx = i;
            }
        }
        let idx = match vacant {
            Some(i) => i,
            None => lru_idx,
        };
        if let Some(target) = self.window.get_mut(idx) {
            *target = WindowSlot {
                sql_token,
                loc_token,
                count: 1,
                last_tick: tick,
            };
        }
        1
    }

    /// Record a first detection for this site, or refresh an existing report's
    /// count. One report per `(sql, file, line)` site.
    fn upsert_report(&mut self, sql: &'static str, caller: &'static Location<'static>, count: u32) {
        let file = caller.file();
        let line = caller.line();
        for report in &mut self.reports {
            if report.line == line
                && core::ptr::eq(report.sql.as_ptr(), sql.as_ptr())
                && core::ptr::eq(report.file.as_ptr(), file.as_ptr())
            {
                report.count = count;
                return;
            }
        }
        self.reports.push(N1Report {
            sql,
            file,
            line,
            count,
        });
    }

    /// The detected N+1 sites so far — one entry per site, count kept current.
    #[must_use]
    pub fn report(&self) -> &[N1Report] {
        &self.reports
    }

    /// Clear the recency window at a logical-operation boundary (a transaction
    /// commit/rollback), so repetition of a query ACROSS separate operations is
    /// forgiven while a per-row loop WITHIN one operation is still caught. The
    /// accumulated [`report`](Self::report) is left intact.
    pub fn reset(&mut self) {
        self.window = [WindowSlot::EMPTY; WINDOW];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn here() -> &'static Location<'static> {
        Location::caller()
    }

    #[test]
    fn flags_a_site_at_the_threshold_and_keeps_count_current() {
        let mut t = N1Tracker::new();
        let sql = "SELECT 1";
        let site = here();

        for _ in 0..(DEFAULT_THRESHOLD - 1) {
            t.record(sql, site);
        }
        assert!(t.report().is_empty(), "K-1 repeats must not flag");

        t.record(sql, site);
        assert_eq!(t.report().len(), 1, "the Kth repeat flags exactly once");
        assert_eq!(t.report()[0].sql, sql);
        assert_eq!(t.report()[0].count, DEFAULT_THRESHOLD);

        t.record(sql, site);
        t.record(sql, site);
        assert_eq!(t.report().len(), 1, "still one report per site");
        assert_eq!(t.report()[0].count, DEFAULT_THRESHOLD + 2);
    }

    #[test]
    fn distinct_sites_are_not_conflated() {
        let mut t = N1Tracker::new();
        let sql = "SELECT 1";
        let site_a = here();
        let site_b = here();
        assert_ne!(
            core::ptr::from_ref(site_a).addr(),
            core::ptr::from_ref(site_b).addr(),
        );

        for _ in 0..(DEFAULT_THRESHOLD - 1) {
            t.record(sql, site_a);
            t.record(sql, site_b);
        }
        assert!(
            t.report().is_empty(),
            "two distinct sites below threshold must not flag",
        );
    }

    #[test]
    fn reset_clears_the_window_but_keeps_reports() {
        let mut t = N1Tracker::new();
        let sql = "SELECT 1";
        let site = here();

        for _ in 0..DEFAULT_THRESHOLD {
            t.record(sql, site);
        }
        assert_eq!(t.report().len(), 1);

        t.reset();
        assert_eq!(t.report().len(), 1, "reset keeps the accumulated report");
        for _ in 0..(DEFAULT_THRESHOLD - 1) {
            t.record(sql, site);
        }
        assert_eq!(t.report().len(), 1, "after reset, K-1 repeats add no report");
        t.record(sql, site);
        assert_eq!(t.report()[0].count, DEFAULT_THRESHOLD, "count reflects the fresh window run");
    }
}
