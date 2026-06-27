//! The decided server-side plan-cache mode for dynamic param-toggle
//! queries — a `const` marker the connection-setup and pool-reset paths
//! consume.
//!
//! # The decision: rely on PostgreSQL's default `auto` mode (no override)
//!
//! A dynamic optional filter is one static SQL form whose predicate is
//! `($N IS NULL OR col = $N)`: passing a value enables the filter, passing
//! `NULL` disables it (the predicate folds to `TRUE` under a per-execution
//! plan). For that one form to stay fast it must use an index when the
//! filter is ENABLED.
//!
//! The concern was PostgreSQL's plan cache: a NAMED prepared statement can
//! switch to a GENERIC plan (one plan reused across executions, parameters
//! left as placeholders). A generic plan cannot fold `$N IS NULL`, so it
//! keeps the whole `OR` as a row filter and falls back to a sequential
//! scan even when a value is bound and an index exists.
//!
//! Live measurement shows that concern does not arise under the default
//! mode, and that overriding the mode carries a real cost. PostgreSQL's
//! `auto` adopts the generic plan ONLY when it is not more expensive than
//! the average custom plan. For the toggle form the generic plan LOSES the
//! index, which makes it far MORE expensive — which is exactly the
//! condition under which `auto` refuses to adopt it. The degradation is
//! therefore self-avoiding: `auto` keeps the toggle form on the
//! per-execution custom (index) plan on its own, with no override.
//!
//! # The live-PostgreSQL measurement (the decision evidence)
//!
//! Fixture: PostgreSQL 15, a 100 000-row table `toggle_demo` with a
//! high-selectivity indexed column `v` (one row per value) and a
//! low-selectivity indexed column `bucket` (`g % 4`, so each value matches
//! ~25% of the table). The toggle forms run as NAMED prepared statements
//! (`PREPARE`/`EXECUTE`) under the DEFAULT `plan_cache_mode = auto`. Every
//! figure below is re-run and asserted by the committed capture harness
//! `crates/postgres/sync/tests/plan_mode_capture.rs`; nothing is cited here
//! that the harness does not reproduce.
//!
//! ```text
//! tog  — 12 × EXECUTE tog(42)   [SELECT id FROM toggle_demo WHERE ($1 IS NULL OR v = $1)]:
//!   pg_prepared_statements:  generic_plans = 0,  custom_plans = 12
//!   EXPLAIN EXECUTE tog(42):
//!     Index Scan using toggle_demo_v_idx on toggle_demo
//!       Index Cond: (v = 42)                       <- index USED; auto kept custom
//!
//! tog2 — 6 DISABLED (NULL) execs to raise the average custom cost, then 8
//!        enabled execs:  generic_plans = 0,  custom_plans = 14
//!   EXPLAIN EXECUTE tog2(42):
//!     Index Scan using toggle_demo_v_idx ...  Index Cond: (v = 42)
//!
//! tog3 — 12 × EXECUTE tog3(1)   [... WHERE ($1 IS NULL OR bucket = $1)], a
//!        LOW-selectivity value (~25% of rows):  generic_plans = 0,  custom_plans = 12
//!   EXPLAIN EXECUTE tog3(1):
//!     Bitmap Heap Scan on toggle_demo
//!       Recheck Cond: (bucket = 1)
//!       ->  Bitmap Index Scan on toggle_demo_bucket_idx
//!             Index Cond: (bucket = 1)             <- index USED via a bitmap scan
//!
//! togm — 12 × EXECUTE through every enable/disable combination
//!        [... WHERE ($1 IS NULL OR v = $1) AND ($2 IS NULL OR bucket = $2)]:
//!   generic_plans = 0,  custom_plans = 12
//!   EXPLAIN EXECUTE togm(42, NULL):  Index Scan ...      Index Cond: (v = 42)
//!   EXPLAIN EXECUTE togm(NULL, 1):   Bitmap Heap Scan ...  Index Cond: (bucket = 1)
//!
//! Control — the generic plan degrades ONLY when FORCED, never chosen by
//! auto:  SET plan_cache_mode = force_generic_plan; EXPLAIN EXECUTE tog(42)
//!     Seq Scan on toggle_demo  (cost=0.00..1791.00 rows=501 width=4)
//!       Filter: (($1 IS NULL) OR (v = $1))         <- index LOST (forced only)
//! ```
//!
//! Across every adversarial shape the harness runs — high-selectivity,
//! low-selectivity (bitmap), a NULL-heavy warmup, and a multi-toggle form,
//! all well past the 5-execution switchover window — `generic_plans` stayed
//! `0` and the index was used. The degradation appears only under
//! `force_generic_plan` (the control), which `auto` never selects for these
//! forms.
//!
//! # Why session-wide `force_custom_plan` was REJECTED (measured collateral)
//!
//! `SET plan_cache_mode = force_custom_plan` is session-wide: it disables
//! generic-plan caching for EVERY prepared statement on the connection, not
//! just toggle forms. A plain `WHERE id = $1` lookup pays for that with
//! nothing to show:
//!
//! ```text
//! Plain `SELECT v FROM toggle_demo WHERE id = $1`, 12 executions:
//!   under auto (default), prepared as pk:
//!     pg_prepared_statements:  generic_plans = 7,  custom_plans = 5
//!     EXPLAIN EXECUTE pk(42):   Index Cond: (id = $1)   <- generic plan CACHED + reused
//!   under force_custom_plan, prepared as pk2:
//!     pg_prepared_statements:  generic_plans = 0,  custom_plans = 12
//!     EXPLAIN EXECUTE pk2(42):  Index Cond: (id = 42)   <- RE-PLANNED every execution
//! ```
//!
//! Under `auto` PostgreSQL plans the plain lookup, caches its generic plan,
//! and reuses it; under `force_custom_plan` it re-plans on every execution
//! — pure planning overhead, for no benefit, since `auto` already keeps the
//! toggle forms on the index. The session-wide override costs real work on
//! the common case and protects nothing. Per-statement-scoped overrides and
//! the unnamed-statement path were likewise rejected: both add machinery
//! (GUC round-trips, or re-parsing every execution and losing the
//! parse-once benefit) to "protect" a form that `auto` already keeps on the
//! index plan.
//!
//! # The chosen mode
//!
//! [`PLAN_CACHE_MODE`] = [`PlanCacheMode::Auto`] — the engine issues NO
//! `plan_cache_mode` `SET` on connect, so each session keeps PostgreSQL's
//! default. [`SET_PLAN_CACHE_MODE_SQL`] is therefore `None` (nothing to
//! issue on connect) and [`RESET_PLAN_CACHE_MODE_SQL`] is `None` (nothing
//! was set, so a pool has nothing to reset). Both are `Option<&'static str>`
//! so a consumer must exhaustively handle the "no override" case — it
//! cannot silently issue a GUC that was never decided.
//!
//! No SQL string is ever built from runtime data here.

/// The server-side plan-cache mode the engine selects for its sessions.
///
/// Only one variant exists: the decision is settled (see the module docs).
/// It is an enum, not a bare bool, so a future second mode is an additive,
/// exhaustively-matched change rather than a silent meaning flip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheMode {
    /// `plan_cache_mode = auto` — PostgreSQL's default. The planner picks a
    /// per-execution custom plan or a reusable generic plan per statement
    /// by cost, adopting the generic plan only when it is not more
    /// expensive than the average custom plan. A toggle form whose generic
    /// plan would lose an index is, for that reason, kept on the custom
    /// (index) plan; a plain lookup whose plans are equivalent gets its
    /// generic plan cached and reused. The engine relies on this and issues
    /// no override.
    Auto,
}

impl PlanCacheMode {
    /// The PostgreSQL `plan_cache_mode` setting value for this mode.
    #[inline]
    #[must_use]
    pub const fn setting_value(self) -> &'static str {
        match self {
            PlanCacheMode::Auto => "auto",
        }
    }
}

/// The decided plan-cache mode for dynamic param-toggle queries:
/// PostgreSQL's default `auto`, which the measurement shows already keeps
/// toggle forms on the index plan.
pub const PLAN_CACHE_MODE: PlanCacheMode = PlanCacheMode::Auto;

/// The `SET` statement the engine issues once per connection to apply
/// [`PLAN_CACHE_MODE`], or `None` when the decided mode is PostgreSQL's
/// default and no override is sent. The chosen mode is
/// [`PlanCacheMode::Auto`], so this is `None`: the engine keeps the server
/// default and issues nothing on connect. `Option` (not a bare `&str`)
/// forces a consumer to handle the no-override case rather than blindly
/// issue a GUC.
pub const SET_PLAN_CACHE_MODE_SQL: Option<&'static str> = None;

/// The `RESET` statement a connection pool issues on return to clear any
/// `plan_cache_mode` override, or `None` when nothing was set. The engine
/// issues no `SET` (see [`SET_PLAN_CACHE_MODE_SQL`]), so there is nothing
/// to reset and this is `None`.
pub const RESET_PLAN_CACHE_MODE_SQL: Option<&'static str> = None;

// Tier-1 drift-pin: the decided mode is PostgreSQL's `auto` default, so the
// engine issues no `plan_cache_mode` override — the `SET`/`RESET` markers
// must both be `None`. Should a future mode ever require an override, this
// pin fails the build until the markers are made `Some(..)` to match, so
// the marker set can never silently disagree with the decided mode.
const _: () = {
    assert!(
        matches!(PLAN_CACHE_MODE, PlanCacheMode::Auto),
        "the decided plan-cache mode is `auto`",
    );
    assert!(
        SET_PLAN_CACHE_MODE_SQL.is_none(),
        "auto issues no plan_cache_mode SET on connect",
    );
    assert!(
        RESET_PLAN_CACHE_MODE_SQL.is_none(),
        "auto sets nothing, so a pool has nothing to reset",
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decided_mode_is_auto() {
        assert_eq!(PLAN_CACHE_MODE, PlanCacheMode::Auto);
        assert_eq!(PLAN_CACHE_MODE.setting_value(), "auto");
    }

    #[test]
    fn auto_issues_no_override() {
        assert!(SET_PLAN_CACHE_MODE_SQL.is_none());
        assert!(RESET_PLAN_CACHE_MODE_SQL.is_none());
    }
}
