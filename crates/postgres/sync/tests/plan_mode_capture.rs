#![forbid(unsafe_code)]
// Live-PostgreSQL capture harness for the dynamic-filter plan-mode
// decision. The `.expect(..)` calls below surface a missing/broken live
// PG loudly (the intended test signal); they are not silent production
// fallbacks. This is a `#[ignore]`d, live-only test target, separate from
// `sync_live` so the latter's count is unchanged.
#![allow(
    clippy::expect_used,
    clippy::unwrap_in_result,
    reason = "live-PG capture harness — expect() panics loudly on a missing server (the intended test signal); no production data-fallback path"
)]

//! Live-PostgreSQL EXPLAIN evidence for the dynamic optional-filter
//! plan-mode decision, and the assertion that the engine's baked plan-mode
//! marker matches the measured winner.
//!
//! # Why this exists
//!
//! A dynamic optional filter is one static form, `($1 IS NULL OR v = $1)`,
//! toggled by binding a value vs `NULL`. The concern was that a NAMED
//! prepared statement could switch to a GENERIC plan, which cannot
//! const-fold `$1 IS NULL` and so keeps the whole `OR` as a row filter —
//! degrading to a sequential scan even when a value is bound and an index
//! exists.
//!
//! The measurement below shows PostgreSQL's DEFAULT `plan_cache_mode = auto`
//! does NOT degrade the toggle form: `auto` adopts a generic plan only when
//! it is not more expensive than the average custom plan, and for the
//! toggle form the generic plan loses the index (making it far more
//! expensive), so `auto` keeps the per-execution custom plan that uses the
//! index. The degradation appears only when a generic plan is FORCED.
//! Meanwhile a session-wide `force_custom_plan` carries a measured
//! collateral cost on plain statements, so the engine relies on `auto` and
//! issues no override.
//!
//! # The fixture (every probe below runs against it)
//!
//! One temp table, `toggle_demo`, of 100 000 rows and three columns:
//! `id int4 PRIMARY KEY` (the plain-lookup column), `v int4` with a B-tree
//! index (the high-selectivity filter column, one row per value), and
//! `bucket int4` with a B-tree index (the low-selectivity filter column,
//! `g % 4`, so each value matches ~25% of the table). The `bucket` column
//! exists so the low-selectivity probe drives a Bitmap scan rather than a
//! plain Index Scan.
//!
//! # Captured evidence (PostgreSQL 15, the fixture above, default `auto`)
//!
//! Every line below is re-run and asserted by this test under DEFAULT
//! `auto`; nothing here is cited that the harness does not reproduce.
//!
//! ```text
//! High-selectivity toggle `SELECT id FROM toggle_demo WHERE ($1 IS NULL OR v = $1)`:
//!
//!   tog  — 12 × EXECUTE tog(42):
//!     generic_plans = 0, custom_plans = 12
//!     EXPLAIN EXECUTE tog(42):
//!       Index Scan using toggle_demo_v_idx ...
//!         Index Cond: (v = 42)                  <- index USED; auto kept custom
//!
//!   tog2 — 6 × EXECUTE tog2(NULL) (raise the average custom cost), then
//!          8 × EXECUTE tog2(42):
//!     generic_plans = 0, custom_plans = 14
//!     EXPLAIN EXECUTE tog2(42):
//!       Index Scan using toggle_demo_v_idx ...
//!         Index Cond: (v = 42)                  <- still custom after a NULL warmup
//!
//! Low-selectivity toggle `SELECT id FROM toggle_demo WHERE ($1 IS NULL OR bucket = $1)`:
//!
//!   tog3 — 12 × EXECUTE tog3(1) (matches ~25% of rows):
//!     generic_plans = 0, custom_plans = 12
//!     EXPLAIN EXECUTE tog3(1):
//!       Bitmap Heap Scan on toggle_demo ...
//!         Recheck Cond: (bucket = 1)
//!         ->  Bitmap Index Scan on toggle_demo_bucket_idx
//!               Index Cond: (bucket = 1)        <- index USED via a bitmap scan
//!
//! Multi-toggle `... WHERE ($1 IS NULL OR v = $1) AND ($2 IS NULL OR bucket = $2)`:
//!
//!   togm — 12 × EXECUTE with mixed enable/disable combinations:
//!     generic_plans = 0, custom_plans = 12
//!     EXPLAIN EXECUTE togm(42, NULL):  Index Scan ... Index Cond: (v = 42)
//!     EXPLAIN EXECUTE togm(NULL, 1):   Bitmap Heap Scan ... Index Cond: (bucket = 1)
//!
//! Control — the degradation exists ONLY when a generic plan is FORCED:
//!   SET plan_cache_mode = force_generic_plan; EXPLAIN EXECUTE tog(42):
//!     Seq Scan on toggle_demo  (cost=0.00..1791.00 rows=501 width=4)
//!       Filter: (($1 IS NULL) OR (v = $1))      <- index LOST (forced only)
//!
//! Collateral — plain `SELECT v FROM toggle_demo WHERE id = $1`, 12 execs:
//!   under auto:               generic_plans = 7  EXPLAIN: Index Cond: (id = $1)
//!   under force_custom_plan:  generic_plans = 0  EXPLAIN: Index Cond: (id = 42)
//! ```
//!
//! # Decision
//!
//! Rely on PostgreSQL's default `auto`: it already keeps every toggle shape
//! on the index when enabled and on a full scan when disabled, with zero
//! override and zero collateral on plain statements. The engine therefore
//! issues NO `plan_cache_mode` `SET` on connect and a pool has nothing to
//! `RESET` on return — there is no plan-cache knob, because the measurement
//! shows the default already wins. Session-wide `force_custom_plan` was
//! rejected for its measured collateral re-plan cost on plain statements.
//!
//! This test re-derives the evidence against a live server and asserts the
//! no-degradation behaviour it records.

use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

fn config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Join an `EXPLAIN` result's plan-text rows into one string.
fn explain(conn: &mut Connection, sql: &str) -> String {
    let result = conn.query_sql(sql).expect("EXPLAIN runs");
    let mut lines = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if let Ok(Some(line)) = row.get_str(0) {
            lines.push(line.to_string());
        }
    }
    lines.join("\n")
}

/// `(generic_plans, custom_plans)` for a named server-side prepared
/// statement, read from `pg_prepared_statements`.
fn plan_counts(conn: &mut Connection, name: &str) -> (i64, i64) {
    let sql = format!(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = '{name}'"
    );
    let result = conn.query_sql(&sql).expect("read plan counts");
    let row = result.rows.first().expect("prepared statement is present");
    (
        row.get_i64(0).expect("generic_plans decodes").expect("generic_plans present"),
        row.get_i64(1).expect("custom_plans decodes").expect("custom_plans present"),
    )
}

/// Run a prepared `EXECUTE` `n` times, discarding the returned rows. Each
/// call drives one custom-vs-generic plan decision recorded in
/// `pg_prepared_statements`.
fn run_execs(conn: &mut Connection, execute_sql: &str, n: usize) {
    for _ in 0..n {
        let _ = conn.query_sql(execute_sql).expect("execute prepared form");
    }
}

/// Assert `auto` kept a named toggle form on per-execution CUSTOM plans —
/// it adopted no generic plan across `execs` executions — and print the
/// counts so a `--nocapture` run shows the cited evidence.
fn assert_auto_kept_custom(conn: &mut Connection, name: &str, execs: i64) {
    let (generic, custom) = plan_counts(conn, name);
    eprintln!("--- auto, {name} after {execs} execs: generic={generic} custom={custom} ---");
    assert_eq!(
        generic, 0,
        "auto must NOT adopt a generic plan for the toggle form `{name}`; got generic_plans={generic}"
    );
    assert_eq!(
        custom, execs,
        "all {execs} executions of `{name}` should be custom plans; got custom_plans={custom}"
    );
}

#[test]
#[ignore = "requires local PG — live plan-mode capture"]
fn dynamic_filter_plan_mode_evidence() {
    let mut conn = Connection::connect(&config()).expect("connect");

    // Scratch fixture: a TEMP table (auto-dropped on disconnect — no
    // scratch DB to clean up) with a high-selectivity indexed column `v`
    // (one row per value) and a low-selectivity indexed column `bucket`
    // (`g % 4`, ~25% of rows per value, the shape that drives a bitmap
    // scan).
    conn.execute_sql(
        "CREATE TEMP TABLE toggle_demo (id int4 PRIMARY KEY, v int4 NOT NULL, bucket int4 NOT NULL)",
    )
    .expect("create temp table");
    conn.execute_sql("INSERT INTO toggle_demo SELECT g, g, g % 4 FROM generate_series(1, 100000) g")
        .expect("seed rows");
    conn.execute_sql("CREATE INDEX toggle_demo_v_idx ON toggle_demo (v)")
        .expect("create v index");
    conn.execute_sql("CREATE INDEX toggle_demo_bucket_idx ON toggle_demo (bucket)")
        .expect("create bucket index");
    conn.execute_sql("ANALYZE toggle_demo").expect("analyze");

    // Confirm the session is on PostgreSQL's DEFAULT mode — the whole point
    // is that no override is needed.
    let mode = explain(&mut conn, "SHOW plan_cache_mode");
    assert_eq!(mode, "auto", "session must be on the default mode; got `{mode}`");

    // ---- (1) High-selectivity toggle under DEFAULT auto: well past the
    //          5-execution switchover window, auto keeps the per-execution
    //          custom plan and uses the index. --------------------------
    conn.execute_sql("PREPARE tog(int4) AS SELECT id FROM toggle_demo WHERE ($1 IS NULL OR v = $1)")
        .expect("prepare toggle form");
    run_execs(&mut conn, "EXECUTE tog(42)", 12);
    assert_auto_kept_custom(&mut conn, "tog", 12);

    let tog_plan = explain(&mut conn, "EXPLAIN EXECUTE tog(42)");
    eprintln!("--- auto, EXPLAIN EXECUTE tog(42) ---\n{tog_plan}");
    assert!(
        tog_plan.contains("Index Scan using toggle_demo_v_idx"),
        "auto keeps the toggle form on the index when enabled; got:\n{tog_plan}"
    );
    assert!(
        tog_plan.contains("Index Cond: (v = 42)"),
        "the kept plan should be a per-execution custom plan (literal 42); got:\n{tog_plan}"
    );
    assert!(
        !tog_plan.contains("Seq Scan"),
        "auto must not degrade the enabled toggle form to a seq scan; got:\n{tog_plan}"
    );

    // ---- (2) Control: the degradation exists ONLY when a generic plan is
    //          FORCED — proving auto's choice above is meaningful, not an
    //          absence of any generic option. The seq-scan cost
    //          (0.00..1791.00) is the documented figure this fixture
    //          reproduces. -----------------------------------------------
    conn.execute_sql("SET plan_cache_mode = force_generic_plan")
        .expect("force generic");
    let forced_generic = explain(&mut conn, "EXPLAIN EXECUTE tog(42)");
    eprintln!("--- force_generic_plan, EXPLAIN EXECUTE tog(42) ---\n{forced_generic}");
    assert!(
        forced_generic.contains("Seq Scan"),
        "a FORCED generic plan degrades to a seq scan; got:\n{forced_generic}"
    );
    assert!(
        forced_generic.contains("cost=0.00..1791.00 rows=501 width=4"),
        "the forced-generic seq scan reproduces the documented cost figure; got:\n{forced_generic}"
    );
    assert!(
        forced_generic.contains("Filter") && forced_generic.contains("IS NULL"),
        "the forced generic plan keeps the unfolded `($1 IS NULL OR ...)` filter; got:\n{forced_generic}"
    );
    assert!(
        !forced_generic.contains("Index Scan"),
        "the forced generic plan must not use the index; got:\n{forced_generic}"
    );
    conn.execute_sql("RESET plan_cache_mode").expect("reset mode");
    conn.execute_sql("DEALLOCATE tog").expect("deallocate toggle form");

    // ---- (3) Adversarial NULL-warmup: run the DISABLED (NULL) form first
    //          to raise the average custom-plan cost (each NULL exec folds
    //          to a whole-table scan), then ENABLE it. auto must still keep
    //          the custom index plan — the high average custom cost does
    //          not lure it into the index-losing generic plan. ----------
    conn.execute_sql("PREPARE tog2(int4) AS SELECT id FROM toggle_demo WHERE ($1 IS NULL OR v = $1)")
        .expect("prepare null-warmup form");
    run_execs(&mut conn, "EXECUTE tog2(NULL)", 6);
    run_execs(&mut conn, "EXECUTE tog2(42)", 8);
    assert_auto_kept_custom(&mut conn, "tog2", 14);

    let tog2_plan = explain(&mut conn, "EXPLAIN EXECUTE tog2(42)");
    eprintln!("--- auto, EXPLAIN EXECUTE tog2(42) (after NULL warmup) ---\n{tog2_plan}");
    assert!(
        tog2_plan.contains("Index Scan using toggle_demo_v_idx")
            && tog2_plan.contains("Index Cond: (v = 42)"),
        "auto keeps the enabled toggle on the index even after a NULL warmup; got:\n{tog2_plan}"
    );
    assert!(
        !tog2_plan.contains("Seq Scan"),
        "the NULL warmup must not push the enabled form onto a seq scan; got:\n{tog2_plan}"
    );
    conn.execute_sql("DEALLOCATE tog2").expect("deallocate null-warmup form");

    // ---- (4) Adversarial low-selectivity value: `bucket = 1` matches ~25%
    //          of the table, so the index is used via a BITMAP scan rather
    //          than a plain Index Scan. auto keeps the custom bitmap plan;
    //          it does not fall back to the generic full scan. ----------
    conn.execute_sql(
        "PREPARE tog3(int4) AS SELECT id FROM toggle_demo WHERE ($1 IS NULL OR bucket = $1)",
    )
    .expect("prepare low-selectivity form");
    run_execs(&mut conn, "EXECUTE tog3(1)", 12);
    assert_auto_kept_custom(&mut conn, "tog3", 12);

    let tog3_plan = explain(&mut conn, "EXPLAIN EXECUTE tog3(1)");
    eprintln!("--- auto, EXPLAIN EXECUTE tog3(1) (low selectivity) ---\n{tog3_plan}");
    assert!(
        tog3_plan.contains("Bitmap Heap Scan"),
        "a low-selectivity value drives a bitmap heap scan; got:\n{tog3_plan}"
    );
    assert!(
        tog3_plan.contains("Bitmap Index Scan on toggle_demo_bucket_idx"),
        "the bitmap scan must use the bucket index; got:\n{tog3_plan}"
    );
    assert!(
        tog3_plan.contains("Index Cond: (bucket = 1)"),
        "the kept plan is a per-execution custom plan (literal bucket = 1); got:\n{tog3_plan}"
    );
    assert!(
        tog3_plan.contains("Recheck Cond: (bucket = 1)"),
        "the bitmap heap scan rechecks the bucket condition; got:\n{tog3_plan}"
    );
    assert!(
        !tog3_plan.contains("Seq Scan"),
        "auto must not degrade the low-selectivity form to a seq scan; got:\n{tog3_plan}"
    );
    conn.execute_sql("DEALLOCATE tog3").expect("deallocate low-selectivity form");

    // ---- (5) Adversarial multi-toggle: two optional filters in one form,
    //          driven through every enable/disable combination. auto keeps
    //          each enabled filter on its index (v → Index Scan, bucket →
    //          Bitmap) and adopts no generic plan. ----------------------
    conn.execute_sql(
        "PREPARE togm(int4, int4) AS SELECT id FROM toggle_demo \
         WHERE ($1 IS NULL OR v = $1) AND ($2 IS NULL OR bucket = $2)",
    )
    .expect("prepare multi-toggle form");
    for _ in 0..3 {
        run_execs(&mut conn, "EXECUTE togm(42, NULL)", 1); // only v enabled
        run_execs(&mut conn, "EXECUTE togm(NULL, 1)", 1); // only bucket enabled
        run_execs(&mut conn, "EXECUTE togm(42, 1)", 1); // both enabled
        run_execs(&mut conn, "EXECUTE togm(NULL, NULL)", 1); // both disabled
    }
    assert_auto_kept_custom(&mut conn, "togm", 12);

    let togm_v = explain(&mut conn, "EXPLAIN EXECUTE togm(42, NULL)");
    eprintln!("--- auto, EXPLAIN EXECUTE togm(42, NULL) (v enabled) ---\n{togm_v}");
    assert!(
        togm_v.contains("Index Scan using toggle_demo_v_idx")
            && togm_v.contains("Index Cond: (v = 42)"),
        "auto keeps the v-enabled multi-toggle on the v index; got:\n{togm_v}"
    );
    assert!(
        !togm_v.contains("Seq Scan"),
        "the v-enabled multi-toggle must not degrade to a seq scan; got:\n{togm_v}"
    );

    let togm_bucket = explain(&mut conn, "EXPLAIN EXECUTE togm(NULL, 1)");
    eprintln!("--- auto, EXPLAIN EXECUTE togm(NULL, 1) (bucket enabled) ---\n{togm_bucket}");
    assert!(
        togm_bucket.contains("Bitmap Heap Scan")
            && togm_bucket.contains("Index Cond: (bucket = 1)"),
        "auto keeps the bucket-enabled multi-toggle on the bucket index via a bitmap scan; got:\n{togm_bucket}"
    );
    assert!(
        !togm_bucket.contains("Seq Scan"),
        "the bucket-enabled multi-toggle must not degrade to a seq scan; got:\n{togm_bucket}"
    );
    conn.execute_sql("DEALLOCATE togm").expect("deallocate multi-toggle form");

    // ---- (6) Collateral: a plain `WHERE id = $1` lookup. Under auto its
    //          generic plan is cached and reused (Index Cond shows `$1`).
    //          Under a session-wide force_custom_plan it re-plans every
    //          execution (Index Cond shows the literal `42`) — real
    //          planning cost, for no benefit. This is why the session-wide
    //          override was rejected. -----------------------------------
    conn.execute_sql("PREPARE pk(int4) AS SELECT v FROM toggle_demo WHERE id = $1")
        .expect("prepare plain lookup");
    run_execs(&mut conn, "EXECUTE pk(42)", 12);
    let (pk_generic, pk_custom) = plan_counts(&mut conn, "pk");
    eprintln!("--- auto, pk after 12 execs: generic={pk_generic} custom={pk_custom} ---");
    assert_eq!(
        pk_custom, 5,
        "under auto the plain lookup uses 5 custom plans before the generic switchover; got custom_plans={pk_custom}"
    );
    assert_eq!(
        pk_generic, 7,
        "under auto the plain lookup caches a generic plan from exec 6 on (execs 6-12); got generic_plans={pk_generic}"
    );
    let pk_auto = explain(&mut conn, "EXPLAIN EXECUTE pk(42)");
    eprintln!("--- auto, EXPLAIN EXECUTE pk(42) ---\n{pk_auto}");
    assert!(
        pk_auto.contains("id = $1"),
        "under auto the plain lookup uses the cached GENERIC plan (placeholder $1); got:\n{pk_auto}"
    );
    conn.execute_sql("DEALLOCATE pk").expect("deallocate plain lookup");

    conn.execute_sql("SET plan_cache_mode = force_custom_plan")
        .expect("force custom");
    conn.execute_sql("PREPARE pk2(int4) AS SELECT v FROM toggle_demo WHERE id = $1")
        .expect("prepare plain lookup under force_custom");
    run_execs(&mut conn, "EXECUTE pk2(42)", 12);
    let (pk2_generic, pk2_custom) = plan_counts(&mut conn, "pk2");
    eprintln!("--- force_custom_plan, pk2 after 12 execs: generic={pk2_generic} custom={pk2_custom} ---");
    assert_eq!(
        pk2_generic, 0,
        "force_custom_plan disables generic-plan caching even for the plain lookup; got generic_plans={pk2_generic}"
    );
    assert_eq!(
        pk2_custom, 12,
        "force_custom_plan re-plans the plain lookup on EVERY execution; got custom_plans={pk2_custom}"
    );
    let pk_custom_plan = explain(&mut conn, "EXPLAIN EXECUTE pk2(42)");
    eprintln!("--- force_custom_plan, EXPLAIN EXECUTE pk2(42) ---\n{pk_custom_plan}");
    assert!(
        pk_custom_plan.contains("id = 42"),
        "under force_custom_plan the plain lookup is re-planned per execution (literal 42); got:\n{pk_custom_plan}"
    );
    conn.execute_sql("RESET plan_cache_mode").expect("reset mode");
    conn.execute_sql("DEALLOCATE pk2").expect("deallocate plain lookup");
    conn.close().expect("close");

    // The evidence above is the whole assertion: PostgreSQL's default `auto`
    // keeps every toggle shape on the index and caches the plain lookup's
    // generic plan, so the engine relies on it and issues no override —
    // there is no plan-cache knob to pin.
}
