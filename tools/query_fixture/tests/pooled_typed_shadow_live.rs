//! THE CROSS-USER WRONG-RESULT REGRESSION (BLOCKER) — TYPED twin (both drivers).
//!
//! The typed peer of the driver crates'
//! `pooled_dynamic_plan_re_resolves_a_temp_shadow_across_users`: the pool must DROP
//! a connection's TYPED (`query!`) prepared-statement cache on checkout, not just
//! the dynamic one, so a pooled connection behaves exactly like a fresh one.
//!
//! User 1 PROMOTES a compile-checked `query::<Pts*>()` over the PERMANENT
//! `public.pts_*` table to a KEPT named server-side statement whose relation name
//! is resolved to `public.pts_*` at `Parse` (pg_temp pre-active + looped past the
//! custom→generic threshold, so a later temp shadow does not auto-invalidate the
//! plan). After returning to the pool, User 2 checks out the SAME connection (pool
//! size 1) and creates a `CREATE TEMP TABLE pts_*` with IDENTICAL columns. The
//! IDENTICAL columns are the crux: the result type is UNCHANGED, so PostgreSQL's
//! `0A000` ("cached plan must not change result type") never fires, and on the OLD
//! kept-warm behaviour a typed cache HIT reused the plan with a bare
//! `Bind`+`Execute` that sent no `Describe`, so the result-schema OID guard never
//! ran. Neither the guard nor `0A000` covers this same-type / different-data-source
//! case — only DROPPING the typed cache does.
//!
//! With the fix, User 2's `query::<Pts*>()` is a cache MISS that re-`Parse`s fresh
//! against User 2's schema and resolves to the TEMP table, so it MUST read the TEMP
//! row. Reading the PERMANENT row here is the silent cross-tenant leak.
//!
//! PARALLEL-SAFE: the async + sync witnesses use SEPARATE permanent tables
//! (`pts_async` / `pts_sync`, from `0024_typed_shadow.sql`), each DROP+CREATEd by
//! its own test, and User 2's shadow is a session-local `TEMP` table — so the two
//! run concurrently in one binary without interfering.
//!
//! Run with:
//!   cargo test -p bsql-query-fixture --test pooled_typed_shadow_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

// The compile-checked carriers, validated at build time against `0024_typed_shadow.sql`.
// One table per driver so the two witnesses never race on a shared permanent table.
bsql::query!(PtsAsync, "SELECT val FROM pts_async");
bsql::query!(PtsSync, "SELECT val FROM pts_sync");

mod sync_driver {
    use super::PtsSync;
    use bsql_postgres_sync::{ConnectConfig, Connection, Pool, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn pooled_typed_plan_re_resolves_a_temp_shadow_across_users() {
        let cfg = config();

        // A PERMANENT `public.pts_sync` with User 1's distinguishable row.
        {
            let mut setup = Connection::connect(&cfg).expect("setup connect");
            setup.execute_sql("DROP TABLE IF EXISTS public.pts_sync").expect("drop");
            setup
                .execute_sql("CREATE TABLE public.pts_sync (val text NOT NULL)")
                .expect("create permanent");
            setup
                .execute_sql("INSERT INTO public.pts_sync (val) VALUES ('PERMANENT')")
                .expect("seed permanent");
            setup.close().expect("setup close");
        }

        // Pool size 1 → User 2 reuses the EXACT connection User 1 warmed.
        let pool = Pool::new(cfg.clone(), 1);

        // User 1: promote the typed carrier to a KEPT named server-side statement
        // bound to `public.pts_sync`. pg_temp pre-active (so a later shadow does not
        // change the search-path OID list → no auto-invalidation); loop past the
        // custom→generic threshold so the kept plan is the generic one.
        {
            let mut g = pool.get().expect("user1 checkout");
            let c = g.conn_mut().expect("user1 conn");
            c.execute_sql("CREATE TEMP TABLE _pgtemp_activate (x int4)")
                .expect("activate pg_temp for the connection's lifetime");
            for _ in 0..12 {
                let row = c.query_one::<PtsSync>(()).expect("user1 typed query");
                assert_eq!(row.val, "PERMANENT", "user 1 reads the permanent table");
            }
            // Back to the pool. On User 2's checkout the reset DROPS the typed cache.
        }

        // User 2: the SAME connection. Shadow the name with an IDENTICAL-column TEMP
        // table (so the result type is unchanged — the OID guard does NOT fire), then
        // run the IDENTICAL carrier: a cache MISS (the reset dropped the typed cache),
        // so it re-`Parse`s fresh against User 2's schema and resolves to the temp.
        {
            let mut g = pool.get().expect("user2 checkout");
            let c = g.conn_mut().expect("user2 conn");
            c.execute_sql("CREATE TEMP TABLE pts_sync (val text NOT NULL)")
                .expect("user2 temp table");
            c.execute_sql("INSERT INTO pts_sync (val) VALUES ('TEMP-USER-2')")
                .expect("user2 temp seed");

            let row = c.query_one::<PtsSync>(()).expect("user2 typed query");
            assert_eq!(
                row.val, "TEMP-USER-2",
                "user 2 MUST read their OWN temp table's row — the reset dropped the typed cache so \
                 the query re-parsed fresh; reading 'PERMANENT' here is the silent cross-user wrong result",
            );
        }

        // Cleanup the permanent table.
        {
            let mut cleanup = Connection::connect(&cfg).expect("cleanup connect");
            cleanup.execute_sql("DROP TABLE IF EXISTS public.pts_sync").expect("cleanup drop");
            cleanup.close().expect("cleanup close");
        }
    }
}

mod async_driver {
    use super::PtsAsync;
    use bsql_postgres_async::{ConnectConfig, Connection, Pool, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn pooled_typed_plan_re_resolves_a_temp_shadow_across_users() {
        let cfg = config();

        // A PERMANENT `public.pts_async` with User 1's distinguishable row.
        {
            let mut setup = Connection::connect(&cfg).await.expect("setup connect");
            setup.execute_sql("DROP TABLE IF EXISTS public.pts_async").await.expect("drop");
            setup
                .execute_sql("CREATE TABLE public.pts_async (val text NOT NULL)")
                .await
                .expect("create permanent");
            setup
                .execute_sql("INSERT INTO public.pts_async (val) VALUES ('PERMANENT')")
                .await
                .expect("seed permanent");
            setup.close().await.expect("setup close");
        }

        // Pool size 1 → User 2 reuses the EXACT connection User 1 warmed.
        let pool = Pool::new(cfg.clone(), 1);

        // User 1: promote the typed carrier to a KEPT named server-side statement
        // bound to `public.pts_async` (see the sync twin for the two reproduction
        // conditions: pg_temp pre-active + the custom→generic loop).
        {
            let mut g = pool.get().await.expect("user1 checkout");
            let c = g.conn_mut().expect("user1 conn");
            c.execute_sql("CREATE TEMP TABLE _pgtemp_activate (x int4)")
                .await
                .expect("activate pg_temp for the connection's lifetime");
            for _ in 0..12 {
                let row = c.query_one::<PtsAsync>(()).await.expect("user1 typed query");
                assert_eq!(row.val, "PERMANENT", "user 1 reads the permanent table");
            }
        }

        // User 2: the SAME connection — shadow with an IDENTICAL-column TEMP table,
        // then re-run the carrier: a MISS (the reset dropped the typed cache), so it
        // re-`Parse`s fresh and resolves to the temp.
        {
            let mut g = pool.get().await.expect("user2 checkout");
            let c = g.conn_mut().expect("user2 conn");
            c.execute_sql("CREATE TEMP TABLE pts_async (val text NOT NULL)")
                .await
                .expect("user2 temp table");
            c.execute_sql("INSERT INTO pts_async (val) VALUES ('TEMP-USER-2')")
                .await
                .expect("user2 temp seed");

            let row = c.query_one::<PtsAsync>(()).await.expect("user2 typed query");
            assert_eq!(
                row.val, "TEMP-USER-2",
                "user 2 MUST read their OWN temp table's row — the reset dropped the typed cache so \
                 the query re-parsed fresh; reading 'PERMANENT' here is the silent cross-user wrong result",
            );
        }

        // Cleanup the permanent table.
        {
            let mut cleanup = Connection::connect(&cfg).await.expect("cleanup connect");
            cleanup.execute_sql("DROP TABLE IF EXISTS public.pts_async").await.expect("cleanup drop");
            cleanup.close().await.expect("cleanup close");
        }
    }
}
