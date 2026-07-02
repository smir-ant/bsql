#![forbid(unsafe_code)]
use bsql_postgres_async::{ConnectConfig, Connection};

// ═══════════════════════════════════════════════════════════
// Driver-specific tests (async I/O, TLS, pool, protocol)
// SQL coverage is in the shared macro at the bottom.
// ═══════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "requires local PG"]
async fn connect_and_ping() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.ping().await.expect("ping");
    assert!(c.is_healthy());
    assert!(c.server_version().is_some());
    assert!(c.backend_pid() > 0);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_1k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 1000)").await.expect("q");
    assert_eq!(r.rows.len(), 1000);
    assert_eq!(r.rows[999].get_i32(0), Some(1000));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_10k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 10000)").await.expect("q");
    assert_eq!(r.rows.len(), 10000);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn error_recovery_and_resilience() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    assert!(c.simple_query("SELCT").await.is_err());
    assert!(c.query_sql("SELECT * FROM nonexistent_xyz").await.is_err());
    assert!(c.query_sql("SELECT 'abc'::int").await.is_err());
    assert!(c.query_sql("SELECT 1/0").await.is_err());
    c.ping().await.expect("recover");
    c.execute_sql("CREATE TEMP TABLE res(v int)").await.expect("create");
    c.execute_sql("INSERT INTO res VALUES (42)").await.expect("insert");
    assert_eq!(c.query_sql("SELECT v FROM res").await.expect("q").rows[0].get_i32(0), Some(42));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_reuse_after_constraint_violation() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE pr(id int PRIMARY KEY)").await.expect("create");
    let stmt = c.prepare("INSERT INTO pr VALUES ($1)").await.expect("prepare");
    c.execute_prepared(&stmt, &(1i32,)).await.expect("ok");
    assert!(c.execute_prepared(&stmt, &(1i32,)).await.is_err());
    c.execute_prepared(&stmt, &(2i32,)).await.expect("after error");
    c.close_statement(stmt).await.expect("close stmt");
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp(v text)").await.expect("create");
    assert_eq!(c.copy_in("cp", vec!["a", "b"]).await.expect("copy"), 2);
    assert_eq!(c.copy_in("cp", Vec::<&str>::new()).await.expect("empty"), 0);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn listen_notify() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut listener = Connection::connect(&config).await.expect("l");
    let mut notifier = Connection::connect(&config).await.expect("n");
    listener.listen("bsql_ch").await.expect("listen");
    notifier.simple_query("NOTIFY bsql_ch, 'hi'").await.expect("notify");
    let n = listener.recv_notification(std::time::Duration::from_secs(5))
        .await.expect("recv").expect("notif");
    assert_eq!(n.payload, "hi");
    listener.close().await.expect("close"); notifier.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_basic() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 3).await.expect("pool");
    let mut c = pool.get().await.expect("get");
    c.conn_mut().expect("live").ping().await.expect("ping");
    drop(c);
    assert_eq!(pool.idle_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires local PG"]
async fn pool_concurrent() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 3).await.expect("pool");
    let handles: Vec<_> = (0..10u32).map(|i| {
        let p = pool.clone();
        tokio::spawn(async move {
            let mut c = p.get().await.expect("get");
            assert_eq!(c.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int")).await.expect("q").rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    for h in handles { h.await.expect("task"); }
}

// ── S23 pool hardening (reset-on-return, acquire timeout, health eviction) ──

// The reset-vs-statement-cache CONSISTENCY proof (the same `query!` reused across
// pooled checkouts, asserting the server holds it exactly once) needs the
// build-time catalog env that `query!` requires, so it lives in the query fixture
// (`pooled_connection_reset_keeps_parsed_plan`). Here we prove isolation
// (no-bleed), backpressure (acquire timeout), and health eviction directly.

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_reset_on_return_no_bleed() {
    // A GUC and a temp table set by one checkout must NOT survive to the next
    // checkout of the SAME physical connection.
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 1).await.expect("pool"); // max_size=1 forces reuse
    let pid1 = {
        let mut c = pool.get().await.expect("get1");
        let conn = c.conn_mut().expect("live1");
        let pid = conn.backend_pid();
        conn.execute_sql("SET search_path TO 'pg_temp'").await.expect("set guc");
        conn.execute_sql("CREATE TEMP TABLE bleed_probe(x int)").await.expect("temp");
        conn.execute_sql("LISTEN bleed_chan").await.expect("listen");
        pid
    }; // returned to pool (dirty)
    let mut c = pool.get().await.expect("get2");
    let conn = c.conn_mut().expect("live2");
    assert_eq!(conn.backend_pid(), pid1, "max_size=1 must reuse the SAME physical connection");
    // GUC reset to default (not pg_temp).
    let sp = conn.query_sql("SHOW search_path").await.expect("show").rows[0]
        .get_str(0).map(String::from);
    assert_ne!(sp.as_deref(), Some("pg_temp"), "search_path GUC bled across checkout");
    // Temp table gone.
    let n = conn.query_sql("SELECT count(*) FROM pg_tables WHERE tablename='bleed_probe'")
        .await.expect("tmp").rows[0].get_i64(0);
    assert_eq!(n, Some(0), "temp table bled across checkout");
    // LISTEN channel gone (UNLISTEN * ran in the reset).
    let listening = conn
        .query_sql("SELECT count(*)::int8 FROM pg_listening_channels() AS c(chan) WHERE chan='bleed_chan'")
        .await.expect("listen check").rows[0].get_i64(0);
    assert_eq!(listening, Some(0), "LISTEN channel bled across checkout");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_get_cancellation_returns_permit() {
    // Regression proof for the permit-leak-on-cancellation bug: dropping a get()
    // future mid-reset must return the capacity permit, not leak it. With
    // max_size=1, a single leaked permit would make the pool permanently
    // PoolTimeout; this asserts a subsequent get() still succeeds.
    use bsql_postgres_async::Pool;
    use std::time::Duration;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 1).await.expect("pool");
    // Warm one connection into the idle set so the next get() takes the RESET
    // path: its first poll acquires the (now-free) permit synchronously, then the
    // reset round-trip suspends — so the future is Pending after one poll.
    {
        let mut c = pool.get().await.expect("warm");
        c.conn_mut().expect("live").ping().await.expect("ping");
    } // returned to idle; the permit is free again
    // Deterministically cancel the get() mid-reset: `biased` polls it first (it
    // acquires the permit, then suspends at the reset -> Pending), then the
    // always-ready branch wins and DROPS the get() future — regardless of load or
    // timer granularity. (A zero-duration `timeout` is NOT reliable here: a fast
    // reset can finish before the zero-delay timer registers as elapsed.)
    tokio::select! {
        biased;
        _ = pool.get() => panic!("get() must not complete before the ready branch (its reset suspends)"),
        () = std::future::ready(()) => {}
    }
    // If the permit leaked on that cancellation, this is a PoolTimeout; if the
    // owned permit was returned on drop, capacity is restored and get() succeeds.
    let mut c = pool
        .get_timeout(Duration::from_secs(5))
        .await
        .expect("capacity must be restored after a cancelled get() (no permit leak)");
    c.conn_mut().expect("live").ping().await.expect("ping after cancellation");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_acquire_timeout_not_hang() {
    // Exhaust a max_size=1 pool by holding the one connection; a second get with
    // a short deadline returns PoolTimeout rather than blocking forever.
    use bsql_postgres_async::Pool;
    use std::time::{Duration, Instant};
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 1).await.expect("pool");
    let _held = pool.get().await.expect("hold the one connection");
    let start = Instant::now();
    let err = pool.get_timeout(Duration::from_millis(200)).await;
    let elapsed = start.elapsed();
    assert!(matches!(err, Err(bsql_postgres_async::DriverError::PoolTimeout)),
        "exhausted pool must return PoolTimeout, got {err:?}");
    assert!(elapsed < Duration::from_secs(5), "must not hang (took {elapsed:?})");
    // After the held connection returns, a subsequent get succeeds.
    drop(_held);
    let mut c = pool.get_timeout(Duration::from_secs(5)).await.expect("get after release");
    c.conn_mut().expect("live").ping().await.expect("ping");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_evicts_dead_connection() {
    // A connection killed server-side is not handed back out; the pool creates a
    // fresh, healthy one instead.
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 1).await.expect("pool");
    let dead_pid = {
        let mut c = pool.get().await.expect("get");
        let conn = c.conn_mut().expect("live");
        let pid = conn.backend_pid();
        // Terminate THIS backend from itself: the next command sees a dead socket.
        let _ = conn.execute_sql(&format!("SELECT pg_terminate_backend({pid})")).await;
        // The connection is now unhealthy; returning it should NOT re-pool it.
        pid
    };
    // The dead connection was evicted on return (not re-pooled) or on acquire; a
    // fresh healthy connection is produced.
    let mut c = pool.get().await.expect("get fresh");
    let conn = c.conn_mut().expect("live fresh");
    conn.ping().await.expect("fresh connection is healthy");
    assert!(conn.backend_pid() != dead_pid || conn.is_healthy(),
        "a fresh healthy connection is served after eviction");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn row_send_across_await() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let row = c.query_sql("SELECT 42::int").await.expect("q").rows[0].clone();
    assert_eq!(tokio::task::spawn(async move { row.get_i32(0) }).await.expect("spawn"), Some(42));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth"]
async fn scram_auth() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string()).password("test_password_123".to_string());
    let mut c = Connection::connect(&config).await.expect("SCRAM");
    assert_eq!(c.query_sql("SELECT current_user").await.expect("q").rows[0].get_raw(0), Some(b"bsql_test_scram".as_slice()));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn ssl_modes() {
    for mode in [bsql_postgres_async::SslMode::Prefer, bsql_postgres_async::SslMode::Disable] {
        let config = ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string()).ssl_mode(mode);
        let mut c = Connection::connect(&config).await.expect("connect");
        c.ping().await.expect("ping");
        c.close().await.expect("close");
    }
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn full_lifecycle() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE lc(id serial PRIMARY KEY, name text, val int)").await.expect("create");
    c.begin().await.expect("begin");
    c.execute_sql("INSERT INTO lc(name, val) VALUES ('alice', 95)").await.expect("ins");
    c.execute_sql("INSERT INTO lc(name, val) VALUES ('bob', 88)").await.expect("ins");
    c.commit().await.expect("commit");
    assert_eq!(c.query_params_one("SELECT name FROM lc WHERE val > $1", &(90i32,))
        .await.expect("p").get_str(0), Some("alice"));
    let stmt = c.prepare("UPDATE lc SET val = val + $1 WHERE name = $2").await.expect("prep");
    c.execute_prepared(&stmt, &(5i32, "bob")).await.expect("update");
    c.close_statement(stmt).await.expect("close stmt");
    assert!(c.execute_sql("INSERT INTO lc(id) VALUES (1)").await.is_err());
    c.ping().await.expect("recover");
    c.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires local PG"]
async fn pool_stress_100_tasks() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 5).await.expect("pool");
    let handles: Vec<_> = (0..100u32).map(|i| {
        let p = pool.clone();
        tokio::spawn(async move {
            let mut c = p.get().await.expect("get");
            let r = c.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int, pg_backend_pid()")).await.expect("q");
            assert_eq!(r.rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    let mut ok = 0u32;
    for h in handles { if h.await.is_ok() { ok += 1; } }
    assert_eq!(ok, 100, "all 100 tasks should succeed");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn transaction_commit_and_recoverable_rollback() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE tx_demo(v int)").await.expect("create");

    // Commit path: both inserts land.
    c.transaction(async |conn| {
        conn.execute_sql("INSERT INTO tx_demo VALUES (1)").await?;
        conn.execute_sql("INSERT INTO tx_demo VALUES (2)").await?;
        Ok(())
    })
    .await
    .expect("transaction commits");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM tx_demo").await.expect("count").rows[0].get_i64(0),
        Some(2)
    );

    // Recoverable-error rollback path: a body error rolls back AND keeps the
    // connection pooled (the Outcome model — a query-level error never kills it).
    let result: Result<(), _> = c
        .transaction(async |conn| {
            conn.execute_sql("INSERT INTO tx_demo VALUES (3)").await?;
            conn.execute_sql("SELECT * FROM nonexistent_xyz").await?; // recoverable server error
            Ok(())
        })
        .await;
    assert!(result.is_err(), "a body error aborts the transaction");
    assert!(c.is_healthy(), "the connection survives a recoverable tx body error");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM tx_demo").await.expect("count").rows[0].get_i64(0),
        Some(2),
        "the failed transaction rolled back (row 3 is gone)"
    );
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn one_connection_everything() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    // DDL
    c.execute_sql("CREATE TEMP TABLE omni(id serial PRIMARY KEY, name text, val int, active bool)").await.expect("create");
    c.execute_sql("CREATE INDEX ON omni(val)").await.expect("index");

    // DML
    c.execute_sql("INSERT INTO omni(name, val, active) VALUES ('a', 10, true)").await.expect("ins");
    c.execute_sql("INSERT INTO omni(name, val, active) VALUES ('b', 20, false)").await.expect("ins");
    c.execute_sql("INSERT INTO omni(name, val, active) VALUES ('c', 30, true)").await.expect("ins");
    c.execute_params("INSERT INTO omni(name, val, active) VALUES ($1, $2, $3)", &("d", 40i32, true)).await.expect("params");

    // Query
    let r = c.query_sql("SELECT count(*) FROM omni").await.expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(4));

    // Query with params
    let r = c.query_params("SELECT name FROM omni WHERE val > $1 ORDER BY val", &(15i32,)).await.expect("qp");
    assert_eq!(r.rows.len(), 3);

    // Prepared
    let stmt = c.prepare("SELECT name, val FROM omni WHERE active = $1 ORDER BY val").await.expect("prep");
    let r = c.query_prepared(&stmt, &(true,)).await.expect("qprep");
    assert_eq!(r.rows.len(), 3);
    c.close_statement(stmt).await.expect("close stmt");

    // Transaction (begin/commit)
    c.begin().await.expect("begin");
    c.execute_sql("UPDATE omni SET val = val * 2 WHERE active").await.expect("update");
    c.commit().await.expect("commit");
    let r = c.query_sql("SELECT SUM(val) FROM omni").await.expect("sum");
    assert_eq!(r.rows[0].get_i64(0), Some(180));

    // Error + recovery
    assert!(c.query_sql("SELECT * FROM nonexistent").await.is_err());
    c.ping().await.expect("recover");

    // COPY IN
    c.execute_sql("CREATE TEMP TABLE cp_omni(v int)").await.expect("create cp");
    c.copy_in("cp_omni", vec!["1", "2", "3"]).await.expect("copy");

    // Column names
    let r = c.query_sql("SELECT id, name, val FROM omni LIMIT 1").await.expect("cols");
    assert_eq!(&*r.column_names, &["id", "name", "val"]);

    // Row clone across task
    let row = c.query_sql("SELECT 'final'::text").await.expect("q").rows[0].clone();
    let v = tokio::task::spawn(async move { row.get_str(0).map(String::from) }).await.expect("spawn");
    assert_eq!(v, Some("final".to_string()));

    c.close().await.expect("close");
}
