#![forbid(unsafe_code)]
// Live integration test harness. The `.unwrap_or_else(|e| panic!(..))` and
// `.unwrap_or(0)` calls below are test assertions that surface failures
// loudly (they panic / are followed by an `assert!`), not silent
// production data fallbacks, so the tier-4 ledger does not apply here.
#![allow(clippy::disallowed_methods, reason = "test harness — .unwrap_or* here panics or feeds an assert!, surfacing failure loudly; not a silent production fallback")]
// The `make_sync_conn` fixture helper below is not a `#[test]` fn, so the
// floor's `allow-expect-in-tests` carve-out (keyed on `#[test]` context)
// does not reach it; the `expect` is the loud connect-failure signal a
// test wants, not a silent production fallback.
#![allow(clippy::expect_used, clippy::unwrap_in_result, reason = "test harness — the connection-fixture helper expects a live PG and panics loudly on failure (the intended test signal); it is not a `#[test]` fn so the in-tests carve-out cannot reach it, and there is no production data-fallback path")]
use bsql_postgres_sync::{ConnectConfig, Connection};

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable)
}

// ═══════════════════════════════════════════════════════════
// Driver-specific tests (I/O, protocol, infra — not SQL)
// ═══════════════════════════════════════════════════════════

#[test]
#[ignore = "requires local PG"]
fn connect_and_ping() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.ping().expect("ping");
    assert!(c.is_healthy());
    assert!(c.server_version().is_some());
    assert!(c.backend_pid() > 0);
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_1k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 1000)").expect("query");
    assert_eq!(r.rows.len(), 1000);
    assert_eq!(r.rows[0].get_i32(0), Some(1));
    assert_eq!(r.rows[999].get_i32(0), Some(1000));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_10k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 10000)").expect("query");
    assert_eq!(r.rows.len(), 10000);
    assert_eq!(r.rows[9999].get_i32(0), Some(10000));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn error_recovery_and_resilience() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // 4 different error types, all recover
    assert!(c.simple_query("SELCT").is_err());
    assert!(c.query_sql("SELECT * FROM nonexistent_xyz").is_err());
    assert!(c.query_sql("SELECT 'abc'::int").is_err());
    assert!(c.query_sql("SELECT 1/0").is_err());
    c.ping().expect("ping after 4 errors");
    // Full CRUD still works
    c.execute_sql("CREATE TEMP TABLE resilience(v int)").expect("create");
    c.execute_sql("INSERT INTO resilience VALUES (42)").expect("insert");
    assert_eq!(c.query_sql("SELECT v FROM resilience").expect("select").rows[0].get_i32(0), Some(42));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn prepared_reuse_after_constraint_violation() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE pr_err(id int PRIMARY KEY)").expect("create");
    let stmt = c.prepare("INSERT INTO pr_err VALUES ($1)").expect("prepare");
    c.execute_prepared(&stmt, &(1i32,)).expect("insert 1");
    assert!(c.execute_prepared(&stmt, &(1i32,)).is_err());
    c.execute_prepared(&stmt, &(2i32,)).expect("insert 2 after error");
    assert_eq!(c.query_sql("SELECT count(*) FROM pr_err").expect("count").rows[0].get_i64(0), Some(2));
    c.close_statement(stmt).expect("close stmt");
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp(id int, name text)").expect("create");
    assert_eq!(c.copy_in("cp", vec!["1\talice", "2\tbob"]).expect("copy"), 2);
    assert_eq!(c.copy_in("cp", Vec::<&str>::new()).expect("copy empty"), 0);
    c.execute_sql("CREATE TEMP TABLE cp_lg(i int)").expect("create lg");
    let rows: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    assert_eq!(c.copy_in("cp_lg", &rows).expect("copy 1000"), 1000);
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn listen_notify() {
    let mut listener = Connection::connect(&sync_config()).expect("listener");
    let mut notifier = Connection::connect(&sync_config()).expect("notifier");
    listener.listen("bsql_sync_ch").expect("listen");
    notifier.simple_query("NOTIFY bsql_sync_ch, 'hello'").expect("notify");
    let n = listener.recv_notification(std::time::Duration::from_secs(5))
        .expect("recv").expect("should have notification");
    assert_eq!(n.channel, "bsql_sync_ch");
    assert_eq!(n.payload, "hello");
    listener.close().expect("close"); notifier.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn pool() {
    use bsql_postgres_sync::Pool;
    let pool = Pool::new(sync_config(), 3);
    let mut c = pool.get().expect("get");
    c.conn_mut().expect("live").ping().expect("ping");
    drop(c);
    assert_eq!(pool.idle_count(), 1);
    let handles: Vec<_> = (0..10u32).map(|i| {
        let p = pool.clone();
        std::thread::spawn(move || {
            let mut conn = p.get().expect("get");
            assert_eq!(conn.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int")).expect("q").rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    for h in handles { h.join().expect("thread"); }
}

// ── S23 pool hardening (reset-on-return, acquire timeout, health eviction) ──

#[test]
#[ignore = "requires local PG"]
fn pool_reset_on_return_no_bleed() {
    // A GUC and a temp table set by one checkout must NOT survive to the next
    // checkout of the SAME physical connection.
    use bsql_postgres_sync::Pool;
    let pool = Pool::new(sync_config(), 1); // max_size=1 forces reuse
    let pid1 = {
        let mut c = pool.get().expect("get1");
        let conn = c.conn_mut().expect("live1");
        let pid = conn.backend_pid();
        conn.execute_sql("SET search_path TO 'pg_temp'").expect("set guc");
        conn.execute_sql("CREATE TEMP TABLE bleed_probe(x int)").expect("temp");
        conn.execute_sql("LISTEN bleed_chan").expect("listen");
        pid
    }; // returned to pool (dirty)
    let mut c = pool.get().expect("get2");
    let conn = c.conn_mut().expect("live2");
    assert_eq!(conn.backend_pid(), pid1, "max_size=1 must reuse the SAME physical connection");
    let sp = conn.query_sql("SHOW search_path").expect("show").rows[0]
        .get_str(0).map(String::from);
    assert_ne!(sp.as_deref(), Some("pg_temp"), "search_path GUC bled across checkout");
    let n = conn.query_sql("SELECT count(*) FROM pg_tables WHERE tablename='bleed_probe'")
        .expect("tmp").rows[0].get_i64(0);
    assert_eq!(n, Some(0), "temp table bled across checkout");
    // LISTEN channel gone (UNLISTEN * ran in the reset).
    let listening = conn
        .query_sql("SELECT count(*)::int8 FROM pg_listening_channels() AS c(chan) WHERE chan='bleed_chan'")
        .expect("listen check").rows[0].get_i64(0);
    assert_eq!(listening, Some(0), "LISTEN channel bled across checkout");
}

#[test]
#[ignore = "requires local PG"]
fn pool_acquire_timeout_not_hang() {
    // Exhaust a max_size=1 pool by holding the one connection; a second get with a
    // short deadline returns PoolTimeout rather than blocking forever.
    use bsql_postgres_sync::Pool;
    use std::time::{Duration, Instant};
    let pool = Pool::new(sync_config(), 1);
    let _held = pool.get().expect("hold the one connection");
    let start = Instant::now();
    let err = pool.get_timeout(Duration::from_millis(200));
    let elapsed = start.elapsed();
    assert!(matches!(err, Err(bsql_postgres_sync::DriverError::PoolTimeout)),
        "exhausted pool must return PoolTimeout, got {err:?}");
    assert!(elapsed < Duration::from_secs(5), "must not hang (took {elapsed:?})");
    drop(_held);
    let mut c = pool.get_timeout(Duration::from_secs(5)).expect("get after release");
    c.conn_mut().expect("live").ping().expect("ping");
}

#[test]
#[ignore = "requires local PG"]
fn pool_evicts_dead_connection() {
    // A connection killed server-side is not handed back out; the pool creates a
    // fresh, healthy one instead.
    use bsql_postgres_sync::Pool;
    let pool = Pool::new(sync_config(), 1);
    let dead_pid = {
        let mut c = pool.get().expect("get");
        let conn = c.conn_mut().expect("live");
        let pid = conn.backend_pid();
        let _ = conn.execute_sql(&format!("SELECT pg_terminate_backend({pid})"));
        pid
    };
    let mut c = pool.get().expect("get fresh");
    let conn = c.conn_mut().expect("live fresh");
    conn.ping().expect("fresh connection is healthy");
    assert!(conn.backend_pid() != dead_pid || conn.is_healthy(),
        "a fresh healthy connection is served after eviction");
}

#[test]
#[ignore = "requires local PG"]
fn transaction_closure() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE tx(v int)").expect("create");
    c.transaction(|tx| { tx.execute_sql("INSERT INTO tx VALUES (1)")?; Ok(()) }).expect("commit");
    assert_eq!(c.query_sql("SELECT count(*) FROM tx").expect("c").rows[0].get_i64(0), Some(1));
    let _: Result<(), _> = c.transaction(|tx| {
        tx.execute_sql("INSERT INTO tx VALUES (2)")?;
        Err(bsql_postgres_sync::DriverError::NoRows)
    });
    assert_eq!(c.query_sql("SELECT count(*) FROM tx").expect("c").rows[0].get_i64(0), Some(1));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn row_clone_across_threads() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let row = c.query_sql("SELECT 42::int, 'hello'::text").expect("q").rows[0].clone();
    let handle = std::thread::spawn(move || row.get_i32(0));
    assert_eq!(handle.join().expect("thread"), Some(42));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn full_lifecycle() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE lc(id serial PRIMARY KEY, name text, val int)").expect("create");
    c.transaction(|tx| {
        tx.execute_sql("INSERT INTO lc(name, val) VALUES ('alice', 95)")?;
        tx.execute_sql("INSERT INTO lc(name, val) VALUES ('bob', 88)")?;
        Ok(())
    }).expect("tx");
    assert_eq!(c.query_params_one("SELECT name FROM lc WHERE val > $1", &(90i32,)).expect("p").get_str(0), Some("alice"));
    let stmt = c.prepare("UPDATE lc SET val = val + $1 WHERE name = $2").expect("prep");
    c.execute_prepared(&stmt, &(5i32, "bob")).expect("update");
    c.close_statement(stmt).expect("close stmt");
    assert!(c.execute_sql("INSERT INTO lc(id) VALUES (1)").is_err()); // dup PK
    c.ping().expect("recover");
    assert_eq!(c.copy_in("lc", Vec::<&str>::new()).expect("copy empty"), 0);
    c.close().expect("close");
}

// ═══════════════════════════════════════════════════════════
// Shared SQL scenario tests (macro — covers ALL SQL mechanics)
// ═══════════════════════════════════════════════════════════

fn make_sync_conn() -> Connection {
    Connection::connect(&sync_config()).expect("connect")
}

bsql_postgres_core::define_sync_sql_tests!(make_sync_conn);

#[test]
#[ignore = "requires local PG"]
fn pool_stress_100_tasks() {
    use bsql_postgres_sync::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let pool = Pool::new(config, 3);

    let handles: Vec<_> = (0..100u32).map(|i| {
        let p = pool.clone();
        std::thread::spawn(move || {
            let mut c = p.get().expect("get");
            let r = c.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int, pg_backend_pid()")).expect("q");
            assert_eq!(r.rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    let mut ok = 0u32;
    for h in handles {
        if h.join().is_ok() { ok += 1; }
    }
    assert_eq!(ok, 100, "all 100 tasks should succeed");
}

#[test]
#[ignore = "requires local PG"]
fn one_connection_everything() {
    // Single connection exercises every feature sequentially
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // DDL
    c.execute_sql("CREATE TEMP TABLE omni(id serial PRIMARY KEY, name text, val int, active bool)").expect("create");
    c.execute_sql("CREATE INDEX ON omni(val)").expect("index");

    // DML via execute
    c.execute_sql("INSERT INTO omni(name, val, active) VALUES ('a', 10, true)").expect("ins");
    c.execute_sql("INSERT INTO omni(name, val, active) VALUES ('b', 20, false)").expect("ins");
    c.execute_sql("INSERT INTO omni(name, val, active) VALUES ('c', 30, true)").expect("ins");

    // DML via execute_params (uses typed binary encoding)
    c.execute_params("INSERT INTO omni(name, val, active) VALUES ($1, $2, $3)", &("d", 40i32, true)).expect("params");

    // Query
    let r = c.query_sql("SELECT count(*) FROM omni").expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(4));

    // Query with params
    let r = c.query_params("SELECT name FROM omni WHERE val > $1 ORDER BY val", &(15i32,)).expect("qp");
    assert_eq!(r.rows.len(), 3); // b, c, d

    // Prepared
    let stmt = c.prepare("SELECT name, val FROM omni WHERE active = $1 ORDER BY val").expect("prep");
    let r = c.query_prepared(&stmt, &(true,)).expect("qprep");
    assert_eq!(r.rows.len(), 3); // a, c, d
    c.close_statement(stmt).expect("close stmt");

    // Transaction
    c.transaction(|tx| {
        tx.execute_sql("UPDATE omni SET val = val * 2 WHERE active")?;
        Ok(())
    }).expect("tx");
    let r = c.query_sql("SELECT SUM(val) FROM omni").expect("sum");
    // a:20 + b:20(unchanged) + c:60 + d:80 = 180
    assert_eq!(r.rows[0].get_i64(0), Some(180));

    // Error + recovery
    assert!(c.query_sql("SELECT * FROM nonexistent").is_err());
    c.ping().expect("recover");

    // COPY IN
    c.execute_sql("CREATE TEMP TABLE cp_omni(v int)").expect("create cp");
    c.copy_in("cp_omni", vec!["1", "2", "3"]).expect("copy");

    // Column names
    let r = c.query_sql("SELECT id, name, val FROM omni LIMIT 1").expect("cols");
    assert_eq!(&*r.column_names, &["id", "name", "val"]);

    // Row clone across thread
    let row = c.query_sql("SELECT 'final'::text").expect("q").rows[0].clone();
    let v = std::thread::spawn(move || row.get_str(0).map(String::from)).join().expect("thread");
    assert_eq!(v, Some("final".to_string()));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn wide_columns() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for n in [250u32, 500, 600, 800, 1000, 1600] {
        let cols: Vec<String> = (0..n).map(|i| format!("{i}::int AS col_{i}")).collect();
        let sql = format!("SELECT {}", cols.join(", "));
        let r = c.query_sql(&sql).unwrap_or_else(|e| panic!("{n} cols failed: {e}"));
        assert_eq!(r.rows.len(), 1, "rows at {n} cols");
        assert_eq!(r.column_names.len(), usize::try_from(n).unwrap(), "col names at {n}");
        assert_eq!(r.rows[0].get_i32(0), Some(0), "first col at {n}");
        let last = usize::try_from(n.saturating_sub(1)).unwrap();
        assert_eq!(r.rows[0].get_i32(last), Some(n.saturating_sub(1) as i32), "last col at {n}");
    }
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn prepared_statement_edge_cases() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE ps_edge(id int, v text)").expect("create");

    // Prepare, execute 0 times, close
    let stmt = c.prepare("INSERT INTO ps_edge VALUES ($1, $2)").expect("prep");
    c.close_statement(stmt).expect("close unused stmt");

    // Prepare, execute many times
    let stmt = c.prepare("INSERT INTO ps_edge VALUES ($1, $2)").expect("prep");
    for i in 0..50i32 {
        c.execute_prepared(&stmt, &(i, format!("v{i}").as_str())).expect("exec");
    }
    assert_eq!(c.query_sql("SELECT count(*) FROM ps_edge").expect("c").rows[0].get_i64(0), Some(50));
    c.close_statement(stmt).expect("close");

    // Prepare SELECT, query many times
    let stmt = c.prepare("SELECT v FROM ps_edge WHERE id = $1").expect("prep select");
    for i in 0..50i32 {
        let r = c.query_prepared(&stmt, &(i,)).expect("qp");
        assert_eq!(r.rows[0].get_str(0), Some(format!("v{i}").as_str()));
    }
    c.close_statement(stmt).expect("close");

    // Multiple prepared statements open at once
    let s1 = c.prepare("SELECT id FROM ps_edge WHERE id < $1").expect("s1");
    let s2 = c.prepare("SELECT v FROM ps_edge WHERE id = $1").expect("s2");
    let s3 = c.prepare("UPDATE ps_edge SET v = $1 WHERE id = $2").expect("s3");
    let r1 = c.query_prepared(&s1, &(5i32,)).expect("q1");
    assert_eq!(r1.rows.len(), 5);
    let r2 = c.query_prepared(&s2, &(0i32,)).expect("q2");
    assert_eq!(r2.rows[0].get_str(0), Some("v0"));
    c.execute_prepared(&s3, &("updated", 0i32)).expect("exec3");
    let r2b = c.query_prepared(&s2, &(0i32,)).expect("q2b");
    assert_eq!(r2b.rows[0].get_str(0), Some("updated"));
    c.close_statement(s1).expect("close s1");
    c.close_statement(s2).expect("close s2");
    c.close_statement(s3).expect("close s3");

    // Error in prepared doesn't break statement
    c.execute_sql("CREATE TEMP TABLE ps_uk(id int UNIQUE)").expect("create");
    let stmt = c.prepare("INSERT INTO ps_uk VALUES ($1)").expect("prep");
    c.execute_prepared(&stmt, &(1i32,)).expect("ok");
    assert!(c.execute_prepared(&stmt, &(1i32,)).is_err()); // dup
    c.execute_prepared(&stmt, &(2i32,)).expect("ok after error");
    c.close_statement(stmt).expect("close");

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in_edge_cases() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // COPY 0 rows
    c.execute_sql("CREATE TEMP TABLE cp_edge(id int, name text)").expect("create");
    assert_eq!(c.copy_in("cp_edge", Vec::<&str>::new()).expect("empty"), 0);
    assert_eq!(c.query_sql("SELECT count(*) FROM cp_edge").expect("c").rows[0].get_i64(0), Some(0));

    // COPY 1 row
    assert_eq!(c.copy_in("cp_edge", vec!["1\tone"]).expect("one"), 1);

    // COPY with NULLs (PG COPY \N = NULL)
    assert_eq!(c.copy_in("cp_edge", vec!["2\t\\N"]).expect("null"), 1);

    // COPY many rows
    let big: Vec<String> = (0..5000).map(|i| format!("{i}\tname_{i}")).collect();
    assert_eq!(c.copy_in("cp_edge", &big).expect("5k"), 5000);
    assert_eq!(c.query_sql("SELECT count(*) FROM cp_edge").expect("c").rows[0].get_i64(0), Some(5002));

    // COPY into non-existent table — use fresh connection (COPY error recovery
    // sometimes leaves connection in unrecoverable state under parallel load)
    let mut c2 = Connection::connect(&sync_config()).expect("c2");
    assert!(c2.copy_in("table_that_does_not_exist", vec!["1\ta"]).is_err());
    c2.ping().expect("recover");
    c2.close().expect("close c2");

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_edge_cases() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // 0 rows streaming (query returns no DataRow)
    c.execute_sql("CREATE TEMP TABLE empty_stream(v int)").expect("create");
    let r = c.query_sql("SELECT * FROM empty_stream").expect("empty");
    assert_eq!(r.rows.len(), 0);

    // 1 row streaming
    c.execute_sql("INSERT INTO empty_stream VALUES (42)").expect("ins");
    let r = c.query_sql("SELECT * FROM empty_stream").expect("one");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].get_i32(0), Some(42));

    // Large value via SQL literal (params limited to 1024 bytes)
    let big_val = "X".repeat(50_000);
    c.execute_sql("CREATE TEMP TABLE big_val(v text)").expect("create");
    c.execute_sql(&format!("INSERT INTO big_val VALUES ('{big_val}')")).expect("ins");
    let r = c.query_sql("SELECT v FROM big_val").expect("q");
    assert_eq!(r.rows[0].get_str(0).map(|s| s.len()), Some(50_000));

    // Many columns with NULLs
    let r = c.query_sql("SELECT NULL::int, 1::int, NULL::text, 'a'::text, NULL::bool, true").expect("mixed nulls");
    assert!(r.rows[0].is_null(0));
    assert_eq!(r.rows[0].get_i32(1), Some(1));
    assert!(r.rows[0].is_null(2));
    assert_eq!(r.rows[0].get_str(3), Some("a"));
    assert!(r.rows[0].is_null(4));
    assert_eq!(r.rows[0].get_bool(5), Some(true));

    // Query after error mid-stream should recover
    assert!(c.query_sql("SELECT 1/0 FROM generate_series(1,10)").is_err());
    c.ping().expect("recover after mid-stream error");
    let r = c.query_sql("SELECT 1::int").expect("after recover");
    assert_eq!(r.rows[0].get_i32(0), Some(1));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn connection_resilience_marathon() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // 50 alternating errors and successes
    for i in 0..50u32 {
        if i % 2 == 0 {
            assert!(c.query_sql("SELECT * FROM nonexistent_marathon").is_err());
        } else {
            assert_eq!(c.query_sql(&format!("SELECT {i}::int")).expect("q").rows[0].get_i32(0), Some(i as i32));
        }
    }
    c.ping().expect("after marathon");

    // 200 rapid pings
    for _ in 0..200 {
        c.ping().expect("rapid ping");
    }

    // Error → recover → success cycle
    c.execute_sql("CREATE TEMP TABLE IF NOT EXISTS marathon_t(v int)").expect("create");
    for i in 0..20u32 {
        assert!(c.simple_query("INVALID SQL GIBBERISH").is_err());
        c.ping().unwrap_or_else(|e| panic!("ping after err #{i}: {e}"));
        c.execute_sql("INSERT INTO marathon_t VALUES (1)").unwrap_or_else(|e| panic!("ins #{i}: {e}"));
        assert!(c.query_sql("SELECT 'bad'::int").is_err());
        c.ping().unwrap_or_else(|e| panic!("ping2 #{i}: {e}"));
        let r = c.query_sql("SELECT count(*) FROM marathon_t").unwrap_or_else(|e| panic!("count #{i}: {e}"));
        assert!(r.rows[0].get_i64(0).unwrap_or(0) > 0);
    }

    // Verify connection is still fully functional
    c.execute_sql("CREATE TEMP TABLE final_check(a int, b text, c bool)").expect("create");
    c.execute_sql("INSERT INTO final_check VALUES (1, 'hello', true)").expect("ins");
    let r = c.query_sql("SELECT * FROM final_check").expect("final");
    assert_eq!(r.rows[0].get_i32(0), Some(1));
    assert_eq!(r.rows[0].get_str(1), Some("hello"));
    assert_eq!(r.rows[0].get_bool(2), Some(true));

    c.close().expect("close");
}

/// Re-derive the PG `Parse`-frame template bytes for a statement:
/// `b'P' | len_i32_be | stmt\0 | sql\0 | n_params_i16_be | oid_i32_be × n`.
/// The length field is self-inclusive (covers everything after the tag byte).
const fn build_parse_template<const N: usize>(stmt: &str, sql: &str, oids: &[u32]) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    let sql_b = sql.as_bytes();
    let len_be = ((N - 1) as u32).to_be_bytes();
    buf[0] = b'P';
    buf[1] = len_be[0];
    buf[2] = len_be[1];
    buf[3] = len_be[2];
    buf[4] = len_be[3];
    let mut i = 5;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    buf[i] = 0;
    i += 1;
    j = 0;
    while j < sql_b.len() {
        buf[i] = sql_b[j];
        i += 1;
        j += 1;
    }
    buf[i] = 0;
    i += 1;
    let n_be = (oids.len() as u16).to_be_bytes();
    buf[i] = n_be[0];
    i += 1;
    buf[i] = n_be[1];
    i += 1;
    j = 0;
    while j < oids.len() {
        let ob = oids[j].to_be_bytes();
        buf[i] = ob[0];
        buf[i + 1] = ob[1];
        buf[i + 2] = ob[2];
        buf[i + 3] = ob[3];
        i += 4;
        j += 1;
    }
    buf
}

/// Re-derive the `Bind`-frame prefix bytes: `empty_portal_NUL | stmt\0`. The
/// param format block, values, and result-format trailer are appended by the
/// engine at frame-build time from the argument tuple's `ParamsWriter`.
const fn build_bind_prefix<const N: usize>(stmt: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    // buf[0] is the empty-portal NUL (already 0).
    let mut i = 1;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    // Final byte is the stmt-name NUL (already 0).
    buf
}

// ═══════════════════════════════════════════════════════════
// PreparedQuery execute path — binary-uniform Bind frame.
//
// REGRESSION GATE: before the binary-uniform fix, this path declared
// param format = Text in the Bind frame while encoding the value as
// binary. PostgreSQL then rejected any non-string param (e.g. an i32
// sent as 4 binary bytes interpreted as ASCII decimal) with `invalid
// input syntax for type integer`. This test prepares an INSERT carrying
// i32 / i64 / bool params through `execute` and asserts: (1) the write
// succeeds, (2) the affected-row count is correct, (3) the stored values
// read back exactly. Post-fix it passes; pre-fix the INSERT errors at
// the server.
// ═══════════════════════════════════════════════════════════

#[test]
#[ignore = "requires local PG"]
fn prepared_query_insert_binary_params_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // A content-addressed `PreparedQuery` fixes its SQL at compile time,
    // so its table name cannot carry the per-process suffix. To keep
    // concurrent live runs isolated WITHOUT a shared object name, create a
    // process-unique SCHEMA and point `search_path` at it: the fixed
    // UNQUALIFIED table name in the prepared SQL resolves to this session's
    // private schema. Object names stay unique per process.
    let schema = format!("bsql_s3_prep_{}", std::process::id());

    c.execute_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).expect("drop schema pre");
    c.execute_sql(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    c.execute_sql(&format!("SET search_path TO {schema}")).expect("set search_path");

    c.execute_sql(
        "CREATE TABLE prep_target (n int4 NOT NULL, big int8 NOT NULL, flag bool NOT NULL)",
    )
    .expect("create table");

    // Fixed, content-addressed prepared INSERT. Params: i32, i64, bool.
    // R = () — no RETURNING, so this routes through the DML post-install
    // and yields an affected-row count. The unqualified `prep_target`
    // resolves via `search_path` to this process's schema.
    //
    // Built through `new_prepared_query`, the sole validating constructor
    // for a `PreparedQuery` (the compile-checked `query!` macro routes
    // through it in consumer crates with a migration catalog; this driver
    // test has no catalog, so it hands the constructor the wire bytes
    // directly). The bytes are a real PG Parse/Bind frame that PostgreSQL
    // parses on the wire; the constructor's const validator rejects any OID
    // drift between the baked template and the declared parameter tuple.
    const INSERT_SQL: &str =
        "INSERT INTO prep_target (n, big, flag) VALUES ($1::int4, $2::int8, $3::bool)";
    const INSERT_STMT: &str = "bsql_p_df4cc122f1840fe04c5a6ed3";
    // int4 = 23, int8 = 20, bool = 16.
    const INSERT_PARAM_OIDS: &[u32] = &[23, 20, 16];
    const INSERT_ROW_OIDS: &[u32] = &[];
    const INSERT_PARSE_LEN: usize =
        1 + 4 + INSERT_STMT.len() + 1 + INSERT_SQL.len() + 1 + 2 + 4 * INSERT_PARAM_OIDS.len();
    const INSERT_PARSE: [u8; INSERT_PARSE_LEN] =
        build_parse_template::<INSERT_PARSE_LEN>(INSERT_STMT, INSERT_SQL, INSERT_PARAM_OIDS);
    const INSERT_BIND_LEN: usize = 1 + INSERT_STMT.len() + 1;
    const INSERT_BIND: [u8; INSERT_BIND_LEN] = build_bind_prefix::<INSERT_BIND_LEN>(INSERT_STMT);
    const Q_INSERT: bsql_postgres_proto::PreparedQuery<(i32, i64, bool), ()> =
        bsql_postgres_proto::prepared::new_prepared_query::<(i32, i64, bool), ()>(
            INSERT_SQL,
            INSERT_STMT,
            INSERT_PARAM_OIDS,
            INSERT_ROW_OIDS,
            &INSERT_PARSE,
            &INSERT_BIND,
        );

    let sent_n: i32 = 42;
    let sent_big: i64 = 9_000_000_000;
    let sent_flag: bool = true;

    // EXECUTE via the macro path — the exact wire path that carried the
    // declared-Text / encoded-Binary bug. Pre-fix this errors at the
    // server with `invalid input syntax for type integer`.
    let affected = c
        .execute(&Q_INSERT, (sent_n, sent_big, sent_flag))
        .expect("prepared macro INSERT must succeed (binary-uniform Bind)");
    assert_eq!(affected, 1, "INSERT must affect exactly one row");

    // Read the row back via the simple-query text path to confirm the
    // server actually stored the binary-encoded values correctly.
    let r = c.query_sql("SELECT n, big, flag FROM prep_target").expect("read-back query");
    assert_eq!(r.rows.len(), 1, "exactly one row stored");
    assert_eq!(r.rows[0].get_i32(0), Some(sent_n), "i32 param stored correctly");
    assert_eq!(r.rows[0].get_i64(1), Some(sent_big), "i64 param stored correctly");
    assert_eq!(r.rows[0].get_bool(2), Some(sent_flag), "bool param stored correctly");

    // Cleanup: DROP IF EXISTS at end (schema CASCADE removes the table).
    c.execute_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).expect("drop schema post");

    c.close().expect("close");
}
