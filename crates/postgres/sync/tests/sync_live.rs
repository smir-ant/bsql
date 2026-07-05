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
use core::str::FromStr as _;

use bsql_postgres_proto::{DecodeError, Json, Numeric};
use bsql_postgres_sync::{ColumnError, ConnectConfig, Connection};

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
    // `server_version` is captured from the handshake `ParameterStatus` — no
    // `SHOW server_version` round-trip. Assert it is present and plausible (a PG
    // version string starts with the major-version digit), proving the captured
    // value matches what the deleted `SHOW` returned.
    let version = c.server_version().expect("server_version captured from handshake");
    assert!(
        version.as_bytes().first().is_some_and(u8::is_ascii_digit),
        "server_version should start with the major-version digit, got {version:?}"
    );
    assert!(c.backend_pid() > 0);
    c.close().expect("close");
}

/// WITNESS (steady-state timeout): `connect_timeout` gates ONLY the
/// TCP-connect + startup/auth handshake, NOT steady-state I/O. A query the
/// server delays LONGER than `connect_timeout` must SUCCEED (not trip a socket
/// read deadline and kill a healthy connection), and the connection must stay
/// usable afterwards.
///
/// RED before the steady-state-disarm fix: the connect-phase `SO_RCVTIMEO`
/// stayed armed for the connection's whole life, so a `pg_sleep` beyond the
/// deadline surfaced as a fatal `DriverError::Timeout`, dropped the linear
/// token, and bricked a healthy connection — any slow/locked/OLAP query would
/// churn a pooled connection. GREEN after: steady-state reads block
/// indefinitely, matching the async driver.
#[test]
#[ignore = "requires local PG"]
fn slow_query_beyond_connect_timeout_survives() {
    // A short 2s connect deadline; the query then sleeps 3s server-side —
    // longer than the deadline, so a still-armed steady-state timeout fires.
    let cfg = sync_config().connect_timeout(2);
    let mut c = Connection::connect(&cfg).expect("connect (localhost handshake is well within 2s)");

    // The server holds the response for 3s (> the 2s connect deadline). This
    // must complete, not time out — the load-bearing assertion.
    let slept = c
        .query_sql("SELECT pg_sleep(3)")
        .expect("a query slower than connect_timeout must succeed, not kill the connection");
    assert_eq!(slept.rows.len(), 1, "pg_sleep returns exactly one (void) row");
    assert!(c.is_healthy(), "connection stays healthy after a slow query");

    // And it stays usable: a second query round-trips on the same connection.
    let again = c
        .query_one_sql("SELECT 'still-usable'")
        .expect("second query on the same connection after the slow one");
    assert_eq!(again.get_str(0), Ok(Some("still-usable")));
    c.close().expect("close");
}

/// WITNESS: startup parameters set on the connection config take effect on the
/// server session. Proven three ways — `SHOW search_path`,
/// `current_setting('application_name')`, `SHOW statement_timeout` — plus the
/// load-bearing schema-isolation proof: a connect-time `search_path` resolves
/// an UNQUALIFIED table into the chosen schema, and SURVIVES the pool-checkout
/// `RESET ALL` (a startup-packet parameter is the session reset value). Without
/// a connect-time search_path a pooled connection would silently escape its
/// schema — the hole this closes.
#[test]
#[ignore = "requires local PG"]
fn startup_params_take_effect() {
    let schema = format!("bsql_s48_sync_{}", std::process::id());

    // A plain connection (no startup params) provisions the isolated schema.
    let mut admin = Connection::connect(&sync_config()).expect("admin connect");
    admin
        .execute_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop stale schema");
    admin
        .execute_sql(&format!("CREATE SCHEMA {schema}"))
        .expect("create schema");
    admin.close().expect("close admin");

    let cfg = sync_config()
        .with_search_path(&schema)
        .with_application_name("bsql_test")
        .with_startup_param("statement_timeout", "5000");
    let mut c = Connection::connect(&cfg).expect("connect with startup params");

    // 1) search_path took effect on the session.
    let sp = c.query_one_sql("SHOW search_path").expect("SHOW search_path");
    assert_eq!(
        sp.get_str(0),
        Ok(Some(schema.as_str())),
        "connect-time search_path must be the session search_path",
    );

    // 2) application_name took effect.
    let an = c
        .query_one_sql("SELECT current_setting('application_name')")
        .expect("current_setting(application_name)");
    assert_eq!(an.get_str(0), Ok(Some("bsql_test")));

    // 3) statement_timeout took effect (PG normalises 5000 ms to "5s").
    let st = c
        .query_one_sql("SHOW statement_timeout")
        .expect("SHOW statement_timeout");
    assert_eq!(st.get_str(0), Ok(Some("5s")));

    // 4) The isolation primitive: an UNQUALIFIED table resolves into the
    //    connect-time search_path schema, not the default.
    c.execute_sql("CREATE TABLE s48_probe (id int)")
        .expect("create unqualified table");
    c.execute_sql("INSERT INTO s48_probe VALUES (1)")
        .expect("insert into unqualified table");
    let located = c
        .query_one_sql("SELECT schemaname FROM pg_tables WHERE tablename = 's48_probe'")
        .expect("locate the probe table");
    assert_eq!(
        located.get_str(0),
        Ok(Some(schema.as_str())),
        "an unqualified table must land in the connect-time search_path schema",
    );

    // 5) The connect-time search_path is the session RESET value: it survives
    //    the pool-checkout RESET ALL, so a pooled connection cannot escape its
    //    schema.
    c.reset_session().expect("reset_session (pool checkout)");
    let sp2 = c
        .query_one_sql("SHOW search_path")
        .expect("SHOW search_path after reset");
    assert_eq!(
        sp2.get_str(0),
        Ok(Some(schema.as_str())),
        "connect-time search_path must survive RESET ALL",
    );

    c.execute_sql(&format!("DROP SCHEMA {schema} CASCADE"))
        .expect("drop schema");
    c.close().expect("close");
}

/// WITNESS: the RUNTIME-SQL escape hatch binds a NON-`Copy` owned param — a
/// `Numeric` and a `Json` — exactly as the compile-checked `query!` path does.
/// Before the runtime path was relaxed off `P: ParamsWriter + Copy`,
/// `&(numeric,)` was a hard `E0277`; now it compiles and round-trips through
/// real PG, closing the typed-vs-runtime asymmetry (both borrow the param
/// tuple to the engine).
#[test]
#[ignore = "requires local PG"]
fn runtime_path_binds_non_copy_params() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let n = Numeric::from_str("12.3400").expect("numeric parses");
    let row = c
        .query_params_one("SELECT $1::numeric AS n", &(n,))
        .expect("numeric param binds via the runtime path");
    assert_eq!(row.get_str(0), Ok(Some("12.3400")));

    let j = Json::new(String::from(r#"{"k":1}"#));
    let row = c
        .query_params_one("SELECT $1::json AS j", &(j,))
        .expect("json param binds via the runtime path");
    assert_eq!(row.get_str(0), Ok(Some(r#"{"k":1}"#)));

    let n2 = Numeric::from_str("1").expect("numeric parses");
    let affected = c
        .execute_params("SELECT $1::numeric", &(n2,))
        .expect("numeric param binds via execute_params");
    let _ = affected;

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_1k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 1000)").expect("query");
    assert_eq!(r.rows.len(), 1000);
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));
    assert_eq!(r.rows[999].get_i32(0), Ok(Some(1000)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn dynamic_getter_classifies_null_and_decode_error_over_the_wire() {
    // End-to-end proof that the dynamic getter's classification survives the real
    // PG text wire (not just the offline arena): every outcome is a distinct value.
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // (1) A real SQL NULL is `Ok(None)` FROM THE GETTER ITSELF — distinct from
    // `is_null`. This proves the typed getter classifies NULL as a present-but-
    // absent value, never conflated with a decode failure or out-of-range.
    let r = c.query_sql("SELECT NULL::int4").expect("null query");
    assert_eq!(r.rows[0].get_i32(0), Ok(None));
    assert!(r.rows[0].is_null(0));

    // (2) An `i32` read of genuinely non-numeric text ('x') is a classified `Err`
    // over the real wire — exactly the failure the retired `.parse().ok()` hid as
    // a silent `None`. Assert the EXACT classified variant, not `.is_err()`.
    let r = c.query_sql("SELECT 'x'::text").expect("text query");
    assert_eq!(
        r.rows[0].get_i32(0),
        Err(ColumnError::Decode(DecodeError::IntParse)),
    );
    // A `bool` read of the same non-bool text classifies too (`BoolParse`),
    // proving the classification holds across decoders on the real wire.
    assert_eq!(
        r.rows[0].get_bool(0),
        Err(ColumnError::Decode(DecodeError::BoolParse)),
    );
    // Read as text the same column is a legitimate value — text is text.
    assert_eq!(r.rows[0].get_str(0), Ok(Some("x")));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_10k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 10000)").expect("query");
    assert_eq!(r.rows.len(), 10000);
    assert_eq!(r.rows[9999].get_i32(0), Ok(Some(10000)));
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
    assert_eq!(c.query_sql("SELECT v FROM resilience").expect("select").rows[0].get_i32(0), Ok(Some(42)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn recv_notification_failed_read_timeout_does_not_strand_the_token() {
    // A zero Duration is rejected by the OS `set_read_timeout` syscall. That
    // fallible call runs BEFORE the linear liveness token is taken, so its
    // failure returns Err with the connection still alive — it must NOT be
    // bricked, because nothing touched the wire.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.recv_notification(std::time::Duration::ZERO);
    assert!(r.is_err(), "a zero-duration read timeout is rejected by the OS");
    assert!(
        c.is_healthy(),
        "a failed set_read_timeout must not strand the linear token / brick the connection",
    );
    // Prove it is genuinely reusable after the rejected timeout.
    c.ping().expect("connection still usable");
    assert_eq!(c.query_sql("SELECT 5::int4").expect("query").rows[0].get_i32(0), Ok(Some(5)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn client_encoding_pinned_to_utf8_and_roundtrips_non_ascii() {
    // The startup message forces client_encoding=UTF8 so the driver's UTF-8
    // TEXT decode is correct regardless of the server's default encoding.
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let enc = c.query_sql("SHOW client_encoding").expect("show").rows[0]
        .get_str(0)
        .expect("client_encoding decodes")
        .map(String::from);
    assert_eq!(enc.as_deref(), Some("UTF8"), "startup must pin client_encoding=UTF8");

    // Non-ASCII (Cyrillic + emoji) round-trips byte-exact under the pinned UTF-8.
    let text = "Привет, мир 🌍";
    let r = c.query_sql(&format!("SELECT '{text}'::text")).expect("query");
    assert_eq!(r.rows[0].get_str(0), Ok(Some(text)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn mixed_width_multi_statement_is_rejected_not_misaddressed() {
    // A single simple-query batch whose statements return DIFFERENT column
    // counts cannot be one fixed-stride result set without mis-addressing
    // cells. It must be a loud MixedResultWidth error, never silent wrong data.
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let mixed = c.query_sql("SELECT 1::int4; SELECT 'a'::text, 'b'::text");
    assert!(
        matches!(mixed, Err(bsql_postgres_sync::DriverError::MixedResultWidth)),
        "mixed-width batch must be rejected as MixedResultWidth, got {mixed:?}",
    );
    // The protocol completed cleanly (both statements ran server-side); only the
    // client-side result shape is rejected, so the connection stays reusable.
    assert!(c.is_healthy(), "connection stays healthy after a rejected result shape");
    assert_eq!(
        c.query_sql("SELECT 7::int4").expect("follow-up query works").rows[0].get_i32(0),
        Ok(Some(7)),
    );

    // A UNIFORM-width multi-statement batch is fine.
    let uniform = c.query_sql("SELECT 1::int4; SELECT 2::int4").expect("uniform batch");
    assert_eq!(uniform.rows.len(), 2);
    assert_eq!(uniform.rows[0].get_i32(0), Ok(Some(1)));
    assert_eq!(uniform.rows[1].get_i32(0), Ok(Some(2)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn listen_unlisten_reject_injection_shaped_channel() {
    // LISTEN/UNLISTEN interpolate the channel name; an injection-shaped name
    // must be a classified Config error, never spliced into SQL.
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let hostile_listen = c.listen("ch; DROP TABLE x --");
    assert!(
        matches!(hostile_listen, Err(bsql_postgres_sync::DriverError::Config(_))),
        "hostile LISTEN channel must be rejected as Config, got {hostile_listen:?}",
    );
    let hostile_unlisten = c.unlisten("ch\"; --");
    assert!(
        matches!(hostile_unlisten, Err(bsql_postgres_sync::DriverError::Config(_))),
        "hostile UNLISTEN channel must be rejected as Config, got {hostile_unlisten:?}",
    );
    assert!(c.is_healthy(), "rejection happens before the wire is touched");

    // A legitimate channel still works.
    c.listen("bsql_valid_ch").expect("legit listen");
    c.unlisten("bsql_valid_ch").expect("legit unlisten");
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in_rejects_injection_and_accepts_schema_qualified() {
    // `COPY` interpolates the table name; an injection-shaped table must be a
    // classified Config error, never spliced into SQL and executed.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_inj(id int4)").expect("create");

    let hostile = c.copy_in("cp_inj; DROP TABLE cp_inj --", vec!["1"]);
    assert!(
        matches!(hostile, Err(bsql_postgres_sync::DriverError::Config(_))),
        "an injection-shaped table must be rejected as Config, got {hostile:?}",
    );
    // Validation runs before the wire is touched, so the connection is untouched.
    assert!(c.is_healthy(), "connection stays healthy after a rejected table name");
    // The injected DROP never ran — the table still exists — and a legit copy works.
    assert_eq!(c.copy_in("cp_inj", vec!["1", "2"]).expect("legit copy"), 2);
    // A schema-qualified `schema.table` is accepted.
    assert_eq!(c.copy_in("pg_temp.cp_inj", vec!["3"]).expect("schema-qualified copy"), 1);
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_inj").expect("count").rows[0].get_i64(0),
        Ok(Some(3)),
    );
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
    assert_eq!(c.query_sql("SELECT count(*) FROM pr_err").expect("count").rows[0].get_i64(0), Ok(Some(2)));
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
fn notify_interleaved_with_a_query_is_captured_not_dropped() {
    // The no-drop witness on a real server: a concurrent session NOTIFYs while the
    // listener is between commands; the server delivers that pending notification
    // INTERLEAVED with the listener's next query response — captured, not dropped.
    let mut listener = Connection::connect(&sync_config()).expect("listener");
    let mut notifier = Connection::connect(&sync_config()).expect("notifier");
    listener.listen("bsql_sync_interleave_ch").expect("listen");
    notifier.simple_query("NOTIFY bsql_sync_interleave_ch, 'mid-query'").expect("notify");

    let r = listener.query_sql("SELECT 1::int4").expect("query");
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));

    assert!(
        listener.buffered_notifications() >= 1,
        "the interleaved NOTIFY was captured during the query, not dropped"
    );
    let n = listener.recv_notification(std::time::Duration::from_secs(5))
        .expect("recv").expect("notif");
    assert_eq!(n.payload, "mid-query");
    assert_eq!(n.channel, "bsql_sync_interleave_ch");
    listener.close().expect("close"); notifier.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn reset_session_clears_the_notification_ledger() {
    // A pooled connection that captured a notification must NOT deliver it to the
    // next user after a reset.
    let mut listener = Connection::connect(&sync_config()).expect("listener");
    let mut notifier = Connection::connect(&sync_config()).expect("notifier");
    listener.listen("bsql_sync_reset_ch").expect("listen");
    notifier.simple_query("NOTIFY bsql_sync_reset_ch, 'prior-user'").expect("notify");
    let r = listener.query_sql("SELECT 1::int4").expect("query"); // captures the notify
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));
    assert!(listener.buffered_notifications() >= 1, "captured before reset");

    listener.reset_session().expect("reset");
    assert_eq!(listener.buffered_notifications(), 0, "reset cleared the ledger");

    let none = listener
        .recv_notification(std::time::Duration::from_millis(200))
        .expect("recv");
    assert!(none.is_none(), "a reset connection must not deliver a prior notification");
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
            assert_eq!(conn.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int")).expect("q").rows[0].get_i32(0), Ok(Some(i as i32)));
        })
    }).collect();
    for h in handles { h.join().expect("thread"); }
}

// ── pool hardening (reset-on-return, acquire timeout, health eviction) ──

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
        .get_str(0).expect("search_path decodes").map(String::from);
    assert_ne!(sp.as_deref(), Some("pg_temp"), "search_path GUC bled across checkout");
    let n = conn.query_sql("SELECT count(*) FROM pg_tables WHERE tablename='bleed_probe'")
        .expect("tmp").rows[0].get_i64(0).expect("count decodes");
    assert_eq!(n, Some(0), "temp table bled across checkout");
    // LISTEN channel gone (UNLISTEN * ran in the reset).
    let listening = conn
        .query_sql("SELECT count(*)::int8 FROM pg_listening_channels() AS c(chan) WHERE chan='bleed_chan'")
        .expect("listen check").rows[0].get_i64(0).expect("listen count decodes");
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
    assert_eq!(c.query_sql("SELECT count(*) FROM tx").expect("c").rows[0].get_i64(0), Ok(Some(1)));
    let _: Result<(), _> = c.transaction(|tx| {
        tx.execute_sql("INSERT INTO tx VALUES (2)")?;
        Err(bsql_postgres_sync::DriverError::NoRows)
    });
    assert_eq!(c.query_sql("SELECT count(*) FROM tx").expect("c").rows[0].get_i64(0), Ok(Some(1)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn row_clone_across_threads() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let row = c.query_sql("SELECT 42::int, 'hello'::text").expect("q").rows[0].clone();
    let handle = std::thread::spawn(move || row.get_i32(0).expect("i32 decodes"));
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
    assert_eq!(c.query_params_one("SELECT name FROM lc WHERE val > $1", &(90i32,)).expect("p").get_str(0), Ok(Some("alice")));
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
            assert_eq!(r.rows[0].get_i32(0), Ok(Some(i as i32)));
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
    assert_eq!(r.rows[0].get_i64(0), Ok(Some(4)));

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
    assert_eq!(r.rows[0].get_i64(0), Ok(Some(180)));

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
    let v = std::thread::spawn(move || row.get_str(0).expect("final decodes").map(String::from)).join().expect("thread");
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
        assert_eq!(r.rows[0].get_i32(0), Ok(Some(0)), "first col at {n}");
        let last = usize::try_from(n.saturating_sub(1)).unwrap();
        assert_eq!(r.rows[0].get_i32(last), Ok(Some(n.saturating_sub(1) as i32)), "last col at {n}");
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
    assert_eq!(c.query_sql("SELECT count(*) FROM ps_edge").expect("c").rows[0].get_i64(0), Ok(Some(50)));
    c.close_statement(stmt).expect("close");

    // Prepare SELECT, query many times
    let stmt = c.prepare("SELECT v FROM ps_edge WHERE id = $1").expect("prep select");
    for i in 0..50i32 {
        let r = c.query_prepared(&stmt, &(i,)).expect("qp");
        assert_eq!(r.rows[0].get_str(0), Ok(Some(format!("v{i}").as_str())));
    }
    c.close_statement(stmt).expect("close");

    // Multiple prepared statements open at once
    let s1 = c.prepare("SELECT id FROM ps_edge WHERE id < $1").expect("s1");
    let s2 = c.prepare("SELECT v FROM ps_edge WHERE id = $1").expect("s2");
    let s3 = c.prepare("UPDATE ps_edge SET v = $1 WHERE id = $2").expect("s3");
    let r1 = c.query_prepared(&s1, &(5i32,)).expect("q1");
    assert_eq!(r1.rows.len(), 5);
    let r2 = c.query_prepared(&s2, &(0i32,)).expect("q2");
    assert_eq!(r2.rows[0].get_str(0), Ok(Some("v0")));
    c.execute_prepared(&s3, &("updated", 0i32)).expect("exec3");
    let r2b = c.query_prepared(&s2, &(0i32,)).expect("q2b");
    assert_eq!(r2b.rows[0].get_str(0), Ok(Some("updated")));
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
    assert_eq!(c.query_sql("SELECT count(*) FROM cp_edge").expect("c").rows[0].get_i64(0), Ok(Some(0)));

    // COPY 1 row
    assert_eq!(c.copy_in("cp_edge", vec!["1\tone"]).expect("one"), 1);

    // COPY with NULLs (PG COPY \N = NULL)
    assert_eq!(c.copy_in("cp_edge", vec!["2\t\\N"]).expect("null"), 1);

    // COPY many rows
    let big: Vec<String> = (0..5000).map(|i| format!("{i}\tname_{i}")).collect();
    assert_eq!(c.copy_in("cp_edge", &big).expect("5k"), 5000);
    assert_eq!(c.query_sql("SELECT count(*) FROM cp_edge").expect("c").rows[0].get_i64(0), Ok(Some(5002)));

    // COPY into a non-existent table on the SAME connection that just ran three
    // successful COPYs: the server never enters copy mode (it sends ErrorResponse
    // instead of CopyInResponse), so `copy_in_finish` drives to a recoverable
    // `ServerErrored` and `settle` restores the token — a failed COPY leaves the
    // connection at a clean idle, deterministically, exactly like a failed query.
    // (An earlier workaround used a fresh connection here on the belief that COPY
    // error recovery could strand a reused connection; the streaming COPY path
    // recovers through the standard drain/settle, proven deterministic by 200
    // reuse iterations serially AND under the parallel test suite.)
    let failed = c.copy_in("table_that_does_not_exist", vec!["1\ta"]);
    assert!(
        matches!(failed, Err(bsql_postgres_sync::DriverError::Db(_))),
        "a COPY into a missing table is a classified Db error, got {failed:?}",
    );
    assert!(c.is_healthy(), "the connection recovers to a clean idle after a failed COPY");
    // The SAME connection is reusable: a follow-up query works and still sees the
    // rows the successful COPYs committed (the failed COPY committed nothing).
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_edge").expect("count").rows[0].get_i64(0),
        Ok(Some(5002)),
    );
    c.ping().expect("ping recovers on the same connection");

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
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(42)));

    // Large value via SQL literal (params limited to 1024 bytes)
    let big_val = "X".repeat(50_000);
    c.execute_sql("CREATE TEMP TABLE big_val(v text)").expect("create");
    c.execute_sql(&format!("INSERT INTO big_val VALUES ('{big_val}')")).expect("ins");
    let r = c.query_sql("SELECT v FROM big_val").expect("q");
    assert_eq!(r.rows[0].get_str(0).expect("big_val decodes").map(|s| s.len()), Some(50_000));

    // Many columns with NULLs
    let r = c.query_sql("SELECT NULL::int, 1::int, NULL::text, 'a'::text, NULL::bool, true").expect("mixed nulls");
    assert!(r.rows[0].is_null(0));
    assert_eq!(r.rows[0].get_i32(1), Ok(Some(1)));
    assert!(r.rows[0].is_null(2));
    assert_eq!(r.rows[0].get_str(3), Ok(Some("a")));
    assert!(r.rows[0].is_null(4));
    assert_eq!(r.rows[0].get_bool(5), Ok(Some(true)));

    // Query after error mid-stream should recover
    assert!(c.query_sql("SELECT 1/0 FROM generate_series(1,10)").is_err());
    c.ping().expect("recover after mid-stream error");
    let r = c.query_sql("SELECT 1::int").expect("after recover");
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));

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
            assert_eq!(c.query_sql(&format!("SELECT {i}::int")).expect("q").rows[0].get_i32(0), Ok(Some(i as i32)));
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
        assert!(r.rows[0].get_i64(0).expect("count decodes").unwrap_or(0) > 0);
    }

    // Verify connection is still fully functional
    c.execute_sql("CREATE TEMP TABLE final_check(a int, b text, c bool)").expect("create");
    c.execute_sql("INSERT INTO final_check VALUES (1, 'hello', true)").expect("ins");
    let r = c.query_sql("SELECT * FROM final_check").expect("final");
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));
    assert_eq!(r.rows[0].get_str(1), Ok(Some("hello")));
    assert_eq!(r.rows[0].get_bool(2), Ok(Some(true)));

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
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(sent_n)), "i32 param stored correctly");
    assert_eq!(r.rows[0].get_i64(1), Ok(Some(sent_big)), "i64 param stored correctly");
    assert_eq!(r.rows[0].get_bool(2), Ok(Some(sent_flag)), "bool param stored correctly");

    // Cleanup: DROP IF EXISTS at end (schema CASCADE removes the table).
    c.execute_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).expect("drop schema post");

    c.close().expect("close");
}

// ─────────────────────────── COPY (streaming) ───────────────────────────

#[test]
#[ignore = "requires local PG"]
fn copy_round_trip_in_then_out() {
    // Stream rows IN via the scoped writer, then stream them back OUT.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_rt(id int4, name text)").expect("create");
    let n = c
        .copy_in_with("cp_rt", |w| {
            for i in 1..=3i32 {
                w.write_row(format!("{i}\tname{i}").as_bytes())?;
            }
            Ok(())
        })
        .expect("copy_in_with");
    assert_eq!(n, 3);

    let mut out: Vec<String> = Vec::new();
    let broke: Option<core::convert::Infallible> = c
        .copy_out("cp_rt", |chunk| {
            out.push(String::from_utf8(chunk.to_vec()).expect("utf8"));
            core::ops::ControlFlow::Continue(())
        })
        .expect("copy_out");
    assert!(broke.is_none());
    let mut sorted = out.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["1\tname1\n".to_string(), "2\tname2\n".to_string(), "3\tname3\n".to_string()],
    );
    assert!(c.is_healthy());
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_rt").expect("count").rows[0].get_i64(0),
        Ok(Some(3)),
    );
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in_abort_mid_stream_recovers() {
    // A copy_in_with whose closure ERRORS mid-stream sends CopyFail; the
    // connection recovers and commits none of the aborted rows.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_ab(id int4)").expect("create");
    let aborted = c.copy_in_with("cp_ab", |w| {
        w.write_row(b"1")?;
        w.write_row(b"2")?;
        Err(bsql_postgres_sync::DriverError::Config("caller aborted"))
    });
    assert!(
        matches!(aborted, Err(bsql_postgres_sync::DriverError::Config("caller aborted"))),
        "the caller's error dominates, got {aborted:?}",
    );
    assert!(c.is_healthy(), "connection recovered after the mid-stream abort");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_ab").expect("count").rows[0].get_i64(0),
        Ok(Some(0)),
        "an aborted COPY commits no rows",
    );
    assert_eq!(c.copy_in("cp_ab", vec!["7", "8"]).expect("recopy"), 2);
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_out_early_break_recovers() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_brk(id int4)").expect("create");
    let rows: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    assert_eq!(c.copy_in("cp_brk", &rows).expect("seed"), 1000);
    let mut seen = 0u32;
    let broke: Option<u32> = c
        .copy_out("cp_brk", |_chunk| {
            seen += 1;
            core::ops::ControlFlow::Break(seen)
        })
        .expect("copy_out");
    assert_eq!(broke, Some(1));
    assert!(c.is_healthy());
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_brk").expect("count").rows[0].get_i64(0),
        Ok(Some(1000)),
    );
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in_streaming_bulk_constant_memory() {
    // Constant-memory witness: stream 100k rows through the writer one at a time
    // (a lazy generator, never a Vec of them all).
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_bulk(id int8, payload text)").expect("create");
    const N: i64 = 100_000;
    let n = c
        .copy_in_with("cp_bulk", |w| {
            for i in 0..N {
                w.write_row(format!("{i}\tpayload-row-{i}").as_bytes())?;
            }
            Ok(())
        })
        .expect("bulk copy_in");
    assert_eq!(n, u64::try_from(N).expect("N fits u64"));
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_bulk").expect("count").rows[0].get_i64(0),
        Ok(Some(N)),
    );
    c.close().expect("close");
}
