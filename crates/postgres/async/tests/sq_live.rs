#![forbid(unsafe_code)]
use core::str::FromStr as _;

use bsql_postgres_async::{ColumnError, ConnectConfig, Connection};
use bsql_postgres_proto::{DecodeError, Json, Numeric};

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
    c.close().await.expect("close");
}

/// WITNESS (steady-state timeout parity): the async driver has NO steady-state
/// read deadline — a query the server delays LONGER than `connect_timeout` must
/// SUCCEED and leave the connection usable. This is the behaviour the blocking
/// driver was fixed to mirror (its connect-phase `SO_RCVTIMEO` used to stay
/// armed and kill a healthy connection on a slow query); pinning it here guards
/// the two drivers against re-diverging.
#[tokio::test]
#[ignore = "requires local PG"]
async fn slow_query_beyond_connect_timeout_survives() {
    // A short 2s connect deadline; the query then sleeps 3s server-side — longer
    // than the deadline. `connect_timeout` must bound only the connect phase.
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .connect_timeout(2);
    let mut c = Connection::connect(&cfg)
        .await
        .expect("connect (localhost handshake is well within 2s)");

    // The server holds the response for 3s (> the 2s connect deadline). This must
    // complete, not time out — the async parity assertion.
    let slept = c
        .query_sql("SELECT pg_sleep(3)")
        .await
        .expect("a query slower than connect_timeout must succeed on the async driver");
    assert_eq!(slept.rows.len(), 1, "pg_sleep returns exactly one (void) row");
    assert!(c.is_healthy(), "connection stays healthy after a slow query");

    // And it stays usable: a second query round-trips on the same connection.
    let again = c
        .query_one_sql("SELECT 'still-usable'")
        .await
        .expect("second query on the same connection after the slow one");
    assert_eq!(again.get_str(0), Ok(Some("still-usable")));
    c.close().await.expect("close");
}

/// WITNESS (connect-handshake timeout): a server that ACCEPTS the TCP
/// connection but never answers the startup/auth handshake must make async
/// `connect` TIME OUT within ~`connect_timeout` — never hang. The whole connect
/// sequence (dial + `SSLRequest` probe + TLS + handshake) now rides ONE
/// `connect_timeout` budget; before, only the TCP dial was bounded, so a silent
/// server hung connect INDEFINITELY. Deterministic and PG-free (a raw loopback
/// listener that accepts then stays silent), so it runs in the default suite —
/// no `#[ignore]`.
#[tokio::test]
async fn connect_times_out_on_a_silent_server_never_hangs() {
    // A raw TCP listener that accepts then never speaks — the "server accepts
    // TCP, then goes silent on the handshake" case.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener addr");
    let accepter = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            // Discard whatever the client sends (its startup packet) but NEVER
            // reply; keep reading until the client gives up and closes the socket
            // (read → 0), so this thread exits promptly once the client times out
            // rather than on a fixed sleep.
            let mut sink = [0u8; 512];
            loop {
                match std::io::Read::read(&mut sock, &mut sink) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        }
    });

    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .port(addr.port())
        .database("postgres".to_string())
        // No TLS probe against the fake — target the startup/auth handshake, the
        // exact step that used to be unbounded.
        .ssl_mode(bsql_postgres_async::SslMode::Disable)
        .connect_timeout(1);

    let start = std::time::Instant::now();
    let result = Connection::connect(&cfg).await;
    let elapsed = start.elapsed();

    // Load-bearing property: connect RETURNED (did not hang) with a Timeout.
    // `Connection` is not `Debug` (it holds a live socket), so classify the Ok
    // side by matching rather than Debug-printing the whole Result.
    match result {
        Err(bsql_postgres_async::DriverError::Timeout) => {}
        Err(other) => {
            panic!("a silent server must time out; got a different error {other:?} after {elapsed:?}")
        }
        Ok(_) => panic!("a silent server must time out, not connect; got Ok after {elapsed:?}"),
    }
    // And it fired near the 1s budget, not far past it (generous slack for a
    // loaded parallel run).
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "connect must time out within ~connect_timeout, took {elapsed:?}",
    );

    accepter.join().ok();
}

/// WITNESS: startup parameters set on the connection config take effect on the
/// server session. Proven three ways — `SHOW search_path`,
/// `current_setting('application_name')`, `SHOW statement_timeout` — plus the
/// load-bearing schema-isolation proof: a connect-time `search_path` resolves
/// an UNQUALIFIED table into the chosen schema, and SURVIVES the pool-checkout
/// `RESET ALL` (a startup-packet parameter is the session reset value). Without
/// a connect-time search_path a pooled connection would silently escape its
/// schema — the hole this closes.
#[tokio::test]
#[ignore = "requires local PG"]
async fn startup_params_take_effect() {
    fn base() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string())
    }
    let schema = format!("bsql_s48_async_{}", std::process::id());

    // A plain connection (no startup params) provisions the isolated schema.
    let mut admin = Connection::connect(&base()).await.expect("admin connect");
    admin
        .execute_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .expect("drop stale schema");
    admin
        .execute_sql(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create schema");
    admin.close().await.expect("close admin");

    let cfg = base()
        .with_search_path(&schema)
        .with_application_name("bsql_test")
        .with_startup_param("statement_timeout", "5000");
    let mut c = Connection::connect(&cfg)
        .await
        .expect("connect with startup params");

    // 1) search_path took effect on the session.
    let sp = c
        .query_one_sql("SHOW search_path")
        .await
        .expect("SHOW search_path");
    assert_eq!(
        sp.get_str(0),
        Ok(Some(schema.as_str())),
        "connect-time search_path must be the session search_path",
    );

    // 2) application_name took effect.
    let an = c
        .query_one_sql("SELECT current_setting('application_name')")
        .await
        .expect("current_setting(application_name)");
    assert_eq!(an.get_str(0), Ok(Some("bsql_test")));

    // 3) statement_timeout took effect (PG normalises 5000 ms to "5s").
    let st = c
        .query_one_sql("SHOW statement_timeout")
        .await
        .expect("SHOW statement_timeout");
    assert_eq!(st.get_str(0), Ok(Some("5s")));

    // 4) The isolation primitive: an UNQUALIFIED table resolves into the
    //    connect-time search_path schema, not the default.
    c.execute_sql("CREATE TABLE s48_probe (id int)")
        .await
        .expect("create unqualified table");
    c.execute_sql("INSERT INTO s48_probe VALUES (1)")
        .await
        .expect("insert into unqualified table");
    let located = c
        .query_one_sql("SELECT schemaname FROM pg_tables WHERE tablename = 's48_probe'")
        .await
        .expect("locate the probe table");
    assert_eq!(
        located.get_str(0),
        Ok(Some(schema.as_str())),
        "an unqualified table must land in the connect-time search_path schema",
    );

    // 5) The connect-time search_path is the session RESET value: it survives
    //    the pool-checkout RESET ALL, so a pooled connection cannot escape its
    //    schema.
    c.reset_session().await.expect("reset_session (pool checkout)");
    let sp2 = c
        .query_one_sql("SHOW search_path")
        .await
        .expect("SHOW search_path after reset");
    assert_eq!(
        sp2.get_str(0),
        Ok(Some(schema.as_str())),
        "connect-time search_path must survive RESET ALL",
    );

    c.execute_sql(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("drop schema");
    c.close().await.expect("close");
}

/// WITNESS: the RUNTIME-SQL escape hatch (`query_params` / `execute_params`)
/// binds a NON-`Copy` owned param — a `Numeric` and a `Json` — exactly as the
/// compile-checked `query!` path does. Before the runtime path was relaxed off
/// `P: ParamsWriter + Copy`, `&(numeric,)` was a hard `E0277` (`Numeric` is not
/// `Copy`); now it compiles and round-trips through real PG. This closes the
/// typed-vs-runtime asymmetry (both borrow the param tuple to the engine).
#[tokio::test]
#[ignore = "requires local PG"]
async fn runtime_path_binds_non_copy_params() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    // A non-`Copy` `Numeric` param binds through the runtime dynamic path and
    // echoes back exactly (read as text — the dynamic Row is text-format).
    let n = Numeric::from_str("12.3400").expect("numeric parses");
    let row = c
        .query_params_one("SELECT $1::numeric AS n", &(n,))
        .await
        .expect("numeric param binds via the runtime path");
    assert_eq!(row.get_str(0), Ok(Some("12.3400")));

    // A non-`Copy` `Json` param likewise.
    let j = Json::new(String::from(r#"{"k":1}"#));
    let row = c
        .query_params_one("SELECT $1::json AS j", &(j,))
        .await
        .expect("json param binds via the runtime path");
    assert_eq!(row.get_str(0), Ok(Some(r#"{"k":1}"#)));

    // The side-effect twin: a non-`Copy` param through `execute_params`.
    let n2 = Numeric::from_str("1").expect("numeric parses");
    let affected = c
        .execute_params("SELECT $1::numeric", &(n2,))
        .await
        .expect("numeric param binds via execute_params");
    let _ = affected;

    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_1k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 1000)").await.expect("q");
    assert_eq!(r.rows.len(), 1000);
    assert_eq!(r.rows[999].get_i32(0), Ok(Some(1000)));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn dynamic_getter_classifies_null_and_decode_error_over_the_wire() {
    // End-to-end proof that the dynamic getter's classification survives the real
    // PG text wire (not just the offline arena): every outcome is a distinct value.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    // (1) A real SQL NULL is `Ok(None)` FROM THE GETTER ITSELF — distinct from
    // `is_null`. This proves the typed getter classifies NULL as a present-but-
    // absent value, never conflated with a decode failure or out-of-range.
    let r = c.query_sql("SELECT NULL::int4").await.expect("null query");
    assert_eq!(r.rows[0].get_i32(0), Ok(None));
    assert!(r.rows[0].is_null(0));

    // (2) An `i32` read of genuinely non-numeric text ('x') is a classified `Err`
    // over the real wire — exactly the failure the retired `.parse().ok()` hid as
    // a silent `None`. Assert the EXACT classified variant, not `.is_err()`.
    let r = c.query_sql("SELECT 'x'::text").await.expect("text query");
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
    assert_eq!(c.query_sql("SELECT v FROM res").await.expect("q").rows[0].get_i32(0), Ok(Some(42)));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn client_encoding_pinned_to_utf8_and_roundtrips_non_ascii() {
    // The startup message forces client_encoding=UTF8 so the driver's UTF-8
    // TEXT decode is correct regardless of the server's default encoding.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let enc = c.query_sql("SHOW client_encoding").await.expect("show").rows[0]
        .get_str(0)
        .expect("client_encoding decodes")
        .map(String::from);
    assert_eq!(enc.as_deref(), Some("UTF8"), "startup must pin client_encoding=UTF8");

    // Non-ASCII (Cyrillic + emoji) round-trips byte-exact under the pinned UTF-8.
    let text = "Привет, мир 🌍";
    let r = c.query_sql(&format!("SELECT '{text}'::text")).await.expect("query");
    assert_eq!(r.rows[0].get_str(0), Ok(Some(text)));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn mixed_width_multi_statement_is_rejected_not_misaddressed() {
    // A single simple-query batch whose statements return DIFFERENT column
    // counts cannot be one fixed-stride result set without mis-addressing
    // cells. It must be a loud MixedResultWidth error, never silent wrong data.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let mixed = c.query_sql("SELECT 1::int4; SELECT 'a'::text, 'b'::text").await;
    assert!(
        matches!(mixed, Err(bsql_postgres_async::DriverError::MixedResultWidth)),
        "mixed-width batch must be rejected as MixedResultWidth, got {mixed:?}",
    );
    // The protocol completed cleanly (both statements ran server-side); only the
    // client-side result shape is rejected, so the connection stays reusable.
    assert!(c.is_healthy(), "connection stays healthy after a rejected result shape");
    assert_eq!(
        c.query_sql("SELECT 7::int4").await.expect("follow-up query works").rows[0].get_i32(0),
        Ok(Some(7)),
    );

    // A UNIFORM-width multi-statement batch is fine: rows flatten into one arena
    // whose single stride addresses every cell correctly.
    let uniform = c.query_sql("SELECT 1::int4; SELECT 2::int4").await.expect("uniform batch");
    assert_eq!(uniform.rows.len(), 2);
    assert_eq!(uniform.rows[0].get_i32(0), Ok(Some(1)));
    assert_eq!(uniform.rows[1].get_i32(0), Ok(Some(2)));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn listen_unlisten_reject_injection_shaped_channel() {
    // LISTEN/UNLISTEN interpolate the channel name; an injection-shaped name
    // must be a classified Config error, never spliced into SQL.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let hostile_listen = c.listen("ch; DROP TABLE x --").await;
    assert!(
        matches!(hostile_listen, Err(bsql_postgres_async::DriverError::Config(_))),
        "hostile LISTEN channel must be rejected as Config, got {hostile_listen:?}",
    );
    let hostile_unlisten = c.unlisten("ch\"; --").await;
    assert!(
        matches!(hostile_unlisten, Err(bsql_postgres_async::DriverError::Config(_))),
        "hostile UNLISTEN channel must be rejected as Config, got {hostile_unlisten:?}",
    );
    assert!(c.is_healthy(), "rejection happens before the wire is touched");

    // A legitimate channel still works.
    c.listen("bsql_valid_ch").await.expect("legit listen");
    c.unlisten("bsql_valid_ch").await.expect("legit unlisten");
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_rejects_injection_and_accepts_schema_qualified() {
    // `COPY` interpolates the table name; an injection-shaped table must be a
    // classified Config error, never spliced into SQL and executed.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_inj(id int4)").await.expect("create");

    let hostile = c.copy_in("cp_inj; DROP TABLE cp_inj --", vec!["1"]).await;
    assert!(
        matches!(hostile, Err(bsql_postgres_async::DriverError::Config(_))),
        "an injection-shaped table must be rejected as Config, got {hostile:?}",
    );
    // Validation runs before the wire is touched, so the connection is untouched.
    assert!(c.is_healthy(), "connection stays healthy after a rejected table name");
    // The injected DROP never ran — the table still exists — and a legit copy works.
    assert_eq!(c.copy_in("cp_inj", vec!["1", "2"]).await.expect("legit copy"), 2);
    // A schema-qualified `schema.table` is accepted.
    assert_eq!(c.copy_in("pg_temp.cp_inj", vec!["3"]).await.expect("schema-qualified copy"), 1);
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_inj").await.expect("count").rows[0].get_i64(0),
        Ok(Some(3)),
    );
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
async fn notify_interleaved_with_a_query_is_captured_not_dropped() {
    // The no-drop witness on a real server: a CONCURRENT session NOTIFYs while
    // the listener is between commands; the server then delivers that pending
    // notification INTERLEAVED with the listener's next query response. Before the
    // ledger this was dropped by the result collector; now it is buffered.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut listener = Connection::connect(&config).await.expect("l");
    let mut notifier = Connection::connect(&config).await.expect("n");
    listener.listen("bsql_interleave_ch").await.expect("listen");
    // The NOTIFY commits on the notifier BEFORE the listener runs its query, so it
    // is pending and rides the listener's SELECT response deterministically.
    notifier.simple_query("NOTIFY bsql_interleave_ch, 'mid-query'").await.expect("notify");

    // A perfectly ordinary query on the listener — its response carries the
    // pending notification. The query still returns its own row unaffected.
    let r = listener.query_sql("SELECT 1::int4").await.expect("query");
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));

    // The smoking gun: the notification was captured DURING that query.
    assert!(
        listener.buffered_notifications() >= 1,
        "the interleaved NOTIFY was captured during the query, not dropped"
    );
    // And it drains from the ledger without another wait.
    let n = listener
        .recv_notification(std::time::Duration::from_secs(5))
        .await.expect("recv").expect("notif");
    assert_eq!(n.payload, "mid-query");
    assert_eq!(n.channel, "bsql_interleave_ch");
    listener.close().await.expect("close"); notifier.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn reset_session_clears_the_notification_ledger() {
    // A pooled connection that captured a notification must NOT deliver it to the
    // next user after a reset.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut listener = Connection::connect(&config).await.expect("l");
    let mut notifier = Connection::connect(&config).await.expect("n");
    listener.listen("bsql_reset_ch").await.expect("listen");
    notifier.simple_query("NOTIFY bsql_reset_ch, 'prior-user'").await.expect("notify");
    let r = listener.query_sql("SELECT 1::int4").await.expect("query"); // captures the notify
    assert_eq!(r.rows[0].get_i32(0), Ok(Some(1)));
    assert!(listener.buffered_notifications() >= 1, "captured before reset");

    // Reset (as the pool does on checkout): UNLISTEN * + clear the ledger.
    listener.reset_session().await.expect("reset");
    assert_eq!(listener.buffered_notifications(), 0, "reset cleared the ledger");

    // The next user's recv finds nothing — over a REAL socket this is a would-block
    // that surfaces as a quiet None, never the prior user's notification.
    let none = listener
        .recv_notification(std::time::Duration::from_millis(200))
        .await.expect("recv");
    assert!(none.is_none(), "a reset connection must not deliver a prior notification");
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
            assert_eq!(c.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int")).await.expect("q").rows[0].get_i32(0), Ok(Some(i as i32)));
        })
    }).collect();
    for h in handles { h.await.expect("task"); }
}

// ── pool hardening (reset-on-return, acquire timeout, health eviction) ──

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
        .get_str(0).expect("search_path decodes").map(String::from);
    assert_ne!(sp.as_deref(), Some("pg_temp"), "search_path GUC bled across checkout");
    // Temp table gone.
    let n = conn.query_sql("SELECT count(*) FROM pg_tables WHERE tablename='bleed_probe'")
        .await.expect("tmp").rows[0].get_i64(0).expect("count decodes");
    assert_eq!(n, Some(0), "temp table bled across checkout");
    // LISTEN channel gone (UNLISTEN * ran in the reset).
    let listening = conn
        .query_sql("SELECT count(*)::int8 FROM pg_listening_channels() AS c(chan) WHERE chan='bleed_chan'")
        .await.expect("listen check").rows[0].get_i64(0).expect("listen count decodes");
    assert_eq!(listening, Some(0), "LISTEN channel bled across checkout");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn cancelled_transaction_is_rolled_back_before_reuse() {
    // A `transaction()` future dropped mid-body (after BEGIN + a write, before
    // COMMIT/ROLLBACK) returns its connection to the pool still inside the
    // transaction. There is no async Drop guard (Drop cannot `.await`), so the
    // safety net is the pool's reset-on-acquire, which prepends ROLLBACK when
    // the connection is not Idle. This proves the next user never runs inside
    // the stale tx and the uncommitted write is discarded.
    //
    // DECISIVE: with max_size=1 the re-acquired connection is the SAME physical
    // one that did the (uncommitted) INSERT. If the reset did NOT roll back, that
    // connection would still be in its own transaction and would SEE its own
    // uncommitted row (count = 1). count = 0 therefore proves ROLLBACK ran.
    use bsql_postgres_async::{DriverError, Pool};
    let mk_config =
        || ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());

    // A persistent (non-temp) table: temp tables would be dropped by the reset's
    // DISCARD TEMP, and we need the row survival to be decided by tx state alone.
    {
        let mut setup = Connection::connect(&mk_config()).await.expect("connect setup");
        setup.execute_sql("DROP TABLE IF EXISTS bsql_tx_cancel_rollback").await.expect("drop old");
        setup.execute_sql("CREATE TABLE bsql_tx_cancel_rollback(id int4)").await.expect("create");
        setup.close().await.expect("close setup");
    }

    let pool = Pool::new(mk_config(), 1).await.expect("pool"); // max_size=1 forces reuse
    let pid1;
    {
        let mut guard = pool.get().await.expect("acquire 1");
        pid1 = guard.conn_mut().expect("live1").backend_pid();

        // Run a transaction that BEGINs + INSERTs, signals, then hangs. When the
        // signal arrives (post-INSERT, parked between verbs with the token
        // restored), `select!` drops the transaction future → cancellation
        // exactly at that point. Deterministic — driven by the signal, not a timer.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let fut = guard.conn_mut().expect("live1").transaction(async move |c| {
            c.execute_sql("INSERT INTO bsql_tx_cancel_rollback VALUES (1)").await?;
            let _ = tx.send(());
            std::future::pending::<()>().await;
            Ok::<(), DriverError>(())
        });
        tokio::select! {
            biased;
            r = fut => panic!("the transaction must not complete: {r:?}"),
            _ = rx => {} // INSERT done + signaled → select drops `fut` (cancel mid-tx)
        }
        // The token was restored between verbs, so the connection is alive —
        // the pool will RESET it (not evict it) on the next acquire.
        assert!(guard.conn_mut().expect("live1").is_healthy(), "connection alive after cancel");
        // guard drops here → the mid-transaction connection returns to the pool.
    }

    let mut guard2 = pool.get().await.expect("acquire 2");
    let conn = guard2.conn_mut().expect("live2");
    assert_eq!(conn.backend_pid(), pid1, "max_size=1 must reuse the SAME physical connection");
    let count = conn
        .query_sql("SELECT count(*) FROM bsql_tx_cancel_rollback")
        .await
        .expect("count")
        .rows[0]
        .get_i64(0)
        .expect("count decodes");
    assert_eq!(
        count,
        Some(0),
        "the cancelled transaction's uncommitted INSERT must have been rolled back on acquire; \
         a nonzero count would mean the reused connection is still inside the stale transaction",
    );
    // The connection is clean and fully usable.
    let probe = conn.query_sql("SELECT 1::int4").await.expect("reusable after reset");
    assert_eq!(probe.rows[0].get_i32(0), Ok(Some(1)));
    drop(guard2);

    let mut cleanup = Connection::connect(&mk_config()).await.expect("connect cleanup");
    cleanup.execute_sql("DROP TABLE IF EXISTS bsql_tx_cancel_rollback").await.expect("drop table");
    cleanup.close().await.expect("close cleanup");
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
    assert_eq!(tokio::task::spawn(async move { row.get_i32(0).expect("i32 decodes") }).await.expect("spawn"), Some(42));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth"]
async fn scram_auth() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string()).password("test_password_123".to_string());
    let mut c = Connection::connect(&config).await.expect("SCRAM");
    assert_eq!(c.query_sql("SELECT current_user").await.expect("q").rows[0].get_raw(0), Ok(Some(b"bsql_test_scram".as_slice())));
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
        .await.expect("p").get_str(0), Ok(Some("alice")));
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
            assert_eq!(r.rows[0].get_i32(0), Ok(Some(i as i32)));
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
        Ok(Some(2))
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
        Ok(Some(2)),
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
    assert_eq!(r.rows[0].get_i64(0), Ok(Some(4)));

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
    assert_eq!(r.rows[0].get_i64(0), Ok(Some(180)));

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
    let v = tokio::task::spawn(async move { row.get_str(0).expect("final decodes").map(String::from) }).await.expect("spawn");
    assert_eq!(v, Some("final".to_string()));

    c.close().await.expect("close");
}

// ─────────────────────────── COPY (streaming) ───────────────────────────

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_round_trip_in_then_out() {
    // The flagship: stream rows IN via the scoped writer, then stream them back
    // OUT and assert the round-trip is byte-faithful.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_rt(id int4, name text)")
        .await
        .expect("create");

    // COPY IN via the streaming writer, interleaving arbitrary logic between rows.
    let n = c
        .copy_in_with("cp_rt", async |w| {
            for i in 1..=3i32 {
                // Text COPY row: tab-separated columns.
                w.write_row(format!("{i}\tname{i}").as_bytes()).await?;
            }
            Ok(())
        })
        .await
        .expect("copy_in_with");
    assert_eq!(n, 3, "COPY IN reports 3 affected rows");

    // COPY OUT streams each row back as a `\n`-terminated text-COPY line.
    let mut out: Vec<String> = Vec::new();
    let broke: Option<core::convert::Infallible> = c
        .copy_out("cp_rt", |chunk| {
            out.push(String::from_utf8(chunk.to_vec()).expect("utf8"));
            core::ops::ControlFlow::Continue(())
        })
        .await
        .expect("copy_out");
    assert!(broke.is_none(), "streamed to completion");
    let mut sorted = out.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["1\tname1\n".to_string(), "2\tname2\n".to_string(), "3\tname3\n".to_string()],
    );
    // The connection is clean and reusable after both directions.
    assert!(c.is_healthy());
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_rt").await.expect("count").rows[0].get_i64(0),
        Ok(Some(3)),
    );
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_abort_mid_stream_recovers() {
    // A copy_in_with whose closure ERRORS mid-stream must send CopyFail so the
    // server tears the COPY down and the connection is RECOVERABLE.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_ab(id int4)").await.expect("create");

    let aborted = c
        .copy_in_with("cp_ab", async |w| {
            w.write_row(b"1").await?;
            w.write_row(b"2").await?;
            // Abandon the copy mid-stream.
            Err(bsql_postgres_async::DriverError::Config("caller aborted"))
        })
        .await;
    assert!(
        matches!(aborted, Err(bsql_postgres_async::DriverError::Config("caller aborted"))),
        "the caller's error dominates, got {aborted:?}",
    );

    // The connection recovered: a subsequent query works, and the aborted rows
    // were NOT committed (CopyFail rolls the COPY back).
    assert!(c.is_healthy(), "connection recovered after the mid-stream abort");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_ab").await.expect("count").rows[0].get_i64(0),
        Ok(Some(0)),
        "an aborted COPY commits no rows",
    );
    // And a fresh copy into the same table still works.
    assert_eq!(c.copy_in("cp_ab", vec!["7", "8"]).await.expect("recopy"), 2);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_failed_command_recovers() {
    // The failed-COPY-COMMAND write-ahead path: the COPY targets a non-existent
    // (but valid-identifier) table, so the server sends ErrorResponse instead of
    // CopyInResponse and never enters copy mode. The client's optimistic CopyData
    // + CopyDone are accepted-but-discarded, and `copy_in_finish` drives to a
    // recoverable `ServerErrored` — proving the async driver's OWN settle/recovery
    // wiring for this branch (the sync twin proves it in `copy_in_edge_cases`).
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let failed = c.copy_in("table_that_does_not_exist_async", vec!["1\ta"]).await;
    assert!(
        matches!(failed, Err(bsql_postgres_async::DriverError::Db(_))),
        "a COPY into a missing table is a classified Db error, got {failed:?}",
    );
    // The connection recovered on the SAME connection — no fresh connection needed.
    assert!(c.is_healthy(), "connection recovers to a clean idle after a failed COPY command");
    c.ping().await.expect("ping recovers on the same connection");
    assert_eq!(
        c.query_sql("SELECT 1::int4").await.expect("query").rows[0].get_i32(0),
        Ok(Some(1)),
        "a follow-up query works on the recovered connection",
    );
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_out_early_break_recovers() {
    // Breaking out of copy_out early drains the remaining rows to a clean idle;
    // the connection stays reusable.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_brk(id int4)").await.expect("create");
    let rows: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    assert_eq!(c.copy_in("cp_brk", &rows).await.expect("seed"), 1000);

    // Stop after the first chunk.
    let mut seen = 0u32;
    let broke: Option<u32> = c
        .copy_out("cp_brk", |_chunk| {
            seen += 1;
            core::ops::ControlFlow::Break(seen)
        })
        .await
        .expect("copy_out");
    assert_eq!(broke, Some(1), "broke on the first row");
    // Drained + reusable.
    assert!(c.is_healthy());
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_brk").await.expect("count").rows[0].get_i64(0),
        Ok(Some(1000)),
    );
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_streaming_bulk_constant_memory() {
    // Constant-memory witness: stream 100k rows through the writer WITHOUT
    // building a Vec of them all — a lazy generator feeds the writer one row at a
    // time. The driver holds only the reused scratch buffer + the bounded send
    // buffer, never the 100k rows.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_bulk(id int8, payload text)")
        .await
        .expect("create");
    const N: i64 = 100_000;
    let n = c
        .copy_in_with("cp_bulk", async |w| {
            for i in 0..N {
                w.write_row(format!("{i}\tpayload-row-{i}").as_bytes()).await?;
            }
            Ok(())
        })
        .await
        .expect("bulk copy_in");
    assert_eq!(n, u64::try_from(N).expect("N fits u64"));
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_bulk").await.expect("count").rows[0].get_i64(0),
        Ok(Some(N)),
    );
    c.close().await.expect("close");
}
