#![forbid(unsafe_code)]
use core::str::FromStr as _;

use bsql_postgres_async::{ColumnError, ConnectConfig, Connection, DriverError, SslMode};
use bsql_postgres_proto::{DecodeError, Json, Numeric};

// ═══════════════════════════════════════════════════════════
// Driver-specific tests (async I/O, TLS, pool, protocol)
// SQL coverage is in the shared macro at the bottom.
// ═══════════════════════════════════════════════════════════

/// WITNESS (unix-domain transport): connect over the LOCAL UNIX SOCKET (host is
/// the socket dir `/tmp`, turned into `<dir>/.s.PGSQL.<port>` by libpq's rule),
/// round-trip a query, and confirm the connection is plaintext (`is_encrypted()`
/// == false). This is the transport the original bsql used and the bench baseline
/// assumed; it proves the new AF_UNIX path end-to-end on the async driver.
#[tokio::test]
#[ignore = "requires local PG on a unix socket"]
async fn connect_over_unix_socket_and_query() {
    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&cfg).await.expect("unix-socket connect");
    c.ping().await.expect("ping over unix socket");
    assert!(c.is_healthy());
    assert!(
        !c.is_encrypted(),
        "a unix-domain socket carries no TLS — is_encrypted() must be false"
    );
    assert!(c.backend_pid() > 0);
    // A real decode round-trip over the socket, not just a framing ping.
    let row = c
        .query_one_sql("SELECT 'bsql-over-unix'")
        .await
        .expect("query over unix socket");
    assert_eq!(row.get_str(0), Ok(Some("bsql-over-unix")));
    c.close().await.expect("close");
}

/// WITNESS (C1a — server NOTICE surfacing): a query that `RAISE NOTICE`s
/// surfaces the notice through the installed diagnostics sink with its severity
/// + SQLSTATE + message, instead of the driver silently dropping it (a `NOTICE`
/// is the primary PL/pgSQL logging channel). Proves `Connection::connect_with`
/// installs the sink and the `capture_notify` adapter routes a `NoticeResponse`
/// to `DiagEvent::ServerNotice` end-to-end.
#[tokio::test]
#[ignore = "requires local PG"]
async fn raise_notice_surfaces_through_the_diagnostics_sink() {
    use std::sync::{Arc, Mutex};

    use bsql_postgres_async::{DiagEvent, Diagnostics};

    let captured: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_in = Arc::clone(&captured);
    let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
        if let DiagEvent::ServerNotice { severity, code, message } = ev {
            captured_in.lock().expect("diag lock").push((
                severity.to_string(),
                code.to_string(),
                message.to_string(),
            ));
        }
    });

    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect_with(&cfg, &diag).await.expect("connect_with");
    // A `DO` block that raises a NOTICE — the PL/pgSQL log channel. Runs as a
    // plain command (no rows), so the notice rides its response stream.
    c.execute_sql("DO $$ BEGIN RAISE NOTICE 'hello from bsql notice'; END $$")
        .await
        .expect("DO with RAISE NOTICE");

    let got = captured.lock().expect("diag lock").clone();
    assert!(
        got.iter()
            .any(|(sev, _code, msg)| sev == "NOTICE" && msg == "hello from bsql notice"),
        "the RAISE NOTICE must surface through the sink with its severity + message, got {got:?}",
    );
    // The connection stays fully usable after surfacing the notice.
    let row = c.query_one_sql("SELECT 42").await.expect("query after notice");
    assert_eq!(row.get_i32(0), Ok(Some(42)));
    drop(c); // cleanup only; the witness assertions ran above
}

/// WITNESS (C1b — SSL downgrade routing): a TCP connect with `SslMode::Prefer`
/// to a server that refuses TLS (the local PG has `ssl=off`) falls back to
/// plaintext AND routes the downgrade through the installed sink as
/// `DiagEvent::SslDowngrade` (naming the host) — instead of the bare stderr
/// warning a headless service cannot capture. Proves the sink is threaded into
/// the connect sequence, so a CONNECT-time event surfaces.
#[tokio::test]
#[ignore = "requires local PG with ssl=off on TCP"]
async fn ssl_prefer_downgrade_routes_through_the_diagnostics_sink() {
    use std::sync::{Arc, Mutex};

    use bsql_postgres_async::{DiagEvent, Diagnostics};

    let downgrades: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let downgrades_in = Arc::clone(&downgrades);
    let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
        if let DiagEvent::SslDowngrade { host } = ev {
            downgrades_in.lock().expect("diag lock").push((*host).to_string());
        }
    });

    // TCP (not unix — the SSLRequest probe is TCP-only) with an EXPLICIT Prefer,
    // so a refusal is a downgrade rather than a loud Require error.
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Prefer);
    let c = Connection::connect_with(&cfg, &diag).await.expect("connect_with over TCP");
    assert!(!c.is_encrypted(), "the server refused TLS — the connection is plaintext");

    let got = downgrades.lock().expect("diag lock").clone();
    assert_eq!(
        got.as_slice(),
        &["127.0.0.1".to_string()],
        "the SSL downgrade must route through the sink with the host, got {got:?}",
    );
    drop(c); // cleanup only; the witness assertions ran above
}

/// WITNESS (C1c — pool saturation): a max-size-1 pool with one connection held
/// times out the second checkout AND surfaces a `DiagEvent::PoolAcquireTimeout`
/// through the sink, with the acquire-timeout counter + waiter high-water mark
/// recorded in `Pool::stats()`. The named "reconnect storm / thousands of
/// connections" blind zone made observable.
#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_acquire_timeout_emits_and_counts() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bsql_postgres_async::{DiagEvent, Pool};

    let timeouts: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let timeouts_in = Arc::clone(&timeouts);
    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let pool = Pool::builder(cfg, 1)
        .acquire_timeout(Duration::from_millis(150))
        .on_diagnostic(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::PoolAcquireTimeout { .. } = ev {
                *timeouts_in.lock().expect("diag lock") += 1;
            }
        })
        .build();

    // Hold the ONLY slot, then a second checkout must time out (no slot free).
    let held = pool.get().await.expect("first checkout");
    match pool.get().await {
        Err(DriverError::PoolTimeout) => {}
        Err(other) => panic!("expected PoolTimeout, got {other:?}"),
        Ok(_) => panic!("a max-size-1 pool must not hand out a second connection"),
    }

    assert_eq!(*timeouts.lock().expect("diag lock"), 1, "the acquire timeout emitted once");
    let stats = pool.stats();
    assert_eq!(stats.acquire_timeouts, 1, "the counter recorded the timeout");
    assert!(stats.waiters_high_water >= 1, "a waiter was queued: {stats:?}");
    drop(held);
}

/// WITNESS (C1d — slow-query detection): with a slow-query threshold set, a
/// query whose round trip exceeds it emits `DiagEvent::SlowQuery` carrying the
/// SQL TEXT (never the param values — no PII), while a fast query below the
/// threshold emits nothing. The zero-cost-off half (no clock read when the
/// threshold is unset) is proven offline by `Diagnostics::slow_query_armed`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn slow_query_emits_with_the_threshold_set() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bsql_postgres_async::{DiagEvent, Diagnostics};

    let slow: Arc<Mutex<Vec<(String, Duration)>>> = Arc::new(Mutex::new(Vec::new()));
    let slow_in = Arc::clone(&slow);
    let diag = Diagnostics::new()
        .slow_query_threshold(Duration::from_millis(50))
        .on_event(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::SlowQuery { sql, elapsed } = ev {
                slow_in.lock().expect("diag lock").push(((*sql).to_string(), *elapsed));
            }
        });
    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect_with(&cfg, &diag).await.expect("connect_with");

    // A fast query is BELOW the 50ms threshold → no event.
    let _row = c.query_one_sql("SELECT 1").await.expect("fast query");
    assert!(slow.lock().expect("diag lock").is_empty(), "a fast query is not reported slow");

    // A slow query (the server sleeps 200ms) is ABOVE it → exactly one event.
    let _qr = c.query_sql("SELECT pg_sleep(0.2)").await.expect("slow query");
    let got = slow.lock().expect("diag lock").clone();
    assert_eq!(got.len(), 1, "the slow query emitted once, got {got:?}");
    assert!(got[0].0.contains("pg_sleep"), "the event carries the SQL text, got {:?}", got[0].0);
    assert!(got[0].1 >= Duration::from_millis(50), "elapsed >= threshold, got {:?}", got[0].1);
    drop(c); // cleanup only; the witness assertions ran above
}

/// FAIL LOUD: `SslMode::Require` over a unix-domain socket is a classified
/// `DriverError::Config` — never a silent plaintext downgrade. Needs NO live PG:
/// the rejection precedes the connect syscall (it still completes within the
/// `connect_timeout` budget wrapping the sequence).
#[tokio::test]
async fn unix_socket_ssl_require_is_a_loud_config_error() {
    let cfg =
        ConnectConfig::new("/var/run/postgresql", "u").ssl_mode(SslMode::Require);
    match Connection::connect(&cfg).await {
        Err(DriverError::Config(msg)) => assert!(
            msg.contains("unix-domain socket"),
            "the error must name the unix-socket cause, got {msg:?}"
        ),
        // `Connection` is not `Debug`, so the `Ok` arm cannot print it — the
        // failure message is explicit instead.
        Ok(_) => panic!("Require over a unix socket must fail, but a connection opened"),
        Err(other) => panic!("Require over a unix socket must be a Config error, got {other:?}"),
    }
}

/// WITNESS (query cancellation): start a long `SELECT pg_sleep(5)` on one
/// connection, then from ANOTHER task send an out-of-band cancel via a
/// `CancelToken` obtained BEFORE the query. The query must return a classified
/// SQLSTATE `57014` (`query_canceled`) WELL under the 5-second sleep, and the
/// connection must be left drained + reusable (a canceled query is a recoverable
/// server error). This proves the whole out-of-band path end-to-end: the token is
/// detached (moved into the cancel task while the query future is in flight), the
/// throwaway socket redials the same endpoint, and the server honors the cancel.
#[tokio::test]
#[ignore = "requires local PG"]
async fn cancel_token_stops_an_inflight_query() {
    let cfg = ConnectConfig::new("localhost", "smir-ant").database("postgres".to_string());
    let mut conn = Connection::connect(&cfg).await.expect("connect");
    // The token is obtained BEFORE the long query and borrows nothing from `conn`.
    let token = conn.cancel_token();
    assert!(token.backend_pid() > 0, "the token names the backend to cancel");
    // From another task, cancel ~300 ms in — long after pg_sleep(5) has started
    // server-side, long before it would finish.
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        token.cancel().await
    });
    let start = std::time::Instant::now();
    let outcome = conn.query_sql("SELECT pg_sleep(5)").await;
    let elapsed = start.elapsed();
    canceller
        .await
        .expect("cancel task join")
        .expect("cancel packet delivered");
    match outcome {
        Err(DriverError::Db(db)) => assert!(
            db.is_code("57014"),
            "a canceled query must be SQLSTATE 57014 query_canceled, got {}",
            db.code()
        ),
        Ok(_) => panic!("pg_sleep(5) must be canceled, not run to completion"),
        Err(other) => panic!("cancel must surface as DriverError::Db(57014), got {other:?}"),
    }
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "cancel must return well under the 5s sleep, took {elapsed:?}"
    );
    // A canceled query is a RECOVERABLE server error: the verb drained the
    // ErrorResponse + ReadyForQuery, so the connection stays healthy and reusable.
    assert!(
        conn.is_healthy(),
        "the connection must be drained + reusable after a cancel"
    );
    let row = conn
        .query_one_sql("SELECT 1")
        .await
        .expect("connection reusable after cancel");
    assert_eq!(row.get_str(0), Ok(Some("1")));
    conn.close().await.expect("close");
}

/// WITNESS (C5 — `is_disconnect`): a connection whose backend is TERMINATED
/// mid-flight (`pg_terminate_backend` from a second connection) fails its
/// in-flight query with an error that `DriverError::is_disconnect()` classifies
/// TRUE — the "reconnect" signal — whether the failure surfaces as a FATAL
/// `57P01` server error or as a torn socket. A plain SYNTAX error on a healthy
/// connection classifies FALSE (fix the query, the connection is fine).
#[tokio::test]
#[ignore = "requires local PG"]
async fn is_disconnect_true_on_terminated_backend_false_on_syntax_error() {
    use std::time::Duration;

    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let mut victim = Connection::connect(&cfg).await.expect("connect victim");
    let mut killer = Connection::connect(&cfg).await.expect("connect killer");
    let pid = victim.backend_pid();
    assert!(pid > 0, "backend pid must be captured from the handshake");

    // Kill the victim MID-FLIGHT: it starts a 3s sleep; ~200ms in the killer
    // terminates its backend, so the in-flight query dies on the wire.
    let sleeping = victim.query_one_sql("SELECT pg_sleep(3)");
    let terminating = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let terminated = killer
            .query_one_sql(&format!("SELECT pg_terminate_backend({pid})"))
            .await
            .expect("terminate the victim backend");
        assert_eq!(terminated.get_str(0), Ok(Some("t")), "pg_terminate_backend returned true");
    };
    let (victim_res, ()) = tokio::join!(sleeping, terminating);

    let disconnect_err = match victim_res {
        Err(e) => e,
        Ok(_) => panic!("a terminated backend must fail the in-flight query"),
    };
    assert!(
        disconnect_err.is_disconnect(),
        "a terminated connection must classify as a disconnect, got {disconnect_err:?}",
    );

    // A syntax error on the STILL-HEALTHY killer connection is NOT a disconnect.
    let syntax_err = match killer.query_one_sql("SELECT bogus not valid sql !!").await {
        Err(e) => e,
        Ok(_) => panic!("a syntax error must fail"),
    };
    assert!(
        !syntax_err.is_disconnect(),
        "a syntax error is not a disconnect (the connection is fine), got {syntax_err:?}",
    );
    // Proof the killer connection survived its own syntax error.
    let row = killer.query_one_sql("SELECT 1").await.expect("healthy after a syntax error");
    assert_eq!(row.get_str(0), Ok(Some("1")));
    killer.close().await.expect("close killer");
}

/// WITNESS (C6 — `statement_timeout`): a connection built with
/// `with_statement_timeout(200ms)` has the SERVER abort a runaway query
/// (`pg_sleep(2)`) with SQLSTATE `57014` `query_canceled`; the cancel is NOT a
/// disconnect (`is_disconnect()` is false), so the connection RECOVERS and is
/// reusable. A connection WITHOUT the timeout runs the same-shape sleep to
/// completion.
#[tokio::test]
#[ignore = "requires local PG"]
async fn statement_timeout_aborts_a_runaway_query_and_the_connection_recovers() {
    use std::time::Duration;

    let cfg = ConnectConfig::new("/tmp", "smir-ant")
        .database("postgres".to_string())
        .with_statement_timeout(Duration::from_millis(200));
    let mut c = Connection::connect(&cfg).await.expect("connect with statement_timeout");

    let err = match c.query_one_sql("SELECT pg_sleep(2)").await {
        Err(e) => e,
        Ok(_) => panic!("pg_sleep(2) must be aborted by statement_timeout=200ms"),
    };
    match &err {
        DriverError::Db(db) => assert!(
            db.is_code("57014"),
            "statement_timeout must abort with 57014 query_canceled, got {}",
            db.code(),
        ),
        other => panic!("statement_timeout must surface as DriverError::Db(57014), got {other:?}"),
    }
    // A statement_timeout abort is a RECOVERABLE server error, never a disconnect.
    assert!(!err.is_disconnect(), "a statement_timeout cancel is not a disconnect");
    let row = c.query_one_sql("SELECT 1").await.expect("connection reusable after statement_timeout");
    assert_eq!(row.get_str(0), Ok(Some("1")));
    c.close().await.expect("close");

    // WITHOUT the timeout, the same-shape sleep runs to completion.
    let plain = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let mut c2 = Connection::connect(&plain).await.expect("connect without statement_timeout");
    let done = c2.query_one_sql("SELECT pg_sleep(0.3)").await.expect("no timeout — sleep completes");
    assert!(done.get_str(0).is_ok(), "the completed pg_sleep row is readable (void)");
    c2.close().await.expect("close");
}

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

/// WITNESS (R5 — a connect-time server error is CLASSIFIED): a connect to a
/// NON-EXISTENT database surfaces the server's `ErrorResponse` as a fully
/// classified `DriverError::Db` — SQLSTATE `3D000` (`invalid_catalog_name`) plus
/// the server's message — NOT a single opaque I/O string. A consumer can match
/// `err.code()` / `is_invalid_catalog_name()` on a CONNECT error exactly as on an
/// active-phase error. Formerly collapsed to
/// `Io("server returned an error during startup")`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn connect_to_missing_database_classifies_3d000() {
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant").database("bsql_r5_no_such_db".to_string());
    match Connection::connect(&cfg).await {
        Err(DriverError::Db(db)) => {
            assert_eq!(db.code(), "3D000", "a wrong-DB connect must classify as 3D000");
            assert!(db.is_invalid_catalog_name(), "the 3D000 predicate must hold");
            assert!(
                db.message.contains("bsql_r5_no_such_db"),
                "the server message must name the missing database, got {:?}",
                db.message,
            );
        }
        Ok(_) => panic!("a connect to a non-existent database must fail"),
        Err(other) => panic!("expected DriverError::Db(3D000), got {other:?}"),
    }
}

/// WITNESS (R5 — bad authorization is CLASSIFIED): a connect as a NON-EXISTENT
/// role surfaces the server's `ErrorResponse` as a classified `DriverError::Db`
/// with an auth SQLSTATE in the `28xxx` class (`28000`
/// invalid_authorization_specification), NOT an opaque string — the same
/// classified `DbError` the active path produces, decoded through the same
/// `parse_error_response`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn connect_as_missing_role_classifies_auth_error() {
    let cfg =
        ConnectConfig::new("127.0.0.1", "bsql_r5_no_such_role").database("postgres".to_string());
    match Connection::connect(&cfg).await {
        Err(DriverError::Db(db)) => assert!(
            db.code().starts_with("28"),
            "a bad-authorization connect must classify in the 28xxx class, got {}",
            db.code(),
        ),
        Ok(_) => panic!("a connect as a non-existent role must fail"),
        Err(other) => panic!("expected DriverError::Db(28xxx), got {other:?}"),
    }
}

/// The async peer of the sync `wide_columns` witness: a 1664-column result — the
/// widest PostgreSQL produces (`MaxTupleAttributeNumber`), now the driver's cap —
/// decodes correctly over the shared `Core<S>`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn wide_columns_1664_decode() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    for n in [1000u32, 1664] {
        let cols: Vec<String> = (0..n).map(|i| format!("{i}::int AS c{i}")).collect();
        let sql = format!("SELECT {}", cols.join(", "));
        let r = match c.query_sql(&sql).await {
            Ok(r) => r,
            Err(e) => panic!("{n} cols failed: {e}"),
        };
        assert_eq!(r.len(), 1, "rows at {n}");
        assert_eq!(r.column_names.len(), usize::try_from(n).unwrap(), "col names at {n}");
        let last = usize::try_from(n.saturating_sub(1)).unwrap();
        assert_eq!(
            r.get(0).expect("row 0").get_i32(last),
            Ok(Some(n.saturating_sub(1) as i32)),
            "last col at {n}"
        );
    }
    // Beyond PG's own 1664 limit is a recoverable SERVER error; the connection
    // survives to serve a follow-up query (the client-side over-cap is witnessed
    // deterministically in `bsql-testkit`'s `overcap_recovery`).
    let over: Vec<String> = (0..1665u32).map(|i| format!("{i}::int AS c{i}")).collect();
    let over_sql = format!("SELECT {}", over.join(", "));
    let err = c.query_sql(&over_sql).await.expect_err("1665 exceeds PG's 1664 limit");
    match err {
        DriverError::Db(db) => assert!(
            format!("{db}").contains("target lists can have at most 1664"),
            "expected PG's target-list-limit error, got: {db}"
        ),
        other => panic!("expected a server Db error, got {other:?}"),
    }
    let r = c.query_sql("SELECT 7").await.expect("connection recovered after the server error");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(7)));
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
    assert_eq!(slept.len(), 1, "pg_sleep returns exactly one (void) row");
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

    // WITNESS: `QueryResult::affected()` surfaces the affected-row count on the
    // dynamic `query_params` result — the capability the `Copy` `CommandTag`
    // closed (a dynamic caller no longer has to string-parse a tag). A
    // non-RETURNING UPDATE yields zero rows but a non-zero affected count.
    c.execute_sql("CREATE TEMP TABLE m1_aff (id int)").await.expect("temp table");
    let inserted = c
        .execute_sql("INSERT INTO m1_aff VALUES (1), (2), (3)")
        .await
        .expect("seed rows");
    assert_eq!(inserted, 3, "execute_sql reports the INSERT count");
    let upd = c
        .query_params("UPDATE m1_aff SET id = id + 10 WHERE id >= $1", &(2_i32,))
        .await
        .expect("parameterized UPDATE");
    assert_eq!(upd.affected(), 2, "query_params result exposes the UPDATE affected count");
    assert_eq!(upd.len(), 0, "a non-RETURNING UPDATE yields no rows");
    let sel = c
        .query_params("SELECT id FROM m1_aff WHERE id >= $1", &(1_i32,))
        .await
        .expect("parameterized SELECT");
    assert_eq!(sel.affected(), 3, "a SELECT's affected() is its returned row count");
    assert_eq!(sel.len(), 3);

    c.close().await.expect("close");
}

/// WITNESS (D1 — dynamic-param TYPE FIDELITY, async twin of
/// `dynamic_param_type_fidelity_sync`): the dynamic `query_params` family declares
/// each parameter's ENCODED type OID in its `Parse`, so a Rust value whose type
/// disagrees with the SQL-inferred type is a LOUD classified server error — never
/// a silent binary reinterpretation. The exact repro: binding `&str "AAAA"`
/// against the int4 `id = $1` used to SILENTLY match `id = 1094795585` (the four
/// ASCII bytes read as int4); now `$1` is declared `text`, so `int4 = text` is a
/// classified `42883` and the connection recovers. Correct + coercible params
/// round-trip; the (SQL, P::OIDS)-keyed cache never reuses a plan across param
/// types.
#[tokio::test]
#[ignore = "requires local PG"]
async fn dynamic_param_type_fidelity_async() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE d1_tf_a (id int4 PRIMARY KEY, big int8, name text)")
        .await
        .expect("temp table");
    c.execute_sql("INSERT INTO d1_tf_a VALUES (1094795585, 999, 'target'), (1, 100, 'one')")
        .await
        .expect("seed rows");

    // Correctly-typed param round-trips (the happy path is unregressed).
    let row = c
        .query_params_one("SELECT name FROM d1_tf_a WHERE id = $1", &(1_i32,))
        .await
        .expect("correctly-typed int4 param round-trips");
    assert_eq!(row.get_str(0), Ok(Some("one")));

    // THE REPRO: a `&str` bound against the int4 `id = $1` MUST be a classified
    // error, NOT the silent match of the `id = 1094795585` row.
    let err = c
        .query_params_one("SELECT id FROM d1_tf_a WHERE id = $1", &("AAAA",))
        .await
        .expect_err("a &str bound against an int4 column must be a LOUD type error");
    match err {
        DriverError::Db(db) => {
            assert_eq!(db.code(), "42883", "int4 = text has no operator, got {}", db.code());
        }
        other => panic!("wrong-typed dynamic param must be DriverError::Db(42883), got {other:?}"),
    }
    // The connection RECOVERS from the classified error.
    let recovered = c
        .query_params_one("SELECT id FROM d1_tf_a WHERE id = $1 AND true", &(1_i32,))
        .await
        .expect("connection recovers after the classified type error");
    assert_eq!(recovered.get_i32(0), Ok(Some(1)));

    // A COERCIBLE int8 param into an int4 comparison is coerced by PG (distinct
    // SQL, first sighting), returning that row's `big`.
    let coerced = c
        .query_params_one("SELECT big FROM d1_tf_a WHERE id = $1", &(1_i64,))
        .await
        .expect("int8 param coerces into the int4 comparison");
    assert_eq!(coerced.get_i64(0), Ok(Some(100)));

    // The CACHED (promoted) plan preserves fidelity (fused→promote→reuse), all correct.
    for _ in 0..3 {
        let r = c
            .query_params_one("SELECT id FROM d1_tf_a WHERE name = $1", &("one",))
            .await
            .expect("cached-plan query round-trips");
        assert_eq!(r.get_i32(0), Ok(Some(1)));
    }
    // CACHE TYPE-FIDELITY: a `float4` sighting of the just-cached `text` SQL is a
    // DISTINCT key — its own plan (`text = float4` has no operator → loud), never a
    // silent reinterpret of the 4 bytes against the text plan.
    match c
        .query_params_one("SELECT id FROM d1_tf_a WHERE name = $1", &(1.0_f32,))
        .await
    {
        Err(DriverError::Db(_)) => {}
        other => panic!("a float4 reuse of a text-cached SQL must be a loud Db error, got {other:?}"),
    }
    let still_cached = c
        .query_params_one("SELECT id FROM d1_tf_a WHERE name = $1", &("one",))
        .await
        .expect("the text-typed cached plan survives the distinct-key float4 sighting");
    assert_eq!(still_cached.get_i32(0), Ok(Some(1)));

    // `execute_params` shares the fused Parse: a wrong-typed bind is loud there too.
    let exec_err = c
        .execute_params("UPDATE d1_tf_a SET name = 'x' WHERE id = $1", &("AAAA",))
        .await
        .expect_err("execute_params must reject a &str bound against int4");
    assert!(matches!(exec_err, DriverError::Db(_)));

    c.close().await.expect("close");
}

/// MAJOR-1 (async twin of `prepared_param_type_fidelity_sync`): the EXPLICIT
/// prepared-statement path verifies the caller's parameter types against the
/// statement's fixed (server-inferred) plan BEFORE binding, so a wrong-typed bind
/// is a LOUD client-side `ParamTypeMismatch`, never a silent reinterpret.
#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_param_type_fidelity_async() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE pf_tf_a (id int4 PRIMARY KEY, name text)")
        .await
        .expect("temp table");
    c.execute_sql("INSERT INTO pf_tf_a VALUES (1094795585, 'target'), (1, 'one')")
        .await
        .expect("seed rows");

    let stmt = c
        .prepare("SELECT name FROM pf_tf_a WHERE id = $1")
        .await
        .expect("prepare");

    // Correctly-typed param round-trips.
    let ok = c
        .query_prepared(&stmt, &(1_i32,))
        .await
        .expect("correct int4 binds");
    let ok_row = ok.get(0).expect("one row");
    assert_eq!(ok_row.get_str(0), Ok(Some("one")));

    // THE REPRO: a `&str` bound against int4 `$1` — a client-side reject, not the
    // silent `id = 1094795585` match.
    let err = c
        .query_prepared(&stmt, &("AAAA",))
        .await
        .expect_err("a &str bound to an int4 prepared param must be a LOUD reject");
    match err {
        DriverError::ParamTypeMismatch { index, expected, found } => {
            assert_eq!(index, 0);
            assert_eq!(expected, 23, "server inferred int4");
            assert_eq!(found, 25, "client bound text");
        }
        other => panic!("expected ParamTypeMismatch, got {other:?}"),
    }

    // The connection is untouched (no Bind was sent).
    let after = c
        .query_prepared(&stmt, &(1_i32,))
        .await
        .expect("stmt still usable");
    let after_row = after.get(0).expect("one row");
    assert_eq!(after_row.get_str(0), Ok(Some("one")));

    // Arity mismatch is caught client-side.
    let arity = c
        .query_prepared(&stmt, &(1_i32, 2_i32))
        .await
        .expect_err("2 params for a 1-param statement must be a LOUD reject");
    assert!(matches!(
        arity,
        DriverError::ParamCountMismatch { expected: 1, found: 2 }
    ));

    // `execute_prepared` verifies identically; a coercible int8 is strict-rejected
    // against the fixed int4 plan (no coercion on a fixed plan).
    let coerce = c
        .query_prepared(&stmt, &(1_i64,))
        .await
        .expect_err("int8 against a fixed int4 plan is strict-rejected");
    assert!(matches!(
        coerce,
        DriverError::ParamTypeMismatch { expected: 23, found: 20, .. }
    ));

    c.close().await.expect("close");
}

/// WITNESS: the DYNAMIC prepared-statement cache is transparent and correct —
/// the SAME parameterized SQL run many times with DIFFERENT params returns each
/// call's OWN row. The first sighting runs the fused unnamed path, the second
/// prepares a named statement, and every later call reuses the server-side plan
/// (`Bind`+`Execute`, no re-parse); a leaked binding or a mis-reused plan would
/// return a stale row. Also asserts a genuinely one-shot query (a distinct SQL
/// run once) still works — it never leaves the fused path.
#[tokio::test]
#[ignore = "requires local PG"]
async fn dynamic_cache_reuse_returns_correct_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    // The SAME SQL text, executed across the loop with different params — the
    // fused→promote→reuse progression happens inside `query_params`. Each result
    // must be the call's own value (text-format dynamic Row).
    let sql = "SELECT ($1::int * 10) AS v";
    for round in 0..3 {
        for i in 0..20_i32 {
            let row = c.query_params_one(sql, &(i,)).await.expect("cached reuse");
            assert_eq!(
                row.get_i32(0),
                Ok(Some(i * 10)),
                "round {round}, i {i}: cached plan returned the wrong row"
            );
        }
    }

    // A one-shot distinct SQL (run once) still works via the fused path.
    let one = c
        .query_params_one("SELECT $1::int + 7 AS w", &(35_i32,))
        .await
        .expect("one-shot fused");
    assert_eq!(one.get_i32(0), Ok(Some(42)));

    c.close().await.expect("close");
}

/// WITNESS: the cache SELF-HEALS a stale plan TRANSPARENTLY after a schema
/// change — the caller never sees a spurious error. A cached `SELECT *` whose
/// result type changes (an `ALTER TABLE ... ADD COLUMN`) would bind a stale plan
/// (PG's `0A000` "cached plan must not change result type"); the reuse path
/// detects that classified SQLSTATE, reclaims the stale statement, and RE-RUNS
/// the query on the fused path IN THE SAME CALL, so the very next query SUCCEEDS
/// with the new column set — a schema change costs one re-parse, never a
/// user-visible error and never a silently-stale result.
#[tokio::test]
#[ignore = "requires local PG"]
async fn dynamic_cache_self_heals_after_schema_change() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    c.execute_sql("DROP TABLE IF EXISTS bsql_cache_heal").await.expect("drop");
    c.execute_sql("CREATE TABLE bsql_cache_heal (id int, name text)")
        .await
        .expect("create");
    c.execute_sql("INSERT INTO bsql_cache_heal VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("seed");

    let sql = "SELECT * FROM bsql_cache_heal WHERE id >= $1 ORDER BY id";
    // Three runs: first sighting (fused) → second (promote to a named statement)
    // → third (reuse the cached plan). All see the 2-column schema.
    for _ in 0..3 {
        let r = c.query_params(sql, &(0_i32,)).await.expect("pre-alter");
        assert_eq!(r.column_names.len(), 2, "two columns before ALTER");
        assert_eq!(r.len(), 2);
    }

    // Change the result type — the cached plan is now stale.
    c.execute_sql("ALTER TABLE bsql_cache_heal ADD COLUMN extra int DEFAULT 0")
        .await
        .expect("alter");

    // The next reuse binds the STALE cached plan (a `0A000` on the wire), but the
    // driver's transparent self-heal re-runs the query on the fused path in the
    // SAME call — so the caller sees SUCCESS with the CURRENT 3-column schema, NOT
    // a spurious error, and NEVER the silently-stale 2-column result. Run several
    // more to prove the re-warmed cache is correct on reuse too.
    for i in 0..4 {
        let r = match c.query_params(sql, &(0_i32,)).await {
            Ok(r) => r,
            Err(e) => panic!("post-alter query must self-heal, not error (iter {i}): {e:?}"),
        };
        assert_eq!(r.column_names.len(), 3, "three columns after ALTER + transparent self-heal");
        assert_eq!(r.len(), 2);
    }

    c.execute_sql("DROP TABLE bsql_cache_heal").await.expect("cleanup");
    c.close().await.expect("close");
}

/// WITNESS: `reset_session` CLEARS the dynamic prepared-statement cache for pool
/// hygiene, `Close`ing the cached server-side statements. Observed directly via
/// `pg_prepared_statements`: a query cached to a named statement is PRESENT
/// before the reset and ABSENT after, and the query still returns correct
/// results afterward (the re-warm). A DIFFERENT logical user checked out of the
/// pool therefore never inherits the prior user's runtime-SQL plans.
#[tokio::test]
#[ignore = "requires local PG"]
async fn reset_session_clears_the_dynamic_prepared_cache() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    // Count bsql-named prepared statements the server currently holds.
    async fn bsql_stmt_count(c: &mut Connection) -> i64 {
        let row = c
            .query_one_sql("SELECT count(*) FROM pg_prepared_statements WHERE name ~ '^_bsql_'")
            .await
            .expect("count query");
        match row.get_i64(0) {
            Ok(Some(n)) => n,
            other => panic!("count was not an i64: {other:?}"),
        }
    }

    // Run the SAME dynamic query TWICE so it is promoted to a cached NAMED
    // server-side statement (first sighting fused, second prepares + caches).
    let sql = "SELECT ($1::int + 1) AS v";
    for _ in 0..2 {
        let row = c.query_params_one(sql, &(41_i32,)).await.expect("cache warm");
        assert_eq!(row.get_i32(0), Ok(Some(42)));
    }
    assert!(bsql_stmt_count(&mut c).await >= 1, "a named statement is cached before reset");

    // The pool-hygiene clear: close + forget the cached statements.
    c.reset_session().await.expect("reset_session");
    assert_eq!(
        bsql_stmt_count(&mut c).await,
        0,
        "reset_session must Close every cached dynamic statement"
    );

    // Re-warm: the same query still returns correct results (a fresh first sighting).
    let r = c.query_params_one(sql, &(41_i32,)).await.expect("re-warm");
    assert_eq!(r.get_i32(0), Ok(Some(42)));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_1k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 1000)").await.expect("q");
    assert_eq!(r.len(), 1000);
    assert_eq!(r.get(999).expect("row 999").get_i32(0), Ok(Some(1000)));
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
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(None));
    assert!(r.get(0).expect("row 0").is_null(0));

    // (2) An `i32` read of genuinely non-numeric text ('x') is a classified `Err`
    // over the real wire — exactly the failure the retired `.parse().ok()` hid as
    // a silent `None`. Assert the EXACT classified variant, not `.is_err()`.
    let r = c.query_sql("SELECT 'x'::text").await.expect("text query");
    assert_eq!(
        r.get(0).expect("row 0").get_i32(0),
        Err(ColumnError::Decode(DecodeError::IntParse)),
    );
    // A `bool` read of the same non-bool text classifies too (`BoolParse`),
    // proving the classification holds across decoders on the real wire.
    assert_eq!(
        r.get(0).expect("row 0").get_bool(0),
        Err(ColumnError::Decode(DecodeError::BoolParse)),
    );
    // Read as text the same column is a legitimate value — text is text.
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("x")));

    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_10k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query_sql("SELECT generate_series(1, 10000)").await.expect("q");
    assert_eq!(r.len(), 10000);
    c.close().await.expect("close");
}

/// WITNESS (dynamic streaming): `query_each_sql` streams a 20 000-row runtime
/// query ONE ROW AT A TIME — every row is seen, in order, with correct values —
/// WITHOUT eager-collecting the result (the escape from `query_sql` for a colossal
/// runtime SELECT). The alloc gate proves the constant memory; this proves the
/// correctness end-to-end against a live server.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_sql_streams_a_large_result_correctly() {
    use core::ops::ControlFlow;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let mut count = 0i64;
    let mut sum = 0i64;
    let mut expected_next = 1i64;
    let mut in_order = true;
    let out = c
        .query_each_sql::<_, ()>("SELECT generate_series(1, 20000) AS n", |row| {
            let n = row.get_i64(0).expect("decode n").expect("n is not NULL");
            if n != expected_next {
                in_order = false;
            }
            expected_next += 1;
            count += 1;
            sum += n;
            ControlFlow::Continue(())
        })
        .await
        .expect("stream completes");

    assert_eq!(out, None, "a full stream returns Ok(None)");
    assert_eq!(count, 20_000, "every row was streamed");
    assert!(in_order, "rows streamed in order");
    // 1 + 2 + … + 20000 = 20000·20001/2 = 200_010_000.
    assert_eq!(sum, 200_010_000, "every value was correct");
    // The connection is clean + reusable after a full stream.
    let after = c.query_one_sql("SELECT 'reusable'").await.expect("reuse");
    assert_eq!(after.get_str(0), Ok(Some("reusable")));
    c.close().await.expect("close");
}

/// WITNESS (dynamic streaming with a runtime param): `query_each_params` streams a
/// parameterised runtime query — the `$1` bound at run time filters the series, and
/// only the matching rows stream.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_params_streams_with_a_runtime_param() {
    use core::ops::ControlFlow;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let mut seen: Vec<i32> = Vec::new();
    let out = c
        .query_each_params::<_, _, ()>(
            "SELECT n FROM generate_series(1, 1000) AS n WHERE n <= $1 ORDER BY n",
            &(5_i32,),
            |row| {
                seen.push(row.get_i32(0).expect("decode").expect("not NULL"));
                ControlFlow::Continue(())
            },
        )
        .await
        .expect("stream completes");
    assert_eq!(out, None);
    assert_eq!(seen, vec![1, 2, 3, 4, 5], "the runtime $1 bound the filter");
    c.close().await.expect("close");
}

/// WITNESS (early break + reuse): a closure `Break` STOPS the stream early; the
/// break payload rides `Ok(Some(_))`, the remaining rows are drained to a clean
/// idle, and the connection is REUSABLE for a follow-up query — the drain keeps a
/// pooled connection healthy after an early abort of a colossal result.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_sql_break_stops_early_and_connection_is_reusable() {
    use core::ops::ControlFlow;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let mut seen = 0i64;
    // Break after 100 of a would-be 1,000,000-row stream.
    let stopped_at = c
        .query_each_sql::<_, i64>("SELECT generate_series(1, 1000000) AS n", |row| {
            let _n = row.get_i64(0).expect("decode").expect("not NULL");
            seen += 1;
            if seen >= 100 {
                ControlFlow::Break(seen)
            } else {
                ControlFlow::Continue(())
            }
        })
        .await
        .expect("stream drains after the early break");

    assert_eq!(stopped_at, Some(100), "the break payload rides Ok(Some(_))");
    assert_eq!(seen, 100, "the closure saw exactly the rows before its break");
    // The connection was drained back to idle — a follow-up query works.
    assert!(c.is_healthy());
    let after = c.query_one_sql("SELECT 7").await.expect("reuse after early break");
    assert_eq!(after.get_i32(0), Ok(Some(7)));
    c.close().await.expect("close");
}

/// WITNESS (oversize-row reassembly): `query_each_sql` streams multiple rows each
/// FAR larger than the 4 KiB read buffer — so each arrives split into `RowChunk`
/// pieces and is REASSEMBLED into the reused scratch before decode. Proves the
/// dynamic streaming path reconstructs a chunk-split row BYTE-EXACT (no truncation
/// at a chunk seam, no cross-row bleed), with an interleaved NULL cell and a
/// trailing small column both correctly positioned AFTER the big value, and the
/// connection reusable after. The dynamic peer of the typed oversize-row coverage.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_sql_reassembles_oversize_rows() {
    use core::ops::ControlFlow;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    // Three rows, each a big text (hundreds of KiB — >> the 4 KiB read buffer, so
    // each spans ~100 `RowChunk`s), a distinct fill char per row (to catch any
    // cross-row bleed), an interleaved SQL NULL, and a small trailing column.
    const SPEC: [(char, usize, &str); 3] = [('a', 400_000, "end-1"), ('b', 300_000, "end-2"), ('c', 500_000, "end-3")];
    let sql = "SELECT repeat('a', 400000) AS big, NULL::text AS mid, 'end-1' AS tail \
               UNION ALL SELECT repeat('b', 300000), NULL, 'end-2' \
               UNION ALL SELECT repeat('c', 500000), NULL, 'end-3'";

    // Collect owned facts per row inside the callback (the borrowed row cannot
    // escape); each row's big value is verified byte-exact (length + every byte the
    // expected fill char — a chunk-seam truncation would change the length).
    let mut rows: Vec<(usize, bool, bool, String)> = Vec::new();
    let out = c
        .query_each_sql::<_, ()>(sql, |row| {
            assert_eq!(row.len(), 3, "each streamed row has three columns");
            let big = row.get_str(0).expect("big decodes").expect("big is not NULL");
            let idx = rows.len();
            let (fill, _len, _tail) = SPEC[idx];
            let all_fill = big.bytes().all(|b| b == fill as u8);
            let mid_is_null = matches!(row.get_raw(1), Ok(None)) && row.is_null(1);
            let tail = row.get_str(2).expect("tail decodes").expect("tail not NULL").to_owned();
            rows.push((big.len(), all_fill, mid_is_null, tail));
            ControlFlow::Continue(())
        })
        .await
        .expect("oversize stream completes");

    assert_eq!(out, None);
    assert_eq!(rows.len(), 3, "every oversize row streamed");
    for (i, (len, all_fill, mid_is_null, tail)) in rows.iter().enumerate() {
        let (_fill, expected_len, expected_tail) = SPEC[i];
        assert_eq!(*len, expected_len, "row {i}: big value reassembled to its FULL length (no chunk-seam truncation)");
        assert!(*all_fill, "row {i}: every byte is this row's fill char (no cross-row chunk bleed)");
        assert!(*mid_is_null, "row {i}: the interleaved NULL cell reads as None after the big value");
        assert_eq!(tail, expected_tail, "row {i}: the trailing small column is correctly positioned after the reassembled big value");
    }
    // The reused oversize scratch is cleared between rows — the connection is clean.
    let after = c.query_one_sql("SELECT 'reusable-after-oversize'").await.expect("reuse");
    assert_eq!(after.get_str(0), Ok(Some("reusable-after-oversize")));
    c.close().await.expect("close");
}

/// WITNESS (streaming inside a transaction): the streaming verb works through the
/// transaction GUARD — a stream inside a `transaction` body sees every row and the
/// transaction commits normally.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_sql_streams_inside_a_transaction() {
    use core::ops::ControlFlow;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let count = c
        .transaction(async |tx| {
            let mut n = 0i64;
            tx.query_each_sql::<_, ()>("SELECT generate_series(1, 500)", |_row| {
                n += 1;
                ControlFlow::Continue(())
            })
            .await?;
            Ok(n)
        })
        .await
        .expect("transaction with a stream commits");
    assert_eq!(count, 500);
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
    assert_eq!(c.query_sql("SELECT v FROM res").await.expect("q").get(0).expect("row 0").get_i32(0), Ok(Some(42)));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn client_encoding_pinned_to_utf8_and_roundtrips_non_ascii() {
    // The startup message forces client_encoding=UTF8 so the driver's UTF-8
    // TEXT decode is correct regardless of the server's default encoding.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");

    let enc = c.query_sql("SHOW client_encoding").await.expect("show").get(0).expect("row 0")
        .get_str(0)
        .expect("client_encoding decodes")
        .map(String::from);
    assert_eq!(enc.as_deref(), Some("UTF8"), "startup must pin client_encoding=UTF8");

    // Non-ASCII (Cyrillic + emoji) round-trips byte-exact under the pinned UTF-8.
    let text = "Привет, мир 🌍";
    let r = c.query_sql(&format!("SELECT '{text}'::text")).await.expect("query");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some(text)));
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
        c.query_sql("SELECT 7::int4").await.expect("follow-up query works").get(0).expect("row 0").get_i32(0),
        Ok(Some(7)),
    );

    // A UNIFORM-width multi-statement batch is fine: rows flatten into one arena
    // whose single stride addresses every cell correctly.
    let uniform = c.query_sql("SELECT 1::int4; SELECT 2::int4").await.expect("uniform batch");
    assert_eq!(uniform.len(), 2);
    assert_eq!(uniform.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(uniform.get(1).expect("row 1").get_i32(0), Ok(Some(2)));
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
        c.query_sql("SELECT count(*) FROM cp_inj").await.expect("count").get(0).expect("row 0").get_i64(0),
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
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));

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
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
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
    let pool = Pool::new(config, 3);
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
    let pool = Pool::new(config, 3);
    let handles: Vec<_> = (0..10u32).map(|i| {
        let p = pool.clone();
        tokio::spawn(async move {
            let mut c = p.get().await.expect("get");
            assert_eq!(c.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int")).await.expect("q").get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
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
    let pool = Pool::new(config, 1); // max_size=1 forces reuse
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
    let sp = conn.query_sql("SHOW search_path").await.expect("show").get(0).expect("row 0")
        .get_str(0).expect("search_path decodes").map(String::from);
    assert_ne!(sp.as_deref(), Some("pg_temp"), "search_path GUC bled across checkout");
    // Temp table gone.
    let n = conn.query_sql("SELECT count(*) FROM pg_tables WHERE tablename='bleed_probe'")
        .await.expect("tmp").get(0).expect("row 0").get_i64(0).expect("count decodes");
    assert_eq!(n, Some(0), "temp table bled across checkout");
    // LISTEN channel gone (UNLISTEN * ran in the reset).
    let listening = conn
        .query_sql("SELECT count(*)::int8 FROM pg_listening_channels() AS c(chan) WHERE chan='bleed_chan'")
        .await.expect("listen check").get(0).expect("row 0").get_i64(0).expect("listen count decodes");
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

    let pool = Pool::new(mk_config(), 1); // max_size=1 forces reuse
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
        .get(0).expect("row 0")
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
    assert_eq!(probe.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
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
    let pool = Pool::new(config, 1);
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
    let pool = Pool::new(config, 1);
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
    let pool = Pool::new(config, 1);
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
    let row = c.query_sql("SELECT 42::int").await.expect("q").get(0).expect("row 0");
    assert_eq!(tokio::task::spawn(async move { row.get_i32(0).expect("i32 decodes") }).await.expect("spawn"), Some(42));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth"]
async fn scram_auth() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string()).password("test_password_123".to_string());
    let mut c = Connection::connect(&config).await.expect("SCRAM");
    assert_eq!(c.query_sql("SELECT current_user").await.expect("q").get(0).expect("row 0").get_raw(0), Ok(Some(b"bsql_test_scram".as_slice())));
    c.close().await.expect("close");
}

/// LIVE interop witness for RFC 4013 SASLprep (RFC 5802 SCRAM).
///
/// The `bsql_test_scram` role's password is set to `pa\u{00A0}ss` — a
/// NO-BREAK SPACE `U+00A0` that SASLprep MAPS to a plain space — via
/// `ALTER ROLE`. PostgreSQL SASLpreps the plaintext when it builds the stored
/// verifier, so the server holds the verifier for `pa ss`. Connecting through
/// bsql with the RAW unicode password `pa\u{00A0}ss` must SUCCEED: the driver's
/// credential builder SASLpreps client-side to `pa ss`, so the proof matches.
///
/// This is the reported defect inverted into a green gate: WITHOUT the SASLprep
/// fix bsql fed the raw bytes (`61 62 c2 a0 63 64`) to PBKDF2 and the server
/// rejected the proof with `28P01`. It is the exact peer of libpq, which has
/// always authenticated this password.
///
/// The password is mutated and RESTORED over a `smir-ant` trust connection. The
/// assert-bearing body runs inside a `tokio::spawn`, so a panic ANYWHERE in it
/// (a failed connect, a wrong `current_user`) is CONTAINED in the `JoinHandle`
/// rather than propagated — the restore below then ALWAYS runs, and the original
/// panic is re-raised AFTER the role is back on its password so the
/// `scram_auth` test above stays green. This is the async-native equivalent of
/// an RAII/`catch_unwind` restore guard (a `Drop` guard cannot `.await` the
/// restore). The password constant contains no `'`, so the fixed-literal
/// `ALTER ROLE` splice has no injection surface.
#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth (mutates+restores bsql_test_scram password)"]
async fn scram_saslprep_normalizes_a_unicode_password() {
    const UNICODE_PW: &str = "pa\u{00A0}ss"; // NO-BREAK SPACE -> SASLprep maps to ' '
    const ORIGINAL_PW: &str = "test_password_123";

    // DDL over a trust connection as smir-ant (a superuser on the dev box).
    let admin_cfg = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut admin = Connection::connect(&admin_cfg).await.expect("admin trust connect");
    admin
        .execute_sql(&format!("ALTER ROLE bsql_test_scram PASSWORD '{UNICODE_PW}'"))
        .await
        .expect("set the SASLprep-sensitive password");

    // Run the panic-prone body as a spawned task so its panic is caught in the
    // JoinHandle; the restore below is thereby guaranteed to run.
    let outcome = tokio::spawn(async {
        // Connect as bsql_test_scram with the RAW unicode password — bsql SASLpreps.
        let scram_cfg = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
            .database("postgres".to_string())
            .password(UNICODE_PW.to_string());
        let mut c = Connection::connect(&scram_cfg)
            .await
            .expect("SCRAM auth with a raw unicode (SASLprep-mapped) password must succeed");
        assert_eq!(
            c.query_sql("SELECT current_user").await.expect("q").get(0).expect("row 0").get_raw(0),
            Ok(Some(b"bsql_test_scram".as_slice())),
        );
        c.close().await.expect("close");
    })
    .await;

    // Restore ALWAYS, even if the body above panicked.
    admin
        .execute_sql(&format!("ALTER ROLE bsql_test_scram PASSWORD '{ORIGINAL_PW}'"))
        .await
        .expect("restore the original password");
    admin.close().await.expect("admin close");

    // Re-raise a body panic now that the role is restored, so the test still fails
    // loudly if the SASLprep fix regresses.
    if let Err(join_err) = outcome {
        std::panic::resume_unwind(join_err.into_panic());
    }
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
    let pool = Pool::new(config, 5);
    let handles: Vec<_> = (0..100u32).map(|i| {
        let p = pool.clone();
        tokio::spawn(async move {
            let mut c = p.get().await.expect("get");
            let r = c.conn_mut().expect("live").query_sql(&format!("SELECT {i}::int, pg_backend_pid()")).await.expect("q");
            assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
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
        c.query_sql("SELECT count(*) FROM tx_demo").await.expect("count").get(0).expect("row 0").get_i64(0),
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
        c.query_sql("SELECT count(*) FROM tx_demo").await.expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(2)),
        "the failed transaction rolled back (row 3 is gone)"
    );
    c.close().await.expect("close");
}

/// The deferred-BEGIN FUSION correctness path, end-to-end over real PG on the
/// async driver: an EMPTY transaction is a true no-op (it arms no BEGIN and issues
/// no COMMIT), a transaction whose FIRST statement is the EXTENDED protocol
/// (`query_params`, one-round-trip) fuses BEGIN into that statement and commits its
/// effect, and a rollback of such a body discards it — proving the prelude drain
/// preserves the statement's result schema over the fused path.
#[tokio::test]
#[ignore = "requires local PG"]
async fn transaction_fusion_empty_and_extended() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE txf_async(v int)").await.expect("create");

    // (1) EMPTY body: a true no-op — no verb ran, so no BEGIN is armed and no
    // COMMIT/ROLLBACK is issued (zero round trips), leaving the connection clean.
    c.transaction(async |_conn| Ok(())).await.expect("empty tx is a clean no-op");
    assert!(c.is_healthy(), "healthy after an empty (no-op) transaction");
    c.execute_sql("INSERT INTO txf_async VALUES (7)").await.expect("post-empty insert");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM txf_async").await.expect("c").get(0).expect("row 0").get_i64(0),
        Ok(Some(1))
    );

    // (2) FIRST statement is the EXTENDED protocol: BEGIN fuses ahead of the
    // Parse+Bind+Describe+Execute batch and the statement's row decodes correctly.
    let fused = c
        .transaction(async |conn| {
            let r = conn.query_params_one("SELECT $1::int + 1 AS n", &(41i32,)).await?;
            let n = r.get_i32(0).expect("decode the fused statement's row");
            conn.execute_sql("INSERT INTO txf_async VALUES (8)").await?;
            Ok(n)
        })
        .await
        .expect("extended-first tx commits");
    assert_eq!(fused, Some(42), "the fused extended statement decoded correctly");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM txf_async").await.expect("c").get(0).expect("row 0").get_i64(0),
        Ok(Some(2)),
        "the committed insert persisted"
    );

    // (3) ROLLBACK of an extended-first body discards its effect.
    let result: Result<(), _> = c
        .transaction(async |conn| {
            drop(conn.query_params_one("SELECT $1::int", &(9i32,)).await?);
            conn.execute_sql("INSERT INTO txf_async VALUES (9)").await?;
            Err(bsql_postgres_async::DriverError::NoRows)
        })
        .await;
    assert!(result.is_err(), "a body error aborts the transaction");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM txf_async").await.expect("c").get(0).expect("row 0").get_i64(0),
        Ok(Some(2)),
        "the rolled-back insert did not persist"
    );
    c.close().await.expect("close");
}

/// COPY inside a transaction (via the borrowing guard) is a legal, ATOMIC bulk
/// load: the copied rows are visible to a query in the SAME transaction, persist
/// on COMMIT, and are gone on ROLLBACK — atomic bulk-load-with-rollback. Also
/// witnesses the deferred BEGIN fusing into a COPY that is the transaction's
/// FIRST statement.
#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_inside_transaction_commits_and_rolls_back() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cptx_async(v int)").await.expect("create");

    // COMMIT path: `copy_in_with` (scoped writer) is the tx's FIRST statement, so
    // the deferred BEGIN fuses into it; a query in the SAME tx sees the rows.
    let count = c
        .transaction(async |tx| {
            let n = tx
                .copy_in_with("cptx_async", async |w| {
                    w.write_row(b"1").await?;
                    w.write_row(b"2").await?;
                    w.write_row(b"3").await?;
                    Ok(())
                })
                .await?;
            assert_eq!(
                tx.query_sql("SELECT count(*) FROM cptx_async").await?.get(0).expect("row 0").get_i64(0),
                Ok(Some(3)),
                "the just-copied rows are visible inside the transaction"
            );
            Ok(n)
        })
        .await
        .expect("copy-in transaction commits");
    assert_eq!(count, 3, "COPY reported 3 loaded rows");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cptx_async").await.expect("q").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
        "the committed COPY rows persist"
    );

    // ROLLBACK path: `copy_in` more rows, then Err → the copied rows are discarded.
    let result: Result<(), _> = c
        .transaction(async |tx| {
            tx.copy_in("cptx_async", vec!["4", "5"]).await?;
            Err(DriverError::NoRows)
        })
        .await;
    assert!(result.is_err(), "the body error rolls the transaction back");
    assert_eq!(
        c.query_sql("SELECT count(*) FROM cptx_async").await.expect("q").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
        "the rolled-back COPY rows are NOT visible (still 3, not 5)"
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
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(4)));

    // Query with params
    let r = c.query_params("SELECT name FROM omni WHERE val > $1 ORDER BY val", &(15i32,)).await.expect("qp");
    assert_eq!(r.len(), 3);

    // Prepared
    let stmt = c.prepare("SELECT name, val FROM omni WHERE active = $1 ORDER BY val").await.expect("prep");
    let r = c.query_prepared(&stmt, &(true,)).await.expect("qprep");
    assert_eq!(r.len(), 3);
    c.close_statement(stmt).await.expect("close stmt");

    // Transaction (begin/commit)
    c.begin().await.expect("begin");
    c.execute_sql("UPDATE omni SET val = val * 2 WHERE active").await.expect("update");
    c.commit().await.expect("commit");
    let r = c.query_sql("SELECT SUM(val) FROM omni").await.expect("sum");
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(180)));

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
    let row = c.query_sql("SELECT 'final'::text").await.expect("q").get(0).expect("row 0");
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
        c.query_sql("SELECT count(*) FROM cp_rt").await.expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
    );
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in_large_chunk_passthrough() {
    // Exercises the LARGE-CHUNK PASSTHROUGH: one `write_chunk` whose body far
    // exceeds the 64 KiB batched-flush threshold is streamed DIRECTLY from the
    // borrowed slice (never buffered). Prove the direct-write path is byte-faithful
    // against real PG: every row lands and a spot-checked value is intact.
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute_sql("CREATE TEMP TABLE cp_big(id int8, payload text)")
        .await
        .expect("create");

    // Build ONE chunk of many text-COPY rows, well over the threshold (~180 KiB).
    const ROWS: i64 = 10_000;
    let mut chunk = String::new();
    for i in 0..ROWS {
        chunk.push_str(&format!("{i}\tpayload-row-{i}\n"));
    }
    assert!(chunk.len() > 64 * 1024, "the single chunk must exceed the threshold");

    let n = c
        .copy_in_with("cp_big", async |w| w.write_chunk(chunk.as_bytes()).await)
        .await
        .expect("large-chunk copy_in_with");
    assert_eq!(n, u64::try_from(ROWS).expect("ROWS fits u64"), "all rows ingested");

    assert_eq!(
        c.query_sql("SELECT count(*) FROM cp_big").await.expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(ROWS)),
    );
    // Spot-check a value survived the direct stream faithfully.
    assert_eq!(
        c.query_sql("SELECT payload FROM cp_big WHERE id = 9999").await.expect("val").get(0).expect("row 0")
            .get_str(0),
        Ok(Some("payload-row-9999")),
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
        c.query_sql("SELECT count(*) FROM cp_ab").await.expect("count").get(0).expect("row 0").get_i64(0),
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
        c.query_sql("SELECT 1::int4").await.expect("query").get(0).expect("row 0").get_i32(0),
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
        c.query_sql("SELECT count(*) FROM cp_brk").await.expect("count").get(0).expect("row 0").get_i64(0),
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
        c.query_sql("SELECT count(*) FROM cp_bulk").await.expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(N)),
    );
    c.close().await.expect("close");
}

// ═══════════════════════════════════════════════════════════
// Shared SQL scenario coverage over the ASYNC driver.
//
// The same `define_sql_scenario_tests!` library the blocking driver runs — the
// full SQL-mechanism suite (joins, CTEs, window functions, aggregates, string /
// type ops, the error zoo, extreme values, transactions, …). The scenario
// bodies are written in blocking shape, so the async driver runs them through a
// thin blocking shim: a per-connection current-thread runtime that drives each
// async verb to completion. This exercises the REAL async driver code — its
// `Transport` impl and its verbs — just synchronously, so one scenario set now
// genuinely covers both drivers.
// ═══════════════════════════════════════════════════════════
mod sql_scenarios {
    use bsql_postgres_async::{ConnectConfig, Connection, DriverError, QueryResult, SslMode};
    use bsql_postgres_proto::params::ParamsWriter;
    use tokio::runtime::Runtime;

    /// Blocking adapter over the async [`Connection`]. Its inherent methods match
    /// exactly the surface the scenario macro calls; each drives one async verb
    /// on `rt` to completion. Sequential top-level `block_on`s never nest.
    struct BlockingConn {
        inner: Connection,
        rt: Runtime,
    }

    /// The transaction body's view of the connection — the two verbs the scenarios
    /// use inside `transaction(|tx| …)`.
    struct BlockingTx<'a> {
        conn: &'a mut BlockingConn,
    }

    impl BlockingConn {
        fn execute_sql(&mut self, sql: &str) -> Result<u64, DriverError> {
            self.rt.block_on(self.inner.execute_sql(sql))
        }
        fn query_sql(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
            self.rt.block_on(self.inner.query_sql(sql))
        }
        fn query_params<P: ParamsWriter>(
            &mut self,
            sql: &str,
            params: &P,
        ) -> Result<QueryResult, DriverError> {
            self.rt.block_on(self.inner.query_params(sql, params))
        }
        fn execute_params<P: ParamsWriter>(
            &mut self,
            sql: &str,
            params: &P,
        ) -> Result<u64, DriverError> {
            self.rt.block_on(self.inner.execute_params(sql, params))
        }
        fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
            self.rt.block_on(self.inner.simple_query(sql))
        }
        fn ping(&mut self) -> Result<(), DriverError> {
            self.rt.block_on(self.inner.ping())
        }
        fn close(&mut self) -> Result<(), DriverError> {
            self.rt.block_on(self.inner.close())
        }

        /// The closure-scoped transaction, driven over the blocking shim: `BEGIN`,
        /// run the (blocking) body, then `COMMIT` on `Ok` / `ROLLBACK` on `Err`,
        /// always surfacing the body's original error. (The async `transaction`
        /// combinator — which takes an async closure — is covered directly by the
        /// `transaction_*` tests above; a blocking body cannot drive it without
        /// nesting `block_on`, so this reissues the same SQL over the async verbs.)
        fn transaction<T>(
            &mut self,
            f: impl FnOnce(&mut BlockingTx<'_>) -> Result<T, DriverError>,
        ) -> Result<T, DriverError> {
            self.simple_query("BEGIN")?;
            let outcome = {
                let mut tx = BlockingTx { conn: &mut *self };
                f(&mut tx)
            };
            match outcome {
                Ok(v) => {
                    self.simple_query("COMMIT")?;
                    Ok(v)
                }
                Err(e) => {
                    let _ = self.simple_query("ROLLBACK");
                    Err(e)
                }
            }
        }
    }

    impl BlockingTx<'_> {
        fn execute_sql(&mut self, sql: &str) -> Result<u64, DriverError> {
            self.conn.execute_sql(sql)
        }
        fn query_sql(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
            self.conn.query_sql(sql)
        }
    }

    // Not a `#[test]` fn, so the floor's `allow-expect-in-tests` carve-out
    // (keyed on `#[test]` context) does not reach it; the `expect`s are the
    // loud runtime-build / connect-failure signal a live test wants, never a
    // silent production fallback (there is no production path here).
    #[expect(
        clippy::expect_used,
        reason = "connection-fixture helper: panics loudly if the runtime cannot build or PG is unreachable — the intended live-test signal, and not a `#[test]` fn so the in-tests carve-out cannot reach it"
    )]
    fn make_async_blocking_conn() -> BlockingConn {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable);
        let inner = rt.block_on(Connection::connect(&cfg)).expect("connect");
        BlockingConn { inner, rt }
    }

    bsql_postgres_core::define_sql_scenario_tests!(make_async_blocking_conn);
}

/// WITNESS (review BLOCKER 2, async — a panicking sink neither aborts nor
/// poisons): a sink that PANICS on every event, threshold ZERO so `SlowQuery`
/// fires, over a `DO … RAISE NOTICE`. Both panics are contained by `catch_unwind`;
/// the test completing proves no abort, and `SELECT 42` proves no poisoning.
#[tokio::test]
#[ignore = "requires local PG"]
async fn panicking_sink_neither_aborts_nor_poisons_the_connection() {
    use std::time::Duration;

    use bsql_postgres_async::{DiagEvent, Diagnostics};

    let diag = Diagnostics::new()
        .slow_query_threshold(Duration::ZERO)
        .on_event(|_ev: &DiagEvent<'_>| panic!("boom — a deliberately buggy sink"));
    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect_with(&cfg, &diag).await.expect("connect_with");

    c.execute_sql("DO $$ BEGIN RAISE NOTICE 'x'; END $$")
        .await
        .expect("the DO completes despite the panicking sink");

    let row = c.query_one_sql("SELECT 42").await.expect("connection still usable, not NotReady");
    assert_eq!(row.get_i32(0), Ok(Some(42)));
    drop(c);
}

/// WITNESS (review MAJOR 4, async — uncontended checkout leaves
/// waiters_high_water at 0): a single checkout on a pool with a free slot never
/// blocks (the acquire fast-paths `try_acquire_owned`), so the gauge stays 0.
#[tokio::test]
#[ignore = "requires local PG"]
async fn uncontended_checkout_leaves_waiters_high_water_zero() {
    use bsql_postgres_async::Pool;

    let cfg = ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string());
    let pool = Pool::builder(cfg, 4).build();
    let c = pool.get().await.expect("uncontended checkout");
    assert_eq!(
        pool.stats().waiters_high_water,
        0,
        "an uncontended checkout must not register a blocked waiter",
    );
    drop(c);
}
