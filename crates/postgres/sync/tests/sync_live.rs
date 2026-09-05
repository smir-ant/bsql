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

/// A config over the local UNIX-DOMAIN socket: the host is the socket DIRECTORY
/// (`/tmp` — Homebrew PG's default on macOS), and libpq's rule turns it into the
/// socket file `<dir>/.s.PGSQL.<port>`. No `sslmode` override needed: a unix
/// socket is plaintext by construction.
fn unix_config() -> ConnectConfig {
    ConnectConfig::new("/tmp", "smir-ant").database("postgres".to_string())
}

// ═══════════════════════════════════════════════════════════
// Driver-specific tests (I/O, protocol, infra — not SQL)
// ═══════════════════════════════════════════════════════════

/// WITNESS (unix-domain transport): connect over the LOCAL UNIX SOCKET (host is
/// the socket dir `/tmp`), round-trip a query, and confirm the connection is
/// plaintext (`is_encrypted()` == false — TLS is not applicable to a local
/// socket). This is the transport the original bsql used and the bench baseline
/// assumed; it proves the new AF_UNIX path end-to-end on the blocking driver.
#[test]
#[ignore = "requires local PG on a unix socket"]
fn connect_over_unix_socket_and_query() {
    let mut c = Connection::connect(&unix_config()).expect("unix-socket connect");
    c.ping().expect("ping over unix socket");
    assert!(c.is_healthy());
    assert!(
        !c.is_encrypted(),
        "a unix-domain socket carries no TLS — is_encrypted() must be false"
    );
    assert!(c.backend_pid() > 0);
    // A real decode round-trip over the socket, not just a framing ping.
    let row = c
        .query_one_raw("SELECT 'bsql-over-unix'")
        .expect("query over unix socket");
    assert_eq!(row.get_str(0), Ok(Some("bsql-over-unix")));
    c.close().expect("close");
}

/// WITNESS (C1a — server NOTICE surfacing, blocking twin): a `RAISE NOTICE`
/// surfaces through the installed diagnostics sink with its severity + message,
/// instead of being silently dropped. The blocking mirror of the async witness.
#[test]
#[ignore = "requires local PG"]
fn raise_notice_surfaces_through_the_diagnostics_sink() {
    use std::sync::{Arc, Mutex};

    use bsql_postgres_sync::{DiagEvent, Diagnostics};

    let captured: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_in = Arc::clone(&captured);
    let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
        if let DiagEvent::ServerNotice { severity, message, .. } = ev {
            captured_in
                .lock()
                .expect("diag lock")
                .push((severity.to_string(), message.to_string()));
        }
    });

    let mut c = Connection::connect_with(&unix_config(), &diag).expect("connect_with");
    c.execute_raw("DO $$ BEGIN RAISE NOTICE 'hello from bsql notice'; END $$")
        .expect("DO with RAISE NOTICE");

    let got = captured.lock().expect("diag lock").clone();
    assert!(
        got.iter()
            .any(|(sev, msg)| sev == "NOTICE" && msg == "hello from bsql notice"),
        "the RAISE NOTICE must surface through the sink, got {got:?}",
    );
    // The connection stays fully usable after surfacing the notice.
    let row = c.query_one_raw("SELECT 42").expect("query after notice");
    assert_eq!(row.get_i32(0), Ok(Some(42)));
    drop(c); // cleanup only; the witness assertions ran above
}

/// WITNESS (C1b — SSL downgrade routing, blocking twin): a TCP connect with
/// `SslMode::Prefer` to a server that refuses TLS falls back to plaintext AND
/// routes the downgrade through the installed sink as `DiagEvent::SslDowngrade`.
#[test]
#[ignore = "requires local PG with ssl=off on TCP"]
fn ssl_prefer_downgrade_routes_through_the_diagnostics_sink() {
    use std::sync::{Arc, Mutex};

    use bsql_postgres_sync::{DiagEvent, Diagnostics, SslMode};

    let downgrades: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let downgrades_in = Arc::clone(&downgrades);
    let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
        if let DiagEvent::SslDowngrade { host } = ev {
            downgrades_in.lock().expect("diag lock").push((*host).to_string());
        }
    });

    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Prefer);
    let c = Connection::connect_with(&cfg, &diag).expect("connect_with over TCP");
    assert!(!c.is_encrypted(), "the server refused TLS — the connection is plaintext");

    let got = downgrades.lock().expect("diag lock").clone();
    assert_eq!(
        got.as_slice(),
        &["127.0.0.1".to_string()],
        "the SSL downgrade must route through the sink with the host, got {got:?}",
    );
    drop(c); // cleanup only; the witness assertions ran above
}

/// WITNESS (C1c — pool saturation, blocking twin): a max-size-1 pool with one
/// connection held times out the second checkout AND surfaces a
/// `DiagEvent::PoolAcquireTimeout`, with the counter + waiter high-water recorded
/// in `Pool::stats()`.
#[test]
#[ignore = "requires local PG"]
fn pool_acquire_timeout_emits_and_counts() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bsql_postgres_sync::{DiagEvent, DriverError, Pool};

    let timeouts: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let timeouts_in = Arc::clone(&timeouts);
    let pool = Pool::builder(unix_config(), 1)
        .acquire_timeout(Duration::from_millis(150))
        .on_diagnostic(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::PoolAcquireTimeout { .. } = ev {
                *timeouts_in.lock().expect("diag lock") += 1;
            }
        })
        .build();

    let held = pool.get().expect("first checkout");
    match pool.get() {
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

/// THE CROSS-USER WRONG-RESULT REGRESSION (BLOCKER, sync twin): a pooled connection
/// keeps its dynamic prepared-statement cache warm across a checkout. User 1
/// PROMOTES a cached plan for an UNQUALIFIED name against PERMANENT
/// `public.pl_shadow`; User 2 reuses the SAME connection (pool size 1) and creates a
/// `TEMP TABLE pl_shadow` shadowing it. The identical query MUST return User 2's
/// TEMP data — never the permanent row. The reset's `DISCARD PLANS` forces the kept
/// plan to re-resolve. See the async twin for the full narrative.
#[test]
#[ignore = "requires local PG"]
fn pooled_dynamic_plan_re_resolves_a_temp_shadow_across_users() {
    use bsql_postgres_sync::Pool;

    let cfg = sync_config();

    {
        let mut setup = Connection::connect(&cfg).expect("setup connect");
        setup.execute_raw("DROP TABLE IF EXISTS public.pl_shadow").expect("drop");
        setup
            .execute_raw("CREATE TABLE public.pl_shadow (id int4 PRIMARY KEY, val text NOT NULL)")
            .expect("create permanent");
        setup
            .execute_raw("INSERT INTO public.pl_shadow (id, val) VALUES (1, 'PERMANENT')")
            .expect("seed permanent");
        setup.close().expect("setup close");
    }

    let pool = Pool::new(cfg.clone(), 1);
    const SQL: &str = "SELECT val FROM pl_shadow WHERE id = $1";

    // User 1: pre-activate pg_temp, then drive PG to a GENERIC plan bound to public
    // (see the async twin for the two reproduction conditions).
    {
        let mut g = pool.get().expect("user1 checkout");
        let c = g.conn_mut().expect("user1 conn");
        c.execute_raw("CREATE TEMP TABLE _pgtemp_activate (x int4)")
            .expect("activate pg_temp for the connection's lifetime");
        for _ in 0..12 {
            let row = c
                .query_params_opt(SQL, &(1i32,))
                .expect("user1 query")
                .expect("user1 row");
            assert_eq!(row.get_str(0).expect("decode").unwrap_or(""), "PERMANENT");
        }
    }

    // User 2: the SAME connection — shadow the name and re-run the cached query.
    {
        let mut g = pool.get().expect("user2 checkout");
        let c = g.conn_mut().expect("user2 conn");
        c.execute_raw("CREATE TEMP TABLE pl_shadow (id int4 PRIMARY KEY, val text NOT NULL)")
            .expect("user2 temp table");
        c.execute_raw("INSERT INTO pl_shadow (id, val) VALUES (1, 'TEMP-USER-2')")
            .expect("user2 temp seed");
        let row = c
            .query_params_opt(SQL, &(1i32,))
            .expect("user2 query")
            .expect("user2 row");
        assert_eq!(
            row.get_str(0).expect("decode").unwrap_or(""),
            "TEMP-USER-2",
            "user 2 MUST read their OWN temp table — reading 'PERMANENT' is the cross-user leak",
        );
    }

    {
        let mut cleanup = Connection::connect(&cfg).expect("cleanup connect");
        cleanup.execute_raw("DROP TABLE IF EXISTS public.pl_shadow").expect("cleanup drop");
        cleanup.close().expect("cleanup close");
    }
}

/// WITNESS (C1d — slow-query detection, blocking twin): with a threshold set, a
/// slow query emits `DiagEvent::SlowQuery` with the SQL text; a fast one emits
/// nothing.
#[test]
#[ignore = "requires local PG"]
fn slow_query_emits_with_the_threshold_set() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bsql_postgres_sync::{DiagEvent, Diagnostics};

    let slow: Arc<Mutex<Vec<(String, Duration)>>> = Arc::new(Mutex::new(Vec::new()));
    let slow_in = Arc::clone(&slow);
    let diag = Diagnostics::new()
        .slow_query_threshold(Duration::from_millis(50))
        .on_event(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::SlowQuery { sql, elapsed } = ev {
                slow_in.lock().expect("diag lock").push(((*sql).to_string(), *elapsed));
            }
        });
    let mut c = Connection::connect_with(&unix_config(), &diag).expect("connect_with");

    let _row = c.query_one_raw("SELECT 1").expect("fast query");
    assert!(slow.lock().expect("diag lock").is_empty(), "a fast query is not reported slow");

    let _qr = c.query_raw("SELECT pg_sleep(0.2)").expect("slow query");
    let got = slow.lock().expect("diag lock").clone();
    assert_eq!(got.len(), 1, "the slow query emitted once, got {got:?}");
    assert!(got[0].0.contains("pg_sleep"), "the event carries the SQL text, got {:?}", got[0].0);
    assert!(got[0].1 >= Duration::from_millis(50), "elapsed >= threshold, got {:?}", got[0].1);
    drop(c); // cleanup only; the witness assertions ran above
}

// The `SslMode::Require`-over-unix rejection is now a single shared
// `core::config` helper (`Endpoint::reject_unix_tls_required`) that BOTH drivers
// call from their `#[cfg(unix)]` dial path, so async/sync parity is structural.
// Its former per-driver live twin here (and the async twin) is replaced by ONE
// offline unit test on the helper — `bsql-postgres-core`'s
// `reject_unix_tls_required_is_a_loud_config_error_only_for_unix_plus_require`.

/// WITNESS (query cancellation, blocking driver): start a long
/// `SELECT pg_sleep(5)`, then from ANOTHER thread send an out-of-band cancel via
/// a `CancelToken` obtained BEFORE the query. The query must return SQLSTATE
/// `57014` (`query_canceled`) WELL under the 5-second sleep, and the connection
/// must be left drained + reusable. The blocking twin of the async witness. The
/// loopback default SslMode (`Prefer`) makes the cancel socket re-run the
/// `SSLRequest` probe, proving the redial honors the original TLS decision.
#[test]
#[ignore = "requires local PG"]
fn cancel_token_stops_an_inflight_query_sync() {
    // Loopback with the DEFAULT SslMode (Prefer) so the cancel socket re-runs the
    // SSLRequest probe (the redial honors the original TLS decision).
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut conn = Connection::connect(&cfg).expect("connect");
    // The token is obtained BEFORE the long query and borrows nothing from `conn`.
    let token = conn.cancel_token();
    assert!(token.backend_pid() > 0, "the token names the backend to cancel");
    // From another THREAD, cancel ~300 ms in — long after pg_sleep(5) has started
    // server-side, long before it would finish.
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(300));
        token.cancel()
    });
    let start = std::time::Instant::now();
    let outcome = conn.query_raw("SELECT pg_sleep(5)");
    let elapsed = start.elapsed();
    canceller
        .join()
        .expect("cancel thread join")
        .expect("cancel packet delivered");
    match outcome {
        Err(bsql_postgres_sync::DriverError::Db(db)) => assert!(
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
    // A canceled query is a RECOVERABLE server error: the connection stays healthy
    // and reusable after the verb drains the ErrorResponse + ReadyForQuery.
    assert!(
        conn.is_healthy(),
        "the connection must be drained + reusable after a cancel"
    );
    let row = conn
        .query_one_raw("SELECT 1")
        .expect("connection reusable after cancel");
    assert_eq!(row.get_str(0), Ok(Some("1")));
    conn.close().expect("close");
}

/// WITNESS (C5 — `is_disconnect`): a connection whose backend is TERMINATED
/// mid-flight fails its in-flight query with an error that
/// `DriverError::is_disconnect()` classifies TRUE (the "reconnect" signal),
/// while a plain SYNTAX error on a healthy connection classifies FALSE.
#[test]
#[ignore = "requires local PG"]
fn is_disconnect_true_on_terminated_backend_false_on_syntax_error_sync() {
    use std::time::Duration;

    let mut victim = Connection::connect(&unix_config()).expect("connect victim");
    let killer = Connection::connect(&unix_config()).expect("connect killer");
    let pid = victim.backend_pid();
    assert!(pid > 0, "backend pid must be captured from the handshake");

    // Terminate the victim MID-FLIGHT from a background thread while the main
    // thread blocks in a 3s sleep. The killer connection is returned so the
    // syntax-error half can reuse it.
    let terminator = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        let mut killer = killer;
        let terminated = killer
            .query_one_raw(&format!("SELECT pg_terminate_backend({pid})"))
            .expect("terminate the victim backend");
        assert_eq!(terminated.get_str(0), Ok(Some("t")), "pg_terminate_backend returned true");
        killer
    });
    let victim_res = victim.query_one_raw("SELECT pg_sleep(3)");
    let mut killer = terminator.join().expect("terminator thread joins");

    let disconnect_err = match victim_res {
        Err(e) => e,
        Ok(_) => panic!("a terminated backend must fail the in-flight query"),
    };
    assert!(
        disconnect_err.is_disconnect(),
        "a terminated connection must classify as a disconnect, got {disconnect_err:?}",
    );

    let syntax_err = match killer.query_one_raw("SELECT bogus not valid sql !!") {
        Err(e) => e,
        Ok(_) => panic!("a syntax error must fail"),
    };
    assert!(
        !syntax_err.is_disconnect(),
        "a syntax error is not a disconnect (the connection is fine), got {syntax_err:?}",
    );
    let row = killer.query_one_raw("SELECT 1").expect("healthy after a syntax error");
    assert_eq!(row.get_str(0), Ok(Some("1")));
    killer.close().expect("close killer");
}

/// WITNESS (C6 — `statement_timeout`): a connection built with
/// `with_statement_timeout(200ms)` has the SERVER abort a runaway query with
/// SQLSTATE `57014`; the cancel is NOT a disconnect, so the connection RECOVERS.
/// A connection WITHOUT the timeout runs the same sleep to completion.
#[test]
#[ignore = "requires local PG"]
fn statement_timeout_aborts_a_runaway_query_and_the_connection_recovers_sync() {
    use std::time::Duration;

    let cfg = unix_config().with_statement_timeout(Duration::from_millis(200));
    let mut c = Connection::connect(&cfg).expect("connect with statement_timeout");

    let err = match c.query_one_raw("SELECT pg_sleep(2)") {
        Err(e) => e,
        Ok(_) => panic!("pg_sleep(2) must be aborted by statement_timeout=200ms"),
    };
    match &err {
        bsql_postgres_sync::DriverError::Db(db) => assert!(
            db.is_code("57014"),
            "statement_timeout must abort with 57014 query_canceled, got {}",
            db.code(),
        ),
        other => panic!("statement_timeout must surface as DriverError::Db(57014), got {other:?}"),
    }
    assert!(!err.is_disconnect(), "a statement_timeout cancel is not a disconnect");
    let row = c.query_one_raw("SELECT 1").expect("connection reusable after statement_timeout");
    assert_eq!(row.get_str(0), Ok(Some("1")));
    c.close().expect("close");

    // WITHOUT the timeout, the same-shape sleep runs to completion.
    let mut c2 = Connection::connect(&unix_config()).expect("connect without statement_timeout");
    let done = c2.query_one_raw("SELECT pg_sleep(0.3)").expect("no timeout — sleep completes");
    assert!(done.get_str(0).is_ok(), "the completed pg_sleep row is readable (void)");
    c2.close().expect("close");
}

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

/// WITNESS (R5 — a connect-time server error is CLASSIFIED): a connect to a
/// NON-EXISTENT database surfaces the server's `ErrorResponse` as a fully
/// classified `DriverError::Db` — SQLSTATE `3D000` (`invalid_catalog_name`) plus
/// the server's message — NOT a single opaque I/O string. `err.code()` /
/// `is_invalid_catalog_name()` match a CONNECT error exactly as an active one.
#[test]
#[ignore = "requires local PG"]
fn connect_to_missing_database_classifies_3d000() {
    let cfg =
        ConnectConfig::new("127.0.0.1", "smir-ant").database("bsql_r5_no_such_db".to_string());
    match Connection::connect(&cfg) {
        Err(bsql_postgres_sync::DriverError::Db(db)) => {
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
/// with an auth SQLSTATE in the `28xxx` class — the same classified `DbError` the
/// active path produces, decoded through the same `parse_error_response`.
#[test]
#[ignore = "requires local PG"]
fn connect_as_missing_role_classifies_auth_error() {
    let cfg =
        ConnectConfig::new("127.0.0.1", "bsql_r5_no_such_role").database("postgres".to_string());
    match Connection::connect(&cfg) {
        Err(bsql_postgres_sync::DriverError::Db(db)) => assert!(
            db.code().starts_with("28"),
            "a bad-authorization connect must classify in the 28xxx class, got {}",
            db.code(),
        ),
        Ok(_) => panic!("a connect as a non-existent role must fail"),
        Err(other) => panic!("expected DriverError::Db(28xxx), got {other:?}"),
    }
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
        .query_raw("SELECT pg_sleep(3)")
        .expect("a query slower than connect_timeout must succeed, not kill the connection");
    assert_eq!(slept.len(), 1, "pg_sleep returns exactly one (void) row");
    assert!(c.is_healthy(), "connection stays healthy after a slow query");

    // And it stays usable: a second query round-trips on the same connection.
    let again = c
        .query_one_raw("SELECT 'still-usable'")
        .expect("second query on the same connection after the slow one");
    assert_eq!(again.get_str(0), Ok(Some("still-usable")));
    c.close().expect("close");
}

/// WITNESS (SSL-probe timeout class parity): a server that ACCEPTS the TCP
/// connection but stays silent on the `SSLRequest` probe byte must make sync
/// `connect` report `DriverError::Timeout` — the SAME class the async driver's
/// connect budget and the post-probe TLS handshake use for a connect-phase
/// timeout — NOT the generic `Io(TimedOut)` a bare `?` on the raw probe
/// `read_exact` would yield. Closes a cross-driver class divergence: both fail
/// fast, but must report the same class. Deterministic + PG-free (a raw loopback
/// listener that accepts then stays silent), so it runs in the default suite —
/// no `#[ignore]`.
///
/// The non-timeout case (a connection reset on the probe) is NOT remapped: the
/// classifier's `_ => DriverError::Io(e)` arm keeps every other io error at its
/// real class, so only a read/write DEADLINE becomes `Timeout`.
#[test]
fn ssl_probe_timeout_is_classified_timeout_matching_async() {
    // A raw TCP listener that accepts then never answers the SSLRequest probe.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener addr");
    let accepter = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            // Read (discard) the client's SSLRequest bytes but NEVER reply; hold
            // until the client gives up and closes (read → 0), so this thread
            // exits promptly once the client times out.
            let mut sink = [0u8; 64];
            loop {
                match std::io::Read::read(&mut sock, &mut sink) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        }
    });

    // SSL wanted (the SSLRequest probe path — NOT ssl_mode(Disable)) with a 1s
    // connect budget, which arms the probe socket's read deadline.
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .port(addr.port())
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Prefer)
        .connect_timeout(1);

    let start = std::time::Instant::now();
    let result = Connection::connect(&cfg);
    let elapsed = start.elapsed();

    // `Connection` is not Debug — classify by matching, not Debug-printing the Ok.
    match result {
        Err(bsql_postgres_sync::DriverError::Timeout) => {}
        Err(other) => panic!(
            "an SSL-probe timeout must be DriverError::Timeout (async parity); got {other:?} after {elapsed:?}"
        ),
        Ok(_) => panic!("a silent server must time out, not connect; got Ok after {elapsed:?}"),
    }
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
#[test]
#[ignore = "requires local PG"]
fn startup_params_take_effect() {
    let schema = format!("bsql_s48_sync_{}", std::process::id());

    // A plain connection (no startup params) provisions the isolated schema.
    let mut admin = Connection::connect(&sync_config()).expect("admin connect");
    admin
        .execute_raw(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop stale schema");
    admin
        .execute_raw(&format!("CREATE SCHEMA {schema}"))
        .expect("create schema");
    admin.close().expect("close admin");

    let cfg = sync_config()
        .with_search_path(&schema)
        .with_application_name("bsql_test")
        .with_startup_param("statement_timeout", "5000");
    let mut c = Connection::connect(&cfg).expect("connect with startup params");

    // 1) search_path took effect on the session.
    let sp = c.query_one_raw("SHOW search_path").expect("SHOW search_path");
    assert_eq!(
        sp.get_str(0),
        Ok(Some(schema.as_str())),
        "connect-time search_path must be the session search_path",
    );

    // 2) application_name took effect.
    let an = c
        .query_one_raw("SELECT current_setting('application_name')")
        .expect("current_setting(application_name)");
    assert_eq!(an.get_str(0), Ok(Some("bsql_test")));

    // 3) statement_timeout took effect (PG normalises 5000 ms to "5s").
    let st = c
        .query_one_raw("SHOW statement_timeout")
        .expect("SHOW statement_timeout");
    assert_eq!(st.get_str(0), Ok(Some("5s")));

    // 4) The isolation primitive: an UNQUALIFIED table resolves into the
    //    connect-time search_path schema, not the default.
    c.execute_raw("CREATE TABLE s48_probe (id int)")
        .expect("create unqualified table");
    c.execute_raw("INSERT INTO s48_probe VALUES (1)")
        .expect("insert into unqualified table");
    let located = c
        .query_one_raw("SELECT schemaname FROM pg_tables WHERE tablename = 's48_probe'")
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
        .query_one_raw("SHOW search_path")
        .expect("SHOW search_path after reset");
    assert_eq!(
        sp2.get_str(0),
        Ok(Some(schema.as_str())),
        "connect-time search_path must survive RESET ALL",
    );

    c.execute_raw(&format!("DROP SCHEMA {schema} CASCADE"))
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

    // WITNESS: `QueryResult::affected()` surfaces the affected-row count on the
    // dynamic `query_params` result — the capability the `Copy` `CommandTag`
    // closed. A non-RETURNING UPDATE yields zero rows but a non-zero count.
    c.execute_raw("CREATE TEMP TABLE m1_aff (id int)").expect("temp table");
    let inserted = c
        .execute_raw("INSERT INTO m1_aff VALUES (1), (2), (3)")
        .expect("seed rows");
    assert_eq!(inserted, 3, "execute_raw reports the INSERT count");
    let upd = c
        .query_params("UPDATE m1_aff SET id = id + 10 WHERE id >= $1", &(2_i32,))
        .expect("parameterized UPDATE");
    assert_eq!(upd.affected(), 2, "query_params result exposes the UPDATE affected count");
    assert_eq!(upd.len(), 0, "a non-RETURNING UPDATE yields no rows");
    let sel = c
        .query_params("SELECT id FROM m1_aff WHERE id >= $1", &(1_i32,))
        .expect("parameterized SELECT");
    assert_eq!(sel.affected(), 3, "a SELECT's affected() is its returned row count");
    assert_eq!(sel.len(), 3);

    c.close().expect("close");
}

/// WITNESS (D1 — dynamic-param TYPE FIDELITY, blocking twin): the dynamic
/// `query_params` family declares each parameter's ENCODED type OID in its
/// `Parse`, so a Rust value whose type disagrees with the SQL-inferred type is a
/// LOUD classified server error — never a silent binary reinterpretation.
///
/// The exact repro: a table with a row at `id = 1094795585` (`0x41414141`, the
/// int4 the four ASCII bytes of `"AAAA"` reinterpret to). Before the fix, binding
/// the `&str "AAAA"` against the `int4` column `id = $1` SILENTLY matched that
/// row (server inferred `$1 = int4`, read the text bytes as int4). After the fix,
/// `$1` is declared `text`, so `int4 = text` has no operator — a classified `Db`
/// error, and the connection RECOVERS. A correctly-typed and a coercible param
/// still round-trip; the cached (promoted) plan preserves the fidelity.
#[test]
#[ignore = "requires local PG"]
fn dynamic_param_type_fidelity_sync() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE d1_tf (id int4 PRIMARY KEY, big int8, name text)")
        .expect("temp table");
    c.execute_raw("INSERT INTO d1_tf VALUES (1094795585, 999, 'target'), (1, 100, 'one')")
        .expect("seed rows");

    // Correctly-typed param round-trips (the happy path is unregressed).
    let row = c
        .query_params_one("SELECT name FROM d1_tf WHERE id = $1", &(1_i32,))
        .expect("correctly-typed int4 param round-trips");
    assert_eq!(row.get_str(0), Ok(Some("one")));

    // THE REPRO (distinct SQL so no cache interaction): a `&str` bound against the
    // int4 `id = $1`. This MUST be a classified server error, NOT the silent match
    // of the `id = 1094795585` row (the four ASCII bytes reinterpreted as int4).
    let err = c
        .query_params_one("SELECT id FROM d1_tf WHERE id = $1", &("AAAA",))
        .expect_err("a &str bound against an int4 column must be a LOUD type error");
    match err {
        bsql_postgres_sync::DriverError::Db(db) => assert_eq!(
            db.code(),
            "42883",
            "int4 = text has no operator — expected 42883, got {}",
            db.code()
        ),
        other => panic!("wrong-typed dynamic param must be DriverError::Db(42883), got {other:?}"),
    }
    // The connection RECOVERS from the classified error (drained to idle).
    let recovered = c
        .query_params_one("SELECT id FROM d1_tf WHERE id = $1 AND true", &(1_i32,))
        .expect("connection recovers after the classified type error");
    assert_eq!(recovered.get_i32(0), Ok(Some(1)));

    // The explicit-cast form: `$1::int4` with `"AAAA"` → the text VALUE is cast,
    // so it is `invalid input syntax for integer` (22P02), not the byte reinterpret.
    let cast_err = c
        .query_params_one("SELECT $1::int4 AS v", &("AAAA",))
        .expect_err("casting the text 'AAAA' to int4 must be a loud parse error");
    match cast_err {
        bsql_postgres_sync::DriverError::Db(db) => assert_eq!(
            db.code(),
            "22P02",
            "text 'AAAA'::int4 is invalid_text_representation — expected 22P02, got {}",
            db.code()
        ),
        other => panic!("text::int4 must be DriverError::Db(22P02), got {other:?}"),
    }

    // A COERCIBLE param is NOT over-rejected: an int8 value against an int4 `id`
    // comparison is coerced by PG's cross-type operator (distinct SQL, first
    // sighting), returning that row's `big`.
    let coerced = c
        .query_params_one("SELECT big FROM d1_tf WHERE id = $1", &(1_i64,))
        .expect("int8 param coerces into the int4 comparison");
    assert_eq!(coerced.get_i64(0), Ok(Some(100)));

    // The CACHED (promoted) plan preserves fidelity: run a correct query 3× so it
    // promotes to a named statement (second sighting) whose Parse also declares
    // `P::OIDS`, then reuses it — all correct.
    for _ in 0..3 {
        let r = c
            .query_params_one("SELECT id FROM d1_tf WHERE name = $1", &("one",))
            .expect("cached-plan query round-trips");
        assert_eq!(r.get_i32(0), Ok(Some(1)));
    }

    // CACHE TYPE-FIDELITY: the SAME SQL text bound with a DIFFERENT param type is a
    // DISTINCT cache key, so a `float4` sighting of the just-cached `text` query is
    // NOT reused against the text plan (which would reinterpret the 4 bytes). It is
    // its own fresh plan — `text = float4` has no operator, a LOUD error, never a
    // silent match. (The cache is keyed on (SQL, P::OIDS).)
    let cross_type = c.query_params_one("SELECT id FROM d1_tf WHERE name = $1", &(1.0_f32,));
    match cross_type {
        Err(bsql_postgres_sync::DriverError::Db(_)) => {}
        other => panic!("a float4 reuse of a text-cached SQL must be a loud Db error, got {other:?}"),
    }
    // …and the original text-typed cached plan still works (its slot is intact).
    let still_cached = c
        .query_params_one("SELECT id FROM d1_tf WHERE name = $1", &("one",))
        .expect("the text-typed cached plan survives the distinct-key float4 sighting");
    assert_eq!(still_cached.get_i32(0), Ok(Some(1)));

    // `execute_params` shares the fused Parse: a wrong-typed bind is loud there too.
    let exec_err = c
        .execute_params("UPDATE d1_tf SET name = 'x' WHERE id = $1", &("AAAA",))
        .expect_err("execute_params must reject a &str bound against int4");
    assert!(matches!(exec_err, bsql_postgres_sync::DriverError::Db(_)));
    let _ = c
        .query_params_one("SELECT id FROM d1_tf WHERE id = $1 AND 1=1", &(1_i32,))
        .expect("recovers");

    c.close().expect("close");
}

/// MAJOR-1: the EXPLICIT prepared-statement path is type-faithful. A prepared
/// statement has a FIXED plan (its `$N` types are pinned at Parse), so the server
/// cannot coerce a differently-typed binary bind against it — a same-width
/// wrong-typed bind would be silently reinterpreted. The driver retains the
/// server-inferred parameter types and VERIFIES the caller's encoded types before
/// the Bind, so a mismatch is a LOUD client-side `ParamTypeMismatch`, never the
/// silent `id = 1094795585` match.
#[test]
#[ignore = "requires local PG"]
fn prepared_param_type_fidelity_sync() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE pf_tf (id int4 PRIMARY KEY, name text)")
        .expect("temp table");
    c.execute_raw("INSERT INTO pf_tf VALUES (1094795585, 'target'), (1, 'one')")
        .expect("seed rows");

    let stmt = c
        .prepare("SELECT name FROM pf_tf WHERE id = $1")
        .expect("prepare");

    // Correctly-typed param round-trips (happy path unregressed).
    let ok = c.query_prepared(&stmt, &(1_i32,)).expect("correct int4 binds");
    let ok_row = ok.get(0).expect("one row");
    assert_eq!(ok_row.get_str(0), Ok(Some("one")));

    // THE REPRO: a `&str` bound against the int4 `$1`. Was `id = 1094795585`
    // (the four ASCII bytes reinterpreted as int4); now a client-side reject
    // BEFORE the Bind — no server round trip, connection untouched.
    let err = c
        .query_prepared(&stmt, &("AAAA",))
        .expect_err("a &str bound to an int4 prepared param must be a LOUD reject");
    match err {
        bsql_postgres_sync::DriverError::ParamTypeMismatch { index, expected, found } => {
            assert_eq!(index, 0, "the first ($1) parameter");
            assert_eq!(expected, 23, "server inferred int4 (OID 23)");
            assert_eq!(found, 25, "the client bound text (OID 25)");
        }
        other => panic!("expected ParamTypeMismatch, got {other:?}"),
    }

    // The connection is UNTOUCHED by the client-side reject (no Bind was sent):
    // the SAME statement runs correctly immediately after.
    let after = c.query_prepared(&stmt, &(1_i32,)).expect("stmt still usable");
    let after_row = after.get(0).expect("one row");
    assert_eq!(after_row.get_str(0), Ok(Some("one")));

    // Arity mismatch is caught client-side too (the tuple supplies the wrong
    // number of params for the statement's one placeholder).
    let arity = c
        .query_prepared(&stmt, &(1_i32, 2_i32))
        .expect_err("2 params for a 1-param statement must be a LOUD reject");
    assert!(
        matches!(
            arity,
            bsql_postgres_sync::DriverError::ParamCountMismatch { expected: 1, found: 2 }
        ),
        "expected ParamCountMismatch {{1,2}}, got {arity:?}"
    );

    // `execute_prepared` verifies identically.
    let dml = c
        .prepare("UPDATE pf_tf SET name = 'x' WHERE id = $1")
        .expect("prepare dml");
    let dml_err = c
        .execute_prepared(&dml, &("AAAA",))
        .expect_err("execute_prepared rejects a &str bound to int4");
    assert!(matches!(
        dml_err,
        bsql_postgres_sync::DriverError::ParamTypeMismatch { .. }
    ));

    // A COERCIBLE type is STRICT-rejected on the fixed-plan path (unlike the
    // dynamic path where PG coerces): the plan pinned int4, an int8 bind cannot
    // reinterpret against it, so the client rejects it with a clear message
    // rather than letting the server misread the 8 bytes.
    let coerce = c
        .query_prepared(&stmt, &(1_i64,))
        .expect_err("int8 against a fixed int4 plan is strict-rejected client-side");
    assert!(matches!(
        coerce,
        bsql_postgres_sync::DriverError::ParamTypeMismatch { expected: 23, found: 20, .. }
    ));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_1k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_raw("SELECT generate_series(1, 1000)").expect("query");
    assert_eq!(r.len(), 1000);
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(r.get(999).expect("row 999").get_i32(0), Ok(Some(1000)));
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
    let r = c.query_raw("SELECT NULL::int4").expect("null query");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(None));
    assert!(r.get(0).expect("row 0").is_null(0));

    // (2) An `i32` read of genuinely non-numeric text ('x') is a classified `Err`
    // over the real wire — exactly the failure the retired `.parse().ok()` hid as
    // a silent `None`. Assert the EXACT classified variant, not `.is_err()`.
    let r = c.query_raw("SELECT 'x'::text").expect("text query");
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

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_10k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_raw("SELECT generate_series(1, 10000)").expect("query");
    assert_eq!(r.len(), 10000);
    assert_eq!(r.get(9999).expect("row 9999").get_i32(0), Ok(Some(10000)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn error_recovery_and_resilience() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // 4 different error types, all recover
    assert!(c.simple_query("SELCT").is_err());
    assert!(c.query_raw("SELECT * FROM nonexistent_xyz").is_err());
    assert!(c.query_raw("SELECT 'abc'::int").is_err());
    assert!(c.query_raw("SELECT 1/0").is_err());
    c.ping().expect("ping after 4 errors");
    // Full CRUD still works
    c.execute_raw("CREATE TEMP TABLE resilience(v int)").expect("create");
    c.execute_raw("INSERT INTO resilience VALUES (42)").expect("insert");
    assert_eq!(c.query_raw("SELECT v FROM resilience").expect("select").get(0).expect("row 0").get_i32(0), Ok(Some(42)));
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
    assert_eq!(c.query_raw("SELECT 5::int4").expect("query").get(0).expect("row 0").get_i32(0), Ok(Some(5)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn client_encoding_pinned_to_utf8_and_roundtrips_non_ascii() {
    // The startup message forces client_encoding=UTF8 so the driver's UTF-8
    // TEXT decode is correct regardless of the server's default encoding.
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let enc = c.query_raw("SHOW client_encoding").expect("show").get(0).expect("row 0")
        .get_str(0)
        .expect("client_encoding decodes")
        .map(String::from);
    assert_eq!(enc.as_deref(), Some("UTF8"), "startup must pin client_encoding=UTF8");

    // Non-ASCII (Cyrillic + emoji) round-trips byte-exact under the pinned UTF-8.
    let text = "Привет, мир 🌍";
    let r = c.query_raw(&format!("SELECT '{text}'::text")).expect("query");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some(text)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn mixed_width_multi_statement_is_rejected_not_misaddressed() {
    // A single simple-query batch whose statements return DIFFERENT column
    // counts cannot be one fixed-stride result set without mis-addressing
    // cells. It must be a loud MixedResultWidth error, never silent wrong data.
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let mixed = c.query_raw("SELECT 1::int4; SELECT 'a'::text, 'b'::text");
    assert!(
        matches!(mixed, Err(bsql_postgres_sync::DriverError::MixedResultWidth)),
        "mixed-width batch must be rejected as MixedResultWidth, got {mixed:?}",
    );
    // The protocol completed cleanly (both statements ran server-side); only the
    // client-side result shape is rejected, so the connection stays reusable.
    assert!(c.is_healthy(), "connection stays healthy after a rejected result shape");
    assert_eq!(
        c.query_raw("SELECT 7::int4").expect("follow-up query works").get(0).expect("row 0").get_i32(0),
        Ok(Some(7)),
    );

    // A UNIFORM-width multi-statement batch is fine.
    let uniform = c.query_raw("SELECT 1::int4; SELECT 2::int4").expect("uniform batch");
    assert_eq!(uniform.len(), 2);
    assert_eq!(uniform.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(uniform.get(1).expect("row 1").get_i32(0), Ok(Some(2)));
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
    c.execute_raw("CREATE TEMP TABLE cp_inj(id int4)").expect("create");

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
        c.query_raw("SELECT count(*) FROM cp_inj").expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
    );
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn prepared_reuse_after_constraint_violation() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE pr_err(id int PRIMARY KEY)").expect("create");
    let stmt = c.prepare("INSERT INTO pr_err VALUES ($1)").expect("prepare");
    c.execute_prepared(&stmt, &(1i32,)).expect("insert 1");
    assert!(c.execute_prepared(&stmt, &(1i32,)).is_err());
    c.execute_prepared(&stmt, &(2i32,)).expect("insert 2 after error");
    assert_eq!(c.query_raw("SELECT count(*) FROM pr_err").expect("count").get(0).expect("row 0").get_i64(0), Ok(Some(2)));
    c.close_statement(stmt).expect("close stmt");
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE cp(id int, name text)").expect("create");
    assert_eq!(c.copy_in("cp", vec!["1\talice", "2\tbob"]).expect("copy"), 2);
    assert_eq!(c.copy_in("cp", Vec::<&str>::new()).expect("copy empty"), 0);
    c.execute_raw("CREATE TEMP TABLE cp_lg(i int)").expect("create lg");
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

    let r = listener.query_raw("SELECT 1::int4").expect("query");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));

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
    let r = listener.query_raw("SELECT 1::int4").expect("query"); // captures the notify
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
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
            assert_eq!(conn.conn_mut().expect("live").query_raw(&format!("SELECT {i}::int")).expect("q").get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
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
        conn.execute_raw("SET search_path TO 'pg_temp'").expect("set guc");
        conn.execute_raw("CREATE TEMP TABLE bleed_probe(x int)").expect("temp");
        conn.execute_raw("LISTEN bleed_chan").expect("listen");
        pid
    }; // returned to pool (dirty)
    let mut c = pool.get().expect("get2");
    let conn = c.conn_mut().expect("live2");
    assert_eq!(conn.backend_pid(), pid1, "max_size=1 must reuse the SAME physical connection");
    let sp = conn.query_raw("SHOW search_path").expect("show").get(0).expect("row 0")
        .get_str(0).expect("search_path decodes").map(String::from);
    assert_ne!(sp.as_deref(), Some("pg_temp"), "search_path GUC bled across checkout");
    let n = conn.query_raw("SELECT count(*) FROM pg_tables WHERE tablename='bleed_probe'")
        .expect("tmp").get(0).expect("row 0").get_i64(0).expect("count decodes");
    assert_eq!(n, Some(0), "temp table bled across checkout");
    // LISTEN channel gone (UNLISTEN * ran in the reset).
    let listening = conn
        .query_raw("SELECT count(*)::int8 FROM pg_listening_channels() AS c(chan) WHERE chan='bleed_chan'")
        .expect("listen check").get(0).expect("row 0").get_i64(0).expect("listen count decodes");
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
        let _ = conn.execute_raw(&format!("SELECT pg_terminate_backend({pid})"));
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
    c.execute_raw("CREATE TEMP TABLE tx(v int)").expect("create");
    c.transaction(|tx| { tx.execute_raw("INSERT INTO tx VALUES (1)")?; Ok(()) }).expect("commit");
    assert_eq!(c.query_raw("SELECT count(*) FROM tx").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    let _: Result<(), _> = c.transaction(|tx| {
        tx.execute_raw("INSERT INTO tx VALUES (2)")?;
        Err(bsql_postgres_sync::DriverError::NoRows)
    });
    assert_eq!(c.query_raw("SELECT count(*) FROM tx").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    c.close().expect("close");
}

/// The deferred-BEGIN FUSION correctness path, end-to-end over real PG:
/// an EMPTY transaction is a true no-op (it arms no BEGIN and issues no COMMIT), a
/// transaction whose FIRST statement is the EXTENDED protocol (`query_params`,
/// one-round-trip) fuses BEGIN into that statement and commits its effect, and a
/// rollback of such a body discards it. Exercises the fused BEGIN over both the
/// simple- and extended-query first-statement paths, proving the prelude drain does
/// not corrupt the statement's result.
#[test]
#[ignore = "requires local PG"]
fn transaction_fusion_empty_and_extended() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE txf(v int)").expect("create");

    // (1) EMPTY body: a true no-op — no verb ran, so no BEGIN is armed and no
    // COMMIT/ROLLBACK is issued, and the connection stays healthy + at a clean idle.
    c.transaction(|_tx| Ok(())).expect("empty tx is a clean no-op");
    assert!(c.is_healthy(), "connection healthy after an empty (no-op) transaction");
    // The connection is reusable and NOT stuck in a transaction (a subsequent
    // stand-alone statement autocommits).
    c.execute_raw("INSERT INTO txf VALUES (7)").expect("post-empty insert");
    assert_eq!(
        c.query_raw("SELECT count(*) FROM txf").expect("c").get(0).expect("row 0").get_i64(0),
        Ok(Some(1))
    );

    // (2) FIRST statement is the EXTENDED protocol: BEGIN fuses ahead of the
    // Parse+Bind+Describe+Execute batch, and the statement's own row decodes
    // correctly (the prelude drain preserved the command's result schema).
    let fused = c
        .transaction(|tx| {
            let r = tx.query_params_one("SELECT $1::int + 1 AS n", &(41i32,))?;
            let n = r.get_i32(0).expect("decode the fused statement's row");
            tx.execute_raw("INSERT INTO txf VALUES (8)")?;
            Ok(n)
        })
        .expect("extended-first tx commits");
    assert_eq!(fused, Some(42), "the fused extended statement decoded correctly");
    assert_eq!(
        c.query_raw("SELECT count(*) FROM txf").expect("c").get(0).expect("row 0").get_i64(0),
        Ok(Some(2)),
        "the committed insert persisted"
    );

    // (3) ROLLBACK of an extended-first body discards its effect.
    let _: Result<(), _> = c.transaction(|tx| {
        drop(tx.query_params_one("SELECT $1::int", &(9i32,))?);
        tx.execute_raw("INSERT INTO txf VALUES (9)")?;
        Err(bsql_postgres_sync::DriverError::NoRows)
    });
    assert_eq!(
        c.query_raw("SELECT count(*) FROM txf").expect("c").get(0).expect("row 0").get_i64(0),
        Ok(Some(2)),
        "the rolled-back insert did not persist"
    );
    c.close().expect("close");
}

/// A transaction body that PANICS before issuing its first statement arms
/// NOTHING: the deferred `BEGIN` is armed INSIDE the first verb (never
/// out-of-band at `transaction()` entry), so a panic before any verb ran never
/// staged it. The connection is therefore already clean — `reset_session` has no
/// prelude to discard, and a subsequent statement flushes only itself. This is
/// the live regression guard for the arm-in-first-verb invariant: reset + reuse
/// after a mid-body panic must just work, with no latent stranded `BEGIN`.
#[test]
#[ignore = "requires local PG"]
fn transaction_panic_before_first_statement_arms_nothing() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // Isolate the user-code panic so we can inspect + reuse the connection.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), _> = c.transaction(|_tx| -> Result<(), bsql_postgres_sync::DriverError> {
            panic!("user code panicked before the first statement");
        });
    }));
    assert!(panicked.is_err(), "the panic propagated out of transaction");
    // Healthy (no verb ran, so the liveness token is intact) AND carrying no
    // armed BEGIN — exactly the clean connection a pool would take back.
    assert!(c.is_healthy(), "connection healthy after a body panic");
    // Nothing was armed, so the reset finds no prelude to fuse and just succeeds.
    c.reset_session().expect("reset succeeds — no stranded prelude exists to clear");
    // The next statement flushes only itself (no fused stale BEGIN) and works.
    assert_eq!(
        c.query_raw("SELECT 1::int").expect("q").get(0).expect("row 0").get_i32(0),
        Ok(Some(1))
    );
    c.close().expect("close");
}

/// COPY inside a transaction (via the borrowing guard) is a legal, ATOMIC bulk
/// load: the copied rows are visible to a query in the SAME transaction, persist
/// on COMMIT, and are gone on ROLLBACK — atomic bulk-load-with-rollback. Also
/// witnesses the deferred BEGIN fusing into a COPY that is the transaction's
/// FIRST statement.
#[test]
#[ignore = "requires local PG"]
fn copy_in_inside_transaction_commits_and_rolls_back() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE cptx(v int)").expect("create");

    // COMMIT path: `copy_in_with` (scoped writer) is the tx's FIRST statement, so
    // the deferred BEGIN fuses into it; a query in the SAME tx sees the rows.
    let count = c
        .transaction(|tx| {
            let n = tx.copy_in_with("cptx", |w| {
                w.write_row(b"1")?;
                w.write_row(b"2")?;
                w.write_row(b"3")?;
                Ok(())
            })?;
            assert_eq!(
                tx.query_raw("SELECT count(*) FROM cptx")?.get(0).expect("row 0").get_i64(0),
                Ok(Some(3)),
                "the just-copied rows are visible inside the transaction"
            );
            Ok(n)
        })
        .expect("copy-in transaction commits");
    assert_eq!(count, 3, "COPY reported 3 loaded rows");
    assert_eq!(
        c.query_raw("SELECT count(*) FROM cptx").expect("q").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
        "the committed COPY rows persist"
    );

    // ROLLBACK path: `copy_in` more rows, then Err → the copied rows are discarded.
    let result: Result<(), _> = c.transaction(|tx| {
        tx.copy_in("cptx", vec!["4", "5"])?;
        Err(bsql_postgres_sync::DriverError::NoRows)
    });
    assert!(result.is_err(), "the body error rolls the transaction back");
    assert_eq!(
        c.query_raw("SELECT count(*) FROM cptx").expect("q").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
        "the rolled-back COPY rows are NOT visible (still 3, not 5)"
    );
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn row_clone_across_threads() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let row = c.query_raw("SELECT 42::int, 'hello'::text").expect("q").get(0).expect("row 0");
    let handle = std::thread::spawn(move || row.get_i32(0).expect("i32 decodes"));
    assert_eq!(handle.join().expect("thread"), Some(42));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn full_lifecycle() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE lc(id serial PRIMARY KEY, name text, val int)").expect("create");
    c.transaction(|tx| {
        tx.execute_raw("INSERT INTO lc(name, val) VALUES ('alice', 95)")?;
        tx.execute_raw("INSERT INTO lc(name, val) VALUES ('bob', 88)")?;
        Ok(())
    }).expect("tx");
    assert_eq!(c.query_params_one("SELECT name FROM lc WHERE val > $1", &(90i32,)).expect("p").get_str(0), Ok(Some("alice")));
    let stmt = c.prepare("UPDATE lc SET val = val + $1 WHERE name = $2").expect("prep");
    c.execute_prepared(&stmt, &(5i32, "bob")).expect("update");
    c.close_statement(stmt).expect("close stmt");
    assert!(c.execute_raw("INSERT INTO lc(id) VALUES (1)").is_err()); // dup PK
    c.ping().expect("recover");
    assert_eq!(c.copy_in("lc", Vec::<&str>::new()).expect("copy empty"), 0);
    c.close().expect("close");
}

// ═══════════════════════════════════════════════════════════
// Shared SQL scenario tests (one macro — covers every SQL mechanic,
// run natively over the blocking driver; the async driver runs the
// SAME scenarios through a blocking shim in `sq_live.rs`).
// ═══════════════════════════════════════════════════════════

fn make_sync_conn() -> Connection {
    Connection::connect(&sync_config()).expect("connect")
}

bsql_postgres_core::define_sql_scenario_tests!(make_sync_conn);

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
            let r = c.conn_mut().expect("live").query_raw(&format!("SELECT {i}::int, pg_backend_pid()")).expect("q");
            assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
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
    c.execute_raw("CREATE TEMP TABLE omni(id serial PRIMARY KEY, name text, val int, active bool)").expect("create");
    c.execute_raw("CREATE INDEX ON omni(val)").expect("index");

    // DML via execute
    c.execute_raw("INSERT INTO omni(name, val, active) VALUES ('a', 10, true)").expect("ins");
    c.execute_raw("INSERT INTO omni(name, val, active) VALUES ('b', 20, false)").expect("ins");
    c.execute_raw("INSERT INTO omni(name, val, active) VALUES ('c', 30, true)").expect("ins");

    // DML via execute_params (uses typed binary encoding)
    c.execute_params("INSERT INTO omni(name, val, active) VALUES ($1, $2, $3)", &("d", 40i32, true)).expect("params");

    // Query
    let r = c.query_raw("SELECT count(*) FROM omni").expect("count");
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(4)));

    // Query with params
    let r = c.query_params("SELECT name FROM omni WHERE val > $1 ORDER BY val", &(15i32,)).expect("qp");
    assert_eq!(r.len(), 3); // b, c, d

    // Prepared
    let stmt = c.prepare("SELECT name, val FROM omni WHERE active = $1 ORDER BY val").expect("prep");
    let r = c.query_prepared(&stmt, &(true,)).expect("qprep");
    assert_eq!(r.len(), 3); // a, c, d
    c.close_statement(stmt).expect("close stmt");

    // Transaction
    c.transaction(|tx| {
        tx.execute_raw("UPDATE omni SET val = val * 2 WHERE active")?;
        Ok(())
    }).expect("tx");
    let r = c.query_raw("SELECT SUM(val) FROM omni").expect("sum");
    // a:20 + b:20(unchanged) + c:60 + d:80 = 180
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(180)));

    // Error + recovery
    assert!(c.query_raw("SELECT * FROM nonexistent").is_err());
    c.ping().expect("recover");

    // COPY IN
    c.execute_raw("CREATE TEMP TABLE cp_omni(v int)").expect("create cp");
    c.copy_in("cp_omni", vec!["1", "2", "3"]).expect("copy");

    // Column names
    let r = c.query_raw("SELECT id, name, val FROM omni LIMIT 1").expect("cols");
    assert_eq!(&*r.column_names, &["id", "name", "val"]);

    // Row clone across thread
    let row = c.query_raw("SELECT 'final'::text").expect("q").get(0).expect("row 0");
    let v = std::thread::spawn(move || row.get_str(0).expect("final decodes").map(String::from)).join().expect("thread");
    assert_eq!(v, Some("final".to_string()));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn wide_columns() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // 1664 is PostgreSQL's MaxTupleAttributeNumber — the widest result it
    // produces, and now the driver's cap. A conforming server never exceeds it.
    for n in [250u32, 500, 1000, 1600, 1664] {
        let cols: Vec<String> = (0..n).map(|i| format!("{i}::int AS col_{i}")).collect();
        let sql = format!("SELECT {}", cols.join(", "));
        let r = c.query_raw(&sql).unwrap_or_else(|e| panic!("{n} cols failed: {e}"));
        assert_eq!(r.len(), 1, "rows at {n} cols");
        assert_eq!(r.column_names.len(), usize::try_from(n).unwrap(), "col names at {n}");
        assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(0)), "first col at {n}");
        let last = usize::try_from(n.saturating_sub(1)).unwrap();
        assert_eq!(r.get(0).expect("row 0").get_i32(last), Ok(Some(n.saturating_sub(1) as i32)), "last col at {n}");
    }
    c.close().expect("close");
}

/// The wide-column BOUNDARY, live: 1665 columns is beyond PostgreSQL's own
/// `MaxTupleAttributeNumber` (1664), so the SERVER rejects the query before any
/// result — a recoverable `DriverError::Db` — and the connection survives to
/// serve a follow-up query (the wide-column corner is recoverable end-to-end, not
/// a teardown). A CLIENT-side over-cap (a nonconforming server wider than 1664)
/// is witnessed deterministically in `bsql-testkit`'s `overcap_recovery`.
#[test]
#[ignore = "requires local PG"]
fn wide_columns_beyond_pg_limit_is_a_recoverable_server_error() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let cols: Vec<String> = (0..1665u32).map(|i| format!("{i}::int AS col_{i}")).collect();
    let sql = format!("SELECT {}", cols.join(", "));
    let err = c.query_raw(&sql).expect_err("1665 columns exceeds PG's own 1664 limit");
    match err {
        bsql_postgres_sync::DriverError::Db(db) => {
            assert!(
                format!("{db}").contains("target lists can have at most 1664"),
                "expected PG's target-list-limit error, got: {db}"
            );
        }
        other => panic!("expected a server Db error, got {other:?}"),
    }
    // Recovered: a follow-up query on the SAME connection succeeds.
    let r = c.query_raw("SELECT 7").expect("connection recovered after the server error");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(7)));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn prepared_statement_edge_cases() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE ps_edge(id int, v text)").expect("create");

    // Prepare, execute 0 times, close
    let stmt = c.prepare("INSERT INTO ps_edge VALUES ($1, $2)").expect("prep");
    c.close_statement(stmt).expect("close unused stmt");

    // Prepare, execute many times
    let stmt = c.prepare("INSERT INTO ps_edge VALUES ($1, $2)").expect("prep");
    for i in 0..50i32 {
        c.execute_prepared(&stmt, &(i, format!("v{i}").as_str())).expect("exec");
    }
    assert_eq!(c.query_raw("SELECT count(*) FROM ps_edge").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(50)));
    c.close_statement(stmt).expect("close");

    // Prepare SELECT, query many times
    let stmt = c.prepare("SELECT v FROM ps_edge WHERE id = $1").expect("prep select");
    for i in 0..50i32 {
        let r = c.query_prepared(&stmt, &(i,)).expect("qp");
        assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some(format!("v{i}").as_str())));
    }
    c.close_statement(stmt).expect("close");

    // Multiple prepared statements open at once
    let s1 = c.prepare("SELECT id FROM ps_edge WHERE id < $1").expect("s1");
    let s2 = c.prepare("SELECT v FROM ps_edge WHERE id = $1").expect("s2");
    let s3 = c.prepare("UPDATE ps_edge SET v = $1 WHERE id = $2").expect("s3");
    let r1 = c.query_prepared(&s1, &(5i32,)).expect("q1");
    assert_eq!(r1.len(), 5);
    let r2 = c.query_prepared(&s2, &(0i32,)).expect("q2");
    assert_eq!(r2.get(0).expect("row 0").get_str(0), Ok(Some("v0")));
    c.execute_prepared(&s3, &("updated", 0i32)).expect("exec3");
    let r2b = c.query_prepared(&s2, &(0i32,)).expect("q2b");
    assert_eq!(r2b.get(0).expect("row 0").get_str(0), Ok(Some("updated")));
    c.close_statement(s1).expect("close s1");
    c.close_statement(s2).expect("close s2");
    c.close_statement(s3).expect("close s3");

    // Error in prepared doesn't break statement
    c.execute_raw("CREATE TEMP TABLE ps_uk(id int UNIQUE)").expect("create");
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
    c.execute_raw("CREATE TEMP TABLE cp_edge(id int, name text)").expect("create");
    assert_eq!(c.copy_in("cp_edge", Vec::<&str>::new()).expect("empty"), 0);
    assert_eq!(c.query_raw("SELECT count(*) FROM cp_edge").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(0)));

    // COPY 1 row
    assert_eq!(c.copy_in("cp_edge", vec!["1\tone"]).expect("one"), 1);

    // COPY with NULLs (PG COPY \N = NULL)
    assert_eq!(c.copy_in("cp_edge", vec!["2\t\\N"]).expect("null"), 1);

    // COPY many rows
    let big: Vec<String> = (0..5000).map(|i| format!("{i}\tname_{i}")).collect();
    assert_eq!(c.copy_in("cp_edge", &big).expect("5k"), 5000);
    assert_eq!(c.query_raw("SELECT count(*) FROM cp_edge").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(5002)));

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
        c.query_raw("SELECT count(*) FROM cp_edge").expect("count").get(0).expect("row 0").get_i64(0),
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
    c.execute_raw("CREATE TEMP TABLE empty_stream(v int)").expect("create");
    let r = c.query_raw("SELECT * FROM empty_stream").expect("empty");
    assert_eq!(r.len(), 0);

    // 1 row streaming
    c.execute_raw("INSERT INTO empty_stream VALUES (42)").expect("ins");
    let r = c.query_raw("SELECT * FROM empty_stream").expect("one");
    assert_eq!(r.len(), 1);
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(42)));

    // Large value via SQL literal (params limited to 1024 bytes)
    let big_val = "X".repeat(50_000);
    c.execute_raw("CREATE TEMP TABLE big_val(v text)").expect("create");
    c.execute_raw(&format!("INSERT INTO big_val VALUES ('{big_val}')")).expect("ins");
    let r = c.query_raw("SELECT v FROM big_val").expect("q");
    assert_eq!(r.get(0).expect("row 0").get_str(0).expect("big_val decodes").map(|s| s.len()), Some(50_000));

    // Many columns with NULLs
    let r = c.query_raw("SELECT NULL::int, 1::int, NULL::text, 'a'::text, NULL::bool, true").expect("mixed nulls");
    assert!(r.get(0).expect("row 0").is_null(0));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(1)));
    assert!(r.get(0).expect("row 0").is_null(2));
    assert_eq!(r.get(0).expect("row 0").get_str(3), Ok(Some("a")));
    assert!(r.get(0).expect("row 0").is_null(4));
    assert_eq!(r.get(0).expect("row 0").get_bool(5), Ok(Some(true)));

    // Query after error mid-stream should recover
    assert!(c.query_raw("SELECT 1/0 FROM generate_series(1,10)").is_err());
    c.ping().expect("recover after mid-stream error");
    let r = c.query_raw("SELECT 1::int").expect("after recover");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn connection_resilience_marathon() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // 50 alternating errors and successes
    for i in 0..50u32 {
        if i % 2 == 0 {
            assert!(c.query_raw("SELECT * FROM nonexistent_marathon").is_err());
        } else {
            assert_eq!(c.query_raw(&format!("SELECT {i}::int")).expect("q").get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
        }
    }
    c.ping().expect("after marathon");

    // 200 rapid pings
    for _ in 0..200 {
        c.ping().expect("rapid ping");
    }

    // Error → recover → success cycle
    c.execute_raw("CREATE TEMP TABLE IF NOT EXISTS marathon_t(v int)").expect("create");
    for i in 0..20u32 {
        assert!(c.simple_query("INVALID SQL GIBBERISH").is_err());
        c.ping().unwrap_or_else(|e| panic!("ping after err #{i}: {e}"));
        c.execute_raw("INSERT INTO marathon_t VALUES (1)").unwrap_or_else(|e| panic!("ins #{i}: {e}"));
        assert!(c.query_raw("SELECT 'bad'::int").is_err());
        c.ping().unwrap_or_else(|e| panic!("ping2 #{i}: {e}"));
        let r = c.query_raw("SELECT count(*) FROM marathon_t").unwrap_or_else(|e| panic!("count #{i}: {e}"));
        assert!(r.get(0).expect("row 0").get_i64(0).expect("count decodes").unwrap_or(0) > 0);
    }

    // Verify connection is still fully functional
    c.execute_raw("CREATE TEMP TABLE final_check(a int, b text, c bool)").expect("create");
    c.execute_raw("INSERT INTO final_check VALUES (1, 'hello', true)").expect("ins");
    let r = c.query_raw("SELECT * FROM final_check").expect("final");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(r.get(0).expect("row 0").get_str(1), Ok(Some("hello")));
    assert_eq!(r.get(0).expect("row 0").get_bool(2), Ok(Some(true)));

    c.close().expect("close");
}

// ═══════════════════════════════════════════════════════════
// Binary-uniform Bind frame — parameterized INSERT round-trip.
//
// REGRESSION GATE: before the binary-uniform fix, the extended-query path
// declared param format = Text in the Bind frame while encoding the value as
// binary. PostgreSQL then rejected any non-string param (e.g. an i32 sent as 4
// binary bytes interpreted as ASCII decimal) with `invalid input syntax for type
// integer`. This test runs an INSERT carrying i32 / i64 / bool params through the
// dynamic parameterized `execute_params` verb (the public verb that exercises the
// binary-uniform Bind machinery — `build_bind_prepared` + `ParamsWriter` — shared
// with the typed `execute::<Q>` / `query!` path) and asserts: (1) the write
// succeeds, (2) the affected-row count is correct, (3) the stored values read back
// exactly. (The TYPED path's binary Bind is covered live by the `query_fixture`
// `execute::<Q>` / `query::<Q>` tests, which have a build catalog this driver crate
// deliberately does not.) Post-fix it passes; pre-fix the INSERT errors at the server.
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

    c.execute_raw(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).expect("drop schema pre");
    c.execute_raw(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    c.execute_raw(&format!("SET search_path TO {schema}")).expect("set search_path");

    c.execute_raw(
        "CREATE TABLE prep_target (n int4 NOT NULL, big int8 NOT NULL, flag bool NOT NULL)",
    )
    .expect("create table");

    // A parameterized INSERT carrying i32 / i64 / bool. `$N::type` casts pin the
    // parameter types; the unqualified `prep_target` resolves via `search_path` to
    // this process's schema. `execute_params` declares each param's encoded OID
    // (`ParamsWriter::OIDS`) and binds binary-uniform — the exact frame that
    // carried the declared-Text / encoded-Binary bug.
    const INSERT_SQL: &str =
        "INSERT INTO prep_target (n, big, flag) VALUES ($1::int4, $2::int8, $3::bool)";

    let sent_n: i32 = 42;
    let sent_big: i64 = 9_000_000_000;
    let sent_flag: bool = true;

    // EXECUTE via the dynamic parameterized path — the exact wire path that carried
    // the declared-Text / encoded-Binary bug. Pre-fix this errors at the server with
    // `invalid input syntax for type integer`.
    let affected = c
        .execute_params(INSERT_SQL, &(sent_n, sent_big, sent_flag))
        .expect("parameterized INSERT must succeed (binary-uniform Bind)");
    assert_eq!(affected, 1, "INSERT must affect exactly one row");

    // Read the row back via the simple-query text path to confirm the
    // server actually stored the binary-encoded values correctly.
    let r = c.query_raw("SELECT n, big, flag FROM prep_target").expect("read-back query");
    assert_eq!(r.len(), 1, "exactly one row stored");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(sent_n)), "i32 param stored correctly");
    assert_eq!(r.get(0).expect("row 0").get_i64(1), Ok(Some(sent_big)), "i64 param stored correctly");
    assert_eq!(r.get(0).expect("row 0").get_bool(2), Ok(Some(sent_flag)), "bool param stored correctly");

    // Cleanup: DROP IF EXISTS at end (schema CASCADE removes the table).
    c.execute_raw(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).expect("drop schema post");

    c.close().expect("close");
}

// ─────────────────────────── COPY (streaming) ───────────────────────────

#[test]
#[ignore = "requires local PG"]
fn copy_round_trip_in_then_out() {
    // Stream rows IN via the scoped writer, then stream them back OUT.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE cp_rt(id int4, name text)").expect("create");
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
        c.query_raw("SELECT count(*) FROM cp_rt").expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(3)),
    );
    c.close().expect("close");
}

/// DATA-CORRECTNESS regression (COPY OUT), sync twin of
/// `copy_out_oversize_rows_are_byte_exact`: an OVERSIZE row (one `CopyData`
/// frame larger than the engine's bounded read buffer) reaches `on_chunk`
/// byte-for-byte, never truncated to the internal 8 KiB oversize prefix. Both
/// drivers share `Core<S>` + the proto engine, so this witnesses the SAME fix
/// over the blocking transport.
#[test]
#[ignore = "requires local PG"]
fn copy_out_oversize_rows_are_byte_exact() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE cp_wide(tag text, payload text)").expect("create");

    const WIDE: usize = 50_000; // > 8192
    const HUGE: usize = 70_000; // > 65536
    c.execute_raw(
        "INSERT INTO cp_wide(tag, payload) \
         VALUES ('A', repeat('x', 50000)), ('B', repeat('y', 70000))",
    )
    .expect("insert wide rows");

    let mut received: Vec<u8> = Vec::new();
    let broke: Option<core::convert::Infallible> = c
        .copy_out("cp_wide", |chunk| {
            received.extend_from_slice(chunk);
            core::ops::ControlFlow::Continue(())
        })
        .expect("copy_out");
    assert!(broke.is_none(), "streamed to completion");

    let expected_total = WIDE + HUGE + 6; // 'A\t'+50000+'\n' + 'B\t'+70000+'\n'
    assert_eq!(
        received.len(),
        expected_total,
        "every COPY-OUT byte delivered (no 8 KiB truncation): got {}, expected {expected_total}",
        received.len(),
    );

    let text = String::from_utf8(received).expect("utf8 copy stream");
    let mut rows: Vec<(char, usize, u8)> = Vec::new();
    for line in text.lines() {
        let (tag, payload) = line.split_once('\t').expect("tab-separated COPY line");
        let first = payload.bytes().next().expect("non-empty payload");
        assert!(
            payload.bytes().all(|b| b == first),
            "payload for tag {tag} is a uniform run — no spliced truncation boundary",
        );
        let tag_char = tag.chars().next().expect("non-empty tag");
        rows.push((tag_char, payload.len(), first));
    }
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![('A', WIDE, b'x'), ('B', HUGE, b'y')],
        "both oversize rows round-trip byte-exact (full length, correct bytes)",
    );

    assert!(c.is_healthy(), "connection reusable after oversize COPY OUT");
    assert_eq!(
        c.query_raw("SELECT count(*) FROM cp_wide").expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(2)),
    );
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in_large_chunk_passthrough() {
    // The sync twin of the async large-chunk passthrough: one `write_chunk` body
    // far exceeding the 64 KiB threshold is streamed DIRECTLY (never buffered).
    // Prove it is byte-faithful against real PG.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE cp_big(id int8, payload text)").expect("create");

    const ROWS: i64 = 10_000;
    let mut chunk = String::new();
    for i in 0..ROWS {
        chunk.push_str(&format!("{i}\tpayload-row-{i}\n"));
    }
    assert!(chunk.len() > 64 * 1024, "the single chunk must exceed the threshold");

    let n = c
        .copy_in_with("cp_big", |w| w.write_chunk(chunk.as_bytes()))
        .expect("large-chunk copy_in_with");
    assert_eq!(n, u64::try_from(ROWS).expect("ROWS fits u64"), "all rows ingested");

    assert_eq!(
        c.query_raw("SELECT count(*) FROM cp_big").expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(ROWS)),
    );
    assert_eq!(
        c.query_raw("SELECT payload FROM cp_big WHERE id = 9999").expect("val").get(0).expect("row 0").get_str(0),
        Ok(Some("payload-row-9999")),
    );
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in_abort_mid_stream_recovers() {
    // A copy_in_with whose closure ERRORS mid-stream sends CopyFail; the
    // connection recovers and commits none of the aborted rows.
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE cp_ab(id int4)").expect("create");
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
        c.query_raw("SELECT count(*) FROM cp_ab").expect("count").get(0).expect("row 0").get_i64(0),
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
    c.execute_raw("CREATE TEMP TABLE cp_brk(id int4)").expect("create");
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
        c.query_raw("SELECT count(*) FROM cp_brk").expect("count").get(0).expect("row 0").get_i64(0),
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
    c.execute_raw("CREATE TEMP TABLE cp_bulk(id int8, payload text)").expect("create");
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
        c.query_raw("SELECT count(*) FROM cp_bulk").expect("count").get(0).expect("row 0").get_i64(0),
        Ok(Some(N)),
    );
    c.close().expect("close");
}

/// WITNESS (blocking driver): the DYNAMIC prepared-statement cache is
/// transparent and correct on the SYNC driver too — the same Core cache logic,
/// driven single-poll. The SAME parameterized SQL run many times with different
/// params returns each call's own row (no leaked binding, no mis-reused plan).
#[test]
#[ignore = "requires local PG"]
fn dynamic_cache_reuse_returns_correct_rows_sync() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let sql = "SELECT ($1::int * 10) AS v";
    for round in 0..3 {
        for i in 0..20_i32 {
            let row = c.query_params_one(sql, &(i,)).expect("cached reuse");
            assert_eq!(row.get_i32(0), Ok(Some(i * 10)), "round {round}, i {i}");
        }
    }
    c.close().expect("close");
}

/// WITNESS (blocking dynamic streaming): `query_each_raw` streams a 20 000-row
/// runtime query one row at a time on the SYNC driver — same constant-memory
/// contract, blocking. Every row seen, in order, correct values, connection
/// reusable after.
#[test]
#[ignore = "requires local PG"]
fn query_each_sql_streams_a_large_result_correctly_sync() {
    use core::ops::ControlFlow;
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let mut count = 0i64;
    let mut sum = 0i64;
    let mut expected_next = 1i64;
    let mut in_order = true;
    let out = c
        .query_each_raw::<_, ()>("SELECT generate_series(1, 20000) AS n", |row| {
            let n = row.get_i64(0).expect("decode n").expect("n is not NULL");
            if n != expected_next {
                in_order = false;
            }
            expected_next += 1;
            count += 1;
            sum += n;
            ControlFlow::Continue(())
        })
        .expect("stream completes");

    assert_eq!(out, None, "a full stream returns Ok(None)");
    assert_eq!(count, 20_000, "every row was streamed");
    assert!(in_order, "rows streamed in order");
    // 1 + 2 + … + 20000 = 20000·20001/2 = 200_010_000.
    assert_eq!(sum, 200_010_000, "every value was correct");
    let after = c.query_one_raw("SELECT 'reusable'").expect("reuse");
    assert_eq!(after.get_str(0), Ok(Some("reusable")));
    c.close().expect("close");
}

/// WITNESS (blocking, runtime param): `query_each_params` streams a parameterised
/// runtime query — the `$1` filters the series at run time.
#[test]
#[ignore = "requires local PG"]
fn query_each_params_streams_with_a_runtime_param_sync() {
    use core::ops::ControlFlow;
    let mut c = Connection::connect(&sync_config()).expect("connect");

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
        .expect("stream completes");
    assert_eq!(out, None);
    assert_eq!(seen, vec![1, 2, 3, 4, 5], "the runtime $1 bound the filter");
    c.close().expect("close");
}

/// WITNESS (blocking, early break + reuse): a closure `Break` stops the stream;
/// the payload rides `Ok(Some(_))`, the remaining rows drain to idle, and the
/// connection is reusable.
#[test]
#[ignore = "requires local PG"]
fn query_each_sql_break_stops_early_and_connection_is_reusable_sync() {
    use core::ops::ControlFlow;
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let mut seen = 0i64;
    // Break after 100 of a 150-row stream (within the 128-frame drain budget).
    let stopped_at = c
        .query_each_raw::<_, i64>("SELECT generate_series(1, 150) AS n", |row| {
            let _n = row.get_i64(0).expect("decode").expect("not NULL");
            seen += 1;
            if seen >= 100 {
                ControlFlow::Break(seen)
            } else {
                ControlFlow::Continue(())
            }
        })
        .expect("stream drains after the early break");

    assert_eq!(stopped_at, Some(100), "the break payload rides Ok(Some(_))");
    assert_eq!(seen, 100);
    assert!(c.is_healthy());
    let after = c.query_one_raw("SELECT 7").expect("reuse after early break");
    assert_eq!(after.get_i32(0), Ok(Some(7)));
    c.close().expect("close");
}

/// Early break on a massive (1,000,000-row) stream must drop socket without hanging.
#[test]
#[ignore = "requires local PG"]
fn query_each_sql_break_huge_stream_drops_socket_cleanly_sync() {
    use core::ops::ControlFlow;
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let mut seen = 0i64;
    let stopped_at = c
        .query_each_raw::<_, i64>("SELECT generate_series(1, 1000000) AS n", |row| {
            let _n = row.get_i64(0).expect("decode").expect("not NULL");
            seen += 1;
            if seen >= 100 {
                ControlFlow::Break(seen)
            } else {
                ControlFlow::Continue(())
            }
        })
        .expect("break returns without error");

    assert_eq!(stopped_at, Some(100));
    assert_eq!(seen, 100);
    assert!(!c.is_healthy());
}

/// WITNESS (blocking, oversize-row reassembly): `query_each_raw` streams multiple
/// rows each FAR larger than the 4 KiB read buffer — each arrives as `RowChunk`
/// pieces and is REASSEMBLED into the reused scratch before decode. Byte-exact
/// reconstruction (no chunk-seam truncation, no cross-row bleed), with an
/// interleaved NULL and a trailing small column both correctly positioned, and the
/// connection reusable. The blocking peer of the async oversize witness.
#[test]
#[ignore = "requires local PG"]
fn query_each_sql_reassembles_oversize_rows_sync() {
    use core::ops::ControlFlow;
    let mut c = Connection::connect(&sync_config()).expect("connect");

    const SPEC: [(char, usize, &str); 3] = [('a', 400_000, "end-1"), ('b', 300_000, "end-2"), ('c', 500_000, "end-3")];
    let sql = "SELECT repeat('a', 400000) AS big, NULL::text AS mid, 'end-1' AS tail \
               UNION ALL SELECT repeat('b', 300000), NULL, 'end-2' \
               UNION ALL SELECT repeat('c', 500000), NULL, 'end-3'";

    let mut rows: Vec<(usize, bool, bool, String)> = Vec::new();
    let out = c
        .query_each_raw::<_, ()>(sql, |row| {
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
    let after = c.query_one_raw("SELECT 'reusable-after-oversize'").expect("reuse");
    assert_eq!(after.get_str(0), Ok(Some("reusable-after-oversize")));
    c.close().expect("close");
}

/// WITNESS (blocking, inside a transaction): the streaming verb works through the
/// blocking transaction GUARD.
#[test]
#[ignore = "requires local PG"]
fn query_each_sql_streams_inside_a_transaction_sync() {
    use core::ops::ControlFlow;
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let mut n = 0i64;
    c.transaction(|tx| {
        tx.query_each_raw::<_, ()>("SELECT generate_series(1, 500)", |_row| {
            n += 1;
            ControlFlow::Continue(())
        })?;
        Ok(())
    })
    .expect("transaction with a stream commits");
    assert_eq!(n, 500);
    c.close().expect("close");
}

/// WITNESS (review BLOCKER 1 — no re-entrancy deadlock): a diagnostics sink that
/// calls `pool.stats()` from inside the `PoolAcquireTimeout` event must NOT
/// deadlock the sync pool (the state lock is released before the sink runs).
/// Before the fix this hung; the test PASSING (get() returns within the deadline)
/// proves no deadlock.
#[test]
#[ignore = "requires local PG"]
fn sync_pool_stats_sink_on_acquire_timeout_does_not_deadlock() {
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use bsql_postgres_sync::{DiagEvent, DriverError, Pool};

    // The sink re-enters the pool via a handle shared through a OnceLock set after
    // build (the pool cannot be captured before it exists).
    let pool_cell: Arc<OnceLock<Pool>> = Arc::new(OnceLock::new());
    let cell_in = Arc::clone(&pool_cell);
    let reentered: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
    let reentered_in = Arc::clone(&reentered);
    let pool = Pool::builder(unix_config(), 1)
        .acquire_timeout(Duration::from_millis(150))
        .on_diagnostic(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::PoolAcquireTimeout { .. } = ev {
                // Re-enter: lock the SAME pool state the emit path just released.
                if let Some(p) = cell_in.get() {
                    *reentered_in.lock().expect("lock") = Some(p.stats().acquire_timeouts);
                }
            }
        })
        .build();
    pool_cell.set(pool.clone()).ok();

    let held = pool.get().expect("first checkout");
    match pool.get() {
        Err(DriverError::PoolTimeout) => {}
        Err(other) => panic!("expected PoolTimeout, got {other:?}"),
        Ok(_) => panic!("a max-size-1 pool must not hand out a second connection"),
    }
    assert!(
        reentered.lock().expect("lock").is_some(),
        "the sink re-entered pool.stats() from PoolAcquireTimeout without deadlocking",
    );
    drop(held);
}

/// WITNESS (review BLOCKER 2 — a panicking sink neither aborts nor poisons): a
/// sink that PANICS on every event, with `slow_query_threshold(ZERO)` so
/// `SlowQuery` also fires, over a `DO … RAISE NOTICE` (which fires `ServerNotice`
/// DURING the pump). Both panics are contained by `catch_unwind`; the test
/// completing proves no process abort, and the follow-up `SELECT 42` returning
/// proves the connection was not poisoned to `NotReady`.
#[test]
#[ignore = "requires local PG"]
fn panicking_sink_neither_aborts_nor_poisons_the_connection() {
    use std::time::Duration;

    use bsql_postgres_sync::{DiagEvent, Diagnostics};

    let diag = Diagnostics::new()
        .slow_query_threshold(Duration::ZERO)
        .on_event(|_ev: &DiagEvent<'_>| panic!("boom — a deliberately buggy sink"));
    let mut c = Connection::connect_with(&unix_config(), &diag).expect("connect_with");

    // Fires ServerNotice (pump) AND SlowQuery (drop) — the sink panics on both,
    // both must be contained.
    c.execute_raw("DO $$ BEGIN RAISE NOTICE 'x'; END $$")
        .expect("the DO completes despite the panicking sink");

    // Not poisoned: the connection is still usable and the result is correct.
    let row = c.query_one_raw("SELECT 42").expect("connection still usable, not NotReady");
    assert_eq!(row.get_i32(0), Ok(Some(42)));
    drop(c);
}

/// WITNESS (review MAJOR 4 — uncontended checkout leaves waiters_high_water at 0):
/// a single checkout on a pool with a free slot never blocks, so the gauge stays 0.
#[test]
#[ignore = "requires local PG"]
fn uncontended_checkout_leaves_waiters_high_water_zero() {
    use bsql_postgres_sync::Pool;

    let pool = Pool::builder(unix_config(), 4).build();
    let c = pool.get().expect("uncontended checkout");
    assert_eq!(
        pool.stats().waiters_high_water,
        0,
        "an uncontended checkout must not register a blocked waiter",
    );
    drop(c);
}

/// WITNESS (Part A — cross-connection prepared-statement safety, sync twin): a
/// `PreparedStatement` minted by connection A, used on connection B, is a LOUD
/// classified `DriverError::WrongConnection` — never a silent wrong result and
/// never a panic; B stays usable and A's handle still runs on A.
#[test]
#[ignore = "requires local PG"]
fn prepared_statement_used_on_a_foreign_connection_is_rejected() {
    use bsql_postgres_sync::DriverError;
    let mut a = Connection::connect(&sync_config()).expect("connect A");
    let mut b = Connection::connect(&sync_config()).expect("connect B");

    // Both prepare their FIRST statement → both hold `_bsql_0` for DIFFERENT plans.
    let stmt_a = a.prepare("SELECT 111::int4 AS n").expect("prepare on A");
    let stmt_b = b.prepare("SELECT 222::int4 AS n").expect("prepare on B");

    // A's handle on B: a LOUD reject, not B's 222.
    let err = b
        .query_prepared(&stmt_a, &())
        .expect_err("A's statement run on B must be rejected");
    assert!(matches!(err, DriverError::WrongConnection), "got {err:?}");
    assert!(!err.is_disconnect(), "wrong-connection is NOT a disconnect");
    assert!(!err.is_config(), "wrong-connection is NOT a config error");

    // `execute_prepared` rejects identically.
    let err2 = b
        .execute_prepared(&stmt_a, &())
        .expect_err("execute_prepared of A's statement on B must be rejected");
    assert!(matches!(err2, DriverError::WrongConnection), "got {err2:?}");

    // B is UNTOUCHED — its OWN statement still returns 222.
    let ok_b = b.query_prepared(&stmt_b, &()).expect("B's own stmt works");
    assert_eq!(ok_b.get(0).expect("row").get_i32(0), Ok(Some(222)));

    // A's handle STILL runs on A.
    let ok_a = a.query_prepared(&stmt_a, &()).expect("A's stmt on A works");
    assert_eq!(ok_a.get(0).expect("row").get_i32(0), Ok(Some(111)));

    // `close_statement` guards the SAME way (the handle is consumed on reject).
    let stmt_a2 = a.prepare("SELECT 333::int4 AS n").expect("prepare A2");
    let err3 = b
        .close_statement(stmt_a2)
        .expect_err("close_statement of A's handle on B must be rejected");
    assert!(matches!(err3, DriverError::WrongConnection), "got {err3:?}");
    a.close_statement(stmt_a).expect("close A's stmt on A");

    a.close().expect("close A");
    b.close().expect("close B");
}

/// WITNESS: a transaction that panics after its first verb is synchronously
/// rolled back on drop during unwinding, leaving the connection in clean Idle state.
#[test]
#[ignore = "requires local PG"]
fn transaction_panic_rolls_back_synchronously() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut c = Connection::connect(&sync_config()).expect("connect");

    c.execute_raw("CREATE TEMP TABLE t_tx_panic(id int PRIMARY KEY, v text)")
        .expect("create table");

    let prior_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        c.transaction(|tx| -> Result<(), bsql_postgres_sync::DriverError> {
            tx.execute_raw("INSERT INTO t_tx_panic VALUES (1, 'panicked')")?;
            panic!("boom inside transaction");
        })
    }));
    std::panic::set_hook(prior_hook);
    assert!(outcome.is_err(), "panic must propagate");

    // Sync drop rolled back immediately:
    assert!(!c.tx_needs_rollback());
    assert_eq!(c.tx_status(), Some(bsql_postgres_sync::TxStatus::Idle));
    assert!(c.is_healthy());

    let rows = c
        .query_raw("SELECT count(*) FROM t_tx_panic")
        .expect("query after panic rollback must succeed");
    assert_eq!(rows.get(0).expect("row").get_i64(0), Ok(Some(0)), "inserted row was rolled back");

    c.close().expect("close");
}

