//! WITNESS: a DIRECT (non-pooled) in-flight query is BOUNDED under a mid-query
//! fault — it can never hang forever. The blocking-driver twin of the async
//! `direct_liveness` suite.
//!
//! The pool's `pool_liveness` suite proves a pooled CHECKOUT is bounded on a dead
//! peer (the `reset_session` deadline). This suite proves the complementary
//! property for the IN-FLIGHT QUERY itself, on both a direct and a pooled
//! connection:
//!
//! * **Mid-query FIN / RST** — the peer closes mid-result. The read returns EOF /
//!   reset at once, so the verb is a classified disconnect error in milliseconds.
//!   (`direct_query_recovers_bounded_on_mid_query_fin`.)
//!
//! * **App-level BLACK-HOLE with a server-side query budget** — the peer's kernel
//!   still ACKs (TCP keepalive is answered, so keepalive canNOT detect it) but the
//!   application forwards nothing, and the server's `statement_timeout` `57014`
//!   abort is itself black-holed, so `statement_timeout` alone leaves the CLIENT
//!   blocked. A silent black-hole is INDISTINGUISHABLE at the socket layer from a
//!   server legitimately taking a long time to produce the first byte, so no fixed
//!   client deadline can catch it without cutting a legitimate slow query — EXCEPT
//!   one derived from the server's own `statement_timeout`, past which the server
//!   would already have aborted the query. A connection configured with
//!   `with_statement_timeout` therefore rests its socket read at a client-liveness
//!   `SO_RCVTIMEO` window (`statement_timeout` + `connect_timeout`, per-read); a
//!   black-holed in-flight query elapses into a classified `DriverError::Timeout`
//!   at that window instead of the kernel's `tcp_retries2` hang — WITHOUT cutting a
//!   query the server allows. Witnessed on a DIRECT connection
//!   (`direct_black_hole_query_is_bounded_with_statement_timeout`) AND a POOLED
//!   connection's in-flight query
//!   (`pooled_inflight_black_hole_query_is_bounded_with_statement_timeout`).
//!
//! HONEST RESIDUAL (by design, not a gap): a connection with NO `statement_timeout`
//! keeps an unbounded in-flight read on a live black-hole — a dead KERNEL is still
//! caught by TCP keepalive, but a live black-hole cannot be distinguished from a
//! legitimate long query without a query budget.
//!
//! Needs a local PG (a black-hole relay in front of `127.0.0.1:5432`), so
//! `#[ignore]`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, Pool, SslMode};

/// Real local PostgreSQL the relay forwards to (trust auth, plaintext).
const UPSTREAM: &str = "127.0.0.1:5432";

/// A byte-transparent TCP relay to real PG that can, per connection generation,
/// either BLACK-HOLE (freeze forwarding but keep the socket open — a live peer
/// whose kernel still ACKs) or KILL (drop the sockets so the client sees a FIN).
struct BlackHoleProxy {
    addr: SocketAddr,
    next_gen: Arc<AtomicU64>,
    freeze_below: Arc<AtomicU64>,
    kill_below: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl BlackHoleProxy {
    #[expect(
        clippy::expect_used,
        reason = "test relay setup: panics loudly if the loopback listener cannot bind — the intended harness-failure signal, and not a `#[test]` fn so the in-tests carve-out cannot reach it"
    )]
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy listener");
        let addr = listener.local_addr().expect("proxy addr");
        listener
            .set_nonblocking(true)
            .expect("proxy listener nonblocking");
        let next_gen = Arc::new(AtomicU64::new(0));
        let freeze_below = Arc::new(AtomicU64::new(0));
        let kill_below = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (ng, fb, kb, sd) = (
            next_gen.clone(),
            freeze_below.clone(),
            kill_below.clone(),
            shutdown.clone(),
        );
        let accept_thread = thread::spawn(move || {
            let mut handlers: Vec<JoinHandle<()>> = Vec::new();
            while !sd.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((down, _)) => {
                        let g = ng.fetch_add(1, Ordering::Relaxed);
                        handlers.push(handle(down, g, fb.clone(), kb.clone(), sd.clone()));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
            for h in handlers {
                h.join().ok();
            }
        });
        Self {
            addr,
            next_gen,
            freeze_below,
            kill_below,
            shutdown,
            accept_thread: Some(accept_thread),
        }
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Black-hole every connection open RIGHT NOW: stop forwarding both ways but
    /// keep the sockets OPEN (a live, silently-vanished peer).
    fn freeze_existing(&self) {
        self.freeze_below
            .store(self.next_gen.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    /// Kill every connection open RIGHT NOW: drop both sockets so the client sees
    /// a prompt FIN mid-query (the half-open / graceful-close fast path).
    fn kill_existing(&self) {
        self.kill_below
            .store(self.next_gen.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

impl Drop for BlackHoleProxy {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.accept_thread.take() {
            h.join().ok();
        }
    }
}

fn handle(
    down: TcpStream,
    my_gen: u64,
    freeze_below: Arc<AtomicU64>,
    kill_below: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let up = match TcpStream::connect(UPSTREAM) {
            Ok(s) => s,
            Err(_) => return,
        };
        down.set_nonblocking(false).ok();
        down.set_read_timeout(Some(Duration::from_millis(50))).ok();
        up.set_read_timeout(Some(Duration::from_millis(50))).ok();
        let down = Arc::new(down);
        let up = Arc::new(up);
        let d2u = {
            let (a, b, fb, kb, sd) = (
                down.clone(),
                up.clone(),
                freeze_below.clone(),
                kill_below.clone(),
                shutdown.clone(),
            );
            thread::spawn(move || relay(&a, &b, my_gen, &fb, &kb, &sd))
        };
        relay(&up, &down, my_gen, &freeze_below, &kill_below, &shutdown);
        d2u.join().ok();
        // Both directions have wound down (on kill or shutdown); dropping the last
        // socket handles here closes the fds, so the client sees a FIN.
        drop(down);
        drop(up);
    })
}

fn relay(
    from: &TcpStream,
    to: &TcpStream,
    my_gen: u64,
    freeze_below: &AtomicU64,
    kill_below: &AtomicU64,
    shutdown: &AtomicBool,
) {
    let mut buf = [0u8; 8192];
    loop {
        if shutdown.load(Ordering::Relaxed) || kill_below.load(Ordering::Relaxed) > my_gen {
            return;
        }
        if freeze_below.load(Ordering::Relaxed) > my_gen {
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        let mut reader: &TcpStream = from;
        match reader.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                if shutdown.load(Ordering::Relaxed)
                    || freeze_below.load(Ordering::Relaxed) > my_gen
                    || kill_below.load(Ordering::Relaxed) > my_gen
                {
                    continue;
                }
                let mut writer: &TcpStream = to;
                if writer.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue
            }
            Err(_) => return,
        }
    }
}

/// A relayed config with a short `connect_timeout` (so the derived liveness
/// window is small and the witness runs fast) and a server-side
/// `statement_timeout` (which ARMS the client-liveness window).
fn budgeted_config(port: u16) -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .port(port)
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
        .connect_timeout(2)
        .with_statement_timeout(Duration::from_millis(300))
}

/// The headline closure witness: a DIRECT connection with a server-side
/// `statement_timeout`, black-holed mid-query, elapses into a classified
/// `DriverError::Timeout` at its client-liveness window — never a `tcp_retries2`
/// hang — and the error is a disconnect (so a resilient consumer reconnects).
#[test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
fn direct_black_hole_query_is_bounded_with_statement_timeout() {
    let proxy = BlackHoleProxy::start();
    let mut conn =
        Connection::connect(&budgeted_config(proxy.port())).expect("connect through the relay");
    let warm = conn.query_one_sql("SELECT 'warm'").expect("warm query works");
    assert_eq!(warm.get_str(0), Ok(Some("warm")));

    // The peer VANISHES (frozen, ESTABLISHED-but-silent): the request and the
    // server's own `57014` abort are both black-holed, so nothing arrives.
    proxy.freeze_existing();
    let start = Instant::now();
    let got = conn.query_one_sql("SELECT 42");
    let elapsed = start.elapsed();

    match got {
        Err(e @ DriverError::Timeout) => assert!(
            e.is_disconnect(),
            "a fatal client-liveness timeout must classify as a disconnect",
        ),
        other => panic!("expected a classified DriverError::Timeout, got {other:?} after {elapsed:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(20),
        "a black-holed direct query must be BOUNDED by the liveness window, took {elapsed:?}",
    );

    drop(proxy);
}

/// The same closure on a POOLED connection's IN-FLIGHT query.
#[test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
fn pooled_inflight_black_hole_query_is_bounded_with_statement_timeout() {
    let proxy = BlackHoleProxy::start();
    let pool = Pool::new(budgeted_config(proxy.port()), 4);
    let mut c = pool.get().expect("check out a pooled connection");
    let conn = c.conn_mut().expect("borrow the checked-out connection");
    let warm = conn.query_one_sql("SELECT 'warm'").expect("warm query works");
    assert_eq!(warm.get_str(0), Ok(Some("warm")));

    proxy.freeze_existing();
    let start = Instant::now();
    let got = conn.query_one_sql("SELECT 42");
    let elapsed = start.elapsed();

    assert!(
        matches!(got, Err(DriverError::Timeout)),
        "a black-holed in-flight pooled query must be a classified Timeout, got {got:?} after {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "a black-holed pooled in-flight query must be BOUNDED, took {elapsed:?}",
    );

    drop(c);
    drop(proxy);
}

/// The fast-path bound that needs no query budget: a mid-query FIN (the peer
/// closes) is a classified disconnect in milliseconds, NOT a hang. Uses NO
/// `statement_timeout` — a transport close is caught by the read returning EOF.
#[test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
fn direct_query_recovers_bounded_on_mid_query_fin() {
    let proxy = BlackHoleProxy::start();
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .port(proxy.port())
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
        .connect_timeout(2);
    let mut conn = Connection::connect(&config).expect("connect through the relay");
    let warm = conn.query_one_sql("SELECT 'warm'").expect("warm query works");
    assert_eq!(warm.get_str(0), Ok(Some("warm")));

    // The peer closes mid-query: the client's read returns EOF at once.
    proxy.kill_existing();
    let start = Instant::now();
    let got = conn.query_one_sql("SELECT 42");
    let elapsed = start.elapsed();

    let err = got.expect_err("a peer that closed mid-query must be an error, not a torn success");
    assert!(
        err.is_disconnect(),
        "a mid-query FIN must classify as a disconnect, got {err:?}",
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "a mid-query FIN must be caught near-instantly, took {elapsed:?}",
    );

    drop(proxy);
}

// ── NEVER-FALSELY-CUT witnesses (no relay — a HEALTHY direct connection) ──────
//
// The blocking twins of the async never-false-cut suite: the window must NEVER
// cut a query the server allows.

/// Unique schema per migration witness so the `_bsql_migrations` ledger is
/// isolated (parallel `--ignored` runs never collide on it).
static SCHEMA_SEQ: AtomicU64 = AtomicU64::new(0);

fn healthy_config(statement_timeout: Duration, connect_timeout: u64) -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
        .connect_timeout(connect_timeout)
        .with_statement_timeout(statement_timeout)
}

/// An under-budget query on a windowed connection RETURNS.
#[test]
#[ignore = "requires local PG"]
fn under_budget_query_is_not_cut() {
    let mut conn = Connection::connect(&healthy_config(Duration::from_secs(5), 2)).expect("connect");
    let row = conn
        .query_one_sql("SELECT pg_sleep(1), 7::int4")
        .expect("an under-budget query must RETURN, never be client-cut");
    assert_eq!(row.get_i32(1), Ok(Some(7)));
}

/// An over-budget query on a HEALTHY connection is aborted by the SERVER (`57014`,
/// NOT a disconnect), never by the client window.
#[test]
#[ignore = "requires local PG"]
fn over_budget_query_hits_server_not_client_window() {
    let mut conn =
        Connection::connect(&healthy_config(Duration::from_millis(500), 10)).expect("connect");
    let start = Instant::now();
    let err = conn
        .query_one_sql("SELECT pg_sleep(3)")
        .expect_err("an over-budget query must be the SERVER's 57014, not a success");
    let elapsed = start.elapsed();
    assert!(
        !matches!(err, DriverError::Timeout) && !err.is_disconnect(),
        "an over-budget query on a HEALTHY peer must be the server's recoverable \
         57014 (not the client-window Timeout / a disconnect), got {err:?}",
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the server's 57014 must arrive first, took {elapsed:?}",
    );
    let row = conn.query_one_sql("SELECT 5::int4").expect("connection recovers");
    assert_eq!(row.get_i32(0), Ok(Some(5)));
}

/// THE headline MAJOR-1(b) witness: a runtime `SET statement_timeout` that RAISES
/// the budget is OBSERVED, so the following long query is NOT cut.
#[test]
#[ignore = "requires local PG"]
fn runtime_set_raising_the_budget_is_not_falsely_cut() {
    let mut conn =
        Connection::connect(&healthy_config(Duration::from_millis(300), 2)).expect("connect");
    conn.execute_sql("SET statement_timeout = '30s'").expect("raise the budget");
    let start = Instant::now();
    let row = conn
        .query_one_sql("SELECT pg_sleep(3), 9::int4")
        .expect("a runtime-raised budget must NOT be falsely client-cut");
    let elapsed = start.elapsed();
    assert_eq!(row.get_i32(1), Ok(Some(9)));
    assert!(
        elapsed >= Duration::from_secs(3),
        "the query must have actually run its full 3 s, took {elapsed:?}",
    );
}

/// A migration that disables the timeout for a long operation is NOT client-cut.
#[test]
#[ignore = "requires local PG"]
fn migration_long_op_is_not_client_cut() {
    use bsql_postgres_core::migrate::MigrationSource;
    let schema = format!(
        "bsql_dl_s_{}_{}",
        std::process::id(),
        SCHEMA_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let mut mon =
        Connection::connect(&healthy_config(Duration::from_secs(30), 5)).expect("monitor connect");
    mon.execute_sql(&format!("CREATE SCHEMA {schema}")).expect("create schema");

    let config = healthy_config(Duration::from_millis(300), 2).with_search_path(&schema);
    let mut conn = Connection::connect(&config).expect("connect");
    let migs = [("0001_slow", "SET LOCAL statement_timeout = 0;\nSELECT pg_sleep(3);")];
    let start = Instant::now();
    let report = conn
        .run_migrations(MigrationSource::embedded(&migs))
        .expect("a migration long op must NOT be client-cut");
    let elapsed = start.elapsed();
    assert_eq!(report.applied.len(), 1);
    assert!(
        elapsed >= Duration::from_secs(3),
        "the migration must have run its full 3 s sleep, took {elapsed:?}",
    );

    let row = conn.query_one_sql("SELECT 1::int4").expect("post-migration query");
    assert_eq!(row.get_i32(0), Ok(Some(1)));

    mon.execute_sql(&format!("DROP SCHEMA {schema} CASCADE")).expect("drop schema");
}
