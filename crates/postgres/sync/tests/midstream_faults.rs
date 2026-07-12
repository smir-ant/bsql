//! GATE (audit-8, --ignored live): the SYNC twin of the async mid-stream fault
//! matrix. A blocking stream that FAILS mid-flight never lies about success and
//! never hangs. For each fault class — a server error, a `cancel_token` (57014),
//! a transport death mid-result, and a `pg_terminate_backend` — the verb resolves
//! to a classified `Err` in BOUNDED time, the connection either drains to a clean
//! idle and is reusable OR classifies as a disconnect and is evicted, and rows
//! delivered before the fault are NEVER a successful result. A repeat loop proves
//! no leak accrues.
//!
//! (The async peer additionally covers a DROPPED streaming future; the blocking
//! driver has no future to drop, so that class is async-only.) Needs a local PG,
//! so `#[ignore]`.

use core::ops::ControlFlow;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

/// Real local PostgreSQL (trust auth, plaintext loopback).
const UPSTREAM: &str = "127.0.0.1:5432";

/// A direct plaintext config to local PG.
fn direct() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// A per-row-sleeping stream that ACTUALLY streams (a row every 50 ms), bounded
/// to 40 rows so even an un-cancelled worst case finishes in ~2 s (never a hang).
const SLOW_STREAM: &str = "SELECT n, pg_sleep(0.05) FROM generate_series(1, 40) AS t(n)";

/// A stream that raises a SERVER error partway: `1/(n-5)` divides by zero at
/// `n = 5` — SQLSTATE `22012`, a per-query error the connection SURVIVES.
const ERRORING_STREAM: &str = "SELECT 1 / (n - 5) FROM generate_series(1, 10) AS t(n)";

// ---------------------------------------------------------------------------
// (1) SERVER error mid-stream: the verb is Err, the connection recovers.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires local PG"]
fn server_error_mid_stream_is_err_and_connection_recovers() {
    let mut conn = Connection::connect(&direct()).expect("connect");

    let outcome = conn.query_each_sql::<_, ()>(ERRORING_STREAM, |_row| ControlFlow::Continue(()));

    let err = match outcome {
        Err(e) => e,
        Ok(_) => panic!("a mid-stream server error must fail the whole verb, not report success"),
    };
    match &err {
        DriverError::Db(db) => assert!(
            db.is_code("22012"),
            "division-by-zero must be 22012, got {}",
            db.code()
        ),
        other => panic!("expected a classified Db error, got {other:?}"),
    }
    assert!(!err.is_disconnect(), "a per-query server error is not a disconnect");
    assert!(conn.is_healthy(), "the connection must drain to a clean idle");

    let row = conn
        .query_one_sql("SELECT 42::int4")
        .expect("connection reusable after a mid-stream server error");
    assert_eq!(row.get_i32(0), Ok(Some(42)));
}

// ---------------------------------------------------------------------------
// (2) cancel_token mid-stream: classified 57014, NOT a disconnect, recovers.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires local PG"]
fn cancel_mid_stream_classifies_57014_and_recovers() {
    let mut conn = Connection::connect(&direct()).expect("connect");
    let token = conn.cancel_token();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        token.cancel()
    });

    let start = Instant::now();
    let outcome = conn.query_each_sql::<_, ()>(SLOW_STREAM, |_row| ControlFlow::Continue(()));
    let elapsed = start.elapsed();
    canceller.join().expect("cancel thread join").expect("cancel packet delivered");

    let err = match outcome {
        Err(e) => e,
        Ok(_) => panic!("a cancelled stream must fail, not run to completion"),
    };
    match &err {
        DriverError::Db(db) => assert!(
            db.is_code("57014"),
            "a cancelled stream must be 57014 query_canceled, got {}",
            db.code()
        ),
        other => panic!("cancel must surface as Db(57014), got {other:?}"),
    }
    assert!(!err.is_disconnect(), "a cancel is not a disconnect");
    assert!(elapsed < Duration::from_secs(5), "cancel must be bounded, took {elapsed:?}");
    assert!(conn.is_healthy(), "reusable after a cancel");
    let row = conn.query_one_sql("SELECT 7::int4").expect("reusable after cancel");
    assert_eq!(row.get_i32(0), Ok(Some(7)));
}

// ---------------------------------------------------------------------------
// (3) pg_terminate_backend mid-stream: classified disconnect, evictable.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires local PG"]
fn terminated_backend_mid_stream_is_a_disconnect() {
    let mut victim = Connection::connect(&direct()).expect("connect victim");
    let killer = Connection::connect(&direct()).expect("connect killer");
    let pid = victim.backend_pid();
    assert!(pid > 0, "victim backend pid");

    // Terminate the victim mid-stream from a background thread; return the killer
    // so the "no global corruption" check can reuse it.
    let terminator = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        let mut killer = killer;
        let t = killer
            .query_one_sql(&format!("SELECT pg_terminate_backend({pid})"))
            .expect("terminate the victim backend");
        assert_eq!(t.get_str(0), Ok(Some("t")), "pg_terminate_backend returned true");
        killer
    });
    let victim_res = victim.query_each_sql::<_, ()>(SLOW_STREAM, |_row| ControlFlow::Continue(()));
    let mut killer = terminator.join().expect("terminator thread joins");

    let err = match victim_res {
        Err(e) => e,
        Ok(_) => panic!("a terminated backend must fail the in-flight stream"),
    };
    assert!(
        err.is_disconnect(),
        "a terminated backend mid-stream must classify as a disconnect, got {err:?}",
    );
    let row = killer.query_one_sql("SELECT 1::int4").expect("killer healthy");
    assert_eq!(row.get_i32(0), Ok(Some(1)));
}

// ---------------------------------------------------------------------------
// (4) transport death mid-result (relay FINs the socket): classified.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires local PG (drop-relay in front of 127.0.0.1:5432)"]
fn transport_death_mid_stream_is_classified_not_a_hang() {
    let relay = DropRelay::start(2000);
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .port(relay.port())
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
        .connect_timeout(5);
    let mut conn = Connection::connect(&cfg).expect("connect through relay");

    let start = Instant::now();
    let outcome = conn.query_each_sql::<_, ()>(
        "SELECT n FROM generate_series(1, 500000) AS t(n)",
        |_row| ControlFlow::Continue(()),
    );
    let elapsed = start.elapsed();

    let err = match outcome {
        Err(e) => e,
        Ok(_) => panic!("the transport died mid-result — the verb cannot report success"),
    };
    assert!(
        err.is_disconnect(),
        "a mid-result transport death must classify as a disconnect, got {err:?}",
    );
    assert!(elapsed < Duration::from_secs(10), "must be bounded, took {elapsed:?}");
    drop(conn);
    drop(relay);
}

// ---------------------------------------------------------------------------
// (5) No leak / no lie under REPEATED mid-stream faults.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires local PG"]
fn no_leak_under_repeated_mid_stream_faults() {
    for i in 0..100 {
        let mut conn = Connection::connect(&direct()).expect("connect");
        let outcome = conn.query_each_sql::<_, ()>(ERRORING_STREAM, |_row| ControlFlow::Continue(()));
        assert!(outcome.is_err(), "round {i}: the erroring stream must fail");
        let row = match conn.query_one_sql("SELECT 1::int4") {
            Ok(r) => r,
            Err(e) => panic!("round {i}: connection must recover after the fault: {e:?}"),
        };
        assert_eq!(row.get_i32(0), Ok(Some(1)), "round {i}: recovered result");
    }
}

// ---------------------------------------------------------------------------
// A byte-transparent TCP relay to real PG that FINs the driver socket after
// `drop_after` server->client bytes — a transport that vanishes mid-stream.
// ---------------------------------------------------------------------------

struct DropRelay {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl DropRelay {
    #[expect(
        clippy::expect_used,
        reason = "test relay setup: a failed loopback bind is the loud harness-failure signal, and this is not a `#[test]` fn so the in-tests carve-out cannot reach it"
    )]
    fn start(drop_after: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind drop relay");
        let addr = listener.local_addr().expect("relay addr");
        listener.set_nonblocking(true).expect("relay nonblocking");
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let accept_thread = thread::spawn(move || {
            let mut handlers: Vec<JoinHandle<()>> = Vec::new();
            while !sd.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((down, _)) => handlers.push(drop_handle(down, drop_after, sd.clone())),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
            for h in handlers {
                h.join().ok();
            }
        });
        Self { addr, shutdown, accept_thread: Some(accept_thread) }
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }
}

impl Drop for DropRelay {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.accept_thread.take() {
            h.join().ok();
        }
    }
}

/// Forward both directions; once `drop_after` server->client bytes have flowed,
/// hard-`shutdown` BOTH sockets so the driver reads a real EOF mid-result.
fn drop_handle(down: TcpStream, drop_after: usize, shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::spawn(move || {
        let up = match TcpStream::connect(UPSTREAM) {
            Ok(s) => s,
            Err(_) => return,
        };
        down.set_read_timeout(Some(Duration::from_millis(100))).ok();
        up.set_read_timeout(Some(Duration::from_millis(100))).ok();
        let down = Arc::new(down);
        let up = Arc::new(up);
        let forwarded = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let c2s = {
            let (a, b, sd, st) = (down.clone(), up.clone(), shutdown.clone(), stop.clone());
            thread::spawn(move || copy_until(&a, &b, &sd, &st, None))
        };
        copy_until(&up, &down, &shutdown, &stop, Some((drop_after, &forwarded)));
        stop.store(true, Ordering::Relaxed);
        down.shutdown(std::net::Shutdown::Both).ok();
        up.shutdown(std::net::Shutdown::Both).ok();
        c2s.join().ok();
    })
}

/// Copy `from` → `to` until EOF/stop/shutdown. If `limit` is `Some((n, counter))`,
/// return once `counter` reaches `n` forwarded bytes (the caller then FINs).
fn copy_until(
    from: &TcpStream,
    to: &TcpStream,
    shutdown: &AtomicBool,
    stop: &AtomicBool,
    limit: Option<(usize, &AtomicUsize)>,
) {
    let mut buf = [0u8; 8192];
    loop {
        if shutdown.load(Ordering::Relaxed) || stop.load(Ordering::Relaxed) {
            return;
        }
        if let Some((n, counter)) = limit
            && counter.load(Ordering::Relaxed) >= n
        {
            return;
        }
        let mut reader: &TcpStream = from;
        match reader.read(&mut buf) {
            Ok(0) => return,
            Ok(read) => {
                let mut writer: &TcpStream = to;
                if writer.write_all(&buf[..read]).is_err() {
                    return;
                }
                if let Some((_, counter)) = limit {
                    counter.fetch_add(read, Ordering::Relaxed);
                }
            }
            Err(ref e)
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) =>
            {
                continue
            }
            Err(_) => return,
        }
    }
}
