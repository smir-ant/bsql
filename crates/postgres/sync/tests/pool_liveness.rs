//! WITNESS (R1): a pooled connection whose peer VANISHED SILENTLY — a half-open
//! socket where no FIN/RST ever arrives (a NAT idle-drop, a cable pull, an AZ
//! partition) — must NEVER hang `pool.get()`. The pool health-gates every reused
//! connection with a `reset_session` on checkout; before the fix that reset ran
//! an unbounded read, so on a vanished peer `get()` blocked inside the reset for
//! the kernel's `tcp_retries2` budget (~15 min) — a silent total-outage hang. The
//! fix arms the reset with the connection's `connect_timeout` socket read+write
//! timeout, so the reset ELAPSES into a classified error, the dead connection is
//! EVICTED, and the caller gets a FRESH connection (or a classified bounded error
//! if no fresh dial can open) — never a multi-minute hang.
//!
//! The half-open case cannot be faked by the in-memory testkit (its transport
//! never blocks) nor by killing a real backend (that sends a clean FIN — the
//! graceful path, already handled). It is reproduced faithfully by a byte-
//! transparent TCP relay to real PG that can FREEZE a connection mid-stream:
//! it stops forwarding both ways WITHOUT closing the socket, so the peer looks
//! ESTABLISHED-but-silent — exactly a vanished peer. Needs a local PG (the relay
//! forwards to `127.0.0.1:5432`), so `#[ignore]`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, Pool, PooledConnection, SslMode};

/// Real local PostgreSQL the relay forwards to (trust auth, plaintext).
const UPSTREAM: &str = "127.0.0.1:5432";

/// A byte-transparent TCP relay to real PG that can BLACK-HOLE selected
/// connections mid-stream: it freezes forwarding both ways WITHOUT closing the
/// socket, so the client sees a still-ESTABLISHED but silent peer — the exact
/// half-open (vanished-peer) case R1 is about. Connections are numbered by a
/// monotonic generation; freezing is scoped by generation so a FRESH dial can
/// still succeed while an older connection is frozen.
struct BlackHoleProxy {
    addr: SocketAddr,
    /// The next connection generation to hand out (monotonic).
    next_gen: Arc<AtomicU64>,
    /// A handler whose generation is `< freeze_below` stops forwarding.
    freeze_below: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl BlackHoleProxy {
    // Not a `#[test]` fn, so clippy's `allow-expect-in-tests` carve-out (keyed on
    // `#[test]` context) does not reach it; a failed listener bind is the loud
    // signal a live-test harness wants, never a production fallback.
    #[expect(
        clippy::expect_used,
        reason = "test relay setup: panics loudly if the loopback listener cannot bind — the intended harness-failure signal, and not a `#[test]` fn so the in-tests carve-out cannot reach it"
    )]
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy listener");
        let addr = listener.local_addr().expect("proxy addr");
        // Non-blocking accept so the loop can observe `shutdown` between polls.
        listener
            .set_nonblocking(true)
            .expect("proxy listener nonblocking");
        let next_gen = Arc::new(AtomicU64::new(0));
        let freeze_below = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (ng, fb, sd) = (next_gen.clone(), freeze_below.clone(), shutdown.clone());
        let accept_thread = thread::spawn(move || {
            let mut handlers: Vec<JoinHandle<()>> = Vec::new();
            while !sd.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((down, _)) => {
                        let g = ng.fetch_add(1, Ordering::Relaxed);
                        handlers.push(handle(down, g, fb.clone(), sd.clone()));
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
            shutdown,
            accept_thread: Some(accept_thread),
        }
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Freeze every connection open RIGHT NOW; connections opened AFTER this still
    /// forward normally (so the pool can dial a FRESH working one and recover).
    fn freeze_existing(&self) {
        self.freeze_below
            .store(self.next_gen.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    /// Freeze everything, present AND future — no fresh dial can succeed.
    fn freeze_all(&self) {
        self.freeze_below.store(u64::MAX, Ordering::Relaxed);
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

/// One accepted connection: dial upstream PG and relay both directions until
/// shutdown, freezing (black-holing) when this generation is frozen. Returns the
/// join handle for the direction thread it owns (the proxy joins it on shutdown).
fn handle(
    down: TcpStream,
    my_gen: u64,
    freeze_below: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let up = match TcpStream::connect(UPSTREAM) {
            Ok(s) => s,
            Err(_) => return,
        };
        // Blocking-with-timeout: each direction re-checks freeze/shutdown promptly.
        down.set_nonblocking(false).ok();
        down.set_read_timeout(Some(Duration::from_millis(100))).ok();
        up.set_read_timeout(Some(Duration::from_millis(100))).ok();
        let down = Arc::new(down);
        let up = Arc::new(up);
        let d2u = {
            let (a, b, fb, sd) = (down.clone(), up.clone(), freeze_below.clone(), shutdown.clone());
            thread::spawn(move || relay(&a, &b, my_gen, &fb, &sd))
        };
        relay(&up, &down, my_gen, &freeze_below, &shutdown);
        d2u.join().ok();
    })
}

/// Copy `from` → `to` until EOF/shutdown; while this generation is frozen, stop
/// forwarding but keep the socket OPEN (the vanished-peer half-open case).
fn relay(
    from: &TcpStream,
    to: &TcpStream,
    my_gen: u64,
    freeze_below: &AtomicU64,
    shutdown: &AtomicBool,
) {
    let mut buf = [0u8; 8192];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        if freeze_below.load(Ordering::Relaxed) > my_gen {
            // Black-hole: forward nothing, but hold the socket open (do not close)
            // so the peer stays silently ESTABLISHED. Poll shutdown to wind down.
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        let mut reader: &TcpStream = from;
        match reader.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                // Re-check AFTER the (possibly-blocked) read: the freeze may have
                // flipped while this read was in flight, and forwarding across it
                // would let a reset slip through (a race, not the fix). Once frozen,
                // DROP the just-read bytes — the peer is silent from here on.
                if shutdown.load(Ordering::Relaxed)
                    || freeze_below.load(Ordering::Relaxed) > my_gen
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

fn proxy_config(port: u16) -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .port(port)
        .database("postgres".to_string())
        // The relay is a plaintext byte pipe; local PG is trust — skip the TLS
        // probe and target the plaintext handshake + reset the fix bounds.
        .ssl_mode(SslMode::Disable)
        // A short connect budget so the reset's inherited liveness deadline is
        // small and the witness runs fast.
        .connect_timeout(2)
}

/// The headline witness: with a pooled connection's peer frozen mid-idle (a
/// vanished half-open peer), `pool.get()` EVICTS the dead connection and hands
/// out a FRESH working one — bounded, never a `tcp_retries2` hang.
#[test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
fn pool_get_recovers_a_fresh_connection_when_a_pooled_peer_vanishes() {
    let proxy = BlackHoleProxy::start();
    let pool = Pool::new(proxy_config(proxy.port()), 4);

    // Warm ONE connection through the relay (generation 0) and return it idle.
    {
        let mut c = pool
            .get()
            .expect("warm a pooled connection through the relay");
        let one = c
            .conn_mut()
            .expect("borrow warm connection")
            .query_one_raw("SELECT 'warm'")
            .expect("warm connection is a real, working PG connection");
        assert_eq!(one.get_str(0), Ok(Some("warm")));
    } // dropped → generation-0 returns to the idle set

    // The peer VANISHES: freeze generation 0 (its socket stays ESTABLISHED but
    // silent). A fresh dial (generation 1+) still forwards, so the pool recovers.
    proxy.freeze_existing();

    // BEFORE the fix: this blocks inside the health-reset for ~15 min.
    // AFTER: the reset's armed timeout elapses at ~connect_timeout, the dead
    // connection is evicted, and a fresh one is dialed and handed out.
    let start = Instant::now();
    let got = pool.get();
    let elapsed = start.elapsed();

    let mut fresh = got.expect("get() must recover with a FRESH connection, never hang");
    let row = fresh
        .conn_mut()
        .expect("borrow fresh connection")
        .query_one_raw("SELECT 'fresh'")
        .expect("the recovered connection is real and usable");
    assert_eq!(row.get_str(0), Ok(Some("fresh")));

    // Bounded: WAY under tcp_retries2 (~15 min), near the 2s connect budget
    // (generous slack for a loaded parallel run).
    assert!(
        elapsed < Duration::from_secs(20),
        "pool.get() must be BOUNDED on a vanished peer, took {elapsed:?}",
    );

    drop(fresh);
    drop(proxy);
}

/// The other branch: when the WHOLE endpoint has vanished (no fresh dial can
/// open either), `pool.get()` returns a CLASSIFIED bounded error — the reset
/// times out, then the fresh dial times out — never a hang.
#[test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
fn pool_get_is_bounded_when_no_fresh_connection_can_open() {
    let proxy = BlackHoleProxy::start();
    // A generous 30s acquire deadline proves the bound comes from the RESET
    // deadline, not the acquire timeout (the slot is free immediately).
    let pool = Pool::with_acquire_timeout(proxy_config(proxy.port()), 4, Duration::from_secs(30));

    {
        let mut c = pool.get().expect("warm a pooled connection");
        let one = c
            .conn_mut()
            .expect("borrow")
            .query_one_raw("SELECT 'warm'")
            .expect("warm connection works");
        assert_eq!(one.get_str(0), Ok(Some("warm")));
    } // generation 0 returns idle

    // The whole endpoint vanishes: existing AND any fresh dial black-hole.
    proxy.freeze_all();

    let start = Instant::now();
    let got = pool.get();
    let elapsed = start.elapsed();

    match got {
        Err(DriverError::Timeout | DriverError::PoolTimeout | DriverError::Io(_)) => {}
        Err(other) => panic!("expected a classified bounded error, got {other:?} after {elapsed:?}"),
        Ok(_) => panic!("no fresh dial can open — get() must fail, not hang, after {elapsed:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(25),
        "pool.get() must be BOUNDED even when no fresh connection can open, took {elapsed:?}",
    );

    drop(proxy);
}

/// A direct-to-PG config (no relay) for the graceful-close witness.
fn direct_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Count how many of `pids` are still present as live backends, via a monitor
/// connection independent of the pool.
#[expect(
    clippy::expect_used,
    reason = "test probe: a failed monitor query is the loud harness-failure signal, and this is not a `#[test]` fn so the in-tests carve-out cannot reach it"
)]
fn live_backends(mon: &mut Connection, pids: &[i32]) -> usize {
    let mut alive = 0;
    for &pid in pids {
        // `pid` is a trusted i32 from `pg_backend_pid()`, so splicing it is safe.
        let sql = format!("SELECT count(*)::int4 FROM pg_stat_activity WHERE pid = {pid}");
        let row = mon.query_one_raw(&sql).expect("stat_activity probe");
        if row.get_i32(0).ok().flatten() != Some(0) {
            alive += 1;
        }
    }
    alive
}

/// C2 WITNESS: `Pool::close` sends a protocol `Terminate` to every idle pooled
/// connection, so the server sees a CLEAN disconnect and the backends EXIT —
/// rather than a bare socket drop that leaves an "unexpected EOF on client
/// connection" in PG's log. Observed via `pg_stat_activity`.
#[test]
#[ignore = "requires local PG"]
fn pool_close_gracefully_terminates_idle_backends() {
    let pool = Pool::new(direct_config(), 4);

    let mut pids: Vec<i32> = Vec::new();
    let mut guards = Vec::new();
    for _ in 0..3 {
        let mut c = pool.get().expect("warm a pooled connection");
        let pid = c
            .conn_mut()
            .expect("borrow")
            .query_one_raw("SELECT pg_backend_pid()")
            .expect("read backend pid")
            .get_i32(0)
            .expect("pid decode")
            .expect("pid present");
        pids.push(pid);
        guards.push(c);
    }
    drop(guards); // all three return to the idle set

    let mut mon = Connection::connect(&direct_config()).expect("monitor connection");
    assert_eq!(
        live_backends(&mut mon, &pids),
        3,
        "the three idle backends must be alive before close",
    );

    pool.close();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let alive = live_backends(&mut mon, &pids);
        if alive == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pool.close() must terminate the idle backends; {alive} still alive",
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(mon);
}

/// C2 WITNESS (bounded): `Pool::close` cannot hang on a black-hole peer.
#[test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
fn pool_close_is_bounded_when_a_pooled_peer_is_black_holed() {
    let proxy = BlackHoleProxy::start();
    let pool = Pool::new(proxy_config(proxy.port()), 4);

    {
        let mut c = pool.get().expect("warm through the relay");
        let one = c
            .conn_mut()
            .expect("borrow")
            .query_one_raw("SELECT 'warm'")
            .expect("warm connection works");
        assert_eq!(one.get_str(0), Ok(Some("warm")));
    }

    proxy.freeze_existing();
    let start = Instant::now();
    pool.close();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(20),
        "pool.close() must be BOUNDED on a black-hole peer, took {elapsed:?}",
    );

    drop(proxy);
}

/// Read the server-side backend pid of a checked-out connection — the identity
/// that CHANGES when the pool reaps + replaces, and STAYS when it reuses.
#[expect(
    clippy::expect_used,
    reason = "test probe: a failed pid query is the loud harness-failure signal, and this is not a `#[test]` fn so the in-tests carve-out cannot reach it"
)]
fn pid_of(c: &mut PooledConnection) -> i32 {
    c.conn_mut()
        .expect("borrow")
        .query_one_raw("SELECT pg_backend_pid()")
        .expect("read backend pid")
        .get_i32(0)
        .expect("pid decode")
        .expect("pid present")
}

/// C4 WITNESS: a pooled connection older than `max_lifetime` is REAPED at
/// checkout and REPLACED with a fresh one (a new backend pid).
#[test]
#[ignore = "requires local PG"]
fn pool_reaps_a_connection_past_max_lifetime() {
    let pool = Pool::builder(direct_config(), 4)
        .max_lifetime(Some(Duration::from_millis(1)))
        .build();

    let pid1 = {
        let mut c = pool.get().expect("warm");
        pid_of(&mut c)
    };
    let evicted_before = pool.stats().connections_evicted;
    std::thread::sleep(Duration::from_millis(20));
    let pid2 = {
        let mut c = pool.get().expect("get after aging");
        pid_of(&mut c)
    };
    assert_ne!(pid1, pid2, "a connection past max_lifetime must be reaped + replaced");
    assert!(
        pool.stats().connections_evicted > evicted_before,
        "the reap must be counted as an eviction",
    );
}

/// C4 WITNESS: a pooled connection idle past `idle_timeout` is REAPED + replaced.
#[test]
#[ignore = "requires local PG"]
fn pool_reaps_a_connection_past_idle_timeout() {
    let pool = Pool::builder(direct_config(), 4)
        .idle_timeout(Some(Duration::from_millis(1)))
        .build();

    let pid1 = {
        let mut c = pool.get().expect("warm");
        pid_of(&mut c)
    };
    std::thread::sleep(Duration::from_millis(20));
    let pid2 = {
        let mut c = pool.get().expect("get after idling");
        pid_of(&mut c)
    };
    assert_ne!(pid1, pid2, "a connection idle past idle_timeout must be reaped + replaced");
}

/// C4 WITNESS (the negative): a connection WITHIN both bounds is REUSED at
/// checkout — same backend pid, nothing reaped.
#[test]
#[ignore = "requires local PG"]
fn pool_reuses_a_connection_within_limits() {
    let pool = Pool::builder(direct_config(), 4)
        .max_lifetime(Some(Duration::from_secs(3600)))
        .idle_timeout(Some(Duration::from_secs(3600)))
        .build();

    let pid1 = {
        let mut c = pool.get().expect("warm");
        pid_of(&mut c)
    };
    let evicted_before = pool.stats().connections_evicted;
    let pid2 = {
        let mut c = pool.get().expect("get within limits");
        pid_of(&mut c)
    };
    assert_eq!(pid1, pid2, "a connection within both bounds must be REUSED, not reaped");
    assert_eq!(
        pool.stats().connections_evicted,
        evicted_before,
        "nothing must be reaped within the limits",
    );
}
