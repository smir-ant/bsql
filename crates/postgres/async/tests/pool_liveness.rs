//! WITNESS (R1): a pooled connection whose peer VANISHED SILENTLY — a half-open
//! socket where no FIN/RST ever arrives (a NAT idle-drop, a cable pull, an AZ
//! partition) — must NEVER hang `pool.get()`. The pool health-gates every reused
//! connection with a `reset_session` on checkout; before the fix that reset ran
//! an unbounded read, so on a vanished peer `get()` blocked inside the reset for
//! the kernel's `tcp_retries2` budget (~15 min) — a silent total-outage hang. The
//! fix arms the reset with the connection's `connect_timeout` deadline, so the
//! reset ELAPSES into a classified error, the dead connection is EVICTED, and the
//! caller gets a FRESH connection (or a classified bounded error if no fresh dial
//! can open) — never a multi-minute hang.
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

use bsql_postgres_async::{ConnectConfig, DriverError, Pool, SslMode};

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
#[tokio::test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
async fn pool_get_recovers_a_fresh_connection_when_a_pooled_peer_vanishes() {
    let proxy = BlackHoleProxy::start();
    let pool = Pool::new(proxy_config(proxy.port()), 4);

    // Warm ONE connection through the relay (generation 0) and return it idle.
    {
        let mut c = pool
            .get()
            .await
            .expect("warm a pooled connection through the relay");
        let one = c
            .conn_mut()
            .expect("borrow warm connection")
            .query_one_sql("SELECT 'warm'")
            .await
            .expect("warm connection is a real, working PG connection");
        assert_eq!(one.get_str(0), Ok(Some("warm")));
    } // dropped → generation-0 returns to the idle set

    // The peer VANISHES: freeze generation 0 (its socket stays ESTABLISHED but
    // silent). A fresh dial (generation 1+) still forwards, so the pool recovers.
    proxy.freeze_existing();

    // BEFORE the fix: this blocks inside the health-reset for ~15 min.
    // AFTER: the reset's armed deadline elapses at ~connect_timeout, the dead
    // connection is evicted, and a fresh one is dialed and handed out.
    let start = Instant::now();
    let got = pool.get().await;
    let elapsed = start.elapsed();

    let mut fresh = got.expect("get() must recover with a FRESH connection, never hang");
    let row = fresh
        .conn_mut()
        .expect("borrow fresh connection")
        .query_one_sql("SELECT 'fresh'")
        .await
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
#[tokio::test]
#[ignore = "requires local PG (black-hole relay in front of 127.0.0.1:5432)"]
async fn pool_get_is_bounded_when_no_fresh_connection_can_open() {
    let proxy = BlackHoleProxy::start();
    // A generous 30s acquire deadline proves the bound comes from the RESET
    // deadline, not the acquire timeout (the permit is free immediately).
    let pool = Pool::with_acquire_timeout(proxy_config(proxy.port()), 4, Duration::from_secs(30));

    {
        let mut c = pool.get().await.expect("warm a pooled connection");
        let one = c
            .conn_mut()
            .expect("borrow")
            .query_one_sql("SELECT 'warm'")
            .await
            .expect("warm connection works");
        assert_eq!(one.get_str(0), Ok(Some("warm")));
    } // generation 0 returns idle

    // The whole endpoint vanishes: existing AND any fresh dial black-hole.
    proxy.freeze_all();

    let start = Instant::now();
    let got = pool.get().await;
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
