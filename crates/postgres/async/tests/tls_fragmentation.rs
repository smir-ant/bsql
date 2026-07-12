//! GATE (audit-8, the crown jewel): a TLS record split across arbitrarily many
//! socket reads — down to 1 byte per read — reassembles BYTE-EXACT or classifies;
//! it NEVER panics and NEVER hangs, and `is_encrypted()` stays `true`.
//!
//! This is the DIRECT regression net for the owner's prior production burn: a TLS
//! load-path panic. Ordinary happy-path tests connect over TLS to a well-behaved
//! kernel that hands the driver whole records, so the partial-record reassembly
//! path (the staging watermark + compaction in `core::tls`) is barely exercised.
//! Here a byte-level fragmenting TCP relay sits between the driver and a real
//! SSL PostgreSQL and flushes the server->client ciphertext stream 1 (and 3)
//! bytes at a time, so EVERY ~16 KiB TLS record arrives fragmented across dozens
//! of reads — the exact panic surface. The relay cannot decrypt TLS; it only
//! splits the raw TCP byte stream, which is precisely a hostile/degraded network
//! path.
//!
//! Through that relay the gate drives, over a genuine end-to-end TLS session:
//! - the TLS handshake itself (its bytes are fragmented too);
//! - a large MULTI-RECORD result (hundreds of rows x 200-char strings);
//! - a 300 KB single value that SPANS many TLS records (the double-reassembly:
//!   an oversize PG frame reassembled WHILE each TLS record is fragmented);
//! - a streamed run (`query_each_sql`, thousands of rows, strict in-order).
//!
//! Every result must be byte-exact, in order, with `is_encrypted() == true` and
//! zero panics.
//!
//! ## Non-flaky by construction
//!
//! The gate spins up its OWN ephemeral SSL PostgreSQL (a temp `initdb` cluster
//! with a self-signed CA -> leaf chain, `ssl=on`, on a free port) and tears it
//! down on drop (RAII, even on panic). If that setup cannot complete — `initdb`
//! / `openssl` / `pg_ctl` not on `PATH`, or the process runs as root (postgres
//! refuses root), or the port is taken — the gate SKIPS CLEANLY (returns with a
//! note), exactly like the `--ignored` live suites skip without a database. So it
//! is either the real thing or a clean skip; never a false red.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

// ---------------------------------------------------------------------------
// Ephemeral SSL PostgreSQL — a self-contained temp cluster, torn down on drop.
// ---------------------------------------------------------------------------

/// A throwaway SSL-enabled PostgreSQL: a temp `initdb` data directory with a
/// self-signed CA -> leaf certificate chain (SAN `DNS:localhost,IP:127.0.0.1`),
/// `ssl=on`, listening on a free loopback port. [`start`](Self::start) returns
/// `None` (→ the test skips) if any setup step fails, so a box without the PG
/// tooling, or one running as root, never produces a false failure. `Drop` stops
/// the server and removes the temp tree.
struct EphemeralSslPg {
    temp: PathBuf,
    data: PathBuf,
    port: u16,
    ca_pem: Vec<u8>,
    started: bool,
}

/// Run a command to completion, returning `true` only on a clean exit. Never
/// panics — a missing binary or a non-zero exit is a `false` (→ skip), the
/// loud-but-non-fatal signal a self-contained live gate wants.
fn ok_status(cmd: &mut Command) -> bool {
    match cmd.output() {
        Ok(out) => out.status.success(),
        Err(_) => false,
    }
}

/// The OS user to run the ephemeral cluster as / connect as. `initdb` needs the
/// bootstrap superuser name; using the current user makes the cluster
/// self-contained. Falls back to `smir-ant` (the suite's conventional local
/// role) when `$USER` is unset.
fn cluster_user() -> String {
    match std::env::var("USER") {
        Ok(u) if !u.is_empty() => u,
        _ => "smir-ant".to_owned(),
    }
}

impl EphemeralSslPg {
    /// Try to build and start the cluster. `None` on any failure (→ skip).
    fn start() -> Option<Self> {
        // A free loopback port: bind :0, read it, release it, hand it to PG.
        let port = free_port()?;
        let user = cluster_user();

        // A unique temp root (pid + nanos) the current user can write.
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_nanos();
        let temp = std::env::temp_dir().join(format!("bsql_a8_sslpg_{}_{}", std::process::id(), nanos));
        let data = temp.join("data");
        std::fs::create_dir_all(&data).ok()?;

        let mut pg = Self {
            temp: temp.clone(),
            data: data.clone(),
            port,
            ca_pem: Vec::new(),
            started: false,
        };

        // initdb (trust auth — the cert secures the channel, the role is trusted).
        if !ok_status(Command::new("initdb").args([
            "-D",
            data_str(&data)?,
            "-U",
            &user,
            "-A",
            "trust",
            "--no-locale",
            "--encoding=UTF8",
        ])) {
            return None; // pg drops -> temp removed
        }

        // Self-signed CA -> leaf chain with SAN. openssl only; no runtime dep.
        if !pg.make_certs() {
            return None;
        }

        // ssl config + a hostssl trust line for loopback.
        pg.write_config()?;

        // Start (pg_ctl -w waits for readiness, bounded to 30 s).
        let log = temp.join("pg.log");
        if !ok_status(Command::new("pg_ctl").args([
            "-D",
            data_str(&data)?,
            "-l",
            data_str(&log)?,
            "-w",
            "-t",
            "30",
            "start",
        ])) {
            return None;
        }
        pg.started = true;

        // Extra bounded readiness poll: a plaintext TCP connect must succeed.
        if !wait_tcp_ready(port, Duration::from_secs(10)) {
            return None; // pg drops -> stopped + removed
        }

        Some(pg)
    }

    /// Generate `ca.crt` + `ca.key` and, in the data dir, `server.crt` +
    /// `server.key` (chained to the CA, SAN `DNS:localhost,IP:127.0.0.1`,
    /// `serverAuth`). Caches the CA PEM for the driver's `with_ca_roots`.
    fn make_certs(&mut self) -> bool {
        let ca_key = self.temp.join("ca.key");
        let ca_crt = self.temp.join("ca.crt");
        let csr = self.temp.join("server.csr");
        let ext = self.temp.join("leaf.ext");
        let srv_key = self.data.join("server.key");
        let srv_crt = self.data.join("server.crt");

        let (Some(ca_key_s), Some(ca_crt_s), Some(csr_s), Some(ext_s), Some(srv_key_s), Some(srv_crt_s)) = (
            data_str(&ca_key),
            data_str(&ca_crt),
            data_str(&csr),
            data_str(&ext),
            data_str(&srv_key),
            data_str(&srv_crt),
        ) else {
            return false;
        };

        // CA.
        if !ok_status(Command::new("openssl").args([
            "req", "-new", "-x509", "-nodes", "-newkey", "rsa:2048",
            "-keyout", ca_key_s, "-out", ca_crt_s, "-days", "3650",
            "-subj", "/CN=bsql-audit8-ca",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        ])) {
            return false;
        }

        // Leaf key + CSR.
        if !ok_status(Command::new("openssl").args([
            "req", "-new", "-nodes", "-newkey", "rsa:2048",
            "-keyout", srv_key_s, "-out", csr_s, "-subj", "/CN=localhost",
        ])) {
            return false;
        }

        // Leaf signing extensions (basicConstraints/keyUsage/EKU/SAN).
        if std::fs::write(
            &ext,
            "basicConstraints=critical,CA:FALSE\n\
             keyUsage=critical,digitalSignature,keyEncipherment\n\
             extendedKeyUsage=serverAuth\n\
             subjectAltName=DNS:localhost,IP:127.0.0.1\n",
        )
        .is_err()
        {
            return false;
        }

        // Sign the leaf.
        if !ok_status(Command::new("openssl").args([
            "x509", "-req", "-in", csr_s, "-CA", ca_crt_s, "-CAkey", ca_key_s,
            "-CAcreateserial", "-out", srv_crt_s, "-days", "3650", "-extfile", ext_s,
        ])) {
            return false;
        }

        // postgres refuses a group/world-readable key.
        if set_key_perms(&srv_key).is_none() {
            return false;
        }

        match std::fs::read(&ca_crt) {
            Ok(pem) if !pem.is_empty() => {
                self.ca_pem = pem;
                true
            }
            _ => false,
        }
    }

    /// Append the ssl / listen settings to `postgresql.conf` and prepend a
    /// `hostssl ... trust` line to `pg_hba.conf`.
    fn write_config(&self) -> Option<()> {
        let conf = self.data.join("postgresql.conf");
        let hba = self.data.join("pg_hba.conf");

        let mut conf_bytes = std::fs::read(&conf).ok()?;
        let sock_dir = data_str(&self.temp)?;
        conf_bytes.extend_from_slice(
            format!(
                "\nssl = on\nssl_cert_file = 'server.crt'\nssl_key_file = 'server.key'\n\
                 listen_addresses = '127.0.0.1'\nport = {}\nunix_socket_directories = '{}'\n",
                self.port, sock_dir
            )
            .as_bytes(),
        );
        std::fs::write(&conf, conf_bytes).ok()?;

        let old_hba = std::fs::read(&hba).ok()?;
        let mut new_hba = b"hostssl all all 127.0.0.1/32 trust\n".to_vec();
        new_hba.extend_from_slice(&old_hba);
        std::fs::write(&hba, new_hba).ok()?;
        Some(())
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn ca_pem(&self) -> &[u8] {
        &self.ca_pem
    }
}

impl Drop for EphemeralSslPg {
    fn drop(&mut self) {
        if self.started && let Some(d) = data_str(&self.data) {
            Command::new("pg_ctl")
                .args(["-D", d, "-m", "immediate", "stop"])
                .output()
                .ok();
        }
        std::fs::remove_dir_all(&self.temp).ok();
    }
}

/// A path as `&str`, or `None` (a non-UTF-8 temp path — never happens under our
/// own ASCII names, but handled rather than unwrapped).
fn data_str(p: &std::path::Path) -> Option<&str> {
    p.to_str()
}

/// Restrict the server key to owner-only (0600) — postgres refuses a laxer key.
/// Uses `chmod` (no `unsafe`/libc); `None` on failure.
fn set_key_perms(key: &std::path::Path) -> Option<()> {
    let s = data_str(key)?;
    if ok_status(Command::new("chmod").args(["600", s])) {
        Some(())
    } else {
        None
    }
}

/// Poll a plaintext TCP connect to `127.0.0.1:port` until it succeeds or the
/// deadline elapses. `true` if the port became connectable (bounded).
fn wait_tcp_ready(port: u16, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// A free loopback TCP port (bind :0, read it, release). `None` on failure.
fn free_port() -> Option<u16> {
    let l = TcpListener::bind("127.0.0.1:0").ok()?;
    let p = l.local_addr().ok()?.port();
    drop(l);
    Some(p)
}

// ---------------------------------------------------------------------------
// Fragmenting TCP relay — faithful byte pipe, server->client split to `chunk`.
// ---------------------------------------------------------------------------

/// A byte-transparent TCP relay to the ephemeral SSL PG that FRAGMENTS the
/// server->client (inbound-to-the-driver) ciphertext: it forwards every byte
/// faithfully but writes them out in `chunk`-sized pieces, each flushed, over a
/// `TCP_NODELAY` socket — so every TLS record reaches the driver split across
/// many tiny reads. Client->server is passthrough (the burn is inbound
/// reassembly). It cannot decrypt TLS; it only splits the raw stream.
struct FragmentingRelay {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl FragmentingRelay {
    #[expect(
        clippy::expect_used,
        reason = "test relay setup: a failed loopback bind is the loud harness-failure signal, and this is not a `#[test]` fn so the in-tests carve-out cannot reach it"
    )]
    fn start(upstream_port: u16, chunk: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fragmenting relay");
        let addr = listener.local_addr().expect("relay addr");
        listener.set_nonblocking(true).expect("relay nonblocking");
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let accept_thread = thread::spawn(move || {
            let mut handlers: Vec<JoinHandle<()>> = Vec::new();
            while !sd.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((down, _)) => handlers.push(handle(down, upstream_port, chunk, sd.clone())),
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
        Self {
            addr,
            shutdown,
            accept_thread: Some(accept_thread),
        }
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }
}

impl Drop for FragmentingRelay {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.accept_thread.take() {
            h.join().ok();
        }
    }
}

/// One accepted connection: dial the ephemeral PG and pipe both directions until
/// EOF/shutdown — client->server passthrough, server->client fragmented.
fn handle(down: TcpStream, upstream_port: u16, chunk: usize, shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::spawn(move || {
        let up = match TcpStream::connect(("127.0.0.1", upstream_port)) {
            Ok(s) => s,
            Err(_) => return,
        };
        down.set_nonblocking(false).ok();
        down.set_read_timeout(Some(Duration::from_millis(100))).ok();
        up.set_read_timeout(Some(Duration::from_millis(100))).ok();
        // Small writes must go out as their own segments, not Nagle-coalesced.
        down.set_nodelay(true).ok();
        let down = Arc::new(down);
        let up = Arc::new(up);
        let c2s = {
            let (a, b, sd) = (down.clone(), up.clone(), shutdown.clone());
            thread::spawn(move || passthrough(&a, &b, &sd))
        };
        // server -> client, fragmented into `chunk`-byte flushed writes.
        fragment(&up, &down, chunk, &shutdown);
        c2s.join().ok();
    })
}

/// Copy `from` → `to` faithfully (whole reads written whole) until EOF/shutdown.
fn passthrough(from: &TcpStream, to: &TcpStream, shutdown: &AtomicBool) {
    let mut buf = [0u8; 16384];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        let mut reader: &TcpStream = from;
        match reader.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                let mut writer: &TcpStream = to;
                if writer.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(ref e) if would_block(e) => continue,
            Err(_) => return,
        }
    }
}

/// Copy `from` → `to` but split every read into `chunk`-byte flushed writes, so
/// the driver reassembles each TLS record from many tiny inbound reads.
fn fragment(from: &TcpStream, to: &TcpStream, chunk: usize, shutdown: &AtomicBool) {
    let mut buf = [0u8; 16384];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        let mut reader: &TcpStream = from;
        match reader.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                let mut writer: &TcpStream = to;
                for piece in buf[..n].chunks(chunk) {
                    if writer.write_all(piece).is_err() {
                        return;
                    }
                    if writer.flush().is_err() {
                        return;
                    }
                }
            }
            Err(ref e) if would_block(e) => continue,
            Err(_) => return,
        }
    }
}

fn would_block(e: &std::io::Error) -> bool {
    matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
}

// ---------------------------------------------------------------------------
// The gate.
// ---------------------------------------------------------------------------

/// Connect the async driver to the ephemeral SSL PG THROUGH the fragmenting
/// relay, over a real TLS session verified against the ephemeral CA.
#[expect(
    clippy::expect_used,
    reason = "test helper: a failed TLS connect is the loud harness-failure signal, and this is not a `#[test]` fn so the in-tests carve-out cannot reach it"
)]
async fn connect_through(relay: &FragmentingRelay, ca_pem: &[u8]) -> Connection {
    let cfg = ConnectConfig::new("localhost", cluster_user())
        .port(relay.port())
        .database("postgres".to_string())
        .ssl_mode(SslMode::Require)
        .with_ca_roots(ca_pem)
        // A generous connect budget: the handshake itself is fragmented byte-by-
        // byte, so it is slow but bounded.
        .connect_timeout(30);
    Connection::connect(&cfg)
        .await
        .expect("TLS handshake + connect must succeed through the fragmenting relay")
}

/// The headline witness: over a byte-fragmented TLS channel, every result
/// reassembles byte-exact, in order, encrypted — zero panics, zero hangs.
#[tokio::test]
#[ignore = "spins up an ephemeral SSL PostgreSQL (initdb + openssl); skips cleanly if it can't start"]
async fn tls_record_fragmentation_reassembles_byte_exact() {
    let Some(pg) = EphemeralSslPg::start() else {
        eprintln!(
            "SKIP tls_record_fragmentation_reassembles_byte_exact: could not start an ephemeral \
             SSL PostgreSQL (initdb/openssl/pg_ctl unavailable, running as root, or port taken)"
        );
        return;
    };

    // Two fragmentation granularities: 1 byte (the harshest) and 3 bytes.
    for chunk in [1usize, 3] {
        let relay = FragmentingRelay::start(pg.port(), chunk);
        let mut conn = connect_through(&relay, pg.ca_pem()).await;

        assert!(
            conn.is_encrypted(),
            "chunk={chunk}: the connection MUST be real TLS (is_encrypted)",
        );

        // (a) A large MULTI-RECORD result: 400 rows x a 200-char string. The
        // reply is well past one 16 KiB TLS record, so it spans many records,
        // each fragmented across tiny reads.
        let rows = conn
            .query_sql("SELECT g AS n, repeat('x', 200) AS s FROM generate_series(1, 400) AS g")
            .await
            .expect("large multi-record result must reassemble, not panic/hang");
        assert_eq!(rows.len(), 400, "chunk={chunk}: every row must arrive");
        for i in 0..rows.len() {
            let row = rows.get(i).expect("row present");
            let n = row.get_i32(0).expect("n decodes").expect("n present");
            let s = row.get_str(1).expect("s decodes").expect("s present");
            assert_eq!(usize::try_from(n).ok(), Some(i + 1), "chunk={chunk}: row {i} order");
            assert_eq!(s.len(), 200, "chunk={chunk}: row {i} string length exact");
            assert!(s.bytes().all(|b| b == b'x'), "chunk={chunk}: row {i} content intact");
        }

        // (b) A 300 KB single value SPANNING many TLS records — the double
        // reassembly (an oversize PG frame rebuilt WHILE each TLS record is
        // fragmented). The exact byte-length is the strong check.
        let big = conn
            .query_one_sql("SELECT repeat('y', 300000)")
            .await
            .expect("record-spanning 300 KB value must reassemble byte-exact");
        let s = big.get_str(0).expect("big decodes").expect("big present");
        assert_eq!(s.len(), 300_000, "chunk={chunk}: 300 KB value length byte-exact");
        assert!(s.bytes().all(|b| b == b'y'), "chunk={chunk}: 300 KB value content intact");

        // (c) A streamed run: 2000 rows via `query_each_sql`, strict in-order,
        // accumulating nothing — the constant-memory streaming reassembly.
        use core::ops::ControlFlow;
        let mut next: i64 = 1;
        let mut streamed: i64 = 0;
        let outcome = conn
            .query_each_sql::<_, String>("SELECT generate_series(1, 2000)::int8 AS n", |row| {
                let got = match row.get_i64(0) {
                    Ok(Some(v)) => v,
                    other => return ControlFlow::Break(format!("bad streamed cell: {other:?}")),
                };
                if got != next {
                    return ControlFlow::Break(format!("out of order: expected {next}, got {got}"));
                }
                next += 1;
                streamed += 1;
                ControlFlow::Continue(())
            })
            .await
            .expect("streamed run must reassemble every row, not panic/hang");
        assert!(outcome.is_none(), "chunk={chunk}: stream must not break early: {outcome:?}");
        assert_eq!(streamed, 2000, "chunk={chunk}: every streamed row arrives in order");

        // Still encrypted, still usable after the fragmented workload.
        assert!(conn.is_encrypted(), "chunk={chunk}: still TLS after the workload");
        let follow = conn
            .query_one_sql("SELECT 42::int4")
            .await
            .expect("connection reusable after the fragmented workload");
        assert_eq!(follow.get_i32(0), Ok(Some(42)));

        drop(conn);
        drop(relay);
    }

    drop(pg);
}
