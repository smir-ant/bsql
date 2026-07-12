//! LIVE WITNESS: SCRAM-SHA-256-PLUS channel binding against a REAL TLS + SCRAM
//! PostgreSQL — the live half of an invariant that was previously proven only
//! OFFLINE (the `ScramServer` fake in `bsql-postgres-proto`'s connect specs) and
//! verified out-of-band. The standard local PG has `ssl=off`, so the `-PLUS`
//! mechanism selection + the `tls-server-end-point` certificate-hash acceptance
//! was never exercised against a genuine server INSIDE the repo. This gate closes
//! that: it stands up its OWN ephemeral SSL + SCRAM PostgreSQL and drives the
//! driver's channel-binding policy end-to-end through it.
//!
//! What it proves, over a real end-to-end TLS + SCRAM handshake:
//!
//! - `channel_binding = Require` over TLS AUTHENTICATES. Real PostgreSQL only
//!   accepts a `Require` client if it sent a correct `SCRAM-SHA-256-PLUS` proof
//!   with the right `tls-server-end-point` cert hash: the driver's `Require` mode
//!   can send only `p=tls-server-end-point,,` (never plain `n,,`/`y,,`), and a PG
//!   built with SSL always offers `-PLUS` over an encrypted channel, so a `y,,`
//!   downgrade would be REJECTED (RFC 5802 §6) and a wrong hash would break the
//!   SCRAM signature. A green auth is therefore a machine proof that `-PLUS` was
//!   the selected mechanism AND the cert hash the driver computed matched the one
//!   real PG computed over the very certificate it presented. (The negotiated
//!   mechanism is not surfaced by the public `Connection` API — the auth-success
//!   proof is the strongest observable signal; the fake-server suite pins the
//!   `-PLUS`-vs-plain SELECTION byte-for-byte.)
//! - `channel_binding = Prefer` over the SAME TLS server also authenticates, so
//!   the default policy uses `-PLUS` when the server offers it.
//! - `channel_binding = Require` over a PLAINTEXT channel FAILS CLOSED with a
//!   classified `DriverError::Config` (a bound proof cannot exist without a cert),
//!   never a silent plaintext fallback.
//!
//! ## Non-flaky by construction
//!
//! Like the `tls_fragmentation` gate, this spins up its own ephemeral cluster (a
//! temp `initdb` with a self-signed CA -> leaf chain, `ssl=on`,
//! `password_encryption=scram-sha-256`, a `hostssl ... scram-sha-256` HBA line,
//! and a login role with a known password) and tears it down on drop (RAII, even
//! on panic). If any setup step cannot complete — `initdb` / `openssl` / `pg_ctl`
//! / `psql` not on `PATH`, or the process runs as root (postgres refuses root),
//! or the port is taken — it SKIPS CLEANLY (returns with a note), exactly like the
//! `--ignored` live suites skip without a database. So it is either the real thing
//! or a clean skip; never a false red.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bsql_postgres_async::{ChannelBindingMode, ConnectConfig, Connection, DriverError, SslMode};

/// The login role the witness authenticates as (SCRAM-SHA-256, `hostssl`).
const CB_ROLE: &str = "bsql_cb_test";
/// Its password. Pure ASCII, so RFC 4013 SASLprep is the identity — the bytes the
/// driver PBKDF2s match the verifier `CREATE ROLE ... PASSWORD` stored.
const CB_PASSWORD: &str = "bsql_cb_pw_123";

// ---------------------------------------------------------------------------
// Ephemeral SSL + SCRAM PostgreSQL — a self-contained temp cluster, torn down on
// drop. Mirrors the `tls_fragmentation` ephemeral SSL cluster, adding SCRAM
// password auth (a scram-encrypted login role + a `hostssl ... scram-sha-256`
// HBA line) so the driver negotiates SCRAM-SHA-256-PLUS.
// ---------------------------------------------------------------------------

/// A throwaway SSL + SCRAM PostgreSQL: a temp `initdb` data directory with a
/// self-signed CA -> leaf certificate chain (SAN `DNS:localhost,IP:127.0.0.1`),
/// `ssl=on`, `password_encryption=scram-sha-256`, a `hostssl` SCRAM HBA line, and
/// the [`CB_ROLE`] login role, listening on a free loopback port.
/// [`start`](Self::start) returns `None` (→ the test skips) if any setup step
/// fails, so a box without the PG tooling, or one running as root, never produces
/// a false failure. `Drop` stops the server and removes the temp tree.
struct EphemeralSslScramPg {
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

/// The OS user to run the ephemeral cluster as / bootstrap superuser name. Using
/// the current user makes the cluster self-contained. Falls back to `smir-ant`
/// (the suite's conventional local role) when `$USER` is unset.
fn cluster_user() -> String {
    match std::env::var("USER") {
        Ok(u) if !u.is_empty() => u,
        _ => "smir-ant".to_owned(),
    }
}

impl EphemeralSslScramPg {
    /// Try to build and start the cluster. `None` on any failure (→ skip).
    fn start() -> Option<Self> {
        // A free loopback port: bind :0, read it, release it, hand it to PG.
        let port = free_port()?;
        let user = cluster_user();

        // A unique temp root (pid + nanos) the current user can write.
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_nanos();
        let temp = std::env::temp_dir().join(format!("bsql_cb_sslpg_{}_{}", std::process::id(), nanos));
        let data = temp.join("data");
        std::fs::create_dir_all(&data).ok()?;

        let mut pg = Self {
            temp: temp.clone(),
            data: data.clone(),
            port,
            ca_pem: Vec::new(),
            started: false,
        };

        // initdb — the bootstrap superuser authenticates via the local unix socket
        // (`local all all trust`, below), which the witness uses only to create the
        // SCRAM login role. TCP is SSL + SCRAM.
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

        // ssl + scram config, and a scram HBA line for loopback TLS.
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

        // Create the SCRAM login role over the local (trust) unix socket. The
        // server's `password_encryption=scram-sha-256` makes `PASSWORD` store a
        // SCRAM verifier, so the `hostssl ... scram-sha-256` line can authenticate
        // it with channel binding.
        if !pg.create_scram_role() {
            return None;
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
            "-subj", "/CN=bsql-cb-ca",
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

        // Sign the leaf (default digest SHA-256, so the cert's signatureAlgorithm
        // is sha256WithRSAEncryption — the `tls-server-end-point` hash the driver
        // and PostgreSQL both compute is SHA-256).
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

    /// Append ssl + scram settings to `postgresql.conf` and write a fresh
    /// `pg_hba.conf`: `local` trust (for role creation over the unix socket) and
    /// `hostssl ... scram-sha-256` for loopback TLS (what the witness exercises).
    fn write_config(&self) -> Option<()> {
        let conf = self.data.join("postgresql.conf");
        let hba = self.data.join("pg_hba.conf");

        let mut conf_bytes = std::fs::read(&conf).ok()?;
        let sock_dir = data_str(&self.temp)?;
        conf_bytes.extend_from_slice(
            format!(
                "\nssl = on\nssl_cert_file = 'server.crt'\nssl_key_file = 'server.key'\n\
                 password_encryption = 'scram-sha-256'\n\
                 listen_addresses = '127.0.0.1'\nport = {}\nunix_socket_directories = '{}'\n",
                self.port, sock_dir
            )
            .as_bytes(),
        );
        std::fs::write(&conf, conf_bytes).ok()?;

        // A fresh, minimal HBA: the unix socket is trust (role bootstrap); every
        // TCP connection must be SSL and SCRAM-authenticated. A plaintext TCP
        // connect has no matching line — but the plaintext `Require` witness fails
        // CLOSED client-side before startup, so it never reaches the server anyway.
        let new_hba = b"local all all trust\n\
                        hostssl all all 127.0.0.1/32 scram-sha-256\n\
                        hostssl all all ::1/128 scram-sha-256\n"
            .to_vec();
        std::fs::write(&hba, new_hba).ok()?;
        Some(())
    }

    /// Create the [`CB_ROLE`] login role (with [`CB_PASSWORD`]) over the local
    /// trust unix socket via `psql`. With `password_encryption=scram-sha-256` the
    /// server stores a SCRAM verifier, so `hostssl ... scram-sha-256` can
    /// authenticate it. `false` (→ skip) if `psql` is absent or the role cannot be
    /// created.
    fn create_scram_role(&self) -> bool {
        let (Some(sock_dir), Some(user)) = (data_str(&self.temp), Some(cluster_user())) else {
            return false;
        };
        let sql = format!("CREATE ROLE {CB_ROLE} LOGIN PASSWORD '{CB_PASSWORD}'");
        ok_status(Command::new("psql").args([
            "-h",
            sock_dir,
            "-p",
            &self.port.to_string(),
            "-U",
            &user,
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &sql,
        ]))
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn ca_pem(&self) -> &[u8] {
        &self.ca_pem
    }
}

impl Drop for EphemeralSslScramPg {
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
// The witness.
// ---------------------------------------------------------------------------

/// Build the base config for the SCRAM login role at `port` (host `localhost` so
/// the leaf SAN matches; verified against the ephemeral CA).
fn cb_config(port: u16, ca_pem: &[u8]) -> ConnectConfig {
    ConnectConfig::new("localhost", CB_ROLE)
        .port(port)
        .database("postgres")
        .password(CB_PASSWORD)
        .with_ca_roots(ca_pem)
        .connect_timeout(30)
}

/// Over a REAL TLS + SCRAM PostgreSQL: `channel_binding=Require`/`Prefer`
/// authenticate via SCRAM-SHA-256-PLUS with the `tls-server-end-point` hash real
/// PG accepts; `Require` over plaintext fails CLOSED.
#[tokio::test]
#[ignore = "spins up an ephemeral SSL + SCRAM PostgreSQL (initdb + openssl + psql); skips cleanly if it can't start (incl. running as root)"]
async fn scram_sha_256_plus_authenticates_over_tls_and_fails_closed_on_plaintext() {
    let Some(pg) = EphemeralSslScramPg::start() else {
        eprintln!(
            "SKIP scram_sha_256_plus_authenticates_over_tls_and_fails_closed_on_plaintext: could \
             not start an ephemeral SSL + SCRAM PostgreSQL (initdb/openssl/pg_ctl/psql unavailable, \
             running as root, or port taken)"
        );
        return;
    };

    // (1) HEADLINE: channel_binding=Require over TLS AUTHENTICATES. Real PG only
    // accepts this if the client sent a correct SCRAM-SHA-256-PLUS proof with the
    // right tls-server-end-point cert hash (a `y,,`/`n,,` downgrade would be
    // rejected by a -PLUS-offering server; a wrong hash would break the signature).
    {
        let cfg = cb_config(pg.port(), pg.ca_pem())
            .ssl_mode(SslMode::Require)
            .channel_binding(ChannelBindingMode::Require);
        let mut conn = match Connection::connect(&cfg).await {
            Ok(c) => c,
            Err(e) => panic!(
                "channel_binding=Require over TLS must authenticate via SCRAM-SHA-256-PLUS \
                 (real PG accepted the tls-server-end-point hash): {e:?}"
            ),
        };
        assert!(
            conn.is_encrypted(),
            "the -PLUS connection MUST be real TLS (is_encrypted)",
        );
        // Prove the authenticated session is fully usable.
        let row = conn
            .query_one_sql("SELECT 1::int4")
            .await
            .expect("a query on the -PLUS-authenticated connection must succeed");
        assert_eq!(row.get_i32(0), Ok(Some(1)), "the session round-trips a value");
        // Prove the server sees us as the SCRAM login role (not the trust
        // superuser) — the SCRAM+channel-binding path really authenticated.
        let who = conn
            .query_one_sql("SELECT current_user::text")
            .await
            .expect("current_user query must succeed");
        assert_eq!(
            who.get_str(0),
            Ok(Some(CB_ROLE)),
            "the connection authenticated as the SCRAM login role",
        );
        drop(conn);
    }

    // (2) channel_binding=Prefer over the SAME TLS server also authenticates — the
    // default policy uses -PLUS when the server offers it (a PG built with SSL
    // always offers -PLUS over an encrypted channel).
    {
        let cfg = cb_config(pg.port(), pg.ca_pem())
            .ssl_mode(SslMode::Require)
            .channel_binding(ChannelBindingMode::Prefer);
        let mut conn = match Connection::connect(&cfg).await {
            Ok(c) => c,
            Err(e) => panic!("channel_binding=Prefer over TLS must authenticate: {e:?}"),
        };
        assert!(conn.is_encrypted(), "the Prefer connection MUST be real TLS");
        let row = conn
            .query_one_sql("SELECT 42::int4")
            .await
            .expect("a query on the Prefer-authenticated connection must succeed");
        assert_eq!(row.get_i32(0), Ok(Some(42)));
        drop(conn);
    }

    // (3) channel_binding=Require over a PLAINTEXT channel FAILS CLOSED with a
    // classified DriverError::Config — a bound proof cannot exist without a server
    // certificate, so the driver refuses BEFORE sending startup (never a silent
    // plaintext fallback). SslMode::Disable forces plaintext; the TCP connect to
    // the live port succeeds, then channel-binding resolution refuses client-side.
    {
        let cfg = cb_config(pg.port(), pg.ca_pem())
            .ssl_mode(SslMode::Disable)
            .channel_binding(ChannelBindingMode::Require)
            .connect_timeout(10);
        let err = match Connection::connect(&cfg).await {
            Ok(_) => panic!("channel_binding=Require over plaintext MUST fail closed, not connect"),
            Err(e) => e,
        };
        assert!(
            matches!(err, DriverError::Config(_)),
            "plaintext + channel_binding=Require must be a classified Config error, got {err:?}",
        );
        // The message names the cause (a bound proof needs TLS) — no silent fallback.
        let text = err.to_string();
        assert!(
            text.contains("channel_binding") && text.contains("TLS"),
            "the fail-closed error must name channel_binding + TLS, got: {text}",
        );
    }

    drop(pg);
}
