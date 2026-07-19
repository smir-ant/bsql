//! LIVE WITNESS: server-driven password authentication against a REAL
//! PostgreSQL — the live half of the mechanism-agnostic `Credentials::Password`
//! path proven OFFLINE in `bsql-postgres-proto`'s `engine_connect_spec`
//! (the `password_credential_*` differential tests over the `ScramServer` /
//! `CapturingServer` fakes). The standard local PG uses one auth method, so the
//! MD5 + cleartext paths — and the load-bearing raw-vs-SASLprep distinction —
//! were never exercised end-to-end against a genuine server inside the repo.
//!
//! What it proves, over a real end-to-end handshake:
//!
//! - **MD5 login SUCCEEDS** (`md5` pg_hba rule / `password_encryption=md5`). The
//!   driver builds an MD5 `PasswordMessage` from the RAW password; a real
//!   PostgreSQL MD5 verifier only accepts it if the client digested the same
//!   bytes PG stored. This exercises the `md5-auth` capability the driver could
//!   NEVER reach before (it only ever built a SCRAM credential).
//! - **A NON-ASCII password round-trips under BOTH SCRAM and MD5.** The SAME
//!   literal password (`pa\u{00A0}ss`, a non-breaking space) is set for an MD5
//!   role AND a SCRAM role. Its SASLprep form (`pa ss`, RFC 4013 maps `U+00A0`→
//!   space) DIFFERS from its raw bytes, so a role authenticates ONLY if the
//!   client picks the RIGHT form per mechanism: MD5 uses the RAW bytes (PG's md5
//!   verifier is over the raw password), SCRAM uses the SASLprep form (PG's SCRAM
//!   verifier is over `SASLprep(password)`). Both succeeding is the LIVE proof of
//!   the raw-vs-prepped correctness the offline byte-differential pins.
//! - **A cleartext challenge over a PLAINTEXT channel FAILS CLOSED** with a
//!   classified `DriverError::Config` (`password` pg_hba rule over an unencrypted
//!   connection) — a cleartext password is never sent in the clear.
//!
//! ## Non-flaky by construction
//!
//! Like `channel_binding_plus`, this spins up its OWN ephemeral cluster (a temp
//! `initdb`, plaintext loopback — MD5 / SCRAM / cleartext all work without TLS —
//! with `md5` / `scram-sha-256` / `password` HBA roles) and tears it down on drop
//! (RAII, even on panic). If any setup step cannot complete — `initdb` / `pg_ctl`
//! / `psql` not on `PATH`, or the process runs as root (postgres refuses root),
//! or the port is taken — it SKIPS CLEANLY (returns with a note), exactly like the
//! `--ignored` live suites skip without a database. So it is either the real thing
//! or a clean skip; never a false red.

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

/// A NON-ASCII password whose SASLprep form DIFFERS from its raw bytes: the
/// non-breaking space `U+00A0` (UTF-8 `c2 a0`) maps to a plain space under RFC
/// 4013 SASLprep. So MD5 (raw bytes `pa\xc2\xa0ss`) and SCRAM (SASLprepped
/// `pa ss`) verify against DIFFERENT stored forms — a role authenticates only if
/// the client selects the correct form for the challenge it received.
const NON_ASCII_PW: &str = "pa\u{00A0}ss";
/// The MD5-authenticated login role.
const MD5_ROLE: &str = "bsql_md5_role";
/// The SCRAM-authenticated login role (same password as MD5).
const SCRAM_ROLE: &str = "bsql_scram_role";
/// The cleartext (`password` HBA) login role — used only to make the server
/// CHALLENGE cleartext, which the driver refuses over a plaintext channel.
const CLEARTEXT_ROLE: &str = "bsql_cleartext_role";

// ---------------------------------------------------------------------------
// Ephemeral plaintext PostgreSQL — a self-contained temp cluster, torn down on
// drop. Plaintext loopback only (no SSL): MD5, SCRAM, and cleartext all work
// without TLS, so no certs are needed.
// ---------------------------------------------------------------------------

/// A throwaway plaintext PostgreSQL: a temp `initdb` data directory listening on
/// a free loopback port, with `md5` / `scram-sha-256` / `password` HBA login
/// roles. [`start`](Self::start) returns `None` (→ the test skips) if any setup
/// step fails. `Drop` stops the server and removes the temp tree.
struct EphemeralPg {
    temp: PathBuf,
    data: PathBuf,
    port: u16,
    started: bool,
}

/// Run a command to completion, returning `true` only on a clean exit. Never
/// panics — a missing binary or a non-zero exit is a `false` (→ skip).
fn ok_status(cmd: &mut Command) -> bool {
    match cmd.output() {
        Ok(out) => out.status.success(),
        Err(_) => false,
    }
}

/// The bootstrap superuser name (the current OS user; postgres runs as it).
fn cluster_user() -> String {
    match std::env::var("USER") {
        Ok(u) if !u.is_empty() => u,
        _ => "smir-ant".to_owned(),
    }
}

impl EphemeralPg {
    /// Try to build and start the cluster. `None` on any failure (→ skip).
    fn start() -> Option<Self> {
        let port = free_port()?;
        let user = cluster_user();

        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_nanos();
        let temp = std::env::temp_dir().join(format!("bsql_md5_pg_{}_{}", std::process::id(), nanos));
        let data = temp.join("data");
        std::fs::create_dir_all(&data).ok()?;

        let mut pg = Self {
            temp: temp.clone(),
            data: data.clone(),
            port,
            started: false,
        };

        // initdb — the bootstrap superuser authenticates via the local unix socket
        // (`local all all trust`, written below), used only to create the roles.
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
            return None;
        }

        pg.write_config()?;

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

        if !wait_tcp_ready(port, Duration::from_secs(10)) {
            return None;
        }

        pg.create_roles()?;
        Some(pg)
    }

    /// Append the listen/port/socket settings to `postgresql.conf` and write a
    /// fresh `pg_hba.conf`: `local` trust (role bootstrap) plus one per-role TCP
    /// line for each mechanism the witness exercises.
    fn write_config(&self) -> Option<()> {
        let conf = self.data.join("postgresql.conf");
        let hba = self.data.join("pg_hba.conf");

        let mut conf_bytes = std::fs::read(&conf).ok()?;
        let sock_dir = data_str(&self.temp)?;
        conf_bytes.extend_from_slice(
            format!(
                "\nlisten_addresses = '127.0.0.1'\nport = {}\nunix_socket_directories = '{}'\n",
                self.port, sock_dir
            )
            .as_bytes(),
        );
        std::fs::write(&conf, conf_bytes).ok()?;

        // Per-role plaintext TCP lines: md5 / scram-sha-256 / cleartext (password).
        let new_hba = format!(
            "local all all trust\n\
             host all {MD5_ROLE} 127.0.0.1/32 md5\n\
             host all {SCRAM_ROLE} 127.0.0.1/32 scram-sha-256\n\
             host all {CLEARTEXT_ROLE} 127.0.0.1/32 password\n\
             host all all 127.0.0.1/32 trust\n"
        );
        std::fs::write(&hba, new_hba.as_bytes()).ok()?;
        Some(())
    }

    /// Create the three login roles over the local trust unix socket. The MD5 and
    /// SCRAM roles share the SAME [`NON_ASCII_PW`], each stored under the matching
    /// `password_encryption` so the verifiers differ (md5-over-raw vs
    /// scram-over-SASLprep) — the crux of the raw-vs-prepped live proof.
    fn create_roles(&self) -> Option<()> {
        let sock_dir = data_str(&self.temp)?;
        let user = cluster_user();
        // One psql invocation, so the roles are created atomically-enough for a
        // throwaway cluster. `password_encryption` is SET per role before its
        // CREATE, so PG stores the matching verifier form.
        let sql = format!(
            "SET password_encryption = 'md5'; \
             CREATE ROLE {MD5_ROLE} LOGIN PASSWORD '{NON_ASCII_PW}'; \
             SET password_encryption = 'scram-sha-256'; \
             CREATE ROLE {SCRAM_ROLE} LOGIN PASSWORD '{NON_ASCII_PW}'; \
             CREATE ROLE {CLEARTEXT_ROLE} LOGIN PASSWORD 'cleartext_pw_123';"
        );
        let mut cmd = Command::new("psql");
        cmd.env("PGCLIENTENCODING", "UTF8").args([
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
        ]);
        ok_status(&mut cmd).then_some(())
    }

    fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for EphemeralPg {
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

fn data_str(p: &Path) -> Option<&str> {
    p.to_str()
}

/// Poll a plaintext TCP connect until it succeeds or the deadline elapses.
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

/// A free loopback TCP port (bind :0, read it, release).
fn free_port() -> Option<u16> {
    let l = TcpListener::bind("127.0.0.1:0").ok()?;
    let p = l.local_addr().ok()?.port();
    drop(l);
    Some(p)
}

// ---------------------------------------------------------------------------
// The witness.
// ---------------------------------------------------------------------------

/// Base config for `role` at `port` over plaintext loopback (SslMode::Disable so
/// the challenge arrives on an UNENCRYPTED channel — the case that gates cleartext
/// and exercises MD5 without TLS).
fn plaintext_config(port: u16, role: &str, password: &str) -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", role)
        .port(port)
        .database("postgres")
        .password(password)
        .ssl_mode(SslMode::Disable)
        .connect_timeout(15)
}

#[tokio::test]
#[ignore = "spins up an ephemeral PostgreSQL (initdb + psql); skips cleanly if it can't start (incl. running as root)"]
async fn server_driven_password_auth_md5_scram_and_cleartext_refusal() {
    let Some(pg) = EphemeralPg::start() else {
        eprintln!(
            "SKIP server_driven_password_auth_md5_scram_and_cleartext_refusal: could not start an \
             ephemeral PostgreSQL (initdb/pg_ctl/psql unavailable, running as root, or port taken)"
        );
        return;
    };

    // (1) HEADLINE: MD5 login SUCCEEDS with the NON-ASCII password. The driver
    // digests the RAW password bytes; real PG's md5 verifier (stored over the raw
    // password) only accepts a matching digest. This is the `md5-auth` capability
    // the driver could never reach before this fix.
    {
        let cfg = plaintext_config(pg.port(), MD5_ROLE, NON_ASCII_PW);
        let mut conn = match Connection::connect(&cfg).await {
            Ok(c) => c,
            Err(e) => panic!(
                "MD5 login with a non-ASCII password must SUCCEED (the driver must digest the RAW \
                 bytes PG's md5 verifier stored): {e:?}"
            ),
        };
        assert!(!conn.is_encrypted(), "the MD5 login is over plaintext");
        let who = conn
            .query_one_raw("SELECT current_user::text")
            .await
            .expect("current_user on the MD5-authenticated connection must succeed");
        assert_eq!(
            who.get_str(0),
            Ok(Some(MD5_ROLE)),
            "the connection authenticated as the MD5 login role (not the trust superuser)",
        );
    }

    // (2) The SAME non-ASCII password authenticates under SCRAM. PG's SCRAM
    // verifier is stored over SASLprep(password) = "pa ss", so this succeeds ONLY
    // if the driver SASLprepped the password (used the PREPPED form, not raw).
    // Together with (1) — MD5 using RAW — this proves each mechanism selects the
    // correct form of the SAME literal password.
    {
        let cfg = plaintext_config(pg.port(), SCRAM_ROLE, NON_ASCII_PW);
        let mut conn = match Connection::connect(&cfg).await {
            Ok(c) => c,
            Err(e) => panic!(
                "SCRAM login with the SAME non-ASCII password must SUCCEED (the driver must SASLprep \
                 it to the form PG's SCRAM verifier stored): {e:?}"
            ),
        };
        let who = conn
            .query_one_raw("SELECT current_user::text")
            .await
            .expect("current_user on the SCRAM-authenticated connection must succeed");
        assert_eq!(who.get_str(0), Ok(Some(SCRAM_ROLE)));
    }

    // (3) A cleartext challenge over a PLAINTEXT channel FAILS CLOSED. The server
    // sends AuthenticationCleartextPassword (the `password` HBA rule); the driver
    // REFUSES to send a cleartext password in the clear — a classified
    // DriverError::Config, never the password on the wire.
    {
        let cfg = plaintext_config(pg.port(), CLEARTEXT_ROLE, "cleartext_pw_123");
        let err = match Connection::connect(&cfg).await {
            Ok(_) => panic!(
                "a cleartext challenge over a plaintext channel MUST be refused, not connected"
            ),
            Err(e) => e,
        };
        assert!(
            matches!(err, DriverError::Config(_)),
            "cleartext over plaintext must be a classified Config error, got {err:?}",
        );
        let text = err.to_string();
        assert!(
            text.contains("cleartext") && (text.contains("TLS") || text.contains("unencrypted")),
            "the fail-closed error must name cleartext + the unencrypted channel, got: {text}",
        );
    }

    drop(pg);
}
