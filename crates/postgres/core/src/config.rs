//! Connection configuration: [`ConnectConfig`], [`SslMode`], endpoint
//! resolution ([`resolve_endpoint`]), and startup-parameter validation.

use std::sync::Arc;
use std::time::Duration;

/// Build the classified [`DriverError::ConfigDynamic`](crate::DriverError) that
/// carries a runtime-computed DSN / environment parse message (one that names its
/// offending value, so it cannot be a `&'static str`). Centralized so every
/// pre-connect parse failure routes through the SAME classified carrier — a
/// consumer matches [`DriverError`](crate::DriverError) (or
/// [`is_config`](crate::DriverError::is_config)) uniformly, never a bare `String`.
fn config_error(msg: impl Into<Box<str>>) -> crate::DriverError {
    crate::DriverError::ConfigDynamic(msg.into())
}

/// SSL negotiation mode.
///
/// # The default is threat-scoped, not a fixed value
///
/// A [`ConnectConfig`] does NOT store a fixed default `SslMode`. When the
/// consumer sets none, the effective mode is resolved at connect against the
/// endpoint by [`ConnectConfig::resolve_ssl_mode`], scoped to where the
/// interception threat actually exists — a network path:
///
/// - a LOCAL endpoint (a unix-domain socket, or a loopback TCP host —
///   `localhost`, `127.0.0.0/8`, `::1`) resolves to [`Prefer`](Self::Prefer):
///   there is no network to intercept, and PostgreSQL offers no TLS on a unix
///   socket.
/// - a REMOTE endpoint (any other host) resolves to [`Require`](Self::Require):
///   a remote server that refuses TLS is a loud error, never a silent plaintext
///   connect an on-path attacker could have forced.
///
/// An explicitly-set mode (builder [`ssl_mode`](ConnectConfig::ssl_mode), DSN
/// `sslmode=`, `PGSSLMODE`) always wins, unchanged.
///
/// `Prefer` falls back to plain TCP if the server refuses SSL (with a stderr
/// warning). An active network attacker can forge the single-byte 'N' refusal
/// (sent before TLS protects the stream), stripping SSL — which is exactly why
/// the default resolves to `Require` on a remote path. `Require` is safe against
/// that downgrade; `Disable` never attempts TLS.
///
/// Deliberately NOT `Default`: there is no single default SSL mode to return,
/// and a `Default` claiming one would contradict the threat-scoped model above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// No SSL. Plain TCP. Use only for localhost/unix socket.
    Disable,
    /// Try SSL; fall back to plain TCP if server refuses.
    /// **WARNING**: vulnerable to active SSL-stripping attacks.
    /// Use `Require` for production over untrusted networks.
    Prefer,
    /// Require SSL. Fail if server refuses. Safe against downgrade.
    Require,
}

// Footprint pin: a 3-variant fieldless enum is a single discriminant byte. A
// variant accidentally carrying data would widen it; the pin catches that.
crate::footprint_pin!(SslMode, size = 1, align = 1);

/// The SCRAM-SHA-256-PLUS channel-binding policy, mirroring libpq's
/// `channel_binding` connection parameter.
///
/// Channel binding ties the SCRAM exchange to the specific TLS channel (via the
/// server's `tls-server-end-point` certificate hash), closing the valid-cert
/// relay/MITM residual that full cert+hostname verification alone leaves open.
/// It applies only over TLS — a plaintext channel cannot be bound.
///
/// The default is [`Prefer`](Self::Prefer), matching libpq: channel binding is
/// used whenever the server offers `SCRAM-SHA-256-PLUS`, but a server that does
/// not (a legacy PostgreSQL, or a SCRAM-speaking pooler) still connects. The
/// threat-scoped [`SslMode`] default already ensures a REMOTE endpoint is
/// encrypted (defeating passive interception); channel binding is
/// defense-in-depth against the narrower active-relay threat, so — like libpq —
/// it is opt-in-strict rather than default-strict, to avoid breaking
/// connectivity to servers that do not implement it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelBindingMode {
    /// Never use channel binding — plain SCRAM-SHA-256 even over TLS (gs2 `n,,`).
    /// The escape hatch for a server whose `-PLUS` support is broken.
    Disable,
    /// Use `SCRAM-SHA-256-PLUS` when the server offers it (over TLS); otherwise
    /// fall back to plain SCRAM (the `y,,` anti-downgrade flag over TLS). The
    /// default.
    Prefer,
    /// REQUIRE channel binding: refuse to authenticate unless the server offers
    /// `SCRAM-SHA-256-PLUS` over TLS. A plaintext connection, or a server without
    /// `-PLUS` (including a downgrade attacker who stripped it), is a fail-closed
    /// [`DriverError::Config`](crate::DriverError) /
    /// `ChannelBindingRequired` refusal, never a fallback.
    Require,
}

// Footprint pin: a 3-variant fieldless enum is a single discriminant byte.
crate::footprint_pin!(ChannelBindingMode, size = 1, align = 1);


/// Connection configuration.
///
/// Password is zeroized on drop via `Zeroizing<String>` wrapper.
/// Debug output redacts the password field.
#[derive(Clone)]
#[non_exhaustive]
pub struct ConnectConfig {
    /// The server host: a TCP hostname / IP, or an absolute path (leading `/`)
    /// selecting a unix-domain socket directory (see [`resolve_endpoint`]).
    pub host: String,
    /// The TCP port, or the `.s.PGSQL.<port>` file suffix for a unix socket.
    pub port: u16,
    /// The PostgreSQL role to authenticate as.
    pub user: String,
    /// The database to open; `None` defaults to the [`user`](Self::user) name
    /// (libpq parity).
    pub database: Option<String>,
    password_inner: Option<zeroize::Zeroizing<String>>,
    /// The connect-timeout budget in seconds — the wall-clock deadline covering
    /// TCP connect, the TLS handshake, and the startup / auth handshake.
    pub connect_timeout_secs: u64,
    /// The consumer's EXPLICIT SSL negotiation mode, or `None` when it was left
    /// to the threat-scoped default resolved at connect by [`resolve_ssl_mode`]
    /// (LOCAL endpoint → `Prefer`, REMOTE endpoint → `Require`; see [`SslMode`]).
    ///
    /// PRIVATE so the builder [`ssl_mode`], the DSN `sslmode=` key, and the
    /// `PGSSLMODE` env are the only way to set it (each stores `Some`), and so
    /// the `None`-means-defaulted encoding cannot leak or be bypassed with a
    /// direct field write — the resolution is centralized in one place, exactly
    /// like `resolve_endpoint` centralizes the unix-vs-TCP rule. `Option<SslMode>`
    /// niche-packs into the same 1 byte as a bare `SslMode` (a 3-variant enum has
    /// spare discriminants for `None`), so the footprint is unchanged.
    ///
    /// [`ssl_mode`]: ConnectConfig::ssl_mode
    /// [`resolve_ssl_mode`]: ConnectConfig::resolve_ssl_mode
    ssl_mode: Option<SslMode>,
    /// Consumer-supplied PostgreSQL startup parameters (GUCs) sent in the
    /// `StartupMessage` — `search_path`, `application_name`,
    /// `statement_timeout`, or any GUC. Set via [`with_startup_param`],
    /// [`with_search_path`], [`with_application_name`]; ordered by insertion.
    ///
    /// Stored raw and validated at connect (each pair must be NUL-free,
    /// bounded, and not a reserved parameter) — consistent with how `user` /
    /// `database` are validated to their bounded wire types at connect, not at
    /// the builder. Kept private so the builders are the only entry point; the
    /// drivers read it via [`startup_params`].
    ///
    /// [`with_startup_param`]: ConnectConfig::with_startup_param
    /// [`with_search_path`]: ConnectConfig::with_search_path
    /// [`with_application_name`]: ConnectConfig::with_application_name
    /// [`startup_params`]: ConnectConfig::startup_params
    startup_params: Vec<(String, String)>,
    /// Consumer-supplied CA certificate roots (PEM), or `None` to verify the
    /// server certificate against the DEFAULT trust anchors (the baked Mozilla
    /// bundle, under the `webpki-roots` feature). Set via [`with_ca_roots`] or
    /// the `sslrootcert=<path>` DSN key / `PGSSLROOTCERT` env var.
    ///
    /// Stored RAW (the PEM bytes) and parsed into a `rustls` root store at
    /// connect — consistent with `startup_params`, which are also stored raw and
    /// validated at connect. An invalid/empty PEM is a classified
    /// [`DriverError::Config`](crate::DriverError) at connect, never a silent
    /// fallback to the default roots or to plaintext. `Arc<[u8]>` so a
    /// `ConnectConfig` clone (e.g. per pool checkout) is an O(1) refcount bump,
    /// not a PEM deep-copy.
    ///
    /// [`with_ca_roots`]: ConnectConfig::with_ca_roots
    ca_roots_pem: Option<Arc<[u8]>>,
    /// The SCRAM-SHA-256-PLUS channel-binding policy (default
    /// [`ChannelBindingMode::Prefer`]). Set via the builder
    /// [`channel_binding`](Self::channel_binding), the DSN `channel_binding=`
    /// key, or `PGCHANNELBINDING`. Only meaningful for a password (SCRAM)
    /// connection over TLS; ignored for Trust auth or plaintext (where binding
    /// is impossible — `Require` then fails closed at connect).
    ///
    /// Stored directly (not `Option`) — unlike [`SslMode`] the default is a
    /// FIXED `Prefer` (not endpoint-dependent), so there is no defaulted-vs-set
    /// distinction to preserve. Niche-packs beside `ssl_mode` into the config's
    /// existing padding, so the footprint is unchanged.
    channel_binding: ChannelBindingMode,
}

// Footprint pin: four owned Strings/Option<String> (host, user, database,
// password) plus a u16 port, a u64 timeout, the `Option<SslMode>` byte (niche-
// packed to 1 B — `SslMode` is a 3-variant enum, so `None` rides a spare
// discriminant; the same width as the bare `SslMode` it replaced), the
// startup-params `Vec<(String, String)>` (one 3-word / 24-byte owned handle;
// its heap buffer is empty until a startup parameter is added), and the
// custom-CA `Option<Arc<[u8]>>` (a 16-byte fat pointer, niche-packed into 16 B;
// `None` until a CA is supplied). Config is built once per connection, so its
// size is not hot — but pinning it keeps a silently-added field on the review
// surface, and the password is a Zeroizing<String> whose 3-word shape must not
// regress. 112 + 24 (the Vec) + 16 (the Arc) = 152.
crate::footprint_pin!(ConnectConfig, size = 152, align = 8);

impl ConnectConfig {
    /// Borrow the configured password as `&str`, or `None` if none was set. The
    /// stored password is a `Zeroizing<String>` — scrubbed on drop, redacted in
    /// `Debug`.
    pub fn password_str(&self) -> Option<&str> {
        self.password_inner.as_deref().map(|s| s.as_str())
    }

    /// The RAW configured SSL mode (`None` = defaulted). Crate-internal: the
    /// [`Redial`](crate::cancel::Redial) snapshot preserves the exact
    /// explicit/defaulted state so a cancel dial reproduces the original SSL
    /// decision (the public surface stays [`resolve_ssl_mode`](Self::resolve_ssl_mode)
    /// / [`ssl_mode_is_explicit`](Self::ssl_mode_is_explicit)).
    pub(crate) fn ssl_mode_raw(&self) -> Option<SslMode> {
        self.ssl_mode
    }

    /// The custom CA-roots PEM as a cheap-to-clone `Arc` handle (`None` = default
    /// trust anchors). Crate-internal: the [`Redial`](crate::cancel::Redial)
    /// snapshot carries the same `Arc` so a cancel dial verifies against the same
    /// roots without deep-copying the PEM.
    pub(crate) fn ca_roots_arc(&self) -> Option<Arc<[u8]>> {
        self.ca_roots_pem.clone()
    }
}

impl core::fmt::Debug for ConnectConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("password", &self.password_inner.as_ref().map(|_| "[REDACTED]"))
            .field("ssl_mode", &self.ssl_mode)
            // Startup parameters are not secret (search_path, application_name,
            // …), so they are shown in full — consistent with host/user/db.
            .field("startup_params", &self.startup_params)
            .finish()
    }
}

impl ConnectConfig {
    /// Parse a PostgreSQL connection string (DSN).
    ///
    /// Format: `postgres://user:password@host:port/database?sslmode=require`
    ///
    /// All components except user are optional:
    /// - `postgres://user@host/db` — no password, default port
    /// - `postgres://user:pass@host` — no database (defaults to user)
    /// - `postgres://user@host?sslmode=disable`
    ///
    /// # Unix-domain sockets — the `host` query parameter (libpq parity)
    ///
    /// A unix socket dir is an absolute path, which cannot ride the URL
    /// *authority* host slot (its leading `/` is the authority/path delimiter, so
    /// `postgres://u@/tmp/db` parses `/tmp/db` as the database on an EMPTY host).
    /// libpq's canonical URL form for a unix socket puts the socket directory in
    /// the `host` **query parameter** instead — and that is supported here:
    ///
    /// - `postgresql://user@/dbname?host=/var/run/postgresql` → unix socket
    /// - `postgresql://user@/dbname?host=/tmp` → unix socket at `/tmp/.s.PGSQL.<port>`
    ///
    /// A `host=` query parameter OVERRIDES the authority host (matching libpq: the
    /// query parameter wins); it works for a TCP host too
    /// (`postgres://u@ignored/db?host=realhost`). The authority `port` (or the
    /// default `5432`) still applies — including to a unix socket's filename.
    ///
    /// An EMPTY authority host with no `host=` parameter (`postgres://u@/db`) is a
    /// loud error naming the `host=` form — never a silent connect to a
    /// port-only TCP address.
    pub fn from_dsn(dsn: &str) -> Result<Self, crate::DriverError> {
        let s = dsn.strip_prefix("postgres://")
            .or_else(|| dsn.strip_prefix("postgresql://"))
            .ok_or_else(|| config_error("DSN must start with postgres:// or postgresql://"))?;

        // Split query string
        let (main, query) = match s.split_once('?') {
            Some((m, q)) => (m, Some(q)),
            None => (s, None),
        };

        // Split userinfo@hostpath
        let (userinfo, hostpath) = match main.split_once('@') {
            Some((u, h)) => (u, h),
            None => {
                return Err(config_error(
                    "missing '@' in DSN (expected postgres://user[:password]@host[:port][/database])",
                ));
            }
        };

        // Parse user:password
        let (user, password) = match userinfo.split_once(':') {
            Some((u, p)) => (u.to_string(), Some(p.to_string())),
            None => (userinfo.to_string(), None),
        };

        if user.is_empty() {
            return Err(config_error("empty user in DSN"));
        }

        // Parse host:port/database
        let (hostport, database) = match hostpath.split_once('/') {
            Some((hp, db)) => (hp, if db.is_empty() { None } else { Some(db.to_string()) }),
            None => (hostpath, None),
        };

        // Split host from port. A bracketed IPv6 literal (`[::1]`, `[2001:db8::1]`)
        // carries the address's OWN colons INSIDE the brackets, so the port
        // separator is only the `:` that FOLLOWS the closing `]`. `rsplit_once(':')`
        // alone would split at the address's last internal colon when no port is
        // present (`[::1]` → port `"1]"`), so the bracket form is handled FIRST; a
        // bare host / `host:port` falls through to the unchanged rsplit path.
        let (host, port) = match hostport.strip_prefix('[') {
            Some(rest) => match rest.split_once(']') {
                Some((addr, after)) => {
                    let port = match after.strip_prefix(':') {
                        Some(p) => p.parse::<u16>().map_err(|_| config_error(format!("invalid port: {p}")))?,
                        None if after.is_empty() => 5432,
                        None => {
                            return Err(config_error(format!(
                                "invalid characters after IPv6 literal in DSN host: {after:?}"
                            )));
                        }
                    };
                    // Keep the brackets: they are what `ToSocketAddrs` dials, and
                    // `host_is_loopback` / `resolve_endpoint` both expect the
                    // bracketed form (matching the already-correct with-port path).
                    (format!("[{addr}]"), port)
                }
                None => {
                    return Err(config_error(format!(
                        "unterminated IPv6 literal in DSN host: {hostport:?}"
                    )));
                }
            },
            None => match hostport.rsplit_once(':') {
                Some((h, p)) => {
                    let port = p.parse::<u16>().map_err(|_| config_error(format!("invalid port: {p}")))?;
                    (h.to_string(), port)
                }
                None => (hostport.to_string(), 5432),
            },
        };

        // Parse query params. `ssl_mode` stays `None` (defaulted — resolved
        // per-endpoint at connect) unless an explicit `sslmode=` sets it.
        let mut ssl_mode: Option<SslMode> = None;
        let mut timeout = 10u64;
        let mut ca_roots_pem: Option<Arc<[u8]>> = None;
        // `channel_binding` defaults to `Prefer` unless a `channel_binding=` key
        // sets it (libpq parity).
        let mut channel_binding = ChannelBindingMode::Prefer;
        // `host=` OVERRIDES the authority host (libpq: the query parameter wins).
        // The canonical way to name a unix-socket directory in a PG URL, whose
        // leading `/` cannot ride the authority slot.
        let mut host_override: Option<String> = None;
        if let Some(q) = query {
            for param in q.split('&') {
                // A parameter with no `=` carries no value. Silently skipping it
                // would, for `?sslmode`, keep the default `prefer` and downgrade
                // SSL without the caller knowing. Reject it loudly instead.
                let (k, v) = match param.split_once('=') {
                    Some(kv) => kv,
                    None => {
                        return Err(config_error(format!(
                            "malformed DSN parameter (missing '='): {param}"
                        )));
                    }
                };
                match k {
                    "host" => {
                        // libpq's unix-socket URL form:
                        // `postgresql://u@/db?host=/var/run/postgresql`. An
                        // absolute-path value routes to a unix socket via
                        // `resolve_endpoint`; a plain name is a TCP host override.
                        // No percent-decoding: `/` is a legal query-component
                        // character (RFC 3986 §3.4), so a socket path needs none,
                        // and a value carrying `&`/`=`/`?` is not a real socket dir.
                        host_override = Some(v.to_string());
                    }
                    "sslmode" => {
                        // An explicit `sslmode=` — stored as `Some`, so it wins
                        // over the threat-scoped default at connect.
                        ssl_mode = Some(match v {
                            "disable" => SslMode::Disable,
                            "prefer" => SslMode::Prefer,
                            "require" => SslMode::Require,
                            other => {
                                return Err(config_error(format!(
                                    "unknown sslmode: {other} (valid: disable, prefer, require)"
                                )));
                            }
                        });
                    }
                    "channel_binding" => {
                        // libpq parity: `disable` / `prefer` / `require`.
                        channel_binding = match v {
                            "disable" => ChannelBindingMode::Disable,
                            "prefer" => ChannelBindingMode::Prefer,
                            "require" => ChannelBindingMode::Require,
                            other => {
                                return Err(config_error(format!(
                                    "unknown channel_binding: {other} (valid: disable, prefer, require)"
                                )));
                            }
                        };
                    }
                    "connect_timeout" => {
                        timeout = v.parse().map_err(|_| config_error(format!("invalid timeout: {v}")))?;
                    }
                    "sslrootcert" => {
                        // Read the CA bundle from the given path NOW. libpq reads
                        // `sslrootcert` at connect; here, DSN parsing is the
                        // connect-config assembly step. A missing/unreadable file
                        // is a loud error carrying the path — never a silent
                        // fallback to the default roots. The PEM contents are
                        // parsed into the rustls root store at connect (fail-closed
                        // on an invalid/empty bundle there), consistent with how
                        // startup parameters are stored raw and validated at connect.
                        let bytes = std::fs::read(v)
                            .map_err(|e| config_error(format!("cannot read sslrootcert file {v}: {e}")))?;
                        ca_roots_pem = Some(Arc::from(bytes));
                    }
                    // An unrecognised key is a misconfiguration (e.g. a
                    // typo'd `sslmod=require` that would otherwise silently
                    // leave the default `prefer`, downgrading security).
                    // Reject it loudly, consistent with how an unknown
                    // sslmode VALUE is already rejected above.
                    other => return Err(config_error(format!("unknown DSN parameter: {other}"))),
                }
            }
        }

        // The `host=` query parameter (if present) wins over the authority host.
        let host = match host_override {
            Some(overridden) => overridden,
            None => host,
        };
        // An empty host is unroutable: `resolve_endpoint("", port)` would yield the
        // port-only TCP address `:<port>`, a confusing loud connect failure. Reject
        // it at parse time with the fix — the `host=` form (the only way to name a
        // unix-socket directory, whose leading `/` cannot ride the URL authority).
        if host.is_empty() {
            return Err(config_error(
                "empty host in DSN authority; give the host via the \"host\" query \
                 parameter — e.g. \"?host=/var/run/postgresql\" for a unix-domain \
                 socket, or \"?host=db.example.com\" for TCP",
            ));
        }

        Ok(Self {
            host,
            port,
            user,
            database,
            password_inner: password.map(zeroize::Zeroizing::new),
            connect_timeout_secs: timeout,
            ssl_mode,
            startup_params: Vec::new(),
            ca_roots_pem,
            channel_binding,
        })
    }

    /// Construct from environment variables.
    ///
    /// Reads: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`,
    /// `PGSSLMODE`, `PGSSLROOTCERT`.
    ///
    /// A variable that is **absent** falls back to a documented default
    /// (host=localhost, port=5432, user=`$USER` then `postgres`,
    /// sslmode=prefer). A variable that is **present but malformed** is a real
    /// misconfiguration and is rejected with `Err` — a typo'd `PGPORT=abc` or
    /// `PGSSLMODE=requ1re` never silently degrades to a default that could send
    /// the connection to the wrong port or strip SSL.
    pub fn from_env() -> Result<Self, crate::DriverError> {
        use std::env::VarError;

        let host = match std::env::var("PGHOST") {
            Ok(h) => h,
            Err(VarError::NotPresent) => "localhost".to_string(),
            Err(VarError::NotUnicode(_)) => return Err(config_error("PGHOST is not valid UTF-8")),
        };

        let port = match std::env::var("PGPORT") {
            Ok(p) => p.parse::<u16>().map_err(|_| config_error(format!("invalid PGPORT: {p}")))?,
            Err(VarError::NotPresent) => 5432,
            Err(VarError::NotUnicode(_)) => return Err(config_error("PGPORT is not valid UTF-8")),
        };

        let user = match std::env::var("PGUSER") {
            Ok(u) => u,
            Err(VarError::NotPresent) => match std::env::var("USER") {
                Ok(u) => u,
                Err(VarError::NotPresent) => "postgres".to_string(),
                Err(VarError::NotUnicode(_)) => return Err(config_error("USER is not valid UTF-8")),
            },
            Err(VarError::NotUnicode(_)) => return Err(config_error("PGUSER is not valid UTF-8")),
        };

        let database = match std::env::var("PGDATABASE") {
            Ok(d) => Some(d),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => return Err(config_error("PGDATABASE is not valid UTF-8")),
        };

        let password = match std::env::var("PGPASSWORD") {
            Ok(p) => Some(p),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => return Err(config_error("PGPASSWORD is not valid UTF-8")),
        };

        let ssl_mode = match std::env::var("PGSSLMODE") {
            // An explicit `PGSSLMODE` — stored as `Some`, so it wins over the
            // threat-scoped default at connect.
            Ok(v) => Some(match v.as_str() {
                "disable" => SslMode::Disable,
                "prefer" => SslMode::Prefer,
                "require" => SslMode::Require,
                // A typo here would otherwise silently fall to the default,
                // which on a local endpoint permits a downgrade. Reject it loudly.
                other => return Err(config_error(format!("unknown PGSSLMODE: {other}"))),
            }),
            // Absent → defaulted (resolved per-endpoint at connect), NOT a fixed
            // `Prefer`: a remote host now defaults to `Require`, closing the
            // silent-plaintext-to-remote hole.
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => return Err(config_error("PGSSLMODE is not valid UTF-8")),
        };

        // `PGSSLROOTCERT` names a CA-bundle file; read it now (a present-but-
        // unreadable path is a loud error carrying the path, never a silent
        // fallback to the default roots). Absent → the default trust anchors.
        let ca_roots_pem = match std::env::var("PGSSLROOTCERT") {
            Ok(path) => {
                let bytes = std::fs::read(&path)
                    .map_err(|e| config_error(format!("cannot read PGSSLROOTCERT file {path}: {e}")))?;
                Some(Arc::from(bytes))
            }
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => {
                return Err(config_error("PGSSLROOTCERT is not valid UTF-8"))
            }
        };

        // `PGCHANNELBINDING` — present-but-malformed is a loud error (never a
        // silent default that could weaken a `require` intent), mirroring how
        // `PGSSLMODE` is handled.
        let channel_binding = match std::env::var("PGCHANNELBINDING") {
            Ok(v) => match v.as_str() {
                "disable" => ChannelBindingMode::Disable,
                "prefer" => ChannelBindingMode::Prefer,
                "require" => ChannelBindingMode::Require,
                other => {
                    return Err(config_error(format!(
                        "unknown PGCHANNELBINDING: {other} (valid: disable, prefer, require)"
                    )));
                }
            },
            Err(VarError::NotPresent) => ChannelBindingMode::Prefer,
            Err(VarError::NotUnicode(_)) => {
                return Err(config_error("PGCHANNELBINDING is not valid UTF-8"))
            }
        };

        Ok(Self {
            host,
            port,
            user,
            database,
            password_inner: password.map(zeroize::Zeroizing::new),
            connect_timeout_secs: 10,
            ssl_mode,
            startup_params: Vec::new(),
            ca_roots_pem,
            channel_binding,
        })
    }

    /// Construct with required fields. Port defaults to 5432.
    pub fn new(host: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 5432,
            user: user.into(),
            database: None,
            password_inner: None,
            connect_timeout_secs: 10,
            // Unset: the effective mode is resolved per-endpoint at connect
            // (LOCAL → Prefer, REMOTE → Require). An explicit `ssl_mode(..)`
            // overrides it.
            ssl_mode: None,
            startup_params: Vec::new(),
            ca_roots_pem: None,
            channel_binding: ChannelBindingMode::Prefer,
        }
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the database.
    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    /// Set the SSL mode EXPLICITLY, overriding the threat-scoped default.
    ///
    /// Without this (and without a DSN `sslmode=` / `PGSSLMODE`), the mode is
    /// resolved per-endpoint at connect: a LOCAL endpoint → [`SslMode::Prefer`],
    /// a REMOTE endpoint → [`SslMode::Require`] (see
    /// [`resolve_ssl_mode`](Self::resolve_ssl_mode)). An explicit setting here
    /// always wins.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = Some(mode);
        self
    }

    /// Set the SCRAM-SHA-256-PLUS channel-binding policy (default
    /// [`ChannelBindingMode::Prefer`]).
    ///
    /// [`Require`](ChannelBindingMode::Require) is the strict mode — it refuses
    /// to authenticate unless channel binding is in use (a TLS server offering
    /// `SCRAM-SHA-256-PLUS`), closing the valid-cert relay/MITM residual. Also
    /// settable via the DSN `channel_binding=` key or `PGCHANNELBINDING`.
    pub fn channel_binding(mut self, mode: ChannelBindingMode) -> Self {
        self.channel_binding = mode;
        self
    }

    /// The effective channel-binding policy (default
    /// [`ChannelBindingMode::Prefer`]).
    #[must_use]
    pub fn channel_binding_mode(&self) -> ChannelBindingMode {
        self.channel_binding
    }

    /// Set connection timeout in seconds.
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    /// Set the password. Default auth is SCRAM-SHA-256.
    /// Server chooses the actual method — SCRAM works for both
    /// `scram-sha-256` and `md5` pg_hba rules (PG falls back).
    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password_inner = Some(zeroize::Zeroizing::new(pw.into()));
        self
    }

    /// Add a PostgreSQL startup parameter (GUC) sent in the `StartupMessage` —
    /// e.g. `("statement_timeout", "5000")`, `("timezone", "UTC")`.
    ///
    /// The pair is stored raw and validated at connect: a name or value
    /// containing a NUL byte (unrepresentable in the NUL-delimited wire frame),
    /// an over-length name/value, or a reserved name (`user`, `database`,
    /// `client_encoding`, `replication`, `options` — managed by the connection)
    /// is a classified [`DriverError::Config`](crate::DriverError) at connect,
    /// never a corrupt packet. Names are case-insensitive to PostgreSQL's GUC
    /// folding, so the reserved check cannot be bypassed with a different case.
    ///
    /// Chainable and order-preserving; setting the same parameter twice sends
    /// it twice (PostgreSQL applies the last).
    #[must_use]
    pub fn with_startup_param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.startup_params.push((name.into(), value.into()));
        self
    }

    /// Set the session `search_path` on connect (convenience over
    /// [`with_startup_param`](Self::with_startup_param)`("search_path", …)`).
    ///
    /// A connect-time `search_path` is the schema-isolation primitive: it
    /// travels in the `StartupMessage`, so it is in effect before the first
    /// query, and it becomes the session's reset value — surviving a pooled
    /// connection's `RESET ALL` on checkout, so a pooled connection cannot
    /// silently escape its schema.
    #[must_use]
    pub fn with_search_path(self, search_path: impl Into<String>) -> Self {
        self.with_startup_param("search_path", search_path)
    }

    /// Set the session `application_name` on connect (convenience over
    /// [`with_startup_param`](Self::with_startup_param)`("application_name",
    /// …)`). Surfaces in `pg_stat_activity` and server logs.
    #[must_use]
    pub fn with_application_name(self, application_name: impl Into<String>) -> Self {
        self.with_startup_param("application_name", application_name)
    }

    /// Set the server-side `statement_timeout` on connect — the SERVER-side
    /// complement to the client `CancelToken`, and the standard production
    /// guardrail against a runaway query.
    ///
    /// PostgreSQL aborts any statement running LONGER than this, returning SQLSTATE
    /// `57014` `query_canceled`; the connection is left drained + REUSABLE (a
    /// `statement_timeout` abort is NOT a disconnect — see
    /// [`DriverError::is_disconnect`](crate::DriverError::is_disconnect)). Unlike a
    /// per-query `SET statement_timeout` round trip, this rides the EXISTING
    /// startup-parameter map (a convenience over
    /// [`with_startup_param`](Self::with_startup_param)`("statement_timeout", …)`),
    /// so it is footprint-neutral (no new [`ConnectConfig`] field), applies from
    /// before the FIRST query, and — as a startup-packet GUC — becomes the
    /// session's reset value, so it SURVIVES a pooled connection's `RESET ALL` on
    /// checkout (the guardrail persists across checkouts).
    ///
    /// # Duration mapping
    ///
    /// PostgreSQL's `statement_timeout` is an integer-millisecond GUC:
    ///
    /// - [`Duration::ZERO`] maps to `"0"`, which — by PostgreSQL's own convention —
    ///   DISABLES the timeout. To leave the timeout UNSET, do not call this method;
    ///   pass `Duration::ZERO` only to EXPLICITLY opt out. **Watch a dynamically
    ///   computed budget**: an already-expired deadline yields
    ///   `deadline.saturating_duration_since(now) == Duration::ZERO`, so passing
    ///   that here DISABLES the guardrail rather than bounding the query to zero —
    ///   guard the exact-zero case yourself if "already expired" should mean "abort
    ///   immediately". (A non-zero sub-ms budget is safe — it rounds up to 1 ms,
    ///   below.)
    /// - a non-zero SUB-millisecond duration is rounded UP to `1` ms (the finest
    ///   granularity PostgreSQL offers), NEVER down to `0` — a requested timeout is
    ///   never silently weakened into "disabled".
    /// - a duration whose whole milliseconds exceed PostgreSQL's 32-bit GUC ceiling
    ///   (`i32::MAX` ms ≈ 24.8 days) is capped there, so an enormous `Duration`
    ///   never produces a value the server rejects.
    ///
    /// The value is stored raw and (like every startup parameter) validated at
    /// connect. This convenience writes the SAME `statement_timeout` startup-GUC
    /// key as [`with_startup_param`](Self::with_startup_param)`("statement_timeout",
    /// …)` / a DSN `options`, and the startup map APPENDS (no dedup), so setting it
    /// twice — or mixing this with a raw `statement_timeout` param — sends two
    /// entries and PostgreSQL applies the LAST (last-wins, the existing startup-map
    /// semantics), never an error.
    #[must_use]
    pub fn with_statement_timeout(self, timeout: Duration) -> Self {
        // PostgreSQL's statement_timeout is a millisecond GUC with a 32-bit
        // (`i32::MAX`) ceiling; `0` means DISABLED.
        const MAX_MS: u128 = 2_147_483_647; // i32::MAX ms ≈ 24.8 days
        let ms: u128 = if timeout.is_zero() {
            0
        } else {
            // Floor to whole ms but never below 1 (a sub-ms request must not
            // collapse to `0` = disabled), and never above the GUC ceiling.
            timeout.as_millis().clamp(1, MAX_MS)
        };
        self.with_startup_param("statement_timeout", ms.to_string())
    }

    /// Borrow the raw, not-yet-validated startup parameters, in insertion
    /// order. The drivers validate each into a wire `StartupParam` at connect.
    #[must_use]
    pub fn startup_params(&self) -> &[(String, String)] {
        &self.startup_params
    }

    /// The client-side read-liveness window derived from the configured
    /// server-side `statement_timeout`, or `None` when no such budget is set.
    ///
    /// # Why this is the ONE safe client-side bound
    ///
    /// A silently-vanished *live* peer — a black-hole proxy whose kernel still
    /// ACKs but whose application forwards NOTHING, an unrecoverable app hang
    /// behind a healthy NAT — cannot be detected by TCP keepalive (the peer
    /// kernel answers the probes) and is INDISTINGUISHABLE at the socket layer
    /// from a server legitimately taking a long time to produce the first result
    /// byte. So no *fixed* client read deadline can catch it without also cutting
    /// a legitimate slow query — the classic tension that leaves every driver's
    /// in-flight read unbounded by default.
    ///
    /// The one budget that makes them distinguishable is the server's own
    /// `statement_timeout`: the server aborts any query exceeding it with
    /// `57014`, so a client that has waited `statement_timeout` PLUS one
    /// round-trip ([`connect_timeout`](Self::connect_timeout), the driver's
    /// existing network-handshake budget) for ANY byte — a result OR the `57014`
    /// abort — is looking at a peer whose response was dropped, NEVER a query the
    /// server would still allow (it would have aborted it first). Arming a
    /// per-read inactivity deadline of this window therefore bounds a black-holed
    /// in-flight query WITHOUT ever cutting a query the server itself would not
    /// have aborted — the closure the raw `statement_timeout` cannot make alone
    /// (its `57014` is black-holed too, so it bounds the SERVER, not the client's
    /// read).
    ///
    /// # When it is `None` (the current unbounded steady-state read)
    ///
    /// - `statement_timeout` unset — no query budget, so no safe client window
    ///   exists (any finite deadline could cut a legitimate long query).
    /// - `statement_timeout` `0` (explicitly disabled, PostgreSQL's convention).
    /// - `statement_timeout` in a form [`parse_statement_timeout_ms`] cannot model
    ///   (`DEFAULT`, garbage) — a fail-SAFE absence, never a spurious bound. Unit
    ///   suffixes PostgreSQL accepts (`us`/`ms`/`s`/`min`/`h`/`d`) DO parse, so a
    ///   raw `("statement_timeout", "30s")` startup param derives its window too.
    ///
    /// Last-wins over the startup map (its last-applied + PostgreSQL's
    /// case-insensitive GUC-name-folding semantics), so it reads the SAME value
    /// the server will.
    ///
    /// # Runtime drift (the window is re-derived, never left stale)
    ///
    /// This is the CONNECT-TIME window. A runtime change to `statement_timeout`
    /// moves the server budget out from under it; a window left stale BELOW a
    /// raised budget would cut a query the server now allows. The drivers therefore
    /// re-derive the window from EVERY executed dynamic runtime-SQL statement (via
    /// [`window_after_statement`] / [`statement_timeout_effect`]), on the connection
    /// AND inside a `transaction` guard: an explicit top-level `SET`/`RESET`
    /// re-derives to the new budget, and a `set_config('statement_timeout', …)` —
    /// which cannot be pinned to a value — DISARMS the window (fail-safe). The
    /// window is SUPPRESSED for bsql's own trusted long operations (the migration
    /// runner). The only residual is a change with NO textual mention of the GUC
    /// name in the executed SQL (a function body, an `EXECUTE` of a prepared plan) —
    /// the theoretical floor, since PostgreSQL does not report `statement_timeout`
    /// via `ParameterStatus`; see [`statement_timeout_effect`].
    #[must_use]
    pub fn client_liveness_window(&self) -> Option<Duration> {
        let ms: u64 = self
            .startup_params
            .iter()
            .rev()
            .find(|(name, _)| name.eq_ignore_ascii_case("statement_timeout"))
            .and_then(|(_, value)| parse_statement_timeout_ms(value))?;
        window_from_statement_timeout_ms(ms, self.connect_timeout_secs)
    }

    /// Verify the server certificate against a CUSTOM set of CA roots supplied
    /// as PEM, instead of the default (baked Mozilla) trust anchors.
    ///
    /// This is the internal/private-CA path: a fleet whose PostgreSQL servers
    /// present certificates signed by an in-house CA can now use
    /// [`SslMode::Require`] (verified TLS) against them, instead of being forced
    /// to [`SslMode::Disable`] (plaintext) because the baked public roots cannot
    /// validate an internal CA. The supplied roots REPLACE the default anchors
    /// (they do not extend them), matching libpq's `sslrootcert`: the server is
    /// verified against precisely this CA, not this CA plus every public root.
    ///
    /// The PEM is stored raw and parsed into a `rustls` root store at connect. An
    /// invalid or empty PEM is a classified
    /// [`DriverError::Config`](crate::DriverError) at connect — fail-closed,
    /// never a silent fallback to the default roots or to plaintext.
    ///
    /// Chainable; a later call replaces an earlier one (the last CA source wins).
    #[must_use]
    pub fn with_ca_roots(mut self, pem: &[u8]) -> Self {
        self.ca_roots_pem = Some(Arc::from(pem));
        self
    }

    /// Borrow the raw, not-yet-parsed custom CA-roots PEM, or `None` when the
    /// default trust anchors are in force. The drivers parse it into a `rustls`
    /// root store at connect (fail-closed on an invalid/empty bundle).
    #[must_use]
    pub fn ca_roots_pem(&self) -> Option<&[u8]> {
        self.ca_roots_pem.as_deref()
    }

    /// Whether the [`SslMode`] was set EXPLICITLY (builder
    /// [`ssl_mode`](Self::ssl_mode) / DSN `sslmode=` / `PGSSLMODE`), as opposed
    /// to left to the threat-scoped default resolved by
    /// [`resolve_ssl_mode`](Self::resolve_ssl_mode). A driver uses this to tell
    /// an explicitly-required TLS refusal (a violated caller contract) apart from
    /// a defaulted-remote one (which names the plaintext opt-out in its error).
    #[must_use]
    pub fn ssl_mode_is_explicit(&self) -> bool {
        self.ssl_mode.is_some()
    }

    /// Resolve the [`SslMode`] in force for a connection to `endpoint` — the
    /// threat-scoped default when the consumer did not set one explicitly.
    ///
    /// An EXPLICIT mode (builder [`ssl_mode`](Self::ssl_mode), DSN `sslmode=`,
    /// `PGSSLMODE`) always wins, unchanged. When none was set, the default is
    /// scoped to where an interception threat can actually exist — a network
    /// path:
    ///
    /// - a LOCAL endpoint — a unix-domain socket, or a loopback TCP host
    ///   (`localhost`, `127.0.0.0/8`, `::1`) — resolves to [`SslMode::Prefer`]:
    ///   there is no network to intercept, and PostgreSQL offers no TLS on a
    ///   unix socket.
    /// - a REMOTE endpoint (any other host, INCLUDING private ranges such as
    ///   `10.0.0.0/8` or `192.168.0.0/16` — still reached over a network path)
    ///   resolves to [`SslMode::Require`]: a remote server that refuses TLS is a
    ///   loud error, never a silent plaintext connect an on-path attacker could
    ///   have forced.
    ///
    /// The local/remote classification is SYNTACTIC — on the CONFIGURED host,
    /// with no DNS resolution (a resolver round trip would be both slow and a
    /// TOCTOU hole; the rule reads the host string the consumer supplied). Both
    /// drivers resolve through this one method, so the rule cannot drift between
    /// them — exactly as [`resolve_endpoint`] centralizes the unix-vs-TCP rule.
    #[must_use]
    pub fn resolve_ssl_mode(&self, endpoint: &Endpoint) -> SslMode {
        match self.ssl_mode {
            Some(explicit) => explicit,
            None if endpoint.is_unix() || host_is_loopback(&self.host) => SslMode::Prefer,
            None => SslMode::Require,
        }
    }
}

/// Strip the surrounding brackets from a DSN-style IPv6-literal host, returning
/// the inner address; every other host is returned verbatim (borrow-preserving,
/// no allocation).
///
/// A DSN authority carries an IPv6 literal in BRACKETS (`[::1]`, `[2001:db8::1]`)
/// so the address's own colons do not collide with the `:port` delimiter, and
/// [`ConnectConfig::from_dsn`] KEEPS those brackets on `host` because they are
/// what `ToSocketAddrs` dials (`[::1]:5432`). Two consumers, however, need the
/// BARE address: the loopback classifier (`host_is_loopback`) parses it as an
/// [`IpAddr`](std::net::IpAddr), and the TLS path (`crate::ssl`) derives a rustls
/// `ServerName` from it — and rustls REJECTS a bracketed literal (`[::1]` is
/// neither a valid DNS name nor a parseable IP; the brackets are DSN syntax, not
/// part of the address). This is the ONE authority both call through, so the
/// unbracket rule cannot drift between the loopback classifier and the TLS
/// server-name derivation.
///
/// Brackets are stripped ONLY when BOTH are present (a leading `[` AND a trailing
/// `]`); a bare host, a DNS name, or a malformed half-bracketed string is
/// returned untouched — fail-safe, so a malformed host fails loudly at its own
/// downstream parse / derivation rather than being silently mangled here.
#[must_use]
pub(crate) fn unbracket_host(host: &str) -> &str {
    // `.and_then(strip_suffix)` yields `Some(inner)` only when a leading `[` AND a
    // trailing `]` are BOTH present; anything else keeps `host` intact. A `match`
    // (not `.unwrap_or(host)`) because `unwrap_or` is on the crate's
    // silent-fallback disallow list — here the fallback is total by construction,
    // and the `match` keeps that visible rather than hidden behind a banned combinator.
    match host.strip_prefix('[').and_then(|inner| inner.strip_suffix(']')) {
        Some(inner) => inner,
        None => host,
    }
}

/// Whether `host` names a LOOPBACK network target — the syntactic classification
/// (no DNS) the threat-scoped SSL default uses to treat an endpoint as local.
///
/// True for the hostname `localhost` (case-insensitive — RFC 6761 mandates it
/// resolve to loopback) and for any IP literal in the loopback ranges
/// (`127.0.0.0/8` for IPv4, `::1` for IPv6, via
/// [`IpAddr::is_loopback`](std::net::IpAddr::is_loopback)). Every other host —
/// a public name, a private-range address (still reached over a network path),
/// or a string that does not parse as an IP — is NOT loopback, so it is treated
/// as remote (and defaults to `Require`).
///
/// A DSN authority carries an IPv6 literal in BRACKETS (`[::1]`) so the colons
/// do not collide with the `:port` delimiter; the brackets are stripped by the
/// shared `unbracket_host` authority (only when BOTH are present) before parsing,
/// so a genuinely-local `[::1]` is classified local rather than mis-parsed as a
/// remote name. The host is not mutated — only the classification reads the
/// unbracketed form (the bracketed form is what `ToSocketAddrs` dials). A
/// non-loopback bracketed literal (`[2001:db8::1]`, or the IPv4-mapped
/// `[::ffff:127.0.0.1]`, which is not `::1`) still classifies remote — fail-safe.
fn host_is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    unbracket_host(host).parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// A resolved connection target derived from a [`ConnectConfig`]'s `host` +
/// `port`, classified by libpq's absolute-path rule.
///
/// # The rule
///
/// A host that is an ABSOLUTE PATH (begins with `/`) selects a **unix-domain
/// socket** at `<host>/.s.PGSQL.<port>` rather than TCP/IP — exactly libpq's
/// convention, so `host=/var/run/postgresql` (or `PGHOST=/tmp`, or
/// `ConnectConfig::new("/tmp", …)`) connects over the local socket with no TCP,
/// no Nagle, and no loopback stack. Every other host is a TCP endpoint
/// `host:port`. Both drivers route through [`resolve_endpoint`], so the rule is
/// defined once and cannot drift between them.
///
/// The abstract-namespace (`@`-prefixed) Linux variant is deliberately NOT
/// modelled: it is Linux-only and non-portable, and the filesystem-path form is
/// the one PostgreSQL creates by default on every platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// A TCP endpoint — the `host:port` string a driver dials.
    Tcp(String),
    /// A unix-domain socket at `<host-dir>/.s.PGSQL.<port>`.
    Unix(std::path::PathBuf),
}

/// The classified `DriverError::Config` message for a unix-domain-socket host
/// requested on a target without unix-domain sockets (e.g. Windows).
///
/// [`resolve_endpoint`] classifies an absolute-path host as [`Endpoint::Unix`]
/// from purely portable data (a leading `/`), so the classification is the same on
/// every platform. The *dial*, however, is platform-specific: a non-unix target has
/// no `UnixStream` to open, so both drivers reject an [`Endpoint::Unix`] there with
/// this message — a loud, classified fault, never a silent fall-through to TCP or a
/// panic. Defined once so the two drivers cannot drift in what they report.
pub const UNIX_SOCKET_UNSUPPORTED: &str =
    "unix-domain sockets are not available on this platform; use a TCP host (host:port)";

/// The classified `DriverError::Config` message for a unix-domain-socket host
/// requested with [`SslMode::Require`].
///
/// A local kernel socket is trusted by filesystem permissions, not TLS, and
/// PostgreSQL does not offer TLS there — so a required-TLS unix connection can
/// never be honored. Both drivers reject it with this message via
/// [`Endpoint::reject_unix_tls_required`]; defined once so they cannot drift in
/// what they report (the `SslMode::Require`-over-unix peer of
/// [`UNIX_SOCKET_UNSUPPORTED`]).
pub const UNIX_SOCKET_TLS_REQUIRED: &str =
    "SslMode::Require cannot be honored over a unix-domain socket \
     (TLS is not available on a local socket)";

impl Endpoint {
    /// Whether this endpoint is a unix-domain socket.
    ///
    /// A driver uses this to gate the TCP-only steps (`TCP_NODELAY`, the
    /// `SSLRequest` probe) and to reject `SslMode::Require`, which a local socket
    /// cannot satisfy.
    #[must_use]
    #[inline]
    pub fn is_unix(&self) -> bool {
        matches!(self, Endpoint::Unix(_))
    }

    /// Reject the fail-loud unix-domain-socket + [`SslMode::Require`] combination.
    ///
    /// A local kernel socket never negotiates TLS, so a required-TLS unix
    /// connection is a pre-connect configuration fault — a classified
    /// [`DriverError::Config`](crate::DriverError::Config) carrying
    /// [`UNIX_SOCKET_TLS_REQUIRED`], never a silent plaintext downgrade. Both
    /// drivers call this from their unix-capable dial path, so the rule lives ONCE
    /// and async/sync parity is a compiler fact — exactly as [`resolve_endpoint`]
    /// centralizes the unix-vs-TCP rule and
    /// [`resolve_ssl_mode`](ConnectConfig::resolve_ssl_mode) the SSL-mode rule.
    ///
    /// On a non-unix target the more fundamental [`UNIX_SOCKET_UNSUPPORTED`] fault
    /// takes precedence (a unix endpoint can never be dialed there at all), so the
    /// drivers gate their call behind `#[cfg(unix)]`.
    ///
    /// # Errors
    ///
    /// [`DriverError::Config`](crate::DriverError::Config)`(`[`UNIX_SOCKET_TLS_REQUIRED`]`)`
    /// when `self` is a unix socket and `ssl_mode` is [`SslMode::Require`];
    /// `Ok(())` otherwise (a TCP endpoint, or any non-`Require` mode).
    #[inline]
    pub fn reject_unix_tls_required(&self, ssl_mode: SslMode) -> Result<(), crate::DriverError> {
        if self.is_unix() && ssl_mode == SslMode::Require {
            return Err(crate::DriverError::Config(UNIX_SOCKET_TLS_REQUIRED));
        }
        Ok(())
    }
}

/// Resolve a `host` + `port` into an [`Endpoint`] via libpq's absolute-path rule
/// (see [`Endpoint`]).
///
/// Pure and infallible: an absolute-path host yields
/// [`Endpoint::Unix`]`(<host>/.s.PGSQL.<port>)`, every other host yields
/// [`Endpoint::Tcp`]`("host:port")`. Whether the resulting socket path or TCP
/// address actually connects is decided later, by the driver's connect syscall
/// (a missing socket file or a refused port is a classified transport error
/// there) — this function only classifies.
#[must_use]
pub fn resolve_endpoint(host: &str, port: u16) -> Endpoint {
    if host.starts_with('/') {
        // libpq's socket filename: `<dir>/.s.PGSQL.<port>`.
        Endpoint::Unix(std::path::Path::new(host).join(format!(".s.PGSQL.{port}")))
    } else {
        Endpoint::Tcp(format!("{host}:{port}"))
    }
}

/// Resolve the SCRAM channel binding for a built connection, centralizing the
/// rule ONE place both drivers thread through — exactly as [`resolve_endpoint`]
/// centralizes the unix-vs-TCP rule and
/// [`resolve_ssl_mode`](ConnectConfig::resolve_ssl_mode) the SSL-mode rule.
///
/// Given the negotiated transport's encryption state and, when encrypted, the
/// server's end-entity certificate DER (from `rustls::peer_certificates`), it
/// produces the [`ChannelBinding`](bsql_postgres_proto::scram::channel_binding::ChannelBinding)
/// the SCRAM credential carries into the engine:
///
/// - [`ChannelBindingMode::Disable`] → `Unbound` (plain SCRAM even over TLS).
/// - [`ChannelBindingMode::Prefer`] / [`Require`](ChannelBindingMode::Require)
///   over TLS → `Available` with the `tls-server-end-point` hash of the peer
///   cert (the engine then selects `-PLUS` iff the server offers it).
/// - `Prefer` on a plaintext channel → `Unbound` (binding is impossible).
/// - `Require` on a plaintext channel → a fail-closed
///   [`DriverError::Config`](crate::DriverError): channel binding needs TLS.
///
/// # Errors
///
/// [`DriverError::Config`](crate::DriverError) when `Require` is set over a
/// plaintext channel, or when an encrypted channel presents no peer certificate
/// (structurally unreachable after a verify-full handshake, surfaced fail-closed
/// rather than silently unbound).
#[cfg(feature = "scram")]
pub fn resolve_channel_binding(
    encrypted: bool,
    peer_cert_der: Option<&[u8]>,
    mode: ChannelBindingMode,
) -> Result<bsql_postgres_proto::scram::channel_binding::ChannelBinding, crate::DriverError> {
    use bsql_postgres_proto::scram::channel_binding::{tls_server_end_point, ChannelBinding};
    match mode {
        ChannelBindingMode::Disable => Ok(ChannelBinding::Unbound),
        ChannelBindingMode::Prefer | ChannelBindingMode::Require => {
            let require = mode == ChannelBindingMode::Require;
            if encrypted {
                match peer_cert_der {
                    Some(der) => Ok(ChannelBinding::Available {
                        data: tls_server_end_point(der),
                        require,
                    }),
                    // A completed verify-full TLS handshake always presents a
                    // peer certificate; its absence is a broken invariant, not a
                    // "server doesn't support binding" case — fail closed.
                    None => Err(crate::DriverError::Config(
                        "TLS channel binding could not read the server certificate",
                    )),
                }
            } else if require {
                Err(crate::DriverError::Config(
                    "channel_binding=require needs a TLS connection — the channel is \
                     plaintext (a unix socket, or an SSL-disabled/refused TCP connection), \
                     so no server certificate exists to bind to; use \
                     channel_binding=prefer/disable, or connect over TLS",
                ))
            } else {
                Ok(ChannelBinding::Unbound)
            }
        }
    }
}

/// Validate a config's raw startup parameters into wire
/// [`StartupParam`](bsql_postgres_proto::StartupParam)s.
///
/// Each `(name, value)` is checked for NUL, over-length, and reserved-name
/// violations (via the wire-authority constructor); the first failure surfaces
/// as a classified [`DriverError::Config`](crate::DriverError). Both drivers
/// call this at connect, BEFORE any startup byte is assembled — so a rejected
/// parameter can never reach the wire, and the `StartupMessage` can never carry
/// a NUL or override a reserved parameter.
///
/// Parse a PostgreSQL `statement_timeout` GUC value to whole milliseconds.
///
/// Accepts a bare integer (milliseconds — the unit
/// [`with_statement_timeout`](ConnectConfig::with_statement_timeout) emits) and
/// the unit-suffixed forms PostgreSQL accepts (`us`/`ms`/`s`/`min`/`h`/`d`),
/// optionally single-quoted. `0` → `Some(0)` (disabled). Returns `None` for
/// `DEFAULT`, an empty/negative/garbage value, or a unit this does not model — a
/// fail-SAFE signal the caller treats as "no bound / disarm the window", never a
/// spurious bound derived from a value it could not read. The result is clamped
/// to PostgreSQL's 32-bit millisecond GUC ceiling (`i32::MAX`), so an enormous
/// value never overflows the downstream clock arithmetic.
#[must_use]
pub fn parse_statement_timeout_ms(value: &str) -> Option<u64> {
    const MAX_MS: u64 = 2_147_483_647; // i32::MAX ms ≈ 24.8 days (PG's GUC ceiling)
    let v = value.trim().trim_matches('\'').trim();
    if v.is_empty() {
        return None;
    }
    // Split the leading digit run from an optional unit suffix.
    let digits_end = v.bytes().take_while(|b| b.is_ascii_digit()).count();
    let (num_str, unit) = (v.get(..digits_end)?, v.get(digits_end..)?);
    let num: u64 = num_str.parse().ok()?;
    let ms = match unit.trim() {
        "" | "ms" => num,
        // Integer floor via `checked_div` (the `/` operator is crate-forbidden);
        // a sub-millisecond `us` budget floors toward `0`, which the caller reads
        // as "disabled" — the SAFE direction (no client bound).
        "us" => num.checked_div(1000)?,
        "s" => num.checked_mul(1000)?,
        "min" => num.checked_mul(60_000)?,
        "h" => num.checked_mul(3_600_000)?,
        "d" => num.checked_mul(86_400_000)?,
        _ => return None, // an unmodeled unit (or trailing garbage) → fail-safe
    };
    Some(ms.min(MAX_MS))
}

/// Derive the client-liveness WINDOW from a `statement_timeout` budget (ms) and
/// the connection's `connect_timeout`: `budget + connect_timeout` (saturating), or
/// `None` when `budget_ms == 0` (disabled → no client bound). One authority for
/// the connect-time derivation AND the runtime re-derivation after an observed
/// `SET`, so the two cannot drift.
#[must_use]
pub fn window_from_statement_timeout_ms(budget_ms: u64, connect_timeout_secs: u64) -> Option<Duration> {
    if budget_ms == 0 {
        // Disabled: no server budget, so no finite client window can be safe.
        return None;
    }
    Some(Duration::from_millis(budget_ms).saturating_add(Duration::from_secs(connect_timeout_secs)))
}

/// The effect an EXECUTED SQL statement has on the session `statement_timeout`,
/// classified conservatively from its text so the client-liveness window can be
/// re-derived (never left STALE below a raised server budget, which would cut a
/// query the server now allows). Only a TOP-LEVEL `SET`/`RESET` is recognized;
/// every ambiguous form fails SAFE to [`Disarm`](Self::Disarm) (drop the window,
/// never a false cut), and every non-`SET`/`RESET` statement is
/// [`Unchanged`](Self::Unchanged).
// Deliberately NOT `#[non_exhaustive]`: the drivers match it EXHAUSTIVELY (per the
// crate convention), so a future effect variant forces a classification decision
// at every application site rather than a silent wildcard fall-through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementTimeoutEffect {
    /// The statement does not observably change `statement_timeout`.
    Unchanged,
    /// `SET [SESSION] statement_timeout = <v>` to a parseable budget (ms; `0` =
    /// disabled). Re-derive the window to this budget.
    SetTo(u64),
    /// `RESET statement_timeout` / `RESET ALL`: back to the connect-time budget.
    ResetToConnect,
    /// A `statement_timeout` change this cannot pin to an exact value (`SET LOCAL`
    /// — transaction-scoped; an unparseable value; `= DEFAULT`): DISARM the window
    /// (drop the client bound rather than risk cutting a legit query).
    Disarm,
}

/// Classify an executed SQL statement's effect on `statement_timeout` — see
/// [`StatementTimeoutEffect`].
///
/// Cheap on the common path: a statement whose first token is not (case-
/// insensitively) `SET` or `RESET` is `Unchanged` after a few-byte check, with no
/// full-text scan. A multi-statement batch is split on `;` and the LAST
/// SET/RESET wins (PostgreSQL applies them in order), with ANY ambiguous piece
/// forcing `Disarm`.
///
/// # Disarm-on-suspicion (the `set_config` catch)
///
/// A `statement_timeout` change made through `set_config('statement_timeout', …)`
/// — the ONLY in-session GUC-setter FUNCTION, reachable from a `SELECT`, a `DO`
/// block, or any statement — is not a `SET`, so the per-piece pass classifies it
/// `Unchanged`. Trusting the window then would leave it STALE-LOW under a
/// `set_config` RAISE — a false cut. So BEFORE the precise pass, if the whole text
/// contains BOTH `set_config` AND `statement_timeout` (ASCII-case-insensitive),
/// this DISARMS the window unconditionally: the `set_config` value cannot be
/// pinned from the text (it may be computed, quoted oddly, or transaction-local
/// via the third arg), so dropping the client bound is the only fail-SAFE choice —
/// keepalive still bounds a dead kernel, and a false cut is impossible with no
/// window. Requiring `set_config` too (not merely the GUC name) keeps a query that
/// only MENTIONS `statement_timeout` as data (a `WHERE name = 'statement_timeout'`,
/// a log row) from disarming — that stays `Unchanged`.
///
/// # The residual is the theoretical floor, not a discipline gap
///
/// What remains unobservable is a `statement_timeout` change whose executed SQL
/// text contains NO contiguous mention of the GUC name: a `SELECT my_func()` whose
/// body calls `set_config`, an `EXECUTE prepared_stmt` whose prepared body does, or
/// an adversarial `set_config('statement' || '_timeout', …)`. PostgreSQL does NOT
/// report `statement_timeout` via `ParameterStatus` (it is absent from the
/// hard-wired GUC_REPORT set), so a client CANNOT learn of such a change without a
/// per-query round trip. This is the theoretical limit — not closeable without
/// server cooperation or a forbidden extra round trip — and it is the ONLY residual
/// that can still leave the window stale-low; every observable form fails safe.
/// A `SET LOCAL` is `Disarm`ed (transaction scope is untrackable here).
#[must_use]
pub fn statement_timeout_effect(sql: &str) -> StatementTimeoutEffect {
    // Disarm-on-suspicion: a `set_config` of `statement_timeout` can move the
    // budget to a value the text cannot pin, so drop the window (fail-SAFE) rather
    // than trust one a raise would leave stale-low. `set_config` scanned first (the
    // rarer token → the common query without it fails this fast and never scans for
    // the GUC name). This overrides a same-batch precise `SetTo`, since a
    // `set_config` later in the batch could be the effective value.
    if contains_ascii_ci(sql, b"set_config") && contains_ascii_ci(sql, b"statement_timeout") {
        return StatementTimeoutEffect::Disarm;
    }
    let mut effect = StatementTimeoutEffect::Unchanged;
    for piece in sql.split(';') {
        match classify_one_statement(piece.trim()) {
            StatementTimeoutEffect::Unchanged => {}
            // Any ambiguous piece taints the whole batch — fail safe.
            StatementTimeoutEffect::Disarm => return StatementTimeoutEffect::Disarm,
            // A recognized SET/RESET: last one in the batch wins.
            other => effect = other,
        }
    }
    effect
}

/// Whether `haystack` contains `needle` as an ASCII-case-insensitive substring,
/// allocation-free (`to_ascii_lowercase` would allocate a `String` per query).
///
/// `needle` must be non-empty ASCII lowercase (`windows(0)` would panic, so the
/// callers pass fixed non-empty literals). A haystack shorter than `needle` yields
/// no windows → `false`. No indexing, no arithmetic — [`<[u8]>::windows`] and
/// [`<[u8]>::eq_ignore_ascii_case`] are total.
fn contains_ascii_ci(haystack: &str, needle: &[u8]) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
}

/// The window update an executed statement's [`StatementTimeoutEffect`] implies,
/// resolved against the connect-time budget context — the ONE mapping authority
/// both drivers (and both the connection-level verbs and the transaction-guard
/// verbs) apply, so the effect→window derivation cannot drift between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowAction {
    /// The statement did not observably touch `statement_timeout`: leave the
    /// steady client-liveness window exactly as it is.
    Unchanged,
    /// Re-derive the steady window to this value (`None` = disarm / unbounded —
    /// the historical steady read; a `Some` window is the black-hole bound).
    Set(Option<Duration>),
}

/// Resolve the [`WindowAction`] an executed statement implies for the client-
/// liveness window, given the connection's `connect_timeout` (for a runtime `SET`)
/// and the connect-time baseline window (for a `RESET`). One authority for both
/// drivers, so a `SET`/`RESET`/`set_config`/disarm never re-derives differently on
/// async vs sync, nor between a connection verb and a transaction-guard verb.
#[must_use]
pub fn window_after_statement(
    sql: &str,
    connect_timeout_secs: u64,
    connect_baseline: Option<Duration>,
) -> WindowAction {
    match statement_timeout_effect(sql) {
        StatementTimeoutEffect::Unchanged => WindowAction::Unchanged,
        StatementTimeoutEffect::SetTo(budget_ms) => {
            WindowAction::Set(window_from_statement_timeout_ms(budget_ms, connect_timeout_secs))
        }
        StatementTimeoutEffect::ResetToConnect => WindowAction::Set(connect_baseline),
        StatementTimeoutEffect::Disarm => WindowAction::Set(None),
    }
}

/// Classify a SINGLE trimmed statement (no `;`). See [`statement_timeout_effect`].
fn classify_one_statement(p: &str) -> StatementTimeoutEffect {
    // `RESET statement_timeout` / `RESET ALL` → back to the connect-time budget.
    if let Some(rest) = strip_leading_keyword(p, "reset") {
        let target = rest.trim_start();
        if starts_with_keyword(target, "statement_timeout") || starts_with_keyword(target, "all") {
            return StatementTimeoutEffect::ResetToConnect;
        }
        return StatementTimeoutEffect::Unchanged;
    }
    // Only a `SET` can raise/lower the budget going forward.
    let Some(after_set) = strip_leading_keyword(p, "set") else {
        return StatementTimeoutEffect::Unchanged;
    };
    let after_set = after_set.trim_start();
    // Peel an optional SESSION / LOCAL qualifier.
    let (after_qual, is_local) = if let Some(r) = strip_leading_keyword(after_set, "local") {
        (r.trim_start(), true)
    } else if let Some(r) = strip_leading_keyword(after_set, "session") {
        (r.trim_start(), false)
    } else {
        (after_set, false)
    };
    // The GUC name runs up to whitespace or `=`. `find` yields a valid char
    // boundary (or the length), so `split_at` never panics — no indexing/unwrap.
    let name_end = match after_qual.find(|c: char| c.is_ascii_whitespace() || c == '=') {
        Some(i) => i,
        None => after_qual.len(),
    };
    let (name, tail_raw) = after_qual.split_at(name_end);
    let tail = tail_raw.trim_start();
    if !name.eq_ignore_ascii_case("statement_timeout") {
        // SET of a DIFFERENT GUC (its value merely mentioning the word is fine).
        return StatementTimeoutEffect::Unchanged;
    }
    // Expect `= <v>` or `TO <v>`.
    let value = if let Some(v) = tail.strip_prefix('=') {
        v.trim()
    } else if let Some(v) = strip_leading_keyword(tail, "to") {
        v.trim()
    } else {
        // `SET statement_timeout` in a form we don't understand → fail-safe.
        return StatementTimeoutEffect::Disarm;
    };
    if is_local {
        // Transaction-scoped: its revert is untrackable here → drop the bound.
        return StatementTimeoutEffect::Disarm;
    }
    match parse_statement_timeout_ms(value) {
        Some(ms) => StatementTimeoutEffect::SetTo(ms),
        None => StatementTimeoutEffect::Disarm, // DEFAULT / unparseable
    }
}

/// If `s` begins (case-insensitively) with `kw` at a word boundary (followed by
/// whitespace, `=`, or end), return the remainder after `kw`; else `None`.
fn strip_leading_keyword<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let head = s.get(..kw.len())?;
    if !head.eq_ignore_ascii_case(kw) {
        return None;
    }
    let rest = s.get(kw.len()..)?;
    if rest.is_empty() || rest.starts_with(|c: char| c.is_ascii_whitespace() || c == '=') {
        Some(rest)
    } else {
        None
    }
}

/// Whether `s` begins (case-insensitively) with `kw` at a word boundary.
fn starts_with_keyword(s: &str, kw: &str) -> bool {
    strip_leading_keyword(s, kw).is_some()
}

/// The wire `StartupParam` carries the precise cause internally
/// (`StartupParamError`); it is collapsed to a `&'static str` here because
/// [`DriverError::Config`](crate::DriverError) is the crate's established
/// pre-connect-validation shape and does not interpolate a dynamic value.
pub fn validate_startup_params(
    config: &ConnectConfig,
) -> Result<Vec<bsql_postgres_proto::StartupParam>, crate::DriverError> {
    use bsql_postgres_proto::{StartupParam, StartupParamError};
    config
        .startup_params()
        .iter()
        .map(|(name, value)| {
            StartupParam::new(name, value).map_err(|err| {
                crate::DriverError::Config(match err {
                    StartupParamError::Name(_) => "invalid startup parameter name",
                    StartupParamError::Value(_) => "invalid startup parameter value",
                    StartupParamError::Reserved => {
                        "reserved startup parameter name — user, database, \
                         client_encoding, replication, and options are managed \
                         by the connection"
                    }
                    // `StartupParamError` is `#[non_exhaustive]`: a future
                    // rejection class must not silently pass as valid.
                    _ => "invalid startup parameter",
                })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_rejects_unknown_parameter() {
        // A typo'd parameter (e.g. `sslmod` for `sslmode`) must be rejected, not
        // silently ignored — silently ignoring it would leave the default mode.
        let err = ConnectConfig::from_dsn("postgres://u@h?sslmod=require");
        assert!(err.is_err(), "unknown DSN parameter must be rejected");
    }

    #[test]
    fn dsn_accepts_known_parameters() {
        // `expect`/`unwrap` are crate-denied even in tests; match instead.
        let cfg = match ConnectConfig::from_dsn(
            "postgres://u@h:5433/db?sslmode=require&connect_timeout=3",
        ) {
            Ok(c) => c,
            Err(e) => panic!("valid DSN must parse: {e}"),
        };
        assert_eq!(cfg.port, 5433);
        // An explicit `sslmode=require` is stored as `Some` (it wins over the
        // threat-scoped default at connect).
        assert_eq!(cfg.ssl_mode, Some(SslMode::Require));
        assert!(cfg.ssl_mode_is_explicit());
        assert_eq!(cfg.connect_timeout_secs, 3);
    }

    #[test]
    fn dsn_rejects_unknown_sslmode_value() {
        assert!(ConnectConfig::from_dsn("postgres://u@h?sslmode=verify-full").is_err());
    }

    #[test]
    fn dsn_rejects_valueless_parameter() {
        // `?sslmode` with no value would silently keep the default `prefer`,
        // downgrading SSL. A parameter missing `=` must be rejected loudly.
        assert!(ConnectConfig::from_dsn("postgres://u@h?sslmode").is_err());
    }

    #[test]
    fn channel_binding_defaults_to_prefer() {
        assert_eq!(
            ConnectConfig::new("h", "u").channel_binding_mode(),
            ChannelBindingMode::Prefer,
        );
    }

    #[test]
    fn builder_sets_channel_binding() {
        assert_eq!(
            ConnectConfig::new("h", "u")
                .channel_binding(ChannelBindingMode::Require)
                .channel_binding_mode(),
            ChannelBindingMode::Require,
        );
    }

    #[test]
    fn dsn_parses_channel_binding() {
        for (dsn, expected) in [
            ("postgres://u@h?channel_binding=require", ChannelBindingMode::Require),
            ("postgres://u@h?channel_binding=prefer", ChannelBindingMode::Prefer),
            ("postgres://u@h?channel_binding=disable", ChannelBindingMode::Disable),
        ] {
            let cfg = match ConnectConfig::from_dsn(dsn) {
                Ok(c) => c,
                Err(e) => panic!("valid DSN {dsn} must parse: {e}"),
            };
            assert_eq!(cfg.channel_binding_mode(), expected, "for {dsn}");
        }
        // Absent → the Prefer default.
        let cfg = match ConnectConfig::from_dsn("postgres://u@h") {
            Ok(c) => c,
            Err(e) => panic!("DSN must parse: {e}"),
        };
        assert_eq!(cfg.channel_binding_mode(), ChannelBindingMode::Prefer);
    }

    #[test]
    fn dsn_rejects_unknown_channel_binding_value() {
        // A malformed value must be loud, never a silent default that could
        // weaken a `require` intent.
        assert!(ConnectConfig::from_dsn("postgres://u@h?channel_binding=verify").is_err());
    }

    #[cfg(feature = "scram")]
    #[test]
    fn resolve_channel_binding_covers_every_policy_and_transport() {
        use bsql_postgres_proto::scram::channel_binding::{tls_server_end_point, ChannelBinding};
        const CERT: &[u8] = b"fake-server-cert-der";

        // Disable → Unbound regardless of transport.
        assert!(matches!(
            resolve_channel_binding(true, Some(CERT), ChannelBindingMode::Disable),
            Ok(ChannelBinding::Unbound),
        ));

        // Prefer over TLS → Available (require = false) with the cert hash.
        let cb = match resolve_channel_binding(true, Some(CERT), ChannelBindingMode::Prefer) {
            Ok(cb) => cb,
            Err(e) => panic!("prefer over TLS must resolve, got {e:?}"),
        };
        match cb {
            ChannelBinding::Available { data, require } => {
                assert!(!require);
                assert_eq!(data.as_slice(), tls_server_end_point(CERT).as_slice());
            }
            _ => panic!("prefer over TLS must be Available, got {cb:?}"),
        }

        // Require over TLS → Available (require = true).
        assert!(matches!(
            resolve_channel_binding(true, Some(CERT), ChannelBindingMode::Require),
            Ok(ChannelBinding::Available { require: true, .. }),
        ));

        // Prefer over plaintext → Unbound (binding impossible).
        assert!(matches!(
            resolve_channel_binding(false, None, ChannelBindingMode::Prefer),
            Ok(ChannelBinding::Unbound),
        ));

        // Require over plaintext → fail closed.
        assert!(matches!(
            resolve_channel_binding(false, None, ChannelBindingMode::Require),
            Err(crate::DriverError::Config(_)),
        ));

        // Encrypted but no peer cert (broken invariant) → fail closed.
        assert!(matches!(
            resolve_channel_binding(true, None, ChannelBindingMode::Prefer),
            Err(crate::DriverError::Config(_)),
        ));
    }

    #[test]
    fn startup_param_builders_preserve_order() {
        let cfg = ConnectConfig::new("localhost", "u")
            .with_search_path("myschema")
            .with_startup_param("statement_timeout", "5000")
            .with_application_name("bsql_test");
        assert_eq!(
            cfg.startup_params(),
            &[
                ("search_path".to_string(), "myschema".to_string()),
                ("statement_timeout".to_string(), "5000".to_string()),
                ("application_name".to_string(), "bsql_test".to_string()),
            ],
        );
    }

    #[test]
    fn a_fresh_config_has_no_startup_params() {
        assert!(ConnectConfig::new("localhost", "u").startup_params().is_empty());
    }

    /// `statement_timeout(Duration)` writes a millisecond value into the EXISTING
    /// startup-parameter map — footprint-neutral (no new field), and formatted the
    /// way PostgreSQL's integer-ms GUC expects.
    #[test]
    fn statement_timeout_formats_as_milliseconds() {
        fn param(cfg: &ConnectConfig) -> Option<&str> {
            cfg.startup_params()
                .iter()
                .find(|(k, _)| k == "statement_timeout")
                .map(|(_, v)| v.as_str())
        }

        // A whole-second / millisecond duration → its millisecond count.
        let cfg = ConnectConfig::new("h", "u").with_statement_timeout(Duration::from_millis(5000));
        assert_eq!(param(&cfg), Some("5000"));
        assert_eq!(
            ConnectConfig::new("h", "u")
                .with_statement_timeout(Duration::from_secs(2))
                .startup_params()
                .iter()
                .find(|(k, _)| k == "statement_timeout")
                .map(|(_, v)| v.as_str()),
            Some("2000"),
        );

        // Duration::ZERO → "0" (PG convention: DISABLED), explicit opt-out.
        let zero = ConnectConfig::new("h", "u").with_statement_timeout(Duration::ZERO);
        assert_eq!(param(&zero), Some("0"), "Duration::ZERO disables the timeout (PG convention)");

        // A non-zero SUB-millisecond duration rounds UP to 1 ms — never down to 0
        // (which would silently DISABLE the guardrail).
        let sub_ms = ConnectConfig::new("h", "u").with_statement_timeout(Duration::from_micros(500));
        assert_eq!(param(&sub_ms), Some("1"), "a sub-ms request must not collapse to 0/disabled");
        let one_ns = ConnectConfig::new("h", "u").with_statement_timeout(Duration::from_nanos(1));
        assert_eq!(param(&one_ns), Some("1"));

        // An enormous duration is capped at PG's 32-bit GUC ceiling (i32::MAX ms).
        let huge = ConnectConfig::new("h", "u").with_statement_timeout(Duration::from_secs(60 * 60 * 24 * 365));
        assert_eq!(param(&huge), Some("2147483647"), "capped at i32::MAX ms");
    }

    /// It rides the SAME map as the other startup builders (footprint-neutral),
    /// composes with them, and preserves insertion order.
    #[test]
    fn statement_timeout_composes_with_other_startup_params() {
        let cfg = ConnectConfig::new("h", "u")
            .with_search_path("myschema")
            .with_statement_timeout(Duration::from_millis(200));
        assert_eq!(
            cfg.startup_params(),
            &[
                ("search_path".to_string(), "myschema".to_string()),
                ("statement_timeout".to_string(), "200".to_string()),
            ],
        );
    }

    /// The client-liveness window derives from a configured `statement_timeout`:
    /// `None` when unset / disabled / non-integer, and `statement_timeout +
    /// connect_timeout` (last-wins, case-insensitive) otherwise.
    #[test]
    fn client_liveness_window_is_derived_from_statement_timeout() {
        // Unset → no window (the historical unbounded steady read).
        assert_eq!(
            ConnectConfig::new("h", "u").client_liveness_window(),
            None,
            "no statement_timeout → no client window",
        );

        // Set via the blessed builder → statement_timeout + connect_timeout
        // (5000 ms + 7 s = 12000 ms).
        let cfg = ConnectConfig::new("h", "u")
            .connect_timeout(7)
            .with_statement_timeout(Duration::from_millis(5000));
        assert_eq!(cfg.client_liveness_window(), Some(Duration::from_millis(12_000)));

        // `0` (explicitly disabled) → no window: no query budget, so no safe bound.
        assert_eq!(
            ConnectConfig::new("h", "u")
                .with_statement_timeout(Duration::ZERO)
                .client_liveness_window(),
            None,
        );

        // A sub-ms budget rounds up to 1 ms in the GUC, so the window is 1 ms +
        // connect_timeout (2 s) = 2001 ms (never collapses to "disabled").
        assert_eq!(
            ConnectConfig::new("h", "u")
                .connect_timeout(2)
                .with_statement_timeout(Duration::from_micros(500))
                .client_liveness_window(),
            Some(Duration::from_millis(2001)),
        );

        // A form the parser still cannot pin (`DEFAULT`) → fail-SAFE None (no
        // spurious bound), NOT a parse panic. (A unit-suffixed `"30s"` DOES parse
        // now — covered in the last-wins block above.)
        assert_eq!(
            ConnectConfig::new("h", "u")
                .with_startup_param("statement_timeout", "DEFAULT")
                .client_liveness_window(),
            None,
        );

        // Last-wins + case-insensitive GUC-name folding: the SECOND value applies,
        // matching how PostgreSQL resolves a repeated startup param.
        // 2000 ms + connect_timeout 1 s = 3000 ms.
        let last_wins = ConnectConfig::new("h", "u")
            .connect_timeout(1)
            .with_startup_param("Statement_Timeout", "1000")
            .with_startup_param("statement_timeout", "2000");
        assert_eq!(
            last_wins.client_liveness_window(),
            Some(Duration::from_millis(3000)),
        );

        // A raw unit-suffixed startup param NOW derives a window (minor-fix: parse
        // PostgreSQL's duration forms, not just bare ms).
        assert_eq!(
            ConnectConfig::new("h", "u")
                .connect_timeout(0)
                .with_startup_param("statement_timeout", "30s")
                .client_liveness_window(),
            Some(Duration::from_millis(30_000)),
        );
    }

    /// The `statement_timeout` value parser accepts PostgreSQL's duration forms
    /// and fail-SAFEs (`None`) on anything it cannot pin to whole milliseconds.
    #[test]
    fn statement_timeout_value_parsing() {
        use super::parse_statement_timeout_ms as p;
        assert_eq!(p("5000"), Some(5000)); // bare ms
        assert_eq!(p("'5000'"), Some(5000)); // quoted
        assert_eq!(p("0"), Some(0)); // disabled
        assert_eq!(p("30s"), Some(30_000));
        assert_eq!(p("'1min'"), Some(60_000));
        assert_eq!(p("2h"), Some(7_200_000));
        assert_eq!(p("1d"), Some(86_400_000));
        assert_eq!(p("500ms"), Some(500));
        assert_eq!(p("500us"), Some(0)); // sub-ms floors to 0 = disabled (SAFE)
        assert_eq!(p("default"), None); // can't pin → fail-safe
        assert_eq!(p("garbage"), None);
        assert_eq!(p(""), None);
        assert_eq!(p("60zz"), None); // unmodeled unit → fail-safe
        // Clamped to PG's i32::MAX ms ceiling.
        assert_eq!(p("999999999999"), Some(2_147_483_647));
    }

    /// The SET/RESET classifier: the common explicit forms are recognized, and
    /// every ambiguous form fails SAFE (`Disarm`) or is `Unchanged` — NEVER a
    /// mis-read that leaves a window stale below a raised budget.
    #[test]
    fn statement_timeout_effect_classification() {
        use super::{statement_timeout_effect as e, StatementTimeoutEffect as E};
        // The common explicit SET a consumer types → re-derive to the new budget.
        assert_eq!(e("SET statement_timeout = '30s'"), E::SetTo(30_000));
        assert_eq!(e("set statement_timeout to 60000"), E::SetTo(60_000));
        assert_eq!(e("SET SESSION statement_timeout = 0"), E::SetTo(0)); // disable
        assert_eq!(e("SET statement_timeout='5s'"), E::SetTo(5000)); // no spaces
        // RESET → back to the connect-time budget.
        assert_eq!(e("RESET statement_timeout"), E::ResetToConnect);
        assert_eq!(e("reset all"), E::ResetToConnect);
        // Ambiguous → Disarm (fail-safe, never a false cut).
        assert_eq!(e("SET LOCAL statement_timeout = '60s'"), E::Disarm);
        assert_eq!(e("SET statement_timeout = DEFAULT"), E::Disarm);
        // A leading SET in a batch is seen; the trailing query rides the new budget.
        assert_eq!(e("SET statement_timeout='60s'; SELECT 1"), E::SetTo(60_000));
        // Unrelated statements never touch the window.
        assert_eq!(e("SELECT 1"), E::Unchanged);
        assert_eq!(e("SET search_path = myschema"), E::Unchanged);
        assert_eq!(e("RESET search_path"), E::Unchanged);
        // Disarm-on-suspicion: a `set_config` of `statement_timeout` cannot be
        // pinned from the text → DISARM (fail-safe), never a stale-low window.
        assert_eq!(e("SELECT set_config('statement_timeout','300s',false)"), E::Disarm);
        assert_eq!(e("select SET_CONFIG('statement_timeout', $1, false)"), E::Disarm); // case-insensitive
        assert_eq!(
            e("DO $$ BEGIN PERFORM set_config('statement_timeout','5min',false); END $$"),
            E::Disarm, // `;` inside the block does not hide the suspicion (whole-text scan)
        );
        // A same-batch `set_config` overrides a precise `SET` (it could win) → Disarm.
        assert_eq!(
            e("SET statement_timeout='5s'; SELECT set_config('statement_timeout','300s',false)"),
            E::Disarm,
        );
        // `set_config` of a DIFFERENT GUC does not disarm (no statement_timeout token).
        assert_eq!(e("SELECT set_config('search_path','x',false)"), E::Unchanged);
        // A query that MENTIONS statement_timeout as data (no set_config) keeps its
        // window — the `set_config` requirement stops a data mention from disarming.
        assert_eq!(e("SELECT * FROM audit WHERE guc = 'statement_timeout'"), E::Unchanged);
        // A value that merely mentions the word is not a false match.
        assert_eq!(e("SET application_name = 'statement_timeout'"), E::Unchanged);
    }

    #[test]
    fn window_after_statement_maps_every_effect() {
        use super::{window_after_statement as w, WindowAction as A};
        let baseline = Some(Duration::from_millis(7_000));
        // A precise runtime SET → new budget + connect_timeout (2 s here).
        assert_eq!(
            w("SET statement_timeout = '5s'", 2, baseline),
            A::Set(Some(Duration::from_millis(7_000))), // 5s + 2s
        );
        // RESET → back to the connect-time baseline (not a fresh derivation).
        assert_eq!(w("RESET statement_timeout", 2, baseline), A::Set(baseline));
        // A `SET statement_timeout = 0` disables the budget → no client window.
        assert_eq!(w("SET statement_timeout = 0", 2, baseline), A::Set(None));
        // Disarm-on-suspicion (set_config) → drop the window (fail-safe).
        assert_eq!(
            w("SELECT set_config('statement_timeout','300s',false)", 2, baseline),
            A::Set(None),
        );
        // An unrelated statement leaves the window exactly as it is.
        assert_eq!(w("SELECT 1", 2, baseline), A::Unchanged);
    }

    #[test]
    fn a_fresh_config_has_no_ca_roots() {
        assert!(ConnectConfig::new("localhost", "u").ca_roots_pem().is_none());
    }

    #[test]
    fn with_ca_roots_stores_the_raw_pem() {
        const PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let cfg = ConnectConfig::new("localhost", "u").with_ca_roots(PEM);
        assert_eq!(cfg.ca_roots_pem(), Some(PEM), "the raw PEM must round-trip");
    }

    #[test]
    fn dsn_sslrootcert_missing_file_is_a_loud_error() {
        // A present-but-unreadable `sslrootcert` path is a loud error carrying
        // the path — never a silent fallback to the default roots.
        let err =
            ConnectConfig::from_dsn("postgres://u@h?sslrootcert=/no/such/bsql/test/ca.pem");
        match err {
            // A parse failure is now a CLASSIFIED `DriverError` a consumer can
            // match (not a bare `String`); the informative message is preserved
            // and reachable via Display.
            Err(e) => {
                assert!(
                    matches!(e, crate::DriverError::ConfigDynamic(_)),
                    "a DSN parse failure must be a classified config error, got {e:?}",
                );
                assert!(e.is_config(), "and it must classify as a config error");
                assert!(
                    e.to_string().contains("sslrootcert"),
                    "the error must name the failing key, got {e}",
                );
            }
            Ok(_) => panic!("an unreadable sslrootcert file must not parse silently"),
        }
    }

    #[test]
    fn dsn_sslrootcert_reads_the_file_into_ca_roots() {
        // The DSN reads the file NOW; the PEM is validated later (at connect).
        const PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let path =
            std::env::temp_dir().join(format!("bsql_w2_ca_{}.pem", std::process::id()));
        if std::fs::write(&path, PEM).is_err() {
            panic!("temp-file write failed; cannot exercise sslrootcert read");
        }
        let dsn = format!("postgres://u@h?sslrootcert={}", path.display());
        let cfg = match ConnectConfig::from_dsn(&dsn) {
            Ok(c) => c,
            Err(e) => panic!("a DSN with a readable sslrootcert must parse: {e}"),
        };
        // Best-effort cleanup before asserting (a leftover temp file is harmless).
        drop(std::fs::remove_file(&path));
        assert_eq!(cfg.ca_roots_pem(), Some(PEM), "the file bytes must be stored");
    }

    #[test]
    fn validate_accepts_ordinary_params() {
        let cfg = ConnectConfig::new("localhost", "u")
            .with_search_path("s")
            .with_startup_param("statement_timeout", "5000");
        let wire = match validate_startup_params(&cfg) {
            Ok(w) => w,
            Err(e) => panic!("ordinary params must validate: {e}"),
        };
        assert_eq!(wire.len(), 2);
    }

    #[test]
    fn validate_rejects_nul_in_value_with_classified_error() {
        // A NUL is unrepresentable in the NUL-delimited StartupMessage; it must
        // be a classified pre-connect error, never a corrupt packet.
        let cfg = ConnectConfig::new("localhost", "u").with_startup_param("search_path", "a\0b");
        match validate_startup_params(&cfg) {
            Err(crate::DriverError::Config(msg)) => {
                assert_eq!(msg, "invalid startup parameter value");
            }
            other => panic!("a NUL value must be a classified Config error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_nul_in_name_with_classified_error() {
        let cfg = ConnectConfig::new("localhost", "u").with_startup_param("a\0b", "x");
        match validate_startup_params(&cfg) {
            Err(crate::DriverError::Config(msg)) => {
                assert_eq!(msg, "invalid startup parameter name");
            }
            other => panic!("a NUL name must be a classified Config error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_reserved_params() {
        // A consumer must not be able to override session identity or the
        // pinned client_encoding via a startup parameter.
        for reserved in ["user", "database", "client_encoding", "replication", "options"] {
            let cfg = ConnectConfig::new("localhost", "u").with_startup_param(reserved, "x");
            match validate_startup_params(&cfg) {
                Err(crate::DriverError::Config(msg)) => assert_eq!(
                    msg,
                    "reserved startup parameter name — user, database, \
                     client_encoding, replication, and options are managed \
                     by the connection",
                ),
                other => panic!("'{reserved}' must be rejected as reserved, got {other:?}"),
            }
        }
    }

    #[test]
    fn validate_reserved_check_is_case_insensitive() {
        // `Client_Encoding=LATIN1` must not slip past and break the UTF-8 pin.
        let cfg =
            ConnectConfig::new("localhost", "u").with_startup_param("Client_Encoding", "LATIN1");
        assert!(matches!(
            validate_startup_params(&cfg),
            Err(crate::DriverError::Config(_)),
        ));
    }

    #[test]
    fn resolve_endpoint_classifies_tcp_hosts() {
        // A hostname and a dotted-quad are both TCP endpoints `host:port`.
        assert_eq!(
            resolve_endpoint("localhost", 5432),
            Endpoint::Tcp("localhost:5432".to_string()),
        );
        assert_eq!(
            resolve_endpoint("127.0.0.1", 6000),
            Endpoint::Tcp("127.0.0.1:6000".to_string()),
        );
        assert!(!resolve_endpoint("localhost", 5432).is_unix());
    }

    #[test]
    fn resolve_endpoint_classifies_absolute_path_hosts_as_unix() {
        // libpq's rule: an absolute-path host selects the unix socket at
        // `<dir>/.s.PGSQL.<port>` — the exact filename PostgreSQL creates.
        assert_eq!(
            resolve_endpoint("/tmp", 5432),
            Endpoint::Unix(std::path::PathBuf::from("/tmp/.s.PGSQL.5432")),
        );
        assert_eq!(
            resolve_endpoint("/var/run/postgresql", 5433),
            Endpoint::Unix(std::path::PathBuf::from("/var/run/postgresql/.s.PGSQL.5433")),
        );
        assert!(resolve_endpoint("/tmp", 5432).is_unix());
    }

    #[test]
    fn resolve_endpoint_trailing_slash_and_root_are_unix() {
        // A trailing slash on the socket dir must not double-up; `Path::join`
        // handles the separator. Root (`/`) is degenerate but still a path host.
        assert_eq!(
            resolve_endpoint("/var/run/postgresql/", 5432),
            Endpoint::Unix(std::path::PathBuf::from("/var/run/postgresql/.s.PGSQL.5432")),
        );
        assert!(resolve_endpoint("/", 5432).is_unix());
    }

    /// The shared unix + `SslMode::Require` rejection both drivers call (from
    /// their `#[cfg(unix)]` dial path). This is the SINGLE source of the
    /// async/sync parity that two hand-duplicated per-driver checks + two live
    /// twin tests formerly pinned — a driver forgetting to reject is now
    /// impossible via divergence because both call THIS one helper.
    #[test]
    fn reject_unix_tls_required_is_a_loud_config_error_only_for_unix_plus_require() {
        let unix = resolve_endpoint("/var/run/postgresql", 5432);
        assert!(unix.is_unix());
        // Unix + Require → the classified Config error naming the unix cause,
        // carrying the shared `UNIX_SOCKET_TLS_REQUIRED` message.
        match unix.reject_unix_tls_required(SslMode::Require) {
            Err(crate::DriverError::Config(msg)) => {
                assert_eq!(msg, UNIX_SOCKET_TLS_REQUIRED);
                assert!(
                    msg.contains("unix-domain socket"),
                    "the error must name the unix-socket cause, got {msg:?}"
                );
            }
            other => panic!("unix + Require must be a Config error, got {other:?}"),
        }
        // Unix + a non-Require mode is fine (Prefer = plaintext, no downgrade;
        // Disable = explicit plaintext).
        assert!(unix.reject_unix_tls_required(SslMode::Prefer).is_ok());
        assert!(unix.reject_unix_tls_required(SslMode::Disable).is_ok());
        // A TCP endpoint accepts Require (TLS is available there).
        let tcp = resolve_endpoint("db.example.com", 5432);
        assert!(!tcp.is_unix());
        assert!(tcp.reject_unix_tls_required(SslMode::Require).is_ok());
    }

    /// Resolve a parsed DSN's `host`/`port` the way a driver does at connect —
    /// the offline proof that a DSN reaches the intended [`Endpoint`] without a
    /// live PG.
    fn dsn_endpoint(dsn: &str) -> Result<Endpoint, String> {
        // `from_dsn` now returns a classified `DriverError`; this offline helper
        // only asserts on the resolved endpoint (Ok cases) or that parsing failed
        // (Err cases), so it flattens the error to its Display string — keeping the
        // helper's `Result<_, String>` shape and every existing assertion intact.
        let cfg = ConnectConfig::from_dsn(dsn).map_err(|e| e.to_string())?;
        Ok(resolve_endpoint(&cfg.host, cfg.port))
    }

    #[test]
    fn dsn_host_query_param_routes_to_unix_socket() {
        // WITNESS: libpq's unix-socket URL form — the socket dir rides `?host=`
        // (its leading `/` cannot ride the authority slot). This is SYMMETRIC with
        // `PGHOST=/tmp` via `from_env`, closing the constructor asymmetry.
        assert_eq!(
            dsn_endpoint("postgresql://user@/db?host=/tmp"),
            Ok(Endpoint::Unix(std::path::PathBuf::from("/tmp/.s.PGSQL.5432"))),
        );
        assert_eq!(
            dsn_endpoint("postgresql://user@/db?host=/var/run/postgresql"),
            Ok(Endpoint::Unix(std::path::PathBuf::from(
                "/var/run/postgresql/.s.PGSQL.5432"
            ))),
        );
    }

    #[test]
    fn dsn_host_query_param_carries_authority_port_into_the_socket_name() {
        // The authority `port` applies to the unix socket's filename too (libpq
        // parity): `@:5433` + `?host=/tmp` → `/tmp/.s.PGSQL.5433`.
        assert_eq!(
            dsn_endpoint("postgresql://user@:5433/db?host=/tmp"),
            Ok(Endpoint::Unix(std::path::PathBuf::from("/tmp/.s.PGSQL.5433"))),
        );
    }

    #[test]
    fn dsn_host_query_param_overrides_authority_host_tcp_parity() {
        // `host=` wins over the authority host (libpq: the query parameter wins),
        // and a plain-name value is a TCP host override — not a unix path.
        let cfg = match ConnectConfig::from_dsn("postgres://u@ignored:5433/db?host=realhost") {
            Ok(c) => c,
            Err(e) => panic!("DSN with a host override must parse: {e}"),
        };
        assert_eq!(cfg.host, "realhost");
        assert_eq!(cfg.port, 5433);
        assert_eq!(
            resolve_endpoint(&cfg.host, cfg.port),
            Endpoint::Tcp("realhost:5433".to_string()),
        );
    }

    #[test]
    fn dsn_normal_tcp_authority_still_resolves_to_tcp() {
        // A plain TCP DSN (host in the authority, no `host=`) is unaffected.
        assert_eq!(
            dsn_endpoint("postgres://u@db.example.com:6000/app"),
            Ok(Endpoint::Tcp("db.example.com:6000".to_string())),
        );
        assert_eq!(
            dsn_endpoint("postgres://u@localhost/app"),
            Ok(Endpoint::Tcp("localhost:5432".to_string())),
        );
    }

    #[test]
    fn unbracket_host_strips_only_a_fully_bracketed_literal() {
        // The single unbracket authority both `host_is_loopback` and the TLS
        // `ServerName` derivation call through. A fully-bracketed IPv6 literal is
        // unwrapped to the bare address; every other form — a DNS name, a bare IPv4,
        // or a MALFORMED half-open bracket — is returned verbatim (fail-safe: a
        // malformed host fails loudly at its own downstream parse, never mangled here).
        assert_eq!(unbracket_host("[::1]"), "::1", "a bracketed IPv6 literal is unwrapped");
        assert_eq!(unbracket_host("[2001:db8::1]"), "2001:db8::1");
        assert_eq!(unbracket_host("db.example.com"), "db.example.com", "a DNS host is untouched");
        assert_eq!(unbracket_host("127.0.0.1"), "127.0.0.1", "a bare IPv4 is untouched");
        assert_eq!(unbracket_host("::1"), "::1", "an already-bare IPv6 is untouched");
        assert_eq!(unbracket_host("[::1"), "[::1", "a leading-only bracket is not stripped");
        assert_eq!(unbracket_host("::1]"), "::1]", "a trailing-only bracket is not stripped");
        assert_eq!(unbracket_host(""), "", "the empty host is untouched");
    }

    #[test]
    fn dsn_bracketed_ipv6_without_port_uses_the_default_port() {
        // WITNESS: a bracketed IPv6 literal with NO explicit port. The port split
        // must key on the `]`, never on the address's internal colons — so
        // `[::1]/db` is the loopback host at the default 5432, not `Err(port "1]")`.
        let cfg = match ConnectConfig::from_dsn("postgres://u@[::1]/db") {
            Ok(c) => c,
            Err(e) => panic!("a bracketed IPv6 loopback with no port must parse: {e}"),
        };
        assert_eq!(cfg.host, "[::1]", "the brackets are kept for dialing");
        assert_eq!(cfg.port, 5432, "no explicit port ⇒ the default");
        assert!(host_is_loopback(&cfg.host), "[::1] is loopback");
        assert_eq!(
            resolve_endpoint(&cfg.host, cfg.port),
            Endpoint::Tcp("[::1]:5432".to_string()),
        );
    }

    #[test]
    fn dsn_bracketed_ipv6_remote_without_port_uses_the_default_port() {
        // A non-loopback bracketed IPv6 literal with no port: the full address
        // survives (internal colons intact) and the default port applies.
        let cfg = match ConnectConfig::from_dsn("postgres://u@[2001:db8::1]/db") {
            Ok(c) => c,
            Err(e) => panic!("a bracketed IPv6 remote with no port must parse: {e}"),
        };
        assert_eq!(cfg.host, "[2001:db8::1]");
        assert_eq!(cfg.port, 5432);
        assert!(!host_is_loopback(&cfg.host), "2001:db8::1 is not loopback");
        assert_eq!(
            resolve_endpoint(&cfg.host, cfg.port),
            Endpoint::Tcp("[2001:db8::1]:5432".to_string()),
        );
    }

    #[test]
    fn dsn_bracketed_ipv6_with_explicit_port_is_unchanged() {
        // The already-correct with-port form must keep parsing: the `]:port`
        // suffix splits the port, the internal colons stay in the host.
        let cfg = match ConnectConfig::from_dsn("postgres://u@[::1]:5432/db") {
            Ok(c) => c,
            Err(e) => panic!("a bracketed IPv6 with an explicit port must parse: {e}"),
        };
        assert_eq!(cfg.host, "[::1]");
        assert_eq!(cfg.port, 5432);
        let cfg2 = match ConnectConfig::from_dsn("postgres://u@[2001:db8::1]:6000/db") {
            Ok(c) => c,
            Err(e) => panic!("a bracketed IPv6 remote with a port must parse: {e}"),
        };
        assert_eq!(cfg2.host, "[2001:db8::1]");
        assert_eq!(cfg2.port, 6000);
    }

    #[test]
    fn dsn_malformed_bracketed_ipv6_is_a_loud_error() {
        // An unterminated literal or a bad port after `]` is a classified Err,
        // never a silent mis-parse.
        assert!(
            ConnectConfig::from_dsn("postgres://u@[::1/db").is_err(),
            "an unterminated IPv6 literal must be a loud error",
        );
        assert!(
            ConnectConfig::from_dsn("postgres://u@[::1]:notaport/db").is_err(),
            "a non-numeric port after ] must be a loud error",
        );
    }

    /// Resolve the effective SslMode for an unset config against `host`/`port`
    /// the way a driver does at connect — the offline proof of the threat-scoped
    /// default without a live PG.
    fn resolve_default(host: &str, port: u16) -> SslMode {
        let cfg = ConnectConfig::new(host, "u").port(port);
        assert!(!cfg.ssl_mode_is_explicit(), "a fresh config must be defaulted");
        cfg.resolve_ssl_mode(&resolve_endpoint(&cfg.host, cfg.port))
    }

    #[test]
    fn threat_scoped_default_local_endpoints_resolve_to_prefer() {
        // WITNESS: an UNSET SslMode over a LOCAL endpoint stays `Prefer` — no
        // network path to intercept. A unix socket, and every loopback TCP host.
        assert_eq!(resolve_default("/var/run/postgresql", 5432), SslMode::Prefer);
        assert_eq!(resolve_default("/tmp", 5432), SslMode::Prefer);
        assert_eq!(resolve_default("127.0.0.1", 5432), SslMode::Prefer);
        assert_eq!(resolve_default("127.0.0.5", 5432), SslMode::Prefer); // 127.0.0.0/8
        assert_eq!(resolve_default("::1", 5432), SslMode::Prefer);
        // A DSN authority brackets an IPv6 literal (`[::1]`); the brackets are
        // stripped for classification, so a genuinely-local IPv6 loopback is local.
        assert_eq!(resolve_default("[::1]", 5432), SslMode::Prefer);
        assert_eq!(resolve_default("localhost", 5432), SslMode::Prefer);
        // RFC 6761 names `localhost` case-insensitively.
        assert_eq!(resolve_default("LocalHost", 5432), SslMode::Prefer);
        assert_eq!(resolve_default("LOCALHOST", 5432), SslMode::Prefer);
    }

    #[test]
    fn threat_scoped_default_remote_endpoints_resolve_to_require() {
        // WITNESS: an UNSET SslMode over a REMOTE endpoint resolves to `Require`
        // — a remote TLS refusal becomes a loud error, never a silent plaintext
        // connect. Private-range addresses are STILL a network path → remote.
        assert_eq!(resolve_default("db.example.com", 5432), SslMode::Require);
        assert_eq!(resolve_default("10.0.0.5", 5432), SslMode::Require);
        assert_eq!(resolve_default("192.168.1.1", 5432), SslMode::Require);
        assert_eq!(resolve_default("172.16.0.9", 5432), SslMode::Require);
        assert_eq!(resolve_default("8.8.8.8", 5432), SslMode::Require);
        // A bracketed IPv6 literal is unbracketed before parsing, but a NON-loopback
        // one stays remote: a public address, and the IPv4-mapped `::ffff:127.0.0.1`
        // (which is not `::1`, so `Ipv6Addr::is_loopback` is false) — fail-safe.
        assert_eq!(resolve_default("[2001:db8::1]", 5432), SslMode::Require);
        assert_eq!(resolve_default("[::ffff:127.0.0.1]", 5432), SslMode::Require);
        // An otherwise-unparseable host is not a recognized loopback literal → remote.
        assert_eq!(resolve_default("[not-an-ip]", 5432), SslMode::Require);
    }

    #[test]
    fn explicit_ssl_mode_always_wins_over_the_threat_scoped_default() {
        // WITNESS: an EXPLICIT setting overrides the endpoint-scoped default in
        // BOTH directions.
        // Explicit Prefer to a REMOTE host → Prefer (the consumer opted out).
        let remote_prefer = ConnectConfig::new("db.example.com", "u").ssl_mode(SslMode::Prefer);
        assert!(remote_prefer.ssl_mode_is_explicit());
        assert_eq!(
            remote_prefer.resolve_ssl_mode(&resolve_endpoint(&remote_prefer.host, 5432)),
            SslMode::Prefer,
        );
        // Explicit Require to a LOCAL (loopback) host → Require.
        let local_require = ConnectConfig::new("localhost", "u").ssl_mode(SslMode::Require);
        assert_eq!(
            local_require.resolve_ssl_mode(&resolve_endpoint(&local_require.host, 5432)),
            SslMode::Require,
        );
        // Explicit Require to a UNIX host → Require (the unix-socket fail-loud is
        // a SEPARATE later check; resolution just honors the explicit choice).
        let unix_require = ConnectConfig::new("/tmp", "u").ssl_mode(SslMode::Require);
        assert_eq!(
            unix_require.resolve_ssl_mode(&resolve_endpoint(&unix_require.host, 5432)),
            SslMode::Require,
        );
        // Explicit Disable to a loopback host → Disable.
        let local_disable = ConnectConfig::new("127.0.0.1", "u").ssl_mode(SslMode::Disable);
        assert_eq!(
            local_disable.resolve_ssl_mode(&resolve_endpoint(&local_disable.host, 5432)),
            SslMode::Disable,
        );
    }

    #[test]
    fn explicit_dsn_sslmode_wins_over_the_threat_scoped_default() {
        // WITNESS: the explicit path also flows through the DSN — `sslmode=` (and
        // by the same mechanism `PGSSLMODE`, which stores `Some` identically)
        // always wins. `sslmode=require` to a LOCAL host stays Require; a REMOTE
        // host with no `sslmode=` defaults to Require anyway (the loud-remote rule).
        let cfg = match ConnectConfig::from_dsn("postgres://u@localhost?sslmode=require") {
            Ok(c) => c,
            Err(e) => panic!("valid DSN must parse: {e}"),
        };
        assert!(cfg.ssl_mode_is_explicit());
        assert_eq!(
            cfg.resolve_ssl_mode(&resolve_endpoint(&cfg.host, cfg.port)),
            SslMode::Require,
        );
    }

    #[test]
    fn a_fresh_config_leaves_ssl_mode_defaulted() {
        // The constructor stores no explicit mode — the resolution decides.
        assert!(!ConnectConfig::new("localhost", "u").ssl_mode_is_explicit());
    }

    /// WITNESS (D1): every `from_dsn` parse failure is a CLASSIFIED
    /// [`DriverError`](crate::DriverError) a consumer can `match` on — not a bare
    /// `String` — and the informative message (naming the offending value) is
    /// preserved and reachable via Display. Covers a bad scheme, a bad port, an
    /// empty user/host, and an unknown parameter.
    #[test]
    fn from_dsn_parse_failures_are_classified_matchable_driver_errors() {
        // (dsn, a substring the preserved message must still contain)
        let cases: &[(&str, &str)] = &[
            ("mysql://u@h/db", "postgres://"),
            ("postgres://u@h:99999/db", "invalid port: 99999"),
            ("postgres://u@h:notaport/db", "invalid port: notaport"),
            ("postgres://@h/db", "empty user"),
            ("postgres://u@/db", "host"),
            ("postgres://u@h?sslmode=verify-full", "unknown sslmode: verify-full"),
            ("postgres://u@h?bogus=1", "unknown DSN parameter: bogus"),
            ("postgres://u@h?sslmode", "missing '='"),
        ];
        for (dsn, needle) in cases {
            match ConnectConfig::from_dsn(dsn) {
                // A consumer matches the classified variant (or `is_config()`),
                // never a stringly-typed error.
                Err(e) => {
                    assert!(
                        matches!(e, crate::DriverError::ConfigDynamic(_)),
                        "DSN {dsn:?} must fail as a classified ConfigDynamic, got {e:?}",
                    );
                    assert!(e.is_config(), "DSN {dsn:?} must classify as a config error");
                    assert!(!e.is_disconnect(), "a parse error is never a disconnect");
                    assert!(
                        e.to_string().contains(needle),
                        "DSN {dsn:?} error must preserve {needle:?}, got {e}",
                    );
                }
                Ok(_) => panic!("DSN {dsn:?} must be a loud parse error, not a silent parse"),
            }
        }
    }

    #[test]
    fn dsn_empty_authority_host_without_host_param_is_a_loud_error() {
        // `postgres://u@/tmp/db` parses `/tmp/db` as the DATABASE on an EMPTY host
        // (URL grammar: authority ends at the first `/`), which is unroutable.
        // Reject it at parse time, naming the `host=` fix — never a silent
        // port-only TCP connect.
        match ConnectConfig::from_dsn("postgres://u@/tmp/db") {
            // A classified `DriverError` a consumer can match, with the
            // host=-form guidance preserved in the Display message.
            Err(e) => {
                assert!(
                    matches!(e, crate::DriverError::ConfigDynamic(_)) && e.is_config(),
                    "the empty-host error must be a classified config error, got {e:?}",
                );
                let msg = e.to_string();
                assert!(
                    msg.contains("host") && msg.contains("query parameter"),
                    "the empty-host error must point at the host= form, got {msg}"
                );
            }
            Ok(_) => panic!("an empty authority host must be a loud parse error"),
        }
    }
}
