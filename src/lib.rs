//! Shared harness primitives for the bsql end-to-end / competitor / peak-RSS
//! benchmarks.
//!
//! Kept deliberately client-agnostic: the only cross-client couplings here are
//! (1) the peak-RSS reader, (2) the connection coordinates, and (3) the exact
//! SQL text every client runs, so a scenario is byte-identical work regardless
//! of which driver executes it. Each client's own connect + prepare + fetch
//! lives with that client (in the benches / bins), because the three driver
//! APIs share no trait.

/// PostgreSQL host. TCP loopback: the cross-CLIENT comparison runs EVERY client
/// over the same loopback TCP (the competitor drivers wired here dial TCP), so a
/// group's bars are apples-to-apples. Loopback TCP is in-kernel (no wire, no
/// switch), which removes the network as a noise source; the residual noise is
/// scheduler + allocator, controlled by the quiet-system + warmup discipline in
/// the scripts. bsql ALSO speaks a unix-domain socket now — the
/// `unix_vs_tcp` bench isolates the bsql-only transport delta (TCP vs the local
/// socket the original bsql used) via [`bsql_config`] vs [`bsql_config_unix`].
pub const PG_HOST: &str = "127.0.0.1";
/// PostgreSQL port.
pub const PG_PORT: u16 = 5432;
/// The directory holding PostgreSQL's unix-domain socket (`<dir>/.s.PGSQL.<port>`).
/// Homebrew PG on macOS defaults to `/tmp`; a Debian/Ubuntu server uses
/// `/var/run/postgresql`. Overridable at build time via `BSQL_BENCH_SOCKET_DIR`.
pub const PG_UNIX_SOCKET_DIR: &str = match option_env!("BSQL_BENCH_SOCKET_DIR") {
    Some(dir) => dir,
    None => "/tmp",
};
/// PostgreSQL user (trust auth on the local dev server).
pub const PG_USER: &str = "smir-ant";
/// PostgreSQL database.
pub const PG_DB: &str = "postgres";

/// Rows seeded into `bench_items` by `setup/pg_setup.sql`.
pub const SEED_ROWS: i32 = 10_000;

/// SELECT a single row by primary key. One row, three columns, every column
/// read by the caller.
pub const SQL_SELECT_BY_PK: &str = "SELECT id, name, val FROM bench_items WHERE id = $1";

/// SELECT a bounded range by primary key: returns the first `$1` rows. Drives
/// the multi-row scaling scenarios (10 / 100 / 1000 / 10000).
pub const SQL_SELECT_RANGE: &str = "SELECT id, name, val FROM bench_items WHERE id <= $1 ORDER BY id";

/// Single-row INSERT into the unlogged sink table.
pub const SQL_INSERT_ONE: &str = "INSERT INTO bench_ins (id, name, val) VALUES ($1, $2, $3)";

/// JOIN + GROUP BY aggregation over the first `$1` items: the representative
/// "complex query" scenario. Returns one row per category label with a count
/// and a sum.
pub const SQL_JOIN_AGG: &str = "SELECT c.label, count(*)::int8, sum(i.val)::int8 \
     FROM bench_items i JOIN bench_cat c ON i.val = c.val \
     WHERE i.id <= $1 GROUP BY c.label ORDER BY c.label";

/// A `libpq`-style connection string for the competitor drivers (loopback TCP,
/// TLS disabled — same transport regime bsql uses).
pub fn pg_conn_string() -> String {
    format!("host={PG_HOST} port={PG_PORT} user={PG_USER} dbname={PG_DB} sslmode=disable")
}

/// A `postgres://` URL for sqlx (loopback TCP, TLS disabled).
pub fn pg_url() -> String {
    format!("postgres://{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=disable")
}

/// A `bsql` `ConnectConfig` for the loopback TCP server with TLS disabled.
pub fn bsql_config() -> bsql::pg::ConnectConfig {
    bsql::pg::ConnectConfig::new(PG_HOST, PG_USER)
        .port(PG_PORT)
        .database(PG_DB)
        .ssl_mode(bsql::pg::SslMode::Disable)
}

/// A `bsql` `ConnectConfig` for the LOCAL UNIX-DOMAIN socket — the absolute-path
/// host ([`PG_UNIX_SOCKET_DIR`]) selects `<dir>/.s.PGSQL.<port>` (a unix socket
/// is plaintext, so no `ssl_mode` override is needed). This is the transport the
/// original bsql used locally; the `unix_vs_tcp` bench measures it against
/// [`bsql_config`] to isolate the transport delta.
pub fn bsql_config_unix() -> bsql::pg::ConnectConfig {
    bsql::pg::ConnectConfig::new(PG_UNIX_SOCKET_DIR, PG_USER)
        .port(PG_PORT)
        .database(PG_DB)
}

/// Current process peak resident-set size, in bytes.
///
/// Reads `getrusage(RUSAGE_SELF).ru_maxrss` and normalises the platform unit:
/// Linux reports kibibytes, macOS/BSD report bytes. This is the process-lifetime
/// MAXIMUM resident size — the exact figure the original bsql's RSS harness
/// reported (peak, not instantaneous) — so a fresh process that connects once
/// and runs a fixed workload yields that workload's footprint.
#[must_use]
pub fn peak_rss_bytes() -> u64 {
    use core::mem::MaybeUninit;

    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes a fully-initialised `rusage` through the
    // out-pointer and returns 0 on success; `RUSAGE_SELF` is always a valid
    // `who` argument and `usage` is a live, correctly-aligned allocation. We
    // only read the struct after confirming the return code is 0.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return 0;
    }
    // SAFETY: `rc == 0` means the kernel fully initialised the struct above.
    let usage = unsafe { usage.assume_init() };
    let maxrss = usage.ru_maxrss;
    if maxrss < 0 {
        return 0;
    }
    let maxrss = maxrss as u64;
    #[cfg(target_os = "linux")]
    {
        maxrss.saturating_mul(1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        maxrss
    }
}

/// Format a byte count as a MiB string with two decimals (e.g. `1.59 MB`),
/// matching the original harness's report units.
#[must_use]
pub fn mib(bytes: u64) -> String {
    let mib = bytes as f64 / (1024.0 * 1024.0);
    format!("{mib:.2} MB")
}

/// Print the process peak RSS in both machine-parseable and human form, reading
/// [`peak_rss_bytes`] EXACTLY ONCE so the two lines cannot disagree (a second
/// read after the first `println!` allocation could observe a higher peak).
pub fn report_rss() {
    let bytes = peak_rss_bytes();
    println!("PEAK_RSS_BYTES {bytes}");
    println!("PEAK_RSS {}", mib(bytes));
}

/// A process-unique base for INSERT ids, so two `rss_*` processes writing into
/// the shared `bench_ins` table (e.g. the RSS gate running its binaries in
/// parallel, or repeated runs) never collide on the primary key and never need a
/// cross-process `TRUNCATE`. `pid * 1_000_000` leaves room for the 1000 inserts
/// per run and stays far below the criterion INSERT bench's `1e15`-based ranges.
#[must_use]
pub fn insert_id_base() -> i64 {
    i64::from(std::process::id()) * 1_000_000
}

/// The peak-RSS workload every `rss_*` binary runs against ITS client: this
/// documents the shape so the four binaries stay identical in what they ask the
/// server to do. `SELECT_ITERS` single-row-by-PK reads, then `INSERT_ITERS`
/// single-row inserts, over one direct connection — mirroring the original
/// bsql's "10k SELECT-by-PK + 1k INSERT" RSS scenario.
pub const RSS_SELECT_ITERS: i32 = 10_000;
/// INSERT iterations for the peak-RSS workload (see [`RSS_SELECT_ITERS`]).
pub const RSS_INSERT_ITERS: i64 = 1_000;

// ─── Environment-overridable connection coordinates ─────────────────────────
//
// The single-op latency / RSS matrix runs against the SHARED local PostgreSQL
// on the fixed [`PG_HOST`]/[`PG_PORT`] above. The DEEP benchmarks (concurrency
// throughput, constant-memory streaming) instead point every client at a
// server named by the standard `PG*` env vars, because `scripts/xlang_measure_deep.sh`
// stands up a DEDICATED ephemeral PostgreSQL on its own port (max_connections
// raised so 128 held connections fit, and isolated so it neither disturbs nor is
// disturbed by any concurrent use of the shared server). The helpers below read
// those env vars with the shared-server constants as the fallback, so a client
// with no env set still targets the shared server. `match` (not `unwrap_or*`) is
// deliberate — the crate's clippy floor bans the silent-fallback combinator; a
// bench-config default is a legitimate, VISIBLE fallback.

/// Read env `key`, or fall back to `default`.
#[must_use]
pub fn env_or(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(v) => v,
        Err(_) => default.to_owned(),
    }
}

/// PostgreSQL host from `PGHOST` (default [`PG_HOST`]).
#[must_use]
pub fn pg_host_env() -> String {
    env_or("PGHOST", PG_HOST)
}

/// PostgreSQL port from `PGPORT` (default [`PG_PORT`]); an unparseable value
/// falls back to the default rather than aborting a benchmark on a typo.
#[must_use]
pub fn pg_port_env() -> u16 {
    match std::env::var("PGPORT") {
        Ok(v) => match v.parse::<u16>() {
            Ok(p) => p,
            Err(_) => PG_PORT,
        },
        Err(_) => PG_PORT,
    }
}

/// PostgreSQL user from `PGUSER` (default [`PG_USER`]).
#[must_use]
pub fn pg_user_env() -> String {
    env_or("PGUSER", PG_USER)
}

/// PostgreSQL database from `PGDATABASE` (default [`PG_DB`]).
#[must_use]
pub fn pg_db_env() -> String {
    env_or("PGDATABASE", PG_DB)
}

/// A `bsql` `ConnectConfig` from the `PG*` env (loopback TCP, TLS disabled) —
/// the env-driven peer of [`bsql_config`] used by the deep benchmarks.
#[must_use]
pub fn bsql_config_env() -> bsql::pg::ConnectConfig {
    bsql::pg::ConnectConfig::new(pg_host_env(), pg_user_env())
        .port(pg_port_env())
        .database(pg_db_env())
        .ssl_mode(bsql::pg::SslMode::Disable)
}

/// A `libpq`-style connection string from the `PG*` env (for tokio-postgres).
#[must_use]
pub fn pg_conn_string_env() -> String {
    format!(
        "host={} port={} user={} dbname={} sslmode=disable",
        pg_host_env(),
        pg_port_env(),
        pg_user_env(),
        pg_db_env(),
    )
}

/// A `postgres://` URL from the `PG*` env (for sqlx).
#[must_use]
pub fn pg_url_env() -> String {
    format!(
        "postgres://{}@{}:{}/{}?sslmode=disable",
        pg_user_env(),
        pg_host_env(),
        pg_port_env(),
        pg_db_env(),
    )
}

/// The by-PK SELECT the CONCURRENCY benchmark runs on every client — one row,
/// three columns, every column read (identical work regardless of driver). It
/// is the SAME shape as [`SQL_SELECT_BY_PK`] but against a small `bench_items`
/// table seeded into the ephemeral server; bsql runs it through the typed
/// compile-checked `query!` flagship (see `src/bin/concurrency_pg.rs`).
pub const SQL_CONC_BY_PK: &str = "SELECT id, name, val FROM bench_items WHERE id = $1";

/// The SQL the STREAMING benchmark runs on every client: a synthetic result of
/// `rows` rows, three columns (int4 / text / int4), produced entirely on the
/// server by `generate_series` — so no seed data is needed and the row count is
/// a free parameter. bsql streams it in O(1) memory via `query_each_sql`; a
/// materialising client (libpq `PQexec`, tokio-postgres `query`) buffers all
/// `rows` at once. `g` ≤ 5·10⁶ and `g*2` ≤ 10⁷ both fit `int4`.
#[must_use]
pub fn stream_sql(rows: u64) -> String {
    format!(
        "SELECT (g)::int4 AS id, ('row_' || g) AS name, (g * 2)::int4 AS val \
         FROM generate_series(1, {rows}) AS g"
    )
}
