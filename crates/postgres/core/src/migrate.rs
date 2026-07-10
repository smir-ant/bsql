//! The migration RUNNER — applies a consumer's migration set to a live
//! PostgreSQL database, exactly once, in the SAME deterministic order the
//! build-time catalog replay uses.
//!
//! # What it guarantees
//!
//! - **Exactly once, in order.** Each migration runs once, in lexicographic
//!   order by its NAME (its path relative to the migrations directory — the
//!   same order `bsql-build` replays for the `query!` catalog). A ledger table
//!   ([`LEDGER_TABLE`]) records what ran; a re-run is a no-op.
//! - **Atomic per migration.** A migration's DDL and its ledger row are one
//!   transaction (PostgreSQL DDL is transactional): either the migration
//!   applied AND is recorded, or neither. A migration that fails mid-way rolls
//!   back and the runner STOPS with a classified [`MigrationError::MigrationFailed`]
//!   naming it — later migrations do not run.
//! - **Checksum-drift is loud.** An already-applied migration whose file
//!   CHANGED (its content checksum no longer matches the ledger) is a classified
//!   [`MigrationError::Drift`] — never silently re-run or ignored.
//! - **Append-only.** A migration inserted BEFORE, or deleting, an
//!   already-applied one is a classified [`MigrationError::Drift`] (the set must
//!   grow only at the end).
//! - **One runner at a time.** The run holds a session-level
//!   `pg_advisory_lock`, so two instances booting simultaneously serialize —
//!   the first applies, the second waits then sees an up-to-date ledger and does
//!   nothing. No double-apply race.
//!
//! # Non-transactional migrations
//!
//! A statement PostgreSQL refuses to run inside a transaction block (e.g.
//! `CREATE INDEX CONCURRENTLY`) makes the wrapping `BEGIN` fail loudly. Mark
//! such a migration with a `-- bsql:no-transaction` comment line: the runner
//! then applies its SQL OUTSIDE a transaction and records the ledger row
//! separately. Such a migration has a WEAKER guarantee (if the DDL commits but
//! the ledger insert then fails, the migration is applied-but-unrecorded and a
//! re-run re-attempts it), documented and opt-in — the atomic default is never
//! silently weakened.
//!
//! A migration file must NOT contain its own transaction control
//! (`BEGIN`/`COMMIT`/`ROLLBACK`) — the runner owns the transaction boundary.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bsql_postgres_proto::engine::Transport;

use crate::driver::Core;
use crate::error::{ColumnError, DriverError};

/// The migration ledger table name. A FIXED compile-time identifier (never
/// user data), spliced only into the constant DDL below — so there is no
/// identifier-injection surface at all (strictly stronger than a runtime
/// `SafeTable` check on a caller-supplied name). Unqualified, so it lands in the
/// first schema of the connection's `search_path` — which respects a
/// connect-time schema isolation (e.g. `#[bsql::test]`).
pub const LEDGER_TABLE: &str = "_bsql_migrations";

/// `CREATE TABLE IF NOT EXISTS` for the ledger. Idempotent and
/// concurrency-safe under the advisory lock. `applied_at` defaults to the
/// transaction timestamp; `ordinal` records the apply position for a stable
/// read order.
const CREATE_LEDGER: &str = "CREATE TABLE IF NOT EXISTS _bsql_migrations (\
    ordinal integer NOT NULL, \
    name text NOT NULL, \
    checksum text NOT NULL, \
    applied_at timestamptz NOT NULL DEFAULT now(), \
    CONSTRAINT _bsql_migrations_pkey PRIMARY KEY (name))";

/// Read the ledger in apply order.
const READ_LEDGER: &str =
    "SELECT name, checksum, applied_at::text FROM _bsql_migrations ORDER BY ordinal";

/// Record one applied migration (`applied_at` defaults). Parameters — never
/// spliced — carry the migration name / checksum, so a hostile filename cannot
/// inject.
const INSERT_LEDGER: &str =
    "INSERT INTO _bsql_migrations (ordinal, name, checksum) VALUES ($1, $2, $3)";

/// SQLSTATE `undefined_table` — the ledger does not exist yet (a fresh
/// database). A read-only [`Core::migration_status`] / [`Core::dry_run_migrations`]
/// maps it to "nothing applied yet" rather than erroring.
const UNDEFINED_TABLE: &str = "42P01";

/// The comment directive marking a migration that must run OUTSIDE a
/// transaction (see the module docs).
const NO_TRANSACTION_MARKER: &str = "bsql:no-transaction";

/// Initial backoff between migration-lock acquisition polls.
pub const LOCK_POLL_INITIAL: Duration = Duration::from_millis(10);

/// Maximum backoff interval between migration-lock acquisition polls.
pub const LOCK_POLL_MAX: Duration = Duration::from_millis(1000);

/// Total budget for acquiring the migration lock before giving up with
/// [`MigrationError::LockTimeout`]. Generous — another instance's long
/// migration (a big `CREATE INDEX CONCURRENTLY`) can hold the lock a while — but
/// bounded so a stuck holder does not hang the boot forever.
pub const LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);

/// The next poll backoff: double, capped at [`LOCK_POLL_MAX`]. The single
/// backoff authority both drivers' acquire loops share, so the policy cannot
/// drift between them.
#[must_use]
pub fn next_backoff(current: Duration) -> Duration {
    match current.checked_mul(2) {
        Some(doubled) => doubled.min(LOCK_POLL_MAX),
        None => LOCK_POLL_MAX,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure, backend-agnostic helpers (checksum / directive scan / drift diff).
// The SQLite runner carries a self-contained twin of these (it shares no
// runtime crate with this one); the offline unit tests below and in the SQLite
// crate pin their behaviour to the same vectors.
// ─────────────────────────────────────────────────────────────────────────────

/// The content checksum of a migration's SQL — dependency-free FNV-1a-64.
///
/// Deterministic and toolchain-stable (explicit constants, unlike
/// `DefaultHasher`, whose output may change across releases), so a value stored
/// in the ledger stays comparable forever. Adequate for detecting an ACCIDENTAL
/// edit of an applied migration — the drift footgun this guards against — for
/// which cryptographic strength is unnecessary (an attacker who can rewrite a
/// migration file can rewrite the ledger too; the checksum is a footgun
/// detector, not a security boundary).
#[must_use]
pub fn migration_checksum(sql: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    sql.as_bytes()
        .iter()
        .fold(OFFSET, |hash, &byte| (hash ^ u64::from(byte)).wrapping_mul(PRIME))
}

/// The checksum rendered as a fixed 16-char lowercase hex string (the ledger's
/// stored form). Comparing these strings IS the drift check.
#[must_use]
pub fn checksum_hex(sql: &str) -> String {
    format!("{:016x}", migration_checksum(sql))
}

/// The `pg_advisory_lock` key for a ledger name: the name's checksum
/// reinterpreted as `i64` (the same numeric value on every platform — a bit
/// reinterpretation, not a lossy cast). A fixed name yields a fixed key, so all
/// runners against one database serialize on the same lock.
#[must_use]
pub fn advisory_lock_key(ledger_name: &str) -> i64 {
    i64::from_ne_bytes(migration_checksum(ledger_name).to_ne_bytes())
}

/// Whether a migration opts out of the wrapping transaction via a
/// `-- bsql:no-transaction` comment line. A dependency-free line scan: a line
/// whose trimmed content, after an optional leading `--`, begins with the
/// marker token. (A false positive would only run a normally-transactional
/// migration non-transactionally; the directive is written deliberately, so the
/// risk is negligible — and documented.)
#[must_use]
pub fn is_non_transactional(sql: &str) -> bool {
    sql.lines().any(|line| {
        let trimmed = line.trim_start();
        let content = match trimmed.strip_prefix("--") {
            Some(after) => after.trim_start(),
            None => trimmed,
        };
        content.starts_with(NO_TRANSACTION_MARKER)
    })
}

/// One migration loaded from its source: its stable NAME and full SQL.
#[derive(Debug, Clone)]
struct LoadedMigration {
    name: String,
    sql: String,
}

/// One ledger row (an already-applied migration).
#[derive(Debug, Clone)]
pub struct AppliedMigration {
    /// The migration's stable name (its ledger primary key).
    pub name: String,
    /// The content checksum recorded when it was applied (16-char hex).
    pub checksum: String,
    /// The apply timestamp, as the database rendered it to text.
    pub applied_at: String,
}

/// Verify the already-applied migrations against the current source and return
/// the count of migrations already applied (so the pending set is
/// `source[count..]`).
///
/// The applied list (in apply order) must be a PREFIX of the source (in name
/// order): same names at the same positions, matching checksums. Any divergence
/// is a classified [`MigrationError::Drift`].
fn plan(
    applied: &[AppliedMigration],
    source: &[LoadedMigration],
) -> Result<usize, MigrationError> {
    // Every applied migration must still exist in the source (by name); an
    // absence is a deletion of an applied migration.
    for a in applied {
        if !source.iter().any(|s| s.name == a.name) {
            return Err(MigrationError::Drift {
                migration: a.name.clone(),
                kind: DriftKind::MissingFromSource,
            });
        }
    }
    // The applied prefix must line up name-for-name and checksum-for-checksum
    // with the source. (Each applied name is in the source and names are unique,
    // so `applied.len() <= source.len()` and `source.get(i)` is always `Some`;
    // the `None` arm stays total.)
    for (i, a) in applied.iter().enumerate() {
        match source.get(i) {
            None => {
                return Err(MigrationError::Drift {
                    migration: a.name.clone(),
                    kind: DriftKind::MissingFromSource,
                });
            }
            Some(s) => {
                if s.name != a.name {
                    return Err(MigrationError::Drift {
                        migration: a.name.clone(),
                        kind: DriftKind::Reordered {
                            applied_ordinal: i,
                            source_name_at_ordinal: s.name.clone(),
                        },
                    });
                }
                let current = checksum_hex(&s.sql);
                if current != a.checksum {
                    return Err(MigrationError::Drift {
                        migration: a.name.clone(),
                        kind: DriftKind::ChecksumMismatch {
                            recorded: a.checksum.clone(),
                            current,
                        },
                    });
                }
            }
        }
    }
    Ok(applied.len())
}

// ─────────────────────────────────────────────────────────────────────────────
// Source loading.
// ─────────────────────────────────────────────────────────────────────────────

/// Where the migration set comes from.
///
/// Build one with [`MigrationSource::embedded`] (the baked
/// `bsql::embed_migrations!()` set — no filesystem at run time) or
/// [`MigrationSource::directory`] (walked at run time — the ops-friendly case).
/// Both feed the SAME runner, in the SAME lexicographic-by-name order.
#[derive(Debug, Clone, Copy)]
pub enum MigrationSource<'a> {
    /// A baked `&[(name, sql)]` set (from `bsql::embed_migrations!()`).
    Embedded(&'a [(&'a str, &'a str)]),
    /// A directory walked at run time (recursing into subdirectories, collecting
    /// `*.sql`). Unlike the embedded path, the directory source parses nothing —
    /// it applies the files as-is (the build-time acknowledgement gate ran on
    /// the EMBEDDED path; see the crate docs).
    Directory(&'a Path),
}

impl<'a> MigrationSource<'a> {
    /// The embedded set baked by `bsql::embed_migrations!()`.
    #[must_use]
    pub fn embedded(migrations: &'a [(&'a str, &'a str)]) -> Self {
        Self::Embedded(migrations)
    }

    /// A migrations directory walked at run time.
    #[must_use]
    pub fn directory<P: AsRef<Path> + ?Sized>(path: &'a P) -> Self {
        Self::Directory(path.as_ref())
    }

    /// Load the set into owned `(name, sql)` pairs, sorted by name (the
    /// deterministic replay order shared with the build-time catalog).
    fn load(self) -> Result<Vec<LoadedMigration>, MigrationSourceError> {
        let mut loaded = match self {
            MigrationSource::Embedded(migrations) => migrations
                .iter()
                .map(|&(name, sql)| LoadedMigration {
                    name: name.to_owned(),
                    sql: sql.to_owned(),
                })
                .collect::<Vec<_>>(),
            MigrationSource::Directory(dir) => walk_directory(dir)?,
        };
        loaded.sort_by(|a, b| a.name.cmp(&b.name));
        // Reject a duplicate name BEFORE any apply — a hand-built embedded slice
        // could carry two migrations of the same name (a directory walk yields
        // unique paths). Without this, PostgreSQL fails loud on the ledger PK but
        // SQLite would silently skip the second (its in-transaction re-check sees
        // the name recorded), a parity break + a silent skip. Loud on BOTH now.
        let duplicate = loaded.windows(2).find_map(|pair| match pair {
            [a, b] if a.name == b.name => Some(a.name.clone()),
            _ => None,
        });
        if let Some(name) = duplicate {
            return Err(MigrationSourceError::DuplicateName { name });
        }
        Ok(loaded)
    }
}

impl<'a> From<&'a [(&'a str, &'a str)]> for MigrationSource<'a> {
    fn from(migrations: &'a [(&'a str, &'a str)]) -> Self {
        Self::Embedded(migrations)
    }
}

impl<'a> From<&'a Path> for MigrationSource<'a> {
    fn from(dir: &'a Path) -> Self {
        Self::Directory(dir)
    }
}

/// Recursively collect `*.sql` files under `dir` as `(relative-name, sql)`,
/// with `/`-normalized names so the runtime name matches the build-baked name
/// for the same file on every platform.
fn walk_directory(dir: &Path) -> Result<Vec<LoadedMigration>, MigrationSourceError> {
    let mut out = Vec::new();
    descend(dir, dir, &mut out)?;
    Ok(out)
}

fn descend(
    root: &Path,
    dir: &Path,
    out: &mut Vec<LoadedMigration>,
) -> Result<(), MigrationSourceError> {
    let entries = std::fs::read_dir(dir).map_err(|e| MigrationSourceError::Io {
        path: dir.to_path_buf(),
        message: e.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| MigrationSourceError::Io {
            path: dir.to_path_buf(),
            message: e.to_string(),
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| MigrationSourceError::Io {
            path: path.clone(),
            message: e.to_string(),
        })?;
        if file_type.is_dir() {
            descend(root, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "sql") {
            let name = relative_name(root, &path)?;
            let sql = std::fs::read_to_string(&path).map_err(|e| MigrationSourceError::Io {
                path: path.clone(),
                message: e.to_string(),
            })?;
            out.push(LoadedMigration { name, sql });
        }
    }
    Ok(())
}

/// The `/`-normalized name of a file relative to the migrations root.
fn relative_name(root: &Path, file: &Path) -> Result<String, MigrationSourceError> {
    let rel = match file.strip_prefix(root) {
        Ok(r) => r,
        Err(_) => file,
    };
    let text = rel.to_str().ok_or_else(|| MigrationSourceError::NonUtf8Path {
        path: file.to_path_buf(),
    })?;
    Ok(text.replace(std::path::MAIN_SEPARATOR, "/"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Result / status / error types.
// ─────────────────────────────────────────────────────────────────────────────

/// The outcome of a migration run (a driver's `run_migrations`).
#[derive(Debug, Clone)]
pub struct MigrationReport {
    /// The names of the migrations applied by THIS run, in order (empty when the
    /// database was already up to date).
    pub applied: Vec<String>,
    /// How many migrations were already applied before this run.
    pub already_applied: usize,
}

impl MigrationReport {
    /// Whether this run applied at least one migration.
    #[must_use]
    pub fn applied_any(&self) -> bool {
        !self.applied.is_empty()
    }
}

/// A read-only snapshot from [`Core::migration_status`].
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// The already-applied migrations, in apply order (from the ledger).
    pub applied: Vec<AppliedMigration>,
    /// The names of migrations present in the source but not yet applied, in
    /// order.
    pub pending: Vec<String>,
}

/// Why a migration source could not be loaded.
#[derive(Debug)]
#[non_exhaustive]
pub enum MigrationSourceError {
    /// A runtime directory could not be read.
    Io {
        /// The directory or file that failed.
        path: PathBuf,
        /// The underlying I/O error, rendered.
        message: String,
    },
    /// A migration file's path is not valid UTF-8, so no stable name can be
    /// formed for the ledger.
    NonUtf8Path {
        /// The offending path.
        path: PathBuf,
    },
    /// Two migrations in the source share a name (only reachable from a
    /// hand-built embedded slice — a directory walk yields unique paths). A
    /// loud pre-flight error on BOTH backends, before any apply, rather than a
    /// silent skip of the second.
    DuplicateName {
        /// The name that appears more than once.
        name: String,
    },
}

impl fmt::Display for MigrationSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationSourceError::Io { path, message } => {
                write!(f, "cannot read migrations at {}: {message}", path.display())
            }
            MigrationSourceError::NonUtf8Path { path } => {
                write!(f, "migration path is not valid UTF-8: {}", path.display())
            }
            MigrationSourceError::DuplicateName { name } => {
                write!(f, "duplicate migration name `{name}` in the source")
            }
        }
    }
}

impl std::error::Error for MigrationSourceError {}

/// How an already-applied migration diverged from the current source.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DriftKind {
    /// The migration file's content changed after it was applied (its checksum
    /// no longer matches the ledger).
    ChecksumMismatch {
        /// The checksum the ledger recorded at apply time.
        recorded: String,
        /// The checksum of the current file.
        current: String,
    },
    /// The migration is no longer at the ordinal it was applied at — a migration
    /// was inserted before it, or the set was reordered. The set must be
    /// append-only.
    Reordered {
        /// The ordinal (0-based apply position) of the applied migration.
        applied_ordinal: usize,
        /// The name the current source places at that ordinal instead.
        source_name_at_ordinal: String,
    },
    /// An already-applied migration is absent from the current source — it was
    /// deleted after being applied.
    MissingFromSource,
}

impl fmt::Display for DriftKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DriftKind::ChecksumMismatch { recorded, current } => write!(
                f,
                "content changed after it was applied (recorded checksum {recorded}, \
                 current {current}) — an applied migration must not be edited; add a \
                 NEW migration instead"
            ),
            DriftKind::Reordered {
                applied_ordinal,
                source_name_at_ordinal,
            } => write!(
                f,
                "was applied at position {applied_ordinal} but the source now has \
                 `{source_name_at_ordinal}` there — a migration was inserted before or \
                 reordered around an applied one; the set must be append-only"
            ),
            DriftKind::MissingFromSource => write!(
                f,
                "is recorded as applied but is absent from the source — it was deleted \
                 after being applied; restore it (an applied migration must not be removed)"
            ),
        }
    }
}

/// Why a migration run failed. `#[non_exhaustive]`: a future classification is
/// an additive change.
#[derive(Debug)]
#[non_exhaustive]
pub enum MigrationError {
    /// The migration source could not be loaded (a runtime directory).
    Source(MigrationSourceError),
    /// An already-applied migration drifted from what the ledger recorded. Names
    /// the migration and how it diverged. The runner applies nothing.
    Drift {
        /// The migration that drifted.
        migration: String,
        /// How it diverged.
        kind: DriftKind,
    },
    /// A migration failed to apply. Its transaction rolled back and the runner
    /// STOPPED (later migrations did not run). Names the failed migration and
    /// carries the classified cause.
    MigrationFailed {
        /// The migration that failed.
        migration: String,
        /// The classified driver/server cause.
        source: Box<DriverError>,
    },
    /// The migration advisory lock could not be acquired within
    /// [`LOCK_ACQUIRE_TIMEOUT`] — another runner has held it that long (it may
    /// be stuck). Distinct from a driver error so a caller can retry / alert.
    LockTimeout,
    /// A ledger / lock / connection operation failed (not attributable to one
    /// migration's DDL).
    Driver(Box<DriverError>),
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationError::Source(e) => write!(f, "migration source error: {e}"),
            MigrationError::Drift { migration, kind } => {
                write!(f, "migration `{migration}` {kind}")
            }
            MigrationError::MigrationFailed { migration, source } => {
                write!(f, "migration `{migration}` failed to apply: {source}")
            }
            MigrationError::LockTimeout => write!(
                f,
                "could not acquire the migration advisory lock within {LOCK_ACQUIRE_TIMEOUT:?} — \
                 another runner has held it that long and may be stuck"
            ),
            MigrationError::Driver(e) => write!(f, "migration runner driver error: {e}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<MigrationSourceError> for MigrationError {
    fn from(e: MigrationSourceError) -> Self {
        MigrationError::Source(e)
    }
}

impl From<DriverError> for MigrationError {
    fn from(e: DriverError) -> Self {
        MigrationError::Driver(Box::new(e))
    }
}

/// Map a dynamic-column read failure (reading the ledger) to a driver error.
fn column_err(e: ColumnError) -> MigrationError {
    MigrationError::Driver(Box::new(DriverError::Column(e)))
}

// ─────────────────────────────────────────────────────────────────────────────
// The runner, defined ONCE over the transport-generic `Core<S>` — so both the
// async and the sync PostgreSQL drivers get it with byte-identical logic.
// ─────────────────────────────────────────────────────────────────────────────

impl<S: Transport<Error = std::io::Error>> Core<S> {
    /// Try (non-blocking) to acquire the migration advisory lock. `true` if
    /// acquired, `false` if another session holds it.
    ///
    /// NON-blocking on purpose: a session BLOCKED in `pg_advisory_lock` holds an
    /// open (implicit) transaction whose virtual-xid a `CREATE INDEX
    /// CONCURRENTLY` running under the lock would wait on — a genuine deadlock
    /// class. Polling `pg_try_advisory_lock` with a client-side backoff (see the
    /// drivers' `run_migrations`) keeps a waiter holding NO long-lived
    /// transaction, so the concurrent-index build never waits on it.
    ///
    /// `#[doc(hidden)]`: the driver-composition seam for `run_migrations`; a
    /// consumer uses the driver's `run_migrations` verb.
    #[doc(hidden)]
    pub async fn try_acquire_migration_lock(&mut self) -> Result<bool, DriverError> {
        let key = advisory_lock_key(LEDGER_TABLE);
        let result = self
            .query_sql(&format!("SELECT pg_try_advisory_lock({key})"))
            .await?;
        // `pg_try_advisory_lock` returns exactly one bool row; the `None` arm is
        // structurally unreachable but handled totally (never a panic).
        let row = result.get(0).ok_or(DriverError::NoRows)?;
        match row.get_bool(0).map_err(DriverError::Column)? {
            Some(got) => Ok(got),
            None => Err(DriverError::Config("pg_try_advisory_lock returned NULL")),
        }
    }

    /// Release the migration advisory lock. `#[doc(hidden)]`: the
    /// driver-composition seam.
    #[doc(hidden)]
    pub async fn release_migration_lock(&mut self) -> Result<(), DriverError> {
        let key = advisory_lock_key(LEDGER_TABLE);
        self.simple_query(&format!("SELECT pg_advisory_unlock({key})")).await?;
        Ok(())
    }

    /// Apply every pending migration from `source`, ASSUMING the caller holds
    /// the migration advisory lock (the drivers' `run_migrations` acquires it
    /// first).
    ///
    /// Ensures the ledger exists, verifies the already-applied migrations
    /// against the source (a drift is a classified error — nothing is applied),
    /// then applies each pending migration in its own transaction and records
    /// it. A migration that fails rolls back and STOPS the run. Returns a
    /// [`MigrationReport`]; a fully up-to-date database is a successful no-op.
    ///
    /// `#[doc(hidden)]`: the driver-composition seam for `run_migrations`.
    ///
    /// # Errors
    ///
    /// [`MigrationError`] — drift, a failed migration (named), a source I/O
    /// error, or an underlying driver error.
    #[doc(hidden)]
    pub async fn apply_pending_locked(
        &mut self,
        source: MigrationSource<'_>,
    ) -> Result<MigrationReport, MigrationError> {
        let loaded = source.load()?;
        self.simple_query(CREATE_LEDGER).await?;
        let applied = self.read_ledger(false).await?;
        let already_applied = plan(&applied, &loaded)?;

        let mut newly_applied = Vec::new();
        for (index, migration) in loaded.iter().enumerate() {
            if index < already_applied {
                continue;
            }
            let ordinal = i32::try_from(index).map_err(|_| {
                MigrationError::Driver(Box::new(DriverError::Config(
                    "too many migrations to record (ordinal exceeds i32)",
                )))
            })?;
            self.apply_one(migration, ordinal).await?;
            newly_applied.push(migration.name.clone());
        }

        Ok(MigrationReport {
            applied: newly_applied,
            already_applied,
        })
    }

    /// Apply one migration and record it. Transactional by default (DDL + ledger
    /// row are one transaction); a `-- bsql:no-transaction` migration runs
    /// outside a transaction and records separately.
    async fn apply_one(
        &mut self,
        migration: &LoadedMigration,
        ordinal: i32,
    ) -> Result<(), MigrationError> {
        let checksum = checksum_hex(&migration.sql);

        if is_non_transactional(&migration.sql) {
            // Outside a transaction: apply, then record (each auto-commits).
            self.simple_query(&migration.sql).await.map_err(|e| {
                MigrationError::MigrationFailed {
                    migration: migration.name.clone(),
                    source: Box::new(e),
                }
            })?;
            self.execute_params(
                INSERT_LEDGER,
                &(ordinal, migration.name.as_str(), checksum.as_str()),
            )
            .await
            .map_err(|e| MigrationError::MigrationFailed {
                migration: migration.name.clone(),
                source: Box::new(e),
            })?;
            return Ok(());
        }

        self.begin().await?;
        let outcome = self.apply_and_record(migration, ordinal, &checksum).await;
        match outcome {
            Ok(()) => {
                self.commit().await?;
                Ok(())
            }
            Err(cause) => {
                // Best-effort rollback so the connection returns clean. If it
                // also fails the connection is dead (I/O) — the apply cause is
                // the meaningful signal (the pool discards the unhealthy
                // connection via `is_healthy`), so surface it either way.
                match self.rollback().await {
                    Ok(()) | Err(_) => {}
                }
                Err(MigrationError::MigrationFailed {
                    migration: migration.name.clone(),
                    source: Box::new(cause),
                })
            }
        }
    }

    /// Run a migration's SQL and record its ledger row, inside the caller's open
    /// transaction. Any error is returned so the caller rolls back.
    async fn apply_and_record(
        &mut self,
        migration: &LoadedMigration,
        ordinal: i32,
        checksum: &str,
    ) -> Result<(), DriverError> {
        self.simple_query(&migration.sql).await?;
        self.execute_params(
            INSERT_LEDGER,
            &(ordinal, migration.name.as_str(), checksum),
        )
        .await?;
        Ok(())
    }

    /// Read the ledger in apply order. With `allow_missing`, a not-yet-created
    /// ledger (SQLSTATE `42P01`) reads as empty rather than erroring — for the
    /// read-only status / dry-run verbs against a fresh database.
    async fn read_ledger(
        &mut self,
        allow_missing: bool,
    ) -> Result<Vec<AppliedMigration>, MigrationError> {
        let result = match self.query_sql(READ_LEDGER).await {
            Ok(r) => r,
            Err(DriverError::Db(db)) if allow_missing && db.is_code(UNDEFINED_TABLE) => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(MigrationError::Driver(Box::new(e))),
        };

        let mut applied = Vec::with_capacity(result.len());
        for row in result.iter() {
            let name = row
                .get_str(0)
                .map_err(column_err)?
                .ok_or_else(null_ledger_cell)?
                .to_owned();
            let checksum = row
                .get_str(1)
                .map_err(column_err)?
                .ok_or_else(null_ledger_cell)?
                .to_owned();
            let applied_at = row
                .get_str(2)
                .map_err(column_err)?
                .ok_or_else(null_ledger_cell)?
                .to_owned();
            applied.push(AppliedMigration {
                name,
                checksum,
                applied_at,
            });
        }
        Ok(applied)
    }

    /// A read-only snapshot: the already-applied migrations (from the ledger)
    /// and the pending ones (in the source but not the ledger), in order.
    ///
    /// Acquires no lock and creates nothing (a fresh database reads as
    /// all-pending). It does NOT verify checksums — that is
    /// [`dry_run_migrations`](Self::dry_run_migrations)' / a driver's
    /// `run_migrations`' job; status is a plain snapshot.
    ///
    /// # Errors
    ///
    /// [`MigrationError`] — a source I/O error or an underlying driver error.
    pub async fn migration_status(
        &mut self,
        source: MigrationSource<'_>,
    ) -> Result<MigrationStatus, MigrationError> {
        let loaded = source.load()?;
        let applied = self.read_ledger(true).await?;
        let pending = loaded
            .iter()
            .filter(|m| !applied.iter().any(|a| a.name == m.name))
            .map(|m| m.name.clone())
            .collect();
        Ok(MigrationStatus { applied, pending })
    }

    /// Report which migrations WOULD be applied by a driver's `run_migrations`,
    /// without applying anything and without acquiring the lock.
    ///
    /// Runs the SAME drift verification as `run_migrations` (a drift is the same
    /// classified error), so a dry run surfaces a checksum-drift / reorder /
    /// deletion before a real run touches the database. Returns the pending
    /// migration names, in order.
    ///
    /// # Errors
    ///
    /// [`MigrationError`] — drift, a source I/O error, or an underlying driver
    /// error.
    pub async fn dry_run_migrations(
        &mut self,
        source: MigrationSource<'_>,
    ) -> Result<Vec<String>, MigrationError> {
        let loaded = source.load()?;
        let applied = self.read_ledger(true).await?;
        let already_applied = plan(&applied, &loaded)?;
        let pending = loaded
            .iter()
            .enumerate()
            .filter(|(i, _)| *i >= already_applied)
            .map(|(_, m)| m.name.clone())
            .collect();
        Ok(pending)
    }
}

/// A `NULL` in a `NOT NULL` ledger column — a tampered/corrupt ledger. Total
/// handling of the getter's `Option` (never a panic, never a silent default).
fn null_ledger_cell() -> MigrationError {
    MigrationError::Driver(Box::new(DriverError::Config(
        "corrupt migration ledger: NULL in a NOT NULL column",
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loaded(name: &str, sql: &str) -> LoadedMigration {
        LoadedMigration {
            name: name.to_owned(),
            sql: sql.to_owned(),
        }
    }

    fn applied(name: &str, sql: &str) -> AppliedMigration {
        AppliedMigration {
            name: name.to_owned(),
            checksum: checksum_hex(sql),
            applied_at: "2026-01-01 00:00:00+00".to_owned(),
        }
    }

    #[test]
    fn checksum_is_deterministic_and_content_sensitive() {
        assert_eq!(migration_checksum("CREATE TABLE t (a int)"), migration_checksum("CREATE TABLE t (a int)"));
        assert_ne!(migration_checksum("CREATE TABLE t (a int)"), migration_checksum("CREATE TABLE t (a INT)"));
        assert_ne!(migration_checksum(""), migration_checksum(" "));
    }

    #[test]
    fn checksum_hex_is_16_lowercase_hex() {
        let hex = checksum_hex("anything");
        assert_eq!(hex.len(), 16);
        assert!(hex.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    }

    #[test]
    fn known_answer_vector_pins_the_algorithm() {
        // FNV-1a-64 of the ASCII bytes of "bsql". Pins THIS implementation to a
        // fixed value; the SQLite twin pins the SAME vector, so the two cannot
        // silently diverge.
        assert_eq!(migration_checksum("bsql"), 0x3587_bc9c_01e6_f51f);
    }

    #[test]
    fn non_transactional_marker_detected() {
        assert!(is_non_transactional("-- bsql:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (a)"));
        assert!(is_non_transactional("  --   bsql:no-transaction  \nVACUUM"));
        assert!(!is_non_transactional("CREATE TABLE t (a int) -- not a directive"));
        assert!(!is_non_transactional("-- bsql:ack-destructive\nDROP TABLE t"));
    }

    #[test]
    fn plan_clean_prefix_returns_applied_count() {
        let source = vec![loaded("0001", "a"), loaded("0002", "b"), loaded("0003", "c")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        assert_eq!(plan(&done, &source).unwrap(), 2);
    }

    #[test]
    fn plan_empty_ledger_is_all_pending() {
        let source = vec![loaded("0001", "a"), loaded("0002", "b")];
        assert_eq!(plan(&[], &source).unwrap(), 0);
    }

    #[test]
    fn plan_checksum_drift_is_classified() {
        let source = vec![loaded("0001", "EDITED")];
        let done = vec![applied("0001", "ORIGINAL")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            MigrationError::Drift { kind: DriftKind::ChecksumMismatch { .. }, .. }
        ));
    }

    #[test]
    fn plan_insert_before_applied_is_reorder() {
        // 0002 was applied; source now inserts 0001 before it.
        let source = vec![loaded("0001", "a"), loaded("0002", "b")];
        let done = vec![applied("0002", "b")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            MigrationError::Drift { kind: DriftKind::Reordered { .. }, .. }
        ));
    }

    #[test]
    fn plan_deleted_applied_migration_is_classified() {
        let source = vec![loaded("0002", "b")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            MigrationError::Drift { migration, kind: DriftKind::MissingFromSource } if migration == "0001"
        ));
    }

    #[test]
    fn embedded_source_loads_and_sorts_by_name() {
        let raw = [("0002_b.sql", "B"), ("0001_a.sql", "A")];
        let loaded = MigrationSource::embedded(&raw).load().unwrap();
        assert_eq!(loaded[0].name, "0001_a.sql");
        assert_eq!(loaded[1].name, "0002_b.sql");
    }

    #[test]
    fn duplicate_name_in_a_slice_is_a_loud_source_error() {
        // A hand-built slice with two same-named migrations is rejected BEFORE
        // any apply (PG parity: PG fails loud on the ledger PK, SQLite would
        // otherwise silently skip the second).
        let raw = [("dup.sql", "SELECT 1"), ("dup.sql", "SELECT 2")];
        let err = MigrationSource::embedded(&raw)
            .load()
            .expect_err("duplicate name must be loud");
        assert!(matches!(err, MigrationSourceError::DuplicateName { name } if name == "dup.sql"));
    }
}
