//! The SQLite migration RUNNER — the cross-backend twin of the PostgreSQL
//! runner, over the SAME `MigrationSource` shape, ledger contract, checksum,
//! drift classification, and dry-run/status verbs, so `conn.run_migrations(..)`
//! reads identically on both backends.
//!
//! The pure logic (checksum, directive scan, drift diff, source loading) is a
//! SELF-CONTAINED copy of the PostgreSQL runner's — the embedded SQLite crate
//! shares no runtime crate with `bsql-postgres-core` (depending on it would drag
//! the whole PostgreSQL + rustls tree in), so, exactly like the N+1 detector, it
//! carries its own copy. The offline unit tests here pin its behaviour to the
//! SAME known-answer vector the PostgreSQL runner uses, so the two cannot
//! silently diverge.
//!
//! # SQLite specifics
//!
//! - **Concurrency.** SQLite has no cross-process session lock. Each migration
//!   applies inside a `BEGIN IMMEDIATE` transaction (which acquires the write
//!   lock up front), so a second process's `BEGIN IMMEDIATE` blocks on the
//!   connection's `busy_timeout` until the first commits. Inside the
//!   transaction the runner RE-CHECKS the ledger, so a migration a concurrent
//!   runner already applied is skipped, never double-applied.
//! - **Atomicity.** SQLite DDL is transactional, so the migration + its ledger
//!   row commit together (or roll back together).
//! - A `-- bsql:no-transaction` migration (for `VACUUM` / a `PRAGMA
//!   foreign_keys` toggle) applies outside a transaction with the documented
//!   weaker guarantee.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::connection::Connection;
use crate::error::SqliteError;

/// The migration ledger table name — a fixed compile-time identifier (never
/// user data), spliced only into the constant DDL below.
pub const LEDGER_TABLE: &str = "_bsql_migrations";

const CREATE_LEDGER: &str = "CREATE TABLE IF NOT EXISTS _bsql_migrations (\
    ordinal INTEGER NOT NULL, \
    name TEXT NOT NULL PRIMARY KEY, \
    checksum TEXT NOT NULL, \
    applied_at TEXT NOT NULL DEFAULT (datetime('now')))";

const READ_LEDGER: &str =
    "SELECT name, checksum, applied_at FROM _bsql_migrations ORDER BY ordinal";

const INSERT_LEDGER: &str =
    "INSERT INTO _bsql_migrations (ordinal, name, checksum) VALUES (?, ?, ?)";

const LEDGER_EXISTS: &str =
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_bsql_migrations'";

const IS_APPLIED: &str = "SELECT 1 FROM _bsql_migrations WHERE name = ?";

/// The comment directive marking a migration that must run OUTSIDE a
/// transaction.
const NO_TRANSACTION_MARKER: &str = "bsql:no-transaction";

// ─────────────────────────────────────────────────────────────────────────────
// Pure, backend-agnostic helpers — a self-contained twin of the PostgreSQL
// runner's, pinned to the SAME behaviour by the tests below.
// ─────────────────────────────────────────────────────────────────────────────

/// The content checksum of a migration's SQL — dependency-free FNV-1a-64
/// (deterministic + toolchain-stable, so a ledger value stays comparable
/// forever). See the PostgreSQL runner for the full rationale.
#[must_use]
pub fn migration_checksum(sql: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    sql.as_bytes()
        .iter()
        .fold(OFFSET, |hash, &byte| (hash ^ u64::from(byte)).wrapping_mul(PRIME))
}

/// The checksum rendered as a fixed 16-char lowercase hex string.
#[must_use]
pub fn checksum_hex(sql: &str) -> String {
    format!("{:016x}", migration_checksum(sql))
}

/// Whether a migration opts out of the wrapping transaction via a
/// `-- bsql:no-transaction` comment line.
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

/// One migration loaded from its source.
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
    /// The apply timestamp, as SQLite rendered it to text.
    pub applied_at: String,
}

/// Verify the already-applied migrations against the current source and return
/// the count already applied (so the pending set is `source[count..]`). Any
/// divergence is a classified [`MigrationError::Drift`].
fn plan(
    applied: &[AppliedMigration],
    source: &[LoadedMigration],
) -> Result<usize, MigrationError> {
    for a in applied {
        if !source.iter().any(|s| s.name == a.name) {
            return Err(MigrationError::Drift {
                migration: a.name.clone(),
                kind: DriftKind::MissingFromSource,
            });
        }
    }
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
// Source loading (identical to the PostgreSQL runner's).
// ─────────────────────────────────────────────────────────────────────────────

/// Where the migration set comes from — the embedded `bsql::embed_migrations!()`
/// set, or a directory walked at run time.
#[derive(Debug, Clone, Copy)]
pub enum MigrationSource<'a> {
    /// A baked `&[(name, sql)]` set (from `bsql::embed_migrations!()`).
    Embedded(&'a [(&'a str, &'a str)]),
    /// A directory walked at run time (recursing into subdirectories, collecting
    /// `*.sql`).
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
        // Reject a duplicate name BEFORE any apply — the in-transaction ledger
        // re-check would otherwise SILENTLY skip the second same-named migration
        // (the PostgreSQL runner fails loud on the ledger PK). Loud on BOTH now.
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

/// The outcome of a [`Connection::run_migrations`] run.
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

/// A read-only snapshot from [`Connection::migration_status`].
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// The already-applied migrations, in apply order (from the ledger).
    pub applied: Vec<AppliedMigration>,
    /// The names of migrations present in the source but not yet applied.
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
    /// hand-built embedded slice). A loud pre-flight error on BOTH backends,
    /// before any apply, rather than a silent skip of the second.
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
    /// The migration file's content changed after it was applied.
    ChecksumMismatch {
        /// The checksum the ledger recorded at apply time.
        recorded: String,
        /// The checksum of the current file.
        current: String,
    },
    /// The migration is no longer at the ordinal it was applied at (a migration
    /// was inserted before it, or the set was reordered).
    Reordered {
        /// The ordinal (0-based apply position) of the applied migration.
        applied_ordinal: usize,
        /// The name the current source places at that ordinal instead.
        source_name_at_ordinal: String,
    },
    /// An already-applied migration is absent from the current source.
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
                 after being applied; restore it"
            ),
        }
    }
}

/// Why a migration run failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum MigrationError {
    /// The migration source could not be loaded (a runtime directory).
    Source(MigrationSourceError),
    /// An already-applied migration drifted from what the ledger recorded. The
    /// runner applies nothing.
    Drift {
        /// The migration that drifted.
        migration: String,
        /// How it diverged.
        kind: DriftKind,
    },
    /// A migration failed to apply. Its transaction rolled back and the runner
    /// STOPPED. Names the failed migration; carries the classified cause.
    MigrationFailed {
        /// The migration that failed.
        migration: String,
        /// The classified SQLite cause.
        source: Box<SqliteError>,
    },
    /// A ledger / transaction / connection operation failed.
    Backend(Box<SqliteError>),
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
            MigrationError::Backend(e) => write!(f, "migration runner backend error: {e}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<MigrationSourceError> for MigrationError {
    fn from(e: MigrationSourceError) -> Self {
        MigrationError::Source(e)
    }
}

impl From<SqliteError> for MigrationError {
    fn from(e: SqliteError) -> Self {
        MigrationError::Backend(Box::new(e))
    }
}

/// Whether an in-transaction migration was applied or skipped (a concurrent
/// runner had already applied it).
enum ApplyOutcome {
    Applied,
    Skipped,
}

// ─────────────────────────────────────────────────────────────────────────────
// The runner.
// ─────────────────────────────────────────────────────────────────────────────

impl Connection {
    /// Apply every pending migration from `source` to the database, exactly
    /// once, in deterministic order — the runtime migration RUNNER, the
    /// cross-backend twin of the PostgreSQL driver's.
    ///
    /// Ensures the ledger exists, verifies the already-applied migrations
    /// against the source (a drift is a classified error — nothing is applied),
    /// then applies each pending migration inside a `BEGIN IMMEDIATE`
    /// transaction (re-checking the ledger, so a concurrent runner's migration
    /// is skipped, never double-applied) and records it. A migration that fails
    /// rolls back and STOPS the run. A fully up-to-date database is a successful
    /// no-op.
    ///
    /// # Errors
    ///
    /// [`MigrationError`] — drift, a failed migration (named), a source I/O
    /// error, or an underlying SQLite error.
    pub fn run_migrations<'a>(
        &self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<MigrationReport, MigrationError> {
        let loaded = source.into().load()?;
        self.inner.execute_batch(CREATE_LEDGER).map_err(SqliteError::from)?;
        let applied = self.read_ledger()?;
        let already_applied = plan(&applied, &loaded)?;

        let mut newly_applied = Vec::new();
        for (index, migration) in loaded.iter().enumerate() {
            if index < already_applied {
                continue;
            }
            let ordinal = i64::try_from(index).map_err(|_| {
                MigrationError::Backend(Box::new(SqliteError::Query(
                    "too many migrations to record (ordinal exceeds i64)".to_owned(),
                )))
            })?;
            if self.apply_one(migration, ordinal)? {
                newly_applied.push(migration.name.clone());
            }
        }

        Ok(MigrationReport {
            applied: newly_applied,
            already_applied,
        })
    }

    /// Apply one migration and record it, returning whether it was newly applied
    /// (a concurrent runner may already have applied it — then `false`).
    fn apply_one(
        &self,
        migration: &LoadedMigration,
        ordinal: i64,
    ) -> Result<bool, MigrationError> {
        let checksum = checksum_hex(&migration.sql);

        if is_non_transactional(&migration.sql) {
            // Outside a transaction: best-effort re-check, then apply + record.
            if self.is_applied(&migration.name)? {
                return Ok(false);
            }
            self.inner.execute_batch(&migration.sql).map_err(|e| {
                MigrationError::MigrationFailed {
                    migration: migration.name.clone(),
                    source: Box::new(SqliteError::from(e)),
                }
            })?;
            self.insert_ledger(ordinal, &migration.name, &checksum).map_err(|e| {
                MigrationError::MigrationFailed {
                    migration: migration.name.clone(),
                    source: Box::new(e),
                }
            })?;
            return Ok(true);
        }

        // `BEGIN IMMEDIATE` acquires the write lock up front, so a concurrent
        // runner's `BEGIN IMMEDIATE` blocks on `busy_timeout` rather than failing
        // mid-transaction.
        self.inner.execute_batch("BEGIN IMMEDIATE").map_err(SqliteError::from)?;
        match self.apply_and_record(migration, ordinal, &checksum) {
            Ok(ApplyOutcome::Applied) => {
                self.inner.execute_batch("COMMIT").map_err(SqliteError::from)?;
                Ok(true)
            }
            Ok(ApplyOutcome::Skipped) => {
                // Nothing changed — a concurrent runner beat us. Release the lock.
                self.inner.execute_batch("ROLLBACK").map_err(SqliteError::from)?;
                Ok(false)
            }
            Err(cause) => {
                // Best-effort rollback so the connection is reusable; the apply
                // cause is the meaningful signal either way.
                match self.inner.execute_batch("ROLLBACK") {
                    Ok(()) | Err(_) => {}
                }
                Err(MigrationError::MigrationFailed {
                    migration: migration.name.clone(),
                    source: Box::new(cause),
                })
            }
        }
    }

    /// Inside the open transaction: re-check the ledger (skip if a concurrent
    /// runner already applied this migration), else apply the SQL and record it.
    fn apply_and_record(
        &self,
        migration: &LoadedMigration,
        ordinal: i64,
        checksum: &str,
    ) -> Result<ApplyOutcome, SqliteError> {
        if self.is_applied(&migration.name)? {
            return Ok(ApplyOutcome::Skipped);
        }
        self.inner.execute_batch(&migration.sql).map_err(SqliteError::from)?;
        self.insert_ledger(ordinal, &migration.name, checksum)?;
        Ok(ApplyOutcome::Applied)
    }

    /// Record one applied migration (`applied_at` defaults). Name + checksum ride
    /// bound parameters, never spliced.
    fn insert_ledger(&self, ordinal: i64, name: &str, checksum: &str) -> Result<(), SqliteError> {
        self.inner
            .execute(INSERT_LEDGER, rusqlite::params![ordinal, name, checksum])
            .map(|_| ())
            .map_err(SqliteError::from)
    }

    /// Whether the ledger already records a migration of this name.
    fn is_applied(&self, name: &str) -> Result<bool, SqliteError> {
        let mut stmt = self.inner.prepare(IS_APPLIED).map_err(SqliteError::from)?;
        stmt.exists(rusqlite::params![name]).map_err(SqliteError::from)
    }

    /// Read the ledger in apply order. A not-yet-created ledger reads as empty
    /// (for a fresh database), so status / dry-run need no table to exist.
    fn read_ledger(&self) -> Result<Vec<AppliedMigration>, MigrationError> {
        let exists = {
            let mut stmt = self.inner.prepare(LEDGER_EXISTS).map_err(SqliteError::from)?;
            stmt.exists([]).map_err(SqliteError::from)?
        };
        if !exists {
            return Ok(Vec::new());
        }

        let mut stmt = self.inner.prepare(READ_LEDGER).map_err(SqliteError::from)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AppliedMigration {
                    name: row.get(0)?,
                    checksum: row.get(1)?,
                    applied_at: row.get(2)?,
                })
            })
            .map_err(SqliteError::from)?;
        let mut applied = Vec::new();
        for row in rows {
            applied.push(row.map_err(SqliteError::from)?);
        }
        Ok(applied)
    }

    /// A read-only snapshot: applied vs pending migrations, in order. Acquires
    /// no lock and creates nothing (a fresh database reads as all-pending). Does
    /// NOT verify checksums (that is `dry_run_migrations` / `run_migrations`).
    ///
    /// # Errors
    ///
    /// [`MigrationError`] — a source I/O error or an underlying SQLite error.
    pub fn migration_status<'a>(
        &self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<MigrationStatus, MigrationError> {
        let loaded = source.into().load()?;
        let applied = self.read_ledger()?;
        let pending = loaded
            .iter()
            .filter(|m| !applied.iter().any(|a| a.name == m.name))
            .map(|m| m.name.clone())
            .collect();
        Ok(MigrationStatus { applied, pending })
    }

    /// Report which migrations WOULD be applied by [`run_migrations`](Self::run_migrations),
    /// running the SAME drift verification, without applying anything.
    ///
    /// # Errors
    ///
    /// [`MigrationError`] — drift, a source I/O error, or an underlying SQLite
    /// error.
    pub fn dry_run_migrations<'a>(
        &self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<Vec<String>, MigrationError> {
        let loaded = source.into().load()?;
        let applied = self.read_ledger()?;
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
            applied_at: "2026-01-01 00:00:00".to_owned(),
        }
    }

    #[test]
    fn known_answer_vector_matches_the_postgres_twin() {
        // The SAME FNV-1a-64 vector the PostgreSQL runner pins — the two copies
        // cannot silently diverge.
        assert_eq!(migration_checksum("bsql"), 0x3587_bc9c_01e6_f51f);
    }

    #[test]
    fn checksum_hex_is_16_lowercase_hex() {
        let hex = checksum_hex("anything");
        assert_eq!(hex.len(), 16);
        assert!(hex.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    }

    #[test]
    fn non_transactional_marker_detected() {
        assert!(is_non_transactional("-- bsql:no-transaction\nVACUUM"));
        assert!(!is_non_transactional("CREATE TABLE t (a int)"));
    }

    #[test]
    fn plan_clean_prefix_returns_applied_count() {
        let source = vec![loaded("0001", "a"), loaded("0002", "b"), loaded("0003", "c")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        assert_eq!(plan(&done, &source).unwrap(), 2);
    }

    #[test]
    fn plan_checksum_drift_is_classified() {
        let source = vec![loaded("0001", "EDITED")];
        let done = vec![applied("0001", "ORIGINAL")];
        assert!(matches!(
            plan(&done, &source).unwrap_err(),
            MigrationError::Drift { kind: DriftKind::ChecksumMismatch { .. }, .. }
        ));
    }

    #[test]
    fn plan_deleted_applied_migration_is_classified() {
        let source = vec![loaded("0002", "b")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        assert!(matches!(
            plan(&done, &source).unwrap_err(),
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
        // The SAME pre-flight duplicate rejection as the PostgreSQL runner — so
        // SQLite fails loud rather than silently skipping the second same-named
        // migration.
        let raw = [("dup.sql", "SELECT 1"), ("dup.sql", "SELECT 2")];
        let err = MigrationSource::embedded(&raw)
            .load()
            .expect_err("duplicate name must be loud");
        assert!(matches!(err, MigrationSourceError::DuplicateName { name } if name == "dup.sql"));
    }
}
