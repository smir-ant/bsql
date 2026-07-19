//! The SQLite migration RUNNER — the cross-backend twin of the PostgreSQL
//! runner, over the SAME `MigrationSource` shape, ledger contract, checksum,
//! drift classification, and dry-run/status verbs, so `conn.run_migrations(..)`
//! reads identically on both backends.
//!
//! The transport-agnostic PURE logic (checksum, directive scan, drift diff,
//! source loading) lives ONCE in the dependency-free [`bsql_common::migrate`]
//! leaf crate — the SAME compiled source the PostgreSQL runner uses, so the two
//! backends cannot silently diverge on the checksum, the apply order, or the
//! drift semantics. This module holds only the SQLite-specific I/O: the ledger
//! DDL/SQL, the `BEGIN IMMEDIATE` + in-transaction re-check apply loop, and this
//! backend's own [`MigrationError`]. It bridges to the shared classifier through
//! [`bsql_common::migrate::plan`] plus a local `From<`[`bsql_common::migrate::Drift`]`>`.
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

use crate::connection::Connection;
use crate::error::SqliteError;

// The migration PURE logic (checksum / ordering / drift authority + source
// loader + the plain data / error types) lives ONCE in the dependency-free
// `bsql-common` leaf crate. Re-exported here so the existing
// `bsql_sqlite::{AppliedMigration, ...}` paths stay stable.
pub use bsql_common::migrate::{
    AppliedMigration, DriftKind, MigrationReport, MigrationSource, MigrationSourceError,
    MigrationStatus, LEDGER_TABLE,
};
use bsql_common::migrate::{checksum_hex, is_non_transactional, plan, Drift, LoadedMigration};

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

// ─────────────────────────────────────────────────────────────────────────────
// Error type (SQLite-specific — lifts the shared `bsql_common::migrate::Drift`
// classifier into its own enum).
// ─────────────────────────────────────────────────────────────────────────────

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
    /// A migration BROKE the per-migration transaction boundary the runner owns:
    /// after applying it, the connection was not in the state the runner requires
    /// (a transactional migration left NO transaction open — a top-level
    /// `COMMIT`/`ROLLBACK`, or an in-body `COMMIT`, closed the runner's
    /// `BEGIN IMMEDIATE`; or a `-- bsql:no-transaction` migration opened a
    /// transaction of its own and left it open). Detected via
    /// `sqlite3_get_autocommit`. The runner STOPPED. The runtime peer of the
    /// PostgreSQL runner's boundary backstop and of the build-time
    /// transaction-control gate; fail-loud AFTER the boundary-breaking migration
    /// already ran (the directory source parses nothing at load time).
    TransactionBoundaryBroken {
        /// The migration that broke the boundary.
        migration: String,
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
            MigrationError::TransactionBoundaryBroken { migration } => write!(
                f,
                "migration `{migration}` broke the per-migration transaction boundary — it \
                 contains its own transaction control (a top-level or in-body \
                 COMMIT/ROLLBACK/BEGIN); the runner owns the transaction, so remove it"
            ),
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

impl From<Drift> for MigrationError {
    fn from(d: Drift) -> Self {
        MigrationError::Drift {
            migration: d.migration,
            kind: d.kind,
        }
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
            // A `-- bsql:no-transaction` migration runs as autocommit statements,
            // so the connection MUST be back at autocommit. If it is not, the
            // migration opened a transaction of its own (a top-level `BEGIN`) and
            // left it open — the ledger insert above landed inside that stray,
            // uncommitted transaction. Fail loud, rolling the stray transaction
            // back so the handle is reusable.
            if !self.inner.is_autocommit() {
                match self.inner.execute_batch("ROLLBACK") {
                    Ok(()) | Err(_) => {}
                }
                return Err(MigrationError::TransactionBoundaryBroken {
                    migration: migration.name.clone(),
                });
            }
            return Ok(true);
        }

        // `BEGIN IMMEDIATE` acquires the write lock up front, so a concurrent
        // runner's `BEGIN IMMEDIATE` blocks on `busy_timeout` rather than failing
        // mid-transaction.
        self.inner.execute_batch("BEGIN IMMEDIATE").map_err(SqliteError::from)?;
        match self.apply_and_record(migration, ordinal, &checksum) {
            Ok(ApplyOutcome::Applied) => {
                // The runner opened this migration's transaction (`BEGIN IMMEDIATE`)
                // and owns its boundary. If the connection is ALREADY back at
                // autocommit, a statement in the migration committed / rolled it
                // back (a top-level or in-body `COMMIT`/`ROLLBACK`) — the
                // per-migration atomicity the runner guarantees is broken, and the
                // migration's earlier statements (plus the ledger row) committed
                // piecemeal. Fail loud WITHOUT the runner's own `COMMIT` (which
                // would itself error "cannot commit - no transaction is active").
                // Nothing to roll back — autocommit means no open transaction. The
                // NATIVE-status runtime peer of the PostgreSQL runner's backstop.
                if self.inner.is_autocommit() {
                    return Err(MigrationError::TransactionBoundaryBroken {
                        migration: migration.name.clone(),
                    });
                }
                match self.inner.execute_batch("COMMIT") {
                    Ok(()) => Ok(true),
                    Err(commit_err) => {
                        // COMMIT failed (e.g. BUSY on the RESERVED→EXCLUSIVE upgrade
                        // blocked by a reader, or an interrupt at COMMIT): the
                        // transaction is still OPEN on the reused handle. Best-effort
                        // ROLLBACK to a clean boundary (swallow its own error) so a
                        // later `run_migrations` can BEGIN cleanly — symmetric with
                        // the Skipped/Err arms — and return the ORIGINAL COMMIT error
                        // UNCHANGED, so its classification (is_busy / is_disconnect,
                        // via the preserved SQLite code) survives as the retry signal.
                        match self.inner.execute_batch("ROLLBACK") {
                            Ok(()) | Err(_) => {}
                        }
                        Err(MigrationError::from(SqliteError::from(commit_err)))
                    }
                }
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
