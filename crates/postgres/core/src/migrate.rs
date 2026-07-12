//! The migration RUNNER — applies a consumer's migration set to a live
//! PostgreSQL database, exactly once, in the SAME deterministic order the
//! build-time catalog replay uses.
//!
//! The transport-agnostic PURE logic (the content checksum, the `/`-normalized
//! name ordering authority, the drift classification, the source loader) lives
//! in the dependency-free [`bsql_common::migrate`] leaf crate — ONE compiled
//! source shared with the embedded SQLite driver, so the cross-backend checksum
//! / order / drift behaviour cannot silently diverge. This module holds the
//! PostgreSQL-specific I/O: the ledger DDL, the non-blocking advisory-lock poll,
//! the per-migration transaction, and this backend's own [`MigrationError`]
//! (which carries a `LockTimeout` variant SQLite has no peer for). It bridges to
//! the shared classifier through [`bsql_common::migrate::plan`] plus a local
//! `From<`[`bsql_common::migrate::Drift`]`>`.
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
use std::time::Duration;

use bsql_postgres_proto::engine::Transport;

use crate::driver::Core;
use crate::error::{ColumnError, DriverError};

// The migration PURE logic (checksum / ordering / drift authority + source
// loader + the plain data / error types) lives ONCE in the dependency-free
// `bsql-common` leaf crate. Re-exported here so the existing
// `bsql_postgres_core::{AppliedMigration, ...}` paths stay stable.
pub use bsql_common::migrate::{
    AppliedMigration, DriftKind, MigrationReport, MigrationSource, MigrationSourceError,
    MigrationStatus, LEDGER_TABLE,
};
use bsql_common::migrate::{checksum_hex, is_non_transactional, migration_checksum, plan, Drift, LoadedMigration};

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

/// The `pg_advisory_lock` key for a ledger name: the name's checksum
/// reinterpreted as `i64` (the same numeric value on every platform — a bit
/// reinterpretation, not a lossy cast). A fixed name yields a fixed key, so all
/// runners against one database serialize on the same lock.
#[must_use]
pub fn advisory_lock_key(ledger_name: &str) -> i64 {
    i64::from_ne_bytes(migration_checksum(ledger_name).to_ne_bytes())
}

// ─────────────────────────────────────────────────────────────────────────────
// Error type (PostgreSQL-specific — carries a `LockTimeout` SQLite has no peer
// for, and lifts the shared `bsql_common::migrate::Drift` classifier).
// ─────────────────────────────────────────────────────────────────────────────

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

impl From<Drift> for MigrationError {
    fn from(d: Drift) -> Self {
        MigrationError::Drift {
            migration: d.migration,
            kind: d.kind,
        }
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
            // Progress events (cold, per-migration): a long migration run is now
            // visible instead of silent between the start and the final report.
            self.diagnostics()
                .emit(&crate::diag::DiagEvent::MigrationApplying { name: &migration.name });
            self.apply_one(migration, ordinal).await?;
            self.diagnostics()
                .emit(&crate::diag::DiagEvent::MigrationApplied { name: &migration.name });
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
