//! Build-time helper for bsql's compile-checked query API.
//!
//! This crate is a **build-dependency only**. A consumer crate calls
//! [`emit_catalog`] from its `build.rs`; nothing here is ever linked
//! into a shipped library's runtime closure. (Both this crate and
//! `sqlparser`, its only dependency, are proven absent from every shipped
//! crate's `cargo tree -e normal` runtime graph by the workspace
//! `runtime_graph_pin` gate — the `deps_pin` gate pins only the lockfile
//! package set and is dependency-kind-blind, so it cannot prove this
//! build-vs-runtime boundary on its own.)
//!
//! # What it does
//!
//! Given a directory of migration `*.sql` files, it:
//!
//! 1. Emits `cargo:rerun-if-changed` for the migrations **directory**
//!    (so ADDING or REMOVING a migration file recompiles — directory
//!    mtime tracks membership, which `include_str!` does not; that gap
//!    is the stale-schema blind spot this design closes), one such line
//!    for **every nested subdirectory** it descends into (so membership
//!    is tracked at every level, not just the top), **plus** a per-file
//!    `cargo:rerun-if-changed` (so EDITING a file's contents recompiles
//!    too).
//! 2. Parses each file's DDL with `sqlparser` and **replays**
//!    `CREATE TABLE` / `ALTER TABLE ADD|DROP|ALTER|RENAME COLUMN` /
//!    `SET|DROP NOT NULL` / `RENAME TO` / `DROP TABLE` into a [`Catalog`]
//!    (table -> column -> `{ pg_type, not_null }`), applied in
//!    lexicographic path order (so migrations replay deterministically).
//! 3. Writes the catalog, in a deterministic line-oriented text format,
//!    to `OUT_DIR/bsql_schema_catalog.txt`, and sets
//!    `cargo:rustc-env=BSQL_SCHEMA_CATALOG=<that path>` so the query
//!    proc-macro can read it at expansion via `std::env::var`.
//!
//! # Identifier case
//!
//! PostgreSQL folds **unquoted** identifiers to lowercase and preserves
//! case only inside double quotes. This replay does the same: an unquoted
//! `CREATE TABLE Accounts (UserId int)` is catalogued as `accounts` /
//! `userid`, while a double-quoted `"Mixed"` keeps its case. So a
//! reference resolves exactly as it would against the live server.
//!
//! # Guarantee boundary
//!
//! The catalog reflects the migration **files**, not the live database. A
//! migration applied out-of-band (e.g. by hand in `psql`) without a
//! corresponding file is invisible here by design. This is still strictly
//! stronger than a live-introspection cache that can silently go stale
//! relative to the migrations under version control: the source of truth
//! is the committed migration set, and any change to it recompiles.
//!
//! # Fail-closed
//!
//! Every error — a missing migrations directory, an unreadable file, a
//! parse error, or a DDL statement the replay cannot faithfully model
//! (e.g. dropping a column that does not exist, a `CREATE TABLE ... AS
//! SELECT` whose columns this replay does not derive, or an `ALTER` shape
//! operation outside the modeled set) — is returned as a [`BuildError`].
//! A consumer's `build.rs` propagates it (`?` in a
//! `fn main() -> Result<...>`), which **fails the build**. Nothing that
//! carries table or column shape is ever silently skipped: a silent skip
//! would let a wrong catalog pass, which is exactly the blind spot this
//! design exists to remove. A DDL form we cannot model faithfully is a
//! loud build error, never a silently-wrong catalog.

#![forbid(unsafe_code)]

mod dynamics;
mod infer;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use dynamics::{
    infer_dynamic_query, DynamicError, DynamicShape, OrderByVariant, ParamShape, WireVariant,
};
pub use infer::{infer_query, InferError, InferredColumn, QueryShape, RustType};
#[cfg(feature = "sqlite")]
pub use sqlite::{
    emit_sqlite_template, verify_sqlite_conformance, SqliteConformanceError,
    SQLITE_TARGET_ENV_VAR, SQLITE_TEMPLATE_ENV_VAR, SQLITE_TEMPLATE_FILE_NAME,
};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, Expr, Ident, IndexColumn,
    ObjectName, RenameTableNameKind, Statement, TableConstraint,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// The basename of the catalog file written into `OUT_DIR`.
pub const CATALOG_FILE_NAME: &str = "bsql_schema_catalog.txt";

/// The environment variable, set via `cargo:rustc-env`, that carries the
/// absolute path of the generated catalog to the query proc-macro.
pub const CATALOG_ENV_VAR: &str = "BSQL_SCHEMA_CATALOG";

/// A column's replayed shape: its canonical PostgreSQL type name and
/// whether the schema marks it `NOT NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Canonical PostgreSQL type name (e.g. `int8`, `text`, `varchar`).
    pub pg_type: String,
    /// `true` when the column is `NOT NULL` (explicitly, or via a
    /// `PRIMARY KEY` constraint, which implies `NOT NULL` in PostgreSQL).
    pub not_null: bool,
}

/// The replayed schema: tables in insertion-stable order, each mapping
/// column name -> [`ColumnInfo`]. `BTreeMap` keeps both levels sorted, so
/// the serialized catalog is byte-deterministic across builds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    /// table name -> (column name -> column info).
    pub tables: BTreeMap<String, BTreeMap<String, ColumnInfo>>,
    /// table name -> the SET of columns forming that table's PRIMARY KEY
    /// (case-folded to match the column keys), declared either at column
    /// level (`id BIGINT PRIMARY KEY`) or at table level
    /// (`PRIMARY KEY (a, b)`), or added later by `ALTER TABLE ... ADD
    /// PRIMARY KEY (...)`. A table with no primary key has no entry. The
    /// set is used to decide functional dependency in an aggregate query: a
    /// relation whose ENTIRE primary key appears in the `GROUP BY` set
    /// determines all of its columns. A `BTreeSet` is order-independent
    /// (membership is all the coverage check needs) and keeps the serialized
    /// form byte-deterministic.
    pub primary_keys: BTreeMap<String, BTreeSet<String>>,
}

/// A build-time failure. Every variant is fatal: the consumer's
/// `build.rs` propagates it and the build fails (fail-closed).
#[derive(Debug)]
pub enum BuildError {
    /// The migrations directory is missing or could not be listed.
    MigrationsDir { path: PathBuf, source: std::io::Error },
    /// A migration file could not be read.
    ReadFile { path: PathBuf, source: std::io::Error },
    /// `sqlparser` rejected a file's SQL.
    Parse { path: PathBuf, message: String },
    /// A parsed statement could not be replayed against the catalog
    /// (e.g. `ALTER TABLE` on an unknown table, or `DROP COLUMN` of a
    /// column that does not exist). Never silently skipped.
    Replay { path: PathBuf, message: String },
    /// Writing the catalog to `OUT_DIR` failed.
    WriteCatalog { path: PathBuf, source: std::io::Error },
    /// A required environment variable (`OUT_DIR`) was absent.
    MissingEnv { var: &'static str },
    /// Creating or removing the SQLite template database in `OUT_DIR`
    /// failed. (Only constructible under the `sqlite` feature.)
    SqliteTemplate { message: String },
    /// SQLite could not replay a migration's DDL into the template database
    /// — an unmodelable migration form (e.g. an `ALTER COLUMN ... SET NOT
    /// NULL`, which SQLite does not support). Never silently skipped. (Only
    /// constructible under the `sqlite` feature.)
    SqliteReplay { path: PathBuf, message: String },
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::MigrationsDir { path, source } => write!(
                f,
                "bsql-build: cannot read migrations directory {}: {source}",
                path.display()
            ),
            BuildError::ReadFile { path, source } => write!(
                f,
                "bsql-build: cannot read migration file {}: {source}",
                path.display()
            ),
            BuildError::Parse { path, message } => write!(
                f,
                "bsql-build: SQL parse error in {}: {message}",
                path.display()
            ),
            BuildError::Replay { path, message } => write!(
                f,
                "bsql-build: cannot replay DDL from {}: {message}",
                path.display()
            ),
            BuildError::WriteCatalog { path, source } => write!(
                f,
                "bsql-build: cannot write catalog {}: {source}",
                path.display()
            ),
            BuildError::MissingEnv { var } => write!(
                f,
                "bsql-build: required environment variable {var} is not set \
                 (this fn must be called from a Cargo build script)"
            ),
            BuildError::SqliteTemplate { message } => {
                write!(f, "bsql-build: SQLite template database error: {message}")
            }
            BuildError::SqliteReplay { path, message } => write!(
                f,
                "bsql-build: SQLite cannot replay migration {}: {message}. A \
                 migration DDL form SQLite cannot execute is a build error \
                 (define the schema in a SQLite-portable form, or do not \
                 target SQLite).",
                path.display()
            ),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuildError::MigrationsDir { source, .. }
            | BuildError::ReadFile { source, .. }
            | BuildError::WriteCatalog { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Emit the PostgreSQL schema catalog channel.
///
/// This is the PostgreSQL-only building block; most consumers call the
/// single-line [`emit`] instead, which layers the SQLite template on top
/// under the `sqlite` feature. Call this directly for a build that is
/// deliberately PostgreSQL-only (it never emits a SQLite template and so
/// never engages the SQLite conformance oracle):
///
/// ```no_run
/// fn main() -> Result<(), bsql_build::BuildError> {
///     bsql_build::emit_catalog("migrations")
/// }
/// ```
///
/// `migrations_dir` is resolved relative to the consumer crate's
/// `CARGO_MANIFEST_DIR`. It:
///
/// * emits `cargo:rerun-if-changed` for the directory and each `*.sql`
///   file inside it,
/// * builds the catalog from the directory's migrations,
/// * writes it to `OUT_DIR` and sets the `BSQL_SCHEMA_CATALOG`
///   rustc-env channel.
///
/// Returns `Err` (failing the build) on any I/O, parse, or replay error.
pub fn emit_catalog(migrations_dir: impl AsRef<Path>) -> Result<(), BuildError> {
    let manifest = env_path("CARGO_MANIFEST_DIR")?;
    let dir = manifest.join(migrations_dir.as_ref());

    // Walk the migrations tree once, collecting every `*.sql` file at any
    // depth and every directory along the way, and replay each file into
    // the catalog. `catalog_from_walk` does no I/O beyond reading the
    // files, so the cargo directives below are emitted from the same walk.
    let walk = scan_sql_tree(&dir)?;

    // Membership tracking (LOAD-BEARING): a directory's mtime changes when
    // a file or subdirectory is added or removed inside it, so emitting a
    // `rerun-if-changed` for EVERY directory (the root and each nested
    // one) is what makes ADD/DELETE of a migration at ANY depth recompile
    // dependents. `include_str!` tracks file CONTENT only — that is the
    // stale-schema blind spot this closes at every level.
    for directory in &walk.dirs {
        println!("cargo:rerun-if-changed={}", directory.display());
    }
    for file in &walk.files {
        // Per-file content tracking (belt): EDITING a file recompiles.
        println!("cargo:rerun-if-changed={}", file.display());
    }

    let catalog = catalog_from_walk(&walk)?;

    let out_dir = env_path("OUT_DIR")?;
    let catalog_path = out_dir.join(CATALOG_FILE_NAME);
    let serialized = serialize(&catalog);
    std::fs::write(&catalog_path, serialized).map_err(|source| BuildError::WriteCatalog {
        path: catalog_path.clone(),
        source,
    })?;

    // The channel: a rustc-env var carrying the catalog's absolute path.
    // Cargo injects rustc-env vars into the rustc invocation that
    // compiles the consumer, so the proc-macro (which runs inside that
    // rustc) reads it via `std::env::var`.
    println!(
        "cargo:rustc-env={CATALOG_ENV_VAR}={}",
        catalog_path.display()
    );

    Ok(())
}

/// The one-line build-script entry point for a bsql consumer.
///
/// Call this from a consumer crate's `build.rs`:
///
/// ```no_run
/// fn main() -> Result<(), bsql_build::BuildError> {
///     bsql_build::emit("migrations")
/// }
/// ```
///
/// It always [`emit_catalog`]s the PostgreSQL schema catalog. When this
/// crate's `sqlite` feature is enabled — a consumer targeting SQLite adds
/// `bsql-build = { features = ["sqlite"] }` to `[build-dependencies]` — it
/// ALSO emits the SQLite conformance template (via `emit_sqlite_template`),
/// from the SAME call. Enabling the SQLite target and emitting its template are
/// therefore INSEPARABLE: there is no separate build-script step to leave
/// out, so the compile-checked SQLite conformance oracle can never silently
/// disengage because a consumer forgot a second call.
///
/// Returns `Err` (failing the build) on any I/O, parse, or replay error —
/// including, under the `sqlite` feature, a migration form SQLite cannot
/// replay. Nothing carrying schema shape is ever silently skipped.
pub fn emit(migrations_dir: impl AsRef<Path>) -> Result<(), BuildError> {
    emit_catalog(&migrations_dir)?;
    #[cfg(feature = "sqlite")]
    emit_sqlite_template(&migrations_dir)?;
    Ok(())
}

/// Build the [`Catalog`] from a directory of migration `*.sql` files,
/// recursing into subdirectories, WITHOUT touching the environment, stdout,
/// or `OUT_DIR`. This is the pure core [`emit_catalog`] wraps with the
/// cargo build-script plumbing; it is exposed so the migrations -> catalog
/// replay can be exercised directly (e.g. in tests, or to inspect the
/// replayed schema).
///
/// Files are replayed in lexicographic path order across the whole tree.
/// Any I/O, parse, or replay error fails closed as a [`BuildError`].
pub fn catalog_from_dir(migrations_dir: impl AsRef<Path>) -> Result<Catalog, BuildError> {
    let walk = scan_sql_tree(migrations_dir.as_ref())?;
    catalog_from_walk(&walk)
}

/// Replay every walked file into a fresh catalog.
fn catalog_from_walk(walk: &Walk) -> Result<Catalog, BuildError> {
    let mut catalog = Catalog::default();
    for file in &walk.files {
        let sql = std::fs::read_to_string(file).map_err(|source| BuildError::ReadFile {
            path: file.clone(),
            source,
        })?;
        replay_file(&mut catalog, file, &sql)?;
    }
    Ok(catalog)
}

/// Read an environment variable as a `PathBuf`, failing closed if absent.
fn env_path(var: &'static str) -> Result<PathBuf, BuildError> {
    match std::env::var_os(var) {
        Some(v) => Ok(PathBuf::from(v)),
        None => Err(BuildError::MissingEnv { var }),
    }
}

/// The result of walking the migrations tree: every `*.sql` file (at any
/// depth, in deterministic path order) and every directory visited.
#[derive(Default)]
struct Walk {
    /// `*.sql` files, sorted by full path so replay order is stable and
    /// independent of filesystem iteration order.
    files: Vec<PathBuf>,
    /// Every directory descended into (the root plus each nested one), so
    /// a `rerun-if-changed` can be emitted per level — membership of a
    /// migration added or removed inside ANY subdirectory is tracked.
    dirs: Vec<PathBuf>,
}

/// Walk the migrations tree rooted at `dir`, returning every `*.sql` file
/// (at any depth, sorted by full path) and every directory visited.
///
/// Recursion (rather than rejecting subdirectories) is deliberate:
/// partitioned migration layouts (e.g. `migrations/2026/0001.sql`) are a
/// legitimate convention, so they must be picked up, not refused. Files
/// are sorted by full path AFTER the walk so the replay sequence is
/// deterministic regardless of the order the filesystem yields entries.
fn scan_sql_tree(dir: &Path) -> Result<Walk, BuildError> {
    let mut walk = Walk::default();
    descend(dir, &mut walk)?;
    // Sort the accumulated files once, by full path, so the whole tree
    // replays in a single global lexicographic order across all depths.
    walk.files.sort();
    Ok(walk)
}

/// Recursive worker for [`scan_sql_tree`]: records `dir`, then visits its
/// entries, descending into subdirectories and collecting `*.sql` files.
fn descend(dir: &Path, walk: &mut Walk) -> Result<(), BuildError> {
    walk.dirs.push(dir.to_path_buf());

    let entries = std::fs::read_dir(dir).map_err(|source| BuildError::MigrationsDir {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| BuildError::MigrationsDir {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| BuildError::MigrationsDir {
            path: path.clone(),
            source,
        })?;
        if file_type.is_dir() {
            descend(&path, walk)?;
        } else if path.extension().is_some_and(|ext| ext == "sql") {
            walk.files.push(path);
        }
    }
    Ok(())
}

/// Test-only re-export of [`replay_file`] so the sibling `infer` module's
/// unit tests can build a [`Catalog`] from DDL strings without duplicating
/// the parse/replay path.
#[cfg(test)]
pub(crate) fn replay_file_for_test(
    catalog: &mut Catalog,
    path: &Path,
    sql: &str,
) -> Result<(), BuildError> {
    replay_file(catalog, path, sql)
}

/// Parse and replay every statement in one migration file.
fn replay_file(catalog: &mut Catalog, path: &Path, sql: &str) -> Result<(), BuildError> {
    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|err| BuildError::Parse {
            path: path.to_path_buf(),
            message: err.to_string(),
        })?;
    for statement in statements {
        replay_statement(catalog, path, statement)?;
    }
    Ok(())
}

/// Replay one DDL statement. DDL we model (`CREATE TABLE`,
/// `ALTER TABLE`, `DROP TABLE`, MySQL-style `RENAME TABLE`) mutates the
/// catalog. Other top-level statements (e.g. `CREATE INDEX`, `INSERT`
/// seed data, `CREATE VIEW`, `COMMENT`) carry no base-table column shape
/// and pass through unchanged — but a statement that DOES carry table or
/// column shape we cannot model faithfully is a loud error, never a
/// silent skip (a silent skip would let a wrong catalog pass).
fn replay_statement(
    catalog: &mut Catalog,
    path: &Path,
    statement: Statement,
) -> Result<(), BuildError> {
    match statement {
        Statement::CreateTable(create) => replay_create_table(catalog, path, create),
        Statement::AlterTable(alter) => {
            let table = replay_relation_key(&alter.name, path)?;
            for op in alter.operations {
                replay_alter_op(catalog, path, &table, op)?;
            }
            Ok(())
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Table,
            names,
            ..
        } => {
            for name in names {
                let table = replay_relation_key(&name, path)?;
                catalog.tables.remove(&table);
                // The primary-key record is keyed by the same table name, so
                // it is removed alongside the columns; leaving it would let a
                // later same-named table inherit a stale key.
                catalog.primary_keys.remove(&table);
            }
            Ok(())
        }
        // `RENAME TABLE old TO new [, ...]` (the MySQL spelling; the
        // PostgreSQL spelling is `ALTER TABLE old RENAME TO new`, handled
        // in `replay_alter_op`). This carries table shape: skipping it
        // would leave the catalog keyed by the OLD name. Re-key so the
        // catalog tracks the rename and an old-name reference stops
        // resolving.
        Statement::RenameTable(renames) => {
            for rename in renames {
                let from = replay_relation_key(&rename.old_name, path)?;
                let to = replay_relation_key(&rename.new_name, path)?;
                rekey_table(catalog, path, &from, to)?;
            }
            Ok(())
        }
        // Statements without base-table column-shape meaning (CREATE
        // INDEX, seed INSERTs, CREATE/ALTER VIEW, COMMENT, GRANT, CREATE
        // SCHEMA/SEQUENCE/TYPE, SET, etc.) carry no change to a tracked
        // table's columns, so passing them through is correct — not a
        // silent skip of schema information this catalog models.
        _ => Ok(()),
    }
}

/// Replay a `CREATE TABLE`. Only the explicit-column-list form is
/// modeled; any form whose final column set this replay cannot derive
/// faithfully is a loud error rather than a silently empty/merged table.
fn replay_create_table(
    catalog: &mut Catalog,
    path: &Path,
    create: sqlparser::ast::CreateTable,
) -> Result<(), BuildError> {
    let table = replay_relation_key(&create.name, path)?;

    // Forms whose columns this replay does NOT derive. Registering them as
    // an empty (or merged) table would silently hide every column they
    // actually have — the exact wrong-catalog blind spot. Reject loudly,
    // naming the unsupported form, instead.
    if create.query.is_some() {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "CREATE TABLE `{table}` ... AS SELECT is not modeled: its \
                 columns come from the query, which this build-time replay \
                 does not evaluate. Define the table with an explicit \
                 column list."
            ),
        });
    }
    if create.like.is_some() {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "CREATE TABLE `{table}` (LIKE ...) is not modeled: its \
                 columns are copied from another table, which this \
                 build-time replay does not resolve. Define the table with \
                 an explicit column list."
            ),
        });
    }
    if create.partition_of.is_some() {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "CREATE TABLE `{table}` PARTITION OF ... is not modeled: it \
                 inherits its parent's columns, which this build-time replay \
                 does not resolve."
            ),
        });
    }
    if create.inherits.is_some() {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "CREATE TABLE `{table}` ... INHERITS (...) is not modeled: it \
                 gains its parents' columns, which this build-time replay \
                 does not resolve."
            ),
        });
    }
    if create.clone.is_some() {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "CREATE TABLE `{table}` ... CLONE ... is not modeled: its \
                 columns are copied from another table, which this \
                 build-time replay does not resolve."
            ),
        });
    }

    // A second `CREATE TABLE` of an existing table WITHOUT `IF NOT EXISTS`
    // is rejected by PostgreSQL. Silently merging columns (the previous
    // `entry().or_default()` behaviour) would diverge from the server.
    // `IF NOT EXISTS` against an existing table is a documented no-op.
    if catalog.tables.contains_key(&table) {
        if create.if_not_exists {
            return Ok(());
        }
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "CREATE TABLE `{table}`: a table by this name already exists \
                 in an earlier migration. PostgreSQL rejects a duplicate \
                 CREATE TABLE without IF NOT EXISTS."
            ),
        });
    }

    let mut columns: BTreeMap<String, ColumnInfo> = BTreeMap::new();
    // The PRIMARY KEY can be declared in exactly one of two places: at COLUMN
    // level (`id BIGINT PRIMARY KEY`) on a single column, or at TABLE level
    // (`PRIMARY KEY (a, b)`) over one or more columns. PostgreSQL allows AT
    // MOST ONE primary key per table ("multiple primary keys for table are not
    // allowed"), so a second declaration in either place is a migration error
    // this replay rejects loudly rather than silently keeping one and dropping
    // the other (which would record a wrong key set). Whichever form declares
    // it, those columns are NOT NULL in PostgreSQL.
    //
    // `pk_columns` is `Some(set)` once the single primary key has been seen.
    let mut pk_columns: Option<BTreeSet<String>> = None;
    let mut set_pk = |names: Vec<String>| -> Result<(), BuildError> {
        if pk_columns.is_some() {
            return Err(BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "CREATE TABLE `{table}` declares more than one PRIMARY KEY. \
                     PostgreSQL allows at most one primary key per table."
                ),
            });
        }
        pk_columns = Some(names.into_iter().collect());
        Ok(())
    };
    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey(pk) = constraint {
            set_pk(index_column_names(&pk.columns))?;
        }
    }
    for column in &create.columns {
        if column_is_primary_key(column) {
            set_pk(vec![fold_ident(&column.name)])?;
        }
    }
    for column in &create.columns {
        let info = column_info(column);
        columns.insert(fold_ident(&column.name), info);
    }
    if let Some(pk) = pk_columns {
        for name in &pk {
            if let Some(info) = columns.get_mut(name) {
                info.not_null = true;
            }
        }
        // Record the key only when it names at least one resolvable column. A
        // table-level `PRIMARY KEY` over solely functional-index expressions
        // (which carry no plain column name) yields an empty set; an empty PK
        // set would spuriously "cover" every column under the functional-
        // dependency rule, so it is omitted rather than stored.
        if !pk.is_empty() {
            catalog.primary_keys.insert(table.clone(), pk);
        }
    }
    catalog.tables.insert(table, columns);
    Ok(())
}

/// Move a table's column map from `from` to `to`, removing the old key.
/// A reference to the old name then stops resolving (the rename is
/// reflected), and a reference to the new name resolves. Renaming a table
/// that does not exist is a loud error, never a silent skip.
fn rekey_table(
    catalog: &mut Catalog,
    path: &Path,
    from: &str,
    to: String,
) -> Result<(), BuildError> {
    let columns = catalog.tables.remove(from).ok_or_else(|| BuildError::Replay {
        path: path.to_path_buf(),
        message: format!("RENAME of unknown table `{from}`: no such table"),
    })?;
    // Re-key the primary-key record under the new table name so the functional
    // dependency continues to resolve after a rename. A table with no primary
    // key has no entry to move.
    if let Some(pk) = catalog.primary_keys.remove(from) {
        catalog.primary_keys.insert(to.clone(), pk);
    }
    catalog.tables.insert(to, columns);
    Ok(())
}

/// Replay one `ALTER TABLE` operation against the named table.
///
/// The match is exhaustive — there is no `_` arm — so a future
/// `sqlparser` upgrade that adds an `AlterTableOperation` variant is a
/// compile error, forcing a human to classify it as shape-irrelevant
/// (allowlist) or shape-carrying (loud). That is the fail-closed
/// guarantee enforced by the compiler, not by a comment.
fn replay_alter_op(
    catalog: &mut Catalog,
    path: &Path,
    table: &str,
    op: AlterTableOperation,
) -> Result<(), BuildError> {
    // `RENAME TO` re-keys the whole table, so it must run BEFORE acquiring
    // a borrow into one table's column map.
    if let AlterTableOperation::RenameTable { table_name } = op {
        let to = match table_name {
            RenameTableNameKind::To(name) | RenameTableNameKind::As(name) => {
                replay_relation_key(&name, path)?
            }
        };
        return rekey_table(catalog, path, table, to);
    }

    let columns = catalog.tables.get_mut(table).ok_or_else(|| BuildError::Replay {
        path: path.to_path_buf(),
        message: format!("ALTER TABLE on unknown table `{table}`"),
    })?;
    match op {
        AlterTableOperation::AddColumn { column_def, .. } => {
            let info = column_info(&column_def);
            columns.insert(fold_ident(&column_def.name), info);
            Ok(())
        }
        AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            for ident in column_names {
                let name = fold_ident(&ident);
                if columns.remove(&name).is_none() && !if_exists {
                    return Err(BuildError::Replay {
                        path: path.to_path_buf(),
                        message: format!(
                            "DROP COLUMN `{name}` on table `{table}`: no such column"
                        ),
                    });
                }
                // Dropping a column that is part of the primary key drops the
                // whole primary key in PostgreSQL (the key can no longer be
                // formed). The recorded key would otherwise name a column that
                // no longer exists and could falsely satisfy the functional
                // dependency, so the entire record is removed. `primary_keys`
                // is a field disjoint from the `columns` borrow above.
                if let Some(pk) = catalog.primary_keys.get(table)
                    && pk.contains(&name)
                {
                    catalog.primary_keys.remove(table);
                }
            }
            Ok(())
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            let old = fold_ident(&old_column_name);
            let new = fold_ident(&new_column_name);
            let info = columns.remove(&old).ok_or_else(|| BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "RENAME COLUMN `{old}` on table `{table}`: no such column"
                ),
            })?;
            columns.insert(new.clone(), info);
            // A renamed column that is part of the primary key keeps its
            // membership under the new name, so the functional dependency
            // continues to resolve after the rename. `primary_keys` is a
            // field disjoint from the `columns` borrow above.
            if let Some(pk) = catalog.primary_keys.get_mut(table)
                && pk.remove(&old)
            {
                pk.insert(new);
            }
            Ok(())
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            let name = fold_ident(&column_name);
            let info = columns.get_mut(&name).ok_or_else(|| BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "ALTER COLUMN `{name}` on table `{table}`: no such column"
                ),
            })?;
            match op {
                AlterColumnOperation::SetNotNull => {
                    info.not_null = true;
                    Ok(())
                }
                AlterColumnOperation::DropNotNull => {
                    info.not_null = false;
                    Ok(())
                }
                AlterColumnOperation::SetDataType { data_type, .. } => {
                    info.pg_type = canonical_type(&data_type);
                    Ok(())
                }
                // SET/DROP DEFAULT and ADD GENERATED change runtime
                // behaviour but not the column's {type, nullability}
                // shape this catalog tracks.
                AlterColumnOperation::SetDefault { .. }
                | AlterColumnOperation::DropDefault
                | AlterColumnOperation::AddGenerated { .. } => Ok(()),
            }
        }
        // A table-level constraint add. Only a PRIMARY KEY changes a
        // column's {type, nullability} shape (its columns become NOT
        // NULL) and establishes the table's primary key; every other
        // constraint kind (UNIQUE, CHECK, FOREIGN KEY) is shape-irrelevant
        // to this catalog. A second PRIMARY KEY (one already exists) is a
        // migration error PostgreSQL rejects ("multiple primary keys ... are
        // not allowed"), so it is loud rather than a silent overwrite of the
        // recorded key. `primary_keys` is a field disjoint from the `columns`
        // borrow above.
        AlterTableOperation::AddConstraint { constraint, .. } => {
            if let TableConstraint::PrimaryKey(pk) = constraint {
                let names = index_column_names(&pk.columns);
                for name in &names {
                    if let Some(info) = columns.get_mut(name) {
                        info.not_null = true;
                    }
                }
                let key: BTreeSet<String> = names.into_iter().collect();
                if !key.is_empty() {
                    if catalog.primary_keys.contains_key(table) {
                        return Err(BuildError::Replay {
                            path: path.to_path_buf(),
                            message: format!(
                                "ALTER TABLE `{table}` ADD PRIMARY KEY: a primary \
                                 key already exists. PostgreSQL allows at most one \
                                 primary key per table."
                            ),
                        });
                    }
                    catalog.primary_keys.insert(table.to_string(), key);
                }
            }
            Ok(())
        }

        // ---- Shape-carrying operations we do NOT model: LOUD. ----
        // Each of these can change a column's name or {type, nullability}
        // in a way this catalog must track but does not derive faithfully,
        // so it fails closed rather than leaving a stale column entry.
        // (`CHANGE`/`MODIFY` are MySQL spellings; PostgreSQL uses the
        // `ALTER COLUMN` form handled above. `DROP PRIMARY KEY` would make
        // its columns nullable again — a nullability change we do not
        // resolve back to the affected columns.)
        AlterTableOperation::ChangeColumn { old_name, .. } => Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "ALTER TABLE `{table}` CHANGE COLUMN `{}` is not modeled (it \
                 renames and retypes a column); use the explicit ALTER \
                 COLUMN / RENAME COLUMN forms.",
                old_name.value
            ),
        }),
        AlterTableOperation::ModifyColumn { col_name, .. } => Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "ALTER TABLE `{table}` MODIFY COLUMN `{}` is not modeled (it \
                 retypes a column); use the explicit ALTER COLUMN ... TYPE \
                 form.",
                col_name.value
            ),
        }),
        AlterTableOperation::DropPrimaryKey { .. } => Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "ALTER TABLE `{table}` DROP PRIMARY KEY is not modeled: it \
                 makes the key columns nullable again, a nullability change \
                 this replay does not resolve back to those columns."
            ),
        }),

        // ---- Genuinely shape-irrelevant operations: allowlist. ----
        // None of these change a tracked column's name, type, or
        // nullability, so they are correct no-ops for this catalog.
        // Listed explicitly (no `_` arm) so a new variant is a compile
        // error, never a silent pass.
        //
        // Constraints / keys / indexes (other than PK add, handled above).
        // `DROP CONSTRAINT <name>` carries only the constraint NAME, not its
        // kind, so this replay cannot tell whether the dropped constraint is
        // the primary key. It is treated as shape-irrelevant — the same
        // name-blind boundary the catalog already applies to a column's
        // NOT NULL flag, which a `DROP CONSTRAINT` on the primary key would
        // likewise leave set. The primary key established by `CREATE TABLE`,
        // `ALTER TABLE ADD PRIMARY KEY`, a `DROP COLUMN` of a key member, and
        // a `RENAME COLUMN` of a key member are all tracked exactly; only the
        // name-only `DROP CONSTRAINT` form sits outside that boundary.
        AlterTableOperation::DropConstraint { .. }
        | AlterTableOperation::ValidateConstraint { .. }
        | AlterTableOperation::RenameConstraint { .. }
        | AlterTableOperation::DropForeignKey { .. }
        | AlterTableOperation::DropIndex { .. }
        // Triggers / rules / row-level security (behaviour, not shape).
        | AlterTableOperation::DisableRule { .. }
        | AlterTableOperation::EnableRule { .. }
        | AlterTableOperation::EnableAlwaysRule { .. }
        | AlterTableOperation::EnableReplicaRule { .. }
        | AlterTableOperation::DisableTrigger { .. }
        | AlterTableOperation::EnableTrigger { .. }
        | AlterTableOperation::EnableAlwaysTrigger { .. }
        | AlterTableOperation::EnableReplicaTrigger { .. }
        | AlterTableOperation::DisableRowLevelSecurity
        | AlterTableOperation::EnableRowLevelSecurity
        | AlterTableOperation::ForceRowLevelSecurity
        | AlterTableOperation::NoForceRowLevelSecurity
        // Partitions (a partition is a separate relation; the partitioned
        // table's own column set is unchanged by attaching/detaching).
        | AlterTableOperation::AttachPartition { .. }
        | AlterTableOperation::DetachPartition { .. }
        | AlterTableOperation::FreezePartition { .. }
        | AlterTableOperation::UnfreezePartition { .. }
        | AlterTableOperation::AddPartitions { .. }
        | AlterTableOperation::DropPartitions { .. }
        | AlterTableOperation::RenamePartitions { .. }
        // Projections (ClickHouse-specific; not a base-table column).
        | AlterTableOperation::AddProjection { .. }
        | AlterTableOperation::DropProjection { .. }
        | AlterTableOperation::MaterializeProjection { .. }
        | AlterTableOperation::ClearProjection { .. }
        // Ownership / properties / replica identity / comments / misc.
        | AlterTableOperation::OwnerTo { .. }
        | AlterTableOperation::SwapWith { .. }
        | AlterTableOperation::SetTblProperties { .. }
        | AlterTableOperation::SetOptionsParens { .. }
        | AlterTableOperation::ReplicaIdentity { .. }
        // Clustering / sort keys (storage layout, not column shape).
        | AlterTableOperation::ClusterBy { .. }
        | AlterTableOperation::DropClusteringKey
        | AlterTableOperation::AlterSortKey { .. }
        | AlterTableOperation::SuspendRecluster
        | AlterTableOperation::ResumeRecluster
        // Dynamic-table lifecycle (Snowflake-specific).
        | AlterTableOperation::Refresh { .. }
        | AlterTableOperation::Suspend
        | AlterTableOperation::Resume
        // Engine knobs (MySQL-specific).
        | AlterTableOperation::Algorithm { .. }
        | AlterTableOperation::Lock { .. }
        | AlterTableOperation::AutoIncrement { .. } => Ok(()),

        // `RenameTable` is handled before the column-map borrow above; this
        // arm is unreachable but keeps the match exhaustive without a `_`.
        AlterTableOperation::RenameTable { .. } => Ok(()),
    }
}

/// Whether a column definition carries a column-level `PRIMARY KEY` option
/// (`id BIGINT PRIMARY KEY`). A column-level primary key names exactly this
/// one column as the table's primary key.
fn column_is_primary_key(column: &ColumnDef) -> bool {
    column
        .options
        .iter()
        .any(|option| matches!(option.option, ColumnOption::PrimaryKey(_)))
}

/// Extract the `{ pg_type, not_null }` shape from a column definition.
fn column_info(column: &ColumnDef) -> ColumnInfo {
    let mut not_null = false;
    for option in &column.options {
        match option.option {
            // Explicit `NOT NULL`, or a column-level `PRIMARY KEY`
            // (which implies NOT NULL in PostgreSQL).
            ColumnOption::NotNull | ColumnOption::PrimaryKey(_) => not_null = true,
            ColumnOption::Null => not_null = false,
            _ => {}
        }
    }
    ColumnInfo {
        pg_type: canonical_type(&column.data_type),
        not_null,
    }
}

/// Map a parsed `DataType` to a canonical lowercase PostgreSQL type
/// name. Common SQL aliases collapse to their PG canonical spelling
/// (e.g. `BIGINT` -> `int8`, `INTEGER` -> `int4`). Unrecognised types
/// pass through as their lowercased rendered form rather than being
/// dropped — fail-open on the type *string* is acceptable here because
/// the type is opaque to the table.column existence check; the
/// `pg_type` consumers for Rust-type inference carry their own
/// exhaustive mapping with its own fail-closed contract.
fn canonical_type(data_type: &sqlparser::ast::DataType) -> String {
    use sqlparser::ast::{ArrayElemTypeDef, DataType, TimezoneInfo};
    // ARRAY types must be canonicalised STRUCTURALLY, as `<element>[]`, never
    // via the rendered head word. The head-word split (on ' ' / '(') drops the
    // trailing `[]` for any MULTI-WORD element spelling —
    // `TIMESTAMP WITH TIME ZONE[]` yields head `timestamp`,
    // `DOUBLE PRECISION[]` yields `double`, `CHARACTER VARYING[]` yields
    // `character` — which would collapse an ARRAY column to its SCALAR element
    // type: a silently-wrong catalog (a `timestamptz[]` column typed as a
    // scalar 8-byte `timestamp`, with the scalar OID baked into the wire).
    // Rendering the array form explicitly keeps it OUTSIDE `rust_type_for_pg`'s
    // supported set, so EVERY array spelling — compact or verbose, any element
    // type, any dimensionality — is a loud `UnsupportedPgType` rather than a
    // silent mis-type. When 1-D array support lands, `rust_type_for_pg` gains
    // the matching `<element>[]` arm; until then all arrays fail closed.
    if let DataType::Array(elem) = data_type {
        return match elem {
            ArrayElemTypeDef::SquareBracket(inner, _)
            | ArrayElemTypeDef::AngleBracket(inner)
            | ArrayElemTypeDef::Parenthesis(inner) => format!("{}[]", canonical_type(inner)),
            // An untyped `ARRAY` (no element type) has no element to
            // canonicalise; a bare `array` marker has no supported arm either.
            ArrayElemTypeDef::None => "array".to_string(),
        };
    }
    // Temporal types must be distinguished STRUCTURALLY, not by the rendered
    // head word: `TIMESTAMP WITH TIME ZONE` and the compact `TIMESTAMPTZ`
    // both denote `timestamptz`, but the verbose form's head word is
    // `timestamp`, which would silently collapse the zone away. Match on the
    // parsed `TimezoneInfo` so every spelling of the zoned type canonicalises
    // to `timestamptz` and the zone-less type to `timestamp`.
    if let DataType::Timestamp(_, tz) = data_type {
        return match tz {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => "timestamptz",
            TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => "timestamp",
        }
        .to_string();
    }
    let rendered = data_type.to_string().to_ascii_lowercase();
    // Normalise on the leading word (strip length/precision args like
    // `varchar(50)` -> `varchar`, `numeric(10,2)` -> `numeric`).
    // `split` always yields at least one element, so `next()` is `Some`;
    // an empty rendered type falls through to the empty head.
    let head = match rendered.split(['(', ' ']).next() {
        Some(head) => head,
        None => rendered.as_str(),
    };
    match head {
        "bigint" | "int8" | "bigserial" | "serial8" => "int8",
        "int" | "integer" | "int4" | "serial" | "serial4" => "int4",
        "smallint" | "int2" | "smallserial" | "serial2" => "int2",
        "text" => "text",
        "varchar" | "char" | "character" => "varchar",
        "boolean" | "bool" => "bool",
        "real" | "float4" => "float4",
        "double" | "float8" => "float8",
        "bytea" => "bytea",
        "uuid" => "uuid",
        "numeric" | "decimal" => "numeric",
        // Pass through the head word for anything else.
        other => other,
    }
    .to_string()
}

/// Extract the bare column names from a `PRIMARY KEY (...)` column list,
/// case-folded to match the column keys. Each entry is an index
/// expression; the plain `PRIMARY KEY (a, b)` form parses each as a bare
/// identifier. Non-identifier index expressions (functional indexes)
/// carry no plain column name and are skipped for nullability inference.
fn index_column_names(columns: &[IndexColumn]) -> Vec<String> {
    let mut names = Vec::new();
    for col in columns {
        if let Expr::Identifier(ident) = &col.column.expr {
            names.push(fold_ident(ident));
        }
    }
    names
}

/// Fold an identifier to its catalog key, matching PostgreSQL resolution:
/// an UNQUOTED identifier folds to lowercase, while a double-quoted one
/// keeps its exact case. `sqlparser` records the original quote character
/// in `quote_style` (`None` when the identifier was written unquoted), so
/// case-sensitivity is decided by how the migration author wrote it — the
/// same rule the live server applies.
fn fold_ident(ident: &Ident) -> String {
    match ident.quote_style {
        // Quoted (PostgreSQL uses `"`): preserve case exactly.
        Some(_) => ident.value.clone(),
        // Unquoted: fold to lowercase, as PostgreSQL does at resolution.
        None => ident.value.to_ascii_lowercase(),
    }
}

/// Whether a leading schema qualifier folds to the default `public` schema,
/// using the SAME PostgreSQL case rule the catalog applies everywhere
/// ([`fold_ident`]): an UNQUOTED `public` folds case-insensitively (so
/// `PUBLIC` matches), and a double-quoted `"public"` matches only when its
/// preserved-case value is exactly `public` (so `"PUBLIC"` does NOT).
///
/// The catalog has a single namespace — the DDL replay keys every table by its
/// bare name — so a qualifier that folds to `public` is the explicit spelling
/// of that one namespace and is dropped, while any other schema names a
/// dimension the catalog does not model and stays loud at the caller.
fn schema_part_folds_to_public(schema: &Ident) -> bool {
    fold_ident(schema) == "public"
}

/// The trailing identifier of an `ObjectName`, case-folded (drops any
/// schema/database qualifier; the catalog keys tables by their bare name
/// for the current scope).
fn object_name_leaf(name: &ObjectName) -> String {
    match name.0.last().and_then(|part| part.as_ident()) {
        Some(ident) => fold_ident(ident),
        // An ObjectName always has at least one part after a successful
        // parse; render the whole thing as a defensive non-empty key
        // rather than fabricate one.
        None => name.to_string(),
    }
}

/// Resolve a DDL relation `ObjectName` to its catalog key, LOUD on any
/// qualifier the single-namespace catalog cannot model.
///
/// The catalog keys every table by its bare name, so a relation is keyed only
/// when it is:
///
/// * a 1-part bare name (`widgets`), folded to its leaf; or
/// * a 2-part name whose leading qualifier folds to the default `public`
///   schema (`public.widgets`, `"public".widgets`), folded to its leaf.
///
/// A NON-`public` schema (`wrongschema.widgets`) or a 3+-part path
/// (`db.schema.widgets`) names a namespace dimension the catalog does not
/// model. Silently re-keying it to the bare leaf would catalog the table under
/// a name a bare query resolves to — but in PostgreSQL that table lives in a
/// different schema and a bare query hits `public` instead, so the catalog
/// would be wrong. It is a loud [`BuildError::Replay`] naming the FULL path
/// (symmetric with the query-side relation resolver), never a silently-wrong
/// catalog. This mirrors the relation resolver: `public.X` and bare `X` both
/// key to `X`.
fn replay_relation_key(name: &ObjectName, path: &Path) -> Result<String, BuildError> {
    let loud = || BuildError::Replay {
        path: path.to_path_buf(),
        message: format!(
            "relation `{name}` is schema-qualified with a schema this \
             single-namespace catalog does not model. Only a bare name or the \
             default `public` schema is keyed; a non-public schema or a \
             multi-part path names a namespace dimension the catalog cannot \
             carry. Define the table in the default schema, or reference it \
             unqualified."
        ),
    };
    match name.0.as_slice() {
        [only] => only.as_ident().map(fold_ident).ok_or_else(loud),
        [schema, table] => {
            let schema = schema.as_ident().ok_or_else(loud)?;
            if schema_part_folds_to_public(schema) {
                table.as_ident().map(fold_ident).ok_or_else(loud)
            } else {
                Err(loud())
            }
        }
        _ => Err(loud()),
    }
}

/// Serialize the catalog to the line-oriented text format the query
/// proc-macro parses. One column per line:
///
/// ```text
/// <table>\t<column>\t<pg_type>\t<0|1 not_null>\t<0|1 primary_key>
/// ```
///
/// Sorted (via `BTreeMap`) so output is byte-deterministic. The format
/// is parsed with `str::lines` + `split('\t')` — no deserialization
/// dependency, fully greppable, and stable across builds. The trailing
/// primary-key field reconstructs each table's key SET as the columns
/// whose flag is `1`; a reader that needs only table/column existence
/// ignores it.
fn serialize(catalog: &Catalog) -> String {
    let mut out = String::new();
    for (table, columns) in &catalog.tables {
        let pk = catalog.primary_keys.get(table);
        for (column, info) in columns {
            let is_pk = match pk {
                Some(set) => set.contains(column),
                None => false,
            };
            out.push_str(table);
            out.push('\t');
            out.push_str(column);
            out.push('\t');
            out.push_str(&info.pg_type);
            out.push('\t');
            out.push(if info.not_null { '1' } else { '0' });
            // A fifth field marks whether the column is part of the table's
            // PRIMARY KEY (`1`) or not (`0`). The reading proc-macro splits on
            // tabs and consumes only the leading table/column fields, so this
            // trailing field is ignored by it while still carrying the key for
            // a reader that reconstructs the primary-key SET from the `1`
            // columns of each table.
            out.push('\t');
            out.push(if is_pk { '1' } else { '0' });
            out.push('\n');
        }
    }
    out
}

/// A failure parsing the line-oriented catalog text back into a
/// [`Catalog`]. The catalog is machine-generated by [`serialize`], so a
/// malformed line means the channel between the build script and the
/// query proc-macro is corrupt — it is a loud error, never a silently
/// dropped line (a dropped line would hide a table or column and reopen
/// the stale-schema blind spot this whole design closes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogParseError {
    /// A non-empty line did not split into the expected number of
    /// tab-separated fields. `line` is the 1-based line number;
    /// `fields` is how many were found.
    FieldCount { line: usize, fields: usize },
    /// A boolean flag field (`not_null` or `primary_key`) held something
    /// other than `0` or `1`. `line` is the 1-based line number; `field`
    /// names which flag; `value` is the offending text.
    BoolFlag {
        line: usize,
        field: &'static str,
        value: String,
    },
}

impl fmt::Display for CatalogParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogParseError::FieldCount { line, fields } => write!(
                f,
                "schema catalog line {line} has {fields} tab-separated field(s), \
                 expected exactly 5 (table, column, pg_type, not_null, primary_key). \
                 The catalog is machine-generated; a malformed line means the \
                 build-script channel is corrupt."
            ),
            CatalogParseError::BoolFlag { line, field, value } => write!(
                f,
                "schema catalog line {line} has a `{field}` flag of `{value}`, \
                 expected `0` or `1`. The catalog is machine-generated; a malformed \
                 flag means the build-script channel is corrupt."
            ),
        }
    }
}

impl std::error::Error for CatalogParseError {}

/// Reconstruct a [`Catalog`] from the line-oriented text [`serialize`]
/// produced. This is the inverse of [`serialize`]: the query proc-macro
/// reads the catalog file the build script wrote and rebuilds the
/// in-memory [`Catalog`] so it can call [`infer_query`].
///
/// Each non-empty line is `table\tcolumn\tpg_type\t<0|1 not_null>\t<0|1
/// primary_key>`. A blank line (a trailing newline yields one) is the
/// only line that is skipped; every other line MUST be well-formed —
/// a wrong field count or a non-`0|1` flag is a loud
/// [`CatalogParseError`], never a silent skip (which would hide a table
/// or column).
///
/// # Errors
///
/// [`CatalogParseError`] when a non-empty line does not have exactly five
/// tab-separated fields, or a flag field is not `0`/`1`.
pub fn parse_catalog(text: &str) -> Result<Catalog, CatalogParseError> {
    let mut catalog = Catalog::default();
    for (idx, line) in text.lines().enumerate() {
        // A trailing newline produces one empty final line; it carries no
        // table/column and is the ONLY skippable form. `line` numbers are
        // 1-based in diagnostics.
        if line.is_empty() {
            continue;
        }
        let number = idx.saturating_add(1);
        let fields: Vec<&str> = line.split('\t').collect();
        // Bind the five fields by slice pattern (no indexing operation).
        // Any other arity is a loud, classified error — never a silent
        // skip that would hide a table or column.
        let [table, column, pg_type, not_null_flag, pk_flag] = match fields.as_slice() {
            [a, b, c, d, e] => [*a, *b, *c, *d, *e],
            other => {
                return Err(CatalogParseError::FieldCount {
                    line: number,
                    fields: other.len(),
                })
            }
        };
        let not_null = parse_flag(not_null_flag, number, "not_null")?;
        let is_pk = parse_flag(pk_flag, number, "primary_key")?;
        catalog
            .tables
            .entry(table.to_string())
            .or_default()
            .insert(
                column.to_string(),
                ColumnInfo {
                    pg_type: pg_type.to_string(),
                    not_null,
                },
            );
        if is_pk {
            catalog
                .primary_keys
                .entry(table.to_string())
                .or_default()
                .insert(column.to_string());
        }
    }
    Ok(catalog)
}

/// Parse a single `0`/`1` flag field, loud on anything else.
fn parse_flag(value: &str, line: usize, field: &'static str) -> Result<bool, CatalogParseError> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        other => Err(CatalogParseError::BoolFlag {
            line,
            field,
            value: other.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_from(sqls: &[&str]) -> Catalog {
        let mut cat = Catalog::default();
        for (i, sql) in sqls.iter().enumerate() {
            let path = PathBuf::from(format!("test_{i}.sql"));
            replay_file(&mut cat, &path, sql).expect("replay");
        }
        cat
    }

    /// Replay a sequence and return the `Replay` error message of the
    /// first statement that fails closed (panicking if none does).
    fn replay_err(sqls: &[&str]) -> String {
        let mut cat = Catalog::default();
        for (i, sql) in sqls.iter().enumerate() {
            let path = PathBuf::from(format!("test_{i}.sql"));
            if let Err(err) = replay_file(&mut cat, &path, sql) {
                match err {
                    BuildError::Replay { message, .. } => return message,
                    other => panic!("expected a Replay error, got: {other:?}"),
                }
            }
        }
        panic!("expected a fail-closed Replay error, but every statement replayed");
    }

    #[test]
    fn create_table_records_columns_and_nullability() {
        let cat = catalog_from(&[
            "CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, bio TEXT)",
        ]);
        let users = cat.tables.get("users").expect("users table");
        assert_eq!(users.get("id").expect("id").pg_type, "int8");
        assert!(users.get("id").expect("id").not_null, "PK implies NOT NULL");
        assert!(users.get("name").expect("name").not_null);
        assert!(!users.get("bio").expect("bio").not_null);
    }

    #[test]
    fn alter_add_drop_column() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t ADD COLUMN b TEXT NOT NULL",
            "ALTER TABLE t DROP COLUMN a",
        ]);
        let t = cat.tables.get("t").expect("t");
        assert!(!t.contains_key("a"), "a was dropped");
        assert_eq!(t.get("b").expect("b").pg_type, "text");
        assert!(t.get("b").expect("b").not_null);
    }

    #[test]
    fn alter_set_drop_not_null() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT NOT NULL)",
            "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
        ]);
        assert!(!cat.tables["t"]["a"].not_null);

        let cat2 = catalog_from(&[
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t ALTER COLUMN a SET NOT NULL",
        ]);
        assert!(cat2.tables["t"]["a"].not_null);
    }

    #[test]
    fn alter_set_type_and_rename() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t ALTER COLUMN a TYPE BIGINT",
            "ALTER TABLE t RENAME COLUMN a TO b",
        ]);
        assert!(!cat.tables["t"].contains_key("a"));
        assert_eq!(cat.tables["t"]["b"].pg_type, "int8");
    }

    #[test]
    fn drop_table_removes_it() {
        let cat = catalog_from(&["CREATE TABLE t (a INT)", "DROP TABLE t"]);
        assert!(!cat.tables.contains_key("t"));
    }

    // ── PRIMARY KEY tracking ───────────────────────────────────────────

    fn pk(cat: &Catalog, table: &str) -> Vec<String> {
        match cat.primary_keys.get(table) {
            Some(set) => set.iter().cloned().collect(),
            None => Vec::new(),
        }
    }

    #[test]
    fn column_level_primary_key_is_recorded() {
        let cat = catalog_from(&["CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)"]);
        assert_eq!(pk(&cat, "t"), vec!["id".to_string()]);
        // PK implies NOT NULL.
        assert!(cat.tables["t"]["id"].not_null);
    }

    #[test]
    fn table_level_composite_primary_key_is_recorded() {
        let cat = catalog_from(&["CREATE TABLE t (a INT, b INT, c TEXT, PRIMARY KEY (a, b))"]);
        assert_eq!(pk(&cat, "t"), vec!["a".to_string(), "b".to_string()]);
        assert!(cat.tables["t"]["a"].not_null);
        assert!(cat.tables["t"]["b"].not_null);
        assert!(!cat.tables["t"]["c"].not_null);
    }

    #[test]
    fn no_primary_key_has_no_record() {
        let cat = catalog_from(&["CREATE TABLE t (a INT, b INT)"]);
        assert!(!cat.primary_keys.contains_key("t"));
    }

    #[test]
    fn multiple_primary_keys_is_loud() {
        // Two column-level PKs.
        let msg = replay_err(&["CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)"]);
        assert!(msg.contains("more than one PRIMARY KEY"), "got: {msg}");
        // Column-level PK plus a table-level PK.
        let msg = replay_err(&["CREATE TABLE t (a INT PRIMARY KEY, b INT, PRIMARY KEY (b))"]);
        assert!(msg.contains("more than one PRIMARY KEY"), "got: {msg}");
    }

    #[test]
    fn alter_add_primary_key_is_recorded() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT NOT NULL, b INT)",
            "ALTER TABLE t ADD PRIMARY KEY (a)",
        ]);
        assert_eq!(pk(&cat, "t"), vec!["a".to_string()]);
    }

    #[test]
    fn alter_add_second_primary_key_is_loud() {
        let msg = replay_err(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT NOT NULL)",
            "ALTER TABLE t ADD PRIMARY KEY (b)",
        ]);
        assert!(msg.contains("a primary key already exists"), "got: {msg}");
    }

    #[test]
    fn dropping_a_primary_key_column_drops_the_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT)",
            "ALTER TABLE t DROP COLUMN a",
        ]);
        assert!(!cat.primary_keys.contains_key("t"), "key removed with col");
    }

    #[test]
    fn dropping_one_composite_key_column_drops_the_whole_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT, b INT, c TEXT, PRIMARY KEY (a, b))",
            "ALTER TABLE t DROP COLUMN a",
        ]);
        assert!(!cat.primary_keys.contains_key("t"), "whole key removed");
    }

    #[test]
    fn renaming_a_primary_key_column_follows_the_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT)",
            "ALTER TABLE t RENAME COLUMN a TO aa",
        ]);
        assert_eq!(pk(&cat, "t"), vec!["aa".to_string()]);
    }

    #[test]
    fn renaming_a_table_moves_its_primary_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT)",
            "ALTER TABLE t RENAME TO u",
        ]);
        assert!(!cat.primary_keys.contains_key("t"));
        assert_eq!(pk(&cat, "u"), vec!["a".to_string()]);
    }

    #[test]
    fn dropping_a_table_removes_its_primary_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT)",
            "DROP TABLE t",
        ]);
        assert!(!cat.primary_keys.contains_key("t"));
    }

    #[test]
    fn recreating_a_dropped_table_does_not_inherit_a_stale_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT)",
            "DROP TABLE t",
            "CREATE TABLE t (a INT, b INT)",
        ]);
        assert!(!cat.primary_keys.contains_key("t"), "no stale key");
    }

    #[test]
    fn alter_unknown_table_is_error() {
        let mut cat = Catalog::default();
        let err = replay_file(
            &mut cat,
            Path::new("x.sql"),
            "ALTER TABLE nope ADD COLUMN c INT",
        )
        .expect_err("must fail closed");
        match err {
            BuildError::Replay { message, .. } => assert!(message.contains("unknown table")),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn drop_unknown_column_is_error() {
        let mut cat = Catalog::default();
        replay_file(&mut cat, Path::new("a.sql"), "CREATE TABLE t (a INT)").expect("create");
        let err = replay_file(&mut cat, Path::new("b.sql"), "ALTER TABLE t DROP COLUMN gone")
            .expect_err("must fail closed");
        match err {
            BuildError::Replay { message, .. } => assert!(message.contains("no such column")),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn parse_error_is_fatal() {
        let mut cat = Catalog::default();
        let err = replay_file(&mut cat, Path::new("bad.sql"), "CREATE TABLE (((")
            .expect_err("must fail closed");
        assert!(matches!(err, BuildError::Parse { .. }));
    }

    #[test]
    fn serialize_is_deterministic_and_tab_separated() {
        let cat = catalog_from(&["CREATE TABLE t (b INT, a TEXT NOT NULL)"]);
        let s = serialize(&cat);
        // Columns sorted: a before b. No primary key, so the trailing
        // PK field is `0` on every row.
        assert_eq!(s, "t\ta\ttext\t1\t0\nt\tb\tint4\t0\t0\n");
    }

    #[test]
    fn serialize_marks_primary_key_columns() {
        // A column-level PK and a composite table-level PK both serialize
        // their key columns with the trailing PK field set to `1`, and
        // non-key columns to `0`.
        let cat = catalog_from(&[
            "CREATE TABLE one (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE two (a INT, b INT, c TEXT, PRIMARY KEY (a, b))",
        ]);
        let s = serialize(&cat);
        // `one`: id is the PK (1), name is not (0).
        assert!(s.contains("one\tid\tint8\t1\t1\n"));
        assert!(s.contains("one\tname\ttext\t1\t0\n"));
        // `two`: a and b are the composite PK (1), c is not (0).
        assert!(s.contains("two\ta\tint4\t1\t1\n"));
        assert!(s.contains("two\tb\tint4\t1\t1\n"));
        assert!(s.contains("two\tc\ttext\t0\t0\n"));
    }

    #[test]
    fn rename_table_rekeys_catalog() {
        // `ALTER TABLE ... RENAME TO ...` moves the column map to the new
        // name; the old name no longer resolves.
        let cat = catalog_from(&[
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)",
            "ALTER TABLE users RENAME TO members",
        ]);
        assert!(!cat.tables.contains_key("users"), "old name removed");
        let members = cat.tables.get("members").expect("renamed table");
        assert_eq!(members.get("id").expect("id").pg_type, "int8");
        assert!(members.get("email").expect("email").not_null);
    }

    #[test]
    fn rename_unknown_table_is_error() {
        let msg = replay_err(&["ALTER TABLE ghost RENAME TO spirit"]);
        assert!(msg.contains("unknown table"), "got: {msg}");
    }

    #[test]
    fn create_table_public_qualified_keys_to_bare_name() {
        // `public.X` and `"public".X` both name the default schema PostgreSQL
        // resolves a bare reference to; the catalog keys them to bare `X`,
        // symmetric with the query-side resolver.
        for ddl in [
            "CREATE TABLE public.widgets (wid INT)",
            "CREATE TABLE PUBLIC.widgets (wid INT)",
            "CREATE TABLE \"public\".widgets (wid INT)",
        ] {
            let cat = catalog_from(&[ddl]);
            assert!(cat.tables.contains_key("widgets"), "keyed to bare name: {ddl}");
        }
    }

    #[test]
    fn create_table_nonpublic_schema_is_loud_and_names_path() {
        // A non-`public` schema is a namespace the single-namespace catalog
        // cannot model: silently re-keying `wrongschema.widgets` to bare
        // `widgets` would catalog a table a bare query would wrongly resolve.
        // It is a loud Replay error naming the full path, never silent.
        let msg = replay_err(&["CREATE TABLE wrongschema.widgets (wid INT)"]);
        assert!(
            msg.contains("wrongschema") && msg.contains("does not model"),
            "must name the path loudly: {msg}"
        );
    }

    #[test]
    fn create_table_quoted_uppercase_public_schema_is_loud() {
        // A quoted `"PUBLIC"` keeps its upper case, so it is a distinct schema
        // (not the default `public`); it is loud.
        let msg = replay_err(&["CREATE TABLE \"PUBLIC\".widgets (wid INT)"]);
        assert!(msg.contains("does not model"), "got: {msg}");
    }

    #[test]
    fn create_table_three_part_path_is_loud() {
        // A 3-part `db.schema.table` path names a database dimension the
        // catalog does not model; it is loud.
        let msg = replay_err(&["CREATE TABLE mydb.public.widgets (wid INT)"]);
        assert!(msg.contains("does not model"), "got: {msg}");
    }

    #[test]
    fn alter_nonpublic_schema_table_is_loud() {
        // The same loud-on-non-public rule applies to ALTER TABLE's relation
        // key, so an ALTER cannot silently target a re-keyed bare table.
        let msg = replay_err(&[
            "CREATE TABLE widgets (wid INT)",
            "ALTER TABLE wrongschema.widgets ADD COLUMN extra INT",
        ]);
        assert!(msg.contains("does not model"), "got: {msg}");
    }

    #[test]
    fn drop_nonpublic_schema_table_is_loud() {
        // DROP TABLE's relation key is loud on a non-public schema too.
        let msg = replay_err(&[
            "CREATE TABLE widgets (wid INT)",
            "DROP TABLE wrongschema.widgets",
        ]);
        assert!(msg.contains("does not model"), "got: {msg}");
    }

    #[test]
    fn rename_to_nonpublic_schema_is_loud() {
        // The MySQL-style `RENAME TABLE old TO new` and the PostgreSQL
        // `ALTER TABLE old RENAME TO new` target are both loud on a non-public
        // schema, so a rename cannot move a table into an unmodeled namespace.
        let msg = replay_err(&[
            "CREATE TABLE widgets (wid INT)",
            "RENAME TABLE widgets TO wrongschema.gadgets",
        ]);
        assert!(msg.contains("does not model"), "got: {msg}");
    }

    #[test]
    fn unquoted_identifiers_fold_to_lowercase() {
        // PostgreSQL folds unquoted identifiers to lowercase. `Accounts` /
        // `UserId` are catalogued lowercased and resolve as such.
        let cat = catalog_from(&["CREATE TABLE Accounts (UserId INT, BALANCE BIGINT)"]);
        let accounts = cat.tables.get("accounts").expect("folded table name");
        assert!(accounts.contains_key("userid"), "folded column name");
        assert!(accounts.contains_key("balance"));
        assert!(!cat.tables.contains_key("Accounts"), "no mixed-case key");
    }

    #[test]
    fn quoted_identifiers_preserve_case() {
        // A double-quoted identifier keeps its exact case.
        let cat = catalog_from(&["CREATE TABLE \"Mixed\" (\"UserId\" INT)"]);
        let mixed = cat.tables.get("Mixed").expect("quoted name preserved");
        assert!(mixed.contains_key("UserId"), "quoted column preserved");
        assert!(!cat.tables.contains_key("mixed"), "not folded");
    }

    #[test]
    fn folded_alter_matches_folded_create() {
        // An unquoted ALTER references the same folded column the unquoted
        // CREATE catalogued.
        let cat = catalog_from(&[
            "CREATE TABLE Accounts (UserId INT)",
            "ALTER TABLE Accounts ALTER COLUMN UserId SET NOT NULL",
        ]);
        assert!(cat.tables["accounts"]["userid"].not_null);
    }

    #[test]
    fn duplicate_create_table_without_if_not_exists_is_error() {
        let msg = replay_err(&[
            "CREATE TABLE t (a INT)",
            "CREATE TABLE t (b INT)",
        ]);
        assert!(msg.contains("already exists"), "got: {msg}");
    }

    #[test]
    fn duplicate_create_table_if_not_exists_is_noop() {
        // The first definition wins; `IF NOT EXISTS` against an existing
        // table is a documented no-op, not a column merge.
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT)",
            "CREATE TABLE IF NOT EXISTS t (b INT)",
        ]);
        let t = cat.tables.get("t").expect("t");
        assert!(t.contains_key("a"));
        assert!(!t.contains_key("b"), "no column merge");
    }

    #[test]
    fn create_table_as_select_is_error() {
        let msg = replay_err(&["CREATE TABLE snapshot AS SELECT * FROM users"]);
        assert!(msg.contains("AS SELECT"), "got: {msg}");
    }

    #[test]
    fn create_table_like_is_error() {
        let msg = replay_err(&[
            "CREATE TABLE base (a INT)",
            "CREATE TABLE copy (LIKE base)",
        ]);
        assert!(msg.contains("LIKE"), "got: {msg}");
    }

    #[test]
    fn create_table_partition_of_is_error() {
        let msg =
            replay_err(&["CREATE TABLE child PARTITION OF parent FOR VALUES IN (1)"]);
        assert!(msg.contains("PARTITION OF"), "got: {msg}");
    }

    #[test]
    fn create_table_inherits_is_error() {
        let msg = replay_err(&["CREATE TABLE child () INHERITS (parent)"]);
        assert!(msg.contains("INHERITS"), "got: {msg}");
    }

    #[test]
    fn change_column_is_error() {
        let msg = replay_err(&[
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t CHANGE COLUMN a b BIGINT",
        ]);
        assert!(msg.contains("CHANGE COLUMN"), "got: {msg}");
    }

    #[test]
    fn modify_column_is_error() {
        let msg = replay_err(&[
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t MODIFY COLUMN a BIGINT",
        ]);
        assert!(msg.contains("MODIFY COLUMN"), "got: {msg}");
    }

    #[test]
    fn drop_primary_key_is_error() {
        let msg = replay_err(&[
            "CREATE TABLE t (a INT PRIMARY KEY)",
            "ALTER TABLE t DROP PRIMARY KEY",
        ]);
        assert!(msg.contains("DROP PRIMARY KEY"), "got: {msg}");
    }

    #[test]
    fn shape_irrelevant_alter_ops_pass() {
        // A representative allowlisted op leaves the catalog unchanged and
        // does not fail closed.
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t ADD CONSTRAINT t_uq UNIQUE (a)",
            "CREATE INDEX t_a_idx ON t (a)",
        ]);
        let t = cat.tables.get("t").expect("t");
        assert!(t.contains_key("a"));
        assert!(!t["a"].not_null, "UNIQUE does not imply NOT NULL");
    }

    /// A unique scratch directory under the system temp dir, removed on
    /// drop, used by the walk/recurse tests below.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let mut path = std::env::temp_dir();
            let pid = std::process::id();
            let nanos = match std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
            {
                Ok(d) => d.as_nanos(),
                Err(e) => panic!("system clock before unix epoch: {e}"),
            };
            path.push(format!("bsql_build_test_{tag}_{pid}_{nanos}"));
            std::fs::create_dir_all(&path).expect("create temp dir");
            TempDir { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn scan_recurses_and_records_every_directory() {
        let tmp = TempDir::new("scan");
        let root = &tmp.path;
        let sub = root.join("nested");
        let deep = sub.join("deeper");
        std::fs::create_dir_all(&deep).expect("subdirs");
        std::fs::write(root.join("0001_top.sql"), "CREATE TABLE top (a int);").expect("w");
        std::fs::write(sub.join("0002_mid.sql"), "CREATE TABLE mid (a int);").expect("w");
        std::fs::write(deep.join("0003_deep.sql"), "CREATE TABLE deep (a int);").expect("w");
        // A non-sql file is ignored.
        std::fs::write(sub.join("notes.txt"), "ignore me").expect("w");

        let walk = scan_sql_tree(root).expect("scan");

        // Every directory (root + each nested one) is recorded, so a
        // `rerun-if-changed` can be emitted per level — membership of a
        // file added/removed in ANY of them is tracked.
        assert!(walk.dirs.contains(root), "root dir tracked");
        assert!(walk.dirs.contains(&sub), "nested dir tracked");
        assert!(walk.dirs.contains(&deep), "deeper dir tracked");

        // Every `*.sql` at any depth is collected, in sorted path order;
        // the `.txt` is not.
        assert_eq!(walk.files.len(), 3, "three sql files, the txt ignored");
        assert!(walk.files.windows(2).all(|w| w[0] <= w[1]), "sorted");
    }

    // ── catalog text round-trip (serialize -> parse_catalog) ───────────

    #[test]
    fn parse_catalog_round_trips_serialize() {
        let cat = catalog_from(&[
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, bio TEXT)",
            "CREATE TABLE pair (a INT, b INT, c TEXT, PRIMARY KEY (a, b))",
        ]);
        let text = serialize(&cat);
        let parsed = parse_catalog(&text).expect("round-trip parse");
        // The reconstructed catalog equals the original: same tables,
        // columns, types, nullability, and primary keys.
        assert_eq!(parsed, cat);
    }

    #[test]
    fn parse_catalog_empty_is_empty() {
        let parsed = parse_catalog("").expect("empty parses");
        assert!(parsed.tables.is_empty());
        assert!(parsed.primary_keys.is_empty());
    }

    #[test]
    fn parse_catalog_wrong_field_count_is_loud() {
        // Three fields where five are required — never a silent skip.
        let err = parse_catalog("t\tcol\tint4\n").expect_err("must fail closed");
        assert!(matches!(err, CatalogParseError::FieldCount { fields: 3, .. }));
    }

    #[test]
    fn parse_catalog_bad_flag_is_loud() {
        let err = parse_catalog("t\tcol\tint4\tyes\t0\n").expect_err("must fail closed");
        assert!(matches!(
            err,
            CatalogParseError::BoolFlag { field: "not_null", .. }
        ));
    }
}
