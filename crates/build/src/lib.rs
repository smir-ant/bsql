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
//!
//! # Destructive-migration acknowledgement
//!
//! A migration that irreversibly destroys data — a `DROP TABLE` (drops every
//! row) or an `ALTER TABLE ... DROP COLUMN` (drops a column's data) — must be
//! ACKNOWLEDGED in the migration file: a `-- bsql:ack-destructive` comment on
//! the line(s) immediately preceding the statement. An unacknowledged
//! destructive statement is a loud [`BuildError::UnackedDestructiveMigration`]
//! that fails the build, catching an ACCIDENTAL data-loss migration at compile
//! time instead of in production. The set is deliberately conservative (only
//! unambiguous destruction): a `RENAME`, an `ADD COLUMN`, or a `DROP NOT NULL`
//! preserves data and needs no acknowledgement, so a developer is never trained
//! to blanket-acknowledge safe DDL. The acknowledgement is parsed with the SQL
//! tokenizer, so the marker text inside a string literal cannot forge one, and
//! the marker must genuinely precede the destructive statement. The
//! acknowledgement per statement is the only override — there is no wholesale
//! opt-out that could silently pre-accept a future accidental destruction.

#![forbid(unsafe_code)]

mod dynamics;
mod infer;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use dynamics::{
    infer_dynamic_query, sqlite_placeholder_form, DynamicError, DynamicShape, OrderByVariant,
    ParamShape, WireVariant,
};
pub use infer::{
    infer_query, scalar_rust_type_for_pg, ElemType, InferError, InferredColumn, QueryShape,
    RustType, UserCompositeId, UserEnumId,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{
    emit_sqlite_template, verify_sqlite_conformance, SqliteConformanceError,
    SQLITE_TARGET_ENV_VAR, SQLITE_TEMPLATE_ENV_VAR, SQLITE_TEMPLATE_FILE_NAME,
};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, AlterType, AlterTypeAddValue,
    AlterTypeAddValuePosition, AlterTypeOperation, AlterTypeRename, AlterTypeRenameValue, ColumnDef,
    ColumnOption, CreateDomain, DropDomain, Expr, Ident, IndexColumn, ObjectName, ObjectType,
    RenameTableNameKind, Statement, TableConstraint, UserDefinedTypeRepresentation,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

/// The basename of the catalog file written into `OUT_DIR`.
pub const CATALOG_FILE_NAME: &str = "bsql_schema_catalog.txt";

/// The environment variable, set via `cargo:rustc-env`, that carries the
/// absolute path of the generated catalog to the query proc-macro.
pub const CATALOG_ENV_VAR: &str = "BSQL_SCHEMA_CATALOG";

/// The basename of the external-type bridge file written into `OUT_DIR` by
/// [`CatalogBuilder::emit`].
pub const BRIDGES_FILE_NAME: &str = "bsql_type_bridges.txt";

/// The environment variable, set via `cargo:rustc-env` by
/// [`CatalogBuilder::emit`], that carries the absolute path of the generated
/// external-type bridge file to the query proc-macro. Absent when a consumer
/// uses the plain [`emit`] / [`emit_catalog`] free functions (no bridges), in
/// which case `query!` decodes into the dep-free native types.
pub const BRIDGES_ENV_VAR: &str = "BSQL_TYPE_BRIDGES";

/// The basename of the embedded-migrations source file written into `OUT_DIR`
/// by [`emit_migrations`].
pub const EMBEDDED_MIGRATIONS_FILE_NAME: &str = "bsql_embedded_migrations.rs";

/// The environment variable, set via `cargo:rustc-env` by [`emit_migrations`],
/// that carries the absolute path of the generated embedded-migrations source
/// file. The `bsql::embed_migrations!()` macro `include!`s the path this
/// variable names, so the migration name + SQL set is baked into the runtime
/// binary with no filesystem dependency at run time.
pub const EMBEDDED_MIGRATIONS_ENV_VAR: &str = "BSQL_EMBEDDED_MIGRATIONS";

/// The basename of the user-defined-types file written into `OUT_DIR` alongside
/// the catalog. Carries every `CREATE TYPE ... AS ENUM` (and, later, `DOMAIN` /
/// composite) declared in the migrations, so the query proc-macro can generate
/// a Rust type per user type and decode the columns that use them.
pub const USER_TYPES_FILE_NAME: &str = "bsql_user_types.txt";

/// The environment variable, set via `cargo:rustc-env`, that carries the
/// absolute path of the generated user-defined-types file to the query
/// proc-macro. Always emitted by every catalog-writing path (its file is empty
/// when the migrations declare no user types), so the macro reads a definite
/// channel rather than inferring absence from a missing variable.
pub const USER_TYPES_ENV_VAR: &str = "BSQL_USER_TYPES";

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

/// A user-defined PostgreSQL type declared by a migration `CREATE TYPE` /
/// `CREATE DOMAIN`. The query proc-macro turns each into a generated Rust type
/// (an `enum` for [`UserType::Enum`]) and decodes the columns that use it.
///
/// The type's OID is DELIBERATELY absent: a user type's OID is server-assigned
/// and dynamic (not in the fixed catalog set), so it cannot be pinned at build
/// time. The build-time guarantee is over the type's NAME and its variant/base
/// SET (exactly what the migration declares) — the wire form of every modeled
/// user type is self-describing enough to decode without the OID (a PG enum is
/// sent as its label text; a domain is transparent over its base), so no
/// dynamic OID is needed on the decode path.
/// One attribute of a user-defined COMPOSITE (row) type: its name and the
/// canonical PostgreSQL type it carries.
///
/// A composite attribute is ALWAYS nullable on the wire — PostgreSQL forbids a
/// `NOT NULL` constraint on a `CREATE TYPE name AS (...)` attribute (it is a
/// syntax error), and the row-type binary frame carries a per-field length that
/// may be `-1` (NULL) for any field. So the generated Rust struct wraps every
/// field in `Option<T>`; there is no per-field nullability to store here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeField {
    /// The attribute name, case-folded the same way a column name is (so a
    /// `query!` and the generated struct agree on the field's spelling).
    pub name: String,
    /// The canonical PostgreSQL type name the attribute carries (already
    /// alias-collapsed by `canonical_type`, e.g. `bigint` -> `int8`). It may name
    /// another user type (an enum, a domain, or a nested composite), resolved
    /// through the same chain a column type is at the query site.
    pub pg_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserType {
    /// `CREATE TYPE name AS ENUM ('a', 'b', ...)` — an ordered set of string
    /// labels. Order is the declaration order (PostgreSQL's enum sort order);
    /// the generated Rust `enum` mirrors it. On the wire a PG enum value is its
    /// label text, so decode is a label-string match and encode is the label
    /// text as an `unknown`-typed (OID 0) bind parameter the server coerces.
    Enum {
        /// The labels in declaration order (PostgreSQL enum ordinal order).
        labels: Vec<String>,
    },
    /// `CREATE DOMAIN name AS base [CHECK (...)]` — a constrained alias for a
    /// base type. A domain is TRANSPARENT on the wire: it sends and receives
    /// exactly its base type's bytes (the `CHECK` is SERVER-enforced, never a
    /// client concern), so a `query!` column typed as a domain decodes as its
    /// base's Rust type (`age AS int` -> `i32`). `base` is the canonical base
    /// type name; it may itself name another domain (a domain over a domain, or
    /// a domain over a user enum), resolved transitively at the query site.
    Domain {
        /// The canonical base type name the domain aliases.
        base: String,
    },
    /// `CREATE TYPE name AS (field type, ...)` — a COMPOSITE (row) type: an
    /// ordered list of named, typed attributes. On the wire a composite value is
    /// its row-type binary frame (an `int32` field count, then per field a
    /// `{uint32 type_oid, int32 len (-1 = NULL), byte[len] value}` triple), so the
    /// generated Rust `struct` decodes it by recursing into each field's own
    /// decoder. Field order is the declared order (the wire frame's field order);
    /// every field is nullable (see [`CompositeField`]).
    Composite {
        /// The attributes in declared (wire) order.
        fields: Vec<CompositeField>,
    },
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
    /// User-defined types declared by `CREATE TYPE` / `CREATE DOMAIN`
    /// migrations, keyed by the canonical (case-folded) type name — the same
    /// spelling a column's `pg_type` carries when it references the type. The
    /// query proc-macro reads this to generate a Rust type per entry and to
    /// resolve a column typed as one. `BTreeMap` keeps the serialized form
    /// byte-deterministic.
    pub user_types: BTreeMap<String, UserType>,
}

/// A build-time failure. Every variant is fatal: the consumer's
/// `build.rs` propagates it and the build fails (fail-closed).
///
/// Its [`fmt::Debug`] renders the same actionable message as its
/// [`fmt::Display`]. This is deliberate: a consumer's `build.rs` is
/// `fn main() -> Result<(), BuildError>`, and Rust's `Termination` for a
/// `Result` prints the error with `Debug`, so delegating `Debug` to `Display`
/// is what surfaces the full, human-readable guidance (which statement, which
/// file and line, and exactly how to fix it) at the point a build actually
/// fails — rather than a bare struct dump that omits the remedy.
pub enum BuildError {
    /// The migrations directory is missing or could not be listed.
    MigrationsDir { path: PathBuf, source: std::io::Error },
    /// A migration file could not be read.
    ReadFile { path: PathBuf, source: std::io::Error },
    /// A directory entry named `*.sql` is NOT a regular file (nor a symlink to
    /// one) — it is a FIFO / socket / block or character device / named pipe.
    /// Reading such an entry as a migration would block the build forever (a
    /// writer-less FIFO) or read without bound (a `/dev/zero`-class device), so
    /// it is rejected LOUDLY, naming the path, rather than admitted as a leaf. A
    /// legitimate symlink to a real `.sql` file IS followed and admitted; only a
    /// non-regular target is an error.
    NonRegularFile { path: PathBuf },
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
    /// A [`CatalogBuilder::bridge`] named a `pg_type` with no native pivot: it
    /// is not a canonical PostgreSQL type name in the natively-supported set
    /// (a typo like `timestamptzz`, or a natively-unsupported type like
    /// `inet` that a column would itself reject). A bridge reshapes a native
    /// decoded value, so a type with no native decoder cannot be bridged.
    /// Loud, never silently ignored. `pg_type` is the offending key.
    UnknownBridgeType { pg_type: String },
    /// Two [`CatalogBuilder::bridge`] registrations collide: either the same
    /// `pg_type` twice, or two distinct canonical types (e.g. `text` and
    /// `varchar`) that resolve to the SAME native pivot type — which bsql
    /// decodes identically (same wire OID, same decoder), so they cannot be
    /// bridged to different targets. Register one bridge for the family. Loud,
    /// never a silent last-wins. `first` / `second` are the two colliding keys.
    ConflictingBridge { first: String, second: String },
    /// A migration statement that irreversibly destroys data (a `DROP TABLE`
    /// or an `ALTER TABLE ... DROP COLUMN`) was replayed without an explicit
    /// acknowledgement. Data-destroying DDL must be acknowledged in the
    /// migration itself — a `-- bsql:ack-destructive` comment on the line(s)
    /// immediately preceding the statement — so an ACCIDENTAL destructive
    /// migration fails the build instead of silently shipping.
    ///
    /// `file` is the migration; `line` is the 1-based line of the statement's
    /// first token; `statement` is a short description of what it destroys.
    /// The [`fmt::Display`] spells out the exact acknowledgement syntax. Loud,
    /// never a warning — a warning scrolls past and reopens the accidental-loss
    /// blind spot this gate closes.
    UnackedDestructiveMigration {
        /// The migration file containing the unacknowledged statement.
        file: PathBuf,
        /// A short description of the destructive statement (e.g.
        /// `DROP TABLE users` or `ALTER TABLE orders DROP COLUMN total`).
        statement: String,
        /// The 1-based source line of the statement's first token.
        line: u64,
    },
    /// A migration file (baked by [`emit_migrations`]) contains a top-level
    /// transaction-control statement (`BEGIN` / `START TRANSACTION` / `COMMIT` /
    /// `ROLLBACK` / `SAVEPOINT` / `RELEASE SAVEPOINT`). The RUNNER owns the
    /// transaction boundary — it wraps each migration in its own transaction — so
    /// a migration must not manage its own: an embedded `COMMIT` would leak the
    /// preceding DDL before a later statement in the same file fails, breaking
    /// atomicity and wedging a re-run. This is a BUILD error, never a runtime
    /// atomicity break. (A `-- bsql:no-transaction` migration is NOT exempt: it
    /// runs as one or more AUTOCOMMIT statements, so a `COMMIT`/`BEGIN` in it is
    /// equally meaningless and rejected.)
    TransactionControlInMigration {
        /// The migration file containing the transaction-control statement.
        file: PathBuf,
        /// The offending statement keyword (`BEGIN`, `COMMIT`, …).
        statement: &'static str,
    },
}

impl fmt::Debug for BuildError {
    // Delegate to `Display`: a build script's `fn main() -> Result<(),
    // BuildError>` is printed by the `Termination` impl using `Debug`, so this
    // is what puts the actionable message (including how to fix it) in front of
    // the developer whose build failed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
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
            BuildError::NonRegularFile { path } => write!(
                f,
                "bsql-build: migration entry {} is not a regular file (it is a \
                 FIFO, socket, or device). Reading it as a migration would hang \
                 the build (a writer-less FIFO) or read without bound (a device), \
                 so it is rejected. A migration must be a regular `.sql` file (a \
                 symlink to one is followed); remove the special file or point it \
                 at real SQL.",
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
            BuildError::UnknownBridgeType { pg_type } => write!(
                f,
                "bsql-build: cannot bridge PostgreSQL type `{pg_type}`: it is \
                 not a canonical type name with a native bsql pivot. A bridge \
                 reshapes a natively-decoded value, so its `pg_type` must be \
                 one of the natively-supported canonical names (int2, int4, \
                 int8, oid, bool, text, varchar, float4, float8, bytea, uuid, \
                 timestamptz, timestamp, json, jsonb, numeric). A natively-\
                 unsupported type (e.g. inet) has no native decoder to bridge \
                 from."
            ),
            BuildError::ConflictingBridge { first, second } => write!(
                f,
                "bsql-build: conflicting bridges for `{first}` and `{second}`: \
                 they resolve to the same native pivot type, which bsql decodes \
                 identically (same wire OID, same decoder). Register a single \
                 bridge for the family rather than two that disagree."
            ),
            BuildError::UnackedDestructiveMigration {
                file,
                statement,
                line,
            } => write!(
                f,
                "bsql-build: unacknowledged destructive migration in {} at line \
                 {line}: `{statement}` irreversibly destroys data. If this is \
                 intentional, acknowledge it by placing the comment \
                 `{ACK_MARKER_SYNTAX}` on the line(s) immediately before the \
                 statement (optionally followed by a reason, e.g. \
                 `{ACK_MARKER_SYNTAX} dropped after export to cold storage`). \
                 The acknowledgement must directly precede THIS statement: one \
                 before another statement, or the marker text inside a string \
                 literal, does not count.",
                file.display()
            ),
            BuildError::TransactionControlInMigration { file, statement } => write!(
                f,
                "bsql-build: migration {} contains a top-level `{statement}` \
                 statement. The migration runner OWNS the transaction boundary \
                 (it wraps each migration in its own transaction), so a migration \
                 must not manage its own — an embedded `{statement}` would break \
                 atomicity. Remove the transaction-control statement; if the \
                 migration must run outside a transaction (e.g. `CREATE INDEX \
                 CONCURRENTLY`), mark it with a `-- bsql:no-transaction` comment \
                 line instead (it then runs as autocommit statements, still \
                 without any `BEGIN`/`COMMIT`).",
                file.display()
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
    // stale-schema blind spot this closes at every level. The per-file
    // directives (content tracking: EDIT recompiles) are the belt.
    emit_rerun_directives(&walk);

    let catalog = catalog_from_walk(&walk)?;
    write_catalog(&catalog)?;
    Ok(())
}

/// Emit the `cargo:rerun-if-changed` directives for a walked migrations tree:
/// one per directory (membership: ADD/DELETE of a migration at any depth
/// recompiles) and one per file (content: EDIT recompiles). Shared by
/// [`emit_catalog`] and [`CatalogBuilder::emit`].
fn emit_rerun_directives(walk: &Walk) {
    for directory in &walk.dirs {
        println!("cargo:rerun-if-changed={}", directory.display());
    }
    for file in &walk.files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
}

/// Serialize the catalog to `OUT_DIR/bsql_schema_catalog.txt` and set the
/// `BSQL_SCHEMA_CATALOG` rustc-env channel the query proc-macro reads. The
/// sole catalog-writing path, shared by [`emit_catalog`] and
/// [`CatalogBuilder::emit`].
fn write_catalog(catalog: &Catalog) -> Result<(), BuildError> {
    let out_dir = env_path("OUT_DIR")?;
    let catalog_path = out_dir.join(CATALOG_FILE_NAME);
    let serialized = serialize(catalog);
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

    // The user-defined types ride their OWN channel alongside the catalog
    // (the catalog line format is pinned at five fields; the user-types file
    // is a distinct file + rustc-env var, exactly like the bridge channel).
    // Emitting it from THIS single catalog-writing path means every emit entry
    // point — the free functions and the builder — always emits it, so the
    // macro's two channels can never fall out of sync.
    write_user_types(catalog)?;
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

/// Bake the consumer's migration set (each migration's stable NAME + its SQL)
/// into the runtime binary, for the runtime migration RUNNER
/// (`conn.run_migrations(..)`).
///
/// Call this from a consumer's `build.rs` (in addition to, or instead of,
/// [`emit`] — it is independent of the `query!` catalog):
///
/// ```no_run
/// fn main() -> Result<(), bsql_build::BuildError> {
///     bsql_build::emit("migrations")?;             // query! catalog (optional)
///     bsql_build::emit_migrations("migrations")     // the embedded runner set
/// }
/// ```
///
/// The consumer then reaches the baked set with the `bsql::embed_migrations!()`
/// macro, which expands to a `&'static [(&'static str, &'static str)]` of
/// `(name, sql)` pairs and hands it to `conn.run_migrations(..)` — no
/// filesystem access at run time.
///
/// It:
///
/// * walks the migrations tree ONCE using the SAME ordering authority as the
///   catalog replay ([`scan_sql_tree`]), so the embedded set replays in the
///   identical deterministic order (lexicographic by path);
/// * emits `cargo:rerun-if-changed` for the directory (membership) and each
///   file (content), so ADD / DELETE / EDIT of any migration re-bakes the set;
/// * parses each migration and re-runs the destructive-migration
///   acknowledgement gate ([`BuildError::UnackedDestructiveMigration`]) — the
///   SAME gate the catalog replay enforces — so an unacknowledged `DROP TABLE`
///   fails the build here too, never silently ships baked into a binary. (The
///   embed gate does NOT require the DDL to be catalog-*modelable* — the runner
///   applies raw SQL to a real database — only that it PARSES so the ack gate
///   can classify it. A migration form `sqlparser` cannot parse is a loud
///   [`BuildError::Parse`]; apply it via the runtime *directory* source, which
///   parses nothing, instead.)
/// * writes the generated source to `OUT_DIR` and sets the
///   [`EMBEDDED_MIGRATIONS_ENV_VAR`] rustc-env channel the macro `include!`s.
///
/// The SQL bytes ride `include_str!` of each migration's absolute path (not an
/// inlined string literal), so there is no escaping hazard and an EDIT to a
/// migration is picked up on recompile.
///
/// Returns `Err` (failing the build) on any I/O, parse, or unacknowledged
/// destructive-migration error.
pub fn emit_migrations(migrations_dir: impl AsRef<Path>) -> Result<(), BuildError> {
    let manifest = env_path("CARGO_MANIFEST_DIR")?;
    let dir = manifest.join(migrations_dir.as_ref());

    // ONE walk — the SAME ordering authority the catalog replay uses, so the
    // embedded set's order is identical to the build-validated catalog order.
    let walk = scan_sql_tree(&dir)?;

    // Membership + content tracking: ADD / DELETE (directory mtime) and EDIT
    // (per-file content) both re-bake the embedded set.
    emit_rerun_directives(&walk);

    let mut generated = String::from("&[\n");
    for file in &walk.files {
        let sql = std::fs::read_to_string(file).map_err(|source| BuildError::ReadFile {
            path: file.clone(),
            source,
        })?;

        // Re-run the destructive-acknowledgement gate on the SAME parsed
        // statements the catalog replay checks — the embed cannot bypass S42.
        parse_and_enforce_acks(file, &sql)?;

        let name = migration_name(&dir, file)?;
        let abs = file.to_str().ok_or_else(|| BuildError::Parse {
            path: file.clone(),
            message: "migration path is not valid UTF-8".to_owned(),
        })?;
        // `{:?}` renders a VALID Rust string literal (escapes quotes /
        // backslashes / control bytes) — no hand-rolled escaping.
        generated.push_str(&format!("    ({name:?}, include_str!({abs:?})),\n"));
    }
    generated.push_str("]\n");

    let out_dir = env_path("OUT_DIR")?;
    let path = out_dir.join(EMBEDDED_MIGRATIONS_FILE_NAME);
    std::fs::write(&path, generated).map_err(|source| BuildError::WriteCatalog {
        path: path.clone(),
        source,
    })?;
    println!("cargo:rustc-env={EMBEDDED_MIGRATIONS_ENV_VAR}={}", path.display());
    Ok(())
}

/// The maximum SQL nesting depth `bsql-build` parses before rejecting a
/// migration with a classified [`BuildError::Parse`].
///
/// `sqlparser`'s `recursive-protection` feature (enabled in `Cargo.toml`) makes
/// a deep parse STRUCTURALLY unable to overflow the stack — its stacker grows
/// the native stack on demand across the parser's deep-recursion productions —
/// so this bound is not a "fire before the stack overflows" safety margin but a
/// POLICY cap on absurd nesting: past it the parser's `RecursionCounter` returns
/// a classified `RecursionLimitExceeded` (mapped to [`BuildError::Parse`]) rather
/// than parse a pathological migration into a giant AST.
///
/// It sits far ABOVE any legitimate migration — real DDL nests a handful deep; a
/// `CHECK` constraint or sub-query hundreds of parens deep is machine-generated
/// or hostile, not authored — so it never false-rejects a real schema, while
/// still keeping the accepted AST shallow enough that the downstream replay walk
/// and the AST's own recursive `Drop` (which the parser's stacker does NOT cover)
/// stay comfortably within the stack.
const MAX_MIGRATION_PARSE_DEPTH: usize = 512;

/// Parse one migration file's SQL into statements with a bounded recursion
/// depth, mapping ANY parse failure — including the recursion-limit rejection
/// of pathologically deep nesting — to a classified [`BuildError::Parse`]
/// naming the file.
///
/// The SINGLE parse authority: every migration the build reads (the catalog
/// replay in [`replay_file`] AND the embed gate in [`parse_and_enforce_acks`])
/// goes through here, so the recursion bound — and the SIGABRT-to-classified
/// conversion it buys — cannot drift between the two paths. Uses the builder
/// (`with_recursion_limit`) rather than the `Parser::parse_sql` free function,
/// which is fixed at sqlparser's default depth of 50; [`MAX_MIGRATION_PARSE_DEPTH`]
/// raises that to a bound generous for real migrations while still bounding the
/// pathological case.
fn parse_migration_sql(path: &Path, sql: &str) -> Result<Vec<Statement>, BuildError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(MAX_MIGRATION_PARSE_DEPTH)
        .try_with_sql(sql)
        .map_err(|err| BuildError::Parse {
            path: path.to_path_buf(),
            message: err.to_string(),
        })?;
    parser.parse_statements().map_err(|err| BuildError::Parse {
        path: path.to_path_buf(),
        message: err.to_string(),
    })
}

/// Parse one migration file and run the destructive-acknowledgement gate on it
/// — the embed-time reuse of the SAME S42 gate the catalog replay enforces, so
/// an unacknowledged destructive migration cannot ship baked into a binary.
/// (Modelability is NOT required — the runner applies raw SQL — only that the
/// file PARSES so the gate can classify it.)
fn parse_and_enforce_acks(file: &Path, sql: &str) -> Result<(), BuildError> {
    let statements = parse_migration_sql(file, sql)?;
    enforce_destructive_acks(file, sql, &statements)?;
    // The runner owns the transaction boundary; a migration must not contain its
    // own BEGIN/COMMIT/ROLLBACK/SAVEPOINT (which would break per-migration
    // atomicity). Reject in the SAME AST pass as the ack gate.
    for statement in &statements {
        if let Some(keyword) = transaction_control_statement(statement) {
            return Err(BuildError::TransactionControlInMigration {
                file: file.to_path_buf(),
                statement: keyword,
            });
        }
    }
    Ok(())
}

/// Classify a top-level transaction-control statement, returning its keyword, or
/// `None` for any other statement. The runner manages transactions itself, so a
/// migration containing one of these is a build error (see
/// [`BuildError::TransactionControlInMigration`]).
fn transaction_control_statement(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::StartTransaction { .. } => Some("BEGIN"),
        Statement::Commit { .. } => Some("COMMIT"),
        Statement::Rollback { .. } => Some("ROLLBACK"),
        Statement::Savepoint { .. } => Some("SAVEPOINT"),
        Statement::ReleaseSavepoint { .. } => Some("RELEASE SAVEPOINT"),
        _ => None,
    }
}

/// The stable migration NAME for a walked file: its path relative to the
/// migrations directory, with `/` separators regardless of host OS, so the
/// baked name matches the runtime directory walk's name for the SAME file on
/// every platform. This name is the ledger's primary key.
fn migration_name(dir: &Path, file: &Path) -> Result<String, BuildError> {
    // Every walked file is under `dir` (the walk builds paths by joining `dir`),
    // so `strip_prefix` succeeds; the `Err` arm degrades to the full path rather
    // than fabricating, keeping the function total.
    let rel = match file.strip_prefix(dir) {
        Ok(r) => r,
        Err(_) => file,
    };
    let text = rel.to_str().ok_or_else(|| BuildError::Parse {
        path: file.to_path_buf(),
        message: "migration path is not valid UTF-8".to_owned(),
    })?;
    Ok(text.replace(std::path::MAIN_SEPARATOR, "/"))
}

// ════════════════════════════════════════════════════════════════════════
// External-type bridges: decode `query!` columns into a consumer-chosen
// external crate type, with bsql depending on and forcing nothing.
// ════════════════════════════════════════════════════════════════════════
//
// A consumer registers a bridge keyed on a canonical PostgreSQL type, giving
// the TARGET TYPE PATH and an INFALLIBLE converter FREE FUNCTION path, both as
// STRINGS — so this build helper (and the proc-macro) depend on no external
// type crate. The free function is the orphan-proof seam: a consumer cannot
// `impl bsql::Cell for chrono::DateTime` (E0117 — both are foreign), but a
// free `fn ts(v: bsql::Timestamptz) -> chrono::DateTime<Utc>` compiles for any
// foreign target. The bridge reshapes only the RECORD FIELD VALUE; the wire
// decode, the row OID list, and the const validator all continue to ride the
// NATIVE pivot type, so the compile-time OID-drift guarantee is untouched.

/// A builder over the migration-replayed [`Catalog`] that additionally
/// registers external-type bridges, then emits both channels the query
/// proc-macro reads: the schema catalog AND the bridge overrides.
///
/// This is the richer form of the [`emit`] / [`emit_catalog`] free functions:
/// use it when a consumer wants `query!` to decode one or more columns into a
/// chosen external crate type (e.g. `chrono::DateTime`, `uuid::Uuid`,
/// `serde_json::Value`) instead of the dep-free native types. bsql depends on
/// and forces NOTHING: the target type and converter travel as strings, and
/// the consumer supplies one infallible converter free function per bridged
/// type.
///
/// ```no_run
/// fn main() -> Result<(), bsql_build::BuildError> {
///     bsql_build::Catalog::from_migrations("migrations")?
///         .bridge("timestamptz", "chrono::DateTime<chrono::Utc>", "crate::bridge::ts")
///         .bridge("uuid", "uuid::Uuid", "crate::bridge::uuid")
///         .emit()
/// }
/// ```
#[derive(Debug)]
pub struct CatalogBuilder {
    /// The replayed schema, built once at construction.
    catalog: Catalog,
    /// The walked migration tree, kept so `emit` can re-emit the
    /// `rerun-if-changed` directives without a second walk.
    walk: Walk,
    /// The original (manifest-relative) migrations dir argument, kept so the
    /// SQLite template emit can re-resolve it. Only the `sqlite`-feature
    /// `emit` path reads it, so the field exists only under that feature.
    #[cfg(feature = "sqlite")]
    migrations_dir: PathBuf,
    /// The registered bridges, in registration order.
    bridges: Vec<BridgeSpec>,
}

impl Catalog {
    /// The [`UserEnumId`] handle for the user enum named `name`, or `None` when
    /// no user type of that name is an enum. `name` is the canonical
    /// (case-folded) type name a column's `pg_type` carries. The id is the
    /// type's index in the sorted `user_types` map; it round-trips through
    /// [`Catalog::user_enum`] against this same catalog.
    #[must_use]
    pub fn user_enum_id(&self, name: &str) -> Option<UserEnumId> {
        let position = self
            .user_types
            .iter()
            .position(|(key, ty)| key == name && matches!(ty, UserType::Enum { .. }))?;
        u32::try_from(position).ok().map(UserEnumId)
    }

    /// Resolve a [`UserEnumId`] back to the enum's canonical name and ordered
    /// label set, or `None` when the id does not name an enum in this catalog
    /// (an out-of-range index, or a non-enum entry at that position). The
    /// inverse of [`Catalog::user_enum_id`] over this same catalog.
    #[must_use]
    pub fn user_enum(&self, id: UserEnumId) -> Option<(&str, &[String])> {
        let index = usize::try_from(id.0).ok()?;
        let (name, ty) = self.user_types.iter().nth(index)?;
        match ty {
            UserType::Enum { labels } => Some((name.as_str(), labels.as_slice())),
            // Neither a domain nor a composite is an enum — a `UserEnumId` never
            // names one.
            UserType::Domain { .. } | UserType::Composite { .. } => None,
        }
    }

    /// The [`UserCompositeId`] handle for the user COMPOSITE named `name`, or
    /// `None` when no user type of that name is a composite. `name` is the
    /// canonical (case-folded) type name a column's `pg_type` carries. The id is
    /// the type's index in the sorted `user_types` map; it round-trips through
    /// [`Catalog::user_composite`] against this same catalog — the peer of
    /// [`Catalog::user_enum_id`].
    #[must_use]
    pub fn user_composite_id(&self, name: &str) -> Option<UserCompositeId> {
        let position = self
            .user_types
            .iter()
            .position(|(key, ty)| key == name && matches!(ty, UserType::Composite { .. }))?;
        u32::try_from(position).ok().map(UserCompositeId)
    }

    /// Resolve a [`UserCompositeId`] back to the composite's canonical name and
    /// ordered field list, or `None` when the id does not name a composite in
    /// this catalog. The inverse of [`Catalog::user_composite_id`] over this same
    /// catalog — the peer of [`Catalog::user_enum`].
    #[must_use]
    pub fn user_composite(&self, id: UserCompositeId) -> Option<(&str, &[CompositeField])> {
        let index = usize::try_from(id.0).ok()?;
        let (name, ty) = self.user_types.iter().nth(index)?;
        match ty {
            UserType::Composite { fields } => Some((name.as_str(), fields.as_slice())),
            // An enum / domain is not a composite — a `UserCompositeId` never
            // names one.
            UserType::Enum { .. } | UserType::Domain { .. } => None,
        }
    }

    /// Resolve one canonical PostgreSQL type name to the [`RustType`] a column of
    /// that type decodes as, consulting BOTH the native set AND this catalog's
    /// user-defined types (an enum -> `UserEnum`, a composite -> `UserComposite`,
    /// a domain -> its base transitively). `None` for a name that is neither
    /// native nor a modeled user type (the fail-closed contract).
    ///
    /// This is the single choke point the query proc-macro resolves a COMPOSITE
    /// FIELD's type through — the same resolution a top-level column uses — so a
    /// composite field and a column of the same type agree on their Rust type.
    #[must_use]
    pub fn resolve_field_type(&self, pg_type: &str) -> Option<RustType> {
        infer::resolve_pg_type(self, pg_type)
    }

    /// Begin a [`CatalogBuilder`] by replaying a consumer's `migrations/`
    /// directory into a [`Catalog`], the same way [`emit_catalog`] does.
    ///
    /// `migrations_dir` is resolved relative to the consumer crate's
    /// `CARGO_MANIFEST_DIR` (so this must be called from a `build.rs`). It
    /// walks the tree once (recursing into subdirectories) and replays every
    /// `*.sql` file into the catalog; any I/O, parse, or replay error fails
    /// closed as a [`BuildError`]. Chain [`CatalogBuilder::bridge`] calls,
    /// then [`CatalogBuilder::emit`].
    ///
    /// # Errors
    ///
    /// A [`BuildError`] on a missing `CARGO_MANIFEST_DIR`, an unreadable
    /// migrations directory or file, a parse error, or a replay error.
    pub fn from_migrations(migrations_dir: impl AsRef<Path>) -> Result<CatalogBuilder, BuildError> {
        let rel = migrations_dir.as_ref();
        let manifest = env_path("CARGO_MANIFEST_DIR")?;
        let dir = manifest.join(rel);
        let walk = scan_sql_tree(&dir)?;
        let catalog = catalog_from_walk(&walk)?;
        Ok(CatalogBuilder {
            catalog,
            walk,
            #[cfg(feature = "sqlite")]
            migrations_dir: rel.to_path_buf(),
            bridges: Vec::new(),
        })
    }

    /// The nearest known name to an unresolved reference in a `query!` — the
    /// data behind the "did you mean `X`?" hint the proc-macro appends to an
    /// [`InferError::UnknownColumn`] / [`InferError::UnknownRelation`].
    ///
    /// The candidate set is exactly the names THIS catalog can vouch for:
    ///
    /// * [`InferError::UnknownRelation`] — every table name in the catalog.
    /// * [`InferError::UnknownColumn`] — the columns of the named relation,
    ///   WHEN it is a catalog base table (the common case: the error names the
    ///   `FROM` table). A derived-table / CTE / subquery alias has no catalog
    ///   columns, so no candidate is offered — a wrong guess is worse than
    ///   none.
    ///
    /// Matching is a restricted Damerau-Levenshtein (optimal string alignment)
    /// distance — an adjacent transposition (`emial` for `email`) counts as a
    /// SINGLE edit, the canonical one-key slip — case-insensitive, bounded to
    /// `max(len, 3) / 3` edits (the same threshold rustc's own
    /// `find_best_match_for_name` uses). Only the single unambiguous closest
    /// candidate is returned; nothing within the threshold, or a tie for
    /// closest, yields `None` rather than a misleading guess. Any other error
    /// (a different [`InferError`] variant, or a [`DynamicError::Sugar`])
    /// carries no name to correct, so it is `None`.
    #[must_use]
    pub fn did_you_mean(&self, err: &DynamicError) -> Option<String> {
        let DynamicError::Infer(inner) = err else {
            return None;
        };
        match inner {
            InferError::UnknownRelation(name) => {
                nearest_name(name, self.tables.keys().map(String::as_str))
            }
            InferError::UnknownColumn { relation, column } => {
                // Exact key first (PG folds unquoted identifiers, so the error
                // usually names the catalog's stored form); fall back to a
                // case-insensitive scan so a quoted / mixed-case relation name
                // still finds its columns for the hint. The column match below
                // is already case-insensitive, so this keeps the two sides
                // consistent — a relation-casing mismatch no longer silently
                // drops the suggestion.
                let columns = self.tables.get(relation).or_else(|| {
                    self.tables
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(relation))
                        .map(|(_, cols)| cols)
                })?;
                nearest_name(column, columns.keys().map(String::as_str))
            }
            _ => None,
        }
    }
}

/// The single closest candidate to `target`, within the rustc "did you mean"
/// threshold (`max(len, 3) / 3` edits), or `None` when nothing is close enough
/// OR the closest is a tie (an ambiguous best is no suggestion at all).
/// Case-insensitive over ASCII; returns the candidate's own spelling.
fn nearest_name<'a>(target: &str, candidates: impl Iterator<Item = &'a str>) -> Option<String> {
    let needle: Vec<char> = target.to_ascii_lowercase().chars().collect();
    // rustc's `find_best_match_for_name` bound is `dist <= max(len, 3) / 3`.
    // Kept as the equivalent `3 * dist <= max(len, 3)` to avoid the forbidden
    // `integer_division` (both sides are the same integer relation).
    let bound = std::cmp::max(needle.len(), 3);
    let mut best: Option<(usize, &str)> = None;
    let mut tie = false;
    for cand in candidates {
        let hay: Vec<char> = cand.to_ascii_lowercase().chars().collect();
        let dist = osa_distance(&needle, &hay);
        if dist * 3 > bound {
            continue;
        }
        match best {
            None => best = Some((dist, cand)),
            Some((best_dist, _)) if dist < best_dist => {
                best = Some((dist, cand));
                tie = false;
            }
            Some((best_dist, _)) if dist == best_dist => tie = true,
            Some(_) => {}
        }
    }
    match best {
        Some((_, name)) if !tie => Some(name.to_string()),
        _ => None,
    }
}

/// Restricted Damerau-Levenshtein (optimal string alignment) distance: the
/// minimum single-character insertions, deletions, substitutions, and
/// ADJACENT transpositions to turn `a` into `b`. Operates on pre-lowercased
/// char slices. Every index is proved in range by the loop bounds (row `i` /
/// col `j` of an `(n+1) × (m+1)` grid), and each `- 1` / `- 2` is guarded by
/// its surrounding `i`/`j` bound, so it cannot panic on any input.
fn osa_distance(a: &[char], b: &[char]) -> usize {
    let n = a.len();
    let m = b.len();
    let mut d = vec![vec![0usize; m + 1]; n + 1];
    // Base row/column: transforming an i-char prefix to/from the empty string
    // costs i edits. (`row.first_mut()` / `d.first_mut()` are always `Some` —
    // every row has `m + 1 >= 1` cells and the grid has `n + 1 >= 1` rows —
    // but the `if let` keeps it panic-free by construction.)
    for (i, row) in d.iter_mut().enumerate() {
        if let Some(first) = row.first_mut() {
            *first = i;
        }
    }
    if let Some(first_row) = d.first_mut() {
        for (j, cell) in first_row.iter_mut().enumerate() {
            *cell = j;
        }
    }
    for i in 1..=n {
        for j in 1..=m {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            let deletion = d[i - 1][j] + 1;
            let insertion = d[i][j - 1] + 1;
            let substitution = d[i - 1][j - 1] + cost;
            let mut best = deletion.min(insertion).min(substitution);
            // Adjacent transposition (`ab` <-> `ba`): one edit, not two subs.
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                best = best.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = best;
        }
    }
    d[n][m]
}

impl CatalogBuilder {
    /// Register an external-type bridge: every `query!` column whose type is
    /// the canonical PostgreSQL type `pg_type` decodes into `target_type_path`
    /// (the BARE target type — no `.0`, no `.into()`, no annotation at the
    /// query site) by applying the infallible converter free function at
    /// `converter_fn_path` to the native decoded value.
    ///
    /// `pg_type` must be a canonical PostgreSQL type name with a native bsql
    /// pivot (int2, int4, int8, oid, bool, text, varchar, float4, float8,
    /// bytea, uuid, timestamptz, timestamp, json, jsonb, numeric); a bridge on
    /// an element type ALSO reshapes each element of that type's 1-D array
    /// (`timestamptz[]` -> `Vec<Option<target>>`). A typo or a natively
    /// unsupported type (e.g. `inet`) is a loud [`BuildError`] at
    /// [`emit`](Self::emit).
    ///
    /// `target_type_path` and `converter_fn_path` are Rust paths that resolve
    /// at the `query!` call site in the CONSUMER crate: `target_type_path` is
    /// the field type (e.g. `chrono::DateTime<chrono::Utc>`); `converter_fn_path`
    /// is a `fn(NativeOwned) -> Target` (e.g. `crate::bridge::ts`, where the
    /// native owned type is `bsql::Timestamptz`). bsql depends on neither —
    /// they travel as strings and are resolved by the consumer's own
    /// dependencies. A path that does not resolve is a normal build error at
    /// the `query!` site.
    #[must_use]
    pub fn bridge(
        mut self,
        pg_type: &str,
        target_type_path: &str,
        converter_fn_path: &str,
    ) -> Self {
        self.bridges.push(BridgeSpec {
            pg_type: pg_type.to_string(),
            target_type_path: target_type_path.to_string(),
            converter_fn_path: converter_fn_path.to_string(),
        });
        self
    }

    /// Validate the registered bridges, then emit both proc-macro channels:
    /// the schema catalog (as [`emit_catalog`]) and the external-type bridge
    /// overrides (`BSQL_TYPE_BRIDGES`). Under the `sqlite` feature it ALSO
    /// emits the SQLite conformance template, exactly as [`emit`] does — the
    /// builder analogue of the [`emit`] free function.
    ///
    /// # Errors
    ///
    /// A [`BuildError::UnknownBridgeType`] for a bridge whose `pg_type` has no
    /// native pivot, a [`BuildError::ConflictingBridge`] for two bridges that
    /// resolve to the same native pivot, any catalog I/O error, or (under the
    /// `sqlite` feature) a SQLite replay error.
    pub fn emit(self) -> Result<(), BuildError> {
        self.emit_channels()?;
        #[cfg(feature = "sqlite")]
        emit_sqlite_template(&self.migrations_dir)?;
        Ok(())
    }

    /// Validate the registered bridges, then emit the PostgreSQL schema catalog
    /// and the external-type bridge overrides, WITHOUT the SQLite conformance
    /// template — the builder analogue of the [`emit_catalog`] free function.
    /// Use this for a deliberately PostgreSQL-only build (e.g. one bridging to
    /// a type that has no portable SQLite form), so no SQLite target is ever
    /// declared regardless of whether the `sqlite` feature happens to be
    /// activated by feature unification in the surrounding build graph.
    ///
    /// # Errors
    ///
    /// A [`BuildError::UnknownBridgeType`] for a bridge whose `pg_type` has no
    /// native pivot, a [`BuildError::ConflictingBridge`] for two bridges that
    /// resolve to the same native pivot, or any catalog / bridge I/O error.
    pub fn emit_catalog(self) -> Result<(), BuildError> {
        self.emit_channels()
    }

    /// The shared core of [`emit`](Self::emit) / [`emit_catalog`](Self::emit_catalog):
    /// validate the bridges, emit the rerun directives, and write both the
    /// catalog and the bridge channels. Never touches the SQLite template.
    fn emit_channels(&self) -> Result<(), BuildError> {
        validate_bridges(&self.bridges, &self.catalog)?;
        emit_rerun_directives(&self.walk);
        write_catalog(&self.catalog)?;
        write_bridges(&self.bridges)?;
        Ok(())
    }
}

/// Validate the registered bridges against the natively-supported set and each
/// other. Each `pg_type` must resolve to a native pivot [`RustType`]
/// ([`BuildError::UnknownBridgeType`] otherwise); no two bridges may resolve
/// to the SAME pivot ([`BuildError::ConflictingBridge`]). A bridge whose
/// `pg_type` matches no table column in the catalog is a clear `cargo:warning`
/// (it may still apply to a CAST result), never a silent drop.
fn validate_bridges(bridges: &[BridgeSpec], catalog: &Catalog) -> Result<(), BuildError> {
    // (resolved pivot rust_name, the pg_type key that produced it) — to catch
    // two distinct keys colliding on one native pivot.
    let mut resolved: Vec<(&'static str, &str)> = Vec::with_capacity(bridges.len());
    for bridge in bridges {
        let pivot = infer::scalar_rust_type_for_pg(&bridge.pg_type).ok_or_else(|| {
            BuildError::UnknownBridgeType {
                pg_type: bridge.pg_type.clone(),
            }
        })?;
        let pivot_name = pivot.rust_name();
        if let Some((_, first)) = resolved.iter().find(|(name, _)| *name == pivot_name) {
            return Err(BuildError::ConflictingBridge {
                first: (*first).to_string(),
                second: bridge.pg_type.clone(),
            });
        }
        resolved.push((pivot_name, &bridge.pg_type));

        if !catalog_uses_pivot(catalog, pivot) {
            // A clear diagnostic (not a silent ignore): the bridge is still
            // emitted — it may legitimately apply to a CAST result the catalog
            // does not enumerate — but a bridge that matches no table column is
            // worth surfacing (it is often a typo in the type name). The match
            // is keyed on the RESOLVED native pivot, NOT the raw pg_type string,
            // so it reflects the exact family the bridge fires on at the query!
            // site: `text` and `varchar` share `RustType::Text`, so a
            // `.bridge("text")` over a `varchar`-only schema (which it DOES fire
            // on) does not spuriously warn.
            println!(
                "cargo:warning=bsql-build: bridge for `{}` matches no table \
                 column in the catalog (it still applies to a CAST result of \
                 that type, if any).",
                bridge.pg_type
            );
        }
    }
    Ok(())
}

/// The native pivot [`RustType`] a catalog column's canonical `pg_type`
/// resolves to for bridge-matching: a scalar column resolves directly; a 1-D
/// array column (`text[]`) resolves to its ELEMENT pivot, because a bridge on
/// the element type fires per element on that array. `None` for a type with no
/// native pivot (a multi-dimensional array, or an unsupported type).
fn column_pivot(pg_type: &str) -> Option<RustType> {
    if let Some(element) = pg_type.strip_suffix("[]") {
        // An array column fires the element-type bridge; `scalar_rust_type_for_pg`
        // rejects any remaining `[]` (a multi-dimensional array), so `text[][]`
        // stays `None`.
        return scalar_rust_type_for_pg(element);
    }
    scalar_rust_type_for_pg(pg_type)
}

/// Whether any catalog table column resolves to the given native pivot type
/// — as a scalar column OR as the element of a 1-D array column. Keyed on the
/// RESOLVED pivot (not the raw `pg_type` string) so it agrees with the query!
/// firing rule, which matches on the native pivot: `text` and `varchar` both
/// resolve to `RustType::Text`, so a `varchar` column counts as a match for a
/// `text` bridge (and vice versa) — the two never disagree with each other.
fn catalog_uses_pivot(catalog: &Catalog, pivot: RustType) -> bool {
    catalog.tables.values().any(|columns| {
        columns
            .values()
            .any(|info| column_pivot(&info.pg_type) == Some(pivot))
    })
}

/// Serialize the bridges to `OUT_DIR/bsql_type_bridges.txt` and set the
/// `BSQL_TYPE_BRIDGES` rustc-env channel. Each line is
/// `pg_type\ttarget_type_path\tconverter_fn_path`. When there are no bridges
/// the file is empty and the channel still points at it (the macro then reads
/// an empty override set — identical to the no-bridge free-fn path).
fn write_bridges(bridges: &[BridgeSpec]) -> Result<(), BuildError> {
    let out_dir = env_path("OUT_DIR")?;
    let bridges_path = out_dir.join(BRIDGES_FILE_NAME);
    let serialized = serialize_bridges(bridges);
    std::fs::write(&bridges_path, serialized).map_err(|source| BuildError::WriteCatalog {
        path: bridges_path.clone(),
        source,
    })?;
    println!(
        "cargo:rustc-env={BRIDGES_ENV_VAR}={}",
        bridges_path.display()
    );
    Ok(())
}

/// Serialize bridges to the tab-separated line format `write_bridges` writes
/// and [`parse_bridges`] reads back. Sorted by `pg_type` for byte-determinism.
/// The target/converter paths carry no tab or newline (they are Rust paths),
/// so the format is unambiguous.
fn serialize_bridges(bridges: &[BridgeSpec]) -> String {
    let mut sorted: Vec<&BridgeSpec> = bridges.iter().collect();
    sorted.sort_by(|a, b| a.pg_type.cmp(&b.pg_type));
    let mut out = String::new();
    for bridge in sorted {
        out.push_str(&bridge.pg_type);
        out.push('\t');
        out.push_str(&bridge.target_type_path);
        out.push('\t');
        out.push_str(&bridge.converter_fn_path);
        out.push('\n');
    }
    out
}

/// One parsed external-type bridge, as the query proc-macro reads it back from
/// the `BSQL_TYPE_BRIDGES` channel via [`parse_bridges`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSpec {
    /// The canonical PostgreSQL type key.
    pub pg_type: String,
    /// The consumer's target type path.
    pub target_type_path: String,
    /// The consumer's converter free-fn path.
    pub converter_fn_path: String,
}

/// A failure parsing the line-oriented bridge file back into [`BridgeSpec`]s.
/// The file is machine-generated by [`CatalogBuilder::emit`], so a malformed
/// line means the build-script channel is corrupt — a loud error, never a
/// silently dropped bridge (which would reopen the native type for a column
/// the consumer chose to bridge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeParseError {
    /// The 1-based line number of the malformed line.
    pub line: usize,
    /// How many tab-separated fields were found (expected exactly 3).
    pub fields: usize,
}

impl fmt::Display for BridgeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "type-bridge file line {} has {} tab-separated field(s), expected \
             exactly 3 (pg_type, target_type_path, converter_fn_path). The \
             file is machine-generated; a malformed line means the \
             build-script channel is corrupt.",
            self.line, self.fields
        )
    }
}

impl std::error::Error for BridgeParseError {}

/// Parse the line-oriented bridge text [`serialize_bridges`] produced back
/// into [`BridgeSpec`]s (the inverse of the emit path). Each non-empty line is
/// `pg_type\ttarget_type_path\tconverter_fn_path`. A blank final line (from a
/// trailing newline) is skipped; every other line MUST have exactly three
/// tab-separated fields, or a loud [`BridgeParseError`].
///
/// # Errors
///
/// [`BridgeParseError`] when a non-empty line does not have exactly three
/// tab-separated fields.
pub fn parse_bridges(text: &str) -> Result<Vec<BridgeSpec>, BridgeParseError> {
    let mut specs = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let number = idx.saturating_add(1);
        let fields: Vec<&str> = line.split('\t').collect();
        let [pg_type, target, converter] = match fields.as_slice() {
            [a, b, c] => [*a, *b, *c],
            other => {
                return Err(BridgeParseError {
                    line: number,
                    fields: other.len(),
                })
            }
        };
        specs.push(BridgeSpec {
            pg_type: pg_type.to_string(),
            target_type_path: target.to_string(),
            converter_fn_path: converter.to_string(),
        });
    }
    Ok(specs)
}

// ════════════════════════════════════════════════════════════════════════
// User-defined types channel (`CREATE TYPE ... AS ENUM`): the migration-
// declared enum set, serialized to `OUT_DIR/bsql_user_types.txt` and read
// back by the query proc-macro so it can generate one Rust type per user
// type. A SEPARATE channel from the schema catalog (whose line format is
// pinned at exactly five fields) — mirroring the external-type bridge
// channel — so the catalog format and its goldens stay byte-identical.
// ════════════════════════════════════════════════════════════════════════

/// Escape one user-type field (a type name or an enum label) for the
/// tab-delimited, line-oriented user-types channel. An enum label is a
/// PostgreSQL string literal and may contain ANY byte except NUL — INCLUDING a
/// tab or newline, which are the channel's field and record delimiters — so the
/// four bytes that would otherwise be ambiguous (`\`, tab, newline, carriage
/// return) are backslash-escaped. Every label round-trips exactly through
/// [`unescape_user_field`], so no label is ever rejected or silently corrupted
/// (universal, not a fail-loud rejection of an unusual-but-legal label).
fn escape_user_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out
}

/// Inverse of [`escape_user_field`]: decode one escaped field back to its
/// literal bytes. Total — an unrecognized or dangling escape is a loud
/// [`UserTypesParseError::BadEscape`] (the channel is machine-generated, so a
/// bad escape means the build-script channel is corrupt), never a silent drop.
fn unescape_user_field(s: &str, line: usize) -> Result<String, UserTypesParseError> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            other => {
                return Err(UserTypesParseError::BadEscape {
                    line,
                    sequence: match other {
                        Some(c) => format!("\\{c}"),
                        None => "\\".to_string(),
                    },
                });
            }
        }
    }
    Ok(out)
}

/// Serialize the user-defined types to the line-oriented text the query
/// proc-macro reads back via [`parse_user_types`]. One line per type; the
/// leading field is a one-letter kind tag. For an enum:
///
/// ```text
/// E<TAB><name>[<TAB><label>]...
/// ```
///
/// Name and labels are [`escape_user_field`]-escaped so a tab/newline in a
/// label cannot break the framing. `BTreeMap` iteration keeps the output
/// byte-deterministic across builds. An enum with zero labels (PostgreSQL
/// permits `AS ENUM ()`) serializes as just `E<TAB><name>`.
fn serialize_user_types(user_types: &BTreeMap<String, UserType>) -> String {
    let mut out = String::new();
    for (name, ty) in user_types {
        match ty {
            UserType::Enum { labels } => {
                out.push('E');
                out.push('\t');
                out.push_str(&escape_user_field(name));
                for label in labels {
                    out.push('\t');
                    out.push_str(&escape_user_field(label));
                }
                out.push('\n');
            }
            UserType::Domain { base } => {
                // `D<TAB><name><TAB><base>` — a domain aliases exactly one base.
                out.push('D');
                out.push('\t');
                out.push_str(&escape_user_field(name));
                out.push('\t');
                out.push_str(&escape_user_field(base));
                out.push('\n');
            }
            UserType::Composite { fields } => {
                // `C<TAB><name>[<TAB><field_name><TAB><field_type>]...` — the
                // field name and type alternate, so the fields following the name
                // are an EVEN count of (name, type) pairs in declared order.
                out.push('C');
                out.push('\t');
                out.push_str(&escape_user_field(name));
                for field in fields {
                    out.push('\t');
                    out.push_str(&escape_user_field(&field.name));
                    out.push('\t');
                    out.push_str(&escape_user_field(&field.pg_type));
                }
                out.push('\n');
            }
        }
    }
    out
}

/// A failure parsing the line-oriented user-types file back into the
/// [`UserType`] map. The file is machine-generated by the emit path, so any
/// malformed line means the build-script channel is corrupt — a loud error,
/// never a silently dropped type (which would reopen a user-typed column as an
/// unsupported/undecodable type the consumer chose to model).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserTypesParseError {
    /// A line's leading kind tag is not a known kind (`E` for enum).
    UnknownKind {
        /// The 1-based line number.
        line: usize,
        /// The offending kind tag.
        kind: String,
    },
    /// A line carried a kind tag but no type-name field.
    MissingName {
        /// The 1-based line number.
        line: usize,
    },
    /// A `D` (domain) line did not have exactly a name and a base field.
    MalformedDomain {
        /// The 1-based line number.
        line: usize,
        /// How many fields followed the kind tag (expected exactly 2).
        fields: usize,
    },
    /// A `C` (composite) line's attribute fields did not form (name, type)
    /// PAIRS — the fields after the type name must be an even count.
    MalformedComposite {
        /// The 1-based line number.
        line: usize,
        /// How many attribute fields followed the type name (expected even).
        fields: usize,
    },
    /// A field held an unrecognized or dangling backslash escape.
    BadEscape {
        /// The 1-based line number.
        line: usize,
        /// The offending escape sequence (e.g. `\x` or a trailing `\`).
        sequence: String,
    },
}

impl fmt::Display for UserTypesParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserTypesParseError::UnknownKind { line, kind } => write!(
                f,
                "user-types file line {line} has an unknown kind tag `{kind}` \
                 (expected `E` for an enum). The file is machine-generated; a \
                 malformed line means the build-script channel is corrupt."
            ),
            UserTypesParseError::MissingName { line } => write!(
                f,
                "user-types file line {line} has a kind tag but no type-name \
                 field. The file is machine-generated; a malformed line means \
                 the build-script channel is corrupt."
            ),
            UserTypesParseError::MalformedDomain { line, fields } => write!(
                f,
                "user-types file line {line} is a domain (`D`) with {fields} \
                 field(s) after the kind, expected exactly 2 (name, base). The \
                 file is machine-generated; a malformed line means the \
                 build-script channel is corrupt."
            ),
            UserTypesParseError::MalformedComposite { line, fields } => write!(
                f,
                "user-types file line {line} is a composite (`C`) with {fields} \
                 attribute field(s) after the name, expected an even count of \
                 (name, type) pairs. The file is machine-generated; a malformed \
                 line means the build-script channel is corrupt."
            ),
            UserTypesParseError::BadEscape { line, sequence } => write!(
                f,
                "user-types file line {line} has an invalid escape sequence \
                 `{sequence}`. The file is machine-generated; a malformed field \
                 means the build-script channel is corrupt."
            ),
        }
    }
}

impl std::error::Error for UserTypesParseError {}

/// Parse the line-oriented user-types text [`serialize_user_types`] produced
/// back into the [`UserType`] map (the inverse of the emit path). Each
/// non-empty line is a kind tag then tab-separated escaped fields; a blank
/// final line (from a trailing newline) is skipped. Every other line MUST be
/// well-formed, or a loud [`UserTypesParseError`].
///
/// # Errors
///
/// [`UserTypesParseError`] for an unknown kind tag, a kind tag without a name,
/// or a field with an invalid backslash escape.
pub fn parse_user_types(text: &str) -> Result<BTreeMap<String, UserType>, UserTypesParseError> {
    let mut types = BTreeMap::new();
    for (idx, line) in text.lines().enumerate() {
        // A trailing newline yields one empty final line; it is the only
        // skippable form. Line numbers are 1-based in diagnostics.
        if line.is_empty() {
            continue;
        }
        let number = idx.saturating_add(1);
        let fields: Vec<&str> = line.split('\t').collect();
        // `split` always yields at least one field; `split_first` binds the
        // kind tag without an indexing operation.
        let (kind, rest) = match fields.split_first() {
            Some((k, r)) => (*k, r),
            None => continue,
        };
        match kind {
            "E" => {
                let (name_field, label_fields) = match rest.split_first() {
                    Some((n, l)) => (*n, l),
                    None => return Err(UserTypesParseError::MissingName { line: number }),
                };
                let name = unescape_user_field(name_field, number)?;
                let mut labels = Vec::with_capacity(label_fields.len());
                for label in label_fields {
                    labels.push(unescape_user_field(label, number)?);
                }
                types.insert(name, UserType::Enum { labels });
            }
            "D" => {
                let [name_field, base_field] = match rest {
                    [name_field, base_field] => [*name_field, *base_field],
                    other => {
                        return Err(UserTypesParseError::MalformedDomain {
                            line: number,
                            fields: other.len(),
                        });
                    }
                };
                let name = unescape_user_field(name_field, number)?;
                let base = unescape_user_field(base_field, number)?;
                types.insert(name, UserType::Domain { base });
            }
            "C" => {
                let (name_field, attr_fields) = match rest.split_first() {
                    Some((n, l)) => (*n, l),
                    None => return Err(UserTypesParseError::MissingName { line: number }),
                };
                // The attribute fields alternate (name, type), so their count
                // must be even. An odd count is a corrupt channel.
                if !attr_fields.len().is_multiple_of(2) {
                    return Err(UserTypesParseError::MalformedComposite {
                        line: number,
                        fields: attr_fields.len(),
                    });
                }
                let name = unescape_user_field(name_field, number)?;
                // One `CompositeField` per (name, type) PAIR. `chunks_exact(2)`
                // over the even-length slice yields exactly that many chunks; the
                // capacity is a hint (`attr_fields.len()` is a safe upper bound,
                // avoiding a division the crate lint forbids).
                let mut fields = Vec::with_capacity(attr_fields.len());
                for pair in attr_fields.chunks_exact(2) {
                    // `chunks_exact(2)` over an even-length slice yields exactly
                    // (name, type) pairs; `split_first` binds each without
                    // indexing.
                    let (field_name, type_rest) = match pair.split_first() {
                        Some((n, r)) => (*n, r),
                        None => continue,
                    };
                    let field_type = match type_rest.first() {
                        Some(t) => *t,
                        None => continue,
                    };
                    fields.push(CompositeField {
                        name: unescape_user_field(field_name, number)?,
                        pg_type: unescape_user_field(field_type, number)?,
                    });
                }
                types.insert(name, UserType::Composite { fields });
            }
            other => {
                return Err(UserTypesParseError::UnknownKind {
                    line: number,
                    kind: other.to_string(),
                });
            }
        }
    }
    Ok(types)
}

/// Serialize the catalog's user types to `OUT_DIR/bsql_user_types.txt` and set
/// the `BSQL_USER_TYPES` rustc-env channel. Called from the one catalog-writing
/// path ([`write_catalog`]) so every emit path (the free functions AND the
/// builder) always emits it — the file is empty when the migrations declare no
/// user types, so the channel is always definite (the macro never has to infer
/// absence from a missing variable).
fn write_user_types(catalog: &Catalog) -> Result<(), BuildError> {
    let out_dir = env_path("OUT_DIR")?;
    let path = out_dir.join(USER_TYPES_FILE_NAME);
    let serialized = serialize_user_types(&catalog.user_types);
    std::fs::write(&path, serialized).map_err(|source| BuildError::WriteCatalog {
        path: path.clone(),
        source,
    })?;
    println!("cargo:rustc-env={USER_TYPES_ENV_VAR}={}", path.display());
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
#[derive(Debug, Default)]
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
/// (at any depth, in the runner's replay order) and every directory visited.
///
/// Recursion (rather than rejecting subdirectories) is deliberate:
/// partitioned migration layouts (e.g. `migrations/2026/0001.sql`) are a
/// legitimate convention, so they must be picked up, not refused. Files are
/// sorted AFTER the walk so the replay sequence is deterministic regardless of
/// the order the filesystem yields entries.
///
/// The sort key is the canonical migration NAME ([`migration_name`]) — the SAME
/// `/`-normalized relative-name string the runtime runner sorts by, NOT the raw
/// `PathBuf`. A `PathBuf`'s component-wise `Ord` disagrees with a byte-wise name
/// compare at the `.` (0x2E) / `/` (0x2F) boundary for nested prefix collisions
/// (e.g. a `PathBuf` sort would order `[a/b.sql, a.sql]`, the runner
/// `[a.sql, a/b.sql]`), so a naive `PathBuf` sort would make the build-validated
/// catalog order DIVERGE from the runtime apply order in nested layouts. Sorting
/// by [`migration_name`] — the ONE name-normalization authority in this crate,
/// the exact string the runner keys on — makes them structurally ONE order: what
/// the catalog type-checks in order is what the runner applies in order.
///
/// [`migration_name`] is fallible on a non-UTF-8 path, so precomputing it here
/// (rather than a separate lossy sort key) surfaces that error at SCAN — earlier
/// and louder than the emit-time name resolution that would otherwise catch it,
/// and against the SAME rejection.
fn scan_sql_tree(dir: &Path) -> Result<Walk, BuildError> {
    let mut walk = Walk::default();
    descend(dir, &mut walk)?;

    // Pair each walked file with its canonical migration name (fallible on a
    // non-UTF-8 path), sort by that name string, then restore the ordered paths.
    let files = core::mem::take(&mut walk.files);
    let mut named = files
        .into_iter()
        .map(|file| Ok((migration_name(dir, &file)?, file)))
        .collect::<Result<Vec<(String, PathBuf)>, BuildError>>()?;
    named.sort_by(|(a, _), (b, _)| a.cmp(b));
    walk.files = named.into_iter().map(|(_, file)| file).collect();

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
            // Admit only a REGULAR file (or a symlink resolving to one).
            // `entry.file_type()` above does NOT traverse symlinks and reports
            // only dir-vs-not, so a FIFO / socket / device / named pipe named
            // `*.sql` would otherwise be pushed as a leaf and then BLOCK the
            // build forever at `read_to_string` (a writer-less FIFO's `open`
            // never returns; a `/dev/zero`-class device would grow the read
            // buffer without bound → OOM). `fs::metadata` FOLLOWS symlinks and
            // does NOT open the file (a `stat`, so it never blocks on a FIFO),
            // so a legitimate symlink to a real `.sql` resolves to a regular
            // file and is admitted, while a non-regular `.sql` is a LOUD
            // classified error naming the path — never a silent skip (a
            // migration that is a device is an authorship error, not an absent
            // migration). A broken/dangling symlink fails the `stat` and is the
            // classified [`BuildError::ReadFile`] naming it.
            let metadata = std::fs::metadata(&path).map_err(|source| BuildError::ReadFile {
                path: path.clone(),
                source,
            })?;
            if metadata.is_file() {
                walk.files.push(path);
            } else {
                return Err(BuildError::NonRegularFile { path });
            }
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
    let statements = parse_migration_sql(path, sql)?;
    // Gate irreversible data destruction BEFORE any statement mutates the
    // catalog: a `DROP TABLE` / `DROP COLUMN` without a co-located
    // acknowledgement fails the build rather than silently discarding data.
    enforce_destructive_acks(path, sql, &statements)?;
    for statement in statements {
        replay_statement(catalog, path, statement)?;
    }
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════
// Destructive-migration acknowledgement gate.
// ════════════════════════════════════════════════════════════════════════
//
// A migration that irreversibly destroys data must be ACKNOWLEDGED in the
// migration file, with a `-- bsql:ack-destructive` comment on the line(s)
// immediately preceding the statement. An unacknowledged destructive statement
// is a loud [`BuildError::UnackedDestructiveMigration`] that fails the
// consumer's build, catching an ACCIDENTAL destructive migration at compile
// time instead of in production.
//
// The COMPLETE destructive set (each destroys irreversible BASE data — no
// query reconstructs the lost rows — and is classified purely by the destroying
// VERB, independent of the catalog):
//
//   * `DROP TABLE`                     — drops every row of a table.
//   * `ALTER TABLE ... DROP COLUMN`    — drops a column's data.
//   * `DROP SCHEMA ... CASCADE`        — drops every table (and all their rows)
//                                        in the schema; a strictly larger loss
//                                        than one `DROP TABLE`.
//   * `TRUNCATE`                       — drops every row of the named table(s).
//   * `DROP DATABASE`                  — drops an entire database.
//
// The set is deliberately CONSERVATIVE — over-flagging safe DDL would train
// developers to blanket-acknowledge everything, defeating the gate. These are
// DELIBERATELY excluded (each omission is documented here so it is auditable,
// never a silent gap):
//
//   * `RENAME`, `ADD COLUMN`, `SET`/`DROP NOT NULL`, a bare `ALTER COLUMN`
//     option change — data is preserved, so no acknowledgement.
//   * `DROP SCHEMA` without `CASCADE` (RESTRICT, the default) — it FAILS on a
//     non-empty schema, so it cannot accidentally destroy data; only the
//     `CASCADE` form is flagged.
//   * `DROP MATERIALIZED VIEW` / `DROP VIEW` / `DROP INDEX` — a view is virtual
//     and an index/materialized view is DERIVED: its rows are reconstructible
//     from the base tables via the object's own definition, so dropping it is
//     recoverable, not irreversible base-data loss. (A materialized view can be
//     expensive to REFRESH, but "expensive to rebuild" is not "data destroyed";
//     flagging it would over-flag a common cache-management operation.)
//   * A LOSSY `ALTER COLUMN ... TYPE` (a narrowing that can truncate) — NOT yet
//     flagged: soundly classifying "lossy" needs a type-width lattice (e.g.
//     `int8 -> int4` loses high bits, `int4 -> int8` is a safe widening) whose
//     subtleties (float precision, `numeric` scale) would over- or under-flag.
//     A clean future extension to `destructive_statement`.
//
// The acknowledgement is parsed GLASS-FREE via the SQL tokenizer, never a
// hand-rolled text scan: `Tokenizer::tokenize_with_location` classifies string
// literals, dollar-quoted bodies, and comments correctly, so the marker text
// inside a string literal is a string token (not a comment) and cannot forge an
// acknowledgement, a `;` inside a string is not a statement separator, and the
// marker must genuinely precede the destructive statement (not a different one).

/// The exact acknowledgement marker a migration author writes as an SQL comment
/// immediately before a destructive statement, e.g. `-- bsql:ack-destructive`.
/// Shown verbatim in [`BuildError::UnackedDestructiveMigration`].
const ACK_MARKER_SYNTAX: &str = "-- bsql:ack-destructive";

/// The bare marker token an acknowledgement comment must LEAD with (the
/// [`ACK_MARKER_SYNTAX`] without its `--` comment prefix), matched against a
/// comment's content by [`comment_is_ack`].
const ACK_MARKER: &str = "bsql:ack-destructive";

/// Render one or more object names as a comma-separated list for a destructive
/// statement's human description.
fn render_object_names(names: &[ObjectName]) -> String {
    names
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Classify a statement as irreversibly data-destroying, returning a short
/// human description of WHAT it destroys, or `None` for a non-destructive
/// statement. This single function is the whole destructive SET: extend it
/// (e.g. for a lossy `ALTER COLUMN ... TYPE`) in exactly one place. The complete
/// set and its deliberate exclusions are documented on the module section above.
///
/// The classification is purely SYNTACTIC — independent of whether the target
/// is in the replayed catalog. A `DROP TABLE users` where `users` came from an
/// out-of-band migration is invisible to the catalog yet still destroys the
/// live table's data, so the destroying VERB is what requires acknowledgement,
/// not the catalog's model of it. `IF EXISTS` does not change that: if the
/// object exists, its data is destroyed.
fn destructive_statement(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            ..
        } => Some(format!("DROP TABLE {}", render_object_names(names))),
        // `DROP SCHEMA ... CASCADE` drops every table (and all their rows) in
        // the schema. Only the CASCADE form: RESTRICT / the default fail on a
        // non-empty schema, so they cannot accidentally destroy data.
        Statement::Drop {
            object_type: ObjectType::Schema,
            names,
            cascade: true,
            ..
        } => Some(format!("DROP SCHEMA {} CASCADE", render_object_names(names))),
        // `DROP DATABASE` drops an entire database.
        Statement::Drop {
            object_type: ObjectType::Database,
            names,
            ..
        } => Some(format!("DROP DATABASE {}", render_object_names(names))),
        // `TRUNCATE` drops every row of the named table(s).
        Statement::Truncate(truncate) => {
            let targets = truncate
                .table_names
                .iter()
                .map(|target| target.name.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Some(format!("TRUNCATE {targets}"))
        }
        Statement::AlterTable(alter) => {
            // An `ALTER TABLE` may carry several operations; describe every
            // `DROP COLUMN` among them. One acknowledgement covers the whole
            // statement (the co-located comment precedes it), and the developer
            // sees the `DROP COLUMN` they are acknowledging in that statement.
            let dropped: Vec<String> = alter
                .operations
                .iter()
                .filter_map(|op| match op {
                    AlterTableOperation::DropColumn { column_names, .. } => Some(
                        column_names
                            .iter()
                            .map(|ident| ident.value.clone())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                    _ => None,
                })
                .collect();
            if dropped.is_empty() {
                None
            } else {
                Some(format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    alter.name,
                    dropped.join(", ")
                ))
            }
        }
        _ => None,
    }
}

/// One parsed statement's acknowledgement metadata, correlated by index with
/// the parser's `Vec<Statement>`: the 1-based source line of its first token
/// and whether a valid acknowledgement comment precedes it.
#[derive(Debug)]
struct StatementAck {
    /// The 1-based source line of the statement's first significant token.
    line: u64,
    /// Whether an acknowledgement marker comment sits in this statement's
    /// leading trivia (the comments after the previous statement's terminating
    /// `;`, or the file start, up to this statement's first significant token).
    acked: bool,
}

/// Fail the build if any statement irreversibly destroys data without a
/// co-located acknowledgement. Runs before the replay so nothing mutates the
/// catalog on the destructive path.
///
/// The classification (`destructive_statement`) comes from the parsed AST; the
/// acknowledgement layout (`scan_statement_acks`) comes from the token stream.
/// They are correlated by statement index — both enumerate statements in source
/// order, so the k-th token-derived boundary is the k-th parsed statement.
fn enforce_destructive_acks(
    path: &Path,
    sql: &str,
    statements: &[Statement],
) -> Result<(), BuildError> {
    // Fast path: the token scan runs only when the file actually contains a
    // destructive statement (the overwhelming common case is none).
    if !statements
        .iter()
        .any(|s| destructive_statement(s).is_some())
    {
        return Ok(());
    }

    let acks = scan_statement_acks(path, sql)?;

    // The token-derived statement boundaries must align 1:1 with the parser's
    // statements (both walk the same tokens in source order; no DDL statement
    // we model carries a top-level `;`). A mismatch would mean the two views
    // disagree — never for valid parsed DDL — so it fails closed rather than
    // risk mis-attributing an acknowledgement to the wrong statement.
    if acks.len() != statements.len() {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "the destructive-migration acknowledgement scan found {} \
                 statement boundaries but the parser produced {} statements. \
                 This is a bsql-build bug; please report it with the migration \
                 that triggered it.",
                acks.len(),
                statements.len()
            ),
        });
    }

    for (statement, ack) in statements.iter().zip(&acks) {
        if let Some(description) = destructive_statement(statement)
            && !ack.acked
        {
            return Err(BuildError::UnackedDestructiveMigration {
                file: path.to_path_buf(),
                statement: description,
                line: ack.line,
            });
        }
    }
    Ok(())
}

/// Tokenize the migration (retaining comments and locations) and, for each
/// statement in source order, record its first-token line and whether an
/// acknowledgement marker sits in its leading trivia.
///
/// Uses the SQL tokenizer — never a hand-rolled scan — so a `;` inside a string
/// literal is not a separator, and the marker text inside a string literal is a
/// string token, not a comment: neither can spoof an acknowledgement. A
/// statement's leading trivia is the comments after the previous statement's
/// terminating `;` (or the file start), so an acknowledgement before a DIFFERENT
/// statement does not carry over — an intervening statement resets it.
fn scan_statement_acks(path: &Path, sql: &str) -> Result<Vec<StatementAck>, BuildError> {
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|err| BuildError::Parse {
            path: path.to_path_buf(),
            message: err.to_string(),
        })?;

    let mut acks: Vec<StatementAck> = Vec::new();
    // Whether the current statement's first significant token has been seen
    // (so later comments in it are NOT leading trivia for the next statement).
    let mut in_statement = false;
    // Whether an acknowledgement marker has appeared in the leading trivia of
    // the statement about to begin.
    let mut pending_ack = false;

    for token in &tokens {
        match &token.token {
            // End of the file's token stream; carries no statement.
            Token::EOF => {}
            // Whitespace and comments. A comment in LEADING position (before the
            // upcoming statement's first significant token) may acknowledge it.
            Token::Whitespace(ws) => {
                if !in_statement
                    && let Some(text) = comment_text(ws)
                    && comment_is_ack(text)
                {
                    pending_ack = true;
                }
            }
            // A statement separator: close the current statement and reset the
            // leading trivia for the next one (so an acknowledgement never
            // carries across a statement boundary).
            Token::SemiColon => {
                in_statement = false;
                pending_ack = false;
            }
            // Any other token is significant. The FIRST such token after a
            // separator (or the file start) begins a new statement; its leading
            // trivia — hence its acknowledgement — is whatever accumulated in
            // `pending_ack`.
            _ => {
                if !in_statement {
                    acks.push(StatementAck {
                        line: token.span.start.line,
                        acked: pending_ack,
                    });
                    in_statement = true;
                    pending_ack = false;
                }
            }
        }
    }
    Ok(acks)
}

/// The text content of a comment token (without its delimiters), or `None` for
/// non-comment whitespace. Both single-line (`-- ...`) and block (`/* ... */`)
/// comments can carry the acknowledgement marker.
fn comment_text(ws: &Whitespace) -> Option<&str> {
    match ws {
        Whitespace::SingleLineComment { comment, .. } => Some(comment),
        Whitespace::MultiLineComment(text) => Some(text),
        Whitespace::Space | Whitespace::Newline | Whitespace::Tab => None,
    }
}

/// Whether a comment's content is a valid destructive-migration acknowledgement:
/// its trimmed text must LEAD with the exact [`ACK_MARKER`] token, followed by
/// end-of-comment or ASCII whitespace (so an optional reason may follow). This
/// rejects a forged near-match (`bsql:ack-destructivex`) and a marker that is
/// not the comment's leading content (`reason: bsql:ack-destructive`).
fn comment_is_ack(content: &str) -> bool {
    match content.trim_start().strip_prefix(ACK_MARKER) {
        Some(rest) => rest.is_empty() || rest.starts_with(|c: char| c.is_ascii_whitespace()),
        None => false,
    }
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
            object_type: ObjectType::Table,
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
        // `DROP SCHEMA ... CASCADE` removes every table in the named schema.
        // This catalog admits only default-schema (`public`) tables — a
        // non-public table is a loud error at CREATE — so a CASCADE drop of
        // `public` removes the ENTIRE catalog, while dropping any other schema
        // removes tables the catalog never modeled (a faithful no-op here).
        // Modeling this keeps the catalog correct after the drop instead of
        // leaving stale tables a `query!` would resolve against a relation the
        // schema drop removed (the silently-wrong-catalog blind spot). A
        // non-CASCADE (RESTRICT / default) schema drop FAILS on a non-empty
        // schema, so it never silently removes modeled tables; leaving the
        // catalog unchanged is consistent with the server (the drop either
        // fails or removes an empty schema).
        Statement::Drop {
            object_type: ObjectType::Schema,
            names,
            cascade,
            ..
        } => {
            if cascade {
                for name in &names {
                    if drop_schema_targets_public(name) {
                        catalog.tables.clear();
                        catalog.primary_keys.clear();
                    }
                }
            }
            Ok(())
        }
        // `DROP DATABASE` and `TRUNCATE` require acknowledgement (they destroy
        // base data), but neither changes this catalog's table SHAPE: `TRUNCATE`
        // removes ROWS, not columns; `DROP DATABASE` targets a DIFFERENT
        // database object — a session cannot drop the database it is connected
        // to, which is the one these migrations build — so the modeled
        // database's tables are unchanged. A no-op here is faithful, not a
        // silent skip of shape this catalog models.
        Statement::Drop {
            object_type: ObjectType::Database,
            ..
        }
        | Statement::Truncate(_) => Ok(()),
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
        // `CREATE TYPE name AS ENUM ('a', 'b', ...)` — a user-defined enum.
        // Record its ordered label set so the query proc-macro can generate a
        // Rust `enum` and decode the columns that reference it. The name is
        // folded with the SAME rule a column's type name is (`object_name_leaf`
        // -> `fold_ident`), so a column typed `m mood` (canonicalised to
        // `mood`) resolves to this entry. The labels are string literals and
        // are CASE-SENSITIVE in PostgreSQL (unlike identifiers), so their exact
        // `value` is kept verbatim — never case-folded. A `CREATE TYPE` with a
        // non-enum representation (a composite `AS (...)`, a `RANGE`, or an SQL
        // definition) is not yet modeled; it passes through here unrecorded, so
        // a column using it stays a loud `UnsupportedPgType` at the query site
        // (fail-closed, never a silently-wrong catalog).
        Statement::CreateType {
            name,
            representation: Some(UserDefinedTypeRepresentation::Enum { labels }),
        } => {
            let type_name = object_name_leaf(&name);
            let labels: Vec<String> = labels.into_iter().map(|label| label.value).collect();
            catalog
                .user_types
                .insert(type_name, UserType::Enum { labels });
            Ok(())
        }
        // `CREATE TYPE name AS (field type, ...)` — a user-defined COMPOSITE (row)
        // type. Record its ordered field list (each attribute's case-folded name
        // and canonical type) so the query proc-macro can generate a Rust `struct`
        // and decode the columns that reference it. The field name is folded with
        // the SAME rule a column name is (`fold_ident`), and the field type is
        // canonicalised with the SAME rule a column type is (`canonical_type`), so
        // a field typed as another user type (an enum, a domain, or a nested
        // composite) is followed transitively at the query site. Field order is
        // preserved — it is the wire frame's field order.
        Statement::CreateType {
            name,
            representation: Some(UserDefinedTypeRepresentation::Composite { attributes }),
        } => {
            let type_name = object_name_leaf(&name);
            let fields: Vec<CompositeField> = attributes
                .into_iter()
                .map(|attr| CompositeField {
                    name: fold_ident(&attr.name),
                    pg_type: canonical_type(&attr.data_type),
                })
                .collect();
            catalog
                .user_types
                .insert(type_name, UserType::Composite { fields });
            Ok(())
        }
        // `ALTER TYPE name {ADD VALUE | RENAME VALUE | RENAME TO}` — evolve a
        // user enum. This MUST reach the catalog: a silent skip would leave the
        // label set out of sync with the migration FILES, defeating the
        // compile-time-drift guarantee (a later `ADD VALUE` the generated enum
        // silently lacked; a `RENAME VALUE` the generated variant mapped to a
        // label the live server rejects). `DROP TYPE` already mutates the
        // catalog; `ALTER TYPE` is its evolving peer.
        Statement::AlterType(AlterType { name, operation }) => {
            replay_alter_type(catalog, path, &name, operation)
        }
        // `CREATE DOMAIN name AS base [CHECK (...)]` — a constrained alias for a
        // base type. Record its canonical base so a column typed as the domain
        // resolves TRANSPARENTLY to the base's Rust type. The `CHECK` is
        // server-enforced and carries no client-side shape, so it is not
        // modeled (a domain's wire form IS its base's). The base is
        // canonicalised with the SAME rule a column type is, so `age AS INTEGER`
        // records base `int4`; a base that is itself a user type (another domain
        // or an enum) is followed transitively at the query site.
        Statement::CreateDomain(CreateDomain {
            name, data_type, ..
        }) => {
            let type_name = object_name_leaf(&name);
            let base = canonical_type(&data_type);
            catalog
                .user_types
                .insert(type_name, UserType::Domain { base });
            Ok(())
        }
        // `DROP TYPE name [, ...]` removes a user-defined type from the catalog
        // so a later same-named `CREATE TYPE` does not inherit stale labels and
        // a column referencing the dropped type resolves as unsupported (loud),
        // exactly as `DROP TABLE` re-keys tables. A `DROP TYPE` is not
        // data-destructive in the base-table sense (it removes a type
        // definition, and the server refuses it while any column still uses the
        // type), so it needs no destructive acknowledgement.
        Statement::Drop {
            object_type: ObjectType::Type,
            names,
            ..
        } => {
            for name in &names {
                catalog.user_types.remove(&object_name_leaf(name));
            }
            Ok(())
        }
        // `DROP DOMAIN name` is a DISTINCT statement in the grammar (not a
        // `DROP ... object_type = Domain`), so it removes the domain the same
        // way `DROP TYPE` removes an enum — keeping a later same-named type from
        // inheriting a stale base, and a column of the dropped domain resolving
        // as unsupported (loud).
        Statement::DropDomain(DropDomain { name, .. }) => {
            catalog.user_types.remove(&object_name_leaf(&name));
            Ok(())
        }
        // Statements without base-table column-shape meaning (CREATE
        // INDEX, seed INSERTs, CREATE/ALTER VIEW, COMMENT, GRANT, CREATE
        // SCHEMA/SEQUENCE, SET, etc.) carry no change to a tracked
        // table's columns, so passing them through is correct — not a
        // silent skip of schema information this catalog models.
        _ => Ok(()),
    }
}

/// Replay an `ALTER TYPE name ...` into the catalog. A `RENAME TO` re-keys ANY
/// modeled user type; `ADD VALUE` / `RENAME VALUE` mutate a modeled ENUM's label
/// set in place (preserving DECLARED ORDER, which the generated enum's derived
/// `Ord` mirrors). An `ALTER TYPE` on a name the catalog does not model is a
/// no-op (it is not one of our enums/domains); `ADD VALUE` / `RENAME VALUE` on a
/// modeled DOMAIN is a loud error (PostgreSQL rejects it — those ops are
/// enum-only), never a silent skip.
fn replay_alter_type(
    catalog: &mut Catalog,
    path: &Path,
    name: &ObjectName,
    operation: AlterTypeOperation,
) -> Result<(), BuildError> {
    let type_name = object_name_leaf(name);
    match operation {
        // `ALTER TYPE old RENAME TO new` — re-key the modeled type under its new
        // name (an enum OR a domain). A column referencing the OLD name stops
        // resolving; the NEW name resolves. A name we do not model is a no-op.
        AlterTypeOperation::Rename(AlterTypeRename { new_name }) => {
            if let Some(ty) = catalog.user_types.remove(&type_name) {
                catalog.user_types.insert(fold_ident(&new_name), ty);
            }
            Ok(())
        }
        // `ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'v' [BEFORE|AFTER 'n']`.
        AlterTypeOperation::AddValue(add) => {
            alter_type_add_value(catalog, path, &type_name, add)
        }
        // `ALTER TYPE name RENAME VALUE 'from' TO 'to'`.
        AlterTypeOperation::RenameValue(AlterTypeRenameValue { from, to }) => {
            alter_type_rename_value(catalog, path, &type_name, &from.value, to.value)
        }
    }
}

/// Apply `ALTER TYPE name ADD VALUE` to a modeled enum's label set, honoring the
/// optional `BEFORE`/`AFTER` position so DECLARED ORDER (PostgreSQL's enum sort
/// order, which the generated enum's derived `Ord` mirrors) stays correct.
fn alter_type_add_value(
    catalog: &mut Catalog,
    path: &Path,
    type_name: &str,
    add: AlterTypeAddValue,
) -> Result<(), BuildError> {
    let AlterTypeAddValue {
        if_not_exists,
        value,
        position,
    } = add;
    let labels = match catalog.user_types.get_mut(type_name) {
        Some(UserType::Enum { labels }) => labels,
        // `ADD VALUE` on a domain or a composite is invalid in PostgreSQL — a
        // loud error, never a silent skip of a migration the live server would
        // reject (ADD VALUE is enum-only).
        Some(UserType::Domain { .. }) => {
            return Err(BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "`ALTER TYPE {type_name} ADD VALUE` targets a DOMAIN; ADD VALUE \
                     is only valid on an enum type."
                ),
            });
        }
        Some(UserType::Composite { .. }) => {
            return Err(BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "`ALTER TYPE {type_name} ADD VALUE` targets a COMPOSITE; ADD VALUE \
                     is only valid on an enum type."
                ),
            });
        }
        // Not a modeled type — nothing to evolve (a native/unmodeled type).
        None => return Ok(()),
    };
    let new_label = value.value;
    if labels.iter().any(|existing| existing == &new_label) {
        // Duplicate. `IF NOT EXISTS` makes it a no-op; otherwise PostgreSQL
        // errors, so the migration is loud here too (never a silent skip).
        return if if_not_exists {
            Ok(())
        } else {
            Err(BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "`ALTER TYPE {type_name} ADD VALUE '{new_label}'`: the label \
                     already exists (use `IF NOT EXISTS` for an idempotent add)."
                ),
            })
        };
    }
    // The insertion index, honoring BEFORE/AFTER. A neighbor that does not exist
    // is a loud error (PostgreSQL rejects it), never a silent append.
    let insert_at = match position {
        None => labels.len(),
        Some(AlterTypeAddValuePosition::Before(neighbor)) => {
            alter_type_neighbor_index(labels, &neighbor.value, path, type_name, "BEFORE")?
        }
        Some(AlterTypeAddValuePosition::After(neighbor)) => {
            alter_type_neighbor_index(labels, &neighbor.value, path, type_name, "AFTER")?
                .saturating_add(1)
        }
    };
    labels.insert(insert_at, new_label);
    Ok(())
}

/// The index of a `BEFORE`/`AFTER` neighbor label in an enum's label set, or a
/// loud [`BuildError::Replay`] when the neighbor does not exist (PostgreSQL
/// rejects an `ADD VALUE ... BEFORE/AFTER` naming an absent value).
fn alter_type_neighbor_index(
    labels: &[String],
    neighbor: &str,
    path: &Path,
    type_name: &str,
    keyword: &str,
) -> Result<usize, BuildError> {
    labels
        .iter()
        .position(|existing| existing == neighbor)
        .ok_or_else(|| BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "`ALTER TYPE {type_name} ADD VALUE ... {keyword} '{neighbor}'`: the \
                 neighbor label `{neighbor}` does not exist in the enum."
            ),
        })
}

/// Apply `ALTER TYPE name RENAME VALUE 'from' TO 'to'` to a modeled enum:
/// relabel IN PLACE at the same index (so declared order is preserved). A
/// missing `from`, a colliding `to`, or a non-enum target is a loud error.
fn alter_type_rename_value(
    catalog: &mut Catalog,
    path: &Path,
    type_name: &str,
    from: &str,
    to: String,
) -> Result<(), BuildError> {
    let labels = match catalog.user_types.get_mut(type_name) {
        Some(UserType::Enum { labels }) => labels,
        Some(UserType::Domain { .. }) => {
            return Err(BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "`ALTER TYPE {type_name} RENAME VALUE` targets a DOMAIN; RENAME \
                     VALUE is only valid on an enum type."
                ),
            });
        }
        Some(UserType::Composite { .. }) => {
            return Err(BuildError::Replay {
                path: path.to_path_buf(),
                message: format!(
                    "`ALTER TYPE {type_name} RENAME VALUE` targets a COMPOSITE; RENAME \
                     VALUE is only valid on an enum type."
                ),
            });
        }
        None => return Ok(()),
    };
    // The target must not collide with a DIFFERENT existing label (a no-op
    // rename to itself is allowed). PostgreSQL rejects a colliding rename.
    if to != from && labels.iter().any(|existing| existing == &to) {
        return Err(BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "`ALTER TYPE {type_name} RENAME VALUE '{from}' TO '{to}'`: the \
                 target label `{to}` already exists."
            ),
        });
    }
    let index = labels
        .iter()
        .position(|existing| existing == from)
        .ok_or_else(|| BuildError::Replay {
            path: path.to_path_buf(),
            message: format!(
                "`ALTER TYPE {type_name} RENAME VALUE '{from}' TO ...`: the source \
                 label `{from}` does not exist in the enum."
            ),
        })?;
    if let Some(slot) = labels.get_mut(index) {
        *slot = to;
    }
    Ok(())
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
    // Rendering the array form explicitly as `<element>[]` is what lets
    // `rust_type_for_pg` decide the boundary correctly: a ONE-dimensional array
    // of a SUPPORTED element (`int4[]`, `text[]`, `uuid[]`, …) resolves to
    // `RustType::Array(element)` (decoding to `Vec<Option<T>>`); a
    // MULTI-dimensional array (`int4[][]`, whose element still renders with a
    // `[]` suffix) and an array of an UNSUPPORTED element (`numeric[]`) stay a
    // loud `UnsupportedPgType`, and an array-typed `$N` parameter is a loud
    // `ArrayParam`. The structural rendering is what makes that split reliable —
    // a head-word split would drop the `[]` (and the zone) and silently collapse
    // an array column to its scalar element (a `timestamptz[]` typed as a scalar
    // 8-byte `timestamp`), a mis-type this arm forecloses.
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
    // `TIME WITH TIME ZONE` (`timetz`) is a DISTINCT PostgreSQL type from the
    // naive `time`, but its rendered head word is `time` — a head-word split
    // would silently collapse the zone away and type a `timetz` column as the
    // zone-less `time` (a wrong 12-byte-vs-8-byte wire shape). Match the parsed
    // `TimezoneInfo` structurally: the zoned spelling canonicalises to `timetz`
    // (which has no supported native pivot, so it stays a loud
    // `UnsupportedPgType`), the zone-less one to `time`. The optional precision
    // (`TIME(3)`) is a typmod that does not change the type OID, so it is
    // dropped by keeping only the type name.
    if let DataType::Time(_, tz) = data_type {
        return match tz {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => "timetz",
            TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => "time",
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

/// Whether a `DROP SCHEMA` names the single namespace this catalog models — the
/// default `public` schema — using the same case rule as everywhere else
/// ([`schema_part_folds_to_public`]). A `DROP SCHEMA public CASCADE` removes
/// every modeled table (the replay admits only default-schema tables), so this
/// decides whether such a drop clears the catalog. A multi-part or
/// non-identifier schema path (`db.app`) names a namespace the catalog does not
/// model, so it targets no modeled table here.
fn drop_schema_targets_public(name: &ObjectName) -> bool {
    match name.0.as_slice() {
        [only] => only.as_ident().is_some_and(schema_part_folds_to_public),
        _ => false,
    }
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

    /// Prefix a destructive statement with the acknowledgement marker, so a
    /// replay-SEMANTICS test (not testing the destructive gate itself) passes
    /// the gate. The dedicated gate tests below exercise the acknowledged /
    /// unacknowledged / spoofed paths directly.
    fn acked(sql: &str) -> String {
        format!("{ACK_MARKER_SYNTAX}\n{sql}")
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
            acked("ALTER TABLE t DROP COLUMN a").as_str(),
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
        let cat = catalog_from(&["CREATE TABLE t (a INT)", acked("DROP TABLE t").as_str()]);
        assert!(!cat.tables.contains_key("t"));
    }

    // ── User-defined ENUM types ────────────────────────────────────────

    #[test]
    fn create_type_enum_records_ordered_labels() {
        let cat = catalog_from(&["CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok')"]);
        match cat.user_types.get("mood") {
            Some(UserType::Enum { labels }) => {
                assert_eq!(labels, &["happy", "sad", "ok"], "labels in declared order");
            }
            other => panic!("expected a mood enum, got {other:?}"),
        }
    }

    #[test]
    fn enum_labels_are_case_sensitive() {
        // Enum LABELS are string literals — case-sensitive, unlike identifiers.
        let cat = catalog_from(&["CREATE TYPE status AS ENUM ('Active', 'INACTIVE')"]);
        match cat.user_types.get("status") {
            Some(UserType::Enum { labels }) => {
                assert_eq!(labels, &["Active", "INACTIVE"], "exact label case kept");
            }
            other => panic!("expected a status enum, got {other:?}"),
        }
    }

    #[test]
    fn drop_type_removes_it() {
        let cat = catalog_from(&[
            "CREATE TYPE mood AS ENUM ('happy')",
            "DROP TYPE mood",
        ]);
        assert!(!cat.user_types.contains_key("mood"), "dropped type is gone");
    }

    // ── ALTER TYPE (enum evolution must reach the catalog) ─────────────

    fn enum_labels<'a>(cat: &'a Catalog, name: &str) -> &'a [String] {
        match cat.user_types.get(name) {
            Some(UserType::Enum { labels }) => labels.as_slice(),
            other => panic!("expected an enum `{name}`, got {other:?}"),
        }
    }

    #[test]
    fn alter_type_add_value_appends_and_positions_preserve_declared_order() {
        // Declared order is PostgreSQL's enum sort order (the generated `Ord`
        // mirrors it), so ADD VALUE must honor BEFORE/AFTER and append.
        let cat = catalog_from(&[
            "CREATE TYPE priority AS ENUM ('low', 'high')",
            "ALTER TYPE priority ADD VALUE 'medium' AFTER 'low'", // [low, medium, high]
            "ALTER TYPE priority ADD VALUE 'urgent'",             // append -> [.., urgent]
            "ALTER TYPE priority ADD VALUE 'lowest' BEFORE 'low'", // [lowest, low, ..]
        ]);
        assert_eq!(
            enum_labels(&cat, "priority"),
            &["lowest", "low", "medium", "high", "urgent"]
        );
    }

    #[test]
    fn alter_type_rename_value_relabels_in_place() {
        let cat = catalog_from(&[
            "CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok')",
            "ALTER TYPE mood RENAME VALUE 'sad' TO 'unhappy'", // same index
        ]);
        assert_eq!(enum_labels(&cat, "mood"), &["happy", "unhappy", "ok"]);
    }

    #[test]
    fn alter_type_rename_to_rekeys_the_type() {
        let cat = catalog_from(&[
            "CREATE TYPE tshirt AS ENUM ('s', 'm', 'l')",
            "ALTER TYPE tshirt RENAME TO garment_size",
        ]);
        assert!(!cat.user_types.contains_key("tshirt"), "old name gone");
        assert_eq!(enum_labels(&cat, "garment_size"), &["s", "m", "l"]);
    }

    #[test]
    fn alter_type_add_value_if_not_exists_is_idempotent() {
        let cat = catalog_from(&[
            "CREATE TYPE t AS ENUM ('a', 'b')",
            "ALTER TYPE t ADD VALUE IF NOT EXISTS 'a'",
        ]);
        assert_eq!(enum_labels(&cat, "t"), &["a", "b"]);
    }

    #[test]
    fn alter_type_add_duplicate_without_if_not_exists_is_loud() {
        let msg = replay_err(&[
            "CREATE TYPE t AS ENUM ('a', 'b')",
            "ALTER TYPE t ADD VALUE 'a'",
        ]);
        assert!(msg.contains("already exists"), "got: {msg}");
    }

    #[test]
    fn alter_type_add_value_unknown_neighbor_is_loud() {
        let msg = replay_err(&[
            "CREATE TYPE t AS ENUM ('a')",
            "ALTER TYPE t ADD VALUE 'b' AFTER 'nonexistent'",
        ]);
        assert!(msg.contains("neighbor"), "got: {msg}");
    }

    #[test]
    fn alter_type_rename_value_unknown_source_is_loud() {
        let msg = replay_err(&[
            "CREATE TYPE t AS ENUM ('a')",
            "ALTER TYPE t RENAME VALUE 'nope' TO 'b'",
        ]);
        assert!(msg.contains("does not exist"), "got: {msg}");
    }

    #[test]
    fn alter_type_add_value_on_a_domain_is_loud() {
        let msg = replay_err(&[
            "CREATE DOMAIN d AS int",
            "ALTER TYPE d ADD VALUE 'x'",
        ]);
        assert!(msg.contains("DOMAIN"), "got: {msg}");
    }

    #[test]
    fn alter_type_on_an_unmodeled_name_is_a_noop() {
        // An ALTER TYPE naming a type the catalog does not model (e.g. a
        // composite, unrecorded) is a no-op, not a crash or a loud error.
        let cat = catalog_from(&[
            "CREATE TABLE t (a int)",
            "ALTER TYPE nonexistent ADD VALUE 'x'",
        ]);
        assert!(!cat.user_types.contains_key("nonexistent"));
    }

    #[test]
    fn column_of_enum_type_resolves_to_a_user_enum_id() {
        // A column declared with the enum's name resolves through the same
        // canonical folding, so its `pg_type` matches the `user_types` key.
        let cat = catalog_from(&[
            "CREATE TYPE mood AS ENUM ('happy', 'sad')",
            "CREATE TABLE feelings (id INT, m mood)",
        ]);
        assert_eq!(cat.tables["feelings"]["m"].pg_type, "mood");
        let id = cat.user_enum_id("mood").expect("mood resolves to an id");
        let (name, labels) = cat.user_enum(id).expect("id round-trips");
        assert_eq!(name, "mood");
        assert_eq!(labels, &["happy", "sad"]);
    }

    #[test]
    fn user_enum_id_is_none_for_a_non_enum_name() {
        let cat = catalog_from(&["CREATE TYPE mood AS ENUM ('happy')"]);
        assert!(cat.user_enum_id("nonexistent").is_none());
        assert!(cat.user_enum_id("int4").is_none(), "a native type is not a user enum");
    }

    #[test]
    fn user_types_channel_round_trips() {
        let cat = catalog_from(&[
            "CREATE TYPE mood AS ENUM ('happy', 'sad')",
            "CREATE TYPE priority AS ENUM ('lo', 'hi')",
        ]);
        let text = serialize_user_types(&cat.user_types);
        let parsed = parse_user_types(&text).expect("round-trip parses");
        assert_eq!(parsed, cat.user_types);
    }

    #[test]
    fn user_types_channel_escapes_tabs_and_newlines_in_labels() {
        // An enum label is a string literal and may contain the channel's own
        // delimiters; escaping keeps them round-tripping (universal, not a
        // fail-loud rejection). A label built directly (sqlparser would accept
        // the equivalent quoted literal).
        let mut user_types = BTreeMap::new();
        user_types.insert(
            "weird".to_string(),
            UserType::Enum {
                labels: vec!["a\tb".to_string(), "c\nd".to_string(), "e\\f".to_string()],
            },
        );
        let text = serialize_user_types(&user_types);
        let parsed = parse_user_types(&text).expect("escaped labels round-trip");
        assert_eq!(parsed, user_types);
    }

    #[test]
    fn empty_user_types_serialize_to_empty_and_parse_back() {
        let empty: BTreeMap<String, UserType> = BTreeMap::new();
        assert_eq!(serialize_user_types(&empty), "");
        assert_eq!(parse_user_types("").expect("empty parses"), empty);
    }

    #[test]
    fn user_types_parse_rejects_unknown_kind() {
        let err = parse_user_types("X\tmood\thappy").expect_err("unknown kind is loud");
        assert!(matches!(err, UserTypesParseError::UnknownKind { .. }));
    }

    // ── User-defined DOMAIN types ──────────────────────────────────────

    #[test]
    fn create_domain_records_canonical_base() {
        // The base canonicalises with the same rule a column type does:
        // `INTEGER` -> `int4`, `VARCHAR(50)` -> `varchar`.
        let cat = catalog_from(&[
            "CREATE DOMAIN age AS INTEGER CHECK (VALUE >= 0)",
            "CREATE DOMAIN username AS VARCHAR(50)",
        ]);
        match cat.user_types.get("age") {
            Some(UserType::Domain { base }) => assert_eq!(base, "int4"),
            other => panic!("expected an age domain, got {other:?}"),
        }
        match cat.user_types.get("username") {
            Some(UserType::Domain { base }) => assert_eq!(base, "varchar"),
            other => panic!("expected a username domain, got {other:?}"),
        }
    }

    #[test]
    fn drop_type_removes_a_domain() {
        let cat = catalog_from(&["CREATE DOMAIN age AS int", "DROP DOMAIN age"]);
        assert!(!cat.user_types.contains_key("age"));
    }

    #[test]
    fn domain_and_enum_channel_round_trip_together() {
        let cat = catalog_from(&[
            "CREATE TYPE mood AS ENUM ('happy', 'sad')",
            "CREATE DOMAIN age AS int",
            "CREATE DOMAIN email AS text",
        ]);
        let text = serialize_user_types(&cat.user_types);
        let parsed = parse_user_types(&text).expect("round-trip");
        assert_eq!(parsed, cat.user_types);
    }

    #[test]
    fn domain_parse_rejects_wrong_field_count() {
        let err = parse_user_types("D\tage").expect_err("a domain needs name + base");
        assert!(matches!(err, UserTypesParseError::MalformedDomain { fields: 1, .. }));
    }

    // ── User-defined COMPOSITE types ───────────────────────────────────

    #[test]
    fn create_composite_records_ordered_canonical_fields() {
        // Field names fold and field types canonicalise with the SAME rules a
        // column does (`STREET` -> `street`, `INTEGER` -> `int4`), and declared
        // ORDER is preserved (it is the wire frame's field order).
        let cat = catalog_from(&["CREATE TYPE addr AS (STREET text, Zip INTEGER, tag VARCHAR(8))"]);
        match cat.user_types.get("addr") {
            Some(UserType::Composite { fields }) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], CompositeField { name: "street".into(), pg_type: "text".into() });
                assert_eq!(fields[1], CompositeField { name: "zip".into(), pg_type: "int4".into() });
                assert_eq!(fields[2], CompositeField { name: "tag".into(), pg_type: "varchar".into() });
            }
            other => panic!("expected an addr composite, got {other:?}"),
        }
    }

    #[test]
    fn drop_type_removes_a_composite() {
        let cat = catalog_from(&["CREATE TYPE addr AS (street text)", "DROP TYPE addr"]);
        assert!(!cat.user_types.contains_key("addr"));
    }

    #[test]
    fn alter_type_rename_to_rekeys_a_composite() {
        // `RENAME TO` re-keys ANY modeled user type — a composite included, via
        // the generic re-key. (Composite ATTRIBUTE-level ALTERs are not modelled
        // by sqlparser, so they are a loud parse error, never a silent skip.)
        let cat = catalog_from(&[
            "CREATE TYPE addr AS (street text, zip int4)",
            "ALTER TYPE addr RENAME TO postal",
        ]);
        assert!(!cat.user_types.contains_key("addr"));
        assert!(matches!(
            cat.user_types.get("postal"),
            Some(UserType::Composite { .. })
        ));
    }

    #[test]
    fn composite_add_attribute_is_a_loud_parse_error_fail_closed() {
        // `ALTER TYPE addr ADD ATTRIBUTE country text` (and DROP / ALTER / RENAME
        // ATTRIBUTE) is a composite ATTRIBUTE-level evolution the pinned
        // `sqlparser` grammar does NOT model (it parses only enum `ALTER TYPE`
        // ops: RENAME TO / ADD VALUE / RENAME VALUE). A form the replay cannot
        // model faithfully is a LOUD build error, never a silently-stale struct —
        // exactly the catalog boundary. This LOCKS that fail-closed behavior.
        let mut cat = Catalog::default();
        replay_file(
            &mut cat,
            &PathBuf::from("t0.sql"),
            "CREATE TYPE addr AS (street text, zip int4)",
        )
        .expect("create composite");
        let err = replay_file(
            &mut cat,
            &PathBuf::from("t1.sql"),
            "ALTER TYPE addr ADD ATTRIBUTE country text",
        )
        .expect_err("ADD ATTRIBUTE is not modeled by sqlparser — a loud parse error");
        assert!(
            matches!(err, BuildError::Parse { .. }),
            "expected a loud BuildError::Parse for ADD ATTRIBUTE, got {err:?}"
        );
        // The catalog is UNCHANGED — the build fails, so no silently-stale struct
        // is emitted (the composite keeps its declared 2-field set).
        match cat.user_types.get("addr") {
            Some(UserType::Composite { fields }) => assert_eq!(fields.len(), 2),
            other => panic!("expected the addr composite intact, got {other:?}"),
        }
    }

    #[test]
    fn nested_and_enum_and_composite_channel_round_trip_together() {
        let cat = catalog_from(&[
            "CREATE TYPE mood AS ENUM ('happy', 'sad')",
            "CREATE TYPE addr AS (street text, zip int4)",
            "CREATE TYPE person AS (name text, home addr, feeling mood)",
            "CREATE DOMAIN email AS text",
        ]);
        let text = serialize_user_types(&cat.user_types);
        let parsed = parse_user_types(&text).expect("round-trip");
        assert_eq!(parsed, cat.user_types);
    }

    #[test]
    fn composite_channel_escapes_delimiters_in_field_names() {
        // A quoted attribute name may carry the channel's own delimiters;
        // escaping keeps them round-tripping (universal, not a fail-loud reject).
        let mut user_types = BTreeMap::new();
        user_types.insert(
            "weird".to_string(),
            UserType::Composite {
                fields: vec![
                    CompositeField { name: "a\tb".into(), pg_type: "text".into() },
                    CompositeField { name: "c\nd".into(), pg_type: "int4".into() },
                ],
            },
        );
        let text = serialize_user_types(&user_types);
        let parsed = parse_user_types(&text).expect("escaped composite fields round-trip");
        assert_eq!(parsed, user_types);
    }

    #[test]
    fn composite_parse_rejects_odd_field_count() {
        // The attribute fields must be (name, type) PAIRS — an odd count is a
        // corrupt channel, loud not silent.
        let err = parse_user_types("C\taddr\tstreet\ttext\tzip")
            .expect_err("a composite needs even attribute fields");
        assert!(matches!(err, UserTypesParseError::MalformedComposite { fields: 3, .. }));
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
            acked("ALTER TABLE t DROP COLUMN a").as_str(),
        ]);
        assert!(!cat.primary_keys.contains_key("t"), "key removed with col");
    }

    #[test]
    fn dropping_one_composite_key_column_drops_the_whole_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT, b INT, c TEXT, PRIMARY KEY (a, b))",
            acked("ALTER TABLE t DROP COLUMN a").as_str(),
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
            acked("DROP TABLE t").as_str(),
        ]);
        assert!(!cat.primary_keys.contains_key("t"));
    }

    #[test]
    fn recreating_a_dropped_table_does_not_inherit_a_stale_key() {
        let cat = catalog_from(&[
            "CREATE TABLE t (a INT PRIMARY KEY, b INT)",
            acked("DROP TABLE t").as_str(),
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
        // Acknowledged, so the gate passes and the replay reaches the
        // fail-closed "no such column" error this test pins.
        let err = replay_file(&mut cat, Path::new("b.sql"), &acked("ALTER TABLE t DROP COLUMN gone"))
            .expect_err("must fail closed");
        match err {
            BuildError::Replay { message, .. } => assert!(message.contains("no such column")),
            other => panic!("wrong error: {other:?}"),
        }
    }

    // ── Destructive-migration acknowledgement gate ─────────────────────

    /// Replay one migration file's SQL into a fresh catalog, returning the
    /// error (panicking if it unexpectedly succeeds).
    fn gate_err(sql: &str) -> BuildError {
        let mut cat = Catalog::default();
        replay_file(&mut cat, Path::new("m.sql"), sql).expect_err("expected a fail-closed error")
    }

    /// Replay one migration file's SQL into a fresh catalog (panicking on any
    /// error), returning the catalog — for asserting an ACKNOWLEDGED
    /// destructive statement's effect on the schema.
    fn gate_ok(sql: &str) -> Catalog {
        let mut cat = Catalog::default();
        replay_file(&mut cat, Path::new("m.sql"), sql).expect("replay");
        cat
    }

    /// Assert the error is an unacknowledged-destructive error whose
    /// description contains `needle`, returning the reported source line.
    fn expect_unacked(err: BuildError, needle: &str) -> u64 {
        match err {
            BuildError::UnackedDestructiveMigration {
                statement, line, ..
            } => {
                assert!(
                    statement.contains(needle),
                    "description `{statement}` missing `{needle}`"
                );
                line
            }
            other => panic!("expected UnackedDestructiveMigration, got: {other:?}"),
        }
    }

    #[test]
    fn unacked_drop_table_fails_the_build() {
        let line = expect_unacked(
            gate_err("CREATE TABLE t (a INT);\nDROP TABLE t;"),
            "DROP TABLE t",
        );
        assert_eq!(line, 2, "the DROP is on line 2");
    }

    #[test]
    fn unacked_drop_column_fails_the_build() {
        expect_unacked(
            gate_err("CREATE TABLE t (a INT, b INT);\nALTER TABLE t DROP COLUMN a;"),
            "ALTER TABLE t DROP COLUMN a",
        );
    }

    #[test]
    fn unacked_drop_table_if_exists_still_fails() {
        // `IF EXISTS` does not change destructiveness: if the table exists, its
        // data is destroyed.
        expect_unacked(gate_err("DROP TABLE IF EXISTS t;"), "DROP TABLE t");
    }

    #[test]
    fn acked_drop_table_removes_it_from_the_catalog() {
        // The acknowledged DROP passes the gate AND still replays: the table is
        // gone from the catalog.
        let cat = gate_ok("CREATE TABLE t (a INT);\n-- bsql:ack-destructive\nDROP TABLE t;");
        assert!(!cat.tables.contains_key("t"), "acked drop removed the table");
    }

    #[test]
    fn acked_drop_column_removes_it_from_the_catalog() {
        let cat = gate_ok(
            "CREATE TABLE t (a INT, b INT);\n\
             -- bsql:ack-destructive\n\
             ALTER TABLE t DROP COLUMN a;",
        );
        assert!(!cat.tables["t"].contains_key("a"), "column a dropped");
        assert!(cat.tables["t"].contains_key("b"), "column b kept");
    }

    #[test]
    fn ack_with_a_trailing_reason_counts() {
        let cat = gate_ok(
            "CREATE TABLE t (a INT);\n\
             -- bsql:ack-destructive dropped after export to cold storage\n\
             DROP TABLE t;",
        );
        assert!(!cat.tables.contains_key("t"));
    }

    #[test]
    fn ack_in_a_block_comment_counts() {
        let cat = gate_ok("CREATE TABLE t (a INT);\n/* bsql:ack-destructive */\nDROP TABLE t;");
        assert!(!cat.tables.contains_key("t"));
    }

    #[test]
    fn drop_column_among_other_ops_needs_one_ack() {
        // One acknowledgement covers a statement whose ops include a DROP
        // COLUMN; the remaining ops still apply.
        let cat = gate_ok(
            "CREATE TABLE t (a INT, b INT);\n\
             -- bsql:ack-destructive\n\
             ALTER TABLE t DROP COLUMN a, ADD COLUMN c INT;",
        );
        assert!(!cat.tables["t"].contains_key("a"), "a dropped");
        assert!(cat.tables["t"].contains_key("c"), "c added");

        // Without the acknowledgement, the same statement fails.
        expect_unacked(
            gate_err("CREATE TABLE t (a INT, b INT);\nALTER TABLE t DROP COLUMN a, ADD COLUMN c INT;"),
            "DROP COLUMN a",
        );
    }

    #[test]
    fn non_destructive_migrations_need_no_ack() {
        // CREATE, ADD COLUMN, RENAME TABLE / COLUMN, SET / DROP NOT NULL, and a
        // (deferred) ALTER COLUMN TYPE all preserve data — none needs an
        // acknowledgement, so over-flagging them (which would train blanket
        // acking) is foreclosed.
        let cat = gate_ok(
            "CREATE TABLE t (a INT NOT NULL, b INT);\n\
             ALTER TABLE t ADD COLUMN c TEXT NOT NULL;\n\
             ALTER TABLE t ALTER COLUMN a DROP NOT NULL;\n\
             ALTER TABLE t ALTER COLUMN b SET NOT NULL;\n\
             ALTER TABLE t ALTER COLUMN b TYPE BIGINT;\n\
             ALTER TABLE t RENAME COLUMN c TO d;\n\
             ALTER TABLE t RENAME TO u;",
        );
        let u = cat.tables.get("u").expect("renamed table u");
        assert!(u.contains_key("a") && u.contains_key("b") && u.contains_key("d"));
    }

    #[test]
    fn unacked_drop_schema_cascade_fails_the_build() {
        expect_unacked(
            gate_err("CREATE TABLE t (a INT);\nDROP SCHEMA public CASCADE;"),
            "DROP SCHEMA public CASCADE",
        );
    }

    #[test]
    fn unacked_truncate_fails_the_build() {
        expect_unacked(
            gate_err("CREATE TABLE t (a INT);\nTRUNCATE t;"),
            "TRUNCATE t",
        );
        // The `TRUNCATE TABLE a, b` spelling is flagged too.
        expect_unacked(
            gate_err("CREATE TABLE a (x INT);\nCREATE TABLE b (y INT);\nTRUNCATE TABLE a, b;"),
            "TRUNCATE a, b",
        );
    }

    #[test]
    fn unacked_drop_database_fails_the_build() {
        expect_unacked(gate_err("DROP DATABASE olddb;"), "DROP DATABASE olddb");
    }

    #[test]
    fn acked_truncate_succeeds_and_keeps_the_table_shape() {
        // TRUNCATE removes ROWS, not the table's {column} shape, so the catalog
        // is unchanged (a faithful no-op) — the table still resolves.
        let cat = gate_ok("CREATE TABLE t (a INT, b TEXT);\n-- bsql:ack-destructive\nTRUNCATE t;");
        let t = cat.tables.get("t").expect("t still present after TRUNCATE");
        assert!(t.contains_key("a") && t.contains_key("b"));
    }

    #[test]
    fn acked_drop_database_succeeds_and_keeps_the_catalog() {
        // DROP DATABASE targets a DIFFERENT database object than the one the
        // migrations build, so the modeled tables are untouched (faithful).
        let cat = gate_ok("CREATE TABLE t (a INT);\n-- bsql:ack-destructive\nDROP DATABASE olddb;");
        assert!(cat.tables.contains_key("t"), "modeled table unaffected");
    }

    #[test]
    fn acked_drop_schema_public_cascade_clears_the_catalog() {
        // The catalog admits only default-schema tables, so DROP SCHEMA public
        // CASCADE removes ALL of them — the replay MODELS this (rather than
        // leaving a silently-wrong catalog with stale tables). A table created
        // AFTER the drop resolves; one from before does not.
        let cat = gate_ok(
            "CREATE TABLE gone_a (x INT);\n\
             CREATE TABLE gone_b (y INT PRIMARY KEY);\n\
             -- bsql:ack-destructive\n\
             DROP SCHEMA public CASCADE;\n\
             CREATE TABLE kept (z INT);",
        );
        assert!(!cat.tables.contains_key("gone_a"), "pre-drop table removed");
        assert!(!cat.tables.contains_key("gone_b"), "pre-drop table removed");
        assert!(
            !cat.primary_keys.contains_key("gone_b"),
            "pre-drop primary key removed"
        );
        assert!(cat.tables.contains_key("kept"), "post-drop table resolves");
    }

    #[test]
    fn acked_drop_schema_nonpublic_cascade_keeps_modeled_tables() {
        // A CASCADE drop of a schema the catalog does not model touches no
        // modeled table (all modeled tables live in the default schema).
        let cat = gate_ok(
            "CREATE TABLE t (a INT);\n\
             -- bsql:ack-destructive\n\
             DROP SCHEMA archived CASCADE;",
        );
        assert!(cat.tables.contains_key("t"), "default-schema table kept");
    }

    #[test]
    fn deliberately_excluded_drops_need_no_ack() {
        // DROP SCHEMA without CASCADE (RESTRICT / default fails on a non-empty
        // schema), and DROP of a DERIVED/virtual object (materialized view,
        // view, index) whose rows are reconstructible from its definition, are
        // deliberately NOT flagged — none needs an acknowledgement.
        for sql in [
            "DROP SCHEMA empty_ns;",
            "DROP SCHEMA empty_ns RESTRICT;",
            "DROP MATERIALIZED VIEW mv;",
            "DROP VIEW v;",
            "DROP INDEX idx;",
        ] {
            let mut cat = Catalog::default();
            replay_file(&mut cat, Path::new("m.sql"), sql)
                .expect("excluded drop needs no acknowledgement");
        }
    }

    #[test]
    fn ack_marker_inside_a_string_literal_does_not_count() {
        // The marker text lives inside a prior statement's STRING LITERAL, not
        // in the DROP's leading comment — the tokenizer classifies it as a
        // string token, so it cannot forge an acknowledgement.
        expect_unacked(
            gate_err(
                "CREATE TABLE audit (msg TEXT);\n\
                 INSERT INTO audit (msg) VALUES ('-- bsql:ack-destructive');\n\
                 DROP TABLE audit;",
            ),
            "DROP TABLE audit",
        );
    }

    #[test]
    fn ack_before_a_different_statement_does_not_count() {
        // The acknowledgement precedes the CREATE, not the DROP; an intervening
        // statement resets the leading trivia, so the DROP is unacknowledged.
        expect_unacked(
            gate_err(
                "-- bsql:ack-destructive\n\
                 CREATE TABLE t (a INT);\n\
                 DROP TABLE t;",
            ),
            "DROP TABLE t",
        );
    }

    #[test]
    fn forged_near_match_marker_does_not_count() {
        // A near-match that is not the exact marker token is not an
        // acknowledgement.
        for forged in [
            "-- bsql:ack-destructivexyz\nDROP TABLE t;",
            "-- please bsql:ack-destructive\nDROP TABLE t;",
            "-- ack-destructive\nDROP TABLE t;",
        ] {
            expect_unacked(gate_err(forged), "DROP TABLE t");
        }
    }

    #[test]
    fn unacked_error_message_is_actionable() {
        let err = gate_err("CREATE TABLE t (a INT);\nDROP TABLE t;");
        let rendered = err.to_string();
        // Names the file, the line, the destructive statement, and the exact
        // acknowledgement syntax to add.
        assert!(rendered.contains("m.sql"), "names the file: {rendered}");
        assert!(rendered.contains("line 2"), "names the line: {rendered}");
        assert!(rendered.contains("DROP TABLE t"), "names the statement: {rendered}");
        assert!(
            rendered.contains("-- bsql:ack-destructive"),
            "spells the ack syntax: {rendered}"
        );
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
        // Acknowledged, so the gate passes and the replay reaches the
        // schema-qualification error this test pins.
        let msg = replay_err(&[
            "CREATE TABLE widgets (wid INT)",
            acked("DROP TABLE wrongschema.widgets").as_str(),
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

    #[test]
    fn build_sort_agrees_with_the_runner_at_the_dot_slash_boundary() {
        // A nested prefix collision: `a.sql` and `a/b.sql`. A raw `PathBuf` sort
        // orders these `[a/b.sql, a.sql]` (component-wise: "a" < "a.sql"), but the
        // runtime runner sorts by the relative-name STRING — where `.` (0x2E)
        // < `/` (0x2F) — giving `[a.sql, a/b.sql]`. The build must match the
        // runner (ONE ordering authority), so `scan_sql_tree` yields the STRING
        // order, and the build-validated catalog order == the apply order.
        let tmp = TempDir::new("dotslash");
        let root = &tmp.path;
        std::fs::create_dir_all(root.join("a")).expect("subdir");
        std::fs::write(root.join("a.sql"), "CREATE TABLE a_top (x int);").expect("w");
        std::fs::write(root.join("a").join("b.sql"), "CREATE TABLE a_b (x int);").expect("w");

        let walk = scan_sql_tree(root).expect("scan");
        let build_order: Vec<String> = walk
            .files
            .iter()
            .map(|f| migration_name(root, f).expect("utf-8 migration name"))
            .collect();

        // The runner's order: the relative names sorted as strings (byte-wise).
        let mut runner_order = build_order.clone();
        runner_order.sort();

        assert_eq!(build_order, vec!["a.sql".to_owned(), "a/b.sql".to_owned()]);
        assert_eq!(
            build_order, runner_order,
            "the build walk order must equal the runner's string-sorted order"
        );
    }

    // ── embed migrations: the S42 ack gate rides emit_migrations ───────

    #[test]
    fn embed_ack_gate_rejects_an_unacknowledged_destructive_migration() {
        // emit_migrations runs the SAME destructive-acknowledgement gate per
        // file, so an unacknowledged DROP TABLE fails the build here too — it
        // can never ship baked into a binary.
        let err = parse_and_enforce_acks(Path::new("0009_drop.sql"), "DROP TABLE users;")
            .expect_err("an unacked DROP TABLE must fail the embed");
        assert!(
            matches!(err, BuildError::UnackedDestructiveMigration { .. }),
            "expected UnackedDestructiveMigration, got {err:?}"
        );
    }

    #[test]
    fn embed_ack_gate_accepts_an_acknowledged_destructive_migration() {
        parse_and_enforce_acks(
            Path::new("0009_drop.sql"),
            "-- bsql:ack-destructive\nDROP TABLE users;",
        )
        .expect("an acked DROP TABLE passes the embed gate");
    }

    #[test]
    fn embed_rejects_a_top_level_transaction_control_statement() {
        // The runner owns the transaction boundary; an embedded COMMIT would leak
        // the CREATE before a later failure. It is a BUILD error.
        for (sql, kw) in [
            ("CREATE TABLE t (a int);\nCOMMIT;", "COMMIT"),
            ("BEGIN;\nCREATE TABLE t (a int);", "BEGIN"),
            ("CREATE TABLE t (a int);\nROLLBACK;", "ROLLBACK"),
            ("SAVEPOINT s;\nCREATE TABLE t (a int);", "SAVEPOINT"),
        ] {
            let err = parse_and_enforce_acks(Path::new("m.sql"), sql)
                .expect_err("transaction control must fail the embed");
            assert!(
                matches!(err, BuildError::TransactionControlInMigration { statement, .. } if statement == kw),
                "expected TransactionControlInMigration({kw}), got {err:?}"
            );
        }
    }

    #[test]
    fn embed_accepts_a_normal_migration_and_a_no_transaction_concurrently() {
        // A normal migration and a `-- bsql:no-transaction` CREATE INDEX
        // CONCURRENTLY (which the runner applies outside a transaction, WITHOUT a
        // BEGIN/COMMIT) both pass the embed gate.
        parse_and_enforce_acks(Path::new("0001.sql"), "CREATE TABLE t (a int)")
            .expect("a normal migration passes");
        parse_and_enforce_acks(
            Path::new("0002.sql"),
            "-- bsql:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (a)",
        )
        .expect("a no-transaction CONCURRENTLY migration passes");
    }

    #[test]
    fn embed_name_normalizes_separators_relative_to_the_dir() {
        let dir = Path::new("/project/migrations");
        let file = Path::new("/project/migrations/2026/0001_init.sql");
        assert_eq!(migration_name(dir, file).expect("name"), "2026/0001_init.sql");
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

    // ─── External-type bridges ──────────────────────────────────────────

    fn bspec(pg: &str, target: &str, conv: &str) -> BridgeSpec {
        BridgeSpec {
            pg_type: pg.to_string(),
            target_type_path: target.to_string(),
            converter_fn_path: conv.to_string(),
        }
    }

    #[test]
    fn bridge_distinct_native_pivots_validate() {
        let cat = catalog_from(&["CREATE TABLE t (a TIMESTAMPTZ, b UUID, c JSONB)"]);
        let bridges = [
            bspec("timestamptz", "chrono::DateTime<chrono::Utc>", "crate::ts"),
            bspec("uuid", "uuid::Uuid", "crate::uuid"),
            bspec("jsonb", "serde_json::Value", "crate::jsonb"),
        ];
        validate_bridges(&bridges, &cat).expect("distinct pivots validate");
    }

    #[test]
    fn bridge_unknown_pg_type_is_loud() {
        let cat = Catalog::default();
        // A typo — not a canonical name.
        let err = validate_bridges(&[bspec("timestamptzz", "X", "y")], &cat)
            .expect_err("typo must fail closed");
        assert!(matches!(
            err,
            BuildError::UnknownBridgeType { ref pg_type } if pg_type == "timestamptzz"
        ));
        // A natively-unsupported type has no pivot to bridge from.
        let err = validate_bridges(&[bspec("inet", "ipnet::IpNet", "d")], &cat)
            .expect_err("inet has no native pivot");
        assert!(matches!(
            err,
            BuildError::UnknownBridgeType { ref pg_type } if pg_type == "inet"
        ));
    }

    #[test]
    fn numeric_is_bridgeable() {
        // `numeric` now has a native pivot (`bsql::Numeric`), so a consumer can
        // bridge it into a decimal crate — the arbitrary-precision value is the
        // faithful pivot the converter reshapes from.
        let cat = catalog_from(&["CREATE TABLE t (amount NUMERIC NOT NULL)"]);
        validate_bridges(&[bspec("numeric", "rust_decimal::Decimal", "crate::to_decimal")], &cat)
            .expect("numeric bridges from its native pivot");
        assert!(scalar_rust_type_for_pg("numeric").is_some(), "numeric has a native pivot");
    }

    #[test]
    fn bridge_array_key_is_rejected() {
        // The bridge key is the INNER element type; an array spelling has no
        // scalar pivot and is loud.
        let err = validate_bridges(&[bspec("timestamptz[]", "X", "y")], &Catalog::default())
            .expect_err("array key must fail closed");
        assert!(matches!(err, BuildError::UnknownBridgeType { .. }));
    }

    #[test]
    fn bridge_conflicting_pivot_is_loud() {
        // `text` and `varchar` decode identically in bsql (same native pivot,
        // same wire OID) — two DIFFERENT targets is a loud conflict.
        let cat = catalog_from(&["CREATE TABLE t (a TEXT, b VARCHAR(9))"]);
        let err = validate_bridges(
            &[
                bspec("text", "smol_str::SmolStr", "crate::a"),
                bspec("varchar", "compact_str::CompactString", "crate::b"),
            ],
            &cat,
        )
        .expect_err("same-pivot conflict must fail closed");
        assert!(matches!(err, BuildError::ConflictingBridge { .. }));
    }

    #[test]
    fn bridge_unused_advisory_is_pivot_keyed_not_string_keyed() {
        // A catalog with ONLY a `varchar` column (no `text` column).
        let cat = catalog_from(&["CREATE TABLE t (a VARCHAR(9))"]);
        let text_pivot = scalar_rust_type_for_pg("text").expect("text has a pivot");
        let varchar_pivot = scalar_rust_type_for_pg("varchar").expect("varchar has a pivot");
        // `text` and `varchar` collapse to the SAME native pivot, so a
        // `.bridge("text")` DOES fire on the `varchar` column — and therefore
        // must NOT be advised as "matches no table column".
        assert_eq!(text_pivot, varchar_pivot);
        assert!(
            catalog_uses_pivot(&cat, text_pivot),
            "a `text` bridge fires on a varchar column (same pivot) — no advisory"
        );
        // A pivot NO column uses still counts as unmatched (the advisory fires).
        let uuid_pivot = scalar_rust_type_for_pg("uuid").expect("uuid has a pivot");
        assert!(
            !catalog_uses_pivot(&cat, uuid_pivot),
            "no column resolves to the uuid pivot — the advisory still fires"
        );
    }

    #[test]
    fn column_pivot_resolves_scalar_and_array_element_to_the_same_pivot() {
        let scalar = column_pivot("timestamptz").expect("scalar pivot");
        let array = column_pivot("timestamptz[]").expect("array element pivot");
        assert_eq!(scalar, array, "a `T[]` column shares its element's pivot");
        // A multi-dimensional array has no bridgeable pivot.
        assert_eq!(column_pivot("timestamptz[][]"), None);
        // A supported scalar (now including `numeric`) resolves to a pivot.
        assert!(column_pivot("numeric").is_some());
        // An unsupported type has none.
        assert_eq!(column_pivot("inet"), None);
    }

    #[test]
    fn bridge_serialize_round_trips() {
        let bridges = [
            bspec("uuid", "uuid::Uuid", "crate::bridge::uuid"),
            bspec("timestamptz", "chrono::DateTime<chrono::Utc>", "crate::bridge::ts"),
        ];
        let text = serialize_bridges(&bridges);
        // Deterministic: sorted by pg_type (`timestamptz` before `uuid`).
        assert!(text.starts_with("timestamptz\t"));
        let parsed = parse_bridges(&text).expect("round-trips");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].pg_type, "timestamptz");
        assert_eq!(parsed[0].target_type_path, "chrono::DateTime<chrono::Utc>");
        assert_eq!(parsed[0].converter_fn_path, "crate::bridge::ts");
        assert_eq!(parsed[1].pg_type, "uuid");
    }

    #[test]
    fn parse_bridges_empty_is_empty() {
        assert!(parse_bridges("").expect("empty parses").is_empty());
    }

    #[test]
    fn parse_bridges_wrong_field_count_is_loud() {
        let err = parse_bridges("uuid\tuuid::Uuid\n").expect_err("must fail closed");
        assert_eq!(err.fields, 2);
    }
}
