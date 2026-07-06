//! Build-time SQLite validation backend for the compile-checked query API.
//!
//! This module is compiled ONLY under the `sqlite` feature (it pulls the
//! build-time-only `rusqlite` edge). A consumer that also targets SQLite
//! does two things:
//!
//! 1. Its `build.rs` calls [`emit_sqlite_template`] alongside
//!    `emit_catalog`. That replays the SAME migration `*.sql` files into a
//!    fresh SQLite database file in `OUT_DIR` (the "template database"),
//!    using real SQLite via `rusqlite` — so the migration DDL is executed
//!    by the engine that will run it, not re-modelled. A migration DDL
//!    form SQLite cannot replay (e.g. an `ALTER COLUMN ... SET NOT NULL`,
//!    which SQLite does not support) is a LOUD [`BuildError`], never a
//!    silent skip.
//!
//! 2. At `query!` expansion the proc-macro calls
//!    [`verify_sqlite_conformance`]. That opens the template database
//!    READ-ONLY, installs a **deny-all-but-readonly** authorizer (so a
//!    malicious query or migration cannot perform any write/DDL/attach at
//!    build time), `prepare`s the query, and reads, per result column, the
//!    declared type (`decltype`) and the base column's `NOT NULL` flag
//!    (`table_info`). It then asserts those AGREE with the inference
//!    lattice's `(RustType, nullable)` for the same query. A disagreement
//!    is a LOUD [`SqliteConformanceError`].
//!
//! # Why a cross-check, not a second source of truth
//!
//! The inference lattice (the shared engine that types both backends) is
//! what drives code generation. This backend does not re-type the query;
//! it confirms that REAL SQLite resolves the same row shape the lattice
//! claimed. Type inference forks from the lattice in exactly one place: the
//! leaf type map. The portable types ({i16, i32, i64, bool, text}) map
//! identically; the one PostgreSQL-only leaf is `oid` (Rust `u32`), which
//! has no SQLite equivalent — a query projecting an `oid` column types on
//! PostgreSQL but is a loud, honest conformance failure here.
//!
//! # The PRIMARY KEY nullability reconciliation
//!
//! SQLite has a historical quirk: only an `INTEGER PRIMARY KEY` (the rowid
//! alias) is implicitly `NOT NULL`; any other `PRIMARY KEY` column is
//! reported by `table_info` as `notnull = 0` and may, in raw SQLite, hold a
//! NULL. Standard SQL — and the PostgreSQL semantics the lattice models —
//! make EVERY primary-key column `NOT NULL`. So when reading `table_info`
//! the effective nullability is `notnull == 0 AND pk == 0`: a `pk` column
//! is treated as `NOT NULL`, matching the lattice. This is the principled
//! reconciliation of the one anomaly, not a silent paper-over: the column
//! is genuinely the key, hence genuinely non-null.
//!
//! # Why there is no nullability-disagreement trybuild fixture (recorded)
//!
//! There IS a `type_disagreement` compile-fail fixture, because a type fork
//! arises organically: the lattice types a PostgreSQL `oid` column as `u32`
//! while SQLite has no equivalent, so a real migration projecting it is a
//! genuine, organically-constructible disagreement. A *nullability*
//! disagreement is DIFFERENT: it CANNOT arise organically in a consumer
//! whose catalog (the PostgreSQL replay the lattice types against) and SQLite
//! template are BOTH derived from the SAME migration set. For a genuine
//! base-column reference — the only column the nullability check applies to
//! (see [`check_columns`]) — a column declared `NOT NULL` is `NOT NULL` in
//! both replays, and a nullable column is nullable in both AND typed nullable
//! by the lattice; the PK quirk is reconciled identically on both sides. So
//! the lattice's nullability and SQLite's base nullability always AGREE for a
//! direct base column built from one migration set. The DANGEROUS direction
//! (lattice NOT NULL, SQLite base nullable) the check guards is therefore not
//! reachable through a real `query!` over real migrations, so no trybuild
//! compile-fail can construct it. The loud-rejection BEHAVIOUR is covered
//! instead by the `nullability_disagreement_is_loud` unit test below, which
//! constructs the disagreeing pair directly. This is a recorded decision, not
//! a silent omission: the path is exercised, just not via trybuild.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use crate::{BuildError, DynamicShape, InferredColumn, ParamShape, RustType};
// The `$N`→`?N` SQLite placeholder rewrite — one authority, defined in `dynamics`
// and shared by this conformance oracle AND the `query!` macro's baked SQLite
// `const SQL`, so the runtime string is byte-identical to the one validated here.
use crate::dynamics::sqlite_placeholder_form as rewrite_placeholders;

/// The basename of the SQLite template database written into `OUT_DIR`.
pub const SQLITE_TEMPLATE_FILE_NAME: &str = "bsql_sqlite_template.db";

/// The environment variable, set via `cargo:rustc-env`, that carries the
/// absolute path of the generated SQLite template database to the query
/// proc-macro. When present, the conformance cross-check opens this template
/// and runs.
pub const SQLITE_TEMPLATE_ENV_VAR: &str = "BSQL_SQLITE_TEMPLATE";

/// The environment variable, set via `cargo:rustc-env`, that DECLARES the
/// consumer targets SQLite for compile-checked queries. [`emit_sqlite_template`]
/// sets it as its first channel. It exists so a missing template is a LOUD
/// build error rather than a silent disengage: if this marker is present but
/// [`SQLITE_TEMPLATE_ENV_VAR`] is not, the query proc-macro emits a
/// `compile_error!` (the build declared a SQLite target but no usable template
/// reached expansion) instead of silently skipping the conformance oracle.
pub const SQLITE_TARGET_ENV_VAR: &str = "BSQL_SQLITE_TARGET";

/// Replay a consumer's migration `*.sql` tree into a fresh SQLite template
/// database in `OUT_DIR`, and set the `BSQL_SQLITE_TEMPLATE` rustc-env
/// channel so the query proc-macro can open it at expansion.
///
/// Call this from a consumer crate's `build.rs`, alongside `emit_catalog`,
/// when the consumer also targets SQLite:
///
/// ```no_run
/// fn main() -> Result<(), bsql_build::BuildError> {
///     bsql_build::emit_catalog("migrations")?;
///     bsql_build::emit_sqlite_template("migrations")
/// }
/// ```
///
/// It walks the same migrations tree (recursing into subdirectories, in
/// deterministic path order), emits `cargo:rerun-if-changed` for the
/// directory and each file (Cargo de-duplicates these against
/// `emit_catalog`'s identical directives), then executes each migration's
/// DDL into a file-backed SQLite database with the real engine.
///
/// # Errors
///
/// Fail-closed on any I/O error, or on a DDL statement SQLite cannot
/// execute (an unmodelable migration form) — returned as a
/// [`BuildError`], which the consumer's `build.rs` propagates to fail the
/// build. A migration that carries schema shape is NEVER silently skipped.
pub fn emit_sqlite_template(migrations_dir: impl AsRef<Path>) -> Result<(), BuildError> {
    let manifest = crate::env_path("CARGO_MANIFEST_DIR")?;
    let dir = manifest.join(migrations_dir.as_ref());

    // Declare the SQLite conformance target FIRST, before any fallible step.
    // This is the per-consumer signal the query proc-macro keys on to turn a
    // missing template into a loud build error instead of a silent skip: once
    // a build declares this target, the template channel must reach expansion.
    println!("cargo:rustc-env={SQLITE_TARGET_ENV_VAR}=1");

    let walk = crate::scan_sql_tree(&dir)?;
    for directory in &walk.dirs {
        println!("cargo:rerun-if-changed={}", directory.display());
    }
    for file in &walk.files {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    let out_dir = crate::env_path("OUT_DIR")?;
    let db_path = out_dir.join(SQLITE_TEMPLATE_FILE_NAME);

    // Start from a clean slate: a stale template from a previous build would
    // make every `CREATE TABLE` fail ("table already exists"). Removing a
    // file that is not there is fine; any OTHER removal error is loud.
    match std::fs::remove_file(&db_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(BuildError::SqliteTemplate {
                message: format!(
                    "cannot remove stale SQLite template {}: {err}",
                    db_path.display()
                ),
            })
        }
    }

    let conn = rusqlite::Connection::open(&db_path).map_err(|err| BuildError::SqliteTemplate {
        message: format!(
            "cannot create SQLite template database {}: {err}",
            db_path.display()
        ),
    })?;

    for file in &walk.files {
        let sql = std::fs::read_to_string(file).map_err(|source| BuildError::ReadFile {
            path: file.clone(),
            source,
        })?;
        // Execute the migration's DDL with the real engine. A form SQLite
        // cannot run (e.g. `ALTER COLUMN ... SET NOT NULL`) surfaces here as
        // a loud replay error naming the file and the engine's message.
        conn.execute_batch(&sql).map_err(|err| BuildError::SqliteReplay {
            path: file.clone(),
            message: err.to_string(),
        })?;
    }
    // Closing flushes the database file to disk. `Connection::Drop` also
    // closes it; the explicit drop makes the ordering before the rustc-env
    // print obvious.
    drop(conn);

    println!(
        "cargo:rustc-env={SQLITE_TEMPLATE_ENV_VAR}={}",
        db_path.display()
    );
    Ok(())
}

/// A SQLite conformance failure surfaced at `query!` expansion. Every
/// variant is fatal: the proc-macro turns it into a `compile_error!`. There
/// is no "could not tell, so accepted" path — an unconfirmable shape that
/// SQLite disagrees with is always one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqliteConformanceError {
    /// The template database could not be opened.
    TemplateOpen { path: String, message: String },
    /// Reading the template's schema (`sqlite_master` / `table_info`)
    /// failed.
    Introspection { message: String },
    /// SQLite rejected the query at `prepare` — an unknown table or column,
    /// invalid SQL, or an action the read-only authorizer denied. `sql` is
    /// the query as the lattice typed it; `message` is SQLite's reason.
    Prepare { sql: String, message: String },
    /// SQLite resolved a different number of result columns than the
    /// lattice.
    ColumnCount { lattice: usize, sqlite: usize },
    /// A result column's SQLite-declared type does not map to the same Rust
    /// type the lattice inferred (or maps to no portable SQLite type — the
    /// `oid` leaf-map fork). `column` is the output name; `lattice` is the
    /// inferred Rust type; `sqlite` describes SQLite's declared type.
    Type {
        column: String,
        lattice: String,
        sqlite: String,
    },
    /// The lattice typed a column as `NOT NULL` (decoded into `T`), but
    /// SQLite's base column is nullable — a NULL could reach a non-`Option`
    /// field. `column` is the output name.
    Nullability { column: String },
    /// A dynamic OPTIONAL (toggle) filter forces a full-table SCAN at
    /// runtime (its `$N IS NULL OR ...` form defeats every index), and the
    /// query did not acknowledge it. `detail` is SQLite's plan line.
    FullScanOnToggle { detail: String },
}

impl fmt::Display for SqliteConformanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqliteConformanceError::TemplateOpen { path, message } => write!(
                f,
                "cannot open the SQLite template database {path}: {message}"
            ),
            SqliteConformanceError::Introspection { message } => {
                write!(f, "cannot read the SQLite template schema: {message}")
            }
            SqliteConformanceError::Prepare { sql, message } => write!(
                f,
                "SQLite rejected this query at build time: {message} \
                 (validated form: `{sql}`). The deny-all-but-readonly \
                 authorizer permits only read-only SELECT/read actions; an \
                 unknown table/column, invalid SQL, or a write action fails \
                 here."
            ),
            SqliteConformanceError::ColumnCount { lattice, sqlite } => write!(
                f,
                "result column count disagreement: the inference lattice \
                 resolved {lattice} column(s) but SQLite resolved {sqlite}."
            ),
            SqliteConformanceError::Type {
                column,
                lattice,
                sqlite,
            } => write!(
                f,
                "type conformance failure on column `{column}`: the inference \
                 lattice typed it `{lattice}`, but SQLite's declared type is \
                 {sqlite}. The shared type lattice forks from SQLite only on \
                 PostgreSQL-only leaves (e.g. `oid`); this query is not \
                 portable to SQLite."
            ),
            SqliteConformanceError::Nullability { column } => write!(
                f,
                "nullability conformance failure on column `{column}`: the \
                 inference lattice typed it NOT NULL, but SQLite's base column \
                 is nullable — a NULL could decode into a non-Option field. \
                 Mark the column nullable or fix the schema."
            ),
            SqliteConformanceError::FullScanOnToggle { detail } => write!(
                f,
                "a dynamic OPTIONAL(...) toggle filter forces a full-table \
                 scan at runtime ({detail}). The `$N IS NULL OR ...` form \
                 cannot use an index, so enabling the filter scans the whole \
                 table. Add an index that serves the enabled filter, or \
                 acknowledge the scan with a `/* bsql:allow-scan: <reason> */` \
                 marker in the query and a documented return plan."
            ),
        }
    }
}

impl std::error::Error for SqliteConformanceError {}

/// Cross-check one (already lattice-typed) query against the SQLite
/// template database. See the module docs for the full mechanism.
///
/// `scan_acknowledged` is `true` when the query carries an
/// `/* bsql:allow-scan */` marker (stripped by the proc-macro before
/// lowering); it suppresses ONLY the full-scan-on-toggle build error.
///
/// # Errors
///
/// [`SqliteConformanceError`] on any open/introspection failure, a SQLite
/// `prepare` rejection, a column-count/type/nullability disagreement with
/// the lattice, or an unacknowledged full-scan toggle.
pub fn verify_sqlite_conformance(
    template_path: &Path,
    shape: &DynamicShape,
    scan_acknowledged: bool,
) -> Result<(), SqliteConformanceError> {
    let conn = rusqlite::Connection::open_with_flags(
        template_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|err| SqliteConformanceError::TemplateOpen {
        path: template_path.display().to_string(),
        message: err.to_string(),
    })?;

    // Introspect base-column nullability BEFORE the authorizer is installed:
    // this reads our OWN template (trusted), and the deny-all authorizer
    // below would (correctly) refuse the `PRAGMA`/`sqlite_master` reads.
    let nullability = collect_base_nullability(&conn)?;

    // From here on, only the user query is prepared, and it is prepared
    // under a deny-all-but-readonly authorizer: a hostile query/migration
    // cannot write, attach, drop, or run a pragma during validation.
    conn.authorizer(Some(readonly_authorizer));

    let has_toggle = shape
        .params
        .iter()
        .any(|p| matches!(p, ParamShape::Optional(_)));

    for variant in &shape.variants {
        // Type + nullability conformance on the portable inference form
        // (no PostgreSQL-only `= ANY($N)`), which SQLite can prepare.
        check_columns(&conn, &variant.infer_sql, &shape.columns, &nullability)?;
        // Full-scan-on-toggle check on the SQLite-preparable SCAN form (the
        // `$N IS NULL OR ...` toggle PRESERVED so the plan reflects the
        // enabled filter, the PostgreSQL-only `= ANY($N)` COLLAPSED to `$N`
        // so SQLite can prepare it) — only when the query actually has a
        // toggle filter. The wire form keeps `= ANY($N)`, which SQLite
        // parses as a call to an unknown function `ANY` and would reject,
        // falsely failing a valid OPTIONAL + `= ANY($M)` query.
        if has_toggle {
            check_no_full_scan(&conn, &variant.scan_sql, scan_acknowledged)?;
        }
    }
    Ok(())
}

/// The base-column nullability of one column name, reconciled across every
/// table it appears in.
#[derive(Clone, Copy)]
struct ColMeta {
    /// `true` when the column is `NOT NULL` (its declared flag OR primary-key
    /// membership — see the module docs on the SQLite PK quirk).
    not_null: bool,
    /// `true` when the same column name appears in more than one table with
    /// DISAGREEING nullability, so no single answer can be trusted.
    ambiguous: bool,
}

/// The deny-all-but-readonly authorizer: permit only the actions a
/// read-only `SELECT` needs during `prepare` (the SELECT itself, per-column
/// reads, function calls, recursive-CTE recursion); deny everything else
/// (every write/DDL/attach/detach/transaction/pragma). A denied action
/// makes `prepare` fail, surfaced as a loud `Prepare` error.
fn readonly_authorizer(ctx: rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};
    match ctx.action {
        AuthAction::Select
        | AuthAction::Read { .. }
        | AuthAction::Function { .. }
        | AuthAction::Recursive => Authorization::Allow,
        _ => Authorization::Deny,
    }
}

/// Read every base table's `table_info` into a `column name -> ColMeta`
/// map, with `NOT NULL` reconciled to include primary-key membership (the
/// SQLite PK quirk) and ambiguity flagged when a name disagrees across
/// tables. Runs before the authorizer is installed.
fn collect_base_nullability(
    conn: &rusqlite::Connection,
) -> Result<BTreeMap<String, ColMeta>, SqliteConformanceError> {
    let intro = |err: rusqlite::Error| SqliteConformanceError::Introspection {
        message: err.to_string(),
    };

    let mut tables: Vec<String> = Vec::new();
    {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
            .map_err(intro)?;
        let mut rows = stmt.query([]).map_err(intro)?;
        while let Some(row) = rows.next().map_err(intro)? {
            tables.push(row.get(0).map_err(intro)?);
        }
    }

    let mut map: BTreeMap<String, ColMeta> = BTreeMap::new();
    for table in &tables {
        // `PRAGMA table_info` takes no bound parameter for the table name, so
        // the trusted name (from `sqlite_master`) is interpolated with its
        // double quotes escaped.
        let pragma = format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\""));
        let mut stmt = conn.prepare(&pragma).map_err(intro)?;
        let mut rows = stmt.query([]).map_err(intro)?;
        while let Some(row) = rows.next().map_err(intro)? {
            // table_info columns: 0 cid, 1 name, 2 type, 3 notnull, 4 dflt, 5 pk.
            let name: String = row.get(1).map_err(intro)?;
            let notnull: i64 = row.get(3).map_err(intro)?;
            let pk: i64 = row.get(5).map_err(intro)?;
            let effective = notnull != 0 || pk != 0;
            match map.get_mut(&name) {
                Some(meta) => {
                    if meta.not_null != effective {
                        meta.ambiguous = true;
                    }
                }
                None => {
                    map.insert(
                        name,
                        ColMeta {
                            not_null: effective,
                            ambiguous: false,
                        },
                    );
                }
            }
        }
    }
    Ok(map)
}

/// Prepare one query under the authorizer and assert its result columns
/// agree with the lattice on count, type, and nullability.
fn check_columns(
    conn: &rusqlite::Connection,
    infer_sql: &str,
    lattice_cols: &[InferredColumn],
    nullability: &BTreeMap<String, ColMeta>,
) -> Result<(), SqliteConformanceError> {
    let rewritten = rewrite_placeholders(infer_sql);
    let stmt = conn
        .prepare(&rewritten)
        .map_err(|err| SqliteConformanceError::Prepare {
            sql: infer_sql.to_string(),
            message: err.to_string(),
        })?;

    let cols = stmt.columns();
    if cols.len() != lattice_cols.len() {
        return Err(SqliteConformanceError::ColumnCount {
            lattice: lattice_cols.len(),
            sqlite: cols.len(),
        });
    }

    for (sqlite_col, lattice_col) in cols.iter().zip(lattice_cols.iter()) {
        // `decl_type` is present IFF this result column is a GENUINE base-
        // column reference: SQLite resolves a declared type only for a column
        // that directly refers to a table column. For an EXPRESSION — a
        // `COUNT(*)`, an arithmetic term, or a `COALESCE(name, 'x')` even
        // when it is ALIASED back to a base column's NAME — it is absent.
        // Both the type AND the nullability cross-checks are valid only for a
        // genuine base-column reference, so both gate on this: an aliased
        // expression is left to the lattice (the honest absence of an
        // independent SQLite signal, NOT a skipped column — the column-count
        // check above already ran). Computing it once keeps the two checks on
        // the SAME definition of "base column".
        let decl_type = sqlite_col.decl_type();

        // TYPE. The lattice's inferred Rust type must map from SQLite's
        // declared type. A genuine fork (e.g. PostgreSQL `oid`, which SQLite
        // has no portable equivalent for) is loud.
        if let Some(decltype) = decl_type {
            match sqlite_rust_type(decltype) {
                Some(rust) if rust == lattice_col.ty => {}
                Some(rust) => {
                    return Err(SqliteConformanceError::Type {
                        column: lattice_col.name.clone(),
                        lattice: lattice_col.ty.to_string(),
                        sqlite: format!("`{decltype}` ({rust})"),
                    })
                }
                None => {
                    return Err(SqliteConformanceError::Type {
                        column: lattice_col.name.clone(),
                        lattice: lattice_col.ty.to_string(),
                        sqlite: format!("`{decltype}` (no portable SQLite type)"),
                    })
                }
            }
        }

        // NULLABILITY. Use SQLite's base-column NOT NULL (PK-reconciled),
        // but ONLY for a genuine base-column reference (`decl_type` present)
        // — its `name()` is then that base column's own name, so the lookup
        // is sound. An EXPRESSION aliased to a base column's name (e.g.
        // `COALESCE(name, 'x') AS name`, which the lattice correctly types
        // NOT NULL) must NOT be matched against that base column's
        // nullability by name; it is left to the lattice. A disagreement is
        // loud ONLY in the DANGEROUS direction: the lattice claims NOT NULL
        // but SQLite's base column is nullable. The lattice being MORE
        // nullable (an outer-join nullable side) is sound and allowed; an
        // ambiguous name (disagreeing across tables) carries no single answer
        // and is likewise left to the lattice.
        if decl_type.is_some()
            && let Some(meta) = nullability.get(sqlite_col.name())
            && !meta.ambiguous
            && !lattice_col.nullable
            && !meta.not_null
        {
            return Err(SqliteConformanceError::Nullability {
                column: lattice_col.name.clone(),
            });
        }
    }
    Ok(())
}

/// Run `EXPLAIN QUERY PLAN` on the SQLite-preparable SCAN form (the toggle
/// preserved, the PostgreSQL-only `= ANY($N)` collapsed) and reject an
/// unacknowledged full-table SCAN.
fn check_no_full_scan(
    conn: &rusqlite::Connection,
    scan_sql: &str,
    acknowledged: bool,
) -> Result<(), SqliteConformanceError> {
    let rewritten = rewrite_placeholders(scan_sql);
    let plan_sql = format!("EXPLAIN QUERY PLAN {rewritten}");
    let prep_err = |err: rusqlite::Error| SqliteConformanceError::Prepare {
        sql: scan_sql.to_string(),
        message: err.to_string(),
    };

    let mut stmt = conn.prepare(&plan_sql).map_err(prep_err)?;
    let param_count = stmt.parameter_count();
    // The plan is independent of bound values, so bind NULL for each slot;
    // SQLite still requires the slots filled to step the EXPLAIN.
    let mut rows = stmt
        .query(rusqlite::params_from_iter(
            (0..param_count).map(|_| rusqlite::types::Null),
        ))
        .map_err(prep_err)?;

    let mut scan: Option<String> = None;
    while let Some(row) = rows.next().map_err(prep_err)? {
        // EXPLAIN QUERY PLAN columns: 0 id, 1 parent, 2 notused, 3 detail.
        let detail: String = row.get(3).map_err(prep_err)?;
        // A full scan begins with "SCAN ..."; an index-assisted lookup
        // begins with "SEARCH ...". (A "SCAN ... USING COVERING INDEX" still
        // reads the whole index — it is a full scan and is flagged.)
        if detail.trim_start().starts_with("SCAN") {
            scan = Some(detail);
            break;
        }
    }

    match scan {
        Some(detail) if !acknowledged => {
            Err(SqliteConformanceError::FullScanOnToggle { detail })
        }
        _ => Ok(()),
    }
}

/// The SQLite leaf type map: a result column's declared type (from
/// `decltype`) to the v1 Rust type, mirroring the catalog leaf map for
/// every portable type. The one deliberate divergence is `oid` (and any
/// other non-portable declared type): it returns `None`, because SQLite has
/// no equivalent — the conformance check turns that into a loud, honest
/// failure rather than silently mapping it.
fn sqlite_rust_type(decltype: &str) -> Option<RustType> {
    let lower = decltype.trim().to_ascii_lowercase();
    let head = match lower.split(['(', ' ']).next() {
        Some(head) => head,
        None => lower.as_str(),
    };
    match head {
        "bigint" | "int8" => Some(RustType::I64),
        "int" | "integer" | "int4" => Some(RustType::I32),
        "smallint" | "int2" => Some(RustType::I16),
        "boolean" | "bool" => Some(RustType::Bool),
        "text" | "varchar" | "char" | "character" | "bpchar" | "clob" => Some(RustType::Text),
        // SQLite has exactly ONE floating type: 8-byte REAL (= Rust `f64`).
        // Every float spelling (`real`, `float`, `double precision`,
        // `float4`, `float8`) resolves to it. A PostgreSQL `float8` /
        // `double precision` column (lattice `f64`) therefore AGREES; a
        // PostgreSQL `float4` / `real` column (lattice `f32`) does NOT — its
        // 4-byte width has no SQLite equivalent, so the conformance check
        // flags the genuine divergence loudly rather than reading 8 SQLite
        // bytes into an `f32`.
        "real" | "float" | "double" | "float4" | "float8" => Some(RustType::F64),
        // `bytea`'s SQLite peer is BLOB — an opaque byte string, decoding to
        // the same `Vec<u8>` / `&[u8]`. A column declared `BLOB` gets SQLite BLOB
        // affinity; a column declared `BYTEA` (the PostgreSQL spelling, so the
        // SAME migration DDL replays into both backends) gets NUMERIC affinity
        // but still stores a bound BLOB value verbatim — both resolve to the
        // lattice `bytea`, so either spelling AGREES.
        "blob" | "bytea" => Some(RustType::Bytea),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template() -> tempdb::TempDb {
        tempdb::TempDb::new(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, name TEXT);\
             CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, total INTEGER);\
             CREATE TABLE widgets (id BIGINT PRIMARY KEY, thing_id OID NOT NULL);",
        )
    }

    fn col(name: &str, ty: RustType, nullable: bool) -> InferredColumn {
        InferredColumn {
            name: name.to_string(),
            ty,
            nullable,
        }
    }

    fn shape(cols: Vec<InferredColumn>, params: Vec<ParamShape>, wire: &str, infer: &str) -> DynamicShape {
        // No `= ANY($N)`, so the scan form is the wire form (the toggle, if
        // any, is preserved unchanged); the dedicated OPTIONAL + `= ANY($M)`
        // test below uses `shape_scan` to give a distinct scan form.
        shape_scan(cols, params, wire, infer, wire)
    }

    fn shape_scan(
        cols: Vec<InferredColumn>,
        params: Vec<ParamShape>,
        wire: &str,
        infer: &str,
        scan: &str,
    ) -> DynamicShape {
        DynamicShape {
            columns: cols,
            params,
            variants: vec![crate::dynamics::WireVariant {
                wire_sql: wire.to_string(),
                infer_sql: infer.to_string(),
                scan_sql: scan.to_string(),
            }],
            order_by: None,
        }
    }

    #[test]
    fn rewrite_dollar_to_question() {
        assert_eq!(
            rewrite_placeholders("WHERE id = $1 AND x = $12"),
            "WHERE id = ?1 AND x = ?12"
        );
        // `$` inside a string literal is untouched.
        assert_eq!(rewrite_placeholders("'$1' = $1"), "'$1' = ?1");
    }

    #[test]
    fn sqlite_leaf_map_matches_portable_and_forks_on_oid() {
        assert_eq!(sqlite_rust_type("BIGINT"), Some(RustType::I64));
        assert_eq!(sqlite_rust_type("INTEGER"), Some(RustType::I32));
        assert_eq!(sqlite_rust_type("TEXT"), Some(RustType::Text));
        assert_eq!(sqlite_rust_type("VARCHAR(50)"), Some(RustType::Text));
        assert_eq!(sqlite_rust_type("BOOLEAN"), Some(RustType::Bool));
        // Every SQLite float spelling resolves to the single 8-byte REAL
        // type = `f64`; `blob` is the `bytea` peer.
        assert_eq!(sqlite_rust_type("REAL"), Some(RustType::F64));
        assert_eq!(sqlite_rust_type("FLOAT8"), Some(RustType::F64));
        assert_eq!(sqlite_rust_type("DOUBLE PRECISION"), Some(RustType::F64));
        // A `float4` / `real` column resolves to 8-byte REAL on SQLite (f64),
        // so it does NOT equal the lattice's `f32` — the differential oracle
        // catches that divergence via `Type` mismatch, not by mapping here.
        assert_eq!(sqlite_rust_type("FLOAT4"), Some(RustType::F64));
        assert_eq!(sqlite_rust_type("BLOB"), Some(RustType::Bytea));
        // The one fork: oid has no SQLite equivalent.
        assert_eq!(sqlite_rust_type("OID"), None);
    }

    #[test]
    fn sqlite_float4_column_diverges_from_lattice_f32() {
        // A migration column typed `float4` in the catalog is `f32` in the
        // lattice; the SQLite peer resolves the same column to 8-byte REAL
        // (`f64`). The conformance oracle must flag this — silently reading 8
        // SQLite bytes into an `f32` decoder would be wrong.
        assert_ne!(sqlite_rust_type("FLOAT4"), Some(RustType::F32));
        assert_eq!(sqlite_rust_type("FLOAT8"), Some(RustType::F64));
    }

    #[test]
    fn conforming_query_passes() {
        let db = template();
        let s = shape(
            vec![col("id", RustType::I64, false), col("name", RustType::Text, true)],
            vec![],
            "SELECT id, name FROM users",
            "SELECT id, name FROM users",
        );
        verify_sqlite_conformance(db.path(), &s, false).expect("conforms");
    }

    #[test]
    fn pk_column_is_not_null_despite_sqlite_quirk() {
        // `id` is a BIGINT PRIMARY KEY: SQLite's table_info reports notnull=0,
        // but the PK reconciliation makes it NOT NULL, matching the lattice.
        let db = template();
        let s = shape(
            vec![col("id", RustType::I64, false)],
            vec![],
            "SELECT id FROM users",
            "SELECT id FROM users",
        );
        verify_sqlite_conformance(db.path(), &s, false).expect("pk is not null");
    }

    #[test]
    fn oid_column_is_a_type_disagreement() {
        let db = template();
        let s = shape(
            vec![col("thing_id", RustType::U32, false)],
            vec![],
            "SELECT thing_id FROM widgets",
            "SELECT thing_id FROM widgets",
        );
        match verify_sqlite_conformance(db.path(), &s, false) {
            Err(SqliteConformanceError::Type { column, .. }) => assert_eq!(column, "thing_id"),
            other => panic!("expected a Type disagreement, got {other:?}"),
        }
    }

    #[test]
    fn nullability_disagreement_is_loud() {
        // Lattice claims `name` is NOT NULL, but it is nullable in SQLite.
        let db = template();
        let s = shape(
            vec![col("name", RustType::Text, false)],
            vec![],
            "SELECT name FROM users",
            "SELECT name FROM users",
        );
        match verify_sqlite_conformance(db.path(), &s, false) {
            Err(SqliteConformanceError::Nullability { column }) => assert_eq!(column, "name"),
            other => panic!("expected a Nullability disagreement, got {other:?}"),
        }
    }

    #[test]
    fn unknown_column_is_rejected_by_sqlite() {
        let db = template();
        let s = shape(
            vec![col("nope", RustType::I64, false)],
            vec![],
            "SELECT nope FROM users",
            "SELECT nope FROM users",
        );
        match verify_sqlite_conformance(db.path(), &s, false) {
            Err(SqliteConformanceError::Prepare { .. }) => {}
            other => panic!("expected a Prepare rejection, got {other:?}"),
        }
    }

    #[test]
    fn toggle_full_scan_is_flagged_unless_acknowledged() {
        let db = template();
        let s = shape(
            vec![col("id", RustType::I64, false)],
            vec![ParamShape::Optional(RustType::Text)],
            "SELECT id FROM users WHERE ($1 IS NULL OR email = $1)",
            "SELECT id FROM users WHERE (email = $1)",
        );
        match verify_sqlite_conformance(db.path(), &s, false) {
            Err(SqliteConformanceError::FullScanOnToggle { .. }) => {}
            other => panic!("expected FullScanOnToggle, got {other:?}"),
        }
        // Acknowledged: the same query passes.
        verify_sqlite_conformance(db.path(), &s, true).expect("acknowledged scan passes");
    }

    #[test]
    fn optional_and_any_scan_check_prepares_without_any_function_error() {
        // A VALID dynamic query combining an OPTIONAL($1) toggle with a
        // `= ANY($2)` in-list on a DIFFERENT param (over unindexed columns,
        // so the enabled toggle genuinely forces a scan). The scan check runs
        // on the SQLite-preparable SCAN form (toggle PRESERVED, `= ANY($2)`
        // COLLAPSED to `= $2`).
        let db = template();

        // First, demonstrate the false-reject the fix avoids: the WIRE form
        // keeps the PostgreSQL-only `= ANY($2)`, which SQLite parses as a call
        // to an unknown function `ANY` and rejects at prepare. Running the
        // scan check on the wire form would surface that as a misleading
        // `Prepare` failure on a valid query.
        {
            let conn = rusqlite::Connection::open_with_flags(
                db.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .expect("open template read-only");
            let wire = rewrite_placeholders(
                "SELECT id FROM users WHERE ($1 IS NULL OR name = $1) AND email = ANY($2)",
            );
            let err = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {wire}"))
                .expect_err("SQLite rejects the PostgreSQL-only `= ANY(...)`")
                .to_string();
            assert!(
                err.to_ascii_lowercase().contains("no such function"),
                "expected an unknown-function `ANY` rejection, got: {err}"
            );
        }

        let s = shape_scan(
            vec![col("id", RustType::I64, false)],
            vec![
                ParamShape::Optional(RustType::Text),
                ParamShape::Array(RustType::Text),
            ],
            // wire: toggle expanded + `= ANY($2)` kept (PostgreSQL runtime).
            "SELECT id FROM users WHERE ($1 IS NULL OR name = $1) AND email = ANY($2)",
            // infer: toggle collapsed + `= ANY($2)` collapsed (portable prepare).
            "SELECT id FROM users WHERE (name = $1) AND email = $2",
            // scan: toggle preserved + `= ANY($2)` collapsed (SQLite-preparable).
            "SELECT id FROM users WHERE ($1 IS NULL OR name = $1) AND email = $2",
        );
        // Unacknowledged: the scan form PREPARES (no `no such function: ANY`)
        // and the toggle over the unindexed columns is detected as a scan,
        // surfaced as FullScanOnToggle — NOT a `Prepare` error. This is the
        // proof the false-reject is fixed.
        match verify_sqlite_conformance(db.path(), &s, false) {
            Err(SqliteConformanceError::FullScanOnToggle { .. }) => {}
            other => panic!(
                "expected FullScanOnToggle (ANY collapsed, scan detected), got {other:?}"
            ),
        }
        // Acknowledged: the same query is accepted.
        verify_sqlite_conformance(db.path(), &s, true)
            .expect("OPTIONAL + `= ANY($M)` accepted when the toggle scan is acknowledged");
    }

    #[test]
    fn aliased_expression_matching_nullable_base_name_is_accepted() {
        // `COALESCE(name, 'x')` is genuinely NOT NULL (the lattice types it
        // so: a non-null argument makes the whole expression non-null), and
        // it is ALIASED back to the NULLABLE base column's name, `name`. The
        // nullability check must be gated on a genuine base-column reference:
        // an EXPRESSION has no SQLite `decltype`, so it is left to the lattice
        // and NOT falsely flagged against `users.name`'s nullability. (Before
        // the gate, the check looked the base nullability up by the result
        // column NAME, falsely rejecting this valid query.)
        let db = template();
        let s = shape(
            // The lattice's typing of the expression: Text, NOT NULL.
            vec![col("name", RustType::Text, false)],
            vec![],
            "SELECT COALESCE(name, 'x') AS name FROM users",
            "SELECT COALESCE(name, 'x') AS name FROM users",
        );
        verify_sqlite_conformance(db.path(), &s, false)
            .expect("an aliased NOT-NULL expression is left to the lattice, not falsely flagged");
    }

    /// A throwaway on-disk SQLite database in the OS temp dir, removed on
    /// drop, so the conformance path (which opens a file read-only) can be
    /// exercised without a real consumer build.
    mod tempdb {
        use std::path::{Path, PathBuf};

        pub struct TempDb {
            path: PathBuf,
        }

        impl TempDb {
            pub fn new(ddl: &str) -> Self {
                let mut path = std::env::temp_dir();
                let unique = format!(
                    "bsql_s13_{}_{}.db",
                    std::process::id(),
                    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                );
                path.push(unique);
                let conn = rusqlite::Connection::open(&path).expect("open temp db");
                conn.execute_batch(ddl).expect("replay ddl");
                drop(conn);
                TempDb { path }
            }

            pub fn path(&self) -> &Path {
                &self.path
            }
        }

        impl Drop for TempDb {
            fn drop(&mut self) {
                // Best-effort cleanup of the throwaway file; both outcomes are
                // fine (the test's correctness does not depend on removal).
                match std::fs::remove_file(&self.path) {
                    Ok(()) | Err(_) => {}
                }
            }
        }

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    }
}
