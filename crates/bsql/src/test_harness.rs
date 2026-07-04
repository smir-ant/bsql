//! Runtime support for the `#[bsql::test]` schema-per-test isolation attribute.
//!
//! NOT a stable API and NOT for direct use — the `#[bsql::test]` expansion names
//! [`run_schema_isolated_test`] (async body) or [`run_schema_isolated_test_sync`]
//! (blocking body) through the hidden `::bsql::__test_rt` re-export. It is
//! compiled only under the non-default `test-harness` feature, so a production
//! build never pulls the runtime or the drivers this harness composes.
//!
//! # What one `#[bsql::test]` does
//!
//! Each test runs against its own freshly-created PostgreSQL schema and drops
//! that schema on exit — even if the test panics. Two such tests run in
//! parallel against the same server without interfering, because each sees only
//! its own schema (the connection's connect-time `search_path` pins every
//! unqualified name to it). The schema name is unique per test invocation and
//! per process, so a repeated run or a leaked prior schema never collides.
//!
//! # Async and sync are twins
//!
//! An `async fn` test drives the async driver behind a per-test tokio runtime; a
//! plain `fn` test drives the blocking driver directly, with no runtime. The two
//! entry points share ALL the driver-agnostic logic — the DSN resolution, the
//! unique injection-safe schema name, the schema DDL, and the error type — so a
//! fix to one can never silently diverge from the other. Only the connect and
//! the run-the-body steps differ (`.await` versus blocking).
//!
//! # The DSN
//!
//! The server is named by the [`DSN_ENV`] environment variable. It is a
//! *test* variable, deliberately distinct from an application's `DATABASE_URL`:
//! this harness `CREATE`s and `DROP`s schemas, so it must never be pointed at a
//! production database by accident. An unset variable is a loud panic naming the
//! variable, never a silent skip.

use core::fmt;
use core::sync::atomic::{AtomicU64, Ordering};
use std::env::VarError;

use bsql_postgres_core::{ConnectConfig, DriverError, sql_ident};

// The async and sync drivers expose a `Connection` each; the harness composes
// both, so each is imported under an explicit alias. `ConnectConfig`,
// `DriverError`, and the `sql_ident` validator live in the shared core, so the
// driver-agnostic logic below names core — not either driver.
use bsql_postgres_async::Connection as AsyncConnection;
use bsql_postgres_sync::Connection as SyncConnection;

// ════════════════════════════════════════════════════════════════════
// Shared, driver-agnostic core (one definition for both the async and the
// sync harness — the DSN resolve, the unique schema name, the schema DDL,
// and the error type).
// ════════════════════════════════════════════════════════════════════

/// The environment variable naming the PostgreSQL DSN each `#[bsql::test]`
/// connects to (e.g. `postgres://user@localhost/postgres`).
///
/// Deliberately bsql-specific rather than the ecosystem's overloaded
/// `DATABASE_URL`: a schema-per-test harness creates and drops schemas, so
/// pointing it at a production database must be an explicit, unambiguous act.
pub const DSN_ENV: &str = "BSQL_TEST_DSN";

/// Maximum bytes in a PostgreSQL identifier (`NAMEDATALEN - 1`). A schema name
/// exceeding it would be truncated by the server; we keep every generated name
/// within it by construction.
const MAX_SCHEMA_LEN: usize = 63;

/// Fixed prefix marking every harness-created schema, so a leaked one is
/// greppable (`DROP SCHEMA bsql_t_%`). Begins with a letter, so the whole
/// generated name is always a valid unquoted identifier.
const SCHEMA_PREFIX: &str = "bsql_t_";

/// Process-wide monotonic counter. Together with the process id it makes every
/// generated schema name unique across parallel tests in one process (the
/// counter) and across separate runs or a leaked prior schema (the pid) —
/// deterministically, with no randomness and no `SystemTime` seed. Shared by
/// both harnesses, so an async and a sync test in the same process never draw
/// the same name.
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A failure setting up (or tearing down) an isolated schema. Surfaced to the
/// test as a loud panic — the correct failure mode for a test that cannot run
/// in isolation.
#[derive(Debug)]
enum HarnessError {
    /// `BSQL_TEST_DSN` is not set.
    DsnMissing,
    /// `BSQL_TEST_DSN` is set but not valid UTF-8.
    DsnNotUnicode,
    /// `BSQL_TEST_DSN` is set but not a parseable DSN.
    DsnParse(String),
    /// The generated schema name failed identifier validation (a harness bug
    /// guard — the name is injection-safe by construction, so this is the
    /// tier-1 splice guard, not an expected path).
    SchemaName(DriverError),
    /// A connection or a `CREATE`/`DROP SCHEMA` statement failed on the server.
    Db(DriverError),
}

impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DsnMissing => write!(
                f,
                "the {DSN_ENV} environment variable is not set — #[bsql::test] \
                 needs a PostgreSQL DSN to run each test in an isolated schema, \
                 e.g. {DSN_ENV}=postgres://user@localhost/postgres"
            ),
            Self::DsnNotUnicode => write!(f, "{DSN_ENV} is not valid UTF-8"),
            Self::DsnParse(msg) => write!(f, "{DSN_ENV} is not a valid DSN: {msg}"),
            Self::SchemaName(e) => write!(f, "generated schema name is not a valid identifier: {e}"),
            Self::Db(e) => write!(f, "{e}"),
        }
    }
}

impl From<DriverError> for HarnessError {
    fn from(e: DriverError) -> Self {
        Self::Db(e)
    }
}

/// Resolve the base connect configuration from the DSN environment lookup.
///
/// Pure over its argument — it takes the `env::var` result rather than reading
/// the process environment — so the missing/malformed cases are testable
/// deterministically without racing parallel tests over a global variable.
/// Shared by the async and sync entry points, so both classify a missing or
/// malformed DSN identically.
fn resolve_dsn(var: Result<String, VarError>) -> Result<ConnectConfig, HarnessError> {
    match var {
        Ok(dsn) => ConnectConfig::from_dsn(&dsn).map_err(HarnessError::DsnParse),
        Err(VarError::NotPresent) => Err(HarnessError::DsnMissing),
        Err(VarError::NotUnicode(_)) => Err(HarnessError::DsnNotUnicode),
    }
}

/// Build a unique, injection-safe schema name for one test invocation.
///
/// Shape: `bsql_t_<pid>_<seq>[_<name>]`. The `<pid>_<seq>` core is the
/// collision-critical part and is always well within [`MAX_SCHEMA_LEN`]; the
/// human-readable `<name>` suffix (the test's name, lowercased with every
/// non-alphanumeric byte mapped to `_`) is appended only as far as the identifier
/// budget allows, so the name is at most 63 bytes and always a valid unquoted
/// identifier — no matter how the test was named.
fn unique_schema_name(test_name: &str) -> String {
    let pid = std::process::id();
    let seq = SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = format!("{SCHEMA_PREFIX}{pid}_{seq}");

    // Sanitize the test name to the unquoted-identifier alphabet. Every byte is
    // ASCII after this, so byte length equals character count and truncation to
    // the byte budget cannot split a multi-byte character.
    let mut sanitized = String::with_capacity(test_name.len());
    for ch in test_name.chars() {
        sanitized.push(if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '_' });
    }

    // Append `_<sanitized>` only if at least one suffix byte fits.
    if !sanitized.is_empty() && name.len() + 1 < MAX_SCHEMA_LEN {
        let budget = MAX_SCHEMA_LEN - name.len() - 1;
        name.push('_');
        name.extend(sanitized.chars().take(budget));
    }
    name
}

/// Validate a generated schema name as a plain unquoted identifier — the tier-1
/// injection guard applied before the name is spliced into DDL text (there is no
/// parameterized form for a schema identifier). The name is injection-safe by
/// construction, so a rejection here is a harness bug, not an expected path.
/// Shared, so the guard is defined once for both harnesses.
fn validate_schema_name(schema: &str) -> Result<(), HarnessError> {
    sql_ident::validate_identifier(schema).map_err(HarnessError::SchemaName)
}

/// The `DROP SCHEMA IF EXISTS <schema> CASCADE` DDL for `schema`. Shared string
/// builder, so the async and sync harness splice the identical text — a fix to
/// one cannot diverge from the other. `IF EXISTS` makes it idempotent (a
/// double-drop, a never-created schema, or a leaked prior schema of the same
/// name is not an error).
fn drop_schema_ddl(schema: &str) -> String {
    format!("DROP SCHEMA IF EXISTS {schema} CASCADE")
}

/// The `CREATE SCHEMA <schema>` DDL for `schema`. Shared string builder.
fn create_schema_ddl(schema: &str) -> String {
    format!("CREATE SCHEMA {schema}")
}

/// Surface an unrecoverable harness condition as a loud panic. This is the
/// single panic site: setup failures (missing DSN, unreachable server, refused
/// `CREATE`/`DROP SCHEMA`) fail the test loudly rather than skipping silently.
#[expect(
    clippy::panic,
    reason = "the #[bsql::test] harness surfaces a setup or teardown failure \
              (missing DSN, unreachable server, refused CREATE/DROP SCHEMA) as a \
              loud panic — the correct failure mode for a test that cannot run in \
              isolation; the harness is test-support and never enters a production path"
)]
fn harness_fail(args: fmt::Arguments<'_>) -> ! {
    panic!("bsql::test: {args}");
}

/// Resolve the base config the same way the run entry points do, but map the
/// classified error to its display string so a fixture can assert the
/// missing/malformed-DSN message without naming an internal type.
///
/// NOT a stable API — exists so the "unset DSN is a loud, named error" witness
/// can be exercised deterministically (no global-env race, no server). Because
/// the resolver is shared, this witness covers both the async and the sync path.
#[doc(hidden)]
pub fn resolve_base_config(var: Result<String, VarError>) -> Result<ConnectConfig, String> {
    resolve_dsn(var).map_err(|e| e.to_string())
}

// ════════════════════════════════════════════════════════════════════
// Async harness — an `async fn` body over the tokio async driver.
// ════════════════════════════════════════════════════════════════════

/// Create the isolated schema on the (public-search_path) admin connection.
///
/// The name is validated as a plain unquoted identifier first (the shared tier-1
/// injection guard), then the shared DDL is spliced. A leaked prior schema of
/// the same name (a crashed run whose teardown never ran, or an OS pid reused
/// after the old process died) is dropped first, so `CREATE` is idempotent.
async fn create_isolated_schema(admin: &mut AsyncConnection, schema: &str) -> Result<(), HarnessError> {
    validate_schema_name(schema)?;
    admin.execute_sql(&drop_schema_ddl(schema)).await?;
    admin.execute_sql(&create_schema_ddl(schema)).await?;
    Ok(())
}

/// Drop the isolated schema (and everything in it) on the admin connection.
async fn drop_isolated_schema(admin: &mut AsyncConnection, schema: &str) -> Result<(), HarnessError> {
    validate_schema_name(schema)?;
    admin.execute_sql(&drop_schema_ddl(schema)).await?;
    Ok(())
}

/// Run one `async` `#[bsql::test]` body in a freshly-created, isolated
/// PostgreSQL schema, dropping that schema on exit even if the body panics.
///
/// Control flow:
/// 1. Build a current-thread tokio runtime (each test owns its own).
/// 2. Resolve [`DSN_ENV`], connect an admin connection (public `search_path`),
///    and `CREATE` the unique schema. Any failure here is a loud panic.
/// 3. Connect the test connection with the schema pinned as its connect-time
///    `search_path`, and hand it to `body`.
/// 4. Run `body` inside [`std::panic::catch_unwind`] so a body panic cannot
///    skip teardown.
/// 5. `DROP` the schema on the admin connection — always.
/// 6. Reconcile: a passing body with a failed teardown is a loud panic (the
///    schema may be leaked); a panicking body re-raises its original panic (so
///    `#[should_panic]` still works) after teardown has run.
///
/// The admin connection — never touched by the body — is what runs `DROP`, so
/// teardown does not depend on the health of a connection a failing test may
/// have left mid-statement.
pub fn run_schema_isolated_test<F>(test_name: &str, body: F)
where
    F: AsyncFnOnce(&mut AsyncConnection),
{
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => harness_fail(format_args!("could not build a tokio runtime: {e}")),
    };

    let base = match resolve_dsn(std::env::var(DSN_ENV)) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!("{e}")),
    };
    let schema = unique_schema_name(test_name);

    let mut admin = match rt.block_on(AsyncConnection::connect(&base)) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!(
            "could not connect the setup connection (via {DSN_ENV}) for schema '{schema}': {e}"
        )),
    };
    if let Err(e) = rt.block_on(create_isolated_schema(&mut admin, &schema)) {
        harness_fail(format_args!("could not create isolated schema '{schema}': {e}"));
    }

    // The test connection is pinned to the isolated schema via its connect-time
    // search_path, so every unqualified name resolves inside it.
    let test_cfg = base.with_search_path(schema.as_str());
    let mut test_conn = match rt.block_on(AsyncConnection::connect(&test_cfg)) {
        Ok(c) => c,
        Err(e) => {
            // The schema is already created; drop it before failing so a
            // test-connection failure never leaks a schema.
            match rt.block_on(drop_isolated_schema(&mut admin, &schema)) {
                Ok(()) => {}
                Err(drop_err) => eprintln!(
                    "bsql::test: WARNING — could not drop schema '{schema}' after a failed \
                     test-connection setup; it may be leaked: {drop_err}"
                ),
            }
            harness_fail(format_args!(
                "could not connect the test connection for schema '{schema}': {e}"
            ));
        }
    };

    // Run the body, catching any panic so teardown always runs. The `&mut`
    // borrow of `test_conn` ends when `catch_unwind` returns.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(body(&mut test_conn))
    }));

    // Teardown, always.
    let teardown = rt.block_on(drop_isolated_schema(&mut admin, &schema));

    match outcome {
        Ok(()) => {
            if let Err(e) = teardown {
                harness_fail(format_args!(
                    "test body passed but dropping isolated schema '{schema}' failed \
                     (it may be leaked): {e}"
                ));
            }
        }
        Err(payload) => {
            // The body's panic is the primary signal. Surface a teardown failure
            // loudly, but propagate the original panic (so `#[should_panic]` and
            // the real failure message survive).
            if let Err(e) = teardown {
                eprintln!(
                    "bsql::test: WARNING — dropping isolated schema '{schema}' failed after the \
                     test panicked; it may be leaked: {e}"
                );
            }
            std::panic::resume_unwind(payload);
        }
    }
}

/// Return whether `schema` currently exists on the server named by
/// [`DSN_ENV`], probed over the async driver. Builds its own runtime and admin
/// connection; a loud panic on any infrastructure failure. NOT a stable API —
/// exists for the teardown witnesses.
#[doc(hidden)]
pub fn schema_exists(schema: &str) -> bool {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => harness_fail(format_args!("could not build a tokio runtime: {e}")),
    };
    let base = match resolve_dsn(std::env::var(DSN_ENV)) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!("{e}")),
    };
    rt.block_on(async {
        let mut admin = match AsyncConnection::connect(&base).await {
            Ok(c) => c,
            Err(e) => harness_fail(format_args!("could not connect to check schema '{schema}': {e}")),
        };
        // Bind-parameterized — no identifier splice — so any string is safe to probe.
        let row = match admin
            .query_params_one(
                "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1",
                &(schema,),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => harness_fail(format_args!("could not query schema existence for '{schema}': {e}")),
        };
        match row.get_i64(0) {
            Ok(Some(n)) => n > 0,
            other => harness_fail(format_args!("unexpected schema-existence count for '{schema}': {other:?}")),
        }
    })
}

// ════════════════════════════════════════════════════════════════════
// Sync harness — a plain `fn` body over the std::net blocking driver. A
// faithful twin of the async path above: it shares the DSN resolve, the
// schema name, the DDL, and the error type, and differs only in that it
// connects and runs the body directly — no tokio runtime, no `.await`.
// ════════════════════════════════════════════════════════════════════

/// Create the isolated schema on the (public-search_path) admin connection —
/// the blocking twin of [`create_isolated_schema`]. Same shared validation and
/// DDL; blocking calls instead of `.await`.
fn create_isolated_schema_sync(admin: &mut SyncConnection, schema: &str) -> Result<(), HarnessError> {
    validate_schema_name(schema)?;
    admin.execute_sql(&drop_schema_ddl(schema))?;
    admin.execute_sql(&create_schema_ddl(schema))?;
    Ok(())
}

/// Drop the isolated schema on the admin connection — the blocking twin of
/// [`drop_isolated_schema`].
fn drop_isolated_schema_sync(admin: &mut SyncConnection, schema: &str) -> Result<(), HarnessError> {
    validate_schema_name(schema)?;
    admin.execute_sql(&drop_schema_ddl(schema))?;
    Ok(())
}

/// Run one synchronous `#[bsql::test]` body in a freshly-created, isolated
/// PostgreSQL schema, dropping that schema on exit even if the body panics —
/// the blocking twin of [`run_schema_isolated_test`].
///
/// The control flow mirrors the async path exactly, minus the runtime: it
/// resolves [`DSN_ENV`], connects an admin connection, `CREATE`s the unique
/// schema, connects the test connection pinned to it, runs `body` inside
/// [`std::panic::catch_unwind`], and `DROP`s the schema on the admin connection —
/// always — reconciling a body panic against a teardown failure the same way.
///
/// One difference from the async twin: the blocking `Connection` has a `Drop`
/// (a best-effort, panic-free graceful terminate over the socket). The test
/// connection is therefore dropped at an explicit point BEFORE the reconcile
/// below, so its `Drop` never runs during the `resume_unwind` unwind — teardown
/// and a re-raised body panic can never combine into a double-panic abort.
pub fn run_schema_isolated_test_sync<F>(test_name: &str, body: F)
where
    F: FnOnce(&mut SyncConnection),
{
    let base = match resolve_dsn(std::env::var(DSN_ENV)) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!("{e}")),
    };
    let schema = unique_schema_name(test_name);

    let mut admin = match SyncConnection::connect(&base) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!(
            "could not connect the setup connection (via {DSN_ENV}) for schema '{schema}': {e}"
        )),
    };
    if let Err(e) = create_isolated_schema_sync(&mut admin, &schema) {
        harness_fail(format_args!("could not create isolated schema '{schema}': {e}"));
    }

    // The test connection is pinned to the isolated schema via its connect-time
    // search_path, so every unqualified name resolves inside it.
    let test_cfg = base.with_search_path(schema.as_str());
    let mut test_conn = match SyncConnection::connect(&test_cfg) {
        Ok(c) => c,
        Err(e) => {
            // The schema is already created; drop it before failing so a
            // test-connection failure never leaks a schema.
            match drop_isolated_schema_sync(&mut admin, &schema) {
                Ok(()) => {}
                Err(drop_err) => eprintln!(
                    "bsql::test: WARNING — could not drop schema '{schema}' after a failed \
                     test-connection setup; it may be leaked: {drop_err}"
                ),
            }
            harness_fail(format_args!(
                "could not connect the test connection for schema '{schema}': {e}"
            ));
        }
    };

    // Run the body, catching any panic so teardown always runs. The `&mut`
    // borrow of `test_conn` ends when `catch_unwind` returns.
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&mut test_conn)));

    // Drop the test connection at THIS controlled point — its (best-effort,
    // panic-free) `Drop` runs here, never on the `resume_unwind` unwind path.
    drop(test_conn);

    // Teardown, always, on the never-touched admin connection.
    let teardown = drop_isolated_schema_sync(&mut admin, &schema);

    match outcome {
        Ok(()) => {
            if let Err(e) = teardown {
                harness_fail(format_args!(
                    "test body passed but dropping isolated schema '{schema}' failed \
                     (it may be leaked): {e}"
                ));
            }
        }
        Err(payload) => {
            // The body's panic is the primary signal. Surface a teardown failure
            // loudly, but propagate the original panic (so `#[should_panic]` and
            // the real failure message survive).
            if let Err(e) = teardown {
                eprintln!(
                    "bsql::test: WARNING — dropping isolated schema '{schema}' failed after the \
                     test panicked; it may be leaked: {e}"
                );
            }
            std::panic::resume_unwind(payload);
        }
    }
}

/// Return whether `schema` currently exists on the server named by
/// [`DSN_ENV`], probed over the blocking driver — the sync twin of
/// [`schema_exists`]. A loud panic on any infrastructure failure. NOT a stable
/// API — exists for the sync teardown witnesses.
#[doc(hidden)]
pub fn schema_exists_sync(schema: &str) -> bool {
    let base = match resolve_dsn(std::env::var(DSN_ENV)) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!("{e}")),
    };
    let mut admin = match SyncConnection::connect(&base) {
        Ok(c) => c,
        Err(e) => harness_fail(format_args!("could not connect to check schema '{schema}': {e}")),
    };
    // Bind-parameterized — no identifier splice — so any string is safe to probe.
    let row = match admin.query_params_one(
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1",
        &(schema,),
    ) {
        Ok(r) => r,
        Err(e) => harness_fail(format_args!("could not query schema existence for '{schema}': {e}")),
    };
    match row.get_i64(0) {
        Ok(Some(n)) => n > 0,
        other => harness_fail(format_args!("unexpected schema-existence count for '{schema}': {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_dsn_is_a_loud_named_error() {
        match resolve_dsn(Err(VarError::NotPresent)) {
            Err(HarnessError::DsnMissing) => {}
            other => panic!("a missing DSN must classify as DsnMissing, got {other:?}"),
        }
        // The message must name the env var, so the failure is actionable.
        let msg = HarnessError::DsnMissing.to_string();
        assert!(msg.contains(DSN_ENV), "missing-DSN message must name {DSN_ENV}, got: {msg}");
    }

    #[test]
    fn malformed_dsn_is_classified() {
        match resolve_dsn(Ok("not-a-dsn".to_string())) {
            Err(HarnessError::DsnParse(_)) => {}
            other => panic!("a malformed DSN must classify as DsnParse, got {other:?}"),
        }
    }

    #[test]
    fn valid_dsn_resolves() {
        match resolve_dsn(Ok("postgres://u@localhost/db".to_string())) {
            Ok(cfg) => assert_eq!(cfg.user, "u"),
            Err(e) => panic!("a valid DSN must resolve, got {e:?}"),
        }
    }

    #[test]
    fn generated_schema_names_are_valid_identifiers() {
        for test_name in [
            "simple",
            "creates_a_user",
            "MixedCaseName",
            "weird-name.with spaces!",
            "",
            &"x".repeat(200),
            "юникод",
        ] {
            let name = unique_schema_name(test_name);
            assert!(name.len() <= MAX_SCHEMA_LEN, "{name:?} exceeds {MAX_SCHEMA_LEN} bytes");
            assert!(
                sql_ident::validate_identifier(&name).is_ok(),
                "{name:?} (from {test_name:?}) must be a valid unquoted identifier",
            );
            assert!(name.starts_with(SCHEMA_PREFIX), "{name:?} must carry the harness prefix");
        }
    }

    #[test]
    fn generated_schema_names_are_unique_across_invocations() {
        // Same test name, repeated: the atomic counter must disambiguate.
        let a = unique_schema_name("same");
        let b = unique_schema_name("same");
        assert_ne!(a, b, "two invocations must produce distinct schema names");
    }

    #[test]
    fn schema_ddl_is_built_from_the_validated_name() {
        // The shared DDL builders splice exactly the name, so the async and sync
        // harnesses emit identical statements.
        assert_eq!(drop_schema_ddl("s"), "DROP SCHEMA IF EXISTS s CASCADE");
        assert_eq!(create_schema_ddl("s"), "CREATE SCHEMA s");
    }

    #[test]
    fn tokio_block_on_propagates_a_body_panic() {
        // Proves the teardown-on-panic mechanism at the tokio boundary, with no
        // server: a panic inside `block_on` unwinds through it and is caught.
        let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => panic!("runtime build failed: {e}"),
        };
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { panic!("boom from inside the future") })
        }));
        assert!(caught.is_err(), "a panic inside block_on must be catchable");
    }

    #[test]
    fn sync_catch_unwind_propagates_a_body_panic() {
        // The sync twin of the boundary proof: a panic inside a plain closure is
        // caught (no runtime), so teardown can always run before the re-raise.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("boom from inside the sync body")
        }));
        assert!(caught.is_err(), "a panic inside the sync body must be catchable");
    }
}
