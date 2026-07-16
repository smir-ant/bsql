//! WRITE-ONCE CROSS-BACKEND PROOF — one generic data layer, two backends.
//!
//! This fixture is the end-to-end evidence for `bsql::SyncBackend`: a data layer
//! written ONCE against the generic trait compiles AND executes unchanged on the
//! blocking PostgreSQL driver (`bsql::pg_sync`) and the embedded SQLite driver
//! (`bsql::sqlite`), over the SAME `query!` record twins. It is the product-vision
//! headline — write once, run on any backend, compile-checked — made concrete.
//!
//! # What it proves (the generic functions below, each CALLED on both backends)
//!
//! * [`load_users`] — a PARAM-FREE typed `query!`, generic over the backend, with
//!   the clean flagship signature: `B: SyncBackend` + ONE
//!   `RunsOn<B, Params<'e> = (), Owned = UserRow>` bound.
//! * [`user_by_id`] — a SCALAR-PARAM typed `query!`, generic over the backend
//!   (`RunsOn<B, Params<'e> = (i64,), Owned = UserById>`). The SAME typed
//!   `Q::Params` tuple binds on BOTH backends — the SQLite `$N` param-bridge
//!   unlocked the cross-backend tuple; before it, a uniform parameterized verb
//!   across the two backends was unexpressible (see `disproof.rs`).
//! * [`find_user_by_email`] — a BORROWED-PARAM typed `query!` taking a RUNTIME
//!   `&str` (`RunsOn<B, Params<'e> = (&'e str, ), Owned = UserByEmail>`). The
//!   canonical `find_by_email` data-layer function, which the `'static`-params
//!   wall used to make INEXPRESSIBLE on the typed path; the `Params` lifetime GAT
//!   closes it — one explicit lifetime, no HRTB. `sqlx` expresses exactly this.
//! * [`load_users_and_orders`] — the honest ergonomic COST: N distinct `query!`s
//!   need N `RunsOn<B, ..>` bounds (here N = 2). One clause each, mechanical, no
//!   lifetime / GAT noise — a real tax, not an explosion.
//! * [`wipe_scratch`] — the raw-SQL verb unifies too (a generic maintenance
//!   helper over any blocking backend).
//! * [`wipe_in_tx`] — the transaction combinator, generic over the backend: an
//!   ATOMIC multi-statement group runs the same on both. Clean for the raw-SQL
//!   verbs (no extra bound), and proof that the guard `B::Tx<'t>` genuinely
//!   implements [`bsql::SyncQueries`].
//!
//! # The tx-guard scope limit (recorded honestly)
//!
//! The transaction combinator ships and the guard genuinely implements the
//! `SyncQueries` verbs, so generic RAW-SQL grouping is clean ([`wipe_in_tx`]).
//! Generic TYPED fetch ON the guard is NOT offered as a clean API: it needs a
//! higher-ranked `for<'t> Q: RunsOn<B::Tx<'t>, ..>` bound whose `B` the trait
//! solver cannot infer at a call site (an `E0277` "known limitation of the trait
//! solver" — a turbofish works around it, but the HRTB bound is off the
//! flagship's clean shape). That failure is recorded verbatim in `disproof.rs`.
//! The recommended pattern keeps typed reads at connection level and uses the
//! transaction for raw-SQL grouping (or a concrete backend for typed work inside
//! a transaction).
//!
//! Each generic function is monomorphised for BOTH `pg_sync::Connection` and
//! `sqlite::Connection`: the offline tests below RUN them in-process on SQLite,
//! and `tests/live_pg.rs` (`--ignored`) RUNS them on a live PostgreSQL. No `dyn`,
//! no boxing — fully static dispatch.

#![forbid(unsafe_code)]

// The DISPROOF half — real code + the verbatim error for the one thing that does
// NOT unify (the naive `Q::Owned` ambiguity that motivates keying `RunsOn` on the
// backend). Gated `#[cfg(any())]` inside, so it is preserved but never compiled.
mod disproof;

use bsql::pg_sync;
use bsql::sqlite;
use bsql::{BackendError, RunsOn, SyncBackend, SyncQueries};

// ── The shared `query!` carriers ───────────────────────────────────────────
//
// Each is typed at build time against the migration catalog AND cross-checked
// against real SQLite (feature `macros-sqlite`), so — projecting only SQLite
// storage classes and binding only SQLite-bindable params — each gains BOTH
// `TypedQuery` (the PG wire model) and `SqliteTypedQuery` (the SQLite value
// model) over the SAME generated record twins.

bsql::query!(UserRow, "SELECT id, email, name FROM users");
bsql::query!(OrderRow, "SELECT id, ref_no FROM orders");
// PARAMETERIZED: the `$1` binds as the typed tuple `(i64,)` on BOTH backends
// (`?1` on SQLite, via the shared placeholder authority + the SQLite param
// bridge). This carrier is the payoff of the unified parameterized verb.
bsql::query!(UserById, "SELECT id, email, name FROM users WHERE id = $1");
// BORROWED PARAM: the `$1` is a `text` column, so its typed tuple is
// `(&'p str,)` — a RUNTIME `&str` binds (not only a `&'static` literal). This is
// the canonical `find_by_email` shape the 'static-params wall used to make
// inexpressible; the `Params` lifetime GAT closes it.
bsql::query!(UserByEmail, "SELECT id, email, name FROM users WHERE email = $1");

// ════════════════════════════════════════════════════════════════════════
// THE GENERIC DATA LAYER — written ONCE, runs on any blocking backend
// ════════════════════════════════════════════════════════════════════════

/// Load every user, generic over the backend — the FLAGSHIP shape.
///
/// The whole ergonomic question lives in this signature: `B: SyncBackend`
/// (names the backend) + ONE `RunsOn<B, Params<'e> = (), Owned = UserRow>`
/// bound (names the carrier's params + owned record) + one explicit lifetime `'e`
/// (the parameter GAT lifetime — unused here). The `Params<'e>`/`Owned` equalities
/// let the argument be `()` and the return the CONCRETE `Vec<UserRow>`
/// rather than an opaque projection. No `dyn`, no HRTB, no unnameable lifetimes.
///
/// # Errors
///
/// The backend's classified error on a SQL / server / decode failure.
pub fn load_users<'e, B>(conn: &mut B) -> Result<Vec<UserRow>, B::Error>
where
    B: SyncBackend,
    UserRow: RunsOn<B, Params<'e> = (), Owned = UserRow>,
{
    conn.fetch_all::<UserRow>(())
}

/// Load one user by primary key, generic over the backend — the PARAMETERIZED
/// flagship shape. The SAME typed `(i64,)` tuple binds on both backends.
///
/// This is what the SQLite `$N` param-bridge unlocked: before it, PG's typed
/// verb took the compile-checked `Q::Params` tuple while SQLite took an untyped
/// `&[ValueRef]` slice, so a uniform parameterized verb across the two was
/// unexpressible generically. Now one signature — `RunsOn<B, Params<'e> = (i64,),
/// Owned = UserById>` — serves both.
///
/// # Errors
///
/// The backend's classified error; a too-many-rows error if the PK is not unique
/// (it is, so this is unreachable in practice).
pub fn user_by_id<'e, B>(conn: &mut B, id: i64) -> Result<Option<UserById>, B::Error>
where
    B: SyncBackend,
    UserById: RunsOn<B, Params<'e> = (i64,), Owned = UserById>,
{
    conn.fetch_opt::<UserById>((id,))
}

/// Load one user by primary key, REQUIRING it to exist — generic over the
/// backend. On zero rows the backend's classified error is a no-rows error, which
/// [`is_no_rows`](BackendError::is_no_rows) recognises IDENTICALLY on both
/// backends (`DriverError::NoRows` / `SqliteError::NoRows`) — so a generic
/// consumer can distinguish "absent" from a real failure without matching a
/// backend-specific enum. Returns `Ok(None)` for the absent case here to witness
/// that classification.
///
/// # Errors
///
/// The backend's classified error for any failure OTHER than no-rows (a no-rows
/// error is folded into `Ok(None)` by the `is_no_rows` check).
pub fn user_by_id_required<'e, B>(conn: &mut B, id: i64) -> Result<Option<UserById>, B::Error>
where
    B: SyncBackend,
    UserById: RunsOn<B, Params<'e> = (i64,), Owned = UserById>,
{
    match conn.fetch_one::<UserById>((id,)) {
        Ok(user) => Ok(Some(user)),
        // The cross-backend no-rows classification: identical on PG and SQLite.
        Err(e) if e.is_no_rows() => Ok(None),
        Err(e) => Err(e),
    }
}

/// Find one user by email — the CANONICAL parameterized data-layer function
/// (`find_by_email`), generic over the backend, taking a RUNTIME `&str`.
///
/// THIS is the function the `'static`-params wall used to make INEXPRESSIBLE: a
/// `text` `$1` parameter was `&'static str`, so a runtime `&str` (a request
/// field, a value from another row) could not be passed on the compile-checked
/// typed path on ANY backend (`E0521` / `E0597`); the only escapes forfeited the
/// compile-checking via the dynamic verbs, or leaked memory. The `Params`
/// lifetime GAT closes it — `Params<'e> = (&'e str,)` accepts the caller's borrow
/// while the const validator keeps riding the `'static` marker (OID pins
/// unchanged). `sqlx` expresses exactly this; bsql now does too, compile-checked,
/// on both backends.
///
/// # Errors
///
/// The backend's classified error on a SQL / server / decode failure.
pub fn find_user_by_email<'e, B>(
    conn: &mut B,
    email: &'e str,
) -> Result<Option<UserByEmail>, B::Error>
where
    B: SyncBackend,
    UserByEmail: RunsOn<B, Params<'e> = (&'e str,), Owned = UserByEmail>,
{
    conn.fetch_opt::<UserByEmail>((email,))
}

/// The honest ergonomic COST: a data-layer function running N distinct `query!`s
/// needs N `RunsOn<B, ..>` bounds (here N = 2). Linear in the number of queries,
/// ONE clause each, mechanical — a real tax, not an explosion. The reader judges
/// whether it clears the bar (it is far below the `sqlx`-went-`dyn` tipping
/// point: no lifetime soup, no opaque types, fully monomorphised).
///
/// # Errors
///
/// The backend's classified error on either query.
pub fn load_users_and_orders<'e, B>(
    conn: &mut B,
) -> Result<(Vec<UserRow>, Vec<OrderRow>), B::Error>
where
    B: SyncBackend,
    UserRow: RunsOn<B, Params<'e> = (), Owned = UserRow>,
    OrderRow: RunsOn<B, Params<'e> = (), Owned = OrderRow>,
{
    let users = conn.fetch_all::<UserRow>(())?;
    let orders = conn.fetch_all::<OrderRow>(())?;
    Ok((users, orders))
}

/// The raw-SQL verb unifies too — a generic maintenance helper over any blocking
/// backend, proving the non-typed half of the surface. Needs no `RunsOn` bound
/// (both drivers already take `&str`).
///
/// # Errors
///
/// The backend's classified error on a SQL / server failure.
pub fn wipe_scratch<B: SyncBackend>(conn: &mut B) -> Result<u64, B::Error> {
    conn.execute_sql("DELETE FROM users WHERE email = ''")
}

/// The transaction combinator, generic over the backend: an ATOMIC group of
/// raw-SQL statements runs the same on both — `Ok` commits, `Err` rolls back.
/// CLEAN for the raw-SQL verbs (no `RunsOn` bound); the guard exposes only the
/// data verbs, so a manual-`commit` desync is a compile error on both backends.
///
/// # Errors
///
/// The backend's classified error from the body (rolled back) or the
/// BEGIN / COMMIT itself.
pub fn wipe_in_tx<B: SyncBackend>(conn: &mut B) -> Result<u64, B::Error> {
    conn.transaction(|tx| {
        tx.execute_sql("DELETE FROM orders")?;
        tx.execute_sql("DELETE FROM users WHERE email = ''")
    })
}

// NOTE: a generic TYPED fetch INSIDE a generic transaction body — the shape
// `fn load_users_in_tx<B: SyncBackend>(..) where for<'t> UserRow:
// RunsOn<B::Tx<'t>, ..>` — is deliberately ABSENT. It is the tx-guard scope
// limit: the higher-ranked bound over the guard lifetime is provable when
// assumed (the fn body type-checks) but the solver cannot infer `B` at a call
// site (E0277), so it is not a clean generic API. `disproof.rs` records the
// verbatim error. Typed reads stay at connection level ([`load_users`] /
// [`user_by_id`]); the transaction is for raw-SQL grouping ([`wipe_in_tx`]).

// ════════════════════════════════════════════════════════════════════════
// DUAL-BACKEND TYPE-CHECK PROOF: the same generics forced to monomorphise
// ════════════════════════════════════════════════════════════════════════
//
// Each function below forces a concrete monomorphisation of a generic above.
// That they all compile is the compile-time half of the write-once proof (the
// runtime half is the tests: SQLite in-process below, live PG in tests/).

/// Force the PostgreSQL monomorphisation of the param-free flagship.
///
/// # Errors
///
/// The PG driver's classified error.
pub fn proof_load_users_pg(
    conn: &mut pg_sync::Connection,
) -> Result<Vec<UserRow>, pg_sync::DriverError> {
    load_users(conn)
}

/// Force the SQLite monomorphisation of the SAME param-free flagship.
///
/// # Errors
///
/// The SQLite driver's classified error.
pub fn proof_load_users_sqlite(
    conn: &mut sqlite::Connection,
) -> Result<Vec<UserRow>, sqlite::SqliteError> {
    load_users(conn)
}

/// Force the PostgreSQL monomorphisation of the PARAMETERIZED flagship.
///
/// # Errors
///
/// The PG driver's classified error.
pub fn proof_user_by_id_pg(
    conn: &mut pg_sync::Connection,
    id: i64,
) -> Result<Option<UserById>, pg_sync::DriverError> {
    user_by_id(conn, id)
}

/// Force the SQLite monomorphisation of the SAME PARAMETERIZED flagship.
///
/// # Errors
///
/// The SQLite driver's classified error.
pub fn proof_user_by_id_sqlite(
    conn: &mut sqlite::Connection,
    id: i64,
) -> Result<Option<UserById>, sqlite::SqliteError> {
    user_by_id(conn, id)
}

/// Force the PostgreSQL monomorphisation of the BORROWED-PARAM flagship — a
/// runtime `&str` on the compile-checked typed path (the wall-closer).
///
/// # Errors
///
/// The PG driver's classified error.
pub fn proof_find_user_by_email_pg(
    conn: &mut pg_sync::Connection,
    email: &str,
) -> Result<Option<UserByEmail>, pg_sync::DriverError> {
    find_user_by_email(conn, email)
}

/// Force the SQLite monomorphisation of the SAME borrowed-param flagship.
///
/// # Errors
///
/// The SQLite driver's classified error.
pub fn proof_find_user_by_email_sqlite(
    conn: &mut sqlite::Connection,
    email: &str,
) -> Result<Option<UserByEmail>, sqlite::SqliteError> {
    find_user_by_email(conn, email)
}

// ════════════════════════════════════════════════════════════════════════
// RUNTIME WITNESS: the generic data layer ACTUALLY EXECUTES (SQLite, in-process)
// ════════════════════════════════════════════════════════════════════════
//
// The proofs above type-check both monomorphisations. This RUNS the SQLite arm
// against a real in-memory database — so the unification is witnessed executing,
// not merely compiling. The live PG arm is `tests/live_pg.rs` (`--ignored`).

#[cfg(test)]
mod tests {
    use super::*;

    /// Seed a fresh schema through the unified raw-SQL verb only — generic over
    /// the backend, so the SAME setup serves any blocking backend.
    fn seed<B: SyncBackend>(conn: &mut B)
    where
        B::Error: core::fmt::Debug,
    {
        conn.execute_sql(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, name TEXT)",
        )
        .expect("create users");
        conn.execute_sql("CREATE TABLE orders (id BIGINT PRIMARY KEY, ref_no TEXT NOT NULL)")
            .expect("create orders");
        conn.execute_sql("INSERT INTO users VALUES (1, 'a@b', 'Alice'), (2, 'c@d', NULL)")
            .expect("insert users");
        conn.execute_sql("INSERT INTO orders VALUES (10, 'R-1'), (20, 'R-2')")
            .expect("insert orders");
    }

    #[test]
    fn param_free_flagship_executes_on_sqlite() {
        let mut conn = sqlite::Connection::open_in_memory().expect("open in-memory db");
        seed(&mut conn);

        // The GENERIC `load_users<B>`, monomorphised for SQLite, actually runs.
        let users = load_users(&mut conn).expect("load_users runs on sqlite");
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].id, 1);
        assert_eq!(users[0].email, "a@b");
        assert_eq!(users[0].name.as_deref(), Some("Alice"));
        assert_eq!(users[1].id, 2);
        assert_eq!(users[1].name, None);
    }

    #[test]
    fn parameterized_flagship_executes_on_sqlite() {
        let mut conn = sqlite::Connection::open_in_memory().expect("open in-memory db");
        seed(&mut conn);

        // The GENERIC PARAMETERIZED `user_by_id<B>` binds the typed `(i64,)`
        // tuple on SQLite (the param-bridge payoff) and decodes the shared twin.
        let alice = user_by_id(&mut conn, 1)
            .expect("user_by_id runs on sqlite")
            .expect("user 1 exists");
        assert_eq!(alice.id, 1);
        assert_eq!(alice.email, "a@b");
        assert_eq!(alice.name.as_deref(), Some("Alice"));

        let bob = user_by_id(&mut conn, 2)
            .expect("user_by_id runs")
            .expect("user 2 exists");
        assert_eq!(bob.name, None);

        // A miss is a clean `None` (at-most-one contract), not an error.
        assert!(
            user_by_id(&mut conn, 999)
                .expect("user_by_id runs")
                .is_none()
        );
    }

    #[test]
    fn borrowed_param_flagship_executes_on_sqlite() {
        let mut conn = sqlite::Connection::open_in_memory().expect("open in-memory db");
        seed(&mut conn);

        // A RUNTIME `String` — NOT a `'static` literal — bound on the typed path.
        // This is exactly what the `'static`-params wall used to reject.
        let email = String::from("a@b");
        let alice = find_user_by_email(&mut conn, &email)
            .expect("find_user_by_email runs on sqlite")
            .expect("alice exists");
        assert_eq!(alice.id, 1);
        assert_eq!(alice.email, "a@b");
        assert_eq!(alice.name.as_deref(), Some("Alice"));

        // A runtime miss is a clean `None`, proving the borrowed param filtered.
        let missing = String::from("nobody@nowhere");
        assert!(
            find_user_by_email(&mut conn, &missing)
                .expect("runs")
                .is_none()
        );
    }

    #[test]
    fn no_rows_classification_is_cross_backend_on_sqlite() {
        let mut conn = sqlite::Connection::open_in_memory().expect("open in-memory db");
        seed(&mut conn);

        // `fetch_one` on a present id yields the row; on an absent id the backend's
        // classified error is a no-rows error, recognised generically via
        // `is_no_rows` (the SAME predicate as on PG).
        assert!(
            user_by_id_required(&mut conn, 1)
                .expect("required load runs")
                .is_some()
        );
        assert!(
            user_by_id_required(&mut conn, 999)
                .expect("no-rows folds to None via is_no_rows")
                .is_none()
        );
    }

    #[test]
    fn two_queries_and_raw_verb_execute_on_sqlite() {
        let mut conn = sqlite::Connection::open_in_memory().expect("open in-memory db");
        seed(&mut conn);

        let (users, orders) =
            load_users_and_orders(&mut conn).expect("two-query helper runs on sqlite");
        assert_eq!(users.len(), 2);
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].ref_no, "R-1");

        // Raw-SQL generic verb: no empty-email rows, so 0 affected.
        assert_eq!(wipe_scratch(&mut conn).expect("wipe_scratch runs"), 0);
    }

    #[test]
    fn transaction_combinator_executes_on_sqlite() {
        let mut conn = sqlite::Connection::open_in_memory().expect("open in-memory db");
        seed(&mut conn);
        // Add an empty-email row so the tx wipe has something to delete.
        conn.execute_sql("INSERT INTO users VALUES (3, '', 'ghost')")
            .expect("insert ghost");

        // The generic transaction combinator: atomic raw-SQL group. Deletes both
        // orders and the one empty-email user, committed as a unit. This proves
        // the guard `B::Tx` implements `SyncQueries` generically (its
        // `execute_sql` is a `SyncQueries` verb).
        let affected = wipe_in_tx(&mut conn).expect("wipe_in_tx runs on sqlite");
        assert_eq!(affected, 1); // the one empty-email user

        // The two real users survive the wipe — read back at CONNECTION level
        // (the clean shape; typed fetch on the tx guard is the scope limit).
        let survivors = load_users(&mut conn).expect("load_users after tx");
        assert_eq!(survivors.len(), 2);
    }
}
