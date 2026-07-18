//! LIVE proof that a typed `query!` parameter bound INTO a NULLABLE column is
//! `Option<T>` and round-trips SQL NULL (both drivers).
//!
//! A compile-checked `INSERT ... VALUES ($N)` / `UPDATE ... SET col = $N` types
//! a `$N` bound as a bare value into a NULLABLE target column as `Option<T>`, so
//! `Some(x)` inserts x and `None` inserts SQL NULL — all through the typed path,
//! with no dynamic escape hatch. A `$N` bound into a NOT NULL column keeps the
//! base type `T` (the bare `i32` below is compile-enforced — the test would not
//! compile if `id`'s param were `Option<i32>`). The wire OID is unchanged
//! (`Option<T>` and `T` bind the same param OID — a NULL is typed by its column),
//! so this is purely the Rust surface type.
//!
//! PARALLEL-SAFE: every test creates its OWN connection and a PER-CONNECTION
//! `CREATE TEMP TABLE np_rows (...)` shadowing the `0025_nullable_param.sql`
//! migration table. A TEMP table is session-local, so tests never interfere —
//! run WITHOUT `--test-threads=1`.
//!
//! Run with:
//!   cargo test -p bsql-query-fixture --test nullable_param_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

// `id` is NOT NULL (PK), so its `$1` param is the base `i32`; `note` (text) and
// `score` (int4) are NULLABLE, so their `$2` / `$3` params are `Option<&str>` /
// `Option<i32>`. RETURNING decodes `note` -> `Option<String>`, `score` ->
// `Option<i32>`.
bsql::query!(
    NpInsert,
    "INSERT INTO np_rows (id, note, score) VALUES ($1, $2, $3) RETURNING id, note, score"
);
// UPDATE the nullable `score` from a bare `$1` — its param is `Option<i32>` too.
bsql::query!(
    NpUpdateScore,
    "UPDATE np_rows SET score = $1 WHERE id = $2 RETURNING id, note, score"
);
bsql::query!(NpById, "SELECT id, note, score FROM np_rows WHERE id = $1");

const TEMP_DDL: &str =
    "CREATE TEMP TABLE np_rows (id int4 PRIMARY KEY, note text, score int4)";

mod sync_driver {
    use super::{NpById, NpInsert, NpUpdateScore, TEMP_DDL};
    use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn some_inserts_the_value_none_inserts_null_and_reads_back() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query(TEMP_DDL).expect("create temp");

        // Row 1: Some(..) into both nullable columns. `id`'s param is a bare
        // `i32` (NOT NULL) — no `Some(..)` wrapper compiles here.
        let one = c
            .query_one::<NpInsert>((1i32, Some("hello"), Some(42i32)))
            .expect("insert Some/Some");
        assert_eq!(one.id, 1);
        assert_eq!(one.note.as_deref(), Some("hello"), "RETURNING note = Some");
        assert_eq!(one.score, Some(42), "RETURNING score = Some");

        // Row 2: None into both nullable columns -> SQL NULL.
        let two = c
            .query_one::<NpInsert>((2i32, None, None))
            .expect("insert None/None");
        assert_eq!(two.id, 2);
        assert_eq!(two.note, None, "None note -> SQL NULL -> RETURNING None");
        assert_eq!(two.score, None, "None score -> SQL NULL -> RETURNING None");

        // Read both back through a SELECT carrier: the NULLs decode as None.
        let r1 = c.query_one::<NpById>((1i32,)).expect("select 1");
        assert_eq!(r1.note.as_deref(), Some("hello"));
        assert_eq!(r1.score, Some(42));
        let r2 = c.query_one::<NpById>((2i32,)).expect("select 2");
        assert_eq!(r2.note, None);
        assert_eq!(r2.score, None);

        // UPDATE the nullable `score` to NULL via a bare `$1` (`Option<i32>`),
        // then back to a value — both through the typed path.
        let cleared = c
            .query_one::<NpUpdateScore>((None, 1i32))
            .expect("update score -> NULL");
        assert_eq!(cleared.score, None, "SET score = None -> SQL NULL");
        let restored = c
            .query_one::<NpUpdateScore>((Some(7i32), 1i32))
            .expect("update score -> 7");
        assert_eq!(restored.score, Some(7), "SET score = Some(7) -> 7");
    }
}

mod async_driver {
    use super::{NpById, NpInsert, NpUpdateScore, TEMP_DDL};
    use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn some_inserts_the_value_none_inserts_null_and_reads_back() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query(TEMP_DDL).await.expect("create temp");

        let one = c
            .query_one::<NpInsert>((1i32, Some("hello"), Some(42i32)))
            .await
            .expect("insert Some/Some");
        assert_eq!(one.id, 1);
        assert_eq!(one.note.as_deref(), Some("hello"));
        assert_eq!(one.score, Some(42));

        let two = c
            .query_one::<NpInsert>((2i32, None, None))
            .await
            .expect("insert None/None");
        assert_eq!(two.note, None);
        assert_eq!(two.score, None);

        let r2 = c.query_one::<NpById>((2i32,)).await.expect("select 2");
        assert_eq!(r2.note, None);
        assert_eq!(r2.score, None);

        let cleared = c
            .query_one::<NpUpdateScore>((None, 1i32))
            .await
            .expect("update score -> NULL");
        assert_eq!(cleared.score, None);
        let restored = c
            .query_one::<NpUpdateScore>((Some(7i32), 1i32))
            .await
            .expect("update score -> 7");
        assert_eq!(restored.score, Some(7));
    }
}
