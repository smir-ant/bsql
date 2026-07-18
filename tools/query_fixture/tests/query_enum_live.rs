//! LIVE user-defined ENUM round-trip over BOTH drivers — the audit-4 flagship.
//!
//! Proves end-to-end against a real PostgreSQL that a `CREATE TYPE ... AS ENUM`
//! migration generates a Rust `enum` (`bsql::user_types!()`), and `query!`:
//!   * DECODES a `mood` column into `Mood` (NOT NULL) / `Option<Mood>` (nullable),
//!   * ENCODES a `Mood` bind parameter (`Mood::Happy.as_label()`) the server
//!     coerces from context (an `unspecified`-typed label — a PG enum has no
//!     `text` cast), round-tripping it back through `RETURNING`,
//!   * classifies an UNKNOWN label (a value ALTERed into the live enum
//!     out-of-band, absent from the migration) as `DecodeError::UnknownEnumLabel`
//!     — never a panic or a plausible-but-wrong variant.
//!
//! The migration `0014_moods.sql` declares the same type/table this test
//! creates on the live server, so the compile-time catalog and the runtime
//! schema agree. Run with:
//!   cargo test -p bsql-query-fixture --test query_enum_live -- --ignored
#![forbid(unsafe_code)]
// Live integration harness: `.expect(..)` / `.unwrap()` here are the loud
// test-failure signals (they panic, surfacing the failure), not production
// fallbacks.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

// The generated enum type from the `0014_moods.sql` migration — zero derives.
bsql::user_types!();

// Decode a `mood` column into `Mood` (NOT NULL) and a nullable one into
// `Option<Mood>`.
bsql::query!(FeelingById, "SELECT id, m, note FROM feelings WHERE id = $1");
// Round-trip an enum PARAMETER: `$1` types as `Mood`.
bsql::query!(FeelingsByMood, "SELECT id FROM feelings WHERE m = $1 ORDER BY id");
// Insert with an enum parameter and RETURN the decoded enum.
bsql::query!(
    InsertFeeling,
    "INSERT INTO feelings (id, m, note) VALUES ($1, $2, $3) RETURNING id, m, note"
);
// A row whose `m` will be an out-of-band-added label the generated enum does
// not know — decoding it must be a classified `UnknownEnumLabel`.
bsql::query!(MoodOfId, "SELECT m FROM feelings WHERE id = $1");

// Create the enum + table in a PER-DRIVER schema (via `search_path`) so the two
// tests run in parallel without colliding on the shared `mood`/`feelings`
// object names the migration catalog fixes. The unqualified `mood` / `feelings`
// the compile-checked queries name resolve through `search_path` to the test's
// own schema. `CASCADE` on the schema drop removes the type + table together.
fn setup_ddl(schema: &str) -> String {
    format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
         CREATE SCHEMA {schema}; \
         SET search_path TO {schema}; \
         CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'in_progress'); \
         CREATE TABLE feelings (id int PRIMARY KEY, m mood NOT NULL, note mood)"
    )
}

// ─────────────────────────────── sync ────────────────────────────────

mod sync_driver {
    use super::{
        setup_ddl, FeelingById, FeelingsByMood, InsertFeeling, Mood, MoodOfId,
    };
    use bsql::DecodeError;
    use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn enum_decode_encode_and_unknown_label_round_trip() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query(&setup_ddl("bsql_enum_test_sync"))
            .expect("setup DDL");

        // ENCODE an enum param + RETURNING decode: insert (1, sad, happy) and
        // (2, in_progress, ok), reading the enum straight back. `note` is a
        // NULLABLE enum column, so its `$3` param is `Option<EnumLabel<Mood>>`
        // (`Some(..)` here; `None` would write SQL NULL); the NOT NULL `m` param
        // stays a bare `EnumLabel<Mood>`.
        let one = c
            .query_one::<InsertFeeling>((1, Mood::Sad.as_label(), Some(Mood::Happy.as_label())))
            .expect("insert 1");
        assert_eq!(one.id, 1);
        assert_eq!(one.m, Mood::Sad, "RETURNING m decodes the bound enum back");
        assert_eq!(one.note, Some(Mood::Happy), "nullable enum -> Some");

        // A second row exercising the snake_case -> PascalCase label mapping
        // (`in_progress` -> `InProgress`).
        let two = c
            .query_one::<InsertFeeling>((2, Mood::InProgress.as_label(), Some(Mood::Ok.as_label())))
            .expect("insert 2");
        assert_eq!(two.m, Mood::InProgress, "in_progress -> InProgress");

        // DECODE both twins: NOT NULL `m` -> `Mood`, nullable `note` -> Option.
        let row1 = c.query_one::<FeelingById>((1,)).expect("select 1");
        assert_eq!(row1.m, Mood::Sad);
        assert_eq!(row1.note, Some(Mood::Happy));

        // A genuine NULL nullable-enum column decodes to `None` (insert (3, ok)
        // leaving `note` NULL via the default).
        c.simple_query("INSERT INTO feelings (id, m) VALUES (3, 'ok')")
            .expect("insert 3");
        let row3 = c.query_one::<FeelingById>((3,)).expect("select 3");
        assert_eq!(row3.m, Mood::Ok);
        assert_eq!(row3.note, None, "an actual NULL enum decodes to None");

        // ENCODE an enum param in a WHERE filter: only row 2 has `m = 'in_progress'`.
        let matches = c
            .query::<FeelingsByMood>((Mood::InProgress.as_label(),))
            .expect("filter by enum param");
        let ids: Vec<i32> = matches.iter().map(|r| r.expect("decode").id).collect();
        assert_eq!(ids, vec![2], "the enum param filters to the matching row");

        // ORDERING: the derived `Ord` follows the declared (PG sort) order.
        assert!(Mood::Happy < Mood::Sad);
        assert!(Mood::Sad < Mood::Ok);
        assert!(Mood::Ok < Mood::InProgress);

        // UNKNOWN LABEL: add a value to the LIVE enum the migration did not
        // declare, insert a row using it, then decode via `query!`. The
        // generated `from_wire_label` does not know it -> a classified
        // `UnknownEnumLabel`, never a panic.
        c.simple_query("ALTER TYPE mood ADD VALUE 'ecstatic'")
            .expect("add out-of-band label");
        c.simple_query("INSERT INTO feelings (id, m) VALUES (99, 'ecstatic')")
            .expect("insert ecstatic");
        let unknown = c.query_one::<MoodOfId>((99,));
        match unknown {
            Err(DriverError::Decode(DecodeError::UnknownEnumLabel)) => {}
            other => panic!("expected a classified UnknownEnumLabel, got: {other:?}"),
        }

        c.simple_query("DROP SCHEMA bsql_enum_test_sync CASCADE")
            .expect("cleanup");
    }
}

// ─────────────────────────────── async ───────────────────────────────

mod async_driver {
    use super::{setup_ddl, FeelingById, InsertFeeling, Mood, MoodOfId};
    use bsql::DecodeError;
    use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn enum_decode_encode_and_unknown_label_round_trip() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query(&setup_ddl("bsql_enum_test_async"))
            .await
            .expect("setup DDL");

        let one = c
            .query_one::<InsertFeeling>((1, Mood::Ok.as_label(), Some(Mood::Sad.as_label())))
            .await
            .expect("insert 1");
        assert_eq!(one.m, Mood::Ok, "RETURNING decodes the bound enum (async)");
        assert_eq!(one.note, Some(Mood::Sad));

        let row = c.query_one::<FeelingById>((1,)).await.expect("select 1");
        assert_eq!(row.m, Mood::Ok);
        assert_eq!(row.note, Some(Mood::Sad));

        c.simple_query("ALTER TYPE mood ADD VALUE 'ecstatic'")
            .await
            .expect("add out-of-band label");
        c.simple_query("INSERT INTO feelings (id, m) VALUES (99, 'ecstatic')")
            .await
            .expect("insert ecstatic");
        match c.query_one::<MoodOfId>((99,)).await {
            Err(DriverError::Decode(DecodeError::UnknownEnumLabel)) => {}
            other => panic!("expected a classified UnknownEnumLabel (async), got: {other:?}"),
        }

        c.simple_query("DROP SCHEMA bsql_enum_test_async CASCADE")
            .await
            .expect("cleanup");
    }
}
