//! LIVE user-defined COMPOSITE round-trip over BOTH drivers.
//!
//! Proves end-to-end against a real PostgreSQL that a `CREATE TYPE ... AS (...)`
//! migration generates Rust `struct`s (`bsql::user_types!()`), and `query!`:
//!   * DECODES an `addr` column into `Addr { street: Option<String>,
//!     zip: Option<i32> }` by walking the row-type binary frame,
//!   * decodes a NULL composite FIELD to `None` and a NULL composite COLUMN
//!     (the whole value) to `None`,
//!   * recurses into a NESTED composite field (`region.seat` is an `addr`),
//!   * recurses into an ENUM composite field (`tagged.feeling` is a `mood`),
//!   * classifies an ARITY drift (a field ADDed to the LIVE type out-of-band,
//!     absent from the migration) as `DecodeError::CompositeArityMismatch` —
//!     never a panic or a silently-wrong record.
//!
//! The migration `0017_composites.sql` declares the same types/table this test
//! creates on the live server, so the compile-time catalog and the runtime
//! schema agree. Run with:
//!   cargo test -p bsql-query-fixture --test query_composite_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

// The generated composite structs (+ the `mood` enum a `tagged` field uses)
// from the fixture migrations — zero derives.
bsql::user_types!();

// Decode composite columns: `a` (a plain composite), `r` (a nested composite).
bsql::query!(PlaceById, "SELECT id, a, r FROM places WHERE id = $1");
// A `tagged` value built in SQL (no `tagged` table column needed).
bsql::query!(TaggedRow, "SELECT ROW('note', 'happy')::tagged AS t");
// For the arity-drift witness: select just the `addr` column.
bsql::query!(AddrOfId, "SELECT a FROM places WHERE id = $1");

// Create the composite types + table in a PER-DRIVER schema (via `search_path`)
// so the two tests run in parallel without colliding on the shared object names
// the migration catalog fixes. `mood` is needed by `tagged`.
fn setup_ddl(schema: &str) -> String {
    format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
         CREATE SCHEMA {schema}; \
         SET search_path TO {schema}; \
         CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'in_progress'); \
         CREATE TYPE addr AS (street text, zip int4); \
         CREATE TYPE region AS (name text, seat addr); \
         CREATE TYPE tagged AS (label text, feeling mood); \
         CREATE TABLE places (id int PRIMARY KEY, a addr, r region)"
    )
}

// ─────────────────────────────── sync ────────────────────────────────

mod sync_driver {
    use super::{setup_ddl, AddrOfIdQuery, PlaceByIdQuery, TaggedRowQuery, Mood};
    use bsql::DecodeError;
    use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn composite_decode_nested_enum_and_arity_drift() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query(&setup_ddl("bsql_comp_test_sync"))
            .expect("setup DDL");

        // Insert a full row: a = ('main st', 5), r = ('west', ('elm st', 7)).
        c.simple_query(
            "INSERT INTO places (id, a, r) VALUES \
             (1, ROW('main st', 5), ROW('west', ROW('elm st', 7)))",
        )
        .expect("insert 1");

        let row = c.query_one::<PlaceByIdQuery>((1,)).expect("select 1");
        let a = row.a.expect("column a present");
        assert_eq!(a.street.as_deref(), Some("main st"));
        assert_eq!(a.zip, Some(5));
        let r = row.r.expect("column r present");
        assert_eq!(r.name.as_deref(), Some("west"));
        let seat = r.seat.expect("nested seat present");
        assert_eq!(seat.street.as_deref(), Some("elm st"), "NESTED composite recurses");
        assert_eq!(seat.zip, Some(7));

        // A NULL composite FIELD (`street` NULL) decodes to None inside the struct.
        c.simple_query("INSERT INTO places (id, a) VALUES (2, ROW(NULL, 9))")
            .expect("insert 2");
        let row2 = c.query_one::<PlaceByIdQuery>((2,)).expect("select 2");
        let a2 = row2.a.expect("column a present");
        assert_eq!(a2.street, None, "a NULL composite field decodes to None");
        assert_eq!(a2.zip, Some(9));
        assert_eq!(row2.r, None, "a NULL composite COLUMN decodes to None");

        // A whole-column NULL composite decodes to None.
        c.simple_query("INSERT INTO places (id) VALUES (3)")
            .expect("insert 3");
        let row3 = c.query_one::<PlaceByIdQuery>((3,)).expect("select 3");
        assert_eq!(row3.a, None, "a NULL composite column is None");

        // An ENUM composite field recurses into the label reshape.
        let tagged = c.query_one::<TaggedRowQuery>(()).expect("tagged");
        let tv = tagged.t.expect("tagged value");
        assert_eq!(tv.label.as_deref(), Some("note"));
        assert_eq!(tv.feeling, Some(Mood::Happy), "ENUM composite field recurses");

        // ARITY DRIFT: add an attribute to the LIVE type the migration did not
        // declare, then decode via `query!`. The generated `Addr` expects 2
        // fields; the wire frame now has 3 -> a classified CompositeArityMismatch,
        // never a panic or a silently-wrong record.
        c.simple_query("ALTER TYPE addr ADD ATTRIBUTE country text CASCADE")
            .expect("add out-of-band attribute");
        let drifted = c.query_one::<AddrOfIdQuery>((1,));
        match drifted {
            Err(DriverError::Decode(DecodeError::CompositeArityMismatch {
                expected: 2,
                found: 3,
            })) => {}
            other => panic!("expected a classified CompositeArityMismatch, got: {other:?}"),
        }

        c.simple_query("DROP SCHEMA bsql_comp_test_sync CASCADE")
            .expect("cleanup");
    }
}

// ─────────────────────────────── async ───────────────────────────────

mod async_driver {
    use super::{setup_ddl, PlaceByIdQuery, TaggedRowQuery, Mood};
    use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn composite_decode_nested_and_enum() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query(&setup_ddl("bsql_comp_test_async"))
            .await
            .expect("setup DDL");

        c.simple_query(
            "INSERT INTO places (id, a, r) VALUES \
             (1, ROW('oak st', 3), ROW('east', ROW('fir st', 8)))",
        )
        .await
        .expect("insert 1");

        let row = c.query_one::<PlaceByIdQuery>((1,)).await.expect("select 1");
        let a = row.a.expect("a present");
        assert_eq!(a.street.as_deref(), Some("oak st"));
        assert_eq!(a.zip, Some(3));
        let seat = row.r.expect("r present").seat.expect("seat present");
        assert_eq!(seat.street.as_deref(), Some("fir st"), "nested recurses (async)");
        assert_eq!(seat.zip, Some(8));

        let tagged = c.query_one::<TaggedRowQuery>(()).await.expect("tagged");
        let tv = tagged.t.expect("tagged value");
        assert_eq!(tv.feeling, Some(Mood::Happy), "enum field recurses (async)");

        c.simple_query("DROP SCHEMA bsql_comp_test_async CASCADE")
            .await
            .expect("cleanup");
    }
}
