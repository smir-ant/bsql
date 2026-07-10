//! LIVE user-defined DOMAIN round-trip over BOTH drivers.
//!
//! A `CREATE DOMAIN` is TRANSPARENT: a domain column decodes/encodes as its BASE
//! type (`age AS int` -> `i32`, `handle AS text` -> `&str`), and its `CHECK` is
//! enforced by the SERVER, never the client. This proves against a real
//! PostgreSQL that:
//!   * a domain column decodes as its base's Rust type (through a
//!     domain-over-domain chain, `adult_age AS age AS int` -> `i32`),
//!   * the `CHECK` is server-enforced — inserting a value that violates it is a
//!     classified server error (SQLSTATE 23514), not a client-side check.
//!
//! Run with:
//!   cargo test -p bsql-query-fixture --test query_domain_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

// A domain column types as its base — `a` is `i32` (through `adult_age AS age AS
// int`), `h` is `Option<&str>` (`handle AS text`, nullable).
bsql::query!(MemberById, "SELECT id, a, h FROM members WHERE id = $1");

fn setup_ddl(schema: &str) -> String {
    format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
         CREATE SCHEMA {schema}; \
         SET search_path TO {schema}; \
         CREATE DOMAIN age AS int CHECK (VALUE >= 0); \
         CREATE DOMAIN adult_age AS age CHECK (VALUE >= 18); \
         CREATE DOMAIN handle AS text; \
         CREATE TABLE members (id int PRIMARY KEY, a adult_age NOT NULL, h handle)"
    )
}

mod sync_driver {
    use super::{setup_ddl, MemberByIdQuery};
    use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn domain_decodes_as_base_and_check_is_server_enforced() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query(&setup_ddl("bsql_domain_test_sync"))
            .expect("setup DDL");

        // A valid row: `a` satisfies the `>= 18` (and transitively `>= 0`)
        // CHECK; the domain columns decode as their BASE Rust types.
        c.simple_query("INSERT INTO members (id, a, h) VALUES (1, 25, 'alice')")
            .expect("insert valid");
        let row = c.query_one::<MemberByIdQuery>((1,)).expect("select 1");
        assert_eq!(row.id, 1);
        assert_eq!(row.a, 25, "domain-over-domain `adult_age` decodes as its `int` base");
        assert_eq!(
            row.h.as_deref(),
            Some("alice"),
            "`handle AS text` decodes as its text base"
        );

        // A NULL nullable-domain column decodes to `None`.
        c.simple_query("INSERT INTO members (id, a) VALUES (2, 40)")
            .expect("insert without handle");
        let row2 = c.query_one::<MemberByIdQuery>((2,)).expect("select 2");
        assert_eq!(row2.h, None);

        // The CHECK is SERVER-enforced: an under-18 value violates `adult_age`.
        // The client never checks — it is a classified server error (23514).
        match c.simple_query("INSERT INTO members (id, a) VALUES (3, 10)") {
            Err(DriverError::Db(e)) => assert_eq!(
                e.code(), "23514",
                "an under-18 value is a server-side check_violation"
            ),
            other => panic!("expected a server check_violation, got: {other:?}"),
        }

        c.simple_query("DROP SCHEMA bsql_domain_test_sync CASCADE")
            .expect("cleanup");
    }
}

mod async_driver {
    use super::{setup_ddl, MemberByIdQuery};
    use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn domain_decodes_as_base_and_check_is_server_enforced() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query(&setup_ddl("bsql_domain_test_async"))
            .await
            .expect("setup DDL");

        c.simple_query("INSERT INTO members (id, a, h) VALUES (1, 30, 'bob')")
            .await
            .expect("insert valid");
        let row = c.query_one::<MemberByIdQuery>((1,)).await.expect("select 1");
        assert_eq!(row.a, 30, "domain decodes as base (async)");
        assert_eq!(row.h.as_deref(), Some("bob"));

        match c
            .simple_query("INSERT INTO members (id, a) VALUES (2, 5)")
            .await
        {
            Err(DriverError::Db(e)) => assert_eq!(e.code(), "23514"),
            other => panic!("expected a server check_violation (async), got: {other:?}"),
        }

        c.simple_query("DROP SCHEMA bsql_domain_test_async CASCADE")
            .await
            .expect("cleanup");
    }
}
