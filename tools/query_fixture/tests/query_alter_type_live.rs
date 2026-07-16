//! LIVE witness that ALTER TYPE evolution round-trips end-to-end.
//!
//! The `0016_alter_type_evolve.sql` migration evolves `priority` with an
//! `ADD VALUE` and a `RENAME VALUE` (and renames `tshirt` to `garment_size` via
//! `RENAME TO`). This proves against a real PostgreSQL that a row carrying the
//! ADDED label (`medium`) and the RENAMED label (`critical`) decodes into the
//! generated `Priority` variants — NOT `UnknownEnumLabel`, which is exactly what
//! a silent ALTER-TYPE drop would have produced. The live enum is built by
//! replaying the SAME ALTER sequence, so the runtime schema matches the catalog.
//!
//! Run with:
//!   cargo test -p bsql-query-fixture --test query_alter_type_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

bsql::user_types!();

// `p` decodes into `Priority` (carrying the ADD VALUE'd `Medium` and the RENAME
// VALUE'd `Critical`); `size` into `GarmentSize` (the RENAME TO'd type name).
bsql::query!(TaskById, "SELECT id, p, size FROM tasks WHERE id = $1");

fn setup_ddl(schema: &str) -> String {
    format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
         CREATE SCHEMA {schema}; \
         SET search_path TO {schema}; \
         CREATE TYPE priority AS ENUM ('low', 'high'); \
         ALTER TYPE priority ADD VALUE 'medium' AFTER 'low'; \
         ALTER TYPE priority ADD VALUE 'urgent'; \
         ALTER TYPE priority RENAME VALUE 'high' TO 'critical'; \
         CREATE TYPE tshirt AS ENUM ('s', 'm', 'l'); \
         ALTER TYPE tshirt RENAME TO garment_size; \
         CREATE TABLE tasks (id int PRIMARY KEY, p priority NOT NULL, size garment_size)"
    )
}

mod sync_driver {
    use super::{setup_ddl, GarmentSize, Priority, TaskById};
    use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn added_and_renamed_labels_round_trip() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query(&setup_ddl("bsql_alter_test_sync"))
            .expect("setup DDL");

        // A row using the ADD VALUE'd label and the RENAME TO'd type.
        c.simple_query("INSERT INTO tasks (id, p, size) VALUES (1, 'medium', 'm')")
            .expect("insert medium");
        let row = c.query_one::<TaskById>((1,)).expect("select 1");
        assert_eq!(row.p, Priority::Medium, "the ADD VALUE'd label decodes");
        assert_eq!(row.size, Some(GarmentSize::M), "the RENAME TO'd type decodes");

        // A row using the RENAME VALUE'd label.
        c.simple_query("INSERT INTO tasks (id, p) VALUES (2, 'critical')")
            .expect("insert critical");
        let row2 = c.query_one::<TaskById>((2,)).expect("select 2");
        assert_eq!(row2.p, Priority::Critical, "the RENAME VALUE'd label decodes");

        c.simple_query("DROP SCHEMA bsql_alter_test_sync CASCADE")
            .expect("cleanup");
    }
}

mod async_driver {
    use super::{setup_ddl, Priority, TaskById};
    use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn added_and_renamed_labels_round_trip() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query(&setup_ddl("bsql_alter_test_async"))
            .await
            .expect("setup DDL");

        c.simple_query("INSERT INTO tasks (id, p) VALUES (1, 'medium')")
            .await
            .expect("insert medium");
        let row = c.query_one::<TaskById>((1,)).await.expect("select 1");
        assert_eq!(row.p, Priority::Medium, "ADD VALUE'd label decodes (async)");

        c.simple_query("INSERT INTO tasks (id, p) VALUES (2, 'critical')")
            .await
            .expect("insert critical");
        let row2 = c.query_one::<TaskById>((2,)).await.expect("select 2");
        assert_eq!(row2.p, Priority::Critical, "RENAME VALUE'd label decodes (async)");

        c.simple_query("DROP SCHEMA bsql_alter_test_async CASCADE")
            .await
            .expect("cleanup");
    }
}
