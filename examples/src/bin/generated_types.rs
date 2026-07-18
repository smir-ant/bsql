//! # generated_types — Rust types generated from your migration DDL
//!
//! `bsql::user_types!()` generates a Rust type for every PostgreSQL user-defined
//! type in your migrations — with ZERO derives and no hand-maintained type name.
//! This example decodes three kinds:
//!   * an ENUM  `mood`         -> `enum Mood { Happy, Sad, Neutral }`
//!   * a DOMAIN `positive_qty` -> transparently its base `i32` (CHECK enforced by
//!     the server)
//!   * a COMPOSITE `address`   -> `struct Address { street, city, zip }` (each
//!     field `Option<_>` — composite attributes are always nullable on the wire)
//!
//! Rename or delete an enum variant / composite field in a later migration and
//! any code naming the old one STOPS COMPILING — drift is a build error. The
//! generated `Mood` derives `Ord` in DECLARED order, which is PostgreSQL's enum
//! sort order, so it matches the server.
//!
//! Features/verbs: `user_types!()` (in `src/lib.rs`), `query!` decoding enum /
//! domain / composite columns, an enum PARAMETER via `Mood::Happy.as_label()`.
//!
//! Backend: PostgreSQL — these types are PostgreSQL-only (SQLite has no enum /
//! composite / domain). Uses a session TEMP shadow of `profiles`.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin generated_types
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::pg::{ConnectConfig, Connection, DriverError};
// The generated types (from `user_types!()` in `bsql_examples`'s lib). `Mood` is
// the enum, `Address` the composite; the domain generates no type (it IS `i32`).
use bsql_examples::{Address, Mood};

// Decode all three user types in one row: `current_mood` -> `Mood`,
// `favorite_mood` -> `Option<Mood>`, `quantity` -> `i32` (domain base),
// `home` -> `Option<Address>`.
bsql::query!(
    AllProfiles,
    "SELECT id, current_mood, favorite_mood, quantity, home FROM profiles ORDER BY id"
);
// An ENUM PARAMETER: `$1` binds a `Mood` label via `Mood::Happy.as_label()`.
bsql::query!(ProfilesByMood, "SELECT id FROM profiles WHERE current_mood = $1");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;

    // The migrations create the `mood` / `address` / `positive_qty` types (and
    // the permanent `profiles` table); ensure they exist.
    bsql_examples::ensure_schema_async(&mut conn).await?;

    // A TEMP shadow of `profiles` (referencing the migration-created types) keeps
    // the example idempotent.
    conn.execute_raw(
        "CREATE TEMP TABLE profiles (\
         id BIGINT PRIMARY KEY, current_mood mood NOT NULL, favorite_mood mood, \
         quantity positive_qty NOT NULL, home address)",
    )
    .await?;
    conn.execute_raw(
        "INSERT INTO profiles (id, current_mood, favorite_mood, quantity, home) VALUES \
         (1, 'happy', 'neutral', 7, ROW('123 Main St', 'Springfield', 90210)::address), \
         (2, 'sad', NULL, 3, NULL)",
    )
    .await?;

    // Decode the generated types straight out of the rows.
    println!("profiles:");
    for row in conn.query::<AllProfiles>(()).await?.iter() {
        let profile = row?;
        // `current_mood` is a real Rust `Mood` enum — matchable, Debug-printable.
        let favorite = match profile.favorite_mood {
            Some(mood) => format!("{mood:?}"),
            None => "(none)".to_string(),
        };
        // `home` is `Option<Address>`; each Address field is itself `Option<_>`.
        let home = match &profile.home {
            Some(addr) => format!("{:?}, {:?} {:?}", addr.street, addr.city, addr.zip),
            None => "(no address)".to_string(),
        };
        println!(
            "  #{}: mood={:?} favorite={favorite} qty={} home=[{home}]",
            profile.id, profile.current_mood, profile.quantity
        );
    }

    // The generated enum has a meaningful `Ord` (declared order == PG sort order):
    println!("\nMood::Happy < Mood::Neutral ? {}", Mood::Happy < Mood::Neutral);

    // Bind an ENUM as a PARAMETER via `as_label()`. (A PG enum has no implicit
    // `text` cast, so a bare `&str` would be rejected — `EnumLabel<Mood>` is the
    // blessed, type-safe way, and `EnumLabel<Mood>` != `EnumLabel<OtherEnum>`.)
    println!("\nprofiles whose current mood is 'happy':");
    for row in conn.query::<ProfilesByMood>((Mood::Happy.as_label(),)).await?.iter() {
        println!("  profile #{}", row?.id);
    }

    // The DOMAIN's CHECK is SERVER-enforced: `quantity >= 0`. Inserting -1 is a
    // classified server error (a `Db` error), never a silent client-side check.
    let violation = conn
        .execute_raw("INSERT INTO profiles (id, current_mood, quantity) VALUES (3, 'happy', -1)")
        .await;
    match violation {
        Err(DriverError::Db(err)) => {
            println!("\ndomain CHECK rejected quantity=-1 (SQLSTATE {})", err.code());
        }
        other => println!("\nunexpected: {other:?}"),
    }

    conn.close().await?;
    Ok(())
}
