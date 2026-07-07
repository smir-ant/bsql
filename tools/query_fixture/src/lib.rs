//! Fixture exercising the compile-checked query chain.
//!
//! Each `query!` below types a SELECT AT COMPILE TIME against the catalog
//! that `build.rs` -> `bsql-build` replays from `migrations/`. If any
//! referenced table or column did not exist in the migration-replayed
//! schema, this crate would fail to compile. That this crate builds at all
//! is the end-to-end proof that the chain works for a real consumer: each
//! query below also stands in for a specific catalog-replay property
//! (`ALTER TABLE ADD COLUMN`, `RENAME TO`, subdirectory recursion,
//! unquoted-identifier case folding) that resolves through the macro.

// Valid references — these columns exist in the replayed schema, so the
// query types and its records compile.
bsql::query!(UsersId, "SELECT id FROM users");
bsql::query!(UsersEmail, "SELECT email FROM users");
// `users.name` was added NOT NULL by `ALTER TABLE ADD COLUMN` in 0002 —
// resolving it proves ALTER ADD COLUMN reached the catalog.
bsql::query!(UsersName, "SELECT name FROM users");
// `orders.status` was added by `ALTER TABLE ADD COLUMN` in 0003.
bsql::query!(OrdersStatus, "SELECT status FROM orders");

// `accounts` exists ONLY under the post-RENAME name: 0004 creates
// `legacy_accounts`, 0005 renames it to `accounts`. That these type at all
// proves `RENAME TO` re-keyed the catalog to the new name. The companion
// compile-fail golden (`query_renamed_away_table`) proves the OLD name
// (`legacy_accounts`) no longer resolves.
bsql::query!(AccountsId, "SELECT id FROM accounts");
// `accounts.balance` survived the rename with its column intact.
bsql::query!(AccountsBalance, "SELECT balance FROM accounts");

// `audit.event` lives in a migration nested under `migrations/regional/`.
// That this resolves proves the build helper RECURSES into subdirectories.
// The table is written `Audit` / `Event` (unquoted), so it resolves under
// the lowercase-folded `audit.event` — proving the unquoted-identifier case
// folding end-to-end.
bsql::query!(AuditEvent, "SELECT event FROM audit");

// ONE-CRATE REACHABILITY PROOF.
//
// USER-DEFINED TYPES from the build catalog (the audit-4 flagship).
//
// `0014_moods.sql` declares `CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok',
// 'in_progress')`. `bsql::user_types!()` generates `enum Mood { Happy, Sad, Ok,
// InProgress }` from that DDL — zero derives, zero manual type name — and
// `query!` below decodes a `mood` column straight into it. NO library in any
// other language does this, because none parses the migration set at build
// time. The generated `Mood` is in scope for the `query!` calls that name it.
bsql::user_types!();

// Decode a `mood` column into `Mood` (NOT NULL -> `Mood`) and a nullable
// `mood` into `Option<Mood>` — proving the enum decode + nullability. `$1` is
// the `int` primary key.
bsql::query!(FeelingById, "SELECT id, m, note FROM feelings WHERE id = $1");
// Round-trip an enum PARAMETER: `$1` types as `Mood` (bind it with
// `Mood::Happy.as_label()`), the server coerces the label to the enum.
bsql::query!(FeelingsByMood, "SELECT id FROM feelings WHERE m = $1");
// Insert with an enum parameter and RETURN the decoded enum back.
bsql::query!(
    InsertFeeling,
    "INSERT INTO feelings (id, m) VALUES ($1, $2) RETURNING id, m"
);

// The generated `Mood` is a real, ergonomic Rust enum: constructible, matchable,
// ordered (declared order = PG sort order), and reachable through `bsql` alone.
const _: () = {
    // A wrong variant would be a compile error here; the variant SET is the
    // migration's, enforced by the compiler.
    const fn assert_pg_enum<E: bsql::PgEnum>() {}
    assert_pg_enum::<Mood>();
};

// A DOMAIN column types TRANSPARENTLY as its base — `a age NOT NULL` is `i32`
// (`age AS int`, resolved through the `adult_age`-style chain when nested), and
// `h handle` is `Option<&str>` (`handle AS text`, nullable). No generated type:
// a domain is its base on the wire, with the server enforcing the CHECK. That
// this `query!` types at all proves `CREATE DOMAIN` reached the catalog and its
// column resolved to the base's Rust type.
bsql::query!(MemberById, "SELECT id, a, h FROM members WHERE id = $1");

// ALTER TYPE evolution reaches the generated enum: `priority` was built by
// `CREATE TYPE ... AS ENUM ('low','high')` then ADD VALUE + RENAME VALUE (0016),
// and `garment_size` is `tshirt` after a RENAME TO. That this `query!` types at
// all — `p` decodes into a `Priority` carrying the added/renamed variants,
// `size` into a `GarmentSize` under the renamed type — proves ALTER TYPE reached
// the catalog (a silent drop would leave the enum missing the added variant or
// the table's type name unresolved).
bsql::query!(TaskById, "SELECT id, p, size FROM tasks WHERE id = $1");

// This crate's ONLY `[dependency]` is `bsql`. That every `query!` above
// expands here — its emitted `::bsql::__rt::` paths resolving — already
// proves the flagship macro is reachable through `bsql` alone. This block
// extends the proof to the user-facing query TYPES: each generated carrier
// implements the umbrella crate's re-exported `bsql::TypedQuery` /
// `bsql::QueryFingerprint`, and the typed result container `bsql::Rows`,
// the const-checked `bsql::PreparedQuery`, and the classified
// `bsql::DecodeError` are all nameable — with no dependency in scope but
// `bsql`. A consumer needs no other crate to WRITE, TYPE, PREPARE, and
// DECODE a compile-checked query.
const _: () = {
    const fn assert_typed_query<Q: bsql::TypedQuery>() {}
    const fn assert_fingerprint<Q: bsql::QueryFingerprint>() {}
    assert_typed_query::<UsersIdQuery>();
    assert_fingerprint::<UsersIdQuery>();
};

/// The bounded typed result container for the `UsersId` query, named through
/// `bsql` alone (`bsql::Rows` over a `query!`-generated carrier).
#[doc(hidden)]
pub type _RowsReachableThroughBsql = bsql::Rows<UsersIdQuery>;

/// The const-checked prepared query type for `UsersId`, spelled through the
/// umbrella crate's `bsql::PreparedQuery` re-export.
#[doc(hidden)]
pub type _PreparedReachableThroughBsql = bsql::PreparedQuery<
    // `PREPARED` is `PreparedQuery<Self::Params<'static>, _>` (the const validator
    // rides the `'static` param marker), so this alias names the `'static`
    // instantiation of the parameter GAT.
    <UsersIdQuery as bsql::TypedQuery>::Params<'static>,
    <UsersIdQuery as bsql::TypedQuery>::Row,
>;

/// The typed decoder for `UsersId`, returning a `bsql::DecodeError` on a
/// classified failure — the decode path spelled through `bsql` alone.
#[doc(hidden)]
pub type _DecodeReachableThroughBsql = fn(&[u8]) -> ::core::result::Result<UsersId, bsql::DecodeError>;

/// The prepared query minted through the proto-owned boundary, reachable as
/// an associated const of the umbrella's re-exported trait.
#[doc(hidden)]
pub const _PREPARED_THROUGH_BSQL: _PreparedReachableThroughBsql =
    <UsersIdQuery as bsql::TypedQuery>::PREPARED;

/// The typed decoder value, reachable through `bsql` alone.
#[doc(hidden)]
pub const _DECODE_THROUGH_BSQL: _DecodeReachableThroughBsql = UsersId::decode;
