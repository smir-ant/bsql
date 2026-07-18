-- PostgreSQL user-defined types — the subject of the `generated_types` example.
-- These are PostgreSQL-ONLY (SQLite has no enum / composite / domain), so a
-- `query!` selecting these columns runs on PostgreSQL but NOT SQLite (the macro
-- simply does not emit a `SqliteTypedQuery` impl for it).
--
-- `bsql::user_types!()` generates a Rust type for each of these from THIS DDL,
-- with zero derives and no hand-maintained type name — a capability no other
-- Rust SQL library has, because only bsql parses the migration set at build time.

-- An ENUM -> a generated `enum Mood { Happy, Sad, Neutral }` (variants in
-- declared order, which is PostgreSQL's sort order, so the derived `Ord` matches
-- the server). Rename or delete a label in a later migration and any code naming
-- the old variant STOPS COMPILING — drift is a build error.
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');

-- A COMPOSITE (row) type -> a generated `struct Address { street: Option<String>,
-- city: Option<String>, zip: Option<i32> }`. Every field is `Option<_>` because
-- a composite attribute is always nullable on the wire (PostgreSQL forbids
-- NOT NULL on a `CREATE TYPE ... AS` attribute).
CREATE TYPE address AS (street text, city text, zip int4);

-- A DOMAIN — a constrained alias for a base type. It is TRANSPARENT: a column of
-- this type decodes exactly as its base (`positive_qty` -> `i32`), and the CHECK
-- is enforced by the SERVER (a violation is a classified server error, never a
-- client check). A domain generates NO Rust type — it IS its base.
CREATE DOMAIN positive_qty AS integer CHECK (VALUE >= 0);

-- A table using all three. `query!` decodes `current_mood` into `Mood`,
-- `home` into `Address`, and `quantity` into `i32` (the domain's base).
CREATE TABLE profiles (
    id           BIGINT PRIMARY KEY,
    current_mood mood NOT NULL,
    favorite_mood mood,                 -- nullable -> Option<Mood>
    quantity     positive_qty NOT NULL, -- domain -> i32 (server enforces >= 0)
    home         address                 -- nullable composite -> Option<Address>
);
