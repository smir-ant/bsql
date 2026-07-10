-- A user-defined COMPOSITE (row) type declared in a migration.
-- `bsql::user_types!()` generates `struct Addr { street: Option<String>,
-- zip: Option<i32> }` from this DDL — one `Option<T>` field per attribute,
-- because a composite attribute is ALWAYS nullable on the wire (PostgreSQL
-- forbids NOT NULL on a `CREATE TYPE ... AS` attribute, and the row-type binary
-- frame carries a per-field length that may be -1). `query!` decodes an `addr`
-- column into `Addr` by walking the row-type binary frame, recursing into each
-- field's own decoder.
CREATE TYPE addr AS (street text, zip int4);

-- A NESTED composite: `seat` is itself a composite (`addr`), so decoding a
-- `region` recurses into `Addr::decode_row`. Generates
-- `struct Region { name: Option<String>, seat: Option<Addr> }`.
CREATE TYPE region AS (name text, seat addr);

-- A composite with an ENUM field (`mood` from 0014_moods.sql), so decoding a
-- `tagged` recurses into the enum label reshape. Generates
-- `struct Tagged { label: Option<String>, feeling: Option<Mood> }`.
CREATE TYPE tagged AS (label text, feeling mood);

CREATE TABLE places (
    id int PRIMARY KEY,
    a  addr,     -- a nullable composite column (the whole `a` may be NULL)
    r  region    -- a nullable nested-composite column
);

-- IDENT-HYGIENE witness: a composite whose attribute names collide with EVERY
-- internal local the generated `decode_row` binds (`__reader`, `__frame`,
-- `__bytes`). Mixed-site hygiene keeps a field local from shadowing the reader /
-- frame; the `__bytes` match binding is inner-scoped, so a `__bytes` field is
-- benign. This must still generate + decode correctly.
CREATE TYPE collide AS (__reader int4, __frame text, __bytes int4, ok int4);
