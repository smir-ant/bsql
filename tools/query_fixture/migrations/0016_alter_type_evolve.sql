-- ALTER TYPE replay witness. The catalog must match the migration FILES, so an
-- enum evolved by a LATER migration (ADD VALUE / RENAME VALUE / RENAME TO) must
-- reach the generated Rust enum — a silent drop would defeat the whole
-- compile-time-drift guarantee (a live row with the added label would only hit
-- UnknownEnumLabel at runtime; a renamed variant would map to a label the live
-- server rejects). A SEPARATE type set from `mood` (0014), so this does not
-- disturb the enum decode/encode tests.
CREATE TYPE priority AS ENUM ('low', 'high');
ALTER TYPE priority ADD VALUE 'medium' AFTER 'low';    -- [low, medium, high]
ALTER TYPE priority ADD VALUE 'urgent';                -- append -> [.., urgent]
ALTER TYPE priority RENAME VALUE 'high' TO 'critical'; -- [low, medium, critical, urgent]

CREATE TYPE tshirt AS ENUM ('s', 'm', 'l');
ALTER TYPE tshirt RENAME TO garment_size;              -- the type is re-keyed

CREATE TABLE tasks (
    id   int PRIMARY KEY,
    p    priority NOT NULL,
    size garment_size
);
