-- A user-defined DOMAIN: a constrained alias for a base type. It is TRANSPARENT
-- on the wire — a domain column decodes/encodes as its BASE type (`age AS int`
-- -> `i32`, `handle AS text` -> `String`/`&str`), and the `CHECK` is enforced by
-- the SERVER, never a client concern. `adult_age` is a domain OVER a domain,
-- resolved transitively to `int`.
CREATE DOMAIN age AS int CHECK (VALUE >= 0);
CREATE DOMAIN adult_age AS age CHECK (VALUE >= 18);
CREATE DOMAIN handle AS text;

CREATE TABLE members (
    id int PRIMARY KEY,
    a  age NOT NULL,
    h  handle
);
