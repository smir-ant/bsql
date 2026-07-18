-- `authors` and `books` — a one-to-many relationship for the JOIN / GROUP BY /
-- subquery example (`joins_aggregates`) and the N+1 detection example
-- (`n1_detection`, which fetches each author's books in a loop).
--
-- Both tables use SQLite-portable types. The `books.author_id -> authors.id`
-- relationship is a logical foreign key; it is expressed in the JOIN queries
-- (no on-table FK CONSTRAINT is declared, since the compile-checked `query!`
-- catalog models columns + nullability, not referential constraints — those are
-- enforced by the server at runtime if you add them).
CREATE TABLE authors (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE books (
    id             BIGINT PRIMARY KEY,
    -- The author this book belongs to (logical FK -> authors.id).
    author_id      BIGINT NOT NULL,
    title          TEXT NOT NULL,
    -- Nullable: some books have no recorded year -> `Option<i32>`.
    published_year INTEGER
);
