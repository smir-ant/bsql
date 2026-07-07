-- A user-defined enum type declared in a migration. `bsql::user_types!()`
-- generates `enum Mood { Happy, Sad, Ok }` from this DDL (variants in declared
-- order — PostgreSQL's enum sort order), and `query!` decodes a `mood` column
-- into it with zero user derives. `in_progress` exercises the snake_case ->
-- PascalCase label mapping (`InProgress`).
CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'in_progress');

CREATE TABLE feelings (
    id   int PRIMARY KEY,
    m    mood NOT NULL,
    note mood
);
