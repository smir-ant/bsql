-- A migration nested in a subdirectory, proving the build helper recurses
-- into `migrations/<subdir>/` and tracks membership at that level.
--
-- `Audit` / `Event` are written UNQUOTED, so PostgreSQL folds them to
-- lowercase; the catalog keys them `audit` / `event`, and that is how a
-- reference resolves (proving the unquoted-identifier case folding).
CREATE TABLE Audit (
    id    BIGINT PRIMARY KEY,
    Event TEXT NOT NULL
);
