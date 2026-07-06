-- Tables that model the outer-join × USING/NATURAL merged-column soundness
-- case. A `LEFT JOIN` null-extends `oj_b`, then its NOT NULL `bk` is the
-- PRESERVED-left side of a second `LEFT JOIN ... USING (bk)`. The merged `bk`
-- therefore INHERITS `oj_b`'s null-extension and CAN be NULL, even though `bk`
-- is declared NOT NULL on every base table.
--
-- Before the nullability fix, the compile-checked `query!` path inferred the
-- merged `bk` as a NON-Option `i32` field — an unsound compile-time NOT-NULL
-- promise that a real server NULL breaks at runtime with a decode error. The
-- live round-trip in `query_live_{async,sync}.rs` proves the field is now
-- `Option<i32>` and a genuine NULL decodes to `None`.
CREATE TABLE oj_a (
    j  INTEGER NOT NULL,
    x  INTEGER
);

CREATE TABLE oj_b (
    j   INTEGER NOT NULL,
    bk  INTEGER NOT NULL,
    y   INTEGER
);

CREATE TABLE oj_c (
    bk  INTEGER NOT NULL,
    z   INTEGER
);
