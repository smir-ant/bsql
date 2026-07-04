-- Fixture schema for the external-type bridge tests. Every `timestamptz`
-- column decodes into the fixture-local `MyTs` stand-in; every `uuid` column
-- decodes into the real `uuid::Uuid`; every `numeric` column into the dep-free
-- `MyDecimal` stand-in (a variable-width bridged target, holding the exact
-- decimal text). The `label` text column is deliberately UNBRIDGED, proving
-- bridged and native columns coexist in one row.
CREATE TABLE events (
    id       uuid          NOT NULL,   -- bridged -> uuid::Uuid
    created  timestamptz   NOT NULL,   -- bridged -> MyTs (fixed-width, fast path)
    updated  timestamptz,              -- bridged, nullable -> Option<MyTs>
    tstamps  timestamptz[] NOT NULL,   -- bridged element -> Vec<Option<MyTs>>
    amount   numeric       NOT NULL,   -- bridged -> MyDecimal (variable-width)
    refund   numeric,                  -- bridged, nullable -> Option<MyDecimal>
    rates    numeric[]     NOT NULL,   -- bridged element -> Vec<Option<MyDecimal>>
    day      date          NOT NULL,   -- bridged -> MyDate (via the civil conversion)
    label    text          NOT NULL    -- UNBRIDGED native -> &str / String
);
