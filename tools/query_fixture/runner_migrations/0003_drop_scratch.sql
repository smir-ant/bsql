-- A destructive migration that MUST carry its acknowledgement — proving
-- the S42 destructive-ack gate rides emit_migrations (the embed) too. Without
-- the marker below, the fixture BUILD fails.
CREATE TABLE app_scratch (x int);

-- bsql:ack-destructive
DROP TABLE app_scratch;
