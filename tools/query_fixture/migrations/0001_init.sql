-- Initial schema.
CREATE TABLE users (
    id    BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    bio   TEXT
);

CREATE TABLE orders (
    id      BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total   INTEGER
);
