CREATE TABLE bench_items (
    id   int4 PRIMARY KEY,
    name text NOT NULL,
    val  int4 NOT NULL
);

CREATE TABLE bench_cat (
    val   int4 PRIMARY KEY,
    label text NOT NULL
);

CREATE TABLE bench_ins (
    id   int8 PRIMARY KEY,
    name text NOT NULL,
    val  int4 NOT NULL
);
