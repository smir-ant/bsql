/*
 * pg_bench.c -- raw libpq benchmark
 *
 * Same queries as the Rust criterion benchmarks:
 *   fetch_one   : SELECT by PK (id = 42)
 *   fetch_many  : SELECT LIMIT N  (10, 100, 1000, 10000 rows)
 *   insert      : single INSERT RETURNING
 *   insert_batch: 100 INSERTs in a transaction
 *   join_agg    : JOIN + GROUP BY + aggregate
 *   subquery    : IN (SELECT ...)
 *
 * Compile:
 *   make pg_bench
 *
 * Run:
 *   BENCH_DATABASE_URL="postgres://smir-ant@localhost/bench_db" ./pg_bench
 */

#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mach/mach_time.h>
#include <arpa/inet.h>  /* htonl */

/* ---------- timing helpers ------------------------------------------------ */

static mach_timebase_info_data_t g_timebase;

static void timing_init(void) {
    mach_timebase_info(&g_timebase);
}

static uint64_t now_ns(void) {
    return mach_absolute_time() * g_timebase.numer / g_timebase.denom;
}

/* ---------- helpers ------------------------------------------------------- */

static void die_if_bad(PGconn *conn, PGresult *res, ExecStatusType expected) {
    if (PQresultStatus(res) != expected) {
        fprintf(stderr, "PG error: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
}

static PGconn *connect_pg(void) {
    const char *url = getenv("BENCH_DATABASE_URL");
    if (!url) {
        fprintf(stderr, "BENCH_DATABASE_URL not set\n");
        exit(1);
    }
    PGconn *conn = PQconnectdb(url);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    return conn;
}

/* ---------- benchmarks ---------------------------------------------------- */

#define ITERATIONS 10000

static void bench_fetch_one(PGconn *conn) {
    /* Prepare */
    PGresult *prep = PQprepare(conn, "fetch_one",
        "SELECT id, name, email FROM bench_users WHERE id = $1",
        1, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    /* Warm up */
    int32_t id_net = htonl(42);
    const char *vals[1] = { (const char *)&id_net };
    int lens[1] = { sizeof(id_net) };
    int fmts[1] = { 1 };  /* binary */

    PGresult *warm = PQexecPrepared(conn, "fetch_one", 1, vals, lens, fmts, 0);
    die_if_bad(conn, warm, PGRES_TUPLES_OK);
    PQclear(warm);

    /* Bench */
    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        PGresult *res = PQexecPrepared(conn, "fetch_one", 1, vals, lens, fmts, 0);
        PQclear(res);
    }
    uint64_t elapsed = now_ns() - start;
    printf("pg_fetch_one:       %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / ITERATIONS), ITERATIONS);
}

static void bench_fetch_many(PGconn *conn, int limit) {
    char stmt_name[64];
    snprintf(stmt_name, sizeof(stmt_name), "fetch_many_%d", limit);

    PGresult *prep = PQprepare(conn, stmt_name,
        "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1",
        1, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    /* Warm up — use text format for the LIMIT param to avoid int4/int8 mismatch */
    char lim_str[16];
    snprintf(lim_str, sizeof(lim_str), "%d", limit);
    const char *vals[1] = { lim_str };
    int *lens = NULL;   /* text format, NULL-terminated strings */
    int fmts[1] = { 0 };  /* text */

    PGresult *warm = PQexecPrepared(conn, stmt_name, 1, vals, lens, fmts, 0);
    die_if_bad(conn, warm, PGRES_TUPLES_OK);
    PQclear(warm);

    int iters = (limit >= 10000) ? 1000 : ITERATIONS;

    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        PGresult *res = PQexecPrepared(conn, stmt_name, 1, vals, lens, fmts, 0);
        PQclear(res);
    }
    uint64_t elapsed = now_ns() - start;
    printf("pg_fetch_many/%d: %*s%llu ns/op  (%d iters)\n",
           limit, (limit < 1000 ? 4 : (limit < 10000 ? 3 : 2)), "",
           (unsigned long long)(elapsed / iters), iters);
}

static void bench_insert_single(PGconn *conn) {
    PGresult *prep = PQprepare(conn, "insert_single",
        "INSERT INTO bench_users (name, email, active, score) "
        "VALUES ($1, $2, true, 0.0) RETURNING id",
        2, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    const char *name = "bench_insert";
    const char *email = "bench@example.com";
    const char *vals[2] = { name, email };
    int lens[2] = { (int)strlen(name), (int)strlen(email) };
    int fmts[2] = { 0, 0 };  /* text params for strings */

    /* Warm up */
    PGresult *warm = PQexecPrepared(conn, "insert_single", 2, vals, lens, fmts, 0);
    die_if_bad(conn, warm, PGRES_TUPLES_OK);
    PQclear(warm);

    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        PGresult *res = PQexecPrepared(conn, "insert_single", 2, vals, lens, fmts, 0);
        PQclear(res);
    }
    uint64_t elapsed = now_ns() - start;
    printf("pg_insert_single:   %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / ITERATIONS), ITERATIONS);
}

static void bench_insert_batch(PGconn *conn) {
    PGresult *prep = PQprepare(conn, "insert_batch",
        "INSERT INTO bench_users (name, email, active, score) "
        "VALUES ($1, $2, true, 0.0)",
        2, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    int iters = 1000;

    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        PGresult *begin = PQexec(conn, "BEGIN");
        PQclear(begin);
        for (int j = 0; j < 100; j++) {
            char name[32], email[48];
            snprintf(name, sizeof(name), "batch_%d", j);
            snprintf(email, sizeof(email), "batch_%d@example.com", j);
            const char *vals[2] = { name, email };
            int lens[2] = { (int)strlen(name), (int)strlen(email) };
            int fmts[2] = { 0, 0 };
            PGresult *res = PQexecPrepared(conn, "insert_batch", 2, vals, lens, fmts, 0);
            PQclear(res);
        }
        PGresult *commit = PQexec(conn, "COMMIT");
        PQclear(commit);
    }
    uint64_t elapsed = now_ns() - start;
    printf("pg_insert_batch/100: %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / iters), iters);
}

static void bench_join_aggregate(PGconn *conn) {
    const char *sql =
        "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount "
        "FROM bench_users u "
        "JOIN bench_orders o ON u.id = o.user_id "
        "WHERE u.active = true "
        "GROUP BY u.name "
        "ORDER BY SUM(o.amount) DESC "
        "LIMIT 100";

    PGresult *prep = PQprepare(conn, "join_agg", sql, 0, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    /* Warm up */
    PGresult *warm = PQexecPrepared(conn, "join_agg", 0, NULL, NULL, NULL, 0);
    die_if_bad(conn, warm, PGRES_TUPLES_OK);
    PQclear(warm);

    int iters = 1000;
    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        PGresult *res = PQexecPrepared(conn, "join_agg", 0, NULL, NULL, NULL, 0);
        PQclear(res);
    }
    uint64_t elapsed = now_ns() - start;
    printf("pg_join_aggregate:  %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / iters), iters);
}

static void bench_subquery(PGconn *conn) {
    const char *sql =
        "SELECT id, name, email FROM bench_users "
        "WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)";

    PGresult *prep = PQprepare(conn, "subquery", sql, 0, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    /* Warm up */
    PGresult *warm = PQexecPrepared(conn, "subquery", 0, NULL, NULL, NULL, 0);
    die_if_bad(conn, warm, PGRES_TUPLES_OK);
    PQclear(warm);

    int iters = 5000;
    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        PGresult *res = PQexecPrepared(conn, "subquery", 0, NULL, NULL, NULL, 0);
        PQclear(res);
    }
    uint64_t elapsed = now_ns() - start;
    printf("pg_subquery:        %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / iters), iters);
}

/* ---------- main ---------------------------------------------------------- */

int main(void) {
    timing_init();
    PGconn *conn = connect_pg();

    printf("=== C (libpq) PostgreSQL Benchmarks ===\n");
    printf("libpq version: %d\n\n", PQlibVersion());

    bench_fetch_one(conn);
    bench_fetch_many(conn, 10);
    bench_fetch_many(conn, 100);
    bench_fetch_many(conn, 1000);
    bench_fetch_many(conn, 10000);
    bench_insert_single(conn);
    bench_insert_batch(conn);
    bench_join_aggregate(conn);
    bench_subquery(conn);

    PQfinish(conn);
    return 0;
}
