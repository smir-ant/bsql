/* Benchmark client — C / libpq. See CONTRACT in task. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/resource.h>
#include <arpa/inet.h>   /* htonl */
#include <libpq-fe.h>

static void die(const char *scenario, const char *msg) {
    fprintf(stderr, "ERR %s %s\n", scenario, msg);
    printf("ERR %s %s\n", scenario, msg);
    exit(1);
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t*)a, y = *(const uint64_t*)b;
    return (x > y) - (x < y);
}

static PGconn *CONN;

/* Read every column of every row into real variables so decode actually happens.
   Returns an accumulator so the compiler can't elide the reads. */
static volatile uint64_t g_sink = 0;

static uint64_t consume(PGresult *res) {
    uint64_t acc = 0;
    int nrows = PQntuples(res);
    int ncols = PQnfields(res);
    for (int r = 0; r < nrows; r++) {
        for (int c = 0; c < ncols; c++) {
            if (PQgetisnull(res, r, c)) continue;
            char *v = PQgetvalue(res, r, c);
            int len = PQgetlength(res, r, c);
            /* touch bytes */
            for (int i = 0; i < len; i++) acc += (unsigned char)v[i];
        }
    }
    return acc;
}

/* Prepare all statements once. */
static void prepare_all(void) {
    PGresult *r;
    r = PQprepare(CONN, "by_pk",
        "SELECT id, name, val FROM bench_items WHERE id = $1", 1, NULL);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) die("prepare_by_pk", PQerrorMessage(CONN));
    PQclear(r);

    r = PQprepare(CONN, "rows_n",
        "SELECT id, name, val FROM bench_items WHERE id <= $1 ORDER BY id", 1, NULL);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) die("prepare_rows_n", PQerrorMessage(CONN));
    PQclear(r);

    r = PQprepare(CONN, "insert",
        "INSERT INTO bench_ins (id, name, val) VALUES ($1, $2, $3)", 3, NULL);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) die("prepare_insert", PQerrorMessage(CONN));
    PQclear(r);

    r = PQprepare(CONN, "join_agg",
        "SELECT c.label, count(*)::int8, sum(i.val)::int8 FROM bench_items i "
        "JOIN bench_cat c ON i.val = c.val WHERE i.id <= $1 GROUP BY c.label ORDER BY c.label",
        1, NULL);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) die("prepare_join_agg", PQerrorMessage(CONN));
    PQclear(r);
}

/* by_pk exec: param is int passed as text. */
static uint64_t exec_by_pk(int id) {
    char buf[16];
    snprintf(buf, sizeof buf, "%d", id);
    const char *vals[1] = { buf };
    PGresult *r = PQexecPrepared(CONN, "by_pk", 1, vals, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) die("by_pk", PQerrorMessage(CONN));
    uint64_t a = consume(r);
    PQclear(r);
    return a;
}

static uint64_t exec_rows_n(int limit) {
    char buf[16];
    snprintf(buf, sizeof buf, "%d", limit);
    const char *vals[1] = { buf };
    PGresult *r = PQexecPrepared(CONN, "rows_n", 1, vals, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) die("rows_n", PQerrorMessage(CONN));
    uint64_t a = consume(r);
    PQclear(r);
    return a;
}

static void exec_insert(int64_t id) {
    char idbuf[24];
    snprintf(idbuf, sizeof idbuf, "%lld", (long long)id);
    const char *vals[3] = { idbuf, "x", "1" };
    PGresult *r = PQexecPrepared(CONN, "insert", 3, vals, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) die("insert", PQerrorMessage(CONN));
    PQclear(r);
}

static uint64_t exec_join_agg(int limit) {
    char buf[16];
    snprintf(buf, sizeof buf, "%d", limit);
    const char *vals[1] = { buf };
    PGresult *r = PQexecPrepared(CONN, "join_agg", 1, vals, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) die("join_agg", PQerrorMessage(CONN));
    uint64_t a = consume(r);
    PQclear(r);
    return a;
}

static void truncate_ins(void) {
    PGresult *r = PQexec(CONN, "TRUNCATE bench_ins");
    if (PQresultStatus(r) != PGRES_COMMAND_OK) die("truncate", PQerrorMessage(CONN));
    PQclear(r);
}

/* Verify a couple results are correct so a silently-wrong decode can't pass. */
static void verify(void) {
    const char *vals[1] = { "1" };
    PGresult *r = PQexecPrepared(CONN, "by_pk", 1, vals, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) die("verify", PQerrorMessage(CONN));
    if (PQntuples(r) != 1) die("verify", "by_pk id=1 not 1 row");
    if (strcmp(PQgetvalue(r, 0, 1), "name_1") != 0) die("verify", "by_pk id=1 name mismatch");
    if (strcmp(PQgetvalue(r, 0, 2), "2") != 0) die("verify", "by_pk id=1 val mismatch");
    PQclear(r);

    const char *v2[1] = { "10" };
    r = PQexecPrepared(CONN, "rows_n", 1, v2, NULL, NULL, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) die("verify", PQerrorMessage(CONN));
    if (PQntuples(r) != 10) die("verify", "rows_10 not 10 rows");
    PQclear(r);
}

/* median-of-7-reps latency runner */
static uint64_t bench_lat(const char *name, int N,
                          void (*body)(int iter, void *ctx), void *ctx) {
    /* warmup */
    for (int i = 0; i < 2000; i++) body(i, ctx);
    uint64_t reps[7];
    for (int rep = 0; rep < 7; rep++) {
        uint64_t t0 = now_ns();
        for (int i = 0; i < N; i++) body(i, ctx);
        uint64_t t1 = now_ns();
        reps[rep] = (t1 - t0) / (uint64_t)N;
    }
    qsort(reps, 7, sizeof reps[0], cmp_u64);
    (void)name;
    return reps[3];
}

/* bodies */
static void body_by_pk(int iter, void *ctx) {
    (void)ctx;
    int id = (iter % 10000) + 1;
    g_sink += exec_by_pk(id);
}
static void body_rows10(int iter, void *ctx)   { (void)iter;(void)ctx; g_sink += exec_rows_n(10); }
static void body_rows100(int iter, void *ctx)  { (void)iter;(void)ctx; g_sink += exec_rows_n(100); }
static void body_rows1000(int iter, void *ctx) { (void)iter;(void)ctx; g_sink += exec_rows_n(1000); }
static void body_join(int iter, void *ctx)     { (void)iter;(void)ctx; g_sink += exec_join_agg(10000); }

/* insert body needs a monotonically increasing id — use a counter in ctx */
static void body_insert(int iter, void *ctx) {
    int64_t *counter = (int64_t*)ctx;
    (void)iter;
    exec_insert(++(*counter));
}

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: pg_bench latency|rss\n"); return 2; }
    const char *mode = argv[1];

    const char *conninfo =
        "host=127.0.0.1 port=5432 user=smir-ant dbname=postgres sslmode=disable";
    CONN = PQconnectdb(conninfo);
    if (PQstatus(CONN) != CONNECTION_OK) die("connect", PQerrorMessage(CONN));

    /* TCP_NODELAY: libpq sets it by default on TCP sockets. Nothing to do. */

    printf("VERSION libpq %d\n", PQlibVersion());
    fflush(stdout);

    prepare_all();
    verify();

    if (strcmp(mode, "latency") == 0) {
        uint64_t v;
        v = bench_lat("by_pk", 20000, body_by_pk, NULL);
        printf("LAT by_pk %llu\n", (unsigned long long)v);

        v = bench_lat("rows_10", 10000, body_rows10, NULL);
        printf("LAT rows_10 %llu\n", (unsigned long long)v);

        v = bench_lat("rows_100", 5000, body_rows100, NULL);
        printf("LAT rows_100 %llu\n", (unsigned long long)v);

        v = bench_lat("rows_1000", 2000, body_rows1000, NULL);
        printf("LAT rows_1000 %llu\n", (unsigned long long)v);

        /* insert: truncate then count up. Warmup also inserts, so truncate before timed reps too.
           bench_lat warms up 2000 then 7 reps of N. Each insert must have a fresh unique id
           because id is PK. We truncate once at start, then use one ever-increasing counter
           across warmup + all reps (ids never collide). */
        truncate_ins();
        int64_t counter = 0;
        v = bench_lat("insert", 10000, body_insert, &counter);
        printf("LAT insert %llu\n", (unsigned long long)v);

        v = bench_lat("join_agg", 500, body_join, NULL);
        printf("LAT join_agg %llu\n", (unsigned long long)v);

        fflush(stdout);
    } else if (strcmp(mode, "rss") == 0) {
        /* reference workload: 10000 by_pk + 1000 inserts */
        for (int i = 0; i < 10000; i++) g_sink += exec_by_pk((i % 10000) + 1);
        truncate_ins();
        for (int64_t i = 1; i <= 1000; i++) exec_insert(i);

        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        /* macOS: ru_maxrss is bytes */
        printf("RSS %lld\n", (long long)ru.ru_maxrss);
        fflush(stdout);
    } else {
        fprintf(stderr, "unknown mode %s\n", mode);
        return 2;
    }

    PQfinish(CONN);
    return 0;
}
