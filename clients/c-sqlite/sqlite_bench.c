/* Cross-language SQLite benchmark client — C / sqlite3.
 *
 * The engine-EXACT reference: compiled against the SAME bundled SQLite
 * amalgamation bsql's `rusqlite`/`libsqlite3-sys 0.35.0` links (SQLite 3.50.2),
 * with the SAME bundled compile defines (SQLITE_ENABLE_API_ARMOR etc.), so the
 * C-vs-bsql delta is PURE wrapper overhead, not an engine-version confound
 * (see clients/c-sqlite/build.sh). Scenarios / output shape mirror the PG C
 * client (clients/c/pg_bench.c): `VERSION`, `LAT <scenario> <ns>`,
 * `SKIP <scenario> <reason>`, `RSS <bytes>`, `ERR <scenario> <msg>`; latency is
 * a 2000-warmup, 7-rep MEDIAN ns/op; every column of every row is read.
 *
 * Idiom: prepare each statement ONCE, reuse across the loop (sqlite3_reset) —
 * the universal competitor shape, matched by every client here. That maps to
 * bsql's `parity_sqlite` PREPARED cells (`by_pk_prepared`, `10row_prepared`);
 * bsql's per-call-prepare / eager API variants (`sqlite_fetch_one`,
 * `sqlite_fetch_one_eager`, `sqlite_fetch_many/10`) have no distinct competitor
 * analogue, so they are SKIPped with a pointer to the prepared cell.
 *
 * Env:  BENCH_SQLITE_PATH   path to the seeded bench.db (REQUIRED)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/resource.h>
#include "sqlite3.h"

static sqlite3 *DB;
static volatile uint64_t g_sink = 0;

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

static void exec_or_die(const char *sql, const char *scenario) {
    char *err = NULL;
    if (sqlite3_exec(DB, sql, NULL, NULL, &err) != SQLITE_OK) {
        die(scenario, err ? err : sqlite3_errmsg(DB));
    }
}

static sqlite3_stmt *prep(const char *sql, const char *scenario) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(DB, sql, -1, &st, NULL) != SQLITE_OK) {
        die(scenario, sqlite3_errmsg(DB));
    }
    return st;
}

/* Step through every row, touch every column (type-dispatched read) so the
   engine actually decodes — the C peer of parity_sqlite's `touch_all`. Returns
   an accumulator so the optimizer cannot elide the reads. Resets the statement
   for reuse. */
static uint64_t consume(sqlite3_stmt *st) {
    uint64_t acc = 0;
    int ncols = sqlite3_column_count(st);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        for (int c = 0; c < ncols; c++) {
            switch (sqlite3_column_type(st, c)) {
                case SQLITE_INTEGER:
                    acc += (uint64_t)sqlite3_column_int64(st, c);
                    break;
                case SQLITE_FLOAT: {
                    double d = sqlite3_column_double(st, c);
                    acc += (uint64_t)d;
                    break;
                }
                case SQLITE_TEXT: {
                    const unsigned char *t = sqlite3_column_text(st, c);
                    int n = sqlite3_column_bytes(st, c);
                    for (int i = 0; i < n; i++) acc += t[i];
                    break;
                }
                case SQLITE_BLOB: {
                    const unsigned char *b = sqlite3_column_blob(st, c);
                    int n = sqlite3_column_bytes(st, c);
                    for (int i = 0; i < n; i++) acc += b[i];
                    break;
                }
                default: break; /* NULL */
            }
        }
    }
    sqlite3_reset(st);
    if (rc != SQLITE_DONE) die("step", sqlite3_errmsg(DB));
    return acc;
}

/* median-of-7-reps latency runner (identical shape to pg_bench.c). */
static uint64_t bench_lat(int warmup, int N, void (*body)(int iter, void *ctx), void *ctx) {
    for (int i = 0; i < warmup; i++) body(i, ctx);
    uint64_t reps[7];
    for (int rep = 0; rep < 7; rep++) {
        uint64_t t0 = now_ns();
        for (int i = 0; i < N; i++) body(i, ctx);
        uint64_t t1 = now_ns();
        reps[rep] = (t1 - t0) / (uint64_t)N;
    }
    qsort(reps, 7, sizeof reps[0], cmp_u64);
    return reps[3];
}

/* ── prepared statements (prepared once, reused) ──────────────────────────── */
static sqlite3_stmt *S_by_pk;      /* SELECT id,name,email WHERE id=?1        */
static sqlite3_stmt *S_many;       /* SELECT 5 cols ORDER BY id LIMIT ?1      */
static sqlite3_stmt *S_join;       /* join + group by, no param              */
static sqlite3_stmt *S_subq;       /* IN (subquery), no param                */
static sqlite3_stmt *S_ins1;       /* INSERT ... RETURNING id                */
static sqlite3_stmt *S_insb;       /* INSERT ... (batch, no RETURNING)       */

static void prepare_all(void) {
    S_by_pk = prep("SELECT id, name, email FROM bench_users WHERE id = ?1", "prepare_by_pk");
    S_many  = prep("SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1", "prepare_many");
    S_join  = prep(
        "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount "
        "FROM bench_users u JOIN bench_orders o ON u.id = o.user_id "
        "WHERE u.active = 1 GROUP BY u.name ORDER BY SUM(o.amount) DESC LIMIT 100",
        "prepare_join");
    S_subq  = prep(
        "SELECT id, name, email FROM bench_users "
        "WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)",
        "prepare_subq");
    S_ins1  = prep(
        "INSERT INTO bench_users (name, email, active, score) "
        "VALUES (?1, ?2, 1, 0.0) RETURNING id", "prepare_ins1");
    S_insb  = prep(
        "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)",
        "prepare_insb");
}

/* Verify a couple of results so a silently-wrong decode can't pass. */
static void verify(void) {
    sqlite3_bind_int64(S_by_pk, 1, 42);
    if (sqlite3_step(S_by_pk) != SQLITE_ROW) die("verify", "by_pk id=42 no row");
    if (sqlite3_column_int64(S_by_pk, 0) != 42) die("verify", "by_pk id mismatch");
    if (strcmp((const char*)sqlite3_column_text(S_by_pk, 1), "user_42") != 0)
        die("verify", "by_pk name mismatch");
    if (strcmp((const char*)sqlite3_column_text(S_by_pk, 2), "user_42@example.com") != 0)
        die("verify", "by_pk email mismatch");
    sqlite3_reset(S_by_pk);

    int n = 0;
    sqlite3_bind_int64(S_many, 1, 10);
    while (sqlite3_step(S_many) == SQLITE_ROW) n++;
    sqlite3_reset(S_many);
    if (n != 10) die("verify", "fetch_many/10 not 10 rows");
}

/* Remove this client's inserted rows so every insert scenario starts from the
   clean 10k baseline regardless of a prior client / rep. */
static void clean_inserts(void) {
    exec_or_die("DELETE FROM bench_users WHERE name = 'bench_insert' OR name LIKE 'batch_%'", "clean");
}

/* ── bodies ───────────────────────────────────────────────────────────────── */
static void body_by_pk(int iter, void *ctx) {
    (void)ctx;
    int id = (iter % 10000) + 1;
    sqlite3_bind_int64(S_by_pk, 1, id);
    g_sink += consume(S_by_pk);
}
static void body_many(int iter, void *ctx) {
    (void)iter;
    int limit = *(int*)ctx;
    sqlite3_bind_int64(S_many, 1, limit);
    g_sink += consume(S_many);
}
static void body_join(int iter, void *ctx) { (void)iter;(void)ctx; g_sink += consume(S_join); }
static void body_subq(int iter, void *ctx) { (void)iter;(void)ctx; g_sink += consume(S_subq); }

static void body_ins1(int iter, void *ctx) {
    (void)iter;(void)ctx;
    sqlite3_bind_text(S_ins1, 1, "bench_insert", -1, SQLITE_STATIC);
    sqlite3_bind_text(S_ins1, 2, "bench@example.com", -1, SQLITE_STATIC);
    g_sink += consume(S_ins1); /* reads RETURNING id */
}

/* 100 discrete INSERTs inside one transaction — the honest peer of
   parity_sqlite's `sqlite_insert_batch/100`. */
static void body_insb(int iter, void *ctx) {
    (void)iter;(void)ctx;
    exec_or_die("BEGIN", "insert_batch");
    for (int j = 0; j < 100; j++) {
        char name[32], email[48];
        snprintf(name, sizeof name, "batch_%d", j);
        snprintf(email, sizeof email, "batch_%d@example.com", j);
        sqlite3_bind_text(S_insb, 1, name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(S_insb, 2, email, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(S_insb) != SQLITE_DONE) die("insert_batch", sqlite3_errmsg(DB));
        sqlite3_reset(S_insb);
    }
    exec_or_die("COMMIT", "insert_batch");
}

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: sqlite_bench latency|rss\n"); return 2; }
    const char *mode = argv[1];

    const char *path = getenv("BENCH_SQLITE_PATH");
    if (!path || !*path) die("open", "BENCH_SQLITE_PATH must be set");

    /* READWRITE | NOMUTEX — matches rusqlite's default open flags (bsql opens
       NO_MUTEX; serialized externally by single-threaded use). */
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX;
    if (sqlite3_open_v2(path, &DB, flags, NULL) != SQLITE_OK) die("open", sqlite3_errmsg(DB));

    /* Match parity_sqlite's connection PRAGMAs (WAL + synchronous=NORMAL). */
    exec_or_die("PRAGMA journal_mode=WAL", "pragma_wal");
    exec_or_die("PRAGMA synchronous=NORMAL", "pragma_sync");

    printf("VERSION sqlite3 %s\n", sqlite3_libversion());
    fflush(stdout);

    prepare_all();
    verify();

    if (strcmp(mode, "latency") == 0) {
        uint64_t v;

        /* bsql API-path variants of by-PK / 10-row that have no distinct
           competitor idiom (a competitor reuses one prepared statement). */
        printf("SKIP sqlite_fetch_one bsql_streaming_per-call-prepare_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared\n");
        printf("SKIP sqlite_fetch_one_eager bsql_eager-cached_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared\n");
        printf("SKIP sqlite_fetch_many/10 bsql_per-call-prepare_streaming_10-row;competitor_prepared-reuse=10row_prepared\n");

        /* READS first (on the pristine table), then writes — parity ordering. */
        v = bench_lat(2000, 20000, body_by_pk, NULL);
        printf("LAT by_pk_prepared %llu\n", (unsigned long long)v);

        int l10 = 10, l100 = 100, l1000 = 1000, l10000 = 10000;
        v = bench_lat(2000, 10000, body_many, &l10);
        printf("LAT 10row_prepared %llu\n", (unsigned long long)v);
        v = bench_lat(2000, 5000, body_many, &l100);
        printf("LAT sqlite_fetch_many/100 %llu\n", (unsigned long long)v);
        v = bench_lat(500, 2000, body_many, &l1000);
        printf("LAT sqlite_fetch_many/1000 %llu\n", (unsigned long long)v);
        v = bench_lat(100, 300, body_many, &l10000);
        printf("LAT sqlite_fetch_many/10000 %llu\n", (unsigned long long)v);

        v = bench_lat(10, 100, body_join, NULL);
        printf("LAT sqlite_join_aggregate %llu\n", (unsigned long long)v);
        v = bench_lat(500, 2000, body_subq, NULL);
        printf("LAT sqlite_subquery %llu\n", (unsigned long long)v);

        clean_inserts();
        v = bench_lat(2000, 10000, body_ins1, NULL);
        printf("LAT sqlite_insert_single %llu\n", (unsigned long long)v);

        clean_inserts();
        v = bench_lat(30, 300, body_insb, NULL);
        printf("LAT sqlite_insert_batch/100 %llu\n", (unsigned long long)v);

        clean_inserts();
        fflush(stdout);
    } else if (strcmp(mode, "rss") == 0) {
        /* Reference workload: 10000 by_pk + 1000 inserts (mirrors the PG client). */
        for (int i = 0; i < 10000; i++) {
            sqlite3_bind_int64(S_by_pk, 1, (i % 10000) + 1);
            g_sink += consume(S_by_pk);
        }
        clean_inserts();
        for (int i = 0; i < 1000; i++) body_ins1(i, NULL);
        clean_inserts();

        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        /* macOS: ru_maxrss is BYTES. (Linux: KiB — divide the PEAK line by 1024.) */
        printf("RSS %lld\n", (long long)ru.ru_maxrss);
        printf("PEAK_RSS %.2f\n", (double)ru.ru_maxrss / 1048576.0);
        fflush(stdout);
    } else {
        fprintf(stderr, "unknown mode %s\n", mode);
        return 2;
    }

    sqlite3_finalize(S_by_pk); sqlite3_finalize(S_many); sqlite3_finalize(S_join);
    sqlite3_finalize(S_subq);  sqlite3_finalize(S_ins1); sqlite3_finalize(S_insb);
    sqlite3_close(DB);
    return 0;
}
