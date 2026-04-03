/*
 * sqlite_bench.c -- raw sqlite3 benchmark
 *
 * Same queries as the Rust criterion benchmarks:
 *   fetch_one    : SELECT by PK (id = 42)
 *   fetch_many   : SELECT LIMIT N  (10, 100, 1000, 10000 rows)
 *   insert_single: single INSERT RETURNING
 *   insert_batch : 100 INSERTs in a transaction
 *   join_agg     : JOIN + GROUP BY + aggregate
 *   subquery     : IN (SELECT ...)
 *
 * Compile:
 *   make sqlite_bench
 *
 * Run:
 *   BENCH_SQLITE_PATH=../bench.db ./sqlite_bench
 */

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mach/mach_time.h>

/* ---------- timing helpers ------------------------------------------------ */

static mach_timebase_info_data_t g_timebase;

static void timing_init(void) {
    mach_timebase_info(&g_timebase);
}

static uint64_t now_ns(void) {
    return mach_absolute_time() * g_timebase.numer / g_timebase.denom;
}

/* ---------- helpers ------------------------------------------------------- */

static sqlite3 *open_db(void) {
    const char *path = getenv("BENCH_SQLITE_PATH");
    if (!path) {
        fprintf(stderr, "BENCH_SQLITE_PATH not set\n");
        exit(1);
    }
    sqlite3 *db;
    /* NOMUTEX: skip SQLite internal locking (same as bsql).
       Both benchmarks serialize access externally. */
    int rc = sqlite3_open_v2(path, &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open %s: %s\n", path, sqlite3_errmsg(db));
        sqlite3_close(db);
        exit(1);
    }
    /* WAL mode for realistic perf */
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL", NULL, NULL, NULL);
    return db;
}

static sqlite3_stmt *prepare(sqlite3 *db, const char *sql) {
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Prepare error: %s\nSQL: %s\n", sqlite3_errmsg(db), sql);
        sqlite3_close(db);
        exit(1);
    }
    return stmt;
}

/* Consume all rows from a stepped statement */
static int consume_rows(sqlite3_stmt *stmt) {
    int rows = 0;
    int ncols = sqlite3_column_count(stmt);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        /* Access ALL columns to force materialization — same work as Rust */
        for (int i = 0; i < ncols; i++) {
            int t = sqlite3_column_type(stmt, i);
            switch (t) {
                case SQLITE_INTEGER: (void)sqlite3_column_int64(stmt, i); break;
                case SQLITE_FLOAT:   (void)sqlite3_column_double(stmt, i); break;
                case SQLITE_TEXT:    (void)sqlite3_column_text(stmt, i);
                                     (void)sqlite3_column_bytes(stmt, i); break;
                case SQLITE_BLOB:    (void)sqlite3_column_blob(stmt, i);
                                     (void)sqlite3_column_bytes(stmt, i); break;
                default: break;
            }
        }
        rows++;
    }
    return rows;
}

/* ---------- benchmarks ---------------------------------------------------- */

#define ITERATIONS 10000

static void bench_fetch_one(sqlite3 *db) {
    sqlite3_stmt *stmt = prepare(db,
        "SELECT id, name, email FROM bench_users WHERE id = ?1");

    /* Warm up */
    sqlite3_bind_int64(stmt, 1, 42);
    consume_rows(stmt);
    sqlite3_reset(stmt);

    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        sqlite3_bind_int64(stmt, 1, 42);
        consume_rows(stmt);
        sqlite3_reset(stmt);
    }
    uint64_t elapsed = now_ns() - start;
    printf("sqlite_fetch_one:       %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / ITERATIONS), ITERATIONS);

    sqlite3_finalize(stmt);
}

static void bench_fetch_many(sqlite3 *db, int limit) {
    sqlite3_stmt *stmt = prepare(db,
        "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1");

    /* Warm up */
    sqlite3_bind_int(stmt, 1, limit);
    consume_rows(stmt);
    sqlite3_reset(stmt);

    int iters = (limit >= 10000) ? 1000 : ITERATIONS;

    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        sqlite3_bind_int(stmt, 1, limit);
        consume_rows(stmt);
        sqlite3_reset(stmt);
    }
    uint64_t elapsed = now_ns() - start;
    printf("sqlite_fetch_many/%d: %*s%llu ns/op  (%d iters)\n",
           limit, (limit < 1000 ? 4 : (limit < 10000 ? 3 : 2)), "",
           (unsigned long long)(elapsed / iters), iters);

    sqlite3_finalize(stmt);
}

static void bench_insert_single(sqlite3 *db) {
    sqlite3_stmt *stmt = prepare(db,
        "INSERT INTO bench_users (name, email, active, score) "
        "VALUES (?1, ?2, 1, 0.0) RETURNING id");

    /* Warm up */
    sqlite3_bind_text(stmt, 1, "bench_insert", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, "bench@example.com", -1, SQLITE_STATIC);
    consume_rows(stmt);
    sqlite3_reset(stmt);

    uint64_t start = now_ns();
    for (int i = 0; i < ITERATIONS; i++) {
        sqlite3_bind_text(stmt, 1, "bench_insert", -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, "bench@example.com", -1, SQLITE_STATIC);
        consume_rows(stmt);
        sqlite3_reset(stmt);
    }
    uint64_t elapsed = now_ns() - start;
    printf("sqlite_insert_single:   %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / ITERATIONS), ITERATIONS);

    sqlite3_finalize(stmt);
}

static void bench_insert_batch(sqlite3 *db) {
    sqlite3_stmt *stmt = prepare(db,
        "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)");

    int iters = 1000;

    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
        for (int j = 0; j < 100; j++) {
            char name[32], email[48];
            snprintf(name, sizeof(name), "batch_%d", j);
            snprintf(email, sizeof(email), "batch_%d@example.com", j);
            sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, email, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_reset(stmt);
        }
        sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    }
    uint64_t elapsed = now_ns() - start;
    printf("sqlite_insert_batch/100: %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / iters), iters);

    sqlite3_finalize(stmt);
}

static void bench_join_aggregate(sqlite3 *db) {
    sqlite3_stmt *stmt = prepare(db,
        "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount "
        "FROM bench_users u "
        "JOIN bench_orders o ON u.id = o.user_id "
        "WHERE u.active = 1 "
        "GROUP BY u.name "
        "ORDER BY SUM(o.amount) DESC "
        "LIMIT 100");

    /* Warm up */
    consume_rows(stmt);
    sqlite3_reset(stmt);

    int iters = 1000;
    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        consume_rows(stmt);
        sqlite3_reset(stmt);
    }
    uint64_t elapsed = now_ns() - start;
    printf("sqlite_join_aggregate:  %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / iters), iters);

    sqlite3_finalize(stmt);
}

static void bench_subquery(sqlite3 *db) {
    sqlite3_stmt *stmt = prepare(db,
        "SELECT id, name, email FROM bench_users "
        "WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)");

    /* Warm up */
    consume_rows(stmt);
    sqlite3_reset(stmt);

    int iters = 5000;
    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) {
        consume_rows(stmt);
        sqlite3_reset(stmt);
    }
    uint64_t elapsed = now_ns() - start;
    printf("sqlite_subquery:        %llu ns/op  (%d iters)\n",
           (unsigned long long)(elapsed / iters), iters);

    sqlite3_finalize(stmt);
}

/* ---------- main ---------------------------------------------------------- */

int main(void) {
    timing_init();
    sqlite3 *db = open_db();

    printf("=== C (sqlite3) SQLite Benchmarks ===\n");
    printf("sqlite3 version: %s\n\n", sqlite3_libversion());

    bench_fetch_one(db);
    bench_fetch_many(db, 10);
    bench_fetch_many(db, 100);
    bench_fetch_many(db, 1000);
    bench_fetch_many(db, 10000);
    bench_insert_single(db);
    bench_insert_batch(db);
    bench_join_aggregate(db);
    bench_subquery(db);

    sqlite3_close(db);
    return 0;
}
