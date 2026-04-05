/*
 * c_pg.c -- memory benchmark for C (libpq)
 *
 * Identical workload to mem_bsql_pg / mem_sqlx_pg / mem_diesel_pg:
 *   connect → 10K SELECT by PK → 1K INSERT → exit
 *
 * Compile: cc -O3 -o mem_c_pg c_pg.c -lpq
 * Run:     BENCH_DATABASE_URL="host=/tmp dbname=bench_db" /usr/bin/time -l ./mem_c_pg
 */

#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

static void die_if_bad(PGconn *conn, PGresult *res, ExecStatusType expected) {
    if (PQresultStatus(res) != expected) {
        fprintf(stderr, "PG error: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
}

int main(void) {
    const char *url = getenv("BENCH_DATABASE_URL");
    if (!url) {
        fprintf(stderr, "BENCH_DATABASE_URL not set\n");
        return 1;
    }

    PGconn *conn = PQconnectdb(url);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    /* Prepare SELECT */
    PGresult *prep = PQprepare(conn, "sel",
        "SELECT id, name, email FROM bench_users WHERE id = $1",
        1, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    /* Prepare INSERT */
    prep = PQprepare(conn, "ins",
        "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)",
        2, NULL);
    die_if_bad(conn, prep, PGRES_COMMAND_OK);
    PQclear(prep);

    /* 10K SELECT queries */
    for (int i = 0; i < 10000; i++) {
        int32_t id = htonl((i % 10000) + 1);
        const char *vals[1] = { (const char *)&id };
        int lens[1] = { 4 };
        int fmts[1] = { 1 };

        PGresult *res = PQexecPrepared(conn, "sel", 1, vals, lens, fmts, 1);
        die_if_bad(conn, res, PGRES_TUPLES_OK);
        /* Read all columns to prevent dead-code elimination */
        int nrows = PQntuples(res);
        for (int r = 0; r < nrows; r++) {
            volatile const char *v0 = PQgetvalue(res, r, 0);
            volatile const char *v1 = PQgetvalue(res, r, 1);
            volatile const char *v2 = PQgetvalue(res, r, 2);
            (void)v0; (void)v1; (void)v2;
        }
        PQclear(res);
    }

    /* 1K INSERT queries */
    for (int i = 0; i < 1000; i++) {
        char name[32], email[32];
        snprintf(name, sizeof(name), "memtest_%d", i);
        snprintf(email, sizeof(email), "mem%d@test.com", i);

        const char *vals[2] = { name, email };
        PGresult *res = PQexecPrepared(conn, "ins", 2, vals, NULL, NULL, 0);
        die_if_bad(conn, res, PGRES_COMMAND_OK);
        PQclear(res);
    }

    PQfinish(conn);
    return 0;
}
