// Cross-language SQLite benchmark client — Go / mattn/go-sqlite3.
//
// mattn/go-sqlite3 is the de-facto standard Go SQLite driver (cgo, wrapping a C
// SQLite the module bundles — so this is the fair "what a Go dev reaches for"
// choice; the pure-Go alternative modernc.org/sqlite is noted in the README).
// Because it is cgo, every call crosses the Go<->C boundary — the honest Go
// cost, reported as-is.
//
// Scenarios / output shape mirror the PG Go client (clients/go/main.go) and the
// C SQLite client: VERSION, LAT <scenario> <ns>, SKIP <scenario> <reason>,
// RSS <bytes>, ERR. Latency is a 2000-warmup, 7-rep MEDIAN ns/op; every column
// of every row is scanned. Idiom: one prepared *sql.Stmt reused across the loop
// (the universal competitor shape) — mapping to bsql's `parity_sqlite` PREPARED
// cells (by_pk_prepared / 10row_prepared); bsql's per-call-prepare / eager API
// variants have no distinct competitor analogue and are SKIPped.
//
// Env:  BENCH_SQLITE_PATH   path to the seeded bench.db (REQUIRED)
package main

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB
var sink uint64

func die(scenario, msg string) {
	fmt.Printf("ERR %s %s\n", scenario, msg)
	os.Exit(1)
}

// prepared statements (prepared once, reused).
var (
	sByPk *sql.Stmt // SELECT id,name,email WHERE id=?
	sMany *sql.Stmt // SELECT 5 cols ORDER BY id LIMIT ?
	sJoin *sql.Stmt // join + group by, no param
	sSubq *sql.Stmt // IN (subquery), no param
	sIns1 *sql.Stmt // INSERT ... RETURNING id
	sInsb *sql.Stmt // INSERT ... (batch, no RETURNING)
)

func prep(sqlText, scenario string) *sql.Stmt {
	st, err := db.Prepare(sqlText)
	if err != nil {
		die(scenario, err.Error())
	}
	return st
}

func prepareAll() {
	sByPk = prep("SELECT id, name, email FROM bench_users WHERE id = ?", "prepare_by_pk")
	sMany = prep("SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?", "prepare_many")
	sJoin = prep(
		"SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount "+
			"FROM bench_users u JOIN bench_orders o ON u.id = o.user_id "+
			"WHERE u.active = 1 GROUP BY u.name ORDER BY SUM(o.amount) DESC LIMIT 100", "prepare_join")
	sSubq = prep(
		"SELECT id, name, email FROM bench_users "+
			"WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)", "prepare_subq")
	sIns1 = prep("INSERT INTO bench_users (name, email, active, score) VALUES (?, ?, 1, 0.0) RETURNING id", "prepare_ins1")
	sInsb = prep("INSERT INTO bench_users (name, email, active, score) VALUES (?, ?, 1, 0.0)", "prepare_insb")
}

// ── per-scenario single-iteration runners (read every column of every row) ──

func runByPk(id int64) error {
	var oid int64
	var name, email string
	if err := sByPk.QueryRow(id).Scan(&oid, &name, &email); err != nil {
		return err
	}
	sink += uint64(oid) + uint64(len(name)) + uint64(len(email))
	return nil
}

func runMany(limit int64) error {
	rows, err := sMany.Query(limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, active int64
		var name, email string
		var score float64
		if err := rows.Scan(&id, &name, &email, &active, &score); err != nil {
			return err
		}
		sink += uint64(id) + uint64(len(name)) + uint64(len(email)) + uint64(active) + uint64(score)
	}
	return rows.Err()
}

func runAgg() error {
	rows, err := sJoin.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var cnt int64
		var total float64
		if err := rows.Scan(&name, &cnt, &total); err != nil {
			return err
		}
		sink += uint64(len(name)) + uint64(cnt) + uint64(total)
	}
	return rows.Err()
}

func runSubq() error {
	rows, err := sSubq.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			return err
		}
		sink += uint64(id) + uint64(len(name)) + uint64(len(email))
	}
	return rows.Err()
}

func runIns1() error {
	var id int64
	if err := sIns1.QueryRow("bench_insert", "bench@example.com").Scan(&id); err != nil {
		return err
	}
	sink += uint64(id)
	return nil
}

// 100 discrete INSERTs inside one transaction.
func runInsBatch() error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	txStmt := tx.Stmt(sInsb)
	for j := 0; j < 100; j++ {
		name := fmt.Sprintf("batch_%d", j)
		email := fmt.Sprintf("batch_%d@example.com", j)
		if _, err := txStmt.Exec(name, email); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func cleanInserts() {
	if _, err := db.Exec("DELETE FROM bench_users WHERE name = 'bench_insert' OR name LIKE 'batch_%'"); err != nil {
		die("clean", err.Error())
	}
}

// verify a couple of results so a silently-wrong decode can't pass.
func verify() {
	var id int64
	var name, email string
	if err := sByPk.QueryRow(int64(42)).Scan(&id, &name, &email); err != nil {
		die("verify", err.Error())
	}
	if id != 42 || name != "user_42" || email != "user_42@example.com" {
		die("verify", fmt.Sprintf("by_pk id=42 got id=%d name=%s email=%s", id, name, email))
	}
	n := 0
	rows, err := sMany.Query(int64(10))
	if err != nil {
		die("verify", err.Error())
	}
	for rows.Next() {
		n++
	}
	rows.Close()
	if n != 10 {
		die("verify", fmt.Sprintf("fetch_many/10 got %d rows", n))
	}
}

// ── 7-rep median latency runner (identical shape to the PG Go client) ──

func medianNsPerOp(scenario string, warmup, n int, body func(iter int) error) int64 {
	for i := 0; i < warmup; i++ {
		if err := body(i); err != nil {
			die(scenario, err.Error())
		}
	}
	const reps = 7
	perOp := make([]int64, reps)
	for r := 0; r < reps; r++ {
		start := time.Now()
		for i := 0; i < n; i++ {
			if err := body(i); err != nil {
				die(scenario, err.Error())
			}
		}
		perOp[r] = time.Since(start).Nanoseconds() / int64(n)
	}
	sort.Slice(perOp, func(a, b int) bool { return perOp[a] < perOp[b] })
	return perOp[reps/2]
}

func latencyMode() {
	// bsql API-path variants that have no distinct competitor idiom.
	fmt.Println("SKIP sqlite_fetch_one bsql_streaming_per-call-prepare_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared")
	fmt.Println("SKIP sqlite_fetch_one_eager bsql_eager-cached_variant_of_by-PK;competitor_prepared-reuse=by_pk_prepared")
	fmt.Println("SKIP sqlite_fetch_many/10 bsql_per-call-prepare_streaming_10-row;competitor_prepared-reuse=10row_prepared")

	fmt.Printf("LAT by_pk_prepared %d\n", medianNsPerOp("by_pk_prepared", 2000, 20000,
		func(i int) error { return runByPk(int64(i%10000) + 1) }))
	fmt.Printf("LAT 10row_prepared %d\n", medianNsPerOp("10row_prepared", 2000, 10000,
		func(i int) error { return runMany(10) }))
	fmt.Printf("LAT sqlite_fetch_many/100 %d\n", medianNsPerOp("sqlite_fetch_many/100", 2000, 5000,
		func(i int) error { return runMany(100) }))
	fmt.Printf("LAT sqlite_fetch_many/1000 %d\n", medianNsPerOp("sqlite_fetch_many/1000", 500, 2000,
		func(i int) error { return runMany(1000) }))
	fmt.Printf("LAT sqlite_fetch_many/10000 %d\n", medianNsPerOp("sqlite_fetch_many/10000", 100, 300,
		func(i int) error { return runMany(10000) }))
	fmt.Printf("LAT sqlite_join_aggregate %d\n", medianNsPerOp("sqlite_join_aggregate", 10, 100,
		func(i int) error { return runAgg() }))
	fmt.Printf("LAT sqlite_subquery %d\n", medianNsPerOp("sqlite_subquery", 500, 2000,
		func(i int) error { return runSubq() }))

	cleanInserts()
	fmt.Printf("LAT sqlite_insert_single %d\n", medianNsPerOp("sqlite_insert_single", 2000, 10000,
		func(i int) error { return runIns1() }))
	cleanInserts()
	fmt.Printf("LAT sqlite_insert_batch/100 %d\n", medianNsPerOp("sqlite_insert_batch/100", 30, 300,
		func(i int) error { return runInsBatch() }))
	cleanInserts()
}

func rssMode() {
	for i := 0; i < 10000; i++ {
		if err := runByPk(int64(i%10000) + 1); err != nil {
			die("by_pk", err.Error())
		}
	}
	cleanInserts()
	for i := 0; i < 1000; i++ {
		if err := runIns1(); err != nil {
			die("insert", err.Error())
		}
	}
	cleanInserts()

	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		die("rss", err.Error())
	}
	// macOS: Maxrss is BYTES. (Linux: KiB — divide PEAK by 1024.)
	fmt.Printf("RSS %d\n", int64(ru.Maxrss))
	fmt.Printf("PEAK_RSS %.2f\n", float64(ru.Maxrss)/1048576.0)
}

func main() {
	if len(os.Args) < 2 {
		die("args", "missing mode (latency|rss)")
	}
	mode := os.Args[1]

	path := os.Getenv("BENCH_SQLITE_PATH")
	if path == "" {
		die("open", "BENCH_SQLITE_PATH must be set")
	}

	dsn := "file:" + path + "?_journal_mode=WAL&_synchronous=NORMAL&_mutex=no&_busy_timeout=5000"
	var err error
	db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		die("open", err.Error())
	}
	defer db.Close()
	// Single connection — the one-socket latency path (no pool scheduling noise).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	var ver string
	if err := db.QueryRow("SELECT sqlite_version()").Scan(&ver); err != nil {
		die("version", err.Error())
	}
	fmt.Println("VERSION mattn/go-sqlite3 v1.14.24")
	fmt.Printf("VERSION sqlite %s\n", ver)

	prepareAll()
	verify()

	switch mode {
	case "latency":
		latencyMode()
	case "rss":
		rssMode()
	default:
		die("args", "unknown mode: "+mode)
	}
}
