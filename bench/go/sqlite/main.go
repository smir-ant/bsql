// sqlite benchmark -- go-sqlite3 SQLite benchmark
//
// Same queries as the Rust criterion benchmarks.
// Uses database/sql with mattn/go-sqlite3 driver.
//
// Run:
//   BENCH_SQLITE_PATH=../bench.db go run ./sqlite/

package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const iterations = 10000

func must[T any](v T, err error) T {
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
	return v
}

func mustNoErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	path := os.Getenv("BENCH_SQLITE_PATH")
	if path == "" {
		fmt.Fprintln(os.Stderr, "BENCH_SQLITE_PATH not set")
		os.Exit(1)
	}

	db := must(sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL&_mutex=no"))
	defer db.Close()

	// Verify connection
	mustNoErr(db.Ping())

	fmt.Println("=== Go (go-sqlite3) SQLite Benchmarks ===")
	fmt.Println()

	benchFetchOne(db)
	benchFetchMany(db, 10)
	benchFetchMany(db, 100)
	benchFetchMany(db, 1000)
	benchFetchMany(db, 10000)
	benchInsertSingle(db)
	benchInsertBatch(db)
	benchJoinAggregate(db)
	benchSubquery(db)
}

func benchFetchOne(db *sql.DB) {
	sqlText := "SELECT id, name, email FROM bench_users WHERE id = ?1"

	stmt := must(db.Prepare(sqlText))
	defer stmt.Close()

	// Warm up
	var id int64
	var name, email string
	mustNoErr(stmt.QueryRow(42).Scan(&id, &name, &email))

	start := time.Now()
	for _ = range iterations {
		_ = stmt.QueryRow(42).Scan(&id, &name, &email)
	}
	elapsed := time.Since(start)
	fmt.Printf("sqlite_fetch_one:       %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/iterations, iterations)
}

func benchFetchMany(db *sql.DB, limit int) {
	sqlText := "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1"

	stmt := must(db.Prepare(sqlText))
	defer stmt.Close()

	// Warm up
	rows := must(stmt.Query(limit))
	for rows.Next() {
		var id int64
		var name, email string
		var active int
		var score float64
		_ = rows.Scan(&id, &name, &email, &active, &score)
	}
	rows.Close()

	iters := iterations
	if limit >= 10000 {
		iters = 1000
	}

	start := time.Now()
	for _ = range iters {
		rows, _ := stmt.Query(limit)
		for rows.Next() {
			var id int64
			var name, email string
			var active int
			var score float64
			_ = rows.Scan(&id, &name, &email, &active, &score)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("sqlite_fetch_many/%d: ", limit)
	if limit < 1000 {
		fmt.Print("   ")
	} else if limit < 10000 {
		fmt.Print("  ")
	} else {
		fmt.Print(" ")
	}
	fmt.Printf("%d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchInsertSingle(db *sql.DB) {
	sqlText := "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0) RETURNING id"

	stmt := must(db.Prepare(sqlText))
	defer stmt.Close()

	// Warm up
	var id int64
	mustNoErr(stmt.QueryRow("bench_insert", "bench@example.com").Scan(&id))

	start := time.Now()
	for _ = range iterations {
		_ = stmt.QueryRow("bench_insert", "bench@example.com").Scan(&id)
	}
	elapsed := time.Since(start)
	fmt.Printf("sqlite_insert_single:   %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/iterations, iterations)
}

func benchInsertBatch(db *sql.DB) {
	sqlText := "INSERT INTO bench_users (name, email, active, score) VALUES (?1, ?2, 1, 0.0)"

	stmt := must(db.Prepare(sqlText))
	defer stmt.Close()

	iters := 1000

	start := time.Now()
	for _ = range iters {
		tx := must(db.Begin())
		for j := range 100 {
			name := fmt.Sprintf("batch_%d", j)
			email := fmt.Sprintf("batch_%d@example.com", j)
			_, _ = tx.Stmt(stmt).Exec(name, email)
		}
		mustNoErr(tx.Commit())
	}
	elapsed := time.Since(start)
	fmt.Printf("sqlite_insert_batch/100: %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchJoinAggregate(db *sql.DB) {
	sqlText := `SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount
		FROM bench_users u
		JOIN bench_orders o ON u.id = o.user_id
		WHERE u.active = 1
		GROUP BY u.name
		ORDER BY SUM(o.amount) DESC
		LIMIT 100`

	stmt := must(db.Prepare(sqlText))
	defer stmt.Close()

	// Warm up
	rows := must(stmt.Query())
	for rows.Next() {
		var name string
		var orderCount int
		var totalAmount float64
		_ = rows.Scan(&name, &orderCount, &totalAmount)
	}
	rows.Close()

	iters := 1000
	start := time.Now()
	for _ = range iters {
		rows, _ := stmt.Query()
		for rows.Next() {
			var name string
			var orderCount int
			var totalAmount float64
			_ = rows.Scan(&name, &orderCount, &totalAmount)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("sqlite_join_aggregate:  %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchSubquery(db *sql.DB) {
	sqlText := `SELECT id, name, email FROM bench_users
		WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)`

	stmt := must(db.Prepare(sqlText))
	defer stmt.Close()

	// Warm up
	rows := must(stmt.Query())
	for rows.Next() {
		var id int64
		var name, email string
		_ = rows.Scan(&id, &name, &email)
	}
	rows.Close()

	iters := 5000
	start := time.Now()
	for _ = range iters {
		rows, _ := stmt.Query()
		for rows.Next() {
			var id int64
			var name, email string
			_ = rows.Scan(&id, &name, &email)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("sqlite_subquery:        %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}
