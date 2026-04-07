// pg benchmark -- pgx/v5 PostgreSQL benchmark
//
// Same queries as the Rust criterion benchmarks.
// Uses pgx direct connection (not pool) for fairest comparison with libpq.
//
// Run:
//   BENCH_DATABASE_URL="postgres://smir-ant@localhost/bench_db" go run ./pg/

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
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
	url := os.Getenv("BENCH_DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "BENCH_DATABASE_URL not set")
		os.Exit(1)
	}

	ctx := context.Background()
	conn := must(pgx.Connect(ctx, url))
	defer conn.Close(ctx)

	fmt.Println("=== Go (pgx/v5) PostgreSQL Benchmarks ===")
	fmt.Println()

	benchFetchOne(ctx, conn)
	benchFetchMany(ctx, conn, 10)
	benchFetchMany(ctx, conn, 100)
	benchFetchMany(ctx, conn, 1000)
	benchFetchMany(ctx, conn, 10000)
	benchInsertSingle(ctx, conn)
	benchInsertBatch(ctx, conn)
	benchInsertBatchPipelined(ctx, conn)
	benchJoinAggregate(ctx, conn)
	benchSubquery(ctx, conn)
	benchDynamic(ctx, conn)
}

func benchFetchOne(ctx context.Context, conn *pgx.Conn) {
	sql := "SELECT id, name, email FROM bench_users WHERE id = $1"

	// Warm up
	var id int32
	var name, email string
	mustNoErr(conn.QueryRow(ctx, sql, 42).Scan(&id, &name, &email))

	start := time.Now()
	for _ = range iterations {
		_ = conn.QueryRow(ctx, sql, 42).Scan(&id, &name, &email)
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_fetch_one:       %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/iterations, iterations)
}

func benchFetchMany(ctx context.Context, conn *pgx.Conn, limit int) {
	sql := "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT $1"

	// Warm up
	rows := must(conn.Query(ctx, sql, limit))
	for rows.Next() {
		var id int32
		var name, email string
		var active bool
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
		rows, _ := conn.Query(ctx, sql, limit)
		for rows.Next() {
			var id int32
			var name, email string
			var active bool
			var score float64
			_ = rows.Scan(&id, &name, &email, &active, &score)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_fetch_many/%d: ", limit)
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

func benchInsertSingle(ctx context.Context, conn *pgx.Conn) {
	sql := "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0) RETURNING id"

	// Warm up
	var id int32
	mustNoErr(conn.QueryRow(ctx, sql, "bench_insert", "bench@example.com").Scan(&id))

	start := time.Now()
	for _ = range iterations {
		_ = conn.QueryRow(ctx, sql, "bench_insert", "bench@example.com").Scan(&id)
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_insert_single:   %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/iterations, iterations)
}

func benchInsertBatch(ctx context.Context, conn *pgx.Conn) {
	sql := "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)"
	iters := 1000

	start := time.Now()
	for _ = range iters {
		tx := must(conn.Begin(ctx))
		for j := range 100 {
			name := fmt.Sprintf("batch_%d", j)
			email := fmt.Sprintf("batch_%d@example.com", j)
			_, _ = tx.Exec(ctx, sql, name, email)
		}
		mustNoErr(tx.Commit(ctx))
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_insert_batch/100: %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchInsertBatchPipelined(ctx context.Context, conn *pgx.Conn) {
	sql := "INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)"
	iters := 1000

	// pgx SendBatch: sends all queries in one round-trip
	start := time.Now()
	for range iters {
		batch := &pgx.Batch{}
		for j := range 100 {
			name := fmt.Sprintf("batch_%d", j)
			email := fmt.Sprintf("batch_%d@example.com", j)
			batch.Queue(sql, name, email)
		}
		br := conn.SendBatch(ctx, batch)
		for range 100 {
			_, _ = br.Exec()
		}
		_ = br.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_insert_batch_pipelined/100: %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchJoinAggregate(ctx context.Context, conn *pgx.Conn) {
	sql := `SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_amount
		FROM bench_users u
		JOIN bench_orders o ON u.id = o.user_id
		WHERE u.active = true
		GROUP BY u.name
		ORDER BY SUM(o.amount) DESC
		LIMIT 100`

	// Warm up
	rows := must(conn.Query(ctx, sql))
	for rows.Next() {
		var name string
		var orderCount int64
		var totalAmount float64
		_ = rows.Scan(&name, &orderCount, &totalAmount)
	}
	rows.Close()

	iters := 3000
	start := time.Now()
	for _ = range iters {
		rows, _ := conn.Query(ctx, sql)
		for rows.Next() {
			var name string
			var orderCount int64
			var totalAmount float64
			_ = rows.Scan(&name, &orderCount, &totalAmount)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_join_aggregate:  %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchSubquery(ctx context.Context, conn *pgx.Conn) {
	sql := `SELECT id, name, email FROM bench_users
		WHERE id IN (SELECT user_id FROM bench_orders WHERE amount > 500 LIMIT 100)`

	// Warm up
	rows := must(conn.Query(ctx, sql))
	for rows.Next() {
		var id int32
		var name, email string
		_ = rows.Scan(&id, &name, &email)
	}
	rows.Close()

	iters := 5000
	start := time.Now()
	for _ = range iters {
		rows, _ := conn.Query(ctx, sql)
		for rows.Next() {
			var id int32
			var name, email string
			_ = rows.Scan(&id, &name, &email)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_subquery:        %d ns/op  (%d iters)\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}

func benchDynamic(ctx context.Context, conn *pgx.Conn) {
	// Dynamic query with 4 optional clauses via string concatenation.
	// Go has no compile-time SQL validation — this is the standard approach.
	activeFilter := true
	minScore := 5
	_ = activeFilter
	_ = minScore

	buildSQL := func() string {
		sql := "SELECT id, name, email, active, score FROM bench_users WHERE 1=1"
		if activeFilter {
			sql += " AND active = true"
		} else {
			sql += " AND active = false"
		}
		if minScore > 0 {
			sql += fmt.Sprintf(" AND score > %d", minScore)
		}
		sql += " ORDER BY id LIMIT 100"
		return sql
	}

	// Warm up
	rows := must(conn.Query(ctx, buildSQL()))
	for rows.Next() {
		var id int32
		var name, email string
		var active bool
		var score int16
		_ = rows.Scan(&id, &name, &email, &active, &score)
	}
	rows.Close()

	iters := iterations
	start := time.Now()
	for i := range iters {
		// Vary parameters to prevent PG plan caching on identical SQL text
		if i%2 == 0 {
			activeFilter = true
		} else {
			activeFilter = false
		}
		minScore = i % 10
		rows, _ := conn.Query(ctx, buildSQL())
		for rows.Next() {
			var id int32
			var name, email string
			var active bool
			var score int16
			_ = rows.Scan(&id, &name, &email, &active, &score)
		}
		rows.Close()
	}
	elapsed := time.Since(start)
	fmt.Printf("pg_dynamic_4clauses: %d ns/op  (%d iters)  [string concat]\n",
		elapsed.Nanoseconds()/int64(iters), iters)
}
