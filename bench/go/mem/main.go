// go_pg.go -- memory benchmark for Go (pgx)
//
// Identical workload to mem_bsql_pg / mem_sqlx_pg / mem_diesel_pg:
//   connect → 10K SELECT by PK → 1K INSERT → exit
//
// Run: BENCH_DATABASE_URL="host=/tmp dbname=bench_db" /usr/bin/time -l go run mem/go_pg.go

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	url := os.Getenv("BENCH_DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "BENCH_DATABASE_URL not set")
		os.Exit(1)
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	// 10K SELECT queries
	for i := 0; i < 10000; i++ {
		id := (i % 10000) + 1
		var rid int32
		var name, email string
		err := conn.QueryRow(ctx,
			"SELECT id, name, email FROM bench_users WHERE id = $1", id,
		).Scan(&rid, &name, &email)
		if err != nil {
			fmt.Fprintf(os.Stderr, "select failed: %v\n", err)
			os.Exit(1)
		}
	}

	// 1K INSERT queries
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("memtest_%d", i)
		email := fmt.Sprintf("mem%d@test.com", i)
		_, err := conn.Exec(ctx,
			"INSERT INTO bench_users (name, email, active, score) VALUES ($1, $2, true, 0.0)",
			name, email,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "insert failed: %v\n", err)
			os.Exit(1)
		}
	}
}
