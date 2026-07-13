package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sys/unix"
)

const (
	dsn        = "host=127.0.0.1 port=5432 user=smir-ant dbname=postgres sslmode=disable"
	sqlByPk    = "SELECT id, name, val FROM bench_items WHERE id = $1"
	sqlRows    = "SELECT id, name, val FROM bench_items WHERE id <= $1 ORDER BY id"
	sqlInsert  = "INSERT INTO bench_ins (id, name, val) VALUES ($1, $2, $3)"
	sqlJoinAgg = "SELECT c.label, count(*)::int8, sum(i.val)::int8 FROM bench_items i JOIN bench_cat c ON i.val = c.val WHERE i.id <= $1 GROUP BY c.label ORDER BY c.label"
)

func die(scenario, msg string) {
	fmt.Printf("ERR %s %s\n", scenario, msg)
	os.Exit(1)
}

func mustConn(ctx context.Context) *pgx.Conn {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		die("connect", err.Error())
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		die("connect", err.Error())
	}
	return conn
}

// prepareAll registers all prepared statements on the connection.
func prepareAll(ctx context.Context, conn *pgx.Conn) {
	stmts := map[string]string{
		"by_pk":    sqlByPk,
		"rows":     sqlRows,
		"insert":   sqlInsert,
		"join_agg": sqlJoinAgg,
	}
	for name, sql := range stmts {
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			die(name, err.Error())
		}
	}
}

// ---- per-scenario single-iteration runners (read every column) ----

func runByPk(ctx context.Context, conn *pgx.Conn, id int32) error {
	var oid, val int32
	var name string
	if err := conn.QueryRow(ctx, "by_pk", id).Scan(&oid, &name, &val); err != nil {
		return err
	}
	_ = oid
	_ = name
	_ = val
	return nil
}

func runRows(ctx context.Context, conn *pgx.Conn, limit int32) error {
	rows, err := conn.Query(ctx, "rows", limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var oid, val int32
		var name string
		if err := rows.Scan(&oid, &name, &val); err != nil {
			return err
		}
		_ = oid
		_ = name
		_ = val
	}
	return rows.Err()
}

func runInsert(ctx context.Context, conn *pgx.Conn, id int64) error {
	_, err := conn.Exec(ctx, "insert", id, "x", int32(1))
	return err
}

func runJoinAgg(ctx context.Context, conn *pgx.Conn, limit int32) error {
	rows, err := conn.Query(ctx, "join_agg", limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var label string
		var cnt, sum int64
		if err := rows.Scan(&label, &cnt, &sum); err != nil {
			return err
		}
		_ = label
		_ = cnt
		_ = sum
	}
	return rows.Err()
}

// verify a couple of results so a silently-wrong decode can't pass.
func verify(ctx context.Context, conn *pgx.Conn) {
	var id, val int32
	var name string
	if err := conn.QueryRow(ctx, "by_pk", int32(1)).Scan(&id, &name, &val); err != nil {
		die("verify", err.Error())
	}
	if id != 1 || name != "name_1" || val != 2 {
		die("verify", fmt.Sprintf("by_pk id=1 got id=%d name=%s val=%d", id, name, val))
	}
	if err := conn.QueryRow(ctx, "by_pk", int32(5000)).Scan(&id, &name, &val); err != nil {
		die("verify", err.Error())
	}
	if id != 5000 || name != "name_5000" || val != 10000 {
		die("verify", fmt.Sprintf("by_pk id=5000 got id=%d name=%s val=%d", id, name, val))
	}
}

func truncateIns(ctx context.Context, conn *pgx.Conn) {
	if _, err := conn.Exec(ctx, "TRUNCATE bench_ins"); err != nil {
		die("insert", err.Error())
	}
}

func medianNsPerOp(ctx context.Context, scenario string, reps, n int, body func(iter int) error) int64 {
	perOp := make([]int64, reps)
	for r := 0; r < reps; r++ {
		start := time.Now()
		for i := 0; i < n; i++ {
			if err := body(i); err != nil {
				die(scenario, err.Error())
			}
		}
		elapsed := time.Since(start).Nanoseconds()
		perOp[r] = elapsed / int64(n)
	}
	sort.Slice(perOp, func(a, b int) bool { return perOp[a] < perOp[b] })
	return perOp[reps/2]
}

func warmup(ctx context.Context, scenario string, iters int, body func(iter int) error) {
	for i := 0; i < iters; i++ {
		if err := body(i); err != nil {
			die(scenario, err.Error())
		}
	}
}

func latencyMode(ctx context.Context, conn *pgx.Conn) {
	const reps = 7

	// by_pk: param cycles 1..10000, N=20000
	{
		body := func(i int) error { return runByPk(ctx, conn, int32(i%10000)+1) }
		warmup(ctx, "by_pk", 2000, body)
		fmt.Printf("LAT by_pk %d\n", medianNsPerOp(ctx, "by_pk", reps, 20000, body))
	}
	// rows_10: $1=10, N=10000
	{
		body := func(i int) error { return runRows(ctx, conn, 10) }
		warmup(ctx, "rows_10", 2000, body)
		fmt.Printf("LAT rows_10 %d\n", medianNsPerOp(ctx, "rows_10", reps, 10000, body))
	}
	// rows_100: $1=100, N=5000
	{
		body := func(i int) error { return runRows(ctx, conn, 100) }
		warmup(ctx, "rows_100", 2000, body)
		fmt.Printf("LAT rows_100 %d\n", medianNsPerOp(ctx, "rows_100", reps, 5000, body))
	}
	// rows_1000: $1=1000, N=2000
	{
		body := func(i int) error { return runRows(ctx, conn, 1000) }
		warmup(ctx, "rows_1000", 2000, body)
		fmt.Printf("LAT rows_1000 %d\n", medianNsPerOp(ctx, "rows_1000", reps, 2000, body))
	}
	// insert: TRUNCATE, then incrementing i64 starting at 1, N=10000
	{
		truncateIns(ctx, conn)
		var next int64 = 1
		body := func(i int) error {
			err := runInsert(ctx, conn, next)
			next++
			return err
		}
		warmup(ctx, "insert", 2000, body)
		fmt.Printf("LAT insert %d\n", medianNsPerOp(ctx, "insert", reps, 10000, body))
	}
	// join_agg: $1=10000, N=500
	{
		body := func(i int) error { return runJoinAgg(ctx, conn, 10000) }
		warmup(ctx, "join_agg", 2000, body)
		fmt.Printf("LAT join_agg %d\n", medianNsPerOp(ctx, "join_agg", reps, 500, body))
	}
}

func rssMode(ctx context.Context, conn *pgx.Conn) {
	// reference workload: 10000 by_pk SELECTs + 1000 INSERTs (prepared, read columns)
	for i := 0; i < 10000; i++ {
		if err := runByPk(ctx, conn, int32(i%10000)+1); err != nil {
			die("by_pk", err.Error())
		}
	}
	truncateIns(ctx, conn)
	for i := int64(1); i <= 1000; i++ {
		if err := runInsert(ctx, conn, i); err != nil {
			die("insert", err.Error())
		}
	}
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		die("rss", err.Error())
	}
	// macOS: ru_maxrss is in BYTES.
	fmt.Printf("RSS %d\n", int64(ru.Maxrss))
}

func main() {
	fmt.Printf("VERSION jackc/pgx v5.10.0\n")

	if len(os.Args) < 2 {
		die("args", "missing mode (latency|rss)")
	}
	mode := os.Args[1]

	ctx := context.Background()
	conn := mustConn(ctx)
	defer conn.Close(ctx)

	// TCP_NODELAY: pgx uses the standard net dialer which sets TCP_NODELAY on by default.
	prepareAll(ctx, conn)
	verify(ctx, conn)

	switch mode {
	case "latency":
		latencyMode(ctx, conn)
	case "rss":
		rssMode(ctx, conn)
	default:
		die("args", "unknown mode: "+mode)
	}
}
