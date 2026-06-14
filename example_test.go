package cubridsql_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	_ "github.com/hexacluster/gocubrid"
	"github.com/hexacluster/gocubrid/cubrid"
)

// The examples in this file are compile-checked by go test but not
// executed (they carry no Output comment); each one also returns early
// unless CUBRID_DSN points at a live broker, e.g.
//
//	CUBRID_DSN=cubrid://dba:password@localhost:33000/demodb

// Example opens a connection pool through database/sql and runs a
// parameterized round-trip: DDL, a multi-row INSERT, and a SELECT.
func Example() {
	dsn := os.Getenv("CUBRID_DSN")
	if dsn == "" {
		return // no live broker available
	}
	db, err := sql.Open("cubrid", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP TABLE IF EXISTS example_quickstart"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"CREATE TABLE example_quickstart (id INT PRIMARY KEY, name VARCHAR(64))"); err != nil {
		log.Fatal(err)
	}
	defer db.Exec("DROP TABLE example_quickstart")

	res, err := db.Exec("INSERT INTO example_quickstart VALUES (?, ?), (?, ?)",
		1, "ada", 2, "grace")
	if err != nil {
		log.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 2 {
		log.Fatalf("inserted %d rows, want 2", n)
	}

	rows, err := db.Query(
		"SELECT id, name FROM example_quickstart WHERE id >= ? ORDER BY id", 1)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// Example_nativeSchema uses the native client (the cubrid subpackage)
// directly: one Connect, then typed catalog introspection, Columns and
// Indexes, without going through database/sql.
func Example_nativeSchema() {
	dsn := os.Getenv("CUBRID_DSN")
	if dsn == "" {
		return // no live broker available
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS example_schema"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE example_schema (
		id      INT PRIMARY KEY,
		email   VARCHAR(128) UNIQUE,
		created DATETIME)`); err != nil {
		log.Fatal(err)
	}
	defer conn.Exec(ctx, "DROP TABLE example_schema")

	cols, err := conn.Columns(ctx, "example_schema")
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range cols {
		fmt.Printf("column %s (not null: %v)\n", c.Name, c.NotNull)
	}

	idxs, err := conn.Indexes(ctx, "example_schema")
	if err != nil {
		log.Fatal(err)
	}
	for _, ix := range idxs {
		fmt.Printf("index %s unique=%v primary=%v columns=%v\n",
			ix.Name, ix.Unique, ix.IsPrimary, ix.Columns)
	}
}

// Example_lobStreaming writes a BLOB through the chunked locator API and
// streams it back with ReadAt, the path to use for LOBs too large for
// the database/sql adapter's 1 MiB materialization cap. Locators are
// transaction-scoped, so LOB work runs with autocommit off.
func Example_lobStreaming() {
	dsn := os.Getenv("CUBRID_DSN")
	if dsn == "" {
		return // no live broker available
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS example_lob"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ctx,
		"CREATE TABLE example_lob (id INT PRIMARY KEY, doc BLOB)"); err != nil {
		log.Fatal(err)
	}
	defer conn.Exec(ctx, "DROP TABLE example_lob")

	conn.SetAutoCommit(false) // locators live within the transaction

	blob, err := conn.NewBlob(ctx)
	if err != nil {
		log.Fatal(err)
	}
	payload := bytes.Repeat([]byte("cubrid-go "), 10_000) // ~100 KiB
	if _, err := blob.Append(ctx, payload); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO example_lob VALUES (?, ?)", 1, blob); err != nil {
		log.Fatal(err)
	}
	if err := conn.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	stmt, err := conn.Prepare(ctx, "SELECT doc FROM example_lob WHERE id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close(ctx)
	rows, err := stmt.Query(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	row, err := rows.Next()
	if err != nil {
		log.Fatal(err)
	}
	lob := row[0].(*cubrid.Lob)

	buf := make([]byte, 32<<10) // stream in 32 KiB slices
	total := 0
	for off := int64(0); ; {
		n, err := lob.ReadAt(ctx, buf, off)
		total += n
		off += int64(n)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := conn.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	conn.SetAutoCommit(true)
	fmt.Printf("streamed %d bytes\n", total)
}

// Example_batch inserts many rows in a single round-trip with the native
// prepared-statement batch API. Per-row failures land in BatchResult.Err
// without aborting the rest of the batch.
func Example_batch() {
	dsn := os.Getenv("CUBRID_DSN")
	if dsn == "" {
		return // no live broker available
	}
	cfg, err := cubrid.ParseDSN(dsn)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	conn, err := cubrid.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS example_batch"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ctx,
		"CREATE TABLE example_batch (id INT PRIMARY KEY, word VARCHAR(40))"); err != nil {
		log.Fatal(err)
	}
	defer conn.Exec(ctx, "DROP TABLE example_batch")

	stmt, err := conn.Prepare(ctx, "INSERT INTO example_batch VALUES (?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close(ctx)

	results, err := stmt.ExecBatch(ctx, [][]any{
		{1, "alpha"},
		{2, "beta"},
		{3, "gamma"},
	})
	if err != nil {
		log.Fatal(err)
	}
	for i, r := range results {
		if r.Err != nil {
			log.Fatalf("row %d: %v", i, r.Err)
		}
	}
	fmt.Printf("%d rows inserted in one round-trip\n", len(results))
}
