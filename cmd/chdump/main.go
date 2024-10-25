package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"net/url"
	"os"
)

func connect(ctx context.Context, host, user, password, db string) (driver.Conn, error) {
	var (
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{host},
			Auth: clickhouse.Auth{
				Database: db,
				Username: user,
				Password: password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func main() {
	ctx := context.Background()
	// Establish a connection to the database

	u, err := url.Parse(os.Args[1])
	if err != nil {
		log.Fatalf("cannot parse url: %v", err)
	}

	password, _ := u.User.Password()

	db, err := connect(ctx, u.Host, u.User.Username(), password, u.Path[1:])
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	// Ping the database to ensure connection is established
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	// Query to retrieve table names in the specified database
	tableNamesQuery := "SHOW TABLES"
	rows, err := db.Query(ctx, tableNamesQuery)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		// Query to retrieve the DDL of each table
		showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
		tableDDL, err := db.Query(ctx, showCreateTableQuery)
		if err != nil {
			log.Fatalf("Failed to get DDL for table %s: %v", tableName, err)
		}
		defer tableDDL.Close()

		if tableDDL.Next() {
			var ddl string
			if err := tableDDL.Scan(&ddl); err != nil {
				log.Fatalf("Failed to scan DDL of table %s: %v", tableName, err)
			}
			fmt.Printf("%s\n;\n----------------------------------------\n", ddl)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error during row iteration: %v", err)
	}
}
