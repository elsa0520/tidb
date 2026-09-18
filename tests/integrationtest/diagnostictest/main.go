// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	normalPort     = flag.Int("normal-port", 4000, "normal TiDB MySQL port")
	diagnosticPort = flag.Int("diagnostic-port", 4001, "diagnostic TiDB MySQL port")
	etcdEndpoint   = flag.String("etcd-endpoint", "127.0.0.1:2379", "Etcd endpoint")
	keyspace       = flag.String("keyspace", "SYSTEM", "keyspace used by both TiDB nodes")
	importFile     = flag.String("import-file", "", "CSV file used by IMPORT INTO")
	legacyPort     = flag.String("port", "4000", "legacy single-node TiDB MySQL port")
	testPath       = flag.String("test", "", "legacy integration test input file")
	resultPath     = flag.String("result", "", "legacy integration test expected result file")
	record         = flag.Bool("record", false, "legacy record the actual result")
	serverInfoPath = "/tidb/server/info/"
	ddlTable       = "diagnostic_online_ddl"
	importTable    = "diagnostic_online_import"
	indexName      = "idx_value"
	checkTimeout   = 3 * time.Minute
	pollInterval   = 200 * time.Millisecond
)

type serverInfo struct {
	Port     uint   `json:"listening_port"`
	Keyspace string `json:"keyspace,omitempty"`
}

func main() {
	flag.Parse()
	if *testPath != "" || *resultPath != "" {
		runLegacyTest()
		return
	}
	if *importFile == "" {
		fatalf("-import-file is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	normal := openDB(*normalPort)
	defer normal.Close()
	diagnostic := openDB(*diagnosticPort)
	defer diagnostic.Close()

	if err := normal.PingContext(ctx); err != nil {
		fatalf("connect to normal TiDB: %v", err)
	}
	if err := diagnostic.PingContext(ctx); err != nil {
		fatalf("connect to diagnostic TiDB: %v", err)
	}

	waitFor(ctx, "normal TiDB serverinfo registration", func() (bool, error) {
		return serverInfoPresent(ctx)
	})
	runDDL(ctx, normal, diagnostic)
	runImport(ctx, normal)
	if !serverInfoPresentOrFatal(ctx) {
		fatalf("normal TiDB serverinfo disappeared from Etcd")
	}
}

type legacyStatement struct {
	sql           string
	expectedError uint16
}

func runLegacyTest() {
	if *testPath == "" || *resultPath == "" {
		fatalf("both -test and -result are required")
	}
	statements, err := readLegacyStatements(*testPath)
	if err != nil {
		fatalf("read test file: %v", err)
	}
	db := openDBString(*legacyPort)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fatalf("connect to TiDB: %v", err)
	}

	var actual strings.Builder
	for _, stmt := range statements {
		actual.WriteString(stmt.sql)
		actual.WriteByte('\n')
		_, err := db.ExecContext(context.Background(), stmt.sql)
		if stmt.expectedError == 0 {
			if err != nil {
				fatalf("execute %q: %v", stmt.sql, err)
			}
			continue
		}
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) {
			fatalf("execute %q: expected MySQL error %d, got %v", stmt.sql, stmt.expectedError, err)
		}
		if mysqlErr.Number != stmt.expectedError {
			fatalf("execute %q: expected MySQL error %d, got %d", stmt.sql, stmt.expectedError, mysqlErr.Number)
		}
		actual.WriteString(mysqlErr.Error())
		actual.WriteByte('\n')
	}
	if *record {
		if err := os.WriteFile(*resultPath, []byte(actual.String()), 0o644); err != nil {
			fatalf("record result file: %v", err)
		}
		return
	}
	expected, err := os.ReadFile(*resultPath)
	if err != nil {
		fatalf("read result file: %v", err)
	}
	if actual.String() != string(expected) {
		fatalf("result mismatch\nexpected:\n%s\nactual:\n%s", expected, actual.String())
	}
}

func openDBString(port string) *sql.DB {
	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%s)/test?timeout=5s&readTimeout=5s&writeTimeout=5s", port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fatalf("open TiDB connection on port %s: %v", port, err)
	}
	return db
}

func readLegacyStatements(path string) ([]legacyStatement, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var statements []legacyStatement
	var pendingError uint16
	var statementText strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "-- error ") {
			code, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "-- error ")), 10, 16)
			if err != nil {
				return nil, fmt.Errorf("parse expected error %q: %w", line, err)
			}
			pendingError = uint16(code)
			continue
		}
		if statementText.Len() > 0 {
			statementText.WriteByte(' ')
		}
		statementText.WriteString(line)
		if !strings.HasSuffix(line, ";") {
			continue
		}
		statements = append(statements, legacyStatement{sql: statementText.String(), expectedError: pendingError})
		statementText.Reset()
		pendingError = 0
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if statementText.Len() != 0 {
		return nil, errors.New("unterminated SQL statement")
	}
	if len(statements) == 0 {
		return nil, errors.New("test file contains no SQL statements")
	}
	return statements, nil
}

func openDB(port int) *sql.DB {
	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%d)/test?timeout=5s&readTimeout=180s&writeTimeout=5s&interpolateParams=true", port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fatalf("open TiDB connection on port %d: %v", port, err)
	}
	return db
}

func runDDL(ctx context.Context, normal, diagnostic *sql.DB) {
	for _, query := range []string{
		"DROP TABLE IF EXISTS " + ddlTable,
		"CREATE TABLE " + ddlTable + " (id INT PRIMARY KEY, value INT)",
		"INSERT INTO " + ddlTable + " VALUES (1, 10), (2, 20)",
		"ALTER TABLE " + ddlTable + " ADD INDEX " + indexName + " (value)",
	} {
		if _, err := normal.ExecContext(ctx, query); err != nil {
			fatalf("execute on normal TiDB %q: %v", query, err)
		}
	}

	waitFor(ctx, "diagnostic schema synchronization", func() (bool, error) {
		var name, definition string
		err := diagnostic.QueryRowContext(ctx, "SHOW CREATE TABLE "+ddlTable).Scan(&name, &definition)
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 {
			return false, nil // The diagnostic schema reload may not have seen CREATE yet.
		}
		return strings.Contains(definition, "KEY `"+indexName+"` (`value`)"), err
	})
}

func runImport(ctx context.Context, normal *sql.DB) {
	for _, query := range []string{
		"DROP TABLE IF EXISTS " + importTable,
		"CREATE TABLE " + importTable + " (id INT PRIMARY KEY, value INT)",
	} {
		if _, err := normal.ExecContext(ctx, query); err != nil {
			fatalf("execute on normal TiDB %q: %v", query, err)
		}
	}

	path := strings.ReplaceAll(filepath.ToSlash(*importFile), "'", "''")
	query := fmt.Sprintf("IMPORT INTO %s FROM '%s' WITH thread=1", importTable, path)
	if _, err := normal.ExecContext(ctx, query); err != nil {
		fatalf("execute IMPORT INTO on normal TiDB: %v", err)
	}

	var count int
	if err := normal.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+importTable).Scan(&count); err != nil {
		fatalf("query imported data on normal TiDB: %v", err)
	}
	if count != 3 {
		fatalf("normal TiDB imported %d rows, want 3", count)
	}
}

func serverInfoPresent(ctx context.Context) (bool, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{*etcdEndpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		return false, err
	}
	defer client.Close()

	response, err := client.Get(ctx, serverInfoPath, clientv3.WithPrefix())
	if err != nil {
		return false, err
	}
	normalFound := false
	for _, kv := range response.Kvs {
		var info serverInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			return false, fmt.Errorf("decode TiDB serverinfo %q: %w", kv.Key, err)
		}
		if info.Port == uint(*normalPort) && (info.Keyspace == "" || strings.EqualFold(info.Keyspace, *keyspace)) {
			normalFound = true
		}
		if info.Port == uint(*diagnosticPort) {
			return false, fmt.Errorf("diagnostic TiDB serverinfo unexpectedly exists: %s", kv.Key)
		}
	}
	return normalFound, nil
}

func serverInfoPresentOrFatal(ctx context.Context) bool {
	present, err := serverInfoPresent(ctx)
	if err != nil {
		fatalf("check TiDB serverinfo: %v", err)
	}
	return present
}

func waitFor(ctx context.Context, description string, condition func() (bool, error)) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		ok, err := condition()
		if err != nil {
			fatalf("check %s: %v", description, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
