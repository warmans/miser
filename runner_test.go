package miser

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

var words = []string{"foo", "bar", "baz"}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

func randWord() string {
	idx := rand.Intn(len(words) - 1)
	return words[idx]
}

func randInt() int64 {
	return rand.Int63n(100)
}

func randFloat() float64 {
	return rand.Float64()
}

func mustSetupTestDB(t *testing.T, conn *sql.DB) {

	if _, err := conn.Exec(`DROP TABLE IF EXISTS test_data_1`); err != nil {
		t.Fatalf("failed test cleanup: %s", err.Error())
		return
	}

	_, err := conn.Exec(`
		CREATE TABLE test_data_1 (
			ts TIMESTAMP,
			key1 TEXT,
			key2 TEXT,
			key3 TEXT,
			val1 INTEGER,
			val2 DECIMAL
		)
	`)
	if err != nil {
		t.Fatalf("create test table failed: %s", err.Error())
	}

	for i := 0; i < 50; i++ {
		ts := time.Now().Add(0 - ((time.Duration(i) * 10) * time.Minute))
		_, err := conn.Exec(
			"INSERT INTO test_data_1 (ts, key1, key2, key3, val1, val2) VALUES ($1, $2, $3, $4, $5, $6)",
			ts.Format(time.RFC3339),
			randWord(),
			randWord(),
			randWord(),
			randInt(),
			randFloat(),
		)
		if err != nil {
			t.Fatalf("test table insert failed: %s", err.Error())
		}
	}
}

func mustSetupTest(t *testing.T) *sql.DB {

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Fatal("No DB_DSN was provided")
		return nil
	}

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Error opening DB connection: %s", err.Error())
		return nil
	}

	mustSetupTestDB(t, conn)

	return conn
}

func mustGetColumnSum(t *testing.T, conn *sql.DB, table string, column string) int64 {
	var num int64
	err := conn.QueryRow(fmt.Sprintf("SELECT SUM(%s) FROM %s", column, table)).Scan(&num)
	if err != nil {
		t.Fatalf("panic querying for column sum: %s", err.Error())
	}
	return num
}


func mustGetRowCount(t *testing.T, conn *sql.DB, table string) int64 {
	var num int64
	err := conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&num)
	if err != nil {
		t.Fatalf("panic querying for row count: %s", err.Error())
	}
	return num
}

func TestRunnerCleanE2E(t *testing.T) {

	if os.Getenv("E2E") == "" {
		t.Skip("e2e tests not enabled")
		return
	}
	conn := mustSetupTest(t)

	if _, err := conn.Exec(`DROP TABLE IF EXISTS test_view_1`); err != nil {
		t.Fatalf("failed test cleanup: %s", err.Error())
		return
	}

	if _, err := conn.Exec(`DROP TABLE IF EXISTS materialiser_log`); err != nil {
		t.Fatalf("failed test cleanup: %s", err.Error())
		return
	}

	runner := NewRunner(
		conn,
		[]View{
			&TimeseriesView{
				Name: "test_view_1",
				SourceTableSpec: "test_data_1",
				UpdateInterval: time.Minute,
				Columns: []*StandardViewColumn{
					{CreateSpec: "ts TIMESTAMP", SelectSpec: "date_trunc('hour', ts)", IsKey: true},
					{CreateSpec: "key1 TEXT", SelectSpec: "key1", IsKey: true},
					{CreateSpec: "key2 TEXT", SelectSpec: "key2", IsKey: true},
					//omit 1 key col
					{CreateSpec: "val1 INTEGER", SelectSpec: "SUM(val1)", IsKey: false},
					{CreateSpec: "val2 DECIMAL", SelectSpec: "SUM(val2)", IsKey: false},
					//add one value col
					{CreateSpec: "val3 DECIMAL", SelectSpec: "SUM(val1::DECIMAL * val2)", IsKey: false},
				},
				TrackBy: &DateTracker{SourceDateColumn: "ts", DestinationDateColumn: "ts", Backtrack: time.Hour},
			},
		},
	)

	if err := runner.Setup(); err != nil {
		t.Errorf("Runner setup failed: %s", err)
		return
	}

	if err := runner.runOnce(); err != nil {
		t.Errorf("Runner failed with error: %s", err)
		return
	}

	//with random values cannot know if any aggregation happened
	t.Logf(
		"view created with %d rows (vs original %d rows)",
		mustGetRowCount(t, conn, "test_view_1"),
		mustGetRowCount(t, conn, "test_data_1"),
	)

	baseTableVal1Sum := mustGetColumnSum(t, conn, "test_data_1", "val1")
	aggTableVal1Sum := mustGetColumnSum(t, conn, "test_view_1", "val1")
	if baseTableVal1Sum != aggTableVal1Sum {
		t.Errorf("val1 should have the same sum in base and aggregate tables. Actually base was %d and aggregate was %d", baseTableVal1Sum, aggTableVal1Sum)
		return
	}

	t.Logf("Dumping stats...")
	json.NewEncoder(os.Stdout).Encode(runner.GetStats())
}

func TestRunnerIterateE2E(t *testing.T) {

	if os.Getenv("E2E") == "" {
		t.Skip("e2e tests not enabled")
		return
	}
	conn := mustSetupTest(t)

	runner := NewRunner(
		conn,
		[]View{
			&TimeseriesView{
				Name: "test_view_2",
				SourceTableSpec: "test_data_1",
				UpdateInterval: time.Millisecond,
				Columns: []*StandardViewColumn{
					{CreateSpec: "ts TIMESTAMP", SelectSpec: "date_trunc('hour', ts)", IsKey: true},
					{CreateSpec: "key1 TEXT", SelectSpec: "key1", IsKey: true},
					{CreateSpec: "key2 TEXT", SelectSpec: "key2", IsKey: true},
					//omit 1 key col
					{CreateSpec: "val1 INTEGER", SelectSpec: "SUM(val1)", IsKey: false},
					{CreateSpec: "val2 DECIMAL", SelectSpec: "SUM(val2)", IsKey: false},
					//add one value col
					{CreateSpec: "val3 DECIMAL", SelectSpec: "SUM(val1::DECIMAL * val2)", IsKey: false},
				},
				TrackBy: &DateTracker{SourceDateColumn: "ts", DestinationDateColumn: "ts", Backtrack: time.Hour},
				Indexes: []string{
					"ts, key1, key2",
				},
			},
		},
	)

	if err := runner.Setup(); err != nil {
		t.Errorf("Runner setup failed: %s", err)
		return
	}

	for i:=0; i< 10; i++ {

		if err := runner.runOnce(); err != nil {
			t.Errorf("Runner failed with error: %s", err)
			return
		}

		//with random values cannot know if any aggregation happened
		t.Logf(
			"view created with %d rows (vs original %d rows)",
			mustGetRowCount(t, conn, "test_view_2"),
			mustGetRowCount(t, conn, "test_data_1"),
		)

		aggTableVal1Sum := mustGetColumnSum(t, conn, "test_view_2", "val1")
		baseTableVal1Sum := mustGetColumnSum(t, conn, "test_data_1", "val1")
		if baseTableVal1Sum != aggTableVal1Sum {
			t.Errorf("val1 should have the same sum in base and aggregate tables. Actually base was %d and aggregate was %d", baseTableVal1Sum, aggTableVal1Sum)
			return
		}

		time.Sleep(time.Millisecond * 5)
	}

	t.Logf("Dumping stats...")
	json.NewEncoder(os.Stdout).Encode(runner.GetStats())
}
