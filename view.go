package miser

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Tracker defines how to get the offsets for the source/dest tables. I.e. you need to know where to start reading in the
// source table based on the last record in dest.
type Tracker interface {
	GetOffsets(destinationTableName string, tx *sql.Tx) (*Offsets, error)
}

// Offsets are sql snippets used in update queries
type Offsets struct {
	SourceOffsetSQL      string
	DestinationOffsetSQL string
}

// DateTracker implements Tracker for timeseries-like data.
type DateTracker struct {
	SourceDateColumn      string
	DestinationDateColumn string
	Backtrack             time.Duration
}

func (t *DateTracker) GetOffsets(destinationTableName string, tx *sql.Tx) (*Offsets, error) {
	lastRowTime := time.Time{}
	err := tx.QueryRow(fmt.Sprintf("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", t.DestinationDateColumn, destinationTableName, t.DestinationDateColumn)).Scan(&lastRowTime)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		}
	}
	//if a last value was not found don't backtrack at all
	backtrackedTime := lastRowTime
	if !lastRowTime.IsZero() {
		backtrackedTime = lastRowTime.Add(time.Duration(0) - t.Backtrack)
	}

	off := &Offsets{
		SourceOffsetSQL:      fmt.Sprintf("%s >= '%s'", t.SourceDateColumn, backtrackedTime.Format(time.RFC3339)),
		DestinationOffsetSQL: fmt.Sprintf("%s >= '%s'", t.DestinationDateColumn, backtrackedTime.Format(time.RFC3339)),
	}
	return off, nil
}

type View interface {
	GetName() string
	Setup(tx *sql.Tx) error
	Update(tx *sql.Tx) error
	GetUpdateInterval() time.Duration
}

type StandardViewColumn struct {
	CreateSpec string
	SelectSpec string
	IsKey      bool
}

//StandardView represents the materialised view capable of doing incremental updates
type StandardView struct {
	Name            string
	SourceTableName string
	UpdateInterval  time.Duration
	Columns         []*StandardViewColumn
	Indexes         []string
	TrackBy         Tracker
}

func (v *StandardView) GetName() string {
	return v.Name
}

func (v *StandardView) GetUpdateInterval() time.Duration {
	return v.UpdateInterval
}

func (v *StandardView) Setup(tx *sql.Tx) error {

	//setup table
	_, err := tx.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, v.Name, strings.Join(v.getCreateColumns(), ", ")))
	if err != nil {
		return fmt.Errorf("Create failed: %s", err)
	}

	//pass though index SQL directly
	for _, index := range v.Indexes {
		_, err := tx.Exec(index)
		if err != nil {
			return fmt.Errorf("Index (%s) failed: %s", index, err)
		}
	}
	return nil
}

func (v *StandardView) Update(tx *sql.Tx) error {

	offsets, err := v.TrackBy.GetOffsets(v.Name, tx)
	if err != nil {
		return err
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s", v.Name, offsets.DestinationOffsetSQL)
	_, err = tx.Exec(deleteSQL)
	if err != nil {
		if rollbackErr := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback error: %s (rollbacked triggered by error: %s)", rollbackErr, err)
		}
	}

	updateSQL := fmt.Sprintf(
		"INSERT INTO %s SELECT %s FROM %s WHERE %s GROUP BY %s",
		v.Name,
		strings.Join(v.getSelectColumns(), ", "),
		v.SourceTableName,
		offsets.SourceOffsetSQL,
		strings.Join(v.getGroupColumns(), ", "),
	)
	_, err = tx.Exec(updateSQL)
	if err != nil {
		if rollbackErr := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback error: %s (rollbacked triggered by error: %s)", rollbackErr, err)
		}
	}
	return nil
}

func (v *StandardView) getCreateColumns() []string {
	columns := make([]string, 0)
	for _, col := range v.Columns {
		columns = append(columns, col.CreateSpec)
	}
	return columns
}

func (v *StandardView) getSelectColumns() []string {
	columns := make([]string, len(v.Columns))
	for k, col := range v.Columns {
		columns[k] = col.SelectSpec
	}
	return columns
}

func (v *StandardView) getGroupColumns() []string {
	columns := make([]string, 0)
	for _, col := range v.Columns {
		if col.IsKey {
			columns = append(columns, col.SelectSpec)
		}
	}
	return columns
}

// SQLView represents a simplified materialised view without incremental updates but more freedom in SELECT
// used to populate table.
type SQLView struct {
	Table          *TableSpec
	DataSelectSQL  string
	UpdateInterval time.Duration
}

func (v *SQLView) GetName() string {
	return v.Table.Name
}

func (v *SQLView) Setup(tx *sql.Tx) error {
	return nil
}

func (v *SQLView) Update(tx *sql.Tx) error {

	//create the temp table
	tempName := fmt.Sprintf(`%s_temp`, v.Table.Name)

	dropTempStmnt := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempName)
	if _, err := tx.Exec(dropTempStmnt); err != nil {
		return fmt.Errorf("drop temp table failed: %s (%s)", err, dropTempStmnt)
	}

	createStmnt := fmt.Sprintf("CREATE TABLE %s %s", tempName, v.Table.Spec)
	if _, err := tx.Exec(createStmnt); err != nil {
		return fmt.Errorf("create table failed: %s (%s)", err, createStmnt)
	}

	//add the indexes
	for k, indexSpec := range v.Table.Indexes {
		indexName := fmt.Sprintf("idx_%s_%d_temp", v.Table.Name, k)
		indexStmnt := fmt.Sprintf("DROP INDEX IF EXISTS %s; CREATE INDEX %s ON %s %s", indexName, indexName, tempName, indexSpec)
		if _, err := tx.Exec(indexStmnt); err != nil {
			return fmt.Errorf("create index %d failed: %s (%s)", k, err, indexStmnt)
		}
	}

	//create the data
	insertStmnt := fmt.Sprintf("INSERT INTO %s %s", tempName, v.DataSelectSQL)
	if _, err := tx.Exec(insertStmnt); err != nil {
		return fmt.Errorf("inserts failed: %s (%s)", err, insertStmnt)
	}

	//remove old table
	dropStmnt := fmt.Sprintf("DROP TABLE IF EXISTS %s", v.Table.Name)
	if _, err := tx.Exec(dropStmnt); err != nil {
		return fmt.Errorf("drop table failed: %s (%s)", err, dropStmnt)
	}

	//replace table
	alterTableStmnt := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempName, v.Table.Name)
	if _, err := tx.Exec(alterTableStmnt); err != nil {
		return fmt.Errorf("rename table failed: %s (%s)", err, alterTableStmnt)
	}

	//replace indxes
	for k := range v.Table.Indexes {
		indexName := fmt.Sprintf("idx_%s_%d", v.Table.Name, k)
		alterIndexStmnt := fmt.Sprintf("DROP INDEX IF EXISTS %s; ALTER INDEX %s_temp RENAME TO %s", indexName, indexName, indexName)
		if _, err := tx.Exec(alterIndexStmnt); err != nil {
			return fmt.Errorf("rename table failed: %s (%s)", err, alterIndexStmnt)
		}
	}
	return nil
}

func (v *SQLView) GetUpdateInterval() time.Duration {
	return v.UpdateInterval
}

type TableSpec struct {
	Name    string
	Spec    string
	Indexes []string
}
