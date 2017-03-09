package miser

import (
	"crypto/md5"
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
	GetVersion() (string)
	GetUpdateInterval() time.Duration
	Update(tx *sql.Tx, replace bool) error
}

type StandardViewColumn struct {
	CreateSpec string
	SelectSpec string
	IsKey      bool
}

//TimeseriesView represents the materialised view capable of doing incremental updates
type TimeseriesView struct {
	Name            string
	SourceTableName string
	UpdateInterval  time.Duration
	Columns         []*StandardViewColumn
	Indexes         []string
	TrackBy         Tracker
}

func (v *TimeseriesView) GetName() string {
	return v.Name
}

func (v *TimeseriesView) GetVersion() string {
	//offsets and table names intentionally blank
	viewDefinition := strings.Replace(v.getCreateTableSQL("") + v.getInsertSQL("", &Offsets{}), " ", "", -1)
	return fmt.Sprintf("%x", md5.Sum([]byte(viewDefinition)))
}

func (v *TimeseriesView) GetUpdateInterval() time.Duration {
	return v.UpdateInterval
}

func (v *TimeseriesView) Update(tx *sql.Tx, replace bool) error {

	var targetTable string
	if replace {
		targetTable = v.Name + "_temp"
	} else {
		targetTable = v.Name
	}

	if err := v.setupTable(targetTable, tx); err != nil {
		return fmt.Errorf("failed to create table: %s", err)
	}

	if !replace {
		if err := v.setupTableIndexes(targetTable, replace, tx); err != nil {
			return fmt.Errorf("failed to create index: %s", err)
		}
	}

	if err :=  v.updateTable(targetTable, tx); err != nil {
		return fmt.Errorf("failed to update table: %s", err)
	}

	if !replace {
		return nil //nothing more to do
	}

	//this is a replace so we need to finish up by moving the new table into place

	//remove old table
	dropStmnt := fmt.Sprintf("DROP TABLE IF EXISTS %s", v.Name)
	if _, err := tx.Exec(dropStmnt); err != nil {
		return fmt.Errorf("drop temp table failed: %s (%s)", err, dropStmnt)
	}

	//replace table
	alterTableStmnt := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", targetTable, v.Name)
	if _, err := tx.Exec(alterTableStmnt); err != nil {
		return fmt.Errorf("rename temp table failed: %s (%s)", err, alterTableStmnt)
	}

	//create indexes (indexes do not move with a rename so they have to be created last)
	if err := v.setupTableIndexes(v.Name, replace, tx); err != nil {
		return fmt.Errorf("failed to create index: %s", err)
	}

	return nil
}

func (v *TimeseriesView) updateTable(table string, tx *sql.Tx) error {

	offsets, err := v.TrackBy.GetOffsets(table, tx)
	if err != nil {
		return err
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s", table, offsets.DestinationOffsetSQL)
	_, err = tx.Exec(deleteSQL)
	if err != nil {
		return err
	}

	if _, err = tx.Exec(v.getInsertSQL(table, offsets)); err != nil {
		return err
	}

	return nil
}


func (v *TimeseriesView) setupTable(table string, tx *sql.Tx) error {

	//setup table
	_, err := tx.Exec(v.getCreateTableSQL(table))
	if err != nil {
		return err
	}

	return nil
}

func (v *TimeseriesView) setupTableIndexes(table string, replace bool, tx *sql.Tx) error {
	//pass though index create SQL directly
	for k, index := range v.Indexes {
		indexName := fmt.Sprintf("%s_%d_idx", table, k)
		if replace {
			_, err := tx.Exec(fmt.Sprintf("DROP INDEX IF EXISTS %s; ", indexName))
			if err != nil {
				return fmt.Errorf("Index (%s) failed: %s", index, err)
			}
		}
		_, err := tx.Exec(fmt.Sprintf("CREATE INDEX %s ON %s (%s)", indexName, table, index))
		if err != nil {
			return fmt.Errorf("Index (%s) failed: %s", index, err)
		}
	}
	return nil
}

func (v *TimeseriesView) getInsertSQL(table string, offsets *Offsets) string {
	return fmt.Sprintf(
		"INSERT INTO %s SELECT %s FROM %s WHERE %s GROUP BY %s",
		table,
		strings.Join(v.getSelectColumns(), ", "),
		v.SourceTableName,
		offsets.SourceOffsetSQL,
		strings.Join(v.getGroupColumns(), ", "),
	)
}

func (v *TimeseriesView) getCreateTableSQL(table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, table, strings.Join(v.getCreateColumns(), ", "))
}

func (v *TimeseriesView) getCreateColumns() []string {
	columns := make([]string, 0)
	for _, col := range v.Columns {
		columns = append(columns, col.CreateSpec)
	}
	return columns
}

func (v *TimeseriesView) getSelectColumns() []string {
	columns := make([]string, len(v.Columns))
	for k, col := range v.Columns {
		columns[k] = col.SelectSpec
	}
	return columns
}

func (v *TimeseriesView) getGroupColumns() []string {
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

func (v *SQLView) GetVersion() string {
	return "na" //no point to versions when it always replaces anyway
}

func (v *SQLView) Update(tx *sql.Tx, replace bool) error {

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
		indexStmnt := fmt.Sprintf("DROP INDEX IF EXISTS %s; CREATE INDEX %s ON %s (%s)", indexName, indexName, tempName, indexSpec)
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

	//replace indexes
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
