package miser

import (
	"time"
	"database/sql"
	"fmt"
	"strings"
)

// Tracker defines how to get the offsets for the source/dest tables. I.e. you need to know where to start reading in the
// source table based on the last record in dest.
type Tracker interface {
	GetOffsets(conn *sql.DB) (*Offsets, error)
}

// Offsets are sql snippets used in update queries
type Offsets struct {
	SourceOffsetSQL      string
	DestinationOffsetSQL string
}

// DateTracker implements Tracker for timeseries-like data.
type DateTracker struct {
	SourceTableName       string
	SourceDateColumn      string
	DestinationTableName  string
	DestinationDateColumn string
	Backtrack             time.Duration
}

func (t *DateTracker) GetOffsets(conn *sql.DB) (*Offsets, error) {
	var lastRowTime time.Time
	err := conn.QueryRow("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", t.DestinationDateColumn, t.DestinationTableName, t.DestinationDateColumn).Scan(&lastRowTime)
	if err != nil {
		return nil, err
	}
	off := &Offsets{
		SourceOffsetSQL:      fmt.Sprintf("%s >= '%s'", t.SourceDateColumn, lastRowTime.Add(time.Duration(0) - t.Backtrack).Format(time.RFC3339)),
		DestinationOffsetSQL: fmt.Sprintf("%s >= '%s'", t.DestinationDateColumn, lastRowTime.Add(time.Duration(0) - t.Backtrack).Format(time.RFC3339)),
	}
	return off, nil
}

//View represents the materialised view
type View struct {
	Name            string
	Columns         []string
	Indexes         []string
	TrackBy         Tracker
	Dimensions      []string
	Metrics         []string
	SourceTableName string
}

func (v *View) Setup() {
	//todo
}

func (v *View) Update(conn *sql.DB) error {

	offsets, err := v.TrackBy.GetOffsets(conn)
	if err != nil {
		return err
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	deleteSQL := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		v.Name,
		offsets.DestinationOffsetSQL,
	)
	_, err = tx.Exec(deleteSQL)
	if err != nil {
		if rollbackErr := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback error: %s (rollbacked triggered by error: %s)", rollbackErr, err)
		}
	}

	updateSQL := fmt.Sprintf(
		"INSERT INTO %s SELECT %s FROM %s WHERE %s",
		v.Name,
		strings.Join(append(v.Dimensions, v.Metrics...), ", "),
		v.SourceTableName,
		offsets.SourceOffsetSQL,
	)
	_, err = tx.Exec(updateSQL)
	if err != nil {
		if rollbackErr := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback error: %s (rollbacked triggered by error: %s)", rollbackErr, err)
		}
	}

	return tx.Commit()
}
