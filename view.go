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

//View represents the materialised view
type StandardView struct {
	Name            string
	SourceTableName string
	UpdateInterval  time.Duration
	Columns         []string
	Indexes         []string
	TrackBy         Tracker
	Dimensions      []string
	Metrics         []string
}

func (v *StandardView) GetName() string {
	return v.Name
}

func (v *StandardView) GetUpdateInterval() time.Duration {
	return v.UpdateInterval
}

func (v *StandardView) Setup(tx *sql.Tx) error {

	//setup table
	_, err := tx.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, v.Name, strings.Join(v.Columns, ", ")))
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
		strings.Join(append(v.Dimensions, v.Metrics...), ", "),
		v.SourceTableName,
		offsets.SourceOffsetSQL,
		strings.Join(v.Dimensions, ", "),
	)
	_, err = tx.Exec(updateSQL)
	if err != nil {
		if rollbackErr := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback error: %s (rollbacked triggered by error: %s)", rollbackErr, err)
		}
	}

	return nil
}
