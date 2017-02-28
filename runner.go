package miser

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
)

func NewRunner(logger *log.Logger, conn *sql.DB, views []View) *Runner {
	return &Runner{logger: logger, conn: conn, views: views}
}

type Runner struct {
	logger *log.Logger
	conn   *sql.DB
	views  []View
}

func (r *Runner) setup() error {
	_, err := r.conn.Exec(`
		CREATE TABLE IF NOT EXISTS agg_log (
			id           SERIAL PRIMARY KEY,
			created      TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'utc'),
			host         TEXT,
			view         TEXT,
			duration_sec DECIMAL
		)
	`)
	return err
}

func (r *Runner) Run() {
	//setup basic stuff needed to run
	if err := r.setup(); err != nil {
		r.logger.Printf("Setup failed. Aggregator must exit: %s", err.Error())
		return
	}
	for {
		if err := r.runOnce(); err != nil {
			r.logger.Printf("Run failed: %s", err.Error())
		}
		r.logger.Printf("All aggregates completed OK")
		time.Sleep(time.Minute)
	}
}

func (r *Runner) runOnce() error {

	started := time.Now()

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	tx, err := r.conn.Begin()
	if err != nil {
		return err
	}
	if err := r.Lock(tx); err != nil {
		return err
	}

	for _, view := range r.views {

		//log a record of the aggregate
		var lastRun time.Time
		if err = tx.QueryRow("SELECT COALESCE(MAX(created), TIMESTAMP 'epoch') FROM agg_log WHERE view = $1", view.GetName()).Scan(&lastRun); err != nil {
			if err != sql.ErrNoRows {
				return fmt.Errorf("Lock log create failed: %s", err)
			}
		}
		if time.Since(lastRun) < view.GetUpdateInterval() {
			continue //nothing to do
		}

		if err := view.Setup(tx); err != nil {
			r.logger.Printf("Setup for %s failed: %s", view.GetName(), err.Error())
			continue
		}

		if err := view.Update(tx); err != nil {
			r.logger.Printf("Update for %s failed: %s", view.GetName(), err.Error())
			continue
		}

		//log a record of the aggregate
		_, err = tx.Exec("INSERT INTO agg_log (host, view, duration_sec) VALUES ($1, $2, $3)", hostname, view.GetName(), time.Since(started).Seconds())
		if err != nil {
			return fmt.Errorf("Lock log create failed: %s", err)
		}
	}
	return tx.Commit()
}

func (r *Runner) Lock(tx *sql.Tx) error {
	_, err := tx.Exec("LOCK TABLE agg_log IN ACCESS EXCLUSIVE MODE NOWAIT")
	if err != nil {
		return fmt.Errorf("Locking not aquired. Another process is probably already running: %s", err)
	}
	return err
}
