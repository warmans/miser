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
		CREATE TABLE IF NOT EXISTS materialiser_log (
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
	retries := 10
	for {
		retries--
		if retries <= 0 {
			r.logger.Printf("Aggregates failed permanently")
			return
		}
		if err := r.setup(); err != nil {
			r.logger.Printf("Setup failed (%s). Retrying... ", err.Error())
			time.Sleep(time.Second * 6)
			continue
		}
		r.logger.Printf("Setup success\n")
		break //success
	}

	for {
		if err := r.runOnce(); err != nil {
			r.logger.Printf("runner encountered error: %s", err.Error())
		}
		time.Sleep(time.Minute)
	}
}

func (r *Runner) runOnce() error {

	started := time.Now()

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	for _, view := range r.views {

		tx, err := r.conn.Begin()
		if err != nil {
			return err
		}
		if err := r.Lock(tx); err != nil {
			return err
		}

		//find last run time for view
		var lastRun time.Time
		if err = tx.QueryRow("SELECT COALESCE(MAX(created), TIMESTAMP 'epoch') FROM agg_log WHERE view = $1", view.GetName()).Scan(&lastRun); err != nil {
			if err != sql.ErrNoRows {
				r.logger.Printf("Failed to find last run time: %s", err)
				if err = tx.Rollback(); err != nil {
					r.logger.Printf("Rollback also failed: %s", err)
				}
				continue
			}
		}
		if time.Since(lastRun) < view.GetUpdateInterval() {
			if err := tx.Commit(); rollbackErr != nil {
				r.logger.Printf("Failed commit empty transaction: %s", err)
			}
			continue //nothing to do
		}

		//run view setup
		if err := view.Setup(tx); err != nil {
			r.logger.Printf("Setup for %s failed: %s", view.GetName(), err.Error())
			if err = tx.Rollback(); err != nil {
				r.logger.Printf("Rollback also failed: %s", err)
			}
			continue
		}

		//run view update
		if err := view.Update(tx); err != nil {
			r.logger.Printf("Update for %s failed: %s", view.GetName(), err.Error())
			if err = tx.Rollback(); err != nil {
				r.logger.Printf("Rollback also failed: %s", err)
			}
			continue
		}

		//log completion stats
		_, err = tx.Exec("INSERT INTO agg_log (host, view, duration_sec) VALUES ($1, $2, $3)", hostname, view.GetName(), time.Since(started).Seconds())
		if err != nil {
			r.logger.Printf("Lock log create failed: %s", err)
			if err = tx.Rollback(); err != nil {
				r.logger.Printf("Rollback also failed: %s", err)
			}
			continue
		}

		//everything worked... try and commit the changes
		if err := tx.Commit(); err != nil {
			r.logger.Printf("Commit failed: %s", err)
		}
	}

	return nil
}

func (r *Runner) Lock(tx *sql.Tx) error {
	_, err := tx.Exec("LOCK TABLE agg_log IN ACCESS EXCLUSIVE MODE NOWAIT")
	if err != nil {
		return fmt.Errorf("Locking not aquired. Another process is probably already running: %s", err)
	}
	return err
}
