package miser

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

var log LogReceiver = &NoopLogReceiver{}

func ReceiveLogs(receiver LogReceiver) {
	log = receiver
}

func NewRunner(conn *sql.DB, views []View) *Runner {
	return &Runner{conn: conn, views: views, stats: &RunnerStats{Views: make([]ViewStats, 0)}}
}

type Runner struct {
	conn  *sql.DB
	views []View
	stats *RunnerStats
}

type Metadata struct {
	ID          int64
	Created     time.Time
	Host        string
	View        string
	Version     string
	DurationSec float64
}

func (r *Runner) Setup() error {

	_, err := r.conn.Exec(`
		CREATE TABLE IF NOT EXISTS materialiser_log (
			id           SERIAL PRIMARY KEY,
			created      TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'utc'),
			host         TEXT,
			view         TEXT,
			version      TEXT,
			duration_sec DECIMAL
		)
	`)
	if err != nil {
		return err
	}

	//setup initial empty views as soon as possible
	for _, v := range r.views {
		if err := v.Create(r.conn); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) Run() {
	//setup basic stuff needed to run
	retries := 10
	for {
		retries--
		if retries <= 0 {
			log.Logf("Aggregates failed permanently")
			return
		}
		if err := r.Setup(); err != nil {
			log.Logf("Setup failed (%s). Retrying... ", err.Error())
			time.Sleep(time.Second * 6)
			continue
		}
		log.Logf("Setup success")
		break //success
	}

	for {
		if err := r.runOnce(); err != nil {
			log.Logf("runner encountered error: %s", err.Error())
		}
		time.Sleep(time.Minute)
	}
}

func (r *Runner) runOnce() error {

	runStartTime := time.Now()

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	viewStats := []ViewStats{}

	for _, view := range r.views {

		started := time.Now()
		log.Debugf("started view %s at %s", view.GetName(), started.Format(time.RFC3339))

		tx, err := r.conn.Begin()
		if err != nil {
			return err
		}
		if err := r.Lock(tx); err != nil {
			r.stats.LockFailures++
			return err
		}

		metadata, err := GetLastUpdateMeta(view.GetName(), tx)
		if err != nil && err != sql.ErrNoRows {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("roll back failed with %s. original error: %s", rollbackErr, err)
			}
			return err
		}

		//if the last version id different from the current one the view must be replaced
		replace := metadata.Version != view.GetVersion()

		if !replace && time.Since(metadata.Created) < view.GetUpdateInterval() {
			if err := tx.Commit(); err != nil {
				log.Logf("Failed commit empty transaction: %s", err)
			}
			continue //nothing to do
		}

		if replace {
			log.Logf("Replace triggered for view: %s (old: %s, new: %s)", view.GetName(), metadata.Version, view.GetVersion())
			r.stats.TableReplacements++
		}

		//run view update
		if err := view.Update(tx, replace); err != nil {
			log.Logf("Update for %s failed: %s", view.GetName(), err.Error())
			if err = tx.Rollback(); err != nil {
				log.Logf("Rollback also failed: %s", err)
			}
			continue
		}

		//log completion stats
		_, err = tx.Exec("INSERT INTO materialiser_log (host, view, version, duration_sec) VALUES ($1, $2, $3, $4)", hostname, view.GetName(), view.GetVersion(), time.Since(started).Seconds())
		if err != nil {
			log.Logf("Lock log create failed: %s", err)
			if err = tx.Rollback(); err != nil {
				log.Logf("Rollback also failed: %s", err)
			}
			continue
		}

		//check the final size of the table
		var viewRowCount int64
		if err := tx.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", view.GetName())).Scan(&viewRowCount); err != nil {
			log.Logf("Table Count: %s", err)
			if err = tx.Rollback(); err != nil {
				log.Logf("Rollback also failed: %s", err)
			}
			continue
		}

		//everything worked... try and commit the changes
		if err := tx.Commit(); err != nil {
			log.Logf("Commit failed: %s", err)
			continue
		}

		//add the stats
		viewStats = append(viewStats, ViewStats{Name: view.GetName(), TableRowsTotal: viewRowCount, LastUpdateSeconds: time.Since(started).Seconds()})
	}

	r.stats.Views = viewStats
	r.stats.LastRunSeconds = time.Since(runStartTime).Seconds()

	return nil
}

func (r *Runner) Lock(tx *sql.Tx) error {
	_, err := tx.Exec("LOCK TABLE materialiser_log IN ACCESS EXCLUSIVE MODE NOWAIT")
	if err != nil {
		return fmt.Errorf("Locking not aquired. Another process is probably already running: %s", err)
	}
	return err
}

func (r *Runner) GetStats() RunnerStats {
	return *r.stats
}

type RunnerStats struct {
	LastRunSeconds    float64      `json:"last_run_seconds"`
	LockFailures      int64        `json:"lock_failures"`
	TableReplacements int64        `json:"table_replacements"`
	Views             []ViewStats  `json:"view_stats"`
}

type ViewStats struct {
	Name              string  `json:"name"`
	TableRowsTotal    int64   `json:"table_rows_total"`
	LastUpdateSeconds float64 `json:"last_update_seconds"`
}

type Msg struct {
	Msg   string
	Debug bool
}

type LogReceiver interface {
	Logf(msg string, args ... interface{})
	Debugf(msg string, args ... interface{})
}

type NoopLogReceiver struct {
}

func (r *NoopLogReceiver) Logf(msg string, args ... interface{}) {
}

func (r *NoopLogReceiver) Debugf(msg string, args ... interface{}) {
}

func NewChanLogReceiver() *ChanLogReceiver {
	return &ChanLogReceiver{logs: make(chan *Msg, 1000)}
}

type ChanLogReceiver struct {
	logs chan *Msg
}

func (r *ChanLogReceiver) Logf(msg string, args ... interface{}) {
	r.logs <- &Msg{Msg: fmt.Sprintf(msg, args...), Debug: false}
}

func (r *ChanLogReceiver) Debugf(msg string, args ... interface{}) {
	r.logs <- &Msg{Msg: fmt.Sprintf(msg, args...), Debug: true}
}

func (r *ChanLogReceiver) Logs() chan *Msg {
	return r.logs
}
