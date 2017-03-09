package miser

import (
	"testing"
	"time"
)

func TestTimeseriesView_GetVersionNumCols(t *testing.T) {

	t1 := &TimeseriesView{
		Name: "test_view_1",
		SourceTableName: "test_data_1",
		UpdateInterval: time.Minute,
		Columns: []*StandardViewColumn{
			{CreateSpec: "ts TEXT", SelectSpec: "date_trunc('hour', ts)", IsKey: true},
			{CreateSpec: "key1 TEXT", SelectSpec: "key1", IsKey: true},
			{CreateSpec: "key2 TEXT", SelectSpec: "key2", IsKey: true},
			//omit 1 key col
			{CreateSpec: "val1 INTEGER", SelectSpec: "SUM(val1)", IsKey: false},
			//add one value col
			{CreateSpec: "val3 DECIMAL", SelectSpec: "SUM(val1::DECIMAL * val2)", IsKey: false},
		},
		TrackBy: &DateTracker{SourceDateColumn: "ts", DestinationDateColumn: "ts", Backtrack: time.Hour},
	}

	t2 := &TimeseriesView{
		Name: "test_view_1",
		SourceTableName: "test_data_1",
		UpdateInterval: time.Minute,
		Columns: []*StandardViewColumn{
			{CreateSpec: "ts TEXT", SelectSpec: "date_trunc('hour', ts)", IsKey: true},
			{CreateSpec: "key1 TEXT", SelectSpec: "key1", IsKey: true},
			{CreateSpec: "key2 TEXT", SelectSpec: "key2", IsKey: true},
			//omit 1 key col
			{CreateSpec: "val1 INTEGER", SelectSpec: "SUM(val1)", IsKey: false},
			{CreateSpec: "val2 DECIMAL", SelectSpec: "SUM(val2)", IsKey: false},
			//add one value col
			{CreateSpec: "val3 DECIMAL", SelectSpec: "SUM(val1::DECIMAL * val2)", IsKey: false},
		},
		TrackBy: &DateTracker{SourceDateColumn: "ts", DestinationDateColumn: "ts", Backtrack: time.Hour},
	}

	if t1.GetVersion() == t2.GetVersion() {
		t.Errorf("Tables with different columns had had the same version: t1 %s t2 %s", t1.GetVersion(), t2.GetVersion())
	}

}

func TestTimeseriesView_GetVersionColContent(t *testing.T) {

	t1 := &TimeseriesView{
		Name: "test_view_1",
		SourceTableName: "test_data_1",
		UpdateInterval: time.Minute,
		Columns: []*StandardViewColumn{
			{CreateSpec: "ts TEXT", SelectSpec: "date_trunc('hour', ts)", IsKey: true},
			{CreateSpec: "key1 TEXT", SelectSpec: "key1", IsKey: true},
			{CreateSpec: "key2 TEXT", SelectSpec: "key2", IsKey: true},
			//omit 1 key col
			{CreateSpec: "val1 INTEGER", SelectSpec: "SUM(val1)", IsKey: false},
			//add one value col
			{CreateSpec: "val3 DECIMAL", SelectSpec: "SUM(val1::DECIMAL * val2)", IsKey: false},
		},
		TrackBy: &DateTracker{SourceDateColumn: "ts", DestinationDateColumn: "ts", Backtrack: time.Hour},
	}

	t2 := &TimeseriesView{
		Name: "test_view_1",
		SourceTableName: "test_data_1",
		UpdateInterval: time.Minute,
		Columns: []*StandardViewColumn{
			{CreateSpec: "ts TEXT", SelectSpec: "date_trunc('hour', ts)", IsKey: true},
			{CreateSpec: "key1 TEXT", SelectSpec: "key1", IsKey: true},
			{CreateSpec: "key2 TEXT", SelectSpec: "key2", IsKey: true},
			//omit 1 key col
			{CreateSpec: "val1 DECIMAL", SelectSpec: "SUM(val1)", IsKey: false},
			//add one value col
			{CreateSpec: "val3 DECIMAL", SelectSpec: "SUM(val1::DECIMAL * val2)", IsKey: false},
		},
		TrackBy: &DateTracker{SourceDateColumn: "ts", DestinationDateColumn: "ts", Backtrack: time.Hour},
	}

	if t1.GetVersion() == t2.GetVersion() {
		t.Errorf("Tables with different columns had had the same version: t1 %s t2 %s", t1.GetVersion(), t2.GetVersion())
	}
}