/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import "testing"

func TestEmitBinlogBacklogGauges(t *testing.T) {
	spy := &gaugeSpy{}
	EmitBinlogBacklogGauges(spy, 250, 1000)

	wantNames := []string{
		"binlog.backlog_size",
		"binlog.backlog_capacity",
		"binlog.backlog_utilization",
	}
	wantVals := []float64{250, 1000, 0.25}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
	}
}

func TestEmitBinlogBacklogGauges_nilSafe(t *testing.T) {
	EmitBinlogBacklogGauges(nil, 1, 2)
}

func TestBinlogBacklogUtilization(t *testing.T) {
	tests := []struct {
		size, capacity int
		want           float64
	}{
		{0, 1000, 0},
		{250, 1000, 0.25},
		{1000, 1000, 1},
		{1500, 1000, 1},
		{-1, 1000, 0},
		{10, 0, 0},
	}
	for _, tt := range tests {
		got := binlogBacklogUtilization(tt.size, tt.capacity)
		if got != tt.want {
			t.Fatalf("utilization(%d, %d) = %v, want %v", tt.size, tt.capacity, got, tt.want)
		}
	}
}
