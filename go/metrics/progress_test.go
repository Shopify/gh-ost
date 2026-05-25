/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import "testing"

func TestEmitProgressGauges(t *testing.T) {
	spy := &gaugeSpy{}
	EmitProgressGauges(spy, 1000, 5000, 42)

	wantNames := []string{
		"row_copy.rows_copied",
		"row_copy.rows_estimate",
		"dml.events_applied",
	}
	wantVals := []float64{1000, 5000, 42}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
	}
}

func TestEmitProgressGauges_nilSafe(t *testing.T) {
	EmitProgressGauges(nil, 1, 2, 3)
}
