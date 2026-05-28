/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import "testing"

func TestEmitLagGauges_notThrottled(t *testing.T) {
	spy := &gaugeSpy{}
	EmitLagGauges(spy, 2.5, 1.25, false)

	wantNames := []string{"lag.replication_seconds", "lag.heartbeat_seconds"}
	wantVals := []float64{2.5, 1.25}
	wantTags := []string{"throttled:false"}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
		if len(spy.tags[i]) != 1 || spy.tags[i][0] != wantTags[0] {
			t.Fatalf("[%d] got tags %v want [%s]", i, spy.tags[i], wantTags[0])
		}
	}
}

func TestEmitLagGauges_throttled(t *testing.T) {
	spy := &gaugeSpy{}
	EmitLagGauges(spy, 4.0, 3.0, true)

	if len(spy.names) != 2 {
		t.Fatalf("got %d gauges, want 2", len(spy.names))
	}
	for i := range spy.names {
		if len(spy.tags[i]) != 1 || spy.tags[i][0] != "throttled:true" {
			t.Fatalf("[%d] got tags %v want [throttled:true]", i, spy.tags[i])
		}
	}
}

func TestEmitLagGauges_nilSafe(t *testing.T) {
	EmitLagGauges(nil, 1, 2, false)
}
