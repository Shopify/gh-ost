/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	rmetrics "runtime/metrics"
	"testing"
	"time"
)

func TestHistDelta(t *testing.T) {
	tests := []struct {
		name string
		cur  []uint64
		prev []uint64
		want []uint64
	}{
		{
			name: "subtract",
			cur:  []uint64{10, 20, 30},
			prev: []uint64{8, 15, 28},
			want: []uint64{2, 5, 2},
		},
		{
			name: "no_prev_takes_full",
			cur:  []uint64{5, 6},
			prev: []uint64{},
			want: []uint64{5, 6},
		},
		{
			name: "reset_smaller_than_prev",
			cur:  []uint64{3, 10},
			prev: []uint64{100, 50},
			want: []uint64{3, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := histDelta(tt.cur, tt.prev)
			if len(got) != len(tt.want) {
				t.Fatalf("len got %d want %d", len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("[%d]: got %d want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestHistQuantile(t *testing.T) {
	t.Run("zero_total", func(t *testing.T) {
		if v := histQuantile([]float64{0, 100}, []uint64{0, 0}, 0.5); v != 0 {
			t.Fatalf("got %v want 0", v)
		}
	})

	t.Run("median_two_buckets_uniform", func(t *testing.T) {
		buckets := []float64{0, 10, 20}
		counts := []uint64{0, 5, 5}
		v := histQuantile(buckets, counts, 0.5)
		if v < 10 || v > 20 {
			t.Fatalf("expected quantile in [10,20], got %v", v)
		}
	})

	t.Run("single_nonempty_bucket", func(t *testing.T) {
		buckets := []float64{0, 10}
		counts := []uint64{10}
		v := histQuantile(buckets, counts, 0.99)
		if want := float64(10); v != want {
			t.Fatalf("got %v want %v", v, want)
		}
	})
}

func TestSampleFloat64_RuntimeMetric(t *testing.T) {
	names := []string{
		gaugeSpecs[0].runtime,
		"/memory/classes/total:bytes",
		"/gc/heap/objects:objects",
	}
	for _, name := range names {
		var s rmetrics.Sample
		s.Name = name
		rmetrics.Read([]rmetrics.Sample{s})
		if s.Value.Kind() == rmetrics.KindBad {
			continue
		}
		v := sampleFloat64(s)
		if v < 0 || v >= 1e15 {
			t.Fatalf("unexpected sample value %v kind=%v name=%s", v, s.Value.Kind(), name)
		}
		return
	}
	t.Skip("no runtime/metrics samples available on this Go/platform")
}

func TestStartRuntimeReporter_NoStatsDClient_NoGoroutineLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{} // nil DogStatsD client — StartRuntimeReporter returns immediately
	StartRuntimeReporter(ctx, c, time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
}
