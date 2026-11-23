package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestP95(t *testing.T) {
	require.Equal(t, time.Duration(0), P95(nil))
	samples := make([]time.Duration, 0, 100)
	for i := 100; i >= 1; i-- {
		samples = append(samples, time.Duration(i)*time.Millisecond)
	}
	require.Equal(t, 95*time.Millisecond, P95(samples))
	require.Equal(t, 100*time.Millisecond, samples[0], "input must not be reordered")
	require.Equal(t, 7*time.Millisecond, P95([]time.Duration{7 * time.Millisecond}))
}

func TestNextChunkSize(t *testing.T) {
	ms := time.Millisecond
	low, high := 10*ms, 20*ms
	tests := []struct {
		name      string
		min       int64
		current   int64
		p95       time.Duration
		throttled float64
		samples   int
		want      int64
		wantStuck bool
	}{
		{"in band holds", 10, 100, 15 * ms, 0, 50, 100, false},
		{"above band shrinks 25%", 10, 100, 30 * ms, 0, 50, 75, false},
		{"below band grows 10%", 10, 100, 5 * ms, 0, 50, 110, false},
		{"grow at least 1", 10, 9, 5 * ms, 0, 50, 10, false},
		{"throttled shrinks despite in band", 10, 100, 15 * ms, 0.5, 5, 75, false},
		{"throttled at 20% does not shrink", 10, 100, 15 * ms, 0.2, 50, 100, false},
		{"no samples, not throttled holds", 10, 100, 0, 0, 0, 100, false},
		{"no samples, throttled shrinks", 10, 100, 0, 1.0, 0, 75, false},
		{"clamped to min", 50, 60, 30 * ms, 0, 50, 50, false},
		{"stuck at min warns", 50, 50, 30 * ms, 0, 50, 50, true},
		{"at min in band no warning", 50, 50, 15 * ms, 0, 50, 50, false},
		{"clamped to max", 10, 4900, 5 * ms, 0, 50, 5000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, stuck := NextChunkSize(tt.current, tt.min, 5000, tt.p95, low, high, tt.throttled, tt.samples)
			require.Equal(t, tt.want, next)
			require.Equal(t, tt.wantStuck, stuck)
		})
	}
}
