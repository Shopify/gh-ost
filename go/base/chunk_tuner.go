package base

import (
	"math"
	"sort"
	"sync"
	"time"
)

const (
	ChunkTunerWindow            = 60 * time.Second
	chunkTunerThrottledMaxRatio = 0.2
	chunkTunerShrinkFactor      = 0.75
	chunkTunerGrowFactor        = 1.10
)

// ChunkTuner collects chunk-copy latencies for one window.
type ChunkTuner struct {
	mu        sync.Mutex
	latencies []time.Duration
}

func (ct *ChunkTuner) RecordLatency(d time.Duration) {
	ct.mu.Lock()
	ct.latencies = append(ct.latencies, d)
	ct.mu.Unlock()
}

// Drain returns the window's samples and resets the window.
func (ct *ChunkTuner) Drain() []time.Duration {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	samples := ct.latencies
	ct.latencies = nil
	return samples
}

func P95(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[int(math.Ceil(0.95*float64(len(sorted))))-1]
}

// NextChunkSize returns the new chunk size, and whether the tuner is pinned at
// its minimum while still over target.
func NextChunkSize(current, minSize, maxSize int64, p95, targetLow, targetHigh time.Duration, throttledRatio float64, samples int) (next int64, stuckAtMin bool) {
	over := throttledRatio > chunkTunerThrottledMaxRatio || (samples > 0 && p95 > targetHigh)
	next = current
	switch {
	case over:
		next = int64(float64(current) * chunkTunerShrinkFactor)
	case samples > 0 && p95 < targetLow:
		next = int64(math.Max(float64(current)*chunkTunerGrowFactor, float64(current+1)))
	}
	if next < minSize {
		next = minSize
	}
	if next > maxSize {
		next = maxSize
	}
	return next, over && current <= minSize
}
