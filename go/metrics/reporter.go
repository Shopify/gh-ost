/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	"math"
	rmetrics "runtime/metrics"
	"runtime/pprof"
	"time"
)

type metricSpec struct {
	runtime string
	statsd  string
}

// gaugeSpecs are non-cumulative runtime metrics emitted as DogStatsD Gauges.
var gaugeSpecs = []metricSpec{
	{"/sched/goroutines:goroutines", "go.runtime.goroutines"},
	{"/memory/classes/heap/objects:bytes", "go.runtime.memory.heap_alloc_bytes"},
	{"/memory/classes/heap/unused:bytes", "go.runtime.memory.heap_idle_bytes"},
	{"/memory/classes/heap/released:bytes", "go.runtime.memory.heap_released_bytes"},
	{"/memory/classes/heap/free:bytes", "go.runtime.memory.heap_free_bytes"},
	{"/memory/classes/total:bytes", "go.runtime.memory.sys_bytes"},
	{"/gc/heap/objects:objects", "go.runtime.memory.heap_objects"},
	{"/gc/heap/live:bytes", "go.runtime.memory.heap_live_bytes"},
	{"/gc/heap/goal:bytes", "go.runtime.memory.next_gc_bytes"},
}

// counterSpecs are cumulative runtime metrics. Each tick we emit the delta as a DogStatsD Count.
var counterSpecs = []metricSpec{
	{"/gc/cycles/total:gc-cycles", "go.runtime.gc.cycles_total"},
	{"/gc/heap/allocs:bytes", "go.runtime.memory.alloc_bytes_total"},
	{"/gc/heap/allocs:objects", "go.runtime.memory.allocs_total"},
	{"/gc/heap/frees:bytes", "go.runtime.memory.free_bytes_total"},
	{"/gc/heap/frees:objects", "go.runtime.memory.frees_total"},
}

const gcPauseMetric = "/gc/pauses:seconds"

// gcPauseQuantiles are approximate GC pause quantiles from the runtime histogram.
var gcPauseQuantiles = []struct {
	p   float64
	tag string
}{
	{0.25, "quantile:0.25"},
	{0.5, "quantile:0.5"},
	{0.75, "quantile:0.75"},
	{0.9, "quantile:0.9"},
	{0.99, "quantile:0.99"},
}

// StartRuntimeReporter emits Go runtime/memory and process metrics via client until ctx is cancelled.
func StartRuntimeReporter(ctx context.Context, client *Client, interval time.Duration) {
	if client.sd == nil || interval <= 0 {
		return
	}

	nGauge := len(gaugeSpecs)
	nCounter := len(counterSpecs)

	samples := make([]rmetrics.Sample, nGauge+nCounter+1)
	for i, s := range gaugeSpecs {
		samples[i].Name = s.runtime
	}
	for i, s := range counterSpecs {
		samples[nGauge+i].Name = s.runtime
	}
	pauseIdx := nGauge + nCounter
	samples[pauseIdx].Name = gcPauseMetric

	prevCounter := make([]uint64, nCounter)
	counterInit := make([]bool, nCounter)
	var prevPauseCounts []uint64

	emitProcess := newProcessEmitter(client)

	emit := func() {
		rmetrics.Read(samples)

		// Gauges
		for i, spec := range gaugeSpecs {
			client.Gauge(spec.statsd, sampleFloat64(samples[i]))
		}

		// go_threads equivalent: OS threads created (matches pprof threadcreate profile)
		if p := pprof.Lookup("threadcreate"); p != nil {
			client.Gauge("go.runtime.threads", float64(p.Count()))
		}

		// Counters: emit delta since last tick
		for i, spec := range counterSpecs {
			s := samples[nGauge+i]
			if s.Value.Kind() != rmetrics.KindUint64 {
				continue
			}
			cur := s.Value.Uint64()
			if counterInit[i] && cur > prevCounter[i] {
				client.Count(spec.statsd, int64(cur-prevCounter[i]))
			}
			prevCounter[i] = cur
			counterInit[i] = true
		}

		// GC pause summary: percentile gauges from delta of the cumulative histogram.
		ps := samples[pauseIdx]
		if ps.Value.Kind() == rmetrics.KindFloat64Histogram {
			h := ps.Value.Float64Histogram()
			delta := histDelta(h.Counts, prevPauseCounts)
			prevPauseCounts = append(prevPauseCounts[:0:0], h.Counts...)
			for _, q := range gcPauseQuantiles {
				client.Gauge("go.runtime.gc.pause_seconds", histQuantile(h.Buckets, delta, q.p), q.tag)
			}
		}

		emitProcess()
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		emit()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				emit()
			}
		}
	}()
}

func sampleFloat64(s rmetrics.Sample) float64 {
	switch s.Value.Kind() {
	case rmetrics.KindUint64:
		return float64(s.Value.Uint64())
	case rmetrics.KindFloat64:
		return s.Value.Float64()
	default:
		return 0
	}
}

// histDelta subtracts prev bucket counts from cur to get the per-interval distribution.
func histDelta(cur, prev []uint64) []uint64 {
	delta := make([]uint64, len(cur))
	for i, c := range cur {
		if i < len(prev) && c >= prev[i] {
			delta[i] = c - prev[i]
		} else {
			delta[i] = c
		}
	}
	return delta
}

// histQuantile computes the p-th quantile from a runtime/metrics histogram.
// buckets has len(counts)+1 entries; the first may be -Inf and last +Inf.
func histQuantile(buckets []float64, counts []uint64, p float64) float64 {
	total := uint64(0)
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	threshold := uint64(math.Ceil(float64(total) * p))
	cumulative := uint64(0)
	for i, c := range counts {
		cumulative += c
		if cumulative < threshold {
			continue
		}
		lo := buckets[i]
		hi := buckets[i+1]
		if math.IsInf(lo, -1) {
			lo = 0
		}
		if math.IsInf(hi, 1) || c == 0 {
			return lo
		}
		fraction := float64(threshold-cumulative+c) / float64(c)
		return lo + fraction*(hi-lo)
	}
	return 0
}
