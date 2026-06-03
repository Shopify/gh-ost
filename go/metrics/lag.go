/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import "fmt"

// EmitLagGauges emits replication and heartbeat lag gauges (namespace is applied by the client):
// gh_ost.lag.replication_seconds, gh_ost.lag.heartbeat_seconds, each tagged throttled:true|false.
//
// These are point-in-time readings each status tick (not a distribution), so gauges are used
// rather than histograms — DogStatsD histogram aggregation exposes count/max series that do not
// match the log line lag values in Prometheus/Grafana.
func EmitLagGauges(emit Emitter, replicationLagSeconds, heartbeatLagSeconds float64, throttled bool) {
	if emit == nil {
		return
	}
	tags := []string{fmt.Sprintf("throttled:%t", throttled)}
	emit.Gauge("lag.replication_seconds", replicationLagSeconds, tags...)
	emit.Gauge("lag.heartbeat_seconds", heartbeatLagSeconds, tags...)
}
