/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import "fmt"

// EmitLagHistograms emits replication and heartbeat lag histograms (namespace is applied by the client):
// gh_ost.lag.replication_seconds, gh_ost.lag.heartbeat_seconds, each tagged throttled:true|false.
func EmitLagHistograms(emit HistogramEmitter, replicationLagSeconds, heartbeatLagSeconds float64, throttled bool) {
	if emit == nil {
		return
	}
	tags := []string{fmt.Sprintf("throttled:%t", throttled)}
	emit.Histogram("lag.replication_seconds", replicationLagSeconds, tags...)
	emit.Histogram("lag.heartbeat_seconds", heartbeatLagSeconds, tags...)
}
