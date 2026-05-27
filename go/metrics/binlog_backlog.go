/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

// EmitBinlogBacklogGauges emits apply-events queue depth gauges (namespace is applied by the client):
// gh_ost.binlog.backlog_size, gh_ost.binlog.backlog_capacity, gh_ost.binlog.backlog_utilization.
func EmitBinlogBacklogGauges(emit MemStatsGaugeEmitter, backlogSize, backlogCapacity int) {
	if emit == nil {
		return
	}
	emit.Gauge("binlog.backlog_size", float64(backlogSize))
	emit.Gauge("binlog.backlog_capacity", float64(backlogCapacity))
	emit.Gauge("binlog.backlog_utilization", binlogBacklogUtilization(backlogSize, backlogCapacity))
}

func binlogBacklogUtilization(backlogSize, backlogCapacity int) float64 {
	if backlogCapacity <= 0 {
		return 0
	}
	utilization := float64(backlogSize) / float64(backlogCapacity)
	if utilization > 1 {
		return 1
	}
	if utilization < 0 {
		return 0
	}
	return utilization
}
