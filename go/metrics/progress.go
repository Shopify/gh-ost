/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

// EmitProgressGauges emits row-copy and DML progress gauges (namespace is applied by the client):
// gh_ost.row_copy.rows_copied, gh_ost.row_copy.rows_estimate, gh_ost.dml.events_applied.
func EmitProgressGauges(emit MemStatsGaugeEmitter, rowsCopied, rowsEstimate, dmlEventsApplied int64) {
	if emit == nil {
		return
	}
	emit.Gauge("row_copy.rows_copied", float64(rowsCopied))
	emit.Gauge("row_copy.rows_estimate", float64(rowsEstimate))
	emit.Gauge("dml.events_applied", float64(dmlEventsApplied))
}
