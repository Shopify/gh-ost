/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import "github.com/github/gh-ost/go/mysql"

// BinlogReader streams rows and transaction boundaries in order.
type BinlogReader interface {
	StreamEvents(canStopStreaming func() bool) error
	GetCurrentBinlogCoordinates() mysql.BinlogCoordinates
	LastTransactionCoordinates() mysql.BinlogCoordinates
	Close()
}
