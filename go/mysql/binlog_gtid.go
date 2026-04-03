/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"sync"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	uuid "github.com/google/uuid"
)

// GTIDBinlogCoordinates describe binary log coordinates in MySQL GTID format.
// In pending mode (pendingGNO != 0), gtidSet is the base from the last committed
// transaction and pendingSID:pendingGNO is the in-flight GTID. The materialized
// set is computed lazily and cached on first use.
type GTIDBinlogCoordinates struct {
	gtidSet    *gomysql.MysqlGTIDSet
	pendingSID uuid.UUID
	pendingGNO int64
	once       sync.Once
	resolved   *gomysql.MysqlGTIDSet
}

func NewGTIDBinlogCoordinates(gtidSet string) (*GTIDBinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &GTIDBinlogCoordinates{gtidSet: set.(*gomysql.MysqlGTIDSet)}, err
}

func NewGTIDBinlogCoordinatesFromSet(set *gomysql.MysqlGTIDSet) *GTIDBinlogCoordinates {
	return &GTIDBinlogCoordinates{gtidSet: set}
}

func (g *GTIDBinlogCoordinates) GTIDSet() *gomysql.MysqlGTIDSet {
	return g.gtidSet
}

// WithPendingGTID returns a new pending coordinate using g.gtidSet as the base.
// g.gtidSet is aliased without cloning; g must not be modified after this call.
func (g *GTIDBinlogCoordinates) WithPendingGTID(sid uuid.UUID, gno int64) *GTIDBinlogCoordinates {
	return &GTIDBinlogCoordinates{gtidSet: g.resolvedGTIDSet(), pendingSID: sid, pendingGNO: gno}
}

func (g *GTIDBinlogCoordinates) resolvedGTIDSet() *gomysql.MysqlGTIDSet {
	if g.pendingGNO != 0 {
		g.once.Do(func() {
			set := g.gtidSet.Clone().(*gomysql.MysqlGTIDSet)
			set.AddGTID(g.pendingSID, g.pendingGNO)
			g.resolved = set
		})
		return g.resolved
	}
	return g.gtidSet
}

// DisplayString returns sid:gno in pending mode, otherwise the full GTID set string.
func (g *GTIDBinlogCoordinates) DisplayString() string {
	if g.pendingGNO != 0 {
		return fmt.Sprintf("%s:%d", g.pendingSID, g.pendingGNO)
	}
	return g.String()
}

func (g *GTIDBinlogCoordinates) String() string {
	return g.resolvedGTIDSet().String()
}

func (g *GTIDBinlogCoordinates) Equals(other BinlogCoordinates) bool {
	if other == nil || g.IsEmpty() || other.IsEmpty() {
		return false
	}
	otherCoords, ok := other.(*GTIDBinlogCoordinates)
	if !ok {
		return false
	}
	return g.resolvedGTIDSet().Equal(otherCoords.resolvedGTIDSet())
}

func (g *GTIDBinlogCoordinates) IsEmpty() bool {
	return g.gtidSet == nil
}

func (g *GTIDBinlogCoordinates) SmallerThan(other BinlogCoordinates) bool {
	if other == nil || g.IsEmpty() || other.IsEmpty() {
		return false
	}
	otherCoords, ok := other.(*GTIDBinlogCoordinates)
	if !ok {
		return false
	}
	// if 'this' does not contain the same sets we assume we are behind 'other'.
	// there are probably edge cases where this isn't true
	return !g.resolvedGTIDSet().Contain(otherCoords.resolvedGTIDSet())
}

func (g *GTIDBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	return g.Equals(other) || g.SmallerThan(other)
}

func (g *GTIDBinlogCoordinates) Clone() BinlogCoordinates {
	out := &GTIDBinlogCoordinates{}
	if g.gtidSet != nil {
		out.gtidSet = g.resolvedGTIDSet().Clone().(*gomysql.MysqlGTIDSet)
	}
	return out
}
