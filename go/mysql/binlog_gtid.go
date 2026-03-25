/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package mysql

import (
	"fmt"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	uuid "github.com/google/uuid"
)

// GTIDBinlogCoordinates describe binary log coordinates in MySQL GTID format.
//
// In committed mode (pendingGNO == 0): GTIDSet is the full, materialised set.
//
// In pending mode (pendingGNO != 0): GTIDSet is the base set from the last
// committed transaction; pendingSID:pendingGNO is the announced-but-not-yet-
// committed GTID. The expensive Clone is deferred until resolvedGTIDSet is called,
// which only happens when comparisons or string representations are needed — not on
// every row event in the hot path.
type GTIDBinlogCoordinates struct {
	GTIDSet *gomysql.MysqlGTIDSet

	pendingSID uuid.UUID // non-zero only in pending mode
	pendingGNO int64     // non-zero only in pending mode
}

// NewGTIDBinlogCoordinates parses a MySQL GTID set into a *GTIDBinlogCoordinates struct.
func NewGTIDBinlogCoordinates(gtidSet string) (*GTIDBinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &GTIDBinlogCoordinates{
		GTIDSet: set.(*gomysql.MysqlGTIDSet),
	}, err
}

// WithPendingGTID returns coordinates for a transaction that has been announced
// (via GTIDEvent) but not yet committed. g.GTIDSet is aliased directly as the base
// without cloning; the Clone is deferred until the coordinates are actually compared
// or stringified. g must not be mutated after this call.
func (g *GTIDBinlogCoordinates) WithPendingGTID(sid uuid.UUID, gno int64) *GTIDBinlogCoordinates {
	return &GTIDBinlogCoordinates{GTIDSet: g.resolvedGTIDSet(), pendingSID: sid, pendingGNO: gno}
}

// resolvedGTIDSet returns the effective MysqlGTIDSet.
// In committed mode (pendingGNO == 0) this is GTIDSet directly — no allocation.
// In pending mode it clones GTIDSet and adds the pending GTID.
func (g *GTIDBinlogCoordinates) resolvedGTIDSet() *gomysql.MysqlGTIDSet {
	if g.pendingGNO != 0 {
		set := g.GTIDSet.Clone().(*gomysql.MysqlGTIDSet)
		set.AddGTID(g.pendingSID, g.pendingGNO)
		return set
	}
	return g.GTIDSet
}

// DisplayString returns a user-friendly string representation.
// In pending mode it returns sid:gno cheaply without cloning the full set.
func (g *GTIDBinlogCoordinates) DisplayString() string {
	if g.pendingGNO != 0 {
		return fmt.Sprintf("%s:%d", g.pendingSID, g.pendingGNO)
	}
	return g.String()
}

// String returns the full GTID set string.
func (g GTIDBinlogCoordinates) String() string {
	return g.resolvedGTIDSet().String()
}

// Equals tests equality of this coordinate and another one.
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

// IsEmpty returns true if the GTID set is empty.
func (g *GTIDBinlogCoordinates) IsEmpty() bool {
	return g.GTIDSet == nil
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
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

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
func (g *GTIDBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	return g.Equals(other) || g.SmallerThan(other)
}

// Clone returns an independent, committed GTIDBinlogCoordinates snapshot.
// In pending mode the set is materialised and deep-copied in one pass.
func (g *GTIDBinlogCoordinates) Clone() BinlogCoordinates {
	out := &GTIDBinlogCoordinates{}
	if g.GTIDSet != nil {
		if g.pendingGNO != 0 {
			set := g.GTIDSet.Clone().(*gomysql.MysqlGTIDSet)
			set.AddGTID(g.pendingSID, g.pendingGNO)
			out.GTIDSet = set
		} else {
			out.GTIDSet = g.GTIDSet.Clone().(*gomysql.MysqlGTIDSet)
		}
	}
	return out
}
