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
type GTIDBinlogCoordinates struct {
	GTIDSet *gomysql.MysqlGTIDSet
	UUIDSet *gomysql.UUIDSet
}

// NewGTIDBinlogCoordinates parses a MySQL GTID set into a *GTIDBinlogCoordinates struct.
func NewGTIDBinlogCoordinates(gtidSet string) (*GTIDBinlogCoordinates, error) {
	set, err := gomysql.ParseMysqlGTIDSet(gtidSet)
	return &GTIDBinlogCoordinates{
		GTIDSet: set.(*gomysql.MysqlGTIDSet),
	}, err
}

// DisplayString returns a user-friendly string representation of these current UUID set or the full GTID set.
func (this *GTIDBinlogCoordinates) DisplayString() string {
	if this.UUIDSet != nil {
		return this.UUIDSet.String()
	}
	return this.String()
}

// String returns a user-friendly string representation of these full GTID set.
func (this GTIDBinlogCoordinates) String() string {
	return this.GTIDSet.String()
}

// Equals tests equality of this coordinate and another one.
func (this *GTIDBinlogCoordinates) Equals(other BinlogCoordinates) bool {
	if other == nil || this.IsEmpty() || other.IsEmpty() {
		return false
	}

	otherCoords, ok := other.(*GTIDBinlogCoordinates)
	if !ok {
		return false
	}

	return this.GTIDSet.Equal(otherCoords.GTIDSet)
}

// IsEmpty returns true if the GTID set is empty.
func (this *GTIDBinlogCoordinates) IsEmpty() bool {
	return this.GTIDSet == nil
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (this *GTIDBinlogCoordinates) SmallerThan(other BinlogCoordinates) bool {
	if other == nil || this.IsEmpty() || other.IsEmpty() {
		return false
	}
	otherCoords, ok := other.(*GTIDBinlogCoordinates)
	if !ok {
		return false
	}

	// if 'this' does not contain the same sets we assume we are behind 'other'.
	// there are probably edge cases where this isn't true
	return !this.GTIDSet.Contain(otherCoords.GTIDSet)
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
func (this *GTIDBinlogCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	return this.Equals(other) || this.SmallerThan(other)
}

func (this *GTIDBinlogCoordinates) Clone() BinlogCoordinates {
	out := &GTIDBinlogCoordinates{}
	if this.GTIDSet != nil {
		out.GTIDSet = this.GTIDSet.Clone().(*gomysql.MysqlGTIDSet)
	}
	if this.UUIDSet != nil {
		out.UUIDSet = this.UUIDSet.Clone()
	}
	return out
}

// LazyGTIDCoordinates describes the in-flight coordinates of a transaction that
// has been announced via GTIDEvent but not yet committed (XIDEvent not yet seen).
// It holds a stable, immutable reference to the last-committed MysqlGTIDSet and
// the current transaction's GTID. The expensive Clone of the full set is deferred
// until Materialize is actually called, which only happens when external callers
// need a snapshot (via GetCurrentBinlogCoordinates) or when a comparison is made —
// not on every row event in the hot path.
type LazyGTIDCoordinates struct {
	base *gomysql.MysqlGTIDSet // last-committed GTIDSet; immutable, not owned
	sid  uuid.UUID             // current transaction's server UUID
	gno  int64                 // current transaction's GNO

	cacheMutex         sync.Mutex
	cachedMaterialized *GTIDBinlogCoordinates
}

// NewLazyGTIDCoordinates creates coordinates for an in-flight transaction.
// base must be the MysqlGTIDSet of the last committed transaction and must
// not be mutated after this call.
func NewLazyGTIDCoordinates(base *gomysql.MysqlGTIDSet, sid uuid.UUID, gno int64) *LazyGTIDCoordinates {
	return &LazyGTIDCoordinates{base: base, sid: sid, gno: gno}
}

// Materialize clones the base set, adds the in-flight GTID, and returns a full
// GTIDBinlogCoordinates. The result is an independent snapshot safe to hold across
// transaction boundaries. This is the only point where a MysqlGTIDSet.Clone occurs.
func (l *LazyGTIDCoordinates) Materialize() *GTIDBinlogCoordinates {
	l.cacheMutex.Lock()
	defer l.cacheMutex.Unlock()

	if l.cachedMaterialized != nil {
		return l.cachedMaterialized
	}

	set := l.base.Clone().(*gomysql.MysqlGTIDSet)
	set.AddGTID(l.sid, l.gno)
	l.cachedMaterialized = &GTIDBinlogCoordinates{GTIDSet: set}
	return l.cachedMaterialized
}

func (l *LazyGTIDCoordinates) String() string        { return l.Materialize().String() }
func (l *LazyGTIDCoordinates) DisplayString() string { return fmt.Sprintf("%s:%d", l.sid, l.gno) }
func (l *LazyGTIDCoordinates) IsEmpty() bool         { return l.base == nil }

func (l *LazyGTIDCoordinates) Equals(other BinlogCoordinates) bool {
	return l.Materialize().Equals(other)
}

func (l *LazyGTIDCoordinates) SmallerThan(other BinlogCoordinates) bool {
	return l.Materialize().SmallerThan(other)
}

func (l *LazyGTIDCoordinates) SmallerThanOrEquals(other BinlogCoordinates) bool {
	return l.Materialize().SmallerThanOrEquals(other)
}

// Clone materializes the full coordinates. The returned *GTIDBinlogCoordinates is
// an independent copy; callers receive a concrete type regardless of which
// BinlogCoordinates implementation produced it.
func (l *LazyGTIDCoordinates) Clone() BinlogCoordinates { return l.Materialize() }
