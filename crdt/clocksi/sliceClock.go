package clocksi

import (
	"fmt"
	"sort"
	"strings"
)

//The idea of this implementation is to provide a clock suitable to be used in scenarios where efficiency of access is critical (e.g., under locks)
//As such, the interface is not the most practical one.

type SliceTimestamp struct {
	vc []int64
}

var (
	sortedIDs []int16
	idToPos   map[int16]int
)

func SetSortedIDs(ids []int16) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	sortedIDs = ids
	idToPos = make(map[int16]int, len(ids))
	for i, id := range ids {
		idToPos[id] = i
	}
}

func GetSortedIDs() []int16 {
	return sortedIDs
}

func NewSliceTimestamp() SliceTimestamp {
	return SliceTimestamp{vc: make([]int64, len(sortedIDs))}
}

func FromClockSiToSlice(other Timestamp) SliceTimestamp {
	st := SliceTimestamp{}
	st.vc = make([]int64, len(sortedIDs))
	for i, id := range sortedIDs {
		st.vc[i] = other.GetPos(id)
	}
	return st
}

func (st SliceTimestamp) GetPosOfId(id int16) int {
	if pos, ok := idToPos[id]; ok {
		return pos
	}
	return -1
}

func (st SliceTimestamp) UpdatePos(pos int, value int64) {
	st.vc[pos] = value
}

func (st SliceTimestamp) GetPosValue(pos int) int64 {
	return st.vc[pos]
}

func (st SliceTimestamp) Copy() []int64 {
	newSlice := make([]int64, len(st.vc))
	copy(newSlice, st.vc)
	return newSlice
}

func FromSortedSliceToClockSi(sortedVc []int64) Timestamp {
	ts := ClockSiTimestamp{VectorClock: make(map[int16]int64, len(sortedVc))}
	for i, id := range sortedIDs {
		ts.VectorClock[id] = sortedVc[i]
	}
	return ts
}

func (st SliceTimestamp) IsHigherOrEqualExceptFor(otherTs SliceTimestamp, pos int) bool {
	if otherTs.vc == nil {
		return false
	}

	//Note: the order is the same on both. Sorted from minID to maxID.
	//Can check if all >=. If one fails to be >=, check if it is the right ID: if is, continue.
	//If not, break and return false

	for i, ourValue := range st.vc {
		if ourValue < otherTs.vc[i] && i != pos {
			return false
		}
	}
	//At this point, all are >=.
	return true
}

func (st SliceTimestamp) Merge(otherTs SliceTimestamp) {
	for i, otherValue := range otherTs.vc {
		if st.vc[i] < otherValue {
			st.vc[i] = otherValue
		}
	}
}

func (st SliceTimestamp) ToSortedString() string {
	var builder strings.Builder
	builder.WriteString("{[")
	for i, id := range sortedIDs {
		builder.WriteString(fmt.Sprint(id))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprint(st.vc[i]))
		builder.WriteString(",")
	}
	builder.WriteString("]}")
	return builder.String()
}
