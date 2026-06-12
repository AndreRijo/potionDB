package clocksi

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

//The idea of this implementation is to provide a clock suitable to be used in scenarios where efficiency of access is critical (e.g., under locks)
//As such, the interface is not the most practical one.

// Note: SliceTimestamp's implementation of the interface Timestamp assumes that all "id" are already the positions in vc.
// I.e., one must first convert from id to position, by using GetPosOfId or GetSortedPosOfId.
type SliceTimestamp struct {
	vc []int64
}

var (
	sortedIDs []uint16
	idToPos   map[uint16]int
)

func SetSortedIDs(ids []uint16) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	sortedIDs = ids
	idToPos = make(map[uint16]int, len(ids))
	for i, id := range ids {
		idToPos[id] = i
	}
}

func GetSortedIDs() []uint16 {
	return sortedIDs
}

func NewSliceTimestamp() SliceTimestamp {
	return SliceTimestamp{vc: make([]int64, nReplicas)}
}

func FromClockSiToSlice(other Timestamp) SliceTimestamp {
	st := SliceTimestamp{}
	st.vc = make([]int64, len(sortedIDs))
	for i, id := range sortedIDs {
		st.vc[i] = other.GetPos(id)
	}
	return st
}

func (st SliceTimestamp) GetPosOfId(id uint16) int {
	if pos, ok := idToPos[id]; ok {
		return pos
	}
	return -1
}

func GetSortedPosOfId(id uint16) uint16 {
	if pos, ok := idToPos[id]; ok {
		return uint16(pos)
	}
	return math.MaxUint16
}

func GetPosFromSortedPos(sortedPos uint16) uint16 {
	return sortedIDs[sortedPos]
}

func (st SliceTimestamp) GetPosValue(pos int) int64 {
	return st.vc[pos]
}

func (st SliceTimestamp) CopyToInt64Slice() []int64 {
	newSlice := make([]int64, len(st.vc))
	copy(newSlice, st.vc)
	return newSlice
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

func (st SliceTimestamp) MergeInto(otherTs Timestamp) {
	otherTsVc := otherTs.(SliceTimestamp).vc
	for i, otherValue := range otherTsVc {
		if st.vc[i] < otherValue {
			st.vc[i] = otherValue
		}
	}
}

func (st SliceTimestamp) NewTimestamp() (newTs Timestamp) {
	st.vc = make([]int64, len(sortedIDs))
	return st
}

func (st SliceTimestamp) NewTimestampFromId(id uint16) (newTs Timestamp) {
	return st.NewTimestamp()
}

// Note: id here is already the pos in the array.
func (st SliceTimestamp) NextTimestamp(id uint16) (newTs Timestamp) {
	newVc := make([]int64, len(st.vc))
	copy(newVc, st.vc)
	newValue := time.Now().UTC().UnixNano()
	if newValue <= newVc[id] {
		newVc[id] += 1
	} else {
		newVc[id] = newValue
	}
	return SliceTimestamp{vc: newVc}
}

func (st SliceTimestamp) IncTimestamp(id uint16) (newTs Timestamp) {
	newVc := make([]int64, len(st.vc))
	copy(newVc, st.vc)
	newVc[id] += 1
	return SliceTimestamp{vc: newVc}
}

func (st SliceTimestamp) SelfIncTimestamp(id uint16) {
	st.vc[id] += 1
}

func (st SliceTimestamp) Compare(otherTs Timestamp) (compResult TsResult) {
	if otherTs == nil {
		compResult = HigherTs
		return
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc
	foundHigher := false
	foundLower := false

	for i, selfValue := range selfVc {
		if selfValue < otherVc[i] {
			foundHigher = true
		} else if selfValue > otherVc[i] {
			foundLower = true
		}
		//No need to continue checking, we already know the clocks are concurrent
		if foundHigher && foundLower {
			return ConcurrentTs
		}
	}
	//If we reach this point we know the clocks aren't concurrent

	if foundHigher {
		//If we found that otherVc only has equal and higher values compared to selfVc, then selfVc has a lower timestamp.
		compResult = LowerTs
	} else if foundLower {
		compResult = HigherTs
	} else {
		compResult = EqualTs
	}

	return
}

func (st SliceTimestamp) IsHigherOrEqual(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return true
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc

	for i, selfValue := range selfVc {
		if selfValue < otherVc[i] {
			return false
		}
	}

	//If we reach this point, then it is equal or higher. Doesn't matter which.
	return true
}

func (st SliceTimestamp) IsHigher(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return true
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc
	foundLower := false

	for i, selfValue := range selfVc {
		if selfValue < otherVc[i] {
			return false
		} else if selfValue > otherVc[i] {
			foundLower = true
		}
	}

	return foundLower
}

func (st SliceTimestamp) CompareLowerOrEqual(otherTs Timestamp) (compResult TsResult) {
	if otherTs == nil {
		return UnknownTs
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc
	lower := false

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] {
			return UnknownTs
		} else if selfValue < otherVc[i] {
			lower = true
		}
	}

	if lower {
		return LowerTs
	}
	return EqualTs
}

func (st SliceTimestamp) IsLowerOrEqual(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] {
			return false
		}
	}

	//If we reach this point, then it is equal or lower. Doesn't matter which.
	return true
}

// Note: self and id must be positions in the array, not the original IDs.
func (st SliceTimestamp) IsLowerOrEqualExceptFor(otherTs Timestamp, self uint16, id uint16) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc
	selfP, idP := int(self), int(id)

	for i, selfValue := range selfVc {
		if i != idP && i != selfP {
			if selfValue > otherVc[i] {
				return false
			}
		}
	}

	return true
}

// Note: self must be a position in the array, not the original ID.
func (st SliceTimestamp) IsEqualExceptForSelf(otherTs Timestamp, self uint16) (isEqual bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc, selfP := st.vc, int(self)

	for i, selfValue := range selfVc {
		if i != selfP {
			if selfValue != otherVc[i] {
				return false
			}
		}
	}

	return true
}

func (st SliceTimestamp) IsLower(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc
	foundHigher := false

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] {
			return false
		} else if selfValue < otherVc[i] {
			foundHigher = true
		}
	}

	return foundHigher
}

func (st SliceTimestamp) IsLowerOrEqualTotalOrder(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	//NOTE: IDs are already sorted. So whoever has the first position that is lower, is the lower timestamp (if concurrent)

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc

	higherMinPos := -1
	lowerMinPos := -1

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] && lowerMinPos == -1 {
			lowerMinPos = i
		} else if selfValue < otherVc[i] && higherMinPos == -1 {
			higherMinPos = i
		}
	}

	if higherMinPos != -1 && lowerMinPos != -1 {
		//Equal
		return true
	}
	if lowerMinPos != -1 {
		//foundHigher = true. Thus, it is higher
		return false
	}
	if higherMinPos != -1 {
		//foundLower = true. Thus, it is lower
		return true
	}
	//Concurrent. Check lowerMinPos and higherMin Pos. If lowerMinPos < higherMinPos, then the lower ID belongs to the other TS, and thus otherTS < thisTS
	if lowerMinPos < higherMinPos {
		//The ID that is lower belongs to the other TS. Thus, this one is not before
		return false
	}
	return true
}

func (st SliceTimestamp) IsEqual(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(SliceTimestamp).vc
	selfVc := st.vc

	for i, selfValue := range selfVc {
		if selfValue != otherVc[i] {
			return false
		}
	}

	return true
}

func (st SliceTimestamp) IsConcurrent(otherTs Timestamp) (compResult bool) {
	//I don't know of a more efficient way to check for concurrency than as it is implemented in Compare.
	return st.Compare(otherTs) == ConcurrentTs
}

func (st SliceTimestamp) ComparePos(id uint16, otherTs Timestamp) (compResult TsResult) {
	thisValue, otherValue := st.vc[id], otherTs.GetPos(id)
	if thisValue == otherValue {
		return EqualTs
	}
	if thisValue < otherValue {
		return LowerTs
	}
	return HigherTs
}

func (st SliceTimestamp) UpdatePos(id uint16, newValue int64) (newTs Timestamp) {
	if st.vc[id] < newValue {
		st.vc[id] = newValue
	}
	return st
}

func (st SliceTimestamp) UpdateForcedPos(id uint16, newValue int64) (newTs Timestamp) {
	st.vc[id] = newValue
	return st
}

func (st SliceTimestamp) GetPos(id uint16) (value int64) {
	return st.vc[id]
}

func (st SliceTimestamp) Merge(otherTs Timestamp) (mergedTs Timestamp) {
	if otherTs == nil {
		//Just do a copy
		vc := make([]int64, len(st.vc))
		copy(vc, st.vc)
		return SliceTimestamp{vc: vc}
	}
	vc := make([]int64, len(st.vc))
	otherTsVc := otherTs.(SliceTimestamp).vc

	for i, value := range st.vc {
		//Keep the max of each position
		if value > otherTsVc[i] {
			vc[i] = value
		} else {
			vc[i] = otherTsVc[i]
		}
	}

	return SliceTimestamp{vc: vc}
}

func (st SliceTimestamp) IsSmallerConcurrent(otherTs Timestamp) (isSmaller bool) {
	//Note: Assuming both are concurrent
	otherTsVc := otherTs.(SliceTimestamp).vc

	for id, value := range st.vc {
		if value < otherTsVc[id] { //IDs are sorted, so the one with the first lowest value is the "smaller concurrent"
			return true
		} else if value > otherTsVc[id] {
			return false
		}
	}
	return false
}

func (st SliceTimestamp) IsDifferent(otherTs Timestamp) bool {
	if otherTs == nil {
		return true
	}
	otherTsVc := otherTs.(SliceTimestamp).vc
	for id, value := range st.vc {
		if value != otherTsVc[id] {
			return true
		}
	}
	return false
}

func (st SliceTimestamp) Update() (sameTs Timestamp) {
	if len(st.vc) == nReplicas {
		return st
	}
	newVC := make([]int64, nReplicas)
	copy(newVC, st.vc)
	st.vc = newVC
	return st
}

func GetSliceTimestampSize() int {
	return len(sortedIDs) * entrySize
}

func GetSliceTimestampSizeForNEntries(nEntries int) int {
	return nEntries * entrySize
}

func (st SliceTimestamp) GetBytesSize() int {
	return len(st.vc) * entrySize
}

func (st SliceTimestamp) ToBytesBuf(bytes []byte) (nWritten int) {
	for i, ts := range st.vc {
		binary.LittleEndian.PutUint64(bytes[i*entrySize:(i+1)*entrySize], uint64(ts))
	}
	//fmt.Printf("[SliceTimestamp]ToBytesBuf(): Encoding clock with %d entries, into a buf with %d bytes available.\n", len(st.vc), len(bytes))
	return len(st.vc) * entrySize
}

func (st SliceTimestamp) ToBytes() (bytes []byte) {
	bytes = make([]byte, len(st.vc)*entrySize)
	for i, ts := range st.vc {
		binary.LittleEndian.PutUint64(bytes[i*entrySize:(i+1)*entrySize], uint64(ts))
	}
	//fmt.Printf("[SliceTimestamp]ToBytes(): Encoding clock with %d entries, using %d bytes.\n", len(st.vc), len(bytes))
	return
}

func (st SliceTimestamp) FromBytes(bytes []byte) (newTs Timestamp) {
	var newVC []int64
	if len(bytes) > 0 {
		newVC = make([]int64, len(bytes)/entrySize)
		for i := 0; i < len(newVC); i++ {
			newVC[i] = int64(binary.LittleEndian.Uint64(bytes[i*entrySize : (i+1)*entrySize]))
		}
		//fmt.Printf("[SliceTimestamp]FromBytes(): Decoded clock with %d entries, using %d bytes.\n", len(newVC), len(bytes))
	} else {
		newVC = make([]int64, len(knownIDs))
	}
	//fmt.Println("Decoded clock:", newVC)
	return SliceTimestamp{vc: newVC}
}

func (st SliceTimestamp) FromBytesInto(bytes []byte) (sameTs Timestamp) {
	if len(bytes) == len(st.vc)*entrySize { //Fast path.
		for i := 0; i < len(st.vc); i++ {
			st.vc[i] = int64(binary.LittleEndian.Uint64(bytes[i*entrySize:]))
		}
		return st
	} else if len(bytes) == 0 && len(st.vc) == len(knownIDs) {
		return st
	} else { //Safe, slow path. Will allocate new.
		return st.FromBytes(bytes)
	}
}

func (st SliceTimestamp) ToString() (tsString string) {
	var builder strings.Builder
	builder.WriteString("{[")
	for i, value := range st.vc {
		builder.WriteString(fmt.Sprint(sortedIDs[i]))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprint(value))
		builder.WriteString(",")
	}
	builder.WriteString("]}")
	return builder.String()
}

func (st SliceTimestamp) ToSortedString() (tsString string) {
	return st.ToString() //SliceTimestamp's vc is already naturally stored in order.
}

func (st SliceTimestamp) ToDebugCompString(otherTs Timestamp) (tsString string) {
	var builder strings.Builder
	builder.WriteString("{[")
	for i, value := range st.vc {
		builder.WriteString(fmt.Sprint(sortedIDs[i]))
		builder.WriteRune(':')
		builder.WriteString(fmt.Sprint(value))
		builder.WriteRune(',')
		builder.WriteString(fmt.Sprint(sortedIDs[i]))
		builder.WriteRune(':')
		builder.WriteString(fmt.Sprint(otherTs.GetPos(uint16(i))))
		builder.WriteRune(',')
	}
	builder.WriteString("]}")
	return builder.String()
}

func (ts SliceTimestamp) GetNumberEntries() int {
	return len(ts.vc)
}

// NOTE: If we one day support adding/removing replicas on the fly this will probably no longer work, as it ignores the replica's ID (map key)
func (st SliceTimestamp) GetMapKey() (key TimestampKey) {
	if usePointerKey {
		return st.getPointerKey()
	}
	if useByteKey {
		return st.getMapByteKey()
	}
	/*if useSliceKey {
		return ts.GetMapSliceKey()
	}*/
	return st.getMapStringKey()
}

// NOTE: THIS ASSUMES THAT st WILL NOT BE MODIFIED!!! ANY SLICETIMESTAMP THAT MAY BE MODIFIED IS NOT SUITABLE FOR POINTERKEY.
// (The idea of getPointerKey() is to avoid a data copy/new allocation, so this restriction is logical.)
// (Furthermore... if the value underneath a key gets modified, the key no longer matches, which is incorrect behaviour...?)
func (st SliceTimestamp) getPointerKey() (key TimestampKey) {
	return PointerKey{ptr: &st.vc[0], len: len(st.vc)}
}

func (st SliceTimestamp) getMapByteKey() (key TimestampKey) { //For SliceTimestamp, this is the same as ToBytes()
	return ByteKey(string(st.ToBytes()))
}

//Slice representation
/*
func (st SliceTimestamp) GetMapSliceKey() (key TimestampKey) {
	//Need to ensure this is written in order, since go randomizes map iteration order
	keys := ts.getSortedKeys()
	intSlice := make([]int64, len(keys))
	for i, key := range keys {
		intSlice[i] = ts.VectorClock[key]
	}
	return SliceKey(intSlice)
}
*/

func (st SliceTimestamp) getMapStringKey() (key TimestampKey) {
	var builder strings.Builder
	for _, ts := range st.vc {
		builder.WriteString(fmt.Sprint(ts, ","))
	}
	return StringKey(builder.String())
}

func (st SliceTimestamp) GetSortedKeys() (keys []uint16) {
	return sortedIDs
}

func (st SliceTimestamp) Copy() (copyTs Timestamp) {
	copyClk := SliceTimestamp{vc: make([]int64, len(st.vc))}
	copy(copyClk.vc, st.vc)
	return copyClk
}

func (st SliceTimestamp) FastCopy() (values []int64) {
	values = make([]int64, len(st.vc))
	copy(values, st.vc)
	return values
}

func (st SliceTimestamp) CopyInto(copyTs Timestamp) {
	sliceCopy := copyTs.(SliceTimestamp)
	copy(sliceCopy.vc, st.vc)
}

func (st SliceTimestamp) FastCopyInto(values []int64) {
	copy(values, st.vc)
}

func (st SliceTimestamp) GetNEntries() int { //Debug method
	return len(st.vc)
}

func FromSliceValuesToSliceTimestamp(values []int64) (ts Timestamp) {
	return SliceTimestamp{vc: values}
}
