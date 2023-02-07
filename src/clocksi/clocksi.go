package clocksi

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type Timestamp interface {
	//Creates a new timestamp representing the initial value, with all known replicaIDs set to 0.
	NewTimestamp() (newTs Timestamp)
	//Creates a new timestamp whose only entry is id
	NewTimestampFromId(id int16) (newTs Timestamp)
	//Gets a timestamp that is more recent than the actual one
	NextTimestamp(id int16) (newTs Timestamp)
	//Gets a timestamp that increments the id value by one
	IncTimestamp(id int16) (newTs Timestamp)
	//Increments this timestamp's value for id by one. This should only be used when it is known that the Timestamp won't be shared.
	SelfIncTimestamp(id int16)
	//Compares two timestamps. Returns HigherTs if the current timestamp is higher than otherTs. Others should be self explanatory.
	//Possible results: HigherTs/LowerTs/EqualTs/ConcurrentTs
	Compare(otherTs Timestamp) (compResult TsResult)
	//Returns true when Compare(otherTs) would return HigherTs or EqualTs.
	//In some implementations (e.g., VectorClocks) and in some situations, this may be more efficient than Compare.
	IsHigherOrEqual(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return LowerTs or EqualTs.
	//In some implementations (e.g., VectorClocks) and in some situations, this may be more efficient than Compare.
	IsLowerOrEqual(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return LowerTs.
	//In some implementations (e.g., VectorClocks) and in some situations, this may be more efficient than Compare.
	IsLower(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return HigherTs.
	//In some implementations (e.g., VectorClocks) and in some situations, this may be more efficient than Compare.
	IsHigher(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return EqualTs.
	//In some implementations (e.g., VectorClocks) and in some situations, this may be more efficient than Compare.
	IsEqual(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return ConcurrentTs.
	IsConcurrent(otherTs Timestamp) (compResult bool)
	//For two given concurrent timestamps, it applies a tiebreaker algorithm to decide if ts is smaller than otherTs.
	IsSmallerConcurrent(otherTs Timestamp) (isSmaller bool)
	//Returns true if the two timestamps are different in some form. Useful for quick timestamp comparisons
	IsDifferent(otherTs Timestamp) bool
	//Compares a position in both timestamps. This operation has no meaning if the Timestamp implementation isn't based on vector clocks or other position based system
	//Returns concurrent if one of the clocks doesn't contain the reffered position
	ComparePos(id int16, otherTs Timestamp) (compResult TsResult)
	//Updates a position with the max between the current value and newValue
	UpdatePos(id int16, newValue int64) (newTs Timestamp)
	//Updates a position with newValue, even if newValue than the actual value. Useful for version management.
	UpdateForcedPos(id int16, newValue int64) (newTs Timestamp)
	//Gets the timestamp value associated to the id
	GetPos(id int16) (value int64)
	//Does the same as IsLowerOrEqual except that it ignores the values associated to self and id positions
	IsLowerOrEqualExceptFor(otherTs Timestamp, self int16, id int16) (compResult bool)
	//Does the same as isEqual except it ignores the value associated to self
	IsEqualExceptForSelf(otherTs Timestamp, self int16) (isEqual bool)
	//Returns the timestamp resulting of merging this timestamp and the argument timestamp
	//In a vector clock, it represents keeping the highest value of each position. The current implementation creates a new timestamp.
	Merge(otherTs Timestamp) (mergedTs Timestamp)
	//Adds entries for all replicas in knownIDs that aren't already existent in the Timestamp. This operation only makes sense for vector clock implementations.
	Update()
	//Converts the timestamp to a byte array
	ToBytes() (bytes []byte)
	//Gets the timestamp that is represented in the byte array
	//Note: This method is safe to call on an empty Timestamp instance.
	FromBytes(bytes []byte) (newTs Timestamp)
	//Useful for debugging purposes
	ToString() (tsString string)
	//Also useful for debugging purposes
	ToSortedString() (tsString string)
	//Gets a representation of this clock that is safe to use in GO maps
	GetMapKey() (key TimestampKey)
	//Performs a deep copy of the current timestamp and returns the copy
	Copy() (copyTs Timestamp)
	//Returns true if this TS happened before otherTS or, if they are concurrent, if by a total order TS should be before orderTS. Also returns true if they are equal.
	IsLowerOrEqualTotalOrder(otherTs Timestamp) (compResult bool)
}

type ClockSiTimestamp struct {
	//VectorClock *[]int64
	VectorClock map[int16]int64 //replicaID to clock value
}

type TsResult int

type TimestampKey interface {
	IsLower(otherKey TimestampKey) bool

	IsEqual(otherKey TimestampKey) bool

	IsHigher(otherKey TimestampKey) bool

	IsLowerOrEqual(otherKey TimestampKey) bool

	//toSliceKey() SliceKey
}

type StringKey string //Literal []int64 to string conversion

type ByteKey string //BigEndian byte conversion of []int64

//type SliceKey []int64

const (
	//Common to all clocks
	EqualTs      TsResult = 0
	HigherTs     TsResult = 1
	LowerTs      TsResult = 2
	ConcurrentTs TsResult = 3

	//Specific to clocksi implementation
	entrySize    = 8
	randomFactor = 500  //max value that can be added to the timestamp to avoid collisions
	useByteKey   = true //true = TimestampKey uses byte representation (ByteKey). Otherwise, uses StringKey.
	//useSliceKey = true //true = SliceKey ([]int64). false = string representation of the numbers
)

var (
	//Useful for comparations or other situations in which we need a temporary, non-significant, timestamp
	DummyTs   = NewClockSiTimestampFromId(0)
	knownIDs  = make([]int16, 0, 5) //Known replicaIDs. All new ClockSiTimestamps generated with NewClockSiTimestamp() will contain entries for these IDs
	HighestTs = ClockSiTimestamp{VectorClock: make(map[int16]int64)}
)

func AddNewID(id int16) {
	knownIDs = append(knownIDs, id)
	addToHighestTs(id)
}

//Creates a new timestamp. This gives the same result as doing: newTs = ClockSiTimestamp{}.NewTimestamp().
//Use whichever option feels more natural.
func NewClockSiTimestampFromId(id int16) (ts Timestamp) {
	return ClockSiTimestamp{}.NewTimestampFromId(id)
}

func (ts ClockSiTimestamp) NewTimestampFromId(id int16) (newTs Timestamp) {
	vc := make(map[int16]int64)
	vc[id] = 0
	//ts = ClockSiTimestamp{VectorClock: vc}
	//return ts
	return ClockSiTimestamp{VectorClock: vc}
}

func NewClockSiTimestamp() (ts Timestamp) {
	return ClockSiTimestamp{}.NewTimestamp()
}

func (ts ClockSiTimestamp) NewTimestamp() (newTs Timestamp) {
	vc := make(map[int16]int64)
	for _, id := range knownIDs {
		vc[id] = 0
	}
	return ClockSiTimestamp{VectorClock: vc}
}

func (ts ClockSiTimestamp) NextTimestamp(id int16) (newTs Timestamp) {
	newVc := make(map[int16]int64)
	for i, value := range ts.VectorClock {
		newVc[i] = value
	}
	newValue := time.Now().UTC().UnixNano()
	//Add a small random value to avoid equal timestamps
	//newValue += rand.Int63n(randomFactor)
	//In case we ask two timestamps in a very small window, this new value might be <= than the previous.
	//So, in that case, add +1
	if newValue <= newVc[id] {
		newVc[id] += 1
	} else {
		newVc[id] = newValue
	}
	return ClockSiTimestamp{VectorClock: newVc}

}

func (ts ClockSiTimestamp) IncTimestamp(id int16) (newTs Timestamp) {
	newVc := make(map[int16]int64)
	for i, value := range ts.VectorClock {
		newVc[i] = value
	}
	newVc[id] += 1
	return ClockSiTimestamp{VectorClock: newVc}
}

func (ts ClockSiTimestamp) SelfIncTimestamp(id int16) {
	ts.VectorClock[id] += 1
}

func (ts ClockSiTimestamp) Compare(otherTs Timestamp) (compResult TsResult) {
	if otherTs == nil {
		compResult = HigherTs
		return
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock
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

func (ts ClockSiTimestamp) IsHigherOrEqual(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return true
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock

	for i, selfValue := range selfVc {
		if selfValue < otherVc[i] {
			return false
		}
	}

	//If we reach this point, then it is equal or higher. Doesn't matter which.
	return true
}

func (ts ClockSiTimestamp) IsHigher(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return true
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock
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

func (ts ClockSiTimestamp) IsLowerOrEqual(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] {
			return false
		}
	}

	//If we reach this point, then it is equal or lower. Doesn't matter which.
	return true
}

func (ts ClockSiTimestamp) IsLowerOrEqualExceptFor(otherTs Timestamp, self int16, id int16) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock

	for i, selfValue := range selfVc {
		if i != id && i != self {
			if selfValue > otherVc[i] {
				return false
			}
		}
	}

	return true
}

func (ts ClockSiTimestamp) IsEqualExceptForSelf(otherTs Timestamp, self int16) (isEqual bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock

	for i, selfValue := range selfVc {
		if i != self {
			if selfValue != otherVc[i] {
				return false
			}
		}
	}

	return true
}

func (ts ClockSiTimestamp) IsLower(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock
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

func (ts ClockSiTimestamp) IsLowerOrEqualTotalOrder(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock

	foundHigher := false
	foundLower := false

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] {
			foundLower = true
		} else if selfValue < otherVc[i] {
			foundHigher = true
		}
	}

	if !foundLower && !foundHigher {
		//Equal
		return true
	}
	if !foundLower {
		//foundHigher = true. Thus, it is higher
		return false
	}
	if !foundHigher {
		//foundLower = true. Thus, it is lower
		return true
	}
	//Concurrent. Check which one has the minimum ID that is "lower"
	minIdLower, minIdHigher := int16(math.MaxInt16), int16(math.MaxInt16)
	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] && minIdLower > i {
			minIdLower = i
		} else if selfValue < otherVc[i] && minIdHigher > i {
			minIdHigher = i
		}
	}
	if minIdLower < minIdHigher {
		//The ID that is lower belongs to the other TS. Thus, this one is not before
		return false
	}
	return true
}

func (ts ClockSiTimestamp) IsEqual(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	otherVc := otherTs.(ClockSiTimestamp).VectorClock
	selfVc := ts.VectorClock

	for i, selfValue := range selfVc {
		if selfValue != otherVc[i] {
			return false
		}
	}

	return true
}

func (ts ClockSiTimestamp) IsConcurrent(otherTs Timestamp) (compResult bool) {
	//I don't know of a more efficient way to check for concurrency than as it is implemented in Compare.
	return ts.Compare(otherTs) == ConcurrentTs
}

func (ts ClockSiTimestamp) ComparePos(id int16, otherTs Timestamp) (compResult TsResult) {
	tsValue, hasTs := ts.VectorClock[id]
	otherTsValue, hasOtherTs := otherTs.(ClockSiTimestamp).VectorClock[id]
	if !hasTs || !hasOtherTs {
		return ConcurrentTs
	}
	if tsValue == otherTsValue {
		return EqualTs
	}
	if tsValue < otherTsValue {
		return LowerTs
	}
	return HigherTs
}

func (ts ClockSiTimestamp) UpdatePos(id int16, newValue int64) (newTs Timestamp) {
	if ts.VectorClock[id] < newValue {
		ts.VectorClock[id] = newValue
	}
	newTs = ts
	return
}

func (ts ClockSiTimestamp) UpdateForcedPos(id int16, newValue int64) (newTs Timestamp) {
	ts.VectorClock[id] = newValue
	newTs = ts
	return
}

func (ts ClockSiTimestamp) GetPos(id int16) (value int64) {
	return ts.VectorClock[id]
}

func (ts ClockSiTimestamp) Merge(otherTs Timestamp) (mergedTs Timestamp) {
	if otherTs == nil {
		//Just do a copy
		vc := make(map[int16]int64)
		for i, value := range ts.VectorClock {
			vc[i] = value
		}
		return ClockSiTimestamp{VectorClock: vc}
	}
	vc := make(map[int16]int64)
	otherTsVc := otherTs.(ClockSiTimestamp).VectorClock

	for i, value := range ts.VectorClock {
		//Keep the max of each position
		if value > otherTsVc[i] {
			vc[i] = value
		} else {
			vc[i] = otherTsVc[i]
		}
	}

	return ClockSiTimestamp{VectorClock: vc}
}

func (ts ClockSiTimestamp) IsSmallerConcurrent(otherTs Timestamp) (isSmaller bool) {
	//Note: Assuming both are concurrent
	isSmaller = false
	smallestId := int16(math.MaxInt16)
	otherTsVc := otherTs.(ClockSiTimestamp).VectorClock

	for id, value := range ts.VectorClock {
		if id < smallestId {
			if value < otherTsVc[id] {
				smallestId, isSmaller = id, true
			} else if value > otherTsVc[id] {
				smallestId, isSmaller = id, false
			}
		}
	}

	return isSmaller
}

func (ts ClockSiTimestamp) IsDifferent(otherTs Timestamp) bool {
	if otherTs == nil {
		return true
	}
	otherTsVc := otherTs.(ClockSiTimestamp).VectorClock
	for id, value := range ts.VectorClock {
		if value != otherTsVc[id] {
			return true
		}
	}
	return false
}

func (ts ClockSiTimestamp) Update() {
	for _, id := range knownIDs {
		if _, has := ts.VectorClock[id]; !has {
			ts.VectorClock[id] = 0
		}
	}
}

func addToHighestTs(id int16) {
	HighestTs.VectorClock[id] = math.MaxInt64
}

func (ts ClockSiTimestamp) ToBytes() (bytes []byte) {
	/*
		bytes = make([]byte, len(*ts.VectorClock)*entrySize)
		for i, vcEntry := range *ts.VectorClock {
			binary.LittleEndian.PutUint64(bytes[i*entrySize:(i+1)*entrySize], uint64(vcEntry))
		}
		return
	*/
	bytes = make([]byte, len(ts.VectorClock)*2*entrySize)
	nAdded := 0
	for i, vcEntry := range ts.VectorClock {
		binary.LittleEndian.PutUint16(bytes[nAdded*2*entrySize:nAdded*2*entrySize+entrySize], uint16(i))
		binary.LittleEndian.PutUint64(bytes[nAdded*2*entrySize+entrySize:(nAdded+1)*2*entrySize], uint64(vcEntry))
		nAdded++
	}
	return
}

func (ts ClockSiTimestamp) FromBytes(bytes []byte) (newTs Timestamp) {
	//Initial/default timestamp
	/*
		if bytes == nil || len(bytes) == 0 {
			newVC := make([]int64, 1)
			newClockSi := &ClockSiTimestamp{VectorClock: &newVC}
			//Should be unecessary.
			(*newClockSi.VectorClock)[0] = 0
			newTs = *newClockSi
		} else {
			newVC := make([]int64, len(bytes)/8)
			newClockSi := &ClockSiTimestamp{VectorClock: &newVC}
			for i := 0; i < len(newVC); i++ {
				newVC[i] = int64(binary.LittleEndian.Uint64(bytes[i*entrySize : (i+1)*entrySize]))
			}
			newTs = *newClockSi
		}
		return
	*/
	newVC := make(map[int16]int64)
	if bytes == nil || len(bytes) == 0 {
		newVC[0] = 0
	} else {
		nEntries := len(bytes) / (entrySize * 2)
		for i := 0; i < nEntries; i++ {
			replicaID := int16(binary.LittleEndian.Uint16(bytes[i*2*entrySize : (i+1)*2*entrySize-entrySize]))
			value := int64(binary.LittleEndian.Uint64(bytes[i*2*entrySize+entrySize : (i+1)*2*entrySize]))
			newVC[replicaID] = value
		}
	}
	//fmt.Println("Decoded clock:", newVC)
	return ClockSiTimestamp{VectorClock: newVC}
}

func (ts ClockSiTimestamp) ToString() (tsString string) {
	var builder strings.Builder
	builder.WriteString("{[")
	for id, value := range ts.VectorClock {
		builder.WriteString(fmt.Sprint(id))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprint(value))
		builder.WriteString(",")
	}
	builder.WriteString("]}")
	return builder.String()
}

func (ts ClockSiTimestamp) ToSortedString() (tsString string) {
	//Need to ensure this is written in order, since go randomizes map iteration order
	keys := ts.getSortedKeys()
	var builder strings.Builder
	builder.WriteString("{[")
	for _, key := range keys {
		builder.WriteString(fmt.Sprint(key))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprint(ts.VectorClock[key]))
		builder.WriteString(",")
	}
	builder.WriteString("]}")
	return builder.String()
}

//NOTE: If we one day support adding/removing replicas on the fly this will probably no longer work, as it ignores the replica's ID (map key)
func (ts ClockSiTimestamp) GetMapKey() (key TimestampKey) {
	if useByteKey {
		return ts.getMapByteKey()
	}
	/*if useSliceKey {
		return ts.GetMapSliceKey()
	}*/
	return ts.getMapStringKey()
}

//Byte representation. Unused atm, probably no longer needed.
func (ts ClockSiTimestamp) getMapByteKey() (key TimestampKey) {
	//Need to ensure this is written in order, since go randomizes map iteration order
	keys := ts.getSortedKeys()
	byteSlice := make([]byte, len(keys)*8)
	for i, key := range keys {
		binary.BigEndian.PutUint64(byteSlice[i*8:i*8+8], uint64(ts.VectorClock[key]))
	}
	return ByteKey(string(byteSlice))
}

//Slice representation
/*
func (ts ClockSiTimestamp) GetMapSliceKey() (key TimestampKey) {
	//Need to ensure this is written in order, since go randomizes map iteration order
	keys := ts.getSortedKeys()
	intSlice := make([]int64, len(keys))
	for i, key := range keys {
		intSlice[i] = ts.VectorClock[key]
	}
	return SliceKey(intSlice)
}
*/

func (ts ClockSiTimestamp) getMapStringKey() (key TimestampKey) {
	//Need to ensure this is written in order, since go randomizes map iteration order
	keys := ts.getSortedKeys()
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(fmt.Sprint(ts.VectorClock[key], ","))
	}
	return StringKey(builder.String())
}

func (ts ClockSiTimestamp) getSortedKeys() (keys []int16) {
	keys = make([]int16, len(ts.VectorClock))
	i := 0
	for key := range ts.VectorClock {
		keys[i] = key
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return
}

func (ts ClockSiTimestamp) Copy() (copyTs Timestamp) {
	copySiTS := ClockSiTimestamp{VectorClock: make(map[int16]int64)}
	for key, value := range ts.VectorClock {
		copySiTS.VectorClock[key] = value
	}
	return copySiTS
}

func ClockArrayToByteArray(clks []Timestamp) (bytes [][]byte) {
	bytes = make([][]byte, len(clks))
	for i, clk := range clks {
		bytes[i] = clk.ToBytes()
	}
	return
}

func ByteArrayToClockArray(bytes [][]byte) (clks []Timestamp) {
	clks = make([]Timestamp, len(bytes))
	for i, clkBytes := range bytes {
		clks[i] = ClockSiTimestamp{}.FromBytes(clkBytes)
	}
	return
}

/////TIMESTAMPKEY methods//////
/*
func (bk ByteKey) toSliceKey() (sliceKey SliceKey) {
	stringKey := string(bk)
	bytesKey := []byte(stringKey)
	values := make([]int64, len(stringKey)/4)
	for i := 0; i < len(bytesKey); i += 4 {
		values[i/4] = int64(binary.LittleEndian.Uint64(bytesKey[i : i+3]))
	}
	return values
}

func (sk StringKey) toSliceKey() (sliceKey SliceKey) {
	stringKey := string(sk)
	subStrings := strings.Split(stringKey, ",")
	values := make([]int64, len(subStrings))
	for i, value := range subStrings {
		values[i], _ = strconv.ParseInt(value, 10, 64)
	}
	return
}

func (k SliceKey) toSliceKey() (sliceKey SliceKey) {
	return k
}*/

func (bk ByteKey) IsLower(otherKey TimestampKey) bool {
	ourBytes, otherBytes := []byte(bk), []byte(otherKey.(ByteKey))
	i, j := 0, 0
	foundHigher := false
	for ; i < len(ourBytes); i += 4 {
		for j = 0; j < 4; j++ {
			if ourBytes[i+j] > otherBytes[i+j] { //if int1 > int2, then one of the leftmost bytes of int1 will be higher.
				return false
			} else if ourBytes[i+j] < otherBytes[i+j] { //Similarly but with "<" and lower.
				foundHigher = true
				break //Skip to the next number
			}
		}
	}
	return foundHigher
}

func (bk ByteKey) IsEqual(otherKey TimestampKey) bool {
	ourBytes, otherBytes := []byte(bk), []byte(otherKey.(ByteKey))
	for i := 0; i < len(ourBytes); i++ {
		if ourBytes[i] != otherBytes[i] {
			return false //Any byte that is different implies the keys are not equal
		}
	}
	return true
}

func (bk ByteKey) IsHigher(otherKey TimestampKey) bool {
	ourBytes, otherBytes := []byte(bk), []byte(otherKey.(ByteKey))
	i, j := 0, 0
	foundLower := false
	for ; i < len(ourBytes); i += 4 {
		for j = 0; j < 4; j++ {
			if ourBytes[i+j] < otherBytes[i+j] { //if int1 < int2, then one of the leftmost bytes of int1 will be lower.
				return false
			} else if ourBytes[i+j] > otherBytes[i+j] { //Similarly but with ">" and higher.
				foundLower = true
				break //Skip to the next number
			}
		}
	}
	return foundLower
}

func (bk ByteKey) IsLowerOrEqual(otherKey TimestampKey) bool {
	ourBytes, otherBytes := []byte(bk), []byte(otherKey.(ByteKey))
	i, j := 0, 0
	for ; i < len(ourBytes); i += 4 {
		for j = 0; j < 4; j++ {
			if ourBytes[i+j] > otherBytes[i+j] { //if int1 > int2, then one of the leftmost bytes of int1 will be higher.
				return false
			} else if ourBytes[i+j] < otherBytes[i+j] {
				break //Skip to the next number (comparing the rest of the bytes could give a wrong result)
			}
		}
	}
	return true
}

func (sk StringKey) IsLower(otherKey TimestampKey) bool {
	foundHigher := false
	ourCommaPos, otherCommaPos := 0, 0
	ourString, otherString := string(sk), string(otherKey.(StringKey))
	var currOurNumber, currOtherNumber string
	for {
		ourCommaPos, otherCommaPos = strings.IndexRune(ourString, ','), strings.IndexRune(otherString, ',')
		currOurNumber, currOtherNumber = ourString[:ourCommaPos], otherString[:otherCommaPos]
		if len(currOurNumber) > len(currOtherNumber) {
			return false
		} else if len(currOurNumber) < len(currOtherNumber) {
			foundHigher = true
		} else {
			//Same len, can compare bytes.
			for i := 0; i < ourCommaPos; i++ {
				if currOurNumber[i] > currOtherNumber[i] {
					return false
				} else if currOurNumber[i] < currOtherNumber[i] {
					foundHigher = true
					break //Go to next number.
				}
			}
		}
		if ourCommaPos == len(ourString)-1 {
			break //Finished comparing the keys
		}
		ourString, otherString = ourString[ourCommaPos+1:], otherString[otherCommaPos+1:]
	}
	return foundHigher
}

func (sk StringKey) IsEqual(otherKey TimestampKey) bool {
	ourString, otherString := string(sk), string(otherKey.(StringKey))
	if len(ourString) != len(otherString) {
		return false
	}
	//Any byte that is different implies the keys are different
	for i := 0; i < len(ourString); i++ {
		if ourString[i] != otherString[i] {
			return false
		}
	}
	return true
}

func (sk StringKey) IsHigher(otherKey TimestampKey) bool {
	foundLower := false
	ourCommaPos, otherCommaPos := 0, 0
	ourString, otherString := string(sk), string(otherKey.(StringKey))
	var currOurNumber, currOtherNumber string
	for {
		ourCommaPos, otherCommaPos = strings.IndexRune(ourString, ','), strings.IndexRune(otherString, ',')
		currOurNumber, currOtherNumber = ourString[:ourCommaPos], otherString[:otherCommaPos]
		if len(currOurNumber) < len(currOtherNumber) {
			return false
		} else if len(currOurNumber) > len(currOtherNumber) {
			foundLower = true
		} else {
			//Same len, can compare bytes.
			for i := 0; i < ourCommaPos; i++ {
				if currOurNumber[i] > currOtherNumber[i] {
					return false
				} else if currOurNumber[i] < currOtherNumber[i] {
					foundLower = true
					break //Go to next number.
				}
			}
		}
		if ourCommaPos == len(ourString)-1 {
			break //Finished comparing the keys
		}
		ourString, otherString = ourString[ourCommaPos+1:], otherString[otherCommaPos+1:]
	}
	return foundLower
}

func (sk StringKey) IsLowerOrEqual(otherKey TimestampKey) bool {
	foundHigher := false
	ourCommaPos, otherCommaPos := 0, 0
	ourString, otherString := string(sk), string(otherKey.(StringKey))
	var currOurNumber, currOtherNumber string
	for {
		ourCommaPos, otherCommaPos = strings.IndexRune(ourString, ','), strings.IndexRune(otherString, ',')
		currOurNumber, currOtherNumber = ourString[:ourCommaPos], otherString[:otherCommaPos]
		if len(currOurNumber) > len(currOtherNumber) {
			return false
		} else if len(currOurNumber) < len(currOtherNumber) {
			foundHigher = true
		} else {
			//Same len, can compare bytes.
			for i := 0; i < ourCommaPos; i++ {
				if currOurNumber[i] > currOtherNumber[i] {
					return false
				} else if currOurNumber[i] < currOtherNumber[i] {
					break //Skip to the next number (comparing the rest of the digits could give a wrong result.
				}
			}
		}
		if ourCommaPos == len(ourString)-1 {
			break //Finished comparing the keys
		}
		ourString, otherString = ourString[ourCommaPos+1:], otherString[otherCommaPos+1:]
	}
	return foundHigher
}

/*
func (sk SliceKey) IsLower(otherKey TimestampKey) bool {
	return isLowerSlice(sk, otherKey.toSliceKey())
}

func (sk SliceKey) IsEqual(otherKey TimestampKey) bool {
	return isEqualSlice(sk, otherKey.toSliceKey())
}

func (sk SliceKey) IsHigher(otherKey TimestampKey) bool {
	return isHigherSlice(sk, otherKey.toSliceKey())
}

func (sk SliceKey) IsLowerOrEqual(otherKey TimestampKey) bool {
	return isLowerOrEqualSlice(sk, otherKey.toSliceKey())
}
*/

/////TIMESTAMPKEY helper methods/////
/*
func isLowerSlice(selfKey, otherKey SliceKey) bool {
	foundHigher := false
	for i, selfValue := range selfKey {
		if selfValue > otherKey[i] {
			return false
		} else if selfValue < otherKey[i] {
			foundHigher = true
		}
	}
	return foundHigher
}

func isEqualSlice(selfKey, otherKey SliceKey) bool {
	for i, selfValue := range selfKey {
		if selfValue != otherKey[i] {
			return false
		}
	}
	return true
}

func isHigherSlice(selfKey, otherKey SliceKey) bool {
	foundLower := false
	for i, selfValue := range selfKey {
		if selfValue < otherKey[i] {
			return false
		} else if selfValue > otherKey[i] {
			foundLower = true
		}
	}
	return foundLower
}

func isLowerOrEqualSlice(selfKey, otherKey SliceKey) bool {
	for i, selfValue := range selfKey {
		if selfValue > otherKey[i] {
			return false
		}
	}
	//At this point, it is either equal or lower.
	return true
}
*/
