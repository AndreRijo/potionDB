package clocksi

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

type Timestamp interface {
	//Creates a new timestamp representing the initial value.
	NewTimestamp(id int16) (newTs Timestamp)
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
	//Returns the timestamp resulting of merging this timestamp and the argument timestamp
	//In a vector clock, it represents keeping the highest value of each position
	Merge(otherTs Timestamp) (mergedTs Timestamp)
	//Converts the timestamp to a byte array
	ToBytes() (bytes []byte)
	//Gets the timestamp that is represented in the byte array
	//Note: This method is safe to call on an empty Timestamp instance.
	FromBytes(bytes []byte) (newTs Timestamp)
	//Useful for debugging purposes
	ToString() (tsString string)
	//Gets a representation of this clock that is safe to use in GO maps
	GetMapKey() (key TimestampKey)
	//Performs a deep copy of the current timestamp and returns the copy
	Copy() (copyTs Timestamp)
}

type ClockSiTimestamp struct {
	//VectorClock *[]int64
	VectorClock map[int16]int64 //replicaID to clock value
}

type TsResult int

type TimestampKey interface{}

const (
	//Common to all clocks
	EqualTs      TsResult = 0
	HigherTs     TsResult = 1
	LowerTs      TsResult = 2
	ConcurrentTs TsResult = 3

	//Specific to clocksi implementation
	entrySize    = 8
	randomFactor = 500 //max value that can be added to the timestamp to avoid collisions
)

var (
	//Useful for comparations or other situations in which we need a temporary, non-significant, timestamp
	DummyTs = NewClockSiTimestamp(0)
)

func prepareDummyHighTs() (ts Timestamp) {
	vc := make(map[int16]int64)
	clockSiTs := ClockSiTimestamp{VectorClock: vc}
	clockSiTs.VectorClock[0] = math.MaxInt64
	return clockSiTs
}

//Creates a new timestamp. This gives the same result as doing: newTs = ClockSiTimestamp{}.NewTimestamp().
//Use whichever option feels more natural.
func NewClockSiTimestamp(id int16) (ts Timestamp) {
	return ClockSiTimestamp{}.NewTimestamp(id)
}

func (ts ClockSiTimestamp) NewTimestamp(id int16) (newTs Timestamp) {
	vc := make(map[int16]int64)
	vc[id] = 0
	ts = ClockSiTimestamp{VectorClock: vc}
	return ts
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

//NOTE: If we one day support adding/removing replicas on the fly this will probably no longer work, as it ignores the replica's ID (map key)
func (ts ClockSiTimestamp) GetMapKey() (key TimestampKey) {
	var builder strings.Builder
	for _, value := range ts.VectorClock {
		builder.WriteString(fmt.Sprint(value, ","))
	}
	return builder.String()
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
