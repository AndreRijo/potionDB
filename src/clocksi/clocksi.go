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
	NewTimestamp() (newTs Timestamp)
	//Gets a timestamp that is more recent than the actual one
	NextTimestamp() (newTs Timestamp)
	//Compares two timestamps. Returns HigherTs if the current timestamp is higher than otherTs. Others should be self explanatory.
	//Possible results: HigherTs/LowerTs/EqualTs/ConcurrentTs
	Compare(otherTs Timestamp) (compResult TsResult)
	//Returns true when Compare(otherTs) would return HigherTs or EqualTs.
	//In some implementations (e.g., vectorClocks) and in some situations, this may be more efficient than Compare.
	IsHigherOrEqual(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return LowerTs or EqualTs.
	//In some implementations (e.g., vectorClocks) and in some situations, this may be more efficient than Compare.
	IsLowerOrEqual(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return LowerTs.
	//In some implementations (e.g., vectorClocks) and in some situations, this may be more efficient than Compare.
	IsLower(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return HigherTs.
	//In some implementations (e.g., vectorClocks) and in some situations, this may be more efficient than Compare.
	IsHigher(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return EqualTs.
	//In some implementations (e.g., vectorClocks) and in some situations, this may be more efficient than Compare.
	IsEqual(otherTs Timestamp) (compResult bool)
	//Returns true when Compare(otherTs) would return ConcurrentTs.
	IsConcurrent(otherTs Timestamp) (compResult bool)
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
}

type ClockSiTimestamp struct {
	//vectorClock *[]int64
	vectorClock map[int64]int64 //replicaID to clock value
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
	entrySize = 8
)

var (
	//Useful for comparations or other situations in which we need a temporary, non-significant, timestamp
	DummyTs = NewClockSiTimestamp()
	//Useful for situations in which we need a timestamp bigger than all others
	DummyHighTs = prepareDummyHighTs()
)

func prepareDummyHighTs() (ts Timestamp) {
	vc := make(map[int64]int64)
	clockSiTs := ClockSiTimestamp{vectorClock: vc}
	clockSiTs.vectorClock[0] = math.MaxInt64
	return clockSiTs
}

//Creates a new timestamp. This gives the same result as doing: newTs = ClockSiTimestamp{}.NewTimestamp().
//Use whichever option feels more natural.
func NewClockSiTimestamp() (ts Timestamp) {
	return ClockSiTimestamp{}.NewTimestamp()
}

func (ts ClockSiTimestamp) NewTimestamp() (newTs Timestamp) {
	vc := make(map[int64]int64)
	vc[0] = 0
	ts = ClockSiTimestamp{vectorClock: vc}
	return ts
}

func (ts ClockSiTimestamp) NextTimestamp() (newTs Timestamp) {
	//TODO: Actually update the correct position
	newVc := make(map[int64]int64)
	newVc[0] = time.Now().UTC().UnixNano()
	return ClockSiTimestamp{vectorClock: newVc}

}

func (ts ClockSiTimestamp) Compare(otherTs Timestamp) (compResult TsResult) {
	if otherTs == nil {
		compResult = HigherTs
		return
	}

	//TODO: Handle vectorClocks with different sizes...?
	otherVc := otherTs.(ClockSiTimestamp).vectorClock
	selfVc := ts.vectorClock
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

	//TODO: Handle vectorClocks with different sizes...?
	otherVc := otherTs.(ClockSiTimestamp).vectorClock
	selfVc := ts.vectorClock

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

	//TODO: Handle vectorClocks with different sizes...?
	otherVc := otherTs.(ClockSiTimestamp).vectorClock
	selfVc := ts.vectorClock
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

	//TODO: Handle vectorClocks with different sizes...?
	otherVc := otherTs.(ClockSiTimestamp).vectorClock
	selfVc := ts.vectorClock

	for i, selfValue := range selfVc {
		if selfValue > otherVc[i] {
			return false
		}
	}

	//If we reach this point, then it is equal or lower. Doesn't matter which.
	return true
}

func (ts ClockSiTimestamp) IsLower(otherTs Timestamp) (compResult bool) {
	if otherTs == nil {
		return false
	}

	//TODO: Handle vectorClocks with different sizes...?
	otherVc := otherTs.(ClockSiTimestamp).vectorClock
	selfVc := ts.vectorClock
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

	otherVc := otherTs.(ClockSiTimestamp).vectorClock
	selfVc := ts.vectorClock

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

func (ts ClockSiTimestamp) Merge(otherTs Timestamp) (mergedTs Timestamp) {
	vc := make(map[int64]int64)
	otherTsVc := otherTs.(ClockSiTimestamp).vectorClock

	for i, value := range ts.vectorClock {
		//Keep the max of each position
		if value > otherTsVc[i] {
			vc[i] = value
		} else {
			vc[i] = otherTsVc[i]
		}
	}

	return ClockSiTimestamp{vectorClock: vc}
}

func (ts ClockSiTimestamp) ToBytes() (bytes []byte) {
	/*
		bytes = make([]byte, len(*ts.vectorClock)*entrySize)
		for i, vcEntry := range *ts.vectorClock {
			binary.LittleEndian.PutUint64(bytes[i*entrySize:(i+1)*entrySize], uint64(vcEntry))
		}
		return
	*/
	bytes = make([]byte, len(ts.vectorClock)*2*entrySize)
	for i, vcEntry := range ts.vectorClock {
		binary.LittleEndian.PutUint64(bytes[i*2*entrySize:i*2*entrySize+entrySize], uint64(i))
		binary.LittleEndian.PutUint64(bytes[i*2*entrySize+entrySize:(i+1)*2*entrySize], uint64(vcEntry))
	}
	return
}

func (ts ClockSiTimestamp) FromBytes(bytes []byte) (newTs Timestamp) {
	//Initial/default timestamp
	/*
		if bytes == nil || len(bytes) == 0 {
			newVC := make([]int64, 1)
			newClockSi := &ClockSiTimestamp{vectorClock: &newVC}
			//Should be unecessary.
			(*newClockSi.vectorClock)[0] = 0
			newTs = *newClockSi
		} else {
			newVC := make([]int64, len(bytes)/8)
			newClockSi := &ClockSiTimestamp{vectorClock: &newVC}
			for i := 0; i < len(newVC); i++ {
				newVC[i] = int64(binary.LittleEndian.Uint64(bytes[i*entrySize : (i+1)*entrySize]))
			}
			newTs = *newClockSi
		}
		return
	*/
	newVC := make(map[int64]int64)
	if bytes == nil || len(bytes) == 0 {
		newVC[0] = 0
	} else {
		nEntries := len(bytes) / (entrySize * 2)
		for i := 0; i < nEntries; i++ {
			replicaID := int64(binary.LittleEndian.Uint64(bytes[i*2*entrySize : (i+1)*2*entrySize-entrySize]))
			value := int64(binary.LittleEndian.Uint64(bytes[i*2*entrySize+entrySize : (i+1)*2*entrySize]))
			newVC[replicaID] = value
		}
	}
	return ClockSiTimestamp{vectorClock: newVC}
}

func (ts ClockSiTimestamp) ToString() (tsString string) {
	var builder strings.Builder
	builder.WriteString("{[")
	for id, value := range ts.vectorClock {
		builder.WriteString(fmt.Sprint(id))
		builder.WriteString(":")
		builder.WriteString(fmt.Sprint(value))
		builder.WriteString(",")
	}
	builder.WriteString("]}")
	return builder.String()
}

//TODO: If we one day support adding/removing replicas on the fly this will probably no longer work, as it ignores the replica's ID (map key)
func (ts ClockSiTimestamp) GetMapKey() (key TimestampKey) {
	var builder strings.Builder
	for _, value := range ts.vectorClock {
		builder.WriteString(fmt.Sprint(value, ","))
	}
	return builder.String()
}
