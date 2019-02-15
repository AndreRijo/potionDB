package clocksi

import (
	"encoding/binary"
	"fmt"
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
	//Converts the timestamp to a byte array
	ToBytes() (bytes []byte)
	//Gets the timestamp that is represented in the byte array
	//Note: This method is safe to call on an empty Timestamp instance.
	FromBytes(bytes []byte) (newTs Timestamp)
	//Useful for debugging purposes
	ToString() (tsString string)
}

type ClockSiTimestamp struct {
	vectorClock *[]int64
}

type TsResult int

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
)

//Creates a new timestamp. This gives the same result as doing: newTs = ClockSiTimestamp{}.NewTimestamp().
//Use whichever option feels more natural.
func NewClockSiTimestamp() (ts Timestamp) {
	return ClockSiTimestamp{}.NewTimestamp()
}

func (ts ClockSiTimestamp) NewTimestamp() (newTs Timestamp) {
	vc := make([]int64, 1)
	ts = ClockSiTimestamp{vectorClock: &vc}
	(*ts.vectorClock)[0] = 0
	return ts
}

func (ts ClockSiTimestamp) NextTimestamp() (newTs Timestamp) {
	//TODO: Actually update the correct position
	newVc := make([]int64, 1)
	newVc[0] = time.Now().UTC().UnixNano()
	newTs = ClockSiTimestamp{vectorClock: &newVc}
	return
}

func (ts ClockSiTimestamp) Compare(otherTs Timestamp) (compResult TsResult) {
	if otherTs == nil {
		compResult = HigherTs
		return
	}

	//TODO: Handle vectorClocks with different sizes...?
	otherVc := *otherTs.(ClockSiTimestamp).vectorClock
	selfVc := *ts.vectorClock
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
	otherVc := *otherTs.(ClockSiTimestamp).vectorClock
	selfVc := *ts.vectorClock

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
	otherVc := *otherTs.(ClockSiTimestamp).vectorClock
	selfVc := *ts.vectorClock
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
	otherVc := *otherTs.(ClockSiTimestamp).vectorClock
	selfVc := *ts.vectorClock

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
	otherVc := *otherTs.(ClockSiTimestamp).vectorClock
	selfVc := *ts.vectorClock
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

	otherVc := *otherTs.(ClockSiTimestamp).vectorClock
	selfVc := *ts.vectorClock

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

func (ts ClockSiTimestamp) ToBytes() (bytes []byte) {
	bytes = make([]byte, len(*ts.vectorClock)*entrySize)
	for i, vcEntry := range *ts.vectorClock {
		binary.LittleEndian.PutUint64(bytes[i*entrySize:(i+1)*entrySize], uint64(vcEntry))
	}
	return
}

func (ts ClockSiTimestamp) FromBytes(bytes []byte) (newTs Timestamp) {
	//Initial/default timestamp
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
}

func (ts ClockSiTimestamp) ToString() (tsString string) {
	var builder strings.Builder
	builder.WriteString("{[")
	for _, value := range *ts.vectorClock {
		builder.WriteString(fmt.Sprint(value) + ",")
	}
	builder.WriteString("]}")
	return builder.String()
}
