//A simple clock that can be used for local debugging. It simply uses the system's clock as a timestamp.

package clocksi

import (
	"encoding/binary"
	"time"
)

type DumbTimestamp struct {
	timestamp int64
}

//Creates a new timestamp. This gives the same result as doing: newTs = DumbTimestamp{}.NewTimestamp().
//Use whichever option feels more natural.
func NewDumbTimestamp() (ts Timestamp) {
	return DumbTimestamp{}.NewTimestamp()
}

func (ts DumbTimestamp) NewTimestamp() (newTs Timestamp) {
	return DumbTimestamp{timestamp: 0}
}

func (ts DumbTimestamp) NextTimestamp() (newTs Timestamp) {
	return DumbTimestamp{timestamp: time.Now().Unix()}
}

func (ts DumbTimestamp) Compare(otherTs Timestamp) (compResult TsResult) {
	if otherTs == nil {
		compResult = HigherTs
		return
	}

	otherDumbTs := otherTs.(DumbTimestamp).timestamp
	if ts.timestamp > otherDumbTs {
		compResult = HigherTs
	} else if ts.timestamp < otherDumbTs {
		compResult = LowerTs
	} else {
		compResult = EqualTs
	}
	return
}

func (ts DumbTimestamp) IsHigherOrEqual(otherTs Timestamp) (compResult bool) {
	result := ts.Compare(otherTs)
	return result == HigherTs || result == EqualTs
}

func (ts DumbTimestamp) IsLowerOrEqual(otherTs Timestamp) (compResult bool) {
	result := ts.Compare(otherTs)
	return result == LowerTs || result == EqualTs
}

func (ts DumbTimestamp) IsLower(otherTs Timestamp) (compResult bool) {
	result := ts.Compare(otherTs)
	return result == LowerTs
}

func (ts DumbTimestamp) IsHigher(otherTs Timestamp) (compResult bool) {
	result := ts.Compare(otherTs)
	return result == HigherTs
}

func (ts DumbTimestamp) IsEqual(otherTs Timestamp) (compResult bool) {
	result := ts.Compare(otherTs)
	return result == EqualTs
}

func (ts DumbTimestamp) IsConcurrent(otherTs Timestamp) (compResult bool) {
	result := ts.Compare(otherTs)
	return result == ConcurrentTs
}

func (ts DumbTimestamp) ToBytes() (bytes []byte) {
	internalTs := ts.timestamp
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(internalTs))
	return
}

func (ts DumbTimestamp) FromBytes(bytes []byte) (newTs Timestamp) {
	if bytes == nil || len(bytes) == 0 {
		newTs = DumbTimestamp{timestamp: 0}
	} else {
		newTs = DumbTimestamp{timestamp: int64(binary.LittleEndian.Uint64(bytes))}
	}
	return
}
