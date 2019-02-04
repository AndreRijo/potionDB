package clocksi

import (
	"encoding/binary"
	"time"
)

type Timestamp struct {
	timestamp int64
}

func NextTimestamp() (timestamp Timestamp) {
	timestamp = Timestamp{timestamp: time.Now().Unix()}
	return
}

func (ts Timestamp) ToBytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(ts.timestamp))
	return
}

func FromBytes(bytes []byte) (ts Timestamp) {
	if bytes == nil || len(bytes) == 0 {
		ts = Timestamp{timestamp: 0}
	} else {
		ts = Timestamp{timestamp: int64(binary.LittleEndian.Uint64(bytes))}
	}
	return
}
