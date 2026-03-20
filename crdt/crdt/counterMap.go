package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	"github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

// IMPORTANT NOTE/possible TODO: Adding a large data set is innefficient if it is added by parts, as a check for existance is required for every key.
// This could be optimized (for the case of data initialization) by defining an initialization method, where we assume all keys are non-existing.
// This is in similar fashion to what is done in, e.g., RWEmbMapCRDT.

// Note on the experience of making a generic CRDT:
// Don't do it :)))))
// It's a pain when it comes to protobufs and specially... when deciding to instantiate all the reads, updates, downstreams and such.
// Need a single case per data type and... then for each data type a switch for each type of upd/down/read... it doesn't scale nicely.

// Map focused on read efficiency. It keeps a slice cache of the values for quick read access (maps are slow).
// However, updates have to access both the map and slice, not to mention this has a higher memory footprint.
// Quick is actively maintained updated.
// This map does not support removals, as this is often not necessary in inc/dec/sum environments.
// Namely, all TPC-H queries that use this CRDT do not need it. It would however be possible to support so, e.g., with LWW timestamps.
type CounterMapCrdt[T SignedNumber] struct {
	CRDTVM
	entries map[int32]int
	quick   tools.SliceWithCounter[KeyCounterPair[T]]
	data    tools.SliceWithCounter[[]byte] //Optional extra data, similarly to in TopKs.
	sumData int                            //Counter with the size (in bytes) of data. For instances of CounterMapCrdts that never use data, this will be 0, thus avoiding some read overhead.
}

type KeyCounterPair[T SignedNumber] struct {
	Key   int32
	Value T
}

//States

type CounterMapState[T SignedNumber] struct {
	Pairs []KeyCounterPair[T]
	Data  [][]byte
}

type CounterMapSingleState[T SignedNumber] struct {
	Value T
	Data  []byte
}
type CounterMapHasKeyState[T SignedNumber] bool

type CounterMapKeysState[T SignedNumber] []int32 //Keys

type CounterMapKeysDataState[T SignedNumber] struct {
	Keys []int32
	Data [][]byte
}

type CounterMapValuesDataState[T SignedNumber] struct {
	Values []T
	Data   [][]byte
}

//Queries

type CounterMapGetValueArguments[T SignedNumber] int32 //Key

type CounterMapHasKeyArguments[T SignedNumber] int32 //Key

type CounterMapGetKeysArguments[T SignedNumber] struct{}

type CounterMapGetValuesArguments[T SignedNumber] []int32 //Keys. The reply uses CounterMapState.

type CounterMapCompareAllArguments[T SignedNumber] struct { //The reply uses CounterMapState, CounterMapKeysDataState or CounterMapValuesDataState.
	CompareArguments
	GetKey, GetValue, GetData bool
}

//Updates

// Sets size of the Map. The map can grow beyond this size. Useful for big maps or performance-critic maps.
type CounterMapInit[T SignedNumber] int32
type CounterMapInc[T SignedNumber] struct {
	Change T
	Key    int32
	Data   []byte
}

type CounterMapDec[T SignedNumber] struct {
	Change T
	Key    int32
	Data   []byte
}

type CounterMapIncAll[T SignedNumber] struct {
	Keys   []int32
	Change T
	Data   []byte
}

type CounterMapDecAll[T SignedNumber] struct {
	Keys   []int32
	Change T
	Data   []byte
}

type CounterMapIncMult[T SignedNumber] struct {
	Keys   []int32
	Change []T
	Data   [][]byte
}

type CounterMapDecMult[T SignedNumber] struct {
	Keys   []int32
	Change []T
	Data   [][]byte
}

// Effects
type CounterMapNewEffect[T SignedNumber] struct {
	Key    int32
	Change T
	Data   []byte
}

// Same for inc and dec.
type CounterMapIncEffect[T SignedNumber] struct {
	Pos    int32
	Change T
}

// Implementation note: Pos may be empty (if there was no keys before); NewKeys may be empty (if there's no new key)
type CounterMapIncAllEffect[T SignedNumber] struct {
	Pos        []int32
	NKeysAdded int32 //Note: keys are always added to the end of the slice. Furthermore, effects are undone sequentially.
	Change     T
	Data       []byte
}

// Special effect for IncAll/IncMult that is used when the number of entries received far outnumbers the existing ones.
// In this case, the positions of each key will get changed.
type CounterMapIncReplaceAllEffect[T SignedNumber] struct {
	OldEntries map[int32]int
	OldQuick   tools.SliceWithCounter[KeyCounterPair[T]]
	OldData    tools.SliceWithCounter[[]byte]
	OldSumData int
}

type CounterMapIncMultEffect[T SignedNumber] struct {
	Pos        []int32
	NKeysAdded int32 //Note: keys are always added to the end of the slice. Furthermore, effects are undone sequentially.
	//PosChanges, NewKeysChanges []T
	Modifier T //-1 for dec, 1 for inc.
	Changes  []T
	Data     [][]byte
}

//Downstream operations
//Uses directly Updates.

//Operation effects for inversibleCRDT
//TODO :)

func (crdt *CounterMapCrdt[T]) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAP_COUNTER }
func (crdt *CounterMapCrdt[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }

// Upds
func (args CounterMapInit[T]) GetCRDTType() proto.CRDTType    { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapInit[T]) GetDATAType() proto.DATAType    { return counterMapDataType[T]() }
func (args CounterMapInc[T]) GetCRDTType() proto.CRDTType     { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapInc[T]) GetDATAType() proto.DATAType     { return counterMapDataType[T]() }
func (args CounterMapDec[T]) GetCRDTType() proto.CRDTType     { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapDec[T]) GetDATAType() proto.DATAType     { return counterMapDataType[T]() }
func (args CounterMapIncAll[T]) GetCRDTType() proto.CRDTType  { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapIncAll[T]) GetDATAType() proto.DATAType  { return counterMapDataType[T]() }
func (args CounterMapDecAll[T]) GetCRDTType() proto.CRDTType  { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapDecAll[T]) GetDATAType() proto.DATAType  { return counterMapDataType[T]() }
func (args CounterMapIncMult[T]) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapIncMult[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }
func (args CounterMapDecMult[T]) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapDecMult[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }

// Downstreams
func (args CounterMapInit[T]) MustReplicate() bool    { return true }
func (args CounterMapInc[T]) MustReplicate() bool     { return true }
func (args CounterMapDec[T]) MustReplicate() bool     { return true }
func (args CounterMapIncAll[T]) MustReplicate() bool  { return true }
func (args CounterMapDecAll[T]) MustReplicate() bool  { return true }
func (args CounterMapIncMult[T]) MustReplicate() bool { return true }
func (args CounterMapDecMult[T]) MustReplicate() bool { return true }

// States
func (args CounterMapState[T]) GetCRDTType() proto.CRDTType       { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapState[T]) GetREADType() proto.READType       { return proto.READType_FULL }
func (args CounterMapState[T]) GetDATAType() proto.DATAType       { return counterMapDataType[T]() }
func (args CounterMapSingleState[T]) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapSingleState[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_VALUE
}
func (args CounterMapSingleState[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }
func (args CounterMapHasKeyState[T]) GetCRDTType() proto.CRDTType { return proto.CRDTType_MAP_COUNTER }
func (args CounterMapHasKeyState[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_HAS_KEY
}
func (args CounterMapHasKeyState[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }
func (args CounterMapKeysState[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapKeysState[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_KEYS
}
func (args CounterMapKeysState[T]) GetDATAType() proto.DATAType {
	return counterMapDataType[T]()
}
func (args CounterMapKeysDataState[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapKeysDataState[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_COMP
}
func (args CounterMapKeysDataState[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }
func (args CounterMapValuesDataState[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapValuesDataState[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_COMP
}
func (args CounterMapValuesDataState[T]) GetDATAType() proto.DATAType { return counterMapDataType[T]() }

// Queries
func (args CounterMapGetValueArguments[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapGetValueArguments[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_VALUE
}
func (args CounterMapGetValueArguments[T]) GetDATAType() proto.DATAType {
	return counterMapDataType[T]()
}
func (args CounterMapHasKeyArguments[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapHasKeyArguments[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_HAS_KEY
}
func (args CounterMapHasKeyArguments[T]) GetDATAType() proto.DATAType {
	return counterMapDataType[T]()
}
func (args CounterMapGetKeysArguments[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapGetKeysArguments[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_KEYS
}
func (args CounterMapGetKeysArguments[T]) GetDATAType() proto.DATAType {
	return counterMapDataType[T]()
}
func (args CounterMapGetValuesArguments[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapGetValuesArguments[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_VALUES
}
func (args CounterMapGetValuesArguments[T]) GetDATAType() proto.DATAType {
	return counterMapDataType[T]()
}
func (args CounterMapCompareAllArguments[T]) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MAP_COUNTER
}
func (args CounterMapCompareAllArguments[T]) GetREADType() proto.READType {
	return proto.READType_MAP_COUNTER_COMP
}
func (args CounterMapCompareAllArguments[T]) GetDATAType() proto.DATAType {
	return counterMapDataType[T]()
}
func (args CounterMapGetValueArguments[T]) HasInnerReads() bool   { return false }
func (args CounterMapHasKeyArguments[T]) HasInnerReads() bool     { return false }
func (args CounterMapGetKeysArguments[T]) HasInnerReads() bool    { return false }
func (args CounterMapGetValuesArguments[T]) HasInnerReads() bool  { return false }
func (args CounterMapCompareAllArguments[T]) HasInnerReads() bool { return false }
func (args CounterMapGetValueArguments[T]) HasVariables() bool    { return false }
func (args CounterMapHasKeyArguments[T]) HasVariables() bool      { return false }
func (args CounterMapGetKeysArguments[T]) HasVariables() bool     { return false }
func (args CounterMapGetValuesArguments[T]) HasVariables() bool   { return false }
func (args CounterMapCompareAllArguments[T]) HasVariables() bool  { return false }

// Used by InitializeCrdt method to create the correct instance of counterMap.
func initializeCounterMap(dataType proto.DATAType, replicaID uint16) (newCrdt CRDT) {
	switch dataType {
	case proto.DATAType_INT, proto.DATAType_DEFAULT:
		newCrdt = (&CounterMapCrdt[int]{}).Initialize(nil, replicaID)
	case proto.DATAType_FLOAT64:
		newCrdt = (&CounterMapCrdt[float64]{}).Initialize(nil, replicaID)
	case proto.DATAType_INT8:
		newCrdt = (&CounterMapCrdt[int8]{}).Initialize(nil, replicaID)
	case proto.DATAType_INT16:
		newCrdt = (&CounterMapCrdt[int16]{}).Initialize(nil, replicaID)
	case proto.DATAType_INT32:
		newCrdt = (&CounterMapCrdt[int32]{}).Initialize(nil, replicaID)
	case proto.DATAType_INT64:
		newCrdt = (&CounterMapCrdt[int64]{}).Initialize(nil, replicaID)
	case proto.DATAType_FLOAT32:
		newCrdt = (&CounterMapCrdt[float32]{}).Initialize(nil, replicaID)
	default:
		fmt.Printf("[CRDT][CounterMap]Unsupported data type for CounterMap: %v.\n", proto.DATAType_name[int32(dataType)])
	}
	return newCrdt
}

func counterMapDataType[T SignedNumber]() proto.DATAType {
	var dummy T
	switch any(dummy).(type) {
	case int64:
		return proto.DATAType_INT64
	case int32:
		return proto.DATAType_INT32
	case float64:
		return proto.DATAType_FLOAT64
	case int:
		return proto.DATAType_INT
	case int16:
		return proto.DATAType_INT16
	case int8:
		return proto.DATAType_INT8
	default:
		return proto.DATAType_DEFAULT
	}
}

func isFloatDataType[T SignedNumber]() bool {
	var dummy T
	switch any(dummy).(type) {
	case float64, float32:
		return true
	default:
		return false
	}
}

func (crdt *CounterMapCrdt[T]) Initialize(startTs *clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	crdt = &CounterMapCrdt[T]{entries: make(map[int32]int, 1), quick: tools.NewSliceWithCounter[KeyCounterPair[T]](0)}
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *CounterMapCrdt[T]) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID uint16) (sameCRDT *CounterMapCrdt[T]) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *CounterMapCrdt[T]) IsBigCRDT() bool { return len(crdt.entries) >= 1000 } //For reading purposes it is like an array.

func (crdt *CounterMapCrdt[T]) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArg := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case CounterMapGetValueArguments[T]:
		return crdt.getValue(int32(typedArg), updsNotYetApplied)
	case CounterMapGetValuesArguments[T]:
		return crdt.getValues([]int32(typedArg), updsNotYetApplied)
	case CounterMapCompareAllArguments[T]:
		return crdt.getCondAllState(typedArg.CompareArguments, updsNotYetApplied, typedArg.GetKey, typedArg.GetValue, typedArg.GetData)
	case CounterMapHasKeyArguments[T]:
		return crdt.hasKey(int32(typedArg), updsNotYetApplied)
	case CounterMapGetKeysArguments[T]:
		return crdt.getKeys(updsNotYetApplied)
	default:
		fmt.Printf("[CounterMapCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *CounterMapCrdt[T]) getState(updsNotYetApplied []UpdateArguments) (state CounterMapState[T]) {
	if len(updsNotYetApplied) == 0 {
		state.Pairs = make([]KeyCounterPair[T], crdt.quick.Len())
		copy(state.Pairs, crdt.quick.ToSlice())
		if len(state.Data) > 0 {
			state.Data = make([][]byte, crdt.quick.Len())
			copy(state.Data, crdt.data.ToSlice())
		}
		return state
	}
	//Let's optimistically assume no new keys are added (most common use case is similar to TopSum: set of starting entries, and then incs/decs on it)
	copySlice := crdt.quick.Copy()
	dataSlice := crdt.data.Copy()
	dataSize := crdt.sumData
	extraMap := make(map[int32]int)
	var pos int
	var has bool
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterMapInc[T]:
			pos, has = crdt.entries[typedUpd.Key]
			if has {
				copySlice.Slice[pos].Value += typedUpd.Change
			} else if pos, has = extraMap[typedUpd.Key]; has { //"New" key that was already added by a previous upd in updsNotYetApplied
				copySlice.Slice[pos].Value += typedUpd.Change
			} else {
				newPos := copySlice.Len()
				copySlice.Append(KeyCounterPair[T]{Key: typedUpd.Key, Value: typedUpd.Change})
				extraMap[typedUpd.Key] = newPos
				dataSlice.Append(typedUpd.Data)
				dataSize += len(typedUpd.Data)
			}
		case CounterMapDec[T]:
			pos, has = crdt.entries[typedUpd.Key]
			if has {
				copySlice.Slice[pos].Value -= typedUpd.Change
			} else if pos, has = extraMap[typedUpd.Key]; has { //"New" key that was already added by a previous upd in updsNotYetApplied
				copySlice.Slice[pos].Value -= typedUpd.Change
			} else {
				newPos := copySlice.Len()
				copySlice.Append(KeyCounterPair[T]{Key: typedUpd.Key, Value: -typedUpd.Change})
				extraMap[typedUpd.Key] = newPos
				dataSlice.Append(typedUpd.Data)
				dataSize += len(typedUpd.Data)
			}
		case CounterMapIncAll[T]:
			for _, key := range typedUpd.Keys {
				pos, has = crdt.entries[key]
				if has {
					copySlice.Slice[pos].Value += typedUpd.Change
				} else if pos, has = extraMap[key]; has { //"New" key that was already added by a previous upd in updsNotYetApplied
					copySlice.Slice[pos].Value += typedUpd.Change
				} else {
					newPos := copySlice.Len()
					copySlice.Append(KeyCounterPair[T]{Key: key, Value: typedUpd.Change})
					extraMap[key] = newPos
					dataSlice.Append(typedUpd.Data)
					dataSize += len(typedUpd.Data)
				}
			}
		case CounterMapDecAll[T]:
			for _, key := range typedUpd.Keys {
				pos, has = crdt.entries[key]
				if has {
					copySlice.Slice[pos].Value -= typedUpd.Change
				} else if pos, has = extraMap[key]; has { //"New" key that was already added by a previous upd in updsNotYetApplied
					copySlice.Slice[pos].Value -= typedUpd.Change
				} else {
					newPos := copySlice.Len()
					copySlice.Append(KeyCounterPair[T]{Key: key, Value: -typedUpd.Change})
					extraMap[key] = newPos
					dataSlice.Append(typedUpd.Data)
					dataSize += len(typedUpd.Data)
				}
			}
		case CounterMapIncMult[T]:
			for i, key := range typedUpd.Keys {
				pos, has = crdt.entries[key]
				if has {
					copySlice.Slice[pos].Value += typedUpd.Change[i]
				} else if pos, has = extraMap[key]; has { //"New" key that was already added by a previous upd in updsNotYetApplied
					copySlice.Slice[pos].Value += typedUpd.Change[i]
				} else {
					newPos := copySlice.Len()
					copySlice.Append(KeyCounterPair[T]{Key: key, Value: typedUpd.Change[i]})
					extraMap[key] = newPos
					dataSlice.Append(typedUpd.Data[i])
					dataSize += len(typedUpd.Data[i])
				}
			}
		case CounterMapDecMult[T]:
			for i, key := range typedUpd.Keys {
				pos, has = crdt.entries[key]
				if has {
					copySlice.Slice[pos].Value -= typedUpd.Change[i]
				} else if pos, has = extraMap[key]; has { //"New" key that was already added by a previous upd in updsNotYetApplied
					copySlice.Slice[pos].Value -= typedUpd.Change[i]
				} else {
					newPos := copySlice.Len()
					copySlice.Append(KeyCounterPair[T]{Key: key, Value: -typedUpd.Change[i]})
					extraMap[key] = newPos
					dataSlice.Append(typedUpd.Data[i])
					dataSize += len(typedUpd.Data[i])
				}
			}
		}
	}
	if dataSize > 0 {
		return CounterMapState[T]{Pairs: copySlice.ToSlice(), Data: dataSlice.ToSlice()}
	} else {
		return CounterMapState[T]{Pairs: copySlice.ToSlice()}
	}
}

func (crdt *CounterMapCrdt[T]) getValue(key int32, updsNotYetApplied []UpdateArguments) (state CounterMapSingleState[T]) {
	if len(updsNotYetApplied) == 0 {
		pos, exists := crdt.entries[key]
		var value T
		if !exists {
			return CounterMapSingleState[T]{Value: value}
		} else if len(crdt.data.Slice[pos]) > 0 {
			return CounterMapSingleState[T]{Value: crdt.quick.Slice[pos].Value, Data: crdt.data.Slice[pos]}
		}
		return CounterMapSingleState[T]{Value: crdt.quick.Slice[pos].Value}
	}

	//Go through updsNotYetApplied. We only care about a specific key, so we will cache that value only (and ignore all updates not to it).
	pos, exists := crdt.entries[key]
	var value T //Automatically initialized to 0.
	var data []byte
	if exists {
		value, data = crdt.quick.Slice[pos].Value, crdt.data.Slice[pos]
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterMapInc[T]:
			if typedUpd.Key == key {
				value += typedUpd.Change
				if len(data) == 0 {
					data = typedUpd.Data
				}
			}
		case CounterMapDec[T]:
			if typedUpd.Key == key {
				value -= typedUpd.Change
				if len(data) == 0 {
					data = typedUpd.Data
				}
			}
		case CounterMapIncAll[T]:
			for _, updKey := range typedUpd.Keys {
				if updKey == key {
					value += typedUpd.Change
					if len(data) == 0 {
						data = typedUpd.Data
					}
					break
				}
			}
		case CounterMapDecAll[T]:
			for _, updKey := range typedUpd.Keys {
				if updKey == key {
					value -= typedUpd.Change
					if len(data) == 0 {
						data = typedUpd.Data
					}
					break
				}
			}
		case CounterMapIncMult[T]:
			for i, updKey := range typedUpd.Keys {
				if updKey == key {
					value += typedUpd.Change[i]
					if len(data) == 0 {
						data = typedUpd.Data[i]
					}
					break
				}
			}
		case CounterMapDecMult[T]:
			for i, updKey := range typedUpd.Keys {
				if updKey == key {
					value -= typedUpd.Change[i]
					if len(data) == 0 {
						data = typedUpd.Data[i]
					}
					break
				}
			}
		}
	}
	if len(data) > 0 {
		return CounterMapSingleState[T]{Value: value, Data: data}
	}
	return CounterMapSingleState[T]{Value: value}
}

func (crdt *CounterMapCrdt[T]) getValues(keys []int32, updsNotYetApplied []UpdateArguments) (state CounterMapState[T]) {
	//Initial part is similar whenever we have updsNotYetApplied or not: we copy the existing values.
	result := make([]KeyCounterPair[T], len(keys)) //We'll return 0 for non-existing keys. Can't ignore non-existing keys, as otherwise the values would match to the wrong keys.
	var resultData [][]byte
	if crdt.sumData > 0 {
		resultData = make([][]byte, len(keys))
		for i, key := range keys {
			pos, exists := crdt.entries[key]
			var value T
			if exists {
				value = crdt.quick.Slice[pos].Value
			}
			result[i] = KeyCounterPair[T]{Key: key, Value: value}
			resultData[i] = crdt.data.Slice[pos]
		}
	} else {
		for i, key := range keys {
			pos, exists := crdt.entries[key]
			var value T
			if exists {
				value = crdt.quick.Slice[pos].Value
			}
			result[i] = KeyCounterPair[T]{Key: key, Value: value}
		}
	}

	if len(updsNotYetApplied) == 0 {
		return CounterMapState[T]{Pairs: result, Data: resultData}
	}

	//Similarly to getValue, we will only update the keys that we are interested in.
	//We need however to build an index of key to pos in the result slice (as keys may be long.)
	keysMap := make(map[int32]int, len(keys))
	for i, key := range keys {
		keysMap[key] = i
	}
	var pos int
	var has bool

	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterMapInc[T]:
			if pos, has = keysMap[typedUpd.Key]; has {
				result[pos].Value += typedUpd.Change
				if len(typedUpd.Data) > 0 {
					resultData[pos] = typedUpd.Data
				}
			}
		case CounterMapDec[T]:
			if pos, has = keysMap[typedUpd.Key]; has {
				result[pos].Value -= typedUpd.Change
				if len(typedUpd.Data) > 0 {
					resultData[pos] = typedUpd.Data
				}
			}
		case CounterMapIncAll[T]:
			for _, updKey := range typedUpd.Keys {
				if pos, has = keysMap[updKey]; has {
					result[pos].Value += typedUpd.Change
					if len(typedUpd.Data) > 0 {
						resultData[pos] = typedUpd.Data
					}
				}
			}
		case CounterMapDecAll[T]:
			for _, updKey := range typedUpd.Keys {
				if pos, has = keysMap[updKey]; has {
					result[pos].Value -= typedUpd.Change
					if len(typedUpd.Data) > 0 {
						resultData[pos] = typedUpd.Data
					}
				}
			}
		case CounterMapIncMult[T]:
			for i, updKey := range typedUpd.Keys {
				if pos, has = keysMap[updKey]; has {
					result[pos].Value += typedUpd.Change[i]
					if len(typedUpd.Data[i]) > 0 {
						resultData[pos] = typedUpd.Data[i]
					}
				}
			}
		case CounterMapDecMult[T]:
			for i, updKey := range typedUpd.Keys {
				if pos, has = keysMap[updKey]; has {
					result[pos].Value -= typedUpd.Change[i]
					if len(typedUpd.Data[i]) > 0 {
						resultData[pos] = typedUpd.Data[i]
					}
				}
			}
		}
	}
	return CounterMapState[T]{Pairs: result, Data: resultData}
}

func (crdt *CounterMapCrdt[T]) hasKey(key int32, updsNotYetApplied []UpdateArguments) (state CounterMapHasKeyState[T]) {
	_, has := crdt.entries[key]
	if has || len(updsNotYetApplied) == 0 {
		return CounterMapHasKeyState[T](has)
	}

	//Has updates and crdt.entries does not have it. We go through the updates, stopping as soon as we find the key
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterMapInc[T]:
			if typedUpd.Key == key {
				return CounterMapHasKeyState[T](true)
			}
		case CounterMapDec[T]:
			if typedUpd.Key == key {
				return CounterMapHasKeyState[T](true)
			}
		case CounterMapIncAll[T]:
			for _, updKey := range typedUpd.Keys {
				if updKey == key {
					return CounterMapHasKeyState[T](true)
				}
			}
		case CounterMapDecAll[T]:
			for _, updKey := range typedUpd.Keys {
				if updKey == key {
					return CounterMapHasKeyState[T](true)
				}
			}
		case CounterMapIncMult[T]:
			for _, updKey := range typedUpd.Keys {
				if updKey == key {
					return CounterMapHasKeyState[T](true)
				}
			}
		case CounterMapDecMult[T]:
			for _, updKey := range typedUpd.Keys {
				if updKey == key {
					return CounterMapHasKeyState[T](true)
				}
			}
		}
	}
	return CounterMapHasKeyState[T](false)
}

func (crdt *CounterMapCrdt[T]) getKeys(updsNotYetApplied []UpdateArguments) (state CounterMapKeysState[T]) {
	keys := make([]int32, crdt.quick.Len())
	for i, pair := range crdt.quick.ToSlice() {
		keys[i] = pair.Key
	}
	if len(updsNotYetApplied) == 0 {
		return CounterMapKeysState[T](keys)
	}

	//Search for new keys in updsNotYetApplied. Most often there will be no new keys.
	//We will use a new map with only the new keys, as we expect the vast majority of updates will find the key in crdt.entries
	newKeys := make(map[int32]struct{})
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterMapInc[T]:
			if _, exists := crdt.entries[typedUpd.Key]; !exists {
				newKeys[typedUpd.Key] = struct{}{}
			}
		case CounterMapDec[T]:
			if _, exists := crdt.entries[typedUpd.Key]; !exists {
				newKeys[typedUpd.Key] = struct{}{}
			}
		case CounterMapIncAll[T]:
			for _, key := range typedUpd.Keys {
				if _, exists := crdt.entries[key]; !exists {
					newKeys[key] = struct{}{}
				}
			}
		case CounterMapDecAll[T]:
			for _, key := range typedUpd.Keys {
				if _, exists := crdt.entries[key]; !exists {
					newKeys[key] = struct{}{}
				}
			}
		case CounterMapIncMult[T]:
			for _, key := range typedUpd.Keys {
				if _, exists := crdt.entries[key]; !exists {
					newKeys[key] = struct{}{}
				}
			}
		case CounterMapDecMult[T]:
			for _, key := range typedUpd.Keys {
				if _, exists := crdt.entries[key]; !exists {
					newKeys[key] = struct{}{}
				}
			}
		}
	}
	if len(newKeys) == 0 {
		return CounterMapKeysState[T](keys)
	}
	newKeysSlice := make([]int32, len(keys)+len(newKeys))
	copy(newKeysSlice, keys)
	i := len(keys)
	for key := range newKeys {
		newKeysSlice[i] = key
		i++
	}
	return CounterMapKeysState[T](newKeysSlice)
}

func (crdt *CounterMapCrdt[T]) getCondAllState(compArgs CompareArguments, updsNotYetApplied []UpdateArguments, readKey, readValue, readData bool) (state State) {
	var condValue T
	var compType CompType
	if isFloatDataType[T]() {
		floatComp := compArgs.(FloatCompareArguments)
		condValue = T(floatComp.Value)
		compType = floatComp.CompType
	} else {
		intComp := compArgs.(IntCompareArguments)
		condValue = T(intComp.Value)
		compType = intComp.CompType
	}
	if len(updsNotYetApplied) == 0 {
		return crdt.condStateResultHelper(condValue, compType, crdt.quick.ToSlice(), crdt.data.ToSlice(), readKey, readValue, readData)
	}

	//If there's updates, some entries may stop meeting the conditions, while others may start meeting it. Furthermore, there could be new keys! :(
	//So we'll create a new buffer to hold *all* entries, so that we can update them. Any new entries will be added with append.
	updSlice := crdt.quick.Copy()
	var dataSlice tools.SliceWithCounter[[]byte]
	if crdt.sumData > 0 {
		dataSlice = tools.NewSliceWithCounter[[]byte](crdt.data.Len())
		copy(dataSlice.Slice, crdt.data.ToSlice())
	}
	newKeysMap := make(map[int32]int) //Holds new keys and their position in condSlice.
	var pos int
	var has bool

	//First, apply all updates.
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CounterMapInc[T]:
			if pos, has = crdt.entries[typedUpd.Key]; has {
				updSlice.Slice[pos].Value += typedUpd.Change
			} else {
				newKeysMap[typedUpd.Key] = updSlice.Len()
				updSlice.Append(KeyCounterPair[T]{Key: typedUpd.Key, Value: typedUpd.Change})
				dataSlice.Append(typedUpd.Data)
			}
		case CounterMapDec[T]:
			if pos, has = crdt.entries[typedUpd.Key]; has {
				updSlice.Slice[pos].Value -= typedUpd.Change
			} else {
				newKeysMap[typedUpd.Key] = updSlice.Len()
				updSlice.Append(KeyCounterPair[T]{Key: typedUpd.Key, Value: -typedUpd.Change})
				dataSlice.Append(typedUpd.Data)
			}
		case CounterMapIncAll[T]:
			for _, key := range typedUpd.Keys {
				if pos, has = crdt.entries[key]; has {
					updSlice.Slice[pos].Value += typedUpd.Change
				} else {
					newKeysMap[key] = updSlice.Len()
					updSlice.Append(KeyCounterPair[T]{Key: key, Value: typedUpd.Change})
					dataSlice.Append(typedUpd.Data)
				}
			}
		case CounterMapDecAll[T]:
			for _, key := range typedUpd.Keys {
				if pos, has = crdt.entries[key]; has {
					updSlice.Slice[pos].Value -= typedUpd.Change
				} else {
					newKeysMap[key] = updSlice.Len()
					updSlice.Append(KeyCounterPair[T]{Key: key, Value: -typedUpd.Change})
					dataSlice.Append(typedUpd.Data)
				}
			}
		case CounterMapIncMult[T]:
			for i, key := range typedUpd.Keys {
				if pos, has = crdt.entries[key]; has {
					updSlice.Slice[pos].Value += typedUpd.Change[i]
				} else {
					newKeysMap[key] = updSlice.Len()
					updSlice.Append(KeyCounterPair[T]{Key: key, Value: typedUpd.Change[i]})
					dataSlice.Append(typedUpd.Data[i])
				}
			}
		case CounterMapDecMult[T]:
			for i, key := range typedUpd.Keys {
				if pos, has = crdt.entries[key]; has {
					updSlice.Slice[pos].Value -= typedUpd.Change[i]
				} else {
					newKeysMap[key] = updSlice.Len()
					updSlice.Append(KeyCounterPair[T]{Key: key, Value: -typedUpd.Change[i]})
					dataSlice.Append(typedUpd.Data[i])
				}
			}
		}
	}

	return crdt.condStateResultHelper(condValue, compType, updSlice.ToSlice(), dataSlice.ToSlice(), readKey, readValue, readData)
}

func (crdt *CounterMapCrdt[T]) condStateResultHelper(condValue T, compType CompType, quickSlice []KeyCounterPair[T], dataSlice [][]byte, readKey, readValue, readData bool) (state State) {
	var condDataSlice tools.SliceWithCounter[[]byte]
	if crdt.sumData == 0 { //If there's no data, we will ignore readData.
		readData = false
	}
	if readData {
		condDataSlice = tools.NewSliceWithCounter[[]byte](len(dataSlice))
	}
	if readKey && readValue {
		condSlice := tools.NewSliceWithCounter[KeyCounterPair[T]](len(quickSlice))
		for i, pair := range quickSlice {
			if crdt.compValue(pair.Value, condValue, compType) {
				condSlice.AddToEnd(pair)
				if readData {
					condDataSlice.AddToEnd(dataSlice[i])
				}
			}
		}
		if readData {
			return CounterMapState[T]{Pairs: condSlice.ToSlice(), Data: condDataSlice.ToSlice()}
		}
		return CounterMapState[T]{Pairs: condSlice.ToSlice()}
	} else if readKey {
		keySlice := tools.NewSliceWithCounter[int32](len(quickSlice))
		for i, pair := range quickSlice {
			if crdt.compValue(pair.Value, condValue, compType) {
				keySlice.AddToEnd(pair.Key)
				if readData {
					condDataSlice.AddToEnd(dataSlice[i])
				}
			}
		}
		if readData {
			return CounterMapKeysDataState[T]{Keys: keySlice.ToSlice(), Data: condDataSlice.ToSlice()}
		}
		return CounterMapKeysDataState[T]{Keys: keySlice.ToSlice()}
	} else if readValue {
		valueSlice := tools.NewSliceWithCounter[T](len(quickSlice))
		for i, pair := range quickSlice {
			if crdt.compValue(pair.Value, condValue, compType) {
				valueSlice.AddToEnd(pair.Value)
				if readData {
					condDataSlice.AddToEnd(dataSlice[i])
				}
			}
		}
		if readData {
			return CounterMapValuesDataState[T]{Values: valueSlice.ToSlice(), Data: condDataSlice.ToSlice()}
		}
		return CounterMapValuesDataState[T]{Values: valueSlice.ToSlice()}
	}
	//Never happens, at least one must be read.
	return CounterMapState[T]{}
}

func (crdt *CounterMapCrdt[T]) compValue(value T, condValue T, compType CompType) (satisfied bool) {
	switch compType {
	case EQ:
		return value == condValue
	case NEQ:
		return value != condValue
	case LEQ:
		return value <= condValue
	case L:
		return value < condValue
	case H:
		return value > condValue
	case HEQ:
		return value >= condValue
	default:
		return false
	}
}

/*
switch typedUpd := upd.(type) {
		case CounterMapInc[T]:

		case CounterMapDec[T]:

		case CounterMapIncAll[T]:

		case CounterMapDecAll[T]:

		case CounterMapIncMult[T]:

		case CounterMapDecMult[T]:

		}
*/

func (crdt *CounterMapCrdt[T]) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	return args.(DownstreamArguments)
}

func (crdt *CounterMapCrdt[T]) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
		return nil
	}
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *CounterMapCrdt[T]) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var tmpEffect Effect = NoEffect{}
	//fmt.Printf("[CounterMapCRDT][applyDownstream] Applying downstream operation of type %T. This CRDT is of type %T", downstreamArgs, crdt)
	switch opType := downstreamArgs.(type) {
	case CounterMapInc[T]:
		tmpEffect = crdt.applyInc(opType.Key, opType.Change, opType.Data)
	case CounterMapDec[T]:
		tmpEffect = crdt.applyInc(opType.Key, -opType.Change, opType.Data) //Just change signal
	case CounterMapIncAll[T]:
		tmpEffect = crdt.applyIncAll(opType.Keys, opType.Change, opType.Data)
	case CounterMapDecAll[T]:
		tmpEffect = crdt.applyIncAll(opType.Keys, -opType.Change, opType.Data) //Just change signal
	case CounterMapIncMult[T]:
		tmpEffect = crdt.applyIncMult(opType.Keys, opType.Change, 1, opType.Data)
	case CounterMapDecMult[T]:
		tmpEffect = crdt.applyIncMult(opType.Keys, opType.Change, -1, opType.Data) //-1: modifier to all values in change, to turn an inc into a dec.
	case CounterMapInit[T]:
		tmpEffect = crdt.applyInit(int(opType))
	default:
		fmt.Printf("[CounterMapCrdt][Downstream]Unsupported downstream type: %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return &tmpEffect
}

func (crdt *CounterMapCrdt[T]) applyInc(key int32, change T, data []byte) (effect Effect) {
	pos, exists := crdt.entries[key]
	if !exists { //New key
		pos = len(crdt.entries)
		crdt.entries[key] = pos
		crdt.quick.Append(KeyCounterPair[T]{Key: key, Value: change})
		effect = CounterMapNewEffect[T]{Key: key, Change: change, Data: data}
		crdt.data.Append(data)
		crdt.sumData += len(data)
	} else {
		crdt.quick.Slice[pos].Value += change
		effect = CounterMapIncEffect[T]{Pos: int32(pos), Change: change}
	}
	return effect
}

func (crdt *CounterMapCrdt[T]) applyIncAll(keys []int32, change T, data []byte) (effect Effect) {
	if len(crdt.entries) == 0 { //Special case: here all keys will be new.
		if crdt.quick.Cap() == 0 {
			crdt.entries, crdt.quick, crdt.data = make(map[int32]int, len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](len(keys)), tools.NewSliceWithCounter[[]byte](len(keys))
		} //Else: empty but already initialized through CounterMapInit
		for i, key := range keys {
			crdt.entries[key] = i
			crdt.quick.AddToEnd(KeyCounterPair[T]{Key: key, Value: change})
			crdt.data.AddToEnd(data)
			crdt.sumData += len(data)
		}
		return CounterMapIncAllEffect[T]{NKeysAdded: int32(len(keys)), Change: change, Data: data}
	}
	if len(keys) > 5*crdt.quick.Cap() { //Many new entries. Better create new map and quick with appropriate size.
		/*newEntries, newQuick := make(map[int32]int, len(crdt.entries)+len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](crdt.quick.Len()+len(keys))
		newQuick.AddAll(crdt.quick.ToSlice())
		tools.MapCopyFromTo(crdt.entries, newEntries)*/
		//Strategy: since there's a lot more new keys than existing ones, it's wasteful to test for the key's existance for every key in keys.
		//So, instead, we start with an empty quick, add all keys, then update/copy from the original quick.
		newEntries, newQuick, newData := make(map[int32]int, crdt.quick.Len()+len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](crdt.quick.Len()+len(keys)), tools.NewSliceWithCounter[[]byte](crdt.quick.Len()+len(keys))
		//Add all keys first
		for i, key := range keys {
			newEntries[key] = i
			newQuick.AddToEnd(KeyCounterPair[T]{Key: key, Value: change})
			newData.AddToEnd(data)
			crdt.sumData += len(data)
		}
		//Now, update newQuick with the old quick.
		quickSlice := crdt.quick.ToSlice()
		var pos int
		var exists bool
		for _, pair := range quickSlice {
			pos, exists = newEntries[pair.Key]
			if exists {
				newQuick.Slice[pos].Value += pair.Value
			} else {
				//New key not in keys slice
				newPos := newQuick.Len()
				newEntries[pair.Key] = newPos
				newQuick.AddToEnd(KeyCounterPair[T]{Key: pair.Key, Value: pair.Value})
				newData.AddToEnd(crdt.data.Slice[pos])
			}
		}
		//Done! Albeit this needs a special effect, as now the keys swifted positions.
		effect := CounterMapIncReplaceAllEffect[T]{OldEntries: crdt.entries, OldQuick: crdt.quick, OldData: crdt.data}
		crdt.entries, crdt.quick, crdt.data = newEntries, newQuick, newData
		return effect
	}
	var pos int
	var exists bool
	//hasNew := false
	//var newKeys []int32
	nKeysAdded := int32(0)
	updatedPos := make([]int32, len(keys))
	/*if len(keys) > crdt.quick.Len() {
		newKeys = make([]int32, 0, len(keys)-crdt.quick.Len())
	}*/
	for j, key := range keys {
		pos, exists = crdt.entries[key]
		if !exists { //New key
			pos = len(crdt.entries)
			crdt.entries[key] = pos
			crdt.quick.Append(KeyCounterPair[T]{Key: key, Value: change})
			crdt.data.Append(data)
			crdt.sumData += len(data)
			//newKeys, hasNew = append(newKeys, key), true
			nKeysAdded++
		} else {
			crdt.quick.Slice[pos].Value += change
		}
		updatedPos[j] = int32(pos)
	}
	/*if hasNew {
		effect = CounterMapIncAllEffect[T]{Pos: updatedPos, NKeysAdded: nKeysAdded, Change: change}
	} else {
		effect = CounterMapIncAllEffect[T]{Pos: updatedPos, Change: change}
	}*/
	effect = CounterMapIncAllEffect[T]{Pos: updatedPos, NKeysAdded: nKeysAdded, Change: change, Data: data}
	return effect
}

func (crdt *CounterMapCrdt[T]) applyIncMult(keys []int32, changes []T, modifier T, data [][]byte) (effect Effect) {
	//Same logic as applyIncAll, but each key has its own change. Code is copy-pasted from there and adapted only for change.
	if len(crdt.entries) == 0 { //Special case: here all keys will be new.
		if crdt.quick.Cap() == 0 {
			crdt.entries, crdt.quick, crdt.data = make(map[int32]int, len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](len(keys)), tools.NewSliceWithCounter[[]byte](len(keys))
		} //Else: empty but already initialized through CounterMapInit
		for i, key := range keys {
			crdt.entries[key] = i
			crdt.quick.AddToEnd(KeyCounterPair[T]{Key: key, Value: changes[i] * modifier})
			crdt.data.AddToEnd(data[i])
			crdt.sumData += len(data[i])
		}
		return CounterMapIncMultEffect[T]{NKeysAdded: int32(len(keys)), Changes: changes, Modifier: modifier, Data: data}
	}
	if len(keys) > 5*crdt.quick.Cap() { //Many new entries. Better create new map and quick with appropriate size.
		/*newEntries, newQuick := make(map[int32]int, len(crdt.entries)+len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](crdt.quick.Len()+len(keys))
		newQuick.AddAll(crdt.quick.ToSlice())
		tools.MapCopyFromTo(crdt.entries, newEntries)*/
		//Strategy: since there's a lot more new keys than existing ones, it's wasteful to test for the key's existance for every key in keys.
		//So, instead, we start with an empty quick, add all keys, then update/copy from the original quick.
		newEntries, newQuick, newData := make(map[int32]int, crdt.quick.Len()+len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](crdt.quick.Len()+len(keys)), tools.NewSliceWithCounter[[]byte](crdt.quick.Len()+len(keys))
		oldSumData := crdt.sumData
		//Add all keys first
		for i, key := range keys {
			newEntries[key] = i
			newQuick.AddToEnd(KeyCounterPair[T]{Key: key, Value: changes[i] * modifier})
			newData.AddToEnd(data[i])
			crdt.sumData += len(data[i])
		}
		//Now, update newQuick with the old quick.
		quickSlice := crdt.quick.ToSlice()
		var pos int
		var exists bool
		for _, pair := range quickSlice {
			pos, exists = newEntries[pair.Key]
			if exists {
				newQuick.Slice[pos].Value += pair.Value
			} else {
				//New key not in keys slice
				newPos := newQuick.Len()
				newEntries[pair.Key] = newPos
				newQuick.AddToEnd(KeyCounterPair[T]{Key: pair.Key, Value: pair.Value})
				newData.AddToEnd(crdt.data.Slice[pos])
			}
		}
		//Done! Albeit this needs a special effect, as now the keys swifted positions.
		effect := CounterMapIncReplaceAllEffect[T]{OldEntries: crdt.entries, OldQuick: crdt.quick, OldData: crdt.data, OldSumData: oldSumData}
		crdt.entries, crdt.quick, crdt.data = newEntries, newQuick, newData
		return effect
	}
	var pos int
	var exists bool
	//hasNew := false
	//var newKeys []int32
	nKeysAdded := int32(0)
	updatedPos := make([]int32, len(keys))
	/*if len(keys) > crdt.quick.Len() {
		newKeys = make([]int32, 0, len(keys)-crdt.quick.Len())
	}*/
	for j, key := range keys {
		pos, exists = crdt.entries[key]
		if !exists { //New key
			pos = len(crdt.entries)
			crdt.entries[key] = pos
			crdt.quick.Append(KeyCounterPair[T]{Key: key, Value: changes[j] * modifier})
			crdt.data.Append(data[j])
			crdt.sumData += len(data[j])
			nKeysAdded++
			//newKeys, hasNew = append(newKeys, key), true
		} else {
			crdt.quick.Slice[pos].Value += changes[j] * modifier
		}
		updatedPos[j] = int32(pos)
	}
	/*if hasNew {
		effect = CounterMapIncMultEffect[T]{Pos: updatedPos, NewKeys: newKeys, Changes: changes, Modifier: modifier}
	} else {
		effect = CounterMapIncMultEffect[T]{Pos: updatedPos, Changes: changes, Modifier: modifier}
	}*/
	effect = CounterMapIncMultEffect[T]{Pos: updatedPos, NKeysAdded: nKeysAdded, Changes: changes, Modifier: modifier, Data: data}
	return effect
}

func (crdt *CounterMapCrdt[T]) applyInit(size int) (effect NoEffect) {
	effect = NoEffect{}
	if size < crdt.quick.Cap() { //Init wouldn't make sense to be executed now.
		return
	}
	entries, quick := make(map[int32]int, size), tools.NewSliceWithCounter[KeyCounterPair[T]](size)
	if crdt.quick.Len() > 0 { //Copy existing entries
		quickSlice := crdt.quick.ToSlice()
		quick.AddAll(quickSlice)
		//We iterate the quick slice to fill entries, as it's quicker than iterating the map.
		for i, pair := range quickSlice {
			entries[pair.Key] = i
		}
	}
	crdt.entries, crdt.quick = entries, quick
	return effect
}

func (crdt *CounterMapCrdt[T]) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE_CRDT

func (crdt *CounterMapCrdt[T]) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := &CounterMapCrdt[T]{
		CRDTVM:  crdt.CRDTVM.copy(),
		entries: tools.MapCopy(crdt.entries),
		quick:   crdt.quick.Copy(),
	}
	return newCrdt
}

func (crdt *CounterMapCrdt[T]) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *CounterMapCrdt[T]) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *CounterMapCrdt[T]) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case CounterMapNewEffect[T]: //Added key is always the last one added.
		delete(crdt.entries, typedEffect.Key)
		crdt.quick.RemoveLast()
		crdt.sumData -= len(crdt.data.RemoveAndGetLast())
	case CounterMapIncEffect[T]:
		crdt.quick.Slice[typedEffect.Pos].Value += typedEffect.Change
	case CounterMapIncAllEffect[T]:
		//Removing new keys from entries and quick
		nKeys, quickLen := int(typedEffect.NKeysAdded), crdt.quick.Len()
		var key int32
		for i := 1; i <= nKeys; i++ {
			key = crdt.quick.Slice[quickLen-i].Key
			delete(crdt.entries, key)
			crdt.quick.RemoveLast()
			crdt.sumData -= len(crdt.data.RemoveAndGetLast())
		}
		//Reverting changes to existing keys
		for _, pos := range typedEffect.Pos {
			crdt.quick.Slice[pos].Value -= typedEffect.Change
		}
	case CounterMapIncReplaceAllEffect[T]:
		crdt.entries, crdt.quick, crdt.data, crdt.sumData = typedEffect.OldEntries, typedEffect.OldQuick, typedEffect.OldData, typedEffect.OldSumData
	case CounterMapIncMultEffect[T]:
		//Removing new keys from entries and quick
		nKeys, quickLen := int(typedEffect.NKeysAdded), crdt.quick.Len()
		var key int32
		for i := 1; i <= nKeys; i++ {
			key = crdt.quick.Slice[quickLen-i].Key
			delete(crdt.entries, key)
			crdt.quick.RemoveLast()
			crdt.sumData -= len(crdt.data.RemoveAndGetLast())
		}
		//Reverting changes to existing keys
		for i, pos := range typedEffect.Pos {
			crdt.quick.Slice[pos].Value -= typedEffect.Changes[i] * typedEffect.Modifier
		}
	default:
		fmt.Printf("[CounterMapCrdt] Unknown effect to undo: %+v (%T)\n", *effect, *effect)
	}
}

func (crdt *CounterMapCrdt[T]) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions
//Note: on these functions, to avoid type switches (and converting T to any), we will just check both int and float entries whenever necessary.
//TODO: Need to replace in .proto DATA_SIZE_TYPE with DATA_TYPE (might as well since DATA_TYPE is defined in the proto...)
//Furthermore, I need to set up DATA_TYPE properly in all the protobufs conversion below :(

func (args CounterMapInit[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CounterMapInit[T](protobuf.GetMapcounterop().GetInit().GetSize())
}

func (args CounterMapInit[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	dataType := counterMapDataType[T]()
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: &proto.ApbMapCounterUpdate{DataType: &dataType, Init: &proto.ApbMapCounterInit{Size: pb.Int32(int32(args))}}}}
}

func (args CounterMapInc[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counterUpd := protobuf.GetMapcounterop()
	if counterUpd.GetIntOp() != nil {
		intUpd := counterUpd.GetIntOp().GetInc()
		args.Key, args.Change, args.Data = intUpd.GetKey(), T(intUpd.GetInc()), intUpd.GetData()
	} else {
		floatUpd := counterUpd.GetDoubleOp().GetInc()
		args.Key, args.Change, args.Data = floatUpd.GetKey(), T(floatUpd.GetInc()), floatUpd.GetData()
	}
	return args
}

func (args CounterMapInc[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	mapOp, isFloat := prepareApbMapCounterUpdate(counterMapDataType[T](), false)
	if isFloat {
		mapOp.IntOp = &proto.ApbMapIntOp{Inc: &proto.ApbMapIntSingleIncOp{Key: pb.Int32(args.Key), Inc: pb.Int64(int64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.IntOp.Inc.Data = args.Data
		}
	} else {
		mapOp.DoubleOp = &proto.ApbMapDoubleOp{Inc: &proto.ApbMapDoubleSingleIncOp{Key: pb.Int32(args.Key), Inc: pb.Float64(float64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.DoubleOp.Inc.Data = args.Data
		}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: mapOp}}
}

func (args CounterMapDec[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counterUpd := protobuf.GetMapcounterop()
	if counterUpd.GetIntOp() != nil {
		intUpd := counterUpd.GetIntOp().GetInc()
		args.Key, args.Change, args.Data = intUpd.GetKey(), T(intUpd.GetInc()), intUpd.GetData()
	} else {
		floatUpd := counterUpd.GetDoubleOp().GetInc()
		args.Key, args.Change, args.Data = floatUpd.GetKey(), T(floatUpd.GetInc()), floatUpd.GetData()
	}
	return args
}

func (args CounterMapDec[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	mapOp, isFloat := prepareApbMapCounterUpdate(counterMapDataType[T](), true)
	if isFloat {
		mapOp.IntOp = &proto.ApbMapIntOp{Inc: &proto.ApbMapIntSingleIncOp{Key: pb.Int32(args.Key), Inc: pb.Int64(int64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.IntOp.Inc.Data = args.Data
		}
	} else {
		mapOp.DoubleOp = &proto.ApbMapDoubleOp{Inc: &proto.ApbMapDoubleSingleIncOp{Key: pb.Int32(args.Key), Inc: pb.Float64(float64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.DoubleOp.Inc.Data = args.Data
		}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: mapOp}}
}

func (args CounterMapIncAll[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counterUpd := protobuf.GetMapcounterop()
	if counterUpd.GetIntOp() != nil {
		intUpd := counterUpd.GetIntOp().GetIncAll()
		args.Keys, args.Change, args.Data = intUpd.GetKeys(), T(intUpd.GetInc()), intUpd.GetData()
	} else {
		floatUpd := counterUpd.GetDoubleOp().GetIncAll()
		args.Keys, args.Change, args.Data = floatUpd.GetKeys(), T(floatUpd.GetInc()), floatUpd.GetData()
	}
	return args
}

func (args CounterMapIncAll[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	mapOp, isFloat := prepareApbMapCounterUpdate(counterMapDataType[T](), false)
	if isFloat {
		mapOp.IntOp = &proto.ApbMapIntOp{IncAll: &proto.ApbMapIntIncAllOp{Keys: args.Keys, Inc: pb.Int64(int64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.IntOp.IncAll.Data = args.Data
		}
	} else {
		mapOp.DoubleOp = &proto.ApbMapDoubleOp{IncAll: &proto.ApbMapDoubleIncAllOp{Keys: args.Keys, Inc: pb.Float64(float64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.DoubleOp.IncAll.Data = args.Data
		}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: mapOp}}
}

func (args CounterMapDecAll[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counterUpd := protobuf.GetMapcounterop()
	if counterUpd.GetIntOp() != nil {
		intUpd := counterUpd.GetIntOp().GetIncAll()
		args.Keys, args.Change, args.Data = intUpd.GetKeys(), T(intUpd.GetInc()), intUpd.GetData()
	} else {
		floatUpd := counterUpd.GetDoubleOp().GetIncAll()
		args.Keys, args.Change, args.Data = floatUpd.GetKeys(), T(floatUpd.GetInc()), floatUpd.GetData()
	}
	return args
}

func (args CounterMapDecAll[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	mapOp, isFloat := prepareApbMapCounterUpdate(counterMapDataType[T](), true)
	if isFloat {
		mapOp.IntOp = &proto.ApbMapIntOp{IncAll: &proto.ApbMapIntIncAllOp{Keys: args.Keys, Inc: pb.Int64(int64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.IntOp.IncAll.Data = args.Data
		}
	} else {
		mapOp.DoubleOp = &proto.ApbMapDoubleOp{IncAll: &proto.ApbMapDoubleIncAllOp{Keys: args.Keys, Inc: pb.Float64(float64(args.Change))}}
		if len(args.Data) > 0 {
			mapOp.DoubleOp.IncAll.Data = args.Data
		}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: mapOp}}
}

func (args CounterMapIncMult[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counterUpd := protobuf.GetMapcounterop()
	if counterUpd.GetIntOp() != nil {
		intUpd := counterUpd.GetIntOp().GetIncMulti()
		protoInc := intUpd.GetInc()
		args.Change = make([]T, len(protoInc))
		for i := range protoInc {
			args.Change[i] = T(protoInc[i])
		}
		args.Keys, args.Data = intUpd.GetKeys(), intUpd.GetData()
	} else {
		floatUpd := counterUpd.GetDoubleOp().GetIncMulti()
		protoInc := floatUpd.GetInc()
		args.Change = make([]T, len(protoInc))
		for i := range protoInc {
			args.Change[i] = T(protoInc[i])
		}
		args.Keys, args.Data = floatUpd.GetKeys(), floatUpd.GetData()
	}
	return args
}

func (args CounterMapIncMult[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	mapOp, isFloat := prepareApbMapCounterUpdate(counterMapDataType[T](), false)
	if isFloat {
		values := make([]float64, len(args.Change))
		for i, val := range args.Change {
			values[i] = float64(val)
		}
		mapOp.DoubleOp = &proto.ApbMapDoubleOp{IncMulti: &proto.ApbMapDoubleIncMultOp{Keys: args.Keys, Inc: values, Data: args.Data}}
	} else {
		values := make([]int64, len(args.Change))
		for i, val := range args.Change {
			values[i] = int64(val)
		}
		mapOp.IntOp = &proto.ApbMapIntOp{IncMulti: &proto.ApbMapIntIncMultOp{Keys: args.Keys, Inc: values, Data: args.Data}}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: mapOp}}
}

func (args CounterMapDecMult[T]) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	counterUpd := protobuf.GetMapcounterop()
	if counterUpd.GetIntOp() != nil {
		intUpd := counterUpd.GetIntOp().GetIncMulti()
		protoInc := intUpd.GetInc()
		args.Change = make([]T, len(protoInc))
		for i := range protoInc {
			args.Change[i] = T(protoInc[i])
		}
		args.Keys, args.Data = intUpd.GetKeys(), intUpd.GetData()
	} else {
		floatUpd := counterUpd.GetDoubleOp().GetIncMulti()
		protoInc := floatUpd.GetInc()
		args.Change = make([]T, len(protoInc))
		for i := range protoInc {
			args.Change[i] = T(protoInc[i])
		}
		args.Keys, args.Data = floatUpd.GetKeys(), floatUpd.GetData()
	}
	return args
}

func (args CounterMapDecMult[T]) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	mapOp, isFloat := prepareApbMapCounterUpdate(counterMapDataType[T](), true)
	if isFloat {
		values := make([]float64, len(args.Change))
		for i, val := range args.Change {
			values[i] = float64(val)
		}
		mapOp.DoubleOp = &proto.ApbMapDoubleOp{IncMulti: &proto.ApbMapDoubleIncMultOp{Keys: args.Keys, Inc: values, Data: args.Data}}
	} else {
		values := make([]int64, len(args.Change))
		for i, val := range args.Change {
			values[i] = int64(val)
		}
		mapOp.IntOp = &proto.ApbMapIntOp{IncMulti: &proto.ApbMapIntIncMultOp{Keys: args.Keys, Inc: values, Data: args.Data}}
	}
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Mapcounterop{Mapcounterop: mapOp}}
}

func (downOp CounterMapInit[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return CounterMapInit[T](protobuf.GetMapCounterOp().GetInit().GetSize())
}

func (downOp CounterMapInit[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: &proto.ProtoMapCounterDownstream{Upd: &proto.ProtoMapCounterDownstream_Init{Init: &proto.ProtoMapCounterInit{Size: pb.Int32(int32(downOp))}}}}}
}

func (downOp CounterMapInc[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	counterUpd := protobuf.GetMapCounterOp()
	if intInc := counterUpd.GetIntInc(); intInc != nil {
		return CounterMapInc[T]{Key: intInc.GetKey(), Change: T(intInc.GetInc()), Data: intInc.GetData()}
	}
	floatInc := counterUpd.GetDoubleInc()
	return CounterMapInc[T]{Key: floatInc.GetKey(), Change: T(floatInc.GetInc()), Data: floatInc.GetData()}
}

func (downOp CounterMapInc[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	mapOp, isFloat := prepareProtoMapCounterDownstream(counterMapDataType[T](), false)
	if isFloat {
		upd := &proto.ProtoMapCounterDownstream_IntInc{IntInc: &proto.ProtoMapCounterIntInc{Key: pb.Int32(downOp.Key), Inc: pb.Int64(int64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.IntInc.Data = downOp.Data
		}
		mapOp.Upd = upd
	} else {
		upd := &proto.ProtoMapCounterDownstream_DoubleInc{DoubleInc: &proto.ProtoMapCounterDoubleInc{Key: pb.Int32(downOp.Key), Inc: pb.Float64(float64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.DoubleInc.Data = downOp.Data
		}
		mapOp.Upd = upd
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: mapOp}}
}

func (downOp CounterMapDec[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	counterUpd := protobuf.GetMapCounterOp()
	if intInc := counterUpd.GetIntInc(); intInc != nil {
		return CounterMapDec[T]{Key: intInc.GetKey(), Change: T(intInc.GetInc()), Data: intInc.GetData()}
	}
	floatInc := counterUpd.GetDoubleInc()
	return CounterMapDec[T]{Key: floatInc.GetKey(), Change: T(floatInc.GetInc()), Data: floatInc.GetData()}
}

func (downOp CounterMapDec[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	mapOp, isFloat := prepareProtoMapCounterDownstream(counterMapDataType[T](), true)
	if isFloat {
		upd := &proto.ProtoMapCounterDownstream_IntInc{IntInc: &proto.ProtoMapCounterIntInc{Key: pb.Int32(downOp.Key), Inc: pb.Int64(int64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.IntInc.Data = downOp.Data
		}
		mapOp.Upd = upd
	} else {
		upd := &proto.ProtoMapCounterDownstream_DoubleInc{DoubleInc: &proto.ProtoMapCounterDoubleInc{Key: pb.Int32(downOp.Key), Inc: pb.Float64(float64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.DoubleInc.Data = downOp.Data
		}
		mapOp.Upd = upd
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: mapOp}}
}

func (downOp CounterMapIncAll[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	counterUpd := protobuf.GetMapCounterOp()
	if intInc := counterUpd.GetIntIncAll(); intInc != nil {
		return CounterMapIncAll[T]{Keys: intInc.GetKeys(), Change: T(intInc.GetInc()), Data: intInc.GetData()}
	}
	floatInc := counterUpd.GetDoubleIncAll()
	return CounterMapIncAll[T]{Keys: floatInc.GetKeys(), Change: T(floatInc.GetInc()), Data: floatInc.GetData()}
}

func (downOp CounterMapIncAll[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	mapOp, isFloat := prepareProtoMapCounterDownstream(counterMapDataType[T](), false)
	if isFloat {
		upd := &proto.ProtoMapCounterDownstream_IntIncAll{IntIncAll: &proto.ProtoMapCounterIntIncAll{Keys: downOp.Keys, Inc: pb.Int64(int64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.IntIncAll.Data = downOp.Data
		}
		mapOp.Upd = upd
	} else {
		upd := &proto.ProtoMapCounterDownstream_DoubleIncAll{DoubleIncAll: &proto.ProtoMapCounterDoubleIncAll{Keys: downOp.Keys, Inc: pb.Float64(float64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.DoubleIncAll.Data = downOp.Data
		}
		mapOp.Upd = upd
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: mapOp}}
}

func (downOp CounterMapDecAll[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	counterUpd := protobuf.GetMapCounterOp()
	if intInc := counterUpd.GetIntIncAll(); intInc != nil {
		return CounterMapDecAll[T]{Keys: intInc.GetKeys(), Change: T(intInc.GetInc()), Data: intInc.GetData()}
	}
	floatInc := counterUpd.GetDoubleIncAll()
	return CounterMapDecAll[T]{Keys: floatInc.GetKeys(), Change: T(floatInc.GetInc()), Data: floatInc.GetData()}
}

func (downOp CounterMapDecAll[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	mapOp, isFloat := prepareProtoMapCounterDownstream(counterMapDataType[T](), true)
	if isFloat {
		upd := &proto.ProtoMapCounterDownstream_IntIncAll{IntIncAll: &proto.ProtoMapCounterIntIncAll{Keys: downOp.Keys, Inc: pb.Int64(int64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.IntIncAll.Data = downOp.Data
		}
		mapOp.Upd = upd
	} else {
		upd := &proto.ProtoMapCounterDownstream_DoubleIncAll{DoubleIncAll: &proto.ProtoMapCounterDoubleIncAll{Keys: downOp.Keys, Inc: pb.Float64(float64(downOp.Change))}}
		if len(downOp.Data) > 0 {
			upd.DoubleIncAll.Data = downOp.Data
		}
		mapOp.Upd = upd
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: mapOp}}
}

func (downOp CounterMapIncMult[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	counterUpd := protobuf.GetMapCounterOp()
	if intInc := counterUpd.GetIntIncMult(); intInc != nil {
		//fmt.Printf("[CounterMapIncMult] FromReplicatorObj, IntIncMult. Proto type: %T.\n", protobuf)
		protoValues := intInc.GetInc()
		changes := make([]T, len(protoValues))
		for i, val := range protoValues {
			changes[i] = T(val)
		}
		return CounterMapIncMult[T]{Keys: intInc.GetKeys(), Change: changes, Data: intInc.GetData()}
	}
	floatInc := counterUpd.GetDoubleIncMult()
	protoValues := floatInc.GetInc()
	changes := make([]T, len(protoValues))
	for i, val := range protoValues {
		changes[i] = T(val)
	}
	return CounterMapIncMult[T]{Keys: floatInc.GetKeys(), Change: changes, Data: floatInc.GetData()}
}

func (downOp CounterMapIncMult[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	mapOp, isFloat := prepareProtoMapCounterDownstream(counterMapDataType[T](), false)
	if isFloat {
		values := make([]float64, len(downOp.Change))
		for i, val := range downOp.Change {
			values[i] = float64(val)
		}
		upd := &proto.ProtoMapCounterDownstream_DoubleIncMult{DoubleIncMult: &proto.ProtoMapCounterDoubleIncMult{Keys: downOp.Keys, Inc: values}}
		if len(downOp.Data) > 0 {
			upd.DoubleIncMult.Data = downOp.Data
		}
		mapOp.Upd = upd
	} else {
		values := make([]int64, len(downOp.Change))
		for i, val := range downOp.Change {
			values[i] = int64(val)
		}
		upd := &proto.ProtoMapCounterDownstream_IntIncMult{IntIncMult: &proto.ProtoMapCounterIntIncMult{Keys: downOp.Keys, Inc: values}}
		if len(downOp.Data) > 0 {
			upd.IntIncMult.Data = downOp.Data
		}
		mapOp.Upd = upd
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: mapOp}}
}

func (downOp CounterMapDecMult[T]) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	counterUpd := protobuf.GetMapCounterOp()
	if intInc := counterUpd.GetIntIncMult(); intInc != nil {
		protoValues := intInc.GetInc()
		changes := make([]T, len(protoValues))
		for i, val := range protoValues {
			changes[i] = T(val)
		}
		return CounterMapDecMult[T]{Keys: intInc.GetKeys(), Change: changes, Data: intInc.GetData()}
	}
	floatInc := counterUpd.GetDoubleIncMult()
	protoValues := floatInc.GetInc()
	changes := make([]T, len(protoValues))
	for i, val := range protoValues {
		changes[i] = T(val)
	}
	return CounterMapDecMult[T]{Keys: floatInc.GetKeys(), Change: changes, Data: floatInc.GetData()}
}

func (downOp CounterMapDecMult[T]) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	mapOp, isFloat := prepareProtoMapCounterDownstream(counterMapDataType[T](), true)
	if isFloat {
		values := make([]float64, len(downOp.Change))
		for i, val := range downOp.Change {
			values[i] = float64(val)
		}
		upd := &proto.ProtoMapCounterDownstream_DoubleIncMult{DoubleIncMult: &proto.ProtoMapCounterDoubleIncMult{Keys: downOp.Keys, Inc: values}}
		if len(downOp.Data) > 0 {
			upd.DoubleIncMult.Data = downOp.Data
		}
		mapOp.Upd = upd
	} else {
		values := make([]int64, len(downOp.Change))
		for i, val := range downOp.Change {
			values[i] = int64(val)
		}
		upd := &proto.ProtoMapCounterDownstream_IntIncMult{IntIncMult: &proto.ProtoMapCounterIntIncMult{Keys: downOp.Keys, Inc: values}}
		if len(downOp.Data) > 0 {
			upd.IntIncMult.Data = downOp.Data
		}
		mapOp.Upd = upd
	}
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_MapCounterOp{MapCounterOp: mapOp}}
}

func (args CounterMapGetValueArguments[T]) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CounterMapGetValueArguments[T](protobuf.GetMapcounter().GetGetvalue().GetKey())
}

func (args CounterMapGetValueArguments[T]) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType := counterMapDataType[T]()
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{
		DataType: &dataType, Getvalue: &proto.ApbMapCounterGetValueRead{Key: pb.Int32(int32(args))}}}}
}

func (args CounterMapHasKeyArguments[T]) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CounterMapHasKeyArguments[T](protobuf.GetMapcounter().GetHaskey().GetKey())
}

func (args CounterMapHasKeyArguments[T]) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType := counterMapDataType[T]()
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{
		DataType: &dataType, Haskey: &proto.ApbMapCounterHasKeyRead{Key: pb.Int32(int32(args))}}}}
}

func (args CounterMapGetKeysArguments[T]) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CounterMapGetKeysArguments[T]{}
}

func (args CounterMapGetKeysArguments[T]) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType := counterMapDataType[T]()
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{
		DataType: &dataType, Getkeys: &proto.ApbMapCounterGetKeysRead{}}}}
}

func (args CounterMapGetValuesArguments[T]) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CounterMapGetValuesArguments[T](protobuf.GetMapcounter().GetGetvalues().GetKeys())
}

func (args CounterMapGetValuesArguments[T]) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType := counterMapDataType[T]()
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{
			DataType: &dataType, Compread: &proto.ApbMapCounterCompRead{}}}}
	}
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{
		DataType: &dataType, Compread: &proto.ApbMapCounterCompRead{}}}}
}

func (args CounterMapCompareAllArguments[T]) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	mapProto := protobuf.GetMapcounter()
	compProto, dataType := mapProto.GetCompread(), mapProto.GetDataType()
	compType := CompType(compProto.GetComp())
	readKeys, readValues, readData := getOptionalBoolFromProto(compProto.ReadKeys, true), getOptionalBoolFromProto(compProto.ReadValues, true), getOptionalBoolFromProto(compProto.ReadData, true)
	//Comp can only be of int or float. However, CounterMapCompareAllArguments itself can be multiple int/float types.
	switch dataType {
	case proto.DATAType_INT, proto.DATAType_INT8, proto.DATAType_INT16, proto.DATAType_INT32, proto.DATAType_INT64, proto.DATAType_DEFAULT:
		return CounterMapCompareAllArguments[T]{CompareArguments: IntCompareArguments{CompType: compType, Value: compProto.GetIntValue()}, GetKey: readKeys, GetValue: readValues, GetData: readData}
	case proto.DATAType_FLOAT32, proto.DATAType_FLOAT64:
		return CounterMapCompareAllArguments[T]{CompareArguments: FloatCompareArguments{CompType: compType, Value: compProto.GetDoubleValue()}, GetKey: readKeys, GetValue: readValues, GetData: readData}
	}
	return CounterMapGetKeysArguments[T]{}
}

// Returns false if nil
func getOptionalBoolFromProto(optBool *bool, defaultValue bool) bool {
	if optBool == nil {
		return defaultValue
	}
	return *optBool
}

func (args CounterMapCompareAllArguments[T]) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType, compType := counterMapDataType[T](), proto.COMPType(args.CompareArguments.GetCompType())
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{DataType: &dataType, Compread: &proto.ApbMapCounterCompRead{Comp: &compType, DoubleValue: pb.Float64(args.CompareArguments.(FloatCompareArguments).Value)}}}}
	} else {
		return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Mapcounter{Mapcounter: &proto.ApbMapCounterPartialRead{DataType: &dataType, Compread: &proto.ApbMapCounterCompRead{Comp: &compType, IntValue: pb.Int64(args.CompareArguments.(IntCompareArguments).Value)}}}}
	}
}

func (crdtState CounterMapState[T]) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	mapProto := protobuf.GetMapcounter()
	keys, intValues, floatValues, data := mapProto.GetKeys(), mapProto.GetIntvalues(), mapProto.GetFloatvalues(), mapProto.GetData()
	crdtState.Pairs = make([]KeyCounterPair[T], len(keys))
	if intValues != nil {
		for i, value := range intValues {
			crdtState.Pairs[i] = KeyCounterPair[T]{Key: keys[i], Value: T(value)}
		}
	} else {
		for i, value := range floatValues {
			crdtState.Pairs[i] = KeyCounterPair[T]{Key: keys[i], Value: T(value)}
		}
	}
	if len(data) > 0 {
		crdtState.Data = data
	}
	return crdtState
}

func (crdtState CounterMapState[T]) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	keys, dataType := make([]int32, len(crdtState.Pairs)), counterMapDataType[T]()
	mapProto := proto.ApbGetMapCounterResp{DataType: &dataType, Keys: keys}
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		floatValues := make([]float64, len(crdtState.Pairs))
		for i, pair := range crdtState.Pairs {
			keys[i], floatValues[i] = pair.Key, float64(pair.Value)
		}
		mapProto.Floatvalues = floatValues
	} else {
		intValues := make([]int64, len(crdtState.Pairs))
		for i, pair := range crdtState.Pairs {
			keys[i], intValues[i] = pair.Key, int64(pair.Value)
		}
		mapProto.Intvalues = intValues
	}
	if len(crdtState.Data) > 0 {
		mapProto.Data = crdtState.Data
	}
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Mapcounter{Mapcounter: &mapProto}}
}

func (crdtState CounterMapSingleState[T]) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	mapProto := protobuf.GetPartread().GetMapcounter()
	dataType := mapProto.GetDataType()
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		return CounterMapSingleState[T]{Value: T(mapProto.GetSingle().GetFloatvalue())}
	} else {
		return CounterMapSingleState[T]{Value: T(mapProto.GetSingle().GetIntvalue())}
	}
}

func (crdtState CounterMapSingleState[T]) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	dataType := counterMapDataType[T]()
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
			Reply: &proto.ApbPartialReadResp_Mapcounter{Mapcounter: &proto.ApbMapCounterReadResp{DataType: &dataType, Single: &proto.ApbMapCounterSingleResp{Floatvalue: pb.Float64(float64(crdtState.Value))}}}}}}
	} else {
		return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
			Reply: &proto.ApbPartialReadResp_Mapcounter{Mapcounter: &proto.ApbMapCounterReadResp{DataType: &dataType, Single: &proto.ApbMapCounterSingleResp{Intvalue: pb.Int64(int64(crdtState.Value))}}}}}}
	}
}

func (crdtState CounterMapHasKeyState[T]) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CounterMapHasKeyState[T](protobuf.GetPartread().GetMapcounter().GetHaskey().GetHas())
}

func (crdtState CounterMapHasKeyState[T]) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
		Reply: &proto.ApbPartialReadResp_Mapcounter{Mapcounter: &proto.ApbMapCounterReadResp{Haskey: &proto.ApbMapCounterHasKeyResp{Has: pb.Bool(bool(crdtState))}}}}}}
}

func (crdtState CounterMapKeysState[T]) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CounterMapKeysState[T](protobuf.GetPartread().GetMapcounter().GetKeys().GetKeys())
}

func (crdtState CounterMapKeysState[T]) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
		Reply: &proto.ApbPartialReadResp_Mapcounter{Mapcounter: &proto.ApbMapCounterReadResp{Keys: &proto.ApbMapCounterKeysResp{Keys: crdtState}}}}}}
}

func (crdtState CounterMapKeysDataState[T]) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	mapProto := protobuf.GetPartread().GetMapcounter().GetKeysData()
	crdtState.Keys = mapProto.GetKeys()
	if len(crdtState.Data) > 0 {
		crdtState.Data = mapProto.GetData()
	}
	return crdtState
}

func (crdtState CounterMapKeysDataState[T]) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	mapProto := proto.ApbMapCounterKeysDataResp{Keys: crdtState.Keys}
	if len(crdtState.Data) > 0 {
		mapProto.Data = crdtState.Data
	}
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
		Reply: &proto.ApbPartialReadResp_Mapcounter{Mapcounter: &proto.ApbMapCounterReadResp{KeysData: &mapProto}}}}}
}

func (crdtState CounterMapValuesDataState[T]) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	mapProto := protobuf.GetPartread().GetMapcounter()
	dataType := mapProto.GetDataType()
	valuesProto := mapProto.GetValuesData()
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		crdtState.Values = tools.ConvertOrCopyGenericSlice[T](valuesProto.GetFloatvalues())
	} else {
		crdtState.Values = tools.ConvertOrCopyGenericSlice[T](valuesProto.GetIntvalues())
	}
	if len(crdtState.Data) > 0 {
		crdtState.Data = valuesProto.GetData()
	}
	return crdtState
}

func (crdtState CounterMapValuesDataState[T]) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	mapProto := proto.ApbMapCounterValuesDataResp{}
	dataType := counterMapDataType[T]()
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		var floatValues []float64
		if casted, ok := any(crdtState.Values).([]float64); ok {
			floatValues = casted
		} else {
			floatValues = make([]float64, len(crdtState.Values))
			for i, val := range crdtState.Values {
				floatValues[i] = float64(val)
			}
		}
		mapProto.Floatvalues = floatValues
	} else {
		var intValues []int64
		if casted, ok := any(crdtState.Values).([]int64); ok {
			intValues = casted
		} else {
			intValues = make([]int64, len(crdtState.Values))
			for i, val := range crdtState.Values {
				intValues[i] = int64(val)
			}
		}
		mapProto.Intvalues = intValues
	}
	if len(crdtState.Data) > 0 {
		mapProto.Data = crdtState.Data
	}
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{
		Reply: &proto.ApbPartialReadResp_Mapcounter{Mapcounter: &proto.ApbMapCounterReadResp{ValuesData: &mapProto}}}}}
}

func (crdt *CounterMapCrdt[T]) ToProtoState() (protobuf *proto.ProtoState) {
	dataType := crdt.GetDATAType()
	keys := make([]int32, crdt.quick.Len())
	toIterate := crdt.quick.ToSlice() //It's ok to copy quick as it includes the keys too.
	if dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32 {
		floatValues := make([]float64, len(keys))
		for i, pair := range toIterate {
			keys[i], floatValues[i] = pair.Key, float64(pair.Value)
		}
		return &proto.ProtoState{State: &proto.ProtoState_MapCounter{MapCounter: &proto.ProtoMapCounterState{
			DataType: &dataType, Keys: keys, DoubleValues: floatValues}}}
	} else {
		intValues := make([]int64, len(keys))
		for i, pair := range toIterate {
			keys[i], intValues[i] = pair.Key, int64(pair.Value)
		}
		return &proto.ProtoState{State: &proto.ProtoState_MapCounter{MapCounter: &proto.ProtoMapCounterState{
			DataType: &dataType, Keys: keys, IntValues: intValues}}}
	}
}

func (crdt *CounterMapCrdt[T]) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	mapProto := proto.GetMapCounter()
	keys := mapProto.GetKeys()
	entries, quick := make(map[int32]int, len(keys)), tools.NewSliceWithCounter[KeyCounterPair[T]](len(keys))
	if intValues := mapProto.GetIntValues(); intValues != nil {
		for i, key := range keys {
			entries[key] = i
			quick.AddToEnd(KeyCounterPair[T]{Key: key, Value: T(intValues[i])})
		}
	} else {
		floatValues := mapProto.GetDoubleValues()
		for i, key := range keys {
			entries[key] = i
			quick.AddToEnd(KeyCounterPair[T]{Key: key, Value: T(floatValues[i])})
		}
	}
	return (&CounterMapCrdt[T]{entries: entries, quick: quick}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *CounterMapCrdt[T]) GetCRDT() CRDT { return crdt }

// Sets data type for update, as well as setting the flag for isDec if appropriate.
func prepareApbMapCounterUpdate(dataType proto.DATAType, isDec bool) (protoUpd *proto.ApbMapCounterUpdate, isFloat bool) {
	if !isDec {
		return &proto.ApbMapCounterUpdate{DataType: &dataType}, isDataTypeArgFloat(dataType)
	}
	return &proto.ApbMapCounterUpdate{DataType: &dataType, IsDec: shared.TRUE_POINTER}, isDataTypeArgFloat(dataType)
}

func prepareProtoMapCounterDownstream(dataType proto.DATAType, isDec bool) (protoUpd *proto.ProtoMapCounterDownstream, isFloat bool) {
	if !isDec {
		return &proto.ProtoMapCounterDownstream{DataType: &dataType}, isDataTypeArgFloat(dataType)
	}
	return &proto.ProtoMapCounterDownstream{DataType: &dataType, IsDec: shared.TRUE_POINTER}, isDataTypeArgFloat(dataType)
}

func isDataTypeArgFloat(dataType proto.DATAType) bool {
	return dataType == proto.DATAType_FLOAT64 || dataType == proto.DATAType_FLOAT32
}
