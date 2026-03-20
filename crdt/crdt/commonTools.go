package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
)

var (
	NReplicas           int32   //Information that may be used by CRDTs if needed. Note: doesn't update when a new replica joins the system besides the intended number. But is that even supported atm?
	emptyData           *[]byte = &[]byte{}
	POINTER_ONE_UINT32          = newUInt32Pointer(1)
	POINTER_ZERO_UINT32         = newUInt32Pointer(0)
)

func newInt32Pointer(value int32) *int32 {
	return &value
}

func newUInt32Pointer(value uint32) *uint32 {
	return &value
}

type PairValueClk struct { //Used by MW-Register
	Value interface{}
	Clk   clocksi.Timestamp
}

type Unique uint64

type Number interface {
	int64 | float64 | uint64 | int32 | float32 | uint32 | int16 | uint16 | int8 | uint8 | int | uint
}

type SignedNumber interface {
	int64 | float64 | int32 | float32 | int16 | int8 | int
}

type AnyInt interface {
	int64 | int32 | int16 | int8 | int
}

type AnyUInt interface {
	uint64 | uint32 | uint16 | uint8 | uint
}

type AnyFloat interface {
	float64 | float32
}

/*
type USetElemPair struct {
	Element
	UniqueSet
}
*/

type UniqueElemPair struct {
	Element
	Unique
}

// Standard map operations also work on this datatype
type UniqueSet map[Unique]struct{}

func makeUniqueSet() (set UniqueSet) {
	return UniqueSet(make(map[Unique]struct{}))
}

func makeUniqueSetWithSize(size int) (set UniqueSet) {
	return UniqueSet(make(map[Unique]struct{}, size))
}

// Adds an element to the set. This hides the internal representation of the set
func (set UniqueSet) add(uniqueId Unique) {
	set[uniqueId] = struct{}{}
}

func (set UniqueSet) addAll(otherSet UniqueSet) {
	for key := range otherSet {
		set[key] = struct{}{}
	}
}

// Removes all elements in the intersection of both sets.
func (set UniqueSet) removeAllIn(sourceSet UniqueSet) {
	for key := range sourceSet {
		delete(set, key)
	}
}

// Same as removeAllIn, but also returns the set of intersected uniques
func (set UniqueSet) getAndRemoveIntersection(sourceSet UniqueSet) (intersectionSet UniqueSet) {
	intersectionSet = makeUniqueSet()
	hasKey := false
	for key := range sourceSet {
		_, hasKey = set[key]
		if hasKey {
			intersectionSet[key] = struct{}{}
			delete(set, key)
		}
	}
	return
}

func (set UniqueSet) copy() (copySet UniqueSet) {
	copySet = makeUniqueSet()
	for unique := range set {
		copySet[unique] = struct{}{}
	}
	return
}

/***** CRDT INITIALIZATION *****/

// dataType is an optional argument only required for CRDTs with generic implementations.
func InitializeCrdt(crdtType proto.CRDTType, dataType proto.DATAType, replicaID uint16) (newCrdt CRDT) {
	if shared.IsCRDTDisabled {
		return (&EmptyCrdt{})
	}
	switch crdtType {
	case proto.CRDTType_COUNTER:
		newCrdt = (&CounterCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_LWWREG:
		newCrdt = (&LwwRegisterCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_COUNTER_FLOAT:
		newCrdt = (&CounterFloatCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_ORSET:
		newCrdt = (&SetAWCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_ORMAP:
		newCrdt = (&ORMapCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_TOPK_RMV:
		newCrdt = (&TopKRmvCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_AVG:
		newCrdt = (&AvgCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_MAXMIN:
		newCrdt = (&MaxMinCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_RRMAP:
		newCrdt = (&RWEmbMapCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_TOPSUM:
		newCrdt = (&TopSumCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_TOPK:
		newCrdt = (&TopKCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_FLAG_LWW:
		newCrdt = (&LwwFlagCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_FLAG_EW:
		newCrdt = (&EwFlagCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_FLAG_DW:
		newCrdt = (&DwFlagCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_FATCOUNTER:
		newCrdt = (&BoundedCounterCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_PAIR_COUNTER:
		newCrdt = (&PairCounterCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_ARRAY_COUNTER:
		newCrdt = (&CounterArrayCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_ARRAY_FLOAT:
		newCrdt = (&FloatArrayCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_MULTI_ARRAY:
		newCrdt = (&MultiArrayCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_MVREG:
		newCrdt = (&MVRegisterCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_SIMPLE_DATE:
		newCrdt = (&SimpleDateCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_SETW_DATE:
		newCrdt = (&SetWDateCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_INCW_DATE:
		newCrdt = (&IncWDateCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_SET_ONLY_DATE:
		newCrdt = (&SetOnlyDateCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_ARRAY_COMPACT:
		newCrdt = (&CompactArrayCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_ARRAY_STRING:
		newCrdt = (&StringArrayCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_MAP_COUNTER:
		newCrdt = initializeCounterMap(dataType, replicaID)
	case proto.CRDTType_ARRAY_BYTE: //TODO: Not yet coded in crdtProtoLib.
		newCrdt = (&ByteArrayCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_TOPK_RMV_EXT:
		newCrdt = (&TopKRmvExtTopCrdt{}).Initialize(nil, replicaID)
	case proto.CRDTType_GMAP, proto.CRDTType_RWSET, proto.CRDTType_LEADERBOARD, proto.CRDTType_NOOP:
		//Ignore.
	default:
		fmt.Printf("[CRDT]Warning - unknown CRDT type in initialization: %v. Maybe a new CRDT was coded but not added to commonTools.go, InitializeCrdt?\n", crdtType)
		newCrdt = nil
	}
	return
}

/***** CONVERSION STUFF *****/

func ElementArrayToByteMatrix(elements []Element) (converted [][]byte) {
	converted = make([][]byte, len(elements))
	for i, value := range elements {
		converted[i] = []byte(value)
	}
	return
}

func ByteMatrixToElementArray(bytes [][]byte) (elements []Element) {
	elements = make([]Element, len(bytes))
	for i, value := range bytes {
		elements[i] = Element(value)
	}
	return
}

func UInt64ArrayToUniqueSet(uniques []uint64) (uniqueSet UniqueSet) {
	uniqueSet = makeUniqueSetWithSize(len(uniques))
	for _, unique := range uniques {
		uniqueSet.add(Unique(unique))
	}
	return
}

func UniqueSetToUInt64Array(uniqueSet UniqueSet) (uniques []uint64) {
	uniques = make([]uint64, len(uniqueSet))
	j := 0
	for unique := range uniqueSet {
		uniques[j] = uint64(unique)
		j++
	}
	return
}

/***** MISCELLANEOUS *****/

func min[V Number](f V, s V) V {
	if f < s {
		return f
	}
	return s
}

func max[V Number](f V, s V) V {
	if f > s {
		return f
	}
	return s
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func DataHelper(data []byte) *[]byte {
	if len(data) == 0 {
		return emptyData
	}
	return &data
}
