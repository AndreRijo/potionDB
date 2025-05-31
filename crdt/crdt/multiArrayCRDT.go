package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"time"

	pb "google.golang.org/protobuf/proto"
)

//Note: for an idea on how to support an average with only one int64, check the end of the file.

//An array holding multiple types, each type on its own array.
//It's useful to help represent table-like information in an efficient way, while keeping datatype-specific operations available.
//Note: For decrements of ints and floats, use the inc version with a negative value.
//For averages, use a negative count.
//This decision was made to reduce the number of operation types, for efficiency.
//Note: For updates of type "Sub", if Changes/Values only has 1 position, it applies that same change to all Pos.

//TODO: The dataCounters do not guarantee convergence - this must be addressed.
//As of now it is assumed their values are set only once.
//Likely solution: similar to lwwRegister, a ts + replicaID for each position of dataCounters.
//Consider doing this in a single int64 slice for efficiency: ts in the upper 48 bits, replicaID in the next 16 bits.

//TODO: If need to squeeze a bit more performance for avg, make a struct of PairInt and squeeze them both together.
//A quick benchmarks shows that updating a struct with two int64 is faster than updating two slices of int64 by around ~13%.

type MultiArrayCrdt struct {
	CRDTVM
	intCounters, sums, counts []int64
	floatCounters             []float64
	dataCounters              [][]byte
	//LWW implementation for dataCounters. The first 48 bits are the ts, the next 16 bits are the replicaID.
	//The lowest 48 bits of a 64bit timestamp are used, as what is relevant is not the absolute time, but just uniqueness and relative time.
	//Uint64 is used in order to facilitate direct comparison with < and >. (as in practice, timestamps are always positive)
	dataTsId  []uint64
	replicaID int64 //ID of this replica. Stored as 64 bits to avoid extra conversions.
}

// Helps hold temporary arrays. Useful during reads for applying pending updates.
type MultiArrayHolder struct {
	IntCounters, Sums, Counts []int64
	FloatCounters             []float64
	DataCounters              [][]byte
}

//States

type IntArrayState []int64
type FloatArrayState []float64
type AvgArrayState struct {
	Sums   []int64
	Counts []int64
}
type DataArrayState [][]byte
type MultiArrayState struct { //ApbGetMultiArrayResp
	IntCounters, Sums, Counts []int64
	FloatCounters             []float64
	DataCounters              [][]byte
}

type IntArraySingleState int64
type FloatArraySingleState float64
type AvgArraySingleState struct {
	Sum   int64
	Count int64
}
type DataArraySingleState []byte
type MultiArraySingleState struct {
	IntValue, Sum, Count int64
	FloatValue           float64
	DataValue            []byte
}

type MultiArrayDataSliceIntPosState struct {
	DataCounters [][]byte
	IntValue     int64
}

type MultiArrayDataSliceFloatPosState struct {
	DataCounters [][]byte
	FloatValue   float64
}

type MultiArrayDataSliceAvgPosState struct {
	DataCounters [][]byte
	Sum, Count   int64
}

type MULTI_ARRAY_TYPE byte
type CUSTOM_TYPE byte

const (
	MultiInts, MultiFloats, MultiData, MultiAvg, Multi, MultiSize  = MULTI_ARRAY_TYPE(0), MULTI_ARRAY_TYPE(1), MULTI_ARRAY_TYPE(2), MULTI_ARRAY_TYPE(3), MULTI_ARRAY_TYPE(4), MULTI_ARRAY_TYPE(100)
	CustomIgnore, CustomFull, CustomSingle, CustomRange, CustomSub = CUSTOM_TYPE(0), CUSTOM_TYPE(1), CUSTOM_TYPE(2), CUSTOM_TYPE(3), CUSTOM_TYPE(4)
)

// Reads
type MultiArrayFullArguments MULTI_ARRAY_TYPE

type MultiArrayPosArguments struct {
	Pos       int32
	ArrayType MULTI_ARRAY_TYPE
}

// Note: [From:To], i.e., excluding To.
type MultiArrayRangeArguments struct {
	From, To  int32
	ArrayType MULTI_ARRAY_TYPE
}

type MultiArraySubArguments struct {
	Positions []int32
	ArrayType MULTI_ARRAY_TYPE
}

type MultiArrayFullTypesArguments []MULTI_ARRAY_TYPE

type MultiArrayPosTypesArguments struct {
	Positions []int32
	Types     []MULTI_ARRAY_TYPE
}

type MultiArrayRangeTypesArguments struct {
	From, To []int32
	Types    []MULTI_ARRAY_TYPE
}

type MultiArraySubTypesArguments struct {
	Positions [][]int32 //This one will probably need to change with the protobufs :(
	Types     []MULTI_ARRAY_TYPE
}

// Checks if a given position respects a given condition.
// If yes, it returns the value of that position + of the full reads.
// Always returns a MultiArrayState.
type MultiArrayComparableArguments struct {
	FullReads        []MULTI_ARRAY_TYPE
	ComparePos       int32
	CompareType      CompareArguments
	CompareArrayType MULTI_ARRAY_TYPE
}

// int32: position of int to return
type MultiArrayDataIntArguments int32

type MultiArrayDataIntComparableArguments struct {
	ComparePos  int32
	CompareType CompareArguments
}

type MultiArrayDataFloatComparableArguments struct {
	ComparePos  int32
	CompareType CompareArguments
}

type MultiArrayDataAvgComparableArguments struct {
	ComparePos  int32
	CompareType CompareArguments
}

// This read can do, for each type, either a full read, single position read, range read or multi position read.
// It will always return a MultiArrayState, for simplicity's sake.
// The idea is the following, for each parameter:
// If not present: do not read that array;
// If present with one position >= 0: read only that position
// If present with one position < 0: read the whole array
// If present with two positions, where [1] > [0]: read from position [0] to [1]
// If present with 3+ positions, or [1] < [0]: read only the positions in the array.
// Note: if wanting to read only 2 positions, put the higher index before the lower index.
type MultiArrayCustomArguments struct {
	IntPos, FloatPos, DataPos, AvgPos []int32
}

// Updates

// This interface helps to identify the type of updates when applying updsNotYetApplied,
// avoiding the usage of long switch statements in some situations.
type MultiArrayUpd interface {
	UpdateArguments
	GetDataType() MULTI_ARRAY_TYPE
}

// All arguments are optional. If 0 or negative, they are ignored.
type MultiArraySetSizes struct {
	IntSize, FloatSize, RegisterSize, AvgSize int32
}

// Counter
// Can't re-use the ones from counterArrayCrdt.go as they would end up in the wrong section of protobufs.
// Besides, this way the name matches the updates of the other types :)))
type MultiArrayIncIntSingle struct {
	Pos    int32
	Change int64
}

// Incs to apply
type MultiArrayIncInt []int64

type MultiArrayIncIntPositions struct {
	Pos     []int32
	Changes []int64
}

type MultiArrayIncIntRange struct { //Note: Includes both ends.
	From, To int32
	Change   int64
}

// FloatCounter
type MultiArrayIncFloatSingle struct {
	Pos    int32
	Change float64
}

// Incs to apply
type MultiArrayIncFloat []float64

type MultiArrayIncFloatPositions struct {
	Pos     []int32
	Changes []float64
}

type MultiArrayIncFloatRange struct {
	From, To int32
	Change   float64
}

// Register
type MultiArraySetRegisterSingle struct {
	Pos   int32
	Value []byte
}

// Values to be set
type MultiArraySetRegister [][]byte

type MultiArraySetRegisterPositions struct {
	Pos    []int32
	Values [][]byte
}

type MultiArraySetRegisterRange struct {
	From, To int32
	Value    []byte
}

// Average
type MultiArrayIncAvgSingle struct {
	Value      int64
	Count, Pos int32
}

type MultiArrayIncAvg struct {
	Value []int64
	Count []int32
}

type MultiArrayIncAvgPositions struct {
	Value      []int64
	Count, Pos []int32
}

type MultiArrayIncAvgRange struct {
	From, To int32
	Value    int64
	Count    int32
}

// Multi. Usefull for setting the initial state e.g. inside a map.

type MultiArrayUpdateAll struct {
	Ints, Sums []int64
	Floats     []float64
	Data       [][]byte
	Counts     []int32
}

// Register downstreams (needed due to clock)

type DownstreamMultiArraySetRegisterSingle struct {
	Pos   int32
	Value []byte
	TsId  uint64
}

type DownstreamMultiArraySetRegister struct {
	Values [][]byte
	TsId   uint64 //Same tsId for all values
}

type DownstreamMultiArraySetRegisterPositions struct {
	Pos    []int32
	Values [][]byte
	TsId   uint64 //Same tsId for all values
}

type DownstreamMultiArraySetRegisterRange struct {
	From, To int32
	Value    []byte
	TsId     uint64 //Same tsId for all values
}

// Multi downstream (needed due to clock for data. If data is not present, ts is ignored)
type DownstreamMultiArrayUpdateAll struct {
	MultiArrayUpdateAll
	TsId uint64 //Only set if data != nil. Also, same tsId for all values.
}

type MultiArraySetSizesEffect MultiArraySetSizes
type MultiArrayIncIntSingleEffect MultiArrayIncIntSingle
type MultiArrayIncIntEffect MultiArrayIncInt
type MultiArrayIncIntPositionsEffect MultiArrayIncIntPositions
type MultiArrayIncIntRangeEffect MultiArrayIncIntRange
type MultiArrayIncFloatSingleEffect MultiArrayIncFloatSingle
type MultiArrayIncFloatEffect MultiArrayIncFloat
type MultiArrayIncFloatPositionsEffect MultiArrayIncFloatPositions
type MultiArrayIncFloatRangeEffect MultiArrayIncFloatRange
type MultiArraySetRegisterSingleEffect struct {
	Pos   int32
	Value []byte
	TsId  uint64
}
type MultiArraySetRegisterEffect struct {
	Values [][]byte
	TsIds  []uint64 //Array as the old values could have different TsIds.
}
type MultiArraySetRegisterPositionsEffect struct {
	Pos    []int32
	Values [][]byte
	TsIds  []uint64
}
type MultiArraySetRegisterRangeEffect struct {
	From, To int32
	Values   [][]byte
	TsIds    []uint64
}
type MultiArrayUpdateAllEffect DownstreamMultiArrayUpdateAll

// type MultiArraySetRegisterAllEffect MultiArraySetRegisterAll
type MultiArrayIncAvgSingleEffect MultiArrayIncAvgSingle
type MultiArrayIncAvgEffect MultiArrayIncAvg
type MultiArrayIncAvgPositionsEffect MultiArrayIncAvgPositions
type MultiArrayIncAvgRangeEffect MultiArrayIncAvgRange

type MultiArrayIncIntPositionsWithSizeEffect struct {
	IncEff  MultiArrayIncIntPositionsEffect
	OldSize int
}
type MultiArrayIncFloatPositionsWithSizeEffect struct {
	IncEff  MultiArrayIncFloatPositionsEffect
	OldSize int
}
type MultiArraySetRegisterPositionsWithSizeEffect struct {
	IncEff  MultiArraySetRegisterPositionsEffect
	OldSize int
}
type MultiArrayIncAvgPositionsWithSizeEffect struct {
	IncEff  MultiArrayIncAvgPositionsEffect
	OldSize int
}
type MultiArrayIncFloatWithSizeEffect struct {
	IncEff  MultiArrayIncFloatEffect
	OldSize int
}
type MultiArraySetRegisterWithSizeEffect struct {
	IncEff  MultiArraySetRegisterEffect
	OldSize int
}
type MultiArrayIncAvgWithSizeEffect struct {
	IncEff  MultiArrayIncAvgEffect
	OldSize int
}
type MultiArrayIncIntRangeWithSizeEffect struct {
	IncEff  MultiArrayIncIntRangeEffect
	OldSize int
}
type MultiArrayIncFloatRangeWithSizeEffect struct {
	IncEff  MultiArrayIncFloatRangeEffect
	OldSize int
}
type MultiArraySetRegisterRangeWithSizeEffect struct {
	IncEff  MultiArraySetRegisterRangeEffect
	OldSize int
}
type MultiArrayIncAvgRangeWithSizeEffect struct {
	IncEff  MultiArrayIncAvgRangeEffect
	OldSize int
}
type MultiArrayUpdateAllWithSizeEffect struct {
	DownstreamMultiArrayUpdateAll
	MultiArraySetSizesEffect
}

func (crdt *MultiArrayCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }

// Updates
func (args MultiArraySetSizes) GetCRDTType() proto.CRDTType        { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncIntSingle) GetCRDTType() proto.CRDTType    { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncInt) GetCRDTType() proto.CRDTType          { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncIntPositions) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncIntRange) GetCRDTType() proto.CRDTType     { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncFloatSingle) GetCRDTType() proto.CRDTType  { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncFloat) GetCRDTType() proto.CRDTType        { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncFloatPositions) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayIncFloatRange) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArraySetRegisterSingle) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArraySetRegister) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArraySetRegisterPositions) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArraySetRegisterRange) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayIncAvgSingle) GetCRDTType() proto.CRDTType    { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncAvg) GetCRDTType() proto.CRDTType          { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncAvgPositions) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayIncAvgRange) GetCRDTType() proto.CRDTType     { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayUpdateAll) GetCRDTType() proto.CRDTType       { return proto.CRDTType_MULTI_ARRAY }
func (args DownstreamMultiArraySetRegisterSingle) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args DownstreamMultiArraySetRegister) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args DownstreamMultiArraySetRegisterPositions) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args DownstreamMultiArraySetRegisterRange) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args DownstreamMultiArrayUpdateAll) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArraySetSizes) MustReplicate() bool                       { return true }
func (args MultiArrayIncIntSingle) MustReplicate() bool                   { return true }
func (args MultiArrayIncInt) MustReplicate() bool                         { return true }
func (args MultiArrayIncIntPositions) MustReplicate() bool                { return true }
func (args MultiArrayIncIntRange) MustReplicate() bool                    { return true }
func (args MultiArrayIncFloatSingle) MustReplicate() bool                 { return true }
func (args MultiArrayIncFloat) MustReplicate() bool                       { return true }
func (args MultiArrayIncFloatPositions) MustReplicate() bool              { return true }
func (args MultiArrayIncFloatRange) MustReplicate() bool                  { return true }
func (args MultiArrayIncAvgSingle) MustReplicate() bool                   { return true }
func (args MultiArrayIncAvg) MustReplicate() bool                         { return true }
func (args MultiArrayIncAvgPositions) MustReplicate() bool                { return true }
func (args MultiArrayIncAvgRange) MustReplicate() bool                    { return true }
func (args DownstreamMultiArraySetRegisterSingle) MustReplicate() bool    { return true }
func (args DownstreamMultiArraySetRegister) MustReplicate() bool          { return true }
func (args DownstreamMultiArraySetRegisterPositions) MustReplicate() bool { return true }
func (args DownstreamMultiArraySetRegisterRange) MustReplicate() bool     { return true }
func (args MultiArraySetRegisterRange) MustReplicate() bool               { return true }
func (args DownstreamMultiArrayUpdateAll) MustReplicate() bool            { return true }

// States
func (state IntArrayState) GetCRDTType() proto.CRDTType         { return proto.CRDTType_MULTI_ARRAY }
func (state IntArrayState) GetREADType() proto.READType         { return proto.READType_MULTI_RANGE }
func (state FloatArrayState) GetCRDTType() proto.CRDTType       { return proto.CRDTType_MULTI_ARRAY }
func (state FloatArrayState) GetREADType() proto.READType       { return proto.READType_MULTI_RANGE }
func (state AvgArrayState) GetCRDTType() proto.CRDTType         { return proto.CRDTType_MULTI_ARRAY }
func (state AvgArrayState) GetREADType() proto.READType         { return proto.READType_MULTI_RANGE }
func (state DataArrayState) GetCRDTType() proto.CRDTType        { return proto.CRDTType_MULTI_ARRAY }
func (state DataArrayState) GetREADType() proto.READType        { return proto.READType_MULTI_RANGE }
func (state MultiArrayState) GetCRDTType() proto.CRDTType       { return proto.CRDTType_MULTI_ARRAY }
func (state MultiArrayState) GetREADType() proto.READType       { return proto.READType_FULL }
func (state IntArraySingleState) GetCRDTType() proto.CRDTType   { return proto.CRDTType_MULTI_ARRAY }
func (state IntArraySingleState) GetREADType() proto.READType   { return proto.READType_MULTI_SINGLE }
func (state FloatArraySingleState) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (state FloatArraySingleState) GetREADType() proto.READType { return proto.READType_MULTI_SINGLE }
func (state AvgArraySingleState) GetCRDTType() proto.CRDTType   { return proto.CRDTType_MULTI_ARRAY }
func (state AvgArraySingleState) GetREADType() proto.READType   { return proto.READType_MULTI_SINGLE }
func (state DataArraySingleState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (state DataArraySingleState) GetREADType() proto.READType {
	return proto.READType_MULTI_SINGLE
}
func (state MultiArraySingleState) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (state MultiArraySingleState) GetREADType() proto.READType { return proto.READType_MULTI_SINGLE }
func (state MultiArrayDataSliceIntPosState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (state MultiArrayDataSliceIntPosState) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_COND
}
func (state MultiArrayDataSliceFloatPosState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (state MultiArrayDataSliceFloatPosState) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_COND
}
func (state MultiArrayDataSliceAvgPosState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (state MultiArrayDataSliceAvgPosState) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_COND
}

// Reads
func (args MultiArrayFullArguments) GetCRDTType() proto.CRDTType  { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayPosArguments) GetCRDTType() proto.CRDTType   { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayRangeArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArraySubArguments) GetCRDTType() proto.CRDTType   { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayFullTypesArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayPosTypesArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayRangeTypesArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArraySubTypesArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayCustomArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_MULTI_ARRAY }
func (args MultiArrayComparableArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayDataIntArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayDataIntComparableArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayDataFloatComparableArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayDataAvgComparableArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_MULTI_ARRAY
}
func (args MultiArrayFullArguments) GetREADType() proto.READType { return proto.READType_MULTI_FULL }
func (args MultiArrayPosArguments) GetREADType() proto.READType  { return proto.READType_MULTI_SINGLE }
func (args MultiArrayRangeArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_RANGE
}
func (args MultiArraySubArguments) GetREADType() proto.READType { return proto.READType_MULTI_SUB }
func (args MultiArrayFullTypesArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_FULL
}
func (args MultiArrayPosTypesArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_SINGLE
}
func (args MultiArrayRangeTypesArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_RANGE
}
func (args MultiArraySubTypesArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_SUB
}
func (args MultiArrayCustomArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_CUSTOM
}
func (args MultiArrayComparableArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_COND
}
func (args MultiArrayDataIntArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_INT
}
func (args MultiArrayDataIntComparableArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_COND
}
func (args MultiArrayDataFloatComparableArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_COND
}
func (args MultiArrayDataAvgComparableArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_DATA_COND
}

func (args MultiArrayFullArguments) HasInnerReads() bool                { return false }
func (args MultiArrayPosArguments) HasInnerReads() bool                 { return false }
func (args MultiArrayRangeArguments) HasInnerReads() bool               { return false }
func (args MultiArraySubArguments) HasInnerReads() bool                 { return false }
func (args MultiArrayFullTypesArguments) HasInnerReads() bool           { return false }
func (args MultiArrayPosTypesArguments) HasInnerReads() bool            { return false }
func (args MultiArrayRangeTypesArguments) HasInnerReads() bool          { return false }
func (args MultiArraySubTypesArguments) HasInnerReads() bool            { return false }
func (args MultiArrayCustomArguments) HasInnerReads() bool              { return false }
func (args MultiArrayComparableArguments) HasInnerReads() bool          { return false }
func (args MultiArrayDataIntArguments) HasInnerReads() bool             { return false }
func (args MultiArrayDataIntComparableArguments) HasInnerReads() bool   { return false }
func (args MultiArrayDataFloatComparableArguments) HasInnerReads() bool { return false }
func (args MultiArrayDataAvgComparableArguments) HasInnerReads() bool   { return false }
func (args MultiArrayFullArguments) HasVariables() bool                 { return false }
func (args MultiArrayPosArguments) HasVariables() bool                  { return false }
func (args MultiArrayRangeArguments) HasVariables() bool                { return false }
func (args MultiArraySubArguments) HasVariables() bool                  { return false }
func (args MultiArrayFullTypesArguments) HasVariables() bool            { return false }
func (args MultiArrayPosTypesArguments) HasVariables() bool             { return false }
func (args MultiArrayRangeTypesArguments) HasVariables() bool           { return false }
func (args MultiArraySubTypesArguments) HasVariables() bool             { return false }
func (args MultiArrayCustomArguments) HasVariables() bool               { return false }
func (args MultiArrayComparableArguments) HasVariables() bool           { return false }
func (args MultiArrayDataIntArguments) HasVariables() bool              { return false }
func (args MultiArrayDataIntComparableArguments) HasVariables() bool    { return false }
func (args MultiArrayDataFloatComparableArguments) HasVariables() bool  { return false }
func (args MultiArrayDataAvgComparableArguments) HasVariables() bool    { return false }

func (args MultiArraySetSizes) GetDataType() MULTI_ARRAY_TYPE                       { return MultiSize }
func (args MultiArrayIncFloatSingle) GetDataType() MULTI_ARRAY_TYPE                 { return MultiFloats }
func (args MultiArrayIncFloat) GetDataType() MULTI_ARRAY_TYPE                       { return MultiFloats }
func (args MultiArrayIncFloatPositions) GetDataType() MULTI_ARRAY_TYPE              { return MultiFloats }
func (args MultiArrayIncFloatRange) GetDataType() MULTI_ARRAY_TYPE                  { return MultiFloats }
func (args MultiArraySetRegisterSingle) GetDataType() MULTI_ARRAY_TYPE              { return MultiData }
func (args MultiArraySetRegister) GetDataType() MULTI_ARRAY_TYPE                    { return MultiData }
func (args MultiArraySetRegisterPositions) GetDataType() MULTI_ARRAY_TYPE           { return MultiData }
func (args MultiArraySetRegisterRange) GetDataType() MULTI_ARRAY_TYPE               { return MultiData }
func (args MultiArrayIncAvgSingle) GetDataType() MULTI_ARRAY_TYPE                   { return MultiAvg }
func (args MultiArrayIncAvg) GetDataType() MULTI_ARRAY_TYPE                         { return MultiAvg }
func (args MultiArrayIncAvgPositions) GetDataType() MULTI_ARRAY_TYPE                { return MultiAvg }
func (args MultiArrayIncAvgRange) GetDataType() MULTI_ARRAY_TYPE                    { return MultiAvg }
func (args MultiArrayIncIntSingle) GetDataType() MULTI_ARRAY_TYPE                   { return MultiInts }
func (args MultiArrayIncInt) GetDataType() MULTI_ARRAY_TYPE                         { return MultiInts }
func (args MultiArrayIncIntPositions) GetDataType() MULTI_ARRAY_TYPE                { return MultiInts }
func (args MultiArrayIncIntRange) GetDataType() MULTI_ARRAY_TYPE                    { return MultiInts }
func (args MultiArrayUpdateAll) GetDataType() MULTI_ARRAY_TYPE                      { return Multi }
func (args DownstreamMultiArraySetRegisterSingle) GetDataType() MULTI_ARRAY_TYPE    { return MultiData }
func (args DownstreamMultiArraySetRegister) GetDataType() MULTI_ARRAY_TYPE          { return MultiData }
func (args DownstreamMultiArraySetRegisterPositions) GetDataType() MULTI_ARRAY_TYPE { return MultiData }
func (args DownstreamMultiArraySetRegisterRange) GetDataType() MULTI_ARRAY_TYPE     { return MultiData }
func (args DownstreamMultiArrayUpdateAll) GetDataType() MULTI_ARRAY_TYPE            { return Multi }

//CRDT Code.

func (crdt *MultiArrayCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &MultiArrayCrdt{replicaID: int64(replicaID), CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *MultiArrayCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *MultiArrayCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	crdt.replicaID = int64(replicaID)
	return crdt
}

func (crdt *MultiArrayCrdt) IsBigCRDT() bool {
	return len(crdt.intCounters)+len(crdt.floatCounters)+len(crdt.dataCounters)+len(crdt.counts) >= 500
}

func (crdt *MultiArrayCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case MultiArrayDataIntArguments:
		return crdt.getDataIntState(updsNotYetApplied, int32(typedArgs))
	case MultiArrayDataIntComparableArguments:
		return crdt.getDataIntComparableState(updsNotYetApplied, typedArgs.ComparePos, typedArgs.CompareType)
	case MultiArrayDataFloatComparableArguments:
		return crdt.getDataFloatComparableState(updsNotYetApplied, typedArgs.ComparePos, typedArgs.CompareType)
	case MultiArrayDataAvgComparableArguments:
		return crdt.getDataAvgComparableState(updsNotYetApplied, typedArgs.ComparePos, typedArgs.CompareType)
	case MultiArrayComparableArguments:
		return crdt.getComparableState(updsNotYetApplied, typedArgs)
	case MultiArrayFullArguments:
		return crdt.getFullStateOfType(updsNotYetApplied, MULTI_ARRAY_TYPE(typedArgs))
	case MultiArrayFullTypesArguments:
		return crdt.getFullStateOfMultipleTypes(updsNotYetApplied, typedArgs)
	case MultiArrayCustomArguments:
		return crdt.getCustomState(updsNotYetApplied, typedArgs)
	case MultiArrayPosArguments:
		return crdt.getPosStateOfType(updsNotYetApplied, int(typedArgs.Pos), typedArgs.ArrayType)
	case MultiArrayPosTypesArguments:
		return crdt.getPosStateOfMultipleTypes(updsNotYetApplied, typedArgs.Positions, typedArgs.Types)
	case MultiArrayRangeArguments:
		return crdt.getRangeStateOfType(updsNotYetApplied, int(typedArgs.From), int(typedArgs.To), typedArgs.ArrayType)
	case MultiArrayRangeTypesArguments:
		return crdt.getRangeStateOfMultipleTypes(updsNotYetApplied, typedArgs.From, typedArgs.To, typedArgs.Types)
	case MultiArraySubArguments:
		return crdt.getSubStateOfType(updsNotYetApplied, typedArgs.Positions, typedArgs.ArrayType)
	case MultiArraySubTypesArguments:
		return crdt.getSubStateOfMultipleTypes(updsNotYetApplied, typedArgs.Positions, typedArgs.Types)
	default:
		fmt.Printf("[MultiArrayCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *MultiArrayCrdt) getCustomState(updsNotYetApplied []UpdateArguments, args MultiArrayCustomArguments) (state State) {
	returnState := MultiArrayState{}
	intRead, floatRead, dataRead, avgRead := crdt.getMultiArrayCustomType(args.IntPos), crdt.getMultiArrayCustomType(args.FloatPos), crdt.getMultiArrayCustomType(args.DataPos), crdt.getMultiArrayCustomType(args.AvgPos)
	if len(updsNotYetApplied) == 0 {
		switch intRead {
		case CustomIgnore:

		case CustomFull:
			returnState.IntCounters = copySlice[int64](crdt.intCounters)
		case CustomSingle:
			returnState.IntCounters = []int64{crdt.intCounters[args.IntPos[0]]}
		case CustomRange:
			returnState.IntCounters = getRangeOfSlice[int64](crdt.intCounters, int(args.IntPos[0]), int(args.IntPos[1]))
		case CustomSub:
			returnState.IntCounters = getSubSlice[int64](crdt.intCounters, args.IntPos)
		}
		switch floatRead {
		case CustomIgnore:

		case CustomFull:
			returnState.FloatCounters = copySlice[float64](crdt.floatCounters)
		case CustomSingle:
			returnState.FloatCounters = []float64{crdt.floatCounters[args.FloatPos[0]]}
		case CustomRange:
			returnState.FloatCounters = getRangeOfSlice[float64](crdt.floatCounters, int(args.FloatPos[0]), int(args.FloatPos[1]))
		case CustomSub:
			returnState.FloatCounters = getSubSlice[float64](crdt.floatCounters, args.FloatPos)
		}
		switch dataRead {
		case CustomIgnore:

		case CustomFull:
			returnState.DataCounters = copySlice[[]byte](crdt.dataCounters)
		case CustomSingle:
			returnState.DataCounters = [][]byte{crdt.dataCounters[args.DataPos[0]]}
		case CustomRange:
			returnState.DataCounters = getRangeOfSlice[[]byte](crdt.dataCounters, int(args.DataPos[0]), int(args.DataPos[1]))
		case CustomSub:
			returnState.DataCounters = getSubSlice[[]byte](crdt.dataCounters, args.DataPos)

		}
		switch avgRead {
		case CustomIgnore:

		case CustomFull:
			returnState.Sums, returnState.Counts = copySlice[int64](crdt.sums), copySlice[int64](crdt.counts)
		case CustomSingle:
			sum, count := getPositionOfAvgSlice(int(args.AvgPos[0]), crdt.sums, crdt.counts)
			returnState.Sums, returnState.Counts = []int64{sum}, []int64{count}
		case CustomRange:
			returnState.Sums, returnState.Counts = getRangeOfAvgSlice(crdt.sums, crdt.counts, int(args.FloatPos[0]), int(args.FloatPos[1]))
		case CustomSub:
			returnState.Sums, returnState.Counts = getSubAvgSlice(crdt.sums, crdt.counts, args.AvgPos)
		}
	}

	//Copying existing state
	if intRead == CustomFull || intRead == CustomSub {
		returnState.IntCounters = copySlice[int64](crdt.intCounters)
	} else if intRead == CustomSingle {
		returnState.IntCounters = []int64{crdt.intCounters[args.IntPos[0]]}
	} else if intRead == CustomRange {
		returnState.IntCounters = getRangeOfSlice[int64](crdt.intCounters, int(args.IntPos[0]), int(args.IntPos[1]))
	}
	if floatRead == CustomFull || floatRead == CustomSub {
		returnState.FloatCounters = copySlice[float64](crdt.floatCounters)
	} else if floatRead == CustomSingle {
		returnState.FloatCounters = []float64{crdt.floatCounters[args.FloatPos[0]]}
	} else if floatRead == CustomRange {
		returnState.FloatCounters = getRangeOfSlice[float64](crdt.floatCounters, int(args.FloatPos[0]), int(args.FloatPos[1]))
	}
	if dataRead == CustomFull || dataRead == CustomSub {
		returnState.DataCounters = copySlice[[]byte](crdt.dataCounters)
	} else if dataRead == CustomSingle {
		returnState.DataCounters = [][]byte{crdt.dataCounters[args.DataPos[0]]}
	} else if dataRead == CustomRange {
		returnState.DataCounters = getRangeOfSlice[[]byte](crdt.dataCounters, int(args.DataPos[0]), int(args.DataPos[1]))
	}

	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		switch typedUpd.GetDataType() {
		case MultiInts:
			switch intRead {
			case CustomIgnore:

			case CustomFull, CustomSub:
				returnState.IntCounters = crdt.applyIntUpdToTmpSlice(typedUpd, returnState.IntCounters)
			case CustomSingle:
				returnState.IntCounters[0] = crdt.applyIntUpdToSingleValue(typedUpd, returnState.IntCounters[0], args.IntPos[0])
			case CustomRange:
				returnState.IntCounters = crdt.applyIntUpdToRangeSlice(typedUpd, returnState.IntCounters, args.IntPos[0], args.IntPos[1])
			}
		case MultiFloats:
			switch floatRead {
			case CustomIgnore:

			case CustomFull, CustomSub:
				returnState.FloatCounters = crdt.applyFloatUpdToTmpSlice(typedUpd, returnState.FloatCounters)
			case CustomSingle:
				returnState.FloatCounters[0] = crdt.applyFloatUpdToSingleValue(typedUpd, returnState.FloatCounters[0], args.FloatPos[0])
			case CustomRange:
				returnState.FloatCounters = crdt.applyFloatUpdToRangeSlice(typedUpd, returnState.FloatCounters, args.FloatPos[0], args.FloatPos[1])
			}
		case MultiData:
			switch dataRead {
			case CustomIgnore:

			case CustomFull, CustomSub:
				returnState.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, returnState.DataCounters)
			case CustomSingle:
				returnState.DataCounters[0] = crdt.applyDataUpdToSingleValue(typedUpd, returnState.DataCounters[0], args.DataPos[0])
			case CustomRange:
				returnState.DataCounters = crdt.applyDataUpdToRangeSlice(typedUpd, returnState.DataCounters, args.DataPos[0], args.DataPos[1])
			}
		case MultiAvg:
			switch avgRead {
			case CustomIgnore:

			case CustomFull, CustomSub:
				returnState.Sums, returnState.Counts = crdt.applyAvgUpdToTmpSlice(typedUpd, returnState.Sums, returnState.Counts)
			case CustomSingle:
				returnState.Sums[0], returnState.Counts[0] = crdt.applyAvgUpdToSingleValue(typedUpd, returnState.Sums[0], returnState.Counts[0], args.AvgPos[0])
			case CustomRange:
				returnState.Sums, returnState.Counts = crdt.applyAvgUpdToRangeSlice(typedUpd, returnState.Sums, returnState.Counts, args.AvgPos[0], args.AvgPos[1])
			}
		}
	}

	if intRead == CustomSub {
		returnState.IntCounters = getSubSlice[int64](returnState.IntCounters, args.IntPos)
	}
	if floatRead == CustomSub {
		returnState.FloatCounters = getSubSlice[float64](returnState.FloatCounters, args.FloatPos)
	}
	if dataRead == CustomSub {
		returnState.DataCounters = getSubSlice[[]byte](returnState.DataCounters, args.DataPos)
	}
	if avgRead == CustomSub {
		returnState.Sums, returnState.Counts = getSubAvgSlice(returnState.Sums, returnState.Counts, args.AvgPos)
	}

	return returnState
}

func (crdt *MultiArrayCrdt) getMultiArrayCustomType(pos []int32) CUSTOM_TYPE {
	if len(pos) > 0 {
		if len(pos) == 1 { //Single read or full read.
			if pos[0] >= 0 { //Single read.
				return CustomSingle
			} else { //Full read.
				return CustomFull
			}
		} else if len(pos) == 2 { //Range read or subread
			if pos[1] > pos[0] { //Range read
				return CustomRange
			} else { //Subread
				return CustomSub
			}
		} else { //Subread
			return CustomSub
		}
	}
	return CustomIgnore
}

func (crdt *MultiArrayCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	returnState := MultiArrayState{IntCounters: copySliceWithNilCheck[int64](crdt.intCounters),
		FloatCounters: copySliceWithNilCheck[float64](crdt.floatCounters), DataCounters: copySliceWithNilCheck[[]byte](crdt.dataCounters),
		Sums: copySliceWithNilCheck[int64](crdt.sums), Counts: copySliceWithNilCheck[int64](crdt.counts)}

	if len(updsNotYetApplied) == 0 {
		return returnState
	}

	var typedUpd MultiArrayUpd
	var ok bool

	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			fmt.Printf("[MultiArrayCrdt]Unexpected update type: %+v\n", upd)
			continue
		}

		switch typedUpd.GetDataType() {
		case MultiInts:
			returnState.IntCounters = crdt.applyIntUpdToTmpSlice(typedUpd, returnState.IntCounters)
		case MultiFloats:
			returnState.FloatCounters = crdt.applyFloatUpdToTmpSlice(typedUpd, returnState.FloatCounters)
		case MultiData:
			returnState.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, returnState.DataCounters)
		case MultiAvg:
			returnState.Sums, returnState.Counts = crdt.applyAvgUpdToTmpSlice(typedUpd, returnState.Sums, returnState.Counts)
		}
	}
	return
}

func (crdt *MultiArrayCrdt) getFullStateOfMultipleTypes(updsNotYetApplied []UpdateArguments, arrayTypes []MULTI_ARRAY_TYPE) (state State) {
	returnState := crdt.multiCopySlices(arrayTypes)

	if len(updsNotYetApplied) == 0 { //Can already return
		return returnState
	}

	//Applies only updates regarding the relevant arrays
	returnState = crdt.multiApplyPendingUpdatesFull(returnState, arrayTypes, updsNotYetApplied)

	return returnState
}

func (crdt *MultiArrayCrdt) getDataIntState(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return MultiArrayDataSliceIntPosState{DataCounters: copySlice[[]byte](crdt.dataCounters), IntValue: crdt.intCounters[pos]}
	}

	//Have to apply updates to both that int position and also the data slice.
	intValue := int64(0)
	if int(pos) < len(crdt.intCounters) {
		intValue = crdt.intCounters[pos]
	}
	var typedUpd MultiArrayUpd
	var ok bool
	returnState := MultiArrayDataSliceIntPosState{DataCounters: copySlice[[]byte](crdt.dataCounters)}
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == MultiInts {
			intValue = crdt.applyIntUpdToSingleValue(typedUpd, intValue, pos)
		} else if typedUpd.GetDataType() == MultiData {
			returnState.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, returnState.DataCounters)
		}
	}
	returnState.IntValue = intValue
	return returnState
}

func (crdt *MultiArrayCrdt) getDataIntComparableState(updsNotYetApplied []UpdateArguments, compPos int32, compareType CompareArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		/*nPosAboveZero := 0
		for _, value := range crdt.intCounters {
			if value > 0 {
				nPosAboveZero++
			}
		}
		fmt.Printf("[MultiArrayCrdt]getDataIntCompare: Has %d above 0, out of %d (%v)\n", nPosAboveZero, len(crdt.intCounters), (float64(nPosAboveZero)/float64(len(crdt.intCounters)))*100)*/
		isCompOk := crdt.checkIntComparison(compareType.(IntCompareArguments), crdt.intCounters[compPos])
		if isCompOk {
			//fmt.Printf("[MultiArrayCrdt][getDataIntComparableState]Comparison successful. Returning state. DataCounters: %v, %v, %v\n", crdt.dataCounters[0], crdt.dataCounters[1], crdt.dataCounters[2])
			return MultiArrayDataSliceIntPosState{DataCounters: copySlice[[]byte](crdt.dataCounters), IntValue: crdt.intCounters[compPos]}
		}
		//fmt.Printf("[MultiArrayCrdt][getDataIntComparableState]Comparison failed. Returning nil.\n")
		return nil
	}

	//Two iterations: one for only the int to verify the compare value. If it succeeds, then another iteration to update the data array.
	intValue := crdt.intCounters[compPos]
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == MultiInts {
			intValue = crdt.applyIntUpdToSingleValue(typedUpd, intValue, compPos)
		}
	}
	if crdt.checkIntComparison(compareType.(IntCompareArguments), intValue) { //OK, now apply the updates to the data array
		returnState := MultiArrayDataSliceIntPosState{IntValue: intValue, DataCounters: copySlice[[]byte](crdt.dataCounters)}
		for _, upd := range updsNotYetApplied {
			typedUpd, ok = upd.(MultiArrayUpd)
			if !ok {
				continue
			}
			if typedUpd.GetDataType() == MultiData {
				returnState.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, returnState.DataCounters)
			}
		}
		return returnState
	}
	return nil
}

func (crdt *MultiArrayCrdt) getDataFloatComparableState(updsNotYetApplied []UpdateArguments, compPos int32, compareType CompareArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		isCompOk := crdt.checkFloatComparison(compareType.(FloatCompareArguments), crdt.floatCounters[compPos])
		if isCompOk {
			return MultiArrayDataSliceFloatPosState{DataCounters: copySlice[[]byte](crdt.dataCounters), FloatValue: crdt.floatCounters[compPos]}
		}
		return nil
	}

	//Two iterations: one for only the float to verify the compare value. If it succeeds, then another iteration to update the data array.
	floatValue := crdt.floatCounters[compPos]
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == MultiFloats {
			floatValue = crdt.applyFloatUpdToSingleValue(typedUpd, floatValue, compPos)
		}
	}
	if crdt.checkFloatComparison(compareType.(FloatCompareArguments), floatValue) { //OK, now apply the updates to the data array
		returnState := MultiArrayDataSliceFloatPosState{FloatValue: floatValue, DataCounters: copySlice[[]byte](crdt.dataCounters)}
		for _, upd := range updsNotYetApplied {
			typedUpd, ok = upd.(MultiArrayUpd)
			if !ok {
				continue
			}
			if typedUpd.GetDataType() == MultiData {
				returnState.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, returnState.DataCounters)
			}
		}
		return returnState
	}
	return nil
}

func (crdt *MultiArrayCrdt) getDataAvgComparableState(updsNotYetApplied []UpdateArguments, compPos int32, compareType CompareArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		isCompOk := crdt.checkFloatComparison(compareType.(FloatCompareArguments), float64(crdt.sums[compPos])/float64(crdt.counts[compPos]))
		if isCompOk {
			return MultiArrayDataSliceAvgPosState{DataCounters: copySlice[[]byte](crdt.dataCounters), Sum: crdt.sums[compPos], Count: crdt.counts[compPos]}
		}
		return nil
	}

	//Two iterations: one for only the int to verify the compare value. If it succeeds, then another iteration to update the data array.
	sum, count := crdt.sums[compPos], crdt.counts[compPos]
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == MultiAvg {
			sum, count = crdt.applyAvgUpdToSingleValue(typedUpd, sum, count, compPos)
		}
	}
	if crdt.checkFloatComparison(compareType.(FloatCompareArguments), float64(sum)/float64(count)) { //OK, now apply the updates to the data array
		returnState := MultiArrayDataSliceAvgPosState{Sum: sum, Count: count, DataCounters: copySlice[[]byte](crdt.dataCounters)}
		for _, upd := range updsNotYetApplied {
			typedUpd, ok = upd.(MultiArrayUpd)
			if !ok {
				continue
			}
			if typedUpd.GetDataType() == MultiData {
				returnState.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, returnState.DataCounters)
			}
		}
		return returnState
	}
	return nil
}

func (crdt *MultiArrayCrdt) getComparableState(updsNotYetApplied []UpdateArguments, args MultiArrayComparableArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		isCompOk := crdt.checkComparison(args.CompareType, args.ComparePos, args.CompareArrayType)
		if isCompOk {
			returnState := crdt.multiCopySlices(args.FullReads)
			//Check if CompareArrayType is already included in fullReads. If it is, then return state directly. Otherwise, read that value.
			if (args.CompareArrayType == MultiInts || args.CompareArrayType == MultiAvg) && returnState.IntCounters != nil {
				return returnState
			}
			if (args.CompareArrayType == MultiFloats && returnState.FloatCounters != nil) || (args.CompareArrayType == MultiData && returnState.DataCounters != nil) {
				return returnState
			}
			switch args.CompareArrayType {
			case MultiInts:
				returnState.IntCounters = []int64{crdt.intCounters[args.ComparePos]}
			case MultiFloats:
				returnState.FloatCounters = []float64{crdt.floatCounters[args.ComparePos]}
			case MultiData:
				returnState.DataCounters = [][]byte{crdt.dataCounters[args.ComparePos]}
			case MultiAvg:
				returnState.Sums, returnState.Counts = []int64{crdt.sums[args.ComparePos]}, []int64{crdt.counts[args.ComparePos]}
			}
			return returnState
		} //else:
		return nil
	}

	//There may be updates to the comparison value. We do two iterations: one to apply only updates to the array of args.CompareArrayType.
	//Then we check the condition. If it holds, we then apply updates to the other(s) array(s).

	holder := crdt.singleCopySlicesToHolder(args.CompareArrayType)
	holder = crdt.applyPendingUpdatesFull(holder, args.CompareArrayType, updsNotYetApplied)

	if crdt.checkComparisonHolder(args.CompareType, args.ComparePos, args.CompareArrayType, holder) {
		returnState := MultiArrayState(holder)
		for i, arrayType := range args.FullReads { //Remove args.CompareArrayType from the slice, as it is already updated.
			found := true
			if arrayType == args.CompareArrayType {
				copy := make([]MULTI_ARRAY_TYPE, len(args.FullReads)-1)
				for j := 0; j < i; j++ {
					copy[j] = args.FullReads[j]
				} //And now, skip i.
				for j := i; j < len(copy); j++ {
					copy[j] = args.FullReads[j+1]
				}
				args.FullReads = copy
			}
			if !found { //The compare type is not included in the list of arrays to read. Thus, we only want to keep the position.
				switch args.CompareArrayType {
				case MultiInts:
					returnState.IntCounters = []int64{crdt.intCounters[args.ComparePos]}
				case MultiFloats:
					returnState.FloatCounters = []float64{crdt.floatCounters[args.ComparePos]}
				case MultiData:
					returnState.DataCounters = [][]byte{crdt.dataCounters[args.ComparePos]}
				case MultiAvg:
					returnState.Sums, returnState.Counts = []int64{crdt.sums[args.ComparePos]}, []int64{crdt.counts[args.ComparePos]}
				}
			}
		}
		if len(args.FullReads) == 0 { //No other array to read.
			return returnState
		}
		//Apply remaining updates
		returnState = crdt.multiApplyPendingUpdatesFull(returnState, args.FullReads, updsNotYetApplied)
		return returnState
	}

	return nil
}

func (crdt *MultiArrayCrdt) checkComparison(compareType CompareArguments, comparePos int32, compareArrayType MULTI_ARRAY_TYPE) bool {
	switch compareArrayType {
	case MultiInts:
		return crdt.checkIntComparison(compareType.(IntCompareArguments), crdt.intCounters[comparePos])
	case MultiFloats:
		return crdt.checkFloatComparison(compareType.(FloatCompareArguments), crdt.floatCounters[comparePos])
	case MultiData:
		return crdt.checkDataComparison(compareType, crdt.dataCounters[comparePos])
	case MultiAvg:
		return crdt.checkFloatComparison(compareType.(FloatCompareArguments), float64(crdt.sums[comparePos])/float64(crdt.counts[comparePos]))
	}
	return false
}

func (crdt *MultiArrayCrdt) checkComparisonHolder(compareType CompareArguments, comparePos int32, compareArrayType MULTI_ARRAY_TYPE, holder MultiArrayHolder) bool {
	switch compareArrayType {
	case MultiInts:
		return crdt.checkIntComparison(compareType.(IntCompareArguments), holder.IntCounters[comparePos])
	case MultiFloats:
		return crdt.checkFloatComparison(compareType.(FloatCompareArguments), holder.FloatCounters[comparePos])
	case MultiData:
		return crdt.checkDataComparison(compareType, holder.DataCounters[comparePos])
	case MultiAvg:
		return crdt.checkFloatComparison(compareType.(FloatCompareArguments), float64(holder.Sums[comparePos])/float64(holder.Counts[comparePos]))
	}
	return false
}

func (crdt *MultiArrayCrdt) checkIntComparison(compareType IntCompareArguments, value int64) bool {
	//fmt.Printf("Compare type: %+v, Argument value: %d, ValueInArray: %d.\n", compareType.CompType, compareType.Value, value)
	switch compareType.CompType {
	case EQ:
		return value == compareType.Value
	case NEQ:
		return value != compareType.Value
	case LEQ:
		return value <= compareType.Value
	case L:
		return value < compareType.Value
	case H:
		return value > compareType.Value
	case HEQ:
		return value >= compareType.Value
	}
	return false
}

func (crdt *MultiArrayCrdt) checkFloatComparison(compareType FloatCompareArguments, value float64) bool {
	switch compareType.CompType {
	case EQ:
		return value == compareType.Value
	case NEQ:
		return value != compareType.Value
	case LEQ:
		return value <= compareType.Value
	case L:
		return value < compareType.Value
	case H:
		return value > compareType.Value
	case HEQ:
		return value >= compareType.Value
	}
	return false
}

func (crdt *MultiArrayCrdt) checkDataComparison(compareType CompareArguments, value []byte) bool {
	switch typedComp := compareType.(type) {
	case BytesCompareArguments:
		if len(typedComp.Value) == len(value) {
			for i, dataByte := range value {
				if dataByte != typedComp.Value[i] {
					return false
				}
			}
			return true
		}
	case StringCompareArguments:
		valueS := string(value)
		return typedComp.IsEqualComp && valueS == typedComp.Value || !typedComp.IsEqualComp && valueS != typedComp.Value
	}
	return false
}

func (crdt *MultiArrayCrdt) getFullStateOfType(updsNotYetApplied []UpdateArguments, arrayType MULTI_ARRAY_TYPE) (state State) {
	if len(updsNotYetApplied) == 0 {
		switch arrayType {
		case MultiInts:
			return IntArrayState(copySlice[int64](crdt.intCounters))
		case MultiFloats:
			return FloatArrayState(copySlice[float64](crdt.floatCounters))
		case MultiData:
			return DataArrayState(copySlice[[]byte](crdt.dataCounters))
		case MultiAvg:
			sums, counts := copySlice[int64](crdt.sums), copySlice[int64](crdt.counts)
			return AvgArrayState{Sums: sums, Counts: counts}
		}
	}

	//Gets a copy of the relevant array (i.e., matching arrayType)
	slices := crdt.singleCopySlicesToHolder(arrayType)
	//Applies only updates regarding the relevant array
	slices = crdt.applyPendingUpdatesFull(slices, arrayType, updsNotYetApplied)

	switch arrayType {
	case MultiInts:
		return IntArrayState(slices.IntCounters)
	case MultiFloats:
		return FloatArrayState(slices.FloatCounters)
	case MultiData:
		return DataArrayState(slices.DataCounters)
	case MultiAvg:
		return AvgArrayState{Sums: slices.Sums, Counts: slices.Counts}
	}
	return
}

// Returns the same slice in case it gets expanded.
func (crdt *MultiArrayCrdt) applyIntUpdToTmpSlice(upd MultiArrayUpd, intC []int64) []int64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncIntSingle:
		if typedUpd.Pos >= int32(len(intC)) {
			intC = copySliceWithSize[int64](intC, typedUpd.Pos+1)
		}
		intC[typedUpd.Pos] += typedUpd.Change
	case MultiArrayIncInt:
		if len(typedUpd) > len(intC) {
			intC = copySliceWithSize[int64](intC, int32(len(typedUpd)))
		}
		for i, change := range typedUpd {
			intC[i] += change
		}
	case MultiArrayIncIntPositions:
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Pos {
				if int(pos) >= len(intC) {
					intC = copySliceWithSize[int64](intC, pos+1)
				}
				intC[pos] += change
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if int(pos) >= len(intC) {
					intC = copySliceWithSize[int64](intC, pos+1)
				}
				intC[pos] += typedUpd.Changes[i]
			}
		}
	case MultiArrayIncIntRange:
		if int(typedUpd.To) >= len(intC) { //If .From >= len(intC), so is .To >= len(intC)
			intC = copySliceWithSize[int64](intC, int32(typedUpd.To+1))
		}
		for i := typedUpd.From; i <= typedUpd.To; i++ {
			intC[i] += typedUpd.Change
		}
	}
	return intC
}

// Returns the same slice in case it gets expanded.
func (crdt *MultiArrayCrdt) applyFloatUpdToTmpSlice(upd MultiArrayUpd, floatC []float64) []float64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncFloatSingle:
		if typedUpd.Pos >= int32(len(floatC)) {
			floatC = copySliceWithSize[float64](floatC, typedUpd.Pos+1)
		}
		floatC[typedUpd.Pos] += typedUpd.Change
	case MultiArrayIncFloat:
		if len(typedUpd) > len(floatC) {
			floatC = copySliceWithSize[float64](floatC, int32(len(typedUpd)))
		}
		for i, change := range typedUpd {
			floatC[i] += change
		}
	case MultiArrayIncFloatPositions:
		if len(typedUpd.Pos) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Pos {
				if int(pos) >= len(floatC) {
					floatC = copySliceWithSize[float64](floatC, pos+1)
				}
				floatC[pos] += change
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if int(pos) >= len(floatC) {
					floatC = copySliceWithSize[float64](floatC, pos+1)
				}
				floatC[pos] += typedUpd.Changes[i]
			}
		}
	case MultiArrayIncFloatRange:
		if int(typedUpd.To) >= len(floatC) { //If .From >= len(intC), so is .To >= len(intC)
			floatC = copySliceWithSize[float64](floatC, int32(typedUpd.To+1))
		}
		for i := typedUpd.From; i <= typedUpd.To; i++ {
			floatC[i] += typedUpd.Change
		}
	}
	return floatC
}

// Returns the same slice in case it gets expanded.
func (crdt *MultiArrayCrdt) applyDataUpdToTmpSlice(upd MultiArrayUpd, dataC [][]byte) [][]byte {
	switch typedUpd := upd.(type) {
	case MultiArraySetRegisterSingle:
		if typedUpd.Pos >= int32(len(dataC)) {
			dataC = copySliceWithSize[[]byte](dataC, typedUpd.Pos+1)
		}
		dataC[typedUpd.Pos] = typedUpd.Value
	case MultiArraySetRegister:
		if len(typedUpd) > len(dataC) {
			dataC = copySliceWithSize[[]byte](dataC, int32(len(typedUpd)))
		}
		for i, value := range typedUpd {
			dataC[i] = value
		}
	case MultiArraySetRegisterPositions:
		if len(typedUpd.Pos) == 1 {
			value := typedUpd.Values[0]
			for _, pos := range typedUpd.Pos {
				if int(pos) >= len(dataC) {
					dataC = copySliceWithSize[[]byte](dataC, pos+1)
				}
				dataC[pos] = value
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if int(pos) >= len(dataC) {
					dataC = copySliceWithSize[[]byte](dataC, pos+1)
				}
				dataC[pos] = typedUpd.Values[i]
			}
		}
	case MultiArraySetRegisterRange:
		if int(typedUpd.To) >= len(dataC) { //If .From >= len(intC), so is .To >= len(intC)
			dataC = copySliceWithSize[[]byte](dataC, int32(typedUpd.To+1))
		}
		for i := typedUpd.From; i <= typedUpd.To; i++ {
			dataC[i] = typedUpd.Value
		}
	}
	return dataC
}

func (crdt *MultiArrayCrdt) applyAvgUpdToTmpSlice(upd MultiArrayUpd, sums, counts []int64) ([]int64, []int64) {
	switch typedUpd := upd.(type) {
	case MultiArrayIncAvgSingle:
		if typedUpd.Pos >= int32(len(sums)) {
			sums, counts = copySliceWithSize[int64](sums, typedUpd.Pos+1), copySliceWithSize[int64](counts, typedUpd.Pos+1)
		}
		sums[typedUpd.Pos] += typedUpd.Value
		counts[typedUpd.Pos] += int64(typedUpd.Count)
	case MultiArrayIncAvg:
		if len(typedUpd.Value) > len(sums) {
			sums, counts = copySliceWithSize[int64](sums, int32(len(typedUpd.Value))), copySliceWithSize[int64](counts, int32(len(typedUpd.Count)))
		}
		for i, value := range typedUpd.Value {
			sums[i] += value
			counts[i] += int64(typedUpd.Count[i])
		}
	case MultiArrayIncAvgPositions:
		if len(typedUpd.Pos) == 1 {
			sum, count := typedUpd.Value[0], typedUpd.Count[0]
			for _, pos := range typedUpd.Pos {
				if int(pos) >= len(sums) {
					sums = copySliceWithSize[int64](sums, pos+1)
					counts = copySliceWithSize[int64](counts, pos+1)
				}
				sums[pos] += sum
				counts[pos] += int64(count)
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if int(pos) >= len(sums) {
					sums = copySliceWithSize[int64](sums, pos+1)
					counts = copySliceWithSize[int64](counts, pos+1)
				}
				sums[pos] += typedUpd.Value[i]
				counts[pos] += int64(typedUpd.Count[i])
			}
		}
	case MultiArrayIncAvgRange:
		if int(typedUpd.To) >= len(sums) { //If .From >= len(intC), so is .To >= len(intC)
			sums, counts = copySliceWithSize[int64](sums, int32(typedUpd.To+1)), copySliceWithSize[int64](counts, int32(typedUpd.To+1))
		}
		for i := typedUpd.From; i <= typedUpd.To; i++ {
			sums[i] += typedUpd.Value
			counts[i] += int64(typedUpd.Count)
		}
	}
	return sums, counts
}

// Copies the relevant array, returning it in an holder
func (crdt *MultiArrayCrdt) singleCopySlicesToHolder(arrayType MULTI_ARRAY_TYPE) (slices MultiArrayHolder) {
	switch arrayType {
	case MultiInts:
		slices.IntCounters = copySlice[int64](crdt.intCounters)
	case MultiFloats:
		slices.FloatCounters = copySlice[float64](crdt.floatCounters)
	case MultiData:
		slices.DataCounters = copySlice[[]byte](crdt.dataCounters)
	case MultiAvg:
		slices.Sums, slices.Counts = copySlice[int64](crdt.sums), copySlice[int64](crdt.counts)
	}
	return
}

// Copies all relevant arrays
func (crdt *MultiArrayCrdt) multiCopySlices(arrayTypes []MULTI_ARRAY_TYPE) (returnState MultiArrayState) {
	for _, arrayType := range arrayTypes {
		switch arrayType {
		case MultiInts:
			returnState.IntCounters = copySlice[int64](crdt.intCounters)
		case MultiFloats:
			returnState.FloatCounters = copySlice[float64](crdt.floatCounters)
		case MultiData:
			returnState.DataCounters = copySlice[[]byte](crdt.dataCounters)
		case MultiAvg:
			returnState.Sums, returnState.Counts = copySlice[int64](crdt.sums), copySlice[int64](crdt.counts)
		}
	}
	return
}

func (crdt *MultiArrayCrdt) multiApplyPendingUpdatesFull(state MultiArrayState, arrayType []MULTI_ARRAY_TYPE, updsNotYetApplied []UpdateArguments) MultiArrayState {
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		for _, thisType := range arrayType {
			if typedUpd.GetDataType() == thisType {
				switch thisType {
				case MultiInts:
					state.IntCounters = crdt.applyIntUpdToTmpSlice(typedUpd, state.IntCounters)
				case MultiFloats:
					state.FloatCounters = crdt.applyFloatUpdToTmpSlice(typedUpd, state.FloatCounters)
				case MultiData:
					state.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, state.DataCounters)
				case MultiAvg:
					state.Sums, state.Counts = crdt.applyAvgUpdToTmpSlice(typedUpd, state.Sums, state.Counts)
				}
				break
			} else if typedUpd.GetDataType() == MultiSize {
				state = crdt.applySizeUpdHelperState(typedUpd.(MultiArraySetSizes), thisType, state)
			}
		}
	}
	return state
}

func (crdt *MultiArrayCrdt) applyPendingUpdatesFull(slices MultiArrayHolder, arrayType MULTI_ARRAY_TYPE, updsNotYetApplied []UpdateArguments) MultiArrayHolder {
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == arrayType {
			switch arrayType {
			case MultiInts:
				slices.IntCounters = crdt.applyIntUpdToTmpSlice(typedUpd, slices.IntCounters)
			case MultiFloats:
				slices.FloatCounters = crdt.applyFloatUpdToTmpSlice(typedUpd, slices.FloatCounters)
			case MultiData:
				slices.DataCounters = crdt.applyDataUpdToTmpSlice(typedUpd, slices.DataCounters)
			case MultiAvg:
				slices.Sums, slices.Counts = crdt.applyAvgUpdToTmpSlice(typedUpd, slices.Sums, slices.Counts)
			}
		} else if typedUpd.GetDataType() == MultiSize {
			slices = crdt.applySizeUpdHelper(typedUpd.(MultiArraySetSizes), arrayType, slices)
		}
	}
	return slices
}

func (crdt *MultiArrayCrdt) getPosStateOfMultipleTypes(updsNotYetApplied []UpdateArguments, positions []int32, arrayTypes []MULTI_ARRAY_TYPE) (state State) {
	returnState := MultiArraySingleState{}

	//Get current value, even if no updates are present
	for i, arrayType := range arrayTypes {
		switch arrayType {
		case MultiInts:
			returnState.IntValue = getPositionOfSlice[int64](int(positions[i]), crdt.intCounters)
		case MultiFloats:
			returnState.FloatValue = getPositionOfSlice[float64](int(positions[i]), crdt.floatCounters)
		case MultiData:
			returnState.DataValue = getPositionOfSlice[[]byte](int(positions[i]), crdt.dataCounters)
		case MultiAvg:
			returnState.Sum, returnState.Count = getPositionOfAvgSlice(int(positions[i]), crdt.sums, crdt.counts)
		}
	}

	if len(updsNotYetApplied) == 0 { //Can already return
		return returnState
	}

	//Apply updates
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		for i, thisType := range arrayTypes {
			if typedUpd.GetDataType() == thisType {
				switch thisType {
				case MultiInts:
					returnState.IntValue = crdt.applyIntUpdToSingleValue(typedUpd, returnState.IntValue, positions[i])
				case MultiFloats:
					returnState.FloatValue = crdt.applyFloatUpdToSingleValue(typedUpd, returnState.FloatValue, positions[i])
				case MultiData:
					returnState.DataValue = crdt.applyDataUpdToSingleValue(typedUpd, returnState.DataValue, positions[i])
				case MultiAvg:
					returnState.Sum, returnState.Count = crdt.applyAvgUpdToSingleValue(typedUpd, returnState.Sum, returnState.Count, positions[i])
				}
			} //MultiSize is not relevant here.
		}
	}

	return returnState
}

func (crdt *MultiArrayCrdt) getPosStateOfType(updsNotYetApplied []UpdateArguments, pos int, arrayType MULTI_ARRAY_TYPE) (state State) {
	//fmt.Printf("[MultiArray][PosRead]Sums: %v. Count: %v\n", crdt.sums, crdt.counts)
	if len(updsNotYetApplied) == 0 {
		switch arrayType {
		case MultiInts:
			return IntArraySingleState(getPositionOfSlice[int64](pos, crdt.intCounters))
		case MultiFloats:
			return FloatArraySingleState(getPositionOfSlice[float64](pos, crdt.floatCounters))
		case MultiData:
			return DataArraySingleState(getPositionOfSlice[[]byte](pos, crdt.dataCounters))
		case MultiAvg:
			//fmt.Printf("[MultiArray][PosRead]Pos: %d. Returning sum %d, count %d (sums: %v, counts: %v)\n", pos, crdt.sums[pos], crdt.counts[pos],
			//crdt.sums, crdt.counts)
			return AvgArraySingleState{Sum: getPositionOfSlice[int64](pos, crdt.sums), Count: getPositionOfSlice[int64](pos, crdt.counts)}
		}
	}

	var intV, sum, count int64
	var floatV float64
	var dataV []byte
	pos32 := int32(pos)

	//Get current value
	switch arrayType {
	case MultiInts:
		intV = getPositionOfSlice[int64](pos, crdt.intCounters)
	case MultiFloats:
		floatV = getPositionOfSlice[float64](pos, crdt.floatCounters)
	case MultiData:
		dataV = getPositionOfSlice[[]byte](pos, crdt.dataCounters)
	case MultiAvg:
		sum, count = getPositionOfAvgSlice(pos, crdt.sums, crdt.counts)
	}

	//Apply updates
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == arrayType {
			switch arrayType {
			case MultiInts:
				intV = crdt.applyIntUpdToSingleValue(typedUpd, intV, pos32)
			case MultiFloats:
				floatV = crdt.applyFloatUpdToSingleValue(typedUpd, floatV, pos32)
			case MultiData:
				dataV = crdt.applyDataUpdToSingleValue(typedUpd, dataV, pos32)
			case MultiAvg:
				sum, count = crdt.applyAvgUpdToSingleValue(typedUpd, sum, count, pos32)
			}
		} //MultiSize is not relevant here.
	}

	switch arrayType {
	case MultiInts:
		return IntArraySingleState(intV)
	case MultiFloats:
		return FloatArraySingleState(floatV)
	case MultiData:
		return DataArraySingleState(dataV)
	case MultiAvg:
		return AvgArraySingleState{Sum: sum, Count: count}
		//return AvgArraySingleState{Sum: intV >> 24, Count: intV & 0xFFFFFF}
	}

	return
}

func (crdt *MultiArrayCrdt) applyIntUpdToSingleValue(upd MultiArrayUpd, intV int64, pos int32) int64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncIntSingle:
		if typedUpd.Pos == pos {
			return intV + typedUpd.Change
		}
	case MultiArrayIncInt:
		if pos < int32(len(typedUpd)) {
			return intV + typedUpd[pos]
		}
	case MultiArrayIncIntPositions:
		for i, thisPos := range typedUpd.Pos {
			if pos == thisPos {
				return intV + typedUpd.Changes[i]
			}
		}
	case MultiArrayIncIntRange:
		if pos >= typedUpd.From && pos <= typedUpd.To {
			return intV + typedUpd.Change
		}
	}
	return intV
}

func (crdt *MultiArrayCrdt) applyFloatUpdToSingleValue(upd MultiArrayUpd, floatV float64, pos int32) float64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncFloatSingle:
		if typedUpd.Pos == pos {
			return floatV + typedUpd.Change
		}
	case MultiArrayIncFloat:
		if pos < int32(len(typedUpd)) {
			return floatV + typedUpd[pos]
		}
	case MultiArrayIncFloatPositions:
		for i, thisPos := range typedUpd.Pos {
			if pos == thisPos {
				return floatV + typedUpd.Changes[i]
			}
		}
	case MultiArrayIncFloatRange:
		if pos >= typedUpd.From && pos <= typedUpd.To {
			return floatV + typedUpd.Change
		}
	}
	return floatV
}

func (crdt *MultiArrayCrdt) applyDataUpdToSingleValue(upd MultiArrayUpd, dataV []byte, pos int32) []byte {
	switch typedUpd := upd.(type) {
	case MultiArraySetRegisterSingle:
		if typedUpd.Pos == pos {
			return typedUpd.Value
		}
	case MultiArraySetRegister:
		if pos < int32(len(typedUpd)) {
			return typedUpd[pos]
		}
	case MultiArraySetRegisterPositions:
		for i, thisPos := range typedUpd.Pos {
			if pos == thisPos {
				return typedUpd.Values[i]
			}
		}
	case MultiArraySetRegisterRange:
		if pos >= typedUpd.From && pos <= typedUpd.To {
			return typedUpd.Value
		}
	}
	return dataV
}

func (crdt *MultiArrayCrdt) applyAvgUpdToSingleValue(upd MultiArrayUpd, sumV, countV int64, pos int32) (int64, int64) {
	switch typedUpd := upd.(type) {
	case MultiArrayIncAvgSingle:
		if typedUpd.Pos == pos {
			return sumV + typedUpd.Value, countV + int64(typedUpd.Count)
		}
	case MultiArrayIncAvg:
		if pos < int32(len(typedUpd.Value)) {
			return sumV + typedUpd.Value[pos], countV + int64(typedUpd.Count[pos])
		}
	case MultiArrayIncAvgPositions:
		for i, thisPos := range typedUpd.Pos {
			if pos == thisPos {
				return sumV + typedUpd.Value[i], countV + int64(typedUpd.Count[i])
			}
		}
	case MultiArrayIncAvgRange:
		if pos >= typedUpd.From && pos <= typedUpd.To {
			return sumV + typedUpd.Value, countV + int64(typedUpd.Count)
		}
	}
	return sumV, countV
}

func (crdt *MultiArrayCrdt) getRangeStateOfMultipleTypes(updsNotYetApplied []UpdateArguments, from, to []int32, arrayTypes []MULTI_ARRAY_TYPE) (state State) {
	returnState := MultiArrayState{}

	//Gets the values in range, even if no updates are present
	for i, arrayType := range arrayTypes {
		switch arrayType {
		case MultiInts:
			returnState.IntCounters = getRangeOfSlice[int64](crdt.intCounters, int(from[i]), int(to[i]))
		case MultiFloats:
			returnState.FloatCounters = getRangeOfSlice[float64](crdt.floatCounters, int(from[i]), int(to[i]))
		case MultiData:
			returnState.DataCounters = getRangeOfSlice[[]byte](crdt.dataCounters, int(from[i]), int(to[i]))
		case MultiAvg:
			returnState.Sums, returnState.Counts = getRangeOfAvgSlice(crdt.sums, crdt.counts, int(from[i]), int(to[i]))
		}
	}

	if len(updsNotYetApplied) == 0 { //Can already return
		return returnState
	}

	//Apply updates
	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		for i, thisType := range arrayTypes {
			if typedUpd.GetDataType() == thisType {
				switch thisType {
				case MultiInts:
					returnState.IntCounters = crdt.applyIntUpdToRangeSlice(typedUpd, returnState.IntCounters, from[i], to[i])
				case MultiFloats:
					returnState.FloatCounters = crdt.applyFloatUpdToRangeSlice(typedUpd, returnState.FloatCounters, from[i], to[i])
				case MultiData:
					returnState.DataCounters = crdt.applyDataUpdToRangeSlice(typedUpd, returnState.DataCounters, from[i], to[i])
				case MultiAvg:
					returnState.Sums, returnState.Counts = crdt.applyAvgUpdToRangeSlice(typedUpd, returnState.Sums, returnState.Counts, from[i], to[i])
				}
			} else if typedUpd.GetDataType() == MultiSize {
				returnState = crdt.applySizeUpdHelperState(typedUpd.(MultiArraySetSizes), thisType, returnState)
			}
		}
	}
	return returnState
}

func (crdt *MultiArrayCrdt) getRangeStateOfType(updsNotYetApplied []UpdateArguments, from, to int, arrayType MULTI_ARRAY_TYPE) (state State) {
	if len(updsNotYetApplied) == 0 {
		switch arrayType {
		case MultiInts:
			return IntArrayState(getRangeOfSlice[int64](crdt.intCounters, from, to))
		case MultiFloats:
			return FloatArrayState(getRangeOfSlice[float64](crdt.floatCounters, from, to))
		case MultiData:
			return DataArrayState(getRangeOfSlice[[]byte](crdt.dataCounters, from, to))
		case MultiAvg:
			sums, counts := getRangeOfAvgSlice(crdt.sums, crdt.counts, from, to)
			return AvgArrayState{Sums: sums, Counts: counts}
		}
	}

	slices := MultiArrayHolder{}

	//Copy the relevant array
	switch arrayType {
	case MultiInts:
		slices.IntCounters = getRangeOfSlice[int64](crdt.intCounters, from, to)
	case MultiFloats:
		slices.FloatCounters = getRangeOfSlice[float64](crdt.floatCounters, from, to)
	case MultiData:
		slices.DataCounters = getRangeOfSlice[[]byte](crdt.dataCounters, from, to)
	case MultiAvg:
		slices.Sums, slices.Counts = getRangeOfAvgSlice(crdt.sums, crdt.counts, from, to)
	}

	var typedUpd MultiArrayUpd
	var ok bool
	for _, upd := range updsNotYetApplied {
		typedUpd, ok = upd.(MultiArrayUpd)
		if !ok {
			continue
		}
		if typedUpd.GetDataType() == arrayType {
			switch arrayType {
			case MultiInts:
				slices.IntCounters = crdt.applyIntUpdToRangeSlice(typedUpd, slices.IntCounters, int32(from), int32(to))
			case MultiFloats:
				slices.FloatCounters = crdt.applyFloatUpdToRangeSlice(typedUpd, slices.FloatCounters, int32(from), int32(to))
			case MultiData:
				slices.DataCounters = crdt.applyDataUpdToRangeSlice(typedUpd, slices.DataCounters, int32(from), int32(to))
			case MultiAvg:
				slices.Sums, slices.Counts = crdt.applyAvgUpdToRangeSlice(typedUpd, slices.Sums, slices.Counts, int32(from), int32(to))
			}
		} else if typedUpd.GetDataType() == MultiSize {
			slices = crdt.applySizeUpdHelper(typedUpd.(MultiArraySetSizes), arrayType, slices)
		}
	}

	switch arrayType {
	case MultiInts:
		return IntArrayState(slices.IntCounters)
	case MultiFloats:
		return FloatArrayState(slices.FloatCounters)
	case MultiData:
		return DataArrayState(slices.DataCounters)
	case MultiAvg:
		return AvgArrayState{Sums: slices.Sums, Counts: slices.Counts}
	}
	return
}

func (crdt *MultiArrayCrdt) applySizeUpdHelper(sizeUpd MultiArraySetSizes, arrayType MULTI_ARRAY_TYPE, slices MultiArrayHolder) MultiArrayHolder {
	switch arrayType {
	case MultiInts:
		if sizeUpd.IntSize > int32(len(slices.IntCounters)) {
			slices.IntCounters = copySliceWithSize[int64](crdt.intCounters, sizeUpd.IntSize)
		}
	case MultiFloats:
		if sizeUpd.FloatSize > int32(len(slices.FloatCounters)) {
			slices.FloatCounters = copySliceWithSize[float64](crdt.floatCounters, sizeUpd.FloatSize)
		}
	case MultiData:
		if sizeUpd.RegisterSize > int32(len(slices.DataCounters)) {
			slices.DataCounters = copySliceWithSize[[]byte](crdt.dataCounters, sizeUpd.RegisterSize)
		}
	case MultiAvg:
		if sizeUpd.IntSize > int32(len(slices.Sums)) {
			slices.Sums, slices.Counts = copySliceWithSize[int64](crdt.sums, sizeUpd.IntSize), copySliceWithSize[int64](crdt.counts, sizeUpd.IntSize)
		}
	}
	return slices
}

func (crdt *MultiArrayCrdt) applySizeUpdHelperState(sizeUpd MultiArraySetSizes, arrayType MULTI_ARRAY_TYPE, state MultiArrayState) MultiArrayState {
	switch arrayType {
	case MultiInts:
		if sizeUpd.IntSize > int32(len(state.IntCounters)) {
			state.IntCounters = copySliceWithSize[int64](state.IntCounters, sizeUpd.IntSize)
		}
	case MultiFloats:
		if sizeUpd.FloatSize > int32(len(state.FloatCounters)) {
			state.FloatCounters = copySliceWithSize[float64](state.FloatCounters, sizeUpd.FloatSize)
		}
	case MultiData:
		if sizeUpd.RegisterSize > int32(len(state.DataCounters)) {
			state.DataCounters = copySliceWithSize[[]byte](state.DataCounters, sizeUpd.RegisterSize)
		}
	case MultiAvg:
		if sizeUpd.IntSize > int32(len(state.Sums)) {
			state.Sums, state.Counts = copySliceWithSize[int64](state.Sums, sizeUpd.IntSize), copySliceWithSize[int64](state.Counts, sizeUpd.IntSize)
		}
	}
	return state
}

func (crdt *MultiArrayCrdt) applyIntUpdToRangeSlice(upd MultiArrayUpd, intC []int64, from, to int32) []int64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncIntSingle:
		if typedUpd.Pos >= to || typedUpd.Pos < from {
			return intC
		}
		if typedUpd.Pos > int32(len(intC)) {
			intC = copySliceWithSize[int64](intC, typedUpd.Pos+1)
		}
		intC[typedUpd.Pos] += typedUpd.Change
	case MultiArrayIncInt:
		upperCap, fromInt := to, int(from)
		if len(typedUpd) > len(intC) && int32(len(intC)) < to {
			if len(typedUpd) > int(to) {
				intC = copySliceWithSize[int64](intC, to)
			} else {
				intC = copySliceWithSize[int64](intC, int32(len(typedUpd)))
				upperCap = int32(len(typedUpd))
			}
		}
		if from >= upperCap { //The query starts in a position after the end of the array
			return intC
		}
		for i, change := range typedUpd[from:upperCap] {
			intC[i+fromInt] += change
		}
	case MultiArrayIncIntPositions:
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(intC)) {
						intC = copySliceWithSize[int64](intC, pos+1)
					}
					intC[pos] += change
				}
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(intC)) {
						intC = copySliceWithSize[int64](intC, pos+1)
					}
					intC[pos] += typedUpd.Changes[i]
				}
			}
		}
	case MultiArrayIncIntRange:
		if typedUpd.To < from || typedUpd.From > to { //Update is out of the query range, so no change needed.
			return intC
		}
		if typedUpd.To >= int32(len(intC)) {
			intC = copySliceWithSize[int64](intC, typedUpd.To+1)
		}
		minStart, minFinish := max(from, typedUpd.From), min(to, typedUpd.To)
		offset := max(0, from-typedUpd.From)
		for i := minStart; i <= minFinish; i++ {
			intC[i+offset] += typedUpd.Change
		}
	}
	return intC
}

func (crdt *MultiArrayCrdt) applyFloatUpdToRangeSlice(upd MultiArrayUpd, floatC []float64, from, to int32) []float64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncFloatSingle:
		if typedUpd.Pos >= to || typedUpd.Pos < from {
			return floatC
		}
		if typedUpd.Pos > int32(len(floatC)) {
			floatC = copySliceWithSize[float64](floatC, typedUpd.Pos+1)
		}
		floatC[typedUpd.Pos] += typedUpd.Change
	case MultiArrayIncFloat:
		upperCap, fromInt := to, int(from)
		if len(typedUpd) > len(floatC) && int32(len(floatC)) < to {
			if len(typedUpd) > int(to) {
				floatC = copySliceWithSize[float64](floatC, to)
			} else {
				floatC = copySliceWithSize[float64](floatC, int32(len(typedUpd)))
				upperCap = int32(len(typedUpd))
			}
		}
		if from >= upperCap { //The query starts in a position after the end of the array
			return floatC
		}
		for i, change := range typedUpd[from:upperCap] {
			floatC[i+fromInt] += change
		}
	case MultiArrayIncFloatPositions:
		if len(typedUpd.Pos) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(floatC)) {
						floatC = copySliceWithSize[float64](floatC, pos+1)
					}
					floatC[pos] += change
				}
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(floatC)) {
						floatC = copySliceWithSize[float64](floatC, pos+1)
					}
					floatC[pos] += typedUpd.Changes[i]
				}
			}
		}
	case MultiArrayIncFloatRange:
		if typedUpd.To < from || typedUpd.From > to { //Update is out of the query range, so no change needed.
			return floatC
		}
		if typedUpd.To >= int32(len(floatC)) {
			floatC = copySliceWithSize[float64](floatC, typedUpd.To+1)
		}
		minStart, minFinish := max(from, typedUpd.From), min(to, typedUpd.To)
		offset := max(0, from-typedUpd.From)
		for i := minStart; i <= minFinish; i++ {
			floatC[i+offset] += typedUpd.Change
		}
	}
	return floatC
}

func (crdt *MultiArrayCrdt) applyDataUpdToRangeSlice(upd MultiArrayUpd, dataC [][]byte, from, to int32) [][]byte {
	switch typedUpd := upd.(type) {
	case MultiArraySetRegisterSingle:
		if typedUpd.Pos >= to || typedUpd.Pos < from {
			return dataC
		}
		if typedUpd.Pos > int32(len(dataC)) {
			dataC = copySliceWithSize[[]byte](dataC, typedUpd.Pos+1)
		}
		dataC[typedUpd.Pos] = typedUpd.Value
	case MultiArraySetRegister:
		upperCap, fromInt := to, int(from)
		if len(typedUpd) > len(dataC) && int32(len(dataC)) < to {
			if len(typedUpd) > int(to) {
				dataC = copySliceWithSize[[]byte](dataC, to)
			} else {
				dataC = copySliceWithSize[[]byte](dataC, int32(len(typedUpd)))
				upperCap = int32(len(typedUpd))
			}
		}
		if from >= upperCap { //The query starts in a position after the end of the array
			return dataC
		}
		for i, value := range typedUpd[from:upperCap] {
			dataC[i+fromInt] = value
		}
	case MultiArraySetRegisterPositions:
		if len(typedUpd.Pos) == 1 {
			value := typedUpd.Values[0]
			for _, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(dataC)) {
						dataC = copySliceWithSize[[]byte](dataC, pos+1)
					}
					dataC[pos] = value
				}
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(dataC)) {
						dataC = copySliceWithSize[[]byte](dataC, pos+1)
					}
					dataC[pos] = typedUpd.Values[i]
				}
			}
		}
	case MultiArraySetRegisterRange:
		if typedUpd.To < from || typedUpd.From > to { //Update is out of the query range, so no change needed.
			return dataC
		}
		if typedUpd.To >= int32(len(dataC)) {
			dataC = copySliceWithSize[[]byte](dataC, typedUpd.To+1)
		}
		minStart, minFinish := max(from, typedUpd.From), min(to, typedUpd.To)
		offset := max(0, from-typedUpd.From)
		for i := minStart; i <= minFinish; i++ {
			dataC[i+offset] = typedUpd.Value
		}
	}
	return dataC
}

func (crdt *MultiArrayCrdt) applyAvgUpdToRangeSlice(upd MultiArrayUpd, sums, counts []int64, from, to int32) ([]int64, []int64) {
	switch typedUpd := upd.(type) {
	case MultiArrayIncAvgSingle:
		if typedUpd.Pos >= to || typedUpd.Pos < from {
			return sums, counts
		}
		if typedUpd.Pos > int32(len(sums)) {
			sums, counts = copySliceWithSize[int64](sums, typedUpd.Pos+1), copySliceWithSize[int64](counts, typedUpd.Pos+1)
		}
		sums[typedUpd.Pos] += typedUpd.Value
		counts[typedUpd.Pos] += int64(typedUpd.Count)
	case MultiArrayIncAvg:
		upperCap, fromInt := to, int(from)
		if len(typedUpd.Value) > len(sums) && int32(len(sums)) < to {
			if len(typedUpd.Value) > int(to) {
				sums = copySliceWithSize[int64](sums, to)
				counts = copySliceWithSize[int64](counts, to)
			} else {
				sums = copySliceWithSize[int64](sums, int32(len(typedUpd.Value)))
				counts = copySliceWithSize[int64](counts, int32(len(typedUpd.Count)))
				upperCap = int32(len(typedUpd.Value))
			}
		}
		if from >= upperCap { //The query starts in a position after the end of the array
			return sums, counts
		}
		for i, value := range typedUpd.Value[from:upperCap] {
			sums[i+fromInt] += value
			counts[i+fromInt] += int64(typedUpd.Count[i])
		}
	case MultiArrayIncAvgPositions:
		if len(typedUpd.Pos) == 1 {
			sum, count := typedUpd.Value[0], typedUpd.Count[0]
			for _, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(sums)) {
						sums = copySliceWithSize[int64](sums, pos+1)
						counts = copySliceWithSize[int64](counts, pos+1)
					}
					sums[pos] += sum
					counts[pos] += int64(count)
				}
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(sums)) {
						sums = copySliceWithSize[int64](sums, pos+1)
						counts = copySliceWithSize[int64](counts, pos+1)
					}
					sums[pos] += typedUpd.Value[i]
					counts[pos] += int64(typedUpd.Count[i])
				}
			}
		}
	case MultiArrayIncAvgRange:
		if typedUpd.To < from || typedUpd.From > to { //Update is out of the query range, so no change needed.
			return sums, counts
		}
		if typedUpd.To >= int32(len(sums)) {
			sums, counts = copySliceWithSize[int64](sums, typedUpd.To+1), copySliceWithSize[int64](counts, typedUpd.To+1)
		}
		minStart, minFinish := max(from, typedUpd.From), min(to, typedUpd.To)
		offset := max(0, from-typedUpd.From)
		for i := minStart; i <= minFinish; i++ {
			sums[i+offset] += typedUpd.Value
			counts[i+offset] += int64(typedUpd.Count)
		}
	}
	return sums, counts
}

func (crdt *MultiArrayCrdt) getSubStateOfMultipleTypes(updsNotYetApplied []UpdateArguments, positions [][]int32, arrayTypes []MULTI_ARRAY_TYPE) (state State) {
	returnState := MultiArrayState{}

	if len(updsNotYetApplied) == 0 {
		for i, arrayType := range arrayTypes {
			switch arrayType {
			case MultiInts:
				returnState.IntCounters = getSubSlice[int64](crdt.intCounters, positions[i])
			case MultiFloats:
				returnState.FloatCounters = getSubSlice[float64](crdt.floatCounters, positions[i])
			case MultiData:
				returnState.DataCounters = getSubSlice[[]byte](crdt.dataCounters, positions[i])
			case MultiAvg:
				returnState.Sums, returnState.Counts = getSubAvgSlice(crdt.sums, crdt.counts, positions[i])
			}
		}
		return returnState
	}

	//For efficiency, it copies the entire slices and updates all positions, filtering at the end.
	returnState = crdt.multiCopySlices(arrayTypes)
	//Applies only updates regarding the relevant arrays. Updates all positions for efficiency.
	returnState = crdt.multiApplyPendingUpdatesFull(returnState, arrayTypes, updsNotYetApplied)

	for i, arrayType := range arrayTypes {
		switch arrayType {
		case MultiInts:
			returnState.IntCounters = getSubSlice[int64](returnState.IntCounters, positions[i])
		case MultiFloats:
			returnState.FloatCounters = getSubSlice[float64](returnState.FloatCounters, positions[i])
		case MultiData:
			returnState.DataCounters = getSubSlice[[]byte](returnState.DataCounters, positions[i])
		case MultiAvg:
			returnState.Sums, returnState.Counts = getSubAvgSlice(returnState.Sums, returnState.Counts, positions[i])
		}
	}

	return returnState
}

func (crdt *MultiArrayCrdt) getSubStateOfType(updsNotYetApplied []UpdateArguments, positions []int32, arrayType MULTI_ARRAY_TYPE) (state State) {
	if len(updsNotYetApplied) == 0 {
		return crdt.makeSubStateReply(arrayType, positions, crdt.intCounters, crdt.sums, crdt.counts, crdt.floatCounters, crdt.dataCounters)
	}

	slices := crdt.singleCopySlicesToHolder(arrayType)
	slices = crdt.applyPendingUpdatesFull(slices, arrayType, updsNotYetApplied)

	return crdt.makeSubStateReply(arrayType, positions, slices.IntCounters, slices.Sums, slices.Counts, slices.FloatCounters, slices.DataCounters)
}

func (crdt *MultiArrayCrdt) makeSubStateReply(arrayType MULTI_ARRAY_TYPE, positions []int32, intC, sums, counts []int64, floatC []float64, dataC [][]byte) (state State) {
	switch arrayType {
	case MultiInts:
		return IntArrayState(getSubSlice[int64](intC, positions))
	case MultiFloats:
		return FloatArrayState(getSubSlice[float64](floatC, positions))
	case MultiData:
		return DataArrayState(getSubSlice[[]byte](dataC, positions))
	case MultiAvg:
		sums, counts := getSubAvgSlice(crdt.sums, crdt.counts, positions)
		return AvgArrayState{Sums: sums, Counts: counts}
	}
	return nil
}

func getSubSlice[V any](slice []V, positions []int32) (result []V) {
	result = make([]V, len(positions))
	for i, pos := range positions {
		if pos < int32(len(slice)) {
			result[i] = slice[pos]
		}
	}
	return
}

func getSubAvgSlice(sums, counts []int64, positions []int32) (resultSums, resultCounts []int64) {
	resultSums, resultCounts = make([]int64, len(sums)), make([]int64, len(counts))
	for i, pos := range positions {
		if pos < int32(len(sums)) {
			resultSums[i], resultCounts[i] = sums[pos], counts[pos]
		}
	}
	return
}

// Returns the default value if the position is out of bounds.
func getPositionOfSlice[V any](pos int, slice []V) (value V) {
	if pos < len(slice) {
		return slice[pos]
	}
	return value
}

func getPositionOfAvgSlice(pos int, sums, counts []int64) (sum, count int64) {
	if pos < len(sums) {
		sum, count = sums[pos], counts[pos]
	}
	return
}

func getRangeOfSlice[V any](slice []V, from, to int) (result []V) {
	if from > len(slice) {
		return make([]V, 0)
	}
	if to > len(slice) {
		to = len(slice)
	}
	return slice[from:to]
}

func getRangeOfAvgSlice(sums, counts []int64, from, to int) (resultSums, resultCounts []int64) {
	if from > len(sums) {
		return make([]int64, 0), make([]int64, 0)
	}
	if to > len(sums) {
		to = len(sums)
	}
	return sums[from:to], counts[from:to]
}

func copySliceWithNilCheck[V any](slice []V) (copySlice []V) {
	if slice == nil {
		return nil
	}
	copySlice = make([]V, len(slice))
	copy(copySlice, slice)
	return
}

func copySlice[V any](slice []V) (copySlice []V) {
	copySlice = make([]V, len(slice))
	copy(copySlice, slice)
	return
}

// Pre: size > len(slice)
func copySliceWithSize[V any](slice []V, size int32) (copySlice []V) {
	copySlice = make([]V, size)
	copy(copySlice, slice)
	return
}

func (crdt *MultiArrayCrdt) expandIntArray(newSize int32) {
	newInts := make([]int64, newSize)
	copy(newInts, crdt.intCounters)
	crdt.intCounters = newInts
}

func (crdt *MultiArrayCrdt) expandFloatArray(newSize int32) {
	newFloats := make([]float64, newSize)
	copy(newFloats, crdt.floatCounters)
	crdt.floatCounters = newFloats
}

func (crdt *MultiArrayCrdt) expandDataArray(newSize int32) {
	newData, newDataTsId := make([][]byte, newSize), make([]uint64, newSize)
	copy(newData, crdt.dataCounters)
	copy(newDataTsId, crdt.dataTsId)
	crdt.dataCounters, crdt.dataTsId = newData, newDataTsId
}

func (crdt *MultiArrayCrdt) expandAvgArray(newSize int32) {
	newSums, newCounts := make([]int64, newSize), make([]int64, newSize)
	copy(newSums, crdt.sums)
	copy(newCounts, crdt.counts)
	crdt.sums, crdt.counts = newSums, newCounts
}

func (crdt *MultiArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	typedArg, ok := args.(MultiArrayUpd)
	if !ok {
		fmt.Printf("[MultiArrayCrdt]Unknown update type: %+v\n", args)
		return
	}
	if typedArg.GetDataType() == MultiData {
		tsId := generate64BitTsAndId(crdt.replicaID)
		switch regArg := typedArg.(type) {
		case MultiArraySetRegisterSingle:
			return DownstreamMultiArraySetRegisterSingle{Pos: regArg.Pos, Value: regArg.Value, TsId: tsId}
		case MultiArraySetRegister:
			return DownstreamMultiArraySetRegister{Values: regArg, TsId: tsId}
		case MultiArraySetRegisterPositions:
			return DownstreamMultiArraySetRegisterPositions{Pos: regArg.Pos, Values: regArg.Values, TsId: tsId}
		case MultiArraySetRegisterRange:
			return DownstreamMultiArraySetRegisterRange{From: regArg.From, To: regArg.To, Value: regArg.Value, TsId: tsId}
		}
	}
	if typedArg.GetDataType() == Multi {
		multiArg := typedArg.(MultiArrayUpdateAll)
		if len(multiArg.Data) > 0 { //Need to include ts
			tsId := generate64BitTsAndId(crdt.replicaID)
			return DownstreamMultiArrayUpdateAll{MultiArrayUpdateAll: multiArg, TsId: tsId}
		}
		return DownstreamMultiArrayUpdateAll{MultiArrayUpdateAll: multiArg}
	}
	if typedArg.GetDataType() == MultiSize {
		sizeArg := typedArg.(MultiArraySetSizes)
		if sizeArg.IntSize < int32(len(crdt.intCounters)) && sizeArg.FloatSize < int32(len(crdt.floatCounters)) && sizeArg.RegisterSize < int32(len(crdt.dataCounters)) {
			return NoOp{} //All sizes are smaller than the current sizes, no need to update.
		}
	}
	return args.(DownstreamArguments)
}

func generate64BitTsAndId(localReplicaID int64) uint64 {
	//The first 48 bits are the timestamp, the last 16 bits are the replica ID. We for the replicaID into a positive range first.
	//We mask the timestamp to extract the lowest 48 bits, then shift 16 bits left. Finally, we "add" (OR) the replicaID's lowest 16 bits.
	return uint64(((time.Now().UnixNano() & 0x0000FFFFFFFFFFFF) << 16) | ((localReplicaID + 32768) & 0x000000000000FFFF))
}

func (crdt *MultiArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *MultiArrayCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	typedUpd, ok := downstreamArgs.(MultiArrayUpd)
	if !ok {
		fmt.Printf("[MultiArrayCrdt]Unknown downstream type: %+v\n", downstreamArgs)
		return nil
	}
	switch typedUpd.GetDataType() {
	case MultiInts:
		return crdt.applyDownstreamInt(typedUpd)
	case MultiFloats:
		return crdt.applyDownstreamFloat(typedUpd)
	case MultiData:
		return crdt.applyDownstreamData(typedUpd)
	case MultiAvg:
		return crdt.applyDownstreamAvg(typedUpd)
	case Multi:
		return crdt.applyDownstreamMulti(typedUpd.(DownstreamMultiArrayUpdateAll))
	default: //Size
		return crdt.applyDownstreamSize(typedUpd.(MultiArraySetSizes))
	}
}

func (crdt *MultiArrayCrdt) applyDownstreamInt(downstreamArgs MultiArrayUpd) (effect *Effect) {
	var effectValue Effect
	switch intUpd := downstreamArgs.(type) {
	case MultiArrayIncIntSingle:
		if int(intUpd.Pos) >= len(crdt.intCounters) {
			effectValue = MultiArraySetSizesEffect{IntSize: int32(len(crdt.intCounters))} //The value of this position before was "0" as it did not belong to the array
			crdt.expandIntArray(intUpd.Pos + 1)
		} else {
			effectValue = MultiArrayIncIntSingleEffect(intUpd)
		}
		crdt.intCounters[intUpd.Pos] += intUpd.Change
	case MultiArrayIncInt:
		if len(intUpd) > len(crdt.intCounters) {
			effectValue = CounterArrayIncMultiWithSizeEffect{IncEff: CounterArrayIncrementMultiEffect(intUpd), OldSize: len(crdt.intCounters)}
			crdt.expandIntArray(int32(len(intUpd)))
		} else {
			effectValue = MultiArrayIncIntEffect(intUpd)
		}
		for i, change := range intUpd {
			crdt.intCounters[i] += change
		}
	case MultiArrayIncIntPositions:
		oldSize := len(crdt.intCounters)
		if len(intUpd.Changes) == 1 {
			change := intUpd.Changes[0]
			for _, pos := range intUpd.Pos {
				if int(pos) >= len(crdt.intCounters) {
					crdt.expandIntArray(pos + 1)
				}
				crdt.intCounters[pos] += change
			}
		} else {
			for i, pos := range intUpd.Pos {
				if int(pos) >= len(crdt.intCounters) {
					crdt.expandIntArray(pos + 1)
				}
				crdt.intCounters[pos] += intUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.intCounters) {
			effectValue = MultiArrayIncIntPositionsWithSizeEffect{IncEff: MultiArrayIncIntPositionsEffect(intUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncIntPositionsEffect(intUpd)
		}
	case MultiArrayIncIntRange:
		oldSize := len(crdt.intCounters)
		if intUpd.To >= int32(len(crdt.intCounters)) { //if intUpd.From is >= len(), so it .To
			crdt.expandIntArray(intUpd.To + 1)
		}
		for i := intUpd.From; i <= intUpd.To; i++ {
			crdt.intCounters[i] += intUpd.Change
		}
		if oldSize != len(crdt.intCounters) {
			effectValue = MultiArrayIncIntRangeWithSizeEffect{IncEff: MultiArrayIncIntRangeEffect(intUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncIntRangeEffect(intUpd)
		}
	}
	return &effectValue
}

func (crdt *MultiArrayCrdt) applyDownstreamFloat(downstreamArgs MultiArrayUpd) (effect *Effect) {
	var effectValue Effect
	switch floatUpd := downstreamArgs.(type) {
	case MultiArrayIncFloatSingle:
		if int(floatUpd.Pos) >= len(crdt.floatCounters) {
			effectValue = MultiArraySetSizesEffect{FloatSize: int32(len(crdt.floatCounters))} //The value of this position before was "0" as it did not belong to the array
			crdt.expandFloatArray(floatUpd.Pos + 1)
		} else {
			effectValue = MultiArrayIncFloatSingleEffect(floatUpd)
		}
		crdt.floatCounters[floatUpd.Pos] += floatUpd.Change
	case MultiArrayIncFloat:
		if len(floatUpd) > len(crdt.floatCounters) {
			effectValue = MultiArrayIncFloatWithSizeEffect{IncEff: MultiArrayIncFloatEffect(floatUpd), OldSize: len(crdt.floatCounters)}
			crdt.expandFloatArray(int32(len(floatUpd)))
		} else {
			effectValue = MultiArrayIncFloatEffect(floatUpd)
		}
		for i, change := range floatUpd {
			crdt.floatCounters[i] += change
		}
	case MultiArrayIncFloatPositions:
		//fmt.Printf("[MultiArrayCrdt]Applying MultiArrayIncFloatPositions: %+v. Old values: %v\n", floatUpd, crdt.floatCounters)
		oldSize := len(crdt.floatCounters)
		if len(floatUpd.Changes) == 1 {
			change := floatUpd.Changes[0]
			for _, pos := range floatUpd.Pos {
				if int(pos) >= len(crdt.floatCounters) {
					crdt.expandFloatArray(pos + 1)
				}
				crdt.floatCounters[pos] += change
			}
		} else {
			for i, pos := range floatUpd.Pos {
				if int(pos) >= len(crdt.floatCounters) {
					crdt.expandFloatArray(pos + 1)
				}
				crdt.floatCounters[pos] += floatUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.floatCounters) {
			effectValue = MultiArrayIncFloatPositionsWithSizeEffect{IncEff: MultiArrayIncFloatPositionsEffect(floatUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncFloatPositionsEffect(floatUpd)
		}
	case MultiArrayIncFloatRange:
		oldSize := len(crdt.floatCounters)
		if floatUpd.To >= int32(len(crdt.intCounters)) { //if intUpd.From is >= len(), so it .To
			crdt.expandIntArray(floatUpd.To + 1)
		}
		for i := floatUpd.From; i <= floatUpd.To; i++ {
			crdt.floatCounters[i] += floatUpd.Change
		}
		if oldSize != len(crdt.intCounters) {
			effectValue = MultiArrayIncFloatRangeWithSizeEffect{IncEff: MultiArrayIncFloatRangeEffect(floatUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncFloatRangeEffect(floatUpd)
		}
	}
	return &effectValue
}

func (crdt *MultiArrayCrdt) applyDownstreamData(downstreamArgs MultiArrayUpd) (effect *Effect) {
	var effectValue Effect = NoEffect{}

	switch dataUpd := downstreamArgs.(type) {
	case DownstreamMultiArraySetRegisterSingle:
		if int(dataUpd.Pos) >= len(crdt.dataCounters) {
			effectValue = MultiArraySetSizesEffect{RegisterSize: int32(len(crdt.dataCounters))} //The value of this position before was "0" as it did not belong to the array
			crdt.expandDataArray(dataUpd.Pos + 1)
		} else if crdt.dataTsId[dataUpd.Pos] < dataUpd.TsId { //Can compare directly: if ts is equal, then we pick the higher replicaID. Perfect!
			effectValue = MultiArraySetRegisterSingleEffect{Pos: dataUpd.Pos, Value: crdt.dataCounters[dataUpd.Pos], TsId: crdt.dataTsId[dataUpd.Pos]}
		} else {
			return &effectValue
		}
		crdt.dataCounters[dataUpd.Pos], crdt.dataTsId[dataUpd.Pos] = dataUpd.Value, dataUpd.TsId
	case DownstreamMultiArraySetRegister:
		if len(dataUpd.Values) > len(crdt.dataCounters) {
			//Safe to use the dataCounters for the effect as we will replace it.
			effectValue = MultiArraySetRegisterWithSizeEffect{IncEff: MultiArraySetRegisterEffect{Values: crdt.dataCounters, TsIds: crdt.dataTsId}, OldSize: len(crdt.dataCounters)}
			crdt.expandDataArray(int32(len(dataUpd.Values)))
		} else {
			effectValue = MultiArraySetRegisterEffect{Values: copySliceWithNilCheck(crdt.dataCounters), TsIds: copySliceWithNilCheck(crdt.dataTsId)}
		}
		atLeastOne := false
		for i, value := range dataUpd.Values {
			if crdt.dataTsId[i] < dataUpd.TsId {
				crdt.dataCounters[i], crdt.dataTsId[i], atLeastOne = value, dataUpd.TsId, true
			}
		}
		if !atLeastOne {
			effectValue = NoEffect{}
			return &effectValue
		}
	case DownstreamMultiArraySetRegisterPositions:
		oldSize, oldData, oldTs, atLeastOne := len(crdt.dataCounters), make([][]byte, len(dataUpd.Pos)), make([]uint64, len(dataUpd.Pos)), false
		if len(dataUpd.Values) == 1 {
			value := dataUpd.Values[0]
			for i, pos := range dataUpd.Pos {
				if int(pos) >= len(crdt.dataCounters) {
					crdt.expandDataArray(pos + 1)
				} else if crdt.dataTsId[pos] < dataUpd.TsId {
					oldData[i], oldTs[i] = crdt.dataCounters[pos], crdt.dataTsId[pos]
				} else {
					continue //Skip this position
				}
				crdt.dataCounters[pos], crdt.dataTsId[pos], atLeastOne = value, dataUpd.TsId, true
			}
		} else {
			for i, pos := range dataUpd.Pos {
				if int(pos) >= len(crdt.dataCounters) {
					crdt.expandDataArray(pos + 1)
				} else if crdt.dataTsId[pos] < dataUpd.TsId {
					oldData[i], oldTs[i] = crdt.dataCounters[pos], crdt.dataTsId[pos]
				} else {
					continue //Skip this position
				}
				crdt.dataCounters[pos], crdt.dataTsId[pos], atLeastOne = dataUpd.Values[i], dataUpd.TsId, true
			}
		}
		if !atLeastOne {
			return &effectValue
		}
		if oldSize != len(crdt.dataCounters) {
			effectValue = MultiArraySetRegisterPositionsWithSizeEffect{
				IncEff: MultiArraySetRegisterPositionsEffect{Pos: dataUpd.Pos, Values: oldData, TsIds: oldTs}, OldSize: oldSize}
		} else {
			effectValue = MultiArraySetRegisterPositionsEffect{Pos: dataUpd.Pos, Values: oldData, TsIds: oldTs}
		}
	case DownstreamMultiArraySetRegisterRange:
		oldSize := len(crdt.dataCounters)
		var oldData [][]byte
		var oldTs []uint64
		if dataUpd.To >= int32(len(crdt.intCounters)) { //if intUpd.From is >= len(), so it .To
			oldData, oldTs = crdt.dataCounters, crdt.dataTsId
			crdt.expandDataArray(dataUpd.To + 1)
		} else {
			oldData, oldTs = make([][]byte, dataUpd.To-dataUpd.From+1), make([]uint64, dataUpd.To-dataUpd.From+1)
			copy(oldData, crdt.dataCounters[dataUpd.From:dataUpd.To+1])
			copy(oldTs, crdt.dataTsId[dataUpd.From:dataUpd.To+1])
		}
		for i := dataUpd.From; i <= dataUpd.To; i++ {
			if dataUpd.TsId > crdt.dataTsId[i] {
				crdt.dataCounters[i] = dataUpd.Value
				crdt.dataTsId[i] = dataUpd.TsId
			}
		}
		if oldSize != len(crdt.dataCounters) {
			effectValue = MultiArraySetRegisterRangeWithSizeEffect{
				IncEff: MultiArraySetRegisterRangeEffect{From: dataUpd.From, To: dataUpd.To, Values: oldData, TsIds: oldTs}, OldSize: oldSize}
		} else {
			effectValue = MultiArraySetRegisterRangeEffect{From: dataUpd.From, To: dataUpd.To, Values: oldData, TsIds: oldTs}
		}
	}
	return &effectValue
}

func (crdt *MultiArrayCrdt) applyDownstreamAvg(downstreamArgs MultiArrayUpd) (effect *Effect) {
	var effectValue Effect
	switch avgUpd := downstreamArgs.(type) {
	case MultiArrayIncAvgSingle:
		if int(avgUpd.Pos) >= len(crdt.sums) {
			effectValue = MultiArraySetSizesEffect{AvgSize: int32(len(crdt.sums))} //The value of this position before was "0" as it did not belong to the array
			crdt.expandAvgArray(avgUpd.Pos + 1)
		} else {
			effectValue = MultiArrayIncAvgSingleEffect(avgUpd)
		}
		crdt.sums[avgUpd.Pos] += avgUpd.Value
		crdt.counts[avgUpd.Pos] += int64(avgUpd.Count)
	case MultiArrayIncAvg:
		if len(avgUpd.Value) > len(crdt.sums) {
			effectValue = MultiArrayIncAvgWithSizeEffect{IncEff: MultiArrayIncAvgEffect(avgUpd), OldSize: len(crdt.sums)}
			crdt.expandAvgArray(int32(len(avgUpd.Value)))
		} else {
			effectValue = MultiArrayIncAvgEffect(avgUpd)
		}
		for i, value := range avgUpd.Value {
			crdt.sums[i] += value
			crdt.counts[i] += int64(avgUpd.Count[i])
		}
		//fmt.Printf("[MultiArrayCrdt][Downstream]Sums: %+v, Counts: %+v\n", crdt.sums, crdt.counts)
	case MultiArrayIncAvgPositions:
		oldSize := len(crdt.sums)
		if len(avgUpd.Value) == 1 {
			value, count := avgUpd.Value[0], avgUpd.Count[0]
			for _, pos := range avgUpd.Pos {
				if int(pos) >= len(crdt.counts) {
					crdt.expandAvgArray(pos + 1)
				}
				crdt.sums[pos] += value
				crdt.counts[pos] += int64(count)
			}
		} else {
			for i, pos := range avgUpd.Pos {
				if int(pos) >= len(crdt.counts) {
					crdt.expandAvgArray(pos + 1)
				}
				crdt.sums[pos] += avgUpd.Value[i]
				crdt.counts[pos] += int64(avgUpd.Count[i])
			}
		}
		if oldSize != len(crdt.sums) {
			effectValue = MultiArrayIncAvgPositionsWithSizeEffect{IncEff: MultiArrayIncAvgPositionsEffect(avgUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncAvgPositionsEffect(avgUpd)
		}
	case MultiArrayIncAvgRange:
		oldSize := len(crdt.sums)
		if avgUpd.To >= int32(len(crdt.intCounters)) { //if intUpd.From is >= len(), so it .To
			crdt.expandAvgArray(avgUpd.To + 1)
			crdt.expandAvgArray(avgUpd.To + 1)
		}
		for i := avgUpd.From; i <= avgUpd.To; i++ {
			crdt.sums[i] += avgUpd.Value
			crdt.counts[i] += int64(avgUpd.Count)
		}
		if oldSize != len(crdt.intCounters) {
			effectValue = MultiArrayIncAvgRangeWithSizeEffect{IncEff: MultiArrayIncAvgRangeEffect(avgUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncAvgRangeEffect(avgUpd)
		}
	}
	return &effectValue
}

func (crdt *MultiArrayCrdt) applyDownstreamMulti(upd DownstreamMultiArrayUpdateAll) (effect *Effect) {
	var effectValue Effect
	oldIntSize, oldFloatSize, oldDataSize, oldAvgSize := -1, -1, -1, -1
	oldDataSlice, oldTsId := crdt.dataCounters, crdt.dataTsId
	if len(upd.Ints) > 0 { //Ints
		oldIntSize = len(crdt.intCounters)
		if len(upd.Ints) > len(crdt.intCounters) {
			crdt.expandIntArray(int32(len(upd.Ints)))
		}
		for i, change := range upd.Ints {
			crdt.intCounters[i] += change
		}
	}
	if len(upd.Floats) > 0 { //Floats
		oldFloatSize = len(crdt.floatCounters)
		if len(upd.Floats) > len(crdt.floatCounters) {
			crdt.expandFloatArray(int32(len(upd.Floats)))
		}
		for i, change := range upd.Floats {
			crdt.floatCounters[i] += change
		}
	}
	if len(upd.Data) > 0 { //Registers
		oldDataSize = len(crdt.dataCounters)
		if len(upd.Data) > len(crdt.dataCounters) {
			crdt.expandDataArray(int32(len(upd.Data)))
		} else {
			oldDataSlice, oldTsId = copySliceWithNilCheck(crdt.dataCounters), copySliceWithNilCheck(crdt.dataTsId)
		}
		atLeastOne := false
		for i, value := range upd.Data {
			if crdt.dataTsId[i] < upd.TsId {
				crdt.dataCounters[i], crdt.dataTsId[i], atLeastOne = value, upd.TsId, true
			}
		}
		if !atLeastOne {
			oldDataSize = -1
		}
	}
	if len(upd.Sums) > 0 { //Avgs
		oldAvgSize = len(crdt.sums)
		if len(upd.Sums) > len(crdt.sums) {
			crdt.expandAvgArray(int32(len(upd.Sums)))
		}
		for i, value := range upd.Sums {
			crdt.sums[i] += value
			crdt.counts[i] += int64(upd.Counts[i])
		}
	}

	if oldIntSize == -1 && oldFloatSize == -1 && oldDataSize == -1 && oldAvgSize == -1 {
		effectValue = NoEffect{} //Happens when there's only update for data but it does not happen due to clock.
		return &effectValue
	}
	if oldIntSize > -1 && oldFloatSize == -1 && oldDataSize == -1 && oldAvgSize == -1 {
		if oldIntSize < len(crdt.intCounters) {
			effectValue = CounterArrayIncMultiWithSizeEffect{IncEff: CounterArrayIncrementMultiEffect(upd.Ints), OldSize: oldIntSize}
		} else {
			effectValue = CounterArrayIncrementMultiEffect(upd.Ints)
		}
	} else if oldIntSize == -1 && oldFloatSize > -1 && oldDataSize == -1 && oldAvgSize == -1 {
		if oldFloatSize < len(crdt.floatCounters) {
			effectValue = MultiArrayIncFloatWithSizeEffect{IncEff: MultiArrayIncFloatEffect(upd.Floats), OldSize: oldFloatSize}
		} else {
			effectValue = MultiArrayIncFloatEffect(upd.Floats)
		}
	} else if oldIntSize == -1 && oldFloatSize == -1 && oldDataSize > -1 && oldAvgSize == -1 {
		if oldDataSize < len(crdt.dataCounters) {
			effectValue = MultiArraySetRegisterWithSizeEffect{IncEff: MultiArraySetRegisterEffect{Values: oldDataSlice, TsIds: oldTsId}, OldSize: oldDataSize}
		} else {
			effectValue = MultiArraySetRegisterEffect{Values: oldDataSlice, TsIds: oldTsId}
		}
	} else if oldIntSize == -1 && oldFloatSize == -1 && oldDataSize == -1 && oldAvgSize > -1 {
		if oldAvgSize < len(crdt.sums) {
			effectValue = MultiArrayIncAvgWithSizeEffect{IncEff: MultiArrayIncAvgEffect{Value: upd.Sums, Count: upd.Counts}, OldSize: oldAvgSize}
		} else {
			effectValue = MultiArrayIncAvgEffect{Value: upd.Sums, Count: upd.Counts}
		}
	} else {
		if oldIntSize == len(crdt.intCounters) && oldFloatSize == len(crdt.floatCounters) && oldDataSize == len(crdt.dataCounters) && oldAvgSize == len(crdt.sums) {
			effectValue = MultiArrayUpdateAllEffect{MultiArrayUpdateAll: MultiArrayUpdateAll{Ints: upd.Ints, Floats: upd.Floats, Data: upd.Data, Sums: upd.Sums, Counts: upd.Counts}, TsId: upd.TsId}
		} else {
			effectValue = MultiArrayUpdateAllWithSizeEffect{DownstreamMultiArrayUpdateAll: DownstreamMultiArrayUpdateAll{MultiArrayUpdateAll: MultiArrayUpdateAll{Ints: upd.Ints, Floats: upd.Floats, Data: upd.Data, Sums: upd.Sums, Counts: upd.Counts}, TsId: upd.TsId},
				MultiArraySetSizesEffect: MultiArraySetSizesEffect{IntSize: int32(oldIntSize), FloatSize: int32(oldFloatSize), RegisterSize: int32(oldDataSize), AvgSize: int32(oldAvgSize)}}
		}
	}
	return &effectValue
}

func (crdt *MultiArrayCrdt) applyDownstreamSize(sizeArg MultiArraySetSizes) (effect *Effect) {
	var effectValue Effect
	sizeEffect := MultiArraySetSizesEffect{IntSize: -1, FloatSize: -1, RegisterSize: -1, AvgSize: -1}
	atLeastOne := false
	if sizeArg.IntSize > int32(len(crdt.intCounters)) {
		sizeEffect.IntSize, atLeastOne = int32(len(crdt.intCounters)), true
		crdt.expandIntArray(sizeArg.IntSize)
	}
	if sizeArg.FloatSize > int32(len(crdt.floatCounters)) {
		sizeEffect.FloatSize, atLeastOne = int32(len(crdt.floatCounters)), true
		crdt.expandFloatArray(sizeArg.FloatSize)
	}
	if sizeArg.RegisterSize > int32(len(crdt.dataCounters)) {
		sizeEffect.RegisterSize, atLeastOne = int32(len(crdt.floatCounters)), true
		crdt.expandDataArray(sizeArg.RegisterSize)
	}
	if sizeArg.AvgSize > int32(len(crdt.sums)) {
		sizeEffect.AvgSize, atLeastOne = int32(len(crdt.counts)), true
		crdt.expandAvgArray(sizeArg.AvgSize)
	}
	if !atLeastOne { //Can still happen (e.g., two concurrent set sizes)
		effectValue = NoEffect{}
	} else {
		effectValue = sizeEffect
	}
	return &effectValue
}

func (crdt *MultiArrayCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *MultiArrayCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := MultiArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), intCounters: copySliceWithNilCheck(crdt.intCounters), floatCounters: copySliceWithNilCheck(crdt.floatCounters),
		dataCounters: copySliceWithNilCheck(crdt.dataCounters), sums: copySliceWithNilCheck(crdt.sums), counts: copySliceWithNilCheck(crdt.counts)}
	return &newCRDT
}

func (crdt *MultiArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *MultiArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *MultiArrayCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case CounterArrayIncrementEffect:
		crdt.intCounters[typedEffect.Position] -= typedEffect.Change
	case CounterArrayIncrementMultiEffect:
		for i, change := range typedEffect {
			crdt.intCounters[i] -= change
		}
	case CounterArrayIncrementSubEffect:
		if len(typedEffect.Changes) == 1 {
			change := typedEffect.Changes[0]
			for _, pos := range typedEffect.Positions {
				crdt.intCounters[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.Positions {
				crdt.intCounters[pos] -= typedEffect.Changes[i]
			}
		}
	case MultiArrayIncFloatSingleEffect:
		crdt.floatCounters[typedEffect.Pos] -= typedEffect.Change
	case MultiArrayIncFloatEffect:
		for i, change := range typedEffect {
			crdt.floatCounters[i] -= change
		}
	case MultiArrayIncFloatPositionsEffect:
		if len(typedEffect.Changes) == 1 {
			change := typedEffect.Changes[0]
			for _, pos := range typedEffect.Pos {
				crdt.floatCounters[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.Pos {
				crdt.floatCounters[pos] -= typedEffect.Changes[i]
			}
		}
	case MultiArraySetRegisterSingleEffect:
		crdt.dataCounters[typedEffect.Pos], crdt.dataTsId[typedEffect.Pos] = typedEffect.Value, typedEffect.TsId
	case MultiArraySetRegisterEffect:
		for i, value := range typedEffect.Values {
			if crdt.dataTsId[i] > typedEffect.TsIds[i] { //This means the value changed with this update.
				crdt.dataCounters[i], crdt.dataTsId[i] = value, typedEffect.TsIds[i]
			}
		}
	case MultiArraySetRegisterPositionsEffect: //Will always have multiple values (remember: this is a copy of the old state.)
		var pos int32
		var value []byte
		for i, oldTs := range typedEffect.TsIds {
			pos, value = typedEffect.Pos[i], typedEffect.Values[i]
			if oldTs < crdt.dataTsId[pos] && value != nil {
				//If nil, it means that the value was not changed by this update (and oldTs is 0)
				crdt.dataCounters[pos], crdt.dataTsId[pos] = value, oldTs
			}
		}
	case MultiArrayIncAvgSingleEffect:
		crdt.sums[typedEffect.Pos] -= typedEffect.Value
		crdt.counts[typedEffect.Pos] -= int64(typedEffect.Count)
	case MultiArrayIncAvgEffect:
		for i, change := range typedEffect.Value {
			crdt.sums[i] -= change
			crdt.counts[i] -= int64(typedEffect.Count[i])
		}
	case MultiArrayIncAvgPositionsEffect:
		if len(typedEffect.Value) == 1 {
			sum, count := typedEffect.Value[0], typedEffect.Count[0]
			for _, pos := range typedEffect.Pos {
				crdt.sums[pos] -= sum
				crdt.counts[pos] -= int64(count)
			}
		} else {
			for i, pos := range typedEffect.Pos {
				crdt.sums[pos] -= typedEffect.Value[i]
				crdt.counts[pos] -= int64(typedEffect.Count[i])
			}
		}
	case MultiArrayUpdateAllEffect:
		if len(crdt.intCounters) > 0 {
			for i, change := range typedEffect.Ints {
				crdt.intCounters[i] -= change
			}
		}
		if len(crdt.floatCounters) > 0 {
			for i, change := range typedEffect.Floats {
				crdt.floatCounters[i] -= change
			}
		}
		if len(crdt.dataCounters) > 0 {
			for i, value := range typedEffect.Data {
				if crdt.dataTsId[i] > typedEffect.TsId { //This means the value changed with this update.
					crdt.dataCounters[i], crdt.dataTsId[i] = value, typedEffect.TsId
				}
			}
		}
		if len(crdt.sums) > 0 {
			for i, change := range typedEffect.Sums {
				crdt.sums[i] -= change
				crdt.counts[i] -= int64(typedEffect.Counts[i])
			}
		}

	case CounterArrayIncMultiWithSizeEffect:
		for i, change := range typedEffect.IncEff {
			crdt.intCounters[i] -= change
		}
		crdt.intCounters = crdt.intCounters[:typedEffect.OldSize]
	case CounterArrayIncSubWithSizeEffect:
		if len(typedEffect.IncEff.Changes) == 1 {
			change := typedEffect.IncEff.Changes[0]
			for _, pos := range typedEffect.IncEff.Positions {
				crdt.intCounters[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.IncEff.Positions {
				crdt.intCounters[pos] -= typedEffect.IncEff.Changes[i]
			}
		}
		crdt.intCounters = crdt.intCounters[:typedEffect.OldSize]
	case MultiArrayIncFloatWithSizeEffect:
		for i, change := range typedEffect.IncEff {
			crdt.floatCounters[i] -= change
		}
		crdt.floatCounters = crdt.floatCounters[:typedEffect.OldSize]
	case MultiArrayIncFloatPositionsWithSizeEffect:
		if len(typedEffect.IncEff.Changes) == 1 {
			change := typedEffect.IncEff.Changes[0]
			for _, pos := range typedEffect.IncEff.Pos {
				crdt.floatCounters[pos] -= change
			}
		} else {
			for i, pos := range typedEffect.IncEff.Pos {
				crdt.floatCounters[pos] -= typedEffect.IncEff.Changes[i]
			}
		}
		crdt.floatCounters = crdt.floatCounters[:typedEffect.OldSize]
	case MultiArraySetRegisterWithSizeEffect:
		for i, value := range typedEffect.IncEff.Values {
			if crdt.dataTsId[i] > typedEffect.IncEff.TsIds[i] { //This means the value changed with this update.
				crdt.dataCounters[i], crdt.dataTsId[i] = value, typedEffect.IncEff.TsIds[i]
			}
		}
		crdt.dataCounters, crdt.dataTsId = crdt.dataCounters[:typedEffect.OldSize], crdt.dataTsId[:typedEffect.OldSize]
	case MultiArraySetRegisterPositionsWithSizeEffect:
		var pos int32
		var value []byte
		for i, oldTs := range typedEffect.IncEff.TsIds {
			pos, value = typedEffect.IncEff.Pos[i], typedEffect.IncEff.Values[i]
			if oldTs < crdt.dataTsId[pos] && value != nil {
				//If nil, it means that the value was not changed by this update (and oldTs is 0)
				crdt.dataCounters[pos], crdt.dataTsId[pos] = value, oldTs
			}
		}
		crdt.dataCounters = crdt.dataCounters[:typedEffect.OldSize]
	case MultiArrayIncAvgWithSizeEffect:
		for i, change := range typedEffect.IncEff.Value {
			crdt.sums[i] -= change
			crdt.counts[i] -= int64(typedEffect.IncEff.Count[i])
		}
	case MultiArrayIncAvgPositionsWithSizeEffect:
		if len(typedEffect.IncEff.Value) == 1 {
			sum, count := typedEffect.IncEff.Value[0], typedEffect.IncEff.Count[0]
			for _, pos := range typedEffect.IncEff.Pos {
				crdt.sums[pos] -= sum
				crdt.counts[pos] -= int64(count)
			}
		} else {
			for i, pos := range typedEffect.IncEff.Pos {
				crdt.sums[pos] -= typedEffect.IncEff.Value[i]
				crdt.counts[pos] -= int64(typedEffect.IncEff.Count[i])
			}
		}
		crdt.sums, crdt.counts = crdt.sums[:typedEffect.OldSize], crdt.counts[:typedEffect.OldSize]
	case MultiArrayUpdateAllWithSizeEffect:
		if len(crdt.intCounters) > 0 {
			for i, change := range typedEffect.Ints {
				crdt.intCounters[i] -= change
			}
			crdt.intCounters = crdt.intCounters[:typedEffect.IntSize]
		}
		if len(crdt.floatCounters) > 0 {
			for i, change := range typedEffect.Floats {
				crdt.floatCounters[i] -= change
			}
			crdt.floatCounters = crdt.floatCounters[:typedEffect.FloatSize]
		}
		if len(crdt.dataCounters) > 0 {
			for i, value := range typedEffect.Data {
				if crdt.dataTsId[i] > typedEffect.TsId { //This means the value changed with this update.
					crdt.dataCounters[i], crdt.dataTsId[i] = value, typedEffect.TsId
				}
			}
		}
		if len(crdt.sums) > 0 {
			for i, change := range typedEffect.Sums {
				crdt.sums[i] -= change
				crdt.counts[i] -= int64(typedEffect.Counts[i])
			}
			crdt.sums, crdt.counts = crdt.sums[:typedEffect.AvgSize], crdt.counts[:typedEffect.AvgSize]
		}
	case MultiArraySetSizesEffect:
		if typedEffect.IntSize >= 0 {
			crdt.intCounters = crdt.intCounters[:typedEffect.IntSize]
		}
		if typedEffect.FloatSize >= 0 {
			crdt.floatCounters = crdt.floatCounters[:typedEffect.FloatSize]
		}
		if typedEffect.RegisterSize >= 0 {
			crdt.dataCounters = crdt.dataCounters[:typedEffect.RegisterSize]
		}
		if typedEffect.AvgSize >= 0 {
			crdt.sums, crdt.counts = crdt.sums[:typedEffect.AvgSize], crdt.counts[:typedEffect.AvgSize]
		}
	}
}

func (crdt *MultiArrayCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp MultiArraySetSizes) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	setSizesProto := protobuf.GetMultiarrayop().GetSizeUpd()
	return MultiArraySetSizes{IntSize: setSizesProto.GetIntSize(), FloatSize: setSizesProto.GetFloatSize(),
		RegisterSize: setSizesProto.GetDataSize(), AvgSize: setSizesProto.GetAvgSize()}
}

func (crdtOp MultiArraySetSizes) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_SIZE
	setSizesProto := proto.ApbMultiArraySetSizeUpdate{}
	if crdtOp.IntSize > 0 {
		setSizesProto.IntSize = &crdtOp.IntSize
	}
	if crdtOp.FloatSize > 0 {
		setSizesProto.FloatSize = &crdtOp.FloatSize
	}
	if crdtOp.RegisterSize > 0 {
		setSizesProto.DataSize = &crdtOp.RegisterSize
	}
	if crdtOp.AvgSize > 0 {
		setSizesProto.AvgSize = &crdtOp.AvgSize
	}
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, SizeUpd: &setSizesProto}}
}

func (crdtOp MultiArrayIncIntSingle) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetIntUpd().GetIncSingle()
	return MultiArrayIncIntSingle{Pos: incProto.GetPos(), Change: incProto.GetChange()}
}

func (crdtOp MultiArrayIncIntSingle) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_INT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, IntUpd: &proto.ApbMultiArrayIntUpdate{
		IncSingle: &proto.ApbMultiArrayIntIncSingle{Pos: &crdtOp.Pos, Change: &crdtOp.Change}}}}
}

func (crdtOp MultiArrayIncInt) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return MultiArrayIncInt(protobuf.GetMultiarrayop().GetIntUpd().GetInc().GetChanges())
}

func (crdtOp MultiArrayIncInt) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_INT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, IntUpd: &proto.ApbMultiArrayIntUpdate{
		Inc: &proto.ApbMultiArrayIntInc{Changes: crdtOp}}}}
}

func (crdtOp MultiArrayIncIntPositions) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetIntUpd().GetIncPos()
	return MultiArrayIncIntPositions{Pos: incProto.GetPos(), Changes: incProto.GetChange()}
}

func (crdtOp MultiArrayIncIntPositions) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_INT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, IntUpd: &proto.ApbMultiArrayIntUpdate{
		IncPos: &proto.ApbMultiArrayIntIncPositions{Change: crdtOp.Changes, Pos: crdtOp.Pos}}}}
}

func (crdtOp MultiArrayIncIntRange) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetIntUpd().GetIncRange()
	return MultiArrayIncIntRange{From: incProto.GetFrom(), To: incProto.GetTo(), Change: incProto.GetChange()}
}

func (crdtOp MultiArrayIncIntRange) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_INT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, IntUpd: &proto.ApbMultiArrayIntUpdate{
		IncRange: &proto.ApbMultiArrayIntIncRange{From: &crdtOp.From, To: &crdtOp.To, Change: &crdtOp.Change}}}}
}

func (crdtOp MultiArrayIncFloatSingle) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetFloatUpd().GetIncSingle()
	return MultiArrayIncFloatSingle{Pos: incProto.GetPos(), Change: incProto.GetChange()}
}

func (crdtOp MultiArrayIncFloatSingle) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_FLOAT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, FloatUpd: &proto.ApbMultiArrayFloatUpdate{
		IncSingle: &proto.ApbMultiArrayFloatIncSingle{Pos: &crdtOp.Pos, Change: &crdtOp.Change}}}}
}

func (crdtOp MultiArrayIncFloat) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return MultiArrayIncFloat(protobuf.GetMultiarrayop().GetFloatUpd().GetInc().GetChanges())
}

func (crdtOp MultiArrayIncFloat) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_FLOAT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, FloatUpd: &proto.ApbMultiArrayFloatUpdate{
		Inc: &proto.ApbMultiArrayFloatInc{Changes: crdtOp}}}}
}

func (crdtOp MultiArrayIncFloatPositions) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetFloatUpd().GetIncPos()
	return MultiArrayIncFloatPositions{Pos: incProto.GetPos(), Changes: incProto.GetChange()}
}

func (crdtOp MultiArrayIncFloatPositions) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_FLOAT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, FloatUpd: &proto.ApbMultiArrayFloatUpdate{
		IncPos: &proto.ApbMultiArrayFloatIncPositions{Change: crdtOp.Changes, Pos: crdtOp.Pos}}}}
}

func (crdtOp MultiArrayIncFloatRange) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetFloatUpd().GetIncRange()
	return MultiArrayIncFloatRange{From: incProto.GetFrom(), To: incProto.GetTo(), Change: incProto.GetChange()}
}

func (crdtOp MultiArrayIncFloatRange) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_FLOAT
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, FloatUpd: &proto.ApbMultiArrayFloatUpdate{
		IncRange: &proto.ApbMultiArrayFloatIncRange{From: &crdtOp.From, To: &crdtOp.To, Change: &crdtOp.Change}}}}
}

func (crdtOp MultiArraySetRegisterSingle) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	setProto := protobuf.GetMultiarrayop().GetDataUpd().GetSetSingle()
	return MultiArraySetRegisterSingle{Pos: setProto.GetPos(), Value: setProto.GetData()}
}

func (crdtOp MultiArraySetRegisterSingle) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_DATA
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, DataUpd: &proto.ApbMultiArrayDataUpdate{
		SetSingle: &proto.ApbMultiArrayDataSetSingle{Pos: &crdtOp.Pos, Data: crdtOp.Value}}}}
}

func (crdtOp MultiArraySetRegister) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return MultiArraySetRegister(protobuf.GetMultiarrayop().GetDataUpd().GetSet().GetData())
}

func (crdtOp MultiArraySetRegister) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_DATA
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, DataUpd: &proto.ApbMultiArrayDataUpdate{
		Set: &proto.ApbMultiArrayDataSet{Data: crdtOp}}}}
}

func (crdtOp MultiArraySetRegisterPositions) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	setProto := protobuf.GetMultiarrayop().GetDataUpd().GetSetPos()
	return MultiArraySetRegisterPositions{Pos: setProto.GetPos(), Values: setProto.GetData()}
}

func (crdtOp MultiArraySetRegisterPositions) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_DATA
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, DataUpd: &proto.ApbMultiArrayDataUpdate{
		SetPos: &proto.ApbMultiArrayDataSetPositions{Pos: crdtOp.Pos, Data: crdtOp.Values}}}}
}

func (crdtOp MultiArraySetRegisterRange) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetIntUpd().GetIncRange()
	return MultiArrayIncIntRange{From: incProto.GetFrom(), To: incProto.GetTo(), Change: incProto.GetChange()}
}

func (crdtOp MultiArraySetRegisterRange) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_DATA
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, DataUpd: &proto.ApbMultiArrayDataUpdate{
		SetRange: &proto.ApbMultiArrayDataSetRange{From: &crdtOp.From, To: &crdtOp.To, Data: crdtOp.Value}}}}
}

func (crdtOp MultiArrayIncAvgSingle) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	avgUpd := protobuf.GetMultiarrayop().GetAvgUpd().GetIncSingle()
	return MultiArrayIncAvgSingle{Pos: avgUpd.GetPos(), Value: avgUpd.GetValue(), Count: avgUpd.GetCount()}
}

func (crdtOp MultiArrayIncAvgSingle) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, AvgUpd: &proto.ApbMultiArrayAvgUpdate{
		IncSingle: &proto.ApbMultiArrayAvgIncSingle{Pos: &crdtOp.Pos, Value: &crdtOp.Value, Count: &crdtOp.Count}}}}
}

func (crdtOp MultiArrayIncAvg) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	avgUpd := protobuf.GetMultiarrayop().GetAvgUpd().GetInc()
	return MultiArrayIncAvg{Value: avgUpd.GetValues(), Count: avgUpd.GetCounts()}
}

func (crdtOp MultiArrayIncAvg) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, AvgUpd: &proto.ApbMultiArrayAvgUpdate{
		Inc: &proto.ApbMultiArrayAvgInc{Values: crdtOp.Value, Counts: crdtOp.Count}}}}
}

func (crdtOp MultiArrayIncAvgPositions) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	avgUpd := protobuf.GetMultiarrayop().GetAvgUpd().GetIncPos()
	return MultiArrayIncAvgPositions{Pos: avgUpd.GetPos(), Value: avgUpd.GetValues(), Count: avgUpd.GetCounts()}
}

func (crdtOp MultiArrayIncAvgPositions) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, AvgUpd: &proto.ApbMultiArrayAvgUpdate{
		IncPos: &proto.ApbMultiArrayAvgIncPositions{Pos: crdtOp.Pos, Values: crdtOp.Value, Counts: crdtOp.Count}}}}
}

func (crdtOp MultiArrayIncAvgRange) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	incProto := protobuf.GetMultiarrayop().GetAvgUpd().GetIncRange()
	return MultiArrayIncAvgRange{From: incProto.GetFrom(), To: incProto.GetTo(), Value: incProto.GetValue(), Count: incProto.GetCount()}
}

func (crdtOp MultiArrayIncAvgRange) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, AvgUpd: &proto.ApbMultiArrayAvgUpdate{
		IncRange: &proto.ApbMultiArrayAvgIncRange{From: &crdtOp.From, To: &crdtOp.To, Value: &crdtOp.Value, Count: &crdtOp.Count}}}}
}

func (crdtOp MultiArrayUpdateAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	multiUpdProto := protobuf.GetMultiarrayop().GetMultiUpd()
	return MultiArrayUpdateAll{Ints: multiUpdProto.GetInts(), Floats: multiUpdProto.GetFloats(), Data: multiUpdProto.GetData(), Counts: multiUpdProto.GetCounts(), Sums: multiUpdProto.GetSums()}
}

func (crdtOp MultiArrayUpdateAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	updType := proto.MultiArrayType_MULTI
	multiUpdProto := &proto.ApbMultiArrayMultiUpdate{}
	if len(crdtOp.Ints) > 0 {
		multiUpdProto.Ints = crdtOp.Ints
	}
	if len(crdtOp.Floats) > 0 {
		multiUpdProto.Floats = crdtOp.Floats
	}
	if len(crdtOp.Data) > 0 {
		multiUpdProto.Data = crdtOp.Data
	}
	if len(crdtOp.Counts) > 0 {
		multiUpdProto.Counts, multiUpdProto.Sums = crdtOp.Counts, crdtOp.Sums
	}
	return &proto.ApbUpdateOperation{Multiarrayop: &proto.ApbMultiArrayUpdate{Type: &updType, MultiUpd: multiUpdProto}}
}

func (crdtState MultiArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	protoState := protobuf.GetMultiarray()
	return MultiArrayState{IntCounters: protoState.GetIntCounters(), FloatCounters: protoState.GetFloatCounters(),
		DataCounters: protoState.GetDataArray(), Sums: protoState.GetSums(), Counts: protoState.GetCounts()}
}

func (crdtState MultiArrayState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	protoState := &proto.ApbGetMultiArrayResp{}
	if len(crdtState.IntCounters) > 0 {
		protoState.IntCounters = crdtState.IntCounters
	}
	if len(crdtState.FloatCounters) > 0 {
		protoState.FloatCounters = crdtState.FloatCounters
	}
	if len(crdtState.DataCounters) > 0 {
		protoState.DataArray = crdtState.DataCounters
	}
	if len(crdtState.Sums) > 0 {
		protoState.Sums = crdtState.Sums
		protoState.Counts = crdtState.Counts
	}
	return &proto.ApbReadObjectResp{Multiarray: protoState}
}

func (crdtState IntArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return IntArrayState(protobuf.GetPartread().GetMultiarray().GetInts().GetIntValues())
}

func (crdtState IntArrayState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_INT
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Ints: &proto.ApbMultiArrayIntResp{IntValues: crdtState}}}}
}

func (crdtState FloatArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return FloatArrayState(protobuf.GetPartread().GetMultiarray().GetFloats().GetFloatValues())
}

func (crdtState FloatArrayState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_FLOAT
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Floats: &proto.ApbMultiArrayFloatResp{FloatValues: crdtState}}}}
}

func (crdtState AvgArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	protoState := protobuf.GetPartread().GetMultiarray().GetAvgs()
	return AvgArrayState{Sums: protoState.GetSums(), Counts: protoState.GetCounts()}
}

func (crdtState AvgArrayState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Avgs: &proto.ApbMultiArrayAvgResp{Sums: crdtState.Sums, Counts: crdtState.Counts}}}}
}

func (crdtState DataArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return DataArrayState(protobuf.GetPartread().GetMultiarray().GetData().GetDataValues())
}

func (crdtState DataArrayState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_DATA
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Data: &proto.ApbMultiArrayDataResp{DataValues: crdtState}}}}
}

func (crdtState IntArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return IntArraySingleState(protobuf.GetPartread().GetMultiarray().GetSingle().GetIntValue())
}

func (crdtState IntArraySingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_INT
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Single: &proto.ApbMultiArraySingleResp{IntValue: pb.Int64(int64(crdtState))}}}}
}

func (crdtState FloatArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return FloatArraySingleState(protobuf.GetPartread().GetMultiarray().GetSingle().GetFloatValue())
}

func (crdtState FloatArraySingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_FLOAT
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Single: &proto.ApbMultiArraySingleResp{FloatValue: pb.Float64(float64(crdtState))}}}}
}

func (crdtState AvgArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	singleProto := protobuf.GetPartread().GetMultiarray().GetSingle()
	return AvgArraySingleState{Sum: singleProto.GetSumValue(), Count: singleProto.GetCountValue()}
}

func (crdtState AvgArraySingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Single: &proto.ApbMultiArraySingleResp{SumValue: pb.Int64(crdtState.Sum), CountValue: pb.Int64(crdtState.Count)}}}}
}

func (crdtState DataArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return DataArraySingleState(protobuf.GetPartread().GetMultiarray().GetSingle().GetDataValue())
}

func (crdtState DataArraySingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_DATA
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, Single: &proto.ApbMultiArraySingleResp{DataValue: crdtState}}}}
}

func (crdtState MultiArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	singleProto := protobuf.GetPartread().GetMultiarray().GetSingle()
	return MultiArraySingleState{IntValue: singleProto.GetIntValue(), FloatValue: singleProto.GetFloatValue(),
		DataValue: singleProto.GetDataValue(), Sum: singleProto.GetSumValue(), Count: singleProto.GetCountValue()}
}

func (crdtState MultiArraySingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_MULTI
	singleProto := proto.ApbMultiArraySingleResp{}
	if crdtState.IntValue != 0 {
		singleProto.IntValue = &crdtState.IntValue
	}
	if crdtState.FloatValue != 0 {
		singleProto.FloatValue = &crdtState.FloatValue
	}
	if crdtState.DataValue != nil {
		singleProto.DataValue = crdtState.DataValue
	}
	if crdtState.Sum != 0 {
		singleProto.SumValue = &crdtState.Sum
		singleProto.CountValue = &crdtState.Count
	}
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{Type: &arrayType, Single: &singleProto}}}
}

func (crdtState MultiArrayDataSliceIntPosState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	stateProto := protobuf.GetPartread().GetMultiarray().GetDataAndSingle()
	//fmt.Printf("[MultiArrayDataSliceIntPosState][FromReadResp]len(data): %d. Int value: %d. Data values: %v, %v, %v.\n", len(stateProto.GetDataValues()), stateProto.GetIntValue(),
	//stateProto.GetDataValues()[0], stateProto.GetDataValues()[1], stateProto.GetDataValues()[2])
	return MultiArrayDataSliceIntPosState{DataCounters: stateProto.GetDataValues(), IntValue: stateProto.GetIntValue()}
}

func (crdtState MultiArrayDataSliceIntPosState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_INT
	//fmt.Printf("[MultiArrayDataSliceIntPosState][ToReadResp]len(data): %d. Int value: %d. Data values: %v, %v, %v.\n", len(crdtState.DataCounters), crdtState.IntValue,
	//crdtState.DataCounters[0], crdtState.DataCounters[1], crdtState.DataCounters[2])
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, DataAndSingle: &proto.ApbMultiArrayDataAndSingleResp{DataValues: crdtState.DataCounters, IntValue: &crdtState.IntValue}}}}
}

func (crdtState MultiArrayDataSliceFloatPosState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	stateProto := protobuf.GetPartread().GetMultiarray().GetDataAndSingle()
	return MultiArrayDataSliceFloatPosState{DataCounters: stateProto.GetDataValues(), FloatValue: stateProto.GetFloatValue()}
}

func (crdtState MultiArrayDataSliceFloatPosState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_FLOAT
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, DataAndSingle: &proto.ApbMultiArrayDataAndSingleResp{DataValues: crdtState.DataCounters, FloatValue: &crdtState.FloatValue}}}}
}

func (crdtState MultiArrayDataSliceAvgPosState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	stateProto := protobuf.GetPartread().GetMultiarray().GetDataAndSingle()
	return MultiArrayDataSliceAvgPosState{DataCounters: stateProto.GetDataValues(), Sum: stateProto.GetIntValue(), Count: stateProto.GetCountValue()}
}

func (crdtState MultiArrayDataSliceAvgPosState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	arrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Multiarray: &proto.ApbMultiArrayPartialReadResp{
		Type: &arrayType, DataAndSingle: &proto.ApbMultiArrayDataAndSingleResp{DataValues: crdtState.DataCounters, IntValue: &crdtState.Sum, CountValue: &crdtState.Count}}}}
}

func (args MultiArrayFullArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	readTypes := protobuf.GetMultiarray().GetTypes()
	return MultiArrayFullArguments(readTypes[0])
}

func (args MultiArrayFullArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Types: []proto.MultiArrayType{proto.MultiArrayType(args)}}}
}

func (args MultiArrayPosArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	return MultiArrayPosArguments{Pos: arrayProto.GetPos().GetPos()[0], ArrayType: MULTI_ARRAY_TYPE(arrayProto.GetTypes()[0])}
}

func (args MultiArrayPosArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Pos: &proto.ApbMultiArrayPosRead{Pos: []int32{args.Pos}},
		Types: []proto.MultiArrayType{proto.MultiArrayType(args.ArrayType)}}}
}

func (args MultiArrayRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	rangeProto := arrayProto.GetRange()
	return MultiArrayRangeArguments{From: rangeProto.GetStart()[0], To: rangeProto.GetEnd()[0],
		ArrayType: MULTI_ARRAY_TYPE(arrayProto.GetTypes()[0])}
}

func (args MultiArrayRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Range: &proto.ApbMultiArrayRangeRead{
		Start: []int32{args.From}, End: []int32{args.To}}, Types: []proto.MultiArrayType{proto.MultiArrayType(args.ArrayType)}}}
}

func (args MultiArraySubArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	return MultiArraySubArguments{Positions: arrayProto.GetSub().GetIndexes()[0].GetValues(), ArrayType: MULTI_ARRAY_TYPE(arrayProto.GetTypes()[0])}
}

func (args MultiArraySubArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Sub: &proto.ApbMultiArraySubRead{
		Indexes: []*proto.IntSlice{{Values: args.Positions}}}, Types: []proto.MultiArrayType{proto.MultiArrayType(args.ArrayType)}}}
}

func (args MultiArrayFullTypesArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return MultiArrayFullTypesArguments(multiArrayCopyProtoTypesToPotionTypes(protobuf.GetMultiarray().GetTypes()))
}

func (args MultiArrayFullTypesArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Types: multiArrayCopyPotionTypesToProtoTypes(args)}}
}

func (args MultiArrayPosTypesArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	return MultiArrayPosTypesArguments{Positions: arrayProto.GetPos().GetPos(), Types: multiArrayCopyProtoTypesToPotionTypes(arrayProto.GetTypes())}
}

func (args MultiArrayPosTypesArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Pos: &proto.ApbMultiArrayPosRead{Pos: args.Positions},
		Types: multiArrayCopyPotionTypesToProtoTypes(args.Types)}}
}

func (args MultiArrayRangeTypesArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	rangeProto := arrayProto.GetRange()
	return MultiArrayRangeTypesArguments{From: rangeProto.GetStart(), To: rangeProto.GetEnd(), Types: multiArrayCopyProtoTypesToPotionTypes(arrayProto.GetTypes())}
}

func (args MultiArrayRangeTypesArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Range: &proto.ApbMultiArrayRangeRead{
		Start: args.From, End: args.To}, Types: multiArrayCopyPotionTypesToProtoTypes(args.Types)}}
}

func (args MultiArraySubTypesArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	protoIndexes := arrayProto.GetSub().GetIndexes()
	positions := make([][]int32, len(protoIndexes))
	for i, protoIndex := range protoIndexes {
		positions[i] = protoIndex.GetValues()
	}
	return MultiArraySubTypesArguments{Positions: positions, Types: multiArrayCopyProtoTypesToPotionTypes(arrayProto.GetTypes())}
}

func (args MultiArraySubTypesArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	protoIndexes := make([]*proto.IntSlice, len(args.Positions))
	for i, positions := range args.Positions {
		protoIndexes[i] = &proto.IntSlice{Values: positions}
	}
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Sub: &proto.ApbMultiArraySubRead{
		Indexes: protoIndexes}, Types: multiArrayCopyPotionTypesToProtoTypes(args.Types)}}
}

func (args MultiArrayCustomArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	customProto := protobuf.GetMultiarray().GetCustom()
	if protoInts := customProto.GetIntIndexes(); len(protoInts) > 0 {
		args.IntPos = protoInts
	}
	if protoFloats := customProto.GetFloatIndexes(); len(protoFloats) > 0 {
		args.FloatPos = protoFloats
	}
	if protoData := customProto.GetDataIndexes(); len(protoData) > 0 {
		args.DataPos = protoData
	}
	if protoAvgs := customProto.GetAvgIndexes(); len(protoAvgs) > 0 {
		args.AvgPos = protoAvgs
	}
	return args
}

func (args MultiArrayCustomArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	customProto, arrayType := &proto.ApbMultiArrayCustomRead{}, []proto.MultiArrayType{proto.MultiArrayType_MULTI}
	if len(args.IntPos) > 0 {
		customProto.IntIndexes = args.IntPos
	}
	if len(args.FloatPos) > 0 {
		customProto.FloatIndexes = args.FloatPos
	}
	if len(args.DataPos) > 0 {
		customProto.DataIndexes = args.DataPos
	}
	if len(args.AvgPos) > 0 {
		customProto.AvgIndexes = args.AvgPos
	}
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Types: arrayType, Custom: customProto}}
}

func (args MultiArrayComparableArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	arrayProto := protobuf.GetMultiarray()
	condProto := arrayProto.GetCond()
	args.FullReads = multiArrayCopyProtoTypesToPotionTypes(arrayProto.GetTypes())
	args.CompareArrayType = MULTI_ARRAY_TYPE(condProto.GetCompareType())
	compType := CompType(condProto.GetComp())
	switch args.CompareArrayType {
	case MultiInts:
		args.CompareType = IntCompareArguments{CompType: compType, Value: condProto.GetCompareInt()}
	case MultiFloats, MultiAvg:
		args.CompareType = FloatCompareArguments{CompType: compType, Value: condProto.GetCompareFloat()}
	case MultiData:
		isEqualComp := compType == EQ
		args.CompareType = BytesCompareArguments{IsEqualComp: isEqualComp, Value: condProto.GetCompareData()}
	}
	args.ComparePos = condProto.GetComparePos()
	return args
}

func (args MultiArrayComparableArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	protoArrayType := proto.MultiArrayType(args.CompareArrayType)
	condProto := &proto.ApbMultiArrayCondRead{CompareType: &protoArrayType, ComparePos: &args.ComparePos}
	switch args.CompareArrayType {
	case MultiInts:
		condProto.CompareInt = pb.Int64(args.CompareType.(IntCompareArguments).Value)
	case MultiFloats, MultiAvg:
		condProto.CompareFloat = pb.Float64(args.CompareType.(FloatCompareArguments).Value)
	case MultiData:
		condProto.CompareData = args.CompareType.(BytesCompareArguments).Value
	}
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Cond: condProto, Types: multiArrayCopyPotionTypesToProtoTypes(args.FullReads)}}
}

func (args MultiArrayDataIntArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return MultiArrayDataIntArguments(protobuf.GetMultiarray().GetDatacond().GetCompareInt())
}

func (args MultiArrayDataIntArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	protoArrayType := proto.MultiArrayType_INT
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Datacond: &proto.ApbMultiArrayDataCondRead{
		ComparePos: pb.Int32(int32(args))}, Types: []proto.MultiArrayType{protoArrayType}}}
}

func (args MultiArrayDataIntComparableArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	condProto := protobuf.GetMultiarray().GetDatacond()
	return MultiArrayDataIntComparableArguments{CompareType: IntCompareArguments{CompType: CompType(condProto.GetComp()), Value: condProto.GetCompareInt()}, ComparePos: condProto.GetComparePos()}
}

func (args MultiArrayDataIntComparableArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	protoCompType, intComp, protoArrayType := proto.COMPType(args.CompareType.GetCompType()), args.CompareType.(IntCompareArguments), proto.MultiArrayType_INT
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Datacond: &proto.ApbMultiArrayDataCondRead{
		Comp: &protoCompType, CompareType: &protoArrayType, CompareInt: &intComp.Value, ComparePos: &args.ComparePos}, Types: []proto.MultiArrayType{protoArrayType}}}
}

func (args MultiArrayDataFloatComparableArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	condProto := protobuf.GetMultiarray().GetDatacond()
	return MultiArrayDataFloatComparableArguments{CompareType: FloatCompareArguments{CompType: CompType(condProto.GetComp()), Value: condProto.GetCompareFloat()}, ComparePos: condProto.GetComparePos()}
}

func (args MultiArrayDataFloatComparableArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	protoCompType, floatComp, protoArrayType := proto.COMPType(args.CompareType.GetCompType()), args.CompareType.(FloatCompareArguments), proto.MultiArrayType_FLOAT
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Datacond: &proto.ApbMultiArrayDataCondRead{
		Comp: &protoCompType, CompareType: &protoArrayType, CompareFloat: &floatComp.Value, ComparePos: &args.ComparePos}, Types: []proto.MultiArrayType{protoArrayType}}}
}

func (args MultiArrayDataAvgComparableArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	condProto := protobuf.GetMultiarray().GetDatacond()
	return MultiArrayDataAvgComparableArguments{CompareType: FloatCompareArguments{CompType: CompType(condProto.GetComp()), Value: condProto.GetCompareFloat()}, ComparePos: condProto.GetComparePos()}
}

func (args MultiArrayDataAvgComparableArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	protoCompType, floatComp, protoArrayType := proto.COMPType(args.CompareType.GetCompType()), args.CompareType.(FloatCompareArguments), proto.MultiArrayType_AVG_TYPE
	return &proto.ApbPartialReadArgs{Multiarray: &proto.ApbMultiArrayPartialRead{Datacond: &proto.ApbMultiArrayDataCondRead{
		Comp: &protoCompType, CompareType: &protoArrayType, CompareFloat: &floatComp.Value, ComparePos: &args.ComparePos}, Types: []proto.MultiArrayType{protoArrayType}}}
}

func (downOp MultiArrayIncIntSingle) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	intProto := protobuf.GetMultiArrayOp().GetIntUpd().GetIncSingle()
	return MultiArrayIncIntSingle{Pos: intProto.GetPos(), Change: intProto.GetChange()}
}

func (downOp MultiArrayIncIntSingle) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_INT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, IntUpd: &proto.ProtoMultiArrayIntDownstream{
		IncSingle: &proto.ProtoMultiArrayIntIncSingleDownstream{Pos: &downOp.Pos, Change: &downOp.Change}}}}
}

func (downOp MultiArrayIncInt) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return MultiArrayIncInt(protobuf.GetMultiArrayOp().GetIntUpd().GetInc().GetChanges())
}

func (downOp MultiArrayIncInt) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_INT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, IntUpd: &proto.ProtoMultiArrayIntDownstream{
		Inc: &proto.ProtoMultiArrayIntIncDownstream{Changes: downOp}}}}
}

func (downOp MultiArrayIncIntPositions) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	intProto := protobuf.GetMultiArrayOp().GetIntUpd().GetIncPos()
	return MultiArrayIncIntPositions{Pos: intProto.GetPos(), Changes: intProto.GetChange()}
}

func (downOp MultiArrayIncIntPositions) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_INT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, IntUpd: &proto.ProtoMultiArrayIntDownstream{
		IncPos: &proto.ProtoMultiArrayIntIncPositionsDownstream{Pos: downOp.Pos, Change: downOp.Changes}}}}
}

func (downOp MultiArrayIncIntRange) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	intProto := protobuf.GetMultiArrayOp().GetIntUpd().GetIncRange()
	return MultiArrayIncIntRange{From: intProto.GetFrom(), To: intProto.GetTo(), Change: intProto.GetChange()}
}

func (downOp MultiArrayIncIntRange) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_INT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, IntUpd: &proto.ProtoMultiArrayIntDownstream{
		IncRange: &proto.ProtoMultiArrayIntIncRangeDownstream{From: &downOp.From, To: &downOp.To, Change: &downOp.Change}}}}
}

func (downOp MultiArrayIncFloatSingle) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	floatProto := protobuf.GetMultiArrayOp().GetFloatUpd().GetIncSingle()
	return MultiArrayIncFloatSingle{Pos: floatProto.GetPos(), Change: floatProto.GetChange()}
}

func (downOp MultiArrayIncFloatSingle) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_FLOAT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, FloatUpd: &proto.ProtoMultiArrayFloatDownstream{
		IncSingle: &proto.ProtoMultiArrayFloatIncSingleDownstream{Pos: &downOp.Pos, Change: &downOp.Change}}}}
}

func (downOp MultiArrayIncFloat) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return MultiArrayIncFloat(protobuf.GetMultiArrayOp().GetFloatUpd().GetInc().GetChanges())
}

func (downOp MultiArrayIncFloat) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_FLOAT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, FloatUpd: &proto.ProtoMultiArrayFloatDownstream{
		Inc: &proto.ProtoMultiArrayFloatIncDownstream{Changes: downOp}}}}
}

func (downOp MultiArrayIncFloatPositions) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	floatProto := protobuf.GetMultiArrayOp().GetFloatUpd().GetIncPos()
	return MultiArrayIncFloatPositions{Pos: floatProto.GetPos(), Changes: floatProto.GetChange()}
}

func (downOp MultiArrayIncFloatPositions) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_FLOAT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, FloatUpd: &proto.ProtoMultiArrayFloatDownstream{
		IncPos: &proto.ProtoMultiArrayFloatIncPositionsDownstream{Pos: downOp.Pos, Change: downOp.Changes}}}}
}

func (downOp MultiArrayIncFloatRange) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	floatProto := protobuf.GetMultiArrayOp().GetFloatUpd().GetIncRange()
	return MultiArrayIncFloatRange{From: floatProto.GetFrom(), To: floatProto.GetTo(), Change: floatProto.GetChange()}
}

func (downOp MultiArrayIncFloatRange) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_FLOAT
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, FloatUpd: &proto.ProtoMultiArrayFloatDownstream{
		IncRange: &proto.ProtoMultiArrayFloatIncRangeDownstream{From: &downOp.From, To: &downOp.To, Change: &downOp.Change}}}}
}

func (downOp DownstreamMultiArraySetRegisterSingle) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	dataProto := protobuf.GetMultiArrayOp().GetDataUpd().GetSetSingle()
	return DownstreamMultiArraySetRegisterSingle{Pos: dataProto.GetPos(), Value: dataProto.GetData(), TsId: dataProto.GetTsId()}
}

func (downOp DownstreamMultiArraySetRegisterSingle) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_DATA
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, DataUpd: &proto.ProtoMultiArrayDataDownstream{
		SetSingle: &proto.ProtoMultiArrayDataSetSingleDownstream{Pos: &downOp.Pos, Data: downOp.Value, TsId: &downOp.TsId}}}}
}

func (downOp DownstreamMultiArraySetRegister) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	dataProto := protobuf.GetMultiArrayOp().GetDataUpd().GetSet()
	return DownstreamMultiArraySetRegister{Values: dataProto.GetData(), TsId: dataProto.GetTsId()}
}

func (downOp DownstreamMultiArraySetRegister) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_DATA
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, DataUpd: &proto.ProtoMultiArrayDataDownstream{
		Set: &proto.ProtoMultiArrayDataSetDownstream{Data: downOp.Values, TsId: &downOp.TsId}}}}
}

func (downOp DownstreamMultiArraySetRegisterPositions) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	dataProto := protobuf.GetMultiArrayOp().GetDataUpd().GetSetPos()
	return DownstreamMultiArraySetRegisterPositions{Pos: dataProto.GetPos(), Values: dataProto.GetData(), TsId: dataProto.GetTsId()}
}

func (downOp DownstreamMultiArraySetRegisterPositions) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_DATA
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, DataUpd: &proto.ProtoMultiArrayDataDownstream{
		SetPos: &proto.ProtoMultiArrayDataSetPositionsDownstream{Pos: downOp.Pos, Data: downOp.Values, TsId: &downOp.TsId}}}}
}

func (downOp DownstreamMultiArraySetRegisterRange) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	dataProto := protobuf.GetMultiArrayOp().GetDataUpd().GetSetRange()
	return DownstreamMultiArraySetRegisterRange{From: dataProto.GetFrom(), To: dataProto.GetTo(), Value: dataProto.GetData(), TsId: dataProto.GetTsId()}
}

func (downOp DownstreamMultiArraySetRegisterRange) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_DATA
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, DataUpd: &proto.ProtoMultiArrayDataDownstream{
		SetRange: &proto.ProtoMultiArrayDataSetRangeDownstream{From: &downOp.From, To: &downOp.To, Data: downOp.Value, TsId: &downOp.TsId}}}}
}

func (downOp MultiArrayIncAvgSingle) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	avgProto := protobuf.GetMultiArrayOp().GetAvgUpd().GetIncSingle()
	return MultiArrayIncAvgSingle{Pos: avgProto.GetPos(), Value: avgProto.GetValue(), Count: avgProto.GetCount()}
}

func (downOp MultiArrayIncAvgSingle) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, AvgUpd: &proto.ProtoMultiArrayAvgDownstream{
		IncSingle: &proto.ProtoMultiArrayAvgIncSingleDownstream{Pos: &downOp.Pos, Value: &downOp.Value, Count: &downOp.Count}}}}
}

func (downOp MultiArrayIncAvg) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	avgProto := protobuf.GetMultiArrayOp().GetAvgUpd().GetInc()
	return MultiArrayIncAvg{Value: avgProto.GetValue(), Count: avgProto.GetCount()}
}

func (downOp MultiArrayIncAvg) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, AvgUpd: &proto.ProtoMultiArrayAvgDownstream{
		Inc: &proto.ProtoMultiArrayAvgIncDownstream{Value: downOp.Value, Count: downOp.Count}}}}
}

func (downOp MultiArrayIncAvgPositions) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	avgProto := protobuf.GetMultiArrayOp().GetAvgUpd().GetIncPos()
	return MultiArrayIncAvgPositions{Pos: avgProto.GetPos(), Value: avgProto.GetValue(), Count: avgProto.GetCount()}
}

func (downOp MultiArrayIncAvgPositions) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, AvgUpd: &proto.ProtoMultiArrayAvgDownstream{
		IncPos: &proto.ProtoMultiArrayAvgIncPositionsDownstream{Pos: downOp.Pos, Value: downOp.Value, Count: downOp.Count}}}}
}

func (downOp MultiArrayIncAvgRange) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	avgProto := protobuf.GetMultiArrayOp().GetAvgUpd().GetIncRange()
	return MultiArrayIncAvgRange{From: avgProto.GetFrom(), To: avgProto.GetTo(), Value: avgProto.GetValue(), Count: avgProto.GetCount()}
}

func (downOp MultiArrayIncAvgRange) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoArrayType := proto.MultiArrayType_AVG_TYPE
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, AvgUpd: &proto.ProtoMultiArrayAvgDownstream{
		IncRange: &proto.ProtoMultiArrayAvgIncRangeDownstream{From: &downOp.From, To: &downOp.To, Value: &downOp.Value, Count: &downOp.Count}}}}
}

func (downOp DownstreamMultiArrayUpdateAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	multiUpdProto := protobuf.GetMultiArrayOp().GetMultiUpd()
	return DownstreamMultiArrayUpdateAll{MultiArrayUpdateAll{Ints: multiUpdProto.GetInts(), Floats: multiUpdProto.GetFloats(), Data: multiUpdProto.GetData(), Counts: multiUpdProto.GetCounts(), Sums: multiUpdProto.GetSums()}, multiUpdProto.GetTsId()}
}

func (downOp DownstreamMultiArrayUpdateAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	multiUpdProto, protoArrayType := proto.ProtoMultiArrayMultiUpdateDownstream{}, proto.MultiArrayType_MULTI
	if len(downOp.Ints) > 0 {
		multiUpdProto.Ints = downOp.Ints
	}
	if len(downOp.Floats) > 0 {
		multiUpdProto.Floats = downOp.Floats
	}
	if len(downOp.Data) > 0 {
		multiUpdProto.Data, multiUpdProto.TsId = downOp.Data, &downOp.TsId
	}
	if len(downOp.Counts) > 0 {
		multiUpdProto.Counts, multiUpdProto.Sums = downOp.Counts, downOp.Sums
	}
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, MultiUpd: &multiUpdProto}}
}

func (downOp MultiArraySetSizes) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	setSizesProto := protobuf.GetMultiArrayOp().GetSizeUpd()
	return MultiArraySetSizes{IntSize: setSizesProto.GetIntSize(), FloatSize: setSizesProto.GetFloatSize(), RegisterSize: setSizesProto.GetDataSize(), AvgSize: setSizesProto.GetAvgSize()}
}

func (downOp MultiArraySetSizes) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	setSizesProto, protoArrayType := proto.ProtoMultiArraySetSizeDownstream{}, proto.MultiArrayType_SIZE
	if downOp.IntSize > 0 {
		setSizesProto.IntSize = &downOp.IntSize
	}
	if downOp.FloatSize > 0 {
		setSizesProto.FloatSize = &downOp.FloatSize
	}
	if downOp.RegisterSize > 0 {
		setSizesProto.DataSize = &downOp.RegisterSize
	}
	if downOp.AvgSize > 0 {
		setSizesProto.AvgSize = &downOp.AvgSize
	}
	return &proto.ProtoOpDownstream{MultiArrayOp: &proto.ProtoMultiArrayDownstream{Type: &protoArrayType, SizeUpd: &setSizesProto}}
}

func (crdt MultiArrayCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{MultiArray: &proto.ProtoMultiArrayState{IntCounters: crdt.intCounters, FloatCounters: crdt.floatCounters,
		Data: crdt.dataCounters, Sums: crdt.sums, Counts: crdt.counts}}
}

func (crdt MultiArrayCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	protoState := proto.GetMultiArray()
	return &MultiArrayCrdt{intCounters: protoState.GetIntCounters(), floatCounters: protoState.GetFloatCounters(),
		dataCounters: protoState.GetData(), sums: protoState.GetSums(), counts: protoState.GetCounts()}
}

func multiArrayCopyProtoTypesToPotionTypes(protoTypes []proto.MultiArrayType) (types []MULTI_ARRAY_TYPE) {
	types = make([]MULTI_ARRAY_TYPE, len(protoTypes))
	for i, protoType := range protoTypes {
		types[i] = MULTI_ARRAY_TYPE(protoType)
	}
	return types
}

func multiArrayCopyPotionTypesToProtoTypes(types []MULTI_ARRAY_TYPE) (protoTypes []proto.MultiArrayType) {
	protoTypes = make([]proto.MultiArrayType, len(types))
	for i, typ := range types {
		protoTypes[i] = proto.MultiArrayType(typ)
	}
	return protoTypes
}

func (crdt *MultiArrayCrdt) GetCRDT() CRDT { return crdt }

/*
switch intUpd := downstreamArgs.(type) {
	case CounterArrayIncrement:
		if int(intUpd.Position) >= len(crdt.intCounters) {
			effectValue = MultiArraySetSizesEffect{intSize: int32(len(crdt.intCounters))} //The value of this position before was "0" as it did not belong to the array
			crdt.expandIntArray(intUpd.Position + 1)
		} else {
			effectValue = CounterArrayIncrementEffect(intUpd)
		}
		crdt.intCounters[intUpd.Position] += intUpd.Change
	case CounterArrayIncrementMulti:
		if len(intUpd) > len(crdt.intCounters) {
			effectValue = CounterArrayIncMultiWithSizeEffect{IncEff: CounterArrayIncrementMultiEffect(intUpd), OldSize: len(crdt.intCounters)}
			crdt.expandIntArray(int32(len(intUpd)))
		} else {
			effectValue = CounterArrayIncrementMultiEffect(intUpd)
		}
		for i, change := range intUpd {
			crdt.intCounters[i] += change
		}
	case CounterArrayIncrementSub:
		oldSize := len(crdt.intCounters)
		if len(intUpd.Changes) == 1 {
			change := intUpd.Changes[0]
			for _, pos := range intUpd.Positions {
				if int(pos) >= len(crdt.intCounters) {
					crdt.expandIntArray(pos + 1)
				}
				crdt.intCounters[pos] += change
			}
		} else {
			for i, pos := range intUpd.Positions {
				if int(pos) >= len(crdt.intCounters) {
					crdt.expandIntArray(pos + 1)
				}
				crdt.intCounters[pos] += intUpd.Changes[i]
			}
		}
		if oldSize != len(crdt.intCounters) {
			effectValue = CounterArrayIncSubWithSizeEffect{IncEff: CounterArrayIncrementSubEffect(intUpd), OldSize: oldSize}
		} else {
			effectValue = CounterArrayIncrementSubEffect(intUpd)
		}
	}
*/

/*func (crdt *MultiArrayCrdt) applyIntUpdToSubSlice(upd MultiArrayUpd, intC []int64, positions []int32) []int64 {
	switch typedUpd := upd.(type) {
	case CounterArrayIncrement:
		if typedUpd.Position > int32(len(intC)) {
			intC = copySliceWithSize[int64](intC, typedUpd.Position+1)
		}
		intC[typedUpd.Position] += typedUpd.Change
	case CounterArrayIncrementMulti:
		if len(typedUpd) > len(intC) {
			intC = copySliceWithSize[int64](intC, int32(len(typedUpd)))
		}
		for i, change := range typedUpd {
			intC[i] += change
		}
	case CounterArrayIncrementSub:
		if len(typedUpd.Changes) == 1 {
			change := typedUpd.Changes[0]
			for _, pos := range typedUpd.Positions {
				if pos > int32(len(intC)) {
					intC = copySliceWithSize[int64](intC, pos+1)
				}
				intC[pos] += change
			}
		} else {
			for i, pos := range typedUpd.Positions {
				if pos > int32(len(intC)) {
					intC = copySliceWithSize[int64](intC, pos+1)
				}
				intC[pos] += typedUpd.Changes[i]
			}
		}
	}
	return intC
}*/

//Old methods and idea related with having an average in a single int64.

//Note: old idea for average that may be relevant later:
//To support an average in an int64 datatype, we can use the highest 40 bits for the sum, and the lowest 24 bits for the count
//This way, we can support sums up to 2^40-1 = 1.099.511.627.775 and counts up to 2^24-1 = 16.777.215
//To read sum and count from elem: sum = elem >> 24; count = elem & 0xFFFFFF
//For count, to fix the sign, we do the following: if count >= 1 << 23 { count -= 1 << 24 }
//To add to elem a sum and count: elem += sum << 24 | count&0xFFFFFF

/*func (crdt *MultiArrayCrdt) applyAvgUpdToTmpSlice(upd MultiArrayUpd, intC []int64) []int64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncAvgSingle:
		if typedUpd.Pos >= int32(len(intC)) {
			intC = copySliceWithSize[int64](intC, typedUpd.Pos+1)
		}
		intC[typedUpd.Pos] += int64(typedUpd.Value)<<24 | int64(typedUpd.Count)&0xFFFFFF //|: same effect as +
	case MultiArrayIncAvg:
		if len(typedUpd.Value) > len(intC) {
			intC = copySliceWithSize[int64](intC, int32(len(typedUpd.Value)))
		}
		for i, value := range typedUpd.Value {
			intC[i] += int64(value)<<24 | int64(typedUpd.Count[i])&0xFFFFFF
		}
	case MultiArrayIncAvgPositions:
		if len(typedUpd.Value) == 1 {
			value, count := typedUpd.Value[0], typedUpd.Count[0]
			for _, pos := range typedUpd.Pos {
				if int(pos) >= len(intC) {
					intC = copySliceWithSize[int64](intC, pos+1)
				}
				intC[pos] += int64(value)<<24 | int64(count)&0xFFFFFF
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if int(pos) >= len(intC) {
					intC = copySliceWithSize[int64](intC, pos+1)
				}
				intC[pos] += int64(typedUpd.Value[i])<<24 | int64(typedUpd.Count[i])&0xFFFFFF
			}
		}
	}
	return intC
}

func (crdt *MultiArrayCrdt) applyAvgUpdToSingleValue(upd MultiArrayUpd, intV int64, pos int32) int64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncAvgSingle:
		if typedUpd.Pos == pos {
			return intV + int64(typedUpd.Value)<<24 | int64(typedUpd.Count)&0xFFFFFF
		}
	case MultiArrayIncAvg:
		if pos < int32(len(typedUpd.Value)) {
			return intV + int64(typedUpd.Value[pos])<<24 | int64(typedUpd.Count[pos])&0xFFFFFF
		}
	case MultiArrayIncAvgPositions:
		for i, thisPos := range typedUpd.Pos {
			if pos == thisPos {
				return intV + int64(typedUpd.Value[i])<<24 | int64(typedUpd.Count[i])&0xFFFFFF
			}
		}
	}
	return intV
}


func (crdt *MultiArrayCrdt) applyAvgUpdToRangeSlice(upd MultiArrayUpd, intC []int64, from, to int32) []int64 {
	switch typedUpd := upd.(type) {
	case MultiArrayIncAvgSingle:
		if typedUpd.Pos > int32(len(intC)) && typedUpd.Pos < to {
			intC = copySliceWithSize[int64](intC, typedUpd.Pos+1)
		}
		if typedUpd.Pos < int32(len(intC)) {
			intC[typedUpd.Pos] += int64(typedUpd.Value)<<24 | int64(typedUpd.Count)&0xFFFFFF
		}
	case MultiArrayIncAvg:
		upperCap, fromInt := to, int(from)
		if len(typedUpd.Value) > len(intC) && int32(len(intC)) < to {
			if len(typedUpd.Value) > int(to) {
				intC = copySliceWithSize[int64](intC, to)
			} else {
				intC = copySliceWithSize[int64](intC, int32(len(typedUpd.Value)))
				upperCap = int32(len(typedUpd.Value))
			}
		}
		if from >= upperCap { //The query starts in a position after the end of the array
			return intC
		}
		for i, value := range typedUpd.Value[from:upperCap] {
			intC[i+fromInt] += int64(value)<<24 | int64(typedUpd.Count[i])&0xFFFFFF
		}
	case MultiArrayIncAvgPositions:
		if len(typedUpd.Value) == 1 {
			value, count := typedUpd.Value[0], typedUpd.Count[0]
			for _, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(intC)) {
						intC = copySliceWithSize[int64](intC, pos+1)
					}
					intC[pos] += int64(value)<<24 | int64(count)&0xFFFFFF
				}
			}
		} else {
			for i, pos := range typedUpd.Pos {
				if pos >= from && pos < to {
					if pos >= int32(len(intC)) {
						intC = copySliceWithSize[int64](intC, pos+1)
					}
					intC[pos] += int64(typedUpd.Value[i])<<24 | int64(typedUpd.Count[i])&0xFFFFFF
				}
			}
		}
	}
	return intC
}

func (crdt *MultiArrayCrdt) toAvgSlice(slice []int64) (sums, counts []int64) {
	sums = make([]int64, len(slice))
	counts = make([]int64, len(slice))
	var count int64
	for i, elem := range slice {
		sums[i] = elem >> 24    //Shifts the lowest 24 bits out.
		count = elem & 0xFFFFFF //Keeps only the lowest 24 bits
		if count >= 1<<23 {     //Ensure the lowest 24 bits retain their signed interpretation
			count -= 1 << 24
		}
		counts[i] = count

	}
	return
}

func (crdt *MultiArrayCrdt) applyDownstreamAvg(downstreamArgs MultiArrayUpd) (effect *Effect) {
	var effectValue Effect
	switch avgUpd := downstreamArgs.(type) {
	case MultiArrayIncAvgSingle:
		if int(avgUpd.Pos) >= len(crdt.intCounters) {
			effectValue = MultiArraySetSizesEffect{intSize: int32(len(crdt.intCounters))} //The value of this position before was "0" as it did not belong to the array
			crdt.expandIntArray(avgUpd.Pos + 1)
		} else {
			effectValue = MultiArrayIncAvgSingleEffect(avgUpd)
		}
		crdt.intCounters[avgUpd.Pos] += int64(avgUpd.Value)<<24 | int64(avgUpd.Count)&0xFFFFFF //|: same effect as +
	case MultiArrayIncAvg:
		if len(avgUpd.Value) > len(crdt.intCounters) {
			effectValue = MultiArrayIncAvgWithSizeEffect{IncEff: MultiArrayIncAvgEffect(avgUpd), OldSize: len(crdt.intCounters)}
			crdt.expandIntArray(int32(len(avgUpd.Value)))
		} else {
			effectValue = MultiArrayIncAvgEffect(avgUpd)
		}
		for i, value := range avgUpd.Value {
			crdt.intCounters[i] += int64(value)<<24 | int64(avgUpd.Count[i])&0xFFFFFF
		}
	case MultiArrayIncAvgPositions:
		oldSize := len(crdt.intCounters)
		if len(avgUpd.Value) == 1 {
			value, count := avgUpd.Value[0], avgUpd.Count[0]
			for _, pos := range avgUpd.Pos {
				if int(pos) >= oldSize {
					crdt.expandIntArray(pos + 1)
				}
				crdt.intCounters[pos] += int64(value)<<24 | int64(count)&0xFFFFFF
			}
		} else {
			for i, pos := range avgUpd.Pos {
				if int(pos) >= oldSize {
					crdt.expandIntArray(pos + 1)
				}
				crdt.intCounters[pos] += int64(avgUpd.Value[i])<<24 | int64(avgUpd.Count[i])&0xFFFFFF
			}
		}
		if oldSize != len(crdt.intCounters) {
			effectValue = MultiArrayIncAvgPositionsWithSizeEffect{IncEff: MultiArrayIncAvgPositionsEffect(avgUpd), OldSize: oldSize}
		} else {
			effectValue = MultiArrayIncAvgPositionsEffect(avgUpd)
		}
	}
	return &effectValue
}*/

//Code for making a sum and count arrays from an array of int64 (highest 40: sum, lowest 24: count)
/*sums, counts := make([]int64, len(positions)), make([]int64, len(positions))
var curr, currCount int64
for i, pos := range positions {
	if pos < int32(len(crdt.intCounters)) {
		curr = crdt.intCounters[pos]
		sums[i], currCount = curr>>24, curr&0xFFFFFF
		if currCount >= 1<<23 { //Ensure the lowest 24 bits retain their signed interpretation
			currCount -= 1 << 24
		}
		counts[i] = currCount
	}
}
return AvgArrayState{Sums: sums, Counts: counts}*/

//For single position:

/*if pos >= len(crdt.intCounters) {
	return AvgArraySingleState{Sum: 0, Count: 0}
}
sum, count := crdt.intCounters[pos]>>24, crdt.intCounters[pos]&0xFFFFFF
if count >= 1<<23 { //Count was negative, making sure the sign is kept
	count -= 1 << 24
}
return AvgArraySingleState{Sum: sum, Count: count}*/
