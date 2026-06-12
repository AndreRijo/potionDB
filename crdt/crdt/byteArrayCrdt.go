package crdt

//NOTE: Untested. But complete other than UpdsNotYetApplied.
//TODO: UpdsNotYetApplied

import (
	"encoding/binary"
	"fmt"
	"math"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
	"strconv"

	tools "github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

//TODO: GC of data []byte. Check if len < cap and, if it is, allocate a new slice and rewrite.

// This is intended as an ultra-compact representation, where memory efficiency is so crucial that even using a slice of strings/any is undesirable.
// When possible, please resort to CompactArrayCrdt or StringArrayCrdt (if memory efficiency is still of high importance), or multiArrayCrdt (for performant access with good semantics).
// For use cases where memory/performance is non-critical, use CRDTs such as map, set, etc.
// This array supports interpreting data positions as registers and counters, supporting adequate read and update operations.
type ByteArrayCrdt struct {
	CRDTVM
	data []byte
	//dataSizes []uint8  //Size (bytes) of each "object" in data.
	dataStarts []uint16
	dataTsId   []uint64 //The highest 48 bits correspond to the lowest 48 bits of a 64-bit timestamp; the lowest 16 bits are used for replicaID.
}

// States
type ByteArrayState [][]byte
type ByteArraySingle []byte
type ByteArrayString string
type ByteArrayInt int64
type ByteArrayFloat float64
type ByteArrayAny struct{ Value any }

// Reads

// Position
type ByteArraySingleAnyArguments int32
type ByteArraySingleIntArguments int32
type ByteArraySingleFloatArguments int32
type ByteArraySingleStringArguments int32
type ByteArraySingleDataArguments int32
type ByteArrayExceptArguments int32

// Positions
type ByteArrayRangeArguments struct{ From, To int32 }
type ByteArraySubArguments []int32

// Updates

type ByteArraySetData struct { //NOTE: These slices will be used directly in the CRDT whenever possible
	Data       []byte
	DataStarts []uint16
	//DataSizes []uint8
}

type ByteArraySetDataInitialize struct { //Same as above, but does not use timestamps. Thus, it must only be called once and come before any other operation.
	Data       []byte
	DataStarts []uint16
	//DataSizes []uint8
}

type ByteArraySetValue struct { //Note: For efficiency, if Pos was already written before, the size should remain the same.
	NewValue []byte
	Pos      int32
}

type ByteArrayIncrement struct{ Change, Pos int32 }
type ByteArrayDecrement struct{ Change, Pos int32 }
type ByteArrayFloatInc struct {
	Change float64
	Pos    int32
}
type ByteArrayFloatDec struct {
	Change float64
	Pos    int32
}

// Downstreams
type DownstreamByteArraySetData struct {
	Data []byte
	//DataSizes []uint8
	DataStarts []uint16
	TsId       uint64
}

type DownstreamByteArraySetValue struct {
	NewValue []byte
	Pos      int32
	TsId     uint64
}

// Effects
type ByteArraySetDataEffect struct {
	OldData []byte
	//OldDataSizes []uint8
	OldDataStarts []uint16
	OldDataTsId   []uint64
}

// Same size, pos in range.
type ByteArraySetValueDirectEffect struct {
	OldValue []byte
	Pos      int32
	OldTsId  uint64
}

// Pos is out of range, so dataTsId is expanded
type ByteArraySetValueExpandEffect struct {
	OldLen uint16
}

// Resizes/moves data
type ByteArraySetValueDiffSizeEffect struct {
	OldValue []byte
	Pos      int32
	OldTsId  uint64
}

// Used for both inc and dec.
type ByteArrayIncEffect struct {
	Change         int32
	Pos, OldNElems uint16
}
type ByteArrayFloatIncEffect struct {
	Change    float64
	Pos       int32
	OldNElems int32
}

//

func (crdt *ByteArrayCrdt) GetCRDTType() proto.CRDTType             { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArraySetDataInitialize) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArraySetData) GetCRDTType() proto.CRDTType           { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArraySetValue) GetCRDTType() proto.CRDTType          { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArrayIncrement) GetCRDTType() proto.CRDTType         { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArrayDecrement) GetCRDTType() proto.CRDTType         { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArrayFloatInc) GetCRDTType() proto.CRDTType          { return proto.CRDTType_ARRAY_BYTE }
func (args ByteArrayFloatDec) GetCRDTType() proto.CRDTType          { return proto.CRDTType_ARRAY_BYTE }

func (args DownstreamByteArraySetData) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_BYTE }
func (args DownstreamByteArraySetValue) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}

func (crdt *ByteArrayCrdt) GetDATAType() proto.DATAType             { return proto.DATAType_DEFAULT }
func (args ByteArraySetDataInitialize) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args ByteArraySetData) GetDATAType() proto.DATAType           { return proto.DATAType_DEFAULT }
func (args ByteArraySetValue) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args ByteArrayIncrement) GetDATAType() proto.DATAType         { return proto.DATAType_DEFAULT }
func (args ByteArrayDecrement) GetDATAType() proto.DATAType         { return proto.DATAType_DEFAULT }
func (args ByteArrayFloatInc) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }
func (args ByteArrayFloatDec) GetDATAType() proto.DATAType          { return proto.DATAType_DEFAULT }

func (args DownstreamByteArraySetData) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (args DownstreamByteArraySetValue) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

func (args DownstreamByteArraySetData) MustReplicate() bool  { return true }
func (args DownstreamByteArraySetValue) MustReplicate() bool { return true }
func (args ByteArraySetDataInitialize) MustReplicate() bool  { return true }
func (args ByteArrayIncrement) MustReplicate() bool          { return true }
func (args ByteArrayDecrement) MustReplicate() bool          { return true }
func (args ByteArrayFloatInc) MustReplicate() bool           { return true }
func (args ByteArrayFloatDec) MustReplicate() bool           { return true }

func (state ByteArrayState) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_BYTE }
func (state ByteArrayState) GetREADType() proto.READType  { return proto.READType_FULL }
func (state ByteArrayState) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (state ByteArraySingle) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_BYTE }
func (state ByteArraySingle) GetREADType() proto.READType { return proto.READType_ARRAY_BYTE_POS }
func (state ByteArraySingle) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state ByteArrayString) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_BYTE }
func (state ByteArrayString) GetREADType() proto.READType { return proto.READType_ARRAY_BYTE_POS }
func (state ByteArrayString) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state ByteArrayInt) GetCRDTType() proto.CRDTType    { return proto.CRDTType_ARRAY_BYTE }
func (state ByteArrayInt) GetREADType() proto.READType    { return proto.READType_ARRAY_BYTE_POS }
func (state ByteArrayInt) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (state ByteArrayFloat) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_BYTE }
func (state ByteArrayFloat) GetREADType() proto.READType  { return proto.READType_ARRAY_BYTE_POS }
func (state ByteArrayFloat) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (state ByteArrayAny) GetCRDTType() proto.CRDTType    { return proto.CRDTType_ARRAY_BYTE }
func (state ByteArrayAny) GetREADType() proto.READType    { return proto.READType_ARRAY_BYTE_POS }
func (state ByteArrayAny) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }

func (args ByteArraySingleAnyArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArraySingleIntArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArraySingleFloatArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArraySingleStringArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArraySingleDataArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArrayExceptArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArrayRangeArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}
func (args ByteArraySubArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_BYTE
}

func (args ByteArraySingleAnyArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_POS
}
func (args ByteArraySingleIntArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_POS
}
func (args ByteArraySingleFloatArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_POS
}
func (args ByteArraySingleStringArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_POS
}
func (args ByteArraySingleDataArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_POS
}
func (args ByteArrayExceptArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_EXCEPT
}
func (args ByteArrayRangeArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_RANGE
}
func (args ByteArraySubArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_BYTE_SUB
}

func (args ByteArraySingleAnyArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArraySingleIntArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArraySingleFloatArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArraySingleStringArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArraySingleDataArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArrayExceptArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArrayRangeArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args ByteArraySubArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}

func (args ByteArraySingleAnyArguments) HasInnerReads() bool    { return false }
func (args ByteArraySingleIntArguments) HasInnerReads() bool    { return false }
func (args ByteArraySingleFloatArguments) HasInnerReads() bool  { return false }
func (args ByteArraySingleStringArguments) HasInnerReads() bool { return false }
func (args ByteArraySingleDataArguments) HasInnerReads() bool   { return false }
func (args ByteArrayExceptArguments) HasInnerReads() bool       { return false }
func (args ByteArrayRangeArguments) HasInnerReads() bool        { return false }
func (args ByteArraySubArguments) HasInnerReads() bool          { return false }
func (args ByteArraySingleAnyArguments) HasVariables() bool     { return false }
func (args ByteArraySingleIntArguments) HasVariables() bool     { return false }
func (args ByteArraySingleFloatArguments) HasVariables() bool   { return false }
func (args ByteArraySingleStringArguments) HasVariables() bool  { return false }
func (args ByteArraySingleDataArguments) HasVariables() bool    { return false }
func (args ByteArrayExceptArguments) HasVariables() bool        { return false }
func (args ByteArrayRangeArguments) HasVariables() bool         { return false }
func (args ByteArraySubArguments) HasVariables() bool           { return false }

func (crdt *ByteArrayCrdt) Initialize(startTs clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	crdt = &ByteArrayCrdt{}
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *ByteArrayCrdt) initializeFromSnapshot(startTs clocksi.Timestamp, replicaID uint16) (sameCRDT *ByteArrayCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *ByteArrayCrdt) IsBigCRDT() bool {
	return len(crdt.dataStarts) >= 500
}

// For reads it is more OK to do copies of data as needed.
func (crdt *ByteArrayCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case ByteArraySingleDataArguments:
		return crdt.getPosData(updsNotYetApplied, int32(typedArgs))
	case ByteArraySingleAnyArguments:
		return crdt.getPosAny(updsNotYetApplied, int32(typedArgs))
	case ByteArraySingleStringArguments:
		return crdt.getPosString(updsNotYetApplied, int32(typedArgs))
	case ByteArraySingleIntArguments:
		return crdt.getPosInt(updsNotYetApplied, int32(typedArgs))
	case ByteArraySingleFloatArguments:
		return crdt.getPosFloat(updsNotYetApplied, int32(typedArgs))
	case ByteArrayExceptArguments:
		return crdt.getExcept(updsNotYetApplied, int32(typedArgs))
	case ByteArrayRangeArguments:
		return crdt.getRange(updsNotYetApplied, int32(typedArgs.From), int32(typedArgs.To))
	case ByteArraySubArguments:
		return crdt.getSub(updsNotYetApplied, typedArgs)
	default:
		fmt.Printf("[CompactArrayCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

// Code is optimized to minimize repeated array accesses.
func (crdt *ByteArrayCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	return ByteArrayState(crdt.getStateHelper(updsNotYetApplied))
}

func (crdt *ByteArrayCrdt) getStateHelper(updsNotYetApplied []UpdateArguments) [][]byte {
	if len(crdt.data) == 0 {
		return [][]byte{}
	}
	if len(crdt.data) == 1 {
		buf := make([]byte, len(crdt.data))
		copy(buf, crdt.data)
		return [][]byte{buf}
	}
	result := make([][]byte, len(crdt.dataStarts))
	var buf []byte
	start, end := crdt.dataStarts[0], crdt.dataStarts[1]
	for i := 1; i < len(crdt.dataStarts)-1; i++ { //Will always execute at least once (len >= 2)
		buf = make([]byte, end-start)
		copy(buf, crdt.data[start:end])
		result[i-1] = buf
		start, end = end, crdt.dataStarts[i+1]
	}
	buf = make([]byte, end-start)
	copy(buf, crdt.data[start:end])
	result[len(crdt.dataStarts)-2] = buf
	buf = make([]byte, len(crdt.data)-int(end))
	copy(buf, crdt.data[end:])
	result[len(crdt.dataStarts)-1] = buf
	return result
}

func (crdt *ByteArrayCrdt) getPosData(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	/*size := crdt.dataSizes[pos]
	buf := make([]byte, size)
	start := int32(0)
	for i := int32(0); i < pos; i++ {
		start += int32(crdt.dataSizes[i])
	}
	copy(buf, crdt.data[start:start+int32(size)])*/
	return ByteArraySingle(crdt.helperCopyPosBytes(pos))
}

func (crdt *ByteArrayCrdt) getPosAny(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	return ByteArrayAny{Value: crdt.helperCopyPosBytes(pos)}
}

func (crdt *ByteArrayCrdt) getPosInt(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	data := string(crdt.helperCopyPosBytes(pos))
	value, _ := strconv.ParseInt(string(data), 10, 64)
	return ByteArrayInt(value)
}

func (crdt *ByteArrayCrdt) getPosFloat(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	data := string(crdt.helperCopyPosBytes(pos))
	value, _ := strconv.ParseFloat(string(data), 64)
	return ByteArrayInt(value)
}

func (crdt *ByteArrayCrdt) getPosString(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	return ByteArrayString(crdt.helperCopyPosBytes(pos))
}

func (crdt *ByteArrayCrdt) getExcept(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	var result [][]byte
	if pos >= int32(len(crdt.dataStarts)) {
		return ByteArrayState(crdt.getStateHelper(updsNotYetApplied))
	}
	if len(crdt.dataStarts) == 1 { //No elements to return
		return ByteArrayState{}
	}
	if len(crdt.dataStarts) == 2 { //Only one element to return
		buf := make([]byte, len(crdt.data))
		copy(buf, crdt.data)
		return ByteArrayState{buf}
	}
	result = make([][]byte, len(crdt.dataStarts)-1)
	var buf []byte
	start, end := crdt.dataStarts[0], crdt.dataStarts[1]
	for i := int32(0); i < tools.Min(pos, int32(len(crdt.dataStarts)-2)); i++ {
		buf = make([]byte, end-start)
		copy(buf, crdt.data[start:end])
		result[i] = buf
		start, end = end, crdt.dataStarts[i+2]
	}
	for i := pos; i < int32(len(crdt.dataStarts)-2); i++ {
		buf = make([]byte, end-start)
		copy(buf, crdt.data[start:end])
		result[i] = buf
		start, end = end, crdt.dataStarts[i+2]
	}
	if pos != int32(len(crdt.dataStarts)-2) { //Copy 2nd to last element
		buf = make([]byte, end-start)
		copy(buf, crdt.data[start:])
		result[len(crdt.dataStarts)-1] = buf
	}
	if pos != int32(len(crdt.dataStarts)-1) { //Copy last element
		buf = make([]byte, len(crdt.data)-int(end))
		copy(buf, crdt.data[end:])
		result[len(crdt.dataStarts)-1] = buf
	}
	return ByteArrayState(result)
}

func (crdt *ByteArrayCrdt) getRange(updsNotYetApplied []UpdateArguments, from, to int32) (state State) {
	//It's basically the same logic as the normal getState, but only between certain positions...
	if len(crdt.data) == 0 || from >= int32(len(crdt.dataStarts)) {
		return ByteArrayState{}
	}

	to = tools.Min(to, int32(len(crdt.dataStarts)-1))
	result := make([][]byte, to-from)
	var buf []byte
	start, end := crdt.dataStarts[from], crdt.dataStarts[from+1]
	for i := from + 1; i < to-1; i++ {
		buf = make([]byte, end-start)
		copy(buf, crdt.data[start:end])
		result[i-from-1] = buf
		start, end = end, crdt.dataStarts[i+1]
	}
	if from-to >= 2 { //Copy 2nd to last element
		buf = make([]byte, end-start)
		copy(buf, crdt.data[start:end])
		result[to-from-2] = buf
	}
	start = end
	if to == int32(len(crdt.dataStarts)-1) { //Last element
		end = uint16(len(crdt.data))
	} else {
		end = crdt.dataStarts[to]
	}
	buf = make([]byte, end-start)
	copy(buf, crdt.data[start:end])
	result[to-from-1] = buf
	return ByteArrayState(result)
}

func (crdt *ByteArrayCrdt) getSub(updsNotYetApplied []UpdateArguments, positions []int32) (state State) {
	result := make([][]byte, len(positions))
	for i, pos := range positions {
		if pos < 0 || pos >= int32(len(crdt.dataStarts)) {
			result[i] = []byte{}
		} else {
			result[i] = crdt.helperCopyPosBytes(pos)
		}
	}
	return ByteArrayState(result)
}

func (crdt *ByteArrayCrdt) helperCopyPosBytes(pos int32) (buf []byte) {
	var start, end uint16
	if pos == int32(len(crdt.data)-1) {
		start, end = crdt.dataStarts[pos], uint16(len(crdt.data))
	} else {
		start, end = crdt.dataStarts[pos], crdt.dataStarts[pos+1]
	}
	buf = make([]byte, end-start)
	copy(buf, crdt.data[start:end])
	return
}

func (crdt *ByteArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch typedArgs := args.(type) {
	case ByteArraySetData:
		tsId := generate64BitTsAndId(int64(shared.ReplicaID))
		return DownstreamByteArraySetData{Data: typedArgs.Data, DataStarts: typedArgs.DataStarts, TsId: tsId}
	case ByteArraySetValue:
		tsId := generate64BitTsAndId(int64(shared.ReplicaID))
		return DownstreamByteArraySetValue{NewValue: typedArgs.NewValue, Pos: typedArgs.Pos, TsId: tsId}
	case ByteArraySetDataInitialize, ByteArrayIncrement, ByteArrayDecrement, ByteArrayFloatInc, ByteArrayFloatDec:
		return typedArgs.(DownstreamArguments)
	case MultiUpd:
		multiDowns := make(MultiUpd, len(typedArgs))
		for i, innerUpd := range typedArgs {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	default:
		fmt.Printf("[ByteArrayCrdt]Unknown update type: %v (%T)\n", args, args)
	}
	return nil
}

func (crdt *ByteArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
	}
	effect := crdt.applyDownstream(downstreamArgs)
	crdt.addToHistory(updTs, downstreamArgs, effect) //Necessary for inversibleCrdt
	return nil
}

func (crdt *ByteArrayCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect Effect) {
	switch typedArgs := downstreamArgs.(type) {
	case DownstreamByteArraySetValue:
		return crdt.applyDownstreamSetValue(typedArgs.Pos, typedArgs.NewValue, typedArgs.TsId)
	case DownstreamByteArraySetData:
		return crdt.applyDownstreamSetData(typedArgs.Data, typedArgs.DataStarts, typedArgs.TsId)
	case ByteArrayIncrement: //Assume 64bits
		return crdt.downstreamIntHelper(typedArgs.Pos, typedArgs.Change)
	case ByteArrayDecrement:
		return crdt.downstreamIntHelper(typedArgs.Pos, -typedArgs.Change)
	case ByteArrayFloatInc:
		return crdt.downstreamFloatHelper(typedArgs.Pos, typedArgs.Change)
	case ByteArrayFloatDec:
		return crdt.downstreamFloatHelper(typedArgs.Pos, -typedArgs.Change)
	case ByteArraySetDataInitialize: //Only once per CRDT, and before any other update.
		crdt.data, crdt.dataStarts = typedArgs.Data, typedArgs.DataStarts
		return NoEffect{}
	default:
		fmt.Printf("[ByteArrayCrdt][Downstream]Unsupported downstream type: %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return nil
}

func (crdt *ByteArrayCrdt) applyDownstreamSetValue(pos int32, newValue []byte, tsId uint64) (effect Effect) {
	if crdt.dataTsId[pos] >= tsId { //No-op
		return NoEffect{}
	}
	var oldValue []byte
	if pos >= int32(len(crdt.dataStarts)) {
		oldValue = crdt.helperCopyPosBytes(pos)
	}
	//Fast path: the new value has the same size as the old one.
	if pos < int32(len(crdt.dataStarts)-1) && len(newValue) == int(crdt.dataStarts[pos+1]-crdt.dataStarts[pos]) {
		effect = ByteArraySetValueDirectEffect{OldValue: oldValue, Pos: pos, OldTsId: crdt.dataTsId[pos]}
		copy(crdt.data[crdt.dataStarts[pos]:crdt.dataStarts[pos+1]], newValue)
		crdt.dataTsId[pos] = tsId
		return effect
	}
	//Also fast path, but at the end of the array.
	if pos == int32(len(crdt.dataStarts)-1) && len(newValue) == int(len(crdt.data)-int(crdt.dataStarts[pos])) {
		effect = ByteArraySetValueDirectEffect{OldValue: oldValue, Pos: pos, OldTsId: crdt.dataTsId[pos]}
		copy(crdt.data[crdt.dataStarts[pos]:], newValue)
		crdt.dataTsId[pos] = tsId
		return effect
	}

	if pos >= int32(len(crdt.dataStarts)) { //Expand array.
		oldSize, oldNElements := len(crdt.data), len(crdt.dataStarts)
		effect = ByteArraySetValueExpandEffect{OldLen: uint16(oldNElements)}
		crdt.expandArray(pos+1, int32(len(crdt.data)+len(newValue)))
		copy(crdt.data[oldSize:], newValue)
		//Must fill all new empty positions of dataStarts with oldSize.
		for i := len(crdt.dataStarts); i < int(pos); i++ {
			crdt.dataStarts[i] = uint16(oldSize)
		}
		crdt.dataTsId[pos] = tsId
		return effect
	}

	effect = ByteArraySetValueDiffSizeEffect{OldValue: oldValue, Pos: pos, OldTsId: crdt.dataTsId[pos]}
	//Pos is somewhere in the existing array, but size is different. It means we will need to "make space".
	//Two options:
	//1 - Always make new slice with appropriate size. Memory-efficient but can lead to many memory allocations/slow.
	//2 - Only make a new slice if it cannot fit. Otherwise, move data around and reslice.
	//In this case, we can re-use the slice if we write a smaller item, or if we write a larger item but there is enough space.
	//If we do 2, we can create an appropriate buffer size during GC().
	//We're going with 2.
	//For simplicity, always move the data in front of pos to the right place. Otherwise, later it may be hard to detect where is the free space, and make it confusing to deal with.
	crdt.dataTsId[pos] = tsId //Update TsId first, as we already know that tsId is higher.
	start := crdt.dataStarts[pos]
	if pos == int32(len(crdt.dataStarts)-1) { //Last item
		size := uint16(len(crdt.data)) - start
		if size > uint16(len(newValue)) { //Can fit in existing space
			copy(crdt.data[start:], newValue)
			crdt.data = crdt.data[:len(crdt.data)-int(size-uint16(len(newValue)))]
		} else { //> (== was already processed before). Have to expand.
			extraNeeded := uint16(len(newValue)) - size
			if len(crdt.data)+int(extraNeeded) <= cap(crdt.data) { //Can fit by reslicing
				crdt.data = crdt.data[:len(crdt.data)+int(extraNeeded)]
				copy(crdt.data[start:], newValue) //Copy the new value. Data starts[Pos] remains unchanged.
			} else {
				newData := make([]byte, len(crdt.data)+int(extraNeeded))
				copy(newData, crdt.data[:start])
				copy(newData[start:], newValue)
				crdt.data = newData
			}
		}
	}

	//Pos is not the last item.
	start, end := crdt.dataStarts[pos], crdt.dataStarts[pos+1]
	currSize := end - start
	if currSize > uint16(len(newValue)) { //Can fit in existing space
		extraSpace := currSize - uint16(len(newValue))
		copy(crdt.data[start:], newValue)
		crdt.dataStarts[pos+1] -= extraSpace
		for i := int(pos + 1); i < len(crdt.dataStarts)-1; i++ {
			copy(crdt.data[crdt.dataStarts[i]:], crdt.data[crdt.dataStarts[i]+extraSpace:crdt.dataStarts[i+1]])
			crdt.dataStarts[i+1] -= extraSpace
		}
		copy(crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]:], crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]+extraSpace:])
		crdt.data = crdt.data[:len(crdt.data)-int(extraSpace)]
	} else { //New item's size is bigger. Let's check if we can fit it by reslicing.
		extraNeeded := uint16(len(newValue)) - currSize
		if len(crdt.data)+int(extraNeeded) <= cap(crdt.data) { //Can fit by reslicing
			crdt.data = crdt.data[:len(crdt.data)+int(extraNeeded)]
			//Copy from the end until Pos.
			copy(crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]+extraNeeded:], crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]:])
			crdt.dataStarts[len(crdt.dataStarts)-1] += extraNeeded
			for i := len(crdt.dataStarts) - 2; i > int(pos); i-- {
				copy(crdt.data[crdt.dataStarts[i]+extraNeeded:], crdt.data[crdt.dataStarts[i]:crdt.dataStarts[i+1]-extraNeeded])
				crdt.dataStarts[i] += extraNeeded
			}
			copy(crdt.data[start:], newValue) //Copy the new value. Data starts[Pos] remains unchanged.
		} else { //Have to make a new slice
			newData := make([]byte, len(crdt.data)+int(extraNeeded))
			copy(newData, crdt.data[:start])
			copy(newData[start:], newValue)
			copy(newData[start+uint16(len(newValue)):], crdt.data[crdt.dataStarts[pos+1]:])
			for i := int(pos + 1); i < len(crdt.dataStarts); i++ { //Fix starting positions
				crdt.dataStarts[i] += extraNeeded
			}
			crdt.data = newData
		}
	}
	return effect
}

func (crdt *ByteArrayCrdt) applyDownstreamSetData(updData []byte, updDataStarts []uint16, tsId uint64) (effect Effect) {
	//Figure out the new data size (iterate both TsId, calculate new starts and decide where to take from)
	//Due to the possibility of different sizes for each "data", we have to always create a new byte slice.
	//Sadly nothing we can reuse :( (unless the new slice wins every TsId)
	takeFromUpd := make([]bool, len(updDataStarts)) //If true, we use the value from typedArgs
	var newDataStarts []uint16
	currStart := uint16(0)
	allTsHigher, allTsLower := true, true
	var oldTsId []uint64
	if len(updDataStarts) > len(crdt.dataStarts) {
		newDataStarts, oldTsId = make([]uint16, len(updDataStarts)), crdt.dataTsId
		crdt.expandTSOnly(len(updDataStarts))
	} else {
		oldTsId = make([]uint64, len(crdt.dataTsId))
		copy(oldTsId, crdt.dataTsId)
		newDataStarts = make([]uint16, len(crdt.dataStarts))
	}
	for i := 0; i < len(crdt.dataStarts)-1; i++ { //Calculate
		if crdt.dataTsId[i] < tsId { //Use typedArgs
			takeFromUpd[i], newDataStarts[i+1] = true, currStart+(updDataStarts[i+1]-updDataStarts[i])
			crdt.dataTsId[i], allTsLower = tsId, false
		} else {
			takeFromUpd[i], newDataStarts[i] = false, currStart+(crdt.dataStarts[i+1]-crdt.dataStarts[i])
			allTsHigher = false
		}
	}
	if allTsLower { //No-op.
		effect = NoEffect{}
	} else if allTsHigher && len(updDataStarts) >= len(crdt.dataStarts) { //Can just use the update slices
		effect = ByteArraySetDataEffect{OldData: crdt.data, OldDataStarts: crdt.dataStarts, OldDataTsId: oldTsId}
		crdt.data, crdt.dataStarts = updData, updDataStarts
	} else { //Have to make new slices
		var size uint16
		if takeFromUpd[len(takeFromUpd)-1] { //Find size of the last element, and add it to the last start.
			size = newDataStarts[len(newDataStarts)-1] + updDataStarts[len(updDataStarts)-1] - updDataStarts[len(updDataStarts)-2]
		} else {
			size = newDataStarts[len(newDataStarts)-1] + crdt.dataStarts[len(crdt.dataStarts)-1] - crdt.dataStarts[len(crdt.dataStarts)-2]
		}
		newData := make([]byte, size)
		for i, start := range newDataStarts[:len(newDataStarts)-1] { //Process last position separately
			if takeFromUpd[i] {
				copy(newData[start:], updData[crdt.dataStarts[i]:crdt.dataStarts[i+1]])
			} else {
				copy(newData[start:], crdt.data[crdt.dataStarts[i]:crdt.dataStarts[i+1]])
			}
		}
		currStart = newDataStarts[len(newDataStarts)-1]
		if takeFromUpd[len(takeFromUpd)-1] {
			copy(newData[currStart:], updData[updDataStarts[len(updDataStarts)-1]:])
		} else {
			copy(newData[currStart:], crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]:])
		}
		effect = ByteArraySetDataEffect{OldData: crdt.data, OldDataStarts: crdt.dataStarts, OldDataTsId: oldTsId}
		crdt.data, crdt.dataStarts = newData, newDataStarts
	}
	return
}

// Applies a downstream update of either ByteArrayIncrement or ByteArrayDecrement
func (crdt *ByteArrayCrdt) downstreamIntHelper(pos, change int32) (effect Effect) {
	effect = ByteArrayIncEffect{Change: change, Pos: uint16(pos), OldNElems: uint16(len(crdt.dataStarts))}
	if pos >= int32(len(crdt.dataStarts)) {
		oldSize := len(crdt.data)
		crdt.expandArray(pos+1, int32(len(crdt.data)+8))
		binary.LittleEndian.PutUint64(crdt.data[oldSize:], uint64(int64(change)))
		crdt.dataStarts[pos] = uint16(oldSize)
	} else {
		value := int64(binary.LittleEndian.Uint64(crdt.data[crdt.dataStarts[pos]:]))
		value += int64(change)
		binary.LittleEndian.PutUint64(crdt.data[crdt.dataStarts[pos]:], uint64(value))
	}
	return
}

func (crdt *ByteArrayCrdt) downstreamFloatHelper(pos int32, change float64) (effect Effect) {
	if pos >= int32(len(crdt.dataStarts)) {
		oldSize := len(crdt.data)
		crdt.expandArray(pos+1, int32(len(crdt.data)+8))
		binary.LittleEndian.PutUint64(crdt.data[oldSize:], math.Float64bits(change))
		crdt.dataStarts[pos] = uint16(oldSize)
	} else {
		value := math.Float64frombits(binary.LittleEndian.Uint64(crdt.data[crdt.dataStarts[pos]:]))
		value += change
		binary.LittleEndian.PutUint64(crdt.data[crdt.dataStarts[pos]:], math.Float64bits(value))
	}
	return ByteArrayFloatIncEffect{Change: change, Pos: pos}
}

func (crdt *ByteArrayCrdt) expandTSOnly(newSize int) {
	newTsId := make([]uint64, newSize)
	copy(newTsId, crdt.dataTsId)
	crdt.dataTsId = newTsId
}

// Expands data, dataStarts and dataTsId. When size < cap(data), data is only resliced.
func (crdt *ByteArrayCrdt) expandArray(nPos, size int32) {
	/*newData, newDataStarts, newDataTsId := make([]byte, size), make([]uint16, nPos), make([]uint64, nPos)
	copy(newData, crdt.data)
	copy(newDataStarts, crdt.dataStarts)
	copy(newDataTsId, crdt.dataTsId)
	crdt.data, crdt.dataStarts, crdt.dataTsId = newData, newDataStarts, newDataTsId
	*/
	newDataStarts, newDataTsId := make([]uint16, nPos), make([]uint64, nPos)
	copy(newDataStarts, crdt.dataStarts)
	copy(newDataTsId, crdt.dataTsId)
	crdt.dataStarts, crdt.dataTsId = newDataStarts, newDataTsId
	if size < int32(cap(crdt.data)) {
		crdt.data = crdt.data[:size]
	} else {
		newData := make([]byte, size)
		copy(newData, crdt.data)
		crdt.data = newData
	}
}

func (crdt *ByteArrayCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *ByteArrayCrdt) Copy() (copyCRDT InversibleCRDT) {
	newData, newDataStarts, newDataTsId := make([]byte, len(crdt.data)), make([]uint16, len(crdt.dataStarts)), make([]uint64, len(crdt.dataTsId))
	copy(newData, crdt.data)
	copy(newDataStarts, crdt.dataStarts)
	copy(newDataTsId, crdt.dataTsId)
	newCRDT := ByteArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), data: newData, dataStarts: newDataStarts}
	if crdt.dataTsId != nil {
		newCRDT.dataTsId = newDataTsId
	}
	return &newCRDT
}

func (crdt *ByteArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Can it be optimized? Maybe similar to stringArrayCrdt?
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *ByteArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *ByteArrayCrdt) undoEffect(effect Effect) { //TODO
	switch typedEffect := (effect).(type) {
	case ByteArraySetDataEffect:
		crdt.data, crdt.dataStarts, crdt.dataTsId = typedEffect.OldData, typedEffect.OldDataStarts, typedEffect.OldDataTsId
	case ByteArraySetValueDirectEffect:
		if typedEffect.Pos < int32(len(crdt.dataStarts)-1) {
			copy(crdt.data[crdt.dataStarts[typedEffect.Pos]:crdt.dataStarts[typedEffect.Pos+1]], typedEffect.OldValue)
		} else {
			copy(crdt.data[crdt.dataStarts[typedEffect.Pos]:], typedEffect.OldValue)
		}
		copy(crdt.data[crdt.dataStarts[typedEffect.Pos]:crdt.dataStarts[typedEffect.Pos+1]], typedEffect.OldValue)
		crdt.dataTsId[typedEffect.Pos] = typedEffect.OldTsId

	case ByteArraySetValueExpandEffect: //It is always the last position of the array. We'll shrink back.
		//Fill data with 0x00 in case later an inc/dec is applied to this position
		start, currLen := crdt.dataStarts[len(crdt.dataStarts)-1], uint16(len(crdt.data))
		for i := start; i < currLen; i++ {
			crdt.data[i] = 0x00
		}
		crdt.data = crdt.data[:start]
		crdt.dataTsId[currLen-1] = 0
		crdt.dataStarts, crdt.dataTsId = crdt.dataStarts[:typedEffect.OldLen], crdt.dataTsId[:typedEffect.OldLen]

	case ByteArraySetValueDiffSizeEffect:

	case ByteArrayIncEffect:
		value := int64(binary.LittleEndian.Uint64(crdt.data[crdt.dataStarts[typedEffect.Pos]:]))
		value -= int64(typedEffect.Change)
		binary.LittleEndian.PutUint64(crdt.data[crdt.dataStarts[typedEffect.Pos]:], uint64(value))
		if int(typedEffect.OldNElems) < len(crdt.dataStarts) {
			crdt.dataStarts, crdt.dataTsId = crdt.dataStarts[:typedEffect.OldNElems], crdt.dataTsId[:typedEffect.OldNElems]
			crdt.data = crdt.data[:len(crdt.data)-8]
		}
	case ByteArrayFloatIncEffect:
		value := math.Float64frombits(binary.LittleEndian.Uint64(crdt.data[crdt.dataStarts[typedEffect.Pos]:]))
		value -= typedEffect.Change
		binary.LittleEndian.PutUint64(crdt.data[crdt.dataStarts[typedEffect.Pos]:], math.Float64bits(value))
		if int(typedEffect.OldNElems) < len(crdt.dataStarts) {
			crdt.dataStarts, crdt.dataTsId = crdt.dataStarts[:typedEffect.OldNElems], crdt.dataTsId[:typedEffect.OldNElems]
			crdt.data = crdt.data[:len(crdt.data)-8]
		}
	}
}

func (crdt *ByteArrayCrdt) notifyRebuiltComplete(currTs clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp ByteArraySetValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetSetValue()
	return ByteArraySetValue{NewValue: byteProto.GetData(), Pos: byteProto.GetIndex()}
}

func (crdtOp ByteArraySetValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_SetValue{SetValue: &proto.ApbByteArraySetValue{Index: pb.Int32(crdtOp.Pos), Data: crdtOp.NewValue}}}}}
}

func (crdtOp ByteArraySetData) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetSetData()
	return ByteArraySetData{Data: byteProto.GetData(), DataStarts: copyUInt32SliceToUInt16Slice(byteProto.GetDataStarts())}
}

func (crdtOp ByteArraySetData) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_SetData{SetData: &proto.ApbByteArraySetData{Data: crdtOp.Data, DataStarts: copyUInt16SliceToUInt32Slice(crdtOp.DataStarts)}}}}}
}

func (crdtOp ByteArraySetDataInitialize) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetSetData()
	return ByteArraySetDataInitialize{Data: byteProto.GetData(), DataStarts: copyUInt32SliceToUInt16Slice(byteProto.GetDataStarts())}
}

func (crdtOp ByteArraySetDataInitialize) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_SetDataInit{SetDataInit: &proto.ApbByteArraySetDataInit{Data: crdtOp.Data, DataStarts: copyUInt16SliceToUInt32Slice(crdtOp.DataStarts)}}}}}
}

func (crdtOp ByteArrayIncrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetIntInc()
	return ByteArrayIncrement{Change: byteProto.GetInc(), Pos: byteProto.GetIndex()}
}

func (crdtOp ByteArrayIncrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_IntInc{IntInc: &proto.ApbByteArrayIntInc{Inc: pb.Int32(crdtOp.Change), Index: pb.Int32(crdtOp.Pos)}}}}}
}

func (crdtOp ByteArrayDecrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetIntInc()
	return ByteArrayDecrement{Change: -byteProto.GetInc(), Pos: byteProto.GetIndex()}
}

func (crdtOp ByteArrayDecrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_IntInc{IntInc: &proto.ApbByteArrayIntInc{Inc: pb.Int32(-crdtOp.Change), Index: pb.Int32(crdtOp.Pos)}}}}}
}

func (crdtOp ByteArrayFloatInc) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetFloatInc()
	return ByteArrayFloatInc{Change: byteProto.GetInc(), Pos: byteProto.GetIndex()}
}

func (crdtOp ByteArrayFloatInc) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_FloatInc{FloatInc: &proto.ApbByteArrayFloatInc{Inc: pb.Float64(crdtOp.Change), Index: pb.Int32(crdtOp.Pos)}}}}}
}

func (crdtOp ByteArrayFloatDec) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	byteProto := protobuf.GetBytearrayop().GetFloatInc()
	return ByteArrayFloatDec{Change: -byteProto.GetInc(), Pos: byteProto.GetIndex()}
}

func (crdtOp ByteArrayFloatDec) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Bytearrayop{Bytearrayop: &proto.ApbByteArrayUpdate{Upd: &proto.ApbByteArrayUpdate_FloatInc{FloatInc: &proto.ApbByteArrayFloatInc{Inc: pb.Float64(-crdtOp.Change), Index: pb.Int32(crdtOp.Pos)}}}}}
}

func (crdtState ByteArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return ByteArrayState(protobuf.GetBytearray().GetValues())
}

func (crdtState ByteArrayState) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Bytearray{Bytearray: &proto.ApbGetArrayByteResp{Values: [][]byte(crdtState)}}}
}

func (crdtState ByteArraySingle) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return ByteArraySingle(protobuf.GetPartread().GetBytearray().GetDataValue().GetValue())
}

func (crdtState ByteArraySingle) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Bytearray{Bytearray: &proto.ApbByteArrayReadResp{DataValue: &proto.ApbBytePosDataResp{Value: []byte(crdtState)}}}}}}
}

func (crdtState ByteArrayString) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return ByteArrayString(protobuf.GetPartread().GetBytearray().GetStringValue().GetValue())
}

func (crdtState ByteArrayString) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Bytearray{Bytearray: &proto.ApbByteArrayReadResp{StringValue: &proto.ApbBytePosStringResp{Value: pb.String(string(crdtState))}}}}}}
}

func (crdtState ByteArrayInt) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return ByteArrayInt(protobuf.GetPartread().GetBytearray().GetIntValue().GetValue())
}

func (crdtState ByteArrayInt) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Bytearray{Bytearray: &proto.ApbByteArrayReadResp{IntValue: &proto.ApbBytePosIntResp{Value: pb.Int64(int64(crdtState))}}}}}}
}

func (crdtState ByteArrayFloat) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return ByteArrayFloat(protobuf.GetPartread().GetBytearray().GetFloatValue().GetValue())
}

func (crdtState ByteArrayFloat) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Bytearray{Bytearray: &proto.ApbByteArrayReadResp{FloatValue: &proto.ApbBytePosFloatResp{Value: pb.Float64(float64(crdtState))}}}}}}
}

func (crdtState ByteArrayAny) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return ByteArrayAny{Value: protobuf.GetPartread().GetBytearray().GetAnyValue().GetValue()}
}

func (crdtState ByteArrayAny) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Bytearray{Bytearray: &proto.ApbByteArrayReadResp{AnyValue: &proto.ApbBytePosAnyResp{Value: []byte(any(crdtState).(string))}}}}}}
}

func (args ByteArraySingleAnyArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArraySingleAnyArguments(protobuf.GetBytearray().GetPos().GetIndex())
}

func (args ByteArraySingleAnyArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Pos: &proto.ApbByteArrayPosRead{Index: pb.Int32(int32(args)), DataType: proto.CA_Type_CA_ANY.Enum()}}}}
}

func (args ByteArraySingleIntArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArraySingleIntArguments(protobuf.GetBytearray().GetPos().GetIndex())
}

func (args ByteArraySingleIntArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Pos: &proto.ApbByteArrayPosRead{Index: pb.Int32(int32(args)), DataType: proto.CA_Type_CA_INT.Enum()}}}}
}

func (args ByteArraySingleFloatArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArraySingleFloatArguments(protobuf.GetBytearray().GetPos().GetIndex())
}

func (args ByteArraySingleFloatArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Pos: &proto.ApbByteArrayPosRead{Index: pb.Int32(int32(args)), DataType: proto.CA_Type_CA_FLOAT.Enum()}}}}
}

func (args ByteArraySingleStringArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArraySingleStringArguments(protobuf.GetBytearray().GetPos().GetIndex())
}

func (args ByteArraySingleStringArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Pos: &proto.ApbByteArrayPosRead{Index: pb.Int32(int32(args)), DataType: proto.CA_Type_CA_STRING.Enum()}}}}
}

func (args ByteArraySingleDataArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArraySingleDataArguments(protobuf.GetBytearray().GetPos().GetIndex())
}

func (args ByteArraySingleDataArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Pos: &proto.ApbByteArrayPosRead{Index: pb.Int32(int32(args)), DataType: proto.CA_Type_CA_DATA.Enum()}}}}
}

func (args ByteArrayExceptArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArrayExceptArguments(protobuf.GetBytearray().GetExcept().GetIndex())
}

func (args ByteArrayExceptArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Except: &proto.ApbByteArrayExceptRead{Index: pb.Int32(int32(args))}}}}
}

func (args ByteArrayRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	byteProto := protobuf.GetBytearray().GetRange()
	return ByteArrayRangeArguments{From: byteProto.GetFrom(), To: byteProto.GetTo()}
}

func (args ByteArrayRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Range: &proto.ApbByteArrayRangeRead{From: &args.From, To: &args.To}}}}
}

func (args ByteArraySubArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return ByteArraySubArguments(protobuf.GetBytearray().GetSub().GetIndexes())
}

func (args ByteArraySubArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Bytearray{Bytearray: &proto.ApbByteArrayPartialRead{Sub: &proto.ApbByteArraySubRead{Indexes: []int32(args)}}}}
}

func (downOp DownstreamByteArraySetValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	byteProto := protobuf.GetByteArrayOp().GetSetValue()
	return DownstreamByteArraySetValue{NewValue: byteProto.GetValue(), Pos: byteProto.GetIndex(), TsId: byteProto.GetTsId()}
}

func (downOp DownstreamByteArraySetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ByteArrayOp{ByteArrayOp: &proto.ProtoByteArrayDownstream{Upd: &proto.ProtoByteArrayDownstream_SetValue{SetValue: &proto.ProtoByteArraySetValue{Index: pb.Int32(downOp.Pos), Value: downOp.NewValue, TsId: pb.Uint64(downOp.TsId)}}}}}
}

func (downOp DownstreamByteArraySetData) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	byteProto := protobuf.GetByteArrayOp().GetSetData()
	return DownstreamByteArraySetData{Data: byteProto.GetData(), DataStarts: copyUInt32SliceToUInt16Slice(byteProto.GetDataStarts()), TsId: byteProto.GetTsId()}
}

func (downOp DownstreamByteArraySetData) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ByteArrayOp{ByteArrayOp: &proto.ProtoByteArrayDownstream{Upd: &proto.ProtoByteArrayDownstream_SetData{SetData: &proto.ProtoByteArraySetData{Data: downOp.Data, DataStarts: copyUInt16SliceToUInt32Slice(downOp.DataStarts), TsId: pb.Uint64(downOp.TsId)}}}}}
}

func (downOp ByteArrayIncrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	byteProto := protobuf.GetByteArrayOp().GetIntInc()
	return ByteArrayIncrement{Pos: byteProto.GetIndex(), Change: byteProto.GetInc()}
}

func (downOp ByteArrayIncrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ByteArrayOp{ByteArrayOp: &proto.ProtoByteArrayDownstream{Upd: &proto.ProtoByteArrayDownstream_IntInc{IntInc: &proto.ProtoByteArrayIntInc{Index: pb.Int32(downOp.Pos), Inc: pb.Int32(downOp.Change)}}}}}
}

func (downOp ByteArrayFloatInc) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	byteProto := protobuf.GetByteArrayOp().GetFloatInc()
	return ByteArrayFloatInc{Pos: byteProto.GetIndex(), Change: byteProto.GetInc()}
}

func (downOp ByteArrayFloatInc) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ByteArrayOp{ByteArrayOp: &proto.ProtoByteArrayDownstream{Upd: &proto.ProtoByteArrayDownstream_FloatInc{FloatInc: &proto.ProtoByteArrayFloatInc{Index: pb.Int32(downOp.Pos), Inc: pb.Float64(downOp.Change)}}}}}
}

func (downOp ByteArraySetDataInitialize) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	byteProto := protobuf.GetByteArrayOp().GetSetArrayInit()
	return ByteArraySetDataInitialize{Data: byteProto.GetData(), DataStarts: copyUInt32SliceToUInt16Slice(byteProto.GetDataStarts())}
}

func (downOp ByteArraySetDataInitialize) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_ByteArrayOp{ByteArrayOp: &proto.ProtoByteArrayDownstream{Upd: &proto.ProtoByteArrayDownstream_SetArrayInit{SetArrayInit: &proto.ProtoByteArraySetDataInit{Data: downOp.Data, DataStarts: copyUInt16SliceToUInt32Slice(downOp.DataStarts)}}}}}
}

func (crdt *ByteArrayCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	copyData, copyDataStarts, copyDataTsId := make([]byte, len(crdt.data)), make([]uint32, len(crdt.dataStarts)), make([]uint64, len(crdt.dataTsId))
	copy(copyData, crdt.data)
	copy(copyDataTsId, crdt.dataTsId)
	for i, v := range crdt.dataStarts {
		copyDataStarts[i] = uint32(v)
	}
	return &proto.ProtoState{State: &proto.ProtoState_ByteArray{ByteArray: &proto.ProtoByteArrayState{
		Data: copyData, DataStarts: copyDataStarts, TsId: copyDataTsId}}}
}

func (crdt *ByteArrayCrdt) FromProtoState(protobuf *proto.ProtoState, ts clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	pbCounter := protobuf.GetBcounter()
	perms, decs, permsPb, decsPb := make(map[uint16]int32), make(map[uint16]int32), pbCounter.GetPermissions(), pbCounter.GetDecs()
	for key, value := range permsPb {
		perms[uint16(key)] = value
	}
	for key, value := range decsPb {
		decs[uint16(key)] = value
	}
	return (&BoundedCounterCrdt{permissions: perms, decs: decs, limit: pbCounter.GetLimit(), value: pbCounter.GetValue()}).
		initializeFromSnapshot(ts, replicaID)
}

func (crdt *ByteArrayCrdt) GetCRDT() CRDT { return crdt }

func copyUInt32SliceToUInt16Slice(src []uint32) (dst []uint16) {
	dst = make([]uint16, len(src))
	for i, val := range src {
		dst[i] = uint16(val)
	}
	return dst
}

func copyUInt16SliceToUInt32Slice(src []uint16) (dst []uint32) {
	dst = make([]uint32, len(src))
	for i, val := range src {
		dst[i] = uint32(val)
	}
	return dst
}

//Old downstream: delete later.
/*
case DownstreamByteArraySetValue:
		if crdt.dataTsId[typedArgs.Pos] >= typedArgs.TsId { //No-op
			return &effectValue
		}
		//Fast path: the new value has the same size as the old one.
		if typedArgs.Pos < int32(len(crdt.dataStarts)-1) && len(typedArgs.NewValue) == int(crdt.dataStarts[typedArgs.Pos+1]-crdt.dataStarts[typedArgs.Pos]) {
			copy(crdt.data[crdt.dataStarts[typedArgs.Pos]:crdt.dataStarts[typedArgs.Pos+1]], typedArgs.NewValue)
			crdt.dataTsId[typedArgs.Pos] = typedArgs.TsId
			return //TODO: Effect
		}
		//Also fast path, but at the end of the array.
		if typedArgs.Pos == int32(len(crdt.dataStarts)-1) && len(typedArgs.NewValue) == int(len(crdt.data)-int(crdt.dataStarts[typedArgs.Pos])) {
			copy(crdt.data[crdt.dataStarts[typedArgs.Pos]:], typedArgs.NewValue)
			crdt.dataTsId[typedArgs.Pos] = typedArgs.TsId
			return //TODO: Effect
		}
		if typedArgs.Pos >= int32(len(crdt.dataStarts)) { //Expand array.
			oldSize := len(crdt.data)
			crdt.expandArray(typedArgs.Pos+1, int32(len(crdt.data)+len(typedArgs.NewValue)))
			copy(crdt.data[oldSize:], typedArgs.NewValue)
			//Must fill all new empty positions of dataStarts with oldSize.
			for i := len(crdt.dataStarts); i < int(typedArgs.Pos); i++ {
				crdt.dataStarts[i] = uint16(oldSize)
			}
			crdt.dataTsId[typedArgs.Pos] = typedArgs.TsId
			return //TODO: Effect
		}

		//Pos is somewhere in the existing array, but size is different. It means we will need to "make space".
		//Two options:
		//1 - Always make new slice with appropriate size. Memory-efficient but can lead to many memory allocations/slow.
		//2 - Only make a new slice if it cannot fit. Otherwise, move data around and reslice.
		//In this case, we can re-use the slice if we write a smaller item, or if we write a larger item but there is enough space.
		//If we do 2, we can create an appropriate buffer size during GC().
		//We're going with 2.
		//For simplicity, always move the data in front of pos to the right place. Otherwise, later it may be hard to detect where is the free space, and make it confusing to deal with.
		crdt.dataTsId[typedArgs.Pos] = typedArgs.TsId //Update TsId first, as we already know that typedArgs.TsId is higher.
		start := crdt.dataStarts[typedArgs.Pos]
		if typedArgs.Pos == int32(len(crdt.dataStarts)-1) { //Last item
			size := uint16(len(crdt.data)) - start
			if size > uint16(len(typedArgs.NewValue)) { //Can fit in existing space
				copy(crdt.data[start:], typedArgs.NewValue)
				crdt.data = crdt.data[:len(crdt.data)-int(size-uint16(len(typedArgs.NewValue)))]
			} else { //> (== was already processed before). Have to expand.
				extraNeeded := uint16(len(typedArgs.NewValue)) - size
				if len(crdt.data)+int(extraNeeded) <= cap(crdt.data) { //Can fit by reslicing
					crdt.data = crdt.data[:len(crdt.data)+int(extraNeeded)]
					copy(crdt.data[start:], typedArgs.NewValue) //Copy the new value. Data starts[Pos] remains unchanged.
				} else {
					newData := make([]byte, len(crdt.data)+int(extraNeeded))
					copy(newData, crdt.data[:start])
					copy(newData[start:], typedArgs.NewValue)
					crdt.data = newData
				}
			}
		}

		//Pos is not the last item.
		start, end := crdt.dataStarts[typedArgs.Pos], crdt.dataStarts[typedArgs.Pos+1]
		currSize := end - start
		if currSize > uint16(len(typedArgs.NewValue)) { //Can fit in existing space
			extraSpace := currSize - uint16(len(typedArgs.NewValue))
			copy(crdt.data[start:], typedArgs.NewValue)
			crdt.dataStarts[typedArgs.Pos+1] -= extraSpace
			for i := int(typedArgs.Pos + 1); i < len(crdt.dataStarts)-1; i++ {
				copy(crdt.data[crdt.dataStarts[i]:], crdt.data[crdt.dataStarts[i]+extraSpace:crdt.dataStarts[i+1]])
				crdt.dataStarts[i+1] -= extraSpace
			}
			copy(crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]:], crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]+extraSpace:])
			crdt.data = crdt.data[:len(crdt.data)-int(extraSpace)]
		} else { //New item's size is bigger. Let's check if we can fit it by reslicing.
			extraNeeded := uint16(len(typedArgs.NewValue)) - currSize
			if len(crdt.data)+int(extraNeeded) <= cap(crdt.data) { //Can fit by reslicing
				crdt.data = crdt.data[:len(crdt.data)+int(extraNeeded)]
				//Copy from the end until Pos.
				copy(crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]+extraNeeded:], crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]:])
				crdt.dataStarts[len(crdt.dataStarts)-1] += extraNeeded
				for i := len(crdt.dataStarts) - 2; i > int(typedArgs.Pos); i-- {
					copy(crdt.data[crdt.dataStarts[i]+extraNeeded:], crdt.data[crdt.dataStarts[i]:crdt.dataStarts[i+1]-extraNeeded])
					crdt.dataStarts[i] += extraNeeded
				}
				copy(crdt.data[start:], typedArgs.NewValue) //Copy the new value. Data starts[Pos] remains unchanged.
			} else { //Have to make a new slice
				newData := make([]byte, len(crdt.data)+int(extraNeeded))
				copy(newData, crdt.data[:start])
				copy(newData[start:], typedArgs.NewValue)
				copy(newData[start+uint16(len(typedArgs.NewValue)):], crdt.data[crdt.dataStarts[typedArgs.Pos+1]:])
				for i := int(typedArgs.Pos + 1); i < len(crdt.dataStarts); i++ { //Fix starting positions
					crdt.dataStarts[i] += extraNeeded
				}
				crdt.data = newData
			}
		}

		case DownstreamByteArraySetData:
		//Figure out the new data size (iterate both TsId, calculate new starts and decide where to take from)
		//Due to the possibility of different sizes for each "data", we have to always create a new byte slice.
		//Sadly nothing we can reuse :( (unless the new slice wins every TsId)
		takeFromUpd := make([]bool, len(typedArgs.DataStarts)) //If true, we use the value from typedArgs
		var newDataStarts []uint16
		currStart := uint16(0)
		allTsHigher, allTsLower := true, true
		if len(typedArgs.DataStarts) > len(crdt.dataStarts) {
			newDataStarts = make([]uint16, len(typedArgs.DataStarts))
			crdt.expandTSOnly(len(typedArgs.DataStarts))
		} else {
			newDataStarts = make([]uint16, len(crdt.dataStarts))
		}
		for i := 0; i < len(crdt.dataStarts)-1; i++ { //Calculate
			if crdt.dataTsId[i] < typedArgs.TsId { //Use typedArgs
				takeFromUpd[i], newDataStarts[i+1] = true, currStart+(typedArgs.DataStarts[i+1]-typedArgs.DataStarts[i])
				crdt.dataTsId[i], allTsLower = typedArgs.TsId, false
			} else {
				takeFromUpd[i], newDataStarts[i] = false, currStart+(crdt.dataStarts[i+1]-crdt.dataStarts[i])
				allTsHigher = false
			}
		}
		if allTsLower { //No-op.
			return &effectValue
		}
		if allTsHigher && len(typedArgs.DataStarts) >= len(crdt.dataStarts) { //Can just use the update slices
			crdt.data, crdt.dataStarts = typedArgs.Data, typedArgs.DataStarts
			return //TODO: Effect
		} else { //Have to make new slices
			var size uint16
			if takeFromUpd[len(takeFromUpd)-1] { //Find size of the last element, and add it to the last start.
				size = newDataStarts[len(newDataStarts)-1] + typedArgs.DataStarts[len(typedArgs.DataStarts)-1] - typedArgs.DataStarts[len(typedArgs.DataStarts)-2]
			} else {
				size = newDataStarts[len(newDataStarts)-1] + crdt.dataStarts[len(crdt.dataStarts)-1] - crdt.dataStarts[len(crdt.dataStarts)-2]
			}
			newData := make([]byte, size)
			for i, start := range newDataStarts[:len(newDataStarts)-1] { //Process last position separately
				if takeFromUpd[i] {
					copy(newData[start:], typedArgs.Data[crdt.dataStarts[i]:crdt.dataStarts[i+1]])
				} else {
					copy(newData[start:], crdt.data[crdt.dataStarts[i]:crdt.dataStarts[i+1]])
				}
			}
			currStart = newDataStarts[len(newDataStarts)-1]
			if takeFromUpd[len(takeFromUpd)-1] {
				copy(newData[currStart:], typedArgs.Data[typedArgs.DataStarts[len(typedArgs.DataStarts)-1]:])
			} else {
				copy(newData[currStart:], crdt.data[crdt.dataStarts[len(crdt.dataStarts)-1]:])
			}
		}
*/
