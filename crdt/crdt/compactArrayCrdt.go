package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
	"unsafe"

	tools "github.com/AndreRijo/go-tools/src/tools"

	pb "google.golang.org/protobuf/proto"
)

// This CRDT is intended when data needs to be represented compactly, sacrificing performance for memory.
// As such, this CRDT also avoids memory allocation whenever possible - namely, CompactArraySetArray will replace the existing array if its size is >= than the one in the CRDT.
// This CRDT supports both counters and registers as the value for each data position
type CompactArrayCrdt struct {
	CRDTVM
	data     []any    //This is better (memory-wise) than slice of []byte, and same as []string. Even when accounting for memory used by each position
	dataTsId []uint64 //The highest 48 bits correspond to the lowest 48 bits of a 64-bit timestamp; the lowest 16 bits are used for replicaID.
	//We will access replicaID from the shared variable.
}

type CompactArrayState []any

// States
type CompactArraySingleAny struct{ Value any }
type CompactArraySingleString string
type CompactArraySingleCounter int64
type CompactArraySingleFloat float64
type CompactArraySingleData []byte

//Reads

// Position
type CompactArraySingleAnyArguments int32
type CompactArraySingleIntArguments int32
type CompactArraySingleFloatArguments int32
type CompactArraySingleStringArguments int32
type CompactArraySingleDataArguments int32
type CompactArrayExceptArguments int32

// Positions
type CompactArrayRangeArguments struct{ From, To int32 }
type CompactArraySubArguments []int32

// Updates
type CompactArrayUpd interface { //Helps identify what array size is required to apply this update.
	UpdateArguments
	GetMinSize() int
	IsMultiPos() bool
}

type CompactArraySetSize int32
type CompactArraySetArray []any                        //NOTE: This slice will be used directly in the CRDT, unless its size is < than the one in the CRDT.
type CompactArrayIncrement struct{ Change, Pos int32 } //Stored as a single int64.
type CompactArrayDecrement struct{ Change, Pos int32 }
type CompactArrayFloatInc struct {
	Change float64
	Pos    int32
}
type CompactArrayFloatDec struct {
	Change float64
	Pos    int32
}

/*
	type CompactArray64Increment struct {
		Change int64
		Pos int32
	}

	type CompactArray64Decrement struct {
		Change int64
		Pos int32
	}
*/
type CompactArraySetValue struct {
	Value any
	Pos   int32
}

// Downstreams. Counter operations use the update operation itself as downstream.
type DownstreamCompactArraySetArray struct {
	Values []any
	TsId   uint64
}

type DownstreamCompactArraySetValue struct {
	Value any
	Pos   int32
	TsId  uint64
}

//Effects
//Note: For storage efficiency reasons, we do not roll back the size of the array unless it was set with SetSize.

type CompactArraySetValueEffect DownstreamCompactArraySetValue
type CompactArraySetArrayEffect struct {
	Values []any
	TsId   []uint64
}
type CompactArrayCounterEffect CompactArrayIncrement //Used both for incs and decs.
type CompactArrayFloatEffect CompactArrayFloatInc    //Used both for incs and decs.
type CompactArraySetSizeEffect CompactArraySetSize

//

func (crdt *CompactArrayCrdt) GetCRDTType() proto.CRDTType     { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArraySetSize) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArraySetArray) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArrayIncrement) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArrayDecrement) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArrayFloatInc) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArrayFloatDec) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_COMPACT }
func (args CompactArraySetValue) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_COMPACT }
func (crdt *CompactArrayCrdt) GetDATAType() proto.DATAType     { return proto.DATAType_DEFAULT }
func (args CompactArraySetSize) GetDATAType() proto.DATAType   { return proto.DATAType_DEFAULT }
func (args CompactArraySetArray) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (args CompactArrayIncrement) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args CompactArrayDecrement) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args CompactArrayFloatInc) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (args CompactArrayFloatDec) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (args CompactArraySetValue) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }

func (args DownstreamCompactArraySetArray) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args DownstreamCompactArraySetValue) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args DownstreamCompactArraySetArray) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args DownstreamCompactArraySetValue) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}

func (args DownstreamCompactArraySetArray) MustReplicate() bool { return true }
func (args DownstreamCompactArraySetValue) MustReplicate() bool { return true }
func (args CompactArraySetSize) MustReplicate() bool            { return true }
func (args CompactArrayIncrement) MustReplicate() bool          { return true }
func (args CompactArrayDecrement) MustReplicate() bool          { return true }
func (args CompactArrayFloatInc) MustReplicate() bool           { return true }
func (args CompactArrayFloatDec) MustReplicate() bool           { return true }
func (args CompactArraySetArray) GetMinSize() int               { return len(args) }
func (args CompactArraySetValue) GetMinSize() int               { return int(args.Pos + 1) }
func (args CompactArrayIncrement) GetMinSize() int              { return int(args.Pos + 1) }
func (args CompactArrayDecrement) GetMinSize() int              { return int(args.Pos + 1) }
func (args CompactArrayFloatInc) GetMinSize() int               { return int(args.Pos + 1) }
func (args CompactArrayFloatDec) GetMinSize() int               { return int(args.Pos + 1) }
func (args CompactArraySetSize) GetMinSize() int                { return int(args) }
func (args CompactArraySetArray) IsMultiPos() bool              { return true }
func (args CompactArraySetValue) IsMultiPos() bool              { return false }
func (args CompactArrayIncrement) IsMultiPos() bool             { return false }
func (args CompactArrayDecrement) IsMultiPos() bool             { return false }
func (args CompactArrayFloatInc) IsMultiPos() bool              { return false }
func (args CompactArrayFloatDec) IsMultiPos() bool              { return false }
func (args CompactArraySetSize) IsMultiPos() bool               { return false }

func (state CompactArrayState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_COMPACT }
func (state CompactArrayState) GetREADType() proto.READType { return proto.READType_FULL }
func (state CompactArrayState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state CompactArraySingleString) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (state CompactArraySingleString) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (state CompactArraySingleString) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state CompactArraySingleCounter) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (state CompactArraySingleCounter) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (state CompactArraySingleCounter) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state CompactArraySingleFloat) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (state CompactArraySingleFloat) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (state CompactArraySingleFloat) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state CompactArraySingleData) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_COMPACT }
func (state CompactArraySingleData) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (state CompactArraySingleData) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state CompactArraySingleAny) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_COMPACT }
func (state CompactArraySingleAny) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (state CompactArraySingleAny) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

func (args CompactArraySingleAnyArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArraySingleIntArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArraySingleFloatArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArraySingleStringArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArraySingleDataArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArrayExceptArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArrayRangeArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArraySubArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_COMPACT
}
func (args CompactArraySingleAnyArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (args CompactArraySingleIntArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (args CompactArraySingleFloatArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (args CompactArraySingleStringArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (args CompactArraySingleDataArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_POS
}
func (args CompactArrayExceptArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_EXCEPT
}
func (args CompactArrayRangeArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_RANGE
}
func (args CompactArraySubArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_COMPACT_SUB
}
func (args CompactArraySingleAnyArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArraySingleIntArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArraySingleFloatArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArraySingleStringArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArraySingleDataArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArrayExceptArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArrayRangeArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArraySubArguments) GetDATAType() proto.DATAType {
	return proto.DATAType_DEFAULT
}
func (args CompactArraySingleAnyArguments) HasInnerReads() bool    { return false }
func (args CompactArraySingleIntArguments) HasInnerReads() bool    { return false }
func (args CompactArraySingleFloatArguments) HasInnerReads() bool  { return false }
func (args CompactArraySingleStringArguments) HasInnerReads() bool { return false }
func (args CompactArraySingleDataArguments) HasInnerReads() bool   { return false }
func (args CompactArrayExceptArguments) HasInnerReads() bool       { return false }
func (args CompactArrayRangeArguments) HasInnerReads() bool        { return false }
func (args CompactArraySubArguments) HasInnerReads() bool          { return false }
func (args CompactArraySingleAnyArguments) HasVariables() bool     { return false }
func (args CompactArraySingleIntArguments) HasVariables() bool     { return false }
func (args CompactArraySingleFloatArguments) HasVariables() bool   { return false }
func (args CompactArraySingleStringArguments) HasVariables() bool  { return false }
func (args CompactArraySingleDataArguments) HasVariables() bool    { return false }
func (args CompactArrayExceptArguments) HasVariables() bool        { return false }
func (args CompactArrayRangeArguments) HasVariables() bool         { return false }
func (args CompactArraySubArguments) HasVariables() bool           { return false }

func (crdt *CompactArrayCrdt) Initialize(startTs clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	crdt = &CompactArrayCrdt{}
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *CompactArrayCrdt) initializeFromSnapshot(startTs clocksi.Timestamp, replicaID uint16) (sameCRDT *CompactArrayCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *CompactArrayCrdt) IsBigCRDT() bool {
	return len(crdt.data) >= 500
}

// For reads it is more OK to do copies of data as needed.
func (crdt *CompactArrayCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case CompactArraySingleAnyArguments:
		return crdt.getPosAny(updsNotYetApplied, int32(typedArgs))
	case CompactArraySingleStringArguments:
		return crdt.getPosString(updsNotYetApplied, int32(typedArgs))
	case CompactArraySingleIntArguments:
		return crdt.getPosInt(updsNotYetApplied, int32(typedArgs))
	case CompactArraySingleFloatArguments:
		return crdt.getPosFloat(updsNotYetApplied, int32(typedArgs))
	case CompactArraySingleDataArguments:
		return crdt.getPosData(updsNotYetApplied, int32(typedArgs))
	case CompactArrayExceptArguments:
		return crdt.getExcept(updsNotYetApplied, int32(typedArgs))
	case CompactArrayRangeArguments:
		return crdt.getRange(updsNotYetApplied, typedArgs.From, typedArgs.To)
	case CompactArraySubArguments:
		return crdt.getSub(updsNotYetApplied, []int32(typedArgs))
	default:
		fmt.Printf("[CompactArrayCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *CompactArrayCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	tmpCopy := copySlice(crdt.data)
	if len(updsNotYetApplied) == 0 {
		return CompactArrayState(tmpCopy)
	}
	for _, upd := range updsNotYetApplied {
		compactUpd, ok := upd.(CompactArrayUpd)
		if !ok {
			continue
		}
		if compactUpd.GetMinSize() > len(crdt.data) {
			tmpCopy = copySliceWithSize(tmpCopy, int32(compactUpd.GetMinSize()))
		}
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			tmpCopy[typedUpd.Pos] = typedUpd.Value
		case CompactArrayIncrement:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(int64) + int64(typedUpd.Change)
		case CompactArrayDecrement:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(int64) - int64(typedUpd.Change)
		case CompactArrayFloatInc:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(float64) + float64(typedUpd.Change)
		case CompactArrayFloatDec:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(float64) + float64(typedUpd.Change)
		case CompactArraySetArray:
			copy(tmpCopy, typedUpd)
		case CompactArraySetSize: //Ignore, already resized

		}
	}
	return CompactArrayState(tmpCopy)
}

func (crdt *CompactArrayCrdt) getPosAny(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CompactArraySingleAny{Value: crdt.data[pos]}
	}
	var value any
	if pos < int32(len(crdt.data)) {
		value = crdt.data[pos]
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos == pos {
				value = typedUpd.Value
			}
		case CompactArrayIncrement:
			if typedUpd.Pos == pos {
				value = (value.(int64)) + int64(typedUpd.Change)
			}
		case CompactArrayDecrement:
			if typedUpd.Pos == pos {
				value = (value.(int64)) - int64(typedUpd.Change)
			}
		case CompactArrayFloatInc:
			if typedUpd.Pos == pos {
				value = (value.(float64)) + float64(typedUpd.Change)
			}
		case CompactArrayFloatDec:
			if typedUpd.Pos == pos {
				value = (value.(float64)) - float64(typedUpd.Change)
			}
		case CompactArraySetArray:
			if int32(len(typedUpd)) > pos {
				value = typedUpd[pos]
			}
		}
	}
	return CompactArraySingleAny{Value: value}
}

func (crdt *CompactArrayCrdt) getPosString(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CompactArraySingleString(crdt.data[pos].(string))
	}
	var value string
	if pos < int32(len(crdt.data)) {
		value = crdt.data[pos].(string)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos == pos {
				value = typedUpd.Value.(string)
			}
		case CompactArraySetArray:
			if int32(len(typedUpd)) > pos {
				value = typedUpd[pos].(string)
			}
		}
	}
	return CompactArraySingleString(value)
}

func (crdt *CompactArrayCrdt) getPosInt(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CompactArraySingleCounter(crdt.data[pos].(int64))
	}
	var value int64
	if pos < int32(len(crdt.data)) {
		value = crdt.data[pos].(int64)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos == pos {
				value = typedUpd.Value.(int64)
			}
		case CompactArrayIncrement:
			if typedUpd.Pos == pos {
				value += int64(typedUpd.Change)
			}
		case CompactArrayDecrement:
			if typedUpd.Pos == pos {
				value -= int64(typedUpd.Change)
			}
		case CompactArraySetArray:
			if int32(len(typedUpd)) > pos {
				value = typedUpd[pos].(int64)
			}
		}
	}
	return CompactArraySingleCounter(value)
}

func (crdt *CompactArrayCrdt) getPosFloat(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CompactArraySingleFloat(crdt.data[pos].(float64))
	}
	var value float64
	if pos < int32(len(crdt.data)) {
		value = crdt.data[pos].(float64)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos == pos {
				value = typedUpd.Value.(float64)
			}
		case CompactArrayFloatInc:
			if typedUpd.Pos == pos {
				value += float64(typedUpd.Change)
			}
		case CompactArrayFloatDec:
			if typedUpd.Pos == pos {
				value -= float64(typedUpd.Change)
			}
		case CompactArraySetArray:
			if int32(len(typedUpd)) > pos {
				value = typedUpd[pos].(float64)
			}
		}
	}
	return CompactArraySingleFloat(value)
}

func (crdt *CompactArrayCrdt) getPosData(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CompactArraySingleData(crdt.data[pos].([]byte))
	}
	var value []byte
	if pos < int32(len(crdt.data)) {
		value = crdt.data[pos].([]byte)
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos == pos {
				value = typedUpd.Value.([]byte)
			}
		case CompactArraySetArray:
			if int32(len(typedUpd)) > pos {
				value = typedUpd[pos].([]byte)
			}
		}
	}
	return CompactArraySingleData(value)
}

func (crdt *CompactArrayCrdt) getExcept(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	var result []any
	if int(pos) >= len(crdt.data) {
		result = copySlice(crdt.data)
	} else {
		result = make([]any, len(crdt.data)-1)
		copy(result, crdt.data[:pos])
		copy(result[pos:], crdt.data[pos+1:])
	}
	if len(updsNotYetApplied) == 0 {
		return CompactArrayState(result)
	}

	for _, upd := range updsNotYetApplied {
		compactUpd, ok := upd.(CompactArrayUpd)
		if !ok {
			continue
		}
		if len(result) >= int(pos) && compactUpd.GetMinSize()-1 > len(result) { //We already skipped pos
			result = copySliceWithSize(result, int32(compactUpd.GetMinSize()-1))
		} else if compactUpd.GetMinSize() > len(result) && compactUpd.GetMinSize() < int(pos) { //Still not reaching pos
			result = copySliceWithSize(result, int32(compactUpd.GetMinSize()))
		} else if compactUpd.GetMinSize() > len(result) && compactUpd.GetMinSize() >= int(pos) { //Now we will skip pos
			result = copySliceWithSize(result, int32(compactUpd.GetMinSize()-1))
		}

		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos < pos {
				result[typedUpd.Pos] = typedUpd.Value
			} else if typedUpd.Pos > pos {
				result[typedUpd.Pos-1] = typedUpd.Value
			}
		case CompactArrayIncrement:
			if typedUpd.Pos < pos {
				result[typedUpd.Pos] = result[typedUpd.Pos].(int64) + int64(typedUpd.Change)
			} else if typedUpd.Pos > pos {
				result[typedUpd.Pos-1] = result[typedUpd.Pos-1].(int64) + int64(typedUpd.Change)
			}
		case CompactArrayDecrement:
			if typedUpd.Pos < pos {
				result[typedUpd.Pos] = result[typedUpd.Pos].(int64) - int64(typedUpd.Change)
			} else if typedUpd.Pos > pos {
				result[typedUpd.Pos-1] = result[typedUpd.Pos-1].(int64) - int64(typedUpd.Change)
			}
		case CompactArrayFloatInc:
			if typedUpd.Pos < pos {
				result[typedUpd.Pos] = result[typedUpd.Pos].(float64) + float64(typedUpd.Change)
			} else if typedUpd.Pos > pos {
				result[typedUpd.Pos-1] = result[typedUpd.Pos-1].(float64) + float64(typedUpd.Change)
			}
		case CompactArrayFloatDec:
			if typedUpd.Pos < pos {
				result[typedUpd.Pos] = result[typedUpd.Pos].(float64) - float64(typedUpd.Change)
			} else if typedUpd.Pos > pos {
				result[typedUpd.Pos-1] = result[typedUpd.Pos-1].(float64) - float64(typedUpd.Change)
			}
		case CompactArraySetArray:
			if len(typedUpd) < int(pos) { //Direct copy
				copy(result, typedUpd)
			} else {
				copy(result, typedUpd[:pos])
				copy(result[pos:], typedUpd[pos+1:])
			}
		}
	}
	return CompactArrayState(result)
}

func (crdt *CompactArrayCrdt) getRange(updsNotYetApplied []UpdateArguments, from, to int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CompactArrayState(copySlice(getRangeOfSlice(crdt.data, int(from), int(to))))
	}

	result := make([]any, to-from) //We make an entire range slice, optimistically
	copy(result, getRangeOfSlice(crdt.data, int(from), int(to)))
	currMax := int32(len(crdt.data))

	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			if typedUpd.Pos >= from && typedUpd.Pos < to {
				result[typedUpd.Pos-from] = typedUpd.Value
				currMax = tools.Max(currMax, typedUpd.Pos+1)
			}
		case CompactArrayIncrement:
			if typedUpd.Pos >= from && typedUpd.Pos < to {
				result[typedUpd.Pos-from] = result[typedUpd.Pos-from].(int64) + int64(typedUpd.Change)
				currMax = tools.Max(currMax, typedUpd.Pos+1)
			}
		case CompactArrayDecrement:
			if typedUpd.Pos >= from && typedUpd.Pos < to {
				result[typedUpd.Pos-from] = result[typedUpd.Pos-from].(int64) - int64(typedUpd.Change)
				currMax = tools.Max(currMax, typedUpd.Pos+1)
			}
		case CompactArrayFloatInc:
			if typedUpd.Pos >= from && typedUpd.Pos < to {
				result[typedUpd.Pos-from] = result[typedUpd.Pos-from].(float64) + float64(typedUpd.Change)
				currMax = tools.Max(currMax, typedUpd.Pos+1)
			}
		case CompactArrayFloatDec:
			if typedUpd.Pos >= from && typedUpd.Pos < to {
				result[typedUpd.Pos-from] = result[typedUpd.Pos-from].(float64) + float64(typedUpd.Change)
				currMax = tools.Max(currMax, typedUpd.Pos+1)
			}
		case CompactArraySetArray:
			for i := from; i <= to && i < int32(len(typedUpd)); i++ {
				result[i-from] = typedUpd[i]
			}
			currMax = tools.Max(currMax, int32(len(typedUpd)))
		}
	}

	currMax -= from
	result = result[:currMax] //if currMax == to, it does nothing.
	return CompactArrayState(result)
}

func (crdt *CompactArrayCrdt) getSub(updsNotYetApplied []UpdateArguments, positions []int32) (state State) {
	result := make([]any, len(positions))

	if len(updsNotYetApplied) == 0 {
		lenInt32 := int32(len(crdt.data))
		for i, pos := range positions {
			if pos < lenInt32 {
				result[i] = crdt.data[pos]
			} else {
				result[i] = nil //Out of bounds
			}
		}
		return CompactArrayState(result)
	}

	tmpCopy := copySlice(crdt.data)
	for _, upd := range updsNotYetApplied {
		compactUpd, ok := upd.(CompactArrayUpd)
		if !ok {
			continue
		}
		if compactUpd.GetMinSize() > len(crdt.data) {
			tmpCopy = copySliceWithSize(tmpCopy, int32(compactUpd.GetMinSize()))
		}
		switch typedUpd := upd.(type) {
		case CompactArraySetValue:
			tmpCopy[typedUpd.Pos] = typedUpd.Value
		case CompactArrayIncrement:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(int64) + int64(typedUpd.Change)
		case CompactArrayDecrement:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(int64) - int64(typedUpd.Change)
		case CompactArrayFloatInc:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(float64) + float64(typedUpd.Change)
		case CompactArrayFloatDec:
			tmpCopy[typedUpd.Pos] = tmpCopy[typedUpd.Pos].(float64) + float64(typedUpd.Change)
		case CompactArraySetArray:
			copy(tmpCopy, typedUpd)
		}
	}

	lenInt32 := int32(len(result))
	for i, pos := range positions {
		if pos < lenInt32 {
			result[i] = tmpCopy[pos]
		} else {
			result[i] = nil
		}
	}
	return CompactArrayState(result)
}

func (crdt *CompactArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch typedArgs := args.(type) {

	case CompactArrayIncrement, CompactArrayDecrement, CompactArrayFloatInc, CompactArrayFloatDec, CompactArraySetSize:
		return typedArgs.(DownstreamArguments)

	case CompactArraySetValue:
		tsId := generate64BitTsAndId(int64(shared.ReplicaID))
		return DownstreamCompactArraySetValue{Value: typedArgs.Value, Pos: typedArgs.Pos, TsId: tsId}

	case CompactArraySetArray:
		tsId := generate64BitTsAndId(int64(shared.ReplicaID))
		return DownstreamCompactArraySetArray{Values: []any(typedArgs), TsId: tsId}

	case MultiUpd:
		multiDowns := make(MultiUpd, len(typedArgs))
		for i, innerUpd := range typedArgs {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	default:
		fmt.Printf("[CompactArrayCrdt][Update]Unknown update type: %v (%T)\n", args, args)
	}

	return nil
}

func (crdt *CompactArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
	}
	effect := crdt.applyDownstream(downstreamArgs)
	crdt.addToHistory(updTs, downstreamArgs, effect) //Necessary for inversibleCrdt
	return nil
}

func (crdt *CompactArrayCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect Effect) {
	effect = NoEffect{}
	compactArrayUpd, ok := downstreamArgs.(CompactArrayUpd)
	if !ok {
		fmt.Printf("[CompactArrayCrdt][Downstream]Unsupported downstream type: %v (%T)\n", downstreamArgs, downstreamArgs)
		return
	}
	if compactArrayUpd.GetMinSize() > len(crdt.data) && !compactArrayUpd.IsMultiPos() {
		crdt.expandArray(int32(compactArrayUpd.GetMinSize()))
	}

	switch typedUpd := downstreamArgs.(type) {

	case DownstreamCompactArraySetValue:
		if crdt.dataTsId[typedUpd.Pos] > typedUpd.TsId { //Cannot apply
			return
		}
		effect = CompactArraySetValueEffect{Value: crdt.data[typedUpd.Pos], Pos: typedUpd.Pos, TsId: crdt.dataTsId[typedUpd.Pos]}
		crdt.data[typedUpd.Pos], crdt.dataTsId[typedUpd.Pos] = typedUpd.Value, typedUpd.TsId
	case CompactArrayIncrement:
		effect = CompactArrayCounterEffect(typedUpd)
		crdt.data[typedUpd.Pos] = (crdt.data[typedUpd.Pos].(int64)) + int64(typedUpd.Change)
	case CompactArrayDecrement:
		effect = CompactArrayCounterEffect{Change: -typedUpd.Change, Pos: typedUpd.Pos}
		crdt.data[typedUpd.Pos] = (crdt.data[typedUpd.Pos].(int64)) - int64(typedUpd.Change)
	case CompactArrayFloatInc:
		effect = CompactArrayFloatEffect(typedUpd)
		crdt.data[typedUpd.Pos] = (crdt.data[typedUpd.Pos].(float64)) + float64(typedUpd.Change)
	case CompactArrayFloatDec:
		effect = CompactArrayFloatEffect{Change: -typedUpd.Change, Pos: typedUpd.Pos}
		crdt.data[typedUpd.Pos] = (crdt.data[typedUpd.Pos].(float64)) - float64(typedUpd.Change)
	case DownstreamCompactArraySetArray:
		if len(typedUpd.Values) > len(crdt.data) { //We will use the argument slice as the new slice, avoiding an extra allocation.
			oldTsSlice := crdt.dataTsId
			crdt.expandTSOnly(int32(len(typedUpd.Values)))
			for i, existingV := range crdt.data {
				if typedUpd.TsId < crdt.dataTsId[i] { //Replace with existing value
					typedUpd.Values[i] = existingV
				} else {
					crdt.dataTsId[i] = typedUpd.TsId //Update timestamp
				}
			}
			effect = CompactArraySetArrayEffect{Values: crdt.data, TsId: oldTsSlice}
			crdt.data = typedUpd.Values //Re-use the argument slice as the new data slice
		} else { //First, search for the first position that will need to be replaced. If none is found, can return gracefully with no data allocation.
			i := 0
			for i = 0; i < len(typedUpd.Values); i++ {
				if crdt.dataTsId[i] < typedUpd.TsId {
					break
				}
			}
			if i == len(typedUpd.Values) { //No position could be replaced, so this is effectively a no-op.
				return
			}
			//Will need to copy as they will be used for the effect.
			copyData, copyDataTs := make([]any, len(crdt.data)), make([]uint64, len(crdt.data))
			copy(copyData, crdt.data)
			copy(copyDataTs, crdt.dataTsId)
			crdt.data[i], crdt.dataTsId[i] = typedUpd.Values[i], typedUpd.TsId
			for j := i + 1; j < len(typedUpd.Values); j++ {
				if crdt.dataTsId[j] < typedUpd.TsId {
					crdt.data[j], crdt.dataTsId[j] = typedUpd.Values[j], typedUpd.TsId
				}
			}
			effect = CompactArraySetArrayEffect{Values: copyData, TsId: copyDataTs}
		}

	case CompactArraySetSize:
		if int(typedUpd) > len(crdt.data) {
			effect = CompactArraySetSizeEffect(typedUpd)
		}
	}
	return
}

func (crdt *CompactArrayCrdt) expandArray(newSize int32) {
	newData, newTsId := make([]any, newSize), make([]uint64, newSize)
	copy(newData, crdt.data)
	copy(newTsId, crdt.dataTsId)
	crdt.data, crdt.dataTsId = newData, newTsId
}

func (crdt *CompactArrayCrdt) expandDataOnly(newSize int32) {
	newData := make([]any, newSize)
	copy(newData, crdt.data)
	crdt.data = newData
}

func (crdt *CompactArrayCrdt) expandTSOnly(newSize int32) {
	newTsId := make([]uint64, newSize)
	copy(newTsId, crdt.dataTsId)
	crdt.dataTsId = newTsId
}

func (crdt *CompactArrayCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *CompactArrayCrdt) Copy() (copyCRDT InversibleCRDT) {
	newData, newDataTsId := make([]any, len(crdt.data)), make([]uint64, len(crdt.dataTsId))
	copy(newData, crdt.data)
	copy(newDataTsId, crdt.dataTsId)
	newCRDT := CompactArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), data: newData, dataTsId: newDataTsId}
	return &newCRDT
}

func (crdt *CompactArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Most likely can do a small optimization to the one possible for Counters.
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *CompactArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *CompactArrayCrdt) undoEffect(effect Effect) {
	switch typedEffect := (effect).(type) {
	case CompactArraySetValueEffect:
		crdt.data[typedEffect.Pos], crdt.dataTsId[typedEffect.Pos] = typedEffect.Value, typedEffect.TsId
	case CompactArrayCounterEffect:
		crdt.data[typedEffect.Pos] = (crdt.data[typedEffect.Pos].(int64)) - int64(typedEffect.Change)
	case CompactArrayFloatEffect:
		crdt.data[typedEffect.Pos] = (crdt.data[typedEffect.Pos].(float64)) - typedEffect.Change
	case CompactArraySetArrayEffect:
		crdt.data, crdt.dataTsId = typedEffect.Values, typedEffect.TsId
	case CompactArraySetSizeEffect:
		crdt.data, crdt.dataTsId = crdt.data[:typedEffect], crdt.dataTsId[:typedEffect]
	}
}

func (crdt *CompactArrayCrdt) notifyRebuiltComplete(currTs clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp CompactArraySetSize) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CompactArraySetSize(protobuf.GetCompactarrayop().GetSetSize().GetSize())
}

func (crdtOp CompactArraySetSize) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{SetSize: &proto.ApbCompArraySetSize{Size: pb.Int32(int32(crdtOp))}}}}
}

func (crdtOp CompactArraySetValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayProto := protobuf.GetCompactarrayop().GetSetValue()
	return CompactArraySetValue{Value: any(arrayProto.GetData), Pos: arrayProto.GetIndex()}
}

func (crdtOp CompactArraySetValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	stringValue := crdtOp.Value.(string)
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{SetValue: &proto.ApbCompArraySetValue{Index: pb.Int32(crdtOp.Pos), Data: unsafe.Slice(unsafe.StringData(stringValue), len(stringValue))}}}}
}

func (crdtOp CompactArrayIncrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayProto := protobuf.GetCompactarrayop().GetIntInc()
	return CompactArrayIncrement{Change: arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (crdtOp CompactArrayIncrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{IntInc: &proto.ApbCompArrayIntInc{Index: pb.Int32(crdtOp.Pos), Inc: pb.Int32(crdtOp.Change)}}}}
}

func (crdtOp CompactArrayDecrement) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayProto := protobuf.GetCompactarrayop().GetIntInc()
	return CompactArrayDecrement{Change: -arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (crdtOp CompactArrayDecrement) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{IntInc: &proto.ApbCompArrayIntInc{Index: pb.Int32(crdtOp.Pos), Inc: pb.Int32(-crdtOp.Change)}}}}
}

func (crdtOp CompactArrayFloatInc) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayProto := protobuf.GetCompactarrayop().GetFloatInc()
	return CompactArrayFloatInc{Change: arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (crdtOp CompactArrayFloatInc) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{FloatInc: &proto.ApbCompArrayFloatInc{Index: pb.Int32(crdtOp.Pos), Inc: pb.Float64(crdtOp.Change)}}}}
}

func (crdtOp CompactArrayFloatDec) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayProto := protobuf.GetCompactarrayop().GetFloatInc()
	return CompactArrayFloatInc{Change: -arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (crdtOp CompactArrayFloatDec) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{FloatInc: &proto.ApbCompArrayFloatInc{Index: pb.Int32(crdtOp.Pos), Inc: pb.Float64(-crdtOp.Change)}}}}
}

func (crdtOp CompactArraySetArray) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return CompactArraySetArray(tools.CopyByteSliceToAnySlice(protobuf.GetCompactarrayop().GetSetArray().GetData()))
}

func (crdtOp CompactArraySetArray) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	bytesArray := tools.CopyAnySliceToByteSlice(crdtOp)
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Compactarrayop{Compactarrayop: &proto.ApbCompactArrayUpdate{SetArray: &proto.ApbCompArraySetArray{Data: bytesArray}}}}
}

func (crdtState CompactArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CompactArrayState(tools.CopyByteSliceToAnySlice(protobuf.GetCompactarray().GetData()))
}

func (crdtState CompactArrayState) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Compactarray{Compactarray: &proto.ApbGetArrayCompResp{Data: tools.CopyAnySliceToByteSlice(crdtState)}}}
}

func (crdtState CompactArraySingleString) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CompactArraySingleString(protobuf.GetPartread().GetCompactarray().GetStringValue().GetValue())
}

func (crdtState CompactArraySingleString) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Compactarray{Compactarray: &proto.ApbCompactArrayReadResp{
		StringValue: &proto.ApbCompactPosStringResp{Value: pb.String(string(crdtState))}}}}}}
}

func (crdtState CompactArraySingleCounter) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CompactArraySingleCounter(protobuf.GetPartread().GetCompactarray().GetIntValue().GetValue())
}

func (crdtState CompactArraySingleCounter) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Compactarray{Compactarray: &proto.ApbCompactArrayReadResp{
		IntValue: &proto.ApbCompactPosIntResp{Value: pb.Int64(int64(crdtState))}}}}}}
}

func (crdtState CompactArraySingleFloat) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CompactArraySingleFloat(protobuf.GetPartread().GetCompactarray().GetFloatValue().GetValue())
}

func (crdtState CompactArraySingleFloat) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Compactarray{Compactarray: &proto.ApbCompactArrayReadResp{
		FloatValue: &proto.ApbCompactPosFloatResp{Value: pb.Float64(float64(crdtState))}}}}}}
}

func (crdtState CompactArraySingleData) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CompactArraySingleData(protobuf.GetPartread().GetCompactarray().GetDataValue().GetValue())
}

func (crdtState CompactArraySingleData) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Compactarray{Compactarray: &proto.ApbCompactArrayReadResp{
		DataValue: &proto.ApbCompactPosDataResp{Value: crdtState}}}}}}
}

func (crdtState CompactArraySingleAny) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return CompactArraySingleAny{Value: protobuf.GetPartread().GetCompactarray().GetAnyValue().GetValue()}
}

func (crdtState CompactArraySingleAny) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	stringV := any(crdtState).(string)
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Compactarray{Compactarray: &proto.ApbCompactArrayReadResp{
		AnyValue: &proto.ApbCompactPosAnyResp{Value: unsafe.Slice(unsafe.StringData(stringV), len(stringV))}}}}}}
}

func (args CompactArraySingleAnyArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArraySingleAnyArguments(protobuf.GetCompactarray().GetPos().GetIndex())
}

func (args CompactArraySingleAnyArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType, index := proto.CA_Type_CA_ANY, pb.Int32(int32(args))
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Pos: &proto.ApbCompactArrayPosRead{Index: index, DataType: &dataType}}}}
}

func (args CompactArraySingleIntArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArraySingleIntArguments(protobuf.GetCompactarray().GetPos().GetIndex())
}

func (args CompactArraySingleIntArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType, index := proto.CA_Type_CA_INT, pb.Int32(int32(args))
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Pos: &proto.ApbCompactArrayPosRead{Index: index, DataType: &dataType}}}}
}

func (args CompactArraySingleFloatArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArraySingleFloatArguments(protobuf.GetCompactarray().GetPos().GetIndex())
}

func (args CompactArraySingleFloatArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType, index := proto.CA_Type_CA_FLOAT, pb.Int32(int32(args))
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Pos: &proto.ApbCompactArrayPosRead{Index: index, DataType: &dataType}}}}
}

func (args CompactArraySingleStringArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArraySingleStringArguments(protobuf.GetCompactarray().GetPos().GetIndex())
}

func (args CompactArraySingleStringArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType, index := proto.CA_Type_CA_DATA, pb.Int32(int32(args))
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Pos: &proto.ApbCompactArrayPosRead{Index: index, DataType: &dataType}}}}
}

func (args CompactArraySingleDataArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArraySingleDataArguments(protobuf.GetCompactarray().GetPos().GetIndex())
}

func (args CompactArraySingleDataArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	dataType, index := proto.CA_Type_CA_ANY, pb.Int32(int32(args))
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Pos: &proto.ApbCompactArrayPosRead{Index: index, DataType: &dataType}}}}
}

func (args CompactArrayExceptArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArrayExceptArguments(protobuf.GetCompactarray().GetExcept().GetIndex())
}

func (args CompactArrayExceptArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Except: &proto.ApbCompactArrayExceptRead{Index: pb.Int32(int32(args))}}}}
}

func (args CompactArrayRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	rangeProto := protobuf.GetCompactarray().GetRange()
	return CompactArrayRangeArguments{From: rangeProto.GetFrom(), To: rangeProto.GetTo()}
}

func (args CompactArrayRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Range: &proto.ApbCompactArrayRangeRead{From: pb.Int32(int32(args.From)), To: pb.Int32(int32(args.To))}}}}
}

func (args CompactArraySubArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return CompactArraySubArguments(protobuf.GetCompactarray().GetSub().GetIndexes())
}

func (args CompactArraySubArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Compactarray{Compactarray: &proto.ApbCompactArrayPartialRead{Sub: &proto.ApbCompactArraySubRead{Indexes: args}}}}
}

func (downOp CompactArraySetSize) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return CompactArraySetSize(protobuf.GetCompactArrayOp().GetSize().GetSize())
}

func (downOp CompactArraySetSize) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{Size: &proto.ProtoCompactArraySetSize{Size: pb.Int32(int32(downOp))}}}}
}

func (downOp DownstreamCompactArraySetValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp().GetSetValue()
	return DownstreamCompactArraySetValue{Value: any(arrayProto.GetValue()), Pos: arrayProto.GetIndex(), TsId: arrayProto.GetTsId()}
}

func (downOp DownstreamCompactArraySetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{
		SetValue: &proto.ProtoCompactArraySetValue{Value: []byte(downOp.Value.(string)), Index: pb.Int32(downOp.Pos), TsId: pb.Uint64(downOp.TsId)}}}}
}

func (downOp CompactArrayIncrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp().GetIntInc()
	return CompactArrayIncrement{Change: arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (downOp CompactArrayIncrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{
		IntInc: &proto.ProtoCompactArrayIntInc{Inc: pb.Int32(downOp.Change), Index: pb.Int32(downOp.Pos)}}}}
}

func (downOp CompactArrayDecrement) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp().GetIntInc()
	return CompactArrayDecrement{Change: -arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (downOp CompactArrayDecrement) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{
		IntInc: &proto.ProtoCompactArrayIntInc{Inc: pb.Int32(-downOp.Change), Index: pb.Int32(downOp.Pos)}}}}
}

func (downOp CompactArrayFloatInc) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp().GetFloatInc()
	return CompactArrayFloatInc{Change: arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (downOp CompactArrayFloatInc) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{
		FloatInc: &proto.ProtoCompactArrayFloatInc{Inc: pb.Float64(downOp.Change), Index: pb.Int32(downOp.Pos)}}}}
}

func (downOp CompactArrayFloatDec) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp().GetFloatInc()
	return CompactArrayFloatDec{Change: -arrayProto.GetInc(), Pos: arrayProto.GetIndex()}
}

func (downOp CompactArrayFloatDec) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{
		FloatInc: &proto.ProtoCompactArrayFloatInc{Inc: pb.Float64(-downOp.Change), Index: pb.Int32(downOp.Pos)}}}}
}

func (downOp DownstreamCompactArraySetArray) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetCompactArrayOp().GetSetArray()
	return DownstreamCompactArraySetArray{Values: tools.CopyByteSliceToAnySlice(arrayProto.GetData()), TsId: arrayProto.GetTsId()}
}

func (downOp DownstreamCompactArraySetArray) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CompactArrayOp{CompactArrayOp: &proto.ProtoCompactArrayDownstream{
		SetArray: &proto.ProtoCompactArraySetArray{Data: tools.CopyAnySliceToByteSlice(downOp.Values), TsId: pb.Uint64(downOp.TsId)}}}}
}

func (crdt CompactArrayCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{State: &proto.ProtoState_CompactArray{CompactArray: &proto.ProtoCompactArrayState{
		Data: tools.CopyAnySliceToByteSlice(crdt.data), TsId: crdt.dataTsId}}}
}

func (crdt CompactArrayCrdt) FromProtoState(proto *proto.ProtoState, ts clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	protoState := proto.GetCompactArray()
	return (&CompactArrayCrdt{data: tools.CopyByteSliceToAnySlice(protoState.GetData()), dataTsId: protoState.GetTsId()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *CompactArrayCrdt) GetCRDT() CRDT { return crdt }
