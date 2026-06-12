package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	tools "github.com/AndreRijo/go-tools/src/tools"

	pb "google.golang.org/protobuf/proto"
)

// This CRDT is intended when data needs to be represented compactly, sacrificing performance for memory.
// As such, this CRDT also avoids memory allocation whenever possible - namely, StringArraySetArray will replace the existing array if its size is >= than the one in the CRDT.
// This CRDT supports both counters and registers as the value for each data position
type StringArrayCrdt struct {
	CRDTVM
	data     []string //This is better (memory-wise) than slice of []byte, and same as []string. Even when accounting for memory used by each position
	dataTsId []uint64 //The highest 48 bits correspond to the lowest 48 bits of a 64-bit timestamp; the lowest 16 bits are used for replicaID.
	//We will access replicaID from the shared variable.
}

type StringArrayState []string

// States
type StringArraySingleState string

//Reads

// Position
type StringArraySingleArguments int32

// Positions
type StringArrayExceptArguments int32
type StringArrayRangeArguments struct{ From, To int32 }
type StringArraySubArguments []int32

// Updates
type StringArrayUpd interface { //Helps identify what array size is required to apply this update.
	UpdateArguments
	GetMinSize() int
	IsMultiPos() bool
}

type StringArraySetSize int32
type StringArraySetArray []string           //NOTE: This slice will be used directly in the CRDT, unless its size is < than the one in the CRDT.
type StringArraySetArrayInitialize []string //Same as above, but does not use timestamps. Thus, it must only be called once and come before any other operation.
type StringArraySetValue struct {
	Value string
	Pos   int32
}

// Downstreams.
type DownstreamStringArraySetArray struct {
	Values []string
	TsId   uint64
}

type DownstreamStringArraySetValue struct {
	Value string
	Pos   int32
	TsId  uint64
}

//Effects
//Note: For storage efficiency reasons, we do not roll back the size of the array unless it was set with SetSize.

type StringArraySetValueEffect DownstreamStringArraySetValue
type StringArraySetArrayEffect struct {
	Values []string
	TsId   []uint64
}
type StringArraySetSizeEffect StringArraySetSize

//

func (crdt *StringArrayCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_STRING }
func (crdt *StringArrayCrdt) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

func (args StringArraySetSize) GetCRDTType() proto.CRDTType  { return proto.CRDTType_ARRAY_STRING }
func (args StringArraySetArray) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_STRING }
func (args StringArraySetValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_STRING }
func (args StringArraySetArrayInitialize) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}

func (args DownstreamStringArraySetArray) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (args DownstreamStringArraySetValue) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (args StringArraySetSize) GetDATAType() proto.DATAType            { return proto.DATAType_DEFAULT }
func (args StringArraySetArray) GetDATAType() proto.DATAType           { return proto.DATAType_DEFAULT }
func (args StringArraySetValue) GetDATAType() proto.DATAType           { return proto.DATAType_DEFAULT }
func (args StringArraySetArrayInitialize) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamStringArraySetArray) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args DownstreamStringArraySetValue) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

func (args StringArraySetArrayInitialize) MustReplicate() bool { return true }
func (args DownstreamStringArraySetArray) MustReplicate() bool { return true }
func (args DownstreamStringArraySetValue) MustReplicate() bool { return true }
func (args StringArraySetSize) MustReplicate() bool            { return true }
func (args StringArraySetArray) GetMinSize() int               { return len(args) }
func (args StringArraySetArrayInitialize) GetMinSize() int     { return len(args) }
func (args StringArraySetValue) GetMinSize() int               { return int(args.Pos + 1) }
func (args StringArraySetSize) GetMinSize() int                { return int(args) }
func (args DownstreamStringArraySetArray) GetMinSize() int     { return len(args.Values) }
func (args DownstreamStringArraySetValue) GetMinSize() int     { return int(args.Pos + 1) }
func (args StringArraySetArray) IsMultiPos() bool              { return true }
func (args StringArraySetArrayInitialize) IsMultiPos() bool    { return true }
func (args StringArraySetValue) IsMultiPos() bool              { return false }
func (args StringArraySetSize) IsMultiPos() bool               { return false }
func (args DownstreamStringArraySetArray) IsMultiPos() bool    { return true }
func (args DownstreamStringArraySetValue) IsMultiPos() bool    { return false }

func (state StringArrayState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ARRAY_STRING }
func (state StringArrayState) GetREADType() proto.READType { return proto.READType_FULL }
func (state StringArrayState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (state StringArraySingleState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (state StringArraySingleState) GetREADType() proto.READType {
	return proto.READType_ARRAY_STRING_POS
}
func (state StringArraySingleState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }

func (args StringArraySingleArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (args StringArrayExceptArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (args StringArrayRangeArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (args StringArraySubArguments) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_ARRAY_STRING
}
func (args StringArraySingleArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_STRING_POS
}
func (args StringArrayExceptArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_STRING_EXCEPT
}
func (args StringArrayRangeArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_STRING_RANGE
}
func (args StringArraySubArguments) GetREADType() proto.READType {
	return proto.READType_ARRAY_STRING_RANGE
}
func (args StringArraySingleArguments) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args StringArrayExceptArguments) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args StringArrayRangeArguments) GetDATAType() proto.DATAType  { return proto.DATAType_DEFAULT }
func (args StringArraySubArguments) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args StringArraySingleArguments) HasInnerReads() bool         { return false }
func (args StringArrayExceptArguments) HasInnerReads() bool         { return false }
func (args StringArrayRangeArguments) HasInnerReads() bool          { return false }
func (args StringArraySubArguments) HasInnerReads() bool            { return false }
func (args StringArraySingleArguments) HasVariables() bool          { return false }
func (args StringArrayExceptArguments) HasVariables() bool          { return false }
func (args StringArrayRangeArguments) HasVariables() bool           { return false }
func (args StringArraySubArguments) HasVariables() bool             { return false }

func (crdt *StringArrayCrdt) Initialize(startTs clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
	//return &StringArrayCrdt{CRDTVM: (&genericInversibleCRDT{}).initialize(crdt)
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *StringArrayCrdt) initializeFromSnapshot(startTs clocksi.Timestamp, replicaID uint16) (sameCRDT *StringArrayCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *StringArrayCrdt) IsBigCRDT() bool {
	return len(crdt.data) >= 500
}

// For reads it is more OK to do copies of data as needed.
func (crdt *StringArrayCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArgs := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case StringArraySingleArguments:
		return crdt.getPos(updsNotYetApplied, int32(typedArgs))
	case StringArrayExceptArguments:
		return crdt.getExcept(updsNotYetApplied, int32(typedArgs))
	case StringArrayRangeArguments:
		return crdt.getRange(updsNotYetApplied, typedArgs.From, typedArgs.To)
	case StringArraySubArguments:
		return crdt.getSub(updsNotYetApplied, []int32(typedArgs))
	default:
		fmt.Printf("[StringArrayCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *StringArrayCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	tmpCopy := copySlice(crdt.data)
	if len(updsNotYetApplied) == 0 {
		return StringArrayState(tmpCopy)
	}
	for _, upd := range updsNotYetApplied {
		stringUpd, ok := upd.(StringArrayUpd)
		if !ok {
			continue
		}
		if stringUpd.GetMinSize() > len(crdt.data) {
			tmpCopy = copySliceWithSize(tmpCopy, int32(stringUpd.GetMinSize()))
		}
		switch typedUpd := upd.(type) {
		case StringArraySetValue:
			tmpCopy[typedUpd.Pos] = typedUpd.Value
		case StringArraySetArray:
			copy(tmpCopy, typedUpd) //SetSize can be ignored as we already resized.
		}
	}
	return StringArrayState(tmpCopy)
}

func (crdt *StringArrayCrdt) getPos(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return StringArraySingleState(crdt.data[pos])
	}
	var value string
	if pos < int32(len(crdt.data)) {
		value = crdt.data[pos]
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case StringArraySetValue:
			if typedUpd.Pos == pos {
				value = typedUpd.Value
			}
		case StringArraySetArray:
			if int32(len(typedUpd)) > pos {
				value = typedUpd[pos]
			}
		}
	}
	return StringArraySingleState(value)
}

func (crdt *StringArrayCrdt) getExcept(updsNotYetApplied []UpdateArguments, pos int32) (state State) {
	var result []string
	if int(pos) >= len(crdt.data) {
		result = copySlice(crdt.data)
	} else {
		result = make([]string, len(crdt.data)-1)
		copy(result, crdt.data[:pos])
		copy(result[pos:], crdt.data[pos+1:])
	}
	if len(updsNotYetApplied) == 0 {
		return StringArrayState(result)
	}

	for _, upd := range updsNotYetApplied {
		stringUpd, ok := upd.(StringArrayUpd)
		if !ok {
			continue
		}
		if len(result) >= int(pos) && stringUpd.GetMinSize()-1 > len(result) { //We already skipped pos
			result = copySliceWithSize(result, int32(stringUpd.GetMinSize()-1))
		} else if stringUpd.GetMinSize() > len(result) && stringUpd.GetMinSize() > len(result) { //Still not reaching pos
			result = copySliceWithSize(result, int32(stringUpd.GetMinSize()))
		} else if stringUpd.GetMinSize() > len(result) && stringUpd.GetMinSize() >= int(pos) { //Now we will skip pos
			result = copySliceWithSize(result, int32(stringUpd.GetMinSize()-1))
		}

		switch typedUpd := upd.(type) {
		case StringArraySetValue:
			if typedUpd.Pos < pos {
				result[typedUpd.Pos] = typedUpd.Value
			} else if typedUpd.Pos > pos {
				result[typedUpd.Pos-1] = typedUpd.Value
			}
		case StringArraySetArray:
			if len(typedUpd) < int(pos) { //Direct copy
				copy(result, typedUpd)
			} else {
				copy(result, typedUpd[:pos])
				copy(result[pos:], typedUpd[pos+1:])
			}
		}
	}
	return StringArrayState(result)
}

func (crdt *StringArrayCrdt) getRange(updsNotYetApplied []UpdateArguments, from, to int32) (state State) {
	if len(updsNotYetApplied) == 0 {
		return StringArrayState(copySlice(getRangeOfSlice(crdt.data, int(from), int(to))))
	}

	result := make([]string, to-from) //We make an entire range slice, optimistically
	copy(result, getRangeOfSlice(crdt.data, int(from), int(to)))
	currMax := int32(len(crdt.data))

	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case StringArraySetValue:
			if typedUpd.Pos >= from && typedUpd.Pos < to {
				result[typedUpd.Pos-from] = typedUpd.Value
				currMax = tools.Max(currMax, typedUpd.Pos+1)
			}
		case StringArraySetArray:
			for i := from; i <= to && i < int32(len(typedUpd)); i++ {
				result[i-from] = typedUpd[i]
			}
			currMax = tools.Max(currMax, int32(len(typedUpd)))
		}
	}

	currMax -= from
	result = result[:currMax] //if currMax == to, it does nothing.
	return StringArrayState(result)
}

func (crdt *StringArrayCrdt) getSub(updsNotYetApplied []UpdateArguments, positions []int32) (state State) {
	result := make([]string, len(positions))

	if len(updsNotYetApplied) == 0 {
		lenInt32 := int32(len(crdt.data))
		for i, pos := range positions {
			if pos < lenInt32 {
				result[i] = crdt.data[pos]
			} else {
				result[i] = "" //Out of bounds
			}
		}
		return StringArrayState(result)
	}

	tmpCopy := copySlice(crdt.data)
	for _, upd := range updsNotYetApplied {
		stringUpd, ok := upd.(StringArrayUpd)
		if !ok {
			continue
		}
		if stringUpd.GetMinSize() > len(crdt.data) {
			tmpCopy = copySliceWithSize(tmpCopy, int32(stringUpd.GetMinSize()))
		}
		switch typedUpd := upd.(type) {
		case StringArraySetValue:
			tmpCopy[typedUpd.Pos] = typedUpd.Value
		case StringArraySetArray:
			copy(tmpCopy, typedUpd)
		}
	}

	lenInt32 := int32(len(result))
	for i, pos := range positions {
		if pos < lenInt32 {
			result[i] = tmpCopy[pos]
		} else {
			result[i] = ""
		}
	}
	return StringArrayState(result)
}

func (crdt *StringArrayCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch typedArgs := args.(type) {

	case StringArraySetValue:
		tsId := generate64BitTsAndId(int64(shared.ReplicaID))
		return DownstreamStringArraySetValue{Value: typedArgs.Value, Pos: typedArgs.Pos, TsId: tsId}

	case StringArraySetArray:
		tsId := generate64BitTsAndId(int64(shared.ReplicaID))
		return DownstreamStringArraySetArray{Values: typedArgs, TsId: tsId}

	case StringArraySetArrayInitialize:
		return typedArgs

	case StringArraySetSize:
		return typedArgs

	case MultiUpd:
		multiDowns := make(MultiUpd, len(typedArgs))
		for i, innerUpd := range typedArgs {
			multiDowns[i] = crdt.Update(innerUpd)
		}
		return multiDowns
	default:
		fmt.Printf("[StringArray][Update]Unknown update type: %v (%T)\n", args, args)
	}

	return nil
}

func (crdt *StringArrayCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	if multiUpd, ok := downstreamArgs.(MultiUpd); ok {
		for _, upd := range multiUpd {
			crdt.Downstream(updTs, upd.(DownstreamArguments))
		}
		return nil
	}
	effect := crdt.applyDownstream(downstreamArgs)
	crdt.addToHistory(updTs, downstreamArgs, effect) //Necessary for inversibleCrdt
	return nil
}

func (crdt *StringArrayCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect Effect) {
	effect = NoEffect{}

	stringArrayUpd, ok := downstreamArgs.(StringArrayUpd)
	if !ok {
		fmt.Printf("[StringArray][Downstream]Unsupported downstream type %v (%T)\n", downstreamArgs, downstreamArgs)
		return
	}
	if stringArrayUpd.GetMinSize() > len(crdt.data) && !stringArrayUpd.IsMultiPos() {
		crdt.expandArray(int32(stringArrayUpd.GetMinSize()))
	}

	switch typedUpd := downstreamArgs.(type) {

	case DownstreamStringArraySetValue:
		if crdt.dataTsId[typedUpd.Pos] > typedUpd.TsId { //Cannot apply
			return
		}
		effect = StringArraySetValueEffect{Value: crdt.data[typedUpd.Pos], Pos: typedUpd.Pos, TsId: crdt.dataTsId[typedUpd.Pos]}
		crdt.data[typedUpd.Pos], crdt.dataTsId[typedUpd.Pos] = typedUpd.Value, typedUpd.TsId
	case DownstreamStringArraySetArray:
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
			effect = StringArraySetArrayEffect{Values: crdt.data, TsId: oldTsSlice}
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
			copyData, copyDataTs := make([]string, len(crdt.data)), make([]uint64, len(crdt.data))
			copy(copyData, crdt.data)
			copy(copyDataTs, crdt.dataTsId)
			crdt.data[i], crdt.dataTsId[i] = typedUpd.Values[i], typedUpd.TsId
			for j := i + 1; j < len(typedUpd.Values); j++ {
				if crdt.dataTsId[j] < typedUpd.TsId {
					crdt.data[j], crdt.dataTsId[j] = typedUpd.Values[j], typedUpd.TsId
				}
			}
			return StringArraySetArrayEffect{Values: copyData, TsId: copyDataTs}
		}

	case StringArraySetArrayInitialize: //Only once per CRDT, and before any other update.
		crdt.data = typedUpd

	case StringArraySetSize:
		if int(typedUpd) > len(crdt.data) {
			return StringArraySetSizeEffect(typedUpd)
		}

	default:
		fmt.Printf("[StringArrayCrdt]Unsupported downstream type: %T\n", downstreamArgs)
	}
	return
}

func (crdt *StringArrayCrdt) expandArray(newSize int32) {
	newData, newTsId := make([]string, newSize), make([]uint64, newSize)
	copy(newData, crdt.data)
	copy(newTsId, crdt.dataTsId)
	crdt.data, crdt.dataTsId = newData, newTsId
}

func (crdt *StringArrayCrdt) expandDataOnly(newSize int32) {
	newData := make([]string, newSize)
	copy(newData, crdt.data)
	crdt.data = newData
}

func (crdt *StringArrayCrdt) expandTSOnly(newSize int32) {
	newTsId := make([]uint64, newSize)
	copy(newTsId, crdt.dataTsId)
	crdt.dataTsId = newTsId
}

func (crdt *StringArrayCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *StringArrayCrdt) Copy() (copyCRDT InversibleCRDT) {
	newData, newDataTsId := make([]string, len(crdt.data)), make([]uint64, len(crdt.dataTsId))
	copy(newData, crdt.data)
	copy(newDataTsId, crdt.dataTsId)
	newCRDT := StringArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), data: newData}
	if crdt.dataTsId == nil {
		newCRDT.dataTsId = newDataTsId
	}
	//newCRDT := StringArrayCrdt{CRDTVM: crdt.CRDTVM.copy(), data: newData, dataTsId: newDataTsId}
	return &newCRDT
}

func (crdt *StringArrayCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Most likely can do a small optimization to the one possible for Counters.
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *StringArrayCrdt) reapplyOp(updArgs DownstreamArguments) (effect Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *StringArrayCrdt) undoEffect(effect Effect) {
	switch typedEffect := (effect).(type) {
	case StringArraySetValueEffect:
		crdt.data[typedEffect.Pos], crdt.dataTsId[typedEffect.Pos] = typedEffect.Value, typedEffect.TsId
	case StringArraySetArrayEffect:
		crdt.data, crdt.dataTsId = typedEffect.Values, typedEffect.TsId
	case StringArraySetSizeEffect:
		crdt.data, crdt.dataTsId = crdt.data[:typedEffect], crdt.dataTsId[:typedEffect]
	}
}

func (crdt *StringArrayCrdt) notifyRebuiltComplete(currTs clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp StringArraySetSize) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return StringArraySetSize(protobuf.GetStringarrayop().GetSetSize().GetSize())
}

func (crdtOp StringArraySetSize) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Stringarrayop{Stringarrayop: &proto.ApbStringArrayUpdate{SetSize: &proto.ApbStringArraySetSize{Size: pb.Int32(int32(crdtOp))}}}}
}

func (crdtOp StringArraySetValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	arrayProto := protobuf.GetStringarrayop().GetSetValue()
	return StringArraySetValue{Value: arrayProto.GetData(), Pos: arrayProto.GetIndex()}
}

func (crdtOp StringArraySetValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Stringarrayop{Stringarrayop: &proto.ApbStringArrayUpdate{SetValue: &proto.ApbStringArraySetValue{Index: &crdtOp.Pos, Data: &crdtOp.Value}}}}
}

func (crdtOp StringArraySetArray) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return StringArraySetArray(protobuf.GetStringarrayop().GetSetArray().GetData())
}

func (crdtOp StringArraySetArray) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Stringarrayop{Stringarrayop: &proto.ApbStringArrayUpdate{SetArray: &proto.ApbStringArraySetArray{Data: crdtOp}}}}
}

func (crdtOp StringArraySetArrayInitialize) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return StringArraySetArrayInitialize(protobuf.GetStringarrayop().GetSetArrayInit().GetData())
}

func (crdtOp StringArraySetArrayInitialize) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Stringarrayop{Stringarrayop: &proto.ApbStringArrayUpdate{SetArrayInit: &proto.ApbStringArraySetArrayInit{Data: crdtOp}}}}
}

func (crdtState StringArrayState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return StringArrayState(protobuf.GetStringarray().GetData())
}

func (crdtState StringArrayState) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Stringarray{Stringarray: &proto.ApbGetArrayStringResp{Data: crdtState}}}
}

func (crdtState StringArraySingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return StringArraySingleState(protobuf.GetPartread().GetStringarray().GetValue())
}

func (crdtState StringArraySingleState) ToReadResp(buf *BufsToReturnToPool) (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Partread{Partread: &proto.ApbPartialReadResp{Reply: &proto.ApbPartialReadResp_Stringarray{
		Stringarray: &proto.ApbStringArrayReadResp{Value: pb.String(string(crdtState))}}}}}
}

func (args StringArraySingleArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return StringArraySingleArguments(protobuf.GetStringarray().GetPos().GetIndex())
}

func (args StringArraySingleArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Stringarray{Stringarray: &proto.ApbStringArrayPartialRead{Pos: &proto.ApbStringArrayPosRead{Index: pb.Int32(int32(args))}}}}
}

func (args StringArrayExceptArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return StringArrayExceptArguments(protobuf.GetStringarray().GetExcept().GetIndex())
}

func (args StringArrayExceptArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Stringarray{Stringarray: &proto.ApbStringArrayPartialRead{Except: &proto.ApbStringArrayExceptRead{Index: pb.Int32(int32(args))}}}}
}

func (args StringArrayRangeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	rangeProto := protobuf.GetStringarray().GetRange()
	return StringArrayRangeArguments{From: rangeProto.GetFrom(), To: rangeProto.GetTo()}
}

func (args StringArrayRangeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Stringarray{Stringarray: &proto.ApbStringArrayPartialRead{Range: &proto.ApbStringArrayRangeRead{From: pb.Int32(int32(args.From)), To: pb.Int32(int32(args.To))}}}}
}

func (args StringArraySubArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return StringArraySubArguments(protobuf.GetStringarray().GetSub().GetIndexes())
}

func (args StringArraySubArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Args: &proto.ApbPartialReadArgs_Stringarray{Stringarray: &proto.ApbStringArrayPartialRead{Sub: &proto.ApbStringArraySubRead{Indexes: args}}}}
}

func (downOp StringArraySetSize) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return StringArraySetSize(protobuf.GetStringArrayOp().GetSize().GetSize())
}

func (downOp StringArraySetSize) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_StringArrayOp{StringArrayOp: &proto.ProtoStringArrayDownstream{Size: &proto.ProtoStringArraySetSize{Size: pb.Int32(int32(downOp))}}}}
}

func (downOp DownstreamStringArraySetValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetStringArrayOp().GetSetValue()
	return DownstreamStringArraySetValue{Value: arrayProto.GetValue(), Pos: arrayProto.GetIndex(), TsId: arrayProto.GetTsId()}
}

func (downOp DownstreamStringArraySetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_StringArrayOp{StringArrayOp: &proto.ProtoStringArrayDownstream{
		SetValue: &proto.ProtoStringArraySetValue{Value: &downOp.Value, Index: &downOp.Pos, TsId: &downOp.TsId}}}}
}

func (downOp DownstreamStringArraySetArray) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetStringArrayOp().GetSetArray()
	return DownstreamStringArraySetArray{Values: arrayProto.GetData(), TsId: arrayProto.GetTsId()}
}

func (downOp DownstreamStringArraySetArray) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_StringArrayOp{StringArrayOp: &proto.ProtoStringArrayDownstream{
		SetArray: &proto.ProtoStringArraySetArray{Data: downOp.Values, TsId: &downOp.TsId}}}}
}

func (downOp StringArraySetArrayInitialize) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	arrayProto := protobuf.GetStringArrayOp().GetSetArray()
	return DownstreamStringArraySetArray{Values: arrayProto.GetData(), TsId: arrayProto.GetTsId()}
}

func (downOp StringArraySetArrayInitialize) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_StringArrayOp{StringArrayOp: &proto.ProtoStringArrayDownstream{
		SetArrayInit: &proto.ProtoStringArraySetArrayInit{Data: downOp}}}}
}

func (crdt StringArrayCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{State: &proto.ProtoState_StringArray{StringArray: &proto.ProtoStringArrayState{Data: crdt.data, TsId: crdt.dataTsId}}}
}

func (crdt StringArrayCrdt) FromProtoState(proto *proto.ProtoState, ts clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	protoState := proto.GetStringArray()
	return (&StringArrayCrdt{data: protoState.GetData(), dataTsId: protoState.GetTsId()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *StringArrayCrdt) GetCRDT() CRDT { return crdt }
